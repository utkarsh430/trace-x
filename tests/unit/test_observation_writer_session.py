"""The observation log's writer session as a state machine, with fakes (ADR-0051; plan §4.1).

The database half -- the lock, the row, the grants and the trigger -- is proved against a real
PostgreSQL in tests/integration/test_producer_sessions.py.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

import pytest

from trace_core.observation.session import (
    WRITER_LOCK_KEY,
    SessionState,
    WriterSession,
    WriterSessionError,
)

pytestmark = pytest.mark.unit


@dataclass
class FakeLock:
    held_elsewhere: bool = False
    held: bool = False
    fail: bool = False
    keys: list[int] = field(default_factory=list)

    def try_acquire(self, key: int) -> bool:
        self.keys.append(key)
        self.held = not self.held_elsewhere
        return self.held

    def still_held(self, key: int) -> bool:
        if self.fail:
            raise ConnectionError("database unreachable")
        return self.held


@dataclass
class FakeLedger:
    open_rows: dict[str, tuple[str, str]] = field(default_factory=dict)
    closed: dict[str, int] = field(default_factory=dict)
    row_open: bool = True

    def open(self, *, session_id: str, producer: str, instance_id: str) -> None:
        self.open_rows[session_id] = (producer, instance_id)

    def heartbeat(self, session_id: str) -> bool:
        return self.row_open and session_id in self.open_rows

    def close(self, session_id: str, *, last_seq: int) -> bool:
        if session_id in self.closed:
            return False
        self.closed[session_id] = last_seq
        return True


def _session(lock: FakeLock | None = None, ledger: FakeLedger | None = None) -> WriterSession:
    return WriterSession(
        ledger=ledger or FakeLedger(),
        lock=lock or FakeLock(),
        producer="trace-gateway@0.1.0",
        instance_id="gw-test",
        new_session_id=lambda: "s-1",
    )


def test_starting_takes_the_lock_opens_a_row_and_numbers_from_one() -> None:
    lock, ledger = FakeLock(), FakeLedger()
    session = _session(lock, ledger)
    assert not session.ready
    assert session.start()
    assert session.ready and session.state is SessionState.ACTIVE
    assert lock.keys == [WRITER_LOCK_KEY]
    assert ledger.open_rows == {"s-1": ("trace-gateway@0.1.0", "gw-test")}
    assert [session.next_seq() for _ in range(3)] == [1, 2, 3]
    assert session.last_seq == 3


def test_a_lock_held_elsewhere_leaves_the_process_not_ready_and_opens_nothing() -> None:
    ledger = FakeLedger()
    session = _session(FakeLock(held_elsewhere=True), ledger)
    assert not session.start()
    assert not session.ready
    assert ledger.open_rows == {}
    with pytest.raises(WriterSessionError):
        session.next_seq()


@pytest.mark.parametrize("failure", ["lock_lost", "row_closed", "unreachable"])
def test_any_failure_to_confirm_the_fence_loses_the_session_at_once(failure: str) -> None:
    lock, ledger = FakeLock(), FakeLedger()
    session = _session(lock, ledger)
    session.start()
    session.next_seq()
    assert session.heartbeat()
    if failure == "lock_lost":
        lock.held = False
    elif failure == "row_closed":
        ledger.row_open = False
    else:
        lock.fail = True
    assert not session.heartbeat()
    assert session.state is SessionState.LOST and not session.ready
    with pytest.raises(WriterSessionError):
        session.next_seq()
    assert not session.heartbeat()
    assert not session.close(confirmed=True), "a lost session never closes"
    assert ledger.closed == {}


def test_a_confirmed_flush_closes_with_the_highest_number_assigned() -> None:
    ledger = FakeLedger()
    session = _session(ledger=ledger)
    session.start()
    for _ in range(5):
        session.next_seq()
    assert session.close(confirmed=True)
    assert session.state is SessionState.CLOSED
    assert ledger.closed == {"s-1": 5}
    with pytest.raises(WriterSessionError):
        session.next_seq()


def test_an_unconfirmed_flush_leaves_the_session_unclosed_and_stops_writing() -> None:
    ledger = FakeLedger()
    session = _session(ledger=ledger)
    session.start()
    session.next_seq()
    assert not session.close(confirmed=False)
    assert ledger.closed == {}
    assert session.state is SessionState.LOST
    with pytest.raises(WriterSessionError):
        session.next_seq()


def test_a_session_starts_once() -> None:
    session = _session()
    session.start()
    with pytest.raises(WriterSessionError):
        session.start()


def test_concurrent_callers_receive_every_number_exactly_once() -> None:
    session = _session()
    session.start()
    numbers: list[int] = []
    guard = threading.Lock()

    def take() -> None:
        for _ in range(500):
            value = session.next_seq()
            with guard:
                numbers.append(value)

    threads = [threading.Thread(target=take) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(numbers) == list(range(1, 4_001))
