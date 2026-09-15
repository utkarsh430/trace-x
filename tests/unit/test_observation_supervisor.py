"""The writer supervisor with fakes: acquire, prepare, keep, lose, take over, stop (ADR-0051 §2).

The clock is injected, so the lease and the takeover grace are exercised exactly, without sleeping.
The PostgreSQL half runs in tests/integration/test_producer_sessions.py, and a backend terminated
under a writer in tests/chaos/test_writer_fence.py.
"""

from __future__ import annotations

import itertools
import threading
from dataclasses import dataclass, field

import pytest

from trace_core.observation.session import WriterSession, WriterSessionError
from trace_core.observation.supervisor import WriterSupervisor

pytestmark = pytest.mark.unit

INTERVAL_S, LEASE_S, GRACE_S = 2.0, 6.0, 8.0
START = 1_000.0


class Clock:
    def __init__(self) -> None:
        self.now = START

    def __call__(self) -> float:
        return self.now


@dataclass
class Row:
    heartbeat_at: float
    closed: bool = False
    last_seq: int | None = None


@dataclass
class Database:
    """One PostgreSQL: which connection's backend holds the lock, and the session rows."""

    clock: Clock
    holder: int | None = None
    rows: dict[str, Row] = field(default_factory=dict)
    reachable: bool = True
    predecessor_unknown: bool = False
    ids: itertools.count[int] = field(default_factory=lambda: itertools.count(1))


@dataclass
class Conn:
    db: Database
    number: int

    def close(self) -> None:
        if self.db.holder == self.number:
            self.db.holder = None


@dataclass
class Lock:
    conn: Conn

    def try_acquire(self, key: int) -> bool:
        if self.conn.db.holder in (None, self.conn.number):
            self.conn.db.holder = self.conn.number
            return True
        return False

    def still_held(self, key: int) -> bool:
        return self.conn.db.holder == self.conn.number


@dataclass
class Ledger:
    db: Database

    def open(self, *, session_id: str, producer: str, instance_id: str) -> None:
        self.db.rows[session_id] = Row(heartbeat_at=self.db.clock())

    def heartbeat(self, session_id: str) -> bool:
        row = self.db.rows.get(session_id)
        if row is None or row.closed:
            return False
        row.heartbeat_at = self.db.clock()
        return True

    def close(self, session_id: str, *, last_seq: int) -> bool:
        row = self.db.rows[session_id]
        if row.closed:
            return False
        row.closed, row.last_seq = True, last_seq
        return True

    def others_live(self, session_id: str, *, within_s: float) -> bool:
        if self.db.predecessor_unknown:
            raise ConnectionError("database unreachable")
        now = self.db.clock()
        return any(
            not row.closed and now - row.heartbeat_at < within_s
            for other, row in self.db.rows.items()
            if other != session_id
        )


def _supervisor(
    db: Database,
    prepared: list[str] | None = None,
    *,
    name: str = "gw",
    fail_prepare: list[bool] | None = None,
    interval_s: float = INTERVAL_S,
    lease_s: float = LEASE_S,
    grace_s: float = GRACE_S,
) -> WriterSupervisor:
    def connect() -> Conn:
        if not db.reachable:
            raise ConnectionError("database unreachable")
        return Conn(db, next(db.ids))

    def factory(conn: Conn) -> WriterSession:
        return WriterSession(
            ledger=Ledger(db),
            lock=Lock(conn),
            producer="trace-gateway@0.1.0",
            instance_id=name,
            new_session_id=lambda: f"s-{next(db.ids)}",
        )

    def on_acquired() -> None:
        if fail_prepare and fail_prepare.pop(0):
            raise RuntimeError("store unreachable")
        if prepared is not None:
            prepared.append(name)

    return WriterSupervisor(
        connect=connect,
        producer="trace-gateway@0.1.0",
        instance_id=name,
        on_acquired=on_acquired,
        interval_s=interval_s,
        lease_s=lease_s,
        takeover_grace_s=grace_s,
        clock=db.clock,
        session_factory=factory,
    )


def test_a_first_writer_acquires_prepares_and_is_ready_at_once() -> None:
    db, prepared = Database(Clock()), list[str]()
    supervisor = _supervisor(db, prepared)
    assert not supervisor.ready and supervisor.status == "not started"
    supervisor.tick()
    assert supervisor.ready, supervisor.status
    assert supervisor.status.startswith("active s-")
    supervisor.tick()
    assert supervisor.ready
    assert prepared == ["gw"], "an active session is heartbeated, never prepared again"


def test_a_lock_held_elsewhere_is_not_ready_and_a_clean_handover_is_immediate() -> None:
    db, prepared = Database(Clock()), list[str]()
    first = _supervisor(db, prepared, name="first")
    second = _supervisor(db, prepared, name="second")
    first.tick()
    second.tick()
    assert first.ready and not second.ready
    assert second.status == "lock held elsewhere"
    assert first.stop(confirmed=True)
    second.tick()
    assert second.ready, "a predecessor that closed cleanly is not waited out"
    assert prepared == ["first", "second"]


def test_a_takeover_writes_nothing_until_the_predecessor_can_no_longer_be_writing() -> None:
    """PostgreSQL released the first writer's lock (a restart, a terminated backend) before the
    first writer learned of it. At no instant may both be ready."""
    clock = Clock()
    db, prepared = Database(clock), list[str]()
    first = _supervisor(db, prepared, name="first")
    second = _supervisor(db, prepared, name="second")
    first.tick()
    assert first.ready
    db.holder = None
    second.tick()
    assert not second.ready
    assert second.status.startswith("taking over: waiting 8.0 s")
    first_stopped: float | None = None
    second_started: float | None = None
    for step in range(1, 200):
        clock.now = START + step * 0.05
        second.tick()
        first_ready, second_ready = first.ready, second.ready
        assert not (first_ready and second_ready), f"two writers were ready at {clock.now}"
        if not first_ready and first_stopped is None:
            first_stopped = clock.now
        if second_ready:
            second_started = clock.now
            break
    assert first_stopped is not None and second_started is not None
    assert first_stopped == pytest.approx(START + LEASE_S)
    assert second_started == pytest.approx(START + GRACE_S)
    assert prepared == ["first", "second"]
    first.tick()
    assert not first.ready
    assert first.status == "lock held elsewhere", "its heartbeat found the lock gone"


def test_the_lease_expires_when_no_heartbeat_confirms_the_fence() -> None:
    clock = Clock()
    supervisor = _supervisor(Database(clock))
    supervisor.tick()
    clock.now = START + LEASE_S - 0.001
    assert supervisor.ready
    clock.now = START + LEASE_S
    assert not supervisor.ready
    assert supervisor.status == "lease expired: the fence was last confirmed 6.0 s ago"
    supervisor.tick()
    assert supervisor.ready, "a late heartbeat that still finds the lock and the row renews it"


def test_a_lost_session_is_replaced_by_a_new_one_that_waits_out_the_old() -> None:
    clock = Clock()
    db, prepared = Database(clock), list[str]()
    supervisor = _supervisor(db, prepared)
    supervisor.tick()
    lost = supervisor.session
    assert lost is not None and lost.session_id is not None
    db.holder = None
    supervisor.tick()
    assert not supervisor.ready
    replacement = supervisor.session
    assert replacement is not None and replacement.session_id != lost.session_id
    assert supervisor.status.startswith("taking over")
    clock.now = START + GRACE_S
    supervisor.tick()
    assert supervisor.ready
    assert prepared == ["gw", "gw"]
    assert not db.rows[lost.session_id].closed, "a lost session is never closed: a bounded gap"


def test_a_predecessor_that_cannot_be_ruled_out_is_waited_out() -> None:
    clock = Clock()
    supervisor = _supervisor(Database(clock, predecessor_unknown=True))
    supervisor.tick()
    assert not supervisor.ready and supervisor.status.startswith("taking over")
    clock.now = START + GRACE_S
    supervisor.tick()
    assert supervisor.ready


def test_an_unreachable_database_is_reported_and_retried() -> None:
    db = Database(Clock(), reachable=False)
    supervisor = _supervisor(db)
    supervisor.tick()
    assert not supervisor.ready
    assert supervisor.status == "unreachable: ConnectionError"
    db.reachable = True
    supervisor.tick()
    assert supervisor.ready


def test_a_failed_preparation_holds_the_fence_but_is_not_ready_until_it_succeeds() -> None:
    db, prepared = Database(Clock()), list[str]()
    supervisor = _supervisor(db, prepared, fail_prepare=[True])
    supervisor.tick()
    assert not supervisor.ready
    assert supervisor.status == "preparing failed: RuntimeError"
    assert db.holder is not None, "the fence is held while preparation is retried"
    supervisor.tick()
    assert supervisor.ready and prepared == ["gw"]


@pytest.mark.parametrize("confirmed", [True, False])
def test_stopping_closes_the_session_only_when_confirmed_and_releases_the_lock(
    confirmed: bool,
) -> None:
    db = Database(Clock())
    supervisor = _supervisor(db)
    supervisor.tick()
    session = supervisor.session
    assert session is not None and session.session_id is not None
    assert supervisor.stop(confirmed=confirmed) is confirmed
    assert not supervisor.ready and supervisor.status == "stopped"
    assert db.rows[session.session_id].closed is confirmed
    assert db.holder is None, "the connection, and so the lock, is released"


def test_stopping_does_not_wait_forever_on_a_stuck_step() -> None:
    db = Database(Clock())
    supervisor = _supervisor(db, interval_s=0.01, lease_s=0.02, grace_s=0.02)
    supervisor.tick()
    holding, release = threading.Event(), threading.Event()

    def stuck_step() -> None:
        with supervisor._gate:
            holding.set()
            release.wait(5.0)

    thread = threading.Thread(target=stuck_step)
    thread.start()
    assert holding.wait(5.0)
    try:
        assert supervisor.stop(confirmed=True) is False
        assert not supervisor.ready
    finally:
        release.set()
        thread.join()
    session = supervisor.session
    assert session is not None and session.session_id is not None
    assert not db.rows[session.session_id].closed, "left unclosed: a gap bounded by its heartbeat"


def test_the_thread_keeps_the_session_and_stops_cleanly() -> None:
    supervisor = _supervisor(Database(Clock()), interval_s=0.01, lease_s=0.05, grace_s=0.05)
    supervisor.start()
    try:
        assert supervisor.ready
        threading.Event().wait(0.1)
        assert supervisor.ready
    finally:
        assert supervisor.stop(confirmed=True)
    assert not supervisor.ready and supervisor.status == "stopped"


@pytest.mark.parametrize(("interval_s", "lease_s", "grace_s"), [(6.0, 6.0, 8.0), (2.0, 6.0, 5.0)])
def test_timings_that_cannot_fence_are_refused(
    interval_s: float, lease_s: float, grace_s: float
) -> None:
    with pytest.raises(WriterSessionError):
        _supervisor(Database(Clock()), interval_s=interval_s, lease_s=lease_s, grace_s=grace_s)
