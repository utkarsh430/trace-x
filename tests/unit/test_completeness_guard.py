"""The completeness guard: an unrecorded observation withdraws completeness, durably.

Fakes stand in for the store and the ledger here because the subject is the guard's state machine.
The PostgreSQL ledger is tested against a real database in
`tests/integration/test_completeness_ledger.py`.
"""

from __future__ import annotations

import datetime as dt

import pytest

from trace_core.domain.time import EventTime
from trace_core.features.completeness import CompletenessGuard, HoleReason

pytestmark = pytest.mark.unit

NOW = dt.datetime(2026, 3, 1, 12, 0, tzinfo=dt.UTC)


class _Store:
    def __init__(self) -> None:
        self.reachable = True
        self.withdrawals: list[EventTime] = []

    def withdraw_completeness(self, *, resume_at: EventTime) -> None:
        if not self.reachable:
            raise ConnectionError("store unreachable")
        self.withdrawals.append(resume_at)


class _Ledger:
    def __init__(self) -> None:
        self.readable = True
        self.writable = True
        self.rows: list[tuple[HoleReason, str, bool]] = []

    def record_hole(self, *, reason: HoleReason, instance_id: str) -> None:
        if not self.writable:
            raise ConnectionError("postgres unreachable")
        self.rows.append((reason, instance_id, False))

    def latest_open_hole(self) -> int | None:
        if not self.readable:
            raise ConnectionError("postgres unreachable")
        open_ids = [i for i, (_, _, cleared) in enumerate(self.rows) if not cleared]
        return max(open_ids) if open_ids else None

    def open_holes(self) -> int:
        if not self.readable:
            raise ConnectionError("postgres unreachable")
        return sum(1 for _, _, cleared in self.rows if not cleared)

    def clear_holes(self, *, through_hole_id: int) -> int:
        if not self.writable:
            raise ConnectionError("postgres unreachable")
        cleared = 0
        for i, (reason, owner, done) in enumerate(self.rows):
            if not done and i <= through_hole_id:
                self.rows[i] = (reason, owner, True)
                cleared += 1
        return cleared


def _guard(store: _Store, ledger: _Ledger | None) -> CompletenessGuard:
    return CompletenessGuard(store, ledger, instance_id="gw-1", clock=lambda: NOW)


def test_with_no_hole_nothing_is_withdrawn() -> None:
    store, ledger = _Store(), _Ledger()
    guard = _guard(store, ledger)
    guard.resume()
    assert not guard.pending
    assert guard.reconcile()
    assert store.withdrawals == []


def test_an_outage_is_recorded_once_not_once_per_lost_observation() -> None:
    store, ledger = _Store(), _Ledger()
    guard = _guard(store, ledger)
    for _ in range(3):
        guard.observation_unrecorded(HoleReason.UNREACHABLE)
    guard.observation_unrecorded(HoleReason.BREAKER_OPEN)
    assert guard.pending
    assert ledger.rows == [(HoleReason.UNREACHABLE, "gw-1", False)]


def test_completeness_resumes_only_past_the_future_skew_bound() -> None:
    """Deleting the epoch is not enough: a lost observation may be dated up to 24 h ahead."""
    store, ledger = _Store(), _Ledger()
    guard = _guard(store, ledger)
    guard.observation_unrecorded(HoleReason.UNREACHABLE)
    assert guard.reconcile()
    assert store.withdrawals == [EventTime(NOW + dt.timedelta(hours=24))]
    assert ledger.open_holes() == 0
    assert not guard.pending


def test_the_hole_stays_pending_while_the_store_is_still_unreachable() -> None:
    store, ledger = _Store(), _Ledger()
    guard = _guard(store, ledger)
    guard.observation_unrecorded(HoleReason.UNREACHABLE)
    store.reachable = False
    assert not guard.reconcile()
    assert guard.pending
    assert ledger.open_holes() == 1


def test_a_hole_the_ledger_could_not_clear_is_withdrawn_again() -> None:
    store, ledger = _Store(), _Ledger()
    guard = _guard(store, ledger)
    guard.observation_unrecorded(HoleReason.REFUSED)
    ledger.writable = False
    assert not guard.reconcile()
    assert guard.pending
    ledger.writable = True
    assert guard.reconcile()
    assert len(store.withdrawals) == 2
    assert not guard.pending


def test_a_restarted_process_inherits_an_open_hole() -> None:
    store, ledger = _Store(), _Ledger()
    first = _guard(store, ledger)
    first.observation_unrecorded(HoleReason.UNREACHABLE)
    # The process dies here, before the store came back.
    second = _guard(store, ledger)
    second.resume()
    assert second.pending
    assert second.reconcile()
    assert ledger.open_holes() == 0
    assert len(ledger.rows) == 1, "an inherited hole is not recorded a second time"


def test_a_ledger_that_cannot_be_read_at_start_cannot_vouch_for_no_hole() -> None:
    store, ledger = _Store(), _Ledger()
    ledger.readable = False
    guard = _guard(store, ledger)
    guard.resume()
    assert guard.pending


def test_an_unreadable_start_later_read_empty_withdraws_nothing() -> None:
    """The ledger answers late -- a database slower to start than the gateway. An empty ledger
    then vouches exactly as it would have at start-up, so nothing is withdrawn: withdrawing anyway
    cost every such restart a day of `history_incomplete` for nothing lost."""
    store, ledger = _Store(), _Ledger()
    ledger.readable = False
    guard = _guard(store, ledger)
    guard.resume()
    assert guard.pending
    assert not guard.reconcile(), "an unreadable ledger still cannot vouch"
    ledger.readable = True
    assert guard.reconcile()
    assert not guard.pending
    assert store.withdrawals == []


def test_an_unreadable_start_later_showing_a_hole_withdraws_it() -> None:
    store, ledger = _Store(), _Ledger()
    ledger.record_hole(reason=HoleReason.UNREACHABLE, instance_id="gw-0")
    ledger.readable = False
    guard = _guard(store, ledger)
    guard.resume()
    ledger.readable = True
    assert guard.reconcile()
    assert len(store.withdrawals) == 1
    assert ledger.open_holes() == 0
    assert len(ledger.rows) == 1, "an inherited hole is not recorded a second time"


def test_an_unreadable_start_with_a_loss_of_its_own_withdraws_whatever_the_ledger_says() -> None:
    """This process lost an observation while it could not write the ledger either, so an empty
    ledger proves nothing about that loss."""
    store, ledger = _Store(), _Ledger()
    ledger.readable = False
    ledger.writable = False
    guard = _guard(store, ledger)
    guard.resume()
    guard.observation_unrecorded(HoleReason.UNREACHABLE)
    ledger.readable = True
    ledger.writable = True
    assert ledger.open_holes() == 0
    assert guard.reconcile()
    assert len(store.withdrawals) == 1


def test_a_hole_the_ledger_refused_is_recorded_on_the_next_lost_observation() -> None:
    store, ledger = _Store(), _Ledger()
    guard = _guard(store, ledger)
    ledger.writable = False
    guard.observation_unrecorded(HoleReason.UNREACHABLE)
    assert guard.pending and ledger.rows == []
    ledger.writable = True
    guard.observation_unrecorded(HoleReason.UNREACHABLE)
    assert ledger.rows == [(HoleReason.UNREACHABLE, "gw-1", False)]


def test_without_a_ledger_the_guard_still_withdraws_within_the_process() -> None:
    store = _Store()
    guard = _guard(store, None)
    guard.resume()
    guard.observation_unrecorded(HoleReason.UNREACHABLE)
    assert guard.pending
    assert guard.reconcile()
    assert len(store.withdrawals) == 1


class _RacingStore(_Store):
    """A store whose withdrawal runs while another instance records a new hole."""

    def __init__(self, ledger: _Ledger) -> None:
        super().__init__()
        self.ledger = ledger

    def withdraw_completeness(self, *, resume_at: EventTime) -> None:
        super().withdraw_completeness(resume_at=resume_at)
        self.ledger.record_hole(reason=HoleReason.UNREACHABLE, instance_id="gw-2")


def test_a_hole_recorded_during_the_withdrawal_is_not_cleared_by_it() -> None:
    ledger = _Ledger()
    store = _RacingStore(ledger)
    guard = _guard(store, ledger)
    guard.observation_unrecorded(HoleReason.UNREACHABLE)
    assert not guard.reconcile(), "the second instance's hole was not covered by this withdrawal"
    assert ledger.open_holes() == 1
    assert guard.pending


def test_the_resume_margin_is_the_ingress_future_skew_bound() -> None:
    """The margin must cover the furthest-future event any recorded stream accepts."""
    from trace_core.contracts.api.transaction import MAX_CLOCK_SKEW_FUTURE_S
    from trace_core.features.completeness import RESUME_MARGIN

    assert dt.timedelta(seconds=MAX_CLOCK_SKEW_FUTURE_S) == RESUME_MARGIN
