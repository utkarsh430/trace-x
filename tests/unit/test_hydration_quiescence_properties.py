"""Under hydration's fence, every lost write lies inside a bounded gap (ADR-0057 §4, decision D2).

The coverage rule vouches only below `Coverage.through`, for three reasons: a session may open
after the ledger read, a heartbeat may commit after it, and records may not have been read yet.
Hydration removes the first two by holding the writer lock from before the read and waiting out the
takeover grace and two clock margins; the third leaves missing sequence numbers, which the rule
places inside that session's own runs and tail.

These histories are the coverage property suite's, drawn with one change: the ledger is read only
once every session's successor could have opened, which is what hydration's wait guarantees.
Nothing is skipped for being at or after `through` -- that is the point of the test -- and the
claim computed from the same coverage must start after the event time any lost write could carry.
"""

from __future__ import annotations

import datetime as dt

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from tests.unit.test_observation_coverage_properties import (
    History,
    _assess,
    _lost,
    _offset,
    _session,
    _t,
)

from trace_core.domain.time import to_millis
from trace_core.observation.coverage import SessionRow
from trace_core.stream.hydration import FUTURE_SKEW, ClaimInputs, compute_claim

pytestmark = [pytest.mark.property, pytest.mark.unit]

QUIESCENCE_MS = 1
"""Hydration waits two clock margins and a little more before it reads the ledger."""


@st.composite
def quiescent_histories(draw: st.DrawFn) -> History:
    """A history whose sessions have all ended before hydration reads the ledger."""
    margin = draw(st.one_of(st.just(0), st.integers(0, 1_000)))
    lease = draw(st.integers(300, 6_000))
    takeover = draw(st.one_of(st.just(0), st.integers(0, 2_000)))
    hosts = [draw(_offset(margin)) for _ in range(draw(st.integers(1, 2)))]
    broker = draw(_offset(margin))
    sessions = []
    not_before = 0
    for index in range(draw(st.integers(1, 3))):
        session, not_before = _session(
            draw,
            f"s{index}",
            host=hosts[draw(st.integers(0, len(hosts) - 1))],
            broker=broker,
            not_before=not_before,
            lease=lease,
            takeover=takeover,
        )
        sessions.append(session)
    # The fence: hydration acquires the lock no earlier than the moment a successor could open,
    # and reads the ledger two clock margins later (ADR-0057 §1).
    ledger_read = not_before + 2 * margin + QUIESCENCE_MS + draw(st.integers(0, 5_000))
    arrivals = [a for session in sessions for w in session.writes for a in w.arrivals] or [0]
    log_through = draw(
        st.one_of(
            st.integers(-1_000, max(arrivals) + 1_000),
            st.sampled_from(arrivals).flatmap(lambda a: st.integers(a - 50, a + 50)),
        )
    )
    return History(
        sessions=tuple(sessions),
        lease=lease,
        takeover=takeover,
        margin=margin,
        ledger_read=ledger_read,
        log_through=log_through,
        probe=(0, 0),
    )


def _rows(history: History) -> tuple[SessionRow, ...]:
    """The ledger as hydration's read sees it: every session, since all opened before it."""
    rows = []
    for session in history.sessions:
        beats = [session.opened]
        beats += [b.start for b in session.heartbeats if b.commit is not None]
        rows.append(
            SessionRow(
                session_id=session.session_id,
                started_at=_t(session.opened),
                heartbeat_at=_t(max(beats)),
                closed_at=None if session.closed is None else _t(session.closed),
                last_seq=None if session.closed is None else len(session.writes),
            )
        )
    return tuple(rows)


SETTINGS = settings(
    max_examples=300,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)


@SETTINGS
@given(quiescent_histories())
def test_under_the_fence_every_lost_write_lies_inside_a_gap_whatever_through_says(
    history: History,
) -> None:
    """D2: the guarantee holds past `through` once no session can open or heartbeat after the read.

    Covers every way a session ends: closed cleanly, left unclosed, its heartbeat lost, and its
    records lost to a delivery timeout (a write that never arrives).
    """
    coverage, seen = _assess(history)
    for session_id, seq, write in _lost(history, seen):
        gaps = coverage.sessions[session_id].gaps
        edges = [(g.start.isoformat(), None if g.end is None else g.end.isoformat()) for g in gaps]
        assert any(gap.contains(write) for gap in gaps), (
            f"lost write {session_id}/{seq} at {write.isoformat()} is in no gap {edges}; "
            f"through={coverage.through.isoformat()}"
        )


@SETTINGS
@given(quiescent_histories())
def test_a_claim_from_that_coverage_starts_after_every_lost_writes_reach(
    history: History,
) -> None:
    """Whatever a lost write could be dated, the claim begins after it."""
    coverage, seen = _assess(history)
    rows = _rows(history)
    inputs = ClaimInputs(
        coverage=coverage,
        sessions=rows,
        ledger_read_at=_t(history.ledger_read),
        clock_margin_s=history.margin / 1_000,
        begin_epoch_ms=to_millis(_t(history.ledger_read) + dt.timedelta(days=60)),
    )
    claim = compute_claim(inputs)
    if not claim.claimable:
        assert claim.reasons
        return
    assert claim.since_ms is not None
    for _session_id, _seq, write in _lost(history, seen):
        assert to_millis(write + FUTURE_SKEW) < claim.since_ms, (
            "a claim may not start at or before the event time a lost write could carry"
        )


@SETTINGS
@given(quiescent_histories())
def test_a_history_with_no_loss_and_no_anomaly_is_always_claimable(history: History) -> None:
    """The evidence is not merely safe: a clean history still yields a claim."""
    coverage, seen = _assess(history)
    inputs = ClaimInputs(
        coverage=coverage,
        sessions=_rows(history),
        ledger_read_at=_t(history.ledger_read),
        clock_margin_s=history.margin / 1_000,
        begin_epoch_ms=to_millis(_t(history.ledger_read) + dt.timedelta(days=60)),
    )
    claim = compute_claim(inputs)
    if _lost(history, seen) or not coverage.gap_free:
        return
    assert claim.claimable, claim.reasons
