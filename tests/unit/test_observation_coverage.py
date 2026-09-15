"""The observation log's coverage rule on hand-built ledgers (ADR-0051 §5; plan §4.1 point 5).

Every clause of the rule has a covered control beside its gap, so a rule that reported gaps for
everything would fail here as surely as one that reported none.
"""

from __future__ import annotations

import datetime as dt

import pytest

from trace_core.observation.coverage import Gap, Observed, SessionRow, assess

pytestmark = pytest.mark.unit

T0 = dt.datetime(2026, 9, 14, 12, 0, tzinfo=dt.UTC)
INTERVAL_S, DELIVERY_S, MARGIN_S = 2.0, 30.0, 1.0
MARGIN = dt.timedelta(seconds=MARGIN_S)


def _at(seconds: float) -> dt.datetime:
    return T0 + dt.timedelta(seconds=seconds)


def _row(
    session_id: str = "s1",
    *,
    heartbeat: float = 60.0,
    closed: float | None = None,
    last_seq: int | None = None,
) -> SessionRow:
    return SessionRow(
        session_id=session_id,
        started_at=T0,
        heartbeat_at=_at(heartbeat),
        closed_at=None if closed is None else _at(closed),
        last_seq=last_seq,
    )


def _seen(*seqs: int, session_id: str | None = "s1") -> list[Observed]:
    return [Observed(session_id=session_id, seq=seq, logged_at=_at(10.0 * seq)) for seq in seqs]


def test_a_closed_session_with_every_number_is_covered() -> None:
    coverage = assess(
        [_row(closed=60.0, last_seq=3)],
        _seen(1, 2, 3),
        heartbeat_interval_s=INTERVAL_S,
        delivery_timeout_s=DELIVERY_S,
        clock_margin_s=MARGIN_S,
    )
    session = coverage.sessions["s1"]
    assert session.covered and session.closed and session.certified_through == 3
    assert coverage.gap_free


def test_a_closed_session_with_no_observations_and_last_seq_zero_is_covered() -> None:
    coverage = assess(
        [_row(closed=5.0, last_seq=0)],
        [],
        heartbeat_interval_s=INTERVAL_S,
        delivery_timeout_s=DELIVERY_S,
        clock_margin_s=MARGIN_S,
    )
    assert coverage.gap_free and coverage.sessions["s1"].certified_through == 0


def test_a_missing_number_in_a_closed_session_is_a_gap_between_its_neighbours() -> None:
    coverage = assess(
        [_row(closed=60.0, last_seq=4)],
        _seen(1, 2, 4),
        heartbeat_interval_s=INTERVAL_S,
        delivery_timeout_s=DELIVERY_S,
        clock_margin_s=MARGIN_S,
    )
    session = coverage.sessions["s1"]
    assert session.certified_through == 2
    assert session.gaps == (Gap("s1", _at(20.0) - MARGIN, _at(40.0) + MARGIN, (3,)),)


def test_a_missing_tail_in_a_closed_session_is_a_gap_to_the_close() -> None:
    coverage = assess(
        [_row(closed=90.0, last_seq=5)],
        _seen(1, 2, 3),
        heartbeat_interval_s=INTERVAL_S,
        delivery_timeout_s=DELIVERY_S,
        clock_margin_s=MARGIN_S,
    )
    assert coverage.sessions["s1"].gaps == (
        Gap("s1", _at(30.0) - MARGIN, _at(90.0) + MARGIN, (4, 5)),
    )


def test_a_missing_first_number_starts_its_gap_at_the_session_start() -> None:
    coverage = assess(
        [_row(closed=60.0, last_seq=2)],
        _seen(2),
        heartbeat_interval_s=INTERVAL_S,
        delivery_timeout_s=DELIVERY_S,
        clock_margin_s=MARGIN_S,
    )
    assert coverage.sessions["s1"].gaps == (Gap("s1", T0 - MARGIN, _at(20.0) + MARGIN, (1,)),)
    assert coverage.sessions["s1"].certified_through == 0


def test_an_unclosed_session_certifies_its_prefix_and_is_a_gap_bounded_by_its_heartbeat() -> None:
    coverage = assess(
        [_row(heartbeat=45.0)],
        _seen(1, 2, 3),
        heartbeat_interval_s=INTERVAL_S,
        delivery_timeout_s=DELIVERY_S,
        clock_margin_s=MARGIN_S,
    )
    session = coverage.sessions["s1"]
    assert not session.closed and session.certified_through == 3
    bound = _at(45.0 + INTERVAL_S + DELIVERY_S)
    assert session.gaps == (Gap("s1", _at(30.0) - MARGIN, bound + MARGIN, ()),)


def test_an_unclosed_session_names_the_numbers_it_knows_are_missing() -> None:
    coverage = assess(
        [_row(heartbeat=45.0)],
        _seen(1, 2, 4),
        heartbeat_interval_s=INTERVAL_S,
        delivery_timeout_s=DELIVERY_S,
        clock_margin_s=MARGIN_S,
    )
    session = coverage.sessions["s1"]
    assert session.certified_through == 2
    (gap,) = session.gaps
    assert gap.start == _at(20.0) - MARGIN and gap.missing == (3,)


def test_an_unclosed_session_with_nothing_logged_is_a_gap_from_its_start() -> None:
    coverage = assess(
        [_row(heartbeat=5.0)],
        [],
        heartbeat_interval_s=INTERVAL_S,
        delivery_timeout_s=DELIVERY_S,
        clock_margin_s=MARGIN_S,
    )
    (gap,) = coverage.sessions["s1"].gaps
    assert gap.start == T0 - MARGIN
    assert gap.end == _at(5.0 + INTERVAL_S + DELIVERY_S) + MARGIN


@pytest.mark.parametrize(
    "observed",
    [
        _seen(1, session_id="not-in-the-ledger"),
        _seen(1, session_id=None),
        [Observed(session_id="s1", seq=None, logged_at=_at(10.0))],
        [Observed(session_id="s1", seq=0, logged_at=_at(10.0))],
    ],
    ids=["unknown-session", "no-session-header", "no-sequence", "sequence-zero"],
)
def test_an_observation_without_a_known_session_and_number_is_a_gap_at_its_own_time(
    observed: list[Observed],
) -> None:
    coverage = assess(
        [_row(closed=60.0, last_seq=0)],
        observed,
        heartbeat_interval_s=INTERVAL_S,
        delivery_timeout_s=DELIVERY_S,
        clock_margin_s=MARGIN_S,
    )
    assert coverage.sessions["s1"].covered
    (gap,) = coverage.unknown
    assert (gap.start, gap.end) == (_at(10.0) - MARGIN, _at(10.0) + MARGIN)
    assert not coverage.gap_free


def test_a_redelivered_number_is_not_a_gap_and_its_earliest_arrival_counts() -> None:
    observed = [*_seen(1, 2), Observed(session_id="s1", seq=2, logged_at=_at(5.0))]
    coverage = assess(
        [_row(closed=60.0, last_seq=2)],
        observed,
        heartbeat_interval_s=INTERVAL_S,
        delivery_timeout_s=DELIVERY_S,
        clock_margin_s=MARGIN_S,
    )
    assert coverage.gap_free


def test_sessions_are_judged_independently() -> None:
    coverage = assess(
        [_row("s1", closed=60.0, last_seq=2), _row("s2", heartbeat=30.0)],
        [*_seen(1, 2, session_id="s1"), *_seen(1, session_id="s2")],
        heartbeat_interval_s=INTERVAL_S,
        delivery_timeout_s=DELIVERY_S,
        clock_margin_s=MARGIN_S,
    )
    assert coverage.sessions["s1"].covered
    assert not coverage.sessions["s2"].covered
    assert len(coverage.gaps) == 1
