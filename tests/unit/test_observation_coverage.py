"""The observation log's coverage rule on hand-built ledgers (ADR-0051 §5; plan §4.1 point 5).

Every clause of the rule has a covered control beside its gap, so a rule that reported gaps for
everything would fail here as surely as one that reported none. The generated counterpart, which
asserts that no lost write escapes a gap, is tests/unit/test_observation_coverage_properties.py.
"""

from __future__ import annotations

import datetime as dt

import pytest

from trace_core.observation.coverage import (
    Coverage,
    CoverageInputError,
    Gap,
    Observed,
    SessionRow,
    assess,
)

pytestmark = pytest.mark.unit

T0 = dt.datetime(2026, 9, 14, 12, 0, tzinfo=dt.UTC)
LEASE_S, TAKEOVER_S, MARGIN_S = 6.0, 2.0, 1.0
MARGIN = dt.timedelta(seconds=MARGIN_S)
LATE = 10_000.0
"""A ledger read and a log high-water mark long after everything in a test happened."""


def _at(seconds: float) -> dt.datetime:
    return T0 + dt.timedelta(seconds=seconds)


def _row(
    session_id: str = "s1",
    *,
    started: float = 0.0,
    heartbeat: float = 60.0,
    closed: float | None = None,
    last_seq: int | None = None,
) -> SessionRow:
    return SessionRow(
        session_id=session_id,
        started_at=_at(started),
        heartbeat_at=_at(heartbeat),
        closed_at=None if closed is None else _at(closed),
        last_seq=last_seq,
    )


def _seen(*seqs: int, session_id: str | None = "s1") -> list[Observed]:
    """Number n is written at 10n s and arrives half a second later."""
    return [
        Observed(
            session_id=session_id,
            seq=seq,
            logged_at=_at(10.0 * seq + 0.5),
            written_at=_at(10.0 * seq),
        )
        for seq in seqs
    ]


def _one(seq: int, *, written: float, arrived: float, session_id: str | None = "s1") -> Observed:
    return Observed(session_id=session_id, seq=seq, logged_at=_at(arrived), written_at=_at(written))


def _assess(
    ledger: list[SessionRow],
    observed: list[Observed],
    *,
    ledger_read: float = LATE,
    log_through: float = LATE,
) -> Coverage:
    return assess(
        ledger,
        observed,
        ledger_read_at=_at(ledger_read),
        log_read_through=_at(log_through),
        lease_s=LEASE_S,
        takeover_margin_s=TAKEOVER_S,
        clock_margin_s=MARGIN_S,
    )


def _well_formed(coverage: Coverage) -> None:
    for gap in coverage.gaps:
        assert gap.end is None or gap.start <= gap.end, gap


def test_a_closed_session_with_every_number_is_covered_and_vouched_for() -> None:
    coverage = _assess([_row(closed=60.0, last_seq=3)], _seen(1, 2, 3))
    session = coverage.sessions["s1"]
    assert session.covered and session.closed and session.certified_through == 3
    assert coverage.gap_free
    assert coverage.through == _at(LATE) - MARGIN
    assert coverage.vouches(T0, _at(100.0))


def test_a_closed_session_with_no_observations_and_last_seq_zero_is_covered() -> None:
    coverage = _assess([_row(closed=5.0, last_seq=0)], [])
    assert coverage.gap_free and coverage.sessions["s1"].certified_through == 0


def test_a_missing_number_in_a_closed_session_is_a_gap_between_its_neighbours_stamps() -> None:
    coverage = _assess([_row(closed=60.0, last_seq=4)], _seen(1, 2, 4))
    session = coverage.sessions["s1"]
    assert session.certified_through == 2
    assert session.gaps == (Gap("s1", _at(20.0) - MARGIN, _at(40.0) + MARGIN, (3,)),)
    assert not coverage.vouches(_at(25.0), _at(35.0))
    assert coverage.vouches(_at(45.0), _at(50.0)), "a gap withdraws only its own span"


def test_a_loss_behind_a_record_that_waited_in_the_producer_buffer_is_inside_its_gap() -> None:
    """A2. Number 1 was handled at 1 s and buffered until 35 s; number 2 was shed at 2 s; number 3
    was handled at 3 s. Gaps from arrival times (34-37 s) missed the loss; the writer's stamps
    cannot."""
    observed = [_one(1, written=1.0, arrived=35.0), _one(3, written=3.0, arrived=36.0)]
    coverage = _assess([_row(closed=40.0, last_seq=3)], observed)
    (gap,) = coverage.sessions["s1"].gaps
    assert gap == Gap("s1", _at(1.0) - MARGIN, _at(3.0) + MARGIN, (2,))
    assert gap.contains(_at(2.0))
    assert not coverage.vouches(_at(2.0), _at(2.0))


def test_a_missing_tail_in_a_closed_session_is_a_gap_to_the_close() -> None:
    coverage = _assess([_row(closed=90.0, last_seq=5)], _seen(1, 2, 3))
    assert coverage.sessions["s1"].gaps == (
        Gap("s1", _at(30.0) - MARGIN, _at(90.0) + MARGIN, (4, 5)),
    )


def test_a_missing_first_number_starts_its_gap_at_the_session_start() -> None:
    coverage = _assess([_row(started=3.0, closed=60.0, last_seq=2)], _seen(2))
    assert coverage.sessions["s1"].gaps == (Gap("s1", _at(3.0) - MARGIN, _at(20.0) + MARGIN, (1,)),)
    assert coverage.sessions["s1"].certified_through == 0


def test_a_number_above_last_seq_is_a_write_after_the_close_and_never_accepted() -> None:
    """C2. The close claimed three; a fourth exists. The session is judged as unclosed."""
    coverage = _assess([_row(closed=60.0, last_seq=3)], _seen(1, 2, 3, 4))
    session = coverage.sessions["s1"]
    assert session.anomalies and "after the close" in session.anomalies[0]
    assert not session.covered and not coverage.gap_free
    (gap,) = session.gaps
    assert gap.start == _at(40.0) - MARGIN and gap.end is None, (
        "the extent after a close is unknown"
    )
    _well_formed(coverage)


def test_an_unclosed_session_read_after_its_lease_ran_out_has_a_bounded_tail() -> None:
    """C1. The writer may write until its lease expires plus the takeover margin."""
    coverage = _assess([_row(heartbeat=45.0)], _seen(1, 2, 3))
    session = coverage.sessions["s1"]
    assert not session.closed and session.certified_through == 3
    bound = _at(45.0 + LEASE_S + TAKEOVER_S) + MARGIN
    assert session.gaps == (Gap("s1", _at(30.0) - MARGIN, bound, ()),)


def test_an_unclosed_session_read_before_its_lease_could_run_out_has_an_open_tail() -> None:
    """A1. A heartbeat committed after the ledger read is invisible to it, so the snapshot cannot
    bound a live writer."""
    read = 45.0 + LEASE_S + TAKEOVER_S + 2 * MARGIN_S
    coverage = _assess([_row(heartbeat=45.0)], _seen(1, 2, 3), ledger_read=read)
    (gap,) = coverage.sessions["s1"].gaps
    assert gap.start == _at(30.0) - MARGIN and gap.end is None
    assert coverage.through == _at(read) - MARGIN


def test_a_live_session_read_before_its_losses_puts_every_loss_in_a_well_formed_gap() -> None:
    """A1, as reproduced: heartbeat at 10 s; 1..899 at 0.1-89.9 s; 900 lost; 901 at 90 s. The
    rule used to report a gap from 88.8 s to 43.0 s."""
    observed = [_one(n, written=0.1 * n, arrived=0.1 * n + 0.05) for n in range(1, 900)]
    observed.append(_one(901, written=90.0, arrived=90.05))
    coverage = _assess([_row(heartbeat=10.0)], observed, ledger_read=11.0, log_through=91.0)
    _well_formed(coverage)
    session = coverage.sessions["s1"]
    assert session.certified_through == 899
    lost_at = _at(89.95)
    assert any(gap.contains(lost_at) and 900 in gap.missing for gap in session.gaps)
    assert not coverage.vouches(lost_at, lost_at)
    assert session.gaps[-1].end is None, "the writer was live after the snapshot"


def test_a_record_written_after_the_lease_bound_shows_the_writer_outlived_the_snapshot() -> None:
    observed = [_one(1, written=5.0, arrived=5.1), _one(2, written=90.0, arrived=90.1)]
    coverage = _assess([_row(heartbeat=10.0)], observed, ledger_read=200.0, log_through=200.0)
    (gap,) = coverage.sessions["s1"].gaps
    assert gap.start == _at(90.0) - MARGIN and gap.end is None
    _well_formed(coverage)


def test_an_unclosed_session_names_the_numbers_it_knows_are_missing() -> None:
    coverage = _assess([_row(heartbeat=45.0)], _seen(1, 2, 4))
    session = coverage.sessions["s1"]
    assert session.certified_through == 2
    interior, tail = session.gaps
    assert interior == Gap("s1", _at(20.0) - MARGIN, _at(40.0) + MARGIN, (3,))
    assert tail.start == _at(40.0) - MARGIN and tail.missing == ()


def test_an_unclosed_session_with_nothing_logged_is_a_gap_from_its_start() -> None:
    coverage = _assess([_row(heartbeat=5.0)], [])
    (gap,) = coverage.sessions["s1"].gaps
    assert gap.start == T0 - MARGIN
    assert gap.end == _at(5.0 + LEASE_S + TAKEOVER_S) + MARGIN


def test_a_session_opened_after_the_ledger_read_is_never_vouched_for() -> None:
    """A1. Its every record was lost, so nothing names it; `through` stops at the ledger read."""
    coverage = _assess([_row(closed=60.0, last_seq=3)], _seen(1, 2, 3), ledger_read=100.0)
    assert coverage.gap_free
    assert coverage.vouches(_at(0.0), _at(98.0))
    assert not coverage.vouches(_at(99.0), _at(99.0)), "the margin before the read"
    assert not coverage.vouches(_at(150.0), _at(150.0)), "a write the ledger could not know of"


def test_a_session_the_ledger_does_not_hold_is_an_open_gap() -> None:
    after_read = _assess(
        [], [_one(1, written=120.0, arrived=120.5, session_id="new")], ledger_read=100.0
    )
    (gap,) = after_read.unknown
    assert (gap.session_id, gap.start, gap.end) == ("new", _at(100.0) - MARGIN, None)
    assert not after_read.anomalies, "it could have opened after the read"
    before_read = _assess(
        [], [_one(1, written=10.0, arrived=10.5, session_id="ghost")], ledger_read=100.0
    )
    (ghost,) = before_read.unknown
    assert (ghost.start, ghost.end) == (_at(10.0) - MARGIN, None)
    assert before_read.anomalies, "the ledger should hold a session that wrote before the read"


@pytest.mark.parametrize(
    "observed",
    [
        [_one(1, written=10.0, arrived=10.5, session_id=None)],
        [Observed(session_id="s1", seq=None, logged_at=_at(10.5), written_at=_at(10.0))],
        [Observed(session_id="s1", seq=0, logged_at=_at(10.5), written_at=_at(10.0))],
    ],
    ids=["no-session-header", "no-sequence", "sequence-zero"],
)
def test_a_record_without_usable_headers_is_a_gap_from_its_stamp_to_its_arrival(
    observed: list[Observed],
) -> None:
    coverage = _assess([_row(closed=60.0, last_seq=0)], observed)
    assert coverage.sessions["s1"].covered
    (gap,) = coverage.unknown
    assert (gap.start, gap.end) == (_at(10.0) - MARGIN, _at(10.5) + MARGIN)
    assert not coverage.gap_free


def test_an_observation_past_the_high_water_mark_is_refused() -> None:
    with pytest.raises(CoverageInputError):
        _assess([_row(closed=60.0, last_seq=1)], _seen(1), log_through=5.0)


def test_a_redelivered_number_is_not_a_gap() -> None:
    observed = [*_seen(1, 2), _one(2, written=20.0, arrived=55.0)]
    assert _assess([_row(closed=60.0, last_seq=2)], observed).gap_free


def test_two_records_under_one_number_are_an_anomaly() -> None:
    observed = [*_seen(1, 2), _one(2, written=15.0, arrived=15.5)]
    coverage = _assess([_row(closed=60.0, last_seq=2)], observed)
    assert coverage.sessions["s1"].anomalies and not coverage.gap_free


def test_stamps_no_serial_writer_produces_are_an_anomaly_and_the_gap_widens_to_the_start() -> None:
    observed = [_one(1, written=50.0, arrived=50.5), _one(3, written=20.0, arrived=20.5)]
    coverage = _assess([_row(started=2.0, closed=60.0, last_seq=3)], observed)
    session = coverage.sessions["s1"]
    assert session.anomalies
    (gap,) = session.gaps
    assert gap == Gap("s1", _at(2.0) - MARGIN, _at(20.0) + MARGIN, (2,))
    _well_formed(coverage)


def test_sessions_are_judged_independently() -> None:
    coverage = _assess(
        [_row("s1", closed=60.0, last_seq=2), _row("s2", heartbeat=30.0)],
        [*_seen(1, 2, session_id="s1"), *_seen(1, session_id="s2")],
    )
    assert coverage.sessions["s1"].covered
    assert not coverage.sessions["s2"].covered
    assert len(coverage.gaps) == 1
