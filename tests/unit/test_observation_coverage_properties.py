"""The coverage rule against generated serial-writer histories (ADR-0051 §5; plan §4.1).

Hand-built cases pin each clause. These generate histories nobody wrote by hand, and hold the rule
to the one thing it exists for: a lost write is never outside a gap, and `vouches` never spans a
loss.

A history is one serial writer, as the gateway is. It handles observations one at a time; each has a
handling window, a store write and a stamp inside it. It heartbeats every `interval` while alive,
and either closes confirmed (nothing lost) or dies with its session unclosed. Deliveries arrive any
time after their stamp, or never. The ledger is read at an arbitrary moment, seeing only what had
committed by then, and the log is read through an arbitrary high-water mark.
"""

from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from trace_core.observation.coverage import Coverage, Observed, SessionRow, assess

pytestmark = [pytest.mark.property, pytest.mark.unit]

T0 = dt.datetime(2026, 9, 15, 12, 0, tzinfo=dt.UTC)
SESSION = "s"


def _t(seconds: float) -> dt.datetime:
    return T0 + dt.timedelta(seconds=seconds)


@dataclass(frozen=True)
class History:
    writes: tuple[float, ...]
    stamps: tuple[float, ...]
    arrivals: tuple[float | None, ...]
    closed_at: float | None
    alive_until: float
    interval: float
    lease: float
    takeover: float
    margin: float
    ledger_read: float
    log_through: float
    probe: tuple[float, float]


@st.composite
def histories(draw: st.DrawFn) -> History:
    interval = draw(st.floats(0.2, 2.0))
    lease = interval * draw(st.floats(1.5, 4.0))
    takeover = draw(st.floats(0.0, 2.0))
    margin = draw(st.floats(0.0, 1.0))
    count = draw(st.integers(0, 20))
    clock = 0.0
    writes: list[float] = []
    stamps: list[float] = []
    for _ in range(count):
        start = clock + draw(st.floats(0.0, 3.0))
        end = start + draw(st.floats(0.0, 0.5))
        writes.append(draw(st.floats(start, end)))
        stamps.append(draw(st.floats(start, end)))
        clock = end
    lost = [draw(st.booleans()) for _ in range(count)]
    arrivals = tuple(
        None if gone else stamp + draw(st.floats(0.0, 40.0))
        for stamp, gone in zip(stamps, lost, strict=True)
    )
    closes = not any(lost) and draw(st.booleans())
    alive_until = clock + draw(st.floats(0.0, 1.0))
    closed_at = alive_until if closes else None
    horizon = alive_until + 60.0
    low, high = sorted((draw(st.floats(0.0, horizon)), draw(st.floats(0.0, horizon))))
    return History(
        writes=tuple(writes),
        stamps=tuple(stamps),
        arrivals=arrivals,
        closed_at=closed_at,
        alive_until=alive_until,
        interval=interval,
        lease=lease,
        takeover=takeover,
        margin=margin,
        ledger_read=draw(st.floats(0.0, horizon)),
        log_through=draw(st.floats(0.0, horizon)),
        probe=(low, high),
    )


def _ledger(history: History) -> list[SessionRow]:
    """The session row as a read at `ledger_read` sees it: what had committed by then."""
    read = history.ledger_read
    if read < 0.0:
        return []
    if history.closed_at is not None and history.closed_at <= read:
        return [
            SessionRow(
                SESSION, _t(0.0), _t(history.closed_at), _t(history.closed_at), len(history.writes)
            )
        ]
    beat_until = min(read, history.alive_until)
    last_beat = math.floor(beat_until / history.interval) * history.interval
    return [SessionRow(SESSION, _t(0.0), _t(last_beat), None, None)]


def _assess(history: History) -> tuple[Coverage, set[int | None]]:
    observed = [
        Observed(SESSION, seq, _t(arrival), _t(stamp))
        for seq, (stamp, arrival) in enumerate(
            zip(history.stamps, history.arrivals, strict=True), start=1
        )
        if arrival is not None and arrival <= history.log_through
    ]
    coverage = assess(
        _ledger(history),
        observed,
        ledger_read_at=_t(history.ledger_read),
        log_read_through=_t(history.log_through),
        lease_s=history.lease,
        takeover_margin_s=history.takeover,
        clock_margin_s=history.margin,
    )
    return coverage, {item.seq for item in observed}


SETTINGS = settings(max_examples=400, deadline=None, suppress_health_check=[HealthCheck.too_slow])


@SETTINGS
@given(histories())
def test_every_lost_write_before_through_lies_inside_a_gap_of_its_session(history: History) -> None:
    coverage, seen = _assess(history)
    gaps = [gap for gap in coverage.gaps if gap.session_id == SESSION]
    for seq, write in enumerate(history.writes, start=1):
        if seq in seen or _t(write) >= coverage.through:
            continue
        assert any(gap.contains(_t(write)) for gap in gaps), (seq, write, coverage)


@SETTINGS
@given(histories())
def test_every_gap_is_well_formed_and_the_certified_prefix_is_present(history: History) -> None:
    coverage, seen = _assess(history)
    for gap in coverage.gaps:
        assert gap.end is None or gap.start <= gap.end, gap
    if SESSION in coverage.sessions:
        certified = coverage.sessions[SESSION].certified_through
        assert set(range(1, certified + 1)) <= seen


@SETTINGS
@given(histories())
def test_the_log_never_vouches_for_a_span_that_contains_a_lost_write(history: History) -> None:
    coverage, seen = _assess(history)
    start, end = _t(history.probe[0]), _t(history.probe[1])
    if not coverage.vouches(start, end):
        return
    for seq, write in enumerate(history.writes, start=1):
        if seq not in seen:
            assert not (start <= _t(write) <= end), (seq, write, coverage)
