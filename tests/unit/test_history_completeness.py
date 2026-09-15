"""Feature history is COMPLETE only where both durable paths vouch (ADR-0051 §5; user, 2026-09-15).

Hand-built coverage and watermarks. Every INCOMPLETE clause has a COMPLETE control beside it, so a
verdict that refused everything would fail here as surely as one that allowed everything.
"""

from __future__ import annotations

import datetime as dt
from types import MappingProxyType

import pytest

from trace_core.observation.coverage import Coverage, Gap, SessionCoverage
from trace_core.observation.history_completeness import (
    HistoryCompleteness,
    HistoryVerdict,
    assess_history,
)

pytestmark = pytest.mark.unit

T0 = dt.datetime(2026, 9, 15, 12, 0, tzinfo=dt.UTC)
MARGIN_S = 1.0


def _at(seconds: float) -> dt.datetime:
    return T0 + dt.timedelta(seconds=seconds)


def _coverage(
    *gaps: Gap, through: float = 200.0, anomalies: tuple[str, ...] = (), session_anomaly: str = ""
) -> Coverage:
    sessions = (
        {"s1": SessionCoverage("s1", True, 0, (), (session_anomaly,))} if session_anomaly else {}
    )
    return Coverage(
        sessions=MappingProxyType(sessions),
        unknown=tuple(gaps),
        through=_at(through),
        anomalies=anomalies,
    )


def _verdict(
    coverage: Coverage | None = None,
    *,
    delivered_through: float | None = 200.0,
    start: float = 0.0,
    end: float = 100.0,
) -> HistoryVerdict:
    return assess_history(
        horizon_start=_at(start),
        horizon_end=_at(end),
        observation_coverage=coverage or _coverage(),
        authorization_delivered_through=None
        if delivered_through is None
        else _at(delivered_through),
        clock_margin_s=MARGIN_S,
    )


def test_both_paths_vouching_for_the_whole_horizon_is_complete() -> None:
    verdict = _verdict()
    assert verdict.completeness is HistoryCompleteness.COMPLETE and verdict.reasons == ()


def test_an_observation_gap_overlapping_the_horizon_keeps_it_incomplete() -> None:
    verdict = _verdict(_coverage(Gap("s1", _at(40.0), _at(50.0), (7,))))
    assert not verdict.complete
    assert any("observation gap" in reason for reason in verdict.reasons)


def test_an_open_gap_that_began_inside_or_before_the_horizon_keeps_it_incomplete() -> None:
    assert not _verdict(_coverage(Gap("s1", _at(90.0), None))).complete
    assert not _verdict(_coverage(Gap("s1", _at(-50.0), None))).complete


def test_a_gap_outside_the_horizon_does_not_block() -> None:
    assert _verdict(_coverage(Gap("s1", _at(150.0), _at(160.0)))).complete
    assert _verdict(_coverage(Gap("s1", _at(150.0), None))).complete


def test_a_horizon_reaching_the_coverage_through_time_is_incomplete() -> None:
    verdict = _verdict(_coverage(through=100.0))
    assert not verdict.complete
    assert any("vouches for nothing" in reason for reason in verdict.reasons)
    assert _verdict(_coverage(through=100.001)).complete


def test_any_coverage_anomaly_keeps_the_horizon_incomplete() -> None:
    assert not _verdict(
        _coverage(anomalies=("session x is in the log but not in the ledger",))
    ).complete
    assert not _verdict(_coverage(session_anomaly="a write after the close")).complete


def test_authorization_delivery_behind_the_horizon_keeps_it_incomplete() -> None:
    behind = _verdict(delivered_through=80.0)
    assert not behind.complete
    assert any("authorization outcomes are delivered only through" in r for r in behind.reasons)
    assert not _verdict(delivered_through=100.5).complete, "it must clear the horizon by the margin"
    assert _verdict(delivered_through=101.0).complete


def test_no_delivery_watermark_is_never_complete() -> None:
    verdict = _verdict(delivered_through=None)
    assert verdict.reasons == ("authorization outcomes have no delivery watermark",)


def test_every_failing_path_is_reported() -> None:
    verdict = _verdict(_coverage(Gap("s1", _at(40.0), _at(50.0))), delivered_through=10.0)
    assert len(verdict.reasons) == 2


def test_an_inverted_horizon_is_refused() -> None:
    with pytest.raises(ValueError, match="before it starts"):
        _verdict(start=100.0, end=0.0)
