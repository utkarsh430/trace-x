"""Feature history is COMPLETE only where both durable paths vouch (ADR-0051 §5; user, 2026-09-15).

Hand-built coverage and watermarks. Every INCOMPLETE clause has a COMPLETE control beside it, so a
verdict that refused everything would fail here as surely as one that allowed everything.
"""

from __future__ import annotations

import datetime as dt
from types import MappingProxyType

import pytest

from trace_core.observation.coverage import Coverage, Gap
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


def _coverage(*gaps: Gap) -> Coverage:
    return Coverage(sessions=MappingProxyType({}), unknown=tuple(gaps))


def _verdict(
    *,
    gaps: tuple[Gap, ...] = (),
    assessed_through: float = 200.0,
    delivered_through: float | None = 200.0,
    start: float = 0.0,
    end: float = 100.0,
) -> HistoryVerdict:
    return assess_history(
        horizon_start=_at(start),
        horizon_end=_at(end),
        observation_coverage=_coverage(*gaps),
        observations_assessed_through=_at(assessed_through),
        authorization_delivered_through=None
        if delivered_through is None
        else _at(delivered_through),
        clock_margin_s=MARGIN_S,
    )


def test_both_paths_vouching_for_the_whole_horizon_is_complete() -> None:
    verdict = _verdict()
    assert verdict.completeness is HistoryCompleteness.COMPLETE and verdict.reasons == ()


def test_an_observation_gap_inside_the_horizon_keeps_it_incomplete() -> None:
    verdict = _verdict(gaps=(Gap("s1", _at(40.0), _at(50.0), (7,)),))
    assert not verdict.complete
    assert any("observation gap" in reason for reason in verdict.reasons)


def test_a_gap_outside_the_horizon_and_its_margin_does_not_block() -> None:
    assert _verdict(gaps=(Gap("s1", _at(150.0), _at(160.0)),)).complete
    touching = _verdict(gaps=(Gap("s1", _at(100.5), _at(110.0)),))
    assert not touching.complete, "within the clock margin of the horizon's end"


def test_an_unknown_session_gap_keeps_the_horizon_incomplete() -> None:
    assert not _verdict(gaps=(Gap(None, _at(10.0), _at(12.0)),)).complete


def test_authorization_delivery_behind_the_horizon_keeps_it_incomplete() -> None:
    behind = _verdict(delivered_through=80.0)
    assert not behind.complete
    assert any("authorization outcomes are delivered only through" in r for r in behind.reasons)
    within_margin = _verdict(delivered_through=100.5)
    assert not within_margin.complete, "the watermark must clear the horizon by the clock margin"
    assert _verdict(delivered_through=101.0).complete


def test_no_delivery_watermark_is_never_complete() -> None:
    verdict = _verdict(delivered_through=None)
    assert not verdict.complete
    assert verdict.reasons == ("authorization outcomes have no delivery watermark",)


def test_observations_assessed_short_of_the_horizon_keep_it_incomplete() -> None:
    assert not _verdict(assessed_through=90.0).complete


def test_every_failing_path_is_reported() -> None:
    verdict = _verdict(gaps=(Gap("s1", _at(40.0), _at(50.0)),), delivered_through=10.0)
    assert len(verdict.reasons) == 2


def test_an_inverted_horizon_is_refused() -> None:
    with pytest.raises(ValueError, match="before it starts"):
        _verdict(start=100.0, end=0.0)
