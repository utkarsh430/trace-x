"""Timing semantics v1 at every boundary (`docs/PHASE3_PLAN.md` §4.3; ADR-0053 §3).

The values are pinned here, so a change that is not a new version fails. The backfill header is
cross-checked against the replay tooling that writes it, read from its source without importing it.
"""

from __future__ import annotations

import ast
import datetime as dt
from pathlib import Path

import pytest

from trace_core.domain.errors import NaiveDatetimeError
from trace_core.stream import timing

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
T = dt.datetime(2026, 9, 15, 12, 0, tzinfo=dt.UTC)
MS = dt.timedelta(milliseconds=1)


def test_the_v1_values_are_the_plan_s() -> None:
    assert timing.TIMING_SEMANTICS_VERSION == 1
    assert dt.timedelta(seconds=600) == timing.LATE_AFTER
    assert dt.timedelta(seconds=86_400) == timing.FUTURE_SKEW_LIMIT
    assert dt.timedelta(seconds=300) == timing.OTHER_PRODUCER_CLOCK_MARGIN


def test_the_backfill_header_is_the_one_the_replay_tooling_writes() -> None:
    tree = ast.parse((ROOT / "eval" / "replay" / "faults.py").read_text())
    constants = {
        node.target.id: ast.literal_eval(node.value)
        for node in tree.body
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value
        if node.target.id in {"REPLAY_MODE_HEADER", "BACKFILL_HEADER_VALUE"}
    }
    assert constants == {
        "REPLAY_MODE_HEADER": timing.REPLAY_MODE_HEADER,
        "BACKFILL_HEADER_VALUE": timing.BACKFILL_MODE,
    }


def test_late_is_strictly_more_than_600_seconds_of_arrival_delay() -> None:
    occurred = T - dt.timedelta(seconds=600)
    assert timing.is_late(logged_at=T, occurred_at=occurred, backfill=False) is False
    assert timing.is_late(logged_at=T + MS, occurred_at=occurred, backfill=False) is True
    assert (
        timing.is_late(logged_at=T, occurred_at=T + dt.timedelta(hours=1), backfill=False) is False
    )
    assert timing.arrival_delay_ms(logged_at=T + MS, occurred_at=occurred) == 600_001


def test_a_backfill_record_is_never_late_or_on_time() -> None:
    old = T - dt.timedelta(days=30)
    assert timing.is_late(logged_at=T, occurred_at=old, backfill=True) is None
    assert timing.is_backfill(
        [("trace_id", b"t"), (timing.REPLAY_MODE_HEADER, timing.BACKFILL_MODE)]
    )
    assert not timing.is_backfill([(timing.REPLAY_MODE_HEADER, b"paced"), ("x", None)])
    assert not timing.is_backfill([(timing.REPLAY_MODE_HEADER, None)])
    assert not timing.is_backfill([])


def test_a_gateway_event_is_skewed_only_beyond_a_day_after_its_own_receipt_time() -> None:
    received = T - dt.timedelta(hours=5)
    limit = received + dt.timedelta(seconds=86_400)
    for producer in ("trace-gateway@0.1.0", "trace-gateway"):
        assert not timing.future_skewed(
            occurred_at=limit, producer=producer, ingested_at=received, logged_at=T
        )
        assert timing.future_skewed(
            occurred_at=limit + MS, producer=producer, ingested_at=received, logged_at=T
        )


def test_another_producer_s_event_is_skewed_only_beyond_a_day_and_a_margin_after_arrival() -> None:
    limit = T + dt.timedelta(seconds=86_400 + 300)
    ingested = T + dt.timedelta(days=3)  # a producer's own clock is never trusted for this
    assert not timing.future_skewed(
        occurred_at=limit, producer="data.generator@1.0.0", ingested_at=ingested, logged_at=T
    )
    assert timing.future_skewed(
        occurred_at=limit + MS, producer="data.generator@1.0.0", ingested_at=ingested, logged_at=T
    )


def test_a_naive_time_is_refused_everywhere() -> None:
    naive = T.replace(tzinfo=None)
    with pytest.raises(NaiveDatetimeError):
        timing.is_late(logged_at=naive, occurred_at=T, backfill=False)
    with pytest.raises(NaiveDatetimeError):
        timing.arrival_delay_ms(logged_at=T, occurred_at=naive)
    with pytest.raises(NaiveDatetimeError):
        timing.future_skewed(
            occurred_at=T, producer="trace-gateway", ingested_at=naive, logged_at=T
        )
