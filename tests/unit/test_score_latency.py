"""The score-latency benchmark's arithmetic, without Redis (Phase 3 Step 12).

The timing itself runs against a real Redis in the benchmark. These hold the pieces that turn
samples into published percentiles and lay out an account's history, so a wrong rank or a history
that leaks out of the raw window fails here.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]


def _load() -> Any:
    spec = importlib.util.spec_from_file_location(
        "score_latency", ROOT / "benchmarks" / "features" / "score_latency.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["score_latency"] = module
    spec.loader.exec_module(module)
    return module


sl = _load()


def test_percentile_is_nearest_rank() -> None:
    samples = [float(v) for v in range(1, 101)]
    assert sl.percentile(samples, 0.50) == 50.0
    assert sl.percentile(samples, 0.99) == 99.0
    assert sl.percentile(samples, 1.0) == 100.0
    assert sl.percentile([7.0], 0.99) == 7.0
    assert sl.percentile([3.0, 1.0, 2.0], 0.5) == 2.0, "order of arrival does not matter"


def test_percentile_refuses_nothing_to_rank_and_a_meaningless_quantile() -> None:
    with pytest.raises(ValueError):
        sl.percentile([], 0.5)
    for q in (0.0, 1.5):
        with pytest.raises(ValueError):
            sl.percentile([1.0], q)


def test_a_history_stays_inside_the_day_before_now_oldest_first() -> None:
    now = 1_790_000_000_000
    for depth in (1, 64, 8_192):
        times = sl.history_times_ms(depth, now)
        assert len(times) == depth
        assert times == sorted(times) and len(set(times)) == depth
        assert now - sl.DAY_MS < times[0] and times[-1] < now
    assert sl.history_times_ms(0, now) == []
