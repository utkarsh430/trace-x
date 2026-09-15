"""The observation log A/B's pre-registered rule and its report (ADR-0051 §7).

The rule is applied to hand-built records, so a comparison that always kept option A would fail
here as surely as one that never did. The runs themselves need the full stack and are recorded
under `benchmarks/gateway/observation-log-ab.md`.
"""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]


def _load() -> Any:
    spec = importlib.util.spec_from_file_location(
        "observation_log_ab", ROOT / "scripts" / "observation_log_ab.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["observation_log_ab"] = module
    spec.loader.exec_module(module)
    return module


ab = _load()
CHECKS = {
    "log-off": {
        "writer_session": "active s",
        "observation_log": "not configured: x",
        "outbox_relay": "disabled",
    },
    "log-on": {"writer_session": "active s", "observation_log": "ok", "outbox_relay": "disabled"},
    "log-relay": {"writer_session": "active s", "observation_log": "ok", "outbox_relay": "running"},
}


def _results(
    p99: dict[str, tuple[float, float]], tps: dict[str, tuple[float, float]] | None = None
) -> list[Any]:
    tps = tps or dict.fromkeys(ab.ARMS, (499.9, 499.9))
    seen: dict[str, int] = {}
    results = []
    for index, arm in enumerate(ab.ORDER):
        nth = seen.get(arm, 0)
        seen[arm] = nth + 1
        results.append(
            ab.Result(
                arm=arm,
                run_id=f"load-20260915-gateway-{index:08x}",
                started_at=f"2026-09-15T02:{index:02d}:00Z",
                server_p99_ms=p99[arm][nth],
                achieved_tps=tps[arm][nth],
                client_p99_ms=9.0,
                dropped_iterations=0,
                http_5xx=0,
                gateway_checks=CHECKS[arm],
                git_commit_sha="a" * 40,
                dirty_worktree=False,
            )
        )
    return results


def test_the_relay_stays_when_it_is_within_the_no_relay_controls_noise() -> None:
    decision = ab.decide(
        _results({"log-off": (0.60, 0.62), "log-on": (0.66, 0.70), "log-relay": (0.69, 0.71)})
    )
    (p99, rate) = decision.relay
    assert p99.noise == pytest.approx(0.04) and p99.difference == pytest.approx(0.02)
    assert p99.within_noise and rate.within_noise
    assert decision.keep_option_a


def test_the_relay_moves_when_it_leaves_the_noise_on_either_metric() -> None:
    slower = ab.decide(
        _results({"log-off": (0.60, 0.61), "log-on": (0.66, 0.67), "log-relay": (0.90, 0.91)})
    )
    assert not slower.relay[0].within_noise and not slower.keep_option_a
    starved = ab.decide(
        _results(
            dict.fromkeys(ab.ARMS, (0.66, 0.66)),
            {"log-off": (499.9, 499.9), "log-on": (499.9, 499.8), "log-relay": (450.0, 451.0)},
        )
    )
    assert starved.relay[0].within_noise and not starved.relay[1].within_noise
    assert not starved.keep_option_a


def test_the_publishers_cost_is_compared_against_its_control() -> None:
    decision = ab.decide(
        _results({"log-off": (0.60, 0.62), "log-on": (0.66, 0.70), "log-relay": (0.69, 0.71)})
    )
    p99 = decision.publisher[0]
    assert (p99.left, p99.right) == ("log-off", "log-on")
    assert p99.left_mean == pytest.approx(0.61) and p99.right_mean == pytest.approx(0.68)


def test_a_comparison_the_pre_registration_does_not_describe_is_refused() -> None:
    good = _results(dict.fromkeys(ab.ARMS, (0.6, 0.6)))
    swapped = [good[0], good[2], good[1], *good[3:]]
    with pytest.raises(ab.ExperimentError, match="order"):
        ab.decide(swapped)
    with pytest.raises(ab.ExperimentError, match="dirty"):
        ab.decide([replace(good[0], dirty_worktree=True), *good[1:]])
    with pytest.raises(ab.ExperimentError, match="different commits"):
        ab.decide([replace(good[0], git_commit_sha="b" * 40), *good[1:]])
    mislabelled = replace(good[1], gateway_checks=CHECKS["log-off"])
    with pytest.raises(ab.ExperimentError, match="labelled log-on"):
        ab.decide([good[0], mislabelled, *good[2:]])


def test_every_measured_number_in_the_report_cites_a_run_id() -> None:
    results = _results({"log-off": (0.60, 0.62), "log-on": (0.66, 0.70), "log-relay": (0.69, 0.71)})
    text = ab.render(results, ab.decide(results))
    rows = [
        line
        for line in text.splitlines()
        if line.startswith("| ") and any(c.isdigit() for c in line)
    ]
    measured = [row for row in rows if not row.startswith("| # ")]
    assert len(measured) == len(ab.ORDER) + 4
    assert all("run_id:" in row for row in measured)
    assert "Option A is kept" in text
