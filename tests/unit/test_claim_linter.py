"""Benchmark-integrity linter self-test (docs/EVALUATION.md §8).

A linter that has never rejected anything is not known to work. These tests
fabricate violations and require the linter to catch each one.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]


def _load_linter():
    spec = importlib.util.spec_from_file_location(
        "check_claims", ROOT / "scripts" / "check_claims.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["check_claims"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def linter(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Point the linter at a throwaway docs tree and manifest directory."""
    mod = _load_linter()
    docs = tmp_path / "docs"
    docs.mkdir()
    manifests = tmp_path / "manifests"
    manifests.mkdir()
    monkeypatch.setattr(mod, "ROOT", tmp_path)
    monkeypatch.setattr(mod, "MANIFEST_DIR", manifests)

    def scan_with(text: str, manifest: dict | None = None) -> list[str]:
        (docs / "REPORT.md").write_text(text)
        monkeypatch.setattr(mod, "SCAN", [docs / "REPORT.md"])
        if manifest is not None:
            (manifests / f"{manifest['run_id']}.json").write_text(json.dumps(manifest))
        return mod.scan()

    return scan_with


def _manifest(**over: object) -> dict:
    base = {
        "run_id": "run-abc123",
        "track": "SYNTHETIC",
        "llm_tier": "EVAL",
        "dirty_worktree": False,
    }
    base.update(over)
    return base


# --- rule 1: every published number must cite a resolvable run_id -----------


def test_fabricated_number_without_run_id_is_rejected(linter) -> None:
    v = linter("TRACE-X achieves PR-AUC of 0.94 on fraud detection.")
    assert v, "a fabricated benchmark number MUST fail the build"
    assert "without a run_id" in v[0]


def test_number_citing_unknown_run_id_is_rejected(linter) -> None:
    v = linter("PR-AUC 0.94 (run_id: does-not-exist)")
    assert any("does not resolve" in x for x in v)


def test_number_with_valid_manifest_is_accepted(linter) -> None:
    v = linter("PR-AUC 0.94 (run_id: run-abc123)", _manifest())
    assert v == [], f"a properly cited number must pass, got {v}"


# --- rule 2: only the EVAL tier may publish quality numbers ------------------


def test_non_eval_tier_quality_number_is_rejected(linter) -> None:
    v = linter("PR-AUC 0.94 (run_id: run-abc123)", _manifest(llm_tier="SMOKE"))
    assert any("only EVAL may publish" in x for x in v)


# --- rule 3: Track-B run_id may not sit beside an agent-quality metric -------


def test_external_run_beside_agent_metric_is_rejected(linter) -> None:
    v = linter(
        "evidence precision of 0.88 measured (run_id: run-abc123)",
        _manifest(track="EXTERNAL"),
    )
    assert any("Track-B" in x for x in v)


# --- rule 4: a dirty worktree invalidates publication -----------------------


def test_dirty_worktree_run_is_rejected(linter) -> None:
    v = linter("PR-AUC 0.94 (run_id: run-abc123)", _manifest(dirty_worktree=True))
    assert any("dirty worktree" in x for x in v)


# --- targets and examples are not results -----------------------------------


@pytest.mark.parametrize(
    "line",
    [
        "Target: p99 < 100 ms at 500 TPS.",
        "The budget is p95 under 60 s per investigation.",
        "The ML arm must beat rules-only on PR-AUC.",
        "e.g. PR-AUC 0.94 would be reported here.",
    ],
)
def test_stated_targets_are_not_treated_as_published_results(linter, line: str) -> None:
    assert linter(line) == [], f"a stated target must not trip the linter: {line!r}"


def test_numbers_inside_code_fences_are_ignored(linter) -> None:
    assert linter("```\nPR-AUC 0.94\n```") == []


# --- latency/cost from a non-EVAL tier IS publishable -----------------------


def test_operational_metrics_publish_from_any_tier(linter) -> None:
    """Tier gating blocks quality claims only -- it must not block latency or cost."""
    v = linter("Measured p99 of 84 ms (run_id: run-abc123)", _manifest(llm_tier="CI"))
    assert not any("only EVAL may publish" in x for x in v), (
        "tier gating must not block operational metrics; it would get bypassed if it did"
    )
