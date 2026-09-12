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
        "record_type": "EVAL",
        "track": "SYNTHETIC",
        "llm_tier": "EVAL",
        "git_commit_sha": "0" * 40,
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


# ------------------------------------- generator run records (Phase 1) ------
#
# Phase 1 produces real measurements before the full RunManifest of ADR-0017
# exists. These tests exist because the linter did NOT catch a generation rate
# written into an ADR during step 6: the throughput pattern required the words
# "throughput" or "sustained" nearby, so "54.6k tx/s" passed silently. A number
# nobody checks is worse than one that is blocked, because it reads as though it
# had been checked.


def _generator_record(**over: object) -> dict:
    base: dict = {
        "run_id": "gen-20260912-probe-abcd1234",
        "record_type": "GENERATOR",
        "track": "SYNTHETIC",
        "git_commit_sha": "0" * 40,
        "dirty_worktree": False,
        "generator_version": "1.0.0",
        "seed": 42,
        "fraud_scenario_config_digest": "sha256:" + "a" * 64,
        "dataset_version": "probe-v1",
        "dataset_digest": "sha256:" + "b" * 64,
        "row_count": 1000,
        "env_lock_digest": "sha256:" + "c" * 64,
        "python_version": "3.12.0",
        "started_at": "2026-09-12T10:00:00Z",
        "finished_at": "2026-09-12T10:00:10Z",
        "validation_policy": "all",
        "measured": {"generation_rate_tx_per_s": 54000.0},
    }
    base.update(over)
    return base


CITED = "(run_id: gen-20260912-probe-abcd1234)"


def test_a_generation_rate_without_a_run_id_is_rejected(linter) -> None:
    """The exact gap that let an unbacked number into an ADR."""
    violations = linter("The generator produced 54,000 tx/s on the reference machine.")
    assert violations
    assert "without a run_id" in violations[0]


def test_a_generation_rate_with_a_resolvable_run_id_passes(linter) -> None:
    assert linter(f"Generation reached 54,000 tx/s {CITED}.", _generator_record()) == []


def test_a_dataset_size_without_a_run_id_is_rejected(linter) -> None:
    assert linter("The dataset occupies 412 MB on disk.")


def test_a_generator_record_cannot_back_a_quality_claim(linter) -> None:
    """A generation run has no model and no inference: it can substantiate
    throughput and size, never accuracy."""
    violations = linter(f"PR-AUC was 0.91 {CITED}.", _generator_record())
    assert violations
    assert any("GENERATOR record" in v for v in violations)


def test_an_incomplete_generator_record_cannot_back_anything(linter) -> None:
    """A number whose provenance is incomplete is indistinguishable from one
    that was invented (ADR-0017)."""
    record = _generator_record()
    del record["dataset_digest"]
    violations = linter(f"Generation reached 54,000 tx/s {CITED}.", record)
    assert violations
    assert any("missing" in v for v in violations)


def test_a_dirty_worktree_record_cannot_back_a_number(linter) -> None:
    """docs/EVALUATION.md §8 rule 4."""
    violations = linter(
        f"Generation reached 54,000 tx/s {CITED}.", _generator_record(dirty_worktree=True)
    )
    assert violations
    assert any("dirty worktree" in v for v in violations)


def test_target_prose_is_still_exempt(linter) -> None:
    """A budget is not a measured result. Gating it would be obstructive, and an
    obstructive rule gets worked around."""
    assert linter("The target is at least 50,000 tx/s single-process.") == []


# --- Phase 2: load-test records --------------------------------------------
#
# The gap these close: `incomplete_fields` previously returned an empty list for
# every record type that was not GENERATOR, so a record could substantiate a
# published p99 simply by declaring a type nobody had defined requirements for.
# Phase 2's exit condition is a committed load report with real measured
# numbers, which makes that hole load-bearing.


def _loadtest_record(**over: object) -> dict:
    base: dict = {
        "run_id": "load-20260912-gateway-abcd1234",
        "record_type": "LOADTEST",
        "track": "SYNTHETIC",
        "git_commit_sha": "0" * 40,
        "dirty_worktree": False,
        "env_lock_digest": "sha256:" + "c" * 64,
        "python_version": "3.12.0",
        "started_at": "2026-09-12T10:00:00Z",
        "finished_at": "2026-09-12T10:10:00Z",
        "service": "trace-gateway",
        "service_version": "0.1.0",
        "tool": "k6",
        "tool_version": "v0.49.0",
        "target_tps": 500,
        "duration_s": 600,
        "rule_pack_digest": "sha256:" + "d" * 64,
        "threshold_config_digest": "sha256:" + "e" * 64,
        "feature_set_version": "1.0.0",
        "degraded_mode": False,
        "measured": {"p99_ms": 42.0, "p50_ms": 7.0, "http_5xx": 0},
    }
    base.update(over)
    return base


LOAD_CITED = "(run_id: load-20260912-gateway-abcd1234)"


def test_a_latency_number_with_a_complete_loadtest_record_passes(linter) -> None:
    assert linter(f"Measured p99 was 42 ms {LOAD_CITED}.", _loadtest_record()) == []


def test_an_incomplete_loadtest_record_cannot_back_a_latency_number(linter) -> None:
    """Without the rule-pack digest a p99 cannot be attributed to the behaviour
    that produced it, so the record substantiates nothing."""
    record = _loadtest_record()
    del record["rule_pack_digest"]
    violations = linter(f"Measured p99 was 42 ms {LOAD_CITED}.", record)
    assert violations
    assert any("missing" in v and "rule_pack_digest" in v for v in violations)


def test_a_loadtest_record_cannot_back_a_quality_claim(linter) -> None:
    """A load run exercises a service; it trains and scores no model."""
    violations = linter(f"PR-AUC was 0.91 {LOAD_CITED}.", _loadtest_record())
    assert violations
    assert any("LOADTEST record" in v for v in violations)


def test_an_unknown_record_type_cannot_back_anything(linter) -> None:
    """The hole itself: a type with no declared requirements must not resolve.

    Before this, `record_type: "WHATEVER"` skipped completeness checking
    entirely and any number citing it passed.
    """
    violations = linter(
        f"Measured p99 was 42 ms {LOAD_CITED}.",
        _loadtest_record(record_type="WHATEVER"),
    )
    assert violations
    assert any("no declared required fields" in v for v in violations)


def test_a_record_with_no_record_type_cannot_back_anything(linter) -> None:
    record = _loadtest_record()
    del record["record_type"]
    violations = linter(f"Measured p99 was 42 ms {LOAD_CITED}.", record)
    assert violations
    assert any("no declared required fields" in v for v in violations)


def test_a_dirty_worktree_loadtest_record_cannot_back_a_number(linter) -> None:
    violations = linter(
        f"Measured p99 was 42 ms {LOAD_CITED}.", _loadtest_record(dirty_worktree=True)
    )
    assert violations
    assert any("dirty worktree" in v for v in violations)


# --- section-scoped run_id resolution ---------------------------------------
#
# Benchmark reports declare a run_id under a heading and then report several
# numbers beneath it. Requiring every line to repeat the id would make reports
# unreadable, and a rule that forces bad writing gets worked around. The scope is
# deliberately ONE section: a heading clears it, so a number cannot inherit
# provenance from an unrelated run earlier in the file.


def test_a_run_id_declared_in_a_section_covers_numbers_beneath_it(linter) -> None:
    report = "\n".join(
        [
            "## Run: generation rate",
            "",
            "- `run_id`: `gen-20260912-probe-abcd1234`",
            "- **34,413.1 tx/s**",
        ]
    )
    assert linter(report, _generator_record()) == []


def test_a_heading_clears_the_section_run_id(linter) -> None:
    """Otherwise a number could inherit provenance from an unrelated run."""
    report = "\n".join(
        [
            "## Run: one",
            "- run_id: gen-20260912-probe-abcd1234",
            "",
            "## Run: two",
            "- **54,000 tx/s**",
        ]
    )
    violations = linter(report, _generator_record())
    assert violations
    assert "without a run_id" in violations[0]


def test_a_section_run_id_that_does_not_resolve_is_still_a_violation(linter) -> None:
    report = "\n".join(
        [
            "## Run: invented",
            "- run_id: gen-does-not-exist",
            "- **54,000 tx/s**",
        ]
    )
    violations = linter(report, _generator_record())
    assert violations
    assert "does not resolve" in violations[0]


def test_a_section_run_id_still_enforces_the_tier_gate(linter) -> None:
    """Widening WHERE an id may be declared must not widen WHAT it licenses."""
    report = "\n".join(
        [
            "## Results",
            "- `run_id`: `gen-20260912-probe-abcd1234`",
            "- PR-AUC of 0.91",
        ]
    )
    violations = linter(report, _generator_record())
    assert violations
    assert any("GENERATOR record" in v for v in violations)


def test_a_number_with_no_run_id_anywhere_is_still_rejected(linter) -> None:
    assert linter("The gateway sustained 54,000 req/s in production.")


def test_benchmark_reports_are_in_scope() -> None:
    """The largest surface where measurements are actually published.

    Leaving `benchmarks/` unscanned meant the gate covered the documents least
    likely to carry a raw number and skipped the ones written to carry them.
    """
    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location(
        "check_claims_scope", ROOT / "scripts" / "check_claims.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["check_claims_scope"] = mod
    spec.loader.exec_module(mod)
    scanned = {p.relative_to(ROOT).parts[0] for p in mod.SCAN}
    assert "benchmarks" in scanned
    assert "docs" in scanned


def test_a_benchmark_record_cannot_back_a_quality_claim(linter) -> None:
    """A component benchmark exercises no model: it can substantiate an
    estimator's error and a representation's memory, never accuracy."""
    record = {
        "run_id": "bench-20260912-cardinality-abcd1234",
        "record_type": "BENCHMARK",
        "track": "SYNTHETIC",
        "git_commit_sha": "0" * 40,
        "dirty_worktree": False,
        "env_lock_digest": "sha256:" + "c" * 64,
        "python_version": "3.12.0",
        "started_at": "2026-09-12T10:00:00Z",
        "finished_at": "2026-09-12T10:05:00Z",
        "subject": "online-feature-store-cardinality",
        "tool": "redis",
        "tool_version": "7.4.0",
        "measured": {"max_relative_error_approximate": 0.0105},
    }
    cited = "(run_id: bench-20260912-cardinality-abcd1234)"
    assert linter(f"Read p99 was 0.25 ms {cited}.", record) == []
    violations = linter(f"PR-AUC was 0.91 {cited}.", record)
    assert any("BENCHMARK record" in v for v in violations)


def test_an_incomplete_benchmark_record_cannot_back_anything(linter) -> None:
    record = {
        "run_id": "bench-20260912-cardinality-abcd1234",
        "record_type": "BENCHMARK",
        "track": "SYNTHETIC",
        "git_commit_sha": "0" * 40,
        "dirty_worktree": False,
        "measured": {"x": 1},
    }
    violations = linter(
        "Read p99 was 0.25 ms (run_id: bench-20260912-cardinality-abcd1234).", record
    )
    assert violations
    assert any("missing" in v and "tool_version" in v for v in violations)


def test_a_latency_result_is_caught_in_either_word_order(linter) -> None:
    """ "p99 was 42 ms" and "measured 42 ms p99" are the same claim."""
    assert linter("The gateway p99 was 42 ms under load.")
    assert linter("The gateway measured 42 ms p99 under load.")


def test_a_budget_reference_is_not_mistaken_for_a_result(linter) -> None:
    """The asymmetry in the two latency patterns, stated as a test.

    "a 100 ms p99" is how every document in this repo refers to the BUDGET, and
    ADR-0001 and ADR-0002 both do so correctly. Neither may be edited to suit a
    linter -- ADRs are immutable once accepted -- so the reversed pattern requires
    a measurement verb. A gate that cries wolf on correct prose is one people
    learn to override.
    """
    assert linter("Cold starts are fatal to a 100 ms p99; Spark cannot serve one.") == []
    assert linter("Investigations are far too slow for a 100 ms p99 synchronous path.") == []


def test_a_comparator_only_exempts_prose_when_a_quantity_follows(linter) -> None:
    """ "under 100 ms" is a budget; "under load" is not a statement about size.

    Listing "under" as a bare target word exempted the single phrasing most
    likely to carry a real measurement.
    """
    assert linter("Scoring stays under 100 ms at the chosen operating point.") == []
    assert linter("The gateway p99 was 42 ms under load.")
