"""The frozen `eval-v2` dataset (Phase 3 Stage 2 step 12).

The dataset is gitignored, so what is committed is its reproduction contract: the candidate
manifest's configuration, stream digests and row counts, the materialised files' digests, the
materialisation run record, and `LPC-5`'s recorded outcome. eval-v2 is NOT `LPC-5` compliant --
strict acceptance under revision 4 is FAIL with zero Category A findings -- and these tests keep the
manifest saying so.

The fast lane checks the contract's integrity. The full reproduction is `slow`-marked, as eval-v1's
is: regenerating a million rows on every commit would be absurd, and skipping it would make the
freeze decorative.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from data.generator.config import GENERATOR_VERSION, GeneratorConfig
from data.generator.lpc5 import declaration as d
from eval.track_a import freeze_candidate

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
FREEZE = ROOT / "eval" / "track_a" / "eval-v2.manifest.json"
CANDIDATE = ROOT / "eval" / "track_a" / "eval-v2.candidate.manifest.json"


@pytest.fixture(scope="module")
def manifest() -> dict[str, Any]:
    assert FREEZE.is_file(), "eval-v2 is not frozen: Stage 2 step 12 is unmet"
    return json.loads(FREEZE.read_text(encoding="utf-8"))


def test_the_freeze_manifest_is_complete(manifest: dict[str, Any]) -> None:
    for field in (
        "dataset_version",
        "dataset_digest",
        "fraud_scenario_config_digest",
        "generator_version",
        "row_count",
        "config",
        "streams",
        "files",
        "run_id",
        "candidate_run_id",
        "git_commit_sha",
        "env_lock_digest",
        "regenerate",
        "lpc5",
        "disclosures",
        "coverage_floor",
    ):
        assert manifest.get(field), f"freeze manifest is missing {field}"
    assert manifest["dataset_version"] == "eval-v2"
    assert set(manifest["files"]) == {f"{topic}.parquet" for topic in manifest["streams"]}


def test_the_frozen_manifest_is_the_candidate_that_was_judged(manifest: dict[str, Any]) -> None:
    candidate = json.loads(CANDIDATE.read_text(encoding="utf-8"))
    for key in ("config", "fraud_scenario_config_digest", "dataset_digest", "row_count", "streams"):
        assert manifest[key] == candidate[key], key
    assert manifest["candidate_run_id"] == candidate["run_id"]


def test_the_recorded_config_reproduces_its_digest_and_the_generator_matches(
    manifest: dict[str, Any],
) -> None:
    config = GeneratorConfig.from_mapping(manifest["config"])
    assert config.digest() == manifest["fraud_scenario_config_digest"]
    assert config.baseline_identity is not None, "eval-v2 is generated under its gate"
    assert manifest["generator_version"] == GENERATOR_VERSION


def test_the_lpc5_outcome_is_recorded_honestly(manifest: dict[str, Any]) -> None:
    lpc5 = manifest["lpc5"]
    assert lpc5["strict_acceptance"] == "FAIL"
    assert lpc5["category_a_findings"] == 0
    assert lpc5["compliant"] is False
    assert lpc5["criterion"]["revision"] == d.REVISION
    assert lpc5["category_b"] and lpc5["category_c"]
    assert (ROOT / lpc5["evidence"]).is_file()
    text = " ".join(manifest["disclosures"])
    assert "not LPC-5 compliant" in text
    assert "natural fraud prevalence" in text
    if manifest["coverage_floor"]["instances_added"]:
        assert manifest["coverage_floor"]["disclosure"] in manifest["disclosures"]


def test_the_materialisation_run_record_is_committed_and_clean(manifest: dict[str, Any]) -> None:
    record = ROOT / "eval" / "manifest" / f"{manifest['run_id']}.json"
    assert record.is_file(), f"the materialisation run record {record.name} is not committed"
    written = json.loads(record.read_text(encoding="utf-8"))
    assert written["dataset_digest"] == manifest["dataset_digest"]
    assert written["dirty_worktree"] is False


@pytest.mark.slow
def test_the_full_dataset_reproduces_every_recorded_stream(manifest: dict[str, Any]) -> None:
    config = GeneratorConfig.from_mapping(manifest["config"])
    measured = freeze_candidate.measure(config)
    assert measured["streams"] == manifest["streams"]
    assert measured["dataset_digest"] == manifest["dataset_digest"]
