"""The frozen `eval-v1` dataset (ROADMAP Phase 1 exit condition).

The dataset itself is gitignored -- datasets are never committed (CLAUDE.md §17)
-- so what is committed is the **reproduction contract**: the seed, the full
config, the generator version, and the digest the combination must produce.

`eval-v1` is referenced by digest and is never regenerated in place. A change to
the generator or the scenario mix changes the digest, and these tests fail
loudly rather than letting a differently-shaped dataset quietly inherit the name
every recorded result cites (`docs/EVALUATION.md` §2).

The full-size reproduction is `slow`-marked: regenerating a million rows on every
commit would be absurd, and skipping the check entirely would make the freeze
decorative. The fast lane verifies the contract's *integrity* and reproduces a
deterministic prefix.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from data.generator.config import GENERATOR_VERSION, GeneratorConfig
from data.generator.digest import digest_of
from data.generator.engine import generate_dataset

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
FREEZE = ROOT / "eval" / "track_a" / "eval-v1.manifest.json"


@pytest.fixture(scope="module")
def manifest() -> dict:
    assert FREEZE.is_file(), "eval-v1 is not frozen; the Phase 1 exit condition is unmet"
    return json.loads(FREEZE.read_text())


def test_the_freeze_manifest_is_complete(manifest: dict) -> None:
    """Everything needed to reproduce the dataset, or the freeze means nothing."""
    for field in (
        "dataset_version",
        "dataset_digest",
        "fraud_scenario_config_digest",
        "generator_version",
        "row_count",
        "config",
        "files",
        "run_id",
        "git_commit_sha",
        "env_lock_digest",
        "regenerate",
    ):
        assert manifest.get(field), f"freeze manifest is missing {field}"


def test_the_recorded_config_reproduces_the_recorded_config_digest(manifest: dict) -> None:
    """The committed config and the committed digest must agree.

    If they did not, the manifest would describe a dataset nobody could rebuild
    -- which is the same as not having frozen anything.
    """
    config = GeneratorConfig.from_mapping(manifest["config"])
    assert config.digest() == manifest["fraud_scenario_config_digest"]


def test_the_generator_version_matches(manifest: dict) -> None:
    """A generator version bump means output may differ for an unchanged config,
    so the freeze must be re-taken deliberately rather than drift."""
    assert manifest["generator_version"] == GENERATOR_VERSION, (
        "the generator version changed since eval-v1 was frozen. If the output changed, "
        "this is eval-v2 plus a manifest-diff note (docs/EVALUATION.md §2) -- never a "
        "silent update of eval-v1."
    )


def test_the_manifest_records_the_row_count_and_fraud_rate(manifest: dict) -> None:
    assert manifest["row_count"] == manifest["config"]["row_count"]
    assert 0 < manifest["realised_fraud_rate"] < 0.05
    assert manifest["fraudulent_transactions"] > 0


def test_the_dataset_was_frozen_from_a_recorded_run(manifest: dict) -> None:
    """The freeze cites a run record, so the measurement and the artefact agree."""
    record = ROOT / "eval" / "manifest" / f"{manifest['run_id']}.json"
    assert record.is_file(), f"freeze cites {manifest['run_id']} but no such run record exists"
    recorded = json.loads(record.read_text())
    assert recorded["dataset_digest"] == manifest["dataset_digest"]
    assert recorded["dirty_worktree"] is False, (
        "eval-v1 was frozen from a dirty worktree; the tree that produced it cannot be "
        "reconstructed, so the dataset is not reproducible (EVALUATION.md §8 rule 4)"
    )


def test_a_prefix_of_eval_v1_reproduces_deterministically(manifest: dict) -> None:
    """Fast-lane reproduction.

    The full million rows are checked by the `slow` test below. This one proves
    the recorded config is *usable* -- that it builds, generates, and gives a
    stable digest -- on every commit.
    """
    config = GeneratorConfig.from_mapping({**manifest["config"], "row_count": 2000})
    first, count = digest_of(r.event for r in generate_dataset(config))
    second, _ = digest_of(r.event for r in generate_dataset(config))
    assert first == second
    assert count > 0


@pytest.mark.slow
def test_eval_v1_reproduces_its_recorded_digest(manifest: dict) -> None:
    """Full-size reproduction: the claim the freeze actually makes.

    Slow-marked because it regenerates a million rows. Run before any release and
    whenever the generator changes: `pytest -m slow`.
    """
    config = GeneratorConfig.from_mapping(manifest["config"])
    digest, count = digest_of(
        row.event for row in generate_dataset(config) if row.topic == "tx.raw.v1"
    )
    assert count == manifest["row_count"]
    assert digest == manifest["dataset_digest"], (
        "eval-v1 no longer reproduces its recorded digest. Either the generator changed "
        "(then this is eval-v2, with a manifest-diff note) or something is non-deterministic "
        "(then ADR-0029's central claim is broken)."
    )
