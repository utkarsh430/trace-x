"""Stage 2 step 10: freeze the eval-v2 candidate for `LPC-5` acceptance.

Generates the candidate once, in memory, and writes the reproduction contract the acceptance run
(step 11) must reproduce before any check runs (`LPC-5` §1.2):
- the full configuration and its digest, with the corrections the eval-v2 gate applies;
- every stream's digest and row count, and the transaction digest eval-v1 is frozen by;
- per pattern, the instances the weighted mix planned, the instances G2 added, the instances with a
  transaction and their fraudulent transactions, with G2's disclosure (§14.4);
- the criterion's id, revision and digest, because a revision after this freeze invalidates the
  candidate (§0);
- the generator version and the provenance of the tree that produced it.

The dataset itself is not written: datasets are never committed (CLAUDE.md §17). A dirty worktree is
refused, because a candidate frozen from a tree nobody can reconstruct could not be cited, and a
frozen candidate is never re-frozen in place.

    python -m eval.track_a.freeze_candidate           # write the candidate manifest
    python -m eval.track_a.freeze_candidate --check   # regenerate and compare (exit 2 on mismatch)
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import platform
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

from data.generator.config import (
    CORRECTIONS,
    GENERATOR_VERSION,
    BaselineIdentityConfig,
    GeneratorConfig,
)
from data.generator.digest import DatasetDigest
from data.generator.emit import NullSink, ValidationPolicy, write_rows
from data.generator.engine import coverage_mix, generate_dataset, plan_fraud
from data.generator.lpc5 import declaration as d
from data.generator.population import build_universe
from data.generator.record import env_lock_digest, git_commit_sha, is_dirty, new_run_id

from trace_core.domain.enums import FraudPattern

ROOT: Final = Path(__file__).resolve().parents[2]
EVAL_V1: Final = ROOT / "eval" / "track_a" / "eval-v1.manifest.json"
CANDIDATE = ROOT / "eval" / "track_a" / "eval-v2.candidate.manifest.json"
DATASET_VERSION: Final = "eval-v2"
COVERAGE_FLOOR_DISCLOSURE: Final = (
    "G2 added instances to patterns below 20: this dataset's scenario mix is a coverage floor, "
    "not natural prevalence (LPC-5 §14.4)."
)


def candidate_config() -> GeneratorConfig:
    """eval-v1's frozen scale, window and seed, with the eval-v2 gate at its declared defaults."""
    frozen = json.loads(EVAL_V1.read_text(encoding="utf-8"))["config"]
    gate = BaselineIdentityConfig().model_dump(mode="json")
    return GeneratorConfig.from_mapping({**frozen, "baseline_identity": gate})


def criterion() -> dict[str, Any]:
    """The criterion as it is on disk."""
    body = (ROOT / d.CRITERION_PATH).read_bytes()
    return {
        "id": d.CRITERION_ID,
        "revision": d.REVISION,
        "sha256": hashlib.sha256(body).hexdigest(),
    }


def gate(config: GeneratorConfig) -> dict[str, list[str]]:
    """Which corrections the configuration's gate applies (§14.3 names each)."""
    block = config.baseline_identity
    applied = [c for c in CORRECTIONS if block is not None and block.applies(c)]
    return {
        "corrections_applied": applied,
        "corrections_disabled": [c for c in CORRECTIONS if c not in applied],
    }


def measure(config: GeneratorConfig) -> dict[str, Any]:
    """One validated generation: every value an acceptance run must reproduce."""
    universe = build_universe(config)
    mix = coverage_mix(plan_fraud(config, universe)[0])
    transactions = DatasetDigest()
    streams: dict[str, DatasetDigest] = {}
    total = 0
    fraudulent: Counter[str] = Counter()
    instances: defaultdict[str, set[str]] = defaultdict(set)
    rows = write_rows(
        generate_dataset(config, universe),
        NullSink(),
        ValidationPolicy.ALL,
        transactions,
        topic_digests=streams,
    )
    for row in rows:
        label = row.label
        if label is None or not label.is_fraud:
            continue
        total += 1
        if label.fraud_pattern is not None:
            fraudulent[label.fraud_pattern.value] += 1
            if label.scenario_instance_id is not None:
                instances[label.fraud_pattern.value].add(label.scenario_instance_id)
    added = sum(extra for _, extra in mix.values())
    return {
        "dataset_digest": transactions.hexdigest(),
        "row_count": transactions.row_count,
        "streams": {
            topic: {"digest": digest.hexdigest(), "rows": digest.row_count}
            for topic, digest in sorted(streams.items())
        },
        "fraudulent_transactions": total,
        "realised_fraud_rate": round(total / max(1, transactions.row_count), 6),
        "scenarios": {
            pattern.value: {
                "planned_by_mix": mix[pattern.value][0],
                "added_by_g2": mix[pattern.value][1],
                "instances_with_a_transaction": len(instances[pattern.value]),
                "fraudulent_transactions": fraudulent[pattern.value],
            }
            for pattern in FraudPattern
        },
        "coverage_floor": {
            "instances_added": added,
            "disclosure": COVERAGE_FLOOR_DISCLOSURE if added else None,
        },
    }


def mismatches(
    manifest: Mapping[str, Any], config: GeneratorConfig, measured: Mapping[str, Any]
) -> list[str]:
    """Every recorded value a regeneration must equal (§1.2), and the criterion of the freeze."""
    expected = {
        "fraud_scenario_config_digest": config.digest(),
        "gate": gate(config),
        "criterion": criterion(),
        **measured,
    }
    return [
        f"{key}: manifest {manifest.get(key)!r}, regenerated {value!r}"
        for key, value in expected.items()
        if manifest.get(key) != value
    ]


def _refusal() -> str | None:
    if is_dirty():
        return "the worktree is dirty, so the frozen candidate could not be cited"
    if CANDIDATE.exists():
        return f"a frozen candidate is never re-frozen in place: {CANDIDATE}"
    if criterion()["sha256"] != d.CRITERION_SHA256:
        return "the criterion document does not match the declaration's digest"
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--check", action="store_true", help="regenerate and compare")
    args = parser.parse_args(argv)
    out = sys.stdout
    if args.check:
        frozen = json.loads(CANDIDATE.read_text(encoding="utf-8"))
        config = GeneratorConfig.from_mapping(frozen["config"])
        problems = mismatches(frozen, config, measure(config))
        out.write("candidate reproduced\n" if not problems else "candidate MISMATCH (invalid)\n")
        for problem in problems:
            out.write(f"  {problem}\n")
        return 2 if problems else 0
    refusal = _refusal()
    if refusal is not None:
        out.write(f"refused: {refusal}\n")
        return 2
    started = dt.datetime.now(dt.UTC)
    config = candidate_config()
    manifest: dict[str, Any] = {
        "$comment": (
            "The eval-v2 candidate's reproduction contract for LPC-5 acceptance (Stage 2 step 10). "
            "The dataset is not committed; the acceptance run regenerates it and must reproduce "
            "every stream digest and row count here before any check runs (LPC-5 §1.2). A "
            "criterion revision after this freeze invalidates the candidate (LPC-5 §0)."
        ),
        "dataset_version": DATASET_VERSION,
        "run_id": new_run_id(DATASET_VERSION, started),
        "config": json.loads(config.canonical_json()),
        "fraud_scenario_config_digest": config.digest(),
        "gate": gate(config),
        "criterion": criterion(),
        **measure(config),
        "validation_policy": ValidationPolicy.ALL,
        "generator_version": GENERATOR_VERSION,
        "git_commit_sha": git_commit_sha(),
        "dirty_worktree": False,
        "env_lock_digest": env_lock_digest(),
        "python_version": platform.python_version(),
        "frozen_at": started.isoformat().replace("+00:00", "Z"),
        "regenerate": "python -m eval.track_a.freeze_candidate --check",
        "acceptance": (
            "python -m eval.track_a.lpc5_control --control eval-v2 "
            "--candidate eval/track_a/eval-v2.candidate.manifest.json"
        ),
    }
    CANDIDATE.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    out.write(f"written: {CANDIDATE}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
