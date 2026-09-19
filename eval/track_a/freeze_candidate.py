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
FINAL = ROOT / "eval" / "track_a" / "eval-v2.manifest.json"
ACCEPTANCE_EVIDENCE: Final = "eval/track_a/audits/stage-2-evidence/lpc5-eval-v2-acceptance-rev4.txt"
LPC5_CATEGORY_B: Final = (
    "documented behavioural consequences outside revision 4's declared values: takeover and "
    "high-value merchant effects, velocity and card-testing burst effects, device-history "
    "availability before the feature horizon, findings inside allowlisted strata, and judged "
    "compositions"
)
LPC5_CATEGORY_C: Final = (
    "support short of thirty legitimate rows for two rare burst values, the MC-1 and MC-2 minimum "
    "effect near misses, and chance cells (high-value amount digits, calendar and merchant-code "
    "cells concentrated in a few instances)"
)
DATASET_VERSION: Final = "eval-v2"
COVERAGE_FLOOR_DISCLOSURE: Final = (
    "G2 added instances to patterns below 20: this dataset's scenario mix is a coverage floor, "
    "not natural prevalence (LPC-5 §14.4)."
)
DISCLOSURES: Final = (
    "eval-v2 is not LPC-5 compliant: strict acceptance under revision 4 is FAIL, with zero "
    "Category A (correctness, leakage or representation) findings.",
    COVERAGE_FLOOR_DISCLOSURE,
    "No natural fraud prevalence may be claimed from eval-v2.",
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


def finalize(run_record: Path, dataset_dir: Path) -> dict[str, Any]:
    """The frozen eval-v2 manifest: the candidate's contract, the materialised files and their
    provenance, and LPC-5's recorded outcome with its disclosures (Stage 2 step 12).

    Refused unless the materialisation reproduced the candidate on a clean tree."""
    candidate = json.loads(CANDIDATE.read_text(encoding="utf-8"))
    record = json.loads(run_record.read_text(encoding="utf-8"))
    problems = [
        f"{key}: candidate {candidate.get(key)!r}, run record {record.get(key)!r}"
        for key in (
            "dataset_version",
            "fraud_scenario_config_digest",
            "dataset_digest",
            "row_count",
        )
        if candidate.get(key) != record.get(key)
    ]
    if record.get("dirty_worktree") is not False:
        problems.append("the materialisation ran on a dirty worktree, so it could not be cited")
    files = {
        path.name: {
            "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
        for path in sorted(dataset_dir.glob("*.parquet"))
    }
    expected = {f"{topic}.parquet" for topic in candidate["streams"]}
    if set(files) != expected:
        problems.append(f"files: expected {sorted(expected)}, found {sorted(files)}")
    if problems:
        raise ValueError("; ".join(problems))
    return {
        **candidate,
        "$comment": (
            "The frozen eval-v2 dataset's reproduction contract (Stage 2 step 12). The dataset is "
            "gitignored; this manifest, the candidate manifest and the materialisation run record "
            "are what is committed. eval-v2 is NOT LPC-5 compliant: see lpc5 and disclosures."
        ),
        "candidate_manifest": "eval/track_a/eval-v2.candidate.manifest.json",
        "candidate_run_id": candidate["run_id"],
        "run_id": record["run_id"],
        "materialised_at": record["finished_at"],
        "materialisation": {
            "git_commit_sha": record["git_commit_sha"],
            "dirty_worktree": record["dirty_worktree"],
            "env_lock_digest": record["env_lock_digest"],
            "python_version": record["python_version"],
        },
        "files": files,
        "lpc5": {
            "criterion": candidate["criterion"],
            "strict_acceptance": "FAIL",
            "category_a_findings": 0,
            "compliant": False,
            "evidence": ACCEPTANCE_EVIDENCE,
            "category_b": LPC5_CATEGORY_B,
            "category_c": LPC5_CATEGORY_C,
            "acceptance_scale_controls": (
                "deferred evaluation hardening (user decision, 2026-09-14); not required for "
                "Phase 3"
            ),
        },
        "disclosures": list(DISCLOSURES),
        "regenerate": (
            'make seed ARGS="--manifest eval/track_a/eval-v2.candidate.manifest.json '
            '--sink parquet --out data/generated"'
        ),
    }


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
    parser.add_argument(
        "--finalize",
        action="store_true",
        help="write the frozen eval-v2 manifest from a materialisation run record",
    )
    parser.add_argument("--run-record", type=Path, help="the materialisation's run record")
    parser.add_argument("--dataset-dir", type=Path, help="the materialised parquet directory")
    args = parser.parse_args(argv)
    out = sys.stdout
    if args.finalize:
        if args.run_record is None or args.dataset_dir is None:
            parser.error("--finalize needs --run-record and --dataset-dir")
        if FINAL.exists():
            out.write(f"refused: a frozen manifest is never re-written in place: {FINAL}\n")
            return 2
        try:
            final = finalize(args.run_record, args.dataset_dir)
        except ValueError as exc:
            out.write(f"refused: {exc}\n")
            return 2
        FINAL.write_text(json.dumps(final, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        out.write(f"written: {FINAL}\n")
        return 0
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
