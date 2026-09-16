#!/usr/bin/env python3
"""Benchmark-integrity linter.

RULE: no numeric performance/quality claim may appear in README.md or docs/**
unless it cites a run_id that resolves to a complete run manifest.

Enforces the five publication rules in docs/EVALUATION.md:
  1. every published number cites a resolvable run_id
  2. quality numbers may only come from llm_tier EVAL
  3. a Track-B (external) run_id may never be cited beside an agent-quality metric
  4. a run recorded with dirty_worktree=true is not publishable
  5. no hardcoded expectations stand in for measured results

Until Phase 9 lands there are no manifests, so the correct behaviour is:
assert that NO number is being published yet. That is a real check, not a stub —
it fails the moment someone writes an unbacked benchmark figure.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANIFEST_DIR = ROOT / "eval" / "manifest"
SCAN = [
    ROOT / "README.md",
    *sorted((ROOT / "docs").rglob("*.md")),
    # Benchmark reports are where measurements are actually published, so leaving
    # them unscanned left the largest surface uncovered by the gate that exists
    # to cover exactly it.
    *sorted((ROOT / "benchmarks").rglob("*.md")),
]

# A claim is either a QUALITY assertion (accuracy/evidence quality -- gated to the
# EVAL tier) or an OPERATIONAL one (latency/throughput/cost -- publishable from any
# tier). Conflating them would make tier gating obstructive, and an obstructive
# gate gets bypassed.
QUALITY_PATTERNS = [
    re.compile(
        r"\b(PR-?AUC|ROC-?AUC|F1|precision|recall|Brier|FPR|accuracy)\b[^.\n]{0,40}?"
        r"(?<![\w-])(0\.\d+|\d{1,3}(?:\.\d+)?\s*%)",
        re.I,
    ),
]
OPERATIONAL_PATTERNS = [
    re.compile(r"\bp(?:50|95|99)\b[^.\n]{0,30}?(\d+(?:\.\d+)?)\s*(ms|s)\b", re.I),
    # The same claim written the other way round -- "measured 0.765 ms p99". The
    # first pattern requires the percentile to precede the number, so this
    # ordering went unchecked, and it is how a results table actually reads.
    #
    # A measurement verb is required here, and the asymmetry is deliberate rather
    # than lazy: "p99 was 42 ms" is nearly always a reported result, while
    # "a 100 ms p99" is nearly always a reference to the BUDGET -- which
    # ADR-0001 and ADR-0002 both make, correctly, and neither may be edited to
    # suit a linter (ADRs are immutable once accepted). Without the verb this
    # pattern flags every mention of the budget, and a gate that cries wolf on
    # correct prose is one people learn to override.
    re.compile(
        r"\b(?:measured|observed|recorded|reached|sustained|achieved|came in at)\b"
        r"[^.\n]{0,30}?(\d+(?:\.\d+)?)\s*(ms|s)\b[^.\n]{0,15}?\bp(?:50|95|99)\b",
        re.I,
    ),
    re.compile(r"\b(?:throughput|sustained)\b[^.\n]{0,30}?(\d[\d,]*)\s*(?:tx|events|req)/s", re.I),
    re.compile(r"\bcost[^.\n]{0,30}?\$(\d+(?:\.\d+)?)\s*(?:per|/)\s*investigation", re.I),
    # Any rate expressed per second, however it is worded. The two patterns
    # above required the words "throughput" or "sustained" nearby, so a
    # generation rate written as "54,000 tx/s" or "54.6k tx/s" passed
    # unnoticed -- and a number nobody checks is worse than one that is
    # blocked, because it reads as though it had been checked.
    re.compile(r"(?<![\w.])(\d[\d,]*(?:\.\d+)?)\s*k?\s*(?:tx|rows|events|msg)/s\b", re.I),
    # Dataset and artefact sizes.
    re.compile(
        r"\b(?:dataset|parquet|output|file|payload)\b[^.\n]{0,30}?"
        r"(\d+(?:\.\d+)?)\s*(?:[KMGT]i?B)\b",
        re.I,
    ),
]
CLAIM_PATTERNS = [(p, "quality") for p in QUALITY_PATTERNS] + [
    (p, "operational") for p in OPERATIONAL_PATTERNS
]

# Prose that states a budget/target/threshold/example rather than a measured result.
TARGET_WORDS = re.compile(
    r"\b(target|budget|threshold|goal|must|should|require[sd]?|aim|SLO|limit|cap|"
    r"tolerance|floor|ceiling|at most|at least|no more than|"
    r"example|placeholder|TBD|not yet|unmeasured|hypothetical|illustrative|"
    r"would be|expected|suppose)\b",
    re.I,
)
# "under", "below" and "above" are comparative ONLY when a quantity follows.
# Listed among the bare words above, "under" matched "under load" -- so
# "the gateway p99 was 42 ms under load" was silently exempted as a budget
# statement. That is a reported measurement, and it was the one phrasing most
# likely to appear in a real report.
TARGET_COMPARATORS = re.compile(r"\b(under|below|above|over)\s+[~<>]?\s*\d", re.I)
# Handled separately: these do not sit on word boundaries.
TARGET_SYMBOLS = re.compile(r"(<|>|≤|≥|e\.g\.|i\.e\.)")


def is_target_prose(line: str) -> bool:
    return bool(
        TARGET_WORDS.search(line) or TARGET_COMPARATORS.search(line) or TARGET_SYMBOLS.search(line)
    )


# The separator class includes backticks and quotes because a markdown report
# writes `run_id`: `gen-...`, and a gate that only understood `run_id: gen-...`
# would report a correctly-cited number as unbacked -- a false alarm that teaches
# people to ignore the gate.
RUN_ID = re.compile(r"run_id[`'\"]*[=:\s]+[`'\"]*([A-Za-z0-9._-]{4,})", re.I)
AGENT_METRIC = re.compile(
    r"\b(evidence[ _-]precision|evidence[ _-]recall|unsupported[ _-]claim|"
    r"investigation[ _-]accuracy|agent[ _-]disagreement|override[ _-]rate)\b",
    re.I,
)


# Required fields per record type. Same discipline as ADR-0017's full manifest,
# smaller surface: a number whose provenance is incomplete is indistinguishable
# from one that was invented.
_SHARED_REQUIRED = (
    "run_id",
    "record_type",
    "track",
    "git_commit_sha",
    "dirty_worktree",
    "env_lock_digest",
    "python_version",
    "started_at",
    "finished_at",
    "measured",
)

GENERATOR_REQUIRED = (
    *_SHARED_REQUIRED,
    "generator_version",
    "seed",
    "fraud_scenario_config_digest",
    "dataset_version",
    "dataset_digest",
    "row_count",
    "validation_policy",
)

# A load run measures a SERVICE, so its provenance is the service's resolved
# configuration, not a dataset's. Without the rule pack and threshold digests a
# recorded p99 cannot be attributed to the behaviour that produced it, and
# without the tool version and target load it cannot be compared to the next run.
LOADTEST_REQUIRED = (
    *_SHARED_REQUIRED,
    "service",
    "service_version",
    "tool",
    "tool_version",
    "target_tps",
    "duration_s",
    "rule_pack_digest",
    "threshold_config_digest",
    "feature_set_version",
    "degraded_mode",
)

# The full 25-field RunManifest is a Phase 9 deliverable (ADR-0017). Declared
# here with only the fields THIS LINTER depends on, so an evaluation record is a
# recognised type rather than an unknown one -- Phase 9 extends the tuple when it
# defines the manifest. Deliberately not guessed at in full: a required-field
# list invented before the thing it describes exists would have to be rewritten,
# and would give a false impression of having been reviewed.
EVAL_REQUIRED = (
    "run_id",
    "record_type",
    "track",
    "git_commit_sha",
    "dirty_worktree",
    "llm_tier",
)

# A component benchmark measures one subsystem in isolation -- an estimator's
# error, a representation's memory. It names its subject and its tool, because a
# number whose instrument is unknown cannot be reproduced or compared.
BENCHMARK_REQUIRED = (
    "run_id",
    "record_type",
    "track",
    "git_commit_sha",
    "dirty_worktree",
    "env_lock_digest",
    "python_version",
    "started_at",
    "finished_at",
    "subject",
    "tool",
    "tool_version",
    "measured",
)

# A parity run measures two IMPLEMENTATIONS against each other on one recorded stream, so its
# provenance is the corpus and the overlay that produced the stream, the semantics both sides were
# judged by, and the toolchain that ran them. `eval/parity/record.py` owns the full contract and
# refuses a record missing any of it; these are the fields a published number needs to be
# attributable (ADR-0056 §6).
PARITY_REQUIRED = (
    "run_id",
    "record_type",
    "track",
    "git_commit_sha",
    "dirty_worktree",
    "env_lock_digest",
    "python_version",
    "started_at",
    "finished_at",
    "mode",
    "publishable",
    "parity_semantics_version",
    "feature_set_version",
    "partition",
    "dataset",
    "overlay",
    "lateness_model",
    "lateness_model_digest",
    "toolchain",
    "gateway",
    "counts",
    "results",
    "guard",
    "verdict",
)

REQUIRED_BY_TYPE: dict[str, tuple[str, ...]] = {
    "GENERATOR": GENERATOR_REQUIRED,
    "LOADTEST": LOADTEST_REQUIRED,
    "BENCHMARK": BENCHMARK_REQUIRED,
    "EVAL": EVAL_REQUIRED,
    "PARITY": PARITY_REQUIRED,
}


def incomplete_fields(manifest: dict[str, object]) -> list[str]:
    """Required fields a manifest is missing, for its record type.

    An UNKNOWN record type is itself a violation. The earlier version returned
    an empty list for anything that was not GENERATOR, so a record could resolve
    a published number by declaring a type nobody had defined requirements for --
    a hole in exactly the gate that exists to stop unbacked numbers.
    """
    record_type = str(manifest.get("record_type", "")).upper()
    required = REQUIRED_BY_TYPE.get(record_type)
    if required is None:
        return [
            f"record_type {record_type or '<missing>'!r} has no declared required fields "
            f"(known: {sorted(REQUIRED_BY_TYPE)}); an unrecognised record cannot "
            f"substantiate a number"
        ]
    return [f for f in required if manifest.get(f) is None or manifest.get(f) == ""]


def manifests() -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    if MANIFEST_DIR.is_dir():
        for f in MANIFEST_DIR.glob("*.json"):
            try:
                m = json.loads(f.read_text())
                if rid := m.get("run_id"):
                    out[str(rid)] = m
            except (OSError, ValueError):
                pass
    return out


HEADING = re.compile(r"^\s{0,3}#{1,6}\s")


def scan() -> list[str]:
    known = manifests()
    violations: list[str] = []
    in_code = False
    for path in SCAN:
        if not path.is_file():
            continue
        rel = path.relative_to(ROOT)
        in_code = False
        # A run_id declared under a heading covers the numbers reported beneath
        # it, and is cleared by the next heading. Requiring every line to repeat
        # the id would make a report unreadable -- and a rule that forces bad
        # writing gets worked around, which is worse than a slightly wider one.
        # The scope is deliberately narrow: one section, never the whole file.
        section_rid: str | None = None
        for n, line in enumerate(path.read_text().splitlines(), 1):
            if HEADING.match(line):
                section_rid = None
            if line.lstrip().startswith("```"):
                in_code = not in_code
                continue
            if in_code:
                continue
            if (declared := RUN_ID.search(line)) is not None:
                section_rid = declared.group(1)
            if is_target_prose(line):
                continue
            for pat, kind in CLAIM_PATTERNS:
                if not pat.search(line):
                    continue
                rid_m = RUN_ID.search(line)
                rid = rid_m.group(1) if rid_m else section_rid
                if not rid:
                    violations.append(
                        f"{rel}:{n}: numeric result published without a run_id\n"
                        f"      {line.strip()[:110]}"
                    )
                    break
                man = known.get(rid)
                if man is None:
                    violations.append(f"{rel}:{n}: run_id '{rid}' does not resolve to a manifest")
                    break
                if missing := incomplete_fields(man):
                    violations.append(
                        f"{rel}:{n}: run_id '{rid}' is missing {missing}; an incomplete "
                        f"record cannot substantiate a number (ADR-0017)"
                    )
                if man.get("dirty_worktree"):
                    violations.append(
                        f"{rel}:{n}: run_id '{rid}' was recorded with a dirty worktree (rule 4)"
                    )
                # Rule 2 gates QUALITY claims only. Latency, throughput and cost
                # publish from any tier -- gating them would obstruct legitimate
                # reporting, and an obstructive rule gets worked around.
                if kind == "quality" and str(man.get("llm_tier", "")).upper() != "EVAL":
                    violations.append(
                        f"{rel}:{n}: run_id '{rid}' is tier {man.get('llm_tier')}; "
                        f"only EVAL may publish quality numbers (rule 2)"
                    )
                # A GENERATOR record describes a data-generation run: no model,
                # no inference, no LLM tier. It can substantiate throughput and
                # size, never accuracy. Without this, the tier gate below would
                # wave it through, because a generator run has no tier to fail.
                if kind == "quality" and str(man.get("record_type", "")).upper() in {
                    "GENERATOR",
                    "LOADTEST",
                    "BENCHMARK",
                }:
                    violations.append(
                        f"{rel}:{n}: run_id '{rid}' is a "
                        f"{str(man.get('record_type', '')).upper()} record and cannot "
                        f"substantiate a quality claim -- it involved no model"
                    )
                if str(man.get("track", "")).upper() == "EXTERNAL" and AGENT_METRIC.search(line):
                    violations.append(
                        f"{rel}:{n}: Track-B run_id '{rid}' cited beside an "
                        f"agent-quality metric (rule 3)"
                    )
                break
    return violations


def main() -> int:
    known = manifests()
    violations = scan()
    print(
        f"\ncheck-claims — scanned {sum(1 for p in SCAN if p.is_file())} documents, "
        f"{len(known)} run manifest(s) available"
    )
    if violations:
        print(f"\033[31m\n{len(violations)} benchmark-integrity violation(s):\033[0m")
        for v in violations:
            print(f"  - {v}")
        print("\n  Rules: docs/EVALUATION.md § Benchmark integrity\n")
        return 1
    if not known:
        print(
            "\033[32mPASS\033[0m — no manifests exist yet, and no unbacked numeric "
            "result is published. Correct state before Phase 9.\n"
        )
    else:
        print("\033[32mPASS\033[0m — every published number resolves to a valid manifest.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
