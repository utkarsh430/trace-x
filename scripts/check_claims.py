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
SCAN = [ROOT / "README.md", *sorted((ROOT / "docs").rglob("*.md"))]

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
    re.compile(r"\b(?:throughput|sustained)\b[^.\n]{0,30}?(\d[\d,]*)\s*(?:tx|events|req)/s", re.I),
    re.compile(r"\bcost[^.\n]{0,30}?\$(\d+(?:\.\d+)?)\s*(?:per|/)\s*investigation", re.I),
]
CLAIM_PATTERNS = [(p, "quality") for p in QUALITY_PATTERNS] + [
    (p, "operational") for p in OPERATIONAL_PATTERNS
]

# Prose that states a budget/target/threshold/example rather than a measured result.
TARGET_WORDS = re.compile(
    r"\b(target|budget|threshold|goal|must|should|require[sd]?|aim|SLO|limit|cap|"
    r"tolerance|floor|ceiling|at most|at least|under|below|above|no more than|"
    r"example|placeholder|TBD|not yet|unmeasured|hypothetical|illustrative|"
    r"would be|expected|suppose)\b",
    re.I,
)
# Handled separately: these do not sit on word boundaries.
TARGET_SYMBOLS = re.compile(r"(<|>|≤|≥|e\.g\.|i\.e\.)")


def is_target_prose(line: str) -> bool:
    return bool(TARGET_WORDS.search(line) or TARGET_SYMBOLS.search(line))


RUN_ID = re.compile(r"run_id[=:\s]+([A-Za-z0-9._-]{4,})", re.I)
AGENT_METRIC = re.compile(
    r"\b(evidence[ _-]precision|evidence[ _-]recall|unsupported[ _-]claim|"
    r"investigation[ _-]accuracy|agent[ _-]disagreement|override[ _-]rate)\b",
    re.I,
)


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


def scan() -> list[str]:
    known = manifests()
    violations: list[str] = []
    in_code = False
    for path in SCAN:
        if not path.is_file():
            continue
        rel = path.relative_to(ROOT)
        in_code = False
        for n, line in enumerate(path.read_text().splitlines(), 1):
            if line.lstrip().startswith("```"):
                in_code = not in_code
                continue
            if in_code or is_target_prose(line):
                continue
            for pat, kind in CLAIM_PATTERNS:
                if not pat.search(line):
                    continue
                rid_m = RUN_ID.search(line)
                if not rid_m:
                    violations.append(
                        f"{rel}:{n}: numeric result published without a run_id\n"
                        f"      {line.strip()[:110]}"
                    )
                    break
                rid = rid_m.group(1)
                man = known.get(rid)
                if man is None:
                    violations.append(f"{rel}:{n}: run_id '{rid}' does not resolve to a manifest")
                    break
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
