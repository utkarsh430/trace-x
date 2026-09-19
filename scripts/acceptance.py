#!/usr/bin/env python3
"""Acceptance-status tooling.

The status file is the machine-readable answer to "what actually works?".
Its central rule: a capability is PASS only with executable evidence.

  report            render the current status
  validate          structural + rule checks (used by make verify and CI)
  set <id> <status> [--result TEXT]   record an outcome
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATUS_PATH = ROOT / "tests" / "acceptance" / "status.json"
VALID = ("NOT_STARTED", "IN_PROGRESS", "PASS", "FAIL", "BLOCKED")
COLOUR = {
    "PASS": "\033[32m",
    "FAIL": "\033[31m",
    "IN_PROGRESS": "\033[36m",
    "BLOCKED": "\033[35m",
    "NOT_STARTED": "\033[2m",
}


def load() -> dict:
    return json.loads(STATUS_PATH.read_text())


def save(doc: dict) -> None:
    doc["updated_at"] = dt.date.today().isoformat()
    STATUS_PATH.write_text(json.dumps(doc, indent=2) + "\n")


def validate(doc: dict) -> list[str]:
    """Rule checks that keep the file honest."""
    errs: list[str] = []
    seen: set[str] = set()
    for c in doc["capabilities"]:
        cid = c.get("id", "<missing id>")
        if cid in seen:
            errs.append(f"{cid}: duplicate id")
        seen.add(cid)
        if c.get("status") not in VALID:
            errs.append(f"{cid}: invalid status {c.get('status')!r}")
        ev = c.get("evidence") or {}
        if not ev.get("command"):
            errs.append(f"{cid}: no acceptance command declared")
        # The core rule: PASS requires evidence; non-PASS must not claim any.
        if c.get("status") == "PASS":
            if not ev.get("verified_at"):
                errs.append(
                    f"{cid}: marked PASS without evidence.verified_at — code existing is not PASS"
                )
            if not ev.get("result"):
                errs.append(f"{cid}: marked PASS without a recorded result")
        elif ev.get("verified_at"):
            errs.append(f"{cid}: status {c['status']} but evidence.verified_at is set")
    return errs


def cmd_report(doc: dict) -> int:
    counts = Counter(c["status"] for c in doc["capabilities"])
    print(
        f"\nTRACE-X acceptance — Phase {doc['current_phase']} ({doc.get('current_phase_name', '')})"
    )
    print(f"last verified commit: {doc.get('last_verified_commit') or 'none yet'}")
    print("-" * 76)
    phases: dict[str, list[dict]] = {}
    for c in doc["capabilities"]:
        phases.setdefault(c["phase"], []).append(c)
    for phase in sorted(phases, key=lambda x: (len(x), x)):
        caps = phases[phase]
        done = sum(1 for c in caps if c["status"] == "PASS")
        required = [c for c in caps if c.get("gating", True)]
        required_done = sum(1 for c in required if c["status"] == "PASS")
        print(
            f"\n  Phase {phase}  [{done}/{len(caps)} PASS; "
            f"required for exit {required_done}/{len(required)}]"
        )
        for c in caps:
            col = COLOUR[c["status"]]
            marker = "" if c.get("gating", True) else "  (non-gating)"
            print(f"    {col}{c['status']:<12}\033[0m {c['id']:<24} {c['name'][:44]}{marker}")
    print("\n" + "-" * 76)
    print("  " + "  ".join(f"{COLOUR[s]}{s}={counts.get(s, 0)}\033[0m" for s in VALID))
    errs = validate(doc)
    if errs:
        print(f"\n\033[31m{len(errs)} integrity error(s):\033[0m")
        for e in errs:
            print(f"  - {e}")
        return 1
    print("\033[32m  status file is internally consistent\033[0m\n")
    return 0


def cmd_validate(doc: dict) -> int:
    errs = validate(doc)
    if errs:
        print(f"\033[31macceptance status: {len(errs)} error(s)\033[0m")
        for e in errs:
            print(f"  - {e}")
        return 1
    n = len(doc["capabilities"])
    print(f"\033[32mPASS\033[0m — acceptance status valid ({n} capabilities)")
    return 0


def cmd_set(doc: dict, cid: str, status: str, result: str | None) -> int:
    if status not in VALID:
        print(f"invalid status {status!r}; expected one of {VALID}", file=sys.stderr)
        return 2
    for c in doc["capabilities"]:
        if c["id"] != cid:
            continue
        if status == "PASS" and not result:
            print(
                "refusing to mark PASS without --result (executable evidence required)",
                file=sys.stderr,
            )
            return 2
        c["status"] = status
        c["evidence"]["verified_at"] = dt.date.today().isoformat() if status == "PASS" else None
        c["evidence"]["result"] = result if status == "PASS" else None
        save(doc)
        print(f"{cid} -> {status}")
        return 0
    print(f"unknown capability id: {cid}", file=sys.stderr)
    return 2


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("report")
    sub.add_parser("validate")
    s = sub.add_parser("set")
    s.add_argument("id")
    s.add_argument("status")
    s.add_argument("--result", default=None)
    a = ap.parse_args()
    doc = load()
    if a.cmd == "report":
        return cmd_report(doc)
    if a.cmd == "validate":
        return cmd_validate(doc)
    return cmd_set(doc, a.id, a.status, a.result)


if __name__ == "__main__":
    raise SystemExit(main())
