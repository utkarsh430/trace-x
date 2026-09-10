#!/usr/bin/env python3
"""Explicit failure for commands whose phase has not landed yet.

The project's Definition of Done forbids commands that pretend to succeed.
A `make` target belonging to an unimplemented phase exits non-zero and says
exactly which phase owns it and where to read its gate.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATUS = ROOT / "tests" / "acceptance" / "status.json"


def current_phase() -> str:
    try:
        return str(json.loads(STATUS.read_text())["current_phase"])
    except (OSError, KeyError, ValueError):
        return "unknown"


def main() -> int:
    if len(sys.argv) < 4:
        print("usage: phase_guard.py <command> <phase> <capability>", file=sys.stderr)
        return 2
    command, phase, capability = sys.argv[1], sys.argv[2], sys.argv[3]
    print(
        f"\n\033[33mPHASE NOT IMPLEMENTED\033[0m\n"
        f"  command     : make {command}\n"
        f"  requires    : Phase {phase} — {capability}\n"
        f"  repo is at  : Phase {current_phase()}\n\n"
        f"  This command exits non-zero by design rather than pretending to succeed.\n"
        f"  Gate for Phase {phase}: docs/ROADMAP.md\n"
        f"  Current state         : docs/PROGRESS.md\n",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
