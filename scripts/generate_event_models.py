#!/usr/bin/env python3
"""Generate Pydantic models FROM the committed event JSON Schemas.

The direction matters, and it is the opposite of the API contracts (ADR-0026): a
JSON Schema is a cross-language contract that Python producers, Spark (JVM)
consumers and a possible future Go gateway all read. Deriving it from Python
types would privilege one runtime and make every other consumer second class.

Generation goes to a temporary directory first and is copied in only on success,
so a failed run cannot leave half-written models behind.

Run via `make codegen`. CI runs the same command and fails if the working tree
changes afterwards, so a hand-edited model is reverted rather than merged.
"""

from __future__ import annotations

import argparse
import filecmp
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCHEMA_DIR = ROOT / "docs" / "contracts" / "events"
OUTPUT_DIR = ROOT / "packages" / "trace_core" / "contracts" / "events"
BASE_CLASS = "trace_core.contracts.base.StrictEventModel"

HEADER = '''"""GENERATED FROM docs/contracts/events/ -- DO NOT EDIT BY HAND.

JSON Schema is the source of truth for events (ADR-0026, ADR-0028). Change the
schema, then run `make codegen`. A CI gate runs the same command and fails on
any diff, so an edit made here is reverted rather than merged.
"""
'''


def _tool(name: str) -> str:
    found = shutil.which(name) or str(Path(sys.executable).parent / name)
    if not Path(found).exists():
        raise SystemExit(
            f"{name} is not installed. It ships in the `dev` extra; run `make setup` "
            f"so the version matches requirements.lock."
        )
    return found


def _render(destination: Path) -> int:
    """Generate into `destination`. Returns a process exit code."""
    codegen = _tool("datamodel-codegen")
    ruff = _tool("ruff")

    schemas = sorted(SCHEMA_DIR.glob("*.json"))
    if not schemas:
        print(f"no schemas found in {SCHEMA_DIR}", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory() as tmp:
        staging = Path(tmp) / "events"
        result = subprocess.run(  # noqa: S603
            [
                codegen,
                # Directory input: cross-file $ref (every event references
                # envelope.v1.json) only resolves in modular mode.
                "--input",
                str(SCHEMA_DIR),
                "--input-file-type",
                "jsonschema",
                "--output",
                str(staging),
                "--output-model-type",
                "pydantic_v2.BaseModel",
                "--base-class",
                BASE_CLASS,
                "--target-python-version",
                "3.12",
                "--use-standard-collections",
                "--use-union-operator",
                "--use-schema-description",
                "--use-field-description",
                # Annotated constraints rather than the deprecated conint/constr.
                "--field-constraints",
                # Without this the header carries a generation timestamp and the
                # drift gate would fail on every run for no real reason.
                "--disable-timestamp",
                "--formatters",
                "ruff-format",
            ],
            capture_output=True,
            text=True,
            cwd=ROOT,
        )
        if result.returncode != 0:
            print(f"codegen failed:\n{result.stderr}", file=sys.stderr)
            return result.returncode

        # The formatter leaves its cache inside the output tree.
        shutil.rmtree(staging / ".ruff_cache", ignore_errors=True)

        modules = sorted(p.stem for p in staging.glob("*.py") if p.stem != "__init__")
        for path in staging.glob("*.py"):
            path.write_text(HEADER + "\n" + path.read_text())

        (staging / "__init__.py").write_text(
            HEADER
            + "\n"
            + "".join(f"from trace_core.contracts.events import {m}\n" for m in modules)
            + "\n__all__ = [\n"
            + "".join(f'    "{m}",\n' for m in modules)
            + "]\n"
        )

        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(staging, destination)

    # Output is captured, not inherited: when rendering into a temp directory for
    # the drift check, ruff's per-file-ignores (scoped to the real package path)
    # do not apply, so it reports findings that are suppressed in the committed
    # location. Those diagnostics are noise, and only the auto-fixes matter here.
    for args in (["format", "-q"], ["check", "--fix", "-q"]):
        subprocess.run(  # noqa: S603
            [ruff, *args, str(destination)], cwd=ROOT, check=False, capture_output=True
        )
    return 0


def generate() -> int:
    rc = _render(OUTPUT_DIR)
    if rc == 0:
        modules = sorted(p.stem for p in OUTPUT_DIR.glob("*.py") if p.stem != "__init__")
        print(f"generated {len(modules)} event model module(s) from {SCHEMA_DIR.name}/:")
        for module in modules:
            print(f"  {(OUTPUT_DIR / f'{module}.py').relative_to(ROOT)}")
    return rc


def check() -> int:
    """Regenerate into a temp directory and diff against the committed models.

    Hermetic on purpose: comparing against a fresh render rather than against
    `git status` means the check gives the same answer on a dirty working tree,
    in CI, and in a test. One definition, three callers -- the Phase 0 secret
    scan went red in CI and green locally precisely because it had two.
    """
    with tempfile.TemporaryDirectory() as tmp:
        fresh = Path(tmp) / "events"
        rc = _render(fresh)
        if rc != 0:
            return rc
        committed = sorted(p.name for p in OUTPUT_DIR.glob("*.py"))
        regenerated = sorted(p.name for p in fresh.glob("*.py"))
        if committed != regenerated:
            print(
                "generated model files differ from the schemas:\n"
                f"  committed:   {committed}\n"
                f"  regenerated: {regenerated}",
                file=sys.stderr,
            )
            return 1
        _same, mismatch, errors = filecmp.cmpfiles(OUTPUT_DIR, fresh, committed, shallow=False)
        if mismatch or errors:
            print(
                "generated event models are out of date with docs/contracts/events/.\n"
                f"  differing: {sorted(mismatch + errors)}\n"
                "  Run `make codegen` and commit the result. Do not hand-edit generated models:\n"
                "  JSON Schema is the source of truth for events (ADR-0026, ADR-0028).",
                file=sys.stderr,
            )
            return 1
    print(f"PASS - generated event models match docs/contracts/events/ ({len(committed)} files)")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail if the committed models differ from a fresh render (CI gate)",
    )
    raise SystemExit(check() if parser.parse_args().check else generate())
