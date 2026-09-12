"""The committed Pydantic event models must match the schemas they came from.

ADR-0026 fixes the direction of truth: JSON Schema -> Pydantic, never the
reverse. That rule only holds if regeneration is verified, otherwise a
hand-edited model quietly becomes a second, divergent contract that only Python
consumers see.

The check runs the same `scripts/generate_event_models.py --check` that CI runs.
One definition, two callers -- in Phase 0 a control implemented twice went green
locally and red in CI, and the fix was to collapse it to one.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.contract

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "generate_event_models.py"
GENERATED = ROOT / "packages" / "trace_core" / "contracts" / "events"


def test_generated_models_are_in_sync_with_the_schemas() -> None:
    result = subprocess.run(  # noqa: S603
        [sys.executable, str(SCRIPT), "--check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert result.returncode == 0, (
        f"generated event models are stale. Run `make codegen` and commit.\n"
        f"{result.stdout}\n{result.stderr}"
    )


def test_generated_models_say_they_are_generated() -> None:
    """A future session must not spend time editing a file that is overwritten."""
    modules = sorted(GENERATED.glob("*.py"))
    assert modules, "no generated models found"
    for path in modules:
        assert "DO NOT EDIT BY HAND" in path.read_text(), f"{path.name} carries no warning"


def test_generated_models_use_the_strict_base_class() -> None:
    """extra='forbid' and frozen come from the base; a plain BaseModel would
    silently accept unknown fields."""
    for path in GENERATED.glob("*.py"):
        if path.stem == "__init__":
            continue
        text = path.read_text()
        assert "from trace_core.contracts.base import StrictEventModel" in text, path.name
        assert "(BaseModel)" not in text, f"{path.name} declares a model outside the strict base"
