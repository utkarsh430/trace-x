"""The local toolchain and CI must install the same thing.

Written after a real CI failure: mypy's scope was extended to cover
`migrations/` (which imports alembic and sqlalchemy), but neither `make setup`
nor `.github/workflows/lint.yml` was updated to install the `db` extra. Locally
everything was already installed, so `make verify` passed while CI failed --
and a fresh clone would have failed too, violating the Phase 0 exit criteria.

The invariant: every extra that mypy's declared `files` scope needs must be
installed by BOTH `make setup` and every workflow that runs mypy.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = ROOT / "pyproject.toml"
MAKEFILE = ROOT / "Makefile"
WORKFLOWS = ROOT / ".github" / "workflows"

# Modules imported by mypy's scope, and the extra that provides each.
SCOPE_REQUIREMENTS = {
    "alembic": "db",
    "sqlalchemy": "db",
    "opentelemetry.exporter.otlp": "obs",
    # Phase 1 put `data/` (the generator and the source adapters) into mypy's
    # scope. Both were added with the scope change, not after it, because the
    # drift is only invisible on a machine that already has them installed.
    "pyarrow": "gen",
    "jsonschema": "gen",
}

EXTRAS_RE = re.compile(r'install[^\n]*-e\s+"\.\[([a-z,\s]+)\]"')


def _extras(text: str) -> list[set[str]]:
    return [{e.strip() for e in m.split(",")} for m in EXTRAS_RE.findall(text)]


@pytest.fixture(scope="module")
def pyproject() -> dict:
    return tomllib.loads(PYPROJECT.read_text())


def test_mypy_scope_is_declared(pyproject: dict) -> None:
    files = pyproject["tool"]["mypy"]["files"]
    assert "packages" in files
    assert "migrations" in files, "migrations carry the security-critical grants; keep them typed"
    assert "data" in files, (
        "the generator and the source adapters live under data/ (CLAUDE.md S4); "
        "outside mypy's scope they would be the largest untyped surface in the repo"
    )


# ------------------------------------------------- phase-gate probe (D-2) ----


def _phase_gated_targets() -> set[str]:
    """Make targets that still route through scripts/phase_guard.py."""
    text = MAKEFILE.read_text()
    return set(re.findall(r"^([a-z-]+):[^\n]*\n\t@\$\(PY\) scripts/phase_guard\.py", text, re.M))


def test_ci_phase_gate_probes_a_command_that_is_still_gated() -> None:
    """A gate that fires when a phase SUCCEEDS is worse than no gate.

    `test-fast.yml` asserts that a phase-gated command exits non-zero. It named
    `make seed` -- which Phase 1 implements, so the job would have gone red the
    moment the generator started working. The probe must always name a command
    whose phase has not landed.
    """
    wf = WORKFLOWS / "test-fast.yml"
    probes = set(re.findall(r"^\s+make ([a-z-]+) >/dev/null", wf.read_text(), re.M))
    assert probes, "test-fast.yml no longer probes a phase-gated command"
    gated = _phase_gated_targets()
    assert gated, "no phase-gated targets found in the Makefile"
    for probe in probes:
        assert probe in gated, (
            f"test-fast.yml probes `make {probe}`, which is no longer phase-gated. "
            f"Repoint it at a command whose phase has not landed: {sorted(gated)}"
        )


def test_required_extras_exist(pyproject: dict) -> None:
    declared = set(pyproject["project"]["optional-dependencies"])
    for module, extra in SCOPE_REQUIREMENTS.items():
        assert extra in declared, f"extra '{extra}' (needed for {module}) is not declared"


def test_make_setup_installs_every_extra_mypy_needs() -> None:
    """A fresh clone must be able to run `make verify` without extra steps."""
    installs = _extras(MAKEFILE.read_text())
    assert installs, "no editable install found in the Makefile"
    setup_extras = installs[0]
    for module, extra in SCOPE_REQUIREMENTS.items():
        assert extra in setup_extras, (
            f"`make setup` does not install '{extra}', which mypy needs for {module}. "
            f"A fresh clone would fail `make verify`."
        )


def _workflows_running(tool: str) -> list[Path]:
    return [
        p
        for p in sorted(WORKFLOWS.glob("*.yml"))
        if re.search(rf"^\s+run:.*\b{tool}\b", p.read_text(), re.M)
    ]


def test_every_workflow_running_mypy_installs_its_extras() -> None:
    running = _workflows_running("mypy")
    assert running, "no workflow runs mypy"
    for wf in running:
        installed: set[str] = set()
        for group in _extras(wf.read_text()):
            installed |= group
        for module, extra in SCOPE_REQUIREMENTS.items():
            assert extra in installed, (
                f"{wf.name} runs mypy but does not install '{extra}' (needed for {module}). "
                f"This is the exact drift that failed CI while `make verify` passed locally."
            )


def test_workflows_and_make_setup_agree_on_extras() -> None:
    """Divergence means CI and local developers test different things."""
    setup_extras = _extras(MAKEFILE.read_text())[0]
    for wf in sorted(WORKFLOWS.glob("*.yml")):
        groups = _extras(wf.read_text())
        for installed in groups:
            assert installed == setup_extras, (
                f"{wf.name} installs {sorted(installed)} but `make setup` installs "
                f"{sorted(setup_extras)}. Keep them identical."
            )


def test_opentelemetry_core_is_not_in_an_extra(pyproject: dict) -> None:
    """trace_core.observability imports it at module level, so it cannot be optional."""
    core = " ".join(pyproject["project"]["dependencies"]).lower()
    assert "opentelemetry-api" in core
    assert "opentelemetry-sdk" in core
    obs = " ".join(pyproject["project"]["optional-dependencies"]["obs"]).lower()
    assert "opentelemetry-sdk" not in obs, "sdk is core; duplicating it in an extra invites drift"
