"""The local toolchain and CI must install the same thing.

Written after a real CI failure: mypy's scope was extended to cover
`migrations/` (which imports alembic and sqlalchemy), but neither `make setup`
nor `.github/workflows/lint.yml` was updated to install the `db` extra. Locally
everything was already installed, so `make verify` passed while CI failed --
and a fresh clone would have failed too, violating the Phase 0 exit criteria.

The invariant: every extra that mypy's declared `files` scope needs must be
installed by BOTH `make setup` and every workflow that runs mypy.

Extended after a second failure of the same family. `make codegen-openapi` ran
`.venv/bin/python`, which every developer machine has and no GitHub runner does
-- setup-python installs into the interpreter on PATH -- so the contracts job
died with "No such file or directory" one step after installing everything it
needed. The Makefile now resolves ONE interpreter (the venv when `make setup`
created it, PATH otherwise) and the second half of this module holds every
target CI calls to that resolution, by dry-running them with the venv pointed
at a directory that does not exist.
"""

from __future__ import annotations

import re
import shutil
import subprocess
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
    # Phase 2 put `services/` (the thin ASGI entrypoints, CLAUDE.md S4) into
    # mypy's scope, and `trace_core.repositories` imports the Redis and psycopg
    # drivers. Added with the scope change rather than after it, for the reason
    # this module exists.
    "fastapi": "api",
    "redis": "db",
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
    assert "services" in files, (
        "the service entrypoints live under services/ (CLAUDE.md S4). They are thin, "
        "but they are where request handling and dependency wiring live -- exactly "
        "the code an untyped gap would hide a defect in"
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


# ------------------------------------------ the interpreter make runs (CI) ----

NOWHERE = "/nonexistent-venv"
"""A `VENV` override that exists on no machine, so make has to fall back."""

CONTRACT_TARGETS = {"codegen-openapi", "contracts-self-test", "contracts-check"}
"""What contracts.yml runs through make. Named so the discovery below cannot
pass on an empty set if a workflow is rewritten to call the scripts directly."""


def _make(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 -- fixed argv, no shell
        ["make", "-s", *args], cwd=ROOT, capture_output=True, text=True, check=False
    )


@pytest.fixture(scope="module")
def make_on_path() -> str:
    binary = shutil.which("make")
    if binary is None:
        pytest.skip(
            "SKIPPED (NOT PASSED): `make` is not on PATH, so the developer command "
            "interface cannot be exercised here"
        )
    return binary


def _targets_ci_calls() -> set[str]:
    """Every `make <target>` a workflow RUNS.

    Comments are dropped, including the inline kind (a `pip install` line that
    ends `# must match make setup`), because a target mentioned is not a target
    called.
    `setup` itself is excluded even if a workflow did call it: it is the target
    that creates the venv, so naming the venv is its job, not a defect.
    """
    targets: set[str] = set()
    for wf in sorted(WORKFLOWS.glob("*.yml")):
        for line in wf.read_text().splitlines():
            code = line.split("#", 1)[0]
            targets.update(re.findall(r"\bmake ([a-z][a-z-]*)", code))
    return targets - {"setup"}


def test_ci_calls_make_targets_at_all() -> None:
    """Without this the dry-run loop below would pass on an empty set."""
    assert _targets_ci_calls() >= CONTRACT_TARGETS


def test_make_resolves_an_interpreter_that_exists_without_a_venv(make_on_path: str) -> None:
    """The CI half of the contract: no venv means the interpreter on PATH."""
    resolved = _make("toolchain", f"VENV={NOWHERE}").stdout.strip()
    assert resolved and NOWHERE not in resolved, f"make resolved {resolved!r} with no venv"
    assert shutil.which(resolved), f"make resolved {resolved!r}, which is not on PATH"


def test_make_prefers_the_venv_when_setup_created_it(make_on_path: str) -> None:
    """The developer half: `make lint` must not quietly run on a system interpreter
    that lacks the dev extras, which would pass by importing nothing."""
    if not (ROOT / ".venv" / "bin" / "python").exists():
        pytest.skip(
            "SKIPPED (NOT PASSED): no .venv here (run `make setup`), so the venv-preference "
            "half of the rule cannot be observed on this machine"
        )
    assert _make("toolchain").stdout.strip() == ".venv/bin/python"


def test_every_make_target_ci_calls_runs_without_a_venv(make_on_path: str) -> None:
    """The exact CI failure, reproduced as a dry run.

    With the venv pointed at a directory that does not exist, the command line
    make would execute for each target CI calls must not name that directory.
    Before the fix it printed `/nonexistent-venv/bin/python scripts/...`, which
    is precisely the line the contracts job died on.
    """
    for target in sorted(_targets_ci_calls()):
        result = _make("-n", target, f"VENV={NOWHERE}", "BASE=/dev/null")
        assert result.returncode == 0, f"make -n {target}: {result.stderr.strip()}"
        assert NOWHERE not in result.stdout, (
            f"`make {target}` is called from CI and would run "
            f"{result.stdout.strip()!r} on a machine with no venv. Use $(VPY), which "
            f"falls back to the interpreter on PATH."
        )


def test_no_recipe_names_the_venv_directly() -> None:
    """The static half of the same rule, for targets CI does not call yet.

    Only `setup` may name `$(VENV)`: it is the target that creates it. Every
    other recipe goes through `$(VPY)`, or it is one CI workflow edit away from
    the failure this module was extended for.
    """
    offenders: list[str] = []
    target = None
    for line in MAKEFILE.read_text().splitlines():
        head = re.match(r"^([a-zA-Z_-]+):", line)
        if head:
            target = head.group(1)
        if not line.startswith("\t") or target == "setup":
            continue
        if re.search(r"\$\(VENV\)|\$\(VPIP\)|\.venv/", line):
            offenders.append(f"{target}: {line.strip()}")
    assert not offenders, "recipes that assume a venv exists:\n  " + "\n  ".join(offenders)
