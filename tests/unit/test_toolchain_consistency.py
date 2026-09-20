"""The local toolchain and CI must install the same thing.

Written after a real CI failure: mypy's scope was extended to cover
`migrations/` (which imports alembic and sqlalchemy), but neither `make setup`
nor `.github/workflows/lint.yml` was updated to install the `db` extra. Locally
everything was already installed, so `make verify` passed while CI failed --
and a fresh clone would have failed too, violating the Phase 0 exit criteria.

The invariant: every extra that mypy's declared `files` scope needs must be
installed by BOTH `make setup` and every workflow that runs mypy.

Phase 3 changed HOW that holds. Installs no longer resolve pyproject ranges at all:
`make setup` and every CI job run one script, `scripts/install_locked.sh`, which
installs the hashed lockfiles. So the extras invariant is now asserted against the
lock (what is installed) and against `make lock` (what regenerates it), and a second
invariant is asserted directly: there is exactly one way to install, and it checks
hashes.

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
from typing import Any

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
    # Phase 3 put `trace_core.stream` into mypy's scope; it imports pyspark (lazily,
    # but mypy follows the import), and the generator's Kafka sink imports
    # confluent_kafka. Both are installed everywhere now, from the lock.
    "pyspark": "stream",
    "confluent_kafka": "stream",
    # Step 3's lake modules import delta-spark's Python API (`delta`), from the same lock.
    "delta": "stream",
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


LOCK = ROOT / "requirements.lock"
INSTALL_SCRIPT = "scripts/install_locked.sh"
FAST_SELECTOR = (
    "not integration and not e2e and not load and not chaos and not external "
    "and not cloud and not slow and not stream"
)


def _lock_extras() -> set[str]:
    header = "\n".join(LOCK.read_text().splitlines()[:8])
    return set(re.findall(r"--extra=([a-z]+)", header))


def _recipe(target: str) -> str:
    text = MAKEFILE.read_text()
    body = text[text.index(f"\n{target}:") :]
    return body[: body.index("\n\n")]


def test_the_lock_covers_every_extra_mypy_needs() -> None:
    """A fresh clone must be able to run `make verify` without extra steps."""
    extras = _lock_extras()
    assert extras, "requirements.lock records no --extra flags in its header"
    for module, extra in SCOPE_REQUIREMENTS.items():
        assert extra in extras, (
            f"requirements.lock does not include '{extra}', which mypy needs for {module}. "
            f"A fresh clone would fail `make verify`."
        )


def test_make_lock_regenerates_exactly_what_the_lock_records() -> None:
    """Otherwise the next `make lock` silently drops an extra the lock has today."""
    assert set(re.findall(r"--extra=([a-z]+)", _recipe("lock"))) == _lock_extras()


INSTALL_SURFACES: list[Path] = [
    MAKEFILE,
    *sorted(WORKFLOWS.glob("*.yml")),
    *sorted((ROOT / "deploy").glob("*Dockerfile*")),
    *sorted((ROOT / "scripts").glob("*.sh")),
]
"""Every file that runs an install. Python sources are excluded on purpose: they only
mention installs in prose (scripts/secret_scan.py's docstring), and scanning prose would
be a guard that cries wolf."""


def _is_project_install(command: str) -> bool:
    """`pip install --no-deps [flags] [-e] .` and nothing else on the command.

    Parsed by token, so `pip install --no-deps somepkg .` -- an unhashed package riding
    along with the project -- is not mistaken for the project's own install."""
    tokens = command.split()
    positionals = [token for token in tokens[2:] if not token.startswith("-")]
    return "--no-deps" in tokens and positionals == ["."]


def _pip_install_commands(text: str) -> list[str]:
    code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    code = re.sub(r"\\\n\s*", " ", code)
    return [match.group(0) for match in re.finditer(r"pip3? install[^\n;&|]*", code)]


def test_every_pip_install_is_hash_checked_or_the_project_itself() -> None:
    """Written after a review found two installs outside the contract that the previous
    test could not see: pip-audit installed unpinned in lint.yml, and the gateway image
    installing plain version constraints. Comments are ignored and line continuations
    joined, so a multi-line command is judged whole."""
    commands = [
        (path, command)
        for path in INSTALL_SURFACES
        for command in _pip_install_commands(path.read_text())
    ]
    assert len(commands) >= 5, f"too few install commands found to trust this scan: {commands}"
    offenders = [
        f"{path.relative_to(ROOT)}: {command.strip()}"
        for path, command in commands
        if "--require-hashes" not in command and not _is_project_install(command)
    ]
    assert not offenders, "installs outside the hashed contract:\n  " + "\n  ".join(offenders)


TOOLING = re.compile(r"\b(pytest|mypy|ruff|bandit|pip_audit|make)\b")


def test_every_ci_job_installs_from_the_lock_before_running_project_tooling() -> None:
    """Parsed as YAML, step by step, so a `run: |` block is read in full and an install
    that happens AFTER the tooling it should precede does not count."""
    import yaml

    checked: set[str] = set()
    for wf in sorted(WORKFLOWS.glob("*.yml")):
        jobs = yaml.safe_load(wf.read_text()).get("jobs") or {}
        for job_name, job in jobs.items():
            installed = False
            for step in job.get("steps", []):
                run = str(step.get("run") or "")
                if f"bash {INSTALL_SCRIPT} python" in run:
                    installed = True
                if TOOLING.search(run):
                    assert installed, (
                        f"{wf.name} job {job_name!r} runs project tooling before installing "
                        f"through {INSTALL_SCRIPT}: {run.strip()[:90]!r}"
                    )
                    checked.add(f"{wf.name}:{job_name}")
    expected = {
        "lint.yml:static-analysis",
        "test-fast.yml:fast",
        "contracts.yml:openapi",
        "test-integration.yml:integration",
        "test-stream.yml:stream",
    }
    assert expected <= checked, f"jobs not examined: {sorted(expected - checked)}"


def test_make_setup_installs_through_the_locked_script() -> None:
    assert f"bash {INSTALL_SCRIPT}" in _recipe("setup")


def test_the_gateway_lock_is_the_runtime_closure_of_the_main_lock() -> None:
    """requirements-gateway.lock is derived, never edited: same versions, same hashes."""
    runtime_lock = _load_script("runtime_lock")
    assert (ROOT / "requirements-gateway.lock").read_text() == runtime_lock.render(), (
        "requirements-gateway.lock is stale; run `make lock`"
    )


def test_the_gateway_image_installs_only_from_the_hashed_runtime_lock() -> None:
    dockerfile = (ROOT / "deploy" / "gateway.Dockerfile").read_text()
    assert (
        "--require-hashes --no-deps --no-build-isolation -r requirements-gateway.lock" in dockerfile
    )
    context = (ROOT / ".dockerignore").read_text()
    assert "!requirements-gateway.lock" in context
    assert "!requirements-build.lock" in context


def test_the_locked_install_checks_hashes_and_never_isolates_a_build() -> None:
    """Build isolation would fetch setuptools for pyspark's source build unhashed."""
    script = (ROOT / INSTALL_SCRIPT).read_text()
    installs = [
        line
        for line in script.splitlines()
        if "pip install" in line and not line.lstrip().startswith("#")
    ]
    from_locks = [line for line in installs if "-r requirements" in line]
    assert len(from_locks) == 2, from_locks
    assert all("--require-hashes" in line for line in from_locks)
    assert all(
        "--no-build-isolation" in line for line in installs if "requirements-build.lock" not in line
    )
    assert "pip check" in script


def test_the_fast_selector_is_one_expression_everywhere_and_excludes_stream() -> None:
    """Spark tests need Java 17 and verified jars; test-fast runs where neither exists."""
    sources = {
        "Makefile": MAKEFILE.read_text(),
        "scripts/verify.sh": (ROOT / "scripts" / "verify.sh").read_text(),
        "test-fast.yml": (WORKFLOWS / "test-fast.yml").read_text(),
    }
    for name, text in sources.items():
        flattened = re.sub(r"\\\n\s*", "", text)
        found = re.findall(r'-m "(not integration[^"]*)"', flattened)
        assert found == [FAST_SELECTOR], (name, found)


def test_every_workflow_running_spark_installs_the_pinned_java(pyproject: dict) -> None:
    java = pyproject["tool"]["trace_x"]["pins"]["java"]
    running = [
        wf
        for wf in sorted(WORKFLOWS.glob("*.yml"))
        # Anywhere in a run block, one-line or multi-line (test-stream loops over files).
        if re.search(r"pytest\s+-m\s+\"?(stream|parity)\b", wf.read_text())
    ]
    assert running, "no workflow runs the stream suite; Spark would never be exercised in CI"
    for wf in running:
        text = wf.read_text()
        assert "actions/setup-java@" in text, f"{wf.name} runs Spark without installing a JDK"
        assert re.search(r"distribution:\s*temurin\b", text), f"{wf.name}: not Temurin"
        assert re.search(rf'java-version:\s*"?{java}"?\s*$', text, re.M), (
            f"{wf.name} does not install Java {java}, the pin in pyproject.toml"
        )


def _load_script(name: str) -> Any:
    """Import a module from scripts/, which is not a package."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


LOCK_PLATFORMS: dict[str, dict[str, str]] = _load_script("runtime_lock").PLATFORMS
"""The platforms every hashed lock is installed on, shared with scripts/runtime_lock.py so
the completeness guard and the gateway lock's derivation cannot disagree."""


def _locked_pins() -> dict[str, str]:
    from packaging.utils import canonicalize_name

    pins: dict[str, str] = {}
    for line in LOCK.read_text().splitlines():
        match = re.match(r"^([A-Za-z0-9_.\-]+)==([^\s\\]+)", line)
        if match:
            pins[canonicalize_name(match.group(1))] = match.group(2)
    return pins


def test_the_lock_is_complete_on_every_platform_it_is_installed_on() -> None:
    """Written after the lock, compiled on macOS arm64, failed `pip check` in a Linux
    container: SQLAlchemy requires greenlet on x86_64 and aarch64 but not on arm64
    macOS, so the resolver never locked it and every CI job would have gone red.

    Checked offline from the installed distributions' own metadata, evaluating each
    requirement's marker for every platform in LOCK_PLATFORMS (and for every extra
    anyone requests of that package)."""
    from importlib import metadata

    from packaging.markers import default_environment
    from packaging.requirements import Requirement
    from packaging.utils import canonicalize_name

    pins = _locked_pins()
    requirements: list[tuple[str, Requirement]] = []
    for name in sorted(pins):
        try:
            distribution = metadata.distribution(name)
        except metadata.PackageNotFoundError:
            pytest.skip(
                f"SKIPPED (NOT PASSED): {name} is locked but not installed, so its metadata cannot "
                f"be read. Install from the lock (`make setup`) and re-run."
            )
        requirements.extend((name, Requirement(raw)) for raw in distribution.requires or [])

    # Extras requested of a package, from pyproject and -- to a fixed point -- from any
    # requirement that itself applies. A requirement inside an extra nobody installs
    # (pip-audit's own `dev` extra pulling `pip-audit[doc]`) must not count, or the
    # guard reports packages nothing installs and teaches people to ignore it.
    project = tomllib.loads(PYPROJECT.read_text())["project"]
    requested: set[tuple[str, str]] = set()
    for group in [project["dependencies"], *project["optional-dependencies"].values()]:
        for raw in group:
            req = Requirement(raw)
            requested |= {(canonicalize_name(req.name), extra) for extra in req.extras}

    environments: dict[str, dict[str, str]] = {
        platform_name: {key: str(value) for key, value in default_environment().items()} | overrides
        for platform_name, overrides in LOCK_PLATFORMS.items()
    }

    def applies(owner: str, req: Requirement, environment: dict[str, str]) -> bool:
        if req.marker is None:
            return True
        extras = ["", *(extra for package, extra in requested if package == owner)]
        return any(req.marker.evaluate({**environment, "extra": extra}) for extra in extras)

    while True:
        grown = requested | {
            (canonicalize_name(req.name), extra)
            for owner, req in requirements
            for environment in environments.values()
            if applies(owner, req, environment)
            for extra in req.extras
        }
        if grown == requested:
            break
        requested = grown

    missing: set[str] = set()
    for platform_name, environment in environments.items():
        for owner, req in requirements:
            if applies(owner, req, environment) and canonicalize_name(req.name) not in pins:
                missing.add(
                    f"{canonicalize_name(req.name)} (required by {owner}) on {platform_name}"
                )
    assert not missing, "requirements.lock is incomplete:\n  " + "\n  ".join(sorted(missing))


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


@pytest.mark.parametrize(
    ("command", "is_project"),
    [
        ("pip install --quiet --no-deps --no-build-isolation -e .", True),
        ("pip install --no-deps --no-build-isolation .", True),
        ("pip install --no-deps somepkg .", False),
        ("pip install -e .", False),
        ("pip3 install pip-audit", False),
    ],
)
def test_the_project_install_exemption_admits_only_the_project(
    command: str, is_project: bool
) -> None:
    assert _is_project_install(command) is is_project


def test_the_install_scan_sees_pip3() -> None:
    assert _pip_install_commands("run: pip3 install something") == ["pip3 install something"]
