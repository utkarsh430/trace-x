"""Shared pytest configuration.

Rule (docs/TESTING.md §2.5): a test skipped because a resource is missing must
say so LOUDLY. A silent skip is a false pass.
"""

from __future__ import annotations

import functools
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
# `packages/` holds the installed distribution; the repo root makes `data.*`
# (the generator and the source adapters, CLAUDE.md §4) and `tests.conformance`
# importable. Both are needed explicitly: adding `tests/conformance/__init__.py`
# changed pytest's rootdir insertion, so relying on it was fragile -- the
# conformance suite passed when run alone and failed in the full run.
for path in (ROOT / "packages", ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


@functools.cache
def _docker_available() -> bool:
    if not shutil.which("docker"):
        return False
    try:
        r = subprocess.run(
            ["docker", "info", "--format", "{{.ServerVersion}}"],
            capture_output=True,
            timeout=10,
        )
        return r.returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def _in_ci() -> bool:
    """GitHub Actions, like most CI systems, sets `CI=true`."""
    return os.environ.get("CI", "").strip().lower() in {"1", "true", "yes"}


_NEEDS_DOCKER = frozenset({"integration", "chaos", "e2e"})


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return ROOT


@pytest.fixture(scope="session")
def docker_available() -> bool:
    return _docker_available()


_STREAM_NODEIDS: set[str] = set()
_STREAM_EXECUTED: list[str] = []
_SKIPPED: list[str] = []
_SERVICE_MARKERS: re.Pattern[str] = re.compile(r"(?<!not )\b(integration|chaos|stream)\b")
_LISTED_SKIPS = 20


def ci_skip_failure(expression: str, skipped: Sequence[str], *, in_ci: bool) -> str | None:
    """The failure message for a CI session that skipped tests it selected for real services.

    A loud skip is right on a laptop missing PostgreSQL, Redis or a JDK. In CI the same skip turns a
    job green while the tests it names proved nothing (docs/PHASE3_PLAN.md §4.4), so a session whose
    marker expression selects `integration`, `chaos` or `stream` fails if any test skipped. None
    when the session may pass.
    """
    if not in_ci or not skipped or not _SERVICE_MARKERS.search(expression):
        return None
    listed = "".join(f"\n  - {nodeid}" for nodeid in skipped[:_LISTED_SKIPS])
    if len(skipped) > _LISTED_SKIPS:
        listed += f"\n  ... and {len(skipped) - _LISTED_SKIPS} more"
    return (
        f"\nFAILED (CI skip guard): {len(skipped)} test(s) skipped in a session that selects real "
        f"services. In CI a skip is not evidence; provision what they need (docs/PHASE3_PLAN.md "
        f"§4.4):{listed}\n"
    )


def _stream_toolchain_failures() -> list[str]:
    # Imported here: trace_core.stream.toolchain is stdlib-only, but the rest of
    # the suite should not pay for inspecting a JDK it does not use.
    from trace_core.stream import toolchain

    return [f"{f.component}: {f.detail}" for f in toolchain.inspect_toolchain() if not f.ok]


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip resource-gated tests with an explicit, visible reason."""
    stream_items = [item for item in items if item.get_closest_marker("stream") is not None]
    _STREAM_NODEIDS.update(item.nodeid for item in stream_items)
    if stream_items and (failures := _stream_toolchain_failures()):
        reason = (
            "SKIPPED (NOT PASSED): the Phase 3 JVM toolchain does not match its pins -- "
            + "; ".join(failures)
            + ". Run `make setup` (and select Temurin 17), then re-run."
        )
        for item in stream_items:
            item.add_marker(pytest.mark.skip(reason=reason))
    if _docker_available() or _in_ci():
        # In CI a missing Docker fails each test at setup instead (`pytest_runtest_setup`).
        return
    reason = (
        "SKIPPED (NOT PASSED): Docker is not available. Integration and chaos tests "
        "require real services -- they are never mocked (docs/TESTING.md §4). "
        "Start Docker and re-run to actually exercise these."
    )
    mark = pytest.mark.skip(reason=reason)
    for item in items:
        if _NEEDS_DOCKER & set(item.keywords):
            item.add_marker(mark)


def pytest_runtest_setup(item: pytest.Item) -> None:
    """In CI, a test that needs Docker FAILS without it rather than skipping.

    A loud skip is right on a laptop without Docker and wrong as acceptance evidence: a CI job
    whose every integration test skipped reports green while proving nothing. The event-transport
    ADR asked for this before any Kafka fixture runs; it holds for every Docker-backed layer.
    """
    if _in_ci() and _NEEDS_DOCKER & set(item.keywords) and not _docker_available():
        pytest.fail(
            "FAILED (CI): Docker is not available, so this integration/chaos/e2e test cannot "
            "exercise the real services it exists for (docs/TESTING.md §4). Provision Docker "
            "for this job; in CI a skip is not evidence.",
            pytrace=False,
        )


def pytest_report_header(config: pytest.Config) -> list[str]:
    lines = [f"trace-x: repo={ROOT.name}"]
    if not _docker_available():
        consequence = "FAIL (CI)" if _in_ci() else "SKIP, not pass"
        lines.append(
            "trace-x: WARNING - Docker unavailable; "
            f"integration/chaos/e2e tests will {consequence}."
        )
    return lines


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if report.when == "call" and report.passed and report.nodeid in _STREAM_NODEIDS:
        _STREAM_EXECUTED.append(report.nodeid)
    if report.skipped:
        _SKIPPED.append(report.nodeid)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """A `-m stream` session that executed no stream test FAILS.

    Loud skips are right for a developer without a JDK, and wrong as acceptance
    evidence: a CI job whose every Spark test skipped reports green while proving
    nothing about Spark (docs/PHASE3_PLAN.md §4.4). So when the marker expression
    asks for stream tests, at least one must actually pass.
    """
    del exitstatus
    expression = str(session.config.getoption("markexpr") or "")
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    if (skip_failure := ci_skip_failure(expression, _SKIPPED, in_ci=_in_ci())) is not None:
        if reporter is not None:
            reporter.write_line(skip_failure, red=True, bold=True)
        else:
            sys.stderr.write(skip_failure)
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
    if re.search(r"(?<!not )\bstream\b", expression) and not _STREAM_EXECUTED:
        message = (
            "\nFAILED (collection guard): `-m stream` selected no stream test that actually "
            "executed. Skips are not evidence; see docs/PHASE3_PLAN.md §4.4.\n"
        )
        reporter = session.config.pluginmanager.get_plugin("terminalreporter")
        if reporter is not None:
            reporter.write_line(message, red=True, bold=True)
        else:
            sys.stderr.write(message)
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
