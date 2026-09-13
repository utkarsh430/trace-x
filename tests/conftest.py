"""Shared pytest configuration.

Rule (docs/TESTING.md §2.5): a test skipped because a resource is missing must
say so LOUDLY. A silent skip is a false pass.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
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


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return ROOT


@pytest.fixture(scope="session")
def docker_available() -> bool:
    return _docker_available()


_STREAM_NODEIDS: set[str] = set()
_STREAM_EXECUTED: list[str] = []


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
    if _docker_available():
        return
    reason = (
        "SKIPPED (NOT PASSED): Docker is not available. Integration and chaos tests "
        "require real services -- they are never mocked (docs/TESTING.md §4). "
        "Start Docker and re-run to actually exercise these."
    )
    mark = pytest.mark.skip(reason=reason)
    for item in items:
        if {"integration", "chaos", "e2e"} & set(item.keywords):
            item.add_marker(mark)


def pytest_report_header(config: pytest.Config) -> list[str]:
    lines = [f"trace-x: repo={ROOT.name}"]
    if not _docker_available():
        lines.append(
            "trace-x: WARNING - Docker unavailable; "
            "integration/chaos/e2e tests will SKIP, not pass."
        )
    return lines


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if report.when == "call" and report.passed and report.nodeid in _STREAM_NODEIDS:
        _STREAM_EXECUTED.append(report.nodeid)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """A `-m stream` session that executed no stream test FAILS.

    Loud skips are right for a developer without a JDK, and wrong as acceptance
    evidence: a CI job whose every Spark test skipped reports green while proving
    nothing about Spark (docs/PHASE3_PLAN.md §4.4). So when the marker expression
    asks for stream tests, at least one must actually pass.
    """
    del exitstatus
    expression = str(session.config.getoption("markexpr") or "")
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
