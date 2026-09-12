"""Shared pytest configuration.

Rule (docs/TESTING.md §2.5): a test skipped because a resource is missing must
say so LOUDLY. A silent skip is a false pass.
"""

from __future__ import annotations

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


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip resource-gated tests with an explicit, visible reason."""
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
