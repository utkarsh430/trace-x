"""Secret scan behaviour.

A security gate whose behaviour depends on which machine runs it is not a gate.
These tests pin the properties that made CI and local disagree: one shared
definition, the pinned tool version, and a scanner that actually detects a
planted credential rather than merely exiting 0.
"""

from __future__ import annotations

import importlib.util
import re
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "secret_scan.py"


@pytest.fixture(scope="module")
def scanner():
    spec = importlib.util.spec_from_file_location("secret_scan", SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["secret_scan"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_script_exists_and_is_the_single_definition() -> None:
    """`make secrets` and CI must call the SAME script, not reimplement it."""
    assert SCRIPT.is_file()
    makefile = (ROOT / "Makefile").read_text()
    assert "scripts/secret_scan.py" in makefile, "`make secrets` must call the shared script"
    lint = (ROOT / ".github" / "workflows" / "lint.yml").read_text()
    assert "scripts/secret_scan.py" in lint, "CI must call the shared script"


def test_ci_does_not_install_the_tool_unpinned() -> None:
    """An unpinned `pip install detect-secrets` made CI and local disagree."""
    lint = (ROOT / ".github" / "workflows" / "lint.yml").read_text()
    assert not re.search(r"pip install\s+detect-secrets\s*$", lint, re.M), (
        "CI must use the locked detect-secrets from the dev extra, not a fresh unpinned install"
    )


def test_tool_version_matches_the_lockfile(scanner) -> None:
    lock = (ROOT / "requirements.lock").read_text()
    m = re.search(r"^detect-secrets==([\d.]+)", lock, re.M)
    assert m, "detect-secrets is not pinned in requirements.lock"
    assert scanner.tool_version() == m.group(1)


def test_clean_tree_passes() -> None:
    r = subprocess.run(  # noqa: S603
        [sys.executable, str(SCRIPT)], cwd=ROOT, capture_output=True, text=True, timeout=300
    )
    assert r.returncode == 0, f"clean tree must pass:\n{r.stdout}\n{r.stderr}"
    assert "PASS" in r.stdout


def test_planted_credential_is_detected(tmp_path: Path) -> None:
    """A scanner that never fails is not a scanner."""
    # Assembled at runtime: writing the literal here would (correctly) make this
    # very test file trip the scanner it is testing.
    fake = "wJalrXUtnFEMI" + "/K7MDENG/bPxRfi" + "CYEXAMPLEKEY"
    probe = ROOT / "_secret_scan_probe.py"
    probe.write_text(f'AWS_SECRET_ACCESS_KEY = "{fake}"\n')
    try:
        r = subprocess.run(  # noqa: S603
            [sys.executable, str(SCRIPT)], cwd=ROOT, capture_output=True, text=True, timeout=300
        )
        assert r.returncode != 0, "a planted AWS key must fail the scan"
        assert "_secret_scan_probe" in r.stderr, "the offending file must be named in the output"
    finally:
        probe.unlink(missing_ok=True)


def test_env_is_gitignored_and_untracked(scanner) -> None:
    """The control that actually matters: .env must never reach the repository."""
    assert scanner.check_env_is_not_committed() == []


def test_env_contents_are_not_scanned(scanner) -> None:
    """A developer's local .env holds real credentials by design."""
    assert any(".env" in pattern for pattern in scanner.EXCLUDE_FILES)


def test_requirements_lock_is_excluded(scanner) -> None:
    """1800+ sha256 wheel hashes are high entropy by design and public."""
    assert any("requirements" in pattern for pattern in scanner.EXCLUDE_FILES)
