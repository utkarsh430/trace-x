"""The dependency lock must exist, be hashed, and match pyproject.

`env_lock_digest` is one of the 25 fields in every evaluation run manifest
(ADR-0017). A run whose environment cannot be pinned cannot be reproduced, and
an unreproducible number is indistinguishable from an invented one.
"""

from __future__ import annotations

import hashlib
import re
import tomllib
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
LOCK = ROOT / "requirements.lock"
PYPROJECT = ROOT / "pyproject.toml"


@pytest.fixture(scope="module")
def lock_text() -> str:
    assert LOCK.is_file(), "requirements.lock is missing; run `make lock`"
    return LOCK.read_text()


def test_lock_exists_and_is_not_empty(lock_text: str) -> None:
    assert len(lock_text.splitlines()) > 50


def test_lock_pins_exact_versions(lock_text: str) -> None:
    """A range in a lockfile defeats the purpose of a lockfile."""
    pins = re.findall(r"^([A-Za-z0-9._-]+)==", lock_text, re.M)
    assert len(pins) >= 40, f"expected a full transitive pin set, found {len(pins)}"
    loose = re.findall(r"^([A-Za-z0-9._-]+)(>=|<=|~=|>|<)(?!=)", lock_text, re.M)
    assert not loose, f"lockfile contains unpinned ranges: {loose[:5]}"


def test_lock_carries_hashes(lock_text: str) -> None:
    """Hashes make the install tamper-evident, not merely reproducible."""
    assert "--hash=sha256:" in lock_text
    assert lock_text.count("--hash=sha256:") >= 40


def test_lock_covers_every_declared_runtime_dependency(lock_text: str) -> None:
    data = tomllib.loads(PYPROJECT.read_text())
    declared = data["project"]["dependencies"]
    names = {re.split(r"[>=<~\[]", d, maxsplit=1)[0].strip().lower() for d in declared}
    locked = {m.lower() for m in re.findall(r"^([A-Za-z0-9._-]+)==", lock_text, re.M)}
    missing = {n for n in names if n.replace("_", "-") not in locked}
    assert not missing, (
        f"declared dependencies absent from the lock: {sorted(missing)}. Run `make lock`."
    )


def test_lock_digest_is_computable() -> None:
    """This exact value is recorded as `env_lock_digest` in every run manifest."""
    digest = hashlib.sha256(LOCK.read_bytes()).hexdigest()
    assert len(digest) == 64
    assert int(digest, 16) >= 0


def test_lock_is_installable_with_hashes(lock_text: str) -> None:
    """pip rejects a hashed requirements file that leaves any package unpinned.

    Without --allow-unsafe, pip-tools leaves `pip`/`setuptools` unpinned and the
    lockfile cannot actually be installed -- a lock that does not install is not
    a lock.
    """
    assert "# WARNING: The following packages were not pinned" not in lock_text, (
        "lockfile has unpinned packages and would be rejected by a hashed install; "
        "regenerate with `make lock` (which passes --allow-unsafe)"
    )


def test_opentelemetry_is_a_core_dependency_not_an_extra() -> None:
    """trace_core.observability imports it at module level for trace_id in logs."""
    data = tomllib.loads(PYPROJECT.read_text())
    core = " ".join(data["project"]["dependencies"]).lower()
    assert "opentelemetry-api" in core
    assert "opentelemetry-sdk" in core
