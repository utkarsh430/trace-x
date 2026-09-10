"""Acceptance-status integrity (docs/TESTING.md §1).

The status file is the machine-readable answer to "what actually works?".
These tests keep it honest: PASS requires executable evidence, and a capability
cannot claim evidence it does not have.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
STATUS = ROOT / "tests" / "acceptance" / "status.json"
VALID = {"NOT_STARTED", "IN_PROGRESS", "PASS", "FAIL", "BLOCKED"}


@pytest.fixture(scope="module")
def doc() -> dict:
    return json.loads(STATUS.read_text())


@pytest.fixture(scope="module")
def acceptance():
    spec = importlib.util.spec_from_file_location("acceptance", ROOT / "scripts" / "acceptance.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["acceptance"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_status_file_is_internally_consistent(doc: dict, acceptance) -> None:
    assert acceptance.validate(doc) == []


def test_every_capability_declares_an_acceptance_command(doc: dict) -> None:
    """A capability with no way to verify it can never legitimately reach PASS."""
    for c in doc["capabilities"]:
        assert c["evidence"]["command"], f"{c['id']} has no acceptance command"


def test_pass_requires_recorded_evidence(doc: dict) -> None:
    """CODE WRITTEN != FEATURE COMPLETE. This is the rule that enforces it."""
    for c in doc["capabilities"]:
        if c["status"] == "PASS":
            assert c["evidence"]["verified_at"], f"{c['id']} is PASS without verified_at"
            assert c["evidence"]["result"], f"{c['id']} is PASS without a recorded result"


def test_non_pass_capabilities_claim_no_evidence(doc: dict) -> None:
    for c in doc["capabilities"]:
        if c["status"] != "PASS":
            assert c["evidence"]["verified_at"] is None, (
                f"{c['id']} is {c['status']} but claims evidence"
            )


def test_capability_ids_are_unique_and_well_formed(doc: dict) -> None:
    ids = [c["id"] for c in doc["capabilities"]]
    assert len(ids) == len(set(ids)), "duplicate capability ids"
    for cid in ids:
        assert re.fullmatch(r"P[0-9]+B?\.[a-z0-9-]+", cid), f"malformed id: {cid}"


def test_all_statuses_are_valid(doc: dict) -> None:
    for c in doc["capabilities"]:
        assert c["status"] in VALID, f"{c['id']}: invalid status {c['status']}"


def test_every_roadmap_phase_has_at_least_one_capability(doc: dict) -> None:
    """A phase with no tracked capability cannot be shown to be complete."""
    phases = {c["phase"] for c in doc["capabilities"]}
    expected = {"0", "1", "2", "3", "4", "4B", "5", "6", "7", "8", "9", "10", "11", "12", "13"}
    assert expected <= phases, f"phases without capabilities: {sorted(expected - phases)}"


def test_release_blocking_capabilities_are_tracked(doc: dict) -> None:
    """The controls whose silent failure would be most damaging must be listed."""
    ids = {c["id"] for c in doc["capabilities"]}
    for required in (
        "P0.groundtruth-isolation",  # leakage invalidates every metric
        "P5.transport-parity",  # authz enforced on one transport only
        "P8.action-safety",  # raw LLM output reaching the executor
        "P9.no-hardcoding",  # a benchmark that measures nothing
        "P7.dynamic-routing",  # the "not predetermined" claim
    ):
        assert required in ids, f"release-blocking capability {required} is not tracked"


def test_setting_pass_without_a_result_is_refused(acceptance, tmp_path: Path, monkeypatch) -> None:
    """The tooling must make the dishonest path impossible, not merely discouraged."""
    scratch = tmp_path / "status.json"
    scratch.write_text(STATUS.read_text())
    monkeypatch.setattr(acceptance, "STATUS_PATH", scratch)
    doc = acceptance.load()
    rc = acceptance.cmd_set(doc, "P0.toolchain", "PASS", None)
    assert rc == 2, "marking PASS without evidence must be refused"
    assert json.loads(scratch.read_text()) == json.loads(STATUS.read_text()), (
        "file must be unchanged"
    )


def test_setting_pass_with_a_result_is_allowed(acceptance, tmp_path: Path, monkeypatch) -> None:
    scratch = tmp_path / "status.json"
    scratch.write_text(STATUS.read_text())
    monkeypatch.setattr(acceptance, "STATUS_PATH", scratch)
    doc = acceptance.load()
    assert (
        acceptance.cmd_set(doc, "P0.toolchain", "PASS", "make lint && make typecheck: exit 0") == 0
    )
    updated = json.loads(scratch.read_text())
    cap = next(c for c in updated["capabilities"] if c["id"] == "P0.toolchain")
    assert cap["status"] == "PASS"
    assert cap["evidence"]["verified_at"] and cap["evidence"]["result"]
