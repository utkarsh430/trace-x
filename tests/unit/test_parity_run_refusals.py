"""A measured parity run refuses provenance it cannot resolve (ADR-0056 §6).

`data.generator.record._git` returns "" when a call fails, so `git_commit_sha` reads "unknown" and
`is_dirty` reads a clean tree. A measured run must refuse rather than record that pair.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from eval.parity import run
from eval.parity.partition import REPRESENTATIVE_LATENESS
from eval.parity.record import resolved_commit
from eval.parity.run import (
    Mode,
    RunPlan,
    RunRefusedError,
    Services,
    execute,
    measured_refusals,
)

from trace_core.stream.lake import LakeConfig

pytestmark = pytest.mark.parity

SHA1 = "a" * 40  # a resolved-looking commit name; low entropy so the secret scan stays quiet
VERIFIED = {"eval_v2_manifest_digest": "sha256:" + "1" * 64}


def _services(tmp_path: Path) -> Services:
    inert: Any = None
    return Services(
        client=inert,
        token="",
        feature_redis=inert,
        open_holes=lambda: 0,
        bootstrap="unused:9092",
        spark=inert,
        lake=LakeConfig.at(tmp_path / "lake"),
        gateway={"target": "unused", "image_digest": "sha256:" + "2" * 64},
    )


def _plan() -> RunPlan:
    return RunPlan(
        name="refusal",
        gated=True,
        base_ref="none",
        warmup=[],
        slice_events=[],
        lateness=REPRESENTATIVE_LATENESS,
        partition={"name": "refusal", "gated": True, "digest": None},
        dataset=VERIFIED,
    )


@pytest.mark.parametrize("sha", ["unknown", "", "abc123", SHA1.upper(), SHA1 + "0"])
def test_a_sha_that_names_no_commit_is_unresolved(sha: str) -> None:
    assert not resolved_commit(sha)


@pytest.mark.parametrize("sha", [SHA1, "a" * 64])
def test_full_sha1_and_sha256_object_names_resolve(sha: str) -> None:
    assert resolved_commit(sha)


def test_an_otherwise_clean_measured_run_has_no_refusal(tmp_path: Path) -> None:
    services = _services(tmp_path)
    assert measured_refusals(sha=SHA1, dirty=False, dataset=VERIFIED, services=services) == []


def test_an_unknown_sha_is_refused_even_when_the_tree_reads_clean(tmp_path: Path) -> None:
    services = _services(tmp_path)
    problems = measured_refusals(sha="unknown", dirty=False, dataset=VERIFIED, services=services)
    assert problems == ["the commit SHA cannot be determined ('unknown')"]


def test_a_measured_run_with_a_failed_sha_lookup_refuses_before_posting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mutation: the lookup fails, so the SHA is "unknown" and the tree reads clean. Every other
    precondition holds, and nothing past the refusal is reachable (the services are inert)."""
    monkeypatch.setattr(run, "git_commit_sha", lambda: "unknown")
    monkeypatch.setattr(run, "is_dirty", lambda: False)
    with pytest.raises(RunRefusedError, match="commit SHA cannot be determined"):
        execute(_plan(), _services(tmp_path), mode=Mode.MEASURED, output_dir=tmp_path / "out")
    assert not (tmp_path / "out").exists()
