"""The `bronze_ingest` entrypoint's own decisions, without a JVM (Step 5)."""

from __future__ import annotations

import hashlib

import pytest
from services.stream.bronze import (
    DIRTY_ENV,
    GIT_SHA_ENV,
    ProvenanceUnavailableError,
    _parser,
    provenance,
)

from trace_core.domain.errors import ProvenanceError

pytestmark = pytest.mark.unit

SHA = hashlib.sha1(b"trace-x bronze service test", usedforsecurity=False).hexdigest()


def test_provenance_from_the_environment_needs_an_explicit_dirty_flag() -> None:
    assert provenance({GIT_SHA_ENV: SHA, DIRTY_ENV: "false"}) == (SHA, False)
    assert provenance({GIT_SHA_ENV: SHA, DIRTY_ENV: "TRUE"}) == (SHA, True)
    with pytest.raises(ProvenanceUnavailableError, match=DIRTY_ENV):
        provenance({GIT_SHA_ENV: SHA})
    with pytest.raises(ProvenanceError):
        provenance({GIT_SHA_ENV: SHA[:12], DIRTY_ENV: "false"})


def test_a_run_must_choose_available_now_or_an_interval() -> None:
    with pytest.raises(SystemExit):
        _parser().parse_args(["run", "--bootstrap", "127.0.0.1:9092"])
    with pytest.raises(SystemExit):
        _parser().parse_args(["run", "--available-now", "--interval-s", "5"])
    args = _parser().parse_args(["run", "--available-now", "--topic", "tx.raw.v1"])
    assert args.available_now and args.topic == ["tx.raw.v1"]
    with pytest.raises(SystemExit):
        _parser().parse_args(["coverage", "--lease-s", "6"])  # the clock margin is measured
    args = _parser().parse_args(["coverage", "--clock-margin-s", "1"])
    assert args.clock_margin_s == 1.0 and args.lease_s is None and args.takeover_margin_s is None
