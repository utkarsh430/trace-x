"""The `bronze_ingest` entrypoint's own decisions, without a JVM (Step 5)."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace
from typing import Any

import pytest
from services.stream.bronze import (
    DIRTY_ENV,
    EXIT_OK,
    EXIT_QUERY_FAILED,
    GIT_SHA_ENV,
    ProvenanceUnavailableError,
    _parser,
    _supervise,
    _verdict,
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


class _Query:
    """A query's two observable facts, changed in Spark's order: failure, then termination."""

    def __init__(
        self,
        *,
        active: bool = True,
        failure: str | None = None,
        fails_after_failure_read: bool = False,
        fails_on_stop: bool = False,
    ) -> None:
        self.isActive = active
        self.failure = failure
        self.fails_after_failure_read = fails_after_failure_read
        self.fails_on_stop = fails_on_stop
        self.stopped = False

    def fail(self, reason: str) -> None:
        self.failure, self.isActive = reason, False

    def exception(self) -> str | None:
        seen = self.failure
        if self.fails_after_failure_read:
            self.fails_after_failure_read = False
            self.fail("died just after its failure was read")
        return seen

    def stop(self) -> None:
        if self.fails_on_stop:
            self.fail("died while stopping")
        self.isActive, self.stopped = False, True


class _Streams:
    def __init__(self, on_wait: Any = None) -> None:
        self.waits = 0
        self.on_wait = on_wait

    def awaitAnyTermination(self, timeout: float) -> None:  # noqa: N802 -- PySpark's name
        self.waits += 1
        if self.on_wait is not None:
            self.on_wait()

    def resetTerminated(self) -> None:  # noqa: N802 -- PySpark's name
        pass


def _handle(query: _Query) -> Any:
    return SimpleNamespace(spec=SimpleNamespace(topic="tx.raw.v1"), query=query)


def test_a_query_failing_between_the_two_reads_is_a_failure_not_a_clean_stop() -> None:
    """Critic finding B2: read failure-then-liveness, this query made the job exit 0."""
    handles = [_handle(_Query(fails_after_failure_read=True))]
    assert _verdict(handles) is None, "seen running when its liveness was read"
    assert _verdict(handles) == EXIT_QUERY_FAILED


def test_the_verdict_while_running_after_a_clean_stop_and_after_a_failure() -> None:
    assert _verdict([_handle(_Query()), _handle(_Query(active=False))]) is None
    assert _verdict([_handle(_Query(active=False)), _handle(_Query(active=False))]) == EXIT_OK
    failed = _Query(active=False, failure="kafka data loss")
    assert _verdict([_handle(_Query()), _handle(failed)]) == EXIT_QUERY_FAILED


def test_a_requested_stop_stops_every_query_then_reports_a_failure_that_raced_it() -> None:
    queries = [_Query(), _Query()]
    code = _supervise(
        _Streams(),
        [_handle(q) for q in queries],
        stop_requested=lambda: True,
        progress=lambda _handle: None,
        query_failure=RuntimeError,
    )
    assert code == EXIT_OK and all(q.stopped for q in queries)
    code = _supervise(
        _Streams(),
        [_handle(_Query()), _handle(_Query(fails_on_stop=True))],
        stop_requested=lambda: True,
        progress=lambda _handle: None,
        query_failure=RuntimeError,
    )
    assert code == EXIT_QUERY_FAILED


def test_supervision_reports_a_query_that_fails_while_it_waits() -> None:
    query = _Query()

    def fail_and_rethrow() -> None:
        query.fail("kafka data loss")
        raise RuntimeError("rethrown by awaitAnyTermination")

    streams = _Streams(on_wait=fail_and_rethrow)
    progressed: list[Any] = []
    code = _supervise(
        streams,
        [_handle(query)],
        stop_requested=lambda: False,
        progress=progressed.append,
        query_failure=RuntimeError,
    )
    assert code == EXIT_QUERY_FAILED
    assert streams.waits == 1 and len(progressed) == 2, "progress is recorded before each verdict"
