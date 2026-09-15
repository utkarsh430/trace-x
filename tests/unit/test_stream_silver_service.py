"""The Silver job's supervision, with fake queries (critic finding B2).

Silver runs under the Bronze job's `_supervise`, whose own ordering tests are in
tests/unit/test_stream_bronze_service.py. These hold Silver's entrypoint to it: a failed query exits
3, and a query that fails just after its failure was read is never a clean stop.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from services.stream import silver as service
from services.stream.bronze import EXIT_OK, EXIT_QUERY_FAILED

pytestmark = pytest.mark.unit


class _Query:
    def __init__(
        self,
        *,
        active: bool = True,
        failure: BaseException | None = None,
        fails_after_read: bool = False,
    ) -> None:
        self.active = active
        self.failure = failure
        self.fails_after_read = fails_after_read
        self.stopped = False

    @property
    def isActive(self) -> bool:  # noqa: N802 -- Spark's name
        return self.active

    def exception(self) -> BaseException | None:
        current = self.failure
        if self.fails_after_read:
            # It fails right after this read: it records the failure, then terminates.
            self.fails_after_read = False
            self.failure = RuntimeError("failed just after its failure was read")
            self.active = False
        return current

    def stop(self) -> None:
        self.stopped = True
        self.active = False


class _Streams:
    def __init__(self) -> None:
        self.waits = 0

    def awaitAnyTermination(self, timeout: float) -> None:  # noqa: N802 -- Spark's name
        self.waits += 1
        if self.waits > 50:
            raise AssertionError("supervision never reached a verdict")

    def resetTerminated(self) -> None:  # noqa: N802 -- Spark's name
        return None


@dataclass(frozen=True)
class _Spec:
    topic: str


@dataclass(frozen=True)
class _Handle:
    spec: _Spec
    query: Any


def _supervise(*queries: _Query, stop: bool = False) -> int:
    handles = [_Handle(_Spec(f"topic.{i}.v1"), q) for i, q in enumerate(queries)]
    return service.supervise(
        _Streams(), handles, stop_requested=lambda: stop, query_failure=RuntimeError
    )


def test_a_failed_silver_query_exits_3() -> None:
    assert (
        _supervise(_Query(), _Query(active=False, failure=RuntimeError("boom")))
        == EXIT_QUERY_FAILED
    )


def test_a_query_that_fails_just_after_its_failure_was_read_is_never_a_clean_stop() -> None:
    assert _supervise(_Query(fails_after_read=True)) == EXIT_QUERY_FAILED


def test_queries_that_all_stop_cleanly_exit_0_and_a_stop_stops_them() -> None:
    assert _supervise(_Query(active=False), _Query(active=False)) == EXIT_OK
    running = _Query()
    assert _supervise(running, stop=True) == EXIT_OK
    assert running.stopped
