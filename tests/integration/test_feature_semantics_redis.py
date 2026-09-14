"""The Redis store against the shared feature-semantics suite, as served.

Declared acceptance command for `P2.online-features`, and part of `P3.semantics-hardening`.

Same suite file the naive reference runs, unmodified: a list scan and a mixture of sorted sets,
HyperLogLogs and hashes are asked identical questions and must give the literal answers
(ADR-0046). The store is driven as the gateway drives it -- earlier deliveries observed, the subject
scored by the one atomic record-and-read -- so a fixture that passes here passes for the code that
serves. `integration`-marked: this uses a real Redis, never a fake -- a fake would agree with the
reference by construction and prove nothing about the store that runs (docs/TESTING.md §4). It
runs in CI's `test-integration` job, not in `make verify`.

The Phase 2 store failed 34 of these fixtures, recorded as strict expected failures with their
exact diverging feature sets until the Step 1 store made them pass (ADR-0046 §5; git history).
What short literal histories cannot reach -- folding, identity across accounts, concurrent
writers -- is `test_redis_feature_store.py`.
"""

from __future__ import annotations

import dataclasses
import os
from collections.abc import Iterator, Sequence
from typing import Any

import pytest
from tests.conformance.feature_semantics_suite import AsServedConformanceSuite

from trace_core.contracts.canonical import CanonicalTransaction
from trace_core.domain.time import EventTime
from trace_core.features import FeatureContext
from trace_core.features.observation import Event
from trace_core.repositories.redis_features import RedisOnlineFeatureStore

pytestmark = [pytest.mark.integration, pytest.mark.parity]

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6389"))
REDIS_DB = int(os.environ.get("REDIS_TEST_DB", "15"))
"""A database the live gateway does not use: this suite flushes what it touches."""


@pytest.fixture
def redis_client() -> Iterator[Any]:
    redis = pytest.importorskip("redis", reason="the `db` extra provides the Redis client")
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    try:
        client.ping()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no Redis at {REDIS_HOST}:{REDIS_PORT} ({exc}). "
            f"Run `make up` -- this suite uses a real Redis and never a fake, because "
            f"a fake would agree with the reference implementation by construction "
            f"(docs/TESTING.md §4)."
        )
    yield client
    client.flushdb()


class TestRedisOnlineFeatureStore(AsServedConformanceSuite):
    """The Redis store must give the literal answers the reference gives."""

    @pytest.fixture(autouse=True)
    def _store(self, redis_client: Any) -> Iterator[None]:
        redis_client.flushdb()
        self._client = redis_client
        yield
        redis_client.flushdb()

    def context_for(
        self,
        log: Sequence[Event],
        subject_index: int,
        subject: CanonicalTransaction,
        *,
        complete_since: EventTime | None,
    ) -> FeatureContext:
        # A fresh store per build: fixtures check more than once with different completeness
        # claims, and the epoch is set with NX.
        self._client.flushdb()
        store = RedisOnlineFeatureStore(self._client)
        if complete_since is not None:
            store.establish_epoch(at=complete_since)
        for event in log[:subject_index]:
            store.observe(event)
        served = store.score(log[subject_index])
        assert served.receipt.position == len({e.identity for e in log[: subject_index + 1]})
        if complete_since is None:
            # The first write stamped the clock as the epoch; the fixture asked for no claim.
            return dataclasses.replace(served.context, complete_since=None)
        return served.context
