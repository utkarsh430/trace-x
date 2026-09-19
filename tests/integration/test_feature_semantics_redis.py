"""The Redis store against the shared feature-semantics suite, as served.

Declared acceptance command for `P2.online-features`, and part of `P3.semantics-hardening`.

Same suite file the naive reference runs, unmodified: a list scan and a mixture of sorted sets,
HyperLogLogs and hashes are asked identical questions and must give the literal answers
(ADR-0046). The store is driven as the gateway drives it -- earlier deliveries observed, the subject
scored by the one atomic record-and-read -- so a fixture that passes here passes for the code that
serves. `integration`-marked: this uses a real Redis, never a fake -- a fake would agree with the
reference by construction and prove nothing about the store that runs (docs/TESTING.md §4). It
does not run in `make verify`, and it skips loudly wherever no Redis is listening -- today that
includes CI's `test-integration` job, which provisions none (docs/PROGRESS.md, debt).

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
from tests.conformance.feature_semantics_suite import (
    ACCOUNT,
    WATCHED_LONG_ENOUGH,
    AsServedConformanceSuite,
    at,
    identity_event,
    transaction,
)

from trace_core.contracts.canonical import CanonicalTransaction
from trace_core.domain.time import EventTime, to_millis
from trace_core.features import FeatureContext, FeatureState
from trace_core.features.context import Completeness
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.observation import Event, transaction_observation
from trace_core.features.semantics import Stream
from trace_core.observation.scored_event import lookback_completeness
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
        # The epoch the position was counted in travels with it (ADR-0051 §3).
        if complete_since is not None:
            assert served.store_epoch_ms == to_millis(complete_since)
        else:
            assert served.store_epoch_ms is not None, "the first write dated the epoch"
        if complete_since is None:
            # The first write stamped the clock as the epoch; the fixture asked for no claim.
            return dataclasses.replace(served.context, complete_since=None)
        return served.context

    def test_a_legacy_identity_set_is_not_held_until_it_expires(self) -> None:
        """ADR-0046 §8 moved identity events from one account set to one set per stream, without
        migrating them: moving a deep set in one atomic script is the stall §8 removes.

        While an account's legacy set exists, its identity windows are not held and not vouched for.
        A new identity event goes to its stream's set, never the legacy one. The legacy set is given
        an expiry within the 25 hours identity events are held, and a read reaching behind what it
        held stays unheld once it has gone.
        """
        client = self._client
        store = RedisOnlineFeatureStore(client)
        store.establish_epoch(at=WATCHED_LONG_ENOUGH)
        legacy = f"f:ie:{ACCOUNT}"
        legacy_ms = to_millis(at(-7_200))
        client.zadd(legacy, {"identity_event:evt_legacy": legacy_ms})

        login = identity_event("evt_new", stream=Stream.IDENTITY_FAILED_LOGIN, occurred_at=at(-60))
        store.observe(login)
        assert client.zscore(f"f:ie:{ACCOUNT}:fl", login.identity) == to_millis(login.occurred_at)
        assert client.zcard(legacy) == 1, "a new identity event was written to the legacy set"
        assert 0 < client.pttl(legacy) <= 25 * 3_600_000

        subject = transaction()
        context = store.score(transaction_observation(subject)).context
        for feature_id in ("failed_logins_1h", "hours_since_identity_change"):
            value = ONLINE_FEATURES.get(feature_id).evaluate(subject, context)
            assert value.state is FeatureState.INSUFFICIENT_HISTORY, feature_id
            assert lookback_completeness(feature_id, context) == Completeness.INCOMPLETE.value

        client.delete(legacy)  # as its expiry would
        later = transaction(transaction_id="tx_later", occurred_at=at(1))
        context = store.score(transaction_observation(later)).context
        # The hour from T0 + 1 s reaches back only to T0 - 3 599 s, after the legacy set's newest.
        assert ONLINE_FEATURES.get("failed_logins_1h").evaluate(later, context).value == 1.0
        assert client.hget(f"f:pf:{ACCOUNT}", "dropped_through_ms") == str(legacy_ms)
