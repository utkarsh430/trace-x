"""The Redis store against the shared feature-semantics suite.

Declared acceptance command for `P2.online-features`.

Same suite file the naive reference implementation runs, unmodified. That is the
point: two genuinely different implementations -- a list scan and a mixture of
sorted sets, HyperLogLogs and bucketed hashes -- are asked identical questions and
must give identical answers. Phase 3 adds Spark as a third subject of the same
file, which is how feature parity gets verified without anyone redefining what a
feature means (ADR-0032).

`integration`-marked: this uses a real Redis, never a fake. `docs/TESTING.md` §4
forbids mocking Redis at this layer -- a fake sorted set would agree with the
reference implementation by construction and prove nothing about the thing that
actually runs.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from tests.conformance.feature_semantics_suite import FeatureSemanticsConformanceSuite

from trace_core.contracts.canonical import CanonicalTransaction
from trace_core.domain.time import EventTime
from trace_core.features import FeatureContext
from trace_core.features.reference import Event
from trace_core.repositories.redis_features import RedisOnlineFeatureStore

pytestmark = [pytest.mark.integration, pytest.mark.parity]

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6389"))


@pytest.fixture
def redis_client() -> Iterator[object]:
    redis = pytest.importorskip("redis", reason="the `db` extra provides the Redis client")
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
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


class TestRedisOnlineFeatureStore(FeatureSemanticsConformanceSuite):
    """The Redis store must pass the suite the reference implementation defines."""

    @pytest.fixture(autouse=True)
    def _store(self, redis_client: object) -> Iterator[None]:
        redis_client.flushdb()  # type: ignore[attr-defined]
        self._client = redis_client
        yield
        redis_client.flushdb()  # type: ignore[attr-defined]

    def build_context(
        self, history: list[Event], *, as_of: EventTime, subject: CanonicalTransaction
    ) -> FeatureContext:
        store = RedisOnlineFeatureStore(self._client)  # type: ignore[arg-type]
        for index, event in enumerate(history):
            # A distinct event_id per observation: the store deduplicates by it,
            # and two genuinely different observations sharing one would silently
            # undercount velocity.
            store.observe(
                Event(
                    **{
                        **{f.name: getattr(event, f.name) for f in _fields(event)},
                        "event_id": f"evt-{index}",
                    }
                )
            )
        return store.snapshot(
            as_of=as_of,
            account_id=subject.account_id,
            currency=subject.currency,
            card_id=subject.card_id,
            device_id=subject.device_id,
            merchant_id=subject.merchant_id,
            ip_id=subject.ip_id,
        )


def _fields(event: Event) -> tuple:
    import dataclasses

    return dataclasses.fields(event)
