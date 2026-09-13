"""Feature state cannot silently evict, and a refused write is visible.

ADR-0044. Two Redis instances with opposite contracts: the feature store runs
`noeviction` and REFUSES writes when full; the cache runs `allkeys-lru` and may
lose anything. These tests pin the two halves that can be checked without a
server -- the declared policy in compose, and what the store does when Redis
says OOM -- so a future edit that quietly flips the policy back, or swallows
the error, fails here before it reaches a load test.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pytest
import yaml
from redis.exceptions import OutOfMemoryError

from trace_core.domain.errors import FeatureWriteFailedError
from trace_core.domain.time import event_time
from trace_core.features.reference import Event
from trace_core.features.semantics import Stream
from trace_core.repositories.redis_features import RedisOnlineFeatureStore

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = yaml.safe_load((ROOT / "deploy" / "compose.yml").read_text())["services"]


def _policy(service: str) -> dict[str, str]:
    parts = COMPOSE[service]["command"].split()
    return {parts[i]: parts[i + 1] for i in range(len(parts) - 1) if parts[i].startswith("--")}


def test_the_feature_store_declares_noeviction() -> None:
    """The correctness decision of Phase 2, as configuration."""
    assert _policy("redis")["--maxmemory-policy"] == "noeviction", (
        "the feature store must refuse writes when full. Under an eviction policy an "
        "evicted feature key reads back empty, which is indistinguishable from an "
        "account with no history, and a transaction that should be CRITICAL is approved "
        "with degraded=false (ADR-0044)."
    )


def test_the_cache_store_may_evict_and_is_a_separate_instance() -> None:
    assert _policy("redis-cache")["--maxmemory-policy"] == "allkeys-lru"
    assert COMPOSE["gateway"]["environment"]["REDIS_CACHE_HOST"] == "redis-cache"
    assert COMPOSE["gateway"]["environment"]["REDIS_HOST"] == "redis"
    assert (
        COMPOSE["gateway"]["environment"]["REDIS_HOST"]
        != (COMPOSE["gateway"]["environment"]["REDIS_CACHE_HOST"])
    ), "eviction policy is per instance; one instance cannot protect one class and evict another"


class _FullRedis:
    """A client whose pipeline reports OOM, and which records what happened next."""

    def __init__(self) -> None:
        self.deleted: list[str] = []
        self.sets: list[tuple[str, Any]] = []

    def pipeline(self, transaction: bool = False) -> _FullRedis:
        return self

    def __getattr__(self, name: str) -> Any:
        # Every pipelined command is accepted and forgotten; only execute matters.
        return lambda *a, **k: None

    def execute(self) -> None:
        raise OutOfMemoryError("OOM command not allowed when used memory > 'maxmemory'.")

    def delete(self, key: str) -> None:
        self.deleted.append(key)


def test_a_refused_write_raises_a_typed_error_and_invalidates_the_epoch() -> None:
    """Loud, typed, and the completeness claim withdrawn.

    An observation that was not recorded is a hole in the history. Every window
    spanning the hole is no longer complete, and the way the store says so is
    by deleting its epoch -- so the next snapshot reports UNKNOWN completeness
    and the next decision carries `history_incomplete` (ADR-0044).
    """
    client = _FullRedis()
    store = RedisOnlineFeatureStore(client)  # type: ignore[arg-type]
    event = Event(
        stream=Stream.TRANSACTION,
        occurred_at=event_time(dt.datetime(2026, 3, 1, 12, 0, tzinfo=dt.UTC)),
        account_id="acct_000001",
        currency="GBP",
        amount_minor=100,
        event_id="evt-1",
    )
    with pytest.raises(FeatureWriteFailedError):
        store.observe(event)
    assert client.deleted == [store.epoch_key], (
        f"expected the epoch to be withdrawn after a refused write, deleted={client.deleted}"
    )
