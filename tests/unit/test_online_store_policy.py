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
import re
from pathlib import Path
from typing import Any

import pytest
import yaml
from redis.exceptions import OutOfMemoryError

from trace_core.domain.errors import FeatureWriteFailedError
from trace_core.domain.time import event_time
from trace_core.features.reference import Event
from trace_core.features.semantics import Stream
from trace_core.repositories.redis_features import (
    READ_SCRIPT,
    WITHDRAW_SCRIPT,
    WRITE_SCRIPT,
    RedisOnlineFeatureStore,
    unsupported_declarations,
)

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
    """A client that refuses every writing script for OOM, and records any other command."""

    def __init__(self) -> None:
        self.commands: list[str] = []

    def register_script(self, script: str) -> Any:
        def run(keys: Any = None, args: Any = None) -> Any:
            raise OutOfMemoryError("OOM command not allowed when used memory > 'maxmemory'.")

        return run

    def __getattr__(self, name: str) -> Any:
        def command(*args: Any, **kwargs: Any) -> None:
            self.commands.append(name)

        return command


def test_a_refused_write_raises_a_typed_error_and_issues_nothing_further() -> None:
    """Loud, typed, and nothing half-done after it.

    An observation that was not recorded is a hole in the history. Withdrawing completeness is
    the gateway's `CompletenessGuard`'s job, through a durable ledger that survives a restart
    (ADR-0046 §5). The store must not answer a refusal with commands of its own -- Phase 2
    deleted the epoch here, which a restart re-established as if nothing had been lost.
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
    with pytest.raises(FeatureWriteFailedError):
        store.score(event)
    with pytest.raises(FeatureWriteFailedError):
        store.withdraw_completeness(resume_at=event.occurred_at)
    assert client.commands == [], f"the store acted after a refusal: {client.commands}"


def test_writing_scripts_are_refused_whole_before_they_run() -> None:
    """All or nothing depends on the shebang.

    Redis refuses a `#!lua` script that may write before it starts when the instance is over
    `maxmemory`. Without the shebang, a script is judged command by command, and a full store
    could keep the record and lose the distinct-count update.

    The read script must answer a full store -- it is what a refused write degrades to -- so it is
    `allow-oom`, and therefore must issue no write command except `PFCOUNT`, whose in-place
    cardinality cache Redis counts as a write but which allocates nothing.
    """
    assert WRITE_SCRIPT.startswith("#!lua\n")
    assert WITHDRAW_SCRIPT.startswith("#!lua\n")
    assert READ_SCRIPT.startswith("#!lua flags=allow-oom\n")
    assert "allow-oom" not in WRITE_SCRIPT + WITHDRAW_SCRIPT
    called = set(re.findall(r"redis\.call\('([A-Z]+)'", READ_SCRIPT))
    assert called <= READ_ONLY_COMMANDS | {"PFCOUNT"}, (
        f"the full-store read script issues {sorted(called - READ_ONLY_COMMANDS - {'PFCOUNT'})}"
    )


READ_ONLY_COMMANDS = frozenset(
    {"GET", "MGET", "HGETALL", "LRANGE", "ZCOUNT", "ZRANGEBYSCORE", "ZSCORE"}
)


def test_the_store_has_a_primitive_for_every_released_feature() -> None:
    """A feature the store cannot answer would read as absent forever, and look like warm-up."""
    assert unsupported_declarations() == []
