"""A full feature store refuses loudly; it does not evict quietly.

ADR-0044, requirement by requirement, against a REAL Redis started for this
file with a deliberately tiny limit and `noeviction`:

* the store cannot silently evict correctness state -- `evicted_keys` stays at
  zero however hard it is pushed;
* a refused write is all or nothing -- the refused observation leaves no trace
  in any structure, and the observation counter does not move;
* write-capacity failure becomes visible -- the pipeline reports
  `feature_write_failed` and `ObserveOutcome.REFUSED`, and the breaker stays
  closed. Withdrawing completeness is the `CompletenessGuard`'s job, through its
  durable ledger (tests/unit/test_gateway_pipeline.py, ADR-0046 §5).

A throwaway container rather than the compose instance: filling the shared
feature store would disturb every other test, and shrinking its limit at
runtime would leave it in a state nobody expects. Skips loudly without Docker
(docs/TESTING.md §2 rule 5).
"""

from __future__ import annotations

import datetime as dt
import shutil
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest

from trace_core.domain.errors import FeatureWriteFailedError
from trace_core.domain.time import event_time
from trace_core.features.reference import Event
from trace_core.features.semantics import Stream
from trace_core.repositories.redis_features import RedisOnlineFeatureStore

pytestmark = pytest.mark.integration

IMAGE = "redis:7-alpine"
MAXMEMORY = "2mb"


def _docker(*args: str) -> subprocess.CompletedProcess[str]:
    binary = shutil.which("docker") or "docker"
    return subprocess.run([binary, *args], capture_output=True, text=True, timeout=60)  # noqa: S603


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture(scope="module")
def tiny_noeviction_redis() -> Iterator[Any]:
    redis = pytest.importorskip("redis", reason="the `db` extra provides the Redis client")
    if not shutil.which("docker"):
        pytest.skip(
            "SKIPPED (NOT PASSED): docker is not available; a full store cannot be produced"
        )
    port = _free_port()
    name = f"tracex-capacity-{uuid.uuid4().hex[:8]}"
    started = _docker(
        "run",
        "-d",
        "--rm",
        "--name",
        name,
        "-p",
        f"127.0.0.1:{port}:6379",
        IMAGE,
        "redis-server",
        "--maxmemory",
        MAXMEMORY,
        "--maxmemory-policy",
        "noeviction",
        "--appendonly",
        "no",
        "--save",
        "",
    )
    if started.returncode != 0:
        pytest.skip(f"SKIPPED (NOT PASSED): could not start {IMAGE}: {started.stderr[:200]}")
    client = redis.Redis(host="127.0.0.1", port=port, decode_responses=True, socket_timeout=2)
    for _ in range(50):
        try:
            if client.ping():
                break
        except Exception:
            time.sleep(0.1)
    try:
        yield client
    finally:
        _docker("rm", "-f", name)


def _event(i: int) -> Event:
    return Event(
        stream=Stream.TRANSACTION,
        occurred_at=event_time(dt.datetime.now(dt.UTC)),
        account_id=f"acct_{i:09d}",
        card_id=f"card_{i:09d}",
        device_id=f"dev_{i:09d}",
        merchant_id=f"mrch_{i % 50:06d}",
        ip_id=f"ip_{i:07d}",
        currency="GBP",
        amount_minor=1_000 + i,
        merchant_mcc="5411",
        merchant_country="GB",
        event_id=f"evt-{i}",
    )


def _fill_until_refused(store: RedisOnlineFeatureStore, limit: int = 200_000) -> int:
    for i in range(limit):
        try:
            store.observe(_event(i))
        except FeatureWriteFailedError:
            return i
    pytest.fail(f"wrote {limit:,} observations into a {MAXMEMORY} store and it never refused")


def test_a_full_feature_store_refuses_rather_than_evicts(tiny_noeviction_redis: Any) -> None:
    store = RedisOnlineFeatureStore(tiny_noeviction_redis, namespace="f")
    store.establish_epoch()
    assert tiny_noeviction_redis.get(store.epoch_key) is not None

    written = _fill_until_refused(store)
    assert written > 0, "the store refused the very first write; the limit is not the cause"

    stats = tiny_noeviction_redis.info("stats")
    assert int(stats["evicted_keys"]) == 0, (
        f"the feature store evicted {stats['evicted_keys']} keys under pressure. With "
        f"`noeviction` that number must be zero forever; a rise is a configuration fault."
    )
    refused = _event(written)
    assert int(tiny_noeviction_redis.get("f:position")) == written, (
        "the observation counter moved for a refused write: part of the script ran"
    )
    for key, member in (
        (f"f:txd:{refused.account_id}", refused.identity),
        (f"f:tx:{refused.account_id}", refused.identity),
    ):
        assert not tiny_noeviction_redis.execute_command(
            "HEXISTS" if ":txd:" in key else "ZSCORE", key, member
        ), f"the refused observation left {member} in {key}"
    for key in (f"f:card:{refused.card_id}", f"f:dev:{refused.device_id}"):
        assert not tiny_noeviction_redis.exists(key), f"the refused observation created {key}"
    assert tiny_noeviction_redis.get(store.epoch_key) is not None, (
        "the store withdrew its own epoch. That is the CompletenessGuard's job, recorded "
        "durably; a store-side delete is re-established by the next write as if nothing "
        "had been lost."
    )
    # Reads still work: the store is full, not gone, and its read script declares it writes nothing.
    # Every id, so the sketch counts run too: Redis flags PFCOUNT as a write.
    recorded = _event(0)
    context = store.snapshot(
        as_of=refused.occurred_at,
        account_id=recorded.account_id,
        currency="GBP",
        card_id=recorded.card_id,
        device_id=recorded.device_id,
        merchant_id=recorded.merchant_id,
        ip_id=recorded.ip_id,
    )
    assert context.complete_since is not None


def test_the_pipeline_reports_a_refused_write_without_tripping_the_breaker(
    tiny_noeviction_redis: Any,
) -> None:
    """Full is not unavailable. The breaker must stay closed, because reads still
    answer, and the decision must name the real condition."""
    from services.gateway.pipeline import (
        REASON_REDIS,
        REASON_WRITE_FAILED,
        ObserveOutcome,
        ScoringPipeline,
    )

    from trace_core.contracts.api.transaction import TransactionRequest
    from trace_core.features.definitions import ONLINE_FEATURES
    from trace_core.repositories.circuit_breaker import CircuitBreaker
    from trace_core.rules.loader import default_loader
    from trace_core.scoring.banding import load_thresholds

    store = RedisOnlineFeatureStore(tiny_noeviction_redis)
    _fill_until_refused(store)  # leave it full
    breaker = CircuitBreaker("redis-features")
    pipeline = ScoringPipeline(
        pack=default_loader(frozenset(ONLINE_FEATURES.ids)).load(),
        thresholds=load_thresholds(),
        feature_store=store,
        breaker=breaker,
    )
    request = TransactionRequest.model_validate(
        {
            "transaction_id": "tx_full_store_000001",
            "account_id": "acct_000000001",
            "amount_minor": 4_200,
            "currency": "GBP",
            "occurred_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
            "merchant_id": "mrch_000001",
            "merchant_mcc": "5411",
            "merchant_country": "GB",
            "channel": "CARD_NOT_PRESENT",
            "entry_mode": "ECOMMERCE",
        }
    )
    outcome = pipeline.score(request)

    assert outcome.observe_outcome is ObserveOutcome.REFUSED, outcome.observe_outcome
    assert REASON_WRITE_FAILED in outcome.degraded_reasons, outcome.degraded_reasons
    assert REASON_REDIS not in outcome.degraded_reasons
    assert breaker.allows(), (
        "the breaker opened on a refused write. The store is answering reads; opening the "
        "circuit would blind scoring to punish a write that did not happen."
    )
    assert breaker.consecutive_failures == 0
