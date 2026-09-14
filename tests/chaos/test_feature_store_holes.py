"""An outage's hole outlives the gateway that saw it (ADR-0046 §5).

The first gateway loses Redis mid-flight. The transaction it scores blind is not recorded, so the
feature store's history has a hole and every window spanning it is incomplete. That gateway stops
before Redis returns -- outages and restarts travel together -- so it never withdraws the store's
completeness, and nothing in Redis says anything was lost: the epoch still vouches for the whole
period.

The next gateway must know anyway. It reads the durable hole from PostgreSQL at start-up, moves the
store's epoch past the hole before serving, and its decisions say `history_incomplete`. Against the
real feature-store container, paused, and a real migrated PostgreSQL; skips loudly without either.
"""

from __future__ import annotations

import datetime as dt
import os
import time
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from services.gateway.app import GatewayState, create_app
from services.gateway.config import GatewaySettings
from services.gateway.pipeline import REASON_REDIS, ScoringPipeline
from tests.chaos.test_redis_down import CONTAINER, SECRET, SOCKET_TIMEOUT_S, _docker, _post
from tests.integration.test_completeness_ledger import _dsn, _truncate

from trace_core.contracts.api.transaction import MAX_CLOCK_SKEW_FUTURE_S
from trace_core.domain.time import event_time, to_millis
from trace_core.features.completeness import CompletenessGuard
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.state_plan import PLAN
from trace_core.observability.metrics import HotPathMetrics
from trace_core.repositories.circuit_breaker import CircuitBreaker
from trace_core.repositories.postgres_completeness import PostgresHoleLedger
from trace_core.repositories.redis_features import RedisOnlineFeatureStore
from trace_core.rules.loader import default_loader
from trace_core.scoring.banding import load_thresholds
from trace_core.security.service_tokens import ServiceTokenVerifier

pytestmark = [pytest.mark.chaos, pytest.mark.integration]


def _answering(client: Any) -> bool:
    for _ in range(50):
        try:
            if client.ping():
                return True
        except Exception:  # noqa: S110 - polling a container that is resuming
            pass
        time.sleep(0.1)
    return False


@pytest.fixture
def feature_redis() -> Iterator[Any]:
    redis = pytest.importorskip("redis", reason="the `db` extra provides the Redis client")
    if _docker("inspect", "--format", "{{.State.Running}}", CONTAINER).stdout.strip() != "true":
        pytest.skip(
            f"SKIPPED (NOT PASSED): container {CONTAINER!r} is not running, so no outage can be "
            f"produced and hole inheritance is NOT exercised. Run `make up`."
        )
    from redis.backoff import NoBackoff
    from redis.retry import Retry

    client = redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", "6389")),
        decode_responses=True,
        socket_timeout=SOCKET_TIMEOUT_S,
        socket_connect_timeout=SOCKET_TIMEOUT_S,
        retry=Retry(NoBackoff(), 0),
        retry_on_timeout=False,
    )
    client.flushdb()
    try:
        yield client
    finally:
        # Always unpause: a paused container left behind breaks every later run.
        _docker("unpause", CONTAINER)
        if _answering(client):
            client.flushdb()


@pytest.fixture
def ledger_pool() -> Iterator[Any]:
    psycopg_pool = pytest.importorskip("psycopg_pool", reason="the `db` extra provides it")
    psycopg = pytest.importorskip("psycopg")
    try:
        with psycopg.connect(_dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD")) as probe:
            probe.execute("SELECT 1 FROM app.feature_store_holes LIMIT 1")
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no migrated PostgreSQL reachable as trace_app ({exc}). The "
            f"hole ledger is a database guarantee and is never mocked (docs/TESTING.md §4)."
        )
    _truncate()
    created = psycopg_pool.ConnectionPool(
        _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD"), min_size=1, max_size=2, open=True
    )
    try:
        yield created
    finally:
        created.close()
        _truncate()


def _gateway(redis_client: Any, pool: Any, instance_id: str) -> GatewayState:
    """Wired as `build_state` wires it: one guard, shared by the pipeline and readiness."""
    store = RedisOnlineFeatureStore(redis_client)
    breaker = CircuitBreaker("redis-features")
    guard = CompletenessGuard(store, PostgresHoleLedger(pool), instance_id=instance_id)
    loader = default_loader(frozenset(ONLINE_FEATURES.ids))
    return GatewayState(
        settings=GatewaySettings.from_environment({}),
        verifier=ServiceTokenVerifier({"psp-one": SECRET}),
        loader=loader,
        pipeline=ScoringPipeline(
            pack=loader.load(),
            thresholds=load_thresholds(),
            feature_store=store,
            breaker=breaker,
            completeness=guard,
        ),
        metrics=HotPathMetrics(),
        redis=redis_client,
        breaker=breaker,
        feature_store=store,
        completeness=guard,
    )


def test_a_restarted_gateway_inherits_the_hole_an_outage_left(
    feature_redis: Any, ledger_pool: Any
) -> None:
    store = RedisOnlineFeatureStore(feature_redis)
    warm_since = event_time(
        dt.datetime.now(dt.UTC) - dt.timedelta(seconds=PLAN.widest_lookback_s + 3_600)
    )
    store.establish_epoch(at=warm_since)
    ledger = PostgresHoleLedger(ledger_pool)

    with TestClient(create_app(_gateway(feature_redis, ledger_pool, "chaos-first"))) as first:
        warm = _post(first)
        assert warm.status_code == 200
        assert warm.headers["X-Trace-Degraded"] == "false", "fixture: the store should be warm"
        _docker("pause", CONTAINER)
        blind = _post(first)
        assert blind.status_code == 200, "an outage must never become a 5xx"
        assert REASON_REDIS in blind.json()["degraded_reasons"]
        assert ledger.open_holes() == 1, "the transaction scored blind was not recorded as a hole"
    # The first gateway is gone before Redis returns, so it withdrew nothing.

    _docker("unpause", CONTAINER)
    assert _answering(feature_redis), "the feature store did not come back after unpause"
    assert feature_redis.get(store.epoch_key) == str(to_millis(warm_since)), (
        "fixture: the epoch moved without the ledger. The point of this test is that nothing in "
        "Redis records the loss"
    )

    restarted_ms = to_millis(dt.datetime.now(dt.UTC))
    with TestClient(create_app(_gateway(feature_redis, ledger_pool, "chaos-second"))) as second:
        epoch_ms = int(feature_redis.get(store.epoch_key))
        assert epoch_ms >= restarted_ms + MAX_CLOCK_SKEW_FUTURE_S * 1000, (
            f"the restarted gateway served with the epoch at {epoch_ms}, not moved past the "
            f"inherited hole: its windows would vouch for a history with a transaction missing"
        )
        assert ledger.open_holes() == 0, "the inherited hole was withdrawn but not cleared"
        after = _post(second)
        assert after.status_code == 200
        assert after.headers["X-Trace-Degraded"] == "true"
        assert "history_incomplete" in after.json()["degraded_reasons"], (
            "a gateway that inherited a hole decided as if its history were complete"
        )
