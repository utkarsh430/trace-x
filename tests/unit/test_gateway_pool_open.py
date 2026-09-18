"""A database that is late at start-up never closes the gateway's pool (PROGRESS D20).

psycopg_pool's `open(wait=True)` closes the pool when its wait expires, and a closed pool cannot be
reopened, so the gateway stayed unready until restarted. These run a real `ConnectionPool` against a
port nothing listens on; recovery once the database appears is proved against a real PostgreSQL in
tests/integration/test_gateway_postgres_pool.py.
"""

from __future__ import annotations

import socket
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from services.gateway import app as gateway_app
from services.gateway.app import GatewayState, create_app, open_pool
from services.gateway.config import GatewaySettings
from services.gateway.pipeline import ScoringPipeline

from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.observability.metrics import HotPathMetrics
from trace_core.rules.loader import default_loader
from trace_core.scoring.banding import load_thresholds
from trace_core.security.service_tokens import MIN_SECRET_LENGTH, ServiceTokenVerifier

pytestmark = pytest.mark.unit

psycopg_pool = pytest.importorskip("psycopg_pool", reason="the `db` extra provides it")


def _closed_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@pytest.fixture
def unreachable_pool() -> Iterator[Any]:
    pool = psycopg_pool.ConnectionPool(
        f"postgresql://nobody@127.0.0.1:{_closed_port()}/none",
        min_size=1,
        max_size=1,
        open=False,
        timeout=1.0,  # bounded as the gateway's own pool is (`_postgres_pool`)
        kwargs={"connect_timeout": 2},
    )
    yield pool
    pool.close()


def test_a_database_that_is_not_ready_leaves_the_pool_open_and_reports_not_ready(
    unreachable_pool: Any,
) -> None:
    assert open_pool(unreachable_pool, ready_wait_s=0.5) is False
    assert not unreachable_pool.closed, "the pool must keep reconnecting, never close"


def test_the_library_wait_this_replaces_closes_the_pool_on_timeout(unreachable_pool: Any) -> None:
    """Pins the trap: if psycopg_pool ever stops closing here, open_pool's reason is gone."""
    with pytest.raises(psycopg_pool.PoolTimeout):
        unreachable_pool.open(wait=True, timeout=0.5)
    assert unreachable_pool.closed
    with pytest.raises(psycopg_pool.PoolClosed):
        unreachable_pool.getconn(timeout=0.1)


def test_the_gateway_start_up_leaves_a_late_databases_pool_open(
    unreachable_pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The start-up path itself, not just `open_pool`: had the lifespan kept
    `pool.open(wait=True, timeout=...)`, the pool would be closed here and never reopen."""
    monkeypatch.setattr(gateway_app, "POOL_READY_WAIT_S", 0.5)
    loader = default_loader(frozenset(ONLINE_FEATURES.ids))
    state = GatewayState(
        settings=GatewaySettings.from_environment({}),
        verifier=ServiceTokenVerifier({"psp-one": "s" * MIN_SECRET_LENGTH}),
        loader=loader,
        pipeline=ScoringPipeline(
            pack=loader.load(), thresholds=load_thresholds(), feature_store=None
        ),
        metrics=HotPathMetrics(),
        pool=unreachable_pool,
    )
    with TestClient(create_app(state)) as client:
        assert not unreachable_pool.closed, "start-up closed the pool: a late database is fatal"
        ready = client.get("/readyz")
        assert ready.status_code == 503, "an unreachable database must read as not ready"
        assert ready.json()["checks"]["postgres"].startswith("unreachable")
