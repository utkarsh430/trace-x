"""The gateway pool's bounds hold against a real PostgreSQL (critic B2; ADR-0051 §2).

Requests run serially on the event loop, so an unbounded wait on the pool would stall every request
and let a fenced write land past its lease. `_postgres_pool` bounds acquiring a connection and each
statement by POSTGRES_TIMEOUT_S. The unit test pins the configuration; this proves the server and
psycopg_pool enforce it.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from typing import Any

import pytest
from services.gateway.app import _postgres_pool
from services.gateway.config import POSTGRES_TIMEOUT_S, GatewaySettings

pytestmark = pytest.mark.integration

SLACK_S = 1.5
"""Scheduling and round-trip allowance above the bound, far below the 30 s pool default."""


@pytest.fixture
def pool() -> Iterator[Any]:
    psycopg_pool = pytest.importorskip("psycopg_pool", reason="the `db` extra provides it")
    settings = GatewaySettings.from_environment(
        {**os.environ, "TRACE_PG_POOL_MIN": "1", "TRACE_PG_POOL_MAX": "1"}
    )
    opened = _postgres_pool(psycopg_pool.ConnectionPool, settings)
    try:
        opened.open(wait=True, timeout=10.0)
    except Exception as exc:  # pragma: no cover - environment dependent
        opened.close()
        pytest.skip(
            f"SKIPPED (NOT PASSED): no PostgreSQL reachable as trace_app ({exc}). Run `make up`; "
            f"the pool's bounds are the server's and psycopg_pool's, and are never faked."
        )
    yield opened
    opened.close()


def test_a_stuck_statement_is_cancelled_by_the_server_after_the_bound(pool: Any) -> None:
    import psycopg

    with pool.connection() as conn:
        assert conn.execute("SHOW statement_timeout").fetchone() == (f"{POSTGRES_TIMEOUT_S:g}s",)
        began = time.monotonic()
        with pytest.raises(psycopg.errors.QueryCanceled):
            conn.execute("SELECT pg_sleep(%s)", (POSTGRES_TIMEOUT_S + 5,))
        waited = time.monotonic() - began
    assert POSTGRES_TIMEOUT_S - 0.1 <= waited < POSTGRES_TIMEOUT_S + SLACK_S


def test_acquiring_from_an_exhausted_pool_gives_up_after_the_bound(pool: Any) -> None:
    import psycopg_pool

    with pool.connection():
        began = time.monotonic()
        with pytest.raises(psycopg_pool.PoolTimeout), pool.connection():
            pass
        waited = time.monotonic() - began
    assert POSTGRES_TIMEOUT_S - 0.1 <= waited < POSTGRES_TIMEOUT_S + SLACK_S
