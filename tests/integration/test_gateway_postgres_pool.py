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
from services.gateway.app import _postgres_pool, open_pool
from services.gateway.config import POSTGRES_TIMEOUT_S, GatewaySettings

pytestmark = pytest.mark.integration

SLACK_S = 1.5
"""Scheduling and round-trip allowance above the bound, far below the 30 s pool default."""

RECOVERY_BOUND_S = 60.0
"""psycopg_pool reconnects with backoff; a role created mid-wait must be served well inside this."""


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


def test_a_pool_opened_before_its_role_exists_serves_once_the_role_is_created() -> None:
    """PROGRESS D20, reproduced: on a first `make up` the gateway starts before `make migrate`
    creates `trace_app`. The same pool, never reopened, must serve once the role exists."""
    import secrets
    import uuid

    import psycopg
    import psycopg_pool
    from psycopg import sql

    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5442")
    db = os.environ.get("POSTGRES_DB", "tracex")
    owner_dsn = (
        f"postgresql://{os.environ.get('POSTGRES_SUPERUSER', '')}:"
        f"{os.environ.get('POSTGRES_SUPERUSER_PASSWORD', '')}@{host}:{port}/{db}"
    )
    try:
        owner = psycopg.connect(owner_dsn, autocommit=True, connect_timeout=5)
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no PostgreSQL reachable as the superuser ({exc}). Run "
            f"`make up`; a late role is the server's behaviour and is never faked."
        )
    role = f"d20_probe_{uuid.uuid4().hex[:12]}"
    password = secrets.token_hex(16)
    pool = psycopg_pool.ConnectionPool(
        f"postgresql://{role}:{password}@{host}:{port}/{db}",
        min_size=1,
        max_size=1,
        open=False,
        kwargs={"connect_timeout": 2},
    )
    try:
        assert open_pool(pool, ready_wait_s=1.0) is False, "the role does not exist yet"
        assert not pool.closed
        # As migration 0001 does for trace_app: the database revokes CONNECT from PUBLIC.
        with owner.transaction():
            owner.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                    sql.Identifier(role), sql.Literal(password)
                )
            )
            owner.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    sql.Identifier(db), sql.Identifier(role)
                )
            )
        began = time.monotonic()
        with pool.connection(timeout=RECOVERY_BOUND_S) as conn:
            assert conn.execute("SELECT current_user").fetchone() == (role,)
        assert time.monotonic() - began < RECOVERY_BOUND_S
    finally:
        pool.close()
        if _role_exists(owner, role):
            owner.execute(
                sql.SQL("REVOKE ALL ON DATABASE {} FROM {}").format(
                    sql.Identifier(db), sql.Identifier(role)
                )
            )
        owner.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))
        owner.close()


def _role_exists(conn: Any, role: str) -> bool:
    return conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)).fetchone() is not None
