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
from services.gateway.app import open_pool

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
