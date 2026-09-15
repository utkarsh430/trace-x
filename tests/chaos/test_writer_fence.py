"""Chaos: PostgreSQL ends a writer's backend under it, and a second writer takes over (ADR-0051 §2).

The failure is produced, not simulated: `pg_terminate_backend` ends the first writer's backend,
which releases its advisory lock while the first process still believes it holds the fence. What
must hold throughout:
- the two processes are never ready at the same instant;
- the first stops being ready when its lease expires, without having learned of the loss;
- the second writes nothing until its takeover grace has passed, after the first's lease;
- the first, at its next heartbeat, finds its session lost and the lock held elsewhere;
- the terminated session is never closed, so its gap stays bounded by its last heartbeat.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from typing import Any

import pytest

from trace_core.observation.session import WRITER_LOCK_KEY
from trace_core.observation.supervisor import WriterSupervisor
from trace_core.repositories.postgres_sessions import writer_connection

pytestmark = pytest.mark.chaos

CHAOS_LOCK_KEY = WRITER_LOCK_KEY + 2
"""Not the gateway's key, so a running gateway is neither disturbed nor contended with."""
INTERVAL_S, LEASE_S, GRACE_S = 0.2, 1.0, 2.0
INSTANCE_PREFIX = "gw-chaos-"


def _dsn(user_env: str, password_env: str) -> str:
    return "postgresql://{u}:{p}@{h}:{port}/{db}".format(
        u=os.environ.get(user_env, ""),
        p=os.environ.get(password_env, ""),
        h=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5442"),
        db=os.environ.get("POSTGRES_DB", "tracex"),
    )


@pytest.fixture
def owner() -> Iterator[Any]:
    psycopg = pytest.importorskip("psycopg")
    try:
        connection = psycopg.connect(
            _dsn("POSTGRES_SUPERUSER", "POSTGRES_SUPERUSER_PASSWORD"), autocommit=True
        )
        connection.execute("SELECT 1 FROM app.producer_sessions LIMIT 1")
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no migrated PostgreSQL reachable as the owner ({exc}). Run "
            f"`make up && make migrate`; the fence is a database guarantee and is never mocked."
        )
    yield connection
    connection.execute(
        "DELETE FROM app.producer_sessions WHERE instance_id LIKE %s", (f"{INSTANCE_PREFIX}%",)
    )
    connection.close()


def _holder_pid(owner: Any) -> int | None:
    row = owner.execute(
        "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted "
        "AND classid = %s::oid AND objid = %s::oid AND objsubid = 1",
        (CHAOS_LOCK_KEY >> 32, CHAOS_LOCK_KEY & 0xFFFFFFFF),
    ).fetchone()
    return None if row is None else int(row[0])


def test_a_terminated_writer_is_fenced_out_before_its_successor_writes(owner: Any) -> None:
    dsn = _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD")
    prepared: list[str] = []

    def supervisor(name: str) -> WriterSupervisor:
        return WriterSupervisor(
            connect=lambda: writer_connection(dsn),
            producer="trace-gateway@chaos",
            instance_id=f"{INSTANCE_PREFIX}{name}",
            on_acquired=lambda: prepared.append(name),
            interval_s=INTERVAL_S,
            lease_s=LEASE_S,
            takeover_grace_s=GRACE_S,
            lock_key=CHAOS_LOCK_KEY,
        )

    first, second = supervisor("first"), supervisor("second")
    try:
        first.tick()
        assert first.ready, first.status
        terminated = first.session
        assert terminated is not None and terminated.session_id is not None
        pid = _holder_pid(owner)
        assert pid is not None, "the first writer's backend holds the lock"
        assert owner.execute("SELECT pg_terminate_backend(%s)", (pid,)).fetchone() == (True,)

        first_stopped: float | None = None
        second_started: float | None = None
        waited_out = False
        deadline = time.monotonic() + GRACE_S + 5.0
        while time.monotonic() < deadline:
            second.tick()
            first_ready, second_ready = first.ready, second.ready
            assert not (first_ready and second_ready), "two writers were ready at once"
            now = time.monotonic()
            waited_out = waited_out or second.status.startswith("taking over")
            if not first_ready and first_stopped is None:
                first_stopped = now
            if second_ready:
                second_started = now
                break
            time.sleep(0.02)

        assert second_started is not None, f"the successor never took over: {second.status}"
        assert first_stopped is not None and first_stopped < second_started
        assert waited_out, "the successor did not wait out an unclosed, recently live predecessor"
        assert second_started - first_stopped >= GRACE_S - LEASE_S - 0.5
        assert prepared == ["first", "second"]

        first.tick()
        assert not first.ready
        assert first.status == "lock held elsewhere", first.status
        row = owner.execute(
            "SELECT closed_at FROM app.producer_sessions WHERE session_id = %s",
            (terminated.session_id,),
        ).fetchone()
        assert row == (None,), "a terminated session is never closed: its gap stays bounded"
    finally:
        first.stop(confirmed=False)
        second.stop(confirmed=True)
