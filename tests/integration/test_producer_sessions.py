"""Producer sessions against a real PostgreSQL (ADR-0051, migration 0006).

The state machine is unit-tested with fakes. This proves the database half:
- the writer's advisory lock fences a second writer and is released with its connection;
- the ledger stamps every time from the database clock;
- `trace_app` can heartbeat an open session and close it once, but can never rewrite a session's
  identity, reopen it, or delete it.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

import pytest

from trace_core.observation.session import WRITER_LOCK_KEY, SessionState, WriterSession
from trace_core.repositories.postgres_sessions import PostgresSessionLedger, PostgresWriterLock

pytestmark = pytest.mark.integration


def _dsn(user_env: str, password_env: str) -> str:
    return "postgresql://{u}:{p}@{h}:{port}/{db}".format(
        u=os.environ.get(user_env, ""),
        p=os.environ.get(password_env, ""),
        h=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5442"),
        db=os.environ.get("POSTGRES_DB", "tracex"),
    )


def _truncate() -> None:
    """As the owner: `trace_app` deliberately has no DELETE or TRUNCATE on the ledger."""
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(
        _dsn("POSTGRES_SUPERUSER", "POSTGRES_SUPERUSER_PASSWORD"), autocommit=True
    ) as owner:
        owner.execute("TRUNCATE app.producer_sessions")


def _app_connection() -> Any:
    psycopg = pytest.importorskip("psycopg")
    return psycopg.connect(_dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD"), autocommit=True)


@pytest.fixture
def conn() -> Iterator[Any]:
    try:
        probe = _app_connection()
        probe.execute("SELECT 1 FROM app.producer_sessions LIMIT 1")
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no migrated PostgreSQL reachable as trace_app ({exc}). "
            f"Run `make up && make migrate`; the session ledger is a database guarantee and is "
            f"never mocked (docs/TESTING.md §4)."
        )
    _truncate()
    yield probe
    probe.close()
    _truncate()


def test_a_session_opens_heartbeats_and_closes_once_on_the_database_clock(conn: Any) -> None:
    ledger = PostgresSessionLedger(conn)
    opened = ledger.open(session_id="s-life", producer="trace-gateway@0.1.0", instance_id="gw-a")
    assert opened.started_at == opened.heartbeat_at
    assert opened.closed_at is None and opened.last_seq is None
    assert ledger.heartbeat("s-life")
    beating = ledger.get("s-life")
    assert beating is not None and beating.heartbeat_at >= opened.heartbeat_at
    assert ledger.close("s-life", last_seq=42)
    closed = ledger.get("s-life")
    assert closed is not None and closed.last_seq == 42 and closed.closed_at is not None
    assert not ledger.heartbeat("s-life"), "a closed session no longer heartbeats"
    assert not ledger.close("s-life", last_seq=43), "a session closes once"
    assert not ledger.heartbeat("s-missing")


def test_the_client_cannot_supply_a_time(conn: Any) -> None:
    psycopg = pytest.importorskip("psycopg")
    ledger = PostgresSessionLedger(conn)
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        conn.execute(
            "INSERT INTO app.producer_sessions (session_id, producer, instance_id, started_at) "
            "VALUES ('s-dated', 'p', 'i', '2000-01-01')"
        )
    ledger.open(session_id="s-clock", producer="p", instance_id="i")
    conn.execute(
        "UPDATE app.producer_sessions SET heartbeat_at = '2000-01-01' WHERE session_id = 's-clock'"
    )
    row = ledger.get("s-clock")
    assert row is not None and row.heartbeat_at.year > 2000, "the trigger stamps now()"


def test_the_application_role_cannot_rewrite_reopen_or_delete_a_session(conn: Any) -> None:
    psycopg = pytest.importorskip("psycopg")
    ledger = PostgresSessionLedger(conn)
    ledger.open(session_id="s-guard", producer="p", instance_id="i")
    for statement in (
        "UPDATE app.producer_sessions SET producer = 'other' WHERE session_id = 's-guard'",
        "DELETE FROM app.producer_sessions WHERE session_id = 's-guard'",
        "TRUNCATE app.producer_sessions",
    ):
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute(statement)
    with pytest.raises(psycopg.errors.RaiseException):
        conn.execute("UPDATE app.producer_sessions SET last_seq = 3 WHERE session_id = 's-guard'")
    ledger.close("s-guard", last_seq=7)
    with pytest.raises(psycopg.errors.RaiseException):
        conn.execute(
            "UPDATE app.producer_sessions SET closed_at = NULL, last_seq = NULL "
            "WHERE session_id = 's-guard'"
        )
    row = ledger.get("s-guard")
    assert row is not None and row.last_seq == 7 and row.producer == "p"


def test_the_writer_lock_fences_a_second_writer_until_its_connection_ends(conn: Any) -> None:
    other = _app_connection()
    try:
        first, second = PostgresWriterLock(conn), PostgresWriterLock(other)
        assert first.try_acquire(WRITER_LOCK_KEY)
        assert first.still_held(WRITER_LOCK_KEY)
        assert not second.try_acquire(WRITER_LOCK_KEY)
        assert not second.still_held(WRITER_LOCK_KEY)
        conn.close()
        assert second.try_acquire(WRITER_LOCK_KEY), "the lock ends with its connection"
        assert second.still_held(WRITER_LOCK_KEY)
    finally:
        other.close()


def test_a_second_writer_session_is_not_ready_while_the_first_holds_the_fence(conn: Any) -> None:
    other = _app_connection()
    try:
        first = WriterSession(
            ledger=PostgresSessionLedger(conn),
            lock=PostgresWriterLock(conn),
            producer="trace-gateway@0.1.0",
            instance_id="gw-first",
        )
        second = WriterSession(
            ledger=PostgresSessionLedger(other),
            lock=PostgresWriterLock(other),
            producer="trace-gateway@0.1.0",
            instance_id="gw-second",
        )
        assert first.start() and first.ready
        assert not second.start() and not second.ready
        assert first.heartbeat()
        for _ in range(3):
            first.next_seq()
        assert first.close(confirmed=True)
        assert first.state is SessionState.CLOSED
        assert first.session_id is not None
        row = PostgresSessionLedger(other).get(first.session_id)
        assert row is not None and row.last_seq == 3
    finally:
        other.close()
