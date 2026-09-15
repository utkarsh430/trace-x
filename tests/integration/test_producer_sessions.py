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
from trace_core.observation.supervisor import WriterSupervisor
from trace_core.repositories.postgres_sessions import (
    PostgresSessionLedger,
    PostgresWriterLock,
    writer_connection,
)

pytestmark = pytest.mark.integration

TEST_LOCK_KEY = WRITER_LOCK_KEY + 1
"""Not the gateway's key: a running gateway holds that one, and these tests must not contend."""


def _dsn(user_env: str, password_env: str) -> str:
    return "postgresql://{u}:{p}@{h}:{port}/{db}".format(
        u=os.environ.get(user_env, ""),
        p=os.environ.get(password_env, ""),
        h=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5442"),
        db=os.environ.get("POSTGRES_DB", "tracex"),
    )


def _truncate() -> None:
    """As the owner: `trace_app` deliberately has no DELETE or TRUNCATE on the ledger.

    Only these tests' sessions: a running gateway's own session (`gateway-…`) is left alone, so a
    test run never takes the fence from under it.
    """
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(
        _dsn("POSTGRES_SUPERUSER", "POSTGRES_SUPERUSER_PASSWORD"), autocommit=True
    ) as owner:
        owner.execute("DELETE FROM app.producer_sessions WHERE instance_id NOT LIKE 'gateway-%'")


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
        assert first.try_acquire(TEST_LOCK_KEY)
        assert first.still_held(TEST_LOCK_KEY)
        assert not second.try_acquire(TEST_LOCK_KEY)
        assert not second.still_held(TEST_LOCK_KEY)
        conn.close()
        assert second.try_acquire(TEST_LOCK_KEY), "the lock ends with its connection"
        assert second.still_held(TEST_LOCK_KEY)
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
            lock_key=TEST_LOCK_KEY,
        )
        second = WriterSession(
            ledger=PostgresSessionLedger(other),
            lock=PostgresWriterLock(other),
            producer="trace-gateway@0.1.0",
            instance_id="gw-second",
            lock_key=TEST_LOCK_KEY,
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


RECENT_S = 5.0
"""How recent a heartbeat must be to count as live in the predecessor test."""


def _skip_if_a_gateway_holds_the_fence(conn: Any) -> None:
    """A running gateway's live session makes any other writer wait, so no absence of a live
    predecessor can be asserted while one holds the gateway's lock."""
    row = conn.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted "
        "AND classid = %s::oid AND objid = %s::oid AND objsubid = 1)",
        (WRITER_LOCK_KEY >> 32, WRITER_LOCK_KEY & 0xFFFFFFFF),
    ).fetchone()
    if row is not None and row[0]:
        pytest.skip(
            "SKIPPED (NOT PASSED): a running gateway holds the writer fence, so the absence of a "
            "live predecessor cannot be asserted. Stop it (`docker compose stop gateway`) and "
            "re-run."
        )


def test_the_writer_connection_is_autocommit_and_time_bounded(conn: Any) -> None:
    with writer_connection(_dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD")) as writer:
        assert writer.autocommit
        assert writer.execute("SHOW statement_timeout").fetchone() == ("2s",)
        assert writer.execute("SHOW application_name").fetchone() == ("trace-gateway-writer",)


def test_a_live_predecessor_is_seen_and_a_closed_or_silent_one_is_not(conn: Any) -> None:
    _skip_if_a_gateway_holds_the_fence(conn)
    ledger = PostgresSessionLedger(conn)
    # The window is short on purpose. `others_live` looks at every session in the shared database,
    # and a writer stopped without a confirmed flush -- a compose gateway with no observation log --
    # stays unclosed by design (ADR-0051 §4). Sixty seconds counted such a session as this test's
    # live predecessor. Five still spans the milliseconds between these statements many times over.
    ledger.open(session_id="s-old", producer="p", instance_id="gw-old")
    ledger.open(session_id="s-new", producer="p", instance_id="gw-new")
    assert ledger.others_live("s-new", within_s=RECENT_S)
    assert not ledger.others_live("s-new", within_s=0.0), "a heartbeat older than the window"
    assert ledger.close("s-old", last_seq=0)
    assert not ledger.others_live("s-new", within_s=RECENT_S), "a cleanly closed predecessor"
    assert ledger.others_live("s-old", within_s=RECENT_S), "the open session is live to anyone else"


def _supervisor(name: str, prepared: list[str]) -> WriterSupervisor:
    dsn = _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD")
    return WriterSupervisor(
        connect=lambda: writer_connection(dsn),
        producer="trace-gateway@0.1.0",
        instance_id=name,
        on_acquired=lambda: prepared.append(name),
        interval_s=0.2,
        lease_s=1.0,
        takeover_grace_s=2.0,
        lock_key=TEST_LOCK_KEY,
    )


def test_a_writer_that_stopped_cleanly_is_succeeded_at_once(conn: Any) -> None:
    _skip_if_a_gateway_holds_the_fence(conn)
    prepared: list[str] = []
    first, second = _supervisor("gw-first", prepared), _supervisor("gw-second", prepared)
    try:
        first.tick()
        second.tick()
        assert first.ready and not second.ready, (first.status, second.status)
        assert second.status == "lock held elsewhere"
        first.tick()
        assert first.ready, "the active session is heartbeated"
        assert first.stop(confirmed=True)
        second.tick()
        assert second.ready, second.status
        assert prepared == ["gw-first", "gw-second"]
        session = first.session
        assert session is not None and session.session_id is not None
        row = PostgresSessionLedger(conn).get(session.session_id)
        assert row is not None and row.closed_at is not None and row.last_seq == 0
    finally:
        first.stop(confirmed=False)
        second.stop(confirmed=True)
