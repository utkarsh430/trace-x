"""The authorization outcomes' delivery watermark against a real PostgreSQL (migration 0008).

Database guarantees, proved against the database and never mocked:
- the watermark stops at the oldest unpublished authorization row, a refused one included, and
  passes it only once that row is marked published;
- an outbox row written by a transaction that has not committed yet is never passed over;
- it never moves backwards, never lies in the future, and survives a new connection (a restart);
- other topics' rows do not hold it, and it is advanced only inside a transaction.
"""

from __future__ import annotations

import datetime as dt
import json
import os
from collections.abc import Iterator
from typing import Any

import pytest

from trace_core.observation import outbox_watermark

pytestmark = pytest.mark.integration


def _dsn(user_env: str, password_env: str) -> str:
    return "postgresql://{u}:{p}@{h}:{port}/{db}".format(
        u=os.environ.get(user_env, ""),
        p=os.environ.get(password_env, ""),
        h=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5442"),
        db=os.environ.get("POSTGRES_DB", "tracex"),
    )


def _reset() -> None:
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(
        _dsn("POSTGRES_SUPERUSER", "POSTGRES_SUPERUSER_PASSWORD"), autocommit=True
    ) as owner:
        owner.execute("TRUNCATE app.outbox, app.outbox_delivery_watermark")


def _app(autocommit: bool = False) -> Any:
    psycopg = pytest.importorskip("psycopg")
    return psycopg.connect(
        _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD"), autocommit=autocommit
    )


@pytest.fixture
def conn() -> Iterator[Any]:
    try:
        probe = _app(autocommit=True)
        table = probe.execute("SELECT to_regclass('app.outbox_delivery_watermark')").fetchone()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no PostgreSQL reachable as trace_app ({exc}). Run `make up && "
            f"make migrate`; the watermark is a database guarantee and is never mocked."
        )
    if table is None or table[0] is None:  # pragma: no cover - environment dependent
        probe.close()
        pytest.skip("SKIPPED (NOT PASSED): migration 0008 is not applied. Run `make migrate`.")
    _reset()
    yield probe
    probe.close()
    _reset()


def _insert(conn: Any, key: str, *, topic: str = "tx.authorization.v1") -> tuple[int, dt.datetime]:
    row = conn.execute(
        "INSERT INTO app.outbox (topic, partition_key, idempotency_key, payload) "
        "VALUES (%s, 'acct_000001', %s, %s) RETURNING outbox_id, created_at",
        (topic, key, json.dumps({"envelope": {}, "payload": {}})),
    ).fetchone()
    return int(row[0]), row[1]


def _require_no_older_open_transaction(conn: Any, than: dt.datetime) -> None:
    """Forward progress can only be asserted when no other `trace_app` transaction older than `than`
    is open: such a transaction could still commit an outbox row, and the watermark must wait."""
    row = conn.execute(
        "SELECT min(xact_start) FROM pg_stat_activity WHERE usename = current_user "
        "AND datname = current_database() AND pid <> pg_backend_pid() AND xact_start IS NOT NULL"
    ).fetchone()
    if row is not None and row[0] is not None and row[0] <= than:
        pytest.skip(
            "SKIPPED (NOT PASSED): another trace_app transaction older than the row under test is "
            "open, so the watermark rightly waits and forward progress cannot be asserted. Re-run "
            "with no other PostgreSQL clients."
        )


def _advance() -> dt.datetime:
    with _app() as relay, relay.transaction():
        return outbox_watermark.advance(relay)


def test_an_empty_outbox_advances_to_the_relays_transaction_start_and_survives_a_restart(
    conn: Any,
) -> None:
    before = conn.execute("SELECT now()").fetchone()[0]
    _require_no_older_open_transaction(conn, before)
    first = _advance()
    assert first >= before
    with _app(autocommit=True) as restarted:
        assert outbox_watermark.read(restarted) == first


def test_an_unpublished_row_holds_the_watermark_until_it_is_marked_published(conn: Any) -> None:
    outbox_id, created_at = _insert(conn, "k-1")
    _require_no_older_open_transaction(conn, created_at)
    assert _advance() == created_at
    assert _advance() == created_at, "a pass that confirms nothing moves nothing"
    conn.execute("UPDATE app.outbox SET published_at = now() WHERE outbox_id = %s", (outbox_id,))
    assert _advance() > created_at


def test_a_refused_row_holds_the_watermark_while_it_stays_unresolved(conn: Any) -> None:
    outbox_id, created_at = _insert(conn, "k-refused")
    conn.execute(
        "UPDATE app.outbox SET attempts = attempts + 1, last_error = 'refused: invalid' "
        "WHERE outbox_id = %s",
        (outbox_id,),
    )
    later_id, _ = _insert(conn, "k-later")
    conn.execute("UPDATE app.outbox SET published_at = now() WHERE outbox_id = %s", (later_id,))
    assert _advance() <= created_at
    _require_no_older_open_transaction(conn, created_at)
    assert _advance() == created_at


def test_an_uncommitted_outbox_row_is_never_passed_over(conn: Any) -> None:
    writer = _app()
    try:
        writer.execute("SELECT 1")  # the writer's transaction is now open
        writer_started = writer.execute("SELECT now()").fetchone()[0]
        _, created_at = _insert(writer, "k-inflight")
        assert created_at == writer_started, "0007 stamps created_at as the transaction's start"
        naive = conn.execute(
            "SELECT least(now(), coalesce((SELECT min(created_at) FROM app.outbox "
            "WHERE published_at IS NULL AND topic = 'tx.authorization.v1'), now()))"
        ).fetchone()[0]
        assert naive > created_at, "without the open-transaction bound the row would be passed"
        assert _advance() <= created_at
        writer.commit()
        assert _advance() <= created_at, "committed but unpublished: still held"
    finally:
        writer.close()


def test_other_topics_do_not_hold_the_authorization_watermark(conn: Any) -> None:
    _, created_at = _insert(conn, "k-case", topic="investigation.requested.v1")
    _require_no_older_open_transaction(conn, created_at)
    assert _advance() > created_at


def test_the_watermark_never_moves_backwards_or_into_the_future(conn: Any) -> None:
    psycopg = pytest.importorskip("psycopg")
    _advance()
    for statement in (
        "UPDATE app.outbox_delivery_watermark SET delivered_through = '2000-01-01'",
        "UPDATE app.outbox_delivery_watermark SET delivered_through = now() + interval '1 day'",
    ):
        with pytest.raises(psycopg.errors.RaiseException):
            conn.execute(statement)
    for statement in (
        "DELETE FROM app.outbox_delivery_watermark",
        "TRUNCATE app.outbox_delivery_watermark",
    ):
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute(statement)


def test_the_watermark_is_advanced_only_inside_a_transaction(conn: Any) -> None:
    with pytest.raises(outbox_watermark.WatermarkError):
        outbox_watermark.advance(conn)
