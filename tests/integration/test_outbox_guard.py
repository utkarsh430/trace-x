"""The outbox's immutability and the relay's grants, against a real PostgreSQL (migration 0007).

Database guarantees, so they are proved against the database and never mocked:
- a row is inserted unpublished, with no attempts and no error, whatever `trace_app` sends;
- what a row relays -- topic, key, idempotency key, payload, creation time -- never changes;
- `published_at` is stamped once from the database clock and is never cleared or moved;
- `attempts` never decreases;
- `trace_app` may update only the relay's three columns, and may not delete or truncate;
- `SELECT ... FOR UPDATE SKIP LOCKED` works under that column grant and hands a row to one claimer.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from typing import Any

import pytest

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
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(
        _dsn("POSTGRES_SUPERUSER", "POSTGRES_SUPERUSER_PASSWORD"), autocommit=True
    ) as owner:
        owner.execute("TRUNCATE app.outbox")


def _app_connection() -> Any:
    psycopg = pytest.importorskip("psycopg")
    return psycopg.connect(_dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD"), autocommit=True)


@pytest.fixture
def conn() -> Iterator[Any]:
    try:
        probe = _app_connection()
        guarded = probe.execute(
            "SELECT 1 FROM pg_trigger WHERE tgname = 'outbox_guard' AND NOT tgisinternal"
        ).fetchone()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no PostgreSQL reachable as trace_app ({exc}). Run "
            f"`make up && make migrate`; these are database guarantees and are never mocked."
        )
    if guarded is None:  # pragma: no cover - environment dependent
        probe.close()
        pytest.skip("SKIPPED (NOT PASSED): migration 0007 is not applied. Run `make migrate`.")
    _truncate()
    yield probe
    probe.close()
    _truncate()


def _insert(conn: Any, key: str = "k-1", **columns: Any) -> int:
    names = ["topic", "partition_key", "idempotency_key", "payload", *columns]
    values = [
        "investigation.requested.v1",
        "case_1",
        key,
        json.dumps({"envelope": {}, "payload": {}}),
        *columns.values(),
    ]
    placeholders = ", ".join(["%s"] * len(values))
    row = conn.execute(
        f"INSERT INTO app.outbox ({', '.join(names)}) VALUES ({placeholders}) RETURNING outbox_id",
        values,
    ).fetchone()
    assert row is not None
    return int(row[0])


def _row(conn: Any, outbox_id: int) -> tuple[Any, ...]:
    row = conn.execute(
        "SELECT published_at, attempts, last_error, created_at FROM app.outbox "
        "WHERE outbox_id = %s",
        (outbox_id,),
    ).fetchone()
    assert row is not None
    return tuple(row)


def test_a_row_is_inserted_unpublished_whatever_the_writer_sends(conn: Any) -> None:
    outbox_id = _insert(
        conn,
        published_at="2000-01-01T00:00:00Z",
        attempts=7,
        last_error="made up",
        created_at="2000-01-01T00:00:00Z",
    )
    published_at, attempts, last_error, created_at = _row(conn, outbox_id)
    assert (published_at, attempts, last_error) == (None, 0, None)
    assert created_at.year > 2000, "the trigger stamps now()"


def test_what_a_row_relays_never_changes(conn: Any) -> None:
    psycopg = pytest.importorskip("psycopg")
    outbox_id = _insert(conn)
    for column, value in (
        ("topic", "tx.authorization.v1"),
        ("payload", json.dumps({"envelope": {"x": 1}, "payload": {}})),
    ):
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute(
                f"UPDATE app.outbox SET {column} = %s WHERE outbox_id = %s",
                (value, outbox_id),
            )


def test_publication_is_stamped_once_by_the_database_and_never_undone(conn: Any) -> None:
    psycopg = pytest.importorskip("psycopg")
    outbox_id = _insert(conn)
    conn.execute(
        "UPDATE app.outbox SET published_at = '2000-01-01' WHERE outbox_id = %s", (outbox_id,)
    )
    published_at, *_ = _row(conn, outbox_id)
    assert published_at is not None and published_at.year > 2000, "stamped from now()"
    for statement in (
        "UPDATE app.outbox SET published_at = NULL WHERE outbox_id = %s",
        "UPDATE app.outbox SET published_at = now() + interval '1 day' WHERE outbox_id = %s",
    ):
        with pytest.raises(psycopg.errors.RaiseException):
            conn.execute(statement, (outbox_id,))


def test_attempts_never_decrease(conn: Any) -> None:
    psycopg = pytest.importorskip("psycopg")
    outbox_id = _insert(conn)
    conn.execute(
        "UPDATE app.outbox SET attempts = attempts + 2, last_error = 'x' WHERE outbox_id = %s",
        (outbox_id,),
    )
    with pytest.raises(psycopg.errors.RaiseException):
        conn.execute("UPDATE app.outbox SET attempts = 0 WHERE outbox_id = %s", (outbox_id,))
    assert _row(conn, outbox_id)[1:3] == (2, "x")


def test_the_application_role_cannot_delete_or_truncate_the_outbox(conn: Any) -> None:
    psycopg = pytest.importorskip("psycopg")
    outbox_id = _insert(conn)
    for statement, params in (
        ("DELETE FROM app.outbox WHERE outbox_id = %s", (outbox_id,)),
        ("TRUNCATE app.outbox", None),
    ):
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute(statement, params)


def test_skip_locked_claims_work_under_the_column_grant_and_never_share_a_row(conn: Any) -> None:
    for i in range(4):
        _insert(conn, key=f"k-{i}")
    claim = (
        "SELECT outbox_id FROM app.outbox WHERE published_at IS NULL "
        "ORDER BY created_at, outbox_id LIMIT 2 FOR UPDATE SKIP LOCKED"
    )
    first, second = _app_connection(), _app_connection()
    try:
        with first.transaction(), second.transaction():
            mine = {row[0] for row in first.execute(claim).fetchall()}
            theirs = {row[0] for row in second.execute(claim).fetchall()}
            assert len(mine) == len(theirs) == 2
            assert not mine & theirs
    finally:
        first.close()
        second.close()
