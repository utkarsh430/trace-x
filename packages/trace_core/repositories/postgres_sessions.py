"""The observation log's producer sessions and writer lock, as `trace_app`.

ADR-0051, migration 0006.

Both run on ONE dedicated connection, opened with `autocommit=True` and held for the process's
lifetime: the advisory lock belongs to that connection's backend, so it is released exactly when the
connection ends. The ledger's three writes are none of them per transaction -- open once, heartbeat
every few seconds, close once -- and the table's trigger stamps every time from the database clock.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from trace_core.domain.errors import TraceXError

if TYPE_CHECKING:  # pragma: no cover - typing only
    from psycopg import Connection


class SessionLedgerError(TraceXError):
    """The session ledger returned nothing where a row was required."""


@dataclass(frozen=True, slots=True)
class SessionRow:
    session_id: str
    producer: str
    instance_id: str
    started_at: dt.datetime
    heartbeat_at: dt.datetime
    closed_at: dt.datetime | None
    last_seq: int | None


def _row(values: Any) -> SessionRow:
    if values is None:
        raise SessionLedgerError("the session ledger returned no row")
    session_id, producer, instance_id, started_at, heartbeat_at, closed_at, last_seq = values
    return SessionRow(
        session_id=str(session_id),
        producer=str(producer),
        instance_id=str(instance_id),
        started_at=started_at,
        heartbeat_at=heartbeat_at,
        closed_at=closed_at,
        last_seq=None if last_seq is None else int(last_seq),
    )


class PostgresSessionLedger:
    """`app.producer_sessions` over the writer's dedicated connection."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._conn = connection

    def open(self, *, session_id: str, producer: str, instance_id: str) -> SessionRow:
        return _row(
            self._conn.execute(
                "INSERT INTO app.producer_sessions (session_id, producer, instance_id) "
                "VALUES (%s, %s, %s) RETURNING session_id, producer, instance_id, started_at, "
                "heartbeat_at, closed_at, last_seq",
                (session_id, producer, instance_id),
            ).fetchone()
        )

    def heartbeat(self, session_id: str) -> bool:
        """True while the session is open; False if it is closed or missing."""
        cursor = self._conn.execute(
            "UPDATE app.producer_sessions SET heartbeat_at = now() "
            "WHERE session_id = %s AND closed_at IS NULL",
            (session_id,),
        )
        return cursor.rowcount == 1

    def close(self, session_id: str, *, last_seq: int) -> bool:
        """Close once, recording the highest sequence number assigned. False if already closed."""
        cursor = self._conn.execute(
            "UPDATE app.producer_sessions SET closed_at = now(), last_seq = %s "
            "WHERE session_id = %s AND closed_at IS NULL",
            (last_seq, session_id),
        )
        return cursor.rowcount == 1

    def get(self, session_id: str) -> SessionRow | None:
        values = self._conn.execute(
            "SELECT session_id, producer, instance_id, started_at, heartbeat_at, closed_at, "
            "last_seq FROM app.producer_sessions WHERE session_id = %s",
            (session_id,),
        ).fetchone()
        return None if values is None else _row(values)


class PostgresWriterLock:
    """The writer's session-scoped advisory lock, held by the dedicated connection's backend."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._conn = connection

    def try_acquire(self, key: int) -> bool:
        row = self._conn.execute("SELECT pg_try_advisory_lock(%s)", (key,)).fetchone()
        return bool(row and row[0])

    def still_held(self, key: int) -> bool:
        """Whether this backend still holds `key`. A bigint key is stored as two 32-bit halves."""
        row = self._conn.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted "
            "AND pid = pg_backend_pid() AND classid = %s::oid AND objid = %s::oid "
            "AND objsubid = 1)",
            (key >> 32, key & 0xFFFFFFFF),
        ).fetchone()
        return bool(row and row[0])


__all__ = ["PostgresSessionLedger", "PostgresWriterLock", "SessionLedgerError", "SessionRow"]
