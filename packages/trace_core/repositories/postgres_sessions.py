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
from typing import TYPE_CHECKING, Any, Final

from trace_core.domain.errors import TraceXError

if TYPE_CHECKING:  # pragma: no cover - typing only
    from psycopg import Connection

WRITER_CONNECT_TIMEOUT_S: Final = 2
"""libpq's connect timeout, in whole seconds (2 is its minimum)."""
WRITER_STATEMENT_TIMEOUT_MS: Final = 2_000
WRITER_TCP_USER_TIMEOUT_MS: Final = 6_000
"""How long unacknowledged data may wait before the kernel drops the connection, where supported."""


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

    def others_live(self, session_id: str, *, within_s: float) -> bool:
        """Whether another session is unclosed and was heartbeated within `within_s`.

        Compared on the database clock, which stamped the heartbeat. A writer taking over reads it
        before it writes: a predecessor that closed cleanly, or whose last heartbeat is older than
        its lease, can no longer be writing (ADR-0051 §2).
        """
        row = self._conn.execute(
            "SELECT EXISTS (SELECT 1 FROM app.producer_sessions WHERE session_id <> %s "
            "AND closed_at IS NULL AND heartbeat_at > now() - make_interval(secs => %s))",
            (session_id, within_s),
        ).fetchone()
        return bool(row and row[0])


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


def writer_connection(dsn: str) -> Connection[Any]:
    """The writer's dedicated connection: autocommit, and bounded however the database fails.

    The statement timeout bounds a slow server. TCP keepalives and `tcp_user_timeout` bound a silent
    network, where no statement timeout fires because nothing comes back. The supervisor's lease
    bounds whatever these do not (ADR-0051 §2).
    """
    import psycopg

    return psycopg.connect(
        dsn,
        autocommit=True,
        connect_timeout=WRITER_CONNECT_TIMEOUT_S,
        application_name="trace-gateway-writer",
        options=f"-c statement_timeout={WRITER_STATEMENT_TIMEOUT_MS}",
        keepalives=1,
        keepalives_idle=5,
        keepalives_interval=1,
        keepalives_count=3,
        tcp_user_timeout=WRITER_TCP_USER_TIMEOUT_MS,
    )


__all__ = [
    "PostgresSessionLedger",
    "PostgresWriterLock",
    "SessionLedgerError",
    "SessionRow",
    "writer_connection",
]
