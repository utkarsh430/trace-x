"""The durable ledger of online-store holes (ADR-0046 §5), as `trace_app`.

One row per hole episode, never one per transaction: a row is written when the first observation of
an outage goes unrecorded, and cleared once the store's completeness has been withdrawn past it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from trace_core.features.completeness import HoleReason

if TYPE_CHECKING:  # pragma: no cover - typing only
    from psycopg_pool import ConnectionPool


class PostgresHoleLedger:
    """`app.feature_store_holes`."""

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def record_hole(self, *, reason: HoleReason, instance_id: str) -> None:
        with self._pool.connection() as conn:
            conn.execute(
                "INSERT INTO app.feature_store_holes (reason, instance_id) VALUES (%s, %s)",
                (reason.value, instance_id),
            )

    def open_holes(self) -> int:
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT count(*) FROM app.feature_store_holes WHERE cleared_at IS NULL"
            ).fetchone()
        return int(row[0]) if row is not None else 0

    def latest_open_hole(self) -> int | None:
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT max(hole_id) FROM app.feature_store_holes WHERE cleared_at IS NULL"
            ).fetchone()
        return None if row is None or row[0] is None else int(row[0])

    def clear_holes(self, *, through_hole_id: int) -> int:
        """Clear the open holes up to `through_hole_id`, and no later one: a hole recorded after
        the withdrawal began was not withdrawn by it."""
        with self._pool.connection() as conn:
            cursor = conn.execute(
                "UPDATE app.feature_store_holes SET cleared_at = now() "
                "WHERE cleared_at IS NULL AND hole_id <= %s",
                (through_hole_id,),
            )
            return cursor.rowcount


__all__ = ["PostgresHoleLedger"]
