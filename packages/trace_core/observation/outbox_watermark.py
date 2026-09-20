"""Authorization outcomes' delivery watermark: how far their history is complete (ADR-0051 §5).

Authorization outcomes reach online state through the transactional outbox, not a writer session
(ADR-0049 §4; ADR-0051 §1). Their history is complete only through `delivered_through`: every
`tx.authorization.v1` outbox row created before it has a confirmed Kafka delivery. The relay marks
a row published only after the broker confirmed it, so the watermark is computed from those marks,
never from how many records Kafka holds. A duplicated delivery therefore changes nothing here.

`advance` runs inside the relay's transaction, after the rows it confirmed are marked:
1. **This transaction's start** (`now()`) caps the candidate.
2. **The start of the oldest other open `trace_app` transaction**, from `pg_stat_activity`. It is
   read BEFORE the rows, which fixes this transaction's activity snapshot first. Any transaction
   that could still commit an outbox row has then either been counted here, committed and become
   visible to the row read below, or begun after this transaction did, past the cap.
3. **The oldest unpublished row's `created_at`** for the topic, a refused row included: an
   unresolved row holds the watermark where it is.

The stored watermark moves to the least of these, and only forward.

Two facts make this sound, and a change to either needs this reconsidered:
- migration 0007's trigger stamps `created_at` as `now()`, the inserting transaction's start;
- only `trace_app` may insert outbox rows, and the open transactions counted are `trace_app`'s.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Final

from psycopg import pq

from trace_core.contracts.topics import TX_AUTHORIZATION_V1
from trace_core.domain.errors import TraceXError

TOPIC: Final = TX_AUTHORIZATION_V1

_OPEN_SINCE: Final = """
    SELECT min(xact_start) FROM pg_stat_activity
    WHERE usename = current_user
      AND datname = current_database()
      AND pid <> pg_backend_pid()
      AND xact_start IS NOT NULL
"""
_OLDEST_UNPUBLISHED: Final = (
    "SELECT min(created_at) FROM app.outbox WHERE topic = %s AND published_at IS NULL"
)
_ADVANCE: Final = """
    INSERT INTO app.outbox_delivery_watermark (topic, delivered_through) VALUES (%s, %s)
    ON CONFLICT (topic) DO UPDATE SET delivered_through = GREATEST(
        app.outbox_delivery_watermark.delivered_through, EXCLUDED.delivered_through
    )
    RETURNING delivered_through
"""


class WatermarkError(TraceXError):
    """The delivery watermark cannot be computed soundly as asked."""


def advance(conn: Any, *, topic: str = TOPIC) -> dt.datetime:
    """Move the topic's watermark forward to what the marks now vouch for; return it.

    Must run inside an open transaction: the three reads share its start and its activity snapshot.
    """
    started = conn.execute("SELECT now()").fetchone()[0]
    if conn.info.transaction_status is not pq.TransactionStatus.INTRANS:
        raise WatermarkError(
            "the delivery watermark is advanced inside the relay's transaction, never in autocommit"
        )
    open_since = conn.execute(_OPEN_SINCE).fetchone()[0]
    oldest_unpublished = conn.execute(_OLDEST_UNPUBLISHED, (topic,)).fetchone()[0]
    candidate = min(t for t in (started, open_since, oldest_unpublished) if t is not None)
    return conn.execute(_ADVANCE, (topic, candidate)).fetchone()[0]  # type: ignore[no-any-return]


def read(conn: Any, *, topic: str = TOPIC) -> dt.datetime | None:
    """The topic's watermark, or None if the relay has never advanced it."""
    row = conn.execute(
        "SELECT delivered_through FROM app.outbox_delivery_watermark WHERE topic = %s", (topic,)
    ).fetchone()
    return None if row is None else row[0]


__all__ = ["TOPIC", "WatermarkError", "advance", "read"]
