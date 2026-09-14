"""The system of record for authorization outcomes (ADR-0049 §4).

**The database decides identity, not a cache.** `transaction_id` is the primary key of
`app.authorization_outcomes`, so a second delivery for one transaction is settled by one
`INSERT ... ON CONFLICT DO NOTHING` and a read of the row that won: identical content is a
duplicate, anything else a conflict. The first delivery stays the observation, and `trace_app`
holds no UPDATE or DELETE grant with which a later one could overwrite it (migration 0005).

**The outcome and its event commit together.** The outbox row that relays the outcome to
`tx.authorization.v1` is written in the same transaction, as triage writes its case with its
`investigation.requested.v1` row (ADR-0007). A duplicate or a conflict writes no outbox row.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from trace_core.contracts.topics import TX_AUTHORIZATION_V1, partition_key
from trace_core.domain.errors import SchemaValidationError

if TYPE_CHECKING:  # pragma: no cover - typing only
    from psycopg_pool import ConnectionPool


class Delivery(StrEnum):
    """What one delivery of an outcome was."""

    RECORDED = "RECORDED"
    """The first delivery for the transaction, now the observation."""
    DUPLICATE = "DUPLICATE"
    """Identical to the recorded outcome. It contributes nothing."""
    CONFLICT = "CONFLICT"
    """Different from the recorded outcome. Not recorded; the first stays the observation."""


@dataclass(frozen=True, slots=True)
class AuthorizationOutcomeRecord:
    """One outcome, at the millisecond precision it is published with."""

    transaction_id: str
    account_id: str
    authorization_outcome: str
    decided_at: dt.datetime
    transaction_occurred_at: dt.datetime
    event_id: str

    def same_content(self, other: AuthorizationOutcomeRecord) -> bool:
        """The content ADR-0049 §2 compares: account, outcome, outcome time, transaction time."""
        return (
            self.account_id == other.account_id
            and self.authorization_outcome == other.authorization_outcome
            and self.decided_at == other.decided_at
            and self.transaction_occurred_at == other.transaction_occurred_at
        )


@dataclass(frozen=True, slots=True)
class DeliveryReceipt:
    """What the system of record did with one delivery."""

    delivery: Delivery
    recorded: AuthorizationOutcomeRecord
    """The observation: this delivery when RECORDED, the first delivery otherwise."""


def validate_event(event: dict[str, Any]) -> None:
    """The released schema, before anything is written: an invalid message is never published."""
    from trace_core.contracts.events import tx_authorization_v1

    try:
        tx_authorization_v1.TxAuthorizationV1.model_validate_json(json.dumps(event))
    except Exception as exc:
        raise SchemaValidationError(
            f"the authorization outcome event does not satisfy its released schema; an invalid "
            f"message is never published (EVENT_CONTRACTS.md §6.1): {exc}"
        ) from exc


def outbox_row(event: dict[str, Any]) -> tuple[str, str, str, str]:
    """`(topic, partition_key, idempotency_key, payload_json)`, with the key resolved and stored."""
    return (
        TX_AUTHORIZATION_V1,
        partition_key(TX_AUTHORIZATION_V1, event),
        str(event["envelope"]["idempotency_key"]),
        json.dumps(event, sort_keys=True, separators=(",", ":")),
    )


class PostgresAuthorizationStore:
    """Records outcome deliveries in `app.authorization_outcomes` (ADR-0049 §4)."""

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def record(self, outcome: AuthorizationOutcomeRecord, event: dict[str, Any]) -> DeliveryReceipt:
        """One delivery, and when it is the first, its outbox row: one transaction."""
        topic, key, idempotency, payload = outbox_row(event)
        with self._pool.connection() as conn, conn.transaction():
            inserted = conn.execute(
                """
                INSERT INTO app.authorization_outcomes (
                    transaction_id, account_id, authorization_outcome, decided_at,
                    transaction_occurred_at, event_id
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
                RETURNING transaction_id
                """,
                (
                    outcome.transaction_id,
                    outcome.account_id,
                    outcome.authorization_outcome,
                    outcome.decided_at,
                    outcome.transaction_occurred_at,
                    outcome.event_id,
                ),
            ).fetchone()
            if inserted is not None:
                conn.execute(
                    """
                    INSERT INTO app.outbox (topic, partition_key, idempotency_key, payload)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (topic, idempotency_key) DO NOTHING
                    """,
                    (topic, key, idempotency, payload),
                )
                return DeliveryReceipt(Delivery.RECORDED, outcome)
            # Another delivery won -- possibly in the microsecond before this one, which is why
            # there was no read first. Under READ COMMITTED this statement sees the committed row.
            first = _read(conn, outcome.transaction_id)
        if first is None:  # pragma: no cover - ON CONFLICT fired, so the row exists
            raise RuntimeError(f"{outcome.transaction_id}: a conflict with no recorded row")
        delivery = Delivery.DUPLICATE if first.same_content(outcome) else Delivery.CONFLICT
        return DeliveryReceipt(delivery, first)

    def recorded(self, transaction_id: str) -> AuthorizationOutcomeRecord | None:
        """The observation for a transaction, if one was delivered."""
        with self._pool.connection() as conn:
            return _read(conn, transaction_id)


def _read(conn: Any, transaction_id: str) -> AuthorizationOutcomeRecord | None:
    row = conn.execute(
        """
        SELECT account_id, authorization_outcome, decided_at, transaction_occurred_at, event_id
        FROM app.authorization_outcomes
        WHERE transaction_id = %s
        """,
        (transaction_id,),
    ).fetchone()
    if row is None:
        return None
    return AuthorizationOutcomeRecord(
        transaction_id=transaction_id,
        account_id=str(row[0]),
        authorization_outcome=str(row[1]),
        decided_at=row[2],
        transaction_occurred_at=row[3],
        event_id=str(row[4]),
    )
