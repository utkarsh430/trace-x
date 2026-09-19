"""The `tx.authorization.v1` event: one authorization outcome for one transaction (ADR-0049 §2).

Two producers build this event: the eval-v2 generator, and the gateway's
`POST /v1/events/authorization` through its transactional outbox. They must mean the same thing
by the same event, so the payload and the idempotency key are defined once, here, and the
generator imports them rather than restating them.

- **Idempotency key.** `sha256:` over the canonical JSON of the outcome's semantic content: the
  transaction id, the account, the outcome, and the outcome time and transaction time in epoch
  milliseconds. Two deliveries of one outcome carry one key, whoever produced them.
- **Times.** The outcome's timestamps are rendered with exactly three fractional digits, so a
  string's length never depends on its value (N11). `transaction_occurred_at` is copied from the
  transaction verbatim.
- **Outcomes.** `APPROVED` and `DECLINED` only. `UNKNOWN` carries no observation, and
  `REVERSED` is a later event with other semantics.

Envelope shapes (the W3C trace id, the correlation id's length) are the released schema's to
enforce: every producer validates the finished event against it before publishing.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from typing import Any, Final
from uuid import UUID

from trace_core.contracts.envelope import SCHEMA_VERSION
from trace_core.contracts.topics import TX_AUTHORIZATION_V1
from trace_core.domain.errors import ContractError
from trace_core.domain.identifiers import uuid7

TOPIC: Final = TX_AUTHORIZATION_V1
EVENT_TYPE: Final = "tx.authorization"
OUTCOMES: Final = frozenset({"APPROVED", "DECLINED"})


def iso_millis(millis: int) -> str:
    """Event time at millisecond precision, always with the fraction (N11)."""
    moment = dt.datetime.fromtimestamp(millis // 1000, tz=dt.UTC)
    return f"{moment:%Y-%m-%dT%H:%M:%S}.{millis % 1000:03d}Z"


def idempotency_key(
    *,
    transaction_id: str,
    account_id: str,
    authorization_outcome: str,
    decided_ms: int,
    transaction_occurred_ms: int,
) -> str:
    """`sha256:` over the outcome's semantic content (ADR-0049 §2)."""
    canonical = json.dumps(
        {
            "account_id": account_id,
            "authorization_outcome": authorization_outcome,
            "decided_ms": decided_ms,
            "transaction_id": transaction_id,
            "transaction_occurred_ms": transaction_occurred_ms,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def payload(
    *,
    transaction_id: str,
    account_id: str,
    authorization_outcome: str,
    transaction_occurred_at: str,
) -> dict[str, Any]:
    """The released payload. `transaction_occurred_at` is copied from the transaction, verbatim."""
    if authorization_outcome not in OUTCOMES:
        raise ContractError(
            f"authorization outcome {authorization_outcome!r} is not an observation; only "
            f"{sorted(OUTCOMES)} are published (ADR-0049 §2)"
        )
    return {
        "transaction_id": transaction_id,
        "account_id": account_id,
        "authorization_outcome": authorization_outcome,
        "transaction_occurred_at": transaction_occurred_at,
    }


def build_event(
    *,
    transaction_id: str,
    account_id: str,
    authorization_outcome: str,
    decided_ms: int,
    transaction_occurred_ms: int,
    transaction_occurred_at: str,
    producer: str,
    trace_id: str,
    correlation_id: str,
    ingested_ms: int,
    event_id: UUID | None = None,
) -> dict[str, Any]:
    """A complete `{envelope, payload}` outcome event.

    `trace_id` and `correlation_id` are the transaction's: a transaction and its outcome are one
    business flow. An outcome decided before its transaction occurred is refused, because it cannot
    be an observation of that transaction."""
    if decided_ms < transaction_occurred_ms:
        raise ContractError(
            f"{transaction_id}: an outcome decided at {decided_ms} precedes its transaction at "
            f"{transaction_occurred_ms} (ADR-0049 §2)"
        )
    body = payload(
        transaction_id=transaction_id,
        account_id=account_id,
        authorization_outcome=authorization_outcome,
        transaction_occurred_at=transaction_occurred_at,
    )
    return {
        "envelope": {
            "event_id": str(event_id or uuid7(millis=decided_ms)),
            "event_type": EVENT_TYPE,
            "schema_version": SCHEMA_VERSION,
            "occurred_at": iso_millis(decided_ms),
            "ingested_at": iso_millis(ingested_ms),
            "producer": producer,
            "trace_id": trace_id,
            "correlation_id": correlation_id,
            "idempotency_key": idempotency_key(
                transaction_id=transaction_id,
                account_id=account_id,
                authorization_outcome=authorization_outcome,
                decided_ms=decided_ms,
                transaction_occurred_ms=transaction_occurred_ms,
            ),
        },
        "payload": body,
    }


__all__ = [
    "EVENT_TYPE",
    "OUTCOMES",
    "TOPIC",
    "build_event",
    "idempotency_key",
    "iso_millis",
    "payload",
]
