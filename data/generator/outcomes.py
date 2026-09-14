"""Authorization outcome events for generated and replayed data (ADR-0049 §7).

- **DM-1**, the declared delay model, lives here. Generation and replay-side derivation date an
  outcome with it; nothing online does.
- **The event** is `trace_core.contracts.authorization`'s: the payload and the idempotency key the
  gateway produces too, so a generated, derived or delivered outcome cannot differ in shape or key.
  Only the event id and the processing time are drawn here, from a substream keyed by the
  transaction id, so a dataset is reproducible and independent of generation order.
"""

from __future__ import annotations

from typing import Any, Final

from data.generator.rng import derive
from trace_core.contracts import authorization
from trace_core.domain.identifiers import uuid7

EVENT_TYPE: Final = authorization.EVENT_TYPE
OUTCOMES: Final = authorization.OUTCOMES

DM1_NAMESPACE: Final = "authorization-latency"
DM1_FLOOR_MS: Final = 40
DM1_MEAN_MS: Final = 300.0
DM1_SD_MS: Final = 150.0
_ENVELOPE_NAMESPACE: Final = "authorization-envelope"

iso_millis = authorization.iso_millis


def decided_ms(seed: int, transaction_id: str, transaction_occurred_ms: int) -> int:
    """DM-1: when the outcome was decided. A pure function of the seed and the transaction id.

    One distribution for every transaction, whatever its outcome, label or scenario."""
    draw = derive(seed, DM1_NAMESPACE, transaction_id).gauss(DM1_MEAN_MS, DM1_SD_MS)
    return transaction_occurred_ms + DM1_FLOOR_MS + int(abs(draw))


def idempotency_key(
    transaction_id: str,
    account_id: str,
    authorization_outcome: str,
    decided: int,
    transaction_occurred_ms: int,
) -> str:
    """ADR-0049 §2's key (`trace_core.contracts.authorization.idempotency_key`)."""
    return authorization.idempotency_key(
        transaction_id=transaction_id,
        account_id=account_id,
        authorization_outcome=authorization_outcome,
        decided_ms=decided,
        transaction_occurred_ms=transaction_occurred_ms,
    )


def outcome_event(
    seed: int,
    *,
    transaction_id: str,
    account_id: str,
    authorization_outcome: str,
    transaction_occurred_at: str,
    transaction_occurred_ms: int,
    producer: str,
    trace_id: str,
    correlation_id: str,
) -> dict[str, Any]:
    """The `tx.authorization.v1` event for one transaction's outcome, dated by DM-1.

    The transaction and its outcome are one business flow, so they share `trace_id` and
    `correlation_id`."""
    if authorization_outcome not in OUTCOMES:
        raise ValueError(
            f"authorization outcome {authorization_outcome!r} is not an observation; only "
            f"{sorted(OUTCOMES)} are emitted (ADR-0049 §2)"
        )
    decided = decided_ms(seed, transaction_id, transaction_occurred_ms)
    rng = derive(seed, _ENVELOPE_NAMESPACE, transaction_id)
    ingested = decided + int(abs(rng.gauss(70, 40))) + 5
    return authorization.build_event(
        transaction_id=transaction_id,
        account_id=account_id,
        authorization_outcome=authorization_outcome,
        decided_ms=decided,
        transaction_occurred_ms=transaction_occurred_ms,
        transaction_occurred_at=transaction_occurred_at,
        producer=producer,
        trace_id=trace_id,
        correlation_id=correlation_id,
        ingested_ms=ingested,
        event_id=uuid7(millis=decided, rng=rng),
    )
