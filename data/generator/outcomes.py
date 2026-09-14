"""Authorization outcome events (ADR-0049): the delay model DM-1 and the `tx.authorization.v1` row.

One builder serves both producers of these rows:
- the eval-v2 generator, when it emits the stream;
- any tool that derives outcomes for a dataset without the stream (ADR-0049 §7).

A derived row and an emitted row therefore cannot differ in shape, so no shape difference can become
a label proxy.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from typing import Any, Final

from data.generator.rng import derive
from trace_core.domain.identifiers import uuid7

EVENT_TYPE: Final = "tx.authorization"
SCHEMA_VERSION: Final = 1
OUTCOMES: Final = frozenset({"APPROVED", "DECLINED"})

DM1_NAMESPACE: Final = "authorization-latency"
DM1_FLOOR_MS: Final = 40
DM1_MEAN_MS: Final = 300.0
DM1_SD_MS: Final = 150.0
_ENVELOPE_NAMESPACE: Final = "authorization-envelope"


def decided_ms(seed: int, transaction_id: str, transaction_occurred_ms: int) -> int:
    """DM-1: when the outcome was decided. A pure function of the seed and the transaction id.

    One distribution for every transaction, whatever its outcome, label or scenario."""
    draw = derive(seed, DM1_NAMESPACE, transaction_id).gauss(DM1_MEAN_MS, DM1_SD_MS)
    return transaction_occurred_ms + DM1_FLOOR_MS + int(abs(draw))


def iso_millis(millis: int) -> str:
    """Event time at millisecond precision, always with the fraction (N11)."""
    moment = dt.datetime.fromtimestamp(millis // 1000, tz=dt.UTC)
    return f"{moment:%Y-%m-%dT%H:%M:%S}.{millis % 1000:03d}Z"


def idempotency_key(
    transaction_id: str,
    account_id: str,
    authorization_outcome: str,
    decided: int,
    transaction_occurred_ms: int,
) -> str:
    """`sha256:` over the outcome's semantic content (ADR-0049 §2)."""
    canonical = json.dumps(
        {
            "account_id": account_id,
            "authorization_outcome": authorization_outcome,
            "decided_ms": decided,
            "transaction_id": transaction_id,
            "transaction_occurred_ms": transaction_occurred_ms,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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
    """The `tx.authorization.v1` event for one transaction's outcome.

    The transaction and its outcome are one business flow, so they share `trace_id` and
    `correlation_id`. The envelope substream is keyed by the transaction id, independent of
    generation order."""
    if authorization_outcome not in OUTCOMES:
        raise ValueError(
            f"authorization outcome {authorization_outcome!r} is not an observation; only "
            f"{sorted(OUTCOMES)} are emitted (ADR-0049 §2)"
        )
    decided = decided_ms(seed, transaction_id, transaction_occurred_ms)
    rng = derive(seed, _ENVELOPE_NAMESPACE, transaction_id)
    ingested = decided + int(abs(rng.gauss(70, 40))) + 5
    return {
        "envelope": {
            "event_id": str(uuid7(millis=decided, rng=rng)),
            "event_type": EVENT_TYPE,
            "schema_version": SCHEMA_VERSION,
            "occurred_at": iso_millis(decided),
            "ingested_at": iso_millis(ingested),
            "producer": producer,
            "trace_id": trace_id,
            "correlation_id": correlation_id,
            "idempotency_key": idempotency_key(
                transaction_id,
                account_id,
                authorization_outcome,
                decided,
                transaction_occurred_ms,
            ),
        },
        "payload": {
            "transaction_id": transaction_id,
            "account_id": account_id,
            "authorization_outcome": authorization_outcome,
            "transaction_occurred_at": transaction_occurred_at,
        },
    }
