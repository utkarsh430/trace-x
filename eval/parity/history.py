"""Silver canonical rows as observations: the complete history the reference reads (ADR-0056 §2).

Mapped here from the released declarations, independently of Gold's SQL, so a defect in how Gold
derives its observations cannot be shared by the oracle it is compared with:
- a transaction is `features.observation.Event` on the TRANSACTION stream, identified by its id,
  without its own authorization outcome (ADR-0049 §3);
- an identity event is an observation only when the **gateway** produced it, and feeds the stream
  `semantics.identity_stream` names. Its identity is the id the online store records it under,
  which the gateway publishes as `correlation_id` (ADR-0055 §9);
- an authorization outcome is an observation only when APPROVED or DECLINED, dated when decided.

Rows carry event time as epoch microseconds (`unix_micros`), never as a session-local timestamp.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

from trace_core.domain.enums import AuthorizationOutcome, TransactionChannel
from trace_core.domain.time import EventTime, from_millis
from trace_core.features.observation import Event, authorization_observation
from trace_core.features.semantics import Stream, identity_stream

OCCURRED_US: Final = "occurred_us"
TRANSACTION_COLUMNS: Final = (
    "transaction_id",
    "account_id",
    "currency",
    "amount_minor",
    "card_id",
    "device_id",
    "merchant_id",
    "ip_id",
    "merchant_mcc",
    "merchant_country",
    "latitude",
    "longitude",
    "channel",
)
IDENTITY_COLUMNS: Final = (
    "correlation_id",
    "account_id",
    "identity_event_type",
    "device_id",
    "ip_id",
    "producer",
)
GATEWAY_PRODUCER: Final = "trace-gateway"
"""Only the gateway's identity events are observations: the store never saw a directly produced
one, and one republished by the gateway would otherwise count twice (ADR-0055 as amended)."""
OUTCOME_COLUMNS: Final = ("transaction_id", "account_id", "authorization_outcome")
_OBSERVED: Final = frozenset(
    {AuthorizationOutcome.APPROVED.value, AuthorizationOutcome.DECLINED.value}
)


def _at(row: Mapping[str, Any]) -> EventTime:
    micros = int(row[OCCURRED_US])
    return EventTime(from_millis(micros // 1000))


def transaction_event(row: Mapping[str, Any]) -> Event:
    channel = row.get("channel")
    return Event(
        stream=Stream.TRANSACTION,
        occurred_at=_at(row),
        account_id=str(row["account_id"]),
        event_id=str(row["transaction_id"]),
        currency=str(row["currency"]),
        amount_minor=int(row["amount_minor"]),
        card_id=row.get("card_id"),
        device_id=row.get("device_id"),
        merchant_id=row.get("merchant_id"),
        ip_id=row.get("ip_id"),
        merchant_mcc=row.get("merchant_mcc"),
        merchant_country=row.get("merchant_country"),
        latitude=row.get("latitude"),
        longitude=row.get("longitude"),
        channel=None if channel is None else TransactionChannel(str(channel)),
        authorization_outcome=None,
    )


def identity_event(row: Mapping[str, Any]) -> Event | None:
    stream = identity_stream(str(row["identity_event_type"]))
    producer = str(row.get("producer") or "").split("@", 1)[0]
    if stream is None or producer != GATEWAY_PRODUCER:
        return None
    return Event(
        stream=stream,
        occurred_at=_at(row),
        account_id=str(row["account_id"]),
        event_id=str(row["correlation_id"]),
        device_id=row.get("device_id"),
        ip_id=row.get("ip_id"),
    )


def outcome_event(row: Mapping[str, Any]) -> Event | None:
    outcome = str(row["authorization_outcome"])
    if outcome not in _OBSERVED:
        return None
    return authorization_observation(
        transaction_id=str(row["transaction_id"]),
        account_id=str(row["account_id"]),
        authorization_outcome=AuthorizationOutcome(outcome),
        decided_at=_at(row),
    )
