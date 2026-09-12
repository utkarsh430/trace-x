"""Building the `investigation.requested.v1` event triage writes to the outbox.

Separate from the store because the store's job is the transaction and this
module's job is the contract. Keeping them apart means the event is built and
**validated against its released JSON Schema before the transaction opens** — a
message that fails validation must never be published, and finding that out
inside a transaction would mean rolling back a case for a reason that had
nothing to do with the database (docs/EVENT_CONTRACTS.md §6.1).

The payload carries rule *ids*, not the feature values each predicate read. Those
are in the synchronous response and in the case row; this topic is retained for
thirty days, and an event is a poor place for data that is already stored.
"""

from __future__ import annotations

import json
from typing import Any, Final

from trace_core.contracts.api.decision import RiskDecision
from trace_core.contracts.envelope import build_event
from trace_core.contracts.topics import INVESTIGATION_REQUESTED_V1, partition_key
from trace_core.domain.errors import SchemaValidationError
from trace_core.domain.time import EventTime
from trace_core.rules.engine import Evaluation

EVENT_TYPE: Final = "investigation.requested"
PRODUCER_NAME: Final = "trace-gateway"


def producer_string(version: str) -> str:
    """`service@semver`, the shape the released envelope schema requires."""
    return f"{PRODUCER_NAME}@{version}"


def build_investigation_requested(
    *,
    decision: RiskDecision,
    evaluation: Evaluation,
    case_id: str,
    account_id: str,
    occurred_at: EventTime,
    producer: str,
    trace_id: str,
    correlation_id: str,
) -> dict[str, Any]:
    """The complete event, validated against its released schema.

    Raises `SchemaValidationError` rather than returning something unpublishable:
    an invalid message is never published, and producers fail fast rather than
    poisoning consumers.
    """
    payload: dict[str, Any] = {
        "case_id": case_id,
        "transaction_id": decision.transaction_id,
        "account_id": account_id,
        "risk_band": decision.risk_band.value,
        "score": decision.score,
        "rule_pack_id": decision.rule_pack_id,
        "rule_pack_digest": decision.rule_pack_digest,
        "threshold_config_digest": decision.threshold_config_digest,
        "feature_set_version": decision.feature_set_version,
        "feature_source": decision.feature_source.value,
        "degraded": decision.degraded,
        "degraded_reasons": list(decision.degraded_reasons),
        "fired_rule_ids": [outcome.rule_id for outcome in evaluation.fired],
        "abstained_rule_count": len(evaluation.abstained),
    }
    event = build_event(
        event_type=EVENT_TYPE,
        occurred_at=occurred_at,
        payload=payload,
        producer=producer,
        trace_id=trace_id,
        correlation_id=correlation_id,
    )
    _validate(event)
    return event


def _validate(event: dict[str, Any]) -> None:
    from trace_core.contracts.events import investigation_requested_v1

    try:
        investigation_requested_v1.InvestigationRequestedV1.model_validate_json(json.dumps(event))
    except Exception as exc:
        raise SchemaValidationError(
            f"investigation.requested.v1 event does not satisfy its released schema; "
            f"an invalid message is never published (EVENT_CONTRACTS.md §6.1): {exc}"
        ) from exc


def outbox_row(event: dict[str, Any]) -> tuple[str, str, str, str]:
    """`(topic, partition_key, idempotency_key, payload_json)` for the outbox.

    The partition key is resolved HERE and stored, rather than re-derived by the
    Phase 3 relay: two derivations of the same key can disagree, and the symptom
    would be a silently repartitioned topic.
    """
    return (
        INVESTIGATION_REQUESTED_V1,
        partition_key(INVESTIGATION_REQUESTED_V1, event),
        str(event["envelope"]["idempotency_key"]),
        json.dumps(event, sort_keys=True, separators=(",", ":")),
    )
