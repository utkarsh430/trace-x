"""The `tx.scored.v1` event: one scored transaction as the gateway served it (ADR-0051 §6).

What it carries is PHASE3_PLAN §3 Q1:
- **the transaction as served**, with `field_coverage`: an optional field is present exactly when
  the request supplied it;
- **the decision summary**: decision, band, score, degraded reasons, the rules that fired, and the
  rule pack, threshold and feature-set versions;
- **every feature the decision read**, as an extensible collection. Each entry has the feature id,
  its state, its value when available, the approximation flag and the source. It also has the
  fields an UNAVAILABLE feature lacked, and whether the store could vouch for the feature's
  lookback. That last one tells a genuinely new entity from a store that could not vouch;
- **what the online store did with the transaction**: the observe outcome, the store position and
  the epoch the position was counted in.

Never labels, causal evidence keys or any evaluation-only data. The producer session and sequence
number travel as headers, never in the payload, so no content depends on which session carried it.
"""

from __future__ import annotations

import datetime as dt
import math
from collections.abc import Mapping
from typing import Any, Final

from trace_core.contracts.api.decision import RiskDecision, RuleReason
from trace_core.contracts.canonical import CanonicalTransaction
from trace_core.contracts.envelope import build_event
from trace_core.domain.errors import ContractError
from trace_core.domain.time import ProcessingTime, event_time, from_millis
from trace_core.features import FeatureContext, FeatureState, FeatureValue
from trace_core.features.context import Completeness
from trace_core.features.state_plan import PLAN

EVENT_TYPE: Final = "tx.scored"
NOT_APPLICABLE: Final = "NOT_APPLICABLE"
"""The lookback completeness of a feature that looks back over nothing."""

TRANSACTION_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "transaction_id",
        "account_id",
        "card_id",
        "device_id",
        "merchant_id",
        "ip_id",
        "amount_minor",
        "currency",
        "channel",
        "entry_mode",
        "merchant_mcc",
        "merchant_country",
        "merchant_name",
        "latitude",
        "longitude",
        "user_agent",
        "memo",
        "authorization_outcome",
    }
)
"""The transaction's own fields. Its event time is the envelope's `occurred_at`, and its
processing time the envelope's `ingested_at`."""


def _iso(moment: dt.datetime) -> str:
    return moment.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


def _iso_millis(millis: int) -> str:
    return f"{from_millis(millis):%Y-%m-%dT%H:%M:%S}.{millis % 1000:03d}Z"


def transaction_fields(canonical: CanonicalTransaction) -> dict[str, Any]:
    """The transaction as served: each optional field present exactly when the request supplied it.

    Refuses a transaction whose coverage and values disagree, because a reader relies on the
    payload and `field_coverage` telling the same story.
    """
    fields = canonical.model_dump(mode="json", include=set(TRANSACTION_FIELDS), exclude_none=True)
    listed = {field.value for field in canonical.field_coverage} & TRANSACTION_FIELDS
    if listed != set(fields):
        raise ContractError(
            f"{canonical.transaction_id}: field_coverage lists {sorted(listed - set(fields))} "
            f"without values and omits {sorted(set(fields) - listed)} that have them"
        )
    return fields


def _fired(reason: RuleReason) -> dict[str, Any]:
    fired: dict[str, Any] = {
        "rule_id": reason.rule_id,
        "rule_version": reason.rule_version,
        "weight": reason.weight,
    }
    if reason.band_floor is not None:
        fired["band_floor"] = reason.band_floor.value
    return fired


def decision_summary(decision: RiskDecision) -> dict[str, Any]:
    """The decision as returned, without its explanations' prose or its latency."""
    summary: dict[str, Any] = {
        "decision": decision.decision,
        "risk_band": decision.risk_band.value,
        "score": decision.score,
        "degraded": decision.degraded,
        "degraded_reasons": list(decision.degraded_reasons),
        "fired_rules": [_fired(reason) for reason in decision.reasons],
        "rule_pack_id": decision.rule_pack_id,
        "rule_pack_digest": decision.rule_pack_digest,
        "threshold_config_digest": decision.threshold_config_digest,
        "feature_set_version": decision.feature_set_version,
        "feature_source": decision.feature_source.value,
        "scored_at": _iso(decision.scored_at),
    }
    if decision.case_id is not None:
        summary["case_id"] = decision.case_id
    return summary


def lookback_completeness(feature_id: str, context: FeatureContext | None) -> str:
    """Whether the served context could vouch for the feature's whole lookback.

    Per feature: a window the store no longer holds is unvouched for the features reading it
    alone (ADR-0046 §5)."""
    if not PLAN.lookback_s.get(feature_id, 0):
        return NOT_APPLICABLE
    if context is None:
        return Completeness.UNKNOWN.value
    completeness = PLAN.completeness(feature_id, context)
    assert completeness is not None
    return completeness.value


def served_features(
    features: Mapping[str, FeatureValue], context: FeatureContext | None
) -> list[dict[str, Any]]:
    """One entry per feature the decision read, in feature-id order."""
    entries: list[dict[str, Any]] = []
    for feature_id in sorted(features):
        feature = features[feature_id]
        entry: dict[str, Any] = {"feature_id": feature_id, "state": feature.state.value}
        if feature.state is FeatureState.AVAILABLE:
            value = feature.value
            if not math.isfinite(value):
                raise ContractError(
                    f"{feature_id} served a non-finite value, which JSON cannot carry and a "
                    f"decision should never have read"
                )
            entry["value"] = value
        entry["approximate"] = feature.approximate
        entry["source"] = feature.source.value
        entry["missing_fields"] = sorted(field.value for field in feature.missing_fields)
        # A feature the bounded score-time read withheld was not vouched for, whatever the store's
        # age (ADR-0046 §8); tx.scored.v1's completeness enum already holds INCOMPLETE.
        entry["lookback_completeness"] = (
            Completeness.INCOMPLETE.value
            if feature.depth_capped or feature.lifetime_unobserved
            else lookback_completeness(feature_id, context)
        )
        entries.append(entry)
    return entries


def build_scored_event(
    *,
    canonical: CanonicalTransaction,
    decision: RiskDecision,
    features: Mapping[str, FeatureValue],
    context: FeatureContext | None,
    observe_outcome: str,
    store_position: int | None,
    store_epoch_ms: int | None,
    producer: str,
    trace_id: str,
) -> dict[str, Any]:
    """A complete `{envelope, payload}` scored event. The publisher validates it on publish."""
    if decision.transaction_id != canonical.transaction_id:
        raise ContractError(
            f"a decision for {decision.transaction_id} cannot describe {canonical.transaction_id}"
        )
    payload: dict[str, Any] = {
        **transaction_fields(canonical),
        "field_coverage": sorted(field.value for field in canonical.field_coverage),
        "decision_summary": decision_summary(decision),
        "served_features": served_features(features, context),
        "observe_outcome": observe_outcome,
        "store_position": store_position,
        "store_epoch": None if store_epoch_ms is None else _iso_millis(store_epoch_ms),
    }
    return build_event(
        event_type=EVENT_TYPE,
        occurred_at=event_time(canonical.occurred_at),
        payload=payload,
        producer=producer,
        trace_id=trace_id,
        correlation_id=canonical.transaction_id,
        ingested_at=ProcessingTime(canonical.ingested_at),
    )


__all__ = [
    "EVENT_TYPE",
    "NOT_APPLICABLE",
    "TRANSACTION_FIELDS",
    "build_scored_event",
    "decision_summary",
    "lookback_completeness",
    "served_features",
    "transaction_fields",
]
