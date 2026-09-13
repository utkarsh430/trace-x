"""Assembling a `RiskDecision` — the one place a score becomes an answer.

Everything needed to re-derive the decision by hand travels with it: the rules
that fired, the values their predicates read, the digests of the rule pack and
the threshold configuration, the feature-set version, and the features that were
absent and why. `docs/ARCHITECTURE.md` §7 calls the rules tier "full
explainability"; this module is where that becomes a returned object rather than
a property of a log line someone has to go and find.

**Phase 2 never returns `DECLINE`.** The hot path is advisory — the authorization
system owns the decline, and the ROADMAP's Phase 2 scope is scoring and triage.
`FLAG` means "approved, and an investigation was opened". The value exists in the
contract because adding it later would be a breaking change (`docs/API_CONTRACTS.md` §2).
"""

from __future__ import annotations

import datetime as dt
from typing import Final

from trace_core.contracts.api.decision import RiskDecision, RuleReason
from trace_core.domain.enums import FeatureSource, RiskBand
from trace_core.features.spec import FEATURE_SET_VERSION, FeatureState, FeatureValue
from trace_core.rules.engine import Evaluation
from trace_core.scoring.banding import ThresholdConfig, band

APPROVE: Final = "APPROVE"
FLAG: Final = "FLAG"


def build_decision(
    *,
    transaction_id: str,
    evaluation: Evaluation,
    features: dict[str, FeatureValue],
    config: ThresholdConfig,
    scored_at: dt.datetime,
    latency_ms: float,
    case_id: str | None = None,
    degraded: bool = False,
    degraded_reasons: tuple[str, ...] = (),
    feature_source: FeatureSource = FeatureSource.ONLINE_ONLY,
) -> RiskDecision:
    """Turn an evaluation into the response the caller receives."""
    score, risk_band = band(evaluation, config)
    return RiskDecision(
        transaction_id=transaction_id,
        decision=FLAG if config.opens_investigation(risk_band) else APPROVE,
        risk_band=risk_band,
        score=score,
        reasons=[_reason(outcome) for outcome in evaluation.fired],
        feature_source=feature_source,
        degraded=degraded,
        degraded_reasons=list(degraded_reasons),
        unavailable_features=_absent(features, FeatureState.UNAVAILABLE),
        insufficient_history_features=_absent(features, FeatureState.INSUFFICIENT_HISTORY),
        rule_pack_id=evaluation.pack_id,
        rule_pack_digest=evaluation.pack_digest,
        threshold_config_digest=config.digest,
        feature_set_version=FEATURE_SET_VERSION,
        case_id=case_id,
        scored_at=scored_at,
        latency_ms=latency_ms,
    )


def _reason(outcome: object) -> RuleReason:
    from trace_core.rules.engine import RuleOutcome

    assert isinstance(outcome, RuleOutcome)
    return RuleReason(
        rule_id=outcome.rule_id,
        rule_version=outcome.rule_version,
        description=outcome.description,
        weight=outcome.weight,
        band_floor=outcome.band_floor,
        features_read=dict(outcome.features_read),
    )


def _absent(features: dict[str, FeatureValue], state: FeatureState) -> list[str]:
    """Feature ids in a given absent state.

    Reported rather than omitted: a decision made with a third of its inputs
    missing is a different fact from one made with all of them, and the two are
    indistinguishable to a caller that is only told the score (ADR-0022).
    """
    return sorted(fid for fid, value in features.items() if value.state is state)


def triage_band(config: ThresholdConfig) -> RiskBand:
    return config.triage_at
