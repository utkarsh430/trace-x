"""`RiskDecision` — what the gateway returns, and what makes it auditable.

A score on its own is not a product. `docs/ARCHITECTURE.md` §7 requires rules to
be *"deterministic, YAML-declared, versioned, individually testable. Zero
latency, full explainability"*, and this model is where explainability becomes a
contract rather than a hope:

* every `reason` names the rule that fired **and the feature values it read**,
  so a decision can be re-derived by hand;
* the `rule_pack_digest` and `threshold_config_digest` pin the exact behaviour
  that produced it -- rules hot-reload (ROADMAP Phase 2), so "which rules were
  live at 14:03?" must be answerable from the response, not from a deploy log;
* `unavailable_features` and `insufficient_history_features` are reported, not
  hidden. A decision made with half its inputs missing is a different fact from
  one made with all of them, and ADR-0022 exists because the tempting
  alternative -- a zero -- is indistinguishable from a real measurement.

**No ground truth appears here, in any form.** Not the label, not the pattern,
not a feature derived from either (CLAUDE.md §11). The application role cannot
read the `groundtruth` schema at all, so this is structural rather than a
convention -- but `tests/unit/test_decision_carries_no_ground_truth.py` asserts
the field names too, because a leak through a plausible-looking field name is
exactly what nobody would notice.
"""

from __future__ import annotations

from typing import Annotated, Final

from pydantic import Field

from trace_core.contracts.api.base import ApiModel, JsonDatetime
from trace_core.domain.enums import FeatureSource, RiskBand

SCORE_MIN: Final = 0.0
SCORE_MAX: Final = 1.0


class RuleReason(ApiModel):
    """One rule that fired, and the evidence it fired on."""

    rule_id: Annotated[str, Field(min_length=1, max_length=64)]
    rule_version: Annotated[str, Field(pattern=r"^\d+\.\d+\.\d+$")]
    description: Annotated[str, Field(min_length=1, max_length=256)]
    weight: Annotated[float, Field(ge=0.0, le=1.0)]
    band_floor: RiskBand | None = None
    """The minimum band this rule forces, if any. A rule may raise a decision's
    band regardless of the weighted score -- `IMPOSSIBLE_TRAVEL` is not a
    "somewhat risky" signal to be averaged away."""
    features_read: dict[str, float] = Field(default_factory=dict)
    """The feature values the predicate actually evaluated against.

    Carried so the decision can be re-derived by hand from the response alone.
    A rule id with no values is an assertion; a rule id with its inputs is
    evidence."""


class RiskDecision(ApiModel):
    """The synchronous scoring outcome for one transaction."""

    transaction_id: Annotated[str, Field(min_length=1, max_length=64)]
    decision: Annotated[str, Field(pattern=r"^(APPROVE|FLAG|DECLINE)$")]
    """APPROVE and FLAG both let the transaction through; FLAG additionally
    opened an investigation. Phase 2 never returns DECLINE: the hot path is
    advisory, and the authorization system owns the decline. The value exists in
    the contract because adding it later would be a breaking change."""
    risk_band: RiskBand
    score: Annotated[float, Field(ge=SCORE_MIN, le=SCORE_MAX)]

    reasons: list[RuleReason] = Field(default_factory=list)
    """Empty is a legitimate, meaningful answer: no rule fired."""

    feature_source: FeatureSource
    """`ONLINE_ONLY` until the warm path reconciles the store (Phase 3). Surfaced
    in the body as well as the `X-Feature-Source` header so it survives a client
    that logs only the payload."""
    degraded: bool
    """True when the decision was made with a dependency unavailable. Every
    fail-open decision is tagged and counted (CLAUDE.md §3.7)."""
    degraded_reasons: list[str] = Field(default_factory=list)

    unavailable_features: list[str] = Field(default_factory=list)
    """Features whose source does not supply their inputs at all (ADR-0022)."""
    insufficient_history_features: list[str] = Field(default_factory=list)
    """Features whose inputs exist but whose entity has too little history yet.

    Kept separate from `unavailable_features` deliberately. "This source has no
    latitude" and "this account is four hours old" are different facts with
    different remedies, and collapsing them would make the coverage report
    wrong."""

    rule_pack_id: Annotated[str, Field(min_length=1, max_length=64)]
    rule_pack_digest: Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
    threshold_config_digest: Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
    feature_set_version: Annotated[str, Field(pattern=r"^\d+\.\d+\.\d+$")]

    case_id: Annotated[str, Field(pattern=r"^case_[0-9a-f]{32}$")] | None = None
    """Present exactly when an investigation was opened (HIGH or CRITICAL)."""

    scored_at: JsonDatetime
    """Processing time: when the decision was made. Distinct from the
    transaction's `occurred_at`, and never substituted for it (ADR-0026)."""
    latency_ms: Annotated[float, Field(ge=0.0)]
