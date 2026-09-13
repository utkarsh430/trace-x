"""No API contract may carry ground truth, in any form.

The real control is a database grant: `trace_app` -- the role the gateway
connects as -- has no grant on the `groundtruth` schema at all (ADR-0004), and a
release-blocking integration test asserts the denial. This is the cheap
structural companion, and it guards a different failure: a field that *looks*
innocuous but names a label.

CLAUDE.md §11: "Agents are never given ground truth in any form, including
indirectly through a feature." The same applies to the caller of the hot path --
a `RiskDecision` that echoed `fraud_pattern` would put the answer in the
response body, and every metric computed from that traffic would be worthless.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from trace_core.contracts.api import (
    AcceptedResponse,
    DeviceEventRequest,
    IdentityEventRequest,
    Problem,
    RiskDecision,
    RuleReason,
    TransactionRequest,
)

pytestmark = [pytest.mark.unit, pytest.mark.security]

API_MODELS: list[type[BaseModel]] = [
    TransactionRequest,
    RiskDecision,
    RuleReason,
    Problem,
    IdentityEventRequest,
    DeviceEventRequest,
    AcceptedResponse,
]

# Names that would mean a label had travelled. Matched as substrings so
# `is_fraud_label`, `true_fraud_pattern` and friends are caught too.
FORBIDDEN_SUBSTRINGS = (
    "is_fraud",
    "fraud_pattern",
    "causal_evidence",
    "ground_truth",
    "groundtruth",
    "label",
    "scenario",
)


def _field_names(model: type[BaseModel]) -> set[str]:
    names = set(model.model_fields)
    schema: dict[str, Any] = model.model_json_schema()
    for definition in schema.get("$defs", {}).values():
        names |= set(definition.get("properties", {}))
    return names


@pytest.mark.parametrize("model", API_MODELS, ids=lambda m: m.__name__)
def test_no_api_model_declares_a_ground_truth_field(model: type[BaseModel]) -> None:
    for name in _field_names(model):
        lowered = name.lower()
        for forbidden in FORBIDDEN_SUBSTRINGS:
            assert forbidden not in lowered, (
                f"{model.__name__}.{name} names ground truth. Labels live only in the "
                f"`groundtruth` schema, which the application role cannot read at all "
                f"(ADR-0004, CLAUDE.md §11); putting one in a response would invalidate "
                f"every metric computed from this traffic."
            )


def test_the_scan_would_catch_a_planted_leak() -> None:
    """Without this, a broken matcher would let every model above pass."""
    from pydantic import ConfigDict

    class Leaky(BaseModel):
        model_config = ConfigDict(extra="forbid")
        transaction_id: str
        fraud_pattern: str

    leaked = [
        name for name in _field_names(Leaky) if any(f in name.lower() for f in FORBIDDEN_SUBSTRINGS)
    ]
    assert leaked == ["fraud_pattern"], "the ground-truth field scan does not actually detect one"


def test_the_decision_explains_itself_without_ground_truth() -> None:
    """The positive half: a decision is auditable from its own contents.

    Every reason names a rule AND the feature values it read, so a decision can
    be re-derived by hand -- which is what makes rules-tier explainability a
    property rather than a claim (docs/ARCHITECTURE.md §7).
    """
    reason_fields = set(RuleReason.model_fields)
    assert {"rule_id", "rule_version", "features_read"} <= reason_fields

    decision_fields = set(RiskDecision.model_fields)
    assert {"rule_pack_digest", "threshold_config_digest", "feature_set_version"} <= (
        decision_fields
    ), "a decision that cannot name the behaviour that produced it is not reproducible"
    assert {"unavailable_features", "insufficient_history_features"} <= decision_fields, (
        "absent inputs must be reported, not hidden: a decision made with half its "
        "features missing is a different fact from one made with all of them (ADR-0022)"
    )
