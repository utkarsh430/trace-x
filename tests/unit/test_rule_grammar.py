"""The predicate grammar, and the third truth value that makes it honest.

Two properties are load-bearing here and neither is obvious from reading the
happy path:

1. **An absent feature makes a predicate UNKNOWN, never FALSE.** A rule that
   evaluated to false on a missing input would silently stop firing on every
   source that lacks the field, and the resulting score would read as a
   confident "not fraud" rather than as "not assessed" -- the same fabrication
   as imputing zero, one level up (ADR-0022, ADR-0033).
2. **Unknown still settles where the answer is already determined.** One false
   conjunct settles an AND and one true disjunct settles an OR, so a missing
   feature costs coverage only where it genuinely mattered. Without this, a
   single cold feature would blind most of the pack.
"""

from __future__ import annotations

import pytest

from trace_core.domain.errors import ContractError
from trace_core.features.spec import FeatureValue
from trace_core.rules.grammar import (
    MAX_DEPTH,
    AllOf,
    AnyOf,
    Compare,
    Comparison,
    EvalContext,
    InSet,
    Node,
    Not,
    Truth,
    validate,
)

pytestmark = pytest.mark.unit

KNOWN = frozenset({"present", "other", "absent_unavailable", "absent_history"})
CONSTANTS = frozenset({"high_risk"})


def _ctx(**categoricals: str | None) -> EvalContext:
    from trace_core.contracts.canonical import CanonicalField

    return EvalContext(
        features={
            "present": FeatureValue.of("present", 5.0),
            "other": FeatureValue.of("other", 1.0),
            "absent_unavailable": FeatureValue.unavailable(
                "absent_unavailable", frozenset({CanonicalField.LATITUDE})
            ),
            "absent_history": FeatureValue.insufficient_history("absent_history"),
        },
        categoricals={"merchant_mcc": "5411", **categoricals},
        constants={"high_risk": frozenset({"7995", "6051"})},
    )


# --- comparisons ------------------------------------------------------------


@pytest.mark.parametrize(
    ("op", "threshold", "expected"),
    [
        (Comparison.GT, 3.0, Truth.TRUE),
        (Comparison.GT, 5.0, Truth.FALSE),
        (Comparison.GTE, 5.0, Truth.TRUE),
        (Comparison.LT, 9.0, Truth.TRUE),
        (Comparison.LTE, 5.0, Truth.TRUE),
        (Comparison.EQ, 5.0, Truth.TRUE),
        (Comparison.NEQ, 5.0, Truth.FALSE),
    ],
)
def test_every_comparison_operator_works(op: Comparison, threshold: float, expected: Truth) -> None:
    assert Compare("present", op, threshold).evaluate(_ctx()) is expected


@pytest.mark.parametrize("feature", ["absent_unavailable", "absent_history", "never_registered"])
def test_a_comparison_on_an_absent_feature_is_unknown_not_false(feature: str) -> None:
    """The central property. FALSE here would be a fabricated measurement."""
    assert Compare(feature, Comparison.GT, 0.0).evaluate(_ctx()) is Truth.UNKNOWN


def test_both_kinds_of_absence_behave_identically_in_a_predicate() -> None:
    """They have different CAUSES and different remedies, but a rule cannot act
    on either, so both must abstain."""
    ctx = _ctx()
    unavailable = Compare("absent_unavailable", Comparison.GT, 0.0).evaluate(ctx)
    history = Compare("absent_history", Comparison.GT, 0.0).evaluate(ctx)
    assert unavailable is history is Truth.UNKNOWN


# --- Kleene logic -----------------------------------------------------------


def test_a_false_conjunct_settles_an_and_despite_an_unknown() -> None:
    """Otherwise one cold feature would blind most of the pack."""
    node = AllOf(
        (
            Compare("present", Comparison.LT, 1.0),  # FALSE
            Compare("absent_history", Comparison.GT, 0.0),  # UNKNOWN
        )
    )
    assert node.evaluate(_ctx()) is Truth.FALSE


def test_an_unknown_conjunct_prevents_a_true_and() -> None:
    node = AllOf(
        (
            Compare("present", Comparison.GT, 1.0),  # TRUE
            Compare("absent_history", Comparison.GT, 0.0),  # UNKNOWN
        )
    )
    assert node.evaluate(_ctx()) is Truth.UNKNOWN


def test_a_true_disjunct_settles_an_or_despite_an_unknown() -> None:
    node = AnyOf(
        (
            Compare("present", Comparison.GT, 1.0),  # TRUE
            Compare("absent_history", Comparison.GT, 0.0),  # UNKNOWN
        )
    )
    assert node.evaluate(_ctx()) is Truth.TRUE


def test_an_unknown_disjunct_prevents_a_false_or() -> None:
    node = AnyOf(
        (
            Compare("present", Comparison.LT, 1.0),  # FALSE
            Compare("absent_history", Comparison.GT, 0.0),  # UNKNOWN
        )
    )
    assert node.evaluate(_ctx()) is Truth.UNKNOWN


def test_negation_preserves_unknown() -> None:
    """`not unknown` is unknown. Mapping it to TRUE would let a rule fire
    BECAUSE a feature was missing, which is worse than not firing."""
    assert Not(Compare("absent_history", Comparison.GT, 0.0)).evaluate(_ctx()) is Truth.UNKNOWN
    assert Not(Compare("present", Comparison.GT, 1.0)).evaluate(_ctx()) is Truth.FALSE
    assert Not(Compare("present", Comparison.LT, 1.0)).evaluate(_ctx()) is Truth.TRUE


def test_all_of_with_every_conjunct_true_is_true() -> None:
    node = AllOf((Compare("present", Comparison.GT, 1.0), Compare("other", Comparison.LT, 5.0)))
    assert node.evaluate(_ctx()) is Truth.TRUE


# --- set membership ---------------------------------------------------------


def test_set_membership_is_evaluated_against_a_declared_constant() -> None:
    assert InSet("merchant_mcc", "high_risk").evaluate(_ctx(merchant_mcc="7995")) is Truth.TRUE
    assert InSet("merchant_mcc", "high_risk").evaluate(_ctx(merchant_mcc="5411")) is Truth.FALSE


def test_negated_set_membership_inverts() -> None:
    node = InSet("merchant_mcc", "high_risk", negate=True)
    assert node.evaluate(_ctx(merchant_mcc="5411")) is Truth.TRUE


def test_a_missing_categorical_is_unknown_not_absent_from_the_set() -> None:
    """ "The source did not supply an MCC" is not "the MCC is not high risk"."""
    assert InSet("merchant_mcc", "high_risk").evaluate(_ctx(merchant_mcc=None)) is Truth.UNKNOWN


# --- load-time validation ---------------------------------------------------


def test_a_predicate_over_an_unregistered_feature_is_refused() -> None:
    """It would abstain on every transaction forever, which looks exactly like a
    rule that simply never matches."""
    with pytest.raises(ContractError, match="unregistered feature"):
        validate(
            Compare("no_such_feature", Comparison.GT, 1.0),
            known_features=KNOWN,
            known_constants=CONSTANTS,
        )


def test_a_predicate_over_an_undeclared_constant_set_is_refused() -> None:
    with pytest.raises(ContractError, match="undeclared constant"):
        validate(
            InSet("merchant_mcc", "not_declared"),
            known_features=KNOWN,
            known_constants=CONSTANTS,
        )


def test_an_attacker_controlled_field_cannot_be_tested() -> None:
    """A rule branching on a merchant name would let a fraudster choose their own
    risk score by renaming their shop (docs/SECURITY.md §5.1)."""
    for field in ("merchant_name", "user_agent", "memo"):
        with pytest.raises(ContractError, match="not a testable categorical field"):
            validate(InSet(field, "high_risk"), known_features=KNOWN, known_constants=CONSTANTS)


def test_an_empty_conjunction_is_refused() -> None:
    """Vacuously true, and never what the author meant."""
    with pytest.raises(ContractError, match="empty all/any"):
        validate(AllOf(()), known_features=KNOWN, known_constants=CONSTANTS)
    with pytest.raises(ContractError, match="empty all/any"):
        validate(AnyOf(()), known_features=KNOWN, known_constants=CONSTANTS)


def test_an_overly_deep_predicate_is_refused() -> None:
    """An unreviewable rule is one nobody can say is correct."""
    node: Node = Compare("present", Comparison.GT, 1.0)
    for _ in range(MAX_DEPTH + 1):
        node = Not(node)
    with pytest.raises(ContractError, match="deep"):
        validate(node, known_features=KNOWN, known_constants=CONSTANTS)


def test_a_predicate_at_the_depth_limit_is_accepted() -> None:
    """Otherwise the limit would be off by one and nobody would notice."""
    node: Node = Compare("present", Comparison.GT, 1.0)
    for _ in range(MAX_DEPTH - 1):
        node = Not(node)
    validate(node, known_features=KNOWN, known_constants=CONSTANTS)


def test_validation_recurses_into_nested_nodes() -> None:
    """A bad leaf three levels down must still be caught at load time."""
    node = AllOf(
        (
            Compare("present", Comparison.GT, 1.0),
            AnyOf((Not(InSet("merchant_name", "high_risk")),)),
        )
    )
    with pytest.raises(ContractError, match="not a testable categorical field"):
        validate(node, known_features=KNOWN, known_constants=CONSTANTS)
