"""Evaluating a rule pack against one transaction's features.

Returns an `Evaluation` that records **three** outcomes per rule, not two:
fired, did not fire, or abstained because an input was absent. The third is what
keeps the result honest. A pack where six rules abstained is a materially
different assessment from one where six rules were checked and found nothing,
and a score that reported them identically would be claiming coverage it did not
have.

Everything a decision needs to be re-derived by hand is carried out of here: the
rule id and version, the rule's description, and **the feature values the
predicate actually read**. A rule id alone is an assertion; a rule id with its
inputs is evidence (`docs/ARCHITECTURE.md` §7 — "full explainability").
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

from trace_core.contracts.canonical import CanonicalTransaction
from trace_core.domain.enums import RiskBand
from trace_core.features.spec import FeatureValue
from trace_core.rules.grammar import CATEGORICAL_FIELDS, EvalContext, Truth
from trace_core.rules.pack import CompiledPack, CompiledRule


@dataclass(frozen=True, slots=True)
class RuleOutcome:
    """What one rule concluded, and what it read to conclude it."""

    rule_id: str
    rule_version: str
    description: str
    result: Truth
    weight: float
    band_floor: RiskBand | None
    features_read: dict[str, float]
    """Only the AVAILABLE values the predicate read. An absent feature appears in
    `absent_features` instead, because reporting it as a number here would be the
    imputation the whole design refuses."""
    absent_features: tuple[str, ...] = ()

    @property
    def fired(self) -> bool:
        return self.result is Truth.TRUE

    @property
    def abstained(self) -> bool:
        return self.result is Truth.UNKNOWN


@dataclass(frozen=True, slots=True)
class Evaluation:
    """The pack's verdict on one transaction."""

    pack_id: str
    pack_version: str
    pack_digest: str
    outcomes: tuple[RuleOutcome, ...] = field(default_factory=tuple)

    @property
    def fired(self) -> tuple[RuleOutcome, ...]:
        return tuple(o for o in self.outcomes if o.fired)

    @property
    def abstained(self) -> tuple[RuleOutcome, ...]:
        return tuple(o for o in self.outcomes if o.abstained)

    @property
    def band_floor(self) -> RiskBand | None:
        """The highest floor any fired rule demands.

        A floor is not averaged. `IMPOSSIBLE_TRAVEL` is not a "somewhat risky"
        observation to be diluted by twenty quiet rules, and a weighted sum alone
        would let exactly that happen.
        """
        floors = [o.band_floor for o in self.fired if o.band_floor is not None]
        if not floors:
            return None
        return max(floors, key=_BAND_ORDER.__getitem__)

    @property
    def coverage(self) -> float:
        """Share of rules that reached a real verdict.

        Reported rather than inferred: a decision made with a third of the pack
        abstaining is weaker evidence than one where every rule was checked, and
        the caller should be able to see the difference.
        """
        if not self.outcomes:
            return 0.0
        return 1.0 - len(self.abstained) / len(self.outcomes)


_BAND_ORDER: Final[dict[RiskBand, int]] = {
    RiskBand.LOW: 0,
    RiskBand.MEDIUM: 1,
    RiskBand.HIGH: 2,
    RiskBand.CRITICAL: 3,
}


def categoricals_of(transaction: CanonicalTransaction) -> dict[str, str | None]:
    """The categorical fields a predicate may test.

    Built from the sanctioned list rather than from the transaction's own
    attributes, so adding a field to `CanonicalTransaction` never silently widens
    what a rule pack can branch on -- in particular it can never reach an
    attacker-controlled field (docs/SECURITY.md §5.1).
    """
    values: dict[str, str | None] = {}
    for name in sorted(CATEGORICAL_FIELDS):
        raw = getattr(transaction, name, None)
        values[name] = str(raw) if raw is not None else None
    return values


def evaluate_pack(
    pack: CompiledPack,
    transaction: CanonicalTransaction,
    features: dict[str, FeatureValue],
) -> Evaluation:
    """Run every rule in `pack`, recording what each one concluded and why."""
    context = EvalContext(
        features=features,
        categoricals=categoricals_of(transaction),
        constants=pack.constants,
    )
    outcomes = tuple(_evaluate_rule(rule, context, features) for rule in pack.rules)
    return Evaluation(
        pack_id=pack.pack_id,
        pack_version=pack.version,
        pack_digest=pack.digest,
        outcomes=outcomes,
    )


def _evaluate_rule(
    rule: CompiledRule, context: EvalContext, features: dict[str, FeatureValue]
) -> RuleOutcome:
    result = rule.node.evaluate(context)
    read: dict[str, float] = {}
    absent: list[str] = []
    for feature_id in sorted(rule.features_used):
        value = features.get(feature_id)
        if value is None or not value.is_available:
            absent.append(feature_id)
        else:
            read[feature_id] = value.value
    return RuleOutcome(
        rule_id=rule.id,
        rule_version=rule.version,
        description=rule.description,
        result=result,
        weight=rule.weight,
        band_floor=rule.band_floor,
        features_read=read,
        absent_features=tuple(absent),
    )
