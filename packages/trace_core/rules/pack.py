"""The rule pack: what a set of rules is, and what pins it (ADR-0033).

A pack is **digest-pinned**. Rules hot-reload, so "which rules were live at
14:03?" cannot be answered from a deploy log — the answer has to travel on the
decision itself. Every `RiskDecision` carries `rule_pack_digest`, and the digest
is computed over the pack's *semantic content* in canonical form, so reformatting
the YAML does not move it while changing a threshold does.

**A rule declares its own effect on the outcome**, in two independent ways:

* `weight` contributes to a bounded weighted score;
* `band_floor` optionally forces a minimum risk band regardless of that score.

The second exists because averaging is the wrong model for some signals.
`IMPOSSIBLE_TRAVEL` is not a "somewhat risky" observation to be diluted by
twenty quiet rules — two card-present transactions implying supersonic travel is
either a data error or fraud, and a weighted sum would let it be outvoted.

**Thresholds are declared configuration, not fitted parameters.** Phase 2 has no
model and no access to labels: the application role cannot read the `groundtruth`
schema at all (ADR-0004). Tuning these against labels inside the application
would be leakage by another route. Phase 4 selects operating points through the
evaluation harness, which may read ground truth because that is its job.
"""

from __future__ import annotations

from typing import Annotated, Any, Final, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from trace_core.contracts.canonical_json import content_hash
from trace_core.domain.enums import RiskBand
from trace_core.domain.errors import ContractError
from trace_core.rules.grammar import (
    AllOf,
    AnyOf,
    Compare,
    Comparison,
    InSet,
    Node,
    Not,
    validate,
)

RULE_ID_PATTERN: Final = r"^R\d{3}_[a-z0-9_]+$"
SEMVER_PATTERN: Final = r"^\d+\.\d+\.\d+$"

MAX_RULES: Final = 200
"""A pack larger than this is not reviewable, and an unreviewable pack is one
nobody can say is correct. Also bounds per-transaction evaluation cost."""


class StrictPackModel(BaseModel):
    """Strict, because a typo in a rule pack must be a load failure.

    `extra="forbid"` in particular: a misspelled `band_flor` that was silently
    ignored would produce a rule that quietly never raises the band, and the
    only symptom would be fraud scoring lower than it should.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, strict=False)


class PredicateModel(StrictPackModel):
    """One node of a predicate, as written in YAML.

    Exactly one of the five forms must be present. Parsed into the closed
    grammar of `rules.grammar`; nothing here is ever evaluated as code.
    """

    feature: str | None = None
    op: Comparison | None = None
    value: float | None = None

    field: str | None = None
    in_set: str | None = None
    negate: bool = False

    all_of: list[PredicateModel] | None = None
    any_of: list[PredicateModel] | None = None
    not_: PredicateModel | None = Field(default=None, alias="not")

    @model_validator(mode="after")
    def _exactly_one_form(self) -> Self:
        forms = [
            self.feature is not None,
            self.field is not None,
            self.all_of is not None,
            self.any_of is not None,
            self.not_ is not None,
        ]
        if sum(forms) != 1:
            raise ValueError(
                "a predicate node must be exactly one of: a feature comparison, a set "
                "membership, all_of, any_of, or not"
            )
        if self.feature is not None and (self.op is None or self.value is None):
            raise ValueError(f"comparison on {self.feature!r} needs both `op` and `value`")
        if self.field is not None and self.in_set is None:
            raise ValueError(f"membership test on {self.field!r} needs `in_set`")
        return self

    def to_node(self) -> Node:
        """Build the grammar node. Structure only — no evaluation happens here."""
        if self.feature is not None:
            assert self.op is not None and self.value is not None
            return Compare(feature=self.feature, op=self.op, threshold=float(self.value))
        if self.field is not None:
            assert self.in_set is not None
            return InSet(field=self.field, constant=self.in_set, negate=self.negate)
        if self.all_of is not None:
            return AllOf(tuple(p.to_node() for p in self.all_of))
        if self.any_of is not None:
            return AnyOf(tuple(p.to_node() for p in self.any_of))
        assert self.not_ is not None
        return Not(self.not_.to_node())


class RuleModel(StrictPackModel):
    """One rule, as written in YAML."""

    id: Annotated[str, Field(pattern=RULE_ID_PATTERN)]
    version: Annotated[str, Field(pattern=SEMVER_PATTERN)]
    description: Annotated[str, Field(min_length=20, max_length=256)]
    """Reaches an analyst through the decision's `reasons`, so it has to say what
    the rule means rather than restate its predicate."""
    weight: Annotated[float, Field(ge=0.0, le=1.0)]
    band_floor: RiskBand | None = None
    when: PredicateModel
    enabled: bool = True
    """Disabled rules stay in the pack rather than being deleted, so the digest
    records that a rule existed and was off — which is a different fact from a
    rule that was never written."""


class RulePackModel(StrictPackModel):
    """A complete, versioned set of rules."""

    pack_id: Annotated[str, Field(min_length=1, max_length=64)]
    version: Annotated[str, Field(pattern=SEMVER_PATTERN)]
    description: Annotated[str, Field(min_length=10, max_length=512)]
    constants: dict[str, list[str]] = Field(default_factory=dict)
    """Named value sets a membership test may reference. One definition per
    concept, so "which MCCs are high risk" is reviewable in one place and the
    digest covers it."""
    rules: list[RuleModel]

    @model_validator(mode="after")
    def _rules_are_well_formed(self) -> Self:
        if not self.rules:
            raise ValueError("a rule pack with no rules would score every transaction zero")
        if len(self.rules) > MAX_RULES:
            raise ValueError(f"{len(self.rules)} rules exceeds the reviewable limit of {MAX_RULES}")
        ids = [r.id for r in self.rules]
        duplicates = sorted({i for i in ids if ids.count(i) > 1})
        if duplicates:
            raise ValueError(
                f"duplicate rule id(s) {duplicates}: a decision citing one would be ambiguous"
            )
        return self


class CompiledRule:
    """A validated rule with its predicate compiled to grammar nodes."""

    __slots__ = ("band_floor", "description", "features_used", "id", "node", "version", "weight")

    def __init__(self, model: RuleModel) -> None:
        self.id = model.id
        self.version = model.version
        self.description = model.description
        self.weight = model.weight
        self.band_floor = model.band_floor
        self.node: Node = model.when.to_node()
        self.features_used: frozenset[str] = self.node.feature_ids()


class CompiledPack:
    """A loaded, validated, digest-pinned rule pack."""

    __slots__ = ("constants", "description", "digest", "pack_id", "rules", "version")

    def __init__(self, model: RulePackModel, *, known_features: frozenset[str]) -> None:
        self.pack_id = model.pack_id
        self.version = model.version
        self.description = model.description
        self.constants: dict[str, frozenset[str]] = {
            name: frozenset(values) for name, values in model.constants.items()
        }
        compiled: list[CompiledRule] = []
        for rule in model.rules:
            if not rule.enabled:
                continue
            node = rule.when.to_node()
            try:
                validate(
                    node,
                    known_features=known_features,
                    known_constants=frozenset(self.constants),
                )
            except ContractError as exc:
                raise ContractError(f"rule {rule.id}: {exc}") from exc
            compiled.append(CompiledRule(rule))
        if not compiled:
            raise ContractError(
                f"pack {model.pack_id!r} has no ENABLED rules; it would score every "
                f"transaction zero, which is indistinguishable from a healthy day"
            )
        self.rules: tuple[CompiledRule, ...] = tuple(compiled)
        self.digest = pack_digest(model)

    @property
    def features_used(self) -> frozenset[str]:
        return frozenset().union(*(r.features_used for r in self.rules))

    def __len__(self) -> int:
        return len(self.rules)


def pack_digest(model: RulePackModel) -> str:
    """`sha256:` over the pack's semantic content.

    Computed from the parsed model rather than from the file bytes: reformatting
    the YAML, reordering keys or changing a comment must not move the digest,
    because a digest that changes for non-reasons trains everyone to ignore it.
    Changing a threshold, a weight, a predicate or a constant set must.
    """
    payload: dict[str, Any] = model.model_dump(mode="json", by_alias=True, exclude_none=True)
    return content_hash(payload)
