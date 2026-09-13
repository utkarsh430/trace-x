"""The closed expression grammar a rule predicate is written in (ADR-0033).

**Why not just evaluate a Python expression.** Rules hot-reload (ROADMAP Phase
2): a YAML file changes scoring behaviour with no deploy and no review gate. An
`eval()` there is arbitrary code execution in the gateway process, reachable by
anyone who can write that file -- and it would be the one place in this system
where the rule that untrusted input never becomes executable code did not hold.
`docs/SECURITY.md` §6 states the same principle for tools ("no LLM-authored
queries exist"); this is that principle applied to configuration.

So a predicate is **data**: a small tree of typed nodes over feature ids,
declared constant sets and a fixed operator set. It has no function calls, no
attribute access, no name resolution, and no way to reach anything the evaluator
does not hand it.

**The third truth value is the point.** A feature can be absent -- `UNAVAILABLE`
because the source does not supply its inputs, or `INSUFFICIENT_HISTORY` because
the entity is too new (ADR-0032). A predicate over an absent feature is not
false; it is **unknown**, the rule abstains, and the abstention is counted.
Treating unknown as false is the same fabrication as imputing zero, one level up:
the rule would silently stop firing on every source that lacks the field, and the
resulting score would read as a confident "not fraud" rather than as "not
assessed". Kleene three-valued logic is therefore the semantics, not a refinement
of it:

    UNKNOWN and FALSE  = FALSE     (one false conjunct settles an AND)
    UNKNOWN and TRUE   = UNKNOWN
    UNKNOWN or  TRUE   = TRUE      (one true disjunct settles an OR)
    UNKNOWN or  FALSE  = UNKNOWN
    not UNKNOWN        = UNKNOWN

The two settling cases matter in practice: a rule whose *other* condition already
decided the outcome still returns a real answer, so a missing feature costs
coverage only where it genuinely mattered.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from trace_core.domain.errors import ContractError
from trace_core.features.spec import FeatureValue


class Truth(StrEnum):
    """Three-valued logic. `UNKNOWN` means "not assessed", never "no"."""

    TRUE = "TRUE"
    FALSE = "FALSE"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def of(cls, value: bool) -> Truth:
        return cls.TRUE if value else cls.FALSE


class Comparison(StrEnum):
    """The complete comparison set. Deliberately small and total."""

    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    EQ = "=="
    NEQ = "!="


CATEGORICAL_FIELDS: Final = frozenset(
    {"merchant_mcc", "merchant_country", "currency", "channel", "entry_mode"}
)
"""Transaction fields a rule may test for set membership.

Closed, and deliberately excluding every attacker-controlled field
(`merchant_name`, `user_agent`, `memo`). Those carry `trust_tier=UNTRUSTED`
(docs/SECURITY.md §5.1) and nothing on the hot path interprets them: a rule that
branched on a merchant name would let a fraudster choose their own risk score by
renaming their shop.
"""


@dataclass(frozen=True, slots=True)
class EvalContext:
    """Everything a predicate is allowed to see.

    A closed record rather than the transaction itself: a predicate must not be
    able to reach a field the grammar has not sanctioned, and passing the whole
    transaction would make that a matter of discipline rather than of type.
    """

    features: dict[str, FeatureValue]
    categoricals: dict[str, str | None]
    constants: dict[str, frozenset[str]]


class _Node:
    """Base for the predicate nodes. Not a public type; use `Node`."""

    __slots__ = ()

    def feature_ids(self) -> frozenset[str]:
        raise NotImplementedError

    def evaluate(self, ctx: EvalContext) -> Truth:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class Compare(_Node):
    """`feature <op> threshold` -- the only leaf that reads a feature."""

    feature: str
    op: Comparison
    threshold: float

    def feature_ids(self) -> frozenset[str]:
        return frozenset({self.feature})

    def evaluate(self, ctx: EvalContext) -> Truth:
        value = ctx.features.get(self.feature)
        if value is None or not value.is_available:
            # Absent, so UNKNOWN. Returning FALSE here is the bug this whole
            # module exists to prevent: the rule would stop firing silently.
            return Truth.UNKNOWN
        match self.op:
            case Comparison.GT:
                return Truth.of(value.value > self.threshold)
            case Comparison.GTE:
                return Truth.of(value.value >= self.threshold)
            case Comparison.LT:
                return Truth.of(value.value < self.threshold)
            case Comparison.LTE:
                return Truth.of(value.value <= self.threshold)
            case Comparison.EQ:
                return Truth.of(value.value == self.threshold)
            case Comparison.NEQ:
                return Truth.of(value.value != self.threshold)


@dataclass(frozen=True, slots=True)
class InSet(_Node):
    """`<categorical field> in <named constant set>`.

    The set is named in the rule pack's `constants`, never inlined at the call
    site, so "which MCCs count as high risk" has one definition a reviewer can
    find and the pack digest covers.
    """

    field: str
    constant: str
    negate: bool = False

    def feature_ids(self) -> frozenset[str]:
        return frozenset()

    def evaluate(self, ctx: EvalContext) -> Truth:
        actual = ctx.categoricals.get(self.field)
        if actual is None:
            # The source did not supply this field on this row. Unknown, not
            # "not in the set" -- the same reasoning as an absent feature.
            return Truth.UNKNOWN
        member = actual in ctx.constants[self.constant]
        return Truth.of(not member if self.negate else member)


@dataclass(frozen=True, slots=True)
class Not(_Node):
    inner: Node

    def feature_ids(self) -> frozenset[str]:
        return self.inner.feature_ids()

    def evaluate(self, ctx: EvalContext) -> Truth:
        match self.inner.evaluate(ctx):
            case Truth.TRUE:
                return Truth.FALSE
            case Truth.FALSE:
                return Truth.TRUE
            case _:
                return Truth.UNKNOWN


@dataclass(frozen=True, slots=True)
class AllOf(_Node):
    """Conjunction. FALSE settles it; otherwise any UNKNOWN propagates."""

    nodes: tuple[Node, ...]

    def feature_ids(self) -> frozenset[str]:
        return frozenset().union(*(n.feature_ids() for n in self.nodes))

    def evaluate(self, ctx: EvalContext) -> Truth:
        seen_unknown = False
        for node in self.nodes:
            match node.evaluate(ctx):
                case Truth.FALSE:
                    return Truth.FALSE
                case Truth.UNKNOWN:
                    seen_unknown = True
                case _:
                    continue
        return Truth.UNKNOWN if seen_unknown else Truth.TRUE


@dataclass(frozen=True, slots=True)
class AnyOf(_Node):
    """Disjunction. TRUE settles it; otherwise any UNKNOWN propagates."""

    nodes: tuple[Node, ...]

    def feature_ids(self) -> frozenset[str]:
        return frozenset().union(*(n.feature_ids() for n in self.nodes))

    def evaluate(self, ctx: EvalContext) -> Truth:
        seen_unknown = False
        for node in self.nodes:
            match node.evaluate(ctx):
                case Truth.TRUE:
                    return Truth.TRUE
                case Truth.UNKNOWN:
                    seen_unknown = True
                case _:
                    continue
        return Truth.UNKNOWN if seen_unknown else Truth.FALSE


Node = Compare | InSet | Not | AllOf | AnyOf

MAX_DEPTH: Final = 6
"""A predicate deeper than this is unreviewable, and an unreviewable rule is one
nobody can say is correct. Bounded depth also bounds evaluation cost, which
matters on a path with a 100 ms budget."""


def depth(node: Node) -> int:
    match node:
        case Not():
            return 1 + depth(node.inner)
        case AllOf() | AnyOf():
            return 1 + max((depth(n) for n in node.nodes), default=0)
        case _:
            return 1


def validate(
    node: Node, *, known_features: frozenset[str], known_constants: frozenset[str]
) -> None:
    """Reject a predicate that could not be evaluated correctly.

    Called at load time, so a malformed pack is refused with the last good pack
    retained (ADR-0033) rather than failing per transaction at scoring time.
    """
    if depth(node) > MAX_DEPTH:
        raise ContractError(
            f"predicate nests {depth(node)} deep, over the limit of {MAX_DEPTH}. "
            f"A predicate nobody can read is a rule nobody can review."
        )
    unknown = node.feature_ids() - known_features
    if unknown:
        raise ContractError(
            f"predicate references unregistered feature(s) {sorted(unknown)}. A rule over a "
            f"feature that does not exist would abstain on every transaction forever, which "
            f"looks exactly like a rule that never matches."
        )
    _validate_node(node, known_features=known_features, known_constants=known_constants)


def _validate_node(
    node: Node, *, known_features: frozenset[str], known_constants: frozenset[str]
) -> None:
    match node:
        case AllOf() | AnyOf():
            if not node.nodes:
                raise ContractError(
                    "an empty all/any is vacuously true or false and never means what its "
                    "author intended"
                )
            for child in node.nodes:
                _validate_node(
                    child, known_features=known_features, known_constants=known_constants
                )
        case Not():
            _validate_node(
                node.inner, known_features=known_features, known_constants=known_constants
            )
        case InSet():
            if node.field not in CATEGORICAL_FIELDS:
                raise ContractError(
                    f"{node.field!r} is not a testable categorical field. Permitted: "
                    f"{sorted(CATEGORICAL_FIELDS)}. Attacker-controlled fields are excluded "
                    f"deliberately -- a rule branching on a merchant name would let a "
                    f"fraudster choose their own risk score (docs/SECURITY.md §5.1)."
                )
            if node.constant not in known_constants:
                raise ContractError(
                    f"predicate references undeclared constant set {node.constant!r}; "
                    f"declared: {sorted(known_constants)}"
                )
        case _:
            return
