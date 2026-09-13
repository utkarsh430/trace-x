"""Feature specifications, and the three answers a feature can give (ADR-0022, ADR-0032).

**The failure this prevents.** IEEE-CIS has no latitude or longitude. A geo
feature computed over a source that does not supply geography must not return
`0.0`. Zero is a *value*: it says "the distance was zero", and it looks exactly
like a real measurement in every model, every SHAP plot and every report. The
honest answer is that the feature could not be computed at all.

So every feature declares the canonical fields it needs, and evaluation compares
that declaration against the source's `field_coverage`:

    required_fields ⊄ field_coverage  ⟹  UNAVAILABLE

Phase 2 adds a **third** answer. A feature can also be well-defined on this
source and still not computable *yet* -- an account opened four hours ago has no
24-hour baseline. That is `INSUFFICIENT_HISTORY`, and it is deliberately not
folded into `UNAVAILABLE`: the two have different causes, different remedies, and
different consequences for a transfer metric. `UNAVAILABLE` disqualifies a
feature from a cross-dataset comparison permanently; `INSUFFICIENT_HISTORY`
resolves by itself as data arrives and is ordinary in healthy traffic.

What all three share: **only the AVAILABLE state carries a number.** Reading
`.value` on either absent state raises, and the type exposes no `__float__` and
no arithmetic, so neither can silently become `0.0`. That is what makes "never
imputed" a property of the type rather than a rule people are asked to remember.

Every feature also declares its `semantics` (ADR-0032), so Phase 3 can compile
the same definition into an event-time Spark aggregate rather than re-deriving
the intent from prose -- which is how two implementations of the same feature
come to disagree.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Final

from trace_core.contracts.canonical import CanonicalField, CanonicalTransaction
from trace_core.domain.enums import FeatureSource
from trace_core.domain.errors import FeatureUnavailableError
from trace_core.features.context import (
    INSUFFICIENT_HISTORY,
    FeatureContext,
    InsufficientHistory,
)
from trace_core.features.semantics import Semantics

FEATURE_SET_VERSION: Final = "1.0.0"
"""Bumped whenever a feature's MEANING changes.

Recorded on every `RiskDecision` and in every run manifest: a latency or quality
number produced by a different feature set is not comparable to one produced by
this one, and a version is how a reader can tell.
"""


class FeatureState(StrEnum):
    """Why a feature does or does not have a value."""

    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"
    """The source does not supply the required inputs at all (ADR-0022)."""
    INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
    """The inputs exist; this entity has too little history to compute from."""


@dataclass(frozen=True, slots=True)
class FeatureValue:
    """A computed feature, or an explicit statement of why it is not one.

    Deliberately not a bare `float | None`: `None` would be indistinguishable
    from a legitimately-null measurement, and it invites `value or 0`.
    """

    feature_id: str
    _value: float | None
    state: FeatureState = FeatureState.AVAILABLE
    missing_fields: frozenset[CanonicalField] = frozenset()
    """Which required fields the source did not cover. Empty unless UNAVAILABLE.

    Carried rather than discarded so a report can say *why* a feature is absent
    -- "IEEE-CIS supplies no latitude" is actionable, "null" is not.
    """
    source: FeatureSource = FeatureSource.ONLINE_ONLY
    """Whether the warm path has reconciled the inputs this was computed from.

    A property of the VALUE rather than of the response, so it survives being
    logged, stored or compared. `ONLINE_ONLY` throughout Phase 2; Phase 3's
    reconciliation job is what makes `RECONCILED` reachable.
    """
    approximate: bool = False
    """True when the online estimator is inexact by construction -- HyperLogLog
    distinct counts (~0.81%, ADR-0003) and running robust-z estimates. The
    parity tolerance for this feature must account for it, and widening a
    tolerance to make a test pass is prohibited (docs/DATA_ENGINEERING.md §4)."""

    @classmethod
    def of(
        cls,
        feature_id: str,
        value: float,
        *,
        source: FeatureSource = FeatureSource.ONLINE_ONLY,
        approximate: bool = False,
    ) -> FeatureValue:
        return cls(
            feature_id=feature_id,
            _value=value,
            state=FeatureState.AVAILABLE,
            source=source,
            approximate=approximate,
        )

    @classmethod
    def unavailable(cls, feature_id: str, missing: frozenset[CanonicalField]) -> FeatureValue:
        if not missing:
            raise ValueError(
                "UNAVAILABLE requires the fields that were missing; without them the "
                "state is indistinguishable from a bug"
            )
        return cls(
            feature_id=feature_id,
            _value=None,
            state=FeatureState.UNAVAILABLE,
            missing_fields=missing,
        )

    @classmethod
    def insufficient_history(cls, feature_id: str) -> FeatureValue:
        """The source supplies the inputs; this entity has too little history."""
        return cls(
            feature_id=feature_id,
            _value=None,
            state=FeatureState.INSUFFICIENT_HISTORY,
        )

    @property
    def is_available(self) -> bool:
        return self._value is not None

    @property
    def value(self) -> float:
        """The value, or raise.

        Raising is the whole point. A caller that wants null propagation asks for
        it explicitly via `or_none()`; a caller that assumed availability finds
        out here rather than silently scoring on a fabricated number.
        """
        if self._value is None:
            raise FeatureUnavailableError(self._absence_message())
        return self._value

    def _absence_message(self) -> str:
        if self.state is FeatureState.UNAVAILABLE:
            return (
                f"feature {self.feature_id!r} is UNAVAILABLE: the source does not cover "
                f"{sorted(f.value for f in self.missing_fields)}. It must propagate as "
                f"null/absent -- never as 0 (ADR-0022)."
            )
        return (
            f"feature {self.feature_id!r} has INSUFFICIENT_HISTORY: the inputs exist but "
            f"this entity has not accumulated enough of them yet. It must propagate as "
            f"null/absent -- never as 0, which would read as a measured low value."
        )

    def or_none(self) -> float | None:
        """Explicit null propagation, for writers that persist absence."""
        return self._value

    def __bool__(self) -> bool:
        """Truthiness is availability, so `if feature:` reads correctly.

        Without this, an absent value would be truthy and
        `if feature: use(feature.value)` would raise.
        """
        return self.is_available


Compute = Callable[[CanonicalTransaction, FeatureContext], float | InsufficientHistory]
"""How a feature is computed from a transaction and a context snapshot.

Returning `INSUFFICIENT_HISTORY` is a first-class answer, not an error path. The
sentinel is a distinct type rather than `None` so mypy separates "declined" from
"forgot to return".
"""


@dataclass(frozen=True, slots=True)
class FeatureSpec:
    """One feature: what it means, what it needs, and how it is computed."""

    feature_id: str
    description: str
    required_fields: frozenset[CanonicalField]
    semantics: Semantics
    """The declarative meaning (ADR-0032). Phase 3 compiles this into an
    event-time Spark aggregate; `compute` is only how the ONLINE path realises
    it. Sharing the meaning rather than the closure is what makes the parity
    test compare like with like."""
    compute: Compute
    higher_is_riskier: bool = True
    """False for the two features where a LOW value is the signal: uniform
    merchant amounts (laundering) and habitual-set membership. Declared so a
    rule cannot be written against the wrong tail by accident."""

    def __post_init__(self) -> None:
        if not self.feature_id:
            raise ValueError("feature_id is required")
        if not self.description.strip():
            raise ValueError(f"feature {self.feature_id!r} has no description")
        if not self.required_fields:
            raise ValueError(
                f"feature {self.feature_id!r} declares no required_fields. A feature that "
                f"needs nothing can never be UNAVAILABLE, which means it silently claims "
                f"to work on every source regardless of coverage (ADR-0022)."
            )

    @property
    def approximate(self) -> bool:
        """Whether the online value is inexact by construction."""
        return self.semantics.is_approximate

    def evaluate(
        self,
        transaction: CanonicalTransaction,
        context: FeatureContext,
    ) -> FeatureValue:
        """Compute, or say precisely why not.

        Coverage is checked first: a source that cannot supply the inputs will
        never accumulate history for them either, and reporting
        INSUFFICIENT_HISTORY there would be misleading about the remedy.
        """
        missing = self.required_fields - transaction.field_coverage
        if missing:
            return FeatureValue.unavailable(self.feature_id, missing)
        result = self.compute(transaction, context)
        if isinstance(result, InsufficientHistory):
            return FeatureValue.insufficient_history(self.feature_id)
        return FeatureValue.of(
            self.feature_id,
            float(result),
            source=context.source,
            approximate=self.approximate,
        )

    def is_computable_on(self, coverage: frozenset[CanonicalField]) -> bool:
        """Whether this feature can run against a source with `coverage`.

        Used to report the intersecting feature subset beside every transfer
        metric, so a thin overlap visibly invalidates the metric (ADR-0021).
        """
        return self.required_fields <= coverage


@dataclass
class FeatureRegistry:
    """The single place features are declared (docs/DATA_ENGINEERING.md §4).

    One definition per feature, shared by the online and offline paths, so the
    two cannot drift in their *semantics* -- only in their implementation, which
    is what `feature_parity_drift` measures.
    """

    _specs: dict[str, FeatureSpec] = field(default_factory=dict)

    def register(self, spec: FeatureSpec) -> FeatureSpec:
        if spec.feature_id in self._specs:
            raise ValueError(f"feature {spec.feature_id!r} is already registered")
        self._specs[spec.feature_id] = spec
        return spec

    def get(self, feature_id: str) -> FeatureSpec:
        try:
            return self._specs[feature_id]
        except KeyError:
            raise KeyError(f"unknown feature {feature_id!r}") from None

    def __contains__(self, feature_id: object) -> bool:
        return feature_id in self._specs

    def __iter__(self) -> Iterator[FeatureSpec]:
        return iter(self._specs.values())

    def __len__(self) -> int:
        return len(self._specs)

    @property
    def ids(self) -> tuple[str, ...]:
        return tuple(self._specs)

    def computable_on(self, coverage: frozenset[CanonicalField]) -> frozenset[str]:
        """Feature ids a source with `coverage` can actually compute."""
        return frozenset(s.feature_id for s in self if s.is_computable_on(coverage))

    def evaluate_all(
        self,
        transaction: CanonicalTransaction,
        context: FeatureContext,
    ) -> dict[str, FeatureValue]:
        """Every feature, available or not.

        Returns absent entries rather than omitting them: a caller must be able
        to tell "not computed" from "not in the registry".
        """
        return {spec.feature_id: spec.evaluate(transaction, context) for spec in self}


__all__ = [
    "FEATURE_SET_VERSION",
    "INSUFFICIENT_HISTORY",
    "Compute",
    "FeatureRegistry",
    "FeatureSpec",
    "FeatureState",
    "FeatureValue",
]
