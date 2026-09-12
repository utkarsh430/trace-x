"""Feature specifications and the UNAVAILABLE mechanism (ADR-0022).

**The failure this prevents.** IEEE-CIS has no latitude or longitude. A geo
feature computed over a source that does not supply geography must not return
`0.0`. Zero is a *value*: it says "the distance was zero", and it looks exactly
like a real measurement in every model, every SHAP plot and every report. The
honest answer is that the feature could not be computed at all.

So every feature declares the canonical fields it needs, and evaluation compares
that declaration against the source's `field_coverage`:

    required_fields ⊄ field_coverage  ⟹  UNAVAILABLE

`UNAVAILABLE` is a distinct state, not a magic number. Reading `.value` on one
raises rather than returning a default, which is what makes "never imputed to
zero" a property of the type rather than a rule people are asked to remember.

Phase 1 ships the mechanism and a handful of reference features to exercise it.
The ~20 production features arrive in Phase 2, and every one of them declares
`required_fields` because `FeatureRegistry` refuses to register a feature that
does not.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Final

from trace_core.contracts.canonical import CanonicalField, CanonicalTransaction
from trace_core.domain.errors import FeatureUnavailableError


@dataclass(frozen=True, slots=True)
class FeatureValue:
    """A computed feature, or an explicit statement that it could not be.

    Deliberately not a bare `float | None`: `None` would be indistinguishable
    from a legitimately-null measurement, and it invites `value or 0`.
    """

    feature_id: str
    _value: float | None
    missing_fields: frozenset[CanonicalField] = frozenset()
    """Which required fields the source did not cover. Empty when available.

    Carried rather than discarded so a report can say *why* a feature is absent
    -- "IEEE-CIS supplies no latitude" is actionable, "null" is not.
    """

    @classmethod
    def of(cls, feature_id: str, value: float) -> FeatureValue:
        return cls(feature_id=feature_id, _value=value)

    @classmethod
    def unavailable(cls, feature_id: str, missing: frozenset[CanonicalField]) -> FeatureValue:
        if not missing:
            raise ValueError(
                "UNAVAILABLE requires the fields that were missing; without them the "
                "state is indistinguishable from a bug"
            )
        return cls(feature_id=feature_id, _value=None, missing_fields=missing)

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
            raise FeatureUnavailableError(
                f"feature {self.feature_id!r} is UNAVAILABLE: the source does not cover "
                f"{sorted(f.value for f in self.missing_fields)}. It must propagate as "
                f"null/absent -- never as 0 (ADR-0022)."
            )
        return self._value

    def or_none(self) -> float | None:
        """Explicit null propagation, for writers that persist absence."""
        return self._value

    def __bool__(self) -> bool:
        """Truthiness is availability, so `if feature:` reads correctly.

        Without this, an UNAVAILABLE value would be truthy and
        `if feature: use(feature.value)` would raise.
        """
        return self.is_available


@dataclass(frozen=True, slots=True)
class FeatureSpec:
    """One feature: what it needs, and how it is computed."""

    feature_id: str
    description: str
    required_fields: frozenset[CanonicalField]
    compute: Callable[[CanonicalTransaction], float]
    """Phase 1 features are pure functions of one canonical transaction.

    Phase 2 features also read the online store, so this signature gains a
    context parameter then. Kept minimal here rather than guessed at, because a
    speculative parameter nobody passes is worse than a later signature change.
    """

    def __post_init__(self) -> None:
        if not self.feature_id:
            raise ValueError("feature_id is required")
        if not self.required_fields:
            raise ValueError(
                f"feature {self.feature_id!r} declares no required_fields. A feature that "
                f"needs nothing can never be UNAVAILABLE, which means it silently claims "
                f"to work on every source regardless of coverage (ADR-0022)."
            )

    def evaluate(self, transaction: CanonicalTransaction) -> FeatureValue:
        """Compute, or return UNAVAILABLE naming the fields that were missing."""
        missing = self.required_fields - transaction.field_coverage
        if missing:
            return FeatureValue.unavailable(self.feature_id, missing)
        return FeatureValue.of(self.feature_id, self.compute(transaction))

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

    def __iter__(self) -> Iterator[FeatureSpec]:
        return iter(self._specs.values())

    def __len__(self) -> int:
        return len(self._specs)

    def computable_on(self, coverage: frozenset[CanonicalField]) -> frozenset[str]:
        """Feature ids a source with `coverage` can actually compute."""
        return frozenset(s.feature_id for s in self if s.is_computable_on(coverage))

    def evaluate_all(self, transaction: CanonicalTransaction) -> dict[str, FeatureValue]:
        """Every feature, available or not.

        Returns UNAVAILABLE entries rather than omitting them: a caller must be
        able to tell "not computed" from "not in the registry".
        """
        return {spec.feature_id: spec.evaluate(transaction) for spec in self}


# ------------------------------------------------- reference features ------
# Deliberately trivial. Their job is to exercise the mechanism end to end before
# any real feature depends on it (ROADMAP Phase 1, first task 5). Phase 2 ships
# the ~20 production features.

REFERENCE_FEATURES: Final = FeatureRegistry()

REFERENCE_FEATURES.register(
    FeatureSpec(
        feature_id="amount_magnitude",
        description="Order of magnitude of the transaction amount in minor units.",
        required_fields=frozenset({CanonicalField.AMOUNT_MINOR}),
        # log1p of the absolute value: defined at zero, and a refund's magnitude
        # is its size, not its sign.
        compute=lambda tx: float(abs(tx.amount_minor)) ** 0.5,
    )
)

REFERENCE_FEATURES.register(
    FeatureSpec(
        feature_id="merchant_context_present",
        description="Whether both merchant identity and MCC accompany the transaction.",
        required_fields=frozenset({CanonicalField.MERCHANT_ID, CanonicalField.MERCHANT_MCC}),
        compute=lambda tx: float(tx.merchant_id is not None and tx.merchant_mcc is not None),
    )
)

REFERENCE_FEATURES.register(
    FeatureSpec(
        feature_id="geo_precision",
        description=(
            "Coordinate precision of the transaction location. The canonical example of a "
            "feature that is UNAVAILABLE rather than zero on a source without geography."
        ),
        required_fields=frozenset({CanonicalField.LATITUDE, CanonicalField.LONGITUDE}),
        compute=lambda tx: abs(tx.latitude or 0.0) + abs(tx.longitude or 0.0),
    )
)
