"""Whether two implementations agree, per transaction and feature (ADR-0056 §3).

The comparison is the feature's declared `ParityComparison` (ADR-0046 §5), never chosen here:
- `EXACT` compares by equality;
- `FLOAT` at `FLOAT_PARITY_RELATIVE_TOLERANCE`, with no absolute tolerance, so a zero must be zero;
- `APPROXIMATE` by the per-stratum RMS bound, but only where one side is an estimator. That is the
  online store's HyperLogLog, in the AS_SERVED pairing. Gold and the reference both compute the
  declared estimand exactly, so in the EVENT_TIME_COMPLETE pairing an approximate feature is
  compared by equality: stricter than the declaration, never looser.

Absences must match: the same state, and in the AS_SERVED pairing the same lookback completeness.
An absent value served as a number is a divergence of state, not a small error.

**Declared exceptions** (ADR-0046 §8 point 5). In four declared situations the online store may
serve an absence where the reference, which never folds or trims, has a value. A comparison is
counted as a declared exception only when the caller names the situation for that read and the
implementation served an absence against the reference's number; a different number is always a
divergence.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Final

from eval.parity.partition import APPROXIMATE_RMS_BOUND, STRATA, ZERO_STRATUM, stratum_of

from trace_core.features.context import Completeness
from trace_core.features.semantics import (
    FLOAT_PARITY_RELATIVE_TOLERANCE,
    ParityComparison,
    WindowedAggregate,
)
from trace_core.features.spec import FeatureSpec, FeatureState, FeatureValue

MAX_RECORDED_EXAMPLES: Final = 50
"""Divergences and declared exceptions kept verbatim in a tally; every one is counted."""


class Pairing(StrEnum):
    AS_SERVED = "AS_SERVED"
    """The online store's served value against the reference's AS_SERVED evaluation."""
    EVENT_TIME_COMPLETE = "EVENT_TIME_COMPLETE"
    """Gold's value against the reference's EVENT_TIME_COMPLETE evaluation."""


@dataclass(frozen=True, slots=True)
class Observed:
    """One side's answer for one feature."""

    state: str
    value: float | None
    lookback_completeness: str | None = None
    """Compared when both sides carry it (the served entry does, ADR-0051 §6)."""

    def __post_init__(self) -> None:
        available = self.state == FeatureState.AVAILABLE.value
        if available != (self.value is not None):
            raise ValueError(
                f"state {self.state} with value {self.value!r}: only AVAILABLE carries a value"
            )
        if self.value is not None and not math.isfinite(self.value):
            raise ValueError(f"a feature value must be finite, got {self.value!r}")

    @classmethod
    def of(cls, feature: FeatureValue, lookback_completeness: str | None = None) -> Observed:
        return cls(feature.state.value, feature.or_none(), lookback_completeness)

    @classmethod
    def served(cls, entry: Mapping[str, Any]) -> Observed:
        value = entry.get("value")
        completeness = entry.get("lookback_completeness")
        return cls(
            str(entry["state"]),
            None if value is None else float(value),
            None if completeness is None else str(completeness),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "value": self.value,
            "lookback_completeness": self.lookback_completeness,
        }


class Outcome(StrEnum):
    EQUAL = "EQUAL"
    DIVERGENT = "DIVERGENT"
    ESTIMATED = "ESTIMATED"
    """An estimator's value at a non-zero true cardinality: judged by its stratum's RMS bound."""
    DECLARED_EXCEPTION = "DECLARED_EXCEPTION"
    """An absence served in a situation ADR-0046 §8 declares, against the reference's number."""
    NOT_VOUCHED = "NOT_VOUCHED"
    """An absence the as-served read did not vouch for: excluded from parity, never dropped."""


@dataclass(frozen=True, slots=True)
class Judgement:
    outcome: Outcome
    reason: str | None = None
    relative_error: float | None = None
    stratum: str | None = None


def judge(
    comparison: ParityComparison,
    pairing: Pairing,
    implementation: Observed,
    reference: Observed,
) -> Judgement:
    """One comparison. `reference` is the reference implementation's answer."""
    if implementation.state != reference.state:
        return Judgement(Outcome.DIVERGENT, "state")
    if (
        implementation.lookback_completeness is not None
        and reference.lookback_completeness is not None
        and implementation.lookback_completeness != reference.lookback_completeness
    ):
        return Judgement(Outcome.DIVERGENT, "lookback_completeness")
    if reference.value is None or implementation.value is None:
        return Judgement(Outcome.EQUAL)
    got, want = implementation.value, reference.value
    exact = comparison is ParityComparison.EXACT or (
        comparison is ParityComparison.APPROXIMATE and pairing is Pairing.EVENT_TIME_COMPLETE
    )
    if exact:
        same = got == want
        return Judgement(Outcome.EQUAL if same else Outcome.DIVERGENT, None if same else "value")
    if comparison is ParityComparison.FLOAT:
        close = math.isclose(got, want, rel_tol=FLOAT_PARITY_RELATIVE_TOLERANCE, abs_tol=0.0)
        return Judgement(Outcome.EQUAL if close else Outcome.DIVERGENT, None if close else "value")
    if want < 0:
        return Judgement(Outcome.DIVERGENT, "negative_cardinality")
    if want == 0:
        zero = got == 0
        return Judgement(
            Outcome.EQUAL if zero else Outcome.DIVERGENT,
            None if zero else "zero_cardinality",
            stratum=ZERO_STRATUM,
        )
    return Judgement(
        Outcome.ESTIMATED, relative_error=(got - want) / want, stratum=stratum_of(want)
    )


def absent_against_a_number(implementation: Observed | None, reference: Observed) -> bool:
    return (
        implementation is not None
        and implementation.state == FeatureState.INSUFFICIENT_HISTORY.value
        and reference.value is not None
    )


def excused(implementation: Observed | None, reference: Observed, declared: frozenset[str]) -> bool:
    """Whether a divergence is one of the declared situations' absences, and nothing more."""
    return bool(declared) and absent_against_a_number(implementation, reference)


def unvouched(implementation: Observed | None, reference: Observed) -> bool:
    """Whether the as-served read served an absence it did not vouch for (ADR-0046 §5).

    Each online structure holds its widest window plus the late-arrival margin; a read reaching
    behind that serves absence and stops vouching for the window. The reference models no
    retention, so such a comparison measures how far the corpus reaches behind what the store still
    held -- not a divergence. It is excluded and counted, per feature and window, never dropped.
    A value served while unvouched is still compared: serving a number is a claim."""
    return (
        absent_against_a_number(implementation, reference)
        and implementation is not None
        and implementation.lookback_completeness != Completeness.COMPLETE.value
    )


def window_of(spec: FeatureSpec) -> str:
    shape = spec.semantics
    return shape.window.label if isinstance(shape, WindowedAggregate) else "-"


@dataclass
class FeatureCounts:
    compared: int = 0
    equal: int = 0
    absent_equal: int = 0
    divergent: int = 0
    estimated: int = 0
    declared_exception: int = 0
    not_vouched: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "compared": self.compared,
            "equal": self.equal,
            "absent_equal": self.absent_equal,
            "divergent": self.divergent,
            "estimated": self.estimated,
            "declared_exception": self.declared_exception,
            "not_vouched": self.not_vouched,
        }


@dataclass
class StratumCounts:
    count: int = 0
    sum_squared_relative_error: float = 0.0
    max_abs_relative_error: float = 0.0

    @property
    def rms(self) -> float | None:
        return math.sqrt(self.sum_squared_relative_error / self.count) if self.count else None

    @property
    def within_bound(self) -> bool | None:
        rms = self.rms
        return None if rms is None else rms <= APPROXIMATE_RMS_BOUND

    def as_dict(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "rms_relative_error": self.rms,
            "max_abs_relative_error": self.max_abs_relative_error,
            "within_bound": self.within_bound,
        }


@dataclass
class ParityTally:
    """Every comparison of one pairing, counted per feature and, for estimators, per stratum."""

    pairing: Pairing
    features: dict[str, FeatureCounts] = field(default_factory=dict)
    strata: dict[str, dict[str, StratumCounts]] = field(default_factory=dict)
    skipped: Counter[str] = field(default_factory=Counter)
    divergences: list[dict[str, Any]] = field(default_factory=list)
    divergent_total: int = 0
    declared: Counter[str] = field(default_factory=Counter)
    """Declared exceptions by situation (a comparison in several situations counts under each)."""
    declared_examples: list[dict[str, Any]] = field(default_factory=list)
    not_vouched: Counter[str] = field(default_factory=Counter)
    """Excluded absences the as-served read did not vouch for, by `feature_id window`."""

    def add(
        self,
        transaction_id: str,
        spec: FeatureSpec,
        implementation: Observed | None,
        reference: Observed,
        *,
        declared: frozenset[str] = frozenset(),
        detail: Mapping[str, Any] | None = None,
    ) -> Judgement:
        """Judge one comparison. A feature the implementation did not report at all diverges.

        `declared`: the ADR-0046 §8 situations this read is in, for this feature."""
        counts = self.features.setdefault(spec.feature_id, FeatureCounts())
        counts.compared += 1
        if implementation is None:
            judgement = Judgement(Outcome.DIVERGENT, "not_reported")
        else:
            judgement = judge(spec.parity, self.pairing, implementation, reference)
        if judgement.outcome is Outcome.DIVERGENT and unvouched(implementation, reference):
            judgement = Judgement(Outcome.NOT_VOUCHED, judgement.reason)
        elif judgement.outcome is Outcome.DIVERGENT and excused(
            implementation, reference, declared
        ):
            judgement = Judgement(Outcome.DECLARED_EXCEPTION, judgement.reason)

        def example() -> dict[str, Any]:
            return {
                "transaction_id": transaction_id,
                "feature_id": spec.feature_id,
                "reason": judgement.reason,
                "implementation": None if implementation is None else implementation.as_dict(),
                "reference": reference.as_dict(),
                **({} if detail is None else {"detail": dict(detail)}),
            }

        if judgement.outcome is Outcome.NOT_VOUCHED:
            counts.compared -= 1  # excluded from parity, and counted below
            counts.not_vouched += 1
            self.not_vouched[f"{spec.feature_id} {window_of(spec)}"] += 1
            if declared:
                self.declared.update(sorted(declared))
        elif judgement.outcome is Outcome.EQUAL:
            counts.equal += 1
            if reference.value is None:
                counts.absent_equal += 1
        elif judgement.outcome is Outcome.DECLARED_EXCEPTION:
            counts.declared_exception += 1
            self.declared.update(sorted(declared))
            if len(self.declared_examples) < MAX_RECORDED_EXAMPLES:
                self.declared_examples.append({**example(), "declared": sorted(declared)})
        elif judgement.outcome is Outcome.DIVERGENT:
            counts.divergent += 1
            self.divergent_total += 1
            if len(self.divergences) < MAX_RECORDED_EXAMPLES:
                self.divergences.append(example())
        else:
            counts.estimated += 1
            assert judgement.stratum is not None and judgement.relative_error is not None
            stratum = self.strata.setdefault(spec.feature_id, {}).setdefault(
                judgement.stratum, StratumCounts()
            )
            stratum.count += 1
            stratum.sum_squared_relative_error += judgement.relative_error**2
            stratum.max_abs_relative_error = max(
                stratum.max_abs_relative_error, abs(judgement.relative_error)
            )
        return judgement

    def skip(self, reason: str, count: int = 1) -> None:
        self.skipped[reason] += count

    @property
    def compared(self) -> int:
        return sum(c.compared for c in self.features.values())

    @property
    def declared_total(self) -> int:
        return sum(c.declared_exception for c in self.features.values())

    @property
    def not_vouched_total(self) -> int:
        return sum(c.not_vouched for c in self.features.values())

    def not_vouched_fraction(self) -> float | None:
        """Excluded absences over every comparison the run offered (ADR-0056 §3)."""
        offered = self.compared + self.not_vouched_total
        return None if offered == 0 else self.not_vouched_total / offered

    def approximate_results(self, features: Iterable[str]) -> dict[str, dict[str, Any]]:
        """Per approximate feature: every declared stratum, exercised or not."""
        results: dict[str, dict[str, Any]] = {}
        for feature_id in sorted(features):
            strata = self.strata.get(feature_id, {})
            results[feature_id] = {
                "overall": sum(s.count for s in strata.values()),
                "strata": {
                    name: strata[name].as_dict() if name in strata else {"count": 0}
                    for name, _low, _high in STRATA
                },
                "unexercised": [name for name, _low, _high in STRATA if name not in strata],
            }
        return results

    def approximate_failures(self) -> list[str]:
        return [
            f"{feature_id} stratum {name}: RMS relative error {counts.rms:.6f} over "
            f"{counts.count} comparisons exceeds {APPROXIMATE_RMS_BOUND}"
            for feature_id, strata in sorted(self.strata.items())
            for name, counts in sorted(strata.items())
            if counts.within_bound is False and counts.rms is not None
        ]

    def as_dict(self, approximate_features: Iterable[str] = ()) -> dict[str, Any]:
        return {
            "pairing": self.pairing.value,
            "compared": self.compared,
            "divergent": self.divergent_total,
            "declared_exceptions": self.declared_total,
            "not_vouched_excluded": self.not_vouched_total,
            "not_vouched_fraction": self.not_vouched_fraction(),
            "not_vouched_by_feature_window": dict(sorted(self.not_vouched.items())),
            "declared_exceptions_by_situation": dict(sorted(self.declared.items())),
            "features": {k: v.as_dict() for k, v in sorted(self.features.items())},
            "approximate": self.approximate_results(approximate_features),
            "approximate_failures": self.approximate_failures(),
            "skipped": dict(sorted(self.skipped.items())),
            "divergences": list(self.divergences),
            "declared_exception_examples": list(self.declared_examples),
        }
