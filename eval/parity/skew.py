"""Arrival skew: how often a served value differs from what the complete history says (ADR-0056 §4).

A separate metric from implementation parity (PHASE3_PLAN §2 B7). Where implementation parity asks
whether two implementations agree on identical input, arrival skew asks how much the as-served
answer, which sees only what had arrived, departs from the event-time-complete answer.

- **Features.** The windowed counters: every windowed COUNT, and every windowed DISTINCT_COUNT with
  EXACT storage. Their online values are exact, so a difference is attributable to arrival and to
  nothing else. HyperLogLog counts are left to implementation parity's error bound, and sums,
  ratios and the coefficient of variation are not counters. Derived from the declarations, so a
  newly declared counter joins without an edit.
- **Denominator.** Comparisons where either side has a value (PHASE3_PLAN §4.3).
- **Capped windows** (ADR-0046 §8). A content window whose score-time read was capped is served
  absent. It is excluded from the metric and counted. Which comparisons were capped is taken from
  the reference's AS_SERVED evaluation of the same read (`FeatureValue.depth_capped`), which
  implementation parity has already required the store to match.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Final

from eval.parity.comparator import Observed
from eval.parity.partition import ARRIVAL_SKEW_BOUND

from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.semantics import (
    Aggregation,
    CardinalityStorage,
    WindowedAggregate,
    reads_observation_content,
)
from trace_core.features.spec import FeatureRegistry

HISTORY_DEPTH_CAPPED: Final = "history_depth_capped"


def windowed_counters(registry: FeatureRegistry = ONLINE_FEATURES) -> tuple[str, ...]:
    return tuple(
        sorted(
            spec.feature_id
            for spec in registry
            if isinstance(spec.semantics, WindowedAggregate)
            and (
                spec.semantics.aggregation is Aggregation.COUNT
                or (
                    spec.semantics.aggregation is Aggregation.DISTINCT_COUNT
                    and spec.semantics.storage is CardinalityStorage.EXACT
                )
            )
        )
    )


def capped_content_features(registry: FeatureRegistry = ONLINE_FEATURES) -> frozenset[str]:
    """The windowed features an as-served read can cap (`semantics.reads_observation_content`)."""
    return frozenset(
        spec.feature_id
        for spec in registry
        if isinstance(spec.semantics, WindowedAggregate)
        and reads_observation_content(spec.semantics)
    )


class SkewClass(StrEnum):
    SAME = "SAME"
    DIFFERENT = "DIFFERENT"
    NEITHER_HAS_VALUE = "NEITHER_HAS_VALUE"
    CAPPED = "CAPPED"


def classify(served: Observed, complete: Observed, *, capped: bool) -> SkewClass:
    if capped:
        return SkewClass.CAPPED
    if served.value is None and complete.value is None:
        return SkewClass.NEITHER_HAS_VALUE
    if served.value is None or complete.value is None:
        return SkewClass.DIFFERENT
    return SkewClass.SAME if served.value == complete.value else SkewClass.DIFFERENT


class SkewVerdict(StrEnum):
    PASS = "PASS"  # noqa: S105 -- a verdict, not a credential
    FAIL = "FAIL"
    RECORDED = "RECORDED"
    """Measured on an ungated partition, or in a diagnostic run: recorded, never gated."""
    NO_DATA = "NO_DATA"


@dataclass
class SkewTally:
    features: dict[str, Counter[str]] = field(default_factory=dict)
    excluded: Counter[str] = field(default_factory=Counter)
    """Subjects or comparisons never classified, by reason."""

    def add(self, feature_id: str, verdict: SkewClass) -> None:
        self.features.setdefault(feature_id, Counter())[verdict.value] += 1

    def exclude(self, reason: str, count: int = 1) -> None:
        self.excluded[reason] += count

    def _total(self, verdict: SkewClass) -> int:
        return sum(c[verdict.value] for c in self.features.values())

    @property
    def different(self) -> int:
        return self._total(SkewClass.DIFFERENT)

    @property
    def denominator(self) -> int:
        return self._total(SkewClass.SAME) + self.different

    @property
    def capped(self) -> int:
        return self._total(SkewClass.CAPPED)

    def fraction(self) -> float | None:
        return None if self.denominator == 0 else self.different / self.denominator

    def verdict(self, *, gated: bool, measured: bool) -> SkewVerdict:
        fraction = self.fraction()
        if fraction is None:
            return SkewVerdict.NO_DATA
        if not (gated and measured):
            return SkewVerdict.RECORDED
        return SkewVerdict.PASS if fraction < ARRIVAL_SKEW_BOUND else SkewVerdict.FAIL

    def as_dict(self, *, gated: bool, measured: bool) -> dict[str, Any]:
        return {
            "features": sorted(self.features),
            "numerator": self.different,
            "denominator": self.denominator,
            "fraction": self.fraction(),
            "bound": ARRIVAL_SKEW_BOUND,
            "capped_excluded": self.capped,
            "neither_has_value": self._total(SkewClass.NEITHER_HAS_VALUE),
            "per_feature": {k: dict(sorted(v.items())) for k, v in sorted(self.features.items())},
            "excluded": dict(sorted(self.excluded.items())),
            "gated": gated,
            "verdict": self.verdict(gated=gated, measured=measured).value,
        }
