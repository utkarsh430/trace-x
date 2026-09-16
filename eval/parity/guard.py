"""The collection guard: a parity run that proved nothing must not look like one that passed.

PHASE3_PLAN §4.4 requires every acceptance command to fail when it compares nothing or when every
selected comparison was skipped. For parity the guard also fails when an exercised stratum of an
approximate feature is below its minimum, when an approximate feature has too few comparisons
overall, when a gated partition classified no arrival-skew comparison, and when Spark did not
execute.

Vacuity violations fail every run. Sample-size violations fail a measured run; a diagnostic run
records them, because a small development run cannot reach the minimums and is never publishable.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from eval.parity.comparator import Pairing, ParityTally
from eval.parity.partition import MIN_COMPARISONS_OVERALL, MIN_COMPARISONS_PER_STRATUM
from eval.parity.skew import SkewTally


@dataclass(frozen=True)
class SparkEvidence:
    """What Spark demonstrably did: rows it counted in each table it wrote, and the Gold build."""

    spark_version: str
    bronze_rows: Mapping[str, int]
    silver_rows: Mapping[str, int]
    gold_build_id: int | None
    gold_rows: Mapping[str, int]

    def executed(self) -> bool:
        return (
            bool(self.spark_version)
            and self.gold_build_id is not None
            and sum(self.bronze_rows.values()) > 0
            and sum(self.silver_rows.values()) > 0
            and self.gold_rows.get("gold.tx_windows", 0) > 0
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "spark_version": self.spark_version,
            "bronze_rows": dict(self.bronze_rows),
            "silver_rows": dict(self.silver_rows),
            "gold_build_id": self.gold_build_id,
            "gold_rows": dict(self.gold_rows),
            "executed": self.executed(),
        }


class GuardKind(StrEnum):
    NOTHING_COMPARED = "NOTHING_COMPARED"
    ALL_SKIPPED = "ALL_SKIPPED"
    SPARK_NOT_EXECUTED = "SPARK_NOT_EXECUTED"
    NO_ARRIVAL_SKEW = "NO_ARRIVAL_SKEW"
    STRATUM_BELOW_MINIMUM = "STRATUM_BELOW_MINIMUM"
    OVERALL_BELOW_MINIMUM = "OVERALL_BELOW_MINIMUM"


VACUITY = frozenset(
    {
        GuardKind.NOTHING_COMPARED,
        GuardKind.ALL_SKIPPED,
        GuardKind.SPARK_NOT_EXECUTED,
        GuardKind.NO_ARRIVAL_SKEW,
    }
)


@dataclass(frozen=True)
class GuardViolation:
    kind: GuardKind
    detail: str

    @property
    def vacuity(self) -> bool:
        return self.kind in VACUITY

    def as_dict(self) -> dict[str, str]:
        return {"kind": self.kind.value, "detail": self.detail}


def collection_violations(
    *,
    tallies: Sequence[ParityTally],
    approximate_features: Sequence[str],
    skew: SkewTally,
    spark: SparkEvidence | None,
    gated: bool,
) -> list[GuardViolation]:
    violations: list[GuardViolation] = []
    for tally in tallies:
        if tally.compared == 0:
            kind = GuardKind.ALL_SKIPPED if tally.skipped else GuardKind.NOTHING_COMPARED
            violations.append(
                GuardViolation(
                    kind, f"{tally.pairing}: 0 comparisons; skipped {dict(tally.skipped)}"
                )
            )
        if tally.pairing is not Pairing.AS_SERVED:
            continue
        for feature_id in approximate_features:
            strata = tally.strata.get(feature_id, {})
            overall = sum(s.count for s in strata.values())
            if overall < MIN_COMPARISONS_OVERALL:
                violations.append(
                    GuardViolation(
                        GuardKind.OVERALL_BELOW_MINIMUM,
                        f"{feature_id}: {overall} comparisons at non-zero cardinality, "
                        f"below {MIN_COMPARISONS_OVERALL}",
                    )
                )
            for name, counts in sorted(strata.items()):
                if 0 < counts.count < MIN_COMPARISONS_PER_STRATUM:
                    violations.append(
                        GuardViolation(
                            GuardKind.STRATUM_BELOW_MINIMUM,
                            f"{feature_id} stratum {name}: {counts.count} comparisons, below "
                            f"{MIN_COMPARISONS_PER_STRATUM}",
                        )
                    )
    if spark is None or not spark.executed():
        violations.append(
            GuardViolation(
                GuardKind.SPARK_NOT_EXECUTED,
                "no evidence Spark wrote Bronze, Silver and a Gold build"
                if spark is None
                else f"Spark evidence incomplete: {spark.as_dict()}",
            )
        )
    if gated and skew.denominator == 0:
        violations.append(
            GuardViolation(
                GuardKind.NO_ARRIVAL_SKEW,
                f"the gated partition classified no comparison; excluded {dict(skew.excluded)}",
            )
        )
    return violations
