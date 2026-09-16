"""The collection guard fails every way a parity run can prove nothing (ADR-0056 §5)."""

from __future__ import annotations

import pytest
from eval.parity.comparator import FeatureCounts, Pairing, ParityTally, StratumCounts
from eval.parity.guard import GuardKind, SparkEvidence, collection_violations
from eval.parity.skew import SkewClass, SkewTally

pytestmark = pytest.mark.parity

HLL = "ip_distinct_accounts_1h"
EVIDENCE = SparkEvidence(
    "4.0.1", {"tx.scored.v1": 3}, {"tx.scored.v1": 3}, 0, {"gold.tx_windows": 9}
)


def _served(strata: dict[str, int]) -> ParityTally:
    tally = ParityTally(Pairing.AS_SERVED)
    tally.features[HLL] = FeatureCounts(compared=max(1, sum(strata.values())))
    for name, count in strata.items():
        tally.strata.setdefault(HLL, {})[name] = StratumCounts(count=count)
    return tally


def _complete() -> ParityTally:
    tally = ParityTally(Pairing.EVENT_TIME_COMPLETE)
    tally.features["account_tx_count_1h"] = FeatureCounts(compared=1, equal=1)
    return tally


def _skew() -> SkewTally:
    skew = SkewTally()
    skew.add("account_tx_count_1h", SkewClass.SAME)
    return skew


def _kinds(served: ParityTally, **kwargs: object) -> list[GuardKind]:
    arguments: dict[str, object] = {
        "tallies": (served, _complete()),
        "approximate_features": (HLL,),
        "skew": _skew(),
        "spark": EVIDENCE,
        "gated": True,
    }
    arguments.update(kwargs)
    return [v.kind for v in collection_violations(**arguments)]  # type: ignore[arg-type]


def test_a_run_with_enough_of_everything_passes_the_guard() -> None:
    assert _kinds(_served({"1-9": 1_000, "10-99": 200})) == []


def test_comparing_nothing_and_skipping_everything_are_distinct_vacuities() -> None:
    empty = ParityTally(Pairing.EVENT_TIME_COMPLETE)
    skipped = ParityTally(Pairing.EVENT_TIME_COMPLETE)
    skipped.skip("no_gold_context", 4)
    served = _served({"1-9": 1_000})
    assert GuardKind.NOTHING_COMPARED in _kinds(served, tallies=(served, empty))
    assert GuardKind.ALL_SKIPPED in _kinds(served, tallies=(served, skipped))


def test_an_exercised_stratum_below_its_minimum_fails_and_an_unexercised_one_is_not_gated() -> None:
    assert _kinds(_served({"1-9": 1_000, "10-99": 199})) == [GuardKind.STRATUM_BELOW_MINIMUM]
    assert _kinds(_served({"1-9": 1_000, "10-99": 0})) == []


def test_too_few_comparisons_overall_fails() -> None:
    assert _kinds(_served({"1-9": 999})) == [GuardKind.OVERALL_BELOW_MINIMUM]


def test_a_run_without_evidence_that_spark_executed_fails() -> None:
    served = _served({"1-9": 1_000})
    assert _kinds(served, spark=None) == [GuardKind.SPARK_NOT_EXECUTED]
    no_build = SparkEvidence("4.0.1", {"t": 1}, {"t": 1}, None, {"gold.tx_windows": 1})
    no_windows = SparkEvidence("4.0.1", {"t": 1}, {"t": 1}, 0, {"gold.tx_windows": 0})
    assert _kinds(served, spark=no_build) == [GuardKind.SPARK_NOT_EXECUTED]
    assert _kinds(served, spark=no_windows) == [GuardKind.SPARK_NOT_EXECUTED]


def test_a_gated_partition_that_classified_no_arrival_skew_is_vacuous() -> None:
    served = _served({"1-9": 1_000})
    empty = SkewTally()
    empty.exclude("warmup_transactions_compressed", 5)
    assert _kinds(served, skew=empty) == [GuardKind.NO_ARRIVAL_SKEW]
    assert _kinds(served, skew=empty, gated=False) == []
