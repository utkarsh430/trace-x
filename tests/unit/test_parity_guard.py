"""The collection guard fails every way a parity run can prove nothing (ADR-0056 §5)."""

from __future__ import annotations

import pytest
from eval.parity.comparator import (
    FeatureCounts,
    Observed,
    Outcome,
    Pairing,
    ParityTally,
    StratumCounts,
)
from eval.parity.guard import (
    MIN_SKEW_COMPARISONS,
    GuardKind,
    SparkEvidence,
    collection_violations,
)
from eval.parity.partition import (
    MAX_EXCUSED_FRACTION_PER_FEATURE,
    MAX_UNCOMPARED_FRACTION_PER_FEATURE,
    MIN_COMPARISONS_PER_STRATUM,
)
from eval.parity.skew import SkewClass, SkewTally

from trace_core.features.definitions import ONLINE_FEATURES

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


def _skew(comparisons: int = MIN_SKEW_COMPARISONS) -> SkewTally:
    skew = SkewTally()
    for _ in range(comparisons):
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


HEALTHY = {"1-9": 1_000, "10-99": 200}


def test_a_feature_whose_comparisons_were_mostly_excluded_cannot_support_a_claim() -> None:
    """Nothing else in the guard bounds the exclusions.

    ADR-0046 §5 excludes an absence the store stopped vouching for, and `ParityTally.add`
    subtracts each one from `compared`. ADR-0056 §3 F1 expects that to be material on the
    representative partition's late band. But no verdict reads `not_vouched_fraction`, and the
    volume floors bind only the approximate features -- so a run could exclude nearly every
    comparison of an EXACT feature, report zero divergences, and pass.
    """
    served = _served(HEALTHY)
    served.features["card_tx_count_5m"] = FeatureCounts(compared=5, not_vouched=600)
    assert GuardKind.FEATURE_MOSTLY_EXCLUDED in _kinds(served)


def test_material_exclusions_that_still_leave_a_feature_compared_are_not_a_violation() -> None:
    """F1 expects exclusions. The bound catches a feature that was not compared, not the
    presence of exclusions."""
    served = _served(HEALTHY)
    served.features["card_tx_count_5m"] = FeatureCounts(compared=600, not_vouched=5)
    assert _kinds(served) == []


def test_the_exclusion_bound_binds_the_event_time_complete_pairing_too() -> None:
    """The exclusion is the store's, but the bound is about evidence, and Gold needs it as much."""
    complete = _complete()
    complete.features["account_tx_count_1h"] = FeatureCounts(compared=3, not_vouched=400)
    served = _served(HEALTHY)
    assert GuardKind.FEATURE_MOSTLY_EXCLUDED in _kinds(served, tallies=(served, complete))


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


def test_the_arrival_skew_floor_is_the_stratum_floor() -> None:
    assert MIN_SKEW_COMPARISONS == MIN_COMPARISONS_PER_STRATUM == 200


def test_one_agreeing_arrival_skew_comparison_cannot_pass_a_gated_partition() -> None:
    """Mutation: before the floor, a gated partition with a denominator of 1 had skew 0 and
    passed. It is a sample-size violation, not a vacuity: a measured run fails on it."""
    one = _skew(1)
    assert one.denominator == 1 and one.fraction() == 0.0
    violations = collection_violations(
        tallies=(_served(HEALTHY), _complete()),
        approximate_features=(HLL,),
        skew=one,
        spark=EVIDENCE,
        gated=True,
    )
    assert [v.kind for v in violations] == [GuardKind.ARRIVAL_SKEW_BELOW_MINIMUM]
    assert not violations[0].vacuity


@pytest.mark.parametrize(
    ("comparisons", "expected"),
    [
        (MIN_SKEW_COMPARISONS - 1, [GuardKind.ARRIVAL_SKEW_BELOW_MINIMUM]),
        (MIN_SKEW_COMPARISONS, []),
    ],
)
def test_the_arrival_skew_floor_is_inclusive(comparisons: int, expected: list[GuardKind]) -> None:
    assert _kinds(_served(HEALTHY), skew=_skew(comparisons)) == expected


def test_a_small_arrival_skew_denominator_is_not_gated_on_an_ungated_partition() -> None:
    assert _kinds(_served(HEALTHY), skew=_skew(1), gated=False) == []


def test_an_empty_gated_skew_is_reported_once_as_vacuity_not_also_as_small() -> None:
    assert _kinds(_served(HEALTHY), skew=SkewTally()) == [GuardKind.NO_ARRIVAL_SKEW]


@pytest.mark.parametrize(
    ("excused", "expected"),
    [(51, [GuardKind.FEATURE_MOSTLY_EXCUSED]), (50, [])],
)
def test_a_feature_mostly_excused_by_declared_exceptions_fails_the_guard(
    excused: int, expected: list[GuardKind]
) -> None:
    """0.51 of the offered comparisons excused is a violation; exactly 0.5 is not."""
    assert MAX_EXCUSED_FRACTION_PER_FEATURE == 0.5
    served = _served(HEALTHY)
    served.features["card_tx_count_5m"] = FeatureCounts(
        compared=100, equal=100 - excused, declared_exception=excused
    )
    assert _kinds(served) == expected


def test_the_excused_bound_binds_the_event_time_complete_pairing_too() -> None:
    complete = _complete()
    complete.features["account_tx_count_1h"] = FeatureCounts(
        compared=100, equal=49, declared_exception=51
    )
    served = _served(HEALTHY)
    assert _kinds(served, tallies=(served, complete)) == [GuardKind.FEATURE_MOSTLY_EXCUSED]


def test_the_excused_fraction_is_over_offered_comparisons_including_unvouched() -> None:
    """Offered is compared plus excluded, as for the exclusion bound: 50 excused of 100 offered
    (60 compared, 40 unvouched) is at the excused bound, not above it -- but only 10 of the 100
    were real comparisons, so the combined bound fires (it did not exist when this was written)."""
    served = _served(HEALTHY)
    served.features["card_tx_count_5m"] = FeatureCounts(
        compared=60, equal=10, declared_exception=50, not_vouched=40
    )
    assert _kinds(served) == [GuardKind.FEATURE_MOSTLY_UNCOMPARED]


@pytest.mark.parametrize(
    ("not_vouched", "excused", "expected"),
    [
        (25, 26, [GuardKind.FEATURE_MOSTLY_UNCOMPARED]),
        (25, 25, []),
        (50, 50, [GuardKind.FEATURE_MOSTLY_UNCOMPARED]),
        (60, 0, [GuardKind.FEATURE_MOSTLY_EXCLUDED]),  # reported once, by its own bound
        (0, 50, []),
    ],
)
def test_unvouched_and_excused_together_may_not_exceed_half_of_a_features_comparisons(
    not_vouched: int, excused: int, expected: list[GuardKind]
) -> None:
    """Each bound alone let a feature through with half excluded and half excused: zero real
    comparisons. Offered = compared + unvouched = 100 here."""
    assert MAX_UNCOMPARED_FRACTION_PER_FEATURE == 0.5
    compared = 100 - not_vouched
    served = _served(HEALTHY)
    served.features["card_tx_count_5m"] = FeatureCounts(
        compared=compared,
        equal=compared - excused,
        declared_exception=excused,
        not_vouched=not_vouched,
    )
    assert _kinds(served) == expected


def test_declared_exceptions_judged_by_the_comparator_trip_the_excused_bound() -> None:
    """Through `ParityTally.add`: a store that serves absence for most reads, each in a declared
    situation, has zero divergences but cannot pass the guard, and the record carries the
    per-feature count."""
    spec = ONLINE_FEATURES.get("card_tx_count_5m")
    absent = Observed("INSUFFICIENT_HISTORY", None, "COMPLETE")
    number = Observed("AVAILABLE", 2.0, "COMPLETE")
    served = _served(HEALTHY)
    declared = frozenset({"scored_ahead_of_wall_clock"})
    for i in range(51):
        judgement = served.add(f"tx_{i}", spec, absent, number, declared=declared)
        assert judgement.outcome is Outcome.DECLARED_EXCEPTION
    for i in range(51, 100):
        assert served.add(f"tx_{i}", spec, number, number).outcome is Outcome.EQUAL
    assert served.divergent_total == 0
    assert served.as_dict()["features"]["card_tx_count_5m"]["declared_exception"] == 51
    assert _kinds(served) == [GuardKind.FEATURE_MOSTLY_EXCUSED]
