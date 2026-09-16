"""The parity comparator: declared comparisons, absences, strata and the RMS bound (ADR-0056 §3)."""

from __future__ import annotations

import pytest
from eval.parity.comparator import (
    MAX_RECORDED_EXAMPLES,
    Observed,
    Outcome,
    Pairing,
    ParityTally,
    judge,
)
from eval.parity.partition import APPROXIMATE_RMS_BOUND

from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.semantics import FLOAT_PARITY_RELATIVE_TOLERANCE, ParityComparison

pytestmark = pytest.mark.parity

PC = ParityComparison
SERVED = Pairing.AS_SERVED
COMPLETE = Pairing.EVENT_TIME_COMPLETE
ABSENT = Observed("INSUFFICIENT_HISTORY", None)


def available(value: float, completeness: str | None = None) -> Observed:
    return Observed("AVAILABLE", value, completeness)


def test_exact_features_compare_by_equality() -> None:
    assert judge(PC.EXACT, SERVED, available(3.0), available(3.0)).outcome is Outcome.EQUAL
    assert judge(PC.EXACT, SERVED, available(3.0), available(4.0)).reason == "value"


def test_float_tolerance_is_the_declared_relative_one_and_nothing_absolute() -> None:
    want = 1_000.0
    inside = want * (1 + 0.9 * FLOAT_PARITY_RELATIVE_TOLERANCE)
    outside = want * (1 + 1.1 * FLOAT_PARITY_RELATIVE_TOLERANCE)
    assert judge(PC.FLOAT, SERVED, available(inside), available(want)).outcome is Outcome.EQUAL
    assert judge(PC.FLOAT, SERVED, available(outside), available(want)).outcome is Outcome.DIVERGENT
    assert judge(PC.FLOAT, SERVED, available(1e-300), available(0.0)).outcome is Outcome.DIVERGENT


def test_absences_match_only_with_the_same_state() -> None:
    assert judge(PC.EXACT, SERVED, ABSENT, ABSENT).outcome is Outcome.EQUAL
    zero = judge(PC.EXACT, SERVED, available(0.0), ABSENT)
    assert (zero.outcome, zero.reason) == (Outcome.DIVERGENT, "state")
    unavailable = Observed("UNAVAILABLE", None)
    assert judge(PC.EXACT, SERVED, unavailable, ABSENT).reason == "state"


def test_lookback_completeness_must_match_when_both_sides_carry_it() -> None:
    got = available(2.0, "COMPLETE")
    assert (
        judge(PC.EXACT, SERVED, got, available(2.0, "INCOMPLETE")).reason == "lookback_completeness"
    )
    assert judge(PC.EXACT, SERVED, got, available(2.0)).outcome is Outcome.EQUAL


def test_an_approximate_feature_is_exact_where_neither_side_estimates() -> None:
    assert judge(PC.APPROXIMATE, COMPLETE, available(11.0), available(10.0)).reason == "value"


def test_at_true_cardinality_zero_both_sides_are_exactly_zero() -> None:
    assert judge(PC.APPROXIMATE, SERVED, available(0.0), available(0.0)).outcome is Outcome.EQUAL
    assert (
        judge(PC.APPROXIMATE, SERVED, available(1.0), available(0.0)).reason == "zero_cardinality"
    )


def test_an_estimate_is_judged_in_its_stratum() -> None:
    judgement = judge(PC.APPROXIMATE, SERVED, available(101.0), available(100.0))
    assert judgement.outcome is Outcome.ESTIMATED
    assert judgement.stratum == "100-999"
    assert judgement.relative_error == pytest.approx(0.01)


def _hll_tally(relative_error: float, count: int = 200) -> ParityTally:
    spec = ONLINE_FEATURES.get("ip_distinct_accounts_1h")
    tally = ParityTally(SERVED)
    for index in range(count):
        tally.add(f"tx_{index}", spec, available(50.0 * (1 + relative_error)), available(50.0))
    return tally


def test_the_rms_bound_holds_at_one_percent_and_fails_beyond_it() -> None:
    assert _hll_tally(0.009).approximate_failures() == []
    assert _hll_tally(APPROXIMATE_RMS_BOUND).approximate_failures() == []
    failed = _hll_tally(0.02).approximate_failures()
    assert len(failed) == 1 and "10-99" in failed[0]
    strata = _hll_tally(0.02).approximate_results(["ip_distinct_accounts_1h"])
    assert strata["ip_distinct_accounts_1h"]["unexercised"] == ["1-9", "100-999", "1000+"]


def test_a_feature_the_implementation_never_reported_diverges() -> None:
    tally = ParityTally(SERVED)
    judgement = tally.add("tx_1", ONLINE_FEATURES.get("account_tx_count_1h"), None, available(1.0))
    assert judgement.reason == "not_reported" and tally.divergent_total == 1


def test_every_divergence_is_counted_and_only_the_first_are_kept() -> None:
    spec = ONLINE_FEATURES.get("account_tx_count_1h")
    tally = ParityTally(SERVED)
    for index in range(MAX_RECORDED_EXAMPLES + 7):
        tally.add(f"tx_{index}", spec, available(2.0), available(1.0))
    assert tally.divergent_total == MAX_RECORDED_EXAMPLES + 7
    assert len(tally.divergences) == MAX_RECORDED_EXAMPLES


def test_a_side_cannot_carry_a_value_with_an_absent_state() -> None:
    with pytest.raises(ValueError):
        Observed("INSUFFICIENT_HISTORY", 0.0)
    with pytest.raises(ValueError):
        Observed("AVAILABLE", None)


def test_a_declared_situation_excuses_a_vouched_absence_and_nothing_else() -> None:
    """ADR-0046 §8's situations excuse an absence the read still vouched for. An unvouched one is
    excluded by §5's measurement rule instead, and its situation is still counted."""
    spec = ONLINE_FEATURES.get("account_tx_count_24h")
    tally = ParityTally(SERVED)
    situation = frozenset({"late_read_after_fold"})
    vouched_absent = Observed("INSUFFICIENT_HISTORY", None, "COMPLETE")
    number = available(3.0, "COMPLETE")
    declared = tally.add("tx_1", spec, vouched_absent, number, declared=situation)
    assert declared.outcome is Outcome.DECLARED_EXCEPTION
    other_number = tally.add("tx_2", spec, available(2.0, "COMPLETE"), number, declared=situation)
    assert other_number.outcome is Outcome.DIVERGENT, "a declared situation never excuses a number"
    undeclared = tally.add("tx_3", spec, vouched_absent, number)
    assert undeclared.outcome is Outcome.DIVERGENT
    unavailable = tally.add("tx_4", spec, Observed("UNAVAILABLE", None), number, declared=situation)
    assert unavailable.outcome is Outcome.DIVERGENT
    assert (tally.declared_total, tally.divergent_total) == (1, 3)
    assert dict(tally.declared) == {"late_read_after_fold": 1}
    assert tally.as_dict()["declared_exception_examples"][0]["transaction_id"] == "tx_1"


def test_an_unvouched_absence_is_excluded_even_in_a_declared_situation() -> None:
    spec = ONLINE_FEATURES.get("account_distinct_devices_24h")
    tally = ParityTally(SERVED)
    situation = frozenset({"cap_at_scored_millisecond"})
    absent = Observed("INSUFFICIENT_HISTORY", None, "INCOMPLETE")
    excluded = tally.add("tx_1", spec, absent, available(4.0, "COMPLETE"), declared=situation)
    assert excluded.outcome is Outcome.NOT_VOUCHED
    assert (tally.declared_total, tally.not_vouched_total, tally.divergent_total) == (0, 1, 0)
    assert dict(tally.declared) == {"cap_at_scored_millisecond": 1}, (
        "the situation is still counted"
    )


UNVOUCHED = Observed("INSUFFICIENT_HISTORY", None, "INCOMPLETE")
VOUCHED_ABSENT = Observed("INSUFFICIENT_HISTORY", None, "COMPLETE")


def test_an_absence_the_read_did_not_vouch_for_is_excluded_and_counted() -> None:
    """ADR-0046 §5: a read behind what the store still holds serves absence and stops vouching.
    The reference models no retention, so the comparison measures the corpus, not a divergence."""
    spec = ONLINE_FEATURES.get("card_tx_count_5m")
    tally = ParityTally(SERVED)
    excluded = tally.add("tx_1", spec, UNVOUCHED, available(2.0, "COMPLETE"))
    assert excluded.outcome is Outcome.NOT_VOUCHED
    assert (tally.divergent_total, tally.compared, tally.not_vouched_total) == (0, 0, 1)
    assert dict(tally.not_vouched) == {"card_tx_count_5m 5m": 1}
    assert tally.as_dict()["not_vouched_fraction"] == 1.0


def test_a_vouched_absence_against_a_number_is_still_a_divergence() -> None:
    spec = ONLINE_FEATURES.get("card_tx_count_5m")
    tally = ParityTally(SERVED)
    assert tally.add("tx_1", spec, VOUCHED_ABSENT, available(2.0, "COMPLETE")).outcome is (
        Outcome.DIVERGENT
    )
    assert (tally.divergent_total, tally.not_vouched_total) == (1, 0)


def test_a_number_served_while_unvouched_is_still_compared() -> None:
    """Serving a number is a claim, whatever the store says about its lookback."""
    spec = ONLINE_FEATURES.get("card_tx_count_5m")
    tally = ParityTally(SERVED)
    wrong = tally.add("tx_1", spec, available(3.0, "INCOMPLETE"), available(2.0, "INCOMPLETE"))
    right = tally.add("tx_2", spec, available(2.0, "INCOMPLETE"), available(2.0, "INCOMPLETE"))
    assert (wrong.outcome, right.outcome) == (Outcome.DIVERGENT, Outcome.EQUAL)
    assert (tally.divergent_total, tally.not_vouched_total, tally.compared) == (1, 0, 2)


def test_both_sides_absent_is_compared_even_when_unvouched() -> None:
    """An absence on both sides still has to agree on its state."""
    spec = ONLINE_FEATURES.get("card_tx_count_5m")
    tally = ParityTally(SERVED)
    agree = tally.add("tx_1", spec, UNVOUCHED, Observed("INSUFFICIENT_HISTORY", None, "INCOMPLETE"))
    differ = tally.add("tx_2", spec, UNVOUCHED, Observed("UNAVAILABLE", None))
    assert (agree.outcome, differ.outcome) == (Outcome.EQUAL, Outcome.DIVERGENT)
    assert (tally.compared, tally.not_vouched_total) == (2, 0)


def test_a_gold_absence_against_a_reference_number_is_a_divergence() -> None:
    """The unvouched exclusion is the online store's, and must not reach the Gold pairing.

    ADR-0046 §5 excludes an absence the *store* stopped vouching for, because the store has
    retention and the reference does not. Gold models no retention and reports no lookback
    completeness at all (both sides of this pairing are built with `Observed.of`, which leaves it
    `None`), so it never makes a vouching claim that could be excluded. ADR-0055 §3 makes a Gold
    window row exist exactly when the reference returns a state, so a Gold absence against a
    reference number is a defect -- a dropped bucket join, a missing profile or previous row -- and
    has to be counted as one.
    """
    spec = ONLINE_FEATURES.get("account_tx_count_1h")
    tally = ParityTally(COMPLETE)
    missing = tally.add("tx_1", spec, ABSENT, available(3.0))
    assert missing.outcome is Outcome.DIVERGENT, (
        "a Gold absence against a number must not be excluded as unvouched"
    )
    assert (tally.divergent_total, tally.not_vouched_total, tally.compared) == (1, 0, 1)


def test_the_as_served_pairing_still_excludes_an_unvouched_absence() -> None:
    """The companion to the above: gating on the pairing must not weaken §5 where it applies."""
    spec = ONLINE_FEATURES.get("account_tx_count_1h")
    tally = ParityTally(SERVED)
    excluded = tally.add("tx_1", spec, UNVOUCHED, available(3.0, "COMPLETE"))
    assert excluded.outcome is Outcome.NOT_VOUCHED
    assert (tally.divergent_total, tally.not_vouched_total, tally.compared) == (0, 1, 0)
