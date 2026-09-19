"""Arrival-skew arithmetic, its feature set, capped windows and the gate (ADR-0056 §4)."""

from __future__ import annotations

import pytest
from eval.parity.comparator import Observed
from eval.parity.skew import (
    SkewClass,
    SkewTally,
    SkewVerdict,
    capped_content_features,
    classify,
    windowed_counters,
)

pytestmark = pytest.mark.parity

ABSENT = Observed("INSUFFICIENT_HISTORY", None, "INCOMPLETE")


def available(value: float) -> Observed:
    return Observed("AVAILABLE", value, "COMPLETE")


def test_windowed_counters_are_the_exact_windowed_counts_of_the_released_set() -> None:
    assert windowed_counters() == (
        "account_distinct_countries_24h",
        "account_distinct_devices_24h",
        "account_distinct_mcc_5m",
        "account_distinct_merchants_1h",
        "account_tx_count_1h",
        "account_tx_count_1m",
        "account_tx_count_24h",
        "account_tx_count_5m",
        "card_tx_count_5m",
        "device_distinct_accounts_24h",
        "failed_logins_1h",
    )


def test_capped_content_features_are_exactly_adr_0046_section_8s_list() -> None:
    assert capped_content_features() == {
        "account_amount_sum_1h",
        "account_distinct_merchants_1h",
        "account_distinct_mcc_5m",
        "account_distinct_devices_24h",
        "account_distinct_countries_24h",
        "device_distinct_accounts_24h",
    }


def test_the_denominator_is_comparisons_where_either_side_has_a_value() -> None:
    tally = SkewTally()
    tally.add("a", classify(available(2.0), available(2.0), capped=False))
    tally.add("a", classify(available(1.0), available(2.0), capped=False))
    tally.add("a", classify(ABSENT, available(1.0), capped=False))
    tally.add("a", classify(ABSENT, ABSENT, capped=False))
    assert (tally.different, tally.denominator) == (2, 3)
    assert tally.fraction() == pytest.approx(2 / 3)


def test_a_capped_window_is_excluded_and_counted() -> None:
    tally = SkewTally()
    tally.add("account_distinct_devices_24h", classify(ABSENT, available(9.0), capped=True))
    tally.add("account_distinct_devices_24h", classify(ABSENT, available(9.0), capped=False))
    assert classify(ABSENT, available(9.0), capped=True) is SkewClass.CAPPED
    assert (tally.capped, tally.different, tally.denominator) == (1, 1, 1)


def _tally(different: int, same: int) -> SkewTally:
    tally = SkewTally()
    for _ in range(different):
        tally.add("account_tx_count_1h", SkewClass.DIFFERENT)
    for _ in range(same):
        tally.add("account_tx_count_1h", SkewClass.SAME)
    return tally


def test_the_gate_is_strictly_below_one_percent_and_only_on_a_gated_measured_run() -> None:
    assert _tally(9, 991).verdict(gated=True, measured=True) is SkewVerdict.PASS
    assert _tally(10, 990).verdict(gated=True, measured=True) is SkewVerdict.FAIL
    assert _tally(10, 990).verdict(gated=False, measured=True) is SkewVerdict.RECORDED
    assert _tally(10, 990).verdict(gated=True, measured=False) is SkewVerdict.RECORDED
    assert SkewTally().verdict(gated=True, measured=True) is SkewVerdict.NO_DATA
