"""The core rule pack: one test per rule, plus the pack-level invariants.

Declared acceptance command for `P2.rules-engine`:
`pytest tests/unit/test_rules.py`.

Every rule gets three assertions, because two of them would let a broken rule
through:

* it **fires** on inputs matching its documented signature;
* it **does not fire** on inputs just inside its threshold -- without this, a
  rule that fired on everything would pass the first assertion;
* it **abstains** when an input is absent, rather than reading absence as "no".

The thresholds asserted here are the ones in the pack. They are declared
configuration derived from published fraud SIGNATURES, never fitted against
labels: the application role cannot read the `groundtruth` schema at all
(ADR-0004), and Phase 4 selects operating points inside the evaluation harness.
"""

from __future__ import annotations

import datetime as dt
from typing import Final

import pytest

from trace_core.contracts.canonical import CanonicalField, CanonicalTransaction
from trace_core.domain.enums import RiskBand, TransactionChannel
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.spec import FeatureValue
from trace_core.rules.engine import evaluate_pack
from trace_core.rules.grammar import Truth
from trace_core.rules.loader import default_loader
from trace_core.rules.pack import CompiledPack

pytestmark = pytest.mark.unit

KNOWN_FEATURES: Final = frozenset(ONLINE_FEATURES.ids)


@pytest.fixture(scope="module")
def pack() -> CompiledPack:
    return default_loader(KNOWN_FEATURES).load()


def _tx(**over: object) -> CanonicalTransaction:
    fields: dict[str, object] = {
        "merchant_id": "mrch_00001",
        "merchant_mcc": "5411",
        "merchant_country": "GB",
        "device_id": "dev_000001",
        "card_id": "card_000001",
        "ip_id": "ip_00001",
        "latitude": 51.5,
        "longitude": -0.12,
        "channel": TransactionChannel.CARD_PRESENT,
    }
    fields.update(over)
    return CanonicalTransaction(
        source_dataset="rules-test",
        source_row_id="row-1",
        field_coverage=frozenset(CanonicalField),
        transaction_id="tx_0000000001",
        account_id="acct_000001",
        amount_minor=5_000,
        currency="GBP",
        occurred_at=dt.datetime(2026, 3, 1, 12, tzinfo=dt.UTC),
        ingested_at=dt.datetime(2026, 3, 1, 12, 0, 0, 40_000, tzinfo=dt.UTC),
        **fields,  # type: ignore[arg-type]
    )


def _features(**values: float) -> dict[str, FeatureValue]:
    """Only the named features are available; every other one is absent.

    That default matters: it means each test states exactly what its rule needs,
    and a rule quietly depending on something else abstains rather than passing
    on a value the test never set.
    """
    return {k: FeatureValue.of(k, v) for k, v in values.items()}


def outcome(pack: CompiledPack, rule_id: str, features: dict[str, FeatureValue], **tx: object):
    result = evaluate_pack(pack, _tx(**tx), features)
    for item in result.outcomes:
        if item.rule_id == rule_id:
            return item
    raise AssertionError(f"{rule_id} is not in the pack")


# --- per-rule: fires / does not fire / abstains ------------------------------

FIRING: Final[dict[str, dict[str, float]]] = {
    "R001_card_testing_burst": {"account_distinct_mcc_5m": 6, "account_tx_count_5m": 9},
    "R002_declined_ratio_high": {"declined_ratio_1h": 0.5, "account_tx_count_1h": 12},
    "R003_card_probing_velocity": {"card_tx_count_5m": 10},
    "R004_velocity_spike_1m": {"account_tx_count_1m": 5},
    "R005_velocity_spike_5m": {"account_tx_count_5m": 12},
    "R006_velocity_spike_1h": {"account_tx_count_1h": 40},
    "R007_impossible_travel": {"implied_speed_kmh_from_last": 1500},
    "R008_identity_change_then_new_device": {
        "hours_since_identity_change": 2,
        "device_is_known_for_account": 0,
    },
    "R009_new_device_high_value": {
        "device_is_known_for_account": 0,
        "amount_zscore_vs_account": 4,
    },
    "R010_device_shared_across_accounts": {"device_distinct_accounts_24h": 6},
    "R011_ip_shared_across_accounts": {"ip_distinct_accounts_1h": 11},
    "R012_failed_login_burst": {"failed_logins_1h": 25},
    "R013_stuffing_origin_and_spend": {"failed_logins_1h": 6, "ip_distinct_accounts_1h": 7},
    "R014_merchant_uniform_amounts": {
        "merchant_amount_cv_24h": 0.01,
        "merchant_distinct_accounts_1h": 25,
    },
    "R015_high_value_anomaly": {"amount_zscore_vs_account": 7},
    "R016_unhabitual_merchant_and_category": {
        "merchant_is_habitual": 0,
        "mcc_is_habitual_for_account": 0,
        "amount_zscore_vs_account": 3,
    },
    "R017_far_from_home_new_device": {
        "device_is_known_for_account": 0,
        "distance_from_account_home_km": 900,
    },
    "R018_high_risk_category_unfamiliar": {
        "mcc_is_habitual_for_account": 0,
        "account_tenure_days": 5,
    },
}

# Inputs just inside the threshold: the rule must NOT fire.
NOT_FIRING: Final[dict[str, dict[str, float]]] = {
    "R001_card_testing_burst": {"account_distinct_mcc_5m": 4, "account_tx_count_5m": 9},
    "R002_declined_ratio_high": {"declined_ratio_1h": 0.39, "account_tx_count_1h": 12},
    "R003_card_probing_velocity": {"card_tx_count_5m": 9},
    "R004_velocity_spike_1m": {"account_tx_count_1m": 4},
    "R005_velocity_spike_5m": {"account_tx_count_5m": 11},
    "R006_velocity_spike_1h": {"account_tx_count_1h": 39},
    "R007_impossible_travel": {"implied_speed_kmh_from_last": 1000},
    "R008_identity_change_then_new_device": {
        "hours_since_identity_change": 25,
        "device_is_known_for_account": 0,
    },
    "R009_new_device_high_value": {
        "device_is_known_for_account": 1,
        "amount_zscore_vs_account": 4,
    },
    "R010_device_shared_across_accounts": {"device_distinct_accounts_24h": 4},
    "R011_ip_shared_across_accounts": {"ip_distinct_accounts_1h": 9},
    "R012_failed_login_burst": {"failed_logins_1h": 19},
    "R013_stuffing_origin_and_spend": {"failed_logins_1h": 4, "ip_distinct_accounts_1h": 7},
    "R014_merchant_uniform_amounts": {
        "merchant_amount_cv_24h": 0.4,
        "merchant_distinct_accounts_1h": 25,
    },
    "R015_high_value_anomaly": {"amount_zscore_vs_account": 5.9},
    "R016_unhabitual_merchant_and_category": {
        "merchant_is_habitual": 1,
        "mcc_is_habitual_for_account": 0,
        "amount_zscore_vs_account": 3,
    },
    "R017_far_from_home_new_device": {
        "device_is_known_for_account": 0,
        "distance_from_account_home_km": 499,
    },
    "R018_high_risk_category_unfamiliar": {
        "mcc_is_habitual_for_account": 1,
        "account_tenure_days": 5,
    },
}

# R018 additionally needs a high-risk MCC on the transaction itself.
TX_OVERRIDES: Final[dict[str, dict[str, object]]] = {
    "R018_high_risk_category_unfamiliar": {"merchant_mcc": "7995"},
}


@pytest.mark.parametrize("rule_id", sorted(FIRING))
def test_each_rule_fires_on_its_documented_signature(pack: CompiledPack, rule_id: str) -> None:
    result = outcome(pack, rule_id, _features(**FIRING[rule_id]), **TX_OVERRIDES.get(rule_id, {}))
    assert result.result is Truth.TRUE, (
        f"{rule_id} did not fire on inputs matching its own documented signature"
    )


@pytest.mark.parametrize("rule_id", sorted(NOT_FIRING))
def test_each_rule_holds_its_threshold(pack: CompiledPack, rule_id: str) -> None:
    """Without this, a rule that fired on everything would pass the test above."""
    result = outcome(
        pack, rule_id, _features(**NOT_FIRING[rule_id]), **TX_OVERRIDES.get(rule_id, {})
    )
    assert result.result is Truth.FALSE, f"{rule_id} fired on inputs just inside its threshold"


@pytest.mark.parametrize("rule_id", sorted(FIRING))
def test_each_rule_abstains_when_an_input_is_absent(pack: CompiledPack, rule_id: str) -> None:
    """Absence is not "no". A rule reading a missing feature as false would stop
    firing silently on every source that lacks it."""
    firing = FIRING[rule_id]
    dropped = sorted(firing)[0]
    partial = {k: v for k, v in firing.items() if k != dropped}
    result = outcome(pack, rule_id, _features(**partial), **TX_OVERRIDES.get(rule_id, {}))
    assert result.result is Truth.UNKNOWN, (
        f"{rule_id} returned {result.result} with {dropped!r} absent; it must abstain"
    )
    assert dropped in result.absent_features


# --- the pack as a whole -----------------------------------------------------


def test_the_pack_meets_the_roadmap_minimum(pack: CompiledPack) -> None:
    """ROADMAP Phase 2: a declarative rule engine with at least 15 rules."""
    assert len(pack) >= 15


def test_every_rule_in_the_pack_is_covered_by_a_test(pack: CompiledPack) -> None:
    """A rule with no test is a rule nobody has checked. This is what makes
    "unit test per rule" (ROADMAP Phase 2) a property rather than an intention."""
    tested = set(FIRING)
    in_pack = {rule.id for rule in pack.rules}
    assert in_pack == tested, (
        f"untested rules: {sorted(in_pack - tested)}; "
        f"tests for rules not in the pack: {sorted(tested - in_pack)}"
    )
    assert set(NOT_FIRING) == in_pack, "every rule needs a negative case too"


def test_every_rule_reads_at_least_one_registered_feature(pack: CompiledPack) -> None:
    for rule in pack.rules:
        assert rule.features_used <= KNOWN_FEATURES
        assert rule.features_used or rule.id == "R018_high_risk_category_unfamiliar"


def test_band_floors_are_reserved_for_signals_that_must_not_be_averaged_away(
    pack: CompiledPack,
) -> None:
    """A floor overrides the weighted score, so it is a strong claim. Only
    impossible travel forces CRITICAL, because only it is self-evidently wrong
    rather than merely unusual."""
    critical = {r.id for r in pack.rules if r.band_floor is RiskBand.CRITICAL}
    assert critical == {"R007_impossible_travel"}
    floored = {r.id for r in pack.rules if r.band_floor is not None}
    assert len(floored) < len(pack.rules), (
        "every rule forces a band floor, which makes the weighted score decorative"
    )


def test_the_weakest_scenario_has_the_weakest_rule(pack: CompiledPack) -> None:
    """docs/FRAUD_SCENARIOS.md §3.10 keeps UNUSUAL_LOCATION_DEVICE deliberately
    ambiguous so the Phase 9 Arm F ablation has something real to measure.
    Its rule must stay correspondingly weak: no band floor, low weight."""
    weak = next(r for r in pack.rules if r.id == "R017_far_from_home_new_device")
    assert weak.band_floor is None
    assert weak.weight <= 0.4
    assert weak.weight == min(r.weight for r in pack.rules)


def test_no_rule_branches_on_an_attacker_controlled_field(pack: CompiledPack) -> None:
    """Enforced by the grammar at load time; asserted here because it is the
    property that would matter most if the grammar ever loosened."""
    from trace_core.rules.grammar import CATEGORICAL_FIELDS

    assert not CATEGORICAL_FIELDS & {"merchant_name", "user_agent", "memo"}


def test_an_all_absent_evaluation_fires_nothing_and_abstains_everywhere(
    pack: CompiledPack,
) -> None:
    """A cold store must produce "not assessed", not "no risk found".

    This is the pack-level form of the feature suite's empty-store test, and it
    is the difference between a gateway that says it cannot see and one that
    confidently approves everything.
    """
    result = evaluate_pack(pack, _tx(), {})
    assert not result.fired
    assert result.band_floor is None

    # Not every rule abstains, and that is correct rather than a shortfall.
    # R018's first conjunct tests the MCC against a declared set; an ordinary
    # grocery MCC makes it decisively FALSE, and one false conjunct settles an
    # AND regardless of what else is missing. The rule reached a real verdict
    # ("this category is not high risk, so this rule does not apply") without
    # reading a single feature -- which is exactly the Kleene property that keeps
    # one cold feature from blinding the whole pack.
    settled = [o for o in result.outcomes if not o.abstained]
    assert [o.rule_id for o in settled] == ["R018_high_risk_category_unfamiliar"]
    assert settled[0].result is Truth.FALSE
    assert len(result.abstained) == len(pack.rules) - 1

    # With a high-risk MCC, that conjunct no longer settles it and the rule
    # abstains like the rest: nothing about a cold store is assessable.
    high_risk = evaluate_pack(pack, _tx(merchant_mcc="7995"), {})
    assert not high_risk.fired
    assert len(high_risk.abstained) == len(pack.rules)
    assert high_risk.coverage == 0.0


def test_coverage_reports_the_share_of_rules_that_reached_a_verdict(
    pack: CompiledPack,
) -> None:
    features = _features(account_tx_count_1m=1, card_tx_count_5m=1)
    result = evaluate_pack(pack, _tx(), features)
    assert 0.0 < result.coverage < 1.0
    assert len(result.abstained) + len([o for o in result.outcomes if not o.abstained]) == len(
        pack.rules
    )


def test_a_fired_rule_carries_the_values_it_read(pack: CompiledPack) -> None:
    """A rule id alone is an assertion; a rule id with its inputs is evidence."""
    result = outcome(pack, "R015_high_value_anomaly", _features(amount_zscore_vs_account=7))
    assert result.fired
    assert result.features_read == {"amount_zscore_vs_account": 7.0}
    assert result.description
    assert result.rule_version == "1.0.0"
