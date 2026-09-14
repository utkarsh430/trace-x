"""`LPC-5` §16.3: exact rules, G1, G3, S6a, S7a, controls and `evaluate`, on hand-built rows."""

from __future__ import annotations

import dataclasses
import datetime as dt
import math

import pytest
from data.generator import outcomes
from data.generator.config import GeneratorConfig
from data.generator.engine import generate_dataset
from data.generator.lpc5 import controls, rules, run
from data.generator.lpc5 import declaration as d
from data.generator.lpc5.attributes import compute_tables
from data.generator.lpc5.fixtures import END_MS, T0, Rows, account, knowledge
from data.generator.lpc5.frame import build_frame
from data.generator.lpc5.frame import knowledge as universe_knowledge
from data.generator.population import AccountProfile, build_universe

from trace_core.domain.entities import Account, Card
from trace_core.domain.enums import FraudPattern
from trace_core.domain.geo import GeoPoint
from trace_core.domain.identifiers import account_id, card_id
from trace_core.domain.time import event_time

ATO = FraudPattern.ACCOUNT_TAKEOVER
HOUR = d.HOUR_MS


def _profile(n: int, mu: float = 7.378, sigma: float = 1.0) -> AccountProfile:
    holder = Account(
        account_id=account_id(n),
        opened_at=event_time(dt.datetime(2025, 1, 1, tzinfo=dt.UTC)),
        home=GeoPoint(51.5, -0.12),
        country="GB",
        currency="GBP",
    )
    return AccountProfile(
        account=holder,
        cards=(Card(card_id(n), holder.account_id, holder.opened_at, "1234"),),
        home_devices=("dev_000001",),
        habitual_merchants=("mrch_00001",),
        home_ips=("ip_00001",),
        amount_mu=mu,
        amount_sigma=sigma,
    )


def test_s2c_passes_on_well_formed_rows_except_the_unreleased_outcome_schema() -> None:
    rows = Rows()
    tx = rows.tx(T0, account(0), authorization_outcome="UNKNOWN")
    rows.outcome(tx, "APPROVED")
    rows.identity(T0 + 5_000, account(0), "LOGIN_SUCCEEDED")
    know = knowledge()
    result = rules.s2c_rows(build_frame(rows.rows, seed=know.seed, validate=True), know)
    assert [f.check for f in result.findings] == ["S2c/1"]
    assert "tx.authorization.v1 has no released schema" in result.findings[0].detail


def test_s2c_names_every_violated_rule() -> None:
    rows = Rows()
    first = rows.tx(T0, account(0), authorization_outcome="APPROVED", memo="ACCOUNT_TAKEOVER")
    rows.outcome(first, "APPROVED", envelope={"occurred_at": outcomes.iso_millis(T0 + 1)})
    rows.tx(
        T0 + 1_000,
        account(1),
        authorization_outcome="UNKNOWN",
        envelope={"occurred_at": "2026-01-15T00:00:01Z"},
    )
    rows.identity(END_MS + 1, account(2), "LOGIN_FAILED")
    rows.tx(
        T0 + 2_000,
        account(3),
        authorization_outcome="UNKNOWN",
        envelope={"correlation_id": "corr_same"},
    )
    rows.tx(
        T0 + 3_000,
        account(4),
        authorization_outcome="UNKNOWN",
        envelope={"correlation_id": "corr_same", "event_id": first.event["envelope"]["event_id"]},
    )
    rows.tx(T0 + 4_000, account(5), authorization_outcome="UNKNOWN", amount_minor=1.5)
    know = knowledge()
    result = rules.s2c_rows(build_frame(rows.rows, seed=know.seed, validate=True), know)
    assert {f.check for f in result.findings} == {f"S2c/{n}" for n in range(1, 9)}  # fmt: skip


def test_s2c_without_validation_reports_rules_one_and_two_as_not_run() -> None:
    rows = Rows()
    tx = rows.tx(T0, account(0), authorization_outcome="UNKNOWN")
    rows.outcome(tx, "APPROVED")
    know = knowledge()
    result = rules.s2c_rows(build_frame(rows.rows, seed=know.seed), know)
    details = {f.check: f.detail for f in result.findings}
    assert set(details) == {"S2c/1", "S2c/2"}
    assert "not run" in details["S2c/1"]


def test_s4_pairs_keep_planted_and_legitimate_pairs_and_drop_mixed_ones() -> None:
    rows = Rows()
    rows.tx(T0, account(0), pattern=ATO, instance="fi_a")
    rows.tx(T0 + 60_000, account(0), pattern=ATO, instance="fi_a")
    rows.tx(T0 + 120_000, account(0))  # after a planted row: a mixed pair, dropped
    rows.tx(T0, account(1))
    rows.tx(T0 + 5_000, account(1))
    rows.identity(
        T0 - 180_000,
        account(2),
        "PASSWORD_CHANGE",
        pattern=ATO,
        instance="fi_b",
        device="dev_000002",
    )
    rows.device(T0 - 60_000, account(2), "dev_000002", pattern=ATO, instance="fi_b")
    frame = build_frame(rows.rows, seed=42)
    pairs = rules.s4_pairs(frame)
    assert sorted((p.group, p.offset_ms) for p in pairs["K1"]) == [
        (ATO.value, 60_000),
        (d.LEGIT, 5_000),
    ]
    assert [(p.group, p.offset_ms) for p in pairs["K2"]] == [(ATO.value, 120_000)]
    assert len(pairs["K7"]) == len(frame.out) == 5


def test_s4_k5_pairs_each_row_with_the_next_payment_by_another_account() -> None:
    rows = Rows()
    rows.tx(T0, account(0), device_id="dev_000002")
    rows.tx(T0 + 1, account(0), device_id="dev_000002")
    rows.tx(T0 + 3, account(1), device_id="dev_000002")
    frame = build_frame(rows.rows, seed=42)
    assert sorted(p.offset_ms for p in rules.s4_pairs(frame)["K5"]) == [2, 3]


def test_g1_regions_hold_and_s5a_detects_wrong_and_unplanned_amounts() -> None:
    profile = _profile(0)
    typical = math.exp(profile.amount_mu)
    velocity = rules.g1_amount(42, "fi_v", 0, FraudPattern.VELOCITY_ATTACK, profile)
    takeover = rules.g1_amount(42, "fi_a", 0, ATO, profile)
    unusual = rules.g1_amount(42, "fi_u", 0, FraudPattern.UNUSUAL_LOCATION_DEVICE, profile)
    high = rules.g1_amount(42, "fi_h", 0, FraudPattern.ANOMALOUS_HIGH_VALUE, profile)
    probe = rules.g1_amount(42, "fi_c", 0, FraudPattern.CARD_TESTING, profile)
    payoff = rules.g1_amount(42, "fi_c", 9, FraudPattern.CARD_TESTING, profile, payoff=True)
    assert velocity is not None and velocity < 3 * typical
    assert takeover is not None and takeover >= 3 * typical
    assert unusual is not None and 0.5 * typical < unusual < 2 * typical
    assert high is not None and high > 20 * typical
    assert probe is not None and 1 <= probe < 250
    assert payoff is not None and payoff >= 3 * typical

    know = dataclasses.replace(knowledge(), profiles={account(0): profile})
    rows = Rows()
    rows.tx(
        T0,
        account(0),
        pattern=FraudPattern.VELOCITY_ATTACK,
        instance="fi_v",
        ordinal=0,
        amount_minor=velocity,
    )
    rows.tx(
        T0 + 1,
        account(0),
        pattern=FraudPattern.VELOCITY_ATTACK,
        instance="fi_v",
        ordinal=1,
        amount_minor=velocity + 1,
    )
    rows.tx(
        T0 + 2,
        account(0),
        pattern=FraudPattern.VELOCITY_ATTACK,
        instance="fi_v",
        amount_minor=velocity,
    )
    frame = build_frame(rows.rows, seed=42)
    s5a, _ = rules.s5_amounts(frame, compute_tables(frame, know), know)
    (finding,) = s5a.findings
    assert finding.detail.startswith("2 violation(s)")


def test_the_region_test_honours_open_and_closed_bounds() -> None:
    uld = d.G1_RULES[FraudPattern.UNUSUAL_LOCATION_DEVICE]
    assert not rules.in_region(50, 100.0, uld)
    assert rules.in_region(51, 100.0, uld)
    assert not rules.in_region(200, 100.0, uld)
    assert rules.in_region(300, 100.0, d.G1_RULES[ATO])
    assert not rules.in_region(299, 100.0, d.G1_RULES[ATO])


def test_g3_bound_is_inclusive_at_900_kmh() -> None:
    assert not rules.too_fast(900.0, HOUR)
    assert rules.too_fast(900.001, HOUR)
    assert not rules.too_fast(0.0, 0)
    assert rules.too_fast(0.1, 0)


def test_g3_flags_a_takeover_that_implies_an_impossible_speed_from_the_customer() -> None:
    rows = Rows()
    rows.tx(T0, account(0))
    rows.tx(
        T0 + 600_000, account(0), pattern=ATO, instance="fi_a", latitude=40.4168, longitude=-3.7038
    )
    rows.tx(T0 + 2 * d.DAY_MS, account(0), pattern=ATO, instance="fi_b")
    result = rules.g3_speed(build_frame(rows.rows, seed=42))
    assert [f.check for f in result.findings] == ["G3"]
    assert result.findings[0].detail.startswith("1 violation(s)")


def test_s6_is_not_computed_without_a_provider_and_fails_on_unavailable() -> None:
    rows = Rows()
    rows.tx(T0, account(0))
    rows.tx(T0 + 1, account(1))
    know = knowledge()
    frame = build_frame(rows.rows, seed=42)
    missing = rules.s6_availability(compute_tables(frame, know))
    assert "not computed" in missing.findings[0].detail

    def provider(_: object) -> dict[str, list[str]]:
        return {
            feature: ["AVAILABLE", "UNAVAILABLE" if feature == "failed_logins_1h" else "AVAILABLE"]
            for feature in d.RELEASED_FEATURES
        }

    result = rules.s6_availability(compute_tables(frame, know, availability=provider))
    assert [f.attribute for f in result.findings] == ["avail:failed_logins_1h"]


def test_s7a_checks_scenario_invariants_and_declared_keys() -> None:
    rows = Rows()
    rows.tx(
        T0,
        account(0),
        pattern=FraudPattern.ANOMALOUS_HIGH_VALUE,
        instance="fi_h",
        amount_minor=1_000,
    )
    rows.tx(T0, account(1), pattern=FraudPattern.IMPOSSIBLE_TRAVEL, instance="fi_t")
    rows.tx(
        T0 + HOUR,
        account(1),
        pattern=FraudPattern.IMPOSSIBLE_TRAVEL,
        instance="fi_t",
        latitude=52.0,
        longitude=-0.12,
    )
    result = rules.s7_instances(build_frame(rows.rows, seed=42), knowledge())
    assert {"S7a/3", "S7a/9", "S7a/11a"} <= {f.check for f in result.findings}


def test_every_ablation_has_an_evaluator() -> None:
    assert set(controls.ABLATION_CHECKS) == set(d.ABLATIONS)


@pytest.fixture(scope="module")
def small_eval_v1_report() -> run.Lpc5Report:
    config = GeneratorConfig(
        seed=7,
        row_count=6_000,
        account_count=240,
        merchant_count=60,
        device_count=290,
        ip_count=120,
        fraud_rate=0.02,
        start_at=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
        end_at=dt.datetime(2026, 3, 1, tzinfo=dt.UTC),
    )
    universe = build_universe(config)
    return run.evaluate(generate_dataset(config, universe), universe_knowledge(universe))


def test_evaluate_reports_every_check_and_why_an_eval_v1_run_cannot_pass(
    small_eval_v1_report: run.Lpc5Report,
) -> None:
    report = small_eval_v1_report
    assert set(report.checks) == set(run.CHECK_ORDER)
    assert not report.passed
    assert any("availability" in reason for reason in report.invalid)
    assert report.outcomes_derived
    assert controls.has_finding(report, ("S2c/3",))
    assert controls.has_finding(report, ("S2c/7",))
    assert controls.has_finding(report, ("S8/disclosure",))
    assert "LPC-5 revision 2" in report.format()
