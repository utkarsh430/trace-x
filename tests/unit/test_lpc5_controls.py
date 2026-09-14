"""`LPC-5` §14.2 and §14.3: a control is its candidate, changed only as the criterion names.

A control that changed anything else -- the seed, a rate, a second correction -- would measure that
change too, and an ablation that failed its check could not be attributed to the correction it
disabled.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest
from data.generator.config import CORRECTIONS, BaselineIdentityConfig, GeneratorConfig
from data.generator.lpc5 import controls
from data.generator.lpc5 import declaration as d

pytestmark = pytest.mark.unit

START = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)


def _candidate(**block: Any) -> GeneratorConfig:
    return GeneratorConfig(
        seed=4242,
        row_count=4_000,
        account_count=240,
        merchant_count=60,
        device_count=290,
        ip_count=120,
        fraud_rate=0.02,
        start_at=START,
        end_at=START + dt.timedelta(days=59),
        baseline_identity=BaselineIdentityConfig(**block),
    )


def _without(config: GeneratorConfig, *fields: str) -> dict[str, Any]:
    dumped = config.model_dump()
    for name in fields:
        dumped["baseline_identity"].pop(name)
    return dumped


@pytest.mark.parametrize("correction", CORRECTIONS)
def test_an_ablation_is_its_candidate_with_that_one_correction_disabled(correction: str) -> None:
    candidate = _candidate(travel_trips_per_account_year=2.0)
    ablation = controls.ablation_control(candidate, correction)
    assert ablation.baseline_identity is not None
    assert ablation.baseline_identity.disabled_corrections == (correction,)
    assert [c for c in CORRECTIONS if not ablation.baseline_identity.applies(c)] == [correction]
    assert _without(ablation, "disabled_corrections") == _without(candidate, "disabled_corrections")
    assert ablation.digest() != candidate.digest()


def test_every_correction_has_a_declared_ablation_and_an_evaluator() -> None:
    assert tuple(d.ABLATIONS) == CORRECTIONS
    assert set(controls.ABLATION_CHECKS) == set(CORRECTIONS)


def test_a_control_is_taken_only_from_a_candidate() -> None:
    gate_off = GeneratorConfig(seed=4242)
    with pytest.raises(ValueError, match="eval-v2 gate"):
        controls.ablation_control(gate_off, "N12")
    with pytest.raises(ValueError, match="eval-v2 gate"):
        controls.zero_rate_control(gate_off)
    with pytest.raises(ValueError, match="disables no correction"):
        controls.ablation_control(_candidate(disabled_corrections=("T1",)), "N12")
    with pytest.raises(ValueError, match="unknown correction"):
        controls.ablation_control(_candidate(), "N13")


def test_the_zero_rate_control_zeroes_exactly_the_named_rates() -> None:
    candidate = _candidate()
    control = controls.zero_rate_control(candidate)
    assert control.baseline_identity is not None
    block = control.baseline_identity.model_dump()
    assert all(block[name] == 0.0 for name in controls.ZERO_RATE_FIELDS)
    fields = controls.ZERO_RATE_FIELDS
    assert _without(control, *fields) == _without(candidate, *fields)


def test_the_zero_rate_fields_are_the_rates_the_criterion_lists() -> None:
    """§14.2: identity and device activity, T1, T3 and N1-N5."""
    t1 = {
        "new_device_payment_share",
        "secondary_device_account_share",
        "secondary_device_transaction_share",
    }
    t3 = {"decline_share_per_transaction", "decline_retry_share_per_transaction"}
    n1_to_n5 = {
        "transaction_away_ip_share",
        "travel_trips_per_account_year",
        "micro_session_share_per_transaction",
        "fixed_price_merchant_share",
        "fixed_price_purchase_share",
        "household_account_share",
    }
    activity = {
        name
        for name in BaselineIdentityConfig.model_fields
        if name.endswith(("_per_account_day", "_per_account_year", "_per_device_year"))
    } - n1_to_n5
    assert activity and t1 | t3 | n1_to_n5 | activity == controls.ZERO_RATE_FIELDS


def test_the_runner_judges_an_ablation_end_to_end() -> None:
    """Fast-lane smoke, diagnostic only (§16.4). The N12 ablation must fail S2c rule 3, and the
    candidate must not: otherwise the check would fire whatever was disabled."""
    from eval.track_a import lpc5_control

    candidate = _candidate(coverage_floor_instances=1)
    [(label, correction, config)] = lpc5_control.control_configs("ablation", candidate, "N12")
    assert (label, correction) == ("§14.3 ablation N12", "N12")
    assert lpc5_control.control_unmet(correction, lpc5_control.evaluate_config(config)) == []
    baseline = lpc5_control.evaluate_config(candidate)
    assert not controls.ABLATION_CHECKS["N12"](baseline)
