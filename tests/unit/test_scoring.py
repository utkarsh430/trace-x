"""Scoring, banding and decision assembly.

The properties asserted here are the ones a reviewer cannot check by reading:
that the score is bounded whatever the pack does, that adding a fired rule never
*lowers* it, that a band floor can only raise a band, and that a decision carries
enough to be re-derived by hand.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from trace_core.domain.enums import FeatureSource, RiskBand
from trace_core.domain.errors import ContractError
from trace_core.features.spec import FeatureValue
from trace_core.rules.engine import Evaluation, RuleOutcome
from trace_core.rules.grammar import Truth
from trace_core.scoring.banding import (
    DEFAULT_CONFIG,
    ThresholdConfig,
    band,
    combine,
    load_thresholds,
)
from trace_core.scoring.decision import APPROVE, FLAG, build_decision

# Both markers: the hypothesis-driven invariants below are the `property`
# layer of docs/TESTING.md §3, and the rest are ordinary unit tests.
pytestmark = [pytest.mark.unit, pytest.mark.property]

SCORED_AT = dt.datetime(2026, 3, 1, 12, 0, 0, tzinfo=dt.UTC)


@pytest.fixture(scope="module")
def config() -> ThresholdConfig:
    return load_thresholds()


def _outcome(
    rule_id: str = "R001_probe",
    *,
    weight: float = 0.5,
    result: Truth = Truth.TRUE,
    band_floor: RiskBand | None = None,
    features_read: dict[str, float] | None = None,
) -> RuleOutcome:
    return RuleOutcome(
        rule_id=rule_id,
        rule_version="1.0.0",
        description="A probe rule used to exercise the combination function.",
        result=result,
        weight=weight,
        band_floor=band_floor,
        features_read=features_read or {},
    )


def _evaluation(*outcomes: RuleOutcome) -> Evaluation:
    return Evaluation(
        pack_id="probe",
        pack_version="1.0.0",
        pack_digest="sha256:" + "a" * 64,
        outcomes=tuple(outcomes),
    )


# --- the combination function ------------------------------------------------


def test_no_fired_rules_scores_zero() -> None:
    assert combine(_evaluation()) == 0.0
    assert combine(_evaluation(_outcome(result=Truth.FALSE))) == 0.0


def test_an_abstaining_rule_contributes_nothing() -> None:
    """It reached no verdict, so it is not evidence either way. The caller
    reports coverage separately, so a blind pack stays distinguishable from a
    quiet one rather than both scoring zero for the same reason."""
    assert combine(_evaluation(_outcome(result=Truth.UNKNOWN, weight=0.9))) == 0.0


def test_one_strong_rule_can_reach_a_high_band_alone(config: ThresholdConfig) -> None:
    """The property a normalised sum would destroy: under `sum(w)/sum(all_w)`,
    one rule of weight 0.9 in a pack of eighteen is indistinguishable from noise.
    Impossible travel is not a seventh of a signal."""
    score, risk = band(_evaluation(_outcome(weight=0.9)), config)
    assert score == pytest.approx(0.9)
    assert risk is RiskBand.CRITICAL


def test_weak_rules_accumulate_without_reaching_high(config: ThresholdConfig) -> None:
    score, risk = band(
        _evaluation(_outcome("R001_a", weight=0.2), _outcome("R002_b", weight=0.2)), config
    )
    assert score == pytest.approx(0.36)
    assert risk is RiskBand.MEDIUM


@given(
    weights=st.lists(st.floats(min_value=0.0, max_value=1.0), min_size=0, max_size=30),
)
def test_the_score_is_always_bounded(weights: list[float]) -> None:
    """Whatever a pack declares, the score stays in [0, 1]. A score outside it
    would make every threshold comparison meaningless."""
    evaluation = _evaluation(*(_outcome(f"R{i:03d}_x", weight=w) for i, w in enumerate(weights)))
    assert 0.0 <= combine(evaluation) <= 1.0


@given(
    weights=st.lists(st.floats(min_value=0.0, max_value=0.99), min_size=1, max_size=20),
    extra=st.floats(min_value=0.0, max_value=0.99),
)
def test_another_fired_rule_never_lowers_the_score(weights: list[float], extra: float) -> None:
    """Monotonicity. A system where more evidence can lower risk is one an
    attacker can steer by deliberately triggering a rule."""
    base = _evaluation(*(_outcome(f"R{i:03d}_x", weight=w) for i, w in enumerate(weights)))
    more = _evaluation(*base.outcomes, _outcome(f"R{len(weights):03d}_extra", weight=extra))
    assert combine(more) >= combine(base) - 1e-12


@given(weight=st.floats(min_value=0.0, max_value=1.0))
def test_a_single_rule_scores_its_own_weight(weight: float) -> None:
    """Each weight reads directly as "how far this rule alone moves the score",
    which is what makes a pack reviewable by reading it."""
    assert combine(_evaluation(_outcome(weight=weight))) == pytest.approx(weight)


# --- band floors --------------------------------------------------------------


def test_a_band_floor_raises_a_band(config: ThresholdConfig) -> None:
    score, risk = band(_evaluation(_outcome(weight=0.1, band_floor=RiskBand.CRITICAL)), config)
    assert score == pytest.approx(0.1)
    assert risk is RiskBand.CRITICAL, "the floor did not override a low score"


def test_a_band_floor_never_lowers_a_band(config: ThresholdConfig) -> None:
    """A rule able to REDUCE a band would be a way for an attacker to buy safety
    by triggering it."""
    _score, risk = band(_evaluation(_outcome(weight=0.95, band_floor=RiskBand.LOW)), config)
    assert risk is RiskBand.CRITICAL


def test_the_highest_floor_among_fired_rules_wins(config: ThresholdConfig) -> None:
    _score, risk = band(
        _evaluation(
            _outcome("R001_a", weight=0.1, band_floor=RiskBand.MEDIUM),
            _outcome("R002_b", weight=0.1, band_floor=RiskBand.HIGH),
        ),
        config,
    )
    assert risk is RiskBand.HIGH


def test_a_floor_on_a_rule_that_did_not_fire_is_ignored(config: ThresholdConfig) -> None:
    _score, risk = band(
        _evaluation(_outcome(weight=0.1, result=Truth.FALSE, band_floor=RiskBand.CRITICAL)),
        config,
    )
    assert risk is RiskBand.LOW


def test_a_floor_on_an_abstaining_rule_is_ignored(config: ThresholdConfig) -> None:
    """Abstention is not evidence. A rule that could raise a band BECAUSE its
    input was missing would make a cold store look like an attack."""
    _score, risk = band(
        _evaluation(_outcome(weight=0.1, result=Truth.UNKNOWN, band_floor=RiskBand.CRITICAL)),
        config,
    )
    assert risk is RiskBand.LOW


# --- threshold configuration ---------------------------------------------------


def test_the_shipped_config_loads_and_is_digest_pinned(config: ThresholdConfig) -> None:
    assert DEFAULT_CONFIG.is_file()
    assert config.digest.startswith("sha256:")
    assert config.triage_at is RiskBand.HIGH


def test_the_digest_moves_when_a_threshold_moves(tmp_path: Path) -> None:
    """Every decision cites it; if it did not move, two different operating
    points would be indistinguishable in the record."""
    original = DEFAULT_CONFIG.read_text()
    altered = tmp_path / "altered.yaml"
    altered.write_text(original.replace("high: 0.60", "high: 0.65"))
    assert load_thresholds(altered).digest != load_thresholds().digest


def test_out_of_order_thresholds_are_refused(tmp_path: Path) -> None:
    """Out of order, a band becomes unreachable and nothing about the output
    would look wrong."""
    broken = tmp_path / "broken.yaml"
    broken.write_text(DEFAULT_CONFIG.read_text().replace("high: 0.60", "high: 0.20"))
    with pytest.raises(ContractError, match="failed validation"):
        load_thresholds(broken)


def test_triage_at_low_is_refused(tmp_path: Path) -> None:
    broken = tmp_path / "broken.yaml"
    broken.write_text(DEFAULT_CONFIG.read_text().replace("triage_at: HIGH", "triage_at: LOW"))
    with pytest.raises(ContractError, match="failed validation"):
        load_thresholds(broken)


def test_a_missing_config_is_fatal(tmp_path: Path) -> None:
    """Inventing a default would be an unrecorded operating point in a system
    whose whole point is that the operating point is recorded."""
    with pytest.raises(ContractError):
        load_thresholds(tmp_path / "does-not-exist.yaml")


@pytest.mark.parametrize(
    ("score", "expected"),
    [
        (0.0, RiskBand.LOW),
        (0.299, RiskBand.LOW),
        (0.30, RiskBand.MEDIUM),
        (0.599, RiskBand.MEDIUM),
        (0.60, RiskBand.HIGH),
        (0.849, RiskBand.HIGH),
        (0.85, RiskBand.CRITICAL),
        (1.0, RiskBand.CRITICAL),
    ],
)
def test_band_boundaries_are_inclusive_at_the_lower_edge(
    config: ThresholdConfig, score: float, expected: RiskBand
) -> None:
    assert config.band_for(score) is expected


# --- decision assembly ---------------------------------------------------------


def test_a_high_band_flags_and_a_low_band_approves(config: ThresholdConfig) -> None:
    high = build_decision(
        transaction_id="tx_1",
        evaluation=_evaluation(_outcome(weight=0.9)),
        features={},
        config=config,
        scored_at=SCORED_AT,
        latency_ms=4.2,
        case_id="case_" + "a" * 32,
    )
    assert high.decision == FLAG
    assert high.case_id is not None

    low = build_decision(
        transaction_id="tx_2",
        evaluation=_evaluation(),
        features={},
        config=config,
        scored_at=SCORED_AT,
        latency_ms=3.1,
    )
    assert low.decision == APPROVE
    assert low.case_id is None


def test_phase_2_never_declines(config: ThresholdConfig) -> None:
    """The hot path is advisory: the authorization system owns the decline. The
    value exists in the contract because adding it later would be breaking."""
    for weight in (0.0, 0.5, 0.99):
        decision = build_decision(
            transaction_id="tx_1",
            evaluation=_evaluation(_outcome(weight=weight)),
            features={},
            config=config,
            scored_at=SCORED_AT,
            latency_ms=1.0,
        )
        assert decision.decision in {APPROVE, FLAG}


def test_a_decision_can_be_re_derived_from_its_own_contents(config: ThresholdConfig) -> None:
    """A rule id alone is an assertion; a rule id with its inputs is evidence."""
    decision = build_decision(
        transaction_id="tx_1",
        evaluation=_evaluation(
            _outcome(weight=0.8, features_read={"amount_zscore_vs_account": 7.0})
        ),
        features={},
        config=config,
        scored_at=SCORED_AT,
        latency_ms=5.0,
    )
    reason = decision.reasons[0]
    assert reason.rule_id == "R001_probe"
    assert reason.features_read == {"amount_zscore_vs_account": 7.0}
    assert decision.rule_pack_digest.startswith("sha256:")
    assert decision.threshold_config_digest == config.digest
    assert decision.feature_set_version


def test_absent_features_are_reported_and_kept_apart(config: ThresholdConfig) -> None:
    """A decision made with a third of its inputs missing is a different fact
    from one made with all of them, and the two causes need different remedies."""
    from trace_core.contracts.canonical import CanonicalField

    features = {
        "geo_distance_from_last_km": FeatureValue.unavailable(
            "geo_distance_from_last_km", frozenset({CanonicalField.LATITUDE})
        ),
        "account_tenure_days": FeatureValue.insufficient_history("account_tenure_days"),
        "account_tx_count_1m": FeatureValue.of("account_tx_count_1m", 3.0),
    }
    decision = build_decision(
        transaction_id="tx_1",
        evaluation=_evaluation(),
        features=features,
        config=config,
        scored_at=SCORED_AT,
        latency_ms=2.0,
    )
    assert decision.unavailable_features == ["geo_distance_from_last_km"]
    assert decision.insufficient_history_features == ["account_tenure_days"]


def test_degradation_is_carried_on_the_decision(config: ThresholdConfig) -> None:
    """Every fail-open decision is tagged and counted (CLAUDE.md §3.7)."""
    decision = build_decision(
        transaction_id="tx_1",
        evaluation=_evaluation(),
        features={},
        config=config,
        scored_at=SCORED_AT,
        latency_ms=2.0,
        degraded=True,
        degraded_reasons=("redis_unavailable",),
        feature_source=FeatureSource.ONLINE_ONLY,
    )
    assert decision.degraded
    assert decision.degraded_reasons == ["redis_unavailable"]
    assert decision.feature_source is FeatureSource.ONLINE_ONLY


def test_the_real_pack_and_config_produce_a_valid_decision() -> None:
    """End to end over the SHIPPED pack and thresholds, not probe fixtures."""
    from trace_core.contracts.canonical import CanonicalField, CanonicalTransaction
    from trace_core.features.definitions import ONLINE_FEATURES
    from trace_core.rules.engine import evaluate_pack
    from trace_core.rules.loader import default_loader

    pack = default_loader(frozenset(ONLINE_FEATURES.ids)).load()
    transaction = CanonicalTransaction(
        source_dataset="scoring-test",
        source_row_id="row-1",
        field_coverage=frozenset(CanonicalField),
        transaction_id="tx_0000000001",
        account_id="acct_000001",
        amount_minor=900_000,
        currency="GBP",
        occurred_at=SCORED_AT,
        ingested_at=SCORED_AT + dt.timedelta(milliseconds=40),
        merchant_mcc="5411",
    )
    features = {
        "amount_zscore_vs_account": FeatureValue.of("amount_zscore_vs_account", 9.0),
        "implied_speed_kmh_from_last": FeatureValue.of("implied_speed_kmh_from_last", 1500.0),
    }
    evaluation = evaluate_pack(pack, transaction, features)
    decision = build_decision(
        transaction_id=transaction.transaction_id,
        evaluation=evaluation,
        features=features,
        config=load_thresholds(),
        scored_at=SCORED_AT,
        latency_ms=6.5,
        case_id="case_" + "b" * 32,
    )
    assert decision.risk_band is RiskBand.CRITICAL, "impossible travel must force CRITICAL"
    assert decision.decision == FLAG
    assert {r.rule_id for r in decision.reasons} >= {
        "R007_impossible_travel",
        "R015_high_value_anomaly",
    }
