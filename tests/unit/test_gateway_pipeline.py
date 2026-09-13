"""The hot-path sequence, and what it does when a dependency is missing.

The degraded behaviour is the substance here. `docs/ARCHITECTURE.md` §18 says
Redis loss degrades the hot path to rules-only and must never produce a 5xx, and
the design choice that makes that safe is that degradation is expressed as
**missing inputs**, not as a separate code path: features become absent, rules
over them abstain, and the response says so. There is therefore no second scoring
implementation that could drift from the first.

The chaos layer kills a real Redis under a real gateway
(`tests/chaos/test_redis_down.py`). This module pins the logic that layer
exercises, so a failure there is attributable.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest
from services.gateway.pipeline import (
    REASON_CLOCK_SKEW,
    REASON_HISTORY_INCOMPLETE,
    REASON_REDIS,
    ScoringPipeline,
    absent_feature_reasons,
)

from trace_core.contracts.api.transaction import TransactionRequest
from trace_core.contracts.canonical import CanonicalField
from trace_core.domain.enums import RiskBand
from trace_core.features import FeatureState
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.reference import Event, ReferenceFeatureStore
from trace_core.rules.loader import default_loader
from trace_core.scoring.banding import load_thresholds

pytestmark = pytest.mark.unit

NOW = dt.datetime(2026, 3, 1, 12, 0, 0, tzinfo=dt.UTC)


class _ReferenceBackedStore:
    """The naive store behind the pipeline's expected shape.

    A genuine second implementation of the same declared semantics (ADR-0032),
    not a stub: the pipeline depends on `snapshot`/`observe`, never on Redis, and
    this proves that boundary holds.
    """

    def __init__(self) -> None:
        self.inner = ReferenceFeatureStore()
        self.observed: list[Event] = []

    def snapshot(self, **kwargs: Any) -> Any:
        return self.inner.snapshot(**kwargs)

    def observe(self, event: Event) -> None:
        self.observed.append(event)
        self.inner.observe(event)


class _BrokenStore:
    """A store that fails the way an unreachable Redis does."""

    def snapshot(self, **kwargs: Any) -> Any:
        raise ConnectionError("redis is gone")

    def observe(self, event: Event) -> None:
        raise ConnectionError("redis is gone")


def _pipeline(store: Any = None) -> ScoringPipeline:
    return ScoringPipeline(
        pack=default_loader(frozenset(ONLINE_FEATURES.ids)).load(),
        thresholds=load_thresholds(),
        feature_store=store,
    )


def _request(**over: Any) -> TransactionRequest:
    payload: dict[str, Any] = {
        "transaction_id": "tx_0000000001",
        "account_id": "acct_000001",
        "amount_minor": 5_000,
        "currency": "GBP",
        "occurred_at": NOW.isoformat().replace("+00:00", "Z"),
        "merchant_id": "mrch_00001",
        "merchant_mcc": "5411",
        "merchant_country": "GB",
        "device_id": "dev_000001",
        "card_id": "card_000001",
        "ip_id": "ip_00001",
        "latitude": 51.5,
        "longitude": -0.12,
        "channel": "CARD_PRESENT",
    }
    payload.update(over)
    return TransactionRequest.model_validate(payload)


# --- canonical mapping ---------------------------------------------------------


def test_processing_time_is_stamped_by_the_gateway() -> None:
    """A caller able to set `ingested_at` would be setting our lag metric."""
    canonical = _pipeline().to_canonical(_request())
    assert canonical.occurred_at == NOW
    assert canonical.ingested_at >= NOW
    assert CanonicalField.INGESTED_AT in canonical.field_coverage


def test_coverage_reflects_what_the_caller_actually_supplied() -> None:
    """Declared per request, not assumed.

    A caller that omits geography must produce UNAVAILABLE geo features rather
    than INSUFFICIENT_HISTORY ones: the first says the source will never supply
    the input, the second says not yet, and only one of them resolves with time.
    """
    with_geo = _pipeline().to_canonical(_request())
    assert CanonicalField.LATITUDE in with_geo.field_coverage

    without_geo = _pipeline().to_canonical(_request(latitude=None, longitude=None))
    assert CanonicalField.LATITUDE not in without_geo.field_coverage

    features = ONLINE_FEATURES.evaluate_all(without_geo, _pipeline().read_features(without_geo)[0])
    assert features["geo_distance_from_last_km"].state is FeatureState.UNAVAILABLE
    assert features["account_tx_count_1m"].state is FeatureState.INSUFFICIENT_HISTORY


# --- clock skew ----------------------------------------------------------------


def test_a_far_future_timestamp_is_rejected_as_clock_skew() -> None:
    pipeline = _pipeline()
    with pytest.raises(ValueError, match="clock-skew"):
        pipeline.check_clock(NOW + dt.timedelta(hours=25), now=NOW)


def test_a_slightly_future_timestamp_is_accepted() -> None:
    """Clocks drift. A tolerance that rejected a few seconds of skew would reject
    honest traffic."""
    assert _pipeline().check_clock(NOW + dt.timedelta(minutes=5), now=NOW) is None


def test_a_very_old_timestamp_is_accepted_and_flagged() -> None:
    """Accepted, never rejected: replaying historical data is legitimate, and a
    silently dropped old event is indistinguishable from a bug
    (docs/EVENT_CONTRACTS.md §6.3)."""
    assert _pipeline().check_clock(NOW - dt.timedelta(days=120), now=NOW) == REASON_CLOCK_SKEW
    outcome = _pipeline().score(
        _request(occurred_at=(NOW - dt.timedelta(days=120)).isoformat().replace("+00:00", "Z")),
        now=NOW,
    )
    assert outcome.decision.degraded
    assert REASON_CLOCK_SKEW in outcome.decision.degraded_reasons


# --- the happy path ------------------------------------------------------------


def test_a_cold_store_scores_low_and_says_its_history_is_incomplete() -> None:
    """No history is not "no risk", but it is also not a refusal: the decision is
    made, reports that its inputs were absent, and approves.

    It is also not a clean decision, and this test used to assert that it was
    ("a cold store is not a degraded store"). A store that has not been
    recording for as long as its features look back cannot tell a new account
    from one it has simply not seen yet, and a decision made on that store must
    say so -- otherwise a Redis restart produces a day of confident LOW answers
    on accounts whose history was wiped (ADR-0044). What it must NOT say is
    that the store was unavailable: it answered, it just has not warmed.
    """
    outcome = _pipeline(_ReferenceBackedStore()).score(_request(), now=NOW)
    assert outcome.decision.decision == "APPROVE"
    assert outcome.decision.risk_band is RiskBand.LOW
    assert outcome.decision.insufficient_history_features
    assert outcome.decision.degraded
    assert REASON_HISTORY_INCOMPLETE in outcome.decision.degraded_reasons
    assert REASON_REDIS not in outcome.decision.degraded_reasons, (
        "the store answered; unwarmed is not unavailable, and conflating them "
        "would send an operator to look for an outage"
    )


def test_accumulated_history_produces_a_firing_rule() -> None:
    """End to end over the real pack: observations in, a banded decision out."""
    store = _ReferenceBackedStore()
    pipeline = _pipeline(store)
    for i in range(1, 14):
        pipeline.observe(
            pipeline.to_canonical(
                _request(
                    transaction_id=f"tx_{i:010d}",
                    occurred_at=(NOW - dt.timedelta(seconds=i * 10))
                    .isoformat()
                    .replace("+00:00", "Z"),
                )
            )
        )
    outcome = pipeline.score(_request(transaction_id="tx_9999999999"), now=NOW)
    fired = {reason.rule_id for reason in outcome.decision.reasons}
    assert fired, "thirteen transactions in two minutes fired no velocity rule"
    assert outcome.decision.score > 0.0


def test_the_decision_carries_the_provenance_of_its_behaviour() -> None:
    outcome = _pipeline(_ReferenceBackedStore()).score(_request(), now=NOW)
    assert outcome.decision.rule_pack_digest.startswith("sha256:")
    assert outcome.decision.threshold_config_digest.startswith("sha256:")
    assert outcome.decision.feature_set_version
    assert outcome.decision.latency_ms >= 0.0


# --- degradation ---------------------------------------------------------------


def test_a_broken_store_degrades_to_rules_only_and_never_raises() -> None:
    """§18's Redis row, which is the whole reason this path exists."""
    outcome = _pipeline(_BrokenStore()).score(_request(), now=NOW)
    assert outcome.decision.degraded
    assert REASON_REDIS in outcome.decision.degraded_reasons
    assert outcome.decision.decision == "APPROVE", "degradation must not become a refusal"


def test_an_absent_store_is_the_same_as_a_broken_one() -> None:
    """Not configured and not reachable are the same fact to a caller, and
    reporting them differently would invite one to be handled and the other not."""
    assert REASON_REDIS in _pipeline(None).score(_request(), now=NOW).degraded_reasons


def test_degradation_makes_rules_abstain_rather_than_report_false() -> None:
    """The property that makes degraded mode safe.

    If a missing feature evaluated to false, the pack would score zero and the
    response would read as a confident "no risk found" rather than "not
    assessed". Abstention is what keeps those two distinguishable.
    """
    outcome = _pipeline(_BrokenStore()).score(_request(), now=NOW)
    assert outcome.evaluation.abstained, "no rule abstained with every feature absent"
    assert not outcome.evaluation.fired
    assert outcome.evaluation.coverage < 1.0


def test_a_failed_observation_does_not_cost_the_caller_its_answer() -> None:
    """The write happens after the decision, and best effort: a store that cannot
    accept the observation must not turn a scored transaction into an error."""
    pipeline = _pipeline(_BrokenStore())
    outcome = pipeline.score(_request(), now=NOW)
    assert pipeline.observe(outcome.canonical) == REASON_REDIS


def test_extra_degradation_reasons_are_carried_through() -> None:
    """The rate limiter degrades independently of the feature store, and both
    reasons must reach the response rather than the last one winning."""
    outcome = _pipeline(_BrokenStore()).score(
        _request(), now=NOW, extra_degraded=("rate_limit_unavailable",)
    )
    reasons = set(outcome.decision.degraded_reasons)
    assert {REASON_REDIS, "rate_limit_unavailable"} <= reasons
    # A store that could not be read yields an empty context with no
    # completeness claim, which is by definition unwarmed: the third reason is
    # not a duplicate of the first, it is what the first implies for history.
    assert REASON_HISTORY_INCOMPLETE in reasons


def test_degradation_reasons_are_deduplicated() -> None:
    outcome = _pipeline(_BrokenStore()).score(_request(), now=NOW, extra_degraded=(REASON_REDIS,))
    assert outcome.decision.degraded_reasons.count(REASON_REDIS) == 1


# --- absence accounting --------------------------------------------------------


def test_the_two_kinds_of_absence_are_counted_separately() -> None:
    """An operator seeing them merged would not know which to act on: one is a
    source that will never supply the input, the other is a store that has not
    warmed up yet."""
    canonical = _pipeline().to_canonical(_request(latitude=None, longitude=None))
    context, _elapsed, _reason = _pipeline().read_features(canonical)
    reasons = absent_feature_reasons(ONLINE_FEATURES.evaluate_all(canonical, context))
    assert reasons["geo_distance_from_last_km"] == FeatureState.UNAVAILABLE.value
    assert reasons["account_tx_count_1m"] == FeatureState.INSUFFICIENT_HISTORY.value
