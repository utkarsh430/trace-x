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
    REASON_WRITE_FAILED,
    ObserveOutcome,
    ScoringPipeline,
    absent_feature_reasons,
)

from trace_core.contracts.api.transaction import TransactionRequest
from trace_core.contracts.canonical import CanonicalField
from trace_core.domain.enums import RiskBand
from trace_core.domain.errors import FeatureWriteFailedError
from trace_core.domain.time import event_time
from trace_core.features import FeatureState
from trace_core.features.completeness import CompletenessGuard, HoleReason
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.observation import Event, transaction_observation
from trace_core.features.reference import ReferenceFeatureStore
from trace_core.rules.loader import default_loader
from trace_core.scoring.banding import load_thresholds

pytestmark = pytest.mark.unit

NOW = dt.datetime(2026, 3, 1, 12, 0, 0, tzinfo=dt.UTC)


class _ReferenceBackedStore:
    """The naive store behind the pipeline's expected shape.

    A genuine second implementation of the same declared semantics (ADR-0032),
    not a stub: the pipeline depends on `score`/`snapshot`, never on Redis, and
    this proves that boundary holds.
    """

    def __init__(self) -> None:
        self.inner = ReferenceFeatureStore()
        self.observed: list[Event] = []

    def score(self, event: Event) -> Any:
        self.observed.append(event)
        return self.inner.score(event)

    def snapshot(self, **kwargs: Any) -> Any:
        return self.inner.snapshot(**kwargs)

    def observe(self, event: Event) -> Any:
        self.observed.append(event)
        return self.inner.observe(event)

    def withdraw_completeness(self, *, resume_at: Any) -> None:
        self.inner.withdraw_completeness(resume_at=resume_at)


class _BrokenStore:
    """A store that fails the way an unreachable Redis does."""

    def score(self, event: Event) -> Any:
        raise ConnectionError("redis is gone")

    def snapshot(self, **kwargs: Any) -> Any:
        raise ConnectionError("redis is gone")

    def observe(self, event: Event) -> Any:
        raise ConnectionError("redis is gone")

    def withdraw_completeness(self, *, resume_at: Any) -> None:
        raise ConnectionError("redis is gone")


class _FullStore(_ReferenceBackedStore):
    """Reachable, and refusing every write, the way a Redis at `maxmemory` does."""

    def score(self, event: Event) -> Any:
        raise FeatureWriteFailedError("OOM command not allowed")


class _FlakyStore(_ReferenceBackedStore):
    """Unreachable until told otherwise."""

    def __init__(self) -> None:
        super().__init__()
        self.reachable = False

    def score(self, event: Event) -> Any:
        if not self.reachable:
            raise ConnectionError("redis is gone")
        return super().score(event)

    def withdraw_completeness(self, *, resume_at: Any) -> None:
        if not self.reachable:
            raise ConnectionError("redis is gone")
        super().withdraw_completeness(resume_at=resume_at)


class _OpenBreaker:
    def allows(self) -> bool:
        return False

    def record_success(self) -> None:
        return None

    def record_failure(self) -> None:
        return None


def _pipeline(store: Any = None, **over: Any) -> ScoringPipeline:
    return ScoringPipeline(
        pack=default_loader(frozenset(ONLINE_FEATURES.ids)).load(),
        thresholds=load_thresholds(),
        feature_store=store,
        **over,
    )


def _guard(store: Any) -> CompletenessGuard:
    return CompletenessGuard(store, None, instance_id="gw-test", clock=lambda: NOW)


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

    features = ONLINE_FEATURES.evaluate_all(
        without_geo, _pipeline().record_and_read(without_geo)[0]
    )
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
        # Scoring records each transaction; there is no separate write.
        pipeline.score(
            _request(
                transaction_id=f"tx_{i:010d}",
                occurred_at=(NOW - dt.timedelta(seconds=i * 10)).isoformat().replace("+00:00", "Z"),
            ),
            now=NOW,
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
    """A store that cannot record the transaction must not turn it into an error."""
    outcome = _pipeline(_BrokenStore()).score(_request(), now=NOW)
    assert outcome.observe_outcome is ObserveOutcome.UNREACHABLE
    assert outcome.decision.decision == "APPROVE"


def test_the_scoring_read_records_the_transaction_it_scores() -> None:
    """One atomic call records and reads (ADR-0046 §2): the transaction is in its own
    windows, and scoring it again is recognised as a redelivery."""
    store = _ReferenceBackedStore()
    pipeline = _pipeline(store)
    first = pipeline.score(_request(), now=NOW)
    assert first.observe_outcome is ObserveOutcome.RECORDED
    assert first.observe_position == 1
    assert first.features["account_tx_count_1m"].value == 1.0
    again = pipeline.score(_request(), now=NOW)
    assert again.observe_outcome is ObserveOutcome.REDELIVERY
    assert again.features["account_tx_count_1m"].value == 1.0
    assert len(store.inner.events) == 1


def test_a_refused_write_serves_a_snapshot_without_the_transaction_and_opens_a_hole() -> None:
    store = _FullStore()
    store.inner.observe(
        transaction_observation(
            _pipeline().to_canonical(
                _request(
                    transaction_id="tx_earlier",
                    occurred_at=(NOW - dt.timedelta(seconds=10)).isoformat().replace("+00:00", "Z"),
                )
            )
        )
    )
    guard = _guard(store)
    outcome = _pipeline(store, completeness=guard).score(_request(), now=NOW)
    assert outcome.observe_outcome is ObserveOutcome.REFUSED
    assert REASON_WRITE_FAILED in outcome.decision.degraded_reasons
    assert REASON_REDIS not in outcome.decision.degraded_reasons
    assert outcome.features["account_tx_count_1m"].value == 1.0, (
        "the snapshot holds the earlier transaction and not the refused one"
    )
    assert guard.pending


def test_an_unreachable_store_opens_a_hole_that_withdraws_completeness_on_recovery() -> None:
    """Deleting nothing is not enough and deleting the epoch is not enough: completeness
    resumes only 24 h after the store came back (ADR-0046 §5)."""
    store = _FlakyStore()
    store.inner.complete_since = event_time(NOW - dt.timedelta(days=90))
    guard = _guard(store)
    pipeline = _pipeline(store, completeness=guard)
    down = pipeline.score(_request(transaction_id="tx_lost"), now=NOW)
    assert down.observe_outcome is ObserveOutcome.UNREACHABLE
    assert guard.pending
    store.reachable = True
    back = pipeline.score(_request(transaction_id="tx_after"), now=NOW)
    assert back.observe_outcome is ObserveOutcome.RECORDED
    assert not guard.pending
    assert store.inner.complete_since == event_time(NOW + dt.timedelta(hours=24))
    assert REASON_HISTORY_INCOMPLETE in back.decision.degraded_reasons


def test_a_pending_hole_suppresses_the_stores_completeness_claim() -> None:
    """While the withdrawal cannot happen, whatever epoch the store still holds is ignored."""
    store = _ReferenceBackedStore()
    store.inner.complete_since = event_time(NOW - dt.timedelta(days=90))
    guard = _guard(_BrokenStore())
    guard.observation_unrecorded(HoleReason.UNREACHABLE)
    outcome = _pipeline(store, completeness=guard).score(_request(), now=NOW)
    assert outcome.observe_outcome is ObserveOutcome.RECORDED
    assert guard.pending
    assert REASON_HISTORY_INCOMPLETE in outcome.decision.degraded_reasons


def test_a_write_skipped_by_an_open_breaker_is_a_hole_too() -> None:
    store = _ReferenceBackedStore()
    guard = _guard(store)
    outcome = _pipeline(store, breaker=_OpenBreaker(), completeness=guard).score(
        _request(), now=NOW
    )
    assert outcome.observe_outcome is ObserveOutcome.SKIPPED
    assert REASON_REDIS in outcome.decision.degraded_reasons
    assert guard.pending
    assert store.observed == []


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
    context, *_ = _pipeline().record_and_read(canonical)
    reasons = absent_feature_reasons(ONLINE_FEATURES.evaluate_all(canonical, context))
    assert reasons["geo_distance_from_last_km"] == FeatureState.UNAVAILABLE.value
    assert reasons["account_tx_count_1m"] == FeatureState.INSUFFICIENT_HISTORY.value


def test_a_transaction_id_reused_for_another_payload_is_decided_rules_only() -> None:
    """The replay cache is disposable, so a reused transaction id can reach scoring. The store
    serves the first delivery's context (ADR-0046 §1); evaluated for another account it would read
    that account's windows as measured zeros. Rules-only, and the decision says why."""
    from services.gateway.pipeline import REASON_OBSERVATION_CONFLICT, ObserveOutcome

    pipeline = _pipeline(_ReferenceBackedStore())
    body = {
        "transaction_id": "tx_reused_000001",
        "account_id": "acct_000000001",
        "amount_minor": 4_200,
        "currency": "GBP",
        "occurred_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
        "merchant_id": "mrch_000001",
        "merchant_mcc": "5411",
        "merchant_country": "GB",
        "channel": "CARD_NOT_PRESENT",
        "entry_mode": "ECOMMERCE",
    }
    scored = pipeline.score(TransactionRequest.model_validate(body))
    assert scored.observe_outcome is ObserveOutcome.RECORDED
    assert scored.features["account_tx_count_1m"].is_available, "fixture: nothing to lose"

    retried = pipeline.score(TransactionRequest.model_validate(body))
    assert retried.observe_outcome is ObserveOutcome.REDELIVERY
    assert REASON_OBSERVATION_CONFLICT not in retried.degraded_reasons
    assert retried.features["account_tx_count_1m"].is_available

    reused = pipeline.score(
        TransactionRequest.model_validate(
            {**body, "account_id": "acct_000000002", "amount_minor": 99_000}
        )
    )
    assert reused.observe_outcome is ObserveOutcome.CONFLICT
    assert REASON_OBSERVATION_CONFLICT in reused.decision.degraded_reasons
    assert not reused.features["account_tx_count_1m"].is_available, (
        "another payload's context was evaluated as this one's"
    )
