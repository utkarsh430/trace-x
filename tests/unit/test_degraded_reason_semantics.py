"""A degraded decision is not automatically a blind one.

**The defect this pins down.** The replay harness reported every degraded
decision as "a gateway that could not read its features". Replaying a frozen
dataset is backdated by construction, so `occurred_at_backdated` was set on all
60,000 decisions while the feature store answered every one of them normally --
`feature_source` was `ONLINE_ONLY`, zero features unavailable. The message sent
a reader to look for a Redis outage that had not happened, and would equally
have hidden a real one inside the noise.

`degraded` is a union of unrelated conditions, and only some of them mean the
features are missing. Collapsing them loses the distinction that decides whether
a band distribution can be trusted at all, so the distinction is asserted here
rather than left to whoever reads the flag next.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest
from eval.replay.gateway_replay import FEATURE_BEARING_DEGRADATIONS, Replayed, _degradation_summary
from services.gateway.pipeline import REASON_CLOCK_SKEW, REASON_RATE_LIMIT, ScoringPipeline

from trace_core.contracts.api.transaction import MAX_BACKDATE_S, TransactionRequest
from trace_core.domain.enums import FeatureSource
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.reference import ReferenceFeatureStore
from trace_core.rules.loader import default_loader
from trace_core.scoring.banding import load_thresholds

pytestmark = pytest.mark.unit


class _WorkingStore:
    def __init__(self) -> None:
        self.inner = ReferenceFeatureStore()
        self.reads = 0

    def snapshot(self, **kwargs: Any) -> Any:
        self.reads += 1
        return self.inner.snapshot(**kwargs)

    def observe(self, event: Any) -> None:
        self.inner.observe(event)


def _pipeline(store: Any) -> ScoringPipeline:
    return ScoringPipeline(
        pack=default_loader(frozenset(ONLINE_FEATURES.ids)).load(),
        thresholds=load_thresholds(),
        feature_store=store,
    )


def _backdated_request() -> TransactionRequest:
    # Beyond the 90-day bound, which is what raises the flag. Inside it, a
    # backdated event is simply accepted, because replaying history is legitimate.
    stale = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=MAX_BACKDATE_S * 2)
    return TransactionRequest.model_validate(
        {
            "transaction_id": "tx_backdated_000001",
            "account_id": "acct_000001",
            "amount_minor": 4_200,
            "currency": "GBP",
            "occurred_at": stale.isoformat().replace("+00:00", "Z"),
            "merchant_id": "mrch_00001",
            "merchant_mcc": "5411",
            "merchant_country": "GB",
            "channel": "CARD_NOT_PRESENT",
            "entry_mode": "ECOMMERCE",
        }
    )


def test_backdating_degrades_the_decision_without_blinding_it() -> None:
    """The exact combination the harness misreported."""
    store = _WorkingStore()
    outcome = _pipeline(store).score(_backdated_request())
    decision = outcome.decision

    assert decision.degraded is True
    assert REASON_CLOCK_SKEW in decision.degraded_reasons
    assert store.reads == 1, "the feature store was not consulted; this test proves nothing"
    assert decision.feature_source is FeatureSource.ONLINE_ONLY, (
        "a backdated event must still be scored against the online store. If it is not, the "
        "harness's old message was right and this whole distinction is moot."
    )
    assert REASON_CLOCK_SKEW not in FEATURE_BEARING_DEGRADATIONS, (
        "`occurred_at_backdated` says the event time is old, not that the feature store "
        "failed. Classifying it as feature-bearing is the defect this file exists for."
    )


def test_only_feature_bearing_reasons_are_counted_as_blind() -> None:
    """The summary must separate 'degraded' from 'scored without features'."""
    decisions = [
        Replayed("tx_a", "LOW", 0.1, "APPROVE", (), True, (REASON_CLOCK_SKEW,), "ONLINE_ONLY"),
        Replayed("tx_b", "LOW", 0.1, "APPROVE", (), True, (REASON_RATE_LIMIT,), "ONLINE_ONLY"),
        Replayed("tx_c", "LOW", 0.1, "APPROVE", (), True, ("redis_unavailable",), "NONE"),
        Replayed("tx_d", "HIGH", 0.7, "REVIEW", (), False, (), "ONLINE_ONLY"),
    ]
    blind, reasons = _degradation_summary(decisions)

    assert blind == 1, (
        f"3 of 4 decisions are degraded but only 1 was scored without features; the summary "
        f"reported {blind}. Reporting 3 is what made a clean run look like an outage."
    )
    assert reasons[REASON_CLOCK_SKEW] == 1
    assert reasons[REASON_RATE_LIMIT] == 1
    assert reasons["redis_unavailable"] == 1


def test_rate_limit_degradation_is_not_a_feature_failure() -> None:
    """A limiter outage fails open on the SCORING path and leaves features intact."""
    assert REASON_RATE_LIMIT not in FEATURE_BEARING_DEGRADATIONS, (
        "an unavailable rate limiter means the request was not counted against a budget, "
        "not that it was scored blind. CLAUDE.md §3.7 makes that fail open deliberately."
    )
