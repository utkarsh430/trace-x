"""The `tx.scored.v1` builder against the released contract (ADR-0051 §6; PHASE3_PLAN §3 Q1)."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import pytest
from services.gateway.pipeline import ScoringOutcome, ScoringPipeline

from trace_core.contracts.api.transaction import TransactionRequest
from trace_core.contracts.canonical_json import canonical_bytes
from trace_core.contracts.events.tx_scored_v1 import TxScoredV1
from trace_core.contracts.publish import _models
from trace_core.domain.errors import ContractError
from trace_core.domain.time import event_time, to_millis
from trace_core.features import FeatureValue
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.reference import ReferenceFeatureStore
from trace_core.observation.scored_event import (
    NOT_APPLICABLE,
    build_scored_event,
    lookback_completeness,
)
from trace_core.rules.loader import default_loader
from trace_core.scoring.banding import load_thresholds

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
NOW = dt.datetime(2026, 3, 1, 12, 0, 0, tzinfo=dt.UTC)
TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
EVALUATION_ONLY = frozenset({"is_fraud", "fraud_pattern", "causal_evidence_keys", "label"})


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
        "merchant_name": "Corner Shop",
        "device_id": "dev_000001",
        "card_id": "card_000001",
        "ip_id": "ip_00001",
        "latitude": 51.5,
        "longitude": -0.12,
        "channel": "CARD_PRESENT",
    }
    payload.update(over)
    return TransactionRequest.model_validate({k: v for k, v in payload.items() if v is not None})


def _score(store: Any, request: TransactionRequest) -> ScoringOutcome:
    pipeline = ScoringPipeline(
        pack=default_loader(frozenset(ONLINE_FEATURES.ids)).load(),
        thresholds=load_thresholds(),
        feature_store=store,
    )
    return pipeline.score(request, now=NOW)


def _event(outcome: ScoringOutcome) -> dict[str, Any]:
    return build_scored_event(
        canonical=outcome.canonical,
        decision=outcome.decision,
        features=outcome.features,
        context=outcome.context,
        observe_outcome=outcome.observe_outcome.value,
        store_position=outcome.observe_position,
        store_epoch_ms=outcome.store_epoch_ms,
        producer="trace-gateway@0.1.0",
        trace_id=TRACE_ID,
    )


def _keys(node: Any) -> set[str]:
    if isinstance(node, dict):
        return set(node) | {k for value in node.values() for k in _keys(value)}
    if isinstance(node, list):
        return {k for value in node for k in _keys(value)}
    return set()


def test_a_scored_event_satisfies_the_released_contract_and_the_publishers_model() -> None:
    since = event_time(NOW - dt.timedelta(days=40))
    outcome = _score(ReferenceFeatureStore(complete_since=since), _request())
    event = _event(outcome)
    encoded = canonical_bytes(event)
    TxScoredV1.model_validate_json(encoded)
    _models()["tx.scored.v1"].model_validate_json(encoded)
    payload = event["payload"]
    assert payload["transaction_id"] == "tx_0000000001"
    assert payload["decision_summary"]["risk_band"] == outcome.decision.risk_band.value
    assert payload["observe_outcome"] == "RECORDED"
    assert payload["store_position"] == 1
    assert payload["store_epoch"] == dt.datetime.fromtimestamp(
        to_millis(since) / 1000, tz=dt.UTC
    ).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    assert event["envelope"]["event_type"] == "tx.scored"
    assert event["envelope"]["correlation_id"] == "tx_0000000001"
    assert {entry["feature_id"] for entry in payload["served_features"]} == set(ONLINE_FEATURES.ids)


def test_an_optional_field_is_present_exactly_when_the_request_supplied_it() -> None:
    outcome = _score(ReferenceFeatureStore(), _request(latitude=None, longitude=None, card_id=None))
    payload = _event(outcome)["payload"]
    for absent in ("latitude", "longitude", "card_id", "memo", "user_agent"):
        assert absent not in payload
        assert absent not in payload["field_coverage"]
    assert "merchant_name" in payload and "merchant_name" in payload["field_coverage"]
    geo = next(
        e for e in payload["served_features"] if e["feature_id"] == "geo_distance_from_last_km"
    )
    assert geo["state"] == "UNAVAILABLE"
    assert {"latitude", "longitude"} <= set(geo["missing_fields"])
    assert "value" not in geo


@pytest.mark.parametrize(
    ("since", "expected"),
    [
        (NOW - dt.timedelta(days=40), "COMPLETE"),
        (NOW - dt.timedelta(minutes=5), "INCOMPLETE"),
        (None, "UNKNOWN"),
    ],
    ids=["store-vouches", "store-too-young", "store-claims-nothing"],
)
def test_an_absence_says_whether_the_store_could_vouch_for_the_lookback(
    since: dt.datetime | None, expected: str
) -> None:
    """A new entity with a store old enough is a measured zero or genuinely new history; a store
    younger than the lookback, or one that claims no epoch, could not vouch (PHASE3_PLAN §3 Q1)."""
    store = ReferenceFeatureStore(complete_since=None if since is None else event_time(since))
    payload = _event(_score(store, _request()))["payload"]
    entry = next(e for e in payload["served_features"] if e["feature_id"] == "account_tx_count_24h")
    assert entry["lookback_completeness"] == expected


def test_a_feature_without_a_lookback_is_not_applicable() -> None:
    """Every released feature looks back today; a later one that does not must not claim a
    completeness it never needed."""
    context = _score(ReferenceFeatureStore(), _request()).context
    assert lookback_completeness("a_feature_with_no_lookback", context) == NOT_APPLICABLE
    assert lookback_completeness("account_tx_count_24h", None) == "UNKNOWN"


def test_nothing_in_the_content_depends_on_the_session_or_the_delivery() -> None:
    outcome = _score(ReferenceFeatureStore(), _request())
    first, second = _event(outcome), _event(outcome)
    assert first["payload"] == second["payload"]
    assert first["envelope"]["idempotency_key"] == second["envelope"]["idempotency_key"]
    assert not {key for key in _keys(first) if "session" in key or key.endswith("seq")}


def test_no_evaluation_only_field_can_appear() -> None:
    event = _event(_score(ReferenceFeatureStore(), _request()))
    assert not _keys(event) & EVALUATION_ONLY
    schema = json.loads((ROOT / "docs/contracts/events/tx.scored.v1.json").read_text())
    assert not _keys(schema) & EVALUATION_ONLY


def test_an_unwritten_store_publishes_null_position_and_epoch() -> None:
    payload = _event(_score(None, _request()))["payload"]
    assert (payload["observe_outcome"], payload["store_position"], payload["store_epoch"]) == (
        "SKIPPED",
        None,
        None,
    )
    TxScoredV1.model_validate_json(canonical_bytes(_event(_score(None, _request()))))


def test_a_non_finite_feature_value_is_refused() -> None:
    outcome = _score(ReferenceFeatureStore(), _request())
    features = dict(outcome.features)
    features["account_tx_count_24h"] = FeatureValue.of("account_tx_count_24h", float("inf"))
    with pytest.raises(ContractError, match="non-finite"):
        build_scored_event(
            canonical=outcome.canonical,
            decision=outcome.decision,
            features=features,
            context=outcome.context,
            observe_outcome=outcome.observe_outcome.value,
            store_position=outcome.observe_position,
            store_epoch_ms=outcome.store_epoch_ms,
            producer="trace-gateway@0.1.0",
            trace_id=TRACE_ID,
        )


def test_a_decision_for_another_transaction_is_refused() -> None:
    one = _score(ReferenceFeatureStore(), _request())
    other = _score(ReferenceFeatureStore(), _request(transaction_id="tx_0000000002"))
    with pytest.raises(ContractError, match="cannot describe"):
        build_scored_event(
            canonical=one.canonical,
            decision=other.decision,
            features=one.features,
            context=one.context,
            observe_outcome=one.observe_outcome.value,
            store_position=one.observe_position,
            store_epoch_ms=one.store_epoch_ms,
            producer="trace-gateway@0.1.0",
            trace_id=TRACE_ID,
        )


def test_a_feature_the_depth_cap_withheld_is_absent_and_not_vouched_for() -> None:
    """ADR-0046 §8 inside the released tx.scored.v1. On a store old enough to vouch for every
    lookback, a content feature past the cap is INSUFFICIENT_HISTORY and INCOMPLETE, the exact count
    beside it stays COMPLETE, and the decision carries `history_depth_capped`."""
    from trace_core.features.observation import Event
    from trace_core.features.semantics import Stream

    store = ReferenceFeatureStore(complete_since=event_time(NOW - dt.timedelta(days=90)))
    for j in range(512, 0, -1):
        store.observe(
            Event(
                stream=Stream.TRANSACTION,
                occurred_at=event_time(NOW - dt.timedelta(seconds=100 * j)),
                account_id="acct_000001",
                event_id=f"tx_d{j:04d}",
                currency="GBP",
                amount_minor=5_000,
                merchant_country="GB",
            )
        )
    event = _event(_score(store, _request()))
    encoded = canonical_bytes(event)
    TxScoredV1.model_validate_json(encoded)
    _models()["tx.scored.v1"].model_validate_json(encoded)
    payload = event["payload"]
    served = {entry["feature_id"]: entry for entry in payload["served_features"]}
    assert served["account_distinct_countries_24h"]["state"] == "INSUFFICIENT_HISTORY"
    assert served["account_distinct_countries_24h"]["lookback_completeness"] == "INCOMPLETE"
    assert served["account_tx_count_24h"]["value"] == 513
    assert served["account_tx_count_24h"]["lookback_completeness"] == "COMPLETE"
    assert "history_depth_capped" in payload["decision_summary"]["degraded_reasons"]
