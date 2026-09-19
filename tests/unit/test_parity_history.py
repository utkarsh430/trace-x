"""Silver rows as observations, and scored deliveries as the as-served side (ADR-0056 §2)."""

from __future__ import annotations

import json

import pytest
from eval.parity.history import identity_event, outcome_event, transaction_event
from eval.parity.served import parse_scored
from tests.unit.test_observation_log_sequencing import _scored

from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.semantics import Stream
from trace_core.observation.log import SEQ_HEADER, SESSION_HEADER

pytestmark = pytest.mark.parity

US = 1_786_795_200_123_999
"""Epoch microseconds with a sub-millisecond part the declared millisecond floor removes."""


def test_a_transaction_row_is_its_observation_at_the_millisecond() -> None:
    event = transaction_event(
        {
            "transaction_id": "tx_1",
            "account_id": "acct_000001",
            "currency": "GBP",
            "amount_minor": 250,
            "card_id": None,
            "device_id": "dev_000001",
            "merchant_id": "mrch_00001",
            "ip_id": None,
            "merchant_mcc": "5411",
            "merchant_country": "GB",
            "latitude": 51.5,
            "longitude": -0.1,
            "channel": "CARD_PRESENT",
            "occurred_us": US,
        }
    )
    assert event.occurred_ms == US // 1000
    assert (event.stream, event.identity, event.card_present) == (
        Stream.TRANSACTION,
        "transaction:tx_1",
        True,
    )
    assert event.authorization_outcome is None


def test_identity_rows_feed_their_declared_stream_under_the_online_store_id() -> None:
    row = {
        "correlation_id": "idev_abc",
        "account_id": "acct_000001",
        "device_id": None,
        "ip_id": "ip_00001",
        "occurred_us": US,
        "producer": "trace-gateway@0.1.0",
    }
    failed = identity_event({**row, "identity_event_type": "LOGIN_FAILED"})
    change = identity_event({**row, "identity_event_type": "MFA_RESET"})
    assert failed is not None and failed.stream is Stream.IDENTITY_FAILED_LOGIN
    assert failed.identity == "identity_event:idev_abc"
    assert change is not None and change.stream is Stream.IDENTITY_CHANGE
    assert identity_event({**row, "identity_event_type": "LOGIN_SUCCEEDED"}) is None


def test_only_approvals_and_declines_are_outcome_observations() -> None:
    row = {"transaction_id": "tx_1", "account_id": "acct_000001", "occurred_us": US}
    declined = outcome_event({**row, "authorization_outcome": "DECLINED"})
    assert declined is not None and declined.stream is Stream.AUTHORIZATION_OUTCOME
    assert outcome_event({**row, "authorization_outcome": "REVERSED"}) is None
    assert outcome_event({**row, "authorization_outcome": "UNKNOWN"}) is None


def test_a_scored_delivery_reads_back_as_served() -> None:
    event = _scored(3)
    score = parse_scored(
        json.dumps(event).encode(), [(SESSION_HEADER, b"session-1"), (SEQ_HEADER, b"7")]
    )
    assert score.transaction_id == event["payload"]["transaction_id"]
    assert (score.session_id, score.seq) == ("session-1", 7)
    assert score.observe_outcome == event["payload"]["observe_outcome"]
    assert set(score.features) == set(ONLINE_FEATURES.ids)
    assert score.canonical.field_coverage == frozenset(
        f for f in score.canonical.field_coverage
    ) and {f.value for f in score.canonical.field_coverage} == set(
        event["payload"]["field_coverage"]
    )


def test_a_feature_served_twice_is_refused() -> None:
    event = _scored(4)
    event["payload"]["served_features"].append(event["payload"]["served_features"][0])
    with pytest.raises(ValueError, match="served twice"):
        parse_scored(event)


def test_only_the_gateways_identity_events_are_observations() -> None:
    """The store never saw a directly produced identity event, and Gold no longer reads one."""
    row = {
        "correlation_id": "idev_abc",
        "account_id": "acct_000001",
        "device_id": None,
        "ip_id": None,
        "occurred_us": US,
        "identity_event_type": "LOGIN_FAILED",
    }
    assert identity_event({**row, "producer": "trace-gateway@0.2.0"}) is not None
    assert identity_event({**row, "producer": "trace-generator@1.0.0"}) is None
    assert identity_event({**row, "producer": ""}) is None


def test_the_reference_side_renders_completeness_exactly_as_the_gateway_does() -> None:
    """`compare_as_served` mirrors `served_features`: an absence caused by the bounded read or by an
    unprovable lifetime start reads INCOMPLETE, whatever the store's age (ADR-0046 §8, §3)."""
    import inspect

    from eval.parity import evaluate

    from trace_core.observation import scored_event

    served = inspect.getsource(scored_event.served_features)
    mirror = inspect.getsource(evaluate.compare_as_served)
    for flag in ("depth_capped", "lifetime_unobserved"):
        assert f"feature.{flag}" in served, f"{flag} is no longer what the gateway renders"
        assert f"reference.{flag}" in mirror, f"the reference side does not mirror {flag}"
