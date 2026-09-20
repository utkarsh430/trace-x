"""What the gateway did with each request, the shifted outcome times, and linking deliveries."""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import pytest
from eval.parity.driver import (
    Applied,
    GatewayDriver,
    Posted,
    UnexpectedGatewayResponseError,
    classify,
)
from eval.parity.evaluate import LinkError, OrderEvidenceError, link
from eval.parity.served import ServedScore

from trace_core.contracts.api.problem import ErrorType
from trace_core.contracts.canonical import CanonicalTransaction

pytestmark = pytest.mark.parity

TX, IDENTITY, OUTCOME = "tx.raw.v1", "identity.events.v1", "tx.authorization.v1"


def _classify(
    topic: str,
    status: int,
    request: dict[str, Any],
    body: Any = None,
    key: str = "k",
    seen: set[bytes] | None = None,
) -> Applied:
    return classify(topic, request, key, status, body, set() if seen is None else seen)


def test_a_transaction_is_scored_once_and_replayed_for_the_same_key_and_payload() -> None:
    seen: set[bytes] = set()
    request = {"transaction_id": "tx_1", "amount_minor": 5}
    assert _classify(TX, 200, request, seen=seen) is Applied.SCORED
    assert _classify(TX, 200, request, seen=seen) is Applied.REPLAYED
    assert _classify(TX, 200, request, key="other", seen=seen) is Applied.SCORED
    assert _classify(TX, 422, request) is Applied.REFUSED
    with pytest.raises(UnexpectedGatewayResponseError):
        _classify(TX, 503, request)


def test_an_identity_event_changes_state_only_when_its_type_feeds_a_stream() -> None:
    assert _classify(IDENTITY, 202, {"identity_event_type": "LOGIN_FAILED"}) is Applied.OBSERVED
    assert _classify(IDENTITY, 202, {"identity_event_type": "LOGIN_SUCCEEDED"}) is Applied.NO_STATE
    assert _classify(IDENTITY, 409, {"identity_event_type": "LOGIN_FAILED"}) is Applied.REFUSED


def test_an_account_mismatch_reaches_the_store_and_a_conflict_does_not() -> None:
    mismatch = {"type": ErrorType.AUTHORIZATION_ACCOUNT_MISMATCH.value}
    conflict = {"type": ErrorType.AUTHORIZATION_CONFLICT.value}
    assert _classify(OUTCOME, 202, {}) is Applied.OBSERVED
    assert _classify(OUTCOME, 409, {}, mismatch) is Applied.OBSERVED
    assert _classify(OUTCOME, 409, {}, conflict) is Applied.REFUSED
    with pytest.raises(UnexpectedGatewayResponseError):
        _classify(OUTCOME, 503, {})


@dataclass
class _Response:
    status_code: int
    payload: dict[str, Any] | None

    @property
    def content(self) -> bytes:
        return b"" if self.payload is None else json.dumps(self.payload).encode()

    def json(self) -> Any:
        return self.payload


@dataclass
class _Client:
    responses: list[_Response]
    calls: list[tuple[str, dict[str, Any], dict[str, str]]] = field(default_factory=list)

    def post(self, url: str, *, json: Any, headers: Mapping[str, str]) -> Any:
        self.calls.append((url, json, dict(headers)))
        return self.responses.pop(0)


def test_an_outcome_keeps_its_transaction_time_on_the_same_uniform_shift() -> None:
    client = _Client([_Response(202, {"accepted": True}), _Response(200, {"decision": "APPROVE"})])
    driver = GatewayDriver(client, token="t.s", shift=dt.timedelta(days=200))
    outcome = {
        "envelope": {"event_id": "e0", "occurred_at": "2026-08-23T12:00:01.300Z"},
        "payload": {
            "transaction_id": "tx_1",
            "account_id": "acct_000001",
            "authorization_outcome": "DECLINED",
            "transaction_occurred_at": "2026-02-04T12:00:01.000Z",
        },
    }
    transaction = {
        "envelope": {"event_id": "e1", "occurred_at": "2026-08-23T12:00:01.000Z"},
        "payload": {"transaction_id": "tx_1", "authorization_outcome": "UNKNOWN", "memo": "m"},
    }
    driver.post(OUTCOME, outcome)
    driver.post(TX, transaction)
    (url, request, headers), (_tx_url, tx_request, tx_headers) = client.calls
    assert url == "/v1/events/authorization" and "X-Idempotency-Key" not in headers
    assert request["transaction_occurred_at"] == "2026-08-23T12:00:01.000Z"
    assert request["decided_at"] == "2026-08-23T12:00:01.300Z"
    assert tx_headers["X-Idempotency-Key"] == "parity-e1"
    assert "authorization_outcome" not in tx_request and "memo" not in tx_request
    assert [p.applied for p in driver.posts] == [Applied.OBSERVED, Applied.SCORED]
    with pytest.raises(RuntimeError, match="publish manifest"):
        driver.label_faults([None])


def _canonical(transaction_id: str) -> CanonicalTransaction:
    return CanonicalTransaction.model_validate_json(
        json.dumps(
            {
                "source_dataset": "gateway",
                "source_row_id": transaction_id,
                "field_coverage": [
                    "transaction_id",
                    "account_id",
                    "amount_minor",
                    "currency",
                    "occurred_at",
                    "ingested_at",
                ],
                "transaction_id": transaction_id,
                "account_id": "acct_000001",
                "amount_minor": 100,
                "currency": "GBP",
                "occurred_at": "2026-09-15T12:00:00.000Z",
                "ingested_at": "2026-09-15T12:00:00.050Z",
            }
        )
    )


def _score(transaction_id: str, seq: int, session: str = "s1") -> ServedScore:
    return ServedScore(
        transaction_id, _canonical(transaction_id), "RECORDED", seq, 0, (), {}, session, seq
    )


def _post(order: int, transaction_id: str) -> Posted:
    return Posted(
        order, TX, {"transaction_id": transaction_id}, "k", 200, {}, Applied.SCORED, "slice"
    )


def test_scored_requests_pair_with_deliveries_in_sequence_order() -> None:
    deliveries = link(
        [_post(0, "tx_a"), _post(1, "tx_b"), _post(2, "tx_a")],
        [_score("tx_a", 3), _score("tx_b", 2), _score("tx_a", 1)],
    )
    assert [(d.served.transaction_id, d.served.seq) for d in deliveries if d.served] == [
        ("tx_a", 1),
        ("tx_b", 2),
        ("tx_a", 3),
    ]


def test_a_delivery_out_of_the_drivers_order_is_refused() -> None:
    with pytest.raises(OrderEvidenceError):
        link([_post(0, "tx_a"), _post(1, "tx_b")], [_score("tx_a", 2), _score("tx_b", 1)])


def test_unpaired_requests_deliveries_and_second_sessions_are_refused() -> None:
    with pytest.raises(LinkError, match=r"no tx\.scored\.v1 delivery"):
        link([_post(0, "tx_a")], [])
    with pytest.raises(LinkError, match="no scored request"):
        link([], [_score("tx_a", 1)])
    with pytest.raises(LinkError, match="writer sessions"):
        link([_post(0, "tx_a"), _post(1, "tx_b")], [_score("tx_a", 1), _score("tx_b", 2, "s2")])
