"""`trace-gateway` over HTTP: status codes, headers and refusals.

Driven through the real ASGI app with `TestClient`, against a real rule pack and
real thresholds. Redis and Postgres are absent here on purpose — that is the
documented degraded mode, and it means this suite establishes the one property
that matters most about degradation: **the gateway answers.** The chaos layer
kills a live Redis under a live gateway; this layer proves the code path exists
and returns the right thing.

The status codes are `docs/API_CONTRACTS.md` §4, and the split between 400 and
422 is §6.1 against §6.6: an unknown field is a shape error the client fixes in
its serialiser, an out-of-enum value is a data error it fixes in its data.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from services.gateway.app import GatewayState, create_app
from services.gateway.config import GatewaySettings
from services.gateway.pipeline import ScoringPipeline

from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.reference import ReferenceFeatureStore
from trace_core.observability.metrics import HotPathMetrics
from trace_core.rules.loader import default_loader
from trace_core.scoring.banding import load_thresholds
from trace_core.security.service_tokens import MIN_SECRET_LENGTH, ServiceTokenVerifier

pytestmark = pytest.mark.contract

SECRET = "s" * MIN_SECRET_LENGTH
TOKEN = f"psp-one.{SECRET}"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


class _ReferenceStore:
    """The naive feature store behind the pipeline's expected shape.

    A genuine second implementation of the declared semantics (ADR-0032), so
    these tests exercise real feature evaluation rather than a stub that agrees
    with whatever the code does.
    """

    def __init__(self) -> None:
        self.inner = ReferenceFeatureStore()

    def snapshot(self, **kwargs: Any) -> Any:
        return self.inner.snapshot(**kwargs)

    def observe(self, event: Any) -> None:
        self.inner.observe(event)


def _state(*, feature_store: Any = None, triage: Any = None) -> GatewayState:
    return GatewayState(
        settings=GatewaySettings.from_environment({}),
        verifier=ServiceTokenVerifier({"psp-one": SECRET}),
        loader=default_loader(frozenset(ONLINE_FEATURES.ids)),
        pipeline=ScoringPipeline(
            pack=default_loader(frozenset(ONLINE_FEATURES.ids)).load(),
            thresholds=load_thresholds(),
            feature_store=feature_store,
        ),
        metrics=HotPathMetrics(),
        triage=triage,
    )


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(create_app(_state(feature_store=_ReferenceStore()))) as running:
        yield running


def _body(**over: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "transaction_id": f"tx_{uuid.uuid4().hex[:16]}",
        "account_id": "acct_000001",
        "amount_minor": 5_000,
        "currency": "GBP",
        "occurred_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
        "merchant_id": "mrch_00001",
        "merchant_mcc": "5411",
        "merchant_country": "GB",
        "device_id": "dev_000001",
        "channel": "CARD_PRESENT",
    }
    payload.update(over)
    return payload


def _headers(**extra: str) -> dict[str, str]:
    return {**AUTH, "X-Idempotency-Key": f"idem-{uuid.uuid4().hex}", **extra}


# --- the happy path ------------------------------------------------------------


def test_a_valid_transaction_is_scored(client: TestClient) -> None:
    response = client.post("/v1/transactions", json=_body(), headers=_headers())
    assert response.status_code == 200, response.text
    decision = response.json()
    assert decision["decision"] in {"APPROVE", "FLAG"}
    assert 0.0 <= decision["score"] <= 1.0
    assert decision["rule_pack_digest"].startswith("sha256:")


def test_the_response_carries_the_documented_headers(client: TestClient) -> None:
    """docs/API_CONTRACTS.md §4. `X-Feature-Source` in particular must never be
    silently ambiguous: a caller has to be able to tell a reconciled decision
    from an online-only one."""
    response = client.post("/v1/transactions", json=_body(), headers=_headers())
    assert response.headers["X-Request-Id"]
    assert response.headers["X-Trace-Degraded"] in {"true", "false"}
    assert response.headers["X-Feature-Source"] == "ONLINE_ONLY"


def test_a_supplied_request_id_is_echoed(client: TestClient) -> None:
    response = client.post(
        "/v1/transactions", json=_body(), headers=_headers(**{"X-Request-Id": "req_mine"})
    )
    assert response.headers["X-Request-Id"] == "req_mine"


# --- authentication ------------------------------------------------------------


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": "Bearer wrong.secret"},
        {"Authorization": f"Bearer psp-unknown.{SECRET}"},
        {"Authorization": TOKEN},
    ],
    ids=["absent", "wrong-secret", "unknown-id", "no-bearer-prefix"],
)
def test_an_unauthenticated_request_is_refused(client: TestClient, headers: dict[str, str]) -> None:
    response = client.post(
        "/v1/transactions",
        json=_body(),
        headers={**headers, "X-Idempotency-Key": "idem-1"},
    )
    assert response.status_code == 401
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json()["type"].endswith("unauthenticated")


def test_authentication_precedes_scoring(client: TestClient) -> None:
    """An unauthenticated caller must not be able to make the gateway do work --
    or to consume a real token's rate budget."""
    response = client.post("/v1/transactions", json={"nonsense": True}, headers={})
    assert response.status_code == 401, "validation ran before authentication"


# --- the 400 / 422 split -------------------------------------------------------


def test_an_unknown_field_is_a_400(client: TestClient) -> None:
    """§6.1. A shape error: the client fixes its serialiser."""
    response = client.post("/v1/transactions", json={**_body(), "surprise": 1}, headers=_headers())
    assert response.status_code == 400
    assert response.json()["type"].endswith("malformed-request")


def test_a_missing_required_field_is_a_400(client: TestClient) -> None:
    body = _body()
    del body["account_id"]
    assert client.post("/v1/transactions", json=body, headers=_headers()).status_code == 400


def test_an_out_of_enum_value_is_a_422(client: TestClient) -> None:
    """§6.6. A data error: the shape is right and the value cannot be true."""
    response = client.post("/v1/transactions", json=_body(channel="TELEPATHY"), headers=_headers())
    assert response.status_code == 422
    assert response.json()["type"].endswith("invalid-request")


def test_a_bad_identifier_pattern_is_a_422(client: TestClient) -> None:
    response = client.post("/v1/transactions", json=_body(account_id="acc_1"), headers=_headers())
    assert response.status_code == 422


def test_a_float_amount_is_refused(client: TestClient) -> None:
    """Money is integer minor units, never a float (CLAUDE.md §6)."""
    assert (
        client.post("/v1/transactions", json=_body(amount_minor=19.99), headers=_headers())
    ).status_code in {400, 422}


def test_an_error_never_echoes_the_submitted_value(client: TestClient) -> None:
    """Request fields are attacker-controlled and may carry PII, and an error
    response is a place people paste into tickets (docs/SECURITY.md §10)."""
    secret_looking = "4111111111111111"
    response = client.post(
        "/v1/transactions",
        json={**_body(), "surprise": secret_looking},
        headers=_headers(),
    )
    assert response.status_code == 400
    assert secret_looking not in response.text


def test_every_error_is_an_rfc_9457_problem_document(client: TestClient) -> None:
    response = client.post("/v1/transactions", json={"bad": 1}, headers=_headers())
    document = response.json()
    assert set(document) >= {
        "type",
        "title",
        "status",
        "detail",
        "instance",
        "trace_id",
        "request_id",
    }
    assert document["status"] == response.status_code


# --- idempotency ----------------------------------------------------------------


def test_a_request_without_an_idempotency_key_is_refused(client: TestClient) -> None:
    """§5: required on every mutating POST. Inventing one would make a retry a
    new request, which is the opposite of what the header is for."""
    response = client.post("/v1/transactions", json=_body(), headers=AUTH)
    assert response.status_code == 400
    assert "X-Idempotency-Key" in response.json()["detail"]


# --- health and readiness --------------------------------------------------------


def test_liveness_touches_no_dependency(client: TestClient) -> None:
    """A liveness probe that fails on a database blip restarts a healthy
    process, turning a dependency wobble into an outage."""
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_readiness_fails_when_the_system_of_record_is_absent(client: TestClient) -> None:
    """ADR-0035: Postgres unreachable means a CRITICAL transaction's
    investigation cannot be durably recorded, so the instance must be drained."""
    response = client.get("/readyz")
    assert response.status_code == 503
    assert response.json()["ready"] is False
    assert "postgres" in response.json()["checks"]


def test_redis_absence_does_not_fail_readiness(client: TestClient) -> None:
    """The hot path is designed to work without it; draining would turn a planned
    degradation into an outage."""
    checks = client.get("/readyz").json()["checks"]
    assert "redis" in checks


def test_metrics_are_exposed_without_a_collector(client: TestClient) -> None:
    """ARCHITECTURE §14: `/metrics` still works with the `obs` profile down. A
    scrape endpoint needs no collector."""
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "# HELP" in response.text or response.text == ""


# --- degraded scoring ------------------------------------------------------------


def test_the_gateway_answers_with_no_feature_store_at_all() -> None:
    """ARCHITECTURE §18's Redis row, at the HTTP layer: rules-only, never a 5xx."""
    with TestClient(create_app(_state(feature_store=None))) as client:
        response = client.post("/v1/transactions", json=_body(), headers=_headers())
    assert response.status_code == 200
    assert response.headers["X-Trace-Degraded"] == "true"
    assert "redis_unavailable" in response.json()["degraded_reasons"]


def test_a_degraded_decision_is_still_a_decision() -> None:
    with TestClient(create_app(_state(feature_store=None))) as client:
        decision = client.post("/v1/transactions", json=_body(), headers=_headers()).json()
    assert decision["decision"] == "APPROVE"
    assert decision["insufficient_history_features"], (
        "a degraded decision must report what it could not see, or it reads as a "
        "confident assessment made on complete information"
    )


# --- event ingress ----------------------------------------------------------------


def test_identity_events_are_accepted(client: TestClient) -> None:
    response = client.post(
        "/v1/events/identity",
        json={
            "account_id": "acct_000001",
            "identity_event_type": "PASSWORD_CHANGE",
            "occurred_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
        },
        headers=AUTH,
    )
    assert response.status_code == 202
    assert response.json()["accepted"] is True


class _RecordingStore(_ReferenceStore):
    """Keeps what the gateway asked the store to record, in order."""

    def __init__(self) -> None:
        super().__init__()
        self.observed: list[Any] = []

    def observe(self, event: Any) -> None:
        self.observed.append(event)
        super().observe(event)


@pytest.mark.parametrize(
    ("event_type", "stream"),
    [
        ("PASSWORD_CHANGE", "IDENTITY_CHANGE"),
        ("EMAIL_CHANGE", "IDENTITY_CHANGE"),
        ("PHONE_CHANGE", "IDENTITY_CHANGE"),
        ("ADDRESS_CHANGE", "IDENTITY_CHANGE"),
        ("MFA_RESET", "IDENTITY_CHANGE"),
        ("LOGIN_FAILED", "IDENTITY_FAILED_LOGIN"),
        ("LOGIN_SUCCEEDED", None),
        ("MFA_ENROLLED", None),
        ("UNKNOWN", None),
    ],
)
def test_an_identity_event_feeds_only_its_declared_stream(
    event_type: str, stream: str | None
) -> None:
    """ADR-0046 §4. The handler used to record every type that was not a failed login as an
    identity change, so a successful login reset `hours_since_identity_change` -- the recency
    signal an account takeover is detected by."""
    store = _RecordingStore()
    with TestClient(create_app(_state(feature_store=store))) as running:
        response = running.post(
            "/v1/events/identity",
            json={
                "account_id": "acct_000001",
                "identity_event_type": event_type,
                "occurred_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
            },
            headers=AUTH,
        )
    assert response.status_code == 202
    assert [event.stream.value for event in store.observed] == ([] if stream is None else [stream])


def test_a_device_event_changes_no_online_state() -> None:
    """No released feature reads a device event (ADR-0046 §4); recording one as a
    transaction would add a phantom to every velocity count on the account."""
    store = _RecordingStore()
    with TestClient(create_app(_state(feature_store=store))) as running:
        response = running.post(
            "/v1/events/device",
            json={
                "device_id": "dev_000001",
                "account_id": "acct_000001",
                "device_event_type": "FIRST_SEEN",
                "occurred_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
            },
            headers=AUTH,
        )
    assert response.status_code == 202
    assert store.observed == []


def test_device_events_are_accepted(client: TestClient) -> None:
    response = client.post(
        "/v1/events/device",
        json={
            "device_id": "dev_000001",
            "account_id": "acct_000001",
            "device_event_type": "FIRST_SEEN",
            "occurred_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
        },
        headers=AUTH,
    )
    assert response.status_code == 202


def test_event_ingress_also_requires_authentication(client: TestClient) -> None:
    response = client.post(
        "/v1/events/identity",
        json={
            "account_id": "acct_000001",
            "identity_event_type": "PASSWORD_CHANGE",
            "occurred_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
        },
    )
    assert response.status_code == 401


@pytest.mark.parametrize(
    ("path", "body"),
    [
        (
            "/v1/events/identity",
            {"account_id": "acct_000001", "identity_event_type": "LOGIN_FAILED"},
        ),
        (
            "/v1/events/device",
            {
                "device_id": "dev_000001",
                "account_id": "acct_000001",
                "device_event_type": "FIRST_SEEN",
            },
        ),
    ],
)
def test_an_event_dated_beyond_the_future_skew_bound_is_refused_and_not_recorded(
    path: str, body: dict[str, Any]
) -> None:
    """The same 24 h bound as transactions (docs/EVENT_CONTRACTS.md §6.3). The completeness
    guard's resume margin assumes no recorded stream accepts anything further ahead."""
    store = _RecordingStore()
    ahead = dt.datetime.now(dt.UTC) + dt.timedelta(days=3)
    with TestClient(create_app(_state(feature_store=store))) as running:
        response = running.post(
            path,
            json={**body, "occurred_at": ahead.isoformat().replace("+00:00", "Z")},
            headers=AUTH,
        )
    assert response.status_code == 422, response.text
    assert store.observed == []
