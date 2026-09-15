"""The gateway behind the writer fence (ADR-0051 §2): not the writer, not ready, no online writes.

A real `WriterSupervisor` over an always-granting fake fence. The PostgreSQL half of the fence is
proved in tests/integration/test_producer_sessions.py and tests/chaos/test_writer_fence.py.
"""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

import pytest
from fastapi.testclient import TestClient
from httpx import Response
from services.gateway.app import GatewayState, create_app
from services.gateway.config import GatewaySettings
from services.gateway.pipeline import ScoringPipeline

from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.observability.metrics import HotPathMetrics
from trace_core.observation.session import WriterSession
from trace_core.observation.supervisor import WriterSupervisor
from trace_core.rules.loader import default_loader
from trace_core.scoring.banding import load_thresholds
from trace_core.security.service_tokens import MIN_SECRET_LENGTH, ServiceTokenVerifier

pytestmark = pytest.mark.unit

SECRET = "s" * MIN_SECRET_LENGTH
AUTH = {"Authorization": f"Bearer psp-one.{SECRET}"}
HOUR_S = 3_600.0


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class Fence:
    """A lock that is always granted and a ledger that always accepts."""

    def try_acquire(self, key: int) -> bool:
        return True

    def still_held(self, key: int) -> bool:
        return True

    def open(self, *, session_id: str, producer: str, instance_id: str) -> None:
        return None

    def heartbeat(self, session_id: str) -> bool:
        return True

    def close(self, session_id: str, *, last_seq: int) -> bool:
        return True

    def others_live(self, session_id: str, *, within_s: float) -> bool:
        return False


class Conn:
    def close(self) -> None:
        return None


class RecordingAuthorizations:
    def __init__(self) -> None:
        self.records: list[Any] = []

    def record(self, record: Any, event: Any) -> Any:
        self.records.append(record)
        raise ConnectionError("not expected to be reached")


def _writer(*, reachable: bool, clock: Clock | None = None) -> WriterSupervisor:
    def connect() -> Conn:
        if not reachable:
            raise ConnectionError("database unreachable")
        return Conn()

    fence = Fence()
    return WriterSupervisor(
        connect=connect,
        producer="trace-gateway@test",
        instance_id="gw-test",
        # An hour between steps: the supervisor's thread takes none during a test, so the injected
        # clock alone decides the lease.
        interval_s=HOUR_S,
        lease_s=2 * HOUR_S,
        takeover_grace_s=2 * HOUR_S,
        clock=clock or Clock(),
        session_factory=lambda conn: WriterSession(
            ledger=fence, lock=fence, producer="trace-gateway@test", instance_id="gw-test"
        ),
    )


def _client(writer: WriterSupervisor, *, authorizations: Any = None) -> TestClient:
    loader = default_loader(frozenset(ONLINE_FEATURES.ids))
    state = GatewayState(
        settings=GatewaySettings.from_environment({}),
        verifier=ServiceTokenVerifier({"psp-one": SECRET}),
        loader=loader,
        pipeline=ScoringPipeline(
            pack=loader.load(), thresholds=load_thresholds(), feature_store=None
        ),
        metrics=HotPathMetrics(),
        authorizations=authorizations,
        writer=writer,
    )
    return TestClient(create_app(state))


def _iso(moment: dt.datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def _score(client: TestClient) -> Response:
    return client.post(
        "/v1/transactions",
        json={
            "transaction_id": f"tx_{uuid.uuid4().hex[:16]}",
            "account_id": "acct_000001",
            "amount_minor": 5_000,
            "currency": "GBP",
            "occurred_at": _iso(dt.datetime.now(dt.UTC)),
            "merchant_id": "mrch_00001",
            "merchant_mcc": "5411",
        },
        headers={**AUTH, "X-Idempotency-Key": f"idem-{uuid.uuid4().hex}"},
    )


def _identity(client: TestClient, kind: str) -> Response:
    return client.post(
        "/v1/events/identity",
        json={
            "account_id": "acct_000001",
            "identity_event_type": kind,
            "occurred_at": _iso(dt.datetime.now(dt.UTC)),
        },
        headers=AUTH,
    )


def _assert_refused_as_not_the_writer(response: Response) -> None:
    assert response.status_code == 503
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.headers["retry-after"] == "5"
    assert "fenced writer" in response.json()["detail"]


def test_an_instance_that_is_not_the_writer_is_not_ready_and_says_why() -> None:
    with _client(_writer(reachable=False)) as client:
        response = client.get("/readyz")
    assert response.status_code == 503
    assert response.json()["checks"]["writer_session"] == "unreachable: ConnectionError"


def test_scoring_is_refused_while_not_the_writer() -> None:
    with _client(_writer(reachable=False)) as client:
        _assert_refused_as_not_the_writer(_score(client))


@pytest.mark.parametrize("kind", ["PASSWORD_CHANGE", "LOGIN_FAILED"])
def test_identity_events_a_feature_reads_are_refused_while_not_the_writer(kind: str) -> None:
    with _client(_writer(reachable=False)) as client:
        _assert_refused_as_not_the_writer(_identity(client, kind))


def test_events_that_change_no_online_state_are_accepted_by_any_instance() -> None:
    with _client(_writer(reachable=False)) as client:
        login = _identity(client, "LOGIN_SUCCEEDED")
        device = client.post(
            "/v1/events/device",
            json={
                "device_id": "dev_000001",
                "account_id": "acct_000001",
                "device_event_type": "FIRST_SEEN",
                "occurred_at": _iso(dt.datetime.now(dt.UTC)),
            },
            headers=AUTH,
        )
    assert (login.status_code, device.status_code) == (202, 202)


def test_an_authorization_outcome_is_refused_before_anything_is_recorded() -> None:
    authorizations = RecordingAuthorizations()
    occurred = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=5)
    with _client(_writer(reachable=False), authorizations=authorizations) as client:
        response = client.post(
            "/v1/events/authorization",
            json={
                "transaction_id": f"tx_{uuid.uuid4().hex[:16]}",
                "account_id": "acct_000001",
                "authorization_outcome": "DECLINED",
                "decided_at": _iso(occurred + dt.timedelta(milliseconds=340)),
                "transaction_occurred_at": _iso(occurred),
            },
            headers=AUTH,
        )
    _assert_refused_as_not_the_writer(response)
    assert authorizations.records == []


def test_the_writer_serves_and_reports_its_session() -> None:
    with _client(_writer(reachable=True)) as client:
        session = client.get("/readyz").json()["checks"]["writer_session"]
        scored = _score(client)
        changed = _identity(client, "PASSWORD_CHANGE")
    assert session.startswith("active ")
    assert scored.status_code == 200
    assert changed.status_code == 202


def test_a_writer_whose_lease_expires_stops_writing_at_once() -> None:
    clock = Clock()
    with _client(_writer(reachable=True, clock=clock)) as client:
        assert _score(client).status_code == 200
        clock.now += 2 * HOUR_S
        refused = _score(client)
        session = client.get("/readyz").json()["checks"]["writer_session"]
    _assert_refused_as_not_the_writer(refused)
    assert session.startswith("lease expired: the fence was last confirmed")
