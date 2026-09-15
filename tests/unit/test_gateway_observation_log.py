"""The gateway's observation log end to end over HTTP, with a fake producer (ADR-0051 §3-4).

Sequence, write online, produce: numbers are contiguous across scoring and identity events, only
writes that change online state consume one, and a session closes only on a confirmed flush of
everything it sequenced. The same path against a real broker is
tests/integration/test_observation_log_kafka.py.
"""

from __future__ import annotations

import datetime as dt
import json
import uuid
from typing import Any

import pytest
from fastapi.testclient import TestClient
from httpx import Response
from services.gateway.app import GatewayState, create_app
from services.gateway.config import GatewaySettings
from services.gateway.pipeline import ScoringPipeline
from tests.unit.test_observation_log_sequencing import Conn, FakeProducer, _publisher

from trace_core.contracts.events.identity_events_v1 import IdentityEventV1
from trace_core.contracts.events.tx_scored_v1 import TxScoredV1
from trace_core.contracts.topics import IDENTITY_EVENTS_V1, TX_SCORED_V1
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.observability.metrics import HotPathMetrics
from trace_core.observation.log import SEQ_HEADER, SESSION_HEADER, ObservationLog
from trace_core.observation.session import WriterSession
from trace_core.observation.supervisor import WriterSupervisor
from trace_core.rules.loader import default_loader
from trace_core.scoring.banding import load_thresholds
from trace_core.security.service_tokens import MIN_SECRET_LENGTH, ServiceTokenVerifier

pytestmark = pytest.mark.unit

SECRET = "s" * MIN_SECRET_LENGTH
AUTH = {"Authorization": f"Bearer psp-one.{SECRET}"}
HOUR_S = 3_600.0


class RecordingFence:
    """An always-granting lock and ledger that remembers how its sessions closed."""

    def __init__(self) -> None:
        self.closed: dict[str, int] = {}

    def try_acquire(self, key: int) -> bool:
        return True

    def still_held(self, key: int) -> bool:
        return True

    def open(self, *, session_id: str, producer: str, instance_id: str) -> None:
        return None

    def heartbeat(self, session_id: str) -> bool:
        return True

    def close(self, session_id: str, *, last_seq: int) -> bool:
        self.closed[session_id] = last_seq
        return True

    def others_live(self, session_id: str, *, within_s: float) -> bool:
        return False


class AlwaysTriage(ScoringPipeline):
    """Every decision opens an investigation, so a missing system of record refuses it."""

    def opens_investigation(self, decision: Any) -> bool:
        return True


def _writer(fence: RecordingFence) -> WriterSupervisor:
    return WriterSupervisor(
        connect=Conn,
        producer="trace-gateway@0.1.0",
        instance_id="gw-test",
        interval_s=HOUR_S,
        lease_s=2 * HOUR_S,
        takeover_grace_s=2 * HOUR_S,
        session_factory=lambda conn: WriterSession(
            ledger=fence, lock=fence, producer="trace-gateway@0.1.0", instance_id="gw-test"
        ),
    )


def _client(
    *,
    fence: RecordingFence,
    with_log: bool = True,
    pipeline_type: type[ScoringPipeline] = ScoringPipeline,
    **fake: Any,
) -> tuple[TestClient, FakeProducer | None]:
    writer = _writer(fence)
    publisher, producer = _publisher(**fake) if with_log else (None, None)
    loader = default_loader(frozenset(ONLINE_FEATURES.ids))
    metrics = HotPathMetrics()
    state = GatewayState(
        settings=GatewaySettings.from_environment({}),
        verifier=ServiceTokenVerifier({"psp-one": SECRET}),
        loader=loader,
        pipeline=pipeline_type(
            pack=loader.load(), thresholds=load_thresholds(), feature_store=None
        ),
        metrics=metrics,
        writer=writer,
        observation_log=ObservationLog(
            writer=writer,
            publisher=publisher,
            topics=(TX_SCORED_V1, IDENTITY_EVENTS_V1),
            retry_s=HOUR_S,
            check_timeout_s=0.01,
        ),
    )
    return TestClient(create_app(state)), producer


def _iso(moment: dt.datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def _score(client: TestClient, *, occurred: dt.datetime | None = None) -> Response:
    return client.post(
        "/v1/transactions",
        json={
            "transaction_id": f"tx_{uuid.uuid4().hex[:16]}",
            "account_id": "acct_000001",
            "amount_minor": 5_000,
            "currency": "GBP",
            "occurred_at": _iso(occurred or dt.datetime.now(dt.UTC)),
        },
        headers={**AUTH, "X-Idempotency-Key": f"idem-{uuid.uuid4().hex}"},
    )


def _identity(client: TestClient, kind: str, *, key: str | None = None) -> Response:
    headers = {**AUTH, "X-Idempotency-Key": key} if key else AUTH
    return client.post(
        "/v1/events/identity",
        json={
            "account_id": "acct_000001",
            "identity_event_type": kind,
            "occurred_at": _iso(dt.datetime(2026, 9, 14, 10, 0, tzinfo=dt.UTC)),
        },
        headers=headers,
    )


def _seqs(producer: FakeProducer) -> list[tuple[str, int]]:
    return [(call["topic"], int(dict(call["headers"])[SEQ_HEADER])) for call in producer.produced]


def test_scoring_and_identity_writes_are_sequenced_contiguously_and_published() -> None:
    fence = RecordingFence()
    client, producer = _client(fence=fence)
    assert producer is not None
    with client:
        assert _score(client).status_code == 200
        assert _identity(client, "PASSWORD_CHANGE").status_code == 202
        assert _score(client).status_code == 200
        assert client.get("/readyz").json()["checks"]["observation_log"] == "ok"
    assert _seqs(producer) == [(TX_SCORED_V1, 1), (IDENTITY_EVENTS_V1, 2), (TX_SCORED_V1, 3)]
    sessions = {dict(call["headers"])[SESSION_HEADER] for call in producer.produced}
    assert len(sessions) == 1
    TxScoredV1.model_validate_json(producer.produced[0]["value"])
    IdentityEventV1.model_validate_json(producer.produced[1]["value"])
    assert list(fence.closed.values()) == [3], "a confirmed flush of all three closes the session"


def test_writes_that_change_no_online_state_consume_no_number() -> None:
    fence = RecordingFence()
    client, producer = _client(fence=fence)
    assert producer is not None
    with client:
        assert _identity(client, "LOGIN_SUCCEEDED").status_code == 202
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
        assert device.status_code == 202
        assert _score(client).status_code == 200
    assert _seqs(producer) == [(TX_SCORED_V1, 1)]


def test_a_transaction_refused_for_clock_skew_consumes_no_number() -> None:
    fence = RecordingFence()
    client, producer = _client(fence=fence)
    assert producer is not None
    with client:
        skewed = _score(client, occurred=dt.datetime.now(dt.UTC) + dt.timedelta(days=3))
        assert skewed.status_code == 422
        assert _score(client).status_code == 200
    assert _seqs(producer) == [(TX_SCORED_V1, 1)]
    assert list(fence.closed.values()) == [1]


def test_a_retried_identity_event_is_the_same_event_on_the_log() -> None:
    fence = RecordingFence()
    client, producer = _client(fence=fence)
    assert producer is not None
    with client:
        for _ in range(2):
            assert _identity(client, "PASSWORD_CHANGE", key="retry-1").status_code == 202
        assert _identity(client, "PASSWORD_CHANGE", key="retry-2").status_code == 202
    ids = [json.loads(call["value"])["envelope"]["event_id"] for call in producer.produced]
    assert ids[0] == ids[1] != ids[2], "the dedup identity follows (token, key), as in the store"
    assert [seq for _, seq in _seqs(producer)] == [1, 2, 3], "each delivery is still sequenced"


def test_a_transaction_triage_refuses_is_still_logged_and_the_session_can_close() -> None:
    fence = RecordingFence()
    client, producer = _client(fence=fence, pipeline_type=AlwaysTriage)
    assert producer is not None
    with client:
        refused = _score(client)
    assert refused.status_code == 503
    assert _seqs(producer) == [(TX_SCORED_V1, 1)]
    summary = json.loads(producer.produced[0]["value"])["payload"]["decision_summary"]
    assert "case_id" not in summary, "no case was recorded, and the log does not claim one"
    assert list(fence.closed.values()) == [1]


def test_a_broker_outage_costs_coverage_never_a_decision() -> None:
    fence = RecordingFence()
    client, producer = _client(fence=fence)
    assert producer is not None
    producer.metadata_error = RuntimeError("broker unreachable")
    with client:
        assert _score(client).status_code == 200
        ready = client.get("/readyz").json()["checks"]
    assert ready["observation_log"].startswith("unavailable")
    assert producer.produced == []
    assert fence.closed == {}, "an unpublished number leaves the session unclosed"


def test_without_a_log_the_session_is_left_unclosed_at_shutdown() -> None:
    """The slice-2 gateway closed its session as if everything had been logged. Nothing was: the
    close must be a gap, not a claim."""
    fence = RecordingFence()
    client, _ = _client(fence=fence, with_log=False)
    with client:
        assert _score(client).status_code == 200
        assert (
            client.get("/readyz").json()["checks"]["observation_log"].startswith("not configured")
        )
    assert fence.closed == {}


def test_a_failed_delivery_leaves_the_session_unclosed() -> None:
    fence = RecordingFence()
    client, producer = _client(fence=fence, outcome="fail")
    assert producer is not None
    with client:
        assert _score(client).status_code == 200
    assert _seqs(producer) == [(TX_SCORED_V1, 1)]
    assert fence.closed == {}


def test_the_outbox_relay_is_off_unless_enabled_and_never_starts_without_a_broker() -> None:
    from services.gateway.app import _outbox_relay

    metrics = HotPathMetrics()
    default = GatewaySettings.from_environment({})
    assert default.outbox_relay is False
    assert _outbox_relay(default, object(), "gw-relay", metrics) is None
    enabled = GatewaySettings.from_environment({"TRACE_GATEWAY_OUTBOX_RELAY": "true"})
    assert enabled.outbox_relay is True
    assert _outbox_relay(enabled, object(), "gw-relay", metrics) is None, "no broker, no relay"
    assert _outbox_relay(enabled, None, "gw-relay", metrics) is None, "no system of record"
