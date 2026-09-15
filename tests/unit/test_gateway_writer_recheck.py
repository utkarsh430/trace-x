"""The writer is read again immediately before every online store write (critic B2; ADR-0051 §2).

Between the route's readiness check and the store write, the gateway reconciles completeness on
PostgreSQL. That wait is bounded by the pool (`_postgres_pool`), and the writer's `ready` is read
again after it, so a write never lands past the lease plus the takeover margin. Here the reconcile
itself outlasts the lease, on an injected clock, and nothing may reach the store.
"""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

import pytest
from fastapi.testclient import TestClient
from services.gateway.app import POOL_CONNECT_TIMEOUT_S, GatewayState, _postgres_pool, create_app
from services.gateway.config import POSTGRES_TIMEOUT_S, GatewaySettings
from services.gateway.pipeline import ScoringPipeline
from tests.unit.test_gateway_observation_log import RecordingFence, _identity, _score, _seqs
from tests.unit.test_gateway_writer_fence import (
    AUTH,
    HOUR_S,
    SECRET,
    Clock,
    _assert_refused_as_not_the_writer,
    _iso,
)
from tests.unit.test_observation_log_sequencing import Conn, FakeProducer, _publisher

from trace_core.contracts.topics import IDENTITY_EVENTS_V1, TX_SCORED_V1
from trace_core.features.completeness import HoleReason
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.reference import ReferenceFeatureStore
from trace_core.observability.metrics import HotPathMetrics
from trace_core.observation.log import ObservationLog
from trace_core.observation.session import WriterSession
from trace_core.observation.supervisor import WriterSupervisor
from trace_core.repositories.postgres_authorization import (
    AuthorizationOutcomeRecord,
    Delivery,
    DeliveryReceipt,
)
from trace_core.rules.loader import default_loader
from trace_core.scoring.banding import load_thresholds
from trace_core.security.service_tokens import ServiceTokenVerifier

pytestmark = pytest.mark.unit


class SpyStore:
    """The reference store, remembering every write that reached it."""

    def __init__(self) -> None:
        self.inner = ReferenceFeatureStore()
        self.writes: list[str] = []

    def score(self, observation: Any) -> Any:
        self.writes.append("score")
        return self.inner.score(observation)

    def observe(self, observation: Any) -> Any:
        self.writes.append("observe")
        return self.inner.observe(observation)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


class LeaseOutlastingGuard:
    """A completeness guard whose reconcile, once armed, takes longer than the writer's lease."""

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.armed = False
        self.unrecorded: list[HoleReason] = []

    @property
    def pending(self) -> bool:
        return False

    def reconcile(self) -> None:
        if self.armed:
            self.clock.now += 3 * HOUR_S

    def resume(self) -> None:
        return None

    def observation_unrecorded(self, reason: HoleReason) -> None:
        self.unrecorded.append(reason)


class RecordingAuthorizations:
    def __init__(self) -> None:
        self.records: list[AuthorizationOutcomeRecord] = []

    def record(self, record: AuthorizationOutcomeRecord, event: Any) -> DeliveryReceipt:
        self.records.append(record)
        return DeliveryReceipt(delivery=Delivery.RECORDED, recorded=record)


class Gateway:
    def __init__(self, *, authorizations: Any = None) -> None:
        self.fence, clock = RecordingFence(), Clock()
        self.writer = WriterSupervisor(
            connect=Conn,
            producer="trace-gateway@0.1.0",
            instance_id="gw-test",
            interval_s=HOUR_S,
            lease_s=2 * HOUR_S,
            takeover_grace_s=2 * HOUR_S,
            clock=clock,
            session_factory=lambda conn: WriterSession(
                ledger=self.fence, lock=self.fence, producer="trace-gateway@0.1.0",
                instance_id="gw-test",
            ),
        )  # fmt: skip
        self.store, self.guard = SpyStore(), LeaseOutlastingGuard(clock)
        publisher, producer = _publisher()
        self.producer: FakeProducer = producer
        loader = default_loader(frozenset(ONLINE_FEATURES.ids))
        state = GatewayState(
            settings=GatewaySettings.from_environment({}),
            verifier=ServiceTokenVerifier({"psp-one": SECRET}),
            loader=loader,
            pipeline=ScoringPipeline(
                pack=loader.load(),
                thresholds=load_thresholds(),
                feature_store=self.store,
                completeness=self.guard,  # type: ignore[arg-type]
                writer=self.writer,
            ),
            metrics=HotPathMetrics(),
            authorizations=authorizations,
            writer=self.writer,
            observation_log=ObservationLog(
                writer=self.writer,
                publisher=publisher,
                topics=(TX_SCORED_V1, IDENTITY_EVENTS_V1),
                retry_s=HOUR_S,
                check_timeout_s=0.01,
            ),
        )
        self.client = TestClient(create_app(state))

    def assigned(self) -> int | None:
        session = self.writer.session
        return None if session is None else session.last_seq


def test_a_fence_lost_while_scoring_waits_writes_nothing_and_publishes_no_number() -> None:
    gateway = Gateway()
    with gateway.client as client:
        assert _score(client).status_code == 200
        assert gateway.store.writes == ["score"]
        gateway.guard.armed = True
        refused = _score(client)
        assigned = gateway.assigned()
    _assert_refused_as_not_the_writer(refused)
    assert assigned == 2, "the refusal came after sequencing, from the check before the write"
    assert gateway.store.writes == ["score"], "the second transaction never reached the store"
    assert _seqs(gateway.producer) == [(TX_SCORED_V1, 1)], "number 2 is never published"
    assert gateway.fence.closed == {}, "an unpublished number leaves the session unclosed"


def test_a_fence_lost_while_an_identity_event_waits_writes_nothing() -> None:
    gateway = Gateway()
    with gateway.client as client:
        gateway.guard.armed = True
        refused = _identity(client, "PASSWORD_CHANGE")
        assigned = gateway.assigned()
    _assert_refused_as_not_the_writer(refused)
    assert assigned == 1, "the refusal came after sequencing, from the check before the write"
    assert gateway.store.writes == []
    assert gateway.producer.produced == []
    assert gateway.fence.closed == {}


def test_a_fence_lost_before_applying_an_outcome_keeps_the_record_and_withdraws_completeness() -> (
    None
):
    authorizations = RecordingAuthorizations()
    gateway = Gateway(authorizations=authorizations)
    occurred = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=5)
    with gateway.client as client:
        gateway.guard.armed = True
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
    assert len(authorizations.records) == 1, "recorded durably first: a retry is a DUPLICATE"
    assert gateway.store.writes == [], "never applied online by this process"
    assert gateway.guard.unrecorded == [HoleReason.BREAKER_OPEN], "completeness is withdrawn"


def test_every_wait_on_the_gateway_pool_is_bounded_by_the_postgres_timeout() -> None:
    captured: dict[str, Any] = {}

    class FakePool:
        def __init__(self, dsn: str, **kwargs: Any) -> None:
            captured.update(kwargs, dsn=dsn)

    settings = GatewaySettings.from_environment({})
    _postgres_pool(FakePool, settings)
    assert captured["dsn"] == settings.postgres_dsn and captured["open"] is False
    assert captured["timeout"] == POSTGRES_TIMEOUT_S, "acquiring a connection"
    assert captured["kwargs"] == {
        "connect_timeout": POOL_CONNECT_TIMEOUT_S,
        "options": f"-c statement_timeout={round(POSTGRES_TIMEOUT_S * 1000)}",
    }, "each statement"
