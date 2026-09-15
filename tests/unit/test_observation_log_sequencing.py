"""The observation log with a fake producer and a fake fence (ADR-0051 §3-4; plan §4.1).

The fake producer honours the callbacks the real factory binds into its configuration, as the
producer-contract tests' fake does, so the verdicts under test are the log's and the publisher's.
The same path against a real broker is tests/integration/test_observation_log_kafka.py.
"""

from __future__ import annotations

import datetime as dt
import threading
from typing import Any

import pytest
from services.gateway.pipeline import ScoringPipeline

from trace_core.contracts.api.transaction import TransactionRequest
from trace_core.contracts.publish import DeliveryLedger, EventPublisher, producer_config
from trace_core.contracts.topics import IDENTITY_EVENTS_V1, TX_SCORED_V1
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.reference import ReferenceFeatureStore
from trace_core.observation.log import SEQ_HEADER, SESSION_HEADER, LogOutcome, ObservationLog
from trace_core.observation.scored_event import build_scored_event
from trace_core.observation.session import WriterSession, WriterSessionError
from trace_core.observation.supervisor import WriterSupervisor
from trace_core.rules.loader import default_loader
from trace_core.scoring.banding import load_thresholds

pytestmark = pytest.mark.unit

NOW = dt.datetime(2026, 3, 1, 12, 0, 0, tzinfo=dt.UTC)
TOPICS = (TX_SCORED_V1, IDENTITY_EVENTS_V1)
HOUR_S = 3_600.0


class FakeError:
    def name(self) -> str:
        return "_MSG_TIMED_OUT"

    def str(self) -> str:
        return "_MSG_TIMED_OUT (fake)"

    def fatal(self) -> bool:
        return False


class FakeMessage:
    def __init__(self, topic: str) -> None:
        self._topic = topic

    def topic(self) -> str:
        return self._topic


class FakeTopic:
    def __init__(self) -> None:
        self.error = None
        self.partitions = {0: object()}


class FakeMetadata:
    def __init__(self, topics: dict[str, FakeTopic]) -> None:
        self.topics = topics


class FakeProducer:
    def __init__(self, config: dict[str, Any], *, outcome: str = "deliver") -> None:
        self.on_delivery = config["on_delivery"]
        self.outcome = outcome
        self.always_full = False
        self.metadata_error: Exception | None = None
        self.metadata_calls = 0
        self.queue: list[dict[str, Any]] = []
        self.produced: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def produce(
        self,
        topic: str,
        *,
        value: bytes | None = None,
        key: bytes | None = None,
        headers: Any = None,
    ) -> None:
        with self._lock:
            if self.always_full:
                raise BufferError("Local: Queue full")
            call = {"topic": topic, "value": value, "key": key, "headers": list(headers or ())}
            self.queue.append(call)
            self.produced.append(call)

    def poll(self, timeout: float = -1) -> int:
        return 0

    def flush(self, timeout: float = -1) -> int:
        with self._lock:
            pending, self.queue = self.queue, []
        for call in pending:
            error = None if self.outcome == "deliver" else FakeError()
            self.on_delivery(error, FakeMessage(call["topic"]))
        return 0

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> FakeMetadata:
        self.metadata_calls += 1
        if self.metadata_error is not None:
            raise self.metadata_error
        return FakeMetadata({name: FakeTopic() for name in TOPICS})


class Fence:
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


def _writer(*, start: bool = True) -> WriterSupervisor:
    fence = Fence()
    writer = WriterSupervisor(
        connect=Conn,
        producer="trace-gateway@test",
        instance_id="gw-test",
        interval_s=HOUR_S,
        lease_s=2 * HOUR_S,
        takeover_grace_s=2 * HOUR_S,
        session_factory=lambda conn: WriterSession(
            ledger=fence, lock=fence, producer="trace-gateway@test", instance_id="gw-test"
        ),
    )
    if start:
        writer.tick()
    return writer


def _publisher(**fake: Any) -> tuple[EventPublisher, FakeProducer]:
    ledger = DeliveryLedger()
    config = producer_config(bootstrap_servers="127.0.0.1:9", client_id="unit", ledger=ledger)
    producer = FakeProducer(config, **fake)
    return EventPublisher(producer, ledger, bootstrap_servers="127.0.0.1:9"), producer


def _log(
    writer: WriterSupervisor, publisher: EventPublisher | None, outcomes: list[Any] | None = None
) -> ObservationLog:
    return ObservationLog(
        writer=writer,
        publisher=publisher,
        topics=TOPICS,
        record=None
        if outcomes is None
        else lambda topic, outcome: outcomes.append((topic, outcome)),
        retry_s=0.01,
        check_timeout_s=0.01,
    )


_PIPELINE = ScoringPipeline(
    pack=default_loader(frozenset(ONLINE_FEATURES.ids)).load(),
    thresholds=load_thresholds(),
    feature_store=ReferenceFeatureStore(),
)


def _scored(i: int) -> dict[str, Any]:
    request = TransactionRequest.model_validate(
        {
            "transaction_id": f"tx_{i:012d}",
            "account_id": "acct_000001",
            "amount_minor": 5_000,
            "currency": "GBP",
            "occurred_at": (NOW + dt.timedelta(seconds=i)).isoformat().replace("+00:00", "Z"),
        }
    )
    outcome = _PIPELINE.score(request, now=NOW + dt.timedelta(seconds=i))
    return build_scored_event(
        canonical=outcome.canonical,
        decision=outcome.decision,
        features=outcome.features,
        context=outcome.context,
        observe_outcome=outcome.observe_outcome.value,
        store_position=outcome.observe_position,
        store_epoch_ms=outcome.store_epoch_ms,
        producer="trace-gateway@0.1.0",
        trace_id="0" * 32,
    )


def test_numbers_are_contiguous_and_travel_as_headers_never_in_the_payload() -> None:
    writer = _writer()
    publisher, producer = _publisher()
    observation_log = _log(writer, publisher)
    observation_log.start()
    assert observation_log.status == "ok"
    for i in range(1, 4):
        sequenced = observation_log.sequence()
        assert sequenced.seq == i
        assert (
            observation_log.publish(sequenced, TX_SCORED_V1, _scored(i)) is LogOutcome.HANDED_OVER
        )
    session = writer.session
    assert session is not None and session.session_id is not None
    for i, call in enumerate(producer.produced, start=1):
        headers = dict(call["headers"])
        assert headers[SESSION_HEADER] == session.session_id.encode()
        assert headers[SEQ_HEADER] == str(i).encode()
        assert call["key"] == b"acct_000001"
        assert b"tracex" not in call["value"], "session identity never enters the payload"


def test_a_session_closes_only_when_every_number_was_handed_over_and_confirmed() -> None:
    writer = _writer()
    publisher, _ = _publisher()
    observation_log = _log(writer, publisher)
    observation_log.start()
    for i in range(1, 3):
        observation_log.publish(observation_log.sequence(), TX_SCORED_V1, _scored(i))
    assert observation_log.close(timeout_s=0.1)


def test_a_session_with_no_observations_may_close() -> None:
    publisher, _ = _publisher()
    observation_log = _log(_writer(), publisher)
    observation_log.start()
    assert observation_log.close(timeout_s=0.1)


def test_a_number_assigned_and_never_produced_makes_the_session_unclosable() -> None:
    publisher, _ = _publisher()
    observation_log = _log(_writer(), publisher)
    observation_log.start()
    observation_log.publish(observation_log.sequence(), TX_SCORED_V1, _scored(1))
    observation_log.unpublished(observation_log.sequence(), TX_SCORED_V1)
    assert not observation_log.close(timeout_s=0.1)


def test_a_shed_record_makes_the_session_unclosable() -> None:
    publisher, producer = _publisher()
    observation_log = _log(_writer(), publisher)
    observation_log.start()
    producer.always_full = True
    outcome = observation_log.publish(observation_log.sequence(), TX_SCORED_V1, _scored(1))
    assert outcome is LogOutcome.SHED
    assert not observation_log.close(timeout_s=0.1)


def test_a_record_the_contract_refuses_makes_the_session_unclosable_and_raises_nothing() -> None:
    publisher, producer = _publisher()
    observation_log = _log(_writer(), publisher)
    observation_log.start()
    invalid = _scored(1)
    invalid["payload"]["observe_outcome"] = "NOT_AN_OUTCOME"
    assert (
        observation_log.publish(observation_log.sequence(), TX_SCORED_V1, invalid)
        is LogOutcome.REFUSED
    )
    assert producer.produced == []
    assert not observation_log.close(timeout_s=0.1)


def test_a_failed_delivery_makes_the_session_unclosable() -> None:
    publisher, _ = _publisher(outcome="fail")
    observation_log = _log(_writer(), publisher)
    observation_log.start()
    assert (
        observation_log.publish(observation_log.sequence(), TX_SCORED_V1, _scored(1))
        is LogOutcome.HANDED_OVER
    )
    assert not observation_log.close(timeout_s=0.1)


def test_without_a_broker_nothing_is_published_and_no_session_closes() -> None:
    outcomes: list[Any] = []
    observation_log = _log(_writer(), None, outcomes)
    observation_log.start()
    assert observation_log.status.startswith("not configured")
    sequenced = observation_log.sequence()
    assert observation_log.publish(sequenced, TX_SCORED_V1, _scored(1)) is LogOutcome.NOT_CONFIGURED
    assert outcomes == [(TX_SCORED_V1, LogOutcome.NOT_CONFIGURED)]
    assert not observation_log.close(timeout_s=0.1)


def test_an_unverified_topic_is_never_published_to_and_is_verified_off_the_request_path() -> None:
    publisher, producer = _publisher()
    producer.metadata_error = RuntimeError("broker unreachable")
    observation_log = _log(_writer(), publisher)
    observation_log.start()
    assert observation_log.status.startswith("unavailable")
    calls = producer.metadata_calls
    sequenced = observation_log.sequence()
    assert (
        observation_log.publish(sequenced, TX_SCORED_V1, _scored(1)) is LogOutcome.TOPIC_UNVERIFIED
    )
    assert producer.produced == []
    assert producer.metadata_calls == calls, "a publish never asks the broker for metadata"
    producer.metadata_error = None
    ready = threading.Event()
    for _ in range(500):
        if observation_log.status == "ok":
            ready.set()
            break
        threading.Event().wait(0.01)
    assert ready.is_set(), observation_log.status
    assert (
        observation_log.publish(observation_log.sequence(), TX_SCORED_V1, _scored(2))
        is LogOutcome.HANDED_OVER
    )
    assert not observation_log.close(timeout_s=0.1), "the session already lost a number"


def test_a_process_that_is_not_the_writer_assigns_nothing() -> None:
    publisher, _ = _publisher()
    observation_log = _log(_writer(start=False), publisher)
    with pytest.raises(WriterSessionError):
        observation_log.sequence()


def test_every_outcome_is_counted_by_topic() -> None:
    outcomes: list[Any] = []
    publisher, producer = _publisher()
    observation_log = _log(_writer(), publisher, outcomes)
    observation_log.start()
    observation_log.publish(observation_log.sequence(), TX_SCORED_V1, _scored(1))
    producer.always_full = True
    observation_log.publish(observation_log.sequence(), TX_SCORED_V1, _scored(2))
    observation_log.unpublished(observation_log.sequence(), IDENTITY_EVENTS_V1)
    assert outcomes == [
        (TX_SCORED_V1, LogOutcome.HANDED_OVER),
        (TX_SCORED_V1, LogOutcome.SHED),
        (IDENTITY_EVENTS_V1, LogOutcome.UNPUBLISHED),
    ]
