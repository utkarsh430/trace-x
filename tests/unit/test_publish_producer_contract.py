"""The producer factory: its contract, validation, keys, trace header, accounting and verdicts.

Unit-layer, so the producer is a fake -- but a fake that honours the callbacks a
real producer is configured with: it reports each message through the
`on_delivery` the factory bound into its configuration, exactly as librdkafka does
from `flush`, and it answers metadata requests the way a broker with automatic
topic creation disabled does. What is under test is the factory's verdict, not
Kafka; the same verdicts against a real, paused broker are in
`tests/integration/test_kafka_platform.py`, which also proves librdkafka accepts
the contract configuration.

Every failure verdict has a passing control beside it, so a close that raised on
everything would fail this file as surely as one that raised on nothing.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import random
import threading
from typing import Any

import pytest
from data.generator.emit import KafkaSink

from trace_core.contracts.envelope import build_event
from trace_core.contracts.publish import (
    DEFAULT_MESSAGE_TIMEOUT_MS,
    PRODUCER_CONTRACT,
    PUBLISH_OUTCOMES_TOTAL,
    PUBLISHER_ERRORS_TOTAL,
    TRACE_ID_HEADER,
    DeliveryLedger,
    EventPublisher,
    is_loopback_bootstrap,
    producer_config,
)
from trace_core.domain.errors import (
    ContractError,
    EventPublishError,
    SchemaValidationError,
    UnreleasedTopicError,
)
from trace_core.domain.identifiers import uuid7
from trace_core.domain.time import event_time, processing_time, to_millis

pytestmark = pytest.mark.unit

APPROVED_CONTRACT = {
    # The approved Phase 3 Step 2 producer settings, written out by hand.
    "enable.idempotence": True,
    "acks": "all",
    "compression.type": "zstd",
    "partitioner": "murmur2_random",
    "queue.buffering.max.kbytes": 65_536,
}
RELEASED = ("tx.raw.v1", "identity.events.v1", "device.events.v1", "investigation.requested.v1")
T0 = dt.datetime(2026, 9, 3, 10, 0, tzinfo=dt.UTC)
_RNG = random.Random(4242)
LOOPBACK = "127.0.0.1:9"


def _envelope(event_type: str, payload: dict[str, Any], trace_id: str) -> dict[str, Any]:
    event: dict[str, Any] = build_event(
        event_type=event_type,
        occurred_at=event_time(T0),
        payload=payload,
        producer="trace-test@1.0.0",
        trace_id=trace_id,
        correlation_id="corr_producer_contract",
        ingested_at=processing_time(T0 + dt.timedelta(milliseconds=10)),
        event_id=uuid7(millis=to_millis(T0), rng=_RNG),
    )
    return event


def _tx(account: str = "acct_000000123", *, trace_id: str = "ab" * 16) -> dict[str, Any]:
    return _envelope(
        "tx.raw",
        {
            "transaction_id": "tx_000000000001",
            "account_id": account,
            "card_id": "card_000000123",
            "device_id": "dev_000000456",
            "merchant_id": "mrch_000001",
            "ip_id": "ip_0000123",
            "amount_minor": 1_250,
            "currency": "GBP",
            "channel": "CARD_PRESENT",
            "entry_mode": "CHIP",
            "merchant_mcc": "5411",
            "merchant_country": "GB",
            "latitude": 51.5,
            "longitude": -0.12,
            "authorization_outcome": "APPROVED",
        },
        trace_id,
    )


def _device(device: str = "dev_000000456") -> dict[str, Any]:
    return _envelope(
        "device.event",
        {
            "device_id": device,
            "account_id": "acct_000000123",
            "device_event_type": "FIRST_SEEN",
            "platform": "ios",
        },
        "cd" * 16,
    )


def _bytes(event: dict[str, Any]) -> bytes:
    return json.dumps(event).encode()


class FakeError:
    def __init__(self, name: str = "_MSG_TIMED_OUT", *, fatal: bool = False) -> None:
        self._name = name
        self._fatal = fatal

    def name(self) -> str:
        return self._name

    def str(self) -> str:
        return f"{self._name} (fake)"

    def fatal(self) -> bool:
        return self._fatal


class FakeMessage:
    def __init__(self, topic: str) -> None:
        self._topic = topic

    def topic(self) -> str:
        return self._topic


class FakeTopic:
    def __init__(self, error: Any = None) -> None:
        self.error = error
        self.partitions = {} if error is not None else {0: object()}


class FakeMetadata:
    def __init__(self, topics: dict[str, FakeTopic]) -> None:
        self.topics = topics


class FakeProducer:
    """Reports outcomes through the configured callbacks, like librdkafka's flush."""

    def __init__(
        self,
        config: dict[str, Any],
        *,
        outcome: str = "deliver",
        full_for: int = 0,
        always_full: bool = False,
        missing_topics: tuple[str, ...] = (),
        metadata_error: Exception | None = None,
    ) -> None:
        self.on_delivery = config["on_delivery"]
        self.error_cb = config["error_cb"]
        self.outcome = outcome
        self.full_for = full_for
        self.always_full = always_full
        self.missing_topics = missing_topics
        self.metadata_error = metadata_error
        self.queue: list[dict[str, Any]] = []
        self.produced: list[dict[str, Any]] = []
        self.polls = 0
        self.metadata_calls = 0
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
            if self.always_full or self.full_for > 0:
                self.full_for -= 1
                raise BufferError("Local: Queue full")
            call = {"topic": topic, "value": value, "key": key, "headers": headers}
            self.queue.append(call)
            self.produced.append(call)

    def poll(self, timeout: float = -1) -> int:
        self.polls += 1
        return 0

    def flush(self, timeout: float = -1) -> int:
        with self._lock:
            if self.outcome == "hang":
                return len(self.queue)
            pending, self.queue = self.queue, []
        if self.outcome == "lose":
            return 0  # the queue emptied but no report arrived: must not pass as delivered
        for call in pending:
            error = None if self.outcome == "deliver" else FakeError()
            self.on_delivery(error, FakeMessage(call["topic"]))
        return 0

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> FakeMetadata:
        self.metadata_calls += 1
        if self.metadata_error is not None:
            raise self.metadata_error
        return FakeMetadata(
            {
                name: FakeTopic(
                    error="UNKNOWN_TOPIC_OR_PART" if name in self.missing_topics else None
                )
                for name in RELEASED
            }
        )


def _publisher(
    *,
    allow_invalid_events: bool = False,
    block_timeout_s: float | None = None,
    **fake: Any,
) -> tuple[EventPublisher, FakeProducer]:
    ledger = DeliveryLedger()
    config = producer_config(bootstrap_servers=LOOPBACK, client_id="unit", ledger=ledger)
    producer = FakeProducer(config, **fake)
    publisher = EventPublisher(
        producer,
        ledger,
        bootstrap_servers=LOOPBACK,
        allow_invalid_events=allow_invalid_events,
        block_timeout_s=block_timeout_s,
    )
    return publisher, producer


# ------------------------------------------------------------------ contract --


def test_every_producer_carries_the_approved_contract() -> None:
    config = producer_config(bootstrap_servers=LOOPBACK, client_id="unit", ledger=DeliveryLedger())
    for key, value in APPROVED_CONTRACT.items():
        assert config[key] == value, f"{key}={config.get(key)!r}, approved {value!r}"
    assert dict(PRODUCER_CONTRACT) == APPROVED_CONTRACT
    assert config["message.timeout.ms"] == DEFAULT_MESSAGE_TIMEOUT_MS == 120_000


def test_delivery_callbacks_are_bound_into_the_configuration() -> None:
    ledger = DeliveryLedger()
    config = producer_config(bootstrap_servers=LOOPBACK, client_id="unit", ledger=ledger)
    assert config["on_delivery"] == ledger.on_delivery
    assert config["error_cb"] == ledger.on_error


def test_a_message_timeout_must_be_finite() -> None:
    with pytest.raises(ValueError, match="retry forever"):
        producer_config(
            bootstrap_servers=LOOPBACK,
            client_id="unit",
            ledger=DeliveryLedger(),
            message_timeout_ms=0,
        )


# ---------------------------------------------------------------------- keys --


def test_keys_come_from_the_ledger_declared_field() -> None:
    publisher, producer = _publisher()
    publisher.publish("tx.raw.v1", _bytes(_tx()))
    publisher.publish("device.events.v1", _bytes(_device()))
    assert [call["key"] for call in producer.produced] == [b"acct_000000123", b"dev_000000456"]


def test_a_key_event_that_disagrees_with_the_value_is_refused() -> None:
    publisher, producer = _publisher()
    with pytest.raises(ContractError, match="key_event names key"):
        publisher.publish(
            "tx.raw.v1", _bytes(_tx("acct_000000001")), key_event=_tx("acct_000000999")
        )
    assert producer.produced == []
    assert publisher.report().refused == {"tx.raw.v1": 1}
    # Control: an agreeing key_event is accepted.
    publisher.publish("tx.raw.v1", _bytes(_tx("acct_000000001")), key_event=_tx("acct_000000001"))
    assert producer.produced[0]["key"] == b"acct_000000001"


def test_an_undecodable_value_needs_a_key_event_and_the_invalid_opt_in() -> None:
    truncated = _bytes(_tx())[:40]
    publisher, producer = _publisher()
    with pytest.raises(ContractError, match="not a JSON object"):
        publisher.publish("tx.raw.v1", truncated)
    with pytest.raises(SchemaValidationError):
        publisher.publish("tx.raw.v1", truncated, key_event=_tx())
    assert producer.produced == []

    faulty, faulty_producer = _publisher(allow_invalid_events=True)
    faulty.publish("tx.raw.v1", truncated, key_event=_tx())
    assert faulty_producer.produced[0]["key"] == b"acct_000000123"


def test_an_unreleased_topic_is_refused() -> None:
    publisher, producer = _publisher()
    with pytest.raises(UnreleasedTopicError):
        publisher.publish("tx.scored.v1", _bytes(_tx()))
    assert producer.produced == []


# ---------------------------------------------------------------- validation --


def test_a_schema_invalid_event_is_never_published_by_default() -> None:
    invalid = _tx()
    invalid["payload"]["channel"] = "NOT_A_CHANNEL"
    publisher, producer = _publisher()
    with pytest.raises(SchemaValidationError, match=r"§6\.1"):
        publisher.publish("tx.raw.v1", _bytes(invalid))
    assert producer.produced == []
    assert publisher.report().refused == {"tx.raw.v1": 1}


def test_publishing_invalid_events_is_an_opt_in_for_loopback_brokers_only() -> None:
    ledger = DeliveryLedger()
    config = producer_config(bootstrap_servers=LOOPBACK, client_id="unit", ledger=ledger)
    for bootstrap in ("kafka:19092", "10.1.2.3:9092", "127.0.0.1:9092,broker.example:9092"):
        with pytest.raises(EventPublishError, match="loopback"):
            EventPublisher(
                FakeProducer(config), ledger, bootstrap_servers=bootstrap, allow_invalid_events=True
            )
        with pytest.raises(EventPublishError, match="loopback"):
            EventPublisher.connect(bootstrap, client_id="unit", allow_invalid_events=True)
    # Control: without the opt-in any bootstrap is accepted.
    EventPublisher(FakeProducer(config), ledger, bootstrap_servers="kafka:19092")


@pytest.mark.parametrize(
    ("bootstrap", "loopback"),
    [
        ("localhost:9092", True),
        ("127.0.0.1:9092", True),
        ("127.0.0.1:1,127.0.0.2:2", True),
        ("[::1]:9092", True),
        ("::1", True),
        ("kafka:19092", False),
        ("10.0.0.1:9092", False),
        ("127.0.0.1:9092,kafka:9092", False),
        ("", False),
    ],
)
def test_loopback_detection(bootstrap: str, loopback: bool) -> None:
    assert is_loopback_bootstrap(bootstrap) is loopback


# -------------------------------------------------------------- trace header --


def test_the_trace_id_header_is_copied_from_the_envelope() -> None:
    publisher, producer = _publisher()
    publisher.publish("tx.raw.v1", _bytes(_tx(trace_id="0f" * 16)), headers=[("session", b"s-1")])
    assert producer.produced[0]["headers"] == [("session", b"s-1"), (TRACE_ID_HEADER, b"0f" * 16)]


def test_a_caller_cannot_supply_a_trace_id_header() -> None:
    publisher, producer = _publisher()
    with pytest.raises(ContractError, match=r"copied from envelope\.trace_id"):
        publisher.publish("tx.raw.v1", _bytes(_tx()), headers=[(TRACE_ID_HEADER, b"ee" * 16)])
    assert producer.produced == []


# -------------------------------------------------------------- topic check --


def test_a_missing_topic_fails_at_the_first_publish() -> None:
    publisher, producer = _publisher(missing_topics=("tx.raw.v1",))
    with pytest.raises(EventPublishError, match=r"kafka_topics\.py apply"):
        publisher.publish("tx.raw.v1", _bytes(_tx()))
    assert producer.produced == []
    assert publisher.report().refused == {"tx.raw.v1": 1}


def test_topic_metadata_is_checked_once_per_topic() -> None:
    publisher, producer = _publisher()
    for _ in range(3):
        publisher.publish("tx.raw.v1", _bytes(_tx()))
    publisher.publish("device.events.v1", _bytes(_device()))
    assert producer.metadata_calls == 2


def test_unreadable_metadata_is_a_publish_error() -> None:
    publisher, _ = _publisher(metadata_error=RuntimeError("all brokers down"))
    with pytest.raises(EventPublishError, match="could not read broker metadata"):
        publisher.publish("tx.raw.v1", _bytes(_tx()))


# ------------------------------------------------------------------ verdicts --


def test_close_confirms_when_every_report_succeeds() -> None:
    publisher, _ = _publisher()
    for _ in range(3):
        publisher.publish("tx.raw.v1", _bytes(_tx()))
    report = publisher.close()
    assert report.confirmed
    assert report.accepted == report.delivered == {"tx.raw.v1": 3}
    assert report.outstanding == 0


def test_close_raises_when_a_delivery_fails() -> None:
    publisher, _ = _publisher(outcome="fail")
    for _ in range(3):
        publisher.publish("tx.raw.v1", _bytes(_tx()))
    with pytest.raises(EventPublishError) as caught:
        publisher.close()
    assert caught.value.failed == {"tx.raw.v1": 3}
    assert caught.value.outstanding == 0


def test_close_raises_when_messages_are_still_queued() -> None:
    publisher, _ = _publisher(outcome="hang")
    publisher.publish("tx.raw.v1", _bytes(_tx()))
    publisher.publish("tx.raw.v1", _bytes(_tx()))
    with pytest.raises(EventPublishError) as caught:
        publisher.close(timeout_s=0.01)
    assert caught.value.outstanding == 2
    assert caught.value.failed == {}


def test_close_raises_when_reports_do_not_account_for_every_accepted_message() -> None:
    """An empty queue with missing reports is not delivery."""
    publisher, _ = _publisher(outcome="lose")
    publisher.publish("tx.raw.v1", _bytes(_tx()))
    with pytest.raises(EventPublishError, match="do not account for what was accepted"):
        publisher.close()


def test_close_can_be_repeated_for_a_fresh_verdict() -> None:
    publisher, producer = _publisher(outcome="hang")
    publisher.publish("tx.raw.v1", _bytes(_tx()))
    with pytest.raises(EventPublishError):
        publisher.close(timeout_s=0.01)
    producer.outcome = "deliver"
    assert publisher.close().confirmed


def test_a_fatal_error_fails_close_and_refuses_further_publishing() -> None:
    publisher, producer = _publisher()
    publisher.publish("tx.raw.v1", _bytes(_tx()))
    producer.error_cb(FakeError("_FATAL", fatal=True))
    with pytest.raises(EventPublishError, match="fatal"):
        publisher.publish("tx.raw.v1", _bytes(_tx()))
    with pytest.raises(EventPublishError) as caught:
        publisher.close()
    assert caught.value.fatal is not None and "_FATAL" in caught.value.fatal


def test_a_transient_client_error_alone_does_not_fail_close() -> None:
    """Control for the fatal case: librdkafka retries through transport errors."""
    publisher, producer = _publisher()
    publisher.publish("tx.raw.v1", _bytes(_tx()))
    producer.error_cb(FakeError("_TRANSPORT", fatal=False))
    assert publisher.close().confirmed


def test_shedding_is_counted_and_fails_close() -> None:
    publisher, producer = _publisher(always_full=True)
    assert publisher.publish("tx.raw.v1", _bytes(_tx()), block=False) is False
    assert producer.produced == []
    with pytest.raises(EventPublishError) as caught:
        publisher.close()
    assert caught.value.shed == {"tx.raw.v1": 1}


def test_a_full_queue_blocks_until_it_drains() -> None:
    publisher, producer = _publisher(full_for=2)
    assert publisher.publish("tx.raw.v1", _bytes(_tx())) is True
    assert len(producer.produced) == 1
    assert producer.polls >= 2, "blocking must poll, or delivery reports never drain the queue"
    assert publisher.close().confirmed


def test_a_queue_that_never_drains_ends_in_an_error_not_a_hang() -> None:
    publisher, _ = _publisher(always_full=True, block_timeout_s=0.1)
    with pytest.raises(EventPublishError, match="stayed full"):
        publisher.publish("tx.raw.v1", _bytes(_tx()))


# ------------------------------------------------------- closed and threaded --


def test_publishing_after_close_is_refused_and_recorded() -> None:
    publisher, producer = _publisher()
    publisher.publish("tx.raw.v1", _bytes(_tx()))
    assert publisher.close().confirmed
    with pytest.raises(EventPublishError, match="closed"):
        publisher.publish("tx.raw.v1", _bytes(_tx()))
    assert len(producer.produced) == 1
    with pytest.raises(EventPublishError) as caught:
        publisher.close()
    assert caught.value.refused == {"tx.raw.v1": 1}, "a publish after close must not vanish"


def test_concurrent_publishers_are_fully_accounted() -> None:
    publisher, _ = _publisher()
    payload = _bytes(_tx())

    def work() -> None:
        for _ in range(100):
            publisher.publish("tx.raw.v1", payload)

    threads = [threading.Thread(target=work) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    report = publisher.close()
    assert report.accepted == report.delivered == {"tx.raw.v1": 800}


def test_close_cannot_interleave_with_a_concurrent_publish() -> None:
    """Every attempt either lands before the close or is refused -- none is unaccounted."""
    publisher, _ = _publisher()
    payload = _bytes(_tx())
    attempts = 0
    stop = threading.Event()

    def work() -> None:
        nonlocal attempts
        while not stop.is_set():
            attempts += 1
            try:
                publisher.publish("tx.raw.v1", payload)
            except EventPublishError:
                stop.set()

    thread = threading.Thread(target=work)
    thread.start()
    while publisher.report().accepted.get("tx.raw.v1", 0) < 50:
        pass
    # A refused attempt after close makes the verdict unconfirmed, as it must.
    with contextlib.suppress(EventPublishError):
        publisher.close()
    stop.set()
    thread.join()
    report = publisher.ledger.report(outstanding=0)
    accepted = report.accepted.get("tx.raw.v1", 0)
    assert accepted == report.delivered.get("tx.raw.v1", 0)
    assert attempts == accepted + report.refused.get("tx.raw.v1", 0)


# ------------------------------------------------------------------- metrics --


def test_every_outcome_is_emitted_as_a_metric() -> None:
    """Observability is part of the contract: each verdict above is also counted."""
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint

    reader = InMemoryMetricReader()
    ledger = DeliveryLedger(meter=MeterProvider(metric_readers=[reader]).get_meter("unit"))
    config = producer_config(bootstrap_servers=LOOPBACK, client_id="unit", ledger=ledger)

    def publisher(**fake: Any) -> EventPublisher:
        return EventPublisher(FakeProducer(config, **fake), ledger, bootstrap_servers=LOOPBACK)

    delivering = publisher()
    delivering.publish("tx.raw.v1", _bytes(_tx()))
    delivering.publish("tx.raw.v1", _bytes(_tx()))
    delivering.producer.flush(0)
    failing = publisher(outcome="fail")
    failing.publish("device.events.v1", _bytes(_device()))
    failing.producer.flush(0)
    publisher(always_full=True).publish("tx.raw.v1", _bytes(_tx()), block=False)
    with pytest.raises(ContractError):
        delivering.publish("tx.raw.v1", b"not json")
    config["error_cb"](FakeError("_TRANSPORT"))

    points: dict[tuple[str, tuple[tuple[str, Any], ...]], Any] = {}
    data = reader.get_metrics_data()
    assert data is not None
    for resource in data.resource_metrics:
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                for point in metric.data.data_points:
                    assert isinstance(point, NumberDataPoint), "publisher metrics are counters"
                    attributes = tuple(sorted((point.attributes or {}).items()))
                    points[(metric.name, attributes)] = point.value

    def outcome(topic: str, name: str) -> Any:
        return points.get((PUBLISH_OUTCOMES_TOTAL, (("outcome", name), ("topic", topic))))

    assert outcome("tx.raw.v1", "delivered") == 2
    assert outcome("device.events.v1", "failed") == 1
    assert outcome("tx.raw.v1", "shed") == 1
    assert outcome("tx.raw.v1", "refused") == 1
    assert outcome("device.events.v1", "delivered") is None, "a failure was counted as delivered"
    assert points[(PUBLISHER_ERRORS_TOTAL, (("error", "_TRANSPORT"), ("fatal", False)))] == 1


# --------------------------------------------------------------- Kafka sink --


def _sink(**fake: Any) -> tuple[KafkaSink, FakeProducer]:
    publisher, producer = _publisher(**fake)
    sink = KafkaSink.__new__(KafkaSink)
    object.__setattr__(sink, "bootstrap_servers", LOOPBACK)
    object.__setattr__(sink, "_publisher", publisher)
    object.__setattr__(sink, "report", None)
    return sink, producer


def test_the_generator_sink_confirms_every_row_at_close() -> None:
    sink, _ = _sink()
    for account in ("acct_000000001", "acct_000000002"):
        sink.write("tx.raw.v1", _bytes(_tx(account)))
    sink.close()
    assert sink.report is not None and sink.report.delivered == {"tx.raw.v1": 2}


def test_the_generator_sink_raises_when_a_row_was_not_delivered() -> None:
    """The defect: the old sink flushed for 30 s and let the rest vanish silently."""
    sink, _ = _sink(outcome="fail")
    sink.write("tx.raw.v1", _bytes(_tx()))
    with pytest.raises(EventPublishError):
        sink.close()


def test_the_generator_sink_raises_when_rows_are_still_queued() -> None:
    sink, _ = _sink(outcome="hang")
    sink.write("tx.raw.v1", _bytes(_tx()))
    with pytest.raises(EventPublishError) as caught:
        sink.close()
    assert caught.value.outstanding == 1


def test_a_refused_sink_write_leaves_the_close_unconfirmed() -> None:
    """Whatever validation policy the run chose, an invalid row is refused and remembered."""
    sink, producer = _sink()
    invalid = _tx()
    invalid["payload"]["currency"] = "pounds"
    with pytest.raises(SchemaValidationError):
        sink.write("tx.raw.v1", _bytes(invalid))
    sink.write("tx.raw.v1", _bytes(_tx()))
    assert len(producer.produced) == 1
    with pytest.raises(EventPublishError) as caught:
        sink.close()
    assert caught.value.refused == {"tx.raw.v1": 1}


def test_a_sink_that_never_connected_cannot_confirm_anything() -> None:
    sink = KafkaSink.__new__(KafkaSink)
    with pytest.raises(EventPublishError, match="without ever connecting"):
        sink.close()
