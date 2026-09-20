"""The Kafka sink must publish keyed, or not at all.

A real Phase 1 defect: `KafkaSink.write` called
`producer.produce(topic, value=payload)` with no `key=`. Kafka then round-robins
the partitions, which silently voids the ordering guarantee
`docs/contracts/RELEASED.json` states for every released topic -- *"per-account
velocity is order-sensitive"*. No consumer existed yet, so nothing failed; Phase
3 would have met it as unattributable velocity drift.

Unit-layer, so the producer is a stub behind the real `EventPublisher`: the
invariant under test is "the sink passes a key derived from the ledger", not
Kafka's own behaviour. Since Phase 3 Step 2 the sink publishes through the one
producer factory, which also validates every value against its released schema,
so the events here are real released events. The broker-level assertion -- that
the partition a key lands on is the Java client's -- is in
`tests/integration/test_kafka_platform.py`.
"""

from __future__ import annotations

import datetime as dt
import json
import random
from typing import Any

import pytest
from data.generator.emit import KafkaSink

from trace_core.contracts.envelope import build_event
from trace_core.contracts.publish import DeliveryLedger, EventPublisher
from trace_core.domain.errors import ContractError, UnreleasedTopicError
from trace_core.domain.identifiers import uuid7
from trace_core.domain.time import event_time, processing_time, to_millis

pytestmark = pytest.mark.unit

_RNG = random.Random(1)
_AT = dt.datetime(2026, 9, 1, 12, 0, tzinfo=dt.UTC)


class _RecordingProducer:
    """Captures produce() calls. Mirrors the confluent_kafka.Producer calls the factory makes."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def produce(
        self,
        topic: str,
        *,
        value: bytes | None = None,
        key: bytes | None = None,
        headers: Any = None,
    ) -> None:
        self.calls.append({"topic": topic, "key": key, "value": value})

    def poll(self, timeout: float = -1) -> int:
        del timeout
        return 0

    def flush(self, timeout: float = -1) -> int:
        del timeout
        return 0

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> Any:
        del timeout

        class _Topic:
            def __init__(self) -> None:
                self.error = None
                self.partitions = {0: None}

        class _Metadata:
            def __init__(self, name: str | None) -> None:
                self.topics = {name: _Topic()}

        return _Metadata(topic)


def _sink() -> tuple[KafkaSink, _RecordingProducer]:
    """Build the sink around a stub producer, without connecting to a broker."""
    producer = _RecordingProducer()
    sink = KafkaSink.__new__(KafkaSink)
    object.__setattr__(sink, "bootstrap_servers", "localhost:9092")
    object.__setattr__(
        sink,
        "_publisher",
        EventPublisher(producer, DeliveryLedger(), bootstrap_servers="localhost:9092"),
    )
    return sink, producer


def _payload(event_type: str, payload: dict[str, Any]) -> bytes:
    return json.dumps(
        build_event(
            event_type=event_type,
            occurred_at=event_time(_AT),
            payload=payload,
            producer="trace-test@1.0.0",
            trace_id="0" * 32,
            correlation_id="corr_keying",
            ingested_at=processing_time(_AT),
            event_id=uuid7(millis=to_millis(_AT), rng=_RNG),
        )
    ).encode()


def _transaction(account_id: str = "acct_000123") -> bytes:
    return _payload(
        "tx.raw",
        {
            "transaction_id": "tx_000000000001",
            "account_id": account_id,
            "card_id": "card_000123",
            "device_id": "dev_000456",
            "merchant_id": "mrch_00001",
            "ip_id": "ip_00001",
            "amount_minor": 100,
            "currency": "GBP",
            "channel": "CARD_PRESENT",
            "entry_mode": "CHIP",
            "merchant_mcc": "5411",
            "merchant_country": "GB",
            "latitude": 51.5,
            "longitude": -0.12,
            "authorization_outcome": "APPROVED",
        },
    )


def _identity() -> bytes:
    return _payload(
        "identity.event", {"account_id": "acct_000123", "identity_event_type": "EMAIL_CHANGE"}
    )


def _device() -> bytes:
    return _payload(
        "device.event",
        {
            "device_id": "dev_000456",
            "account_id": "acct_000123",
            "device_event_type": "FIRST_SEEN",
            "platform": "android",
        },
    )


def test_transactions_are_keyed_by_account_id() -> None:
    sink, producer = _sink()
    sink.write("tx.raw.v1", _transaction())
    assert len(producer.calls) == 1
    assert producer.calls[0]["key"] == b"acct_000123", (
        "tx.raw.v1 must be keyed by account_id: per-account velocity is order-sensitive"
    )


def test_device_events_are_keyed_by_device_id() -> None:
    sink, producer = _sink()
    sink.write("device.events.v1", _device())
    assert producer.calls[0]["key"] == b"dev_000456", (
        "device.events.v1 keys on the device, not the account: device-sharing "
        "detection is what is order-sensitive here"
    )


def test_no_event_is_ever_published_without_a_key() -> None:
    """The regression itself."""
    sink, producer = _sink()
    sink.write("tx.raw.v1", _transaction())
    sink.write("identity.events.v1", _identity())
    sink.write("device.events.v1", _device())
    assert len(producer.calls) == 3, "nothing was produced; the test would pass vacuously"
    for call in producer.calls:
        assert call["key"], f"{call['topic']} was produced with key={call['key']!r}"


def test_a_missing_key_field_refuses_to_publish() -> None:
    """Refusing is correct: an unkeyed publish is a silently reordered topic."""
    sink, producer = _sink()
    event = json.loads(_transaction())
    del event["payload"]["account_id"]
    with pytest.raises(ContractError):
        sink.write("tx.raw.v1", json.dumps(event).encode())
    assert producer.calls == [], "nothing may be published when the key cannot be resolved"


def test_an_unreleased_topic_refuses_to_publish() -> None:
    sink, producer = _sink()
    with pytest.raises(UnreleasedTopicError):
        sink.write("audit.v1", _transaction())
    assert producer.calls == []


def test_the_value_is_the_unmodified_encoded_row() -> None:
    """Keying must not disturb the bytes the digest was computed over."""
    sink, producer = _sink()
    payload = _transaction()
    sink.write("tx.raw.v1", payload)
    assert producer.calls[0]["value"] == payload
