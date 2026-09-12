"""The Kafka sink must publish keyed, or not at all.

A real Phase 1 defect: `KafkaSink.write` called
`producer.produce(topic, value=payload)` with no `key=`. Kafka then round-robins
the partitions, which silently voids the ordering guarantee
`docs/contracts/RELEASED.json` states for every released topic -- *"per-account
velocity is order-sensitive"*. No consumer existed yet, so nothing failed; Phase
3 would have met it as unattributable velocity drift.

Unit-layer, so the producer is a stub: the invariant under test is "the sink
passes a key derived from the ledger", not Kafka's own behaviour. The broker-level
assertion lands in Phase 3 with the `stream` extra and a real container.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from data.generator.emit import KafkaSink

from trace_core.domain.errors import ContractError, UnreleasedTopicError

pytestmark = pytest.mark.unit


class _RecordingProducer:
    """Captures produce() calls. Mirrors confluent_kafka.Producer's signature."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def produce(self, topic: str, *, key: bytes | None = None, value: bytes) -> None:
        self.calls.append({"topic": topic, "key": key, "value": value})

    def poll(self, timeout: float) -> int:
        del timeout
        return 0

    def flush(self, timeout: float) -> int:
        del timeout
        return 0


def _sink() -> tuple[KafkaSink, _RecordingProducer]:
    """Build the sink without __post_init__, which would import confluent_kafka.

    The `stream` extra is not installed until Phase 3, and requiring it here
    would make this test skip on every machine -- which is how the original
    defect survived.
    """
    producer = _RecordingProducer()
    sink = KafkaSink.__new__(KafkaSink)
    object.__setattr__(sink, "bootstrap_servers", "localhost:9092")
    object.__setattr__(sink, "_producer", producer)
    return sink, producer


def _event(account_id: str = "acct_000123", device_id: str = "dev_000456") -> bytes:
    return json.dumps(
        {
            "envelope": {"event_type": "tx.raw"},
            "payload": {"account_id": account_id, "device_id": device_id},
        }
    ).encode()


def test_transactions_are_keyed_by_account_id() -> None:
    sink, producer = _sink()
    sink.write("tx.raw.v1", _event())
    assert len(producer.calls) == 1
    assert producer.calls[0]["key"] == b"acct_000123", (
        "tx.raw.v1 must be keyed by account_id: per-account velocity is order-sensitive"
    )


def test_device_events_are_keyed_by_device_id() -> None:
    sink, producer = _sink()
    sink.write("device.events.v1", _event())
    assert producer.calls[0]["key"] == b"dev_000456", (
        "device.events.v1 keys on the device, not the account: device-sharing "
        "detection is what is order-sensitive here"
    )


def test_no_event_is_ever_published_without_a_key() -> None:
    """The regression itself."""
    sink, producer = _sink()
    for topic in ("tx.raw.v1", "identity.events.v1", "device.events.v1"):
        sink.write(topic, _event())
    assert producer.calls, "nothing was produced; the test would pass vacuously"
    for call in producer.calls:
        assert call["key"], f"{call['topic']} was produced with key={call['key']!r}"


def test_a_missing_key_field_refuses_to_publish() -> None:
    """Refusing is correct: an unkeyed publish is a silently reordered topic."""
    sink, producer = _sink()
    payload = json.dumps({"envelope": {}, "payload": {"device_id": "dev_000456"}}).encode()
    with pytest.raises(ContractError):
        sink.write("tx.raw.v1", payload)
    assert producer.calls == [], "nothing may be published when the key cannot be resolved"


def test_an_unreleased_topic_refuses_to_publish() -> None:
    sink, producer = _sink()
    with pytest.raises(UnreleasedTopicError):
        sink.write("tx.scored.v1", _event())
    assert producer.calls == []


def test_the_value_is_the_unmodified_encoded_row() -> None:
    """Keying must not disturb the bytes the digest was computed over."""
    sink, producer = _sink()
    payload = _event()
    sink.write("tx.raw.v1", payload)
    assert producer.calls[0]["value"] == payload
