"""The one Kafka producer factory: keyed, validated, idempotent, Java-partitioned, and loud.

Every process that publishes an event builds its producer here -- the generator's
Kafka sink today, the gateway and the outbox relay from Phase 3 Step 4 -- because
each property below prevents a failure that is silent where it happens and
expensive where it is found (docs/PHASE3_PLAN.md §2 B6, §3 Q3):

* **`enable.idempotence=true`**, which implies `acks=all` (stated explicitly
  anyway). A retried send cannot duplicate or reorder a message inside a
  partition, so the per-key ordering the release ledger promises survives
  transient network faults.
* **`partitioner=murmur2_random`.** librdkafka's default, `consistent_random`,
  hashes keys with CRC32; the Java client -- and therefore Spark's Kafka sink --
  hashes with murmur2. The same key would land on different partitions depending
  on which client wrote it. `murmur2` below is a port of Kafka's own
  `Utils.murmur2`, pinned to Kafka's test vectors, so placement is predicted and
  asserted rather than assumed.
* **`compression.type=zstd`**, with a bounded buffer
  (`queue.buffering.max.kbytes=65536`) so a slow broker applies back-pressure.
* **`message.timeout.ms=120000`**: how long a message may wait before it is
  reported FAILED. Finite on purpose -- a producer that retries forever turns a
  broker outage into a hang with no verdict.

**Every published value is a valid released event** (docs/EVENT_CONTRACTS.md
§6.1), validated here against its generated contract model, whatever the caller
already checked. The single exception is `allow_invalid_events=True`, which
exists for the fault overlay (`eval/replay/faults.py`) and is refused unless every
bootstrap server is a loopback address -- a deliberately invalid record can reach
a throwaway local broker and nothing else.

**Keys and trace ids are never arguments.** The key is read from the event by
`trace_core.contracts.topics.partition_key`, which is diffed against the release
ledger; when a `key_event` is supplied for a value that also decodes, both must
name the same key. The `trace_id` header is copied from `envelope.trace_id`
(§6.6: header and body), and a caller cannot supply a different one.

**A topic that does not exist fails at the first publish**, from a metadata
check, instead of after the full message timeout: automatic topic creation is
disabled on every TRACE-X broker, so a missing topic means `kafka_topics.py apply`
was not run.

**Nothing is lost quietly.** A `DeliveryLedger` bound into the producer's
configuration counts, per topic, every message the producer ACCEPTED and every
delivery report. `EventPublisher.close` refuses further publishes, flushes, and
raises `EventPublishError` unless nothing is queued, nothing failed, nothing was
shed or refused, no fatal error occurred, and delivered + failed equals accepted
for every topic -- so the report vouches for exactly what was handed over.
"""

from __future__ import annotations

import functools
import ipaddress
import json
import threading
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final, Protocol

from pydantic import ValidationError

from trace_core.contracts.topics import (
    DEVICE_EVENTS_V1,
    IDENTITY_EVENTS_V1,
    INVESTIGATION_REQUESTED_V1,
    TX_AUTHORIZATION_V1,
    TX_RAW_V1,
    TX_SCORED_V1,
    partition_key,
)
from trace_core.domain.errors import (
    ContractError,
    EventPublishError,
    MissingDependencyError,
    SchemaValidationError,
)
from trace_core.observability.logging import get_logger
from trace_core.observability.telemetry import get_meter

if TYPE_CHECKING:
    from confluent_kafka import KafkaError, Message
    from opentelemetry.metrics import Counter as MetricCounter
    from opentelemetry.metrics import Meter

PRODUCER_CONTRACT: Final[Mapping[str, str | int | bool]] = MappingProxyType(
    {
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "zstd",
        "partitioner": "murmur2_random",
        "queue.buffering.max.kbytes": 65_536,
    }
)
"""Settings every TRACE-X producer carries. None of them is a parameter."""

DEFAULT_MESSAGE_TIMEOUT_MS: Final = 120_000
FLUSH_MARGIN_S: Final = 5.0
"""Added to the message timeout when flushing, so a flush outlasts every message's
own deadline and ends with a verdict for each one rather than "still queued"."""

TOPIC_CHECK_TIMEOUT_S: Final = 10.0
BLOCKED_POLL_S: Final = 0.05
MAX_ERROR_SAMPLES: Final = 16

TRACE_ID_HEADER: Final = "trace_id"
"""Kafka header carrying `envelope.trace_id` (EVENT_CONTRACTS §6.6)."""

PUBLISH_OUTCOMES_TOTAL: Final = "event_publish_outcomes_total"
"""Counter, attributes `topic` and `outcome` (delivered | failed | shed | refused)."""
PUBLISHER_ERRORS_TOTAL: Final = "event_publisher_errors_total"
"""Counter of client-level errors (`error_cb`), attributes `error` and `fatal`."""

OUTCOME_DELIVERED: Final = "delivered"
OUTCOME_FAILED: Final = "failed"
OUTCOME_SHED: Final = "shed"
OUTCOME_REFUSED: Final = "refused"

Headers = Sequence[tuple[str, bytes]]

_log = get_logger(__name__)


# ------------------------------------------------------------------ murmur2 --

_MURMUR_SEED: Final = 0x9747B28C
_MURMUR_M: Final = 0x5BD1E995
_MASK32: Final = 0xFFFFFFFF


def murmur2(data: bytes) -> int:
    """Kafka's `org.apache.kafka.common.utils.Utils.murmur2`, as a signed 32-bit int.

    A line-for-line port. Every value is kept as an unsigned 32-bit Python int, so
    `>>` behaves as Java's `>>>`, and the result is reinterpreted as signed at the
    end to match the Java return value the test vectors are written in.
    """
    length = len(data)
    h = (_MURMUR_SEED ^ length) & _MASK32
    whole = length & ~3
    for i in range(0, whole, 4):
        k = data[i] | (data[i + 1] << 8) | (data[i + 2] << 16) | (data[i + 3] << 24)
        k = (k * _MURMUR_M) & _MASK32
        k ^= k >> 24
        k = (k * _MURMUR_M) & _MASK32
        h = (h * _MURMUR_M) & _MASK32
        h ^= k
    tail = length - whole
    if tail == 3:
        h ^= data[whole + 2] << 16
    if tail >= 2:
        h ^= data[whole + 1] << 8
    if tail >= 1:
        h ^= data[whole]
        h = (h * _MURMUR_M) & _MASK32
    h ^= h >> 13
    h = (h * _MURMUR_M) & _MASK32
    h ^= h >> 15
    return h - (1 << 32) if h & 0x80000000 else h


def java_partition(key: bytes, num_partitions: int) -> int:
    """The partition the Java client assigns a keyed record: `toPositive(murmur2) % n`.

    `Utils.toPositive` masks the sign bit rather than taking an absolute value, so
    `Integer.MIN_VALUE` maps to 0 instead of overflowing.
    """
    if num_partitions <= 0:
        raise ValueError(f"num_partitions must be positive, got {num_partitions}")
    return (murmur2(key) & 0x7FFFFFFF) % num_partitions


def _bootstrap_host(server: str) -> str:
    """The host of one `host:port` entry: `[::1]:9092`, `127.0.0.1:9092` or a bare host."""
    if server.startswith("["):
        return server[1 : server.index("]")]
    if server.count(":") == 1:
        return server.split(":", 1)[0]
    return server


def is_loopback_bootstrap(bootstrap_servers: str) -> bool:
    """True only if every entry names `localhost` or a loopback IP address."""
    hosts = [_bootstrap_host(s.strip()) for s in bootstrap_servers.split(",") if s.strip()]
    if not hosts:
        return False
    for host in hosts:
        if host == "localhost":
            continue
        try:
            if not ipaddress.ip_address(host).is_loopback:
                return False
        except ValueError:
            return False
    return True


# ------------------------------------------------------------------ ledger --


class ProducerLike(Protocol):
    """The part of `confluent_kafka.Producer` this module uses."""

    def produce(
        self,
        topic: str,
        *,
        value: bytes | None = None,
        key: bytes | None = None,
        headers: Any = None,
    ) -> None: ...

    def poll(self, timeout: float = -1) -> int: ...

    def flush(self, timeout: float = -1) -> int: ...

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> Any: ...


def _create_instruments(meter: Meter) -> tuple[MetricCounter, MetricCounter]:
    outcomes = meter.create_counter(
        PUBLISH_OUTCOMES_TOTAL,
        description="Events handed to Kafka, by topic and final outcome.",
    )
    errors = meter.create_counter(
        PUBLISHER_ERRORS_TOTAL,
        description="Client-level producer errors, by librdkafka error name.",
    )
    return outcomes, errors


@functools.cache
def _instruments() -> tuple[MetricCounter, MetricCounter]:
    """Created once per process: OpenTelemetry identifies instruments by name."""
    return _create_instruments(get_meter("trace_core.publish"))


@dataclass(frozen=True, slots=True)
class DeliveryReport:
    """What a producer can vouch for at one moment."""

    accepted: Mapping[str, int]
    delivered: Mapping[str, int]
    failed: Mapping[str, int]
    shed: Mapping[str, int]
    refused: Mapping[str, int]
    outstanding: int
    fatal: str | None
    error_samples: tuple[str, ...]

    def problems(self) -> list[str]:
        found: list[str] = []
        if self.outstanding:
            found.append(f"{self.outstanding} message(s) still queued after the flush")
        if sum(self.failed.values()):
            found.append(f"delivery failed for {dict(self.failed)}")
        if sum(self.shed.values()):
            found.append(f"shed with the producer queue full: {dict(self.shed)}")
        if sum(self.refused.values()):
            found.append(f"refused before reaching the producer: {dict(self.refused)}")
        if self.fatal:
            found.append(f"the idempotent producer reported a fatal error: {self.fatal}")
        topics = set(self.accepted) | set(self.delivered) | set(self.failed)
        reported = {t: self.delivered.get(t, 0) + self.failed.get(t, 0) for t in topics}
        if self.outstanding == 0:
            unaccounted = {
                t: (self.accepted.get(t, 0), reported[t])
                for t in sorted(topics)
                if reported[t] != self.accepted.get(t, 0)
            }
            if unaccounted:
                found.append(
                    "delivery reports do not account for what was accepted "
                    f"(topic: (accepted, reported)): {unaccounted}"
                )
        elif sum(reported.values()) + self.outstanding != sum(self.accepted.values()):
            found.append(
                f"accepted {sum(self.accepted.values())} but reported "
                f"{sum(reported.values())} with {self.outstanding} outstanding"
            )
        return found

    @property
    def confirmed(self) -> bool:
        """Every event accepted was acknowledged by the broker, and nothing else happened."""
        return not self.problems()


class DeliveryLedger:
    """Accepted messages and delivery reports, counted per topic.

    Bound into the producer's configuration as `on_delivery` and `error_cb`, so
    every message produced through that producer is counted whichever code path
    produced it. librdkafka serves those callbacks from whichever thread calls
    `poll` or `flush`, so the counters are guarded by a lock.

    `meter` exists so a test can read the emitted metrics from its own reader; in a
    process the ledger uses the process-wide meter.
    """

    def __init__(self, *, meter: Meter | None = None) -> None:
        self._outcomes, self._errors = (
            _instruments() if meter is None else _create_instruments(meter)
        )
        self._lock = threading.Lock()
        self._accepted: Counter[str] = Counter()
        self._delivered: Counter[str] = Counter()
        self._failed: Counter[str] = Counter()
        self._shed: Counter[str] = Counter()
        self._refused: Counter[str] = Counter()
        self._samples: list[str] = []
        self._seen_errors: set[str] = set()
        self._fatal: str | None = None

    def _sample(self, text: str) -> None:
        if len(self._samples) < MAX_ERROR_SAMPLES:
            self._samples.append(text)

    def on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        topic = msg.topic() or "<unknown>"
        with self._lock:
            if err is None:
                self._delivered[topic] += 1
            else:
                self._failed[topic] += 1
                self._sample(f"{topic}: {err.name()}: {err.str()}")
        outcome = OUTCOME_DELIVERED if err is None else OUTCOME_FAILED
        self._outcomes.add(1, {"topic": topic, "outcome": outcome})

    def on_error(self, err: KafkaError) -> None:
        name = err.name()
        fatal = err.fatal()
        with self._lock:
            first_of_kind = name not in self._seen_errors
            self._seen_errors.add(name)
            self._sample(f"{name}: {err.str()}")
            first_fatal = fatal and self._fatal is None
            if first_fatal:
                self._fatal = f"{name}: {err.str()}"
        self._errors.add(1, {"error": name, "fatal": fatal})
        if first_fatal:
            _log.error("event_publisher_fatal_error", error=name, detail=err.str())
        elif first_of_kind:
            # Transport errors repeat on every reconnect attempt during an outage;
            # the counter carries the rate, the log carries the first occurrence.
            _log.warning("event_publisher_error", error=name, detail=err.str())

    def record_accepted(self, topic: str) -> None:
        with self._lock:
            self._accepted[topic] += 1

    def record_shed(self, topic: str) -> None:
        with self._lock:
            self._shed[topic] += 1
        self._outcomes.add(1, {"topic": topic, "outcome": OUTCOME_SHED})

    def record_refused(self, topic: str) -> None:
        with self._lock:
            self._refused[topic] += 1
        self._outcomes.add(1, {"topic": topic, "outcome": OUTCOME_REFUSED})

    @property
    def fatal(self) -> str | None:
        with self._lock:
            return self._fatal

    def report(self, *, outstanding: int) -> DeliveryReport:
        with self._lock:
            return DeliveryReport(
                accepted=MappingProxyType(dict(self._accepted)),
                delivered=MappingProxyType(dict(self._delivered)),
                failed=MappingProxyType(dict(self._failed)),
                shed=MappingProxyType(dict(self._shed)),
                refused=MappingProxyType(dict(self._refused)),
                outstanding=outstanding,
                fatal=self._fatal,
                error_samples=tuple(self._samples),
            )


# ----------------------------------------------------------------- factory --


def producer_config(
    *,
    bootstrap_servers: str,
    client_id: str,
    ledger: DeliveryLedger,
    message_timeout_ms: int = DEFAULT_MESSAGE_TIMEOUT_MS,
) -> dict[str, Any]:
    """The complete librdkafka configuration: the contract plus identity and callbacks."""
    if not bootstrap_servers:
        raise ValueError("bootstrap_servers is required")
    if not client_id:
        raise ValueError("client_id is required: it names the producer in broker logs and metrics")
    if message_timeout_ms <= 0:
        raise ValueError(
            f"message_timeout_ms must be positive, got {message_timeout_ms}; zero means "
            f"'retry forever', which turns a broker outage into a hang with no verdict"
        )
    return {
        **PRODUCER_CONTRACT,
        "bootstrap.servers": bootstrap_servers,
        "client.id": client_id,
        "message.timeout.ms": message_timeout_ms,
        "on_delivery": ledger.on_delivery,
        "error_cb": ledger.on_error,
    }


def build_producer(
    *,
    bootstrap_servers: str,
    client_id: str,
    ledger: DeliveryLedger,
    message_timeout_ms: int = DEFAULT_MESSAGE_TIMEOUT_MS,
) -> ProducerLike:
    """A `confluent_kafka.Producer` carrying the contract, its reports counted by `ledger`."""
    config = producer_config(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
        ledger=ledger,
        message_timeout_ms=message_timeout_ms,
    )
    try:
        from confluent_kafka import Producer
    except ModuleNotFoundError as exc:
        raise MissingDependencyError(
            "confluent_kafka", "stream", "Publishing events to Kafka"
        ) from exc
    producer: ProducerLike = Producer(config)
    return producer


@functools.cache
def _models() -> Mapping[str, Any]:
    from trace_core.contracts.events import (
        device_events_v1,
        identity_events_v1,
        investigation_requested_v1,
        tx_authorization_v1,
        tx_raw_v1,
        tx_scored_v1,
    )

    return MappingProxyType(
        {
            TX_RAW_V1: tx_raw_v1.TxRawV1,
            IDENTITY_EVENTS_V1: identity_events_v1.IdentityEventV1,
            DEVICE_EVENTS_V1: device_events_v1.DeviceEventV1,
            INVESTIGATION_REQUESTED_V1: investigation_requested_v1.InvestigationRequestedV1,
            TX_AUTHORIZATION_V1: tx_authorization_v1.TxAuthorizationV1,
            TX_SCORED_V1: tx_scored_v1.TxScoredV1,
        }
    )


def _decode(value: bytes) -> dict[str, Any] | None:
    try:
        decoded = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return decoded if isinstance(decoded, dict) else None


class EventPublisher:
    """A contract producer and its ledger. Thread-safe; closing is final for publishing.

    `publish` hands one record over; `close` flushes and either returns a confirmed
    `DeliveryReport` or raises. After the first `close`, `publish` is refused, but
    `close` may be called again for a fresh verdict: a message still queued when one
    close gave up can be delivered later, while a failed delivery stays failed.
    """

    def __init__(
        self,
        producer: ProducerLike,
        ledger: DeliveryLedger,
        *,
        bootstrap_servers: str,
        message_timeout_ms: int = DEFAULT_MESSAGE_TIMEOUT_MS,
        allow_invalid_events: bool = False,
        block_timeout_s: float | None = None,
        topic_check_timeout_s: float = TOPIC_CHECK_TIMEOUT_S,
    ) -> None:
        if allow_invalid_events and not is_loopback_bootstrap(bootstrap_servers):
            raise EventPublishError(
                f"allow_invalid_events is for fault injection into a throwaway local broker "
                f"only; refusing bootstrap {bootstrap_servers!r}, which is not loopback. "
                f"docs/EVENT_CONTRACTS.md §6.1: an invalid message is never published."
            )
        self.producer = producer
        self.ledger = ledger
        self.bootstrap_servers = bootstrap_servers
        self.message_timeout_ms = message_timeout_ms
        self.allow_invalid_events = allow_invalid_events
        self.block_timeout_s = (
            message_timeout_ms / 1000 + FLUSH_MARGIN_S
            if block_timeout_s is None
            else block_timeout_s
        )
        self.topic_check_timeout_s = topic_check_timeout_s
        self._gate = threading.Lock()
        self._closed = False
        self._known_topics: set[str] = set()

    @classmethod
    def connect(
        cls,
        bootstrap_servers: str,
        *,
        client_id: str,
        message_timeout_ms: int = DEFAULT_MESSAGE_TIMEOUT_MS,
        allow_invalid_events: bool = False,
    ) -> EventPublisher:
        if allow_invalid_events and not is_loopback_bootstrap(bootstrap_servers):
            raise EventPublishError(
                f"allow_invalid_events refused for non-loopback bootstrap {bootstrap_servers!r}"
            )
        ledger = DeliveryLedger()
        producer = build_producer(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            ledger=ledger,
            message_timeout_ms=message_timeout_ms,
        )
        return cls(
            producer,
            ledger,
            bootstrap_servers=bootstrap_servers,
            message_timeout_ms=message_timeout_ms,
            allow_invalid_events=allow_invalid_events,
        )

    @property
    def delivery_timeout_s(self) -> float:
        return self.message_timeout_ms / 1000

    @property
    def closed(self) -> bool:
        return self._closed

    def publish(
        self,
        topic: str,
        value: bytes,
        *,
        key_event: dict[str, Any] | None = None,
        headers: Headers | None = None,
        block: bool = True,
    ) -> bool:
        """Hand a record to the producer. False means it was shed (only when `block=False`).

        Held under the publisher's gate, so `close` cannot interleave: a publish either
        completes before a close begins or is refused as closed.
        """
        with self._gate:
            try:
                if self._closed:
                    raise EventPublishError(
                        f"the publisher is closed; refusing to publish to {topic}. A record "
                        f"handed over after close could never appear in a delivery verdict."
                    )
                fatal = self.ledger.fatal
                if fatal is not None:
                    raise EventPublishError(
                        f"the producer reported a fatal error and can deliver nothing more: "
                        f"{fatal}",
                        fatal=fatal,
                    )
                key, header_list = self._prepare(topic, value, key_event, headers)
                self._ensure_topic(topic)
                accepted = self._produce(topic, value, key, header_list, shed=not block)
            except Exception:
                # Counted, then re-raised: a record that never reached the producer
                # leaves the session unconfirmable, whatever the caller does next.
                self.ledger.record_refused(topic)
                raise
            if not accepted:
                self.ledger.record_shed(topic)
            return accepted

    def _prepare(
        self,
        topic: str,
        value: bytes,
        key_event: dict[str, Any] | None,
        headers: Headers | None,
    ) -> tuple[bytes, list[tuple[str, bytes]]]:
        decoded = _decode(value)
        if key_event is not None:
            key = partition_key(topic, key_event)
            if decoded is not None and partition_key(topic, decoded) != key:
                raise ContractError(
                    f"key_event names key {key!r} but the value itself names "
                    f"{partition_key(topic, decoded)!r}; a record's key must come from its own "
                    f"content whenever that content can be read"
                )
        elif decoded is None:
            raise ContractError(
                f"a value for {topic} is not a JSON object, so its partition key cannot be read "
                f"from it. Pass the event the key belongs to as key_event; a record is never "
                f"published unkeyed."
            )
        else:
            key = partition_key(topic, decoded)

        if not self.allow_invalid_events:
            model = _models().get(topic)
            if model is None:
                raise SchemaValidationError(f"no released contract model for topic {topic!r}")
            try:
                model.model_validate_json(value)
            except ValidationError as exc:
                raise SchemaValidationError(
                    f"a value for {topic} does not satisfy its released schema and is not "
                    f"published (docs/EVENT_CONTRACTS.md §6.1): {exc}"
                ) from exc

        header_list = list(headers or ())
        if any(name == TRACE_ID_HEADER for name, _ in header_list):
            raise ContractError(
                f"the {TRACE_ID_HEADER!r} header is copied from envelope.trace_id; a caller may "
                f"not supply it, or header and body could disagree"
            )
        source = decoded if decoded is not None else key_event
        envelope = source.get("envelope") if isinstance(source, dict) else None
        trace_id = envelope.get("trace_id") if isinstance(envelope, dict) else None
        if isinstance(trace_id, str) and trace_id:
            header_list.append((TRACE_ID_HEADER, trace_id.encode()))
        return key.encode(), header_list

    def check_topics(self, topics: Sequence[str], *, timeout_s: float) -> list[str]:
        """The topics the broker does not confirm, checked WITHOUT holding the publish gate.

        A publisher on a hot path verifies its topics here, off the request thread, so no publish
        ever waits on broker metadata: a confirmed topic is known, and `publish` skips the check.
        """
        unconfirmed: list[str] = []
        for topic in topics:
            if topic in self._known_topics:
                continue
            try:
                metadata = self.producer.list_topics(topic, timeout=timeout_s)
            except Exception as exc:  # confluent_kafka.KafkaException, imported lazily
                _log.warning(
                    "event_publish_topic_unverified", topic=topic, error=type(exc).__name__
                )
                unconfirmed.append(topic)
                continue
            described = metadata.topics.get(topic)
            if described is None or described.error is not None or not described.partitions:
                unconfirmed.append(topic)
                continue
            with self._gate:
                self._known_topics.add(topic)
        return unconfirmed

    def _ensure_topic(self, topic: str) -> None:
        """Fail at the first publish, not after the message timeout, if the topic is absent."""
        if topic in self._known_topics:
            return
        try:
            metadata = self.producer.list_topics(topic, timeout=self.topic_check_timeout_s)
        except Exception as exc:  # confluent_kafka.KafkaException, imported lazily
            raise EventPublishError(
                f"could not read broker metadata for {topic} within "
                f"{self.topic_check_timeout_s:.0f}s: {exc}"
            ) from exc
        described = metadata.topics.get(topic)
        if described is None or described.error is not None or not described.partitions:
            detail = "absent" if described is None else str(described.error)
            raise EventPublishError(
                f"topic {topic} does not exist on the broker ({detail}). Automatic topic "
                f"creation is disabled: run `scripts/kafka_topics.py apply --environment local`."
            )
        self._known_topics.add(topic)

    def _produce(
        self, topic: str, value: bytes, key: bytes, headers: list[tuple[str, bytes]], *, shed: bool
    ) -> bool:
        deadline: float | None = None
        while True:
            try:
                self.producer.produce(topic, value=value, key=key, headers=headers or None)
            except BufferError:
                if shed:
                    return False
                now = time.monotonic()
                if deadline is None:
                    deadline = now + self.block_timeout_s
                elif now >= deadline:
                    raise EventPublishError(
                        f"the producer queue stayed full for {self.block_timeout_s:.1f}s while "
                        f"publishing to {topic}; the broker is not draining it"
                    ) from None
                self.producer.poll(BLOCKED_POLL_S)
                continue
            self.ledger.record_accepted(topic)
            self.producer.poll(0)
            return True

    def report(self) -> DeliveryReport:
        """A non-raising snapshot; `flush(0)` serves pending reports without waiting."""
        with self._gate:
            return self.ledger.report(outstanding=self.producer.flush(0))

    def close(self, timeout_s: float | None = None) -> DeliveryReport:
        """Refuse further publishes, flush, and vouch for everything accepted -- or raise.

        `flush` returns the number of messages still queued. librdkafka guarantees a
        delivery report for every message it accepted, so once the queue is empty the
        ledger must show a final outcome for each one; any gap is itself a problem.
        """
        wait = self.delivery_timeout_s + FLUSH_MARGIN_S if timeout_s is None else timeout_s
        with self._gate:
            self._closed = True
            outstanding = self.producer.flush(wait)
            report = self.ledger.report(outstanding=outstanding)
        problems = report.problems()
        if problems:
            _log.error(
                "event_publish_unconfirmed",
                accepted=dict(report.accepted),
                outstanding=report.outstanding,
                failed=dict(report.failed),
                shed=dict(report.shed),
                refused=dict(report.refused),
                fatal=report.fatal,
                errors=list(report.error_samples),
            )
            samples = f" First errors: {list(report.error_samples)}" if report.error_samples else ""
            raise EventPublishError(
                "events could not be confirmed as delivered: "
                + "; ".join(problems)
                + "."
                + samples,
                outstanding=report.outstanding,
                failed=dict(report.failed),
                shed=dict(report.shed),
                refused=dict(report.refused),
                fatal=report.fatal,
            )
        _log.info("event_publish_confirmed", delivered=dict(report.delivered))
        return report
