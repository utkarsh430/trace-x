"""The Kafka platform against a real broker: declared topics, the producer, the overlay.

One throwaway broker for the whole module, started as `deploy/compose.yml` runs it:
the same image and broker settings, a named volume at the log directory, a user
network on which the broker answers to the alias `kafka`, and the compose
healthcheck executed inside it. Only the advertised host port differs. The
container, volume and network are named `tracex-kafka-agent-*` and removed when the
module ends, including when a test fails.

Resource gating follows docs/TESTING.md §2 rule 5 -- skipped loudly, never silently --
with one addition: when `CI` is set, a missing Docker, a missing `stream` extra or
a broker that cannot start FAILS instead of skipping, because a CI run whose Kafka
tests all skipped would be green while proving nothing about Kafka.

Tests run in file order against the shared broker. Each test that disturbs the
broker restores it, consumption is always bounded by watermarks taken around the
test's own publishing, and the pause test runs last.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import time
import uuid
import zlib
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

IN_CI = bool(os.environ.get("CI"))


def _unavailable(reason: str) -> None:
    if IN_CI:
        pytest.fail(
            f"FAILED (CI): {reason}. A CI run must execute the Kafka integration tests; a run "
            f"in which they all skipped would report green while proving nothing.",
            pytrace=False,
        )
    pytest.skip(f"SKIPPED (NOT PASSED): {reason}")


try:
    import confluent_kafka  # noqa: F401 -- presence check; used through the factory
except ModuleNotFoundError:  # pragma: no cover - the stream extra is in the lock
    if IN_CI:
        raise
    pytest.skip(
        "SKIPPED (NOT PASSED): the `stream` extra (confluent-kafka) is not installed",
        allow_module_level=True,
    )

from data.generator.emit import KafkaSink, ValidationPolicy, encode_and_validate  # noqa: E402
from eval.replay.faults import (  # noqa: E402
    ARRIVAL_MARGIN_S,
    DEDUP_IDENTITY,
    FaultClass,
    FaultPlan,
    Timing,
    build_overlay,
    publish_overlay,
)

from trace_core.contracts.canonical_json import canonical_bytes  # noqa: E402
from trace_core.contracts.envelope import build_event  # noqa: E402
from trace_core.contracts.publish import (  # noqa: E402
    TRACE_ID_HEADER,
    DeliveryLedger,
    EventPublisher,
    java_partition,
    producer_config,
)
from trace_core.contracts.topics import partition_key  # noqa: E402
from trace_core.domain.errors import EventPublishError  # noqa: E402
from trace_core.domain.identifiers import uuid7  # noqa: E402
from trace_core.domain.time import event_time, processing_time, to_millis  # noqa: E402

pytestmark = pytest.mark.integration

# docs/PHASE3_PLAN.md §4.3 timing semantics v1, written out by hand.
LATE_THRESHOLD_S = 600
FUTURE_BOUND_S = 86_400
CLOCK_MARGIN_S = 300

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "deploy" / "compose.yml"
SCRIPT = ROOT / "scripts" / "kafka_topics.py"
LOG_DIR = "/var/lib/kafka/data"
TX = "tx.raw.v1"
MIB = 1024 * 1024


def _load_tool() -> ModuleType:
    spec = importlib.util.spec_from_file_location("kafka_topics_it", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["kafka_topics_it"] = module
    spec.loader.exec_module(module)
    return module


kt = _load_tool()
DECLARATION = kt.load_declaration()
COMPOSE_KAFKA = yaml.safe_load(COMPOSE.read_text())["services"]["kafka"]


# ------------------------------------------------------------------- broker --


@dataclass(frozen=True)
class Broker:
    name: str
    bootstrap: str


def _docker(*args: str, timeout: float = 120) -> subprocess.CompletedProcess[str]:
    binary = shutil.which("docker") or "docker"
    return subprocess.run([binary, *args], capture_output=True, text=True, timeout=timeout)  # noqa: S603


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _admin(broker: Broker) -> Any:
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": broker.bootstrap})


def _healthcheck_command() -> str:
    """The compose healthcheck as the shell inside the container receives it."""
    test = COMPOSE_KAFKA["healthcheck"]["test"]
    assert test[0] == "CMD-SHELL", test
    return str(test[1]).replace("$$", "$")  # compose un-escapes $$ before the shell sees it


@pytest.fixture(scope="module")
def broker() -> Iterator[Broker]:
    if not shutil.which("docker") or _docker("info").returncode != 0:
        _unavailable("docker is not available; no broker can be started")
    suffix = uuid.uuid4().hex[:8]
    name = f"tracex-kafka-agent-{suffix}"
    network = f"tracex-kafka-agent-net-{suffix}"
    volume = f"tracex-kafka-agent-vol-{suffix}"
    port = _free_port()
    env = {key: str(value) for key, value in COMPOSE_KAFKA["environment"].items()}
    env["KAFKA_ADVERTISED_LISTENERS"] = env["KAFKA_ADVERTISED_LISTENERS"].replace(
        "localhost:${KAFKA_PORT:-9092}", f"127.0.0.1:{port}"
    )
    unresolved = {k: v for k, v in env.items() if "${" in v}
    assert not unresolved, f"compose interpolation this test does not resolve: {unresolved}"
    mount = next(str(v) for v in COMPOSE_KAFKA["volumes"] if str(v).endswith(f":{LOG_DIR}"))
    memory = str(COMPOSE_KAFKA["deploy"]["resources"]["limits"]["memory"]).lower()

    try:
        for created in (_docker("network", "create", network), _docker("volume", "create", volume)):
            if created.returncode != 0:
                _unavailable(f"could not create the broker's network or volume: {created.stderr}")
        args = ["run", "-d", "--name", name, "--network", network, "--network-alias", "kafka"]
        args += ["-v", f"{volume}:{mount.split(':', 1)[1]}", "--memory", memory]
        args += ["-p", f"127.0.0.1:{port}:9092"]
        for key, value in env.items():
            args += ["-e", f"{key}={value}"]
        started = _docker(*args, COMPOSE_KAFKA["image"], timeout=300)
        if started.returncode != 0:
            _unavailable(f"could not start {COMPOSE_KAFKA['image']}: {started.stderr}")
        handle = Broker(name=name, bootstrap=f"127.0.0.1:{port}")
        from confluent_kafka import KafkaException

        deadline = time.monotonic() + 120
        while True:
            try:
                _admin(handle).list_topics(timeout=5)
                break
            except KafkaException:
                if time.monotonic() > deadline:
                    logs = _docker("logs", "--tail", "40", name).stdout
                    pytest.fail(f"broker {name} never became reachable:\n{logs}")
                time.sleep(1)
        yield handle
    finally:
        _docker("unpause", name)
        _docker("rm", "-f", "-v", name)
        _docker("volume", "rm", "-f", volume)
        _docker("network", "rm", network)


# ------------------------------------------------------------------ helpers --


def _tool(bootstrap: str, command: str, timeout_s: int = 20) -> tuple[int, str, dict[str, Any]]:
    proc = subprocess.run(  # noqa: S603
        [
            sys.executable,
            str(SCRIPT),
            command,
            "--environment",
            "local",
            "--bootstrap",
            bootstrap,
            "--timeout",
            str(timeout_s),
        ],
        capture_output=True,
        text=True,
        timeout=240,
        cwd=ROOT,
    )
    output = proc.stdout + proc.stderr
    lines = proc.stdout.strip().splitlines()
    assert lines, f"kafka_topics.py {command} printed nothing:\n{output}"
    return proc.returncode, output, json.loads(lines[-1])


def _cluster(broker: Broker) -> Any:
    return kt.fetch_cluster(_admin(broker), timeout_s=20)


def _wait_for(predicate: Any, what: str, timeout_s: float = 60) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() > deadline:
            pytest.fail(f"timed out waiting for {what}")
        time.sleep(0.5)


def _alter(broker: Broker, topic: str, settings: dict[str, str]) -> None:
    from confluent_kafka.admin import AlterConfigOpType, ConfigEntry, ConfigResource

    resource = ConfigResource(
        ConfigResource.Type.TOPIC,
        topic,
        incremental_configs=[
            ConfigEntry(key, value, incremental_operation=AlterConfigOpType.SET)
            for key, value in settings.items()
        ],
    )
    # The client must outlive its futures: a garbage-collected AdminClient destroys
    # its handle, and every pending future then fails with _DESTROY.
    admin = _admin(broker)
    for future in admin.incremental_alter_configs([resource]).values():
        future.result(timeout=30)
    _wait_for(
        lambda: all(_cluster(broker).topics[topic].config.get(k) == v for k, v in settings.items()),
        f"{topic} to report {settings}",
    )


def _revert(broker: Broker, topic: str, keys: list[str]) -> None:
    """Remove topic-level settings so the broker default applies again."""
    from confluent_kafka.admin import AlterConfigOpType, ConfigEntry, ConfigResource

    resource = ConfigResource(
        ConfigResource.Type.TOPIC,
        topic,
        incremental_configs=[
            ConfigEntry(key, None, incremental_operation=AlterConfigOpType.DELETE) for key in keys
        ],
    )
    admin = _admin(broker)
    for future in admin.incremental_alter_configs([resource]).values():
        future.result(timeout=30)
    _wait_for(
        lambda: not set(keys) & _cluster(broker).topics[topic].overridden,
        f"{topic} to drop {keys}",
    )


def _delete(broker: Broker, topic: str) -> None:
    admin = _admin(broker)
    if topic not in admin.list_topics(timeout=10).topics:
        return
    for future in admin.delete_topics([topic], operation_timeout=30).values():
        future.result(timeout=30)
    _wait_for(lambda: topic not in admin.list_topics(timeout=10).topics, f"{topic} deleted")


def _create(broker: Broker, topic: str, partitions: int, config: dict[str, str]) -> None:
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import NewTopic

    admin = _admin(broker)
    deadline = time.monotonic() + 60
    while True:
        future = admin.create_topics(
            [NewTopic(topic, num_partitions=partitions, replication_factor=1, config=config)]
        )[topic]
        try:
            future.result(timeout=30)
            break
        except KafkaException:
            # A just-deleted topic can still be marked for deletion for a moment.
            if time.monotonic() > deadline:
                raise
            time.sleep(1)

    def ready() -> bool:
        described = admin.list_topics(timeout=10).topics.get(topic)
        return described is not None and len(described.partitions) == partitions

    _wait_for(ready, f"{topic} to have {partitions} partitions")


@dataclass(frozen=True)
class Consumed:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes
    timestamp_type: int
    timestamp_ms: int
    headers: tuple[tuple[str, bytes], ...]


def _headers(raw: Any) -> tuple[tuple[str, bytes], ...]:
    """Kafka headers as (name, bytes) pairs; the client types them as a list or a dict."""
    items = raw.items() if isinstance(raw, dict) else (raw or [])
    pairs: list[tuple[str, bytes]] = []
    for name, value in items:
        encoded = value.encode() if isinstance(value, str) else bytes(value or b"")
        pairs.append((str(name), encoded))
    return tuple(pairs)


def _watermarks(broker: Broker, topic: str) -> dict[int, int]:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": broker.bootstrap, "group.id": f"wm-{uuid.uuid4()}"})
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        return {
            p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
            for p in partitions
        }
    finally:
        consumer.close()


def _consume(broker: Broker, topic: str, start: dict[int, int]) -> list[Consumed]:
    """Every record between `start` and the current high watermark, in partition order."""
    from confluent_kafka import Consumer, TopicPartition

    end = _watermarks(broker, topic)
    consumer = Consumer(
        {
            "bootstrap.servers": broker.bootstrap,
            "group.id": f"it-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "enable.partition.eof": False,
        }
    )
    pending = {p: start.get(p, 0) for p in end if end[p] > start.get(p, 0)}
    records: list[Consumed] = []
    try:
        if pending:
            consumer.assign([TopicPartition(topic, p, offset) for p, offset in pending.items()])
        deadline = time.monotonic() + 60
        while any(pending[p] < end[p] for p in pending) and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            assert message.error() is None, message.error()
            partition, offset, value = message.partition(), message.offset(), message.value()
            assert partition is not None and offset is not None and value is not None
            timestamp_type, timestamp_ms = message.timestamp()
            headers = _headers(message.headers())
            records.append(
                Consumed(
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    key=message.key(),
                    value=value,
                    timestamp_type=timestamp_type,
                    timestamp_ms=timestamp_ms,
                    headers=headers,
                )
            )
            pending[partition] = offset + 1
    finally:
        consumer.close()
    short = {p: (pending[p], end[p]) for p in pending if pending[p] < end[p]}
    assert not short, f"{topic}: partitions not consumed to their watermark: {short}"
    return records


_RNG = random.Random(314159)
_BASE_TIME = dt.datetime(2026, 9, 1, 9, 0, tzinfo=dt.UTC)


def _envelope(event_type: str, at: dt.datetime, payload: dict[str, Any]) -> dict[str, Any]:
    event: dict[str, Any] = build_event(
        event_type=event_type,
        occurred_at=event_time(at),
        payload=payload,
        producer="trace-test@1.0.0",
        trace_id=f"{_RNG.getrandbits(128):032x}",  # distinct per event, so a header check can fail
        correlation_id="corr_kafka_platform",
        ingested_at=processing_time(at + dt.timedelta(milliseconds=30)),
        event_id=uuid7(millis=to_millis(at), rng=_RNG),
    )
    return event


def _tx(i: int, account: int, at: dt.datetime | None = None) -> dict[str, Any]:
    moment = at or _BASE_TIME + dt.timedelta(milliseconds=200 * i)
    return _envelope(
        "tx.raw",
        moment,
        {
            "transaction_id": f"tx_it_{i:010d}",
            "account_id": f"acct_{account:09d}",
            "card_id": f"card_{account:09d}",
            "device_id": f"dev_{account:09d}",
            "merchant_id": f"mrch_{i % 11:06d}",
            "ip_id": f"ip_{account:07d}",
            "amount_minor": 500 + i,
            "currency": "EUR",
            "channel": "CARD_PRESENT",
            "entry_mode": "CHIP",
            "merchant_mcc": "5812",
            "merchant_country": "DE",
            "latitude": 52.52,
            "longitude": 13.4,
            "authorization_outcome": "APPROVED",
        },
    )


def _identity(i: int, account: int, at: dt.datetime) -> dict[str, Any]:
    return _envelope(
        "identity.event",
        at,
        {"account_id": f"acct_{account:09d}", "identity_event_type": "PASSWORD_CHANGE"},
    )


def _device(i: int, device: int, at: dt.datetime) -> dict[str, Any]:
    return _envelope(
        "device.event",
        at,
        {
            "device_id": f"dev_{device:09d}",
            "account_id": f"acct_{i % 40:09d}",
            "device_event_type": "FIRST_SEEN",
            "platform": "android",
        },
    )


def _investigation(i: int, case: int, at: dt.datetime) -> dict[str, Any]:
    return _envelope(
        "investigation.requested",
        at,
        {
            "case_id": f"case_{case:032x}",
            "transaction_id": f"tx_it_{i:010d}",
            "account_id": f"acct_{i % 40:09d}",
            "risk_band": "CRITICAL",
            "score": 0.91,
            "rule_pack_id": "core",
            "rule_pack_digest": "sha256:" + "c" * 64,
            "threshold_config_digest": "sha256:" + "d" * 64,
            "feature_set_version": "1.0.0",
            "feature_source": "ONLINE_ONLY",
            "degraded": False,
            "fired_rule_ids": ["R002_amount"],
        },
    )


def _mixed_stream(
    n: int, *, start: dt.datetime, spacing_ms: int = 200
) -> list[tuple[str, dict[str, Any]]]:
    stream: list[tuple[str, dict[str, Any]]] = []
    for i in range(n):
        at = start + dt.timedelta(milliseconds=spacing_ms * i)
        stream.append((TX, _tx(i, i % 40, at)))
        if i % 3 == 0:
            stream.append(("identity.events.v1", _identity(i, i % 40, at)))
        if i % 4 == 0:
            stream.append(("device.events.v1", _device(i, i % 9, at)))
        if i % 5 == 0:
            stream.append(("investigation.requested.v1", _investigation(i, i % 7, at)))
    return stream


def _occurred_s(value: bytes) -> float:
    occurred = json.loads(value)["envelope"]["occurred_at"]
    return dt.datetime.fromisoformat(occurred.replace("Z", "+00:00")).timestamp()


def _decodes(value: bytes) -> bool:
    try:
        json.loads(value)
    except ValueError:
        return False
    return True


def _trace_header(record: Consumed) -> bytes | None:
    return next((v for name, v in record.headers if name == TRACE_ID_HEADER), None)


# -------------------------------------------------------------------- tests --


def test_an_unreachable_broker_exits_three_and_names_no_cluster() -> None:
    """Runs first and needs no broker: the exit a missing broker must produce."""
    nowhere = f"127.0.0.1:{_free_port()}"
    for command in ("verify", "apply"):
        code, output, summary = _tool(nowhere, command, timeout_s=3)
        assert code == 3, output
        assert summary["exit"] == 3 and summary["cluster_id"] is None
        assert summary["environment"] == "local"
        assert "UNREACHABLE" in output


def test_the_compose_healthcheck_passes_and_catches_an_unreachable_broker(broker: Broker) -> None:
    command = _healthcheck_command()

    def healthy() -> bool:
        return _docker("exec", broker.name, "sh", "-c", command).returncode == 0

    _wait_for(healthy, "the compose healthcheck to pass", timeout_s=90)
    # Negative control: bootstrap through PLAINTEXT, whose advertised address is the
    # HOST port and is not reachable from inside the container. The tool itself still
    # exits 0 while printing "-> ERROR"; the compose check must not.
    unreachable = command.replace("kafka:19092", "localhost:9092")
    assert _docker("exec", broker.name, "sh", "-c", unreachable).returncode != 0
    bare = "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092"
    tool = _docker("exec", broker.name, "sh", "-c", bare)
    assert tool.returncode == 0 and "-> ERROR" in tool.stdout, "the tool's exit is not a signal"


def test_apply_creates_the_declaration_and_a_second_apply_changes_nothing(broker: Broker) -> None:
    from confluent_kafka import KafkaException, Producer

    code, output, summary = _tool(broker.bootstrap, "apply")
    assert code == 0, output
    assert summary["created"] == sorted(DECLARATION.topics), output
    assert summary["environment"] == "local"
    live_cluster_id = _admin(broker).list_topics(timeout=10).cluster_id
    assert summary["cluster_id"] == live_cluster_id and live_cluster_id
    assert f"cluster_id={live_cluster_id}" in output

    before = _cluster(broker)
    for name, topic in DECLARATION.topics.items():
        live = before.topics[name]
        assert live.partitions == topic.partitions, name
        assert live.config["message.timestamp.type"] == "LogAppendTime", name
        # Every declared key is one this broker actually knows.
        assert set(topic.applied_config) <= set(live.config), name
        for key, value in topic.applied_config.items():
            assert live.config[key] == value, (name, key)
    assert all(before.topics[name].topic_id for name in DECLARATION.topics)
    assert summary["topic_ids"] == {
        n: before.topics[n].topic_id for n in sorted(DECLARATION.topics)
    }

    code, output, summary = _tool(broker.bootstrap, "apply")
    assert code == 0, output
    assert summary["created"] == [], "a second apply created something"
    after = _cluster(broker)
    assert {
        n: (t.partitions, dict(t.config), t.topic_id)
        for n, t in after.topics.items()
        if n in DECLARATION.topics
    } == {
        n: (t.partitions, dict(t.config), t.topic_id)
        for n, t in before.topics.items()
        if n in DECLARATION.topics
    }
    code, output, _ = _tool(broker.bootstrap, "verify")
    assert code == 0, output

    # librdkafka accepts every contract setting (the factory builds real producers in
    # every test below), and refuses a value it does not know.
    config = producer_config(
        bootstrap_servers=broker.bootstrap, client_id="it-config", ledger=DeliveryLedger()
    )
    with pytest.raises(KafkaException):
        Producer({**config, "compression.type": "not-a-codec"})


def test_verify_detects_drift_and_a_create_time_topic_is_observable(broker: Broker) -> None:
    from confluent_kafka import TIMESTAMP_CREATE_TIME

    topic = "identity.events.v1"
    declared = DECLARATION.topics[topic].applied_config
    _alter(
        broker,
        topic,
        {
            "message.timestamp.type": "CreateTime",
            "retention.bytes": str(MIB),
            # Not declared at all: verify must still see it.
            "max.message.bytes": "524288",
        },
    )
    try:
        code, output, summary = _tool(broker.bootstrap, "verify")
        assert code == 1, output
        assert any("message.timestamp.type" in d for d in summary["drift"]), output
        assert any("retention.bytes" in d for d in summary["drift"]), output
        assert any("max.message.bytes" in d and "not declared" in d for d in summary["drift"]), (
            output
        )

        # apply never alters an existing topic's configuration; it reports the drift.
        code, output, summary = _tool(broker.bootstrap, "apply")
        assert code == 1 and summary["created"] == [], output

        # Negative control for the LogAppendTime checks below: a CreateTime topic
        # really does deliver CREATE_TIME records.
        start = _watermarks(broker, topic)
        publisher = EventPublisher.connect(broker.bootstrap, client_id="it-create-time")
        publisher.publish(topic, canonical_bytes(_identity(1, 1, _BASE_TIME)))
        publisher.close(timeout_s=30)
        consumed = _consume(broker, topic, start)
        assert [c.timestamp_type for c in consumed] == [TIMESTAMP_CREATE_TIME]
    finally:
        _alter(
            broker,
            topic,
            {
                "message.timestamp.type": declared["message.timestamp.type"],
                "retention.bytes": declared["retention.bytes"],
            },
        )
        _revert(broker, topic, ["max.message.bytes"])
    code, output, _ = _tool(broker.bootstrap, "verify")
    assert code == 0, output


def test_a_wrong_partition_count_or_cleanup_policy_is_refused_and_never_altered(
    broker: Broker,
) -> None:
    device, identity, investigation = (
        "device.events.v1",
        "identity.events.v1",
        "investigation.requested.v1",
    )
    original_id = _cluster(broker).topics[device].topic_id
    for name in (device, identity, investigation):
        _delete(broker, name)
    _create(broker, device, 1, DECLARATION.topics[device].applied_config)
    _create(
        broker,
        identity,
        DECLARATION.topics[identity].partitions,
        {**DECLARATION.topics[identity].applied_config, "cleanup.policy": "compact"},
    )
    try:
        code, output, summary = _tool(broker.bootstrap, "apply")
        assert code == 2, output
        assert "REFUSED" in output
        assert any(device in r and "partition" in r for r in summary["refused"]), output
        assert any(identity in r and "cleanup.policy" in r for r in summary["refused"]), output
        # All-or-nothing: the missing, non-conflicting topic was not created either.
        live = _cluster(broker)
        assert investigation not in live.topics
        assert live.topics[device].partitions == 1, "apply changed a partition count"
        assert live.topics[identity].config["cleanup.policy"] == "compact"
        code, output, _ = _tool(broker.bootstrap, "verify")
        assert code == 1, output

        # A publisher meets the missing topic at its first publish, not after the
        # full message timeout.
        publisher = EventPublisher.connect(broker.bootstrap, client_id="it-missing-topic")
        began = time.monotonic()
        with pytest.raises(EventPublishError, match="does not exist"):
            publisher.publish(investigation, canonical_bytes(_investigation(1, 1, _BASE_TIME)))
        assert time.monotonic() - began < 20
    finally:
        _delete(broker, device)
        _delete(broker, identity)
        code, output, _ = _tool(broker.bootstrap, "apply")
        assert code == 0, output
    recreated = _cluster(broker).topics[device]
    assert recreated.partitions == DECLARATION.topics[device].partitions
    assert recreated.topic_id and recreated.topic_id != original_id, (
        "a recreated topic kept its id; the id would not reveal a replaced history"
    )


def test_the_generator_sink_publishes_validated_rows_and_confirms_them(broker: Broker) -> None:
    """The rebuilt KafkaSink against a real broker: keyed, placed, confirmed at close."""
    stream = _mixed_stream(150, start=dt.datetime.now(dt.UTC))
    stream = [(topic, event) for topic, event in stream if topic != "investigation.requested.v1"]
    topics = sorted({topic for topic, _ in stream})
    starts = {topic: _watermarks(broker, topic) for topic in topics}
    sink = KafkaSink(bootstrap_servers=broker.bootstrap)
    for position, (topic, event) in enumerate(stream):
        sink.write(topic, encode_and_validate(topic, event, ValidationPolicy.ALL, position))
    sink.close()
    assert sink.report is not None and sink.report.confirmed
    assert dict(sink.report.delivered) == dict(Counter(topic for topic, _ in stream))
    for topic in topics:
        consumed = _consume(broker, topic, starts[topic])
        assert len(consumed) == sink.report.delivered[topic]
        partitions = DECLARATION.topics[topic].partitions
        assert all(
            c.key is not None and c.partition == java_partition(c.key, partitions) for c in consumed
        )


def test_every_acknowledged_event_is_consumed_keyed_traced_and_log_append_timed(
    broker: Broker,
) -> None:
    from confluent_kafka import TIMESTAMP_LOG_APPEND_TIME

    stream = _mixed_stream(400, start=dt.datetime.now(dt.UTC))
    topics = sorted({topic for topic, _ in stream})
    starts = {topic: _watermarks(broker, topic) for topic in topics}
    publisher = EventPublisher.connect(broker.bootstrap, client_id="it-conservation")
    began = time.time()
    sent: Counter[tuple[str, bytes, bytes]] = Counter()
    for topic, event in stream:
        value = canonical_bytes(event)
        publisher.publish(topic, value)
        sent[(topic, partition_key(topic, event).encode(), value)] += 1
    report = publisher.close(timeout_s=60)
    finished = time.time()
    assert report.confirmed
    expected_per_topic = Counter(topic for topic, _ in stream)
    assert dict(report.accepted) == dict(report.delivered) == dict(expected_per_topic)

    got: Counter[tuple[str, bytes, bytes]] = Counter()
    for topic in topics:
        consumed = _consume(broker, topic, starts[topic])
        # Acknowledged = consumed, and nothing else landed in the range.
        assert len(consumed) == report.delivered[topic], topic
        partitions = DECLARATION.topics[topic].partitions
        for record in consumed:
            assert record.key is not None
            got[(topic, record.key, record.value)] += 1
            assert record.timestamp_type == TIMESTAMP_LOG_APPEND_TIME, topic
            assert began - 120 <= record.timestamp_ms / 1000 <= finished + 120, (
                "broker clock and host clock disagree by minutes"
            )
            assert record.partition == java_partition(record.key, partitions), (
                f"{topic}: key {record.key!r} landed on partition {record.partition}, the Java "
                f"client would use {java_partition(record.key, partitions)}"
            )
            body_trace = json.loads(record.value)["envelope"]["trace_id"].encode()
            assert _trace_header(record) == body_trace, "trace_id header and body disagree"
    assert got == sent


def test_the_java_partition_prediction_catches_the_wrong_partitioner(broker: Broker) -> None:
    from confluent_kafka import Producer

    accounts = list(range(300))
    events = [_tx(10_000 + a, a) for a in accounts]
    placements: dict[str, dict[bytes, int]] = {}
    for partitioner in ("murmur2_random", "consistent_random"):
        ledger = DeliveryLedger()
        placed: dict[bytes, int] = {}

        def on_delivery(err: Any, msg: Any, placed: dict[bytes, int] = placed) -> None:
            assert err is None, err
            placed[msg.key()] = msg.partition()

        config = producer_config(
            bootstrap_servers=broker.bootstrap, client_id=f"it-{partitioner}", ledger=ledger
        )
        producer = Producer({**config, "partitioner": partitioner, "on_delivery": on_delivery})
        for event in events:
            producer.produce(
                TX, value=canonical_bytes(event), key=partition_key(TX, event).encode()
            )
        assert producer.flush(60) == 0
        assert len(placed) == len(accounts)
        placements[partitioner] = placed

    n = DECLARATION.topics[TX].partitions
    assert all(p == java_partition(key, n) for key, p in placements["murmur2_random"].items())
    wrong = [k for k, p in placements["consistent_random"].items() if p != java_partition(k, n)]
    assert wrong, (
        "consistent_random agreed with murmur2 on every key; the check discriminates nothing"
    )
    assert all(p == zlib.crc32(k) % n for k, p in placements["consistent_random"].items())


def test_a_paced_overlay_arrives_in_exactly_its_injected_counts_and_classes(
    broker: Broker,
) -> None:
    """Published on its schedule through a real broker, with arrival judged by LogAppendTime.

    The held-back late copies are exercised on a simulated clock in the unit tests: a
    real one would wait past the late threshold.
    """
    from confluent_kafka import TIMESTAMP_LOG_APPEND_TIME

    base = _mixed_stream(100, start=_BASE_TIME, spacing_ms=150)
    counts = {
        fault: 0
        if fault in (FaultClass.LATE_EXACT_DUPLICATE, FaultClass.LATE_RETRY_NEW_EVENT_ID)
        else 3
        for fault in FaultClass
    }
    plan = FaultPlan(seed=20260913, counts=counts, timing=Timing.PACED, duplicate_delay_s=(1, 5))
    overlay = build_overlay(base, plan, base_dataset_ref="fixture:kafka-platform-mixed-100")
    assert overlay.provenance.planned_fault_free_delay_s is not None
    topics = sorted({record.topic for record in overlay.records})
    starts = {topic: _watermarks(broker, topic) for topic in topics}

    publisher = EventPublisher.connect(
        broker.bootstrap, client_id="it-overlay", allow_invalid_events=True
    )
    manifest = publish_overlay(overlay, publisher)
    assert publisher.close(timeout_s=60).confirmed
    assert manifest.max_schedule_slip_s < 5, manifest.max_schedule_slip_s

    consumed = [record for topic in topics for record in _consume(broker, topic, starts[topic])]
    expected = Counter((p.record.topic, p.record.key.encode(), p.value) for p in manifest.records)
    assert Counter((c.topic, c.key, c.value) for c in consumed) == expected

    by_value: dict[tuple[str, bytes], list[Any]] = {}
    for published in manifest.records:
        by_value.setdefault((published.record.topic, published.value), []).append(published)
    arrived: Counter[str] = Counter()
    where: dict[int, Consumed] = {}
    for arrival in consumed:
        published = by_value[(arrival.topic, arrival.value)].pop()
        where[published.record.position] = arrival
        if published.record.fault is not None:
            arrived[published.record.fault.value] += 1
    assert dict(arrived) == {k: v for k, v in overlay.provenance.injected.items() if v}

    on_time_delays: list[float] = []
    for published in manifest.records:
        record, got = published.record, where[published.record.position]
        assert got.timestamp_type == TIMESTAMP_LOG_APPEND_TIME
        partitions = DECLARATION.topics[record.topic].partitions
        assert got.partition == java_partition(record.key.encode(), partitions)
        assert _trace_header(got) == record.key_event["envelope"]["trace_id"].encode()
        if record.malformed_rank is not None:
            continue
        delay = got.timestamp_ms / 1000 - _occurred_s(got.value)
        fault = record.fault
        if fault is FaultClass.LATE:
            assert delay > LATE_THRESHOLD_S, delay
        elif fault is FaultClass.FUTURE_WITHIN_24H:
            assert 0 < -delay < FUTURE_BOUND_S, delay
        elif fault is FaultClass.FUTURE_BEYOND_24H:
            assert -delay > FUTURE_BOUND_S + CLOCK_MARGIN_S, delay
        else:
            on_time_delays.append(delay)
            assert -60 < delay < LATE_THRESHOLD_S - ARRIVAL_MARGIN_S, (fault, delay)
    assert on_time_delays, "no on-time record was checked"

    originals = [p.record for p in manifest.records if p.record.malformed_rank is None]
    for held in (p.record for p in manifest.records if p.record.fault is FaultClass.REORDERED):
        successor = min(
            (
                r
                for r in originals
                if r.topic == held.topic
                and r.key == held.key
                and r.base_index > held.base_index
                and r.fault is None
            ),
            key=lambda r: r.base_index,
        )
        first, second = where[successor.position], where[held.position]
        assert first.partition == second.partition and first.offset < second.offset

    for duplicated in (FaultClass.RETRY_NEW_EVENT_ID, FaultClass.CONFLICTING_DUPLICATE):
        for published in (p for p in manifest.records if p.record.fault is duplicated):
            topic = published.record.topic
            section, field = DEDUP_IDENTITY[topic]
            identity = json.loads(published.value)[section][field]
            same = [
                c
                for c in consumed
                if c.topic == topic
                and _decodes(c.value)
                and json.loads(c.value)[section][field] == identity
            ]
            assert len({c.value for c in same}) >= 2, (duplicated, identity)
    for published in (p for p in manifest.records if p.record.fault is FaultClass.MALFORMED_JSON):
        assert not _decodes(where[published.record.position].value)


def test_retention_holds_a_partition_within_its_declared_bound_while_traffic_flows(
    broker: Broker,
) -> None:
    """The disk budget's per-partition assumption, measured on the volume.

    While traffic flows the partition stays within R + 2S + rate x (2I + D) -- which the
    broker's default 300 s check interval could not keep, since this test writes more
    than that bound without any trim -- and once it stops the partition settles below
    R + S without being trimmed below R.
    """
    from confluent_kafka import Producer

    topic = "retention-bound-probe"
    retention, segment = 4 * MIB, 1 * MIB
    check_s = int(DECLARATION.broker["log.retention.check.interval.ms"]) / 1000
    delete_s = int(DECLARATION.broker["log.segment.delete.delay.ms"]) / 1000
    window_s = 2 * check_s + delete_s
    _create(
        broker,
        topic,
        1,
        {
            "retention.bytes": str(retention),
            "segment.bytes": str(segment),
            "message.timestamp.type": "LogAppendTime",
        },
    )
    directory = f"{LOG_DIR}/{topic}-0"

    def on_disk_kib(pattern: str = "") -> int:
        command = f"du -ck {directory}/{pattern} | tail -n 1" if pattern else f"du -sk {directory}"
        result = _docker("exec", broker.name, "sh", "-c", command)
        assert result.returncode == 0, f"{directory} is not on the volume: {result.stderr}"
        return int(result.stdout.split()[0])

    try:
        producer = Producer({"bootstrap.servers": broker.bootstrap, "compression.type": "none"})
        rate_bytes_s = 1 * MIB
        duration_s = 45.0
        chunk = 64 * 1024
        written = 0
        peak_kib = 0
        began = time.monotonic()
        next_sample = began
        while (elapsed := time.monotonic() - began) < duration_s:
            while written < rate_bytes_s * elapsed:
                producer.produce(topic, value=os.urandom(chunk), key=b"k")
                written += chunk
            producer.poll(0)
            if time.monotonic() >= next_sample:
                peak_kib = max(peak_kib, on_disk_kib())
                next_sample += 1.0
            time.sleep(0.05)
        assert producer.flush(60) == 0
        peak_kib = max(peak_kib, on_disk_kib())
        measured_rate = written / duration_s
        bound_kib = (retention + 2 * segment + measured_rate * window_s) / 1024 + 1024
        assert written / 1024 > bound_kib, "control: the test wrote less than the bound"
        assert peak_kib <= bound_kib, (
            f"while traffic flowed the partition reached {peak_kib} KiB, over the derived bound "
            f"{bound_kib:.0f} KiB (R + 2S + rate x (2I + D) plus 1 MiB of slack)"
        )

        slack_kib = 256  # snapshot and checkpoint files; index files are sparse
        deadline = time.monotonic() + check_s + delete_s + 30
        total_kib = on_disk_kib()
        while total_kib > (retention + segment) // 1024 + slack_kib and time.monotonic() < deadline:
            time.sleep(1)
            total_kib = on_disk_kib()
        assert total_kib <= (retention + segment) // 1024 + slack_kib, (
            f"after traffic stopped the partition still holds {total_kib} KiB, over R + S"
        )
        live_kib = on_disk_kib("*.log")
        assert live_kib >= retention // 1024 - 64, f"retention trimmed below R: {live_kib} KiB"
        stray = _docker("exec", broker.name, "test", "-e", "/tmp/kafka-logs")  # noqa: S108
        assert stray.returncode != 0, "the broker wrote a log directory under /tmp"
    finally:
        _delete(broker, topic)


def test_delivery_failures_and_shedding_are_counted_while_the_broker_is_paused(
    broker: Broker,
) -> None:
    events = [_tx(20_000 + i, i % 13) for i in range(20)]

    # Control: identical settings deliver against the healthy broker.
    control = EventPublisher.connect(
        broker.bootstrap, client_id="it-pause-control", message_timeout_ms=3_000
    )
    for event in events[:5]:
        control.publish(TX, canonical_bytes(event))
    assert control.close(timeout_s=30).confirmed

    # Producers that are already connected, with topic metadata cached, when the outage starts.
    failing = EventPublisher.connect(
        broker.bootstrap, client_id="it-pause-failing", message_timeout_ms=3_000
    )
    failing.publish(TX, canonical_bytes(events[0]))
    queued = EventPublisher.connect(
        broker.bootstrap, client_id="it-pause-queued", message_timeout_ms=60_000
    )
    queued.publish(TX, canonical_bytes(events[0]))
    shedder = EventPublisher.connect(
        broker.bootstrap,
        client_id="it-pause-shed",
        message_timeout_ms=3_000,
        allow_invalid_events=True,
    )
    shedder.publish(TX, canonical_bytes(events[0]))
    sink = KafkaSink(bootstrap_servers=broker.bootstrap, message_timeout_ms=3_000)
    sink.write(TX, canonical_bytes(events[0]))
    for warm in (failing, queued, shedder):
        assert warm.report().delivered.get(TX) == 1 or warm.producer.flush(10) == 0

    paused = _docker("pause", broker.name)
    assert paused.returncode == 0, paused.stderr
    try:
        for event in events:
            failing.publish(TX, canonical_bytes(event))
        began = time.monotonic()
        with pytest.raises(EventPublishError) as caught:
            failing.close(timeout_s=30)
        assert caught.value.failed.get(TX) == len(events), caught.value
        assert caught.value.outstanding == 0
        assert time.monotonic() - began < 25, "the verdict came from the flush timeout"

        for event in events[:5]:
            queued.publish(TX, canonical_bytes(event))
        with pytest.raises(EventPublishError) as still_queued:
            queued.close(timeout_s=1)
        assert still_queued.value.outstanding == 5
        assert not still_queued.value.failed

        # The defect this step fixes: the generator's sink used to return from close
        # with rows still unsent. It now raises, naming what failed.
        for event in events[:10]:
            sink.write(TX, canonical_bytes(event))
        with pytest.raises(EventPublishError) as sink_failure:
            sink.close()
        assert sink_failure.value.failed.get(TX) == 10

        # A real BufferError: 512 KiB values against the contract's 64 MiB queue.
        big = os.urandom(512 * 1024)
        outcomes = [shedder.publish(TX, big, key_event=events[1], block=False) for _ in range(200)]
        assert outcomes.count(False) > 0, "the producer queue never filled"
        with pytest.raises(EventPublishError) as shed:
            shedder.close(timeout_s=30)
        assert shed.value.shed.get(TX) == outcomes.count(False)
    finally:
        _docker("unpause", broker.name)

    # Outstanding is not failed: once the broker answers, the queue is delivered.
    recovered = queued.close(timeout_s=90)
    assert recovered.delivered.get(TX) == 6
