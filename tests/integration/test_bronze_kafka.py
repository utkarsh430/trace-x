"""Bronze against a real broker, real Spark and real Delta (Phase 3 Step 5; `P3.kafka-ingest`).

One throwaway broker for this module, started exactly as tests/integration/test_kafka_platform.py
starts it, and one Spark session on the pinned toolchain. Each test recreates the topics it uses
from their declarations and writes its own lake, so no test reads another's records. Nothing is
mocked: the broker, the Kafka connector, Delta and (for coverage) PostgreSQL are the real ones.

The independent oracle is the broker itself: every Bronze row is compared, byte for byte, with the
record a plain consumer reads at the same partition and offset.

What is proved here, each with the failure it catches:
- every released topic lands in Bronze exactly as published, and conservation holds per partition;
- a restart after a crash between the sink's commit and Spark's neither loses nor duplicates, and a
  lost checkpoint is refused instead of silently re-reading from the start;
- a poison value is stored raw between valid records and blocks nothing;
- offsets trimmed before Bronze read them stop the query loudly, while trimming what Bronze already
  read does not;
- a deleted and recreated topic is refused, where Spark on its own silently skips records, and a
  topic recreated under a running query stops it;
- a reset that begins above offsets no checkpoint read is refused, or, at earliest, reported as
  skipped by conservation for good;
- the coverage rule reports exactly the gaps it defines over Bronze and the real session ledger.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from tests.integration.test_kafka_platform import (  # noqa: F401 -- `broker` is a fixture
    DECLARATION,
    Broker,
    Consumed,
    _admin,
    _consume,
    _create,
    _delete,
    _identity,
    _mixed_stream,
    _tx,
    _wait_for,
    _watermarks,
    broker,
)
from tests.integration.test_observation_log_kafka import _identity_event
from tests.unit.test_observation_log_sequencing import _PIPELINE, _scored

from trace_core.contracts import authorization
from trace_core.contracts.api.transaction import TransactionRequest
from trace_core.contracts.canonical_json import canonical_bytes
from trace_core.contracts.envelope import build_event
from trace_core.contracts.publish import EventPublisher, java_partition
from trace_core.contracts.topics import (
    IDENTITY_EVENTS_V1,
    TX_AUTHORIZATION_V1,
    TX_RAW_V1,
    TX_SCORED_V1,
)
from trace_core.domain.errors import CheckpointRefusedError
from trace_core.domain.time import event_time
from trace_core.observation.log import SEQ_HEADER, SESSION_HEADER
from trace_core.observation.scored_event import build_scored_event
from trace_core.observation.supervisor import LEASE_S, TAKEOVER_MARGIN_S
from trace_core.repositories.postgres_sessions import PostgresSessionLedger
from trace_core.stream import bronze, checkpoints
from trace_core.stream.bronze import (
    BRONZE_TOPICS,
    SPARK_LOG_APPEND_TIME,
    BronzeQuery,
    Trigger,
    record_progress,
    start_bronze_query,
)
from trace_core.stream.bronze_conservation import (
    ConservationReport,
    OffsetRange,
    check_conservation,
)
from trace_core.stream.bronze_coverage import assess_bronze_coverage, logged_at
from trace_core.stream.checkpoints import KafkaSourceStart, SparkProgress, StartAction
from trace_core.stream.lake import LakeConfig

pytestmark = [pytest.mark.integration, pytest.mark.stream]

SHA = hashlib.sha1(b"trace-x bronze kafka integration", usedforsecurity=False).hexdigest()
CONFLUENT_LOG_APPEND_TIME = 2
"""confluent_kafka.TIMESTAMP_LOG_APPEND_TIME; Spark numbers the same type 1."""
DATA_LOSS = re.compile(r"KAFKA_DATA_LOSS|data may have been (lost|missed)", re.IGNORECASE)
LOUD_RECREATION = re.compile(
    r"deleted and recreated|cannot read the topic's id|KAFKA_DATA_LOSS|data may have been "
    r"(lost|missed)",
    re.IGNORECASE,
)
"""Every way a topic recreated under a running query may be noticed: the per-batch topic check,
the topic missing while it is recreated, or Spark's own data-loss detection."""
INSTANCE_PREFIX = "gw-bronze-coverage-"


@pytest.fixture(scope="module")
def spark() -> Iterator[Any]:
    from trace_core.stream.session import build_session

    session = build_session("trace-x-bronze-kafka")
    try:
        yield session
    finally:
        session.stop()


# ------------------------------------------------------------------- helpers ---


def _fresh(target: Broker, *topics: str) -> None:
    for topic in topics:
        _delete(target, topic)
        declared = DECLARATION.topics[topic]
        _create(target, topic, declared.partitions, dict(declared.applied_config))


Headers = tuple[tuple[str, bytes], ...]
RawHeaders = tuple[tuple[str, bytes | None], ...]
"""A Kafka header value may be null, and Bronze keeps it null."""


def _publish(target: Broker, items: list[tuple[str, dict[str, Any], Headers]]) -> None:
    publisher = EventPublisher.connect(
        target.bootstrap, client_id=f"it-bronze-{uuid.uuid4().hex[:8]}"
    )
    for topic, event, headers in items:
        assert publisher.publish(topic, canonical_bytes(event), headers=headers or None)
    publisher.close(30.0)  # raises unless every accepted record was delivered


def _produce_raw(
    target: Broker, topic: str, records: list[tuple[bytes, bytes, RawHeaders]]
) -> None:
    """A client outside the producer factory, as a buggy or hostile producer would be."""
    from confluent_kafka import Producer

    failures: list[Any] = []
    producer = Producer(
        {
            "bootstrap.servers": target.bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "partitioner": "murmur2_random",
        }
    )
    for key, value, headers in records:
        producer.produce(
            topic,
            key=key,
            value=value,
            headers=list(headers) or None,
            on_delivery=lambda error, _message: failures.append(error) if error else None,
        )
    assert producer.flush(30) == 0 and not failures, failures


def _trim(target: Broker, topic: str, partition: int, *, before: int) -> None:
    from confluent_kafka import TopicPartition

    admin = _admin(target)  # must outlive its futures
    futures = admin.delete_records(
        [TopicPartition(topic, partition, before)], request_timeout=30, operation_timeout=30
    )
    for future in futures.values():
        future.result(timeout=30)


def _ingest(
    spark: Any, lake: LakeConfig, target: Broker, topic: str, *, max_offsets: int | None = None
) -> BronzeQuery:
    handle = start_bronze_query(
        spark,
        lake,
        topic,
        bootstrap_servers=target.bootstrap,
        git_sha=SHA,
        dirty_worktree=False,
        now=dt.datetime.now(dt.UTC),
        trigger=Trigger(available_now=True),
        max_offsets_per_trigger=max_offsets,
    )
    handle.query.awaitTermination()
    assert handle.query.exception() is None, handle.query.exception()
    return handle


@dataclass(frozen=True)
class Landed:
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: RawHeaders | None
    timestamp_us: int
    timestamp_type: int
    trust_tier: str
    batch_id: int
    checkpoint_id: str
    topic_id: str


def _bytes(value: Any) -> bytes | None:
    return None if value is None else bytes(value)


def _landed(spark: Any, lake: LakeConfig, topic: str) -> list[Landed]:
    path = BRONZE_TOPICS[topic].table.local_path(lake)
    rows = (
        spark.read.format("delta")
        .load(str(path))
        .selectExpr("*", "unix_micros(kafka_timestamp) AS ts_us")
        .orderBy("kafka_partition", "kafka_offset")
        .collect()
    )
    return [
        Landed(
            partition=int(r["kafka_partition"]),
            offset=int(r["kafka_offset"]),
            key=_bytes(r["kafka_key"]),
            value=_bytes(r["kafka_value"]),
            headers=None
            if r["kafka_headers"] is None
            else tuple((str(h["key"]), _bytes(h["value"])) for h in r["kafka_headers"]),
            timestamp_us=int(r["ts_us"]),
            timestamp_type=int(r["kafka_timestamp_type"]),
            trust_tier=str(r["trust_tier"]),
            batch_id=int(r["bronze_batch_id"]),
            checkpoint_id=str(r["bronze_checkpoint_id"]),
            topic_id=str(r["kafka_topic_id"]),
        )
        for r in rows
    ]


def _exact_headers(
    target: Broker, topic: str, start: dict[int, int]
) -> dict[tuple[int, int], RawHeaders]:
    """Every record's headers exactly as the broker holds them. `_consume` reads a null header
    value as empty bytes; Bronze keeps it null, so the oracle must too."""
    from confluent_kafka import Consumer, TopicPartition

    end = _watermarks(target, topic)
    consumer = Consumer(
        {
            "bootstrap.servers": target.bootstrap,
            "group.id": f"it-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    pending = {p: start.get(p, 0) for p in end if end[p] > start.get(p, 0)}
    headers: dict[tuple[int, int], RawHeaders] = {}
    try:
        if pending:
            consumer.assign([TopicPartition(topic, p, offset) for p, offset in pending.items()])
        deadline = time.monotonic() + 60
        while any(pending[p] < end[p] for p in pending) and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            assert message.error() is None, message.error()
            partition, offset = message.partition(), message.offset()
            assert partition is not None and offset is not None
            raw: Any = message.headers() or []
            headers[(partition, offset)] = tuple(
                (str(name), value.encode() if isinstance(value, str) else value)
                for name, value in raw
            )
            pending[partition] = offset + 1
    finally:
        consumer.close()
    return headers


def _as_the_broker_holds(record: Consumed, headers: RawHeaders) -> tuple[Any, ...]:
    assert record.timestamp_type == CONFLUENT_LOG_APPEND_TIME
    return (record.partition, record.offset, record.key, record.value, headers,
            record.timestamp_ms * 1000)  # fmt: skip


def _as_bronze_holds(row: Landed) -> tuple[Any, ...]:
    assert row.timestamp_type == SPARK_LOG_APPEND_TIME
    return (row.partition, row.offset, row.key, row.value, row.headers or (), row.timestamp_us)


def _broker_view(target: Broker, topic: str, start: dict[int, int] | None = None) -> list[Any]:
    exact = _exact_headers(target, topic, start or {})
    return sorted(
        _as_the_broker_holds(r, exact[(r.partition, r.offset)])
        for r in _consume(target, topic, start or {})
    )


def _conservation(spark: Any, lake: LakeConfig, target: Broker, topic: str) -> ConservationReport:
    """Conservation as the service runs it: compared with the broker's current topic id."""
    broker_topic_id = bronze.fetch_topic_identity(target.bootstrap, topic).topic_id
    return check_conservation(spark, lake, topic, broker_topic_id=broker_topic_id)


def _authorization(i: int) -> dict[str, Any]:
    occurred = 1_757_000_000_000 + i * 1_000
    return authorization.build_event(
        transaction_id=f"tx_it_{i:010d}",
        account_id=f"acct_{i % 4:09d}",
        authorization_outcome="DECLINED" if i % 2 else "APPROVED",
        decided_ms=occurred + 340,
        transaction_occurred_ms=occurred,
        transaction_occurred_at=authorization.iso_millis(occurred),
        producer="trace-test@1.0.0",
        trace_id=f"{i + 1:032x}",
        correlation_id=f"tx_it_{i:010d}",
        ingested_ms=occurred + 400,
    )


def _session_headers(session_id: str, seq: int) -> Headers:
    return ((SESSION_HEADER, session_id.encode()), (SEQ_HEADER, str(seq).encode()))


def _scored_for(i: int, account: str) -> dict[str, Any]:
    """A gateway scored event on `account`, stamped by this host's clock, as the gateway does."""
    now = dt.datetime.now(dt.UTC)
    request = TransactionRequest.model_validate(
        {
            "transaction_id": f"tx_{i:012d}",
            "account_id": account,
            "amount_minor": 5_000,
            "currency": "GBP",
            "occurred_at": now.isoformat().replace("+00:00", "Z"),
        }
    )
    outcome = _PIPELINE.score(request, now=now)
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


def _identity_for(i: int, account: str) -> dict[str, Any]:
    return build_event(
        event_type="identity.events",
        occurred_at=event_time(dt.datetime.now(dt.UTC)),
        payload={"account_id": account, "identity_event_type": "PASSWORD_CHANGE"},
        producer="trace-gateway@0.1.0",
        trace_id="0" * 32,
        correlation_id=f"idev_{i:032d}",
    )


def _one_account_per_partition(topic: str) -> list[str]:
    count = DECLARATION.topics[topic].partitions
    found: dict[int, str] = {}
    i = 0
    while len(found) < count:
        account = f"acct_{i:06d}"
        found.setdefault(java_partition(account.encode(), count), account)
        i += 1
    return [found[partition] for partition in range(count)]


# --------------------------------------------------------------------- tests ---


def test_every_released_topic_lands_in_bronze_byte_for_byte_and_is_conserved(
    broker: Broker,  # noqa: F811
    spark: Any,
    tmp_path: Path,
) -> None:
    topics = sorted(BRONZE_TOPICS)
    _fresh(broker, *topics)
    session = uuid.uuid4().hex
    items: list[tuple[str, dict[str, Any], Headers]] = [
        (topic, event, ()) for topic, event in _mixed_stream(24, start=dt.datetime.now(dt.UTC))
    ]
    items += [(TX_SCORED_V1, _scored(i), _session_headers(session, i)) for i in range(1, 9)]
    items += [(TX_AUTHORIZATION_V1, _authorization(i), ()) for i in range(6)]
    _publish(broker, items)
    _produce_raw(
        broker,
        TX_RAW_V1,
        [
            (b"acct_000000001", b"\xff\xfenot json", (("x", b"1"), ("x", b"2"), ("n", None))),
            (b"acct_000000002", b"", ()),
        ],
    )
    lake = LakeConfig.at(tmp_path / "lake")
    for topic in topics:
        handle = _ingest(spark, lake, broker, topic, max_offsets=7)
        progressed = record_progress(handle, set())
        expected = _broker_view(broker, topic)
        landed = _landed(spark, lake, topic)
        assert expected, f"{topic}: the test published nothing"
        assert sorted(_as_bronze_holds(row) for row in landed) == expected, topic
        assert sum(p.input_rows for p in progressed) == len(expected)
        assert {row.trust_tier for row in landed} == {"UNTRUSTED"}
        assert {row.checkpoint_id for row in landed} == {handle.opened.identity.app_id}
        report = _conservation(spark, lake, broker, topic)
        assert report.conserved, report.summary()
        assert {row.topic_id for row in landed} == {report.topic_id} == {report.broker_topic_id}
        high = _watermarks(broker, topic)
        assert {p: r.end for p, r in report.consumed.ranges.items()} == high
        assert {p: r.start for p, r in report.consumed.ranges.items()} == dict.fromkeys(high, 0)
        assert not report.started_after_trim
    raw = _landed(spark, lake, TX_RAW_V1)
    assert len({row.batch_id for row in raw}) >= 4, "26 records at 7 per batch is several batches"
    assert b"\xff\xfenot json" in {row.value for row in raw}
    assert (("x", b"1"), ("x", b"2"), ("n", None)) in {row.headers for row in raw}, "null kept"


def test_a_restart_from_checkpoint_neither_loses_nor_duplicates(
    broker: Broker,  # noqa: F811
    spark: Any,
    tmp_path: Path,
) -> None:
    _fresh(broker, TX_RAW_V1)
    at = dt.datetime.now(dt.UTC)
    _publish(broker, [(TX_RAW_V1, _tx(i, i % 5, at), ()) for i in range(30)])
    lake = LakeConfig.at(tmp_path / "lake")
    first = _ingest(spark, lake, broker, TX_RAW_V1, max_offsets=4)
    before = {
        (r.partition, r.offset): (r.batch_id, r.value) for r in _landed(spark, lake, TX_RAW_V1)
    }
    assert len(before) == 30

    _publish(broker, [(TX_RAW_V1, _tx(i, i % 5, at), ()) for i in range(30, 55)])
    # A crash after the sink committed the last batch and before Spark recorded it.
    last = max(SparkProgress.read(first.opened.directory).committed)
    (first.opened.directory / "commits" / str(last)).unlink()
    (first.opened.directory / "commits" / f".{last}.crc").unlink(missing_ok=True)

    second = _ingest(spark, lake, broker, TX_RAW_V1, max_offsets=4)
    assert second.opened.action is StartAction.RESUME
    assert second.opened.identity == first.opened.identity
    replayed = record_progress(second, set())
    assert replayed[0].batch_id == last, "Spark re-ran the unrecorded batch from its offsets"
    landed = _landed(spark, lake, TX_RAW_V1)
    assert sorted(_as_bronze_holds(r) for r in landed) == _broker_view(broker, TX_RAW_V1)
    assert len(landed) == 55 == len({(r.partition, r.offset) for r in landed})
    assert {
        key: (r.batch_id, r.value) for r in landed if (key := (r.partition, r.offset)) in before
    } == before, "the replayed batch was skipped by Delta, not written again"
    assert _conservation(spark, lake, broker, TX_RAW_V1).conserved

    # The checkpoint is lost: Bronze refuses rather than re-reading the topic from earliest.
    second.opened.directory.parent.rename(tmp_path / "lost-checkpoint")
    with pytest.raises(CheckpointRefusedError, match="no checkpoint exists"):
        _ingest(spark, lake, broker, TX_RAW_V1)
    assert len(_landed(spark, lake, TX_RAW_V1)) == 55


def test_a_poison_value_between_valid_records_is_stored_raw_and_blocks_nothing(
    broker: Broker,  # noqa: F811
    spark: Any,
    tmp_path: Path,
) -> None:
    _fresh(broker, TX_RAW_V1)
    account = 7
    key = f"acct_{account:09d}".encode()
    at = dt.datetime.now(dt.UTC)
    valid = [_tx(0, account, at), _tx(1, account, at)]
    poison = b'{"envelope": {"producer": "\xc3\x28'
    _publish(broker, [(TX_RAW_V1, valid[0], ())])
    _produce_raw(broker, TX_RAW_V1, [(key, poison, (("trace_id", b"not-a-trace-id"),))])
    _publish(broker, [(TX_RAW_V1, valid[1], ())])

    lake = LakeConfig.at(tmp_path / "lake")
    _ingest(spark, lake, broker, TX_RAW_V1)
    landed = _landed(spark, lake, TX_RAW_V1)
    partition = java_partition(key, DECLARATION.topics[TX_RAW_V1].partitions)
    assert [(r.partition, r.offset) for r in landed] == [(partition, o) for o in range(3)]
    assert [r.value for r in landed] == [
        canonical_bytes(valid[0]),
        poison,
        canonical_bytes(valid[1]),
    ]
    assert landed[1].headers == (("trace_id", b"not-a-trace-id"),)
    assert sorted(_as_bronze_holds(r) for r in landed) == _broker_view(broker, TX_RAW_V1)
    assert _conservation(spark, lake, broker, TX_RAW_V1).conserved


def test_trimmed_unread_offsets_stop_bronze_loudly_and_trimmed_read_ones_do_not(
    broker: Broker,  # noqa: F811
    spark: Any,
    tmp_path: Path,
) -> None:
    from pyspark.errors import StreamingQueryException

    _fresh(broker, TX_RAW_V1)
    account = 3
    partition = java_partition(
        f"acct_{account:09d}".encode(), DECLARATION.topics[TX_RAW_V1].partitions
    )
    at = dt.datetime.now(dt.UTC)

    def publish(first: int, count: int) -> None:
        _publish(
            broker, [(TX_RAW_V1, _tx(i, account, at), ()) for i in range(first, first + count)]
        )

    lake = LakeConfig.at(tmp_path / "lake")
    publish(0, 10)
    _ingest(spark, lake, broker, TX_RAW_V1)
    # Control: removing records Bronze has already read is retention, not loss.
    _trim(broker, TX_RAW_V1, partition, before=5)
    publish(10, 10)
    _ingest(spark, lake, broker, TX_RAW_V1)
    assert [r.offset for r in _landed(spark, lake, TX_RAW_V1)] == list(range(20))
    assert _conservation(spark, lake, broker, TX_RAW_V1).conserved

    # Loss: records Bronze has not read are removed before it reads them.
    publish(20, 10)
    _trim(broker, TX_RAW_V1, partition, before=25)
    handle = start_bronze_query(
        spark,
        lake,
        TX_RAW_V1,
        bootstrap_servers=broker.bootstrap,
        git_sha=SHA,
        dirty_worktree=False,
        now=dt.datetime.now(dt.UTC),
        trigger=Trigger(available_now=True),
    )
    with pytest.raises(StreamingQueryException) as failure:
        handle.query.awaitTermination()
    assert DATA_LOSS.search(str(failure.value)), str(failure.value)[:2000]
    assert [r.offset for r in _landed(spark, lake, TX_RAW_V1)] == list(range(20)), (
        "nothing after the loss was written, so no later offset can hide the hole"
    )
    report = _conservation(spark, lake, broker, TX_RAW_V1)
    assert report.conserved and report.consumed.ranges[partition] == OffsetRange(0, 20)

    # The only way on is a reset (critic finding A1). One starting above where v1 stopped would
    # skip 20-24 silently, and is refused before anything is published ...
    spec = BRONZE_TOPICS[TX_RAW_V1]

    def reset(start: str) -> checkpoints.CheckpointIdentity:
        return checkpoints.reset_checkpoint(
            spark,
            lake,
            spec.query,
            targets=[spec.table],
            sources=[KafkaSourceStart(TX_RAW_V1, start)],
            reason="Kafka data loss stopped the query",
            now=dt.datetime.now(dt.UTC),
        )

    ends = {str(p): r.end for p, r in report.consumed.ranges.items()}
    skipping = json.dumps({TX_RAW_V1: {**ends, str(partition): 25}})
    with pytest.raises(
        CheckpointRefusedError, match=rf"\[{partition}\] would start at 25, above 20"
    ):
        reset(skipping)
    # ... while one at earliest begins above the hole, where only the broker knows. v2 then reads
    # [25, 30) completely, and conservation reports the five offsets no version read, for good.
    assert reset("earliest").version == 2
    _ingest(spark, lake, broker, TX_RAW_V1)
    after = _conservation(spark, lake, broker, TX_RAW_V1)
    assert after.consumed.ranges[partition] == OffsetRange(25, 30)
    assert not after.conserved
    assert dict(after.skipped) == {partition: (OffsetRange(20, 25),)}, after.summary()
    assert [(s.checkpoint_version, s.conserved) for s in after.superseded] == [(1, True)]
    assert not after.started_after_trim, "v1, the first to read the partition, began at 0"


def test_a_recreated_topic_is_refused_where_spark_alone_silently_skips_its_first_records(
    broker: Broker,  # noqa: F811
    spark: Any,
    tmp_path: Path,
) -> None:
    _fresh(broker, TX_RAW_V1)
    account = 1
    at = dt.datetime.now(dt.UTC)
    _publish(broker, [(TX_RAW_V1, _tx(i, account, at), ()) for i in range(8)])
    lake = LakeConfig.at(tmp_path / "lake")
    _ingest(spark, lake, broker, TX_RAW_V1)
    assert len(_landed(spark, lake, TX_RAW_V1)) == 8

    _fresh(broker, TX_RAW_V1)  # deleted and recreated, then refilled past the checkpoint's offset
    new = [_tx(100 + i, account, at) for i in range(12)]
    _publish(broker, [(TX_RAW_V1, event, ()) for event in new])
    with pytest.raises(CheckpointRefusedError, match="deleted and recreated"):
        _ingest(spark, lake, broker, TX_RAW_V1)
    assert len(_landed(spark, lake, TX_RAW_V1)) == 8

    # What the refusal prevents: the same checkpoint, started without the topic-identity guard.
    spec = BRONZE_TOPICS[TX_RAW_V1]
    opened = checkpoints.open_checkpoint(
        spark,
        lake,
        spec.query,
        targets=[spec.table],
        sources=[KafkaSourceStart(TX_RAW_V1, "earliest")],
        git_sha=SHA,
        dirty_worktree=False,
        now=dt.datetime.now(dt.UTC),
    )
    recorded = bronze.read_topic_identity(opened.directory)
    assert recorded is not None
    query = bronze._start_stream(
        spark,
        spec,
        opened,
        bootstrap_servers=broker.bootstrap,
        trigger=Trigger(available_now=True),
        max_offsets_per_trigger=None,
        recorded=recorded,
        current_topic=None,  # neither guard: what Spark alone does
    )
    query.awaitTermination()
    assert query.exception() is None, "failOnDataLoss saw nothing wrong"
    values = {r.value for r in _landed(spark, lake, TX_RAW_V1)}
    published = [canonical_bytes(event) for event in new]
    assert all(value in values for value in published[8:])
    assert not any(value in values for value in published[:8]), (
        "Spark resumed the new topic at offset 8: its first eight records never reached Bronze"
    )
    alone = check_conservation(spark, lake, TX_RAW_V1, broker_topic_id=None)
    assert alone.conserved, "and offsets alone cannot show it"
    compared = _conservation(spark, lake, broker, TX_RAW_V1)
    assert not compared.conserved, "the broker's topic id can"
    assert "deleted and recreated" in " ".join(compared.problems), compared.summary()


def test_a_topic_recreated_under_a_running_query_stops_it_and_lands_nothing_of_the_new_topic(
    broker: Broker,  # noqa: F811
    spark: Any,
    tmp_path: Path,
) -> None:
    """Critic finding B3, live. The start-time guard cannot see a topic deleted, recreated and
    refilled past the checkpoint's offset while the query runs. Whatever notices first -- the
    per-batch topic check, the topic missing mid-recreation, or Spark's own data-loss detection --
    the query must stop loudly, and no record of the new topic may land under the old topic's id."""
    from pyspark.errors import StreamingQueryException

    _fresh(broker, TX_RAW_V1)
    account = 2
    at = dt.datetime.now(dt.UTC)
    _publish(broker, [(TX_RAW_V1, _tx(i, account, at), ()) for i in range(8)])
    lake = LakeConfig.at(tmp_path / "lake")
    handle = start_bronze_query(
        spark,
        lake,
        TX_RAW_V1,
        bootstrap_servers=broker.bootstrap,
        git_sha=SHA,
        dirty_worktree=False,
        now=dt.datetime.now(dt.UTC),
        trigger=Trigger(interval_s=30.0),
    )
    new = [_tx(100 + i, account, at) for i in range(12)]
    try:
        # Batch 0 done, and recorded by Spark, so its after-write check has run.
        _wait_for(
            lambda: 0 in SparkProgress.read(handle.opened.directory).committed,
            "Spark to record batch 0",
            timeout_s=120,
        )
        assert len(_landed(spark, lake, TX_RAW_V1)) == 8
        _fresh(broker, TX_RAW_V1)  # well inside the 30 s before the next trigger
        _publish(broker, [(TX_RAW_V1, event, ()) for event in new])
        with pytest.raises(StreamingQueryException) as failure:
            handle.query.awaitTermination(120)  # returns False, and fails here, if it ran on
    finally:
        if handle.query.isActive:
            handle.query.stop()
    assert LOUD_RECREATION.search(str(failure.value)), str(failure.value)[:2000]
    values = {r.value for r in _landed(spark, lake, TX_RAW_V1)}
    assert len(values) == 8 and not values & {canonical_bytes(event) for event in new}
    with pytest.raises(CheckpointRefusedError, match="deleted and recreated"):
        _ingest(spark, lake, broker, TX_RAW_V1)


def _dsn(user_env: str, password_env: str) -> str:
    return "postgresql://{u}:{p}@{h}:{port}/{db}".format(
        u=os.environ.get(user_env, ""),
        p=os.environ.get(password_env, ""),
        h=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5442"),
        db=os.environ.get("POSTGRES_DB", "tracex"),
    )


@pytest.fixture
def ledger_connections() -> Iterator[tuple[Any, Any]]:
    psycopg = pytest.importorskip("psycopg")
    opened: list[Any] = []
    try:
        for user, password in (
            ("POSTGRES_SUPERUSER", "POSTGRES_SUPERUSER_PASSWORD"),
            ("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD"),
            ("TRACE_STREAM_DB_USER", "TRACE_STREAM_DB_PASSWORD"),
        ):
            opened.append(psycopg.connect(_dsn(user, password), autocommit=True))
        opened[2].execute("SELECT 1 FROM app.producer_sessions LIMIT 1")
    except Exception as exc:  # pragma: no cover - environment dependent
        for connection in opened:
            connection.close()
        pytest.skip(
            f"SKIPPED (NOT PASSED): no migrated PostgreSQL reachable as the owner, trace_app and "
            f"trace_stream ({exc}). Run `make up && make migrate`; the session ledger is never "
            f"mocked."
        )
    owner, app, stream = opened
    try:
        yield app, stream
    finally:
        owner.execute(
            "DELETE FROM app.producer_sessions WHERE instance_id LIKE %s", (f"{INSTANCE_PREFIX}%",)
        )
        for connection in opened:
            connection.close()


def test_bronze_coverage_reports_exactly_the_gaps_the_rule_defines(
    broker: Broker,  # noqa: F811
    spark: Any,
    tmp_path: Path,
    ledger_connections: tuple[Any, Any],
) -> None:
    app, stream = ledger_connections
    _fresh(broker, TX_SCORED_V1, IDENTITY_EVENTS_V1)
    ledger = PostgresSessionLedger(app)
    tag = uuid.uuid4().hex[:8]
    ids = {name: f"bronze-coverage-{name}-{tag}" for name in ("hole", "whole", "open")}
    for session_id in ids.values():
        ledger.open(
            session_id=session_id,
            producer="trace-gateway@test",
            instance_id=f"{INSTANCE_PREFIX}{tag}",
        )
    at = dt.datetime.now(dt.UTC)
    _publish(
        broker,
        [
            (TX_SCORED_V1, _scored(1), _session_headers(ids["hole"], 1)),
            (TX_SCORED_V1, _scored(2), _session_headers(ids["hole"], 2)),
            (TX_SCORED_V1, _scored(4), _session_headers(ids["hole"], 4)),  # 3 never published
            (TX_SCORED_V1, _scored(5), _session_headers(ids["whole"], 1)),
            (IDENTITY_EVENTS_V1, _identity_event(6), _session_headers(ids["whole"], 2)),
            (TX_SCORED_V1, _scored(7), _session_headers(ids["open"], 1)),
            (TX_SCORED_V1, _scored(8), _session_headers(ids["open"], 2)),
            (TX_SCORED_V1, _scored(9), _session_headers(f"bronze-coverage-unknown-{tag}", 1)),
            (TX_SCORED_V1, _scored(10), ()),  # the gateway's topic, no session headers
            (IDENTITY_EVENTS_V1, _identity_event(11), ()),  # the gateway's identity event, none
            (IDENTITY_EVENTS_V1, _identity(12, 1, at), ()),  # another producer's identity event
        ],
    )
    assert ledger.close(ids["hole"], last_seq=4)
    assert ledger.close(ids["whole"], last_seq=2)
    open_row = ledger.get(ids["open"])
    assert open_row is not None and open_row.closed_at is None

    lake = LakeConfig.at(tmp_path / "lake")
    for topic in (TX_SCORED_V1, IDENTITY_EVENTS_V1):
        _ingest(spark, lake, broker, topic)
    result = assess_bronze_coverage(spark, lake, stream, clock_margin_s=1.0)

    sessions = result.coverage.sessions
    hole = sessions[ids["hole"]]
    assert hole.closed and hole.certified_through == 2
    (gap,) = hole.gaps
    assert gap.missing == (3,)
    whole = sessions[ids["whole"]]
    assert whole.covered and whole.certified_through == 2, "one session across both topics"
    still_open = sessions[ids["open"]]
    assert not still_open.closed and still_open.certified_through == 2
    (tail,) = still_open.gaps
    fence, margin = dt.timedelta(seconds=LEASE_S + TAKEOVER_MARGIN_S), dt.timedelta(seconds=1.0)
    if open_row.heartbeat_at + fence + 2 * margin < result.ledger_read_at:
        assert tail.end == open_row.heartbeat_at + fence + margin, "the writer's lease ran out"
    else:
        assert tail.end is None, "a heartbeat after the read may exist, so the tail is open"
    assert len(result.coverage.unknown) == 3, "unknown session, and two gateway records unheaded"
    assert (result.observations, result.not_observations) == (10, 1)
    assert set(result.table_versions) == {TX_SCORED_V1, IDENTITY_EVENTS_V1}
    closed_at = ledger.get(ids["whole"])
    assert closed_at is not None and closed_at.closed_at is not None
    assert result.ledger_read_at >= closed_at.closed_at
    # Every record went to one partition of each topic, so the other partitions bound nothing.
    assert result.bronze_high_water is None and result.log_read_through is None
    assert result.beyond_high_water == 0 and result.high_water_withheld == ()
    assert not result.coverage.vouches(at, at), "without a mark nothing is vouched for"


def test_bronze_coverage_vouches_before_the_high_water_once_every_partition_has_arrived(
    broker: Broker,  # noqa: F811
    spark: Any,
    tmp_path: Path,
    ledger_connections: tuple[Any, Any],
) -> None:
    """Critic C2, A2 and B1: the mark exists only once every partition the checkpoints read holds
    a row. Rows at or after it are left out, and writes that arrived before it are vouched for."""
    app, stream = ledger_connections
    _fresh(broker, TX_SCORED_V1, IDENTITY_EVENTS_V1)
    ledger = PostgresSessionLedger(app)
    tag = uuid.uuid4().hex[:8]
    early, late = (f"bronze-high-water-{name}-{tag}" for name in ("early", "late"))
    instance_id = f"{INSTANCE_PREFIX}{tag}"
    ledger.open(session_id=early, producer="trace-gateway@test", instance_id=instance_id)
    _publish(
        broker,
        [
            (TX_SCORED_V1, _scored_for(1, "acct_000001"), _session_headers(early, 1)),
            (IDENTITY_EVENTS_V1, _identity_for(2, "acct_000001"), _session_headers(early, 2)),
            (TX_SCORED_V1, _scored_for(3, "acct_000001"), _session_headers(early, 3)),
        ],
    )
    assert ledger.close(early, last_seq=3)
    time.sleep(3.0)  # the late session opens, and its rows arrive, past the clock margin
    ledger.open(session_id=late, producer="trace-gateway@test", instance_id=instance_id)
    late_items: list[tuple[str, dict[str, Any], Headers]] = []
    for topic, build in ((TX_SCORED_V1, _scored_for), (IDENTITY_EVENTS_V1, _identity_for)):
        for account in _one_account_per_partition(topic):
            seq = len(late_items) + 1
            late_items.append((topic, build(100 + seq, account), _session_headers(late, seq)))
    _publish(broker, late_items)

    lake = LakeConfig.at(tmp_path / "lake")
    for topic in (TX_SCORED_V1, IDENTITY_EVENTS_V1):
        _ingest(spark, lake, broker, topic)
    result = assess_bronze_coverage(spark, lake, stream, clock_margin_s=1.0)

    newest: dict[tuple[int, str], int] = {}
    stamps: dict[str, list[dt.datetime]] = {early: [], late: []}
    rows = 0
    for topic in (TX_SCORED_V1, IDENTITY_EVENTS_V1):
        for row in _landed(spark, lake, topic):
            rows += 1
            key = (row.partition, topic)
            newest[key] = max(newest.get(key, 0), row.timestamp_us)
            assert row.value is not None and row.headers is not None
            session_id = (dict(row.headers)[SESSION_HEADER] or b"").decode()
            stamp = json.loads(row.value)["envelope"]["ingested_at"]
            stamps[session_id].append(dt.datetime.fromisoformat(stamp))
    assert len(newest) == sum(
        DECLARATION.topics[t].partitions for t in (TX_SCORED_V1, IDENTITY_EVENTS_V1)
    )
    mark = logged_at(min(newest.values()))
    assert result.high_water_withheld == ()
    assert result.bronze_high_water == mark
    assert result.log_read_through == mark - dt.timedelta(microseconds=1)
    assert result.beyond_high_water == len(late_items) == rows - 3, (
        "every late row is at or past it"
    )
    assert result.observations == 3
    verdict = result.coverage.sessions[early]
    assert verdict.covered and verdict.certified_through == 3
    first, last = min(stamps[early]), max(stamps[early])
    assert last < result.coverage.through
    # `vouches`, restricted to this test's sessions: the ledger read sees every gateway session.
    ours = [gap for gap in result.coverage.gaps if gap.session_id in (early, late, None)]
    assert not any(gap.overlaps(first, last) for gap in ours), "the early writes are vouched for"
    late_verdict = result.coverage.sessions[late]
    assert late_verdict.certified_through == 0, "its rows are past the mark, so treated as unread"
    assert all(any(gap.contains(s) for gap in late_verdict.gaps) for s in stamps[late])
