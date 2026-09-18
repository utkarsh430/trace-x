"""Bronze on a live Spark session and real Delta tables, without a broker (Phase 3 Step 5).

What a pure test cannot show: that the declared row is exactly what Spark's Kafka connector
delivers, that every Bronze table is created at the minimum protocol and refuses rewrites, that a
micro-batch lands byte for byte through the checkpoint convention, and that the Spark aggregation
and extraction the conservation check and the coverage adapter run agree with their pure
definitions.
The broker-backed half is `tests/integration/test_bronze_kafka.py`.

Marker `stream`: skipped loudly when the toolchain does not match its pins, and a `-m stream`
session in which no stream test executed FAILS (tests/conftest.py).
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from trace_core.contracts.topics import IDENTITY_EVENTS_V1, TX_RAW_V1, TX_SCORED_V1
from trace_core.domain.errors import CheckpointRefusedError, LakeContractError
from trace_core.observation.coverage import Observed
from trace_core.observation.log import SEQ_HEADER, SESSION_HEADER
from trace_core.stream import checkpoints
from trace_core.stream.bronze import (
    BRONZE_TOPICS,
    KAFKA_SOURCE_COLUMNS,
    bronze_declaration,
    bronze_frame,
    bronze_schema,
)
from trace_core.stream.bronze_conservation import (
    ConsumedRanges,
    OffsetRange,
    PartitionVerdict,
    Record,
    judge_conservation,
    partition_rows_from_records,
    read_partition_rows,
)
from trace_core.stream.bronze_coverage import BronzeRecord, _bronze_records, observation_from_record
from trace_core.stream.checkpoints import KafkaSourceStart
from trace_core.stream.lake import AppId, LakeConfig
from trace_core.stream.tables import (
    BASELINE_FEATURES,
    DECLARED_RETENTION,
    CommitProvenance,
    create_table,
    snapshot_facts,
)

pytestmark = pytest.mark.stream

SHA = hashlib.sha1(b"trace-x bronze stream tests", usedforsecurity=False).hexdigest()
NOW = datetime(2026, 9, 15, 8, 0, tzinfo=UTC)
AT = datetime(2026, 9, 15, 7, 59, 58, 123_000, tzinfo=UTC)
AT_US = (AT - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)
TOPIC_ID = "q1Z8-topic-id-one"
OTHER_TOPIC_ID = "r2Y9-topic-id-two"
SPARK_TO_BRONZE = {
    "topic": "kafka_topic",
    "partition": "kafka_partition",
    "offset": "kafka_offset",
    "timestamp": "kafka_timestamp",
    "timestampType": "kafka_timestamp_type",
    "key": "kafka_key",
    "value": "kafka_value",
    "headers": "kafka_headers",
}


@pytest.fixture(scope="module")
def spark() -> Iterator[Any]:
    from trace_core.stream.session import build_session

    session = build_session("trace-x-bronze-tables")
    try:
        yield session
    finally:
        session.stop()


def _kafka_schema(spark: Any) -> Any:
    """The connector's own schema. Defining a Kafka source contacts no broker."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "127.0.0.1:9")
        .option("subscribe", "bronze-schema-probe")
        .option("includeHeaders", "true")
        .load()
        .schema
    )


def _create(spark: Any, lake: LakeConfig, topic: str) -> Any:
    spec = BRONZE_TOPICS[topic]
    return create_table(
        spark, bronze_declaration(topic), lake, CommitProvenance(SHA, False, spec.query)
    )


def _bytes(value: Any) -> bytes | None:
    return None if value is None else bytes(value)


def _bronze_row(
    topic: str,
    partition: int,
    offset: int,
    writer: str,
    *,
    headers: Sequence[tuple[str, bytes | None]] | None = None,
    value: bytes | None = b"{}",
    topic_id: str = TOPIC_ID,
) -> tuple[Any, ...]:
    """A Bronze row written directly, as a fixture: only the code under test reads it back."""
    return (
        topic, topic_id, partition, offset, AT, 1, b"k", value, headers, "UNTRUSTED", NOW, 0, writer
    )  # fmt: skip


def test_the_declared_row_is_exactly_what_the_kafka_connector_delivers(spark: Any) -> None:
    connector = {field.name: field for field in _kafka_schema(spark).fields}
    declared = {field.name: field for field in bronze_schema().fields}
    assert set(connector) == set(KAFKA_SOURCE_COLUMNS) == set(SPARK_TO_BRONZE)
    for source, target in SPARK_TO_BRONZE.items():
        assert connector[source].dataType == declared[target].dataType, source


def test_every_bronze_table_is_created_at_the_minimum_protocol_and_refuses_rewrites(
    spark: Any, tmp_path: Path
) -> None:
    lake = LakeConfig.at(tmp_path / "lake")
    for topic in BRONZE_TOPICS:
        live = _create(spark, lake, topic)
        assert (live.min_reader_version, live.min_writer_version) == (1, 2)
        assert live.table_features == BASELINE_FEATURES
        assert dict(live.properties) == {**DECLARED_RETENTION, "delta.appendOnly": "true"}
        assert _create(spark, lake, topic).table_id == live.table_id, "created once, then checked"
    identifier = BRONZE_TOPICS[TX_RAW_V1].table.path_identifier(lake)
    with pytest.raises(Exception, match="DELTA_CANNOT_MODIFY_APPEND_ONLY"):
        spark.sql(f"DELETE FROM {identifier} WHERE true")
    with pytest.raises(Exception, match="DELTA_CANNOT_MODIFY_APPEND_ONLY"):
        spark.sql(f"UPDATE {identifier} SET kafka_value = NULL")


def test_a_micro_batch_lands_byte_for_byte_through_the_checkpoint_and_is_never_parsed(
    spark: Any, tmp_path: Path
) -> None:
    lake = LakeConfig.at(tmp_path / "lake")
    spec = BRONZE_TOPICS[TX_RAW_V1]
    _create(spark, lake, TX_RAW_V1)
    opened = checkpoints.open_checkpoint(
        spark,
        lake,
        spec.query,
        targets=[spec.table],
        sources=[KafkaSourceStart(TX_RAW_V1, "earliest")],
        git_sha=SHA,
        dirty_worktree=False,
        now=NOW,
    )
    # key, value, topic, partition, offset, timestamp, timestampType, headers
    delivered = [
        (b"acct_1", b'{"envelope": {}}', TX_RAW_V1, 0, 0, AT, 1, [("trace_id", b"ab")]),
        (b"acct_1", b"\xff\xfe{not json", TX_RAW_V1, 0, 1, AT, 1, None),
        (None, b"", TX_RAW_V1, 0, 2, AT, 1, []),
        (b"acct_1", None, TX_RAW_V1, 0, 3, AT, 1, [("x", b"1"), ("x", b"2"), ("x", None)]),
    ]  # fmt: skip
    frame = spark.createDataFrame(delivered, _kafka_schema(spark))
    app_id = opened.identity.app_id
    opened.append(
        bronze_frame(frame, batch_id=0, checkpoint_id=app_id, topic_id=TOPIC_ID),
        batch_id=0,
        target=spec.table,
    )

    rows = (
        spark.read.format("delta")
        .load(str(spec.table.local_path(lake)))
        .selectExpr("*", "unix_micros(kafka_timestamp) AS ts_us")
        .orderBy("kafka_offset")
        .collect()
    )
    landed = [
        (
            _bytes(r["kafka_key"]),
            _bytes(r["kafka_value"]),
            r["kafka_topic"],
            r["kafka_partition"],
            r["kafka_offset"],
            r["ts_us"],
            r["kafka_timestamp_type"],
            None
            if r["kafka_headers"] is None
            else [(h["key"], _bytes(h["value"])) for h in r["kafka_headers"]],
        )
        for r in rows
    ]
    assert landed == [(k, v, t, p, o, AT_US, tt, h) for k, v, t, p, o, _, tt, h in delivered]
    assert {
        (r["trust_tier"], r["bronze_batch_id"], r["bronze_checkpoint_id"], r["kafka_topic_id"])
        for r in rows
    } == {("UNTRUSTED", 0, app_id, TOPIC_ID)}
    evidence = checkpoints.read_target_evidence(spark, lake, spec.table)
    assert evidence.transactions == {app_id: 0}
    with pytest.raises(LakeContractError, match="includeHeaders"):
        bronze_frame(frame.drop("headers"), batch_id=1, checkpoint_id=app_id, topic_id=TOPIC_ID)
    with pytest.raises(CheckpointRefusedError, match="identifies no topic"):
        bronze_frame(frame, batch_id=1, checkpoint_id=app_id, topic_id="AAAAAAAAAAAAAAAAAAAAAA")


def test_conservation_statistics_computed_in_spark_equal_the_reference(
    spark: Any, tmp_path: Path
) -> None:
    lake = LakeConfig.at(tmp_path / "lake")
    spec = BRONZE_TOPICS[TX_RAW_V1]
    _create(spark, lake, TX_RAW_V1)
    current = str(AppId.new(spec.query, 2))
    old = str(AppId.new(spec.query, 1))
    records: list[Record] = [
        *((0, o, current, TOPIC_ID) for o in range(10) if o != 4),  # 4 missing
        (0, 7, current, TOPIC_ID),  # 7 twice
        (0, 8, old, OTHER_TOPIC_ID),  # 8 again, but of a recreated topic: not a duplicate
        *((1, o, current, TOPIC_ID) for o in range(6)),  # 5 is beyond [0, 5)
        *((2, o, old, TOPIC_ID) for o in (3, 4, 5)),  # an earlier checkpoint's rows
        (2, 5, current, TOPIC_ID),  # ... and 5 again
        (3, 0, current, OTHER_TOPIC_ID),  # under an id this checkpoint never read; 1 missing
        (4, 0, current, TOPIC_ID),  # a partition with no consumed range
    ]
    ranges = {
        0: OffsetRange(0, 10),
        1: OffsetRange(0, 5),
        2: OffsetRange(5, 6),
        3: OffsetRange(0, 2),
    }
    rows = [_bronze_row(TX_RAW_V1, p, o, w, topic_id=t) for p, o, w, t in records]
    rows.append(_bronze_row(TX_SCORED_V1, 0, 0, current))  # another topic's row
    path = spec.table.local_path(lake)
    spark.createDataFrame(rows, bronze_schema()).write.format("delta").mode("append").save(
        str(path)
    )
    facts = snapshot_facts(spark, path)
    assert facts is not None

    stats, foreign = read_partition_rows(
        spark,
        path,
        version=facts.version,
        topic=TX_RAW_V1,
        checkpoint_id=current,
        ranges=ranges,
        topic_id=TOPIC_ID,
    )
    assert stats == partition_rows_from_records(
        records, checkpoint_id=current, ranges=ranges, topic_id=TOPIC_ID
    )
    assert foreign == 1
    report = judge_conservation(
        ConsumedRanges(TX_RAW_V1, current, 1, 1, 1, ranges),
        stats,
        table=str(spec.table),
        table_version=facts.version,
        foreign_topic_rows=foreign,
    )
    assert not report.conserved
    # Derived by hand from `records`: missing, duplicates, out of range.
    assert report.partitions == (
        PartitionVerdict(0, OffsetRange(0, 10), 1, 1, 0),
        PartitionVerdict(1, OffsetRange(0, 5), 0, 0, 1),
        PartitionVerdict(2, OffsetRange(5, 6), 0, 1, 0),
        PartitionVerdict(3, OffsetRange(0, 2), 1, 0, 0, other_topic_id=1),
        PartitionVerdict(4, None, 0, 0, 1),
    )


def test_coverage_rows_read_in_spark_equal_the_rows_that_were_written(
    spark: Any, tmp_path: Path
) -> None:
    lake = LakeConfig.at(tmp_path / "lake")
    stamp = "2026-09-15T07:59:58.083Z"
    stamped = datetime(2026, 9, 15, 7, 59, 58, 83_000, tzinfo=UTC)

    def envelope(producer: str) -> bytes:
        return (
            b'{"envelope": {"producer": "' + producer.encode() + b'", "ingested_at": "'
            + stamp.encode() + b'"}, "payload": {}}'
        )  # fmt: skip

    gateway, generator = envelope("trace-gateway@0.1.0"), envelope("data.generator@1.0.0")
    not_utf8, too_deep = b"\xff\xfe", b"[" * 100_000
    session, seq = SESSION_HEADER, SEQ_HEADER
    scored, identity = TX_SCORED_V1, IDENTITY_EVENTS_V1
    one = [("trace_id", b"t"), (session, b"s1"), (seq, b"1")]
    ambiguous = [(session, b"s1"), (session, b"s2"), (seq, None)]
    two = [(session, b"s1"), (seq, b"2")]
    written = {
        scored: [
            _bronze_row(scored, 0, 0, "w", headers=one, value=gateway),
            _bronze_row(scored, 0, 1, "w", headers=None),
            _bronze_row(scored, 0, 2, "w", headers=ambiguous, value=not_utf8),
        ],
        identity: [
            _bronze_row(identity, 1, 0, "w", headers=two, value=gateway),
            _bronze_row(identity, 1, 1, "w", headers=None, value=gateway),
            _bronze_row(identity, 1, 2, "w", headers=[("trace_id", b"t")], value=generator),
            _bronze_row(identity, 1, 3, "w", headers=None, value=too_deep),
            _bronze_row(identity, 1, 4, "w", headers=None, value=None),
        ],
    }
    gw, gen = "trace-gateway@0.1.0", "data.generator@1.0.0"
    expected = {
        scored: [
            BronzeRecord(scored, 0, 0, AT_US, 1, (b"s1",), (b"1",), gw, stamp, TOPIC_ID),
            BronzeRecord(scored, 0, 1, AT_US, 1, (), (), None, None, TOPIC_ID),
            BronzeRecord(scored, 0, 2, AT_US, 1, (b"s1", b"s2"), (None,), None, None, TOPIC_ID),
        ],
        identity: [
            BronzeRecord(identity, 1, 0, AT_US, 1, (b"s1",), (b"2",), gw, stamp, TOPIC_ID),
            BronzeRecord(identity, 1, 1, AT_US, 1, (), (), gw, stamp, TOPIC_ID),
            BronzeRecord(identity, 1, 2, AT_US, 1, (), (), gen, stamp, TOPIC_ID),
            BronzeRecord(identity, 1, 3, AT_US, 1, (), (), None, None, TOPIC_ID),
            BronzeRecord(identity, 1, 4, AT_US, 1, (), (), None, None, TOPIC_ID),
        ],
    }
    for topic, rows in written.items():
        _create(spark, lake, topic)
        path = BRONZE_TOPICS[topic].table.local_path(lake)
        spark.createDataFrame(rows, bronze_schema()).write.format("delta").mode("append").save(
            str(path)
        )
        facts = snapshot_facts(spark, path)
        assert facts is not None
        got = sorted(_bronze_records(spark, lake, topic, facts.version), key=lambda r: r.offset)
        assert got == expected[topic]
    observed = [observation_from_record(r) for topic in written for r in expected[topic]]
    assert observed == [
        Observed("s1", 1, AT, stamped),
        Observed(None, None, AT, AT),  # no stamp: a gap at its arrival
        Observed(None, None, AT, AT),  # not UTF-8, so no stamp, extracted as null in Spark
        Observed("s1", 2, AT, stamped),
        Observed(None, None, AT, stamped),
        None,
        None,  # nested too deep to parse: null in Spark, never parsed on the driver
        None,
    ]
