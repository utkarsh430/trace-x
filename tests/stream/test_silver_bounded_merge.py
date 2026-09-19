"""Silver's bounded MERGEs against real Delta (ADR-0053 Amendment 1).

- Exact deduplication is unchanged on a multi-file canonical table: a retry days late in the oldest
  file is a duplicate, a conflicting delivery with a distant `occurred_at` is a conflict, and a
  replayed batch changes nothing.
- A `tx.scored.v1` supersede whose replaced rows sit at both ends of the batch's range converges.
- The MERGEs read only the files the bound names. Proven by removing every other data file from
  disk: a MERGE that read one would fail with a missing file, as the unbounded control does.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import pytest
from services.gateway.pipeline import ScoringPipeline
from tests.stream.test_silver_tables import (
    SHA,
    T0,
    _actual,
    _append_bronze,
    _authorization,
    _crash_before_spark_commit,
    _create_bronze,
    _expected,
    _pipeline,
    _row,
    _rows,
    _run_silver,
    _scored,
    _snapshot,
)

from trace_core.contracts import authorization
from trace_core.contracts.api.transaction import TransactionRequest
from trace_core.contracts.canonical_json import canonical_bytes
from trace_core.contracts.topics import TX_AUTHORIZATION_V1, TX_SCORED_V1
from trace_core.stream import silver_rules as rules
from trace_core.stream.lake import LakeConfig
from trace_core.stream.silver import (
    OccurredRange,
    SilverPruningError,
    _canonical_merge,
    batch_merge_bounds,
    canonical_schema,
    late_events_merge_condition,
    late_events_schema,
    shared_declarations,
)
from trace_core.stream.silver_conservation import check_silver_conservation
from trace_core.stream.tables import CommitProvenance, create_table

pytestmark = pytest.mark.stream

EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
MISSING_FILE = r"FAILED_READ_FILE\.FILE_NOT_EXIST"


@pytest.fixture(scope="module")
def spark() -> Iterator[Any]:
    from trace_core.stream.session import build_session

    session = build_session("trace-x-silver-bounded-merge")
    try:
        yield session
    finally:
        session.stop()


def _us(moment: dt.datetime) -> int:
    return (moment - EPOCH) // dt.timedelta(microseconds=1)


def _authorization_at(transaction: int, occurred: dt.datetime, outcome: str) -> bytes:
    """A gateway outcome of `transaction` claiming another event time: a conflicting delivery."""
    occurred_ms = _us(occurred) // 1_000
    return canonical_bytes(
        authorization.build_event(
            transaction_id=f"tx_{transaction:012d}",
            account_id="acct_000001",
            authorization_outcome=outcome,
            decided_ms=occurred_ms + 250,
            transaction_occurred_ms=occurred_ms,
            transaction_occurred_at=authorization.iso_millis(occurred_ms),
            producer="trace-gateway@0.1.0",
            trace_id=uuid.uuid4().hex,
            correlation_id=f"tx_{transaction:012d}",
            ingested_ms=occurred_ms + 260,
        )
    )


def _data_files(table: Path) -> list[Path]:
    return sorted(p for p in table.rglob("*.parquet") if "_delta_log" not in p.parts)


def _file_of(spark: Any, table: Path) -> dict[str, Path]:
    from pyspark.sql import functions as F  # noqa: N812

    frame = spark.read.format("delta").load(str(table))
    return {
        str(row["silver_identity"]): Path(unquote(urlparse(row["file"]).path))
        for row in frame.select("silver_identity", F.input_file_name().alias("file")).collect()
    }


def _version(spark: Any, table: Path) -> int:
    from delta.tables import DeltaTable

    (entry,) = DeltaTable.forPath(spark, str(table)).history(1).collect()
    return int(entry["version"])


def _last_metrics(spark: Any, table: Path) -> dict[str, str]:
    from delta.tables import DeltaTable

    (entry,) = DeltaTable.forPath(spark, str(table)).history(1).collect()
    assert entry["operation"] == "MERGE", entry["operation"]
    return dict(entry["operationMetrics"])


def _file_count(spark: Any, table: Path) -> int:
    from pyspark.sql import functions as F  # noqa: N812

    frame = spark.read.format("delta").load(str(table))
    return int(frame.select(F.input_file_name()).distinct().count())


# ------------------------------------------------------------- exact dedup ---


def test_dedup_stays_exact_on_a_multi_file_table_with_distant_retries_and_conflicts(
    spark: Any, tmp_path: Path
) -> None:
    """A retry of the oldest row, arriving days late, is a duplicate; a delivery of a stored
    identity claiming an `occurred_at` a day away is a conflict, found by identity; a replay of the
    batch, whose late row is re-derived from the bounded read, changes nothing."""
    lake = LakeConfig.at(tmp_path / "lake")
    topic = TX_AUTHORIZATION_V1
    _create_bronze(spark, lake, topic)
    committed: dict[str, rules.Canonical] = {}
    expected: dict[tuple[int, int], str] = {}
    day = dt.timedelta(days=1)
    batches = [
        [_row(0, 0, T0 + dt.timedelta(seconds=2), _authorization(1))],
        [_row(0, 1, T0 + dt.timedelta(seconds=100_001), _authorization(100_000))],
        [
            _row(0, 2, T0 + dt.timedelta(seconds=200_001), _authorization(200_000)),
            _row(1, 0, T0 + dt.timedelta(seconds=200_002), _authorization(200_001)),
        ],
        [
            _row(0, 3, T0 + 4 * day, _authorization(1)),  # a retry, days late, of file one
            _row(1, 1, T0 + 4 * day, _authorization(200_000)),  # a retry of file three
            _row(0, 4, T0 + 4 * day, _authorization_at(100_000, T0 + dt.timedelta(seconds=5),
                                                       "APPROVED")),  # a distant conflict
            _row(1, 2, T0 + dt.timedelta(seconds=300_700), _authorization(300_000)),  # late, new
        ],
    ]  # fmt: skip
    for index, rows in enumerate(batches):
        _append_bronze(spark, lake, topic, rows, batch=index)
        expected.update(_expected(topic, rows, committed))
        _run_silver(spark, lake, topic)
        assert _actual(spark, lake, topic) == expected, index

    spec = rules.silver_topic(topic)
    table = spec.table.local_path(lake)
    assert _file_count(spark, table) >= 4, "the canonical table spans several files"
    assert expected[(0, 3)] == "duplicate" and expected[(1, 1)] == "duplicate"
    assert expected[(0, 4)] == "conflict" and expected[(1, 2)] == "canonical"
    identities = [str(r["silver_identity"]) for r in _rows(spark, lake, spec.table)]
    assert len(identities) == len(set(identities)) == 5
    late = _rows(spark, lake, rules.LATE_EVENTS, topic)
    assert [(r["kafka_partition"], r["kafka_offset"]) for r in late] == [(1, 2)]
    report = check_silver_conservation(spark, lake, topic)
    assert report.conserved, report.summary()

    before = _snapshot(spark, lake, topic)
    _crash_before_spark_commit(lake, topic)
    _run_silver(spark, lake, topic)
    assert _snapshot(spark, lake, topic) == before
    assert check_silver_conservation(spark, lake, topic).conserved


# -------------------------------------------------------------- supersede ---


def _score_at(pipeline: ScoringPipeline, transaction: str, occurred: dt.datetime) -> Any:
    request = TransactionRequest.model_validate(
        {
            "transaction_id": transaction,
            "account_id": "acct_000901",
            "amount_minor": 7_500,
            "currency": "GBP",
            "occurred_at": occurred.isoformat().replace("+00:00", "Z"),
        }
    )
    return pipeline.score(request, now=occurred)


def test_a_supersede_at_both_edges_of_the_batch_s_range_converges(
    spark: Any, tmp_path: Path
) -> None:
    """Two retries, stored first in their own files, are replaced in one batch by their recorded
    deliveries. Their `occurred_at` are the batch's range's two ends; a third file lies outside."""
    lake = LakeConfig.at(tmp_path / "lake")
    topic = TX_SCORED_V1
    pipeline = _pipeline()
    hour = dt.timedelta(hours=1)
    values: dict[str, bytes] = {}
    for name, transaction, occurred in (
        ("a", "tx_000000000911", T0),
        ("b", "tx_000000000912", T0 + hour),
    ):
        recorded = _score_at(pipeline, transaction, occurred)
        redelivered = _score_at(pipeline, transaction, occurred)
        assert (recorded.observe_outcome.value, redelivered.observe_outcome.value) == (
            "RECORDED",
            "REDELIVERY",
        )
        values[f"{name}_recorded"], values[f"{name}_retry"] = (
            _scored(recorded),
            _scored(redelivered),
        )
    values["c"] = _scored(_score_at(pipeline, "tx_000000000913", T0 + 2 * hour))
    for name in ("a", "b"):
        first = rules.admit(
            rules.BronzeRecord(
                topic, rules.Coordinates("x", 0, 0), T0 + 3 * hour, 1, values[f"{name}_retry"]
            )
        )
        second = rules.admit(
            rules.BronzeRecord(
                topic, rules.Coordinates("x", 0, 1), T0 + 3 * hour, 1, values[f"{name}_recorded"]
            )
        )
        assert first.digest == second.digest and first.occurred_at == second.occurred_at

    _create_bronze(spark, lake, topic)
    batches = [
        [_row(0, 0, T0 + dt.timedelta(seconds=1), values["a_retry"])],
        [_row(0, 1, T0 + hour + dt.timedelta(seconds=1), values["b_retry"])],
        [_row(0, 2, T0 + 2 * hour + dt.timedelta(seconds=1), values["c"])],
        [
            _row(0, 3, T0 + 3 * hour, values["a_recorded"]),
            _row(0, 4, T0 + 3 * hour, values["b_recorded"]),
        ],
    ]
    committed: dict[str, rules.Canonical] = {}
    expected: dict[tuple[int, int], str] = {}
    for index, rows in enumerate(batches):
        _append_bronze(spark, lake, topic, rows, batch=index)
        expected.update(_expected(topic, rows, committed))
        _run_silver(spark, lake, topic)
        assert _actual(spark, lake, topic) == expected, index

    assert expected[(0, 0)] == expected[(0, 1)] == "superseded"
    assert expected[(0, 3)] == expected[(0, 4)] == "canonical"
    canonical = {
        (r["kafka_partition"], r["kafka_offset"]): r
        for r in _rows(spark, lake, rules.silver_topic(topic).table)
    }
    assert sorted(canonical) == [(0, 2), (0, 3), (0, 4)]
    assert {canonical[k]["observe_outcome"] for k in ((0, 3), (0, 4))} == {"RECORDED"}
    report = check_silver_conservation(spark, lake, topic)
    assert report.conserved, report.summary()
    assert report.superseded == 2


# ------------------------------------------------------ what the MERGEs read ---


def _template(spark: Any, lake: LakeConfig, topic: str) -> dict[str, Any]:
    _create_bronze(spark, lake, topic)
    value = (
        _authorization(1)
        if topic == TX_AUTHORIZATION_V1
        else _scored(_score_at(_pipeline(), "tx_000000000921", T0))
    )
    _append_bronze(spark, lake, topic, [_row(0, 0, T0 + dt.timedelta(seconds=1), value)], batch=0)
    _run_silver(spark, lake, topic)
    (row,) = _rows(spark, lake, rules.silver_topic(topic).table)
    return dict(row.asDict())


def _canonical_frame(spark: Any, topic: str, rows: list[dict[str, Any]]) -> Any:
    schema = canonical_schema(topic)
    return spark.createDataFrame(
        [tuple(row[field.name] for field in schema.fields) for row in rows], schema
    )


@pytest.mark.parametrize("topic", [TX_AUTHORIZATION_V1, TX_SCORED_V1])
def test_the_canonical_merge_reads_only_the_files_its_bound_names(
    spark: Any, tmp_path: Path, topic: str
) -> None:
    """`tx.authorization.v1` runs Delta's insert-only MERGE, `tx.scored.v1` its classic MERGE.

    The kept file holds one row at a sub-millisecond `occurred_at`, the range's only value, so the
    file's millisecond statistics lie below it: the statistics slack is what keeps it read."""
    lake = LakeConfig.at(tmp_path / "lake")
    template = _template(spark, lake, topic)
    table = rules.silver_topic(topic).table.local_path(lake)
    edge = T0 + dt.timedelta(days=3, microseconds=123_456)
    placed = {
        "far_a": T0 + dt.timedelta(days=1),
        "far_b": T0 + dt.timedelta(days=2),
        "edge": edge,
        "far_c": T0 + dt.timedelta(days=4),
    }
    for offset, (identity, occurred) in enumerate(placed.items(), start=100):
        row = {**template, "silver_identity": identity, "occurred_at": occurred}
        row["kafka_offset"] = offset
        _canonical_frame(spark, topic, [row]).write.format("delta").mode("append").save(str(table))
    files = _file_of(spark, table)
    assert len(set(files.values())) == len(files) == 5, "one file per row"

    # A delivery of the edge row, earlier in the order (a lower offset): matched, never inserted.
    match = _canonical_frame(
        spark, topic, [{**template, "silver_identity": "edge", "occurred_at": edge}]
    ).persist()
    match.count()
    for path in _data_files(table):
        if path != files["edge"]:
            path.unlink()

    from delta.tables import DeltaTable

    with pytest.raises(Exception, match=MISSING_FILE):  # the unbounded control
        (
            DeltaTable.forPath(spark, str(table))
            .alias("t")
            .merge(match.alias("s"), "t.silver_identity = s.silver_identity")
            .whenNotMatchedInsertAll()
            .execute()
        )
    at_edge = OccurredRange(_us(edge), _us(edge))
    version = _version(spark, table)
    rows = spark.read.format("delta").load(str(table)).count()
    _canonical_merge(DeltaTable.forPath(spark, str(table)), match, topic, at_edge).execute()
    if topic == TX_SCORED_V1:
        metrics = _last_metrics(spark, table)
        assert metrics["numTargetRowsInserted"] == "0", metrics
        assert metrics["numTargetRowsUpdated"] == "1", metrics
    else:
        # An insert-only MERGE whose every source row matched inserts nothing, and Delta then
        # commits nothing at all: no new version, the same rows.
        assert _version(spark, table) == version
        assert spark.read.format("delta").load(str(table)).count() == rows

    # A batch that supersedes nothing reads no file at all.
    new = _canonical_frame(
        spark, topic, [{**template, "silver_identity": "new", "occurred_at": edge}]
    ).persist()
    new.count()
    for path in _data_files(table):
        path.unlink()
    _canonical_merge(DeltaTable.forPath(spark, str(table)), new, topic, None).execute()
    assert _last_metrics(spark, table)["numTargetRowsInserted"] == "1"


def test_the_late_events_merge_reads_only_the_files_its_bound_names(
    spark: Any, tmp_path: Path
) -> None:
    lake = LakeConfig.at(tmp_path / "lake")
    provenance = CommitProvenance(SHA, False, "stream_test")
    for declaration in shared_declarations():
        if declaration.ref == rules.LATE_EVENTS:
            create_table(spark, declaration, lake, provenance)
    table = rules.LATE_EVENTS.local_path(lake)
    topic = TX_AUTHORIZATION_V1
    schema = late_events_schema()
    edge = T0 + dt.timedelta(days=2, microseconds=999)

    def late_row(identity: str, occurred: dt.datetime, offset: int) -> tuple[Any, ...]:
        values = {
            "silver_topic": topic,
            "silver_identity": identity,
            "occurred_at": occurred,
            "kafka_timestamp": occurred + dt.timedelta(hours=1),
            "arrival_delay_ms": 3_600_000,
            "kafka_topic_id": "topic-id",
            "kafka_partition": 0,
            "kafka_offset": offset,
            "silver_batch_id": 0,
            "silver_checkpoint_id": "checkpoint",
            "silver_recorded_at": T0,
        }
        return tuple(values[field.name] for field in schema.fields)

    for offset, (identity, occurred) in enumerate(
        (("far_a", T0), ("edge", edge), ("far_b", T0 + dt.timedelta(days=4)))
    ):
        frame = spark.createDataFrame([late_row(identity, occurred, offset)], schema)
        frame.write.format("delta").mode("append").save(str(table))
    files = _file_of(spark, table)
    assert len(set(files.values())) == 3
    source = spark.createDataFrame([late_row("edge", edge, 7)], schema).persist()
    source.count()
    for path in _data_files(table):
        if path != files["edge"]:
            path.unlink()

    from delta.tables import DeltaTable

    unbounded = (
        f"t.silver_topic = '{topic}' AND t.silver_topic = s.silver_topic "
        f"AND t.silver_identity = s.silver_identity"
    )
    with pytest.raises(Exception, match=MISSING_FILE):
        DeltaTable.forPath(spark, str(table)).alias("t").merge(
            source.alias("s"), unbounded
        ).whenMatchedDelete().execute()
    bounded = late_events_merge_condition(topic, OccurredRange(_us(edge), _us(edge)))
    DeltaTable.forPath(spark, str(table)).alias("t").merge(
        source.alias("s"), bounded
    ).whenMatchedDelete().whenNotMatchedInsertAll().execute()
    metrics = _last_metrics(spark, table)
    assert (metrics["numTargetRowsDeleted"], metrics["numTargetRowsInserted"]) == ("1", "0")


# -------------------------------------------------------------------- guard ---


def test_batch_bounds_leave_out_a_conflict_s_own_time_and_refuse_a_moved_supersede(
    spark: Any,
) -> None:
    columns = ["disposition", "occurred_at", "existing_occurred_at_us"]
    d = rules.Disposition
    rows = [
        (d.ADMIT.value, 50, None),
        (d.DUPLICATE.value, 40, 40),
        (d.SUPERSEDE.value, 45, 45),
        (d.CONFLICT.value, 1, 60),  # its own time is not a committed row's; its canonical's is
    ]
    bounds = batch_merge_bounds(spark.createDataFrame(rows, columns))
    assert bounds.supersede == OccurredRange(45, 45)
    assert bounds.committed == OccurredRange(40, 60)
    moved = [*rows, (d.SUPERSEDE.value, 46, 47)]
    with pytest.raises(SilverPruningError, match="1 superseding row"):
        batch_merge_bounds(spark.createDataFrame(moved, columns))
