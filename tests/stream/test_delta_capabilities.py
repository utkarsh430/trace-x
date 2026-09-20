"""The Delta 4.0.1 capability spike (Phase 3 Step 3), executed on the pinned toolchain.

Each test records what the pinned Delta DID on a live table, and its assertion states that
behaviour, so a future pin that behaves differently fails here rather than in a pipeline. Where
Delta contradicts what the plan assumed, the test says so in its docstring.

No test asserts a timing or a size Delta chose: every number compared is derived from how the
data was built or read back from Delta or Spark itself, so nothing here is a benchmark.

Marker `stream`: skipped loudly when the toolchain does not match its pins, and a `-m stream`
session in which no stream test executed FAILS (tests/conftest.py).
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import shutil
import time
from collections import Counter
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from trace_core.domain.errors import (
    CheckpointRefusedError,
    ScanMeasurementError,
    StreamingSourceRetentionError,
    TableDeclarationError,
    TableDriftError,
)
from trace_core.stream import checkpoints, tables
from trace_core.stream.checkpoints import DeltaSourceStart, OpenedCheckpoint, StartAction
from trace_core.stream.lake import AppId, LakeConfig, Tier
from trace_core.stream.tables import (
    CheckConstraint,
    CommitProvenance,
    DeltaSourceOffset,
    TableDeclaration,
    TableLayout,
    TableRef,
)

pytestmark = pytest.mark.stream

SHA = hashlib.sha1(b"trace-x delta capability spike", usedforsecurity=False).hexdigest()
NOW = datetime(2026, 9, 13, 8, 0, tzinfo=UTC)
RETENTION_CHECK = "spark.databricks.delta.retentionDurationCheck.enabled"
MAX_FILE_SIZE = "spark.databricks.delta.optimize.maxFileSize"
IGNORE_MISSING_FILES = "spark.sql.files.ignoreMissingFiles"
SHORT_RETENTION = (
    "TBLPROPERTIES ('delta.checkpointInterval' = '3', "
    "'delta.logRetentionDuration' = 'interval 1 hours')"
)


@pytest.fixture(scope="module")
def spark() -> Iterator[Any]:
    from trace_core.stream.session import build_session

    session = build_session("trace-x-delta-capabilities")
    try:
        yield session
    finally:
        session.stop()


# ---------------------------------------------------------------------- helpers ---


def delta_table(spark: Any, path: Path) -> Any:
    from delta.tables import DeltaTable

    return DeltaTable.forPath(spark, str(path))


def latest_version(spark: Any, path: Path) -> int:
    return int(delta_table(spark, path).history(1).collect()[0]["version"])


def values(spark: Any, path: Path, column: str = "id") -> list[Any]:
    """Every value of `column`, read row by row (never a statistics-only count)."""
    return sorted(row[column] for row in spark.read.format("delta").load(str(path)).collect())


def append(
    spark: Any, path: Path, data: list[tuple[Any, ...]], schema: str, files: int = 1
) -> None:
    frame = spark.createDataFrame(data, schema)
    frame = frame.coalesce(1) if files == 1 else frame.repartition(files)
    frame.write.format("delta").mode("append").save(str(path))


def active_files(path: Path) -> list[dict[str, Any]]:
    """The table's live `add` actions, replayed from its JSON commits."""
    log = path / "_delta_log"
    assert not list(log.glob("*.checkpoint*.parquet")), "replaying JSON alone needs no checkpoint"
    active: dict[str, dict[str, Any]] = {}
    for commit in sorted(log.glob("*.json")):
        for line in commit.read_text().splitlines():
            action = json.loads(line)
            if "add" in action:
                active[action["add"]["path"]] = action["add"]
            elif "remove" in action:
                active.pop(action["remove"]["path"], None)
    return list(active.values())


def id_range(add: dict[str, Any]) -> tuple[int, int]:
    stats = json.loads(add["stats"])
    return int(stats["minValues"]["id"]), int(stats["maxValues"]["id"])


def overlapping_pairs(adds: list[dict[str, Any]]) -> int:
    ranges = [id_range(add) for add in adds]
    return sum(
        1
        for i, (lo, hi) in enumerate(ranges)
        for other_lo, other_hi in ranges[i + 1 :]
        if lo <= other_hi and other_lo <= hi
    )


def progress(query: Any) -> list[dict[str, Any]]:
    return [json.loads(event.json) for event in query.recentProgress]


def seeded_rows(seed: int, count: int, high: int) -> list[tuple[int, str]]:
    rng = random.Random(seed)
    return [(rng.randrange(high), "x" * 50) for _ in range(count)]


def batches(directory: Path, kind: str) -> list[int]:
    return sorted(int(p.name) for p in (directory / kind).iterdir() if p.name.isdigit())


def age_log_below(source: Path, version: int) -> None:
    """Make every log file below `version` older than a one-hour log retention.

    Delta's metadata cleanup, which runs when the next log checkpoint is written, then
    removes them -- the real cleanup path, not a hand deletion."""
    aged = time.time() - 3 * 86400
    for entry in (source / "_delta_log").iterdir():
        if entry.name[:20].isdigit() and int(entry.name[:20]) < version:
            os.utime(entry, (aged, aged))


class Pipeline:
    """A Delta source, a declared target and a query that follows the checkpoint convention."""

    def __init__(
        self,
        spark: Any,
        root: Path,
        query: str,
        *,
        source_clause: str = "",
        starting_version: int | None = None,
    ) -> None:
        from pyspark.sql.types import LongType, StructField, StructType

        self.spark, self.query = spark, query
        self.lake = LakeConfig.at(root / "lake")
        self.source = TableRef(Tier.BRONZE, f"{query}_source")
        self.target = TableRef(Tier.SILVER, f"{query}_target")
        self.start = DeltaSourceStart(self.source, starting_version)
        self.declaration = TableDeclaration(
            ref=self.target, schema=StructType([StructField("id", LongType())])
        )
        spark.sql(
            f"CREATE TABLE {self.source.path_identifier(self.lake)} (id BIGINT) USING delta "
            f"{source_clause}"
        )
        self.create_target()

    def create_target(self) -> None:
        stamp = CommitProvenance(SHA, False, self.query)  # made outside any checkpoint
        tables.create_table(self.spark, self.declaration, self.lake, stamp)

    @property
    def source_path(self) -> Path:
        return self.source.local_path(self.lake)

    @property
    def target_path(self) -> Path:
        return self.target.local_path(self.lake)

    def add(self, *ids: int) -> None:
        """One source commit per id."""
        for value in ids:
            append(self.spark, self.source_path, [(value,)], "id BIGINT")

    def open(self) -> OpenedCheckpoint:
        return checkpoints.open_checkpoint(
            self.spark,
            self.lake,
            self.query,
            targets=[self.target],
            sources=[self.start],
            git_sha=SHA,
            dirty_worktree=False,
            now=NOW,
        )

    def reset(self, reason: str) -> checkpoints.CheckpointIdentity:
        return checkpoints.reset_checkpoint(
            self.spark,
            self.lake,
            self.query,
            targets=[self.target],
            sources=[self.start],
            reason=reason,
            now=NOW,
        )

    def run(
        self,
        opened: OpenedCheckpoint,
        *,
        sink: Callable[[Any, int], None] | None = None,
        options: dict[str, str] | None = None,
        reader: Any = None,
    ) -> Any:
        def append_to_target(frame: Any, batch_id: int) -> None:
            opened.append(frame, batch_id=batch_id, target=self.target)

        frame = (
            reader if reader is not None else opened.delta_source(self.spark, self.source, options)
        )
        query = (
            frame.writeStream.foreachBatch(sink or append_to_target)
            .option("checkpointLocation", str(opened.directory))
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        return query

    def targets(self) -> list[Any]:
        return values(self.spark, self.target_path)


# --------------------------------------------------------------- (a) appendOnly ---


def test_a_append_only_refuses_delete_and_update_but_admits_insert_only_merge(
    spark: Any, tmp_path: Path
) -> None:
    """Observed: even `DELETE ... WHERE false` is refused. Insert-only MERGE is not, so an
    append-only canonical Silver table can still be written by §4.2's insert-only MERGE."""
    path = tmp_path / "append_only"
    spark.sql(
        f"CREATE TABLE delta.`{path}` (id BIGINT, v STRING) USING delta "
        f"TBLPROPERTIES ('delta.appendOnly'='true')"
    )
    spark.sql(f"INSERT INTO delta.`{path}` VALUES (1, 'x'), (2, 'y')")
    before = latest_version(spark, path)
    for statement in (
        f"DELETE FROM delta.`{path}` WHERE id = 1",
        f"UPDATE delta.`{path}` SET v = 'z' WHERE id = 1",
        f"DELETE FROM delta.`{path}` WHERE false",
    ):
        with pytest.raises(Exception, match="DELTA_CANNOT_MODIFY_APPEND_ONLY"):
            spark.sql(statement)
    assert latest_version(spark, path) == before

    source = spark.createDataFrame([(2, "dup"), (3, "new")], "id BIGINT, v STRING")
    delta_table(spark, path).alias("t").merge(
        source.alias("s"), "t.id = s.id"
    ).whenNotMatchedInsertAll().execute()
    assert values(spark, path) == [1, 2, 3]


# ------------------------------------------------------ (b) idempotent writes ---


def test_b_a_replayed_transaction_version_is_skipped_and_a_new_app_id_writes(
    spark: Any, tmp_path: Path
) -> None:
    path = tmp_path / "txn"
    one_row = spark.createDataFrame([(1,)], "id BIGINT")

    def write(app_id: str, version: int) -> tuple[int, int]:
        (
            one_row.write.format("delta")
            .mode("append")
            .option("txnAppId", app_id)
            .option("txnVersion", str(version))
            .save(str(path))
        )
        return latest_version(spark, path), len(values(spark, path))

    assert write("writer-a", 0) == (0, 1)
    assert write("writer-a", 0) == (0, 1)  # the replay commits nothing
    assert write("writer-a", 5) == (1, 2)
    assert write("writer-a", 3) == (1, 2)  # a LOWER version is a replay too
    assert write("writer-b", 0) == (2, 3)  # a new app id writes
    facts = tables.snapshot_facts(spark, path)
    assert facts is not None and facts.transactions == {"writer-a": 5, "writer-b": 0}


def test_a_new_checkpoint_reusing_an_old_app_id_silently_loses_its_batches(
    spark: Any, tmp_path: Path
) -> None:
    """The trap `trace_core.stream.checkpoints` exists to close, reproduced raw."""
    source, target = tmp_path / "source", tmp_path / "target"
    append(spark, source, [(0,), (1,)], "id BIGINT")
    append(spark, source, [(2,), (3,)], "id BIGINT")
    spark.sql(f"CREATE TABLE delta.`{target}` (id BIGINT) USING delta")

    def sink(frame: Any, batch_id: int) -> None:
        (
            frame.write.format("delta")
            .mode("append")
            .option("txnAppId", "silver-writer")
            .option("txnVersion", str(batch_id))
            .save(str(target))
        )

    def run(checkpoint: Path, **options: str) -> Any:
        reader = spark.readStream.format("delta")
        for key, value in options.items():
            reader = reader.option(key, value)
        query = (
            reader.load(str(source))
            .writeStream.foreachBatch(sink)
            .option("checkpointLocation", str(checkpoint))
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        return query

    original = tmp_path / "checkpoint-original"
    first = run(original, maxFilesPerTrigger="1")
    assert [event["batchId"] for event in progress(first)] == [0, 1]
    assert values(spark, target) == [0, 1, 2, 3]

    append(spark, source, [(4,), (5,), (6,)], "id BIGINT")
    replacement = run(tmp_path / "checkpoint-replacement")
    assert replacement.exception() is None
    [batch] = progress(replacement)
    assert (batch["batchId"], batch["numInputRows"]) == (0, 0)  # reported as a completed batch
    assert values(spark, target) == [0, 1, 2, 3]  # rows 4, 5 and 6 are gone

    run(original, maxFilesPerTrigger="1")
    assert values(spark, target) == [0, 1, 2, 3, 4, 5, 6]  # control: only the reuse lost them


def test_b_the_checkpoint_convention_pairs_the_app_id_with_its_checkpoint(
    spark: Any, tmp_path: Path
) -> None:
    pipeline = Pipeline(spark, tmp_path, "pairing_spike")
    pipeline.add(0, 1)
    sessions: list[bool] = []
    created = pipeline.open()
    assert created.action is StartAction.CREATE

    def sink(frame: Any, batch_id: int) -> None:
        sessions.append(bool(frame.sparkSession._jsparkSession.equals(spark._jsparkSession)))
        created.append(frame, batch_id=batch_id, target=pipeline.target)

    pipeline.run(created, sink=sink)
    assert pipeline.targets() == [0, 1]
    assert (created.directory / checkpoints.IDENTITY_FILENAME).is_file()  # Spark left it alone
    assert (created.directory / "offsets").is_dir()  # ... in the directory Spark used
    # Observed: the batch's session is not the session that started the query. That it is a
    # separate clone per query is Spark's documented design, not something observed here.
    assert sessions and not any(sessions)
    evidence = checkpoints.read_target_evidence(spark, pipeline.lake, pipeline.target)
    assert evidence.transactions == {created.identity.app_id: 0}
    stamped = [(s.query, s.checkpoint_id, s.batch_id) for s in evidence.provenance]
    assert stamped == [("pairing_spike", created.identity.app_id, 0), ("pairing_spike", None, None)]

    pipeline.add(2)
    resumed = pipeline.open()
    assert (resumed.action, resumed.identity) == (StartAction.RESUME, created.identity)
    pipeline.run(resumed)
    assert pipeline.targets() == [0, 1, 2]

    # The checkpoint is lost: moved aside here, standing in for a deleted volume.
    created.directory.parent.rename(tmp_path / "lost-checkpoints")
    with pytest.raises(CheckpointRefusedError, match="no checkpoint exists"):
        pipeline.open()

    reset = pipeline.reset("checkpoint volume lost (spike)")
    assert reset.version == 2 and reset.app_id != created.identity.app_id
    reopened = pipeline.open()
    assert (reopened.action, reopened.identity) == (StartAction.RESUME, reset)
    pipeline.run(reopened)
    # Unlike the trap, the new app id writes: the source is re-read from its recorded start and,
    # this sink being a plain append, appended again -- which is why a reset is an explicit act.
    assert pipeline.targets() == [0, 0, 1, 1, 2, 2]


def test_b_a_lost_commit_marker_replays_safely_and_lost_offsets_are_refused(
    spark: Any, tmp_path: Path
) -> None:
    pipeline = Pipeline(spark, tmp_path, "crash_spike")
    pipeline.add(0, 1)
    opened = pipeline.open()
    pipeline.run(opened)
    assert pipeline.targets() == [0, 1]

    def forget(kind: str, batch: int) -> None:
        (opened.directory / kind / str(batch)).unlink()
        (opened.directory / kind / f".{batch}.crc").unlink(missing_ok=True)

    # A crash after the sink committed batch 0 and before Spark recorded it -- with new rows
    # already in the source, so a batch 0 re-planned over a wider range would take them too.
    forget("commits", 0)
    pipeline.add(2, 3)
    assert pipeline.open().action is StartAction.RESUME
    replay = progress(pipeline.run(pipeline.open()))
    assert [(e["batchId"], e["numInputRows"]) for e in replay] == [(0, 0), (1, 2)]
    assert pipeline.targets() == [0, 1, 2, 3]  # batch 0 replayed from its offsets and skipped

    pipeline.add(4, 5)
    forget("commits", 1)
    forget("offsets", 1)
    with pytest.raises(CheckpointRefusedError, match="silently skip"):
        pipeline.open()

    # What the refusal prevents: batch 1, re-planned over rows 2-5, is skipped as a replay.
    bypass = OpenedCheckpoint(
        opened.identity, opened.directory, StartAction.RESUME, opened.lake, SHA, False
    )
    pipeline.run(bypass)
    assert pipeline.targets() == [0, 1, 2, 3]
    assert values(spark, pipeline.source_path) == [0, 1, 2, 3, 4, 5]


def test_b_a_second_idempotent_commit_to_one_table_in_one_batch_is_skipped_and_refused(
    spark: Any, tmp_path: Path
) -> None:
    raw = tmp_path / "raw"
    for value in (1, 2):
        (
            spark.createDataFrame([(value,)], "id BIGINT")
            .write.format("delta")
            .mode("append")
            .option("txnAppId", "raw-writer")
            .option("txnVersion", "0")
            .save(str(raw))
        )
    assert values(spark, raw) == [1]  # observed: the second write of the batch is skipped

    pipeline = Pipeline(spark, tmp_path, "double_spike")
    opened = pipeline.open()
    opened.append(spark.createDataFrame([(1,)], "id BIGINT"), batch_id=0, target=pipeline.target)
    with pytest.raises(CheckpointRefusedError, match="second idempotent commit"):
        opened.append(
            spark.createDataFrame([(2,)], "id BIGINT"), batch_id=0, target=pipeline.target
        )
    with pytest.raises(CheckpointRefusedError, match="second idempotent commit"):
        opened.merge(
            spark,
            batch_id=0,
            target=pipeline.target,
            build=lambda table: (
                table.alias("t")
                .merge(spark.createDataFrame([(3,)], "id BIGINT").alias("s"), "t.id = s.id")
                .whenNotMatchedInsertAll()
            ),
        )
    opened.append(spark.createDataFrame([(4,)], "id BIGINT"), batch_id=1, target=pipeline.target)
    assert pipeline.targets() == [1, 4]


def test_b_the_native_delta_sink_keys_idempotency_on_the_checkpoint_query_id(
    spark: Any, tmp_path: Path
) -> None:
    """Observed: Delta's own streaming sink uses the checkpoint's persistent query id as its app
    id, honours `userMetadata`, and a new checkpoint over the same target writes again --
    duplicating rows in an append sink rather than skipping them."""
    source, target = tmp_path / "source", tmp_path / "target"
    append(spark, source, [(0,), (1,)], "id BIGINT")
    stamp = CommitProvenance(SHA, False, "bronze_spike", str(AppId.new("bronze_spike", 1)), 0)

    def run(checkpoint: Path) -> Any:
        query = (
            spark.readStream.format("delta")
            .load(str(source))
            .writeStream.format("delta")
            .options(**stamp.writer_options())
            .option("checkpointLocation", str(checkpoint))
            .trigger(availableNow=True)
            .start(str(target))
        )
        query.awaitTermination()
        return query

    query = run(tmp_path / "cp")
    query_id = json.loads((tmp_path / "cp" / "metadata").read_text())["id"]
    assert query_id == str(query.id)
    facts = tables.snapshot_facts(spark, target)
    assert facts is not None and facts.transactions == {query_id: 0}
    history = delta_table(spark, target).history(1).collect()[0]
    assert CommitProvenance.from_user_metadata(history["userMetadata"]) == stamp
    run(tmp_path / "cp-new")
    assert values(spark, target) == [0, 0, 1, 1]


def test_b_merge_idempotency_rides_on_session_conf_that_delta_leaves_set(
    spark: Any, tmp_path: Path
) -> None:
    path = tmp_path / "merge_txn"
    spark.sql(f"CREATE TABLE delta.`{path}` (id BIGINT) USING delta")
    auto_reset = "spark.databricks.delta.write.txnVersion.autoReset.enabled"

    def merge(target: Path, ids: list[int]) -> None:
        source = spark.createDataFrame([(i,) for i in ids], "id BIGINT")
        delta_table(spark, target).alias("t").merge(
            source.alias("s"), "t.id = s.id"
        ).whenNotMatchedInsertAll().execute()

    assert spark.conf.get(auto_reset) == "false"
    spark.conf.set(checkpoints.TXN_APP_ID_CONF, "raw-merge")
    spark.conf.set(checkpoints.TXN_VERSION_CONF, "7")
    try:
        merge(path, [1])
        assert spark.conf.get(checkpoints.TXN_VERSION_CONF) == "7"  # still set after the commit
        merge(path, [2])  # a different MERGE on the same session ...
        assert values(spark, path) == [1]  # ... silently skipped as a replay
        spark.conf.set(auto_reset, "true")
        spark.conf.set(checkpoints.TXN_VERSION_CONF, "8")
        merge(path, [3])
        assert values(spark, path) == [1, 3]
        assert spark.conf.get(checkpoints.TXN_VERSION_CONF, None) is None  # reset with the flag
        assert spark.conf.get(checkpoints.TXN_APP_ID_CONF) == "raw-merge"  # ... the app id is not
    finally:
        for key in (checkpoints.TXN_APP_ID_CONF, checkpoints.TXN_VERSION_CONF, auto_reset):
            spark.conf.unset(key)

    pipeline = Pipeline(spark, tmp_path, "merge_spike")
    opened = pipeline.open()

    def insert_only(ids: list[int]) -> Callable[[Any], Any]:
        def build(table: Any) -> Any:
            source = spark.createDataFrame([(i,) for i in ids], "id BIGINT")
            return (
                table.alias("t").merge(source.alias("s"), "t.id = s.id").whenNotMatchedInsertAll()
            )

        return build

    for batch_id, ids in ((0, [2]), (1, [2, 3])):
        opened.merge(spark, batch_id=batch_id, target=pipeline.target, build=insert_only(ids))
        assert spark.conf.get(checkpoints.TXN_APP_ID_CONF, None) is None
        assert spark.conf.get(tables.USER_METADATA_CONF, None) is None
    assert pipeline.targets() == [2, 3]
    evidence = checkpoints.read_target_evidence(spark, pipeline.lake, pipeline.target)
    assert evidence.transactions == {opened.identity.app_id: 1}
    assert [s.batch_id for s in evidence.provenance if s.checkpoint_id] == [1, 0]


def test_b_a_target_that_lost_delivered_rows_refuses_the_resume(spark: Any, tmp_path: Path) -> None:
    """A RESTORE keeps the table id and the transaction identifiers (observed), so only the
    retained history shows it; a dropped and recreated table changes its id (observed)."""
    restored = Pipeline(spark, tmp_path / "restored", "restore_spike")
    restored.add(0)
    restored.run(restored.open())
    restored.add(1)
    resumed = restored.open()
    restored.run(resumed)
    assert restored.targets() == [0, 1]
    before = tables.snapshot_facts(spark, restored.target_path)
    spark.sql(f"RESTORE TABLE {restored.target.path_identifier(restored.lake)} TO VERSION AS OF 1")
    after = tables.snapshot_facts(spark, restored.target_path)
    assert before is not None and after is not None
    assert restored.targets() == [0]
    assert (after.table_id, after.transactions) == (before.table_id, before.transactions)
    with pytest.raises(CheckpointRefusedError, match="RESTOREd"):
        restored.open()

    recreated = Pipeline(spark, tmp_path / "recreated", "recreate_spike")
    recreated.add(0, 1)
    recreated.run(recreated.open())
    shutil.rmtree(recreated.target_path)
    recreated.create_target()
    assert recreated.targets() == []
    with pytest.raises(CheckpointRefusedError, match="dropped and recreated"):
        recreated.open()


# ------------------------------------------------------------------ (c), (d) MERGE ---


def _insert_only_merge(spark: Any, path: Path, data: list[tuple[int, str]]) -> None:
    source = spark.createDataFrame(data, "id BIGINT, v STRING")
    delta_table(spark, path).alias("t").merge(
        source.alias("s"), "t.id = s.id"
    ).whenNotMatchedInsertAll().execute()


def test_c_insert_only_merge_deduplicates_across_batches_and_the_first_write_wins(
    spark: Any, tmp_path: Path
) -> None:
    path = tmp_path / "canonical"
    spark.sql(f"CREATE TABLE delta.`{path}` (id BIGINT, v STRING) USING delta")
    _insert_only_merge(spark, path, [(1, "b1"), (2, "b1"), (3, "b1")])
    _insert_only_merge(spark, path, [(3, "b2"), (4, "b2")])
    rows = sorted(tuple(r) for r in spark.read.format("delta").load(str(path)).collect())
    assert rows == [(1, "b1"), (2, "b1"), (3, "b1"), (4, "b2")]
    metrics = delta_table(spark, path).history(1).collect()[0]["operationMetrics"]
    assert (metrics["numSourceRows"], metrics["numTargetRowsInserted"]) == ("2", "1")


def test_d_duplicate_source_keys_are_inserted_twice_by_an_insert_only_merge(
    spark: Any, tmp_path: Path
) -> None:
    """CONTRADICTS the plan's assumption that a MERGE with duplicate source keys fails.

    Only a MERGE with a matched clause fails, and only when the duplicates match an existing
    target row. An insert-only MERGE -- §4.2's canonical Silver write -- inserts every duplicate
    of a new key, silently. Deduplicating each batch before the MERGE is therefore the
    uniqueness guarantee itself, not a precaution against an error."""
    path = tmp_path / "duplicates"
    spark.sql(f"CREATE TABLE delta.`{path}` (id BIGINT, v STRING) USING delta")
    _insert_only_merge(spark, path, [(1, "seed")])

    _insert_only_merge(spark, path, [(5, "x"), (5, "y")])
    assert values(spark, path).count(5) == 2  # no error; both inserted
    _insert_only_merge(spark, path, [(1, "x"), (1, "y")])
    assert values(spark, path).count(1) == 1  # matched duplicates: no error, nothing inserted

    before = latest_version(spark, path)
    duplicates = spark.createDataFrame([(1, "x"), (1, "y")], "id BIGINT, v STRING")
    with pytest.raises(Exception, match="DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE"):
        delta_table(spark, path).alias("t").merge(
            duplicates.alias("s"), "t.id = s.id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    assert latest_version(spark, path) == before

    unmatched = spark.createDataFrame([(6, "x"), (6, "y")], "id BIGINT, v STRING")
    delta_table(spark, path).alias("t").merge(
        unmatched.alias("s"), "t.id = s.id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    assert values(spark, path).count(6) == 2  # even with a matched clause, if nothing matches


# ------------------------------------------------------------- (e) constraints ---


def test_e_a_constraint_violation_fails_the_whole_write(spark: Any, tmp_path: Path) -> None:
    path = tmp_path / "constrained"
    spark.sql(f"CREATE TABLE delta.`{path}` (id BIGINT NOT NULL, amount_minor BIGINT) USING delta")
    spark.sql(f"ALTER TABLE delta.`{path}` ADD CONSTRAINT amount_nonneg CHECK (amount_minor >= 0)")
    append(spark, path, [(1, 5)], "id BIGINT, amount_minor BIGINT")
    before = latest_version(spark, path)

    for data, code in (
        ([(2, 1), (None, 3), (4, 4)], "DELTA_NOT_NULL_CONSTRAINT_VIOLATED"),
        ([(2, 1), (3, -1), (4, 4)], "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES"),
    ):
        with pytest.raises(Exception, match=code):
            append(spark, path, data, "id BIGINT, amount_minor BIGINT", files=2)
        assert (latest_version(spark, path), values(spark, path)) == (before, [1])

    violating = spark.createDataFrame([(7, -5), (8, 1)], "id BIGINT, amount_minor BIGINT")
    with pytest.raises(Exception, match="DELTA_VIOLATE_CONSTRAINT_WITH_VALUES"):
        delta_table(spark, path).alias("t").merge(
            violating.alias("s"), "t.id = s.id"
        ).whenNotMatchedInsertAll().execute()
    assert (latest_version(spark, path), values(spark, path)) == (before, [1])


# ------------------------------------------------------------------ (f) VACUUM ---


def test_f_vacuum_refuses_retention_below_the_tables_own_retention(
    spark: Any, tmp_path: Path
) -> None:
    """Observed: the floor is the table's `delta.deletedFileRetentionDuration` (default 7 days),
    not a fixed 168 hours -- lowering that property lowers the floor. The drift check is what
    catches an undeclared change to it."""
    from pyspark.sql.types import LongType, StructField, StructType

    assert spark.conf.get(RETENTION_CHECK) == "true"  # Delta's default; the session keeps it
    path = tmp_path / "vacuumed"
    spark.sql(f"CREATE TABLE delta.`{path}` (id BIGINT NOT NULL) USING delta")
    spark.range(10).write.format("delta").mode("append").save(str(path))

    def vacuum(hours: int) -> None:
        spark.sql(f"VACUUM delta.`{path}` RETAIN {hours} HOURS DRY RUN").collect()

    for hours in (0, 167):
        with pytest.raises(Exception, match="such a low retention period"):
            vacuum(hours)
    vacuum(168)

    spark.sql(
        f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES "
        f"('delta.deletedFileRetentionDuration' = 'interval 1 hours')"
    )
    vacuum(1)
    with pytest.raises(Exception, match="such a low retention period"):
        vacuum(0)

    declaration = TableDeclaration(
        ref=TableRef(Tier.BRONZE, "vacuum_spike"),
        schema=StructType([StructField("id", LongType(), nullable=False)]),
    )
    live = tables.describe_live_table(spark, f"delta.`{path}`")
    assert [drift.aspect for drift in tables.check_drift(declaration, live)] == ["property"]


# --------------------------------------------------- (g) streaming source retention ---


def test_g_a_running_query_with_fail_on_data_loss_false_silently_loses_rows(
    spark: Any, tmp_path: Path
) -> None:
    """The silent case the first round of this spike missed (found by the critic, re-derived
    here): a query that is ALREADY RUNNING when the log it needs is cleaned completes without
    error under `failOnDataLoss=false`, and the rows never arrive. No pre-start guard can see
    this; the convention's reader forces `failOnDataLoss=true`, which fails loudly."""
    retained: list[int] = []

    def fall_behind(source: Path) -> None:
        for value in range(5, 12):
            append(spark, source, [(value,)], "id BIGINT")
        age_log_below(source, 9)
        append(spark, source, [(12,)], "id BIGINT")  # writes a log checkpoint, which cleans up
        log = tables.retained_log(source)
        assert log is not None
        retained.append(log.earliest)

    raw = tmp_path / "raw_source"
    spark.sql(f"CREATE TABLE delta.`{raw}` (id BIGINT) USING delta {SHORT_RETENTION}")
    for value in range(1, 5):
        append(spark, raw, [(value,)], "id BIGINT")
    seen: list[int] = []

    def raw_sink(frame: Any, batch_id: int) -> None:
        seen.extend(row["id"] for row in frame.collect())
        if batch_id == 0:
            fall_behind(raw)

    query = (
        spark.readStream.format("delta")
        .option("startingVersion", "1")
        .option("maxFilesPerTrigger", "1")
        .option("failOnDataLoss", "false")
        .load(str(raw))
        .writeStream.foreachBatch(raw_sink)
        .option("checkpointLocation", str(tmp_path / "raw_cp"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    assert query.exception() is None  # completed
    assert retained == [9]  # versions 2-4, holding rows 2-4, were cleaned mid-run
    assert seen == [1]  # ... and rows 2, 3 and 4, present before the query started, never arrived

    pipeline = Pipeline(
        spark, tmp_path, "fall_behind_spike", source_clause=SHORT_RETENTION, starting_version=1
    )
    pipeline.add(1, 2, 3, 4)
    opened = pipeline.open()
    with pytest.raises(CheckpointRefusedError, match="failOnDataLoss"):
        opened.delta_source(spark, pipeline.source, {"failOnDataLoss": "false"})

    def sink(frame: Any, batch_id: int) -> None:
        opened.append(frame, batch_id=batch_id, target=pipeline.target)
        if batch_id == 0:
            fall_behind(pipeline.source_path)

    with pytest.raises(Exception, match="DELTA_MISSING_FILES_UNEXPECTED_VERSION"):
        pipeline.run(opened, sink=sink, options={"maxFilesPerTrigger": "1"})
    assert pipeline.targets() == [1]  # the same loss, but loud


def test_g_a_restart_behind_the_retained_log_fails_loudly_and_the_guard_refuses_first(
    spark: Any, tmp_path: Path
) -> None:
    """At a RESTART Delta itself refuses (`DELTA_LOG_FILE_NOT_FOUND_FOR_STREAMING_SOURCE`), and
    `failOnDataLoss=false` does not change that. The guard refuses before start, with a remedy
    other than Delta's ("delete its checkpoint")."""
    pipeline = Pipeline(spark, tmp_path, "retention_spike", source_clause=SHORT_RETENTION)
    pipeline.add(1, 2)
    opened = pipeline.open()
    pipeline.run(opened)
    assert pipeline.targets() == [1, 2]
    pipeline.open().delta_source(spark, pipeline.source)  # healthy: nothing refused
    [offset_text] = checkpoints.committed_source_offsets(opened.directory)
    assert offset_text is not None
    needed = DeltaSourceOffset.parse(offset_text).reservoir_version

    pipeline.add(*range(3, 12))
    age_log_below(pipeline.source_path, 9)
    pipeline.add(12)
    retained = tables.retained_log(pipeline.source_path)
    assert retained is not None and retained.earliest > needed

    with pytest.raises(StreamingSourceRetentionError, match=f"needs version {needed}"):
        pipeline.open().delta_source(spark, pipeline.source)

    for name, options in (("default", {}), ("tolerant", {"failOnDataLoss": "false"})):
        copy = tmp_path / f"cp-{name}"
        shutil.copytree(opened.directory, copy)
        reader = spark.readStream.format("delta")
        for key, value in options.items():
            reader = reader.option(key, value)
        query = (
            reader.load(str(pipeline.source_path))
            .writeStream.foreachBatch(lambda frame, batch_id: None)
            .option("checkpointLocation", str(copy))
            .trigger(availableNow=True)
            .start()
        )
        with pytest.raises(Exception, match="DELTA_LOG_FILE_NOT_FOUND_FOR_STREAMING_SOURCE"):
            query.awaitTermination()
    assert pipeline.targets() == [1, 2]


def test_g_ignore_missing_files_advances_the_checkpoint_past_vacuumed_rows_for_good(
    spark: Any, tmp_path: Path
) -> None:
    """With the log intact the retention guard sees nothing; the silent skip here is
    `spark.sql.files.ignoreMissingFiles=true`, which the convention's reader refuses."""
    pipeline = Pipeline(spark, tmp_path, "vacuumed_spike")
    pipeline.add(0, 1)
    opened = pipeline.open()
    pipeline.run(opened)
    pipeline.add(2, 3, 4, 5)
    spark.sql(f"OPTIMIZE {pipeline.source.path_identifier(pipeline.lake)}").collect()
    spark.conf.set(RETENTION_CHECK, "false")
    try:
        spark.sql(
            f"VACUUM {pipeline.source.path_identifier(pipeline.lake)} RETAIN 0 HOURS"
        ).collect()
    finally:
        spark.conf.set(RETENTION_CHECK, "true")

    with pytest.raises(Exception, match=r"FAILED_READ_FILE\.FILE_NOT_EXIST"):
        pipeline.run(pipeline.open())
    assert pipeline.targets() == [0, 1]
    committed_before = batches(opened.directory, "commits")
    [text_before] = checkpoints.committed_source_offsets(opened.directory)
    assert text_before is not None

    spark.conf.set(IGNORE_MISSING_FILES, "true")
    try:
        with pytest.raises(StreamingSourceRetentionError, match="ignoreMissingFiles"):
            pipeline.open().delta_source(spark, pipeline.source)
        tolerant = pipeline.open()
        raw_reader = spark.readStream.format("delta").load(str(pipeline.source_path))
        pipeline.run(tolerant, reader=raw_reader)  # completes
    finally:
        spark.conf.set(IGNORE_MISSING_FILES, "false")
    assert pipeline.targets() == [0, 1]
    assert len(batches(opened.directory, "commits")) > len(committed_before)  # it advanced
    [text_after] = checkpoints.committed_source_offsets(opened.directory)
    assert text_after is not None
    assert (
        DeltaSourceOffset.parse(text_after).reservoir_version
        > DeltaSourceOffset.parse(text_before).reservoir_version
    )

    pipeline.run(pipeline.open())  # the setting off again: no error ...
    assert pipeline.targets() == [0, 1]  # ... and rows 2-5 are gone for good
    assert values(spark, pipeline.source_path) == [0, 1, 2, 3, 4, 5]


# ----------------------------------------------------------- (h), (i) layout ---


def test_h_liquid_clustering_is_applied_by_optimize_not_on_write(
    spark: Any, tmp_path: Path
) -> None:
    path = tmp_path / "clustered"
    spark.sql(f"CREATE TABLE delta.`{path}` (id BIGINT, pad STRING) USING delta CLUSTER BY (id)")
    live = tables.describe_live_table(spark, f"delta.`{path}`")
    assert live.clustering_columns == ("id",)
    assert live.table_features >= tables.CLUSTERING_FEATURES
    for seed in range(4):
        append(spark, path, seeded_rows(seed, 1000, 4000), "id BIGINT, pad STRING", files=2)

    appended = active_files(path)
    assert len(appended) == 8
    assert all(add.get("clusteringProvider") is None for add in appended)
    assert overlapping_pairs(appended) == len(appended) * (len(appended) - 1) // 2

    spark.conf.set(MAX_FILE_SIZE, str(4 * 1024))  # several output files, so ranges can separate
    try:
        spark.sql(f"OPTIMIZE delta.`{path}`").collect()
    finally:
        spark.conf.unset(MAX_FILE_SIZE)
    optimized = active_files(path)
    assert len(optimized) > 1
    assert all(add.get("clusteringProvider") == "liquid" for add in optimized)
    assert overlapping_pairs(optimized) <= len(optimized) - 1 < overlapping_pairs(appended)
    parameters = delta_table(spark, path).history(1).collect()[0]["operationParameters"]
    assert parameters["clusterBy"] == '["id"]'

    append(spark, path, seeded_rows(9, 1000, 4000), "id BIGINT, pad STRING", files=2)
    optimized_paths = {a["path"] for a in optimized}
    later = [add for add in active_files(path) if add["path"] not in optimized_paths]
    assert len(later) == 2 and all(add.get("clusteringProvider") is None for add in later)

    spark.createDataFrame(seeded_rows(10, 2000, 4000), "id BIGINT, pad STRING").repartition(
        3
    ).createOrReplaceTempView("unclustered_rows")
    ctas = tmp_path / "ctas"
    spark.sql(
        f"CREATE TABLE delta.`{ctas}` USING delta CLUSTER BY (id) AS SELECT * FROM unclustered_rows"
    )
    assert all(add.get("clusteringProvider") is None for add in active_files(ctas))

    # With the default maxFileSize a table this small is compacted into ONE file: tagged as
    # clustered, with no second file for a query to skip.
    small = tmp_path / "small"
    spark.sql(f"CREATE TABLE delta.`{small}` (id BIGINT, pad STRING) USING delta CLUSTER BY (id)")
    for seed in range(4):
        append(spark, small, seeded_rows(seed, 1000, 4000), "id BIGINT, pad STRING", files=2)
    spark.sql(f"OPTIMIZE delta.`{small}`").collect()
    [single] = active_files(small)
    assert single["clusteringProvider"] == "liquid"


def test_h_clustering_cannot_be_combined_with_partitioning(spark: Any, tmp_path: Path) -> None:
    from delta.tables import DeltaTable

    with pytest.raises(Exception, match="Clustering and partitioning cannot both be specified"):
        spark.sql(
            f"CREATE TABLE delta.`{tmp_path / 'sql'}` (id BIGINT, d STRING) USING delta "
            f"PARTITIONED BY (d) CLUSTER BY (id)"
        )
    with pytest.raises(Exception, match="DELTA_CLUSTER_BY_WITH_PARTITIONED_BY"):
        (
            DeltaTable.create(spark)
            .location(str(tmp_path / "builder"))
            .addColumn("id", "BIGINT")
            .addColumn("d", "STRING")
            .partitionedBy("d")
            .clusterBy("id")
            .execute()
        )
    partitioned = tmp_path / "partitioned"
    spark.sql(
        f"CREATE TABLE delta.`{partitioned}` (id BIGINT, d STRING) USING delta PARTITIONED BY (d)"
    )
    with pytest.raises(
        Exception, match="DELTA_ALTER_TABLE_CLUSTER_BY_ON_PARTITIONED_TABLE_NOT_ALLOWED"
    ):
        spark.sql(f"ALTER TABLE delta.`{partitioned}` CLUSTER BY (id)")
    clustered = tmp_path / "clustered"
    spark.sql(f"CREATE TABLE delta.`{clustered}` (id BIGINT) USING delta CLUSTER BY (id)")
    with pytest.raises(Exception, match="DELTA_CLUSTERING_WITH_ZORDER_BY"):
        spark.sql(f"OPTIMIZE delta.`{clustered}` ZORDER BY (id)").collect()
    with pytest.raises(TableDeclarationError):  # the convention refuses it before Delta must
        TableLayout(partition_columns=("d",), clustering_columns=("id",))


def test_i_optimize_zorder_by_rewrites_only_the_selected_partition(
    spark: Any, tmp_path: Path
) -> None:
    path = tmp_path / "partitioned"
    spark.sql(
        f"CREATE TABLE delta.`{path}` (id BIGINT, bucket INT, pad STRING) USING delta "
        f"PARTITIONED BY (bucket)"
    )
    for seed in range(4):
        rows = [(i, seed % 2, pad) for i, pad in seeded_rows(seed, 900, 4000)]
        append(spark, path, rows, "id BIGINT, bucket INT, pad STRING", files=3)

    def by_bucket() -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for add in active_files(path):
            grouped.setdefault(add["partitionValues"]["bucket"], []).append(add)
        return grouped

    before = by_bucket()
    spark.conf.set(MAX_FILE_SIZE, str(4 * 1024))
    try:
        spark.sql(f"OPTIMIZE delta.`{path}` WHERE bucket = 1 ZORDER BY (id)").collect()
    finally:
        spark.conf.unset(MAX_FILE_SIZE)
    after = by_bucket()

    assert {a["path"] for a in after["0"]} == {a["path"] for a in before["0"]}  # untouched
    assert not {a["path"] for a in after["1"]} & {a["path"] for a in before["1"]}  # rewritten
    assert len(after["1"]) > 1
    assert overlapping_pairs(after["1"]) <= len(after["1"]) - 1 < overlapping_pairs(before["1"])
    parameters = delta_table(spark, path).history(1).collect()[0]["operationParameters"]
    assert parameters["zOrderBy"] == '["id"]'
    with pytest.raises(Exception, match="DELTA_ZORDERING_ON_PARTITION_COLUMN"):
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (bucket)").collect()
    live = tables.describe_live_table(spark, f"delta.`{path}`")
    assert (live.min_reader_version, live.min_writer_version) == (1, 2)
    assert live.table_features == tables.BASELINE_FEATURES  # Z-ordering adds no feature


# ----------------------------------------------------------- (j) measurement ---


@pytest.fixture
def eight_blocks(spark: Any, tmp_path: Path) -> Path:
    """Eight files, each holding exactly ids [block * 1000, block * 1000 + 999]."""
    path = tmp_path / "measured"
    for block in range(8):
        append(
            spark,
            path,
            [(block * 1000 + j, "p" * 200) for j in range(1000)],
            "id BIGINT, pad STRING",
        )
    return path


def test_j_selection_and_reads_are_measured_from_the_plan_that_ran(
    spark: Any, eight_blocks: Path
) -> None:
    from pyspark.sql import functions

    adds = active_files(eight_blocks)
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{eight_blocks}`").collect()[0]
    frame = spark.read.format("delta").load(str(eight_blocks))

    full = tables.measure_scans(frame)
    assert (full.rows, full.scans, full.scan_output_rows, full.input_records) == (
        8000,
        1,
        8000,
        8000,
    )
    assert full.selected_files == detail["numFiles"] == len(adds)
    assert full.selected_bytes == detail["sizeInBytes"] == sum(int(a["size"]) for a in adds)
    assert full.input_bytes > 0

    pruned = tables.measure_scans(frame.where("id < 2000"))
    expected = [a for a in adds if id_range(a)[0] < 2000]
    assert (pruned.rows, pruned.input_records, len(expected)) == (2000, 2000, 2)
    assert pruned.selected_files == len(expected)
    assert pruned.selected_bytes == sum(int(a["size"]) for a in expected)
    assert 0 < pruned.input_bytes < full.input_bytes

    counted = tables.measure_scans(frame.groupBy().count())
    assert counted.rows == 1  # answered from the log's statistics:
    assert (counted.scans, counted.selected_files, counted.input_bytes) == (0, 0, 0)

    summed = tables.measure_scans(frame.agg(functions.sum("id")))  # through adaptive stages
    assert (summed.scans, summed.selected_files, summed.input_records) == (1, len(adds), 8000)


def test_j_selected_bytes_are_blind_to_column_pruning_and_row_group_skipping(
    spark: Any, eight_blocks: Path, tmp_path: Path
) -> None:
    """CONTRADICTS the first round's claim that `filesSize` is bytes read: it is the size of
    the files a scan selected. Actual reads come from task metrics."""
    import pyarrow.parquet as pq

    frame = spark.read.format("delta").load(str(eight_blocks))
    every_column = tables.measure_scans(frame)
    one_column = tables.measure_scans(frame.select("id"))
    assert one_column.selected_bytes == every_column.selected_bytes
    assert one_column.input_bytes <= every_column.input_bytes

    grouped = tmp_path / "row_groups"
    spark.conf.set("parquet.block.size", str(64 * 1024))
    try:
        spark.range(0, 200_000).selectExpr("id", "cast(id as string) as s").coalesce(
            1
        ).write.format("delta").save(str(grouped))
    finally:
        spark.conf.unset("parquet.block.size")
    [data_file] = active_files(grouped)
    assert pq.ParquetFile(grouped / data_file["path"]).num_row_groups > 1

    lookup = tables.measure_scans(
        spark.read.format("delta").load(str(grouped)).where("id = 150000")
    )
    assert (lookup.rows, lookup.selected_files) == (1, 1)
    assert lookup.selected_bytes == int(data_file["size"])  # the whole file is "selected" ...
    assert lookup.scan_output_rows < 200_000  # ... while row groups were skipped ...
    assert lookup.input_bytes < lookup.selected_bytes  # ... and far less was read


def test_j_reused_exchanges_count_once_and_an_unattributable_plan_is_refused(
    spark: Any, eight_blocks: Path
) -> None:
    frame = spark.read.format("delta").load(str(eight_blocks))
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try:
        left = frame.where("id < 3000")
        joined = left.alias("l").join(left.alias("r"), "id")
        reused = tables.measure_scans(joined)
        assert "ReusedExchange" in joined._jdf.queryExecution().executedPlan().toString()
        spark.conf.set("spark.sql.exchange.reuse", "false")
        try:
            separate = tables.measure_scans(left.alias("l").join(left.alias("r"), "id"))
        finally:
            spark.conf.unset("spark.sql.exchange.reuse")
        assert (reused.rows, separate.rows) == (3000, 3000)
        assert (reused.scans, separate.scans) == (1, 2)
        assert separate.input_records == 2 * reused.input_records == 6000
        assert separate.selected_files == 2 * reused.selected_files == 6

        empty_side = frame.where("pad = 'nope'").join(frame, "id")
        with pytest.raises(ScanMeasurementError, match="final plan lacks"):
            tables.measure_scans(empty_side)
    finally:
        spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
    with pytest.raises(ScanMeasurementError, match="CollectLimitExec"):
        tables.measure_scans(frame.limit(5))


# ------------------------------------------------------------ (k) protocol ---

BASELINE = set(tables.BASELINE_FEATURES)
PROTOCOL_CASES: dict[str, tuple[str, str, tuple[int, int], set[str]]] = {
    "plain": ("(id BIGINT)", "", (1, 2), BASELINE),
    "append_only": ("(id BIGINT)", "TBLPROPERTIES ('delta.appendOnly'='true')", (1, 2), BASELINE),
    "not_null": ("(id BIGINT NOT NULL)", "", (1, 2), BASELINE),
    "check_constraint_at_creation": (
        "(id BIGINT NOT NULL)",
        "TBLPROPERTIES ('delta.appendOnly'='true', 'delta.constraints.id_positive'='id > 0')",
        (1, 3),
        BASELINE | {"checkConstraints"},
    ),
    "partitioned": ("(id BIGINT, d STRING)", "PARTITIONED BY (d)", (1, 2), BASELINE),
    "clustered": (
        "(id BIGINT)",
        "CLUSTER BY (id)",
        (1, 7),
        BASELINE | {"clustering", "domainMetadata"},
    ),
    "deletion_vectors": (
        "(id BIGINT)",
        "TBLPROPERTIES ('delta.enableDeletionVectors'='true')",
        (3, 7),
        BASELINE | {"deletionVectors"},
    ),
    "feature_deletion_vectors": (
        "(id BIGINT)",
        "TBLPROPERTIES ('delta.feature.deletionVectors'='supported')",
        (3, 7),
        BASELINE | {"deletionVectors"},
    ),
    "column_mapping": (
        "(id BIGINT)",
        "TBLPROPERTIES ('delta.columnMapping.mode'='name')",
        (2, 7),
        BASELINE | {"columnMapping"},
    ),
    "type_widening": (
        "(id INT)",
        "TBLPROPERTIES ('delta.enableTypeWidening'='true')",
        (3, 7),
        BASELINE | {"typeWidening"},
    ),
    "change_data_feed": (
        "(id BIGINT)",
        "TBLPROPERTIES ('delta.enableChangeDataFeed'='true')",
        (1, 7),
        BASELINE | {"changeDataFeed"},
    ),
    "row_tracking": (
        "(id BIGINT)",
        "TBLPROPERTIES ('delta.enableRowTracking'='true')",
        (1, 7),
        BASELINE | {"domainMetadata", "rowTracking"},
    ),
}


@pytest.mark.parametrize("case", sorted(PROTOCOL_CASES))
def test_k_the_protocol_and_features_each_table_property_produces(
    spark: Any, tmp_path: Path, case: str
) -> None:
    columns, clause, protocol, features = PROTOCOL_CASES[case]
    identifier = f"delta.`{tmp_path / case}`"
    spark.sql(f"CREATE TABLE {identifier} {columns} USING delta {clause}")
    live = tables.describe_live_table(spark, identifier)
    assert (live.min_reader_version, live.min_writer_version) == protocol
    assert live.table_features == features
    assert delta_table(spark, tmp_path / case).history().count() == 1  # one commit, always
    reader, writer = tables.protocol_ceiling(live.table_features)
    assert protocol[0] <= reader and protocol[1] <= writer


@pytest.mark.parametrize("api", ["sql", "builder"])
@pytest.mark.parametrize(
    ("nullable", "protocol", "features"), [(True, (1, 1), set()), (False, (1, 7), {"invariants"})]
)
def test_k_a_protocol_property_yields_a_protocol_set_by_not_null_not_by_the_api(
    spark: Any,
    tmp_path: Path,
    api: str,
    nullable: bool,
    protocol: tuple[int, int],
    features: set[str],
) -> None:
    """Observed: under delta.minWriterVersion=7 the protocol follows whether a column is NOT
    NULL, identically through SQL and the builder -- neither is the (1, 2) baseline, and the
    NOT NULL table exceeds the ceiling its features need, which drift reports."""
    from delta.tables import DeltaTable

    path = tmp_path / "table"
    if api == "sql":
        column = "id BIGINT" if nullable else "id BIGINT NOT NULL"
        spark.sql(
            f"CREATE TABLE delta.`{path}` ({column}) USING delta "
            f"TBLPROPERTIES ('delta.minWriterVersion'='7')"
        )
    else:
        (
            DeltaTable.create(spark)
            .location(str(path))
            .addColumn("id", "BIGINT", nullable=nullable)
            .property("delta.minWriterVersion", "7")
            .execute()
        )
    live = tables.describe_live_table(spark, f"delta.`{path}`")
    assert (live.min_reader_version, live.min_writer_version) == protocol
    assert live.table_features == features
    ceiling = tables.protocol_ceiling(live.table_features)
    assert (live.min_writer_version > ceiling[1]) == (not nullable)


def test_k_allowed_properties_add_no_protocol_feature(spark: Any, tmp_path: Path) -> None:
    from pyspark.sql.types import LongType, StructField, StructType

    declaration = TableDeclaration(
        ref=TableRef(Tier.BRONZE, "allowed_properties"),
        schema=StructType([StructField("id", LongType(), nullable=False)]),
        properties={
            "delta.appendOnly": "true",
            "delta.checkpointInterval": "10",
            "delta.logRetentionDuration": "interval 30 days",
            "delta.deletedFileRetentionDuration": "interval 7 days",
            "delta.dataSkippingNumIndexedCols": "8",
            "delta.dataSkippingStatsColumns": "id",
        },
    )
    assert set(declaration.properties) == set(tables.ALLOWED_PROPERTIES)
    stamp = CommitProvenance(SHA, False, "table_setup")
    live = tables.create_table(spark, declaration, LakeConfig.at(tmp_path), stamp)
    assert (live.min_reader_version, live.min_writer_version) == (1, 2)
    assert live.table_features == tables.BASELINE_FEATURES


def test_k_a_table_created_implicitly_stores_every_column_as_nullable(
    spark: Any, tmp_path: Path
) -> None:
    """Observed: NOT NULL survives only DDL or the table builder. A DataFrame write, a
    `writeTo().create()` and a streaming sink's first batch each create an all-nullable table,
    even from a non-nullable schema -- so tables are created from declarations, never by a
    query, and drift reports it when they were not."""
    from pyspark.sql.types import LongType, StructField, StructType

    schema = StructType([StructField("id", LongType(), nullable=False)])
    declaration = TableDeclaration(ref=TableRef(Tier.BRONZE, "nullability_spike"), schema=schema)
    frame = spark.createDataFrame([(1,)], schema)
    assert frame.schema.fields[0].nullable is False  # the precondition

    def stored_nullability(path: Path) -> bool:
        return bool(spark.read.format("delta").load(str(path)).schema.fields[0].nullable)

    written = tmp_path / "written"
    frame.write.format("delta").save(str(written))
    created = tmp_path / "write_to"
    frame.writeTo(f"delta.`{created}`").using("delta").create()
    source = tmp_path / "source"
    spark.sql(f"CREATE TABLE delta.`{source}` (id BIGINT NOT NULL) USING delta")
    spark.sql(f"INSERT INTO delta.`{source}` VALUES (1)")
    stream = spark.readStream.format("delta").load(str(source))
    assert stream.schema.fields[0].nullable is False  # the precondition
    sunk = tmp_path / "sunk"
    stream.writeStream.format("delta").option("checkpointLocation", str(tmp_path / "cp")).trigger(
        availableNow=True
    ).start(str(sunk)).awaitTermination()

    for path in (written, created, sunk):
        assert stored_nullability(path) is True, path.name
        [drift] = tables.check_drift(
            declaration, tables.describe_live_table(spark, f"delta.`{path}`")
        )
        assert drift.aspect == "schema" and "nullable=True" in drift.detail

    declared = tmp_path / "declared"
    spark.sql(f"CREATE TABLE delta.`{declared}` (id BIGINT NOT NULL) USING delta")
    frame.write.format("delta").mode("append").save(str(declared))
    assert stored_nullability(declared) is False
    live = tables.describe_live_table(spark, f"delta.`{declared}`")
    assert tables.check_drift(declaration, live) == ()


def test_k_a_table_is_created_from_its_declaration_in_one_commit_and_drift_is_refused(
    spark: Any, tmp_path: Path
) -> None:
    from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

    lake = LakeConfig.at(tmp_path / "lake")
    schema = StructType(
        [StructField("id", LongType(), nullable=False), StructField("amount_minor", LongType())]
    )
    declaration = TableDeclaration(
        ref=TableRef(Tier.SILVER, "declared_spike"),
        schema=schema,
        properties={"delta.appendOnly": "true"},
        check_constraints=(CheckConstraint("amount_nonneg", "amount_minor >= 0"),),
    )
    stamp = CommitProvenance(SHA, False, "table_setup")
    path = declaration.ref.local_path(lake)

    spark.conf.set(f"{tables.SESSION_PROPERTY_DEFAULTS_PREFIX}enableDeletionVectors", "true")
    try:
        with pytest.raises(TableDeclarationError, match="copies them into every new table"):
            tables.create_table(spark, declaration, lake, stamp)
    finally:
        spark.conf.unset(f"{tables.SESSION_PROPERTY_DEFAULTS_PREFIX}enableDeletionVectors")
    assert not path.exists()

    live = tables.create_table(spark, declaration, lake, stamp)
    assert tables.check_drift(declaration, live) == ()
    assert (live.min_reader_version, live.min_writer_version) == (1, 3)
    [creation] = delta_table(spark, path).history().collect()
    assert creation["operation"] == "CREATE TABLE"
    assert CommitProvenance.from_user_metadata(creation["userMetadata"]) == stamp
    assert spark.conf.get(tables.USER_METADATA_CONF, None) is None  # the stamp did not leak
    with pytest.raises(Exception, match="DELTA_VIOLATE_CONSTRAINT_WITH_VALUES"):
        append(spark, path, [(1, -1)], "id BIGINT, amount_minor BIGINT")
    assert tables.create_table(spark, declaration, lake, stamp) == live  # exists and matches
    assert latest_version(spark, path) == 0  # ... and committed nothing

    spark.sql(
        f"ALTER TABLE {declaration.ref.path_identifier(lake)} "
        f"SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')"
    )
    with pytest.raises(TableDriftError) as raised:
        tables.create_table(spark, declaration, lake, stamp)
    for aspect in ("feature", "protocol", "property"):
        assert f"- {aspect}:" in str(raised.value)

    clustered = TableDeclaration(
        ref=TableRef(Tier.GOLD, "clustered_spike"),
        schema=schema,
        layout=TableLayout(clustering_columns=("id",)),
    )
    clustered_live = tables.create_table(spark, clustered, lake, stamp)
    assert clustered_live.clustering_columns == ("id",)
    assert tables.check_drift(clustered, clustered_live) == ()

    nested = TableDeclaration(
        ref=TableRef(Tier.SILVER, "nested_spike"),
        schema=StructType(
            [
                StructField("tags", ArrayType(StringType(), containsNull=False), nullable=False),
                StructField("s", StructType([StructField("x", LongType(), nullable=False)])),
            ]
        ),
    )
    assert tables.check_drift(nested, tables.create_table(spark, nested, lake, stamp)) == ()


# ------------------------------------------------------------ (l) state store ---


def test_l_rocksdb_changelog_state_resumes_from_its_checkpoint_without_double_counting(
    spark: Any, tmp_path: Path
) -> None:
    provider = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
    changelog = "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled"
    assert spark.conf.get("spark.sql.streaming.stateStore.providerClass") == provider
    assert spark.conf.get(changelog) == "true"
    source, counts, checkpoint = tmp_path / "source", tmp_path / "counts", tmp_path / "cp"

    def add(keys: list[str]) -> None:
        append(spark, source, [(key,) for key in keys], "k STRING")

    def overwrite(frame: Any, batch_id: int) -> None:
        frame.write.format("delta").mode("overwrite").save(str(counts))

    def run() -> Any:
        query = (
            spark.readStream.format("delta")
            .load(str(source))
            .groupBy("k")
            .count()
            .writeStream.outputMode("complete")
            .foreachBatch(overwrite)
            .option("checkpointLocation", str(checkpoint))
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        return query

    first_keys, second_keys = ["a", "a", "b"], ["a", "c"]
    add(first_keys)
    first = run()
    assert sum(event["numInputRows"] for event in progress(first)) == len(first_keys)
    state_files = [p for p in (checkpoint / "state").rglob("*") if p.is_file()]
    assert any(p.suffix == ".changelog" for p in state_files)  # changelog checkpointing in use
    offsets_conf = json.loads((checkpoint / "offsets" / "0").read_text().splitlines()[1])["conf"]
    assert offsets_conf["spark.sql.streaming.stateStore.providerClass"] == provider

    # Unload every state store in this JVM, so the next run can only rebuild from the checkpoint.
    state_store = spark.sparkContext._jvm.org.apache.spark.sql.execution.streaming.state.StateStore
    state_store.stop()
    assert not state_store.isMaintenanceRunning()

    add(second_keys)
    second = run()
    assert sum(event["numInputRows"] for event in progress(second)) == len(second_keys)
    counted = {
        row["k"]: row["count"] for row in spark.read.format("delta").load(str(counts)).collect()
    }
    assert counted == Counter(first_keys + second_keys)
    [operator] = progress(second)[-1]["stateOperators"]
    assert operator["numRowsTotal"] == len(set(first_keys + second_keys))
