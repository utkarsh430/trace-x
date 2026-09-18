"""`P3.resource-bounds` on real Delta 4.0.1: Bronze's audited retention floor, Silver reading
through it, crashes at every maintenance step, and the VACUUM and OPTIMIZE guards (ADR-0052
amendment 1).

Bronze is written by `resource_bounds_lake.BronzeWriter`, through the checkpoint convention's own
append; Silver is the real query; conservation and maintenance are the code under test. Marker
`stream`: skipped loudly when the toolchain does not match its pins.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql.types import LongType, StructField, StructType
from tests.stream.resource_bounds_lake import SHA, TOPIC, TOPIC_ID, BronzeWriter, run_silver

from trace_core.contracts.topics import TX_SCORED_V1
from trace_core.domain.errors import (
    CheckpointRefusedError,
    StreamingSourceRetentionError,
    TableDriftError,
)
from trace_core.stream import checkpoints
from trace_core.stream.bronze import Trigger, bronze_declaration, start_bronze_query
from trace_core.stream.bronze_conservation import ConservationReport, check_conservation
from trace_core.stream.bronze_coverage import assess_bronze_coverage
from trace_core.stream.checkpoints import DeltaSourceStart
from trace_core.stream.lake import LakeConfig, Tier
from trace_core.stream.maintenance import (
    RETENTION_AUDIT,
    RETENTION_CHECK_CONF,
    RetentionRefusedError,
    RetentionReport,
    Step,
    advance_retention,
    declared_tables,
    optimize_table,
    reset_silver,
    vacuum_table,
)
from trace_core.stream.silver import silver_sources, silver_targets, start_silver_query
from trace_core.stream.silver_conservation import check_silver_conservation
from trace_core.stream.silver_rules import silver_topic
from trace_core.stream.tables import (
    DECLARED_RETENTION,
    DELETED_FILE_RETENTION_PROPERTY,
    LOG_RETENTION_PROPERTY,
    CommitProvenance,
    TableDeclaration,
    TableRef,
    check_drift,
    create_table,
    describe_live_table,
    retention_hours,
    snapshot_facts,
)

pytestmark = pytest.mark.stream

FLOORS = {0: 8, 1: 8, 2: 8}
"""The end of the second of three batches of four records per partition."""


@pytest.fixture(scope="module")
def spark() -> Iterator[Any]:
    from trace_core.stream.session import build_session

    session = build_session("trace-x-resource-bounds")
    try:
        yield session
    finally:
        session.stop()


def _lake(tmp_path: Path) -> LakeConfig:
    return LakeConfig.at(tmp_path / "lake")


def _three_batches(spark: Any, lake: LakeConfig) -> BronzeWriter:
    writer = BronzeWriter(spark, lake)
    writer.append({0: 4, 1: 4, 2: 4})
    writer.append({0: 4, 1: 4, 2: 4}, files=2)
    writer.append({0: 4, 1: 4, 2: 4})
    return writer


def _advance(
    spark: Any,
    lake: LakeConfig,
    floors: dict[int, int] | None,
    after_step: Callable[[Step], None] | None = None,
) -> RetentionReport:
    return advance_retention(
        spark,
        lake,
        TOPIC,
        requested_floors=floors,
        broker_topic_id=TOPIC_ID,
        git_sha=SHA,
        dirty_worktree=False,
        now=dt.datetime.now(dt.UTC),
        after_step=after_step,
    )


def _bronze(spark: Any, lake: LakeConfig) -> ConservationReport:
    return check_conservation(spark, lake, TOPIC, broker_topic_id=TOPIC_ID)


def _audit(spark: Any, lake: LakeConfig) -> list[Any]:
    path = RETENTION_AUDIT.local_path(lake)
    if snapshot_facts(spark, path) is None:
        return []
    return spark.read.format("delta").load(str(path)).collect()


def _version(spark: Any, writer: BronzeWriter) -> int:
    facts = snapshot_facts(spark, writer.path)
    assert facts is not None
    return facts.version


def _actions(path: Path, version: int) -> list[str]:
    commit = path / "_delta_log" / f"{version:020d}.json"
    kinds = []
    for line in commit.read_text().splitlines():
        action = json.loads(line)
        for kind, body in action.items():
            kinds.append(f"{kind}:{body.get('dataChange')}" if kind in ("add", "remove") else kind)
    return kinds


# ---------------------------------------------------------------- retention ---


def test_resource_bounds_delta_default_retention_is_the_declared_retention(
    spark: Any, tmp_path: Path
) -> None:
    """Observed: a table without either property retains 30 days of log and 7 days of removed
    files, which is what `DECLARED_RETENTION` declares, so the drift check may read an absent
    property as the declared value."""
    path = tmp_path / "bare"
    spark.sql(f"CREATE TABLE delta.`{path}` (id BIGINT) USING delta")
    jvm = spark.sparkContext._jvm
    snapshot = jvm.org.apache.spark.sql.delta.DeltaLog.forTable(
        spark._jsparkSession, str(path)
    ).update(False, jvm.scala.Option.empty(), jvm.scala.Option.empty())
    configs = jvm.org.apache.spark.sql.delta.DeltaConfigs
    metadata = snapshot.metadata()
    observed = (
        int(configs.getMilliSeconds(configs.LOG_RETENTION().fromMetaData(metadata))),
        int(configs.getMilliSeconds(configs.TOMBSTONE_RETENTION().fromMetaData(metadata))),
    )
    declared = (
        retention_hours(DECLARED_RETENTION[LOG_RETENTION_PROPERTY]) * 3_600_000,
        retention_hours(DECLARED_RETENTION[DELETED_FILE_RETENTION_PROPERTY]) * 3_600_000,
    )
    assert observed == declared
    live = describe_live_table(spark, f"delta.`{path}`")
    assert not set(DECLARED_RETENTION) & set(live.properties)


def test_resource_bounds_every_declared_table_is_created_with_its_declared_retention(
    spark: Any, tmp_path: Path
) -> None:
    lake = _lake(tmp_path)
    stamp = CommitProvenance(SHA, False, "resource_bounds_declarations")
    for ref, declaration in declared_tables().items():
        live = create_table(spark, declaration, lake, stamp)
        assert {k: live.properties.get(k) for k in DECLARED_RETENTION} == (
            declaration.retention_properties()
        ), ref
        assert check_drift(declaration, live) == (), ref


# ---------------------------------------------------------- reading through ---


def test_resource_bounds_silver_reads_through_a_retention_delete_and_every_row_stays_accounted(
    spark: Any, tmp_path: Path
) -> None:
    lake = _lake(tmp_path)
    writer = _three_batches(spark, lake)
    run_silver(spark, lake)
    running = start_silver_query(
        spark,
        lake,
        TOPIC,
        git_sha=SHA,
        dirty_worktree=False,
        now=dt.datetime.now(dt.UTC),
        trigger=Trigger(interval_s=0.5),
    )
    try:
        running.query.processAllAvailable()
        report = _advance(spark, lake, FLOORS)
        assert report.rows_deleted == 24 and report.files_deleted >= 2, report.summary()
        assert report.delete_commit_version is not None
        kinds = _actions(writer.path, report.delete_commit_version)
        assert kinds.count("remove:True") == report.files_deleted
        assert not [k for k in kinds if k.startswith("add")], "a removes-only commit"
        writer.append({0: 2, 1: 2, 2: 2})
        running.query.processAllAvailable()
        assert running.query.exception() is None
    finally:
        running.query.stop()

    bronze = _bronze(spark, lake)
    assert bronze.conserved, bronze.summary()
    assert dict(bronze.floors) == {(TOPIC_ID, p): 8 for p in range(3)}
    assert all((v.missing, v.retired_present) == (0, 0) for v in bronze.partitions)
    silver = check_silver_conservation(spark, lake, TOPIC)
    assert silver.conserved, silver.summary()
    assert silver.bronze_rows == 18, "batches 2 and 3; the retired rows are left out"
    canonical = spark.read.format("delta").load(str(silver_topic(TOPIC).table.local_path(lake)))
    assert canonical.count() == 42, "Silver keeps every event it admitted, retired or not"

    audit = _audit(spark, lake)
    floors = sorted(
        (r["kafka_partition"], r["new_floor"]) for r in audit if r["event"] == "floor_advanced"
    )
    assert floors == [(0, 8), (1, 8), (2, 8)]
    deletes = [r for r in audit if r["event"] == "rows_deleted"]
    assert [(r["bronze_version"], r["rows_deleted"]) for r in deletes] == [
        (report.delete_commit_version, 24)
    ]
    assert not any(r["reconciled"] for r in audit)

    version = _version(spark, writer)
    again = _advance(spark, lake, FLOORS)
    assert (again.floor_commit_version, again.delete_commit_version, again.audit_rows_written) == (
        None,
        None,
        0,
    )
    assert _version(spark, writer) == version, "an idempotent re-run commits nothing"


CRASHES = [
    Step.EVIDENCE,
    Step.FLOOR_COMMITTED,
    Step.APPEND_ONLY_LIFTED,
    Step.DELETED,
    Step.APPEND_ONLY_RESTORED,
]


class CrashError(Exception):
    """The process dies at a step boundary."""


@pytest.mark.parametrize("step", CRASHES, ids=lambda s: s.value)
def test_resource_bounds_a_crash_at_each_maintenance_step_is_judged_correctly_by_conservation(
    spark: Any, tmp_path: Path, step: Step
) -> None:
    lake = _lake(tmp_path)
    _three_batches(spark, lake)
    run_silver(spark, lake)

    def crash(reached: Step) -> None:
        if reached is step:
            raise CrashError(step.value)

    with pytest.raises(CrashError):
        _advance(spark, lake, FLOORS, after_step=crash)
    floor_committed = step is not Step.EVIDENCE
    deleted = step in (Step.DELETED, Step.APPEND_ONLY_RESTORED)
    lifted = step in (Step.APPEND_ONLY_LIFTED, Step.DELETED)

    bronze = _bronze(spark, lake)
    assert dict(bronze.floors) == ({(TOPIC_ID, p): 8 for p in range(3)} if floor_committed else {})
    assert bronze.append_only_lifted is lifted
    assert bronze.conserved_rows, bronze.summary()
    assert bronze.conserved is not lifted
    for verdict in bronze.partitions:
        assert (verdict.missing, verdict.duplicates, verdict.out_of_range) == (0, 0, 0)
        assert verdict.retired_present == (8 if floor_committed and not deleted else 0), verdict
    assert check_silver_conservation(spark, lake, TOPIC).conserved
    assert _audit(spark, lake) == [], "the audit is written after the last Bronze commit"
    if lifted:
        with pytest.raises(TableDriftError, match=r"delta\.appendOnly"):
            start_bronze_query(
                spark,
                lake,
                TOPIC,
                bootstrap_servers="127.0.0.1:9",
                git_sha=SHA,
                dirty_worktree=False,
                now=dt.datetime.now(dt.UTC),
                trigger=Trigger(available_now=True),
            )

    report = _advance(spark, lake, FLOORS)
    assert report.append_only_restored is (step is not Step.APPEND_ONLY_RESTORED)
    create_table(spark, bronze_declaration(TOPIC), lake, CommitProvenance(SHA, False, "probe"))
    bronze = _bronze(spark, lake)
    assert bronze.conserved, bronze.summary()
    assert all(v.retired_present == 0 for v in bronze.partitions)
    audit = _audit(spark, lake)
    floor_rows = [r for r in audit if r["event"] == "floor_advanced"]
    delete_rows = [r for r in audit if r["event"] == "rows_deleted"]
    assert sorted(r["kafka_partition"] for r in floor_rows) == [0, 1, 2]
    assert len(delete_rows) == 1 and delete_rows[0]["rows_deleted"] == 24
    assert {r["reconciled"] for r in floor_rows} == {floor_committed}
    assert delete_rows[0]["reconciled"] is deleted
    third = _advance(spark, lake, FLOORS)
    assert (
        third.floor_commit_version,
        third.delete_commit_version,
        third.audit_rows_written,
        third.append_only_restored,
    ) == (None, None, 0, False)


# ------------------------------------------------------------------ refusals ---


def test_resource_bounds_maintenance_never_passes_a_consumer_and_commits_nothing_when_refused(
    spark: Any, tmp_path: Path
) -> None:
    lake = _lake(tmp_path)
    writer = _three_batches(spark, lake)
    before = _version(spark, writer)
    with pytest.raises(RetentionRefusedError, match="no Silver checkpoint has read"):
        _advance(spark, lake, FLOORS)
    run_silver(spark, lake)
    writer.append({0: 4, 1: 4, 2: 4})
    before = _version(spark, writer)
    query = silver_topic(TOPIC).query
    with pytest.raises(RetentionRefusedError, match=f"Silver checkpoint {query} v1 has read whole"):
        _advance(spark, lake, {0: 16})
    with pytest.raises(RetentionRefusedError, match="where Bronze's checkpoint has consumed to"):
        _advance(spark, lake, {0: 17})
    assert _version(spark, writer) == before and not _bronze(spark, lake).floors
    _advance(spark, lake, {0: 8})
    with pytest.raises(RetentionRefusedError, match="never lowered"):
        _advance(spark, lake, {0: 4})


def test_resource_bounds_ignore_deletes_is_granted_to_bronze_topic_sources_only(
    spark: Any, tmp_path: Path
) -> None:
    lake = _lake(tmp_path)
    writer = BronzeWriter(spark, lake)
    writer.append({0: 1})
    target = TableRef(Tier.SILVER, "allowance_target")
    create_table(
        spark,
        TableDeclaration(ref=target, schema=StructType([StructField("id", LongType())])),
        lake,
        CommitProvenance(SHA, False, "allowance_probe"),
    )

    def opened(query: str, source: TableRef) -> Any:
        return checkpoints.open_checkpoint(
            spark,
            lake,
            query,
            targets=[target],
            sources=[DeltaSourceStart(source, 0)],
            git_sha=SHA,
            dirty_worktree=False,
            now=dt.datetime.now(dt.UTC),
        )

    bronze = opened("allowance_probe", writer.spec.table)
    bronze.delta_source(spark, writer.spec.table)
    bronze.delta_source(spark, writer.spec.table, {"ignoreDeletes": "true"})
    with pytest.raises(CheckpointRefusedError, match="may not be turned off"):
        bronze.delta_source(spark, writer.spec.table, {"ignoreDeletes": "false"})
    for option in ("skipChangeCommits", "ignoreChanges"):
        with pytest.raises(StreamingSourceRetentionError, match=option):
            bronze.delta_source(spark, writer.spec.table, {option: "true"})
    other = TableRef(Tier.BRONZE, "not_a_topic_table")
    spark.sql(f"CREATE TABLE {other.path_identifier(lake)} (id BIGINT) USING delta")
    with pytest.raises(StreamingSourceRetentionError, match="ignoreDeletes"):
        opened("allowance_other", other).delta_source(spark, other, {"ignoreDeletes": "true"})


def test_resource_bounds_a_silver_reset_after_retention_starts_at_the_first_live_version(
    spark: Any, tmp_path: Path
) -> None:
    lake = _lake(tmp_path)
    writer = _three_batches(spark, lake)
    run_silver(spark, lake)
    _advance(spark, lake, FLOORS)
    writer.append({0: 2, 1: 2, 2: 2})
    run_silver(spark, lake)
    first_live = writer.versions[2]
    spec = silver_topic(TOPIC)
    checkpoints.reset_checkpoint(
        spark,
        lake,
        spec.query,
        targets=silver_targets(TOPIC),
        sources=silver_sources(TOPIC, 0),
        reason="resource_bounds: a reset from version 0 after retention",
        now=dt.datetime.now(dt.UTC),
    )
    with pytest.raises(StreamingSourceRetentionError, match=f"must start at version {first_live}"):
        run_silver(spark, lake)
    identity = reset_silver(
        spark,
        lake,
        TOPIC,
        reason="resource_bounds: reset after retention",
        now=dt.datetime.now(dt.UTC),
    )
    assert identity.sources[f"delta:{writer.spec.table}"].start == f"version:{first_live}"
    run_silver(spark, lake)
    silver = check_silver_conservation(spark, lake, TOPIC)
    assert silver.conserved, silver.summary()
    assert (silver.checkpoint_version, silver.bronze_rows) == (identity.version, 18)


def test_resource_bounds_vacuum_waits_for_silver_and_retention_and_optimize_skips_bronze(
    spark: Any, tmp_path: Path
) -> None:
    lake = _lake(tmp_path)
    writer = _three_batches(spark, lake)
    run_silver(spark, lake)
    _advance(spark, lake, FLOORS)
    table = writer.spec.table
    query = silver_topic(TOPIC).query
    with pytest.raises(
        RetentionRefusedError, match=f"Silver checkpoint {query} v1 has read or pinned"
    ):
        vacuum_table(spark, lake, table)
    writer.append({0: 1, 1: 1, 2: 1})
    run_silver(spark, lake)
    with pytest.raises(
        RetentionRefusedError, match=re.escape("below bronze.identity_events_v1's declared")
    ):
        vacuum_table(spark, lake, table, retain_hours=1)
    spark.conf.set(RETENTION_CHECK_CONF, "false")
    try:
        with pytest.raises(RetentionRefusedError, match="Delta's own retention check"):
            vacuum_table(spark, lake, table)
    finally:
        spark.conf.set(RETENTION_CHECK_CONF, "true")
    report = vacuum_table(spark, lake, table)
    assert report.retain_hours == 168 and report.removal_version is not None
    assert report.files_deleted == 0, "removed files stay for the declared seven days"
    assert (
        _bronze(spark, lake).conserved and check_silver_conservation(spark, lake, TOPIC).conserved
    )
    with pytest.raises(RetentionRefusedError, match="OPTIMIZE is refused"):
        optimize_table(spark, lake, table)
    assert optimize_table(spark, lake, silver_topic(TOPIC).table) > 0


# ------------------------------------------------------------------ coverage ---


class _Ledger:
    """The producer-session ledger as `assess_bronze_coverage` reads it: one statement, one
    snapshot.

    A stub stands in for PostgreSQL here because this test is about the Bronze half of the rule --
    which rows are retired, read from Delta at the rows' own snapshot. The rule against a real
    ledger is tests/integration/test_bronze_kafka.py's coverage test.
    """

    def __init__(self, read_at: dt.datetime, sessions: dict[str, int]) -> None:
        self.rows = [
            (read_at, session_id, read_at - dt.timedelta(minutes=5), read_at, None, last_seq)
            for session_id, last_seq in sorted(sessions.items())
        ]

    def execute(self, query: str, params: tuple[object, ...]) -> Any:
        del query, params
        return self

    def fetchall(self) -> list[Any]:
        return self.rows


def test_resource_bounds_coverage_reads_the_floors_from_the_rows_own_snapshot(
    spark: Any, tmp_path: Path
) -> None:
    """Retired offsets are neither observations nor gaps, and the retired region bounds what the
    log vouches for. The floors come from the same snapshot the rows are read at."""
    lake = _lake(tmp_path)
    writer = BronzeWriter(spark, lake, session_id="resource-bounds-identity")
    writer.append({0: 4, 1: 4, 2: 4})
    writer.append({0: 4, 1: 4, 2: 4})
    run_silver(spark, lake)
    # tx.scored.v1 is covered too, so its table and checkpoint must exist for the rule to run. Its
    # own session keeps its sequence numbers apart from the identity writer's.
    scored = BronzeWriter(
        spark, lake, partitions=6, session_id="resource-bounds-scored", topic=TX_SCORED_V1
    )
    scored.append(dict.fromkeys(range(6), 1))
    ledger = _Ledger(
        dt.datetime.now(dt.UTC),
        {"resource-bounds-identity": writer.events, "resource-bounds-scored": scored.events},
    )

    before = assess_bronze_coverage(spark, lake, ledger, clock_margin_s=1.0)
    assert (before.retired, before.retired_through, before.retired_open) == (0, None, False)
    # Every row is accounted for: read as an observation, or held back because it arrived at or
    # after the high-water mark (the rule's own accounting, ADR-0051 §5).
    assert (
        before.observations + before.beyond_high_water + before.not_observations
        == writer.events + scored.events
    )
    assert before.observations > 0, "the first batch is inside the mark"

    report = _advance(spark, lake, {0: 4, 1: 4, 2: 4})
    assert report.rows_deleted == 12
    after = assess_bronze_coverage(spark, lake, ledger, clock_margin_s=1.0)

    assert after.retired_through is not None and not after.retired_open, after.retired_reasons
    assert after.retired == 0, "the retired rows are deleted, so none is read at all"
    assert after.observations == before.observations - 12
    assert [gap for gap in after.coverage.gaps if gap.missing] == [], (
        "the retired numbers are not reported as lost writes"
    )
    (span,) = [gap for gap in after.coverage.gaps if gap.session_id is None and gap.end is not None]
    assert span.start == dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
    assert span.end == after.retired_through + dt.timedelta(seconds=1)
    assert not after.coverage.vouches(after.retired_through, after.retired_through)


# --------------------------------------------------------------- concurrency ---


def test_resource_bounds_an_append_in_flight_across_maintenance_fails_loudly_and_state_holds(
    spark: Any, tmp_path: Path
) -> None:
    """A Bronze commit whose transaction began before maintenance's metadata commits fails with a
    Delta conflict; nothing it held is lost, and conservation holds. An append that commits
    between two maintenance steps is untouched."""
    lake = _lake(tmp_path)
    writer = _three_batches(spark, lake)
    run_silver(spark, lake)
    jvm = spark.sparkContext._jvm
    delta_log = jvm.org.apache.spark.sql.delta.DeltaLog.forTable(
        spark._jsparkSession, str(writer.path)
    )
    in_flight = delta_log.startTransaction()
    in_flight.metadata()  # every write reads the table's metadata first
    report = _advance(spark, lake, FLOORS)
    assert report.rows_deleted == 24
    actions = jvm.java.util.ArrayList()
    actions.add(
        jvm.org.apache.spark.sql.delta.actions.SetTransaction(
            "resource-bounds-race", 1, jvm.scala.Option.empty()
        )
    )
    operation = (
        jvm.org.apache.spark.util.Utils.classForName(
            "org.apache.spark.sql.delta.DeltaOperations$ManualUpdate$", False, False
        )
        .getField("MODULE$")
        .get(None)
    )
    with pytest.raises(Exception) as raised:
        in_flight.commit(
            jvm.scala.jdk.javaapi.CollectionConverters.asScala(actions).toList(), operation
        )
    assert re.search(r"MetadataChanged|METADATA_CHANGED|Concurrent", str(raised.value)), (
        raised.value
    )
    facts = snapshot_facts(spark, writer.path)
    assert facts is not None and "resource-bounds-race" not in facts.transactions
    assert _bronze(spark, lake).conserved

    def append_between(reached: Step) -> None:
        if reached is Step.FLOOR_COMMITTED:
            writer.append({0: 4, 1: 4, 2: 4})

    writer.append({0: 4, 1: 4, 2: 4})
    run_silver(spark, lake)
    racing = _advance(spark, lake, {0: 12, 1: 12, 2: 12}, after_step=append_between)
    assert racing.rows_deleted == 12, "the batch appended mid-run is not in the plan"
    bronze = _bronze(spark, lake)
    assert bronze.conserved, bronze.summary()
    run_silver(spark, lake)
    assert check_silver_conservation(spark, lake, TOPIC).conserved
