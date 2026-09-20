"""Lake maintenance: Bronze's audited retention floor, and guarded VACUUM and OPTIMIZE.

ADR-0052 amendment 1 (the user's decision of 2026-09-15). Local development only (PHASE3_PLAN
Q8): the production Bronze contract stays append-only, and this is not a retention framework.

**The floor.** One per (Kafka topic id, partition), in Bronze's own table properties
`trace_x.retention_floor.<topic id>.<partition>`. An offset below it is RETIRED from the commit
that set it, whether its row still exists or not. The Bronze table and its log are the authority;
`bronze_conservation` reads the floors and the rows from one snapshot.

**Advancing it** (`advance_retention`), one commit per step, idempotent on re-run:
1. **Evidence.** Bronze conservation; the current Silver checkpoint's conservation and the Bronze
   version it has read whole; and every Bronze batch at the snapshot Bronze conservation judged.
   `plan_retention` names every blocker, and nothing is committed while one exists.
2. **The floor properties,** only when a value changes: re-setting a value commits a new version
   (observed on Delta 4.0.1).
3. **`delta.appendOnly=false`,** only when there is a batch to delete.
4. **One DELETE of whole Bronze batches.** A batch is deleted only when every row it holds is below
   its floor and every current Silver checkpoint has read all of it. The predicate names the
   batches AND bounds each partition's offset below its floor, so only rows strictly below the floor
   can match, and the commit is removes-only (observed). A commit with a data-changing add is a
   defect, raised loudly.
5. **`delta.appendOnly=true`.**
6. **The audit** (`bronze.retention_audit`, append-only). Non-authoritative, never assumed atomic
   with Bronze: every run reconciles the rows missing for the current floors and for every retained
   maintenance DELETE, keyed so a re-run writes each at most once.

**A crash at any step boundary** leaves one of:
- rows below a committed floor still present: retired, never missing;
- `appendOnly` lifted: the drift check refuses a Bronze start and conservation until maintenance
  runs again, which restores it;
- rows deleted and the floor committed: retired, never missing;
- an audit row missing: the next run writes it.

**Concurrency.** One maintenance run per table, by a lock directory under the lake root. Delta
conflicts with a running Bronze query raise; they are never retried here.

**VACUUM** (`vacuum_table`) refuses:
- a retention below the table's declared `delta.deletedFileRetentionDuration`;
- a session without Delta's retention check, or with a loss-tolerant setting;
- files a consumer still needs: the Silver checkpoint reading a Bronze table, and the Gold build
  pins on Silver.

**OPTIMIZE** (`optimize_table`) is refused on Bronze: it merges batches into one file, which the
file-aligned delete can then never remove.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import socket
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final

from trace_core.domain.errors import LakeContractError, NaiveDatetimeError, TraceXError
from trace_core.observability import get_logger
from trace_core.stream import checkpoints
from trace_core.stream.bronze import BRONZE_TOPICS, bronze_declaration, bronze_topic
from trace_core.stream.bronze_conservation import check_conservation
from trace_core.stream.lake import AppId, LakeConfig, Tier, require_identifier
from trace_core.stream.silver import (
    shared_declarations,
    silver_declaration,
    silver_sources,
    silver_targets,
)
from trace_core.stream.silver_conservation import check_silver_conservation, consumed_bronze
from trace_core.stream.silver_rules import SILVER_TOPICS, silver_topic
from trace_core.stream.tables import (
    LOSS_TOLERANT_SESSION_CONF,
    CommitProvenance,
    FloorKey,
    TableDeclaration,
    TableRef,
    create_table,
    describe_live_table,
    latest_removal_version,
    require_no_drift,
    require_no_loss_tolerance,
    retention_floor_key,
    retention_floors,
    retention_start,
    snapshot_facts,
    stamped_commits,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

_log = get_logger(__name__)

MAINTENANCE_JOB: Final = "bronze_maintenance"
RETENTION_AUDIT: Final = TableRef(Tier.BRONZE, "retention_audit")
RETENTION_CHECK_CONF: Final = "spark.databricks.delta.retentionDurationCheck.enabled"
LOCK_DIRNAME: Final = "_maintenance"


class RetentionRefusedError(LakeContractError):
    """Maintenance refused before its next commit: every blocker is named."""


class MaintenanceDefectError(LakeContractError):
    """A maintenance commit is not what its plan proved it would be. Raised after the commit, so
    the state is loud: a Silver reader stops at a rewrite, and the drift check refuses a lifted
    table."""


def _utc(moment: dt.datetime) -> dt.datetime:
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise NaiveDatetimeError(f"maintenance timestamps must be timezone-aware UTC: {moment!r}")
    return moment.astimezone(dt.UTC)


def maintenance_query(table: TableRef) -> str:
    """The provenance query name of the maintenance commits to `table`."""
    return require_identifier("query name", f"{MAINTENANCE_JOB}_{table.name}")


# ------------------------------------------------------------------ the audit ---


def retention_audit_schema() -> StructType:
    from pyspark.sql.types import (
        BooleanType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("audit_key", StringType(), nullable=False),
            StructField("event", StringType(), nullable=False),
            StructField("bronze_table", StringType(), nullable=False),
            StructField("bronze_table_id", StringType(), nullable=False),
            StructField("topic_id", StringType(), nullable=True),
            StructField("kafka_partition", IntegerType(), nullable=True),
            StructField("previous_floor", LongType(), nullable=True),
            StructField("new_floor", LongType(), nullable=True),
            StructField("bronze_version", LongType(), nullable=True),
            StructField("files_deleted", LongType(), nullable=True),
            StructField("rows_deleted", LongType(), nullable=True),
            StructField("positions_checked", StringType(), nullable=False),
            StructField("reconciled", BooleanType(), nullable=False),
            StructField("recorded_at", TimestampType(), nullable=False),
            StructField("git_sha", StringType(), nullable=False),
            StructField("dirty_worktree", BooleanType(), nullable=False),
        ]
    )


def retention_audit_declaration() -> TableDeclaration:
    """Append-only: an audit row is never rewritten. Insert-only MERGE, observed to be admitted
    under `delta.appendOnly` (ADR-0048 (a)), makes a reconciliation write each key at most once."""
    return TableDeclaration(
        ref=RETENTION_AUDIT,
        schema=retention_audit_schema(),
        properties={"delta.appendOnly": "true"},
    )


class AuditEvent(StrEnum):
    FLOOR_ADVANCED = "floor_advanced"
    ROWS_DELETED = "rows_deleted"


def floor_audit_key(table_id: str, key: FloorKey, floor: int) -> str:
    return f"{AuditEvent.FLOOR_ADVANCED.value}:{table_id}:{key[0]}:{key[1]}:{floor}"


def delete_audit_key(table_id: str, version: int) -> str:
    return f"{AuditEvent.ROWS_DELETED.value}:{table_id}:{version}"


# ------------------------------------------------------------------ evidence ---


@dataclass(frozen=True, slots=True)
class SilverPosition:
    """What one current Silver checkpoint has read of a Bronze table."""

    query: str
    checkpoint_version: int | None
    through_version: int | None
    """The last Bronze version it read whole; None when it has read none."""
    conserved: bool
    problems: tuple[str, ...]
    ends: Mapping[FloorKey, int]
    """Per (topic id, partition), one past the highest offset Bronze holds at `through_version`."""


@dataclass(frozen=True, slots=True)
class BronzeBatch:
    """One Bronze micro-batch's rows at the judged snapshot."""

    checkpoint_id: str
    batch_id: int
    rows: int
    max_offsets: Mapping[FloorKey, int]
    rows_read_by_silver: int
    """Its rows present at the lowest `through_version` of the current Silver checkpoints: equal to
    `rows` only when every file of the batch was added at or before that version."""


@dataclass(frozen=True, slots=True)
class RetentionEvidence:
    table: str
    table_id: str
    bronze_version: int
    topic_id: str | None
    """The topic id the current Bronze checkpoint version recorded reading."""
    floors: Mapping[FloorKey, int]
    append_only_lifted: bool
    bronze_conserved: bool
    bronze_problems: tuple[str, ...]
    bronze_ends: Mapping[FloorKey, int]
    """Per (topic id, partition), the highest consumed end over Bronze's checkpoint versions."""
    silver: tuple[SilverPosition, ...]
    batches: tuple[BronzeBatch, ...]

    def positions(self) -> dict[str, Any]:
        return {
            "bronze_table": self.table,
            "bronze_version": self.bronze_version,
            "bronze_conserved": self.bronze_conserved,
            "bronze_consumed_ends": {
                f"{t}.{p}": e for (t, p), e in sorted(self.bronze_ends.items())
            },
            "silver": [
                {
                    "query": s.query,
                    "checkpoint_version": s.checkpoint_version,
                    "through_version": s.through_version,
                    "conserved": s.conserved,
                    "ends": {f"{t}.{p}": e for (t, p), e in sorted(s.ends.items())},
                }
                for s in self.silver
            ],
        }


@dataclass(frozen=True, slots=True)
class RetentionPlan:
    floors: Mapping[FloorKey, int]
    """Every floor after this run."""
    advanced: Mapping[FloorKey, tuple[int | None, int]]
    """(previous, new) for each floor this run commits."""
    deletable: tuple[BronzeBatch, ...]
    blockers: tuple[str, ...]


def _label(key: FloorKey) -> str:
    return f"floor(topic id {key[0]}, partition {key[1]})"


def plan_retention(
    evidence: RetentionEvidence, requested: Mapping[FloorKey, int] | None
) -> RetentionPlan:
    """The floors to commit and the batches to delete, or every reason not to.

    `requested` None advances each floor as far as every consumer allows: the lower of Bronze's
    consumed end and what every current Silver checkpoint has read whole. A requested floor is
    refused, never clamped."""
    blockers: list[str] = []
    table = evidence.table
    if not evidence.bronze_conserved:
        detail = "; ".join(evidence.bronze_problems) or "see its report"
        blockers.append(
            f"Bronze conservation of {table} fails ({detail}): retirement never hides a missing, "
            f"skipped or duplicated offset"
        )
    if not evidence.silver:
        blockers.append(
            f"no Silver checkpoint has read {table}: retiring rows would remove them before "
            f"Silver reads them"
        )
    for position in evidence.silver:
        if position.through_version is None or not position.conserved:
            detail = "; ".join(position.problems) or (
                "it has read no whole Bronze version"
                if position.through_version is None
                else "its conservation fails"
            )
            blockers.append(
                f"Silver checkpoint {position.query} v{position.checkpoint_version} cannot vouch "
                f"for what it read ({detail})"
            )
    bounds = {
        key: min([end, *(position.ends.get(key, 0) for position in evidence.silver)])
        for key, end in evidence.bronze_ends.items()
    }
    wanted = dict(bounds) if requested is None else dict(requested)
    floors = dict(evidence.floors)
    advanced: dict[FloorKey, tuple[int | None, int]] = {}
    for key, value in sorted(wanted.items()):
        current = evidence.floors.get(key)
        if value < 0:
            blockers.append(f"{_label(key)} = {value} is negative")
            continue
        if current is not None and value <= current:
            if value < current and requested is not None:
                blockers.append(
                    f"{_label(key)} = {value} is below the committed floor {current}: a floor is "
                    f"never lowered"
                )
            continue
        if requested is None and value == 0:
            continue
        if key not in evidence.bronze_ends:
            blockers.append(f"{_label(key)}: Bronze's checkpoints never read this partition")
            continue
        refused = False
        if value > evidence.bronze_ends[key]:
            blockers.append(
                f"{_label(key)} = {value} is beyond offset {evidence.bronze_ends[key]}, where "
                f"Bronze's checkpoint has consumed to"
            )
            refused = True
        for position in evidence.silver:
            end = position.ends.get(key, 0)
            if value > end:
                blockers.append(
                    f"{_label(key)} = {value} is beyond offset {end}, the end of what Silver "
                    f"checkpoint {position.query} v{position.checkpoint_version} has read whole "
                    f"(Bronze version {position.through_version})"
                )
                refused = True
        if not refused:
            floors[key] = value
            advanced[key] = (current, value)
    if blockers:
        return RetentionPlan(MappingProxyType(dict(evidence.floors)), {}, (), tuple(blockers))
    deletable = tuple(
        batch
        for batch in evidence.batches
        if batch.rows > 0
        and batch.rows_read_by_silver == batch.rows
        and all(offset < floors.get(key, 0) for key, offset in batch.max_offsets.items())
    )
    return RetentionPlan(MappingProxyType(floors), MappingProxyType(advanced), deletable, ())


def delete_predicate(plan: RetentionPlan) -> str:
    """The DELETE predicate: the deletable batches, AND each partition's offset below its floor.

    Every literal is validated first: checkpoint ids are TRACE-X app ids and topic ids are floor
    key segments, so none can carry a quote."""
    if not plan.deletable:
        raise RetentionRefusedError("a delete predicate needs at least one deletable batch")
    by_checkpoint: dict[str, set[int]] = {}
    for batch in plan.deletable:
        if AppId.parse(batch.checkpoint_id) is None:
            raise RetentionRefusedError(
                f"batch {batch.batch_id} was written by {batch.checkpoint_id!r}, not a TRACE-X "
                f"checkpoint app id; refusing to name it in a DELETE"
            )
        by_checkpoint.setdefault(batch.checkpoint_id, set()).add(batch.batch_id)
    batches = " OR ".join(
        f"(bronze_checkpoint_id = '{checkpoint}' AND bronze_batch_id IN "
        f"({', '.join(str(b) for b in sorted(ids))}))"
        for checkpoint, ids in sorted(by_checkpoint.items())
    )
    below = []
    for key, floor in sorted(plan.floors.items()):
        retention_floor_key(*key)  # validates the topic id and partition
        if floor > 0:
            below.append(
                f"(kafka_topic_id = '{key[0]}' AND kafka_partition = {key[1]} "
                f"AND kafka_offset < {floor})"
            )
    return f"({batches}) AND ({' OR '.join(below)})"


def _offset_ends(spark: SparkSession, path: Path, version: int) -> dict[FloorKey, int]:
    from pyspark.sql import functions as F  # noqa: N812

    rows = (
        spark.read.format("delta")
        .option("versionAsOf", str(version))
        .load(str(path))
        .groupBy("kafka_topic_id", "kafka_partition")
        .agg(F.max("kafka_offset").alias("highest"))
        .collect()
    )
    return {
        (str(r["kafka_topic_id"]), int(r["kafka_partition"])): int(r["highest"]) + 1 for r in rows
    }


def _bronze_batches(
    spark: SparkSession, path: Path, version: int, silver_version: int | None
) -> tuple[BronzeBatch, ...]:
    from pyspark.sql import functions as F  # noqa: N812

    identity = ["bronze_checkpoint_id", "bronze_batch_id"]
    groups = (
        spark.read.format("delta")
        .option("versionAsOf", str(version))
        .load(str(path))
        .groupBy(*identity, "kafka_topic_id", "kafka_partition")
        .agg(F.count(F.lit(1)).alias("rows"), F.max("kafka_offset").alias("highest"))
        .collect()
    )
    read: dict[tuple[str, int], int] = {}
    if silver_version is not None:
        for row in (
            spark.read.format("delta")
            .option("versionAsOf", str(silver_version))
            .load(str(path))
            .groupBy(*identity)
            .count()
            .collect()
        ):
            read[(str(row["bronze_checkpoint_id"]), int(row["bronze_batch_id"]))] = int(
                row["count"]
            )
    rows: dict[tuple[str, int], int] = {}
    highest: dict[tuple[str, int], dict[FloorKey, int]] = {}
    for row in groups:
        batch = (str(row["bronze_checkpoint_id"]), int(row["bronze_batch_id"]))
        rows[batch] = rows.get(batch, 0) + int(row["rows"])
        key = (str(row["kafka_topic_id"]), int(row["kafka_partition"]))
        highest.setdefault(batch, {})[key] = int(row["highest"])
    return tuple(
        BronzeBatch(
            checkpoint_id=batch[0],
            batch_id=batch[1],
            rows=count,
            max_offsets=MappingProxyType(highest[batch]),
            rows_read_by_silver=read.get(batch, 0),
        )
        for batch, count in sorted(rows.items())
    )


def gather_evidence(
    spark: SparkSession, lake: LakeConfig, topic: str, *, broker_topic_id: str | None
) -> RetentionEvidence:
    """Everything `plan_retention` judges, read from live tables and checkpoints."""
    spec = bronze_topic(topic)
    path = spec.table.local_path(lake)
    bronze = check_conservation(spark, lake, topic, broker_topic_id=broker_topic_id)
    facts = snapshot_facts(spark, path)
    if facts is None or bronze.table_version is None:
        raise LakeContractError(f"{spec.table} does not exist at {path}")
    ends: dict[FloorKey, int] = {}
    for report in (*bronze.superseded, bronze):
        if report.topic_id is None:
            continue
        for partition, consumed in report.consumed.ranges.items():
            key = (report.topic_id, partition)
            ends[key] = max(ends.get(key, 0), consumed.end)
    positions: list[SilverPosition] = []
    silver_spec = silver_topic(topic)
    if checkpoints.read_state(lake, silver_spec.query).identity is not None:
        silver = check_silver_conservation(spark, lake, topic)
        through = silver.through_version
        positions.append(
            SilverPosition(
                query=silver_spec.query,
                checkpoint_version=silver.checkpoint_version,
                through_version=through,
                conserved=silver.conserved,
                problems=silver.problems,
                ends=MappingProxyType(
                    {} if through is None else _offset_ends(spark, path, through)
                ),
            )
        )
    lowest = min(
        (p.through_version for p in positions if p.through_version is not None), default=None
    )
    problems = list(bronze.problems)
    if bronze.append_only_lifted:
        problems.append("appendOnly is lifted: a maintenance run was interrupted")
    return RetentionEvidence(
        table=str(spec.table),
        table_id=facts.table_id,
        bronze_version=bronze.table_version,
        topic_id=bronze.topic_id,
        floors=MappingProxyType(dict(bronze.floors)),
        append_only_lifted=bronze.append_only_lifted,
        bronze_conserved=bronze.conserved_rows,
        bronze_problems=tuple(problems),
        bronze_ends=MappingProxyType(ends),
        silver=tuple(positions),
        batches=_bronze_batches(spark, path, bronze.table_version, lowest),
    )


# ---------------------------------------------------------------------- lock ---


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


@contextmanager
def maintenance_lock(lake: LakeConfig, table: TableRef) -> Iterator[Path]:
    """One maintenance run per table. A lock left by a process that no longer exists on this host
    is taken over, loudly; any other held lock is a refusal."""
    directory = lake.root / LOCK_DIRNAME
    directory.mkdir(parents=True, exist_ok=True)
    lock = directory / f"{table.tier.value}_{table.name}.lock"
    host = socket.gethostname()
    for _attempt in range(2):
        try:
            lock.mkdir()
            break
        except FileExistsError:
            owner_path = lock / "owner.json"
            try:
                owner = json.loads(owner_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                owner = None
            stale = (
                isinstance(owner, dict)
                and owner.get("host") == host
                and isinstance(owner.get("pid"), int)
                and not _alive(owner["pid"])
            )
            if not stale:
                raise RetentionRefusedError(
                    f"maintenance of {table} is already running ({owner}); run one at a time"
                ) from None
            _log.warning("maintenance_stale_lock_taken_over", table=str(table), owner=owner)
            shutil.rmtree(lock, ignore_errors=True)
    else:
        raise RetentionRefusedError(f"could not take the maintenance lock of {table}")
    (lock / "owner.json").write_text(
        json.dumps({"host": host, "pid": os.getpid(), "table": str(table)}), encoding="utf-8"
    )
    try:
        yield lock
    finally:
        shutil.rmtree(lock, ignore_errors=True)


# ----------------------------------------------------------------- advancing ---


class Step(StrEnum):
    """The boundaries after which a run can stop. `after_step` is called at each one reached."""

    EVIDENCE = "evidence"
    FLOOR_COMMITTED = "floor_committed"
    APPEND_ONLY_LIFTED = "append_only_lifted"
    DELETED = "deleted"
    APPEND_ONLY_RESTORED = "append_only_restored"
    AUDITED = "audited"


@dataclass(frozen=True, slots=True)
class RetentionReport:
    table: str
    bronze_version: int
    floors_before: Mapping[FloorKey, int]
    floors_after: Mapping[FloorKey, int]
    advanced: Mapping[FloorKey, tuple[int | None, int]]
    floor_commit_version: int | None
    delete_commit_version: int | None
    files_deleted: int
    rows_deleted: int
    append_only_restored: bool
    audit_rows_written: int

    def summary(self) -> dict[str, Any]:
        def floors(values: Mapping[FloorKey, int]) -> dict[str, int]:
            return {f"{t}.{p}": v for (t, p), v in sorted(values.items())}

        return {
            "table": self.table,
            "bronze_version": self.bronze_version,
            "floors_before": floors(self.floors_before),
            "floors_after": floors(self.floors_after),
            "advanced": {
                f"{t}.{p}": {"previous": prev, "new": new}
                for (t, p), (prev, new) in sorted(self.advanced.items())
            },
            "floor_commit_version": self.floor_commit_version,
            "delete_commit_version": self.delete_commit_version,
            "files_deleted": self.files_deleted,
            "rows_deleted": self.rows_deleted,
            "append_only_restored": self.append_only_restored,
            "audit_rows_written": self.audit_rows_written,
        }


def _set_properties(
    spark: SparkSession,
    path: Path,
    identifier: str,
    properties: Mapping[str, str],
    provenance: CommitProvenance,
) -> int:
    clause = ", ".join(f"'{key}' = '{value}'" for key, value in sorted(properties.items()))
    with stamped_commits(spark, provenance):
        spark.sql(f"ALTER TABLE {identifier} SET TBLPROPERTIES ({clause})")
    facts = snapshot_facts(spark, path)
    assert facts is not None
    return facts.version


def _commit(path: Path, version: int) -> list[dict[str, Any]]:
    file = path / "_delta_log" / f"{version:020d}.json"
    return [json.loads(line) for line in file.read_text(encoding="utf-8").splitlines() if line]


def _delete(
    spark: SparkSession,
    path: Path,
    identifier: str,
    plan: RetentionPlan,
    provenance: CommitProvenance,
) -> tuple[int | None, int, int]:
    """Run the plan's DELETE; (commit version, files removed, rows deleted)."""
    facts = snapshot_facts(spark, path)
    assert facts is not None
    before = facts.version
    with stamped_commits(spark, provenance):
        # Every literal in the predicate is validated by delete_predicate.
        spark.sql(f"DELETE FROM {identifier} WHERE {delete_predicate(plan)}")  # noqa: S608  # nosec B608
    after = snapshot_facts(spark, path)
    assert after is not None
    expected = sum(batch.rows for batch in plan.deletable)
    stamp = provenance.to_user_metadata()
    for version in range(before + 1, after.version + 1):
        actions = _commit(path, version)
        info: dict[str, Any] = next((a["commitInfo"] for a in actions if "commitInfo" in a), {})
        if info.get("operation") != "DELETE" or info.get("userMetadata") != stamp:
            continue
        rewritten = [a for a in actions if "add" in a and a["add"].get("dataChange", True)]
        removed = sum(1 for a in actions if "remove" in a)
        metrics = info.get("operationMetrics") or {}
        deleted = int(metrics.get("numDeletedRows", -1))
        if rewritten or deleted != expected:
            raise MaintenanceDefectError(
                f"the retention DELETE at version {version} of {path} added {len(rewritten)} "
                f"data file(s) and deleted {deleted} row(s), where the plan proved a removes-only "
                f"commit of {expected} rows; a Silver reader stops at a rewrite, loudly"
            )
        return version, removed, deleted
    if expected:
        raise MaintenanceDefectError(
            f"the retention DELETE of {expected} rows from {path} made no commit"
        )
    return None, 0, 0


def _floor_commit_version(history: Sequence[Any], key: str, floor: int) -> int | None:
    for row in sorted(history, key=lambda r: -int(r["version"])):
        if row["operation"] != "SET TBLPROPERTIES":
            continue
        try:
            properties = json.loads((row["operationParameters"] or {}).get("properties", "{}"))
        except ValueError:
            continue
        if properties.get(key) == str(floor):
            return int(row["version"])
    return None


def reconcile_audit(
    spark: SparkSession,
    lake: LakeConfig,
    topic: str,
    *,
    positions: Mapping[str, Any],
    advanced: Mapping[FloorKey, tuple[int | None, int]],
    floor_commit_version: int | None,
    delete_commit_version: int | None,
    git_sha: str,
    dirty_worktree: bool,
    now: dt.datetime,
) -> int:
    """Write the audit rows the current floors and the retained maintenance DELETEs lack; the number
    written. Insert-only on `audit_key`, so a re-run and a race write each at most once."""
    from delta.tables import DeltaTable

    spec = bronze_topic(topic)
    path = spec.table.local_path(lake)
    query = maintenance_query(spec.table)
    provenance = CommitProvenance(git_sha, dirty_worktree, query)
    create_table(spark, retention_audit_declaration(), lake, provenance)
    facts = snapshot_facts(spark, path)
    assert facts is not None
    audit_path = RETENTION_AUDIT.local_path(lake)
    existing = (
        spark.read.format("delta")
        .load(str(audit_path))
        .filter(f"bronze_table_id = '{facts.table_id}'")
        .select("audit_key", "event", "topic_id", "kafka_partition", "new_floor")
        .collect()
    )
    keys = {str(r["audit_key"]) for r in existing}
    audited: dict[FloorKey, int] = {}
    for row in existing:
        if row["event"] == AuditEvent.FLOOR_ADVANCED.value:
            floor_key = (str(row["topic_id"]), int(row["kafka_partition"]))
            audited[floor_key] = max(audited.get(floor_key, 0), int(row["new_floor"]))
    history = (
        DeltaTable.forPath(spark, str(path))
        .history()
        .select("version", "operation", "operationParameters", "operationMetrics", "userMetadata")
        .collect()
    )
    recorded_at = _utc(now)
    checked = json.dumps(dict(positions), sort_keys=True)
    rows: list[tuple[Any, ...]] = []
    for floor_key, floor in sorted(retention_floors(facts.properties).items()):
        audit_key = floor_audit_key(facts.table_id, floor_key, floor)
        if audit_key in keys:
            continue
        mine = advanced.get(floor_key)
        if mine is not None and mine[1] == floor:
            previous, version, reconciled = mine[0], floor_commit_version, False
        else:
            previous = audited.get(floor_key)
            version = _floor_commit_version(history, retention_floor_key(*floor_key), floor)
            reconciled = True
        rows.append(
            (audit_key, AuditEvent.FLOOR_ADVANCED.value, str(spec.table), facts.table_id,
             floor_key[0], floor_key[1], previous, floor, version, None, None, checked,
             reconciled, recorded_at, git_sha, dirty_worktree)
        )  # fmt: skip
    for row in history:
        stamp = CommitProvenance.from_user_metadata(row["userMetadata"])
        if row["operation"] != "DELETE" or stamp is None or stamp.query != query:
            continue
        version = int(row["version"])
        audit_key = delete_audit_key(facts.table_id, version)
        if audit_key in keys:
            continue
        metrics = row["operationMetrics"] or {}
        rows.append(
            (audit_key, AuditEvent.ROWS_DELETED.value, str(spec.table), facts.table_id, None,
             None, None, None, version, int(metrics.get("numRemovedFiles", 0)),
             int(metrics.get("numDeletedRows", 0)), checked, version != delete_commit_version,
             recorded_at, git_sha, dirty_worktree)
        )  # fmt: skip
    if not rows:
        return 0
    source = spark.createDataFrame(rows, retention_audit_schema())
    with stamped_commits(spark, provenance):
        (
            DeltaTable.forPath(spark, str(audit_path))
            .alias("t")
            .merge(source.alias("s"), "t.audit_key = s.audit_key")
            .whenNotMatchedInsertAll()
            .execute()
        )
    _log.info("retention_audit_reconciled", table=str(spec.table), rows=len(rows))
    return len(rows)


def advance_retention(
    spark: SparkSession,
    lake: LakeConfig,
    topic: str,
    *,
    requested_floors: Mapping[int, int] | None,
    broker_topic_id: str | None,
    git_sha: str,
    dirty_worktree: bool,
    now: dt.datetime,
    after_step: Callable[[Step], None] | None = None,
) -> RetentionReport:
    """Advance Bronze's retention floors and delete what they retire (module docstring).

    `requested_floors` maps partitions of the topic id the current Bronze checkpoint recorded to a
    floor; None advances every floor as far as the consumers allow. `RetentionRefusedError` names
    every blocker before anything is committed; an interrupted run's lifted `appendOnly` is restored
    even then."""
    spec = bronze_topic(topic)
    path = spec.table.local_path(lake)
    identifier = spec.table.path_identifier(lake)
    provenance = CommitProvenance(git_sha, dirty_worktree, maintenance_query(spec.table))
    reached = after_step or (lambda _step: None)
    with maintenance_lock(lake, spec.table):
        evidence = gather_evidence(spark, lake, topic, broker_topic_id=broker_topic_id)
        requested: dict[FloorKey, int] | None = None
        if requested_floors is not None:
            if evidence.topic_id is None:
                raise RetentionRefusedError(
                    f"{spec.table}: the current Bronze checkpoint recorded no topic id, so no "
                    f"partition floor can be named"
                )
            requested = {(evidence.topic_id, p): f for p, f in requested_floors.items()}
        plan = plan_retention(evidence, requested)
        _log.info(
            "retention_planned",
            table=str(spec.table),
            bronze_version=evidence.bronze_version,
            advanced={f"{t}.{p}": v for (t, p), v in sorted(plan.advanced.items())},
            deletable_batches=len(plan.deletable),
            blockers=list(plan.blockers),
        )
        if plan.blockers:
            if evidence.append_only_lifted:
                _set_properties(spark, path, identifier, {"delta.appendOnly": "true"}, provenance)
                _log.warning("retention_append_only_restored_on_refusal", table=str(spec.table))
            raise RetentionRefusedError(
                f"refusing to advance the retention floor of {spec.table}: "
                + " ".join(f"({i}) {b}" for i, b in enumerate(plan.blockers, 1))
            )
        reached(Step.EVIDENCE)

        floor_version: int | None = None
        if plan.advanced:
            floor_version = _set_properties(
                spark,
                path,
                identifier,
                {retention_floor_key(*key): str(new) for key, (_, new) in plan.advanced.items()},
                provenance,
            )
            facts = snapshot_facts(spark, path)
            assert facts is not None
            committed = retention_floors(facts.properties)
            lowered = {k: v for k, v in plan.floors.items() if committed.get(k, -1) < v}
            if lowered:
                raise MaintenanceDefectError(
                    f"{spec.table}: after the floor commit at version {floor_version} the floors "
                    f"{committed} are below the plan's {dict(plan.floors)}"
                )
            _log.info("retention_floor_committed", table=str(spec.table), version=floor_version)
            reached(Step.FLOOR_COMMITTED)

        lifted = evidence.append_only_lifted
        delete_version: int | None = None
        files_deleted = rows_deleted = 0
        if plan.deletable:
            if not lifted:
                _set_properties(spark, path, identifier, {"delta.appendOnly": "false"}, provenance)
                lifted = True
                reached(Step.APPEND_ONLY_LIFTED)
            delete_version, files_deleted, rows_deleted = _delete(
                spark, path, identifier, plan, provenance
            )
            _log.info(
                "retention_rows_deleted",
                table=str(spec.table),
                version=delete_version,
                files=files_deleted,
                rows=rows_deleted,
            )
            reached(Step.DELETED)
        if lifted:
            _set_properties(spark, path, identifier, {"delta.appendOnly": "true"}, provenance)
            reached(Step.APPEND_ONLY_RESTORED)
        require_no_drift(bronze_declaration(topic), describe_live_table(spark, identifier))
        written = reconcile_audit(
            spark,
            lake,
            topic,
            positions=evidence.positions(),
            advanced=plan.advanced,
            floor_commit_version=floor_version,
            delete_commit_version=delete_version,
            git_sha=git_sha,
            dirty_worktree=dirty_worktree,
            now=now,
        )
        reached(Step.AUDITED)
    return RetentionReport(
        table=str(spec.table),
        bronze_version=evidence.bronze_version,
        floors_before=evidence.floors,
        floors_after=plan.floors,
        advanced=plan.advanced,
        floor_commit_version=floor_version,
        delete_commit_version=delete_version,
        files_deleted=files_deleted,
        rows_deleted=rows_deleted,
        append_only_restored=lifted,
        audit_rows_written=written,
    )


# --------------------------------------------------------------- Silver reset ---


def reset_silver(
    spark: SparkSession, lake: LakeConfig, topic: str, *, reason: str, now: dt.datetime
) -> checkpoints.CheckpointIdentity:
    """Publish a new Silver checkpoint version starting at Bronze's retention start: version 0
    without floors, else the first version whose files are all live. A start that cannot deliver
    exactly the unretired rows is refused (`tables.retention_start`)."""
    source = bronze_topic(topic).table
    start = retention_start(spark, source.local_path(lake))
    return checkpoints.reset_checkpoint(
        spark,
        lake,
        silver_topic(topic).query,
        targets=silver_targets(topic),
        sources=silver_sources(topic, start.start_version),
        reason=reason,
        now=now,
    )


# -------------------------------------------------------------- VACUUM, OPTIMIZE ---


def declared_tables() -> dict[TableRef, TableDeclaration]:
    """Every table the lake declares, by reference."""
    from trace_core.stream.gold import gold_declarations

    declarations = [
        *(bronze_declaration(topic) for topic in BRONZE_TOPICS),
        *(silver_declaration(topic) for topic in SILVER_TOPICS),
        *shared_declarations(),
        *gold_declarations(),
        retention_audit_declaration(),
    ]
    return {declaration.ref: declaration for declaration in declarations}


@dataclass(frozen=True, slots=True)
class ConsumerPin:
    """A reader that still needs a table's files from `version` on."""

    name: str
    version: int | None
    """The last version it has read whole or pinned; None when unknown."""


def plan_vacuum(
    *,
    table: str,
    declared_hours: int,
    retain_hours: int | None,
    session_conf: Mapping[str, str],
    removal_version: int | None,
    consumers: Sequence[ConsumerPin],
    supported: bool = True,
) -> tuple[int, tuple[str, ...]]:
    """(the retention to VACUUM with, every blocker)."""
    retain = declared_hours if retain_hours is None else retain_hours
    blockers: list[str] = []
    if not supported:
        blockers.append(
            f"{table}: this tooling does not know every reader of the table, so it never VACUUMs it"
        )
    if retain < declared_hours:
        blockers.append(
            f"RETAIN {retain} HOURS is below {table}'s declared deletedFileRetentionDuration of "
            f"{declared_hours} hours"
        )
    check = session_conf.get(RETENTION_CHECK_CONF, "true").strip().lower()
    if check != "true":
        blockers.append(
            f"{RETENTION_CHECK_CONF}={check}: VACUUM runs only with Delta's own retention check"
        )
    try:
        require_no_loss_tolerance(session_conf=session_conf, source_options={})
    except LakeContractError as exc:
        blockers.append(str(exc))
    if removal_version is not None:
        for consumer in consumers:
            if consumer.version is None or consumer.version < removal_version:
                blockers.append(
                    f"{consumer.name} has read or pinned {table} only through version "
                    f"{consumer.version}, but version {removal_version} removed files VACUUM may "
                    f"delete, which it would still read"
                )
    return retain, tuple(blockers)


def _gold_pins(lake: LakeConfig, ref: TableRef) -> list[ConsumerPin]:
    from trace_core.stream.gold_plan import GOLD_JOB, GOLD_SOURCES, parse_plan

    if ref not in GOLD_SOURCES:
        return []
    state = checkpoints.read_state(lake, GOLD_JOB)
    if state.identity is None:
        return []
    directory = checkpoints.query_directory(lake, GOLD_JOB) / f"v{state.identity.version}"
    builds = set(state.progress.planned - state.progress.committed)
    if state.progress.last_committed is not None:
        builds.add(state.progress.last_committed)
    pins = []
    for build in sorted(builds):
        path = directory / "offsets" / str(build)
        plan = parse_plan(path.read_text(encoding="utf-8"), source=path)
        pins.append(ConsumerPin(f"Gold build {build}", plan.pin(ref).version))
    return pins


def _silver_readers(lake: LakeConfig, ref: TableRef) -> list[ConsumerPin]:
    topic = next((t for t, spec in BRONZE_TOPICS.items() if spec.table == ref), None)
    if topic is None:
        return []
    spec = silver_topic(topic)
    state = checkpoints.read_state(lake, spec.query)
    if state.identity is None:
        return []
    directory = checkpoints.query_directory(lake, spec.query) / f"v{state.identity.version}"
    consumed = consumed_bronze(
        directory, state.identity, f"delta:{ref}", ref.local_path(lake), floors_exist=True
    )
    version = consumed.through_version
    if version is None and consumed.last_committed_batch is None and consumed.start_version:
        version = consumed.start_version - 1
    return [ConsumerPin(f"Silver checkpoint {spec.query} v{state.identity.version}", version)]


@dataclass(frozen=True, slots=True)
class VacuumReport:
    table: str
    retain_hours: int
    removal_version: int | None
    files_deleted: int
    consumers: tuple[ConsumerPin, ...] = field(default=())


def vacuum_table(
    spark: SparkSession, lake: LakeConfig, ref: TableRef, *, retain_hours: int | None = None
) -> VacuumReport:
    """VACUUM a declared table, or `RetentionRefusedError` naming every blocker."""
    from trace_core.stream.gold_plan import GOLD_TARGETS

    declaration = declared_tables().get(ref)
    if declaration is None:
        raise RetentionRefusedError(f"{ref} is not a declared table; refusing to VACUUM it")
    identifier = ref.path_identifier(lake)
    path = ref.local_path(lake)
    with maintenance_lock(lake, ref):
        require_no_drift(declaration, describe_live_table(spark, identifier))
        conf: Any = spark.conf
        session = {
            key: str(conf.get(key, default))
            for key, default in (
                (RETENTION_CHECK_CONF, "true"),
                *((key, "false") for key in LOSS_TOLERANT_SESSION_CONF),
            )
        }
        consumers = [*_silver_readers(lake, ref), *_gold_pins(lake, ref)]
        removal = latest_removal_version(path)
        retain, blockers = plan_vacuum(
            table=str(ref),
            declared_hours=declaration.deleted_file_retention_hours(),
            retain_hours=retain_hours,
            session_conf=session,
            removal_version=removal,
            consumers=consumers,
            supported=ref not in GOLD_TARGETS,
        )
        if blockers:
            raise RetentionRefusedError(
                f"refusing to VACUUM {ref}: "
                + " ".join(f"({i}) {b}" for i, b in enumerate(blockers, 1))
            )
        doomed = spark.sql(f"VACUUM {identifier} RETAIN {retain} HOURS DRY RUN").collect()
        spark.sql(f"VACUUM {identifier} RETAIN {retain} HOURS").collect()
    _log.info("vacuum_completed", table=str(ref), retain_hours=retain, files=len(doomed))
    return VacuumReport(str(ref), retain, removal, len(doomed), tuple(consumers))


def optimize_table(spark: SparkSession, lake: LakeConfig, ref: TableRef) -> int:
    """OPTIMIZE a declared table that is not Bronze; the resulting version."""
    if ref.tier is Tier.BRONZE:
        raise RetentionRefusedError(
            f"OPTIMIZE is refused on {ref}: it merges Bronze batches into one file (observed), "
            f"which the file-aligned retention delete can then never remove"
        )
    declaration = declared_tables().get(ref)
    if declaration is None:
        raise RetentionRefusedError(f"{ref} is not a declared table; refusing to OPTIMIZE it")
    identifier = ref.path_identifier(lake)
    with maintenance_lock(lake, ref):
        require_no_drift(declaration, describe_live_table(spark, identifier))
        spark.sql(f"OPTIMIZE {identifier}").collect()
    facts = snapshot_facts(spark, ref.local_path(lake))
    assert facts is not None
    return facts.version


__all__ = [
    "LOCK_DIRNAME",
    "MAINTENANCE_JOB",
    "RETENTION_AUDIT",
    "RETENTION_CHECK_CONF",
    "AuditEvent",
    "BronzeBatch",
    "ConsumerPin",
    "MaintenanceDefectError",
    "RetentionEvidence",
    "RetentionPlan",
    "RetentionRefusedError",
    "RetentionReport",
    "SilverPosition",
    "Step",
    "TraceXError",
    "VacuumReport",
    "advance_retention",
    "declared_tables",
    "delete_audit_key",
    "delete_predicate",
    "floor_audit_key",
    "gather_evidence",
    "maintenance_lock",
    "maintenance_query",
    "optimize_table",
    "plan_retention",
    "plan_vacuum",
    "reconcile_audit",
    "reset_silver",
    "retention_audit_declaration",
    "retention_audit_schema",
    "vacuum_table",
]
