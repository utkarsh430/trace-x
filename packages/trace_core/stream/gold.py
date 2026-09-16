"""Gold: a batch build of primitive state and point-in-time context from Silver (ADR-0055).

A build, in order:

1. **Refuses before anything is written** when a Silver source table is missing, when the session is
   configured to tolerate data loss (ADR-0048), or when a released declaration does not compile
   (`gold_plan.compile_windows`).
2. **Creates every Gold table from its declaration** and opens the `gold_build` checkpoint, which
   records the Silver tables' ids and refuses a replaced source or a target that lost delivered
   commits (ADR-0048 §6).
3. **Plans**: the next build number, with each Silver table pinned at its current version, written
   to the checkpoint's `offsets/<n>` before any target is touched. A planned build that never
   committed is **replayed with its own pins**, however far Silver has advanced since, so every Gold
   table of one build describes one set of Silver versions.
4. **Replaces** each Gold table by one MERGE on its key -- insert, update what changed, delete what
   the build no longer holds -- as the build's one idempotent commit to that table. A key held twice
   stops the build before the write. A replayed build's already-committed tables are skipped by
   Delta through the checkpoint's transaction identity.
5. **Records** the build in `gold.builds` (append-only): the pinned Silver versions and their commit
   times, every Gold table's version and row count, and the lag behind the newest Silver commit it
   covers (PHASE3_PLAN §4.3). Then `commits/<n>`.

Readers take a build's Gold tables at the versions its `gold.builds` row records
(`read_context_rows`). `check_gold` judges the latest build against Silver at its pins.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final

from trace_core.domain.time import to_millis
from trace_core.features.semantics import Dimension, Entity, Stream
from trace_core.features.spec import FEATURE_SET_VERSION
from trace_core.observability import get_logger
from trace_core.stream import checkpoints
from trace_core.stream.checkpoints import DeltaSourceStart, OpenedCheckpoint, SparkProgress
from trace_core.stream.gold_features import (
    distinct_buckets,
    distinct_buckets_schema,
    gateway_produced,
    minute_buckets,
    minute_buckets_schema,
    observations,
    observations_schema,
    read_pinned,
    tx_previous,
    tx_previous_schema,
    tx_profiles,
    tx_profiles_schema,
    tx_windows,
    tx_windows_schema,
)
from trace_core.stream.gold_plan import (
    AUTHORIZATIONS,
    BUILDS,
    DISTINCT_BUCKETS,
    GOLD_JOB,
    GOLD_SOURCES,
    GOLD_TARGETS,
    IDENTITY_EVENTS,
    IDENTITY_TYPE_STREAMS,
    MINUTE_BUCKETS,
    OBSERVATIONS,
    OBSERVED_OUTCOMES,
    REPLACED_TABLES,
    TABLE_KEYS,
    TRANSACTIONS,
    TX_PREVIOUS,
    TX_PROFILES,
    TX_WINDOWS,
    BuildPlan,
    ContextRows,
    GoldRefusedError,
    GoldUniquenessError,
    PreviousRow,
    ProfileRow,
    SourcePin,
    WindowRow,
    WindowSpec,
    compile_windows,
    distinct_column,
    lag_ms,
    next_build,
    read_plan,
    write_commit,
    write_plan,
)
from trace_core.stream.lake import LakeConfig
from trace_core.stream.tables import (
    LOSS_TOLERANT_SESSION_CONF,
    CommitProvenance,
    TableDeclaration,
    TableRef,
    create_table,
    require_no_loss_tolerance,
    snapshot_facts,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

_log = get_logger(__name__)

APPEND_ONLY: Final = MappingProxyType({"delta.appendOnly": "true"})


# ---------------------------------------------------------------- declarations ---


def builds_schema() -> StructType:
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    source = StructType(
        [
            StructField("table", StringType()),
            StructField("table_id", StringType()),
            StructField("version", LongType()),
            StructField("committed_at_ms", LongType()),
        ]
    )
    target = StructType(
        [
            StructField("table", StringType()),
            StructField("table_id", StringType()),
            StructField("version", LongType()),
            StructField("rows", LongType()),
        ]
    )
    return StructType(
        [
            StructField("build_id", LongType(), nullable=False),
            StructField("checkpoint_id", StringType(), nullable=False),
            StructField("git_sha", StringType(), nullable=False),
            StructField("dirty_worktree", BooleanType(), nullable=False),
            StructField("feature_set_version", StringType(), nullable=False),
            StructField("planned_at", TimestampType(), nullable=False),
            StructField("finished_at", TimestampType(), nullable=False),
            StructField("newest_silver_commit_at", TimestampType(), nullable=False),
            StructField("lag_ms", LongType(), nullable=False),
            StructField("sources", ArrayType(source, containsNull=True), nullable=False),
            StructField("targets", ArrayType(target, containsNull=True), nullable=False),
        ]
    )


def gold_declarations() -> tuple[TableDeclaration, ...]:
    """Unpartitioned and unclustered: layout is the Step 14 benchmark's (ADR-0015). Only
    `gold.builds` is append-only; every other Gold table is replaced by each build."""
    schemas: dict[TableRef, StructType] = {
        OBSERVATIONS: observations_schema(),
        MINUTE_BUCKETS: minute_buckets_schema(),
        DISTINCT_BUCKETS: distinct_buckets_schema(),
        TX_WINDOWS: tx_windows_schema(),
        TX_PROFILES: tx_profiles_schema(),
        TX_PREVIOUS: tx_previous_schema(),
        BUILDS: builds_schema(),
    }
    return tuple(
        TableDeclaration(
            ref=ref,
            schema=schemas[ref],
            properties=dict(APPEND_ONLY) if ref == BUILDS else {},
        )
        for ref in GOLD_TARGETS
    )


def create_gold_tables(
    spark: SparkSession, lake: LakeConfig, *, git_sha: str, dirty_worktree: bool
) -> None:
    provenance = CommitProvenance(git_sha=git_sha, dirty_worktree=dirty_worktree, query=GOLD_JOB)
    for declaration in gold_declarations():
        create_table(spark, declaration, lake, provenance)


# -------------------------------------------------------------------- records ---


@dataclass(frozen=True, slots=True)
class TargetVersion:
    table: str
    table_id: str
    version: int
    rows: int


@dataclass(frozen=True, slots=True)
class BuildRecord:
    """One committed build, as `gold.builds` holds it."""

    build_id: int
    checkpoint_id: str
    git_sha: str
    dirty_worktree: bool
    feature_set_version: str
    planned_at_ms: int
    finished_at_ms: int
    lag_ms: int
    sources: tuple[SourcePin, ...]
    targets: tuple[TargetVersion, ...]

    def target(self, ref: TableRef) -> TargetVersion:
        return next(t for t in self.targets if t.table == str(ref))

    def summary(self) -> dict[str, Any]:
        return {
            "build_id": self.build_id,
            "checkpoint_id": self.checkpoint_id,
            "feature_set_version": self.feature_set_version,
            "lag_ms": self.lag_ms,
            "sources": {p.table: p.version for p in self.sources},
            "targets": {t.table: {"version": t.version, "rows": t.rows} for t in self.targets},
        }


@dataclass(frozen=True, slots=True)
class BuildResult:
    record: BuildRecord
    replayed: bool


# ----------------------------------------------------------------- the build ---


def _commit_ms(spark: SparkSession, ref: TableRef, lake: LakeConfig, version: int) -> int:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F  # noqa: N812

    rows = (
        DeltaTable.forPath(spark, str(ref.local_path(lake)))
        .history()
        .filter(F.col("version") == version)
        .select(F.unix_millis("timestamp").alias("ms"))
        .collect()
    )
    if len(rows) != 1:
        raise GoldRefusedError(f"{ref}: version {version} is not in its retained history")
    return int(rows[0]["ms"])


def pin_sources(
    spark: SparkSession, lake: LakeConfig, build_id: int, now: dt.datetime
) -> BuildPlan:
    """Build `build_id`'s plan: every Silver source at its current version."""
    pins = []
    for ref in GOLD_SOURCES:
        facts = snapshot_facts(spark, ref.local_path(lake))
        if facts is None:
            raise GoldRefusedError(f"{ref} does not exist: Gold reads Silver, so run Silver first")
        pins.append(
            SourcePin(
                str(ref), facts.table_id, facts.version, _commit_ms(spark, ref, lake, facts.version)
            )
        )
    return BuildPlan(build_id, to_millis(now), tuple(pins))


def _require_pins_readable(spark: SparkSession, lake: LakeConfig, plan: BuildPlan) -> None:
    problems = []
    for ref in GOLD_SOURCES:
        pin = plan.pin(ref)
        facts = snapshot_facts(spark, ref.local_path(lake))
        if facts is None:
            problems.append(f"{ref} no longer exists")
        elif facts.table_id != pin.table_id:
            problems.append(f"{ref} is table id {facts.table_id}, not {pin.table_id} as planned")
        elif facts.version < pin.version:
            problems.append(f"{ref} is at version {facts.version}, behind its pin {pin.version}")
    if problems:
        raise GoldRefusedError(
            f"build {plan.build_id} cannot read what it planned: " + "; ".join(problems)
        )


def compute_frames(
    spark: SparkSession, lake: LakeConfig, plan: BuildPlan, specs: Sequence[WindowSpec]
) -> dict[TableRef, DataFrame]:
    """Every replaced Gold table's content for `plan`, persisted (the caller unpersists)."""
    from pyspark import StorageLevel

    obs = observations(spark, lake, plan).persist(StorageLevel.MEMORY_AND_DISK)
    minute = minute_buckets(spark, obs).persist(StorageLevel.MEMORY_AND_DISK)
    distinct = distinct_buckets(spark, obs).persist(StorageLevel.MEMORY_AND_DISK)
    frames = {
        OBSERVATIONS: obs,
        MINUTE_BUCKETS: minute,
        DISTINCT_BUCKETS: distinct,
        TX_WINDOWS: tx_windows(obs, minute, distinct, specs),
        TX_PROFILES: tx_profiles(obs),
        TX_PREVIOUS: tx_previous(spark, obs),
    }
    for ref in (TX_WINDOWS, TX_PROFILES, TX_PREVIOUS):
        frames[ref] = frames[ref].persist(StorageLevel.MEMORY_AND_DISK)
    return frames


def require_unique(frame: DataFrame, ref: TableRef) -> None:
    from pyspark.sql import functions as F  # noqa: N812

    keys = TABLE_KEYS[ref]
    repeated = frame.groupBy(*keys).count().filter(F.col("count") > 1).limit(3).collect()
    if repeated:
        raise GoldUniquenessError(
            f"{ref} would hold keys {list(keys)} more than once: "
            f"{[tuple(row[k] for k in keys) for row in repeated]}"
        )


def replace_table(opened: OpenedCheckpoint, frame: DataFrame, ref: TableRef, build_id: int) -> None:
    """`ref` becomes exactly `frame`, by key, in build `build_id`'s one idempotent commit to it."""
    require_unique(frame, ref)
    keys = TABLE_KEYS[ref]
    values = [f.name for f in frame.schema.fields if f.name not in keys]
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    changed = " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in values)

    def build(target: Any) -> Any:
        merge = target.alias("t").merge(frame.alias("s"), on)
        if changed:
            # A table whose columns are all key columns has nothing to update.
            merge = merge.whenMatchedUpdateAll(condition=changed)
        return merge.whenNotMatchedInsertAll().whenNotMatchedBySourceDelete()

    opened.merge(frame.sparkSession, batch_id=build_id, target=ref, build=build)


def _target_version(spark: SparkSession, lake: LakeConfig, ref: TableRef) -> TargetVersion:
    facts = snapshot_facts(spark, ref.local_path(lake))
    if facts is None:
        raise GoldRefusedError(f"{ref} vanished during the build")
    rows = (
        spark.read.format("delta")
        .option("versionAsOf", str(facts.version))
        .load(str(ref.local_path(lake)))
        .count()
    )
    return TargetVersion(str(ref), facts.table_id, facts.version, int(rows))


def _builds_row(
    spark: SparkSession,
    opened: OpenedCheckpoint,
    plan: BuildPlan,
    targets: Sequence[TargetVersion],
    finished_ms: int,
) -> DataFrame:
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import LongType, StructField, StructType

    schema = builds_schema()
    staged = StructType(
        [
            *[
                f
                for f in schema.fields
                if f.name not in {"planned_at", "finished_at", "newest_silver_commit_at"}
            ],
            StructField("planned_ms", LongType(), nullable=False),
            StructField("finished_ms", LongType(), nullable=False),
            StructField("newest_ms", LongType(), nullable=False),
        ]
    )
    values = {
        "build_id": plan.build_id,
        "checkpoint_id": opened.identity.app_id,
        "git_sha": opened.git_sha,
        "dirty_worktree": opened.dirty_worktree,
        "feature_set_version": FEATURE_SET_VERSION,
        "lag_ms": lag_ms(finished_ms, plan),
        "sources": [(p.table, p.table_id, p.version, p.committed_at_ms) for p in plan.sources],
        "targets": [(t.table, t.table_id, t.version, t.rows) for t in targets],
        "planned_ms": plan.planned_at_ms,
        "finished_ms": finished_ms,
        "newest_ms": plan.newest_commit_ms,
    }
    raw = spark.createDataFrame([tuple(values[f.name] for f in staged.fields)], staged)
    return raw.select(
        "build_id",
        "checkpoint_id",
        "git_sha",
        "dirty_worktree",
        "feature_set_version",
        F.timestamp_millis("planned_ms").alias("planned_at"),
        F.timestamp_millis("finished_ms").alias("finished_at"),
        F.timestamp_millis("newest_ms").alias("newest_silver_commit_at"),
        "lag_ms",
        "sources",
        "targets",
    ).select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def read_build(
    spark: SparkSession, lake: LakeConfig, checkpoint_id: str, build_id: int
) -> BuildRecord:
    from pyspark.sql import functions as F  # noqa: N812

    rows = (
        spark.read.format("delta")
        .load(str(BUILDS.local_path(lake)))
        .filter((F.col("checkpoint_id") == checkpoint_id) & (F.col("build_id") == build_id))
        .select(
            "build_id",
            "checkpoint_id",
            "git_sha",
            "dirty_worktree",
            "feature_set_version",
            F.unix_millis("planned_at").alias("planned_ms"),
            F.unix_millis("finished_at").alias("finished_ms"),
            "lag_ms",
            "sources",
            "targets",
        )
        .collect()
    )
    if len(rows) != 1:
        raise GoldRefusedError(
            f"{BUILDS} holds {len(rows)} rows for build {build_id} of {checkpoint_id}, not one"
        )
    row = rows[0]
    return BuildRecord(
        build_id=int(row["build_id"]),
        checkpoint_id=str(row["checkpoint_id"]),
        git_sha=str(row["git_sha"]),
        dirty_worktree=bool(row["dirty_worktree"]),
        feature_set_version=str(row["feature_set_version"]),
        planned_at_ms=int(row["planned_ms"]),
        finished_at_ms=int(row["finished_ms"]),
        lag_ms=int(row["lag_ms"]),
        sources=tuple(
            SourcePin(
                str(s["table"]), str(s["table_id"]), int(s["version"]), int(s["committed_at_ms"])
            )
            for s in row["sources"]
        ),
        targets=tuple(
            TargetVersion(str(t["table"]), str(t["table_id"]), int(t["version"]), int(t["rows"]))
            for t in row["targets"]
        ),
    )


def build_gold(
    spark: SparkSession,
    lake: LakeConfig,
    *,
    git_sha: str,
    dirty_worktree: bool,
    now: dt.datetime,
) -> BuildResult:
    """Run the next Gold build, or replay a planned one that never committed (module docstring)."""
    missing = [
        str(ref) for ref in GOLD_SOURCES if snapshot_facts(spark, ref.local_path(lake)) is None
    ]
    if missing:
        raise GoldRefusedError(f"{missing} do not exist: Gold reads Silver, so run Silver first")
    conf: Any = spark.conf
    require_no_loss_tolerance(
        session_conf={key: str(conf.get(key, "false")) for key in LOSS_TOLERANT_SESSION_CONF},
        source_options={},
    )
    specs = compile_windows()
    create_gold_tables(spark, lake, git_sha=git_sha, dirty_worktree=dirty_worktree)
    opened = checkpoints.open_checkpoint(
        spark,
        lake,
        GOLD_JOB,
        targets=list(GOLD_TARGETS),
        sources=[DeltaSourceStart(ref) for ref in GOLD_SOURCES],
        git_sha=git_sha,
        dirty_worktree=dirty_worktree,
        now=now,
    )
    build_id, replay = next_build(SparkProgress.read(opened.directory))
    if replay:
        plan = read_plan(opened.directory, build_id)
    else:
        plan = pin_sources(spark, lake, build_id, now)
        write_plan(opened.directory, plan)
    _require_pins_readable(spark, lake, plan)
    _log.info(
        "gold_build_started",
        build_id=build_id,
        replay=replay,
        checkpoint_id=opened.identity.app_id,
        sources={p.table: p.version for p in plan.sources},
    )
    frames = compute_frames(spark, lake, plan, specs)
    try:
        for ref in REPLACED_TABLES:
            replace_table(opened, frames[ref], ref, build_id)
    finally:
        for frame in frames.values():
            frame.unpersist()
    targets = [_target_version(spark, lake, ref) for ref in REPLACED_TABLES]
    finished_ms = to_millis(dt.datetime.now(dt.UTC))
    opened.append(
        _builds_row(spark, opened, plan, targets, finished_ms), batch_id=build_id, target=BUILDS
    )
    write_commit(opened.directory, build_id)
    record = read_build(spark, lake, opened.identity.app_id, build_id)
    _log.info(
        "gold_build_committed",
        build_id=record.build_id,
        replayed=replay,
        checkpoint_id=record.checkpoint_id,
        lag_ms=record.lag_ms,
        sources={p.table: p.version for p in record.sources},
        rows={t.table: t.rows for t in record.targets},
    )
    return BuildResult(record, replay)


def latest_build(lake: LakeConfig, spark: SparkSession) -> BuildRecord | None:
    """The last committed build of the current checkpoint version, or None."""
    state = checkpoints.read_state(lake, GOLD_JOB)
    if state.identity is None or state.progress.last_committed is None:
        return None
    return read_build(spark, lake, state.identity.app_id, state.progress.last_committed)


# ------------------------------------------------------------------ reading ---


def _at(spark: SparkSession, lake: LakeConfig, record: BuildRecord, ref: TableRef) -> DataFrame:
    target = record.target(ref)
    return (
        spark.read.format("delta")
        .option("versionAsOf", str(target.version))
        .load(str(ref.local_path(lake)))
    )


def _int(value: Decimal | int | None) -> int | None:
    return None if value is None else int(value)


def read_context_rows(
    spark: SparkSession,
    lake: LakeConfig,
    record: BuildRecord,
    transaction_ids: Sequence[str] | None = None,
) -> dict[str, ContextRows]:
    """Each transaction's point-in-time context rows, from `record`'s versions of the Gold
    tables."""
    from pyspark.sql import functions as F  # noqa: N812

    def selected(frame: DataFrame, column: str) -> DataFrame:
        return (
            frame if transaction_ids is None else frame.filter(F.col(column).isin(*transaction_ids))
        )

    subjects = selected(
        _at(spark, lake, record, OBSERVATIONS).filter(F.col("stream") == Stream.TRANSACTION.value),
        "event_id",
    ).select("event_id", "occurred_ms")
    windows: dict[str, list[WindowRow]] = {}
    for row in selected(_at(spark, lake, record, TX_WINDOWS), "transaction_id").collect():
        windows.setdefault(str(row["transaction_id"]), []).append(
            WindowRow(
                entity=Entity(row["entity"]),
                entity_id=str(row["entity_id"]),
                stream=Stream(row["stream"]),
                window_label=str(row["window_label"]),
                count=_int(row["count"]),
                amount_sum_minor=_int(row["amount_sum_minor"]),
                aligned_count=_int(row["aligned_count"]),
                aligned_amount_sum_minor=_int(row["aligned_amount_sum_minor"]),
                aligned_amount_sum_squares=_int(row["aligned_amount_sum_squares"]),
                declined_count=_int(row["declined_count"]),
                outcome_known_count=_int(row["outcome_known_count"]),
                distinct={
                    d: int(row[distinct_column(d)])
                    for d in Dimension
                    if row[distinct_column(d)] is not None
                },
            )
        )
    profiles: dict[str, ProfileRow] = {}
    for row in selected(_at(spark, lake, record, TX_PROFILES), "transaction_id").collect():
        profiles[str(row["transaction_id"])] = ProfileRow(
            account_id=str(row["account_id"]),
            first_seen_ms=int(row["first_seen_ms"]),
            observation_count=int(row["observation_count"]),
            amount_median_minor=row["amount_median_minor"],
            amount_mad_minor=row["amount_mad_minor"],
            habitual_merchants=frozenset(row["habitual_merchants"]),
            habitual_mccs=frozenset(row["habitual_mccs"]),
            known_devices=frozenset(row["known_devices"]),
            home_latitude=row["home_latitude"],
            home_longitude=row["home_longitude"],
        )
    previous: dict[str, list[PreviousRow]] = {}
    for row in selected(_at(spark, lake, record, TX_PREVIOUS), "transaction_id").collect():
        previous.setdefault(str(row["transaction_id"]), []).append(
            PreviousRow(
                entity=Entity(row["entity"]),
                entity_id=str(row["entity_id"]),
                stream=Stream(row["stream"]),
                occurred_ms=int(row["occurred_ms"]),
                latitude=row["latitude"],
                longitude=row["longitude"],
                card_present=bool(row["card_present"]),
            )
        )
    return {
        str(row["event_id"]): ContextRows(
            transaction_id=str(row["event_id"]),
            as_of_ms=int(row["occurred_ms"]),
            windows=tuple(windows.get(str(row["event_id"]), ())),
            profile=profiles.get(str(row["event_id"])),
            previous=tuple(previous.get(str(row["event_id"]), ())),
        )
        for row in subjects.collect()
    }


# -------------------------------------------------------------------- check ---


@dataclass(frozen=True, slots=True)
class GoldCheckReport:
    build_id: int | None
    problems: tuple[str, ...]
    expected: Mapping[str, int]

    @property
    def consistent(self) -> bool:
        return self.build_id is not None and not self.problems

    def summary(self) -> dict[str, Any]:
        return {
            "build_id": self.build_id,
            "consistent": self.consistent,
            "problems": list(self.problems),
            "expected": dict(self.expected),
        }


def check_gold(spark: SparkSession, lake: LakeConfig) -> GoldCheckReport:
    """The latest committed build against its own record and against Silver at its pins.

    - every Gold table is still the table, at the version and row count, the build recorded;
    - every Silver source is still the table it pinned;
    - `gold.observations` holds exactly one row per Silver observation at the pins: every
      transaction, every gateway-produced identity event whose type feeds a stream, every
      observed outcome;
    - no Gold table holds a key twice;
    - every transaction has its context windows, and profiles and previous observations name only
      transactions.
    """
    from pyspark.sql import functions as F  # noqa: N812

    record = latest_build(lake, spark)
    if record is None:
        return GoldCheckReport(None, ("no committed Gold build",), {})
    problems: list[str] = []
    for target in record.targets:
        ref = next(r for r in REPLACED_TABLES if str(r) == target.table)
        facts = snapshot_facts(spark, ref.local_path(lake))
        if facts is None or facts.table_id != target.table_id:
            problems.append(f"{target.table} is not the table build {record.build_id} wrote")
            continue
        rows = _at(spark, lake, record, ref).count()
        if rows != target.rows:
            problems.append(
                f"{target.table} holds {rows} rows at v{target.version}, recorded {target.rows}"
            )
        keys = TABLE_KEYS[ref]
        if (
            _at(spark, lake, record, ref)
            .groupBy(*keys)
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .collect()
        ):
            problems.append(f"{target.table} holds a key {list(keys)} more than once")
    plan = BuildPlan(record.build_id, record.planned_at_ms, record.sources)
    for pin in record.sources:
        ref = next(r for r in GOLD_SOURCES if str(r) == pin.table)
        facts = snapshot_facts(spark, ref.local_path(lake))
        if facts is None or facts.table_id != pin.table_id or facts.version < pin.version:
            problems.append(f"{pin.table} is no longer the table version {pin.version} it pinned")
    if problems:
        return GoldCheckReport(record.build_id, tuple(problems), {})

    transactions = read_pinned(spark, lake, plan, TRANSACTIONS)
    expected = {
        Stream.TRANSACTION.value: transactions.count(),
        "identity_events": read_pinned(spark, lake, plan, IDENTITY_EVENTS)
        .filter(F.col("identity_event_type").isin(*IDENTITY_TYPE_STREAMS))
        .filter(gateway_produced(F.col("producer")))
        .count(),
        Stream.AUTHORIZATION_OUTCOME.value: read_pinned(spark, lake, plan, AUTHORIZATIONS)
        .filter(F.col("authorization_outcome").isin(*OBSERVED_OUTCOMES))
        .count(),
    }
    obs = _at(spark, lake, record, OBSERVATIONS)
    held = {
        Stream.TRANSACTION.value: obs.filter(F.col("stream") == Stream.TRANSACTION.value).count(),
        "identity_events": obs.filter(
            F.col("stream").isin(*{s.value for s in IDENTITY_TYPE_STREAMS.values()})
        ).count(),
        Stream.AUTHORIZATION_OUTCOME.value: obs.filter(
            F.col("stream") == Stream.AUTHORIZATION_OUTCOME.value
        ).count(),
    }
    for name, count in expected.items():
        if held[name] != count:
            problems.append(f"gold.observations holds {held[name]} {name}, Silver {count}")
    tx_ids = transactions.select(F.col("transaction_id").alias("transaction_id")).distinct()
    covered = _at(spark, lake, record, TX_WINDOWS).select("transaction_id").distinct()
    if covered.join(tx_ids, "transaction_id", "left_anti").limit(1).collect():
        problems.append("gold.tx_windows names a transaction Silver does not hold")
    if tx_ids.join(covered, "transaction_id", "left_anti").limit(1).collect():
        problems.append("a Silver transaction has no context windows in gold.tx_windows")
    for ref in (TX_PROFILES, TX_PREVIOUS):
        named = _at(spark, lake, record, ref).select("transaction_id").distinct()
        if named.join(tx_ids, "transaction_id", "left_anti").limit(1).collect():
            problems.append(f"{ref} names a transaction Silver does not hold")
    return GoldCheckReport(record.build_id, tuple(problems), expected)


__all__ = [
    "APPEND_ONLY",
    "BuildRecord",
    "BuildResult",
    "GoldCheckReport",
    "TargetVersion",
    "build_gold",
    "builds_schema",
    "check_gold",
    "compute_frames",
    "create_gold_tables",
    "gold_declarations",
    "latest_build",
    "pin_sources",
    "read_build",
    "read_context_rows",
    "replace_table",
    "require_unique",
]
