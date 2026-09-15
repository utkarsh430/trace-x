"""Conservation from Bronze into Silver: every consumed Bronze row is accounted for exactly once.

ADR-0053 §5. For one topic and the current version of its Silver checkpoint:

- **The consumed Bronze rows.** A Silver checkpoint reads its Bronze table's commit log from
  version 0 (`silver.silver_sources`), so its offsets are commit positions, never a snapshot's.
  The offset of the last committed batch is `(reservoir_version, index)`, the next position to
  read:
  - `index == -1`: every version before `reservoir_version` was read. `(0, -1)` reads nothing, and
    Spark never commits a batch there, so it is refused;
  - `index >= 0`: files `0..index` of `reservoir_version` were read too, so that version was read
    whole only when `index` is its last data file. A version read in part cannot be judged, and is
    reported as a problem: the check fails closed.
  Bronze is append-only, so the rows of versions `0..through` are exactly the table at version
  `through`.
- **The accounted rows.** Canonical rows of the topic's table (from any checkpoint version, or
  from a batch this version has committed); and `silver.duplicates` (identical duplicates and
  superseded rows) and `silver.quarantine` rows of the topic written by this checkpoint version in
  a batch it has committed. A batch still being written is left out, so a running query is never
  judged on half a batch; duplicates and quarantine rows are per checkpoint version, so a reset
  version accounts for Bronze again from its own rows.
- **Conserved** when there is no problem and every consumed Bronze coordinate (topic id, partition,
  offset) is accounted exactly once, nothing is accounted that Bronze did not hold, and no
  coordinate Bronze holds twice. And the tables agree with each other:
  - every `silver.duplicates` row has a canonical row with its identity and content digest;
  - every `silver.late_events` row of the topic matches its canonical row's identity and
    coordinates, and that row is late; every late canonical row has its `late_events` row.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from trace_core.domain.errors import CheckpointRefusedError, LakeContractError
from trace_core.observability import get_logger
from trace_core.stream import checkpoints
from trace_core.stream.bronze import bronze_topic
from trace_core.stream.lake import LakeConfig
from trace_core.stream.silver_rules import (
    DUPLICATES,
    LATE_EVENTS,
    QUARANTINE,
    DuplicateKind,
    silver_topic,
)
from trace_core.stream.tables import DeltaSourceOffset, snapshot_facts

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

_log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class ConsumedBronze:
    """How far a Silver checkpoint has read its Bronze table."""

    through_version: int | None
    """Bronze versions `0..through_version` were read whole; None when nothing was committed."""
    last_committed_batch: int | None
    problems: tuple[str, ...] = ()


def through_version(
    offset: DeltaSourceOffset, data_files_in: Callable[[int], int]
) -> tuple[int | None, str | None]:
    """The last Bronze version read whole, from the end offset of the last committed batch."""
    if offset.is_starting_version:
        return None, (
            f"the checkpoint is reading a snapshot of version {offset.reservoir_version}; a "
            f"Silver checkpoint reads the commit log from version 0, so its consumed range is "
            f"unknown"
        )
    if offset.index == -1:
        if offset.reservoir_version == 0:
            return None, (
                "a committed batch ends at offset (0, -1), which reads no Bronze version; Spark "
                "never commits one there, so the checkpoint is not trusted"
            )
        return offset.reservoir_version - 1, None
    files = data_files_in(offset.reservoir_version)
    if offset.index == files - 1:
        return offset.reservoir_version, None
    return offset.reservoir_version - 1, (
        f"version {offset.reservoir_version} was read up to data file {offset.index} of {files}; "
        f"a version read in part is not judged"
    )


def data_files_in_commit(table_path: Path, version: int) -> int:
    """The data files a Delta commit added: the positions a streaming source indexes."""
    path = table_path / "_delta_log" / f"{version:020d}.json"
    if not path.is_file():
        raise LakeContractError(f"{path} is missing, so the files of version {version} are unknown")
    count = 0
    for line in path.read_text().splitlines():
        action = json.loads(line) if line.strip() else {}
        add = action.get("add")
        if isinstance(add, dict) and add.get("dataChange", True):
            count += 1
    return count


def consumed_bronze(
    directory: Path, identity: checkpoints.CheckpointIdentity, source_key: str, table_path: Path
) -> ConsumedBronze:
    progress = checkpoints.SparkProgress.read(directory)
    record = identity.sources.get(source_key)
    if record is None or record.kind != "delta":
        raise CheckpointRefusedError(f"{source_key} is not a Delta source of {identity.query}")
    if record.start != "version:0":
        return ConsumedBronze(
            None,
            progress.last_committed,
            (f"the checkpoint starts at {record.start}, not at Bronze version 0",),
        )
    if progress.last_committed is None:
        return ConsumedBronze(None, None)
    offsets = checkpoints.committed_source_offsets(directory)
    if len(offsets) != 1 or offsets[0] is None:
        return ConsumedBronze(
            None, progress.last_committed, (f"expected one Delta source offset, got {offsets}",)
        )
    offset = DeltaSourceOffset.parse(offsets[0])
    through, problem = through_version(offset, lambda v: data_files_in_commit(table_path, v))
    return ConsumedBronze(through, progress.last_committed, () if problem is None else (problem,))


@dataclass(frozen=True, slots=True)
class SilverConservationReport:
    topic: str
    query: str
    checkpoint_version: int | None
    through_version: int | None
    bronze_rows: int
    canonical: int
    duplicates: int
    """Every `silver.duplicates` row accounted, superseded rows included."""
    quarantined: int
    missing: int
    double_counted: int
    unexpected: int
    bronze_repeated: int
    superseded: int = 0
    dangling_duplicates: int = 0
    """Duplicates rows with no canonical row of the same identity and content digest."""
    late_mismatched: int = 0
    """`late_events` rows that do not match a late canonical row's identity and coordinates."""
    late_missing: int = 0
    """Late canonical rows without their `late_events` row."""
    problems: tuple[str, ...] = field(default=())

    @property
    def conserved(self) -> bool:
        return (
            not self.problems
            and self.missing == 0
            and self.double_counted == 0
            and self.unexpected == 0
            and self.bronze_repeated == 0
            and self.dangling_duplicates == 0
            and self.late_mismatched == 0
            and self.late_missing == 0
            and self.bronze_rows == self.canonical + self.duplicates + self.quarantined
        )

    def summary(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "query": self.query,
            "conserved": self.conserved,
            "checkpoint_version": self.checkpoint_version,
            "through_version": self.through_version,
            "bronze_rows": self.bronze_rows,
            "canonical": self.canonical,
            "duplicates": self.duplicates,
            "superseded": self.superseded,
            "quarantined": self.quarantined,
            "missing": self.missing,
            "double_counted": self.double_counted,
            "unexpected": self.unexpected,
            "bronze_repeated": self.bronze_repeated,
            "dangling_duplicates": self.dangling_duplicates,
            "late_mismatched": self.late_mismatched,
            "late_missing": self.late_missing,
            "problems": list(self.problems),
        }


_COORDS = ("kafka_topic_id", "kafka_partition", "kafka_offset")


def _judge(bronze: DataFrame, accounted: DataFrame) -> dict[str, int]:
    from pyspark.sql import functions as F  # noqa: N812

    b = bronze.groupBy(*_COORDS).agg(F.count(F.lit(1)).alias("bronze_n"))
    a = accounted.groupBy(*_COORDS).agg(
        F.count(F.lit(1)).alias("accounted_n"),
        F.sum(F.when(F.col("source") == "canonical", 1).otherwise(0)).alias("canonical_n"),
        F.sum(F.when(F.col("source") == "duplicates", 1).otherwise(0)).alias("duplicates_n"),
        F.sum(F.when(F.col("source") == "quarantine", 1).otherwise(0)).alias("quarantine_n"),
    )
    missing = F.col("bronze_n").isNotNull() & F.col("accounted_n").isNull()
    row = (
        b.join(a, list(_COORDS), "full_outer")
        .agg(
            F.coalesce(F.sum("bronze_n"), F.lit(0)).alias("bronze_rows"),
            F.coalesce(F.sum("canonical_n"), F.lit(0)).alias("canonical"),
            F.coalesce(F.sum("duplicates_n"), F.lit(0)).alias("duplicates"),
            F.coalesce(F.sum("quarantine_n"), F.lit(0)).alias("quarantined"),
            F.sum(F.when(missing, 1).otherwise(0)).alias("missing"),
            F.sum(F.when(F.col("accounted_n") > 1, 1).otherwise(0)).alias("double_counted"),
            F.sum(F.when(F.col("bronze_n").isNull(), 1).otherwise(0)).alias("unexpected"),
            F.sum(F.when(F.col("bronze_n") > 1, 1).otherwise(0)).alias("bronze_repeated"),
        )
        .collect()[0]
    )
    names = (
        "bronze_rows",
        "canonical",
        "duplicates",
        "quarantined",
        "missing",
        "double_counted",
        "unexpected",
        "bronze_repeated",
    )
    return {name: int(row[name] or 0) for name in names}


def check_silver_conservation(
    spark: SparkSession, lake: LakeConfig, topic: str
) -> SilverConservationReport:
    """Judge the topic's Silver tables against its Bronze table (module docstring)."""
    from pyspark.sql import functions as F  # noqa: N812

    spec = silver_topic(topic)
    source = bronze_topic(topic).table
    state = checkpoints.read_state(lake, spec.query)
    if state.identity is None:
        raise CheckpointRefusedError(f"{spec.query}: no checkpoint identity, so nothing to judge")
    directory = checkpoints.query_directory(lake, spec.query) / f"v{state.identity.version}"
    bronze_path = source.local_path(lake)
    consumed = consumed_bronze(directory, state.identity, f"delta:{source}", bronze_path)
    if snapshot_facts(spark, bronze_path) is None:
        raise LakeContractError(f"{source} does not exist at {bronze_path}")
    coords = [F.col(c) for c in _COORDS]
    if consumed.through_version is None:
        bronze = spark.read.format("delta").load(str(bronze_path)).select(*coords).limit(0)
    else:
        bronze = (
            spark.read.format("delta")
            .option("versionAsOf", str(consumed.through_version))
            .load(str(bronze_path))
            .select(*coords)
        )
    app_id = state.identity.app_id
    committed = -1 if consumed.last_committed_batch is None else consumed.last_committed_batch
    ours = (F.col("silver_checkpoint_id") != app_id) | (F.col("silver_batch_id") <= committed)
    canonical_all = spark.read.format("delta").load(str(spec.table.local_path(lake)))
    canonical = canonical_all.filter(ours)
    this_version = (
        (F.col("silver_topic") == topic)
        & (F.col("silver_checkpoint_id") == app_id)
        & (F.col("silver_batch_id") <= committed)
    )
    duplicates = (
        spark.read.format("delta").load(str(DUPLICATES.local_path(lake))).filter(this_version)
    )
    quarantine = (
        spark.read.format("delta").load(str(QUARANTINE.local_path(lake))).filter(this_version)
    )
    accounted = (
        canonical.select(*coords, F.lit("canonical").alias("source"))
        .unionByName(duplicates.select(*coords, F.lit("duplicates").alias("source")))
        .unionByName(quarantine.select(*coords, F.lit("quarantine").alias("source")))
    )
    counts = _judge(bronze, accounted)

    superseded = duplicates.filter(F.col("disposition") == DuplicateKind.SUPERSEDED.value).count()
    dangling = duplicates.join(
        canonical_all.select("silver_identity", "content_digest"),
        ["silver_identity", "content_digest"],
        "left_anti",
    ).count()
    late = (
        spark.read.format("delta")
        .load(str(LATE_EVENTS.local_path(lake)))
        .filter(F.col("silver_topic") == topic)
    )
    late_canonical = canonical_all.filter(F.coalesce(F.col("is_late"), F.lit(False)))
    keys = ["silver_identity", *_COORDS]
    late_mismatched = late.select(*keys).join(late_canonical.select(*keys), keys, "left_anti")
    late_missing = (
        canonical.filter(F.coalesce(F.col("is_late"), F.lit(False)))
        .select(*keys)
        .join(late.select(*keys), keys, "left_anti")
    )
    report = SilverConservationReport(
        topic=topic,
        query=spec.query,
        checkpoint_version=state.identity.version,
        through_version=consumed.through_version,
        superseded=superseded,
        dangling_duplicates=dangling,
        late_mismatched=late_mismatched.count(),
        late_missing=late_missing.count(),
        problems=consumed.problems,
        **counts,
    )
    (_log.info if report.conserved else _log.error)("silver_conservation", **report.summary())
    return report


__all__ = [
    "ConsumedBronze",
    "SilverConservationReport",
    "check_silver_conservation",
    "consumed_bronze",
    "data_files_in_commit",
    "through_version",
]
