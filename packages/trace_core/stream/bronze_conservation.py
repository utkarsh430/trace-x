"""Conservation into Bronze: every offset a Bronze query consumed is in its table exactly once.

The check backing `P3.kafka-ingest`. For each partition of a Bronze query's topic:

- **The consumed range** of a checkpoint version is `[start, end)`. `start` is where the version
  began reading: the initial offsets Spark's Kafka source records in `<checkpoint>/sources/0/0`.
  `end` is the end offset of the latest batch that is done: the later of the last batch Spark
  committed and the last batch the table recorded for this version's app id (a batch the sink
  committed before Spark recorded it -- Delta commits are atomic, so its rows are either all there
  or none are).
- **Every checkpoint version is judged, not only the current one.** A version judged alone forgets
  a loss inside an earlier version, and the offsets between two versions that neither read: a
  query stopped by trimmed, unread offsets can only be reset at `earliest`, which begins above the
  hole. So, per partition:
  - each version's rows hold every offset of its range exactly once and none outside it, all
    under the topic id it recorded reading;
  - a version that starts above the highest end of the earlier versions that read the same topic
    id skipped the offsets in between (`skipped`). The query is then never conserved again: the
    table is append-only and the loss is real;
  - no (topic id, offset) appears twice in the table under any checkpoint. A deleted and recreated
    topic has a new id and starts its offsets again at 0, so the id is part of the key.
- **Conserved** when all of that holds, no checkpoint problem was found, and, when the caller gives
  the broker's topic id, it is the one the current version recorded: a recreated topic's offsets
  are not the offsets the checkpoint consumed.

Counting distinct offsets inside the range is enough to prove completeness: offsets on these topics
are contiguous, because every producer is idempotent and none is transactional (ADR-0047) and no
topic is compacted. A transactional producer would put control records in the offset space, and
this check would report them as missing -- loudly, never silently.

**Read order, for a running query.** Spark's commits first, then the table's snapshot, then Spark's
planned batches, then the rows at that snapshot's version. A batch Spark committed before the
snapshot was taken is visible in it, so a committed batch missing from the table is a loss, never a
race. A batch the snapshot holds was planned before the snapshot was taken, so it is never reported
as never planned by a race either.

Spark checkpoint file formats are read by `trace_core.stream.checkpoints` (Spark 4.0.1's), and
exercised against a live query by `tests/integration/test_bronze_kafka.py`.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from trace_core.domain.errors import CheckpointRefusedError, LakeContractError
from trace_core.observability import get_logger
from trace_core.stream import checkpoints
from trace_core.stream.bronze import (
    TOPIC_IDENTITY_FILENAME,
    bronze_declaration,
    bronze_topic,
    read_topic_identity,
)
from trace_core.stream.checkpoints import (
    INITIAL_OFFSETS_PARTS,
    CheckpointIdentity,
    SparkProgress,
    parse_batch_offsets,
    parse_initial_offsets,
    read_batch_offsets,
    read_initial_offsets,
)
from trace_core.stream.lake import LakeConfig
from trace_core.stream.tables import describe_live_table, require_no_drift, snapshot_facts

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_log = get_logger(__name__)


# ------------------------------------------------------------- the judgement ---


@dataclass(frozen=True, slots=True, order=True)
class OffsetRange:
    """`[start, end)` of one partition."""

    start: int
    end: int

    def __post_init__(self) -> None:
        if self.start < 0 or self.end < self.start:
            raise ValueError(f"invalid offset range [{self.start}, {self.end})")

    @property
    def length(self) -> int:
        return self.end - self.start


@dataclass(frozen=True, slots=True)
class ConsumedRanges:
    """What one checkpoint version of a Bronze query has consumed, per partition."""

    topic: str
    checkpoint_id: str
    last_committed: int | None
    written_batch: int | None
    through_batch: int | None
    ranges: Mapping[int, OffsetRange]
    problems: tuple[str, ...] = ()


def consumed_ranges(
    *,
    topic: str,
    checkpoint_id: str,
    directory: Path,
    progress: SparkProgress,
    written_batch: int | None,
) -> ConsumedRanges:
    """The consumed range of every partition, from the checkpoint's own files."""
    problems: list[str] = []
    if written_batch is not None and written_batch not in progress.planned:
        problems.append(
            f"the table recorded batch {written_batch} for {checkpoint_id}, which this checkpoint "
            f"never planned; the range is judged without it"
        )
        written_batch = None
    done = [b for b in (progress.last_committed, written_batch) if b is not None]
    through = max(done) if done else None
    initial = read_initial_offsets(directory, topic)
    ranges: dict[int, OffsetRange] = {}
    if through is None:
        ranges = {p: OffsetRange(o, o) for p, o in (initial or {}).items()}
    elif initial is None:
        problems.append(
            f"batches up to {through} are done but the checkpoint holds no initial offsets, so "
            f"where it began reading is unknown"
        )
    else:
        end = read_batch_offsets(directory, through, topic)
        for partition in sorted(set(initial) | set(end)):
            if partition not in initial:
                problems.append(
                    f"partition {partition} appeared after the query started: its start is not "
                    f"recorded, so its consumption cannot be verified"
                )
            elif partition not in end:
                problems.append(f"partition {partition} is absent from batch {through}'s offsets")
            elif end[partition] < initial[partition]:
                problems.append(
                    f"partition {partition} ends at {end[partition]}, before its start "
                    f"{initial[partition]}: the topic's offsets went backwards"
                )
            else:
                ranges[partition] = OffsetRange(initial[partition], end[partition])
    return ConsumedRanges(
        topic=topic,
        checkpoint_id=checkpoint_id,
        last_committed=progress.last_committed,
        written_batch=written_batch,
        through_batch=through,
        ranges=MappingProxyType(ranges),
        problems=tuple(problems),
    )


def read_progress_around_snapshot[K, T](
    directories: Mapping[K, Path], snapshot: Callable[[], T]
) -> tuple[dict[K, SparkProgress], T]:
    """Each checkpoint version's progress, read around one table snapshot (module docstring): its
    commits before `snapshot()` and its planned batches after it."""
    committed = {
        key: SparkProgress.read(directory).committed for key, directory in directories.items()
    }
    taken = snapshot()
    return {
        key: replace(SparkProgress.read(directory), committed=committed[key])
        for key, directory in directories.items()
    }, taken


@dataclass(frozen=True, slots=True)
class VersionConsumption:
    """What one checkpoint version consumed, and the topic id it recorded reading."""

    version: int | None
    topic_id: str | None
    consumed: ConsumedRanges


def skipped_between_versions(
    history: Iterable[VersionConsumption],
) -> dict[int, tuple[OffsetRange, ...]]:
    """Per partition, the offsets no checkpoint version read, between versions of one topic id.

    `history` is in version order. A version starting above the highest end of the earlier
    versions that read the same topic id skipped the offsets in between. A version starting at or
    below it re-read them, which the duplicate count reports. The first version to read a
    partition under a topic id skips nothing: what came before it was never in any range
    (`ConservationReport.started_after_trim`)."""
    ends: dict[tuple[str | None, int], int] = {}
    skipped: dict[int, list[OffsetRange]] = {}
    for entry in history:
        for partition, consumed in sorted(entry.consumed.ranges.items()):
            key = (entry.topic_id, partition)
            previous = ends.get(key)
            if previous is not None and consumed.start > previous:
                skipped.setdefault(partition, []).append(OffsetRange(previous, consumed.start))
            ends[key] = consumed.end if previous is None else max(previous, consumed.end)
    return {partition: tuple(ranges) for partition, ranges in skipped.items()}


@dataclass(frozen=True, slots=True)
class PartitionRows:
    """What one partition's Bronze rows hold, relative to a checkpoint and its consumed range."""

    partition: int
    rows: int
    distinct_offsets: int
    """Distinct (topic id, offset) pairs, under any checkpoint."""
    current_rows: int
    """Rows written by the checkpoint under judgement."""
    current_in_range_distinct: int
    current_out_of_range: int
    current_other_topic_id: int = 0
    """Rows the checkpoint under judgement wrote under a topic id other than the one it recorded."""


Record = tuple[int, int, str, str]
"""(partition, offset, writing checkpoint's app id, topic id) of one Bronze row."""


def partition_rows_from_records(
    records: Iterable[Record],
    *,
    checkpoint_id: str,
    ranges: Mapping[int, OffsetRange],
    topic_id: str | None,
) -> dict[int, PartitionRows]:
    """The reference implementation of `read_partition_rows`, over `Record` tuples. Used by tests
    to hold the Spark aggregation to the definition."""
    by_partition: dict[int, list[tuple[int, str, str]]] = {}
    for partition, offset, writer, row_topic_id in records:
        by_partition.setdefault(partition, []).append((offset, writer, row_topic_id))
    result: dict[int, PartitionRows] = {}
    for partition, items in by_partition.items():
        expected = ranges.get(partition)

        def inside(offset: int, expected: OffsetRange | None = expected) -> bool:
            return expected is not None and expected.start <= offset < expected.end

        current = [(offset, row_topic_id) for offset, writer, row_topic_id in items
                   if writer == checkpoint_id]  # fmt: skip
        result[partition] = PartitionRows(
            partition=partition,
            rows=len(items),
            distinct_offsets=len({(row_topic_id, offset) for offset, _, row_topic_id in items}),
            current_rows=len(current),
            current_in_range_distinct=len({offset for offset, _ in current if inside(offset)}),
            current_out_of_range=sum(1 for offset, _ in current if not inside(offset)),
            current_other_topic_id=0
            if topic_id is None
            else sum(1 for _, row_topic_id in current if row_topic_id != topic_id),
        )
    return result


@dataclass(frozen=True, slots=True)
class PartitionVerdict:
    partition: int
    expected: OffsetRange | None
    missing: int
    """Offsets in the consumed range that this checkpoint's rows do not hold."""
    duplicates: int
    """Rows beyond the first for a (topic id, offset), under any checkpoint."""
    out_of_range: int
    """Rows this checkpoint wrote outside its consumed range."""
    skipped: int = 0
    """Offsets no checkpoint version read, between versions of one topic id."""
    other_topic_id: int = 0
    """Rows this checkpoint wrote under a topic id other than the one it recorded reading."""

    @property
    def conserved(self) -> bool:
        return (
            self.missing == 0
            and self.duplicates == 0
            and self.out_of_range == 0
            and self.skipped == 0
            and self.other_topic_id == 0
        )


@dataclass(frozen=True, slots=True)
class ConservationReport:
    topic: str
    table: str
    table_version: int | None
    consumed: ConsumedRanges
    partitions: tuple[PartitionVerdict, ...]
    foreign_topic_rows: int
    problems: tuple[str, ...]
    checkpoint_version: int | None = None
    topic_id: str | None = None
    """The topic id this checkpoint version recorded reading (`trace-kafka-topic.json`)."""
    skipped: Mapping[int, tuple[OffsetRange, ...]] = field(
        default_factory=lambda: MappingProxyType({})
    )
    first_starts: Mapping[int, int] = field(default_factory=lambda: MappingProxyType({}))
    """Per partition, where the first version that read it under this topic id began."""
    superseded: tuple[ConservationReport, ...] = ()
    """The earlier checkpoint versions of the query, each judged against its own range."""
    broker_topic_id: str | None = None
    """The broker's id for the topic when this was judged; None when it was not compared."""

    @property
    def conserved(self) -> bool:
        return (
            not self.problems
            and self.foreign_topic_rows == 0
            and all(verdict.conserved for verdict in self.partitions)
            and all(report.conserved for report in self.superseded)
        )

    @property
    def started_after_trim(self) -> bool:
        """The first checkpoint version to read some partition of this topic began above offset 0:
        records published before Bronze first read the topic had already been removed. Not a
        conservation failure -- they were never in any consumed range -- but a limit on what
        Bronze can vouch for."""
        return any(start > 0 for start in self.first_starts.values())

    def summary(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "table": self.table,
            "table_version": self.table_version,
            "checkpoint_version": self.checkpoint_version,
            "checkpoint_id": self.consumed.checkpoint_id,
            "topic_id": self.topic_id,
            "broker_topic_id": self.broker_topic_id,
            "through_batch": self.consumed.through_batch,
            "conserved": self.conserved,
            "started_after_trim": self.started_after_trim,
            "foreign_topic_rows": self.foreign_topic_rows,
            "problems": list(self.problems),
            "partitions": {
                str(v.partition): {
                    "range": None if v.expected is None else [v.expected.start, v.expected.end],
                    "missing": v.missing,
                    "duplicates": v.duplicates,
                    "out_of_range": v.out_of_range,
                    "skipped": [[r.start, r.end] for r in self.skipped.get(v.partition, ())],
                    "other_topic_id": v.other_topic_id,
                }
                for v in self.partitions
            },
            "superseded": [report.summary() for report in self.superseded],
        }


def judge_conservation(
    consumed: ConsumedRanges,
    rows: Mapping[int, PartitionRows],
    *,
    table: str,
    table_version: int | None,
    foreign_topic_rows: int,
    version: int | None = None,
    topic_id: str | None = None,
    superseded: Sequence[ConservationReport] = (),
    broker_topic_id: str | None = None,
) -> ConservationReport:
    """Judge one checkpoint version's rows against its consumed range.

    With `superseded` -- the query's earlier versions, each judged alone, in version order -- also
    judge the offsets between versions that no version read."""
    numbers = [report.checkpoint_version for report in superseded] + [version]
    if superseded and (
        any(number is None for number in numbers)
        or numbers != sorted(set(numbers), key=lambda n: -1 if n is None else n)
    ):
        raise ValueError(f"superseded versions must be numbered and precede v{version}: {numbers}")
    history = [
        VersionConsumption(report.checkpoint_version, report.topic_id, report.consumed)
        for report in superseded
    ]
    history.append(VersionConsumption(version, topic_id, consumed))
    skipped = skipped_between_versions(history)
    first_starts: dict[int, int] = {}
    for entry in history:
        if entry.topic_id == topic_id:
            for partition, consumed_range in entry.consumed.ranges.items():
                first_starts.setdefault(partition, consumed_range.start)
    verdicts: list[PartitionVerdict] = []
    for partition in sorted(set(consumed.ranges) | set(rows) | set(skipped)):
        expected = consumed.ranges.get(partition)
        held = rows.get(partition, PartitionRows(partition, 0, 0, 0, 0, 0))
        verdicts.append(
            PartitionVerdict(
                partition=partition,
                expected=expected,
                missing=0 if expected is None else expected.length - held.current_in_range_distinct,
                duplicates=held.rows - held.distinct_offsets,
                out_of_range=held.current_out_of_range,
                skipped=sum(r.length for r in skipped.get(partition, ())),
                other_topic_id=held.current_other_topic_id,
            )
        )
    return ConservationReport(
        topic=consumed.topic,
        table=table,
        table_version=table_version,
        consumed=consumed,
        partitions=tuple(verdicts),
        foreign_topic_rows=foreign_topic_rows,
        problems=consumed.problems,
        checkpoint_version=version,
        topic_id=topic_id,
        skipped=MappingProxyType(skipped),
        first_starts=MappingProxyType(first_starts),
        superseded=tuple(superseded),
        broker_topic_id=broker_topic_id,
    )


# --------------------------------------------------------------------- Spark ---


def read_partition_rows(
    spark: SparkSession,
    path: Path,
    *,
    version: int,
    topic: str,
    checkpoint_id: str,
    ranges: Mapping[int, OffsetRange],
    topic_id: str | None,
) -> tuple[dict[int, PartitionRows], int]:
    """Per-partition statistics of a Bronze table at `version`, and its rows of another topic."""
    from pyspark.sql import functions as F  # noqa: N812

    frame = spark.read.format("delta").option("versionAsOf", str(version)).load(str(path))
    partition, offset = F.col("kafka_partition"), F.col("kafka_offset")
    in_range: Any = None
    for number, expected in sorted(ranges.items()):
        condition = (offset >= F.lit(expected.start)) & (offset < F.lit(expected.end))
        in_range = (
            F.when(partition == F.lit(number), condition)
            if in_range is None
            else in_range.when(partition == F.lit(number), condition)
        )
    inside = F.lit(False) if in_range is None else in_range.otherwise(F.lit(False))
    current = F.col("bronze_checkpoint_id") == F.lit(checkpoint_id)
    other_id = F.lit(False) if topic_id is None else F.col("kafka_topic_id") != F.lit(topic_id)
    foreign = frame.where(F.col("kafka_topic") != F.lit(topic)).count()
    stats = (
        frame.where(F.col("kafka_topic") == F.lit(topic))
        .groupBy(partition)
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct(F.col("kafka_topic_id"), offset).alias("distinct_offsets"),
            F.sum(F.when(current, 1).otherwise(0)).alias("current_rows"),
            F.countDistinct(F.when(current & inside, offset)).alias("current_in_range_distinct"),
            F.sum(F.when(current & ~inside, 1).otherwise(0)).alias("current_out_of_range"),
            F.sum(F.when(current & other_id, 1).otherwise(0)).alias("current_other_topic_id"),
        )
        .collect()
    )
    return {
        int(row["kafka_partition"]): PartitionRows(
            partition=int(row["kafka_partition"]),
            rows=int(row["rows"]),
            distinct_offsets=int(row["distinct_offsets"]),
            current_rows=int(row["current_rows"]),
            current_in_range_distinct=int(row["current_in_range_distinct"]),
            current_out_of_range=int(row["current_out_of_range"]),
            current_other_topic_id=int(row["current_other_topic_id"]),
        )
        for row in stats
    }, int(foreign)


def _version_identity(directory: Path) -> CheckpointIdentity:
    path = directory / checkpoints.IDENTITY_FILENAME
    if not path.is_file():
        raise CheckpointRefusedError(
            f"{directory} has no {checkpoints.IDENTITY_FILENAME}: the app id its rows carry is "
            f"unknown, so what it consumed cannot be judged"
        )
    return CheckpointIdentity.from_json(path.read_text(encoding="utf-8"), source=path)


def check_conservation(
    spark: SparkSession, lake: LakeConfig, topic: str, *, broker_topic_id: str | None
) -> ConservationReport:
    """Judge the topic's Bronze table against every checkpoint version of its query.

    `broker_topic_id` is the broker's current id for the topic (`bronze.fetch_topic_identity`),
    compared with the id the current version recorded. None judges the lake alone, and the report
    and its log say it was not compared."""
    spec = bronze_topic(topic)
    state = checkpoints.read_state(lake, spec.query)
    if state.identity is None:
        raise CheckpointRefusedError(
            f"{spec.query}: no checkpoint identity, so there is no consumed range to judge"
        )
    root = checkpoints.query_directory(lake, spec.query)
    path = spec.table.local_path(lake)
    directories = {number: root / f"v{number}" for number in state.versions}
    progress, facts = read_progress_around_snapshot(
        directories, lambda: snapshot_facts(spark, path)
    )
    if facts is None:
        raise LakeContractError(f"{spec.table} does not exist at {path}")
    require_no_drift(
        bronze_declaration(topic), describe_live_table(spark, spec.table.path_identifier(lake))
    )
    reports: list[ConservationReport] = []
    for number in state.versions:
        directory = directories[number]
        current = number == state.identity.version
        identity = state.identity if current else _version_identity(directory)
        recorded = read_topic_identity(directory)
        consumed = consumed_ranges(
            topic=topic,
            checkpoint_id=identity.app_id,
            directory=directory,
            progress=progress[number],
            written_batch=facts.transactions.get(identity.app_id),
        )
        problems = list(consumed.problems)
        read_anything = (
            read_initial_offsets(directory, topic) is not None
            or bool(progress[number].planned)
            or identity.app_id in facts.transactions
        )
        if recorded is None and read_anything:
            problems.append(
                f"checkpoint v{number} read {topic} but recorded no topic id "
                f"({TOPIC_IDENTITY_FILENAME}), so nothing proves which topic its offsets belong to"
            )
        elif recorded is not None and recorded.topic != topic:
            problems.append(
                f"checkpoint v{number} recorded reading topic {recorded.topic!r}, not {topic!r}"
            )
        elif current and recorded is not None and broker_topic_id not in (None, recorded.topic_id):
            problems.append(
                f"the broker's {topic} has id {broker_topic_id}, but checkpoint v{number} recorded "
                f"reading id {recorded.topic_id}: the topic was deleted and recreated, so the "
                f"broker's offsets are not the offsets this checkpoint consumed"
            )
        consumed = replace(consumed, problems=tuple(problems))
        topic_id = None if recorded is None else recorded.topic_id
        rows, foreign = read_partition_rows(
            spark,
            path,
            version=facts.version,
            topic=topic,
            checkpoint_id=identity.app_id,
            ranges=consumed.ranges,
            topic_id=topic_id,
        )
        reports.append(
            judge_conservation(
                consumed,
                rows,
                table=str(spec.table),
                table_version=facts.version,
                foreign_topic_rows=foreign,
                version=number,
                topic_id=topic_id,
                superseded=tuple(reports) if current else (),
                broker_topic_id=broker_topic_id if current else None,
            )
        )
    report = reports[-1]
    if broker_topic_id is None:
        _log.warning("bronze_conservation_topic_id_not_compared", topic=topic, query=spec.query)
    (_log.info if report.conserved else _log.error)("bronze_conservation", **report.summary())
    return report


__all__ = [
    "INITIAL_OFFSETS_PARTS",
    "ConservationReport",
    "ConsumedRanges",
    "OffsetRange",
    "PartitionRows",
    "PartitionVerdict",
    "Record",
    "VersionConsumption",
    "check_conservation",
    "consumed_ranges",
    "judge_conservation",
    "parse_batch_offsets",
    "parse_initial_offsets",
    "partition_rows_from_records",
    "read_batch_offsets",
    "read_initial_offsets",
    "read_partition_rows",
    "read_progress_around_snapshot",
    "skipped_between_versions",
]
