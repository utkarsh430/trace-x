"""Bronze: every released Kafka topic, byte for byte, in its own append-only Delta table.

Bronze is the replayable record of what actually arrived (docs/DATA_ENGINEERING.md §2). It never
parses: a poison value is stored exactly as the broker delivered it, and Silver decides what it is.

**One table and one query per released topic.**
- Table `bronze.<topic with dots as underscores>` (`bronze.tx_raw_v1`), query
  `bronze_ingest_<table>` (`bronze_ingest_tx_raw_v1`), checkpoint
  `<lake>/_checkpoints/<query>/v<N>/` (ADR-0048; U9).
- Why not one query over every topic: a checkpoint records its sources, and changing them needs
  `reset_checkpoint`, which re-reads every source from its recorded start and so duplicates every
  row of an append-only sink. Releasing a topic must not force that. Per topic, a newly released
  topic is a new query and nothing else is touched; each commit covers exactly one topic's batch;
  and conservation is judged per checkpoint.

**Row (Q6).** The raw Kafka value and key bytes, every header in delivery order (a Kafka header
name may repeat, so they are an array, never a map), topic, the broker's topic id the checkpoint
recorded reading, partition, offset, the Kafka timestamp and its type, `trust_tier = UNTRUSTED` for
the whole record, the Bronze ingest time, and the micro-batch and checkpoint app id that wrote the
row -- so a duplicate, should one ever exist, is attributable to the checkpoint that wrote it, and
offsets of a deleted and recreated topic are never mistaken for duplicates of the old topic's.

**No skipped data (PHASE3_PLAN §4.1 point 8).**
- `failOnDataLoss=true` always, from the checkpoint's own options.
- A new checkpoint starts at `earliest`. A start position is never `latest`, never unspecified,
  and never an explicit `-1` (Spark's spelling of latest), which `KafkaSourceStart` does not see.
- A resumed query requests the start its checkpoint recorded, so a reset with explicit offsets
  resumes rather than being refused.
- **A recreated topic is refused.** Spark tracks offsets, not topic identity: a topic deleted and
  recreated, and refilled past the checkpoint's offset, is resumed from that offset and every
  earlier record of the new topic is skipped with no error. The checkpoint version records the
  topic id it read (`trace-kafka-topic.json`) before Spark records anything, and a different id is
  refused: before the query starts, and again by every micro-batch before and after it is written,
  since a topic can be recreated under a running query.

pyspark and confluent-kafka are imported only inside the functions that use them.
"""

from __future__ import annotations

import functools
import json
import os
import re
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final

from trace_core.contracts.topics import PARTITION_KEY_FIELD
from trace_core.domain.enums import TrustTier
from trace_core.domain.errors import (
    CheckpointRefusedError,
    LakeContractError,
    LakeNameError,
    StreamingSourceRetentionError,
    UnreleasedTopicError,
)
from trace_core.observability import get_logger, get_meter
from trace_core.stream import checkpoints
from trace_core.stream.checkpoints import KafkaSourceStart, OpenedCheckpoint, SparkProgress
from trace_core.stream.lake import LakeConfig, Tier, require_identifier
from trace_core.stream.tables import CommitProvenance, TableDeclaration, TableRef, create_table

if TYPE_CHECKING:
    from opentelemetry.metrics import Counter as MetricCounter
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery
    from pyspark.sql.types import StructType

BRONZE_JOB: Final = "bronze_ingest"
"""The Databricks job and the prefix of every Bronze query name (DATA_ENGINEERING §8; U9)."""

TOPIC_IDENTITY_FILENAME: Final = "trace-kafka-topic.json"
TOPIC_IDENTITY_FORMAT: Final = 1

SPARK_LOG_APPEND_TIME: Final = 1
"""Spark's `timestampType` column is Kafka's Java `TimestampType.id`: 0 CreateTime, 1
LogAppendTime. confluent-kafka numbers the same types 1 and 2: the values never compare directly."""

SPARK_EARLIEST_OFFSET: Final = -2
SPARK_LATEST_OFFSET: Final = -1
"""Spark's explicit `startingOffsets` JSON spells earliest -2 and latest -1."""

KAFKA_SOURCE_COLUMNS: Final = (
    "key",
    "value",
    "topic",
    "partition",
    "offset",
    "timestamp",
    "timestampType",
    "headers",
)
"""What Spark's Kafka source delivers with `includeHeaders=true`."""

BRONZE_COLUMNS: Final = (
    "kafka_topic",
    "kafka_topic_id",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "kafka_timestamp_type",
    "kafka_key",
    "kafka_value",
    "kafka_headers",
    "trust_tier",
    "bronze_ingested_at",
    "bronze_batch_id",
    "bronze_checkpoint_id",
)

BRONZE_PROPERTIES: Final[Mapping[str, str]] = MappingProxyType({"delta.appendOnly": "true"})
"""Bronze is never rewritten (DATA_ENGINEERING §2): `appendOnly` refuses DELETE and UPDATE and was
observed to add no protocol feature (ADR-0048 (a), (k)). Step 11's local retention will have to
change this declaration explicitly; the drift check refuses a table that changed silently."""

_PARTITION: Final = re.compile(r"0|[1-9][0-9]{0,9}")
_NO_TOPIC_ID: Final = frozenset(
    {"", "AAAAAAAAAAAAAAAAAAAAAA", "00000000-0000-0000-0000-000000000000"}
)
"""Kafka's zero UUID, as base64 and as hex: what a broker without topic ids reports."""

_log = get_logger(__name__)


# ------------------------------------------------------------------ registry ---


@dataclass(frozen=True, slots=True)
class BronzeTopic:
    """A released topic, the Bronze table it lands in, and the query that writes it."""

    topic: str
    table: TableRef
    query: str


def bronze_table_name(topic: str) -> str:
    """`tx.raw.v1` -> `tx_raw_v1`. Refused unless the result is a lake identifier."""
    return require_identifier("Bronze table name", topic.replace(".", "_"))


def _registry() -> Mapping[str, BronzeTopic]:
    entries: dict[str, BronzeTopic] = {}
    for topic in sorted(PARTITION_KEY_FIELD):
        name = bronze_table_name(topic)
        query = require_identifier("query name", f"{BRONZE_JOB}_{name}")
        entries[topic] = BronzeTopic(topic, TableRef(Tier.BRONZE, name), query)
    names = [entry.table.name for entry in entries.values()]
    if len(set(names)) != len(names):
        raise LakeNameError(f"two released topics map to one Bronze table: {sorted(names)}")
    return MappingProxyType(entries)


BRONZE_TOPICS: Final[Mapping[str, BronzeTopic]] = _registry()
"""Every released topic (`trace_core.contracts.topics`, diffed against RELEASED.json by a contract
test, and against this registry by `tests/unit/test_bronze_registry.py`)."""


def bronze_topic(topic: str) -> BronzeTopic:
    try:
        return BRONZE_TOPICS[topic]
    except KeyError:
        raise UnreleasedTopicError(
            f"{topic!r} is not a released topic, so it has no Bronze table; Bronze ingests only "
            f"topics in docs/contracts/RELEASED.json ({sorted(BRONZE_TOPICS)})"
        ) from None


def bronze_schema() -> StructType:
    """The Bronze row. Nested header fields stay nullable: a Kafka header value may be null, and
    Delta does not enforce NOT NULL inside an array."""
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    header = StructType([StructField("key", StringType()), StructField("value", BinaryType())])
    return StructType(
        [
            StructField("kafka_topic", StringType(), nullable=False),
            StructField("kafka_topic_id", StringType(), nullable=False),
            StructField("kafka_partition", IntegerType(), nullable=False),
            StructField("kafka_offset", LongType(), nullable=False),
            StructField("kafka_timestamp", TimestampType(), nullable=False),
            StructField("kafka_timestamp_type", IntegerType(), nullable=False),
            StructField("kafka_key", BinaryType(), nullable=True),
            StructField("kafka_value", BinaryType(), nullable=True),
            StructField("kafka_headers", ArrayType(header, containsNull=True), nullable=True),
            StructField("trust_tier", StringType(), nullable=False),
            StructField("bronze_ingested_at", TimestampType(), nullable=False),
            StructField("bronze_batch_id", LongType(), nullable=False),
            StructField("bronze_checkpoint_id", StringType(), nullable=False),
        ]
    )


def bronze_declaration(topic: str) -> TableDeclaration:
    """Unpartitioned and unclustered: layout is decided by the Step 14 benchmark (ADR-0015)."""
    return TableDeclaration(
        ref=bronze_topic(topic).table, schema=bronze_schema(), properties=dict(BRONZE_PROPERTIES)
    )


# ----------------------------------------------------------------- transform ---


def bronze_frame(
    frame: DataFrame, *, batch_id: int, checkpoint_id: str, topic_id: str
) -> DataFrame:
    """A Kafka source micro-batch as Bronze rows. Selects and renames; never parses.

    `topic_id` is the id the checkpoint recorded reading (`read_topic_identity`); the sink proves
    the broker still has it around every batch (`bronze_sink`)."""
    from pyspark.sql import functions as F  # noqa: N812

    if batch_id < 0:
        raise CheckpointRefusedError(f"batch id {batch_id} is negative")
    if topic_id.strip() in _NO_TOPIC_ID:
        raise CheckpointRefusedError(f"topic id {topic_id!r} identifies no topic")
    missing = sorted(set(KAFKA_SOURCE_COLUMNS) - set(frame.columns))
    if missing:
        raise LakeContractError(
            f"the micro-batch lacks Kafka source columns {missing}; Bronze reads with "
            f"includeHeaders=true so that every header is kept"
        )
    return frame.select(
        F.col("topic").alias("kafka_topic"),
        F.lit(topic_id).alias("kafka_topic_id"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("timestampType").alias("kafka_timestamp_type"),
        F.col("key").alias("kafka_key"),
        F.col("value").alias("kafka_value"),
        F.col("headers").alias("kafka_headers"),
        F.lit(TrustTier.UNTRUSTED.value).alias("trust_tier"),
        F.current_timestamp().alias("bronze_ingested_at"),
        F.lit(batch_id).cast("bigint").alias("bronze_batch_id"),
        F.lit(checkpoint_id).alias("bronze_checkpoint_id"),
    )


# ------------------------------------------------------------ start position ---


def require_never_latest(topic: str, starting_offsets: str | None) -> str:
    """`earliest`, or explicit offsets naming only `topic` with every offset >= 0 or -2.

    Unspecified means latest for a streaming read, and so does an explicit -1, which
    `KafkaSourceStart` does not look inside the JSON for."""
    if starting_offsets is None:
        raise CheckpointRefusedError(
            f"{topic}: no startingOffsets; a streaming Kafka read without one starts at latest and "
            f"skips everything already published (PHASE3_PLAN §4.1 point 8)"
        )
    text = starting_offsets.strip()
    if text == "earliest":
        return text
    if text.lower() == "latest":
        raise CheckpointRefusedError(f"{topic}: a Bronze query may never start at latest")
    try:
        offsets = json.loads(text)
    except ValueError as exc:
        raise CheckpointRefusedError(
            f"{topic}: startingOffsets {text!r} is neither 'earliest' nor offsets JSON"
        ) from exc
    if not isinstance(offsets, dict) or set(offsets) != {topic}:
        raise CheckpointRefusedError(f"{topic}: explicit starting offsets must name only {topic}")
    partitions = offsets[topic]
    if not isinstance(partitions, dict) or not partitions:
        raise CheckpointRefusedError(f"{topic}: explicit starting offsets name no partition")
    for partition, offset in partitions.items():
        if not isinstance(partition, str) or not _PARTITION.fullmatch(partition):
            raise CheckpointRefusedError(f"{topic}: {partition!r} is not a partition number")
        if isinstance(offset, bool) or not isinstance(offset, int):
            raise CheckpointRefusedError(
                f"{topic}[{partition}]: offset {offset!r} is not an integer"
            )
        if offset == SPARK_LATEST_OFFSET:
            raise CheckpointRefusedError(
                f"{topic}[{partition}]: offset -1 is Spark's spelling of latest; a Bronze query "
                f"may never start there"
            )
        if offset < SPARK_EARLIEST_OFFSET:
            raise CheckpointRefusedError(f"{topic}[{partition}]: offset {offset} is not valid")
    return text


def kafka_reader_options(
    topic: str,
    recorded: Mapping[str, str],
    *,
    bootstrap_servers: str,
    max_offsets_per_trigger: int | None = None,
) -> dict[str, str]:
    """The only options a Bronze Kafka reader is built with.

    `recorded` is `OpenedCheckpoint.kafka_options(topic)`. Nothing else may add a start position, a
    topic pattern or a loss tolerance: every option is set here, from arguments that cannot."""
    if recorded.get("failOnDataLoss") != "true":
        raise StreamingSourceRetentionError(
            f"{topic}: the checkpoint's reader options do not set failOnDataLoss=true"
        )
    starting = require_never_latest(topic, recorded.get("startingOffsets"))
    if not bootstrap_servers.strip():
        raise LakeContractError(f"{topic}: no Kafka bootstrap servers")
    options = {
        "kafka.bootstrap.servers": bootstrap_servers.strip(),
        "subscribe": topic,
        "startingOffsets": starting,
        "failOnDataLoss": "true",
        "includeHeaders": "true",
    }
    if max_offsets_per_trigger is not None:
        if max_offsets_per_trigger < 1:
            raise ValueError(f"max_offsets_per_trigger must be positive: {max_offsets_per_trigger}")
        options["maxOffsetsPerTrigger"] = str(max_offsets_per_trigger)
    return options


def recorded_start(lake: LakeConfig, spec: BronzeTopic) -> str:
    """The start the current checkpoint version recorded for the topic, or `earliest` for a new one.

    `open_checkpoint` still refuses anything else that changed; this only avoids refusing a resume
    after a reset that recorded explicit offsets."""
    state = checkpoints.read_state(lake, spec.query)
    record = None if state.identity is None else state.identity.sources.get(f"kafka:{spec.topic}")
    return "earliest" if record is None else record.start


# ------------------------------------------------------------ topic identity ---


@dataclass(frozen=True, slots=True)
class TopicIdentity:
    """The broker's id for a topic: it changes when a topic is deleted and recreated."""

    topic: str
    topic_id: str

    def to_json(self) -> str:
        return json.dumps(
            {"format": TOPIC_IDENTITY_FORMAT, "topic": self.topic, "topic_id": self.topic_id},
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, text: str, *, source: Path) -> TopicIdentity:
        try:
            data = json.loads(text)
            if data != {
                "format": TOPIC_IDENTITY_FORMAT,
                "topic": data.get("topic"),
                "topic_id": data.get("topic_id"),
            }:
                raise ValueError(f"unexpected content {data!r}")
            if not isinstance(data["topic"], str) or not isinstance(data["topic_id"], str):
                raise ValueError("topic and topic_id must be strings")
            return cls(data["topic"], data["topic_id"])
        except (ValueError, AttributeError, TypeError) as exc:
            raise CheckpointRefusedError(f"unreadable topic identity {source}: {exc}") from exc


def read_topic_identity(directory: Path) -> TopicIdentity | None:
    """The topic id a checkpoint version recorded reading, or None when it recorded none."""
    path = directory / TOPIC_IDENTITY_FILENAME
    if not path.is_file():
        return None
    return TopicIdentity.from_json(path.read_text(encoding="utf-8"), source=path)


def require_same_topic(
    recorded: TopicIdentity, current: TopicIdentity, *, directory: Path, when: str
) -> None:
    """Refuse unless the broker still has the topic a checkpoint version recorded reading."""
    if recorded != current:
        raise CheckpointRefusedError(
            f"{current.topic}: checkpoint {directory} read topic {recorded.topic!r} with id "
            f"{recorded.topic_id}, but the broker now has {current.topic!r} with id "
            f"{current.topic_id} ({when}). The topic was deleted and recreated, and reading on "
            f"from the old offsets would silently skip the new topic's earlier records. Reset the "
            f"checkpoint with trace_core.stream.checkpoints.reset_checkpoint and a recorded "
            f"reason; never delete it."
        )


def require_topic_identity(
    directory: Path, current: TopicIdentity, progress: SparkProgress
) -> bool:
    """Record the topic id a new checkpoint version reads, or refuse a resume over another topic.

    True when it recorded the id (Spark had recorded nothing yet), False when the recorded id
    matched. A version that planned batches, or recorded where it began reading, without a record
    is refused: nothing proves which topic it read.
    """
    recorded = read_topic_identity(directory)
    if recorded is not None:
        require_same_topic(recorded, current, directory=directory, when="at start")
        return False
    began = directory.joinpath(*checkpoints.INITIAL_OFFSETS_PARTS).exists()
    if progress.planned or progress.committed or began:
        raise CheckpointRefusedError(
            f"{current.topic}: checkpoint {directory} has "
            f"{'planned batches' if progress.planned or progress.committed else 'initial offsets'} "
            f"but no {TOPIC_IDENTITY_FILENAME}, so nothing proves it read the topic that exists now"
        )
    path = directory / TOPIC_IDENTITY_FILENAME
    staging = directory / f".{TOPIC_IDENTITY_FILENAME}.{uuid.uuid4().hex}"
    with staging.open("x", encoding="utf-8") as handle:
        handle.write(current.to_json())
        handle.flush()
        os.fsync(handle.fileno())
    staging.replace(path)
    return True


def fetch_topic_identity(
    bootstrap_servers: str, topic: str, *, timeout_s: float = 10.0
) -> TopicIdentity:
    """The broker's topic id, or a refusal when it cannot be read."""
    from confluent_kafka import TopicCollection
    from confluent_kafka.admin import AdminClient

    admin: Any = AdminClient({"bootstrap.servers": bootstrap_servers})
    try:
        futures = admin.describe_topics(TopicCollection([topic]), request_timeout=timeout_s)
        description = futures[topic].result(timeout=timeout_s)
    except Exception as exc:  # confluent_kafka.KafkaException, imported lazily
        raise CheckpointRefusedError(
            f"{topic}: cannot read the topic's id from {bootstrap_servers} ({exc}); Bronze does "
            f"not start without proving which topic its checkpoint reads"
        ) from exc
    topic_id = "" if description.topic_id is None else str(description.topic_id).strip()
    if topic_id in _NO_TOPIC_ID:
        raise CheckpointRefusedError(f"{topic}: the broker reported no topic id ({topic_id!r})")
    return TopicIdentity(topic, topic_id)


# ---------------------------------------------------------------- the query ---


@dataclass(frozen=True, slots=True)
class Trigger:
    """`available_now` drains what is published and stops; otherwise a processing-time interval."""

    available_now: bool = False
    interval_s: float | None = None

    def __post_init__(self) -> None:
        if self.available_now == (self.interval_s is not None):
            raise ValueError("a trigger is either available_now or an interval, exactly one")
        if self.interval_s is not None and self.interval_s <= 0:
            raise ValueError(f"interval_s must be positive: {self.interval_s}")


@dataclass(frozen=True, slots=True)
class BronzeQuery:
    spec: BronzeTopic
    opened: OpenedCheckpoint
    query: StreamingQuery


def start_bronze_query(
    spark: SparkSession,
    lake: LakeConfig,
    topic: str,
    *,
    bootstrap_servers: str,
    git_sha: str,
    dirty_worktree: bool,
    now: datetime,
    trigger: Trigger,
    max_offsets_per_trigger: int | None = None,
) -> BronzeQuery:
    """Create the topic's table from its declaration, open its checkpoint, prove the topic, start.

    Every refusal happens before the query starts: a drifted table, an untrustworthy checkpoint
    (`open_checkpoint`), a start at latest, or a recreated topic."""
    spec = bronze_topic(topic)
    create_table(
        spark,
        bronze_declaration(topic),
        lake,
        CommitProvenance(git_sha=git_sha, dirty_worktree=dirty_worktree, query=spec.query),
    )
    opened = checkpoints.open_checkpoint(
        spark,
        lake,
        spec.query,
        targets=[spec.table],
        sources=[KafkaSourceStart(topic, recorded_start(lake, spec))],
        git_sha=git_sha,
        dirty_worktree=dirty_worktree,
        now=now,
    )
    identity = fetch_topic_identity(bootstrap_servers, topic)
    recorded = require_topic_identity(
        opened.directory, identity, SparkProgress.read(opened.directory)
    )
    _log.info(
        "bronze_topic_identity",
        topic=topic,
        topic_id=identity.topic_id,
        recorded_now=recorded,
        checkpoint_version=opened.identity.version,
    )
    query = _start_stream(
        spark,
        spec,
        opened,
        bootstrap_servers=bootstrap_servers,
        trigger=trigger,
        max_offsets_per_trigger=max_offsets_per_trigger,
        recorded=identity,
        current_topic=functools.partial(fetch_topic_identity, bootstrap_servers, topic),
    )
    return BronzeQuery(spec, opened, query)


def bronze_sink(
    spec: BronzeTopic,
    opened: OpenedCheckpoint,
    *,
    recorded: TopicIdentity,
    current_topic: Callable[[], TopicIdentity] | None,
) -> Callable[[DataFrame, int], None]:
    """The `foreachBatch` function: one idempotent append per micro-batch, between two topic checks.

    Spark planned the batch's offsets against whatever topic the broker had then, and reads its
    records when the append runs. So the broker's topic id is compared with the recorded one before
    the append -- a recreated topic writes nothing -- and again after it, before Spark records the
    batch as done: a topic recreated while the batch was read stops the query instead of letting it
    run on. A failed check raises, which fails the query. `current_topic` None skips both checks;
    only a test demonstrating what the checks prevent passes it."""
    checkpoint_id = opened.identity.app_id

    def check(batch_id: int, when: str) -> None:
        if current_topic is not None:
            require_same_topic(
                recorded,
                current_topic(),
                directory=opened.directory,
                when=f"batch {batch_id}, {when}",
            )

    def sink(batch: DataFrame, batch_id: int) -> None:
        check(batch_id, "before it was written")
        opened.append(
            bronze_frame(
                batch, batch_id=batch_id, checkpoint_id=checkpoint_id, topic_id=recorded.topic_id
            ),
            batch_id=batch_id,
            target=spec.table,
        )
        check(batch_id, "after it was written")

    return sink


def _start_stream(
    spark: SparkSession,
    spec: BronzeTopic,
    opened: OpenedCheckpoint,
    *,
    bootstrap_servers: str,
    trigger: Trigger,
    max_offsets_per_trigger: int | None,
    recorded: TopicIdentity,
    current_topic: Callable[[], TopicIdentity] | None,
) -> StreamingQuery:
    """The query itself, with no pre-start guard. Only `start_bronze_query` should call it."""
    options = kafka_reader_options(
        spec.topic,
        opened.kafka_options(spec.topic),
        bootstrap_servers=bootstrap_servers,
        max_offsets_per_trigger=max_offsets_per_trigger,
    )
    reader: Any = spark.readStream.format("kafka")
    for key, value in sorted(options.items()):
        reader = reader.option(key, value)
    frame: DataFrame = reader.load()
    checkpoint_id = opened.identity.app_id
    writer: Any = (
        frame.writeStream.queryName(spec.query)
        .option("checkpointLocation", str(opened.directory))
        .foreachBatch(bronze_sink(spec, opened, recorded=recorded, current_topic=current_topic))
    )
    if trigger.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{trigger.interval_s} seconds")
    query: StreamingQuery = writer.start()
    _log.info(
        "bronze_query_started",
        topic=spec.topic,
        query=spec.query,
        table=str(spec.table),
        checkpoint_version=opened.identity.version,
        app_id=checkpoint_id,
        action=opened.action.value,
        starting_offsets=options["startingOffsets"],
        available_now=trigger.available_now,
        topic_id=recorded.topic_id,
        topic_checked_per_batch=current_topic is not None,
    )
    return query


# ------------------------------------------------------------------ progress ---

BRONZE_INPUT_ROWS_TOTAL: Final = "bronze_input_rows_total"
BRONZE_BATCHES_TOTAL: Final = "bronze_batches_total"


@functools.cache
def _instruments() -> tuple[MetricCounter, MetricCounter]:
    meter = get_meter("trace_core.stream.bronze")
    rows = meter.create_counter(
        BRONZE_INPUT_ROWS_TOTAL, description="Kafka records a Bronze micro-batch read, by topic."
    )
    batches = meter.create_counter(
        BRONZE_BATCHES_TOTAL, description="Bronze micro-batches completed, by topic."
    )
    return rows, batches


@dataclass(frozen=True, slots=True)
class BatchProgress:
    """One completed micro-batch, from Spark's progress event."""

    topic: str
    batch_id: int
    input_rows: int
    end_offsets: Mapping[int, int]
    latest_offsets: Mapping[int, int] | None

    @property
    def offsets_behind(self) -> int | None:
        """Records published but not yet read when the batch was planned, over all partitions."""
        if self.latest_offsets is None:
            return None
        return sum(
            max(0, self.latest_offsets.get(p, 0) - end) for p, end in self.end_offsets.items()
        )


def _partition_offsets(value: Any, topic: str) -> dict[int, int] | None:
    if value is None:
        return None
    data = json.loads(value) if isinstance(value, str) else value
    if not isinstance(data, dict) or topic not in data or not isinstance(data[topic], dict):
        raise LakeContractError(f"{topic}: unreadable offsets in a progress event: {value!r}")
    return {int(p): int(o) for p, o in data[topic].items()}


def batch_progress(topic: str, progress: Mapping[str, Any]) -> BatchProgress:
    """Parse a `StreamingQueryProgress` JSON object of a Bronze query (one Kafka source)."""
    sources = progress.get("sources")
    if not isinstance(sources, list) or len(sources) != 1:
        raise LakeContractError(
            f"{topic}: a Bronze progress event must have one source: {sources!r}"
        )
    source = sources[0]
    end = _partition_offsets(source.get("endOffset"), topic)
    if end is None:
        raise LakeContractError(f"{topic}: a completed batch has no end offset")
    return BatchProgress(
        topic=topic,
        batch_id=int(progress["batchId"]),
        input_rows=int(progress["numInputRows"]),
        end_offsets=end,
        latest_offsets=_partition_offsets(source.get("latestOffset"), topic),
    )


def record_progress(handle: BronzeQuery, seen: set[int]) -> list[BatchProgress]:
    """Log and count every completed batch not yet in `seen`, and add it there."""
    rows_total, batches_total = _instruments()
    recorded: list[BatchProgress] = []
    for event in handle.query.recentProgress:
        data = json.loads(event.json)
        if int(data["batchId"]) in seen:
            continue
        progress = batch_progress(handle.spec.topic, data)
        seen.add(progress.batch_id)
        rows_total.add(progress.input_rows, {"topic": handle.spec.topic})
        batches_total.add(1, {"topic": handle.spec.topic})
        _log.info(
            "bronze_batch_completed",
            topic=handle.spec.topic,
            query=handle.spec.query,
            batch_id=progress.batch_id,
            input_rows=progress.input_rows,
            offsets_behind=progress.offsets_behind,
        )
        recorded.append(progress)
    return recorded


def released_topics(selected: Sequence[str] | None = None) -> tuple[str, ...]:
    """Every released topic, or the selected ones -- each of which must be released."""
    if not selected:
        return tuple(BRONZE_TOPICS)
    return tuple(bronze_topic(topic).topic for topic in selected)


__all__ = [
    "BRONZE_BATCHES_TOTAL",
    "BRONZE_COLUMNS",
    "BRONZE_INPUT_ROWS_TOTAL",
    "BRONZE_JOB",
    "BRONZE_PROPERTIES",
    "BRONZE_TOPICS",
    "KAFKA_SOURCE_COLUMNS",
    "SPARK_EARLIEST_OFFSET",
    "SPARK_LATEST_OFFSET",
    "SPARK_LOG_APPEND_TIME",
    "TOPIC_IDENTITY_FILENAME",
    "BatchProgress",
    "BronzeQuery",
    "BronzeTopic",
    "TopicIdentity",
    "Trigger",
    "batch_progress",
    "bronze_declaration",
    "bronze_frame",
    "bronze_schema",
    "bronze_sink",
    "bronze_table_name",
    "bronze_topic",
    "fetch_topic_identity",
    "kafka_reader_options",
    "read_topic_identity",
    "record_progress",
    "recorded_start",
    "released_topics",
    "require_never_latest",
    "require_same_topic",
    "require_topic_identity",
    "start_bronze_query",
]
