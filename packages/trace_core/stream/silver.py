"""Silver: Bronze rows become validated, exactly deduplicated, late-tagged canonical events.

ADR-0053. One Structured Streaming query per released topic, `silver_transform_<topic_name>`, reads
that topic's Bronze table through its checkpoint's loss-refusing Delta source, from Bronze version
0. Each micro-batch:

1. **Admission** (`silver_rules.admit`) runs on the executors as a row-at-a-time Python UDF, with
   the producers' own strict contract models. Bronze columns the decision does not need stay in the
   JVM; timestamps cross into Python as epoch microseconds, so no time zone is ever interpreted.
   Spark's Arrow map APIs would batch this, but they require pandas, which is not a locked
   dependency.
2. **Classification** (`silver_rules.classify`, mirrored in Spark SQL and held equal to it by the
   stream tests) against the canonical rows the batch's identities already have: admit, supersede,
   replayed, duplicate, or conflict.
3. **Writes**, each the batch's one idempotent commit to its target through `OpenedCheckpoint`, in
   this order, so a replay after a crash between any two re-derives the rest:
   - `silver.quarantine` (append): every rejected record and every identity conflict, with its raw
     bytes;
   - `silver.duplicates` (append): every identical duplicate, and every canonical row a supersede
     replaces, with the canonical row it now points to;
   - the canonical table (MERGE on the identity): insert-only, except `tx.scored.v1`, where a
     record with the same digest and an earlier key replaces the row;
   - `silver.late_events` (MERGE on topic and identity): re-derived from the committed canonical
     rows of the batch's identities, so it is exactly "the canonical rows that are late".

   Both MERGEs, and the read of the committed rows, are bounded on `occurred_at` so that their cost
   does not grow with the table, exactly (the bounded-MERGEs section; ADR-0053 Amendment 1).
4. **Assertions** after the writes: no identity of the batch appears twice in the canonical table,
   and no (topic, identity) twice in `late_events`. A failure raises and stops the query.

**Replays and resets.** A replayed batch recomputes the same decisions: commits that landed are
skipped by their transaction identity, and a canonical row is recognised as its own record by its
Kafka coordinates. A checkpoint reset re-reading Bronze from version 0 re-derives the same canonical
rows and the same `late_events`. `silver.duplicates` and `silver.quarantine` are written per
checkpoint version: a reset appends them again under the new version's app id, and a reader filters
them by `silver_checkpoint_id`.

pyspark is imported only inside functions that run with a session or on an executor.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final

from trace_core.domain.errors import LakeContractError
from trace_core.observability import get_logger
from trace_core.stream import checkpoints
from trace_core.stream.bronze import Trigger, bronze_topic
from trace_core.stream.checkpoints import DeltaSourceStart, OpenedCheckpoint
from trace_core.stream.lake import LakeConfig
from trace_core.stream.silver_rules import (
    DIGESTED_EVENT_TIME,
    DUPLICATES,
    KEY_NULL,
    LATE_EVENTS,
    QUARANTINE,
    RECORDED,
    SHARED_TABLES,
    SUPERSEDABLE_TOPIC,
    BronzeRecord,
    ColumnSpec,
    Coordinates,
    Disposition,
    DuplicateKind,
    Outcome,
    QuarantineReason,
    SilverTopic,
    admit,
    column_value,
    event_columns,
    silver_topic,
)
from trace_core.stream.tables import (
    CommitProvenance,
    TableDeclaration,
    TableLayout,
    TableRef,
    create_table,
    retention_start,
    snapshot_facts,
)

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery
    from pyspark.sql.types import DataType, StructType

APPEND_ONLY: Final = MappingProxyType({"delta.appendOnly": "true"})
"""Rows are only ever inserted: every Silver table but the two `REWRITTEN_TABLES`."""

_log = get_logger(__name__)


# ------------------------------------------------------------------- schemas ---


def _spark_type(kind: str) -> DataType:
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        TimestampType,
    )

    return {
        "string": StringType(),
        "json": StringType(),
        "long": LongType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "timestamp": TimestampType(),
    }[kind]


def _headers_type() -> DataType:
    from pyspark.sql.types import ArrayType, BinaryType, StringType, StructField, StructType

    header = StructType([StructField("key", StringType()), StructField("value", BinaryType())])
    return ArrayType(header, containsNull=True)


def canonical_schema(topic: str) -> StructType:
    """The event's columns (from its released model), then Silver's own."""
    from pyspark.sql.types import (
        BooleanType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    fields = [
        StructField(column.name, _spark_type(column.kind), nullable=column.nullable)
        for column in event_columns(topic)
    ]
    fields += [
        StructField("silver_identity", StringType(), nullable=False),
        StructField("content_digest", StringType(), nullable=False),
        StructField("is_late", BooleanType(), nullable=True),
        StructField("arrival_delay_ms", LongType(), nullable=False),
        StructField("is_backfill", BooleanType(), nullable=False),
        StructField("trust_tier", StringType(), nullable=False),
        StructField("kafka_topic", StringType(), nullable=False),
        StructField("kafka_topic_id", StringType(), nullable=False),
        StructField("kafka_partition", IntegerType(), nullable=False),
        StructField("kafka_offset", LongType(), nullable=False),
        StructField("kafka_timestamp", TimestampType(), nullable=False),
        StructField("bronze_batch_id", LongType(), nullable=False),
        StructField("bronze_checkpoint_id", StringType(), nullable=False),
        StructField("silver_batch_id", LongType(), nullable=False),
        StructField("silver_checkpoint_id", StringType(), nullable=False),
        StructField("silver_admitted_at", TimestampType(), nullable=False),
    ]
    return StructType(fields)


def quarantine_schema() -> StructType:
    """Every Bronze row Silver did not admit, with its raw bytes.

    Rows are per checkpoint version: a checkpoint reset re-reads Bronze and appends them again
    under the new version's app id. A reader filters by `silver_checkpoint_id`."""
    from pyspark.sql.types import (
        BinaryType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("silver_topic", StringType(), nullable=False),
            StructField("reason", StringType(), nullable=False),
            StructField("detail", StringType(), nullable=True),
            StructField("silver_identity", StringType(), nullable=True),
            StructField("content_digest", StringType(), nullable=True),
            StructField("canonical_kafka_topic_id", StringType(), nullable=True),
            StructField("canonical_kafka_partition", IntegerType(), nullable=True),
            StructField("canonical_kafka_offset", LongType(), nullable=True),
            StructField("canonical_content_digest", StringType(), nullable=True),
            StructField("kafka_topic", StringType(), nullable=False),
            StructField("kafka_topic_id", StringType(), nullable=False),
            StructField("kafka_partition", IntegerType(), nullable=False),
            StructField("kafka_offset", LongType(), nullable=False),
            StructField("kafka_timestamp", TimestampType(), nullable=False),
            StructField("kafka_timestamp_type", IntegerType(), nullable=False),
            StructField("kafka_key", BinaryType(), nullable=True),
            StructField("kafka_value", BinaryType(), nullable=True),
            StructField("kafka_headers", _headers_type(), nullable=True),
            StructField("trust_tier", StringType(), nullable=False),
            StructField("consumer_git_sha", StringType(), nullable=False),
            StructField("bronze_batch_id", LongType(), nullable=False),
            StructField("bronze_checkpoint_id", StringType(), nullable=False),
            StructField("silver_batch_id", LongType(), nullable=False),
            StructField("silver_checkpoint_id", StringType(), nullable=False),
            StructField("silver_recorded_at", TimestampType(), nullable=False),
        ]
    )


def duplicates_schema() -> StructType:
    """Every admitted record that is not canonical: an identical `duplicate`, or a canonical row
    `superseded` by an earlier record in the order. Each points to the canonical row it was judged
    against; a later supersede can move that row, so a reader joins by identity and digest.

    Rows are per checkpoint version: a checkpoint reset re-reads Bronze and appends them again
    under the new version's app id. A reader filters by `silver_checkpoint_id`."""
    from pyspark.sql.types import (
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("silver_topic", StringType(), nullable=False),
            StructField("disposition", StringType(), nullable=False),
            StructField("silver_identity", StringType(), nullable=False),
            StructField("content_digest", StringType(), nullable=False),
            StructField("kafka_topic_id", StringType(), nullable=False),
            StructField("kafka_partition", IntegerType(), nullable=False),
            StructField("kafka_offset", LongType(), nullable=False),
            StructField("kafka_timestamp", TimestampType(), nullable=False),
            StructField("canonical_kafka_topic_id", StringType(), nullable=False),
            StructField("canonical_kafka_partition", IntegerType(), nullable=False),
            StructField("canonical_kafka_offset", LongType(), nullable=False),
            StructField("bronze_batch_id", LongType(), nullable=False),
            StructField("bronze_checkpoint_id", StringType(), nullable=False),
            StructField("silver_batch_id", LongType(), nullable=False),
            StructField("silver_checkpoint_id", StringType(), nullable=False),
            StructField("silver_recorded_at", TimestampType(), nullable=False),
        ]
    )


def late_events_schema() -> StructType:
    from pyspark.sql.types import (
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("silver_topic", StringType(), nullable=False),
            StructField("silver_identity", StringType(), nullable=False),
            StructField("occurred_at", TimestampType(), nullable=False),
            StructField("kafka_timestamp", TimestampType(), nullable=False),
            StructField("arrival_delay_ms", LongType(), nullable=False),
            StructField("kafka_topic_id", StringType(), nullable=False),
            StructField("kafka_partition", IntegerType(), nullable=False),
            StructField("kafka_offset", LongType(), nullable=False),
            StructField("silver_batch_id", LongType(), nullable=False),
            StructField("silver_checkpoint_id", StringType(), nullable=False),
            StructField("silver_recorded_at", TimestampType(), nullable=False),
        ]
    )


REWRITTEN_TABLES: Final = (silver_topic(SUPERSEDABLE_TOPIC).table, LATE_EVENTS)
"""The two Silver tables whose rows are rewritten, so they are not `delta.appendOnly`:

- `silver.tx_scored_v1`: its canonical row is the first of one total order (`order_key`), whatever
  batch each record arrives in. A retry recorded in a later batch, with the same digest and an
  earlier key, must replace the row an earlier batch admitted (MERGE ... WHEN MATCHED UPDATE).
- `silver.late_events`: a projection of the committed canonical rows that are late, keyed by topic
  and identity. It is re-derived by MERGE after each canonical commit, so a supersede that moves or
  un-lates a row updates or deletes its entry, and a replay or reset changes nothing.

Every other Silver table only ever inserts rows."""


def _properties(ref: TableRef) -> dict[str, str]:
    return {} if ref in REWRITTEN_TABLES else dict(APPEND_ONLY)


def silver_declaration(topic: str) -> TableDeclaration:
    """Unpartitioned and unclustered: layout is decided by the Step 14 benchmark (ADR-0015).

    Append-only, except `silver.tx_scored_v1` (see `REWRITTEN_TABLES`)."""
    ref = silver_topic(topic).table
    return TableDeclaration(ref=ref, schema=canonical_schema(topic), properties=_properties(ref))


LATE_EVENTS_LAYOUT: Final = TableLayout(partition_columns=("silver_topic",))
"""`silver.late_events` is partitioned by `silver_topic`, in every environment, for correctness.

Every topic's query MERGEs into it on every batch, and `silver run` runs the queries at once. On
the unpartitioned table, a MERGE that committed while another was in flight failed the other with
`ConcurrentAppendException` ("files were added to the root of the table"), which stops that query:
tests/stream/test_silver_tables.py reproduced it in 7 of 16 concurrent batches. Partitioned by
topic, with the literal `t.silver_topic = '<topic>'` predicate in each MERGE condition, a MERGE
reads and writes only its own topic's partition, so topic-disjoint MERGEs cannot conflict. The
Step 14 layout benchmark may not remove this partitioning; it may only add to it."""


def shared_declarations() -> tuple[TableDeclaration, ...]:
    """`silver.late_events` is rewritten (see `REWRITTEN_TABLES`) and partitioned by topic (see
    `LATE_EVENTS_LAYOUT`). `silver.duplicates` and `silver.quarantine` are append-only and
    unpartitioned: their writes are blind appends, which do not conflict with each other."""
    schemas = {
        LATE_EVENTS: late_events_schema(),
        QUARANTINE: quarantine_schema(),
        DUPLICATES: duplicates_schema(),
    }
    layouts = {LATE_EVENTS: LATE_EVENTS_LAYOUT}
    return tuple(
        TableDeclaration(
            ref=ref,
            schema=schemas[ref],
            layout=layouts.get(ref, TableLayout()),
            properties=_properties(ref),
        )
        for ref in SHARED_TABLES
    )


# ------------------------------------------------------------------ admission ---

PASS_THROUGH: Final = (
    "kafka_topic",
    "kafka_topic_id",
    "kafka_partition",
    "kafka_offset",
    "logged_at_us",
    "kafka_timestamp_type",
    "kafka_key",
    "kafka_value",
    "kafka_headers",
    "trust_tier",
    "bronze_batch_id",
    "bronze_checkpoint_id",
)
"""Bronze columns kept beside the decision (the timestamp as epoch microseconds)."""

DECISION_COLUMNS: Final = (
    ("outcome", "string"),
    ("reason", "string"),
    ("detail", "string"),
    ("silver_identity", "string"),
    ("content_digest", "string"),
    ("is_late", "boolean"),
    ("arrival_delay_ms", "long"),
    ("is_backfill", "boolean"),
    ("occurred_at_us", "long"),
    ("pref_recorded", "boolean"),
    ("pref_epoch_us", "long"),
    ("pref_position", "long"),
)

_EPOCH: Final = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
_ADMISSION: Final = "_admission"


def _micros(moment: dt.datetime) -> int:
    return (moment - _EPOCH) // dt.timedelta(microseconds=1)


def _from_micros(value: int) -> dt.datetime:
    return _EPOCH + dt.timedelta(microseconds=value)


def admission_type(topic: str) -> StructType:
    """What admission returns per Bronze row: the decision, then the event's columns, all nullable
    (a quarantined row has none), timestamps as epoch microseconds."""
    from pyspark.sql.types import LongType, StructField, StructType

    fields = [
        StructField(name, _spark_type(kind), nullable=True) for name, kind in DECISION_COLUMNS
    ]
    fields += [
        StructField(
            column.name,
            LongType() if column.kind == "timestamp" else _spark_type(column.kind),
            nullable=True,
        )
        for column in event_columns(topic)
    ]
    return StructType(fields)


def admit_row(topic: str) -> Callable[..., tuple[Any, ...]]:
    """The UDF body for `topic`: one Bronze row in, `admission_type(topic)` out, as a tuple."""
    columns: tuple[ColumnSpec, ...] = event_columns(topic)

    def run(
        kafka_topic: str,
        topic_id: str,
        partition: int,
        offset: int,
        logged_at_us: int,
        timestamp_type: int,
        value: bytes | bytearray | None,
        headers: list[Any] | None,
    ) -> tuple[Any, ...]:
        if kafka_topic != topic:
            raise LakeContractError(
                f"a {kafka_topic!r} row in the Bronze table of {topic!r}: Bronze conservation "
                f"should have refused it"
            )
        pairs = tuple(
            (str(h["key"]), None if h["value"] is None else bytes(h["value"]))
            for h in (headers or ())
            if h is not None
        )
        admission = admit(
            BronzeRecord(
                topic=topic,
                coordinates=Coordinates(str(topic_id), int(partition), int(offset)),
                logged_at=_from_micros(int(logged_at_us)),
                timestamp_type=int(timestamp_type),
                value=None if value is None else bytes(value),
                headers=pairs,
            )
        )
        decision = (
            admission.outcome.value,
            None if admission.reason is None else admission.reason.value,
            admission.detail,
            admission.identity,
            admission.digest,
            admission.is_late,
            admission.arrival_delay_ms,
            admission.backfill,
            None if admission.occurred_at is None else _micros(admission.occurred_at),
            admission.recorded,
            admission.store_epoch_us,
            admission.store_position,
        )
        event = admission.event
        values: list[Any] = []
        for column in columns:
            typed = None if event is None else column_value(column, event)
            if column.kind == "timestamp" and typed is not None:
                typed = _micros(typed)
            values.append(typed)
        return decision + tuple(values)

    return run


def decide(batch: DataFrame, topic: str) -> DataFrame:
    """Admission over a Bronze batch: `PASS_THROUGH`, then every field of `admission_type(topic)`,
    one row per Bronze row. The UDF is marked non-deterministic so the optimizer never evaluates it
    once per field it expands into; the caller persists the result."""
    from pyspark.sql import functions as F  # noqa: N812

    prepared = batch.select(
        "kafka_topic",
        "kafka_topic_id",
        "kafka_partition",
        "kafka_offset",
        F.unix_micros("kafka_timestamp").alias("logged_at_us"),
        "kafka_timestamp_type",
        "kafka_key",
        "kafka_value",
        "kafka_headers",
        "trust_tier",
        "bronze_batch_id",
        "bronze_checkpoint_id",
    )
    admission = F.udf(admit_row(topic), admission_type(topic)).asNondeterministic()
    decided = prepared.withColumn(
        _ADMISSION,
        admission(
            F.col("kafka_topic"),
            F.col("kafka_topic_id"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
            F.col("logged_at_us"),
            F.col("kafka_timestamp_type"),
            F.col("kafka_value"),
            F.col("kafka_headers"),
        ),
    )
    return decided.select(*PASS_THROUGH, f"{_ADMISSION}.*")


# ------------------------------------------------------------- classification ---


def _key(
    rank: Column,
    epoch: Column,
    position: Column,
    logged_us: Column,
    partition: Column,
    offset: Column,
    topic_id: Column,
) -> Column:
    """`silver_rules.order_key` as a Spark struct: structs compare field by field."""
    from pyspark.sql import functions as F  # noqa: N812

    return F.struct(
        rank.cast("int").alias("k0"),
        epoch.cast("long").alias("k1"),
        position.cast("long").alias("k2"),
        logged_us.cast("long").alias("k3"),
        partition.cast("int").alias("k4"),
        offset.cast("long").alias("k5"),
        topic_id.cast("string").alias("k6"),
    )


def key_sql(alias: str) -> str:
    """`silver_rules.order_key` of a `tx.scored.v1` canonical row, as SQL over `alias`."""
    return (
        f"named_struct('k0', CAST(CASE WHEN {alias}.observe_outcome = '{RECORDED}' THEN 0 ELSE 1 "
        f"END AS INT), 'k1', coalesce(unix_micros({alias}.store_epoch), {KEY_NULL}L), "
        f"'k2', coalesce({alias}.store_position, {KEY_NULL}L), "
        f"'k3', unix_micros({alias}.kafka_timestamp), 'k4', CAST({alias}.kafka_partition AS INT), "
        f"'k5', CAST({alias}.kafka_offset AS BIGINT), 'k6', CAST({alias}.kafka_topic_id AS STRING))"
    )


_COORDS: Final = ("kafka_topic_id", "kafka_partition", "kafka_offset")


def classify_frame(admitted: DataFrame, canonical: DataFrame, topic: str) -> DataFrame:
    """`silver_rules.classify` in Spark SQL: adds `disposition`, the canonical row's coordinates and
    digest (`canonical_*`), and the committed row's own columns (`existing_*`) to every admitted
    row."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F  # noqa: N812

    supersedable = topic == SUPERSEDABLE_TOPIC
    null_key = F.lit(KEY_NULL).cast("long")
    identities = admitted.select("silver_identity").distinct()
    if supersedable:
        e_rank = F.when(F.col("observe_outcome") == F.lit(RECORDED), 0).otherwise(1)
        e_epoch = F.coalesce(F.unix_micros("store_epoch"), null_key)
        e_position = F.coalesce(F.col("store_position"), null_key)
    else:
        e_rank, e_epoch, e_position = F.lit(1), null_key, null_key
    existing = canonical.join(identities, "silver_identity", "left_semi").select(
        "silver_identity",
        F.col("content_digest").alias("existing_digest"),
        F.col("kafka_topic_id").alias("existing_topic_id"),
        F.col("kafka_partition").alias("existing_partition"),
        F.col("kafka_offset").alias("existing_offset"),
        F.col("kafka_timestamp").alias("existing_kafka_timestamp"),
        F.unix_micros(DIGESTED_EVENT_TIME).alias("existing_occurred_at_us"),
        F.col("bronze_batch_id").alias("existing_bronze_batch_id"),
        F.col("bronze_checkpoint_id").alias("existing_bronze_checkpoint_id"),
        _key(
            e_rank,
            e_epoch,
            e_position,
            F.unix_micros("kafka_timestamp"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
            F.col("kafka_topic_id"),
        ).alias("existing_key"),
    )
    row_key = _key(
        F.when(F.col("pref_recorded"), 0).otherwise(1),
        F.coalesce(F.col("pref_epoch_us"), null_key),
        F.coalesce(F.col("pref_position"), null_key),
        F.col("logged_at_us"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_topic_id"),
    )
    has_existing = F.col("existing_digest").isNotNull()
    same_as_existing = F.coalesce(
        (F.col("kafka_topic_id") == F.col("existing_topic_id"))
        & (F.col("kafka_partition") == F.col("existing_partition"))
        & (F.col("kafka_offset") == F.col("existing_offset")),
        F.lit(False),
    )
    eligible = F.coalesce(
        F.lit(supersedable)
        & has_existing
        & ~same_as_existing
        & (F.col("content_digest") == F.col("existing_digest"))
        & (F.col("row_key") < F.col("existing_key")),
        F.lit(False),
    )
    window = Window.partitionBy("silver_identity").orderBy("row_key")
    whole = window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    staged = (
        admitted.join(existing, "silver_identity", "left")
        .withColumn("row_key", row_key)
        .withColumn("has_existing", has_existing)
        .withColumn("same_as_existing", same_as_existing)
        .withColumn("eligible", eligible)
        .withColumn("arrival_rank", F.row_number().over(window))
        .withColumn("first_digest", F.first("content_digest").over(whole))
    )
    for column in _COORDS:
        staged = staged.withColumn(f"first_{column}", F.first(column).over(whole)).withColumn(
            f"sup_{column}",
            F.first(F.when(F.col("eligible"), F.col(column)), ignorenulls=True).over(whole),
        )
    at_superseder = F.coalesce(
        F.col("eligible")
        & (F.col("kafka_topic_id") == F.col("sup_kafka_topic_id"))
        & (F.col("kafka_partition") == F.col("sup_kafka_partition"))
        & (F.col("kafka_offset") == F.col("sup_kafka_offset")),
        F.lit(False),
    )
    staged = staged.withColumn("at_superseder", at_superseder).withColumn(
        "superseder_rank",
        F.min(F.when(F.col("at_superseder"), F.col("arrival_rank"))).over(whole),
    )
    has_superseder = F.col("sup_kafka_offset").isNotNull()
    canonical_digest = F.when(F.col("has_existing"), F.col("existing_digest")).otherwise(
        F.col("first_digest")
    )
    disposition = (
        F.when(
            ~F.col("has_existing") & (F.col("arrival_rank") == 1), F.lit(Disposition.ADMIT.value)
        )
        .when(F.col("has_existing") & F.col("same_as_existing"), F.lit(Disposition.REPLAYED.value))
        .when(
            F.col("at_superseder") & (F.col("arrival_rank") == F.col("superseder_rank")),
            F.lit(Disposition.SUPERSEDE.value),
        )
        .when(F.col("content_digest") == canonical_digest, F.lit(Disposition.DUPLICATE.value))
        .otherwise(F.lit(Disposition.CONFLICT.value))
    )
    staged = staged.withColumn("disposition", disposition).withColumn(
        "canonical_content_digest", canonical_digest
    )
    for column in _COORDS:
        existing_column = f"existing_{column.removeprefix('kafka_')}"
        staged = staged.withColumn(
            f"canonical_{column}",
            F.when(
                F.col("has_existing"),
                F.when(has_superseder, F.col(f"sup_{column}")).otherwise(F.col(existing_column)),
            ).otherwise(F.col(f"first_{column}")),
        )
    return staged.drop(
        "row_key",
        "existing_key",
        "has_existing",
        "same_as_existing",
        "eligible",
        "arrival_rank",
        "first_digest",
        "at_superseder",
        "superseder_rank",
        *[f"first_{column}" for column in _COORDS],
        *[f"sup_{column}" for column in _COORDS],
    )


# ------------------------------------------------------------- bounded MERGEs ---
#
# ADR-0053 Amendment 1. Without a target predicate, each batch's MERGEs joined the whole canonical
# table, and the `late_events` MERGE its topic's whole partition: per-batch cost grew with the
# table. They are bounded on `occurred_at`, exactly:
#
# - Every row an identity has ever had in its canonical table carries one content digest: an
#   insert-only MERGE never replaces a row, and a `tx.scored.v1` supersede requires the stored
#   digest (`classify_frame`'s `eligible`, and `_canonical_merge`'s update condition). The digest
#   covers `envelope.occurred_at` (`silver_rules.DIGESTED_EVENT_TIME`), so all those rows, and the
#   `late_events` row copied from one of them, share one `occurred_at`.
# - The canonical MERGE's source is the batch's `admit` and `supersede` rows. An `admit` row's
#   identity has no committed row (classification read the table this MERGE writes, and one writer
#   per canonical table is required, §7), so it matches nothing, whatever the predicate. A
#   `supersede` row matches the committed row of its identity, which has its digest, and so its
#   `occurred_at`. The target predicate is the range of the superseding rows' `occurred_at`, or
#   FALSE when the batch supersedes nothing.
# - After the canonical commit, an identity of the batch has one committed row: the batch's `admit`
#   or `supersede` row, or the row classification found (`existing_occurred_at_us`), which a
#   `duplicate` or `replayed` row equals in `occurred_at`. The `late_events` source read and its
#   MERGE target are bounded by the range of those values; a `conflict` row's own `occurred_at` is
#   left out, since it differs by definition and never becomes canonical.
#
# Nothing here is a lateness bound: a record of any age is admitted and deduplicated. The guards
# refuse, before anything is written, a `supersede` whose `occurred_at` differs from the row it
# replaces, and, after the canonical commit, a batch identity whose committed row the bounded read
# did not find. The uniqueness assertion (`_assert_unique`) still reads by identity, unbounded.

STATS_SLACK_US: Final = 1_000
"""Each bound is widened by 1 ms. Delta's file statistics hold timestamps at millisecond precision,
so a file's recorded maximum can be below its true maximum by under 1 ms. Widening only adds files
to read; it never changes which rows match."""

_MIN_US: Final = -62_135_596_800_000_000
"""0001-01-01T00:00:00Z, the earliest timestamp a literal can name."""
_MAX_US: Final = 253_402_300_799_999_999
"""9999-12-31T23:59:59.999999Z, the latest."""


class SilverPruningError(LakeContractError):
    """A bounded MERGE's exactness premise failed: a row that must match lay outside the range. The
    batch is refused rather than deduplicated against part of the table."""


@dataclass(frozen=True, slots=True)
class OccurredRange:
    """An inclusive range of `occurred_at`, in epoch microseconds."""

    low_us: int
    high_us: int

    def __post_init__(self) -> None:
        if self.low_us > self.high_us:
            raise SilverPruningError(f"an empty occurred_at range: {self.low_us} > {self.high_us}")


@dataclass(frozen=True, slots=True)
class MergeBounds:
    """What the batch's two MERGEs may match (see the section comment)."""

    supersede: OccurredRange | None
    """The canonical MERGE's target range: the superseding rows' `occurred_at`; None, nothing."""
    committed: OccurredRange | None
    """The committed canonical rows of the batch's identities, and their `late_events` rows."""


def merge_bounds(
    *,
    supersede_low_us: int | None,
    supersede_high_us: int | None,
    committed_low_us: int | None,
    committed_high_us: int | None,
    supersede_mismatched: int,
) -> MergeBounds:
    """The batch's bounds from its aggregates; refuses a `supersede` whose `occurred_at` is not the
    replaced row's, the one case in which the canonical MERGE's range could miss its match."""
    if supersede_mismatched:
        raise SilverPruningError(
            f"{supersede_mismatched} superseding row(s) carry an occurred_at other than the "
            f"canonical row they replace, although the two share a content digest that covers "
            f"envelope.{DIGESTED_EVENT_TIME}: refusing the batch rather than prune the MERGE"
        )

    def between(low: int | None, high: int | None) -> OccurredRange | None:
        if (low is None) != (high is None):
            raise SilverPruningError(f"a half-open occurred_at range: [{low}, {high}]")
        return None if low is None or high is None else OccurredRange(low, high)

    bounds = MergeBounds(
        between(supersede_low_us, supersede_high_us),
        between(committed_low_us, committed_high_us),
    )
    if bounds.supersede is not None and (
        bounds.committed is None
        or bounds.supersede.low_us < bounds.committed.low_us
        or bounds.supersede.high_us > bounds.committed.high_us
    ):
        raise SilverPruningError(
            f"the superseding range {bounds.supersede} is not inside the committed range "
            f"{bounds.committed}"
        )
    return bounds


def _timestamp_literal(micros: int) -> str:
    moment = _EPOCH + dt.timedelta(microseconds=min(max(micros, _MIN_US), _MAX_US))
    return f"TIMESTAMP '{moment.isoformat(sep=' ', timespec='microseconds')}'"


def occurred_at_predicate(column: str, bounds: OccurredRange | None) -> str:
    """`column` inside `bounds`, widened by `STATS_SLACK_US`, as SQL whose bounds are timestamp
    literals, which Delta's data skipping compares with each file's statistics; FALSE for None,
    which reads no file."""
    if bounds is None:
        return "FALSE"
    low = _timestamp_literal(bounds.low_us - STATS_SLACK_US)
    high = _timestamp_literal(bounds.high_us + STATS_SLACK_US)
    return f"({column} >= {low} AND {column} <= {high})"


def batch_merge_bounds(classified: DataFrame) -> MergeBounds:
    """`merge_bounds` over a classified batch: one aggregate over the persisted batch."""
    from pyspark.sql import functions as F  # noqa: N812

    own = F.col(DIGESTED_EVENT_TIME)  # the event's column, in epoch microseconds here
    existing = F.col("existing_occurred_at_us")
    supersede = F.col("disposition") == Disposition.SUPERSEDE.value
    kept = F.when(F.col("disposition") != Disposition.CONFLICT.value, own)
    row = classified.agg(
        F.min(F.when(supersede, own)).alias("supersede_low_us"),
        F.max(F.when(supersede, own)).alias("supersede_high_us"),
        F.min(F.least(kept, existing)).alias("committed_low_us"),
        F.max(F.greatest(kept, existing)).alias("committed_high_us"),
        F.count(F.when(supersede & ~own.eqNullSafe(existing), 1)).alias("supersede_mismatched"),
    ).first()
    if row is None:
        raise LakeContractError("an aggregate returned no row")
    return merge_bounds(
        supersede_low_us=row["supersede_low_us"],
        supersede_high_us=row["supersede_high_us"],
        committed_low_us=row["committed_low_us"],
        committed_high_us=row["committed_high_us"],
        supersede_mismatched=int(row["supersede_mismatched"]),
    )


# -------------------------------------------------------------------- the sink ---


@dataclass(frozen=True, slots=True)
class BatchCounts:
    """What one micro-batch did with its Bronze rows. Every row is exactly one of the first five;
    `superseded` counts committed canonical rows the batch replaced."""

    admitted: int
    replayed: int
    duplicates: int
    quarantined: int
    """Rejected records plus identity conflicts."""
    superseding: int
    superseded: int


def _conform(frame: DataFrame, schema: StructType) -> DataFrame:
    """Columns in the declared order and types; nullability is enforced by Delta at write time."""
    from pyspark.sql import functions as F  # noqa: N812

    return frame.select(
        *[F.col(field.name).cast(field.dataType).alias(field.name) for field in schema.fields]
    )


def _canonical_rows(
    classified: DataFrame, topic: str, batch_id: int, checkpoint_id: str
) -> DataFrame:
    from pyspark.sql import functions as F  # noqa: N812

    projected = []
    for column in event_columns(topic):
        source = F.col(column.name)
        if column.kind == "timestamp":
            source = F.timestamp_micros(source)
        projected.append(source.cast(_spark_type(column.kind)).alias(column.name))
    projected += [
        F.col("silver_identity"),
        F.col("content_digest"),
        F.col("is_late"),
        F.col("arrival_delay_ms"),
        F.col("is_backfill"),
        F.col("trust_tier"),
        F.col("kafka_topic"),
        F.col("kafka_topic_id"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.timestamp_micros("logged_at_us").alias("kafka_timestamp"),
        F.col("bronze_batch_id"),
        F.col("bronze_checkpoint_id"),
        F.lit(batch_id).cast("long").alias("silver_batch_id"),
        F.lit(checkpoint_id).alias("silver_checkpoint_id"),
        F.current_timestamp().alias("silver_admitted_at"),
    ]
    return _conform(classified.select(*projected), canonical_schema(topic))


def silver_sink(
    spec: SilverTopic, opened: OpenedCheckpoint, *, git_sha: str
) -> Callable[[DataFrame, int], None]:
    """The `foreachBatch` function (module docstring)."""
    from pyspark.sql import functions as F  # noqa: N812

    checkpoint_id = opened.identity.app_id
    topic = spec.topic

    def sink(batch: DataFrame, batch_id: int) -> None:
        session = batch.sparkSession
        decided = decide(batch, topic).persist()
        try:
            admitted = decided.filter(F.col("outcome") == Outcome.ADMITTED.value)
            canonical_path = str(spec.table.local_path(opened.lake))
            canonical = session.read.format("delta").load(canonical_path)
            classified = classify_frame(admitted, canonical, topic).persist()
            try:
                counts = _write_batch(
                    spec, opened, decided, classified, batch_id=batch_id, git_sha=git_sha
                )
                _assert_unique(session, spec, opened, classified)
            finally:
                classified.unpersist()
        finally:
            decided.unpersist()
        _log.info(
            "silver_batch_written",
            topic=topic,
            query=spec.query,
            batch_id=batch_id,
            checkpoint_id=checkpoint_id,
            admitted=counts.admitted,
            superseding=counts.superseding,
            replayed=counts.replayed,
            duplicates=counts.duplicates,
            superseded=counts.superseded,
            quarantined=counts.quarantined,
        )

    return sink


def _write_batch(
    spec: SilverTopic,
    opened: OpenedCheckpoint,
    decided: DataFrame,
    classified: DataFrame,
    *,
    batch_id: int,
    git_sha: str,
) -> BatchCounts:
    from pyspark.sql import functions as F  # noqa: N812

    checkpoint_id = opened.identity.app_id
    topic = spec.topic
    now = F.current_timestamp()
    batch_columns = [
        F.lit(batch_id).cast("long").alias("silver_batch_id"),
        F.lit(checkpoint_id).alias("silver_checkpoint_id"),
        now.alias("silver_recorded_at"),
    ]
    raw_columns: list[Column | str] = [
        "kafka_topic",
        "kafka_topic_id",
        "kafka_partition",
        "kafka_offset",
        F.timestamp_micros("logged_at_us").alias("kafka_timestamp"),
        "kafka_timestamp_type",
        "kafka_key",
        "kafka_value",
        "kafka_headers",
        "trust_tier",
        F.lit(git_sha).alias("consumer_git_sha"),
        "bronze_batch_id",
        "bronze_checkpoint_id",
    ]
    rejected = decided.filter(F.col("outcome") == Outcome.QUARANTINED.value).select(
        F.lit(topic).alias("silver_topic"),
        "reason",
        "detail",
        "silver_identity",
        "content_digest",
        F.lit(None).cast("string").alias("canonical_kafka_topic_id"),
        F.lit(None).cast("int").alias("canonical_kafka_partition"),
        F.lit(None).cast("long").alias("canonical_kafka_offset"),
        F.lit(None).cast("string").alias("canonical_content_digest"),
        *raw_columns,
        *batch_columns,
    )
    conflicts = classified.filter(F.col("disposition") == Disposition.CONFLICT.value).select(
        F.lit(topic).alias("silver_topic"),
        F.lit(QuarantineReason.IDENTITY_CONFLICT.value).alias("reason"),
        F.lit("the identity already has a canonical row with different content").alias("detail"),
        "silver_identity",
        "content_digest",
        "canonical_kafka_topic_id",
        "canonical_kafka_partition",
        "canonical_kafka_offset",
        "canonical_content_digest",
        *raw_columns,
        *batch_columns,
    )
    quarantined = _conform(rejected.unionByName(conflicts), quarantine_schema())
    identical = classified.filter(F.col("disposition") == Disposition.DUPLICATE.value).select(
        F.lit(topic).alias("silver_topic"),
        F.lit(DuplicateKind.DUPLICATE.value).alias("disposition"),
        "silver_identity",
        "content_digest",
        "kafka_topic_id",
        "kafka_partition",
        "kafka_offset",
        F.timestamp_micros("logged_at_us").alias("kafka_timestamp"),
        "canonical_kafka_topic_id",
        "canonical_kafka_partition",
        "canonical_kafka_offset",
        "bronze_batch_id",
        "bronze_checkpoint_id",
        *batch_columns,
    )
    superseded = classified.filter(F.col("disposition") == Disposition.SUPERSEDE.value).select(
        F.lit(topic).alias("silver_topic"),
        F.lit(DuplicateKind.SUPERSEDED.value).alias("disposition"),
        "silver_identity",
        F.col("existing_digest").alias("content_digest"),
        F.col("existing_topic_id").alias("kafka_topic_id"),
        F.col("existing_partition").alias("kafka_partition"),
        F.col("existing_offset").alias("kafka_offset"),
        F.col("existing_kafka_timestamp").alias("kafka_timestamp"),
        F.col("kafka_topic_id").alias("canonical_kafka_topic_id"),
        F.col("kafka_partition").alias("canonical_kafka_partition"),
        F.col("kafka_offset").alias("canonical_kafka_offset"),
        F.col("existing_bronze_batch_id").alias("bronze_batch_id"),
        F.col("existing_bronze_checkpoint_id").alias("bronze_checkpoint_id"),
        *batch_columns,
    )
    duplicates = _conform(identical.unionByName(superseded), duplicates_schema())
    inserts = _canonical_rows(
        classified.filter(
            F.col("disposition").isin(Disposition.ADMIT.value, Disposition.SUPERSEDE.value)
        ),
        topic,
        batch_id,
        checkpoint_id,
    )

    by_disposition = {
        str(row["disposition"]): int(row["count"])
        for row in classified.groupBy("disposition").count().collect()
    }
    rejected_count = decided.filter(F.col("outcome") == Outcome.QUARANTINED.value).count()
    # Before the first commit: a refused premise leaves nothing of the batch written.
    bounds = batch_merge_bounds(classified)

    # The order matters for a replay after a crash between two commits (module docstring).
    if not quarantined.isEmpty():
        opened.append(quarantined, batch_id=batch_id, target=QUARANTINE)
    if not duplicates.isEmpty():
        opened.append(duplicates, batch_id=batch_id, target=DUPLICATES)
    if not inserts.isEmpty():
        opened.merge(
            inserts.sparkSession,
            batch_id=batch_id,
            target=spec.table,
            build=lambda target: _canonical_merge(target, inserts, topic, bounds.supersede),
        )
    if not classified.isEmpty():
        _merge_late_events(spec, opened, classified, bounds.committed, batch_id=batch_id)
    supersede_count = by_disposition.get(Disposition.SUPERSEDE.value, 0)
    return BatchCounts(
        admitted=by_disposition.get(Disposition.ADMIT.value, 0),
        replayed=by_disposition.get(Disposition.REPLAYED.value, 0),
        duplicates=by_disposition.get(Disposition.DUPLICATE.value, 0),
        quarantined=rejected_count + by_disposition.get(Disposition.CONFLICT.value, 0),
        superseding=supersede_count,
        superseded=supersede_count,
    )


def canonical_merge_condition(supersede: OccurredRange | None) -> str:
    """The canonical MERGE's condition: the identity, and the target rows a superseding row can
    match (`MergeBounds.supersede`), as a target-only conjunct Delta prunes files with."""
    return (
        f"t.silver_identity = s.silver_identity AND "
        f"{occurred_at_predicate(f't.{DIGESTED_EVENT_TIME}', supersede)}"
    )


def _canonical_merge(
    target: Any, inserts: DataFrame, topic: str, supersede: OccurredRange | None
) -> Any:
    """Insert-only, except `tx.scored.v1`: there a row with the same digest and an earlier key
    replaces the committed row. A differing digest is a conflict, and never reaches the MERGE.
    Bounded by `supersede` (the bounded-MERGEs section)."""
    builder = target.alias("t").merge(inserts.alias("s"), canonical_merge_condition(supersede))
    if topic == SUPERSEDABLE_TOPIC:
        builder = builder.whenMatchedUpdateAll(
            condition=f"t.content_digest = s.content_digest AND {key_sql('s')} < {key_sql('t')}"
        )
    return builder.whenNotMatchedInsertAll()


LATE_COLUMNS: Final = (
    "silver_topic",
    "silver_identity",
    "occurred_at",
    "kafka_timestamp",
    "arrival_delay_ms",
    "kafka_topic_id",
    "kafka_partition",
    "kafka_offset",
    "silver_batch_id",
    "silver_checkpoint_id",
    "silver_recorded_at",
)


def late_events_merge_condition(topic: str, committed: OccurredRange | None) -> str:
    """The `late_events` MERGE's condition: the topic's partition (`LATE_EVENTS_LAYOUT`), the
    batch's committed range (`MergeBounds.committed`), then topic and identity."""
    return (
        f"t.silver_topic = '{topic}' AND "
        f"{occurred_at_predicate(f't.{DIGESTED_EVENT_TIME}', committed)} "
        f"AND t.silver_topic = s.silver_topic AND t.silver_identity = s.silver_identity"
    )


def _merge_late_events(
    spec: SilverTopic,
    opened: OpenedCheckpoint,
    classified: DataFrame,
    committed_range: OccurredRange | None,
    *,
    batch_id: int,
) -> None:
    """Re-derive `late_events` for the batch's identities from the committed canonical rows.

    Read after the canonical MERGE, so it sees that commit (or, on a replay, the commit a previous
    attempt made): matched and late with other coordinates, update; matched and not late, delete;
    not matched and late, insert; otherwise nothing. The read and the MERGE are bounded by
    `committed_range` (the bounded-MERGEs section); a batch identity whose committed row the
    bounded read does not find refuses the batch before the MERGE."""
    from pyspark.sql import functions as F  # noqa: N812

    session = classified.sparkSession
    identities = classified.select("silver_identity").distinct()
    found = (
        session.read.format("delta")
        .load(str(spec.table.local_path(opened.lake)))
        .filter(F.expr(occurred_at_predicate(DIGESTED_EVENT_TIME, committed_range)))
        .join(identities, "silver_identity", "left_semi")
        .persist()
    )
    try:
        expected = identities.count()
        located = found.select("silver_identity").distinct().count()
        if located != expected:
            raise SilverPruningError(
                f"{spec.table}: {expected - located} of the batch's {expected} identities have "
                f"no committed row inside the occurred_at range {committed_range}: refusing to "
                f"re-derive {LATE_EVENTS} from part of the table"
            )
        _late_events_commit(spec, opened, found, committed_range, batch_id=batch_id)
    finally:
        found.unpersist()


def _late_events_commit(
    spec: SilverTopic,
    opened: OpenedCheckpoint,
    found: DataFrame,
    committed_range: OccurredRange | None,
    *,
    batch_id: int,
) -> None:
    from pyspark.sql import functions as F  # noqa: N812

    session = found.sparkSession
    source = found.select(
        F.lit(spec.topic).alias("silver_topic"),
        "silver_identity",
        "occurred_at",
        "kafka_timestamp",
        "arrival_delay_ms",
        "kafka_topic_id",
        "kafka_partition",
        "kafka_offset",
        F.lit(batch_id).cast("long").alias("silver_batch_id"),
        F.lit(opened.identity.app_id).alias("silver_checkpoint_id"),
        F.current_timestamp().alias("silver_recorded_at"),
        F.coalesce(F.col("is_late"), F.lit(False)).alias("source_is_late"),
    )
    values = {column: f"s.{column}" for column in LATE_COLUMNS}
    moved = (
        "NOT (t.kafka_topic_id = s.kafka_topic_id AND t.kafka_partition = s.kafka_partition "
        "AND t.kafka_offset = s.kafka_offset)"
    )

    def build(target: Any) -> Any:
        return (
            target.alias("t")
            .merge(source.alias("s"), late_events_merge_condition(spec.topic, committed_range))
            .whenMatchedUpdate(condition=f"s.source_is_late AND {moved}", set=values)
            .whenMatchedDelete(condition="NOT s.source_is_late")
            .whenNotMatchedInsert(condition="s.source_is_late", values=values)
        )

    opened.merge(session, batch_id=batch_id, target=LATE_EVENTS, build=build)


class SilverUniquenessError(LakeContractError):
    """A Silver table holds an identity twice: exact deduplication failed, and the query stops."""


def _assert_unique(
    session: SparkSession, spec: SilverTopic, opened: OpenedCheckpoint, classified: DataFrame
) -> None:
    from pyspark.sql import functions as F  # noqa: N812

    identities = classified.select("silver_identity").distinct()
    canonical = session.read.format("delta").load(str(spec.table.local_path(opened.lake)))
    repeated = (
        canonical.join(identities, "silver_identity", "left_semi")
        .groupBy("silver_identity")
        .count()
        .filter(F.col("count") > 1)
        .limit(5)
        .collect()
    )
    if repeated:
        raise SilverUniquenessError(
            f"{spec.table} holds identities more than once after a MERGE: "
            f"{[row['silver_identity'] for row in repeated]}"
        )
    late = session.read.format("delta").load(str(LATE_EVENTS.local_path(opened.lake)))
    repeated_late = (
        late.filter(F.col("silver_topic") == spec.topic)
        .join(identities, "silver_identity", "left_semi")
        .groupBy("silver_identity")
        .count()
        .filter(F.col("count") > 1)
        .limit(5)
        .collect()
    )
    if repeated_late:
        raise SilverUniquenessError(
            f"{LATE_EVENTS} holds ({spec.topic}, identity) more than once: "
            f"{[row['silver_identity'] for row in repeated_late]}"
        )


# --------------------------------------------------------------------- queries ---


@dataclass(frozen=True, slots=True)
class SilverQuery:
    spec: SilverTopic
    opened: OpenedCheckpoint
    query: StreamingQuery


def create_silver_tables(
    spark: SparkSession, lake: LakeConfig, topic: str, *, git_sha: str, dirty_worktree: bool
) -> None:
    """Every table the topic's query writes, from its declaration (ADR-0048), before it starts."""
    spec = silver_topic(topic)
    provenance = CommitProvenance(git_sha=git_sha, dirty_worktree=dirty_worktree, query=spec.query)
    create_table(spark, silver_declaration(topic), lake, provenance)
    for declaration in shared_declarations():
        create_table(spark, declaration, lake, provenance)


def silver_sources(topic: str, starting_version: int = 0) -> list[DeltaSourceStart]:
    """From a Bronze version, never a snapshot: commit-log offsets are what conservation can judge
    exactly (`silver_conservation`). Version 0, until Bronze has retention floors (ADR-0052
    amendment 1); see `silver_start_version`."""
    return [DeltaSourceStart(bronze_topic(topic).table, starting_version=starting_version)]


def silver_start_version(spark: SparkSession, lake: LakeConfig, topic: str) -> int:
    """The Bronze version the topic's Silver checkpoint starts at: the one its current version
    recorded, or, for a new checkpoint, Bronze's retention start -- version 0 without retention
    floors, else the first version whose files are all live (`tables.retention_start`)."""
    spec = silver_topic(topic)
    source = bronze_topic(topic).table
    state = checkpoints.read_state(lake, spec.query)
    record = None if state.identity is None else state.identity.sources.get(f"delta:{source}")
    if record is not None and record.start.startswith("version:"):
        return int(record.start.split(":", 1)[1])
    return retention_start(spark, source.local_path(lake)).start_version


def silver_targets(topic: str) -> list[TableRef]:
    return [silver_topic(topic).table, *SHARED_TABLES]


def start_silver_query(
    spark: SparkSession,
    lake: LakeConfig,
    topic: str,
    *,
    git_sha: str,
    dirty_worktree: bool,
    now: dt.datetime,
    trigger: Trigger,
) -> SilverQuery:
    """Create the tables, open the checkpoint on the topic's Bronze table, and start."""
    spec = silver_topic(topic)
    source: TableRef = bronze_topic(topic).table
    if snapshot_facts(spark, source.local_path(lake)) is None:
        raise LakeContractError(
            f"{source} does not exist: Silver reads Bronze, so run the Bronze query first"
        )
    create_silver_tables(spark, lake, topic, git_sha=git_sha, dirty_worktree=dirty_worktree)
    opened = checkpoints.open_checkpoint(
        spark,
        lake,
        spec.query,
        targets=silver_targets(topic),
        sources=silver_sources(topic, silver_start_version(spark, lake, topic)),
        git_sha=git_sha,
        dirty_worktree=dirty_worktree,
        now=now,
    )
    frame = opened.delta_source(spark, source)
    writer: Any = (
        frame.writeStream.queryName(spec.query)
        .option("checkpointLocation", str(opened.directory))
        .foreachBatch(silver_sink(spec, opened, git_sha=git_sha))
    )
    if trigger.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{trigger.interval_s} seconds")
    query: StreamingQuery = writer.start()
    _log.info(
        "silver_query_started",
        topic=topic,
        query=spec.query,
        table=str(spec.table),
        source=str(source),
        checkpoint_version=opened.identity.version,
        app_id=opened.identity.app_id,
        action=opened.action.value,
        available_now=trigger.available_now,
    )
    return SilverQuery(spec, opened, query)


__all__ = [
    "APPEND_ONLY",
    "DECISION_COLUMNS",
    "LATE_COLUMNS",
    "LATE_EVENTS_LAYOUT",
    "PASS_THROUGH",
    "REWRITTEN_TABLES",
    "BatchCounts",
    "SilverQuery",
    "SilverUniquenessError",
    "admission_type",
    "admit_row",
    "canonical_schema",
    "classify_frame",
    "create_silver_tables",
    "decide",
    "duplicates_schema",
    "key_sql",
    "late_events_schema",
    "quarantine_schema",
    "shared_declarations",
    "silver_declaration",
    "silver_sink",
    "silver_sources",
    "silver_start_version",
    "silver_targets",
    "start_silver_query",
]
