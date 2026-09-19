"""Gold's batch computation in Spark SQL: observations, primitive state, point-in-time context.

ADR-0055. Every reduction mirrors one in `trace_core.features.reference` (`window_state`,
`lifetime_profile`, `previous_observation`, `event_time_complete_context`) for meaning, and is
written as Spark SQL -- joins, window functions and higher-order functions, no Python UDF -- so it
runs on the JVM and scales with Spark, never by calling the reference.

**Bounded joins.** A subject transaction is joined only with rows inside its window: both sides
carry a bucket of the window's width, the subject is exploded over the (at most two) buckets its
range touches, and the join is an equi-join on the entity and the bucket followed by the exact range
predicate (`_range_join`). Unbounded history -- the account lifetime -- is read by as-of lookups
over one sort per account (`_latest_before`), then by equi-joins on row numbers for the bounded
samples.

**Exact arithmetic.** Sums and sums of squares are `DECIMAL(38,0)`; ANSI mode (the session contract)
makes an overflow fail the build rather than wrap. Event time is floored to the millisecond with
`pmod`, so a timestamp before the epoch floors as Python does. An even-count median of integer
amounts goes through an exact decimal and its string, so it is the correctly rounded double Python
computes. Distances use the reference formula term by term; the JVM's `sin`, `cos` and `asin` are
not bit-equal to the platform libm, which the float parity tolerance covers and the whole-metre
medoid rounding makes irrelevant away from exact half-metre ties (ADR-0055, Risks).

pyspark is imported only inside functions that run with a session.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Final

from trace_core.domain.enums import TransactionChannel
from trace_core.domain.geo import EARTH_RADIUS_KM
from trace_core.features.context import MIN_OBSERVATIONS_FOR_ROBUST_Z
from trace_core.features.observation import Verification
from trace_core.features.semantics import (
    ALIGNED_MINUTE_MS,
    AMOUNT_SAMPLE_SIZE,
    APPROXIMATE_BUCKET_MS,
    HABITUAL_MIN_VISITS,
    HOME_MIN_OBSERVATIONS,
    HOME_SAMPLE_SIZE,
    PROFILE_LIFETIME_GAP_S,
    CurrentObservation,
    Dimension,
    Entity,
    Stream,
)
from trace_core.features.state_plan import PLAN
from trace_core.stream.gold_plan import (
    AUTHORIZATIONS,
    DIMENSION_COLUMNS,
    ENTITY_COLUMNS,
    IDENTITY_EVENTS,
    IDENTITY_TYPE_STREAMS,
    NAMESPACE_VALUES,
    OBSERVED_OUTCOMES,
    SCALAR_FIELDS,
    TRANSACTIONS,
    BuildPlan,
    WindowSpec,
    distinct_column,
)
from trace_core.stream.lake import LakeConfig
from trace_core.stream.tables import TableRef

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession
    from pyspark.sql.types import StructType

DECIMAL: Final = "DECIMAL(38,0)"
_GAP_MS: Final = PROFILE_LIFETIME_GAP_S * 1000
_TX: Final = Stream.TRANSACTION.value
_CARD_PRESENT: Final = TransactionChannel.CARD_PRESENT.value


def floor_div(expression: str, divisor: int) -> str:
    """`floor(expression / divisor)` in exact integer SQL, for negative values too."""
    return f"((({expression}) - pmod(({expression}), {divisor}L)) div {divisor}L)"


def millis(column: str) -> str:
    """A timestamp floored to epoch milliseconds, as `trace_core.domain.time.to_millis` does."""
    return floor_div(f"unix_micros({column})", 1000)


def _sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _sql_list(values: Sequence[str]) -> str:
    return ", ".join(_sql_string(v) for v in values)


# ---------------------------------------------------------------- observations ---


def observations_schema() -> StructType:
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    nullable_strings = (
        "card_id",
        "device_id",
        "merchant_id",
        "ip_id",
        "merchant_mcc",
        "merchant_country",
    )
    return StructType(
        [
            StructField("stream", StringType(), nullable=False),
            StructField("identity_namespace", StringType(), nullable=False),
            StructField("event_id", StringType(), nullable=False),
            StructField("observation_identity", StringType(), nullable=False),
            StructField("occurred_at", TimestampType(), nullable=False),
            StructField("occurred_ms", LongType(), nullable=False),
            StructField("account_id", StringType(), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("amount_minor", LongType(), nullable=False),
            *[StructField(name, StringType(), nullable=True) for name in nullable_strings],
            StructField("latitude", DoubleType(), nullable=True),
            StructField("longitude", DoubleType(), nullable=True),
            StructField("channel", StringType(), nullable=True),
            StructField("authorization_outcome", StringType(), nullable=True),
            StructField("verification", StringType(), nullable=True),
            StructField("correlation_id", StringType(), nullable=False),
            StructField("source_table", StringType(), nullable=False),
        ]
    )


def read_pinned(spark: SparkSession, lake: LakeConfig, plan: BuildPlan, ref: TableRef) -> DataFrame:
    """A Silver table at the version the plan pinned; Delta refuses a version it no longer has."""
    pin = plan.pin(ref)
    return (
        spark.read.format("delta")
        .option("versionAsOf", str(pin.version))
        .load(str(ref.local_path(lake)))
    )


def _conform(frame: DataFrame, schema: StructType) -> DataFrame:
    from pyspark.sql import functions as F  # noqa: N812

    return frame.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


GATEWAY_PRODUCER: Final = "trace-gateway"
"""The producer name, before `@`, whose identity events the online store observes. Compared on the
name alone, as Silver's content digest does, so a redeploy does not change what Gold reads."""


def gateway_produced(producer: Column) -> Column:
    """Whether a Silver row's `producer` is the gateway's, whatever its version."""
    from pyspark.sql import functions as F  # noqa: N812

    return F.substring_index(producer, "@", 1) == F.lit(GATEWAY_PRODUCER)


def observations(spark: SparkSession, lake: LakeConfig, plan: BuildPlan) -> DataFrame:
    """Every observation once under its identity, as `features.observation.Event` holds it.

    - Transactions from `silver.tx_scored_v1`, whatever the store did with them (`observe_outcome`):
      a complete history holds every transaction the gateway accepted. Their own
      `authorization_outcome` field is not carried (ADR-0049 §3).
    - Identity events from `silver.identity_events_v1` whose type feeds a stream, produced by the
      gateway. `identity.events.v1` has two kinds of producer: the generator publishes events
      directly, and the gateway republishes what it ingests, carrying the online store's observation
      id in `correlation_id`. The store observes only the gateway's, so Gold reads only those, under
      that id: a directly produced event the store never saw would otherwise be counted, and a
      replayed one counted twice under two identities.
    - Authorization outcomes from `silver.tx_authorization_v1`, with their verification against the
      complete history's transactions (ADR-0049 §2): VERIFIED, PENDING or REJECTED.
    """
    from pyspark.sql import functions as F  # noqa: N812

    schema = observations_schema()
    null_string = F.lit(None).cast("string")
    null_double = F.lit(None).cast("double")

    def common(source: TableRef, stream: Column, namespace: Column, event_id: str) -> list[Column]:
        return [
            stream.alias("stream"),
            namespace.alias("identity_namespace"),
            F.col(event_id).alias("event_id"),
            F.concat(namespace, F.lit(":"), F.col(event_id)).alias("observation_identity"),
            F.col("occurred_at"),
            F.expr(millis("occurred_at")).alias("occurred_ms"),
            F.col("account_id"),
            F.lit(str(source)).alias("source_table"),
            F.col("correlation_id"),
        ]

    tx = read_pinned(spark, lake, plan, TRANSACTIONS).select(
        *common(
            TRANSACTIONS, F.lit(_TX), F.lit(NAMESPACE_VALUES[Stream.TRANSACTION]), "transaction_id"
        ),
        "currency",
        "amount_minor",
        "card_id",
        "device_id",
        "merchant_id",
        "ip_id",
        "merchant_mcc",
        "merchant_country",
        "latitude",
        "longitude",
        "channel",
        null_string.alias("authorization_outcome"),
        null_string.alias("verification"),
    )

    stream_of_type: Column | None = None
    for identity_type, stream in sorted(IDENTITY_TYPE_STREAMS.items()):
        condition = F.col("identity_event_type") == F.lit(identity_type)
        stream_of_type = (
            F.when(condition, F.lit(stream.value))
            if stream_of_type is None
            else stream_of_type.when(condition, F.lit(stream.value))
        )
    assert stream_of_type is not None
    namespaces = {NAMESPACE_VALUES[s] for s in IDENTITY_TYPE_STREAMS.values()}
    if len(namespaces) != 1:
        raise AssertionError(f"identity streams span namespaces {sorted(namespaces)}")
    identity = (
        read_pinned(spark, lake, plan, IDENTITY_EVENTS)
        .withColumn("_stream", stream_of_type)
        .filter(F.col("_stream").isNotNull())
        .filter(gateway_produced(F.col("producer")))
        .select(
            *common(IDENTITY_EVENTS, F.col("_stream"), F.lit(namespaces.pop()), "correlation_id"),
            F.lit("").alias("currency"),
            F.lit(0).cast("long").alias("amount_minor"),
            null_string.alias("card_id"),
            "device_id",
            null_string.alias("merchant_id"),
            "ip_id",
            null_string.alias("merchant_mcc"),
            null_string.alias("merchant_country"),
            null_double.alias("latitude"),
            null_double.alias("longitude"),
            null_string.alias("channel"),
            null_string.alias("authorization_outcome"),
            null_string.alias("verification"),
        )
    )

    known = tx.select(F.col("event_id").alias("_tx_id"), F.col("account_id").alias("_tx_account"))
    verdict = (
        F.when(F.col("_tx_account").isNull(), F.lit(Verification.PENDING.value))
        .when(F.col("_tx_account") == F.col("account_id"), F.lit(Verification.VERIFIED.value))
        .otherwise(F.lit(Verification.REJECTED.value))
    )
    outcome_stream = Stream.AUTHORIZATION_OUTCOME
    outcomes = (
        read_pinned(spark, lake, plan, AUTHORIZATIONS)
        .filter(F.col("authorization_outcome").isin(*OBSERVED_OUTCOMES))
        .join(known, F.col("transaction_id") == F.col("_tx_id"), "left")
        .select(
            *common(
                AUTHORIZATIONS,
                F.lit(outcome_stream.value),
                F.lit(NAMESPACE_VALUES[outcome_stream]),
                "transaction_id",
            ),
            F.lit("").alias("currency"),
            F.lit(0).cast("long").alias("amount_minor"),
            *[null_string.alias(c) for c in ("card_id", "device_id", "merchant_id", "ip_id")],
            null_string.alias("merchant_mcc"),
            null_string.alias("merchant_country"),
            null_double.alias("latitude"),
            null_double.alias("longitude"),
            null_string.alias("channel"),
            F.col("authorization_outcome"),
            verdict.alias("verification"),
        )
    )
    return (
        _conform(tx, schema)
        .unionByName(_conform(identity, schema))
        .unionByName(_conform(outcomes, schema))
    )


# ---------------------------------------------------------------- primitives ---


def minute_buckets_schema() -> StructType:
    from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType

    return StructType(
        [
            StructField("entity", StringType(), nullable=False),
            StructField("entity_id", StringType(), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("minute", LongType(), nullable=False),
            StructField("observation_count", LongType(), nullable=False),
            StructField("amount_sum_minor", DecimalType(38, 0), nullable=False),
            StructField("amount_sum_squares", DecimalType(38, 0), nullable=False),
        ]
    )


def distinct_buckets_schema() -> StructType:
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    return StructType(
        [
            StructField("entity", StringType(), nullable=False),
            StructField("entity_id", StringType(), nullable=False),
            StructField("dimension", StringType(), nullable=False),
            StructField("bucket", LongType(), nullable=False),
            StructField("value", StringType(), nullable=False),
        ]
    )


def _empty(spark: SparkSession, schema: StructType) -> DataFrame:
    return spark.createDataFrame([], schema)


def minute_buckets(spark: SparkSession, obs: DataFrame) -> DataFrame:
    """PLAN's minute buckets: per entity, currency and declared minute, the transactions' count, sum
    and sum of squares, exactly (`ALIGNED_MINUTE_MS`)."""
    from pyspark.sql import functions as F  # noqa: N812

    parts = []
    for entity in sorted(PLAN.buckets, key=lambda e: e.value):
        column = ENTITY_COLUMNS[entity]
        parts.append(
            obs.filter((F.col("stream") == _TX) & F.col(column).isNotNull()).select(
                F.lit(entity.value).alias("entity"),
                F.col(column).alias("entity_id"),
                "currency",
                F.expr(floor_div("occurred_ms", ALIGNED_MINUTE_MS)).alias("minute"),
                F.col("amount_minor").cast("DECIMAL(20,0)").alias("_amount"),
            )
        )
    schema = minute_buckets_schema()
    if not parts:
        return _empty(spark, schema)
    union = parts[0]
    for part in parts[1:]:
        union = union.unionByName(part)
    grouped = union.groupBy("entity", "entity_id", "currency", "minute").agg(
        F.count(F.lit(1)).alias("observation_count"),
        F.sum("_amount").cast(DECIMAL).alias("amount_sum_minor"),
        F.sum(F.col("_amount") * F.col("_amount")).cast(DECIMAL).alias("amount_sum_squares"),
    )
    return _conform(grouped, schema)


def distinct_buckets(spark: SparkSession, obs: DataFrame) -> DataFrame:
    """PLAN's approximate distinct counts' estimand: each value once per entity and five-minute
    bucket (`APPROXIMATE_BUCKET_MS`). The online store holds these as HyperLogLogs."""
    from pyspark.sql import functions as F  # noqa: N812

    parts = []
    for entity, dimension in sorted(PLAN.approx_distinct, key=lambda k: (k[0].value, k[1].value)):
        entity_column, value_column = ENTITY_COLUMNS[entity], DIMENSION_COLUMNS[dimension]
        parts.append(
            obs.filter(
                (F.col("stream") == _TX)
                & F.col(entity_column).isNotNull()
                & F.col(value_column).isNotNull()
            ).select(
                F.lit(entity.value).alias("entity"),
                F.col(entity_column).alias("entity_id"),
                F.lit(dimension.value).alias("dimension"),
                F.expr(floor_div("occurred_ms", APPROXIMATE_BUCKET_MS)).alias("bucket"),
                F.col(value_column).alias("value"),
            )
        )
    schema = distinct_buckets_schema()
    if not parts:
        return _empty(spark, schema)
    union = parts[0]
    for part in parts[1:]:
        union = union.unionByName(part)
    return _conform(union.distinct(), schema)


# ------------------------------------------------------------ joins, lookups ---


def _range_join(
    left: DataFrame,
    right: DataFrame,
    *,
    keys: Sequence[tuple[str, str]],
    low: str,
    high: str,
    position: str,
    width: int,
) -> DataFrame:
    """`left` joined with the `right` rows whose `position` lies in `[low, high]` (SQL over `left`'s
    columns), as an equi-join on `keys` and a bucket of `width` followed by the exact predicate.
    With `width` at least `high - low + 1`, a left row is exploded over at most two buckets."""
    from pyspark.sql import functions as F  # noqa: N812

    first, last = floor_div(low, width), floor_div(high, width)
    exploded = left.withColumn(
        "_left_bucket", F.explode(F.expr(f"sequence({first}, greatest({first}, {last}))"))
    )
    bucketed = right.withColumn("_right_bucket", F.expr(floor_div(position, width)))
    condition = [F.col(a) == F.col(b) for a, b in keys]
    condition.append(F.col("_left_bucket") == F.col("_right_bucket"))
    return (
        exploded.join(bucketed, condition)
        .drop("_left_bucket", "_right_bucket")
        .filter(F.expr(f"{position} BETWEEN {low} AND {high}"))
    )


def _latest_before(
    rows: DataFrame,
    lookups: DataFrame,
    *,
    partition: Sequence[str],
    payload: Column,
) -> DataFrame:
    """For each lookup `(partition..., s_ms, s_tx)`, the payload of the latest row -- by
    `(ms, ns, id)` -- whose `ms` is strictly before `s_ms`, or null. One sort per partition: lookups
    are unioned in and sort before every row at their own millisecond."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F  # noqa: N812

    payload_type = rows.select(payload.alias("_p")).schema["_p"].dataType
    data = rows.select(
        *partition,
        F.col("ms").alias("_ms"),
        F.lit(1).alias("_kind"),
        F.col("ns").alias("_ns"),
        F.col("id").alias("_id"),
        payload.alias("_payload"),
        F.lit(None).cast("string").alias("s_tx"),
    )
    probes = lookups.select(
        *partition,
        F.col("s_ms").alias("_ms"),
        F.lit(0).alias("_kind"),
        F.lit(None).cast("string").alias("_ns"),
        F.lit(None).cast("string").alias("_id"),
        F.lit(None).cast(payload_type).alias("_payload"),
        "s_tx",
    )
    order = (
        Window.partitionBy(*partition)
        .orderBy("_ms", "_kind", "_ns", "_id")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return (
        data.unionByName(probes)
        .withColumn("_latest", F.last("_payload", ignorenulls=True).over(order))
        .filter(F.col("_kind") == 0)
        .select("s_tx", "_latest")
    )


def subjects(obs: DataFrame) -> DataFrame:
    """Every transaction, as the subject of its own point-in-time context."""
    from pyspark.sql import functions as F  # noqa: N812

    return obs.filter(F.col("stream") == _TX).select(
        F.col("event_id").alias("s_tx"),
        F.col("occurred_ms").alias("s_ms"),
        F.col("identity_namespace").alias("s_ns"),
        F.col("event_id").alias("s_id"),
        F.col("currency").alias("s_currency"),
        F.col("amount_minor").alias("s_amount"),
        *[F.col(c).alias(f"s_{c}") for c in sorted(set(ENTITY_COLUMNS.values()))],
    )


# ------------------------------------------------------------------- windows ---


def tx_windows_schema() -> StructType:
    from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType

    decimals = {"amount_sum_minor", "aligned_amount_sum_minor", "aligned_amount_sum_squares"}
    return StructType(
        [
            StructField("transaction_id", StringType(), nullable=False),
            StructField("entity", StringType(), nullable=False),
            StructField("entity_id", StringType(), nullable=False),
            StructField("stream", StringType(), nullable=False),
            StructField("window_label", StringType(), nullable=False),
            StructField("window_seconds", LongType(), nullable=False),
            *[
                StructField(name, DecimalType(38, 0) if name in decimals else LongType(), True)
                for name in SCALAR_FIELDS
            ],
            *[
                StructField(distinct_column(d), LongType(), nullable=True)
                for d in sorted(Dimension, key=lambda d: d.value)
            ],
        ]
    )


_VALUE_COLUMNS: Final = (*SCALAR_FIELDS, *[distinct_column(d) for d in Dimension])


def _partial(
    frame: DataFrame, spec: WindowSpec, present: Column, values: Mapping[str, Column]
) -> DataFrame:
    """One source's contribution to a declared window, with every other value column null."""
    from pyspark.sql import functions as F  # noqa: N812

    schema = tx_windows_schema()
    types = {f.name: f.dataType for f in schema.fields}
    return frame.select(
        F.col("s_tx").alias("transaction_id"),
        F.lit(spec.entity.value).alias("entity"),
        F.col("s_entity").alias("entity_id"),
        F.lit(spec.stream.value).alias("stream"),
        F.lit(spec.label).alias("window_label"),
        F.lit(spec.window.seconds).cast("long").alias("window_seconds"),
        present.alias("_present"),
        *[
            (values[name] if name in values else F.lit(None)).cast(types[name]).alias(name)
            for name in _VALUE_COLUMNS
        ],
    )


def _member_sql(spec: WindowSpec) -> str:
    """Whether candidate `c_*` is a member of subject `s_*`'s window (ADR-0046 §2, ADR-0049 §5)."""
    lower = f"c_ms > s_ms - {spec.window_ms}L"
    if spec.current_observation is CurrentObservation.PRIOR_KNOWN:
        return (
            f"{lower} AND c_ms < s_ms AND c_id <> s_tx "
            f"AND c_verification = {_sql_string(Verification.VERIFIED.value)} "
            f"AND c_outcome IN ({_sql_list(OBSERVED_OUTCOMES)})"
        )
    if spec.current_observation is CurrentObservation.INCLUDED:
        return (
            f"{lower} AND (c_ms < s_ms OR (c_ms = s_ms AND "
            f"named_struct('n', c_ns, 'i', c_id) <= named_struct('n', s_ns, 'i', s_id)))"
        )
    raise AssertionError(f"{spec.current_observation} is not compiled (gold_plan)")


def _raw_partials(
    obs: DataFrame, subject: DataFrame, specs: Sequence[WindowSpec]
) -> list[DataFrame]:
    """Counts, same-currency sums, exact distinct counts and outcome counts, from raw observations,
    one join per `(entity, stream)` over its widest declared window."""
    from pyspark.sql import functions as F  # noqa: N812

    groups: dict[tuple[Entity, Stream], list[WindowSpec]] = {}
    for spec in specs:
        if spec.needs_raw:
            groups.setdefault((spec.entity, spec.stream), []).append(spec)
    partials: list[DataFrame] = []
    for (entity, stream), group in sorted(
        groups.items(), key=lambda g: (g[0][0].value, g[0][1].value)
    ):
        column = ENTITY_COLUMNS[entity]
        widest = max(spec.window_ms for spec in group)
        dimensions = sorted(
            {d for spec in group for d in spec.exact_dimensions}, key=lambda d: d.value
        )
        left = subject.filter(F.col(f"s_{column}").isNotNull()).select(
            "s_tx", "s_ms", "s_ns", "s_id", "s_currency", F.col(f"s_{column}").alias("s_entity")
        )
        right = obs.filter((F.col("stream") == stream.value) & F.col(column).isNotNull()).select(
            F.col(column).alias("c_entity"),
            F.col("occurred_ms").alias("c_ms"),
            F.col("identity_namespace").alias("c_ns"),
            F.col("event_id").alias("c_id"),
            F.col("currency").alias("c_currency"),
            F.col("amount_minor").alias("c_amount"),
            F.col("authorization_outcome").alias("c_outcome"),
            F.col("verification").alias("c_verification"),
            *[F.col(DIMENSION_COLUMNS[d]).alias(f"c_dim_{d.value.lower()}") for d in dimensions],
        )
        joined = _range_join(
            left,
            right,
            keys=[("s_entity", "c_entity")],
            low=f"s_ms - {widest}L + 1",
            high="s_ms",
            position="c_ms",
            width=widest,
        )
        aggregates: list[Column] = []
        for index, spec in enumerate(group):
            member = _member_sql(spec)
            prefix = f"w{index}_"
            aggregates.append(
                F.sum(F.expr(f"CASE WHEN {member} THEN 1L ELSE 0L END")).alias(f"{prefix}count")
            )
            if "amount_sum_minor" in spec.fields:
                aggregates.append(
                    F.sum(
                        F.expr(
                            f"CASE WHEN {member} AND c_currency = s_currency "
                            f"THEN CAST(c_amount AS {DECIMAL}) ELSE CAST(0 AS {DECIMAL}) END"
                        )
                    ).alias(f"{prefix}amount_sum_minor")
                )
            if "declined_count" in spec.fields:
                aggregates.append(
                    F.sum(
                        F.expr(
                            f"CASE WHEN {member} AND c_outcome = "
                            f"{_sql_string(OBSERVED_OUTCOMES[1])} THEN 1L ELSE 0L END"
                        )
                    ).alias(f"{prefix}declined_count")
                )
            for dimension in spec.exact_dimensions:
                aggregates.append(
                    F.count_distinct(
                        F.expr(f"CASE WHEN {member} THEN c_dim_{dimension.value.lower()} END")
                    ).alias(f"{prefix}{distinct_column(dimension)}")
                )
        grouped = joined.groupBy("s_tx", "s_entity").agg(*aggregates)
        for index, spec in enumerate(group):
            prefix = f"w{index}_"
            values: dict[str, Column] = {}
            for name in spec.raw_fields:
                source = "count" if name == "outcome_known_count" else name
                values[name] = F.col(f"{prefix}{source}")
            for dimension in spec.exact_dimensions:
                values[distinct_column(dimension)] = F.col(f"{prefix}{distinct_column(dimension)}")
            present = F.lit(True) if spec.present_by_construction else F.col(f"{prefix}count") > 0
            partials.append(_partial(grouped, spec, present, values))
    return partials


def _approximate_partials(
    buckets: DataFrame, subject: DataFrame, specs: Sequence[WindowSpec]
) -> list[DataFrame]:
    """The declared edge-inclusive five-minute-bucket estimand (ADR-0046 §2), from
    `gold.distinct_buckets`: buckets `floor((as_of - W) / 5 min)` through `floor(as_of / 5 min)`."""
    from pyspark.sql import functions as F  # noqa: N812

    partials: list[DataFrame] = []
    for spec in specs:
        column = ENTITY_COLUMNS[spec.entity]
        for dimension in sorted(spec.approximate_dimensions, key=lambda d: d.value):
            left = subject.filter(F.col(f"s_{column}").isNotNull()).select(
                "s_tx", "s_ms", F.col(f"s_{column}").alias("s_entity")
            )
            right = buckets.filter(
                (F.col("entity") == spec.entity.value) & (F.col("dimension") == dimension.value)
            ).select(
                F.col("entity_id").alias("c_entity"),
                F.col("bucket").alias("c_bucket"),
                F.col("value").alias("c_value"),
            )
            joined = _range_join(
                left,
                right,
                keys=[("s_entity", "c_entity")],
                low=floor_div(f"s_ms - {spec.window_ms}L", APPROXIMATE_BUCKET_MS),
                high=floor_div("s_ms", APPROXIMATE_BUCKET_MS),
                position="c_bucket",
                width=spec.window_ms // APPROXIMATE_BUCKET_MS + 1,
            )
            counts = joined.groupBy("s_tx").agg(F.count_distinct("c_value").alias("_n"))
            frame = left.join(counts, "s_tx", "left")
            values = {distinct_column(dimension): F.coalesce(F.col("_n"), F.lit(0))}
            partials.append(_partial(frame, spec, F.lit(True), values))
    return partials


def _aligned_partials(
    buckets: DataFrame, subject: DataFrame, specs: Sequence[WindowSpec]
) -> list[DataFrame]:
    """The merchant CV's declared minute-aligned window (ADR-0046 §2), from `gold.minute_buckets`:
    same-currency whole minutes strictly between the minute of `as_of - W` and the minute of
    `as_of`, plus the scored transaction itself, which lies in the excluded minute of `as_of`."""
    from pyspark.sql import functions as F  # noqa: N812

    partials: list[DataFrame] = []
    for spec in specs:
        if not spec.needs_aligned:
            continue
        column = ENTITY_COLUMNS[spec.entity]
        left = subject.filter(F.col(f"s_{column}").isNotNull()).select(
            "s_tx", "s_ms", "s_currency", "s_amount", F.col(f"s_{column}").alias("s_entity")
        )
        right = buckets.filter(F.col("entity") == spec.entity.value).select(
            F.col("entity_id").alias("c_entity"),
            F.col("currency").alias("c_currency"),
            F.col("minute").alias("c_minute"),
            F.col("observation_count").alias("c_n"),
            F.col("amount_sum_minor").alias("c_s"),
            F.col("amount_sum_squares").alias("c_q"),
        )
        joined = _range_join(
            left,
            right,
            keys=[("s_entity", "c_entity"), ("s_currency", "c_currency")],
            low=f"{floor_div(f's_ms - {spec.window_ms}L', ALIGNED_MINUTE_MS)} + 1",
            high=f"{floor_div('s_ms', ALIGNED_MINUTE_MS)} - 1",
            position="c_minute",
            width=spec.window_ms // ALIGNED_MINUTE_MS + 1,
        )
        sums = joined.groupBy("s_tx").agg(
            F.sum("c_n").alias("_n"), F.sum("c_s").alias("_s"), F.sum("c_q").alias("_q")
        )
        frame = left.join(sums, "s_tx", "left")
        amount = F.col("s_amount").cast("DECIMAL(20,0)")
        zero = F.lit(0).cast(DECIMAL)
        # INCLUDED on the transaction stream (gold_plan): the scored transaction is added once.
        values = {
            "aligned_count": F.coalesce(F.col("_n"), F.lit(0)) + F.lit(1),
            "aligned_amount_sum_minor": F.coalesce(F.col("_s"), zero) + amount,
            "aligned_amount_sum_squares": F.coalesce(F.col("_q"), zero) + amount * amount,
        }
        partials.append(_partial(frame, spec, F.lit(True), values))
    return partials


def tx_windows(
    obs: DataFrame,
    minute: DataFrame,
    distinct: DataFrame,
    specs: Sequence[WindowSpec],
) -> DataFrame:
    """Every declared window each transaction's context holds, as `reference.window_state` would
    return it, one row per `(transaction, entity, stream, window)`; an absent window has no row."""
    from pyspark.sql import functions as F  # noqa: N812

    subject = subjects(obs)
    partials = [
        *_raw_partials(obs, subject, specs),
        *_approximate_partials(distinct, subject, specs),
        *_aligned_partials(minute, subject, specs),
    ]
    union = partials[0]
    for part in partials[1:]:
        union = union.unionByName(part)
    keys = ["transaction_id", "entity", "entity_id", "stream", "window_label", "window_seconds"]
    merged = (
        union.groupBy(*keys)
        .agg(F.max("_present").alias("_present"), *[F.max(c).alias(c) for c in _VALUE_COLUMNS])
        .filter(F.col("_present"))
    )
    return _conform(merged, tx_windows_schema())


# ------------------------------------------------------------------ previous ---


def tx_previous_schema() -> StructType:
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("transaction_id", StringType(), nullable=False),
            StructField("entity", StringType(), nullable=False),
            StructField("entity_id", StringType(), nullable=False),
            StructField("stream", StringType(), nullable=False),
            StructField("occurred_ms", LongType(), nullable=False),
            StructField("latitude", DoubleType(), nullable=True),
            StructField("longitude", DoubleType(), nullable=True),
            StructField("card_present", BooleanType(), nullable=False),
        ]
    )


def tx_previous(spark: SparkSession, obs: DataFrame) -> DataFrame:
    """PLAN's previous observations (ADR-0046 §4): the latest `(occurred_ms, namespace, id)` on the
    stream in `(as_of - L, as_of)`, never the scored transaction itself."""
    from pyspark.sql import functions as F  # noqa: N812

    subject = subjects(obs)
    parts = []
    for (entity, stream), lookback in sorted(
        PLAN.previous.items(), key=lambda item: (item[0][0].value, item[0][1].value)
    ):
        column = ENTITY_COLUMNS[entity]
        lookback_ms = lookback.seconds * 1000
        left = subject.filter(F.col(f"s_{column}").isNotNull()).select(
            "s_tx", "s_ms", "s_ns", "s_id", F.col(f"s_{column}").alias("s_entity")
        )
        right = obs.filter((F.col("stream") == stream.value) & F.col(column).isNotNull()).select(
            F.col(column).alias("c_entity"),
            F.col("occurred_ms").alias("c_ms"),
            F.col("identity_namespace").alias("c_ns"),
            F.col("event_id").alias("c_id"),
            F.col("latitude").alias("c_lat"),
            F.col("longitude").alias("c_lon"),
            F.col("channel").alias("c_channel"),
        )
        joined = _range_join(
            left,
            right,
            keys=[("s_entity", "c_entity")],
            low=f"s_ms - {lookback_ms}L + 1",
            high="s_ms - 1",
            position="c_ms",
            width=lookback_ms,
        ).filter(~((F.col("c_ns") == F.col("s_ns")) & (F.col("c_id") == F.col("s_id"))))
        latest = joined.groupBy("s_tx", "s_entity").agg(
            F.max_by(
                F.struct("c_ms", "c_lat", "c_lon", "c_channel"), F.struct("c_ms", "c_ns", "c_id")
            ).alias("_p")
        )
        parts.append(
            latest.select(
                F.col("s_tx").alias("transaction_id"),
                F.lit(entity.value).alias("entity"),
                F.col("s_entity").alias("entity_id"),
                F.lit(stream.value).alias("stream"),
                F.col("_p.c_ms").alias("occurred_ms"),
                F.col("_p.c_lat").alias("latitude"),
                F.col("_p.c_lon").alias("longitude"),
                F.coalesce(F.col("_p.c_channel") == F.lit(_CARD_PRESENT), F.lit(False)).alias(
                    "card_present"
                ),
            )
        )
    schema = tx_previous_schema()
    if not parts:
        return _empty(spark, schema)
    union = parts[0]
    for part in parts[1:]:
        union = union.unionByName(part)
    return _conform(union, schema)


# ------------------------------------------------------------------ profiles ---


def tx_profiles_schema() -> StructType:
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    # Nested nullability is not enforced by Delta inside an array (as for Bronze's headers), and
    # an empty-array default is typed with nullable elements, so elements are declared nullable.
    names = ArrayType(StringType(), containsNull=True)
    return StructType(
        [
            StructField("transaction_id", StringType(), nullable=False),
            StructField("account_id", StringType(), nullable=False),
            StructField("first_seen_ms", LongType(), nullable=False),
            StructField("observation_count", LongType(), nullable=False),
            StructField("amount_median_minor", DoubleType(), nullable=True),
            StructField("amount_mad_minor", DoubleType(), nullable=True),
            StructField("habitual_merchants", names, nullable=False),
            StructField("habitual_mccs", names, nullable=False),
            StructField("known_devices", names, nullable=False),
            StructField("home_latitude", DoubleType(), nullable=True),
            StructField("home_longitude", DoubleType(), nullable=True),
        ]
    )


def _median_sql(array: str, *, integers: bool) -> str:
    """`statistics.median` of a sorted, non-empty array: the middle value, or the mean of the middle
    two. For integers the mean goes through an exact decimal and its string, so the double is the
    correctly rounded one Python's `(a + b) / 2` returns."""
    n = f"size({array})"
    middle = f"element_at({array}, CAST(({n} div 2) + 1 AS INT))"
    below = f"element_at({array}, CAST({n} div 2 AS INT))"
    if integers:
        even = (
            f"CAST(CAST((CAST({below} AS {DECIMAL}) + CAST({middle} AS {DECIMAL})) / 2 "
            f"AS STRING) AS DOUBLE)"
        )
        odd = f"CAST({middle} AS DOUBLE)"
    else:
        even = f"(({below}) + ({middle})) / 2.0D"
        odd = middle
    return f"(CASE WHEN {n} % 2 = 1 THEN {odd} ELSE {even} END)"


def _haversine_metres_sql(p: str, q: str) -> str:
    """`profile_math.whole_metres(domain.geo.haversine_km(p, q))`, term by term."""
    lat1, lon1, lat2, lon2 = (
        f"radians({p}.lat)",
        f"radians({p}.lon)",
        f"radians({q}.lat)",
        f"radians({q}.lon)",
    )
    sin_lat = f"sin(({lat2} - {lat1}) / 2.0D)"
    sin_lon = f"sin(({lon2} - {lon1}) / 2.0D)"
    h = f"({sin_lat} * {sin_lat} + cos({lat1}) * cos({lat2}) * ({sin_lon} * {sin_lon}))"
    km = f"(2.0D * {EARTH_RADIUS_KM!r}D * asin(sqrt(least(1.0D, {h}))))"
    return f"CAST(floor({km} * 1000.0D + 0.5D) AS BIGINT)"


def _medoid_sql(points: str) -> str:
    """`profile_math.geodesic_medoid`: the sample point with the least whole-metre distance sum to
    the others, ties to the earliest `(occurred_ms, identity)`; null below the minimum."""
    total = (
        f"aggregate({points}, 0L, (acc, q) -> acc + CASE WHEN p.identity = q.identity THEN 0L "
        f"ELSE {_haversine_metres_sql('p', 'q')} END)"
    )
    ranked = (
        f"transform({points}, p -> named_struct('total', {total}, 'ms', p.ms, "
        f"'identity', p.identity, 'lat', p.lat, 'lon', p.lon))"
    )
    return f"(CASE WHEN size({points}) >= {HOME_MIN_OBSERVATIONS} THEN array_min({ranked}) END)"


def tx_profiles(obs: DataFrame) -> DataFrame:
    """The account's lifetime strictly before each transaction's `as_of`, reduced
    (`reference.lifetime_profile`, ADR-0046 §3).

    The lifetime of subject `s` is the run -- consecutive transactions less than 30 days apart -- of
    `L`, the latest account transaction strictly before `as_of`, up to `L`; there is no profile when
    `L` is absent or at least 30 days old. The amount sample is the last 128 same-currency amounts
    of that lifetime, the home sample its last 20 located points, and a merchant or category is
    habitual once its third visit in the lifetime precedes `as_of`.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F  # noqa: N812

    history = obs.filter(F.col("stream") == _TX).select(
        F.col("account_id").alias("a"),
        F.col("occurred_ms").alias("ms"),
        F.col("identity_namespace").alias("ns"),
        F.col("event_id").alias("id"),
        F.col("observation_identity").alias("identity"),
        F.col("currency").alias("cur"),
        F.col("amount_minor").alias("amount"),
        F.col("merchant_id").alias("merchant"),
        F.col("merchant_mcc").alias("mcc"),
        F.col("device_id").alias("device"),
        F.col("latitude").alias("lat"),
        F.col("longitude").alias("lon"),
    )
    ordered = Window.partitionBy("a").orderBy("ms", "ns", "id")
    running = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    previous_ms = F.lag("ms").over(ordered)
    history = history.withColumn(
        "run",
        F.sum(
            F.when(previous_ms.isNull() | (F.col("ms") - previous_ms >= _GAP_MS), 1).otherwise(0)
        ).over(running),
    )
    history = history.withColumn("run_start_ms", F.min("ms").over(Window.partitionBy("a", "run")))

    lookups = subjects(obs).select(
        "s_tx", "s_ms", F.col("s_account_id").alias("a"), F.col("s_currency").alias("cur")
    )

    latest = _latest_before(
        history, lookups, partition=["a"], payload=F.struct("ms", "run", "run_start_ms")
    )
    base = (
        lookups.join(latest, "s_tx")
        .filter(F.col("_latest").isNotNull() & (F.col("s_ms") - F.col("_latest.ms") < _GAP_MS))
        .select(
            "s_tx",
            "s_ms",
            "a",
            "cur",
            F.col("_latest.run").alias("life_run"),
            F.col("_latest.run_start_ms").alias("first_seen_ms"),
        )
    )

    by_currency = history.withColumn(
        "cur_rn",
        F.row_number().over(Window.partitionBy("a", "cur").orderBy("ms", "ns", "id")).cast("long"),
    )
    latest_same_currency = _latest_before(
        by_currency, lookups, partition=["a", "cur"], payload=F.struct("run", "cur_rn")
    )
    amount_rows = by_currency.select(
        "a", "cur", "cur_rn", F.col("run").alias("r_run"), F.abs("amount").alias("abs_amount")
    )
    amounts = (
        base.join(latest_same_currency, "s_tx")
        .filter(F.col("_latest.run") == F.col("life_run"))
        .select(
            "s_tx",
            "a",
            "cur",
            "life_run",
            F.explode(
                F.expr(
                    f"sequence(greatest(_latest.cur_rn - {AMOUNT_SAMPLE_SIZE - 1}L, 1L), "
                    f"_latest.cur_rn)"
                )
            ).alias("cur_rn"),
        )
        .join(amount_rows, ["a", "cur", "cur_rn"])
        .filter(F.col("r_run") == F.col("life_run"))
        .groupBy("s_tx")
        .agg(F.array_sort(F.collect_list("abs_amount")).alias("_amounts"))
    )

    located = history.filter(F.col("lat").isNotNull() & F.col("lon").isNotNull()).withColumn(
        "loc_rn", F.row_number().over(ordered).cast("long")
    )
    latest_located = _latest_before(
        located, lookups, partition=["a"], payload=F.struct("run", "loc_rn")
    )
    point_rows = located.select(
        "a", "loc_rn", F.col("run").alias("r_run"), "ms", "identity", "lat", "lon"
    )
    points = (
        base.join(latest_located, "s_tx")
        .filter(F.col("_latest.run") == F.col("life_run"))
        .select(
            "s_tx",
            "a",
            "life_run",
            F.explode(
                F.expr(
                    f"sequence(greatest(_latest.loc_rn - {HOME_SAMPLE_SIZE - 1}L, 1L), "
                    f"_latest.loc_rn)"
                )
            ).alias("loc_rn"),
        )
        .join(point_rows, ["a", "loc_rn"])
        .filter(F.col("r_run") == F.col("life_run"))
        .groupBy("s_tx")
        .agg(F.collect_list(F.struct("ms", "identity", "lat", "lon")).alias("_points"))
    )

    def members(column: str, visits: int, alias: str) -> DataFrame:
        """Values whose `visits`-th visit in a run precedes the subject's `as_of`."""
        visit = Window.partitionBy("a", "run", column).orderBy("ms", "ns", "id")
        reached = (
            history.filter(F.col(column).isNotNull())
            .withColumn("_visit", F.row_number().over(visit))
            .filter(F.col("_visit") == visits)
            .select(
                "a",
                F.col("run").alias("life_run"),
                F.col(column).alias("_value"),
                F.col("ms").alias("_since_ms"),
            )
        )
        return (
            base.join(reached, ["a", "life_run"])
            .filter(F.col("_since_ms") < F.col("s_ms"))
            .groupBy("s_tx")
            .agg(F.array_sort(F.collect_set("_value")).alias(alias))
        )

    empty_names = F.expr("CAST(array() AS ARRAY<STRING>)")
    median = _median_sql("_amounts", integers=True)
    deviations = f"array_sort(transform(_amounts, x -> abs(CAST(x AS DOUBLE) - {median})))"
    home = _medoid_sql("_points")
    profile = (
        base.join(amounts, "s_tx", "left")
        .join(points, "s_tx", "left")
        .join(members("merchant", HABITUAL_MIN_VISITS, "_merchants"), "s_tx", "left")
        .join(members("mcc", HABITUAL_MIN_VISITS, "_mccs"), "s_tx", "left")
        .join(members("device", 1, "_devices"), "s_tx", "left")
        .withColumn("_n", F.coalesce(F.size("_amounts"), F.lit(0)))
        .withColumn("_home", F.expr(home))
        .select(
            F.col("s_tx").alias("transaction_id"),
            F.col("a").alias("account_id"),
            "first_seen_ms",
            F.col("_n").cast("long").alias("observation_count"),
            F.expr(f"CASE WHEN _n >= {MIN_OBSERVATIONS_FOR_ROBUST_Z} THEN {median} END").alias(
                "amount_median_minor"
            ),
            F.expr(
                f"CASE WHEN _n >= {MIN_OBSERVATIONS_FOR_ROBUST_Z} THEN "
                f"{_median_sql(deviations, integers=False)} END"
            ).alias("amount_mad_minor"),
            F.coalesce("_merchants", empty_names).alias("habitual_merchants"),
            F.coalesce("_mccs", empty_names).alias("habitual_mccs"),
            F.coalesce("_devices", empty_names).alias("known_devices"),
            F.col("_home.lat").alias("home_latitude"),
            F.col("_home.lon").alias("home_longitude"),
        )
    )
    return _conform(profile, tx_profiles_schema())


__all__ = [
    "distinct_buckets",
    "distinct_buckets_schema",
    "floor_div",
    "millis",
    "minute_buckets",
    "minute_buckets_schema",
    "observations",
    "observations_schema",
    "read_pinned",
    "subjects",
    "tx_previous",
    "tx_previous_schema",
    "tx_profiles",
    "tx_profiles_schema",
    "tx_windows",
    "tx_windows_schema",
]
