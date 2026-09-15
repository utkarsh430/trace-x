"""The Bronze side of the observation log's coverage rule (ADR-0051 §5; PHASE3_PLAN §4.1 point 5).

Bronze holds what the log delivered; `app.producer_sessions` holds what each writer session
claimed. `coverage_from_bronze` is the ONE place Bronze rows meet the rule: it turns rows into
`trace_core.observation.coverage.Observed` and calls `assess`, which is never reimplemented here.

**Which Bronze rows are observations.**
- `tx.scored.v1`: every record. Its only producer is the gateway, so a record without usable session
  headers is a gap at its own arrival time.
- `identity.events.v1` has other producers (the generator). A record with session headers is an
  observation. A record without them is an observation -- a gap -- only when its envelope names the
  gateway as producer. Every gateway record passed schema validation before it was produced
  (ADR-0047), so a value that does not parse is not a gateway record, and is counted, not gapped.
- Usable headers: exactly one `tracex-session-id` (UTF-8, non-empty) and exactly one `tracex-seq`
  (a positive decimal). Anything else reaches `assess` as an observation without a usable session or
  number, which the rule reports as a gap.
- Arrival time is the broker's LogAppendTime. A row stamped with any other timestamp type means the
  topic's declared contract was broken, and assessment is refused rather than run on a producer's
  clock.
- Write time is the writer's own stamp, the envelope's `ingested_at`. Spark extracts it and the
  envelope's producer from the raw value, so no value is shipped to the driver or parsed there. A
  stamp that is absent, does not parse, or has no UTC offset leaves the record without a usable
  session or number: a gap at its arrival. That is sound, not a guess at its write time: the
  record's number is then missing from its session, and the session's own gap holds the write.

**The high-water mark.** For each partition a covered topic's checkpoint reads, the newest arrival
Bronze holds; the mark is the earliest of those. Every record that arrived strictly before it is in
Bronze: LogAppendTime does not decrease along a partition, so a record still unread arrived at or
after its partition's newest, which is at or after the mark. Broker timestamps are milliseconds, so
records AT the mark may be unread: rows at or after it are left out (`beyond_high_water`), and the
rule reads the log through one microsecond before it. The mark is withheld, and the verdict vouches
for nothing, when a checkpoint reports a problem, records no partition, or a partition it reads
holds no row. A hole in Bronze (a conservation failure) leaves numbers missing, so it is reported as
gaps, never vouched for.

**Order of reads.** Bronze first: each table at a pinned snapshot version, and from it the
high-water arrival time. The ledger after, in one statement that also returns the time it read at.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final, LiteralString, Protocol

from trace_core.contracts.topics import IDENTITY_EVENTS_V1, TX_SCORED_V1
from trace_core.domain.errors import ContractError, LakeContractError, NaiveDatetimeError
from trace_core.observability import get_logger
from trace_core.observation.coverage import Coverage, Observed, SessionRow, assess
from trace_core.observation.log import SEQ_HEADER, SESSION_HEADER
from trace_core.observation.supervisor import LEASE_S, TAKEOVER_MARGIN_S
from trace_core.repositories.triage_event import PRODUCER_NAME as GATEWAY_PRODUCER
from trace_core.stream import checkpoints
from trace_core.stream.bronze import SPARK_LOG_APPEND_TIME, bronze_declaration, bronze_topic
from trace_core.stream.bronze_conservation import consumed_ranges
from trace_core.stream.lake import LakeConfig
from trace_core.stream.tables import describe_live_table, require_no_drift, snapshot_facts

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

COVERED_TOPICS: Final = (TX_SCORED_V1, IDENTITY_EVENTS_V1)
"""Topics carrying observations of online state today (ADR-0051 §1)."""

_EPOCH: Final = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
_UNBOUNDED: Final = dt.datetime.max.replace(tzinfo=dt.UTC)
_ONE_MICROSECOND: Final = dt.timedelta(microseconds=1)
_SEQ: Final = re.compile(rb"[1-9][0-9]{0,18}")

LEDGER_QUERY: Final[LiteralString] = (
    "WITH read AS (SELECT statement_timestamp() AS read_at) "
    "SELECT read.read_at, s.session_id, s.started_at, s.heartbeat_at, s.closed_at, s.last_seq "
    "FROM read LEFT JOIN app.producer_sessions s ON split_part(s.producer, '@', 1) = %s "
    "ORDER BY s.started_at, s.session_id"
)
"""One statement, so the rows and the time they were read at come from one snapshot. The LEFT JOIN
returns the read time even when the ledger holds no session."""

_log = get_logger(__name__)


# ------------------------------------------------------------- rows -> Observed ---


@dataclass(frozen=True, slots=True)
class BronzeRecord:
    """What the rule needs from one Bronze row."""

    topic: str
    partition: int
    offset: int
    logged_at_us: int
    """Kafka timestamp as microseconds since the epoch (read as a number, never as a local time)."""
    timestamp_type: int
    session_ids: tuple[bytes | None, ...]
    """Every `tracex-session-id` header value, in delivery order."""
    seqs: tuple[bytes | None, ...]
    producer: str | None
    """The envelope's `producer`, extracted in Spark; None when absent or the value is not JSON."""
    ingested_at: str | None
    """The envelope's `ingested_at`, extracted in Spark: the writer's stamp, as text."""


def logged_at(microseconds: int) -> dt.datetime:
    return _EPOCH + dt.timedelta(microseconds=microseconds)


def _session_id(values: tuple[bytes | None, ...]) -> str | None:
    if len(values) != 1 or values[0] is None:
        return None
    try:
        text = values[0].decode("utf-8")
    except UnicodeDecodeError:
        return None
    return text if text.strip() else None


def _seq(values: tuple[bytes | None, ...]) -> int | None:
    if len(values) != 1 or values[0] is None or not _SEQ.fullmatch(values[0]):
        return None
    return int(values[0])


def _is_gateway(producer: str | None) -> bool:
    return producer is not None and producer.split("@", 1)[0] == GATEWAY_PRODUCER


def written_at(text: str | None) -> dt.datetime | None:
    """The writer's stamp, or None when it is absent, does not parse, or has no UTC offset."""
    if text is None:
        return None
    try:
        moment = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if moment.tzinfo is None or moment.utcoffset() is None:
        return None
    return moment.astimezone(dt.UTC)


def observation_from_record(record: BronzeRecord) -> Observed | None:
    """The observation a Bronze row is, or None when the row observes nothing (module docstring)."""
    if record.topic not in COVERED_TOPICS:
        raise ContractError(
            f"{record.topic} carries no observations of online state ({COVERED_TOPICS})"
        )
    if record.timestamp_type != SPARK_LOG_APPEND_TIME:
        raise ContractError(
            f"{record.topic}[{record.partition}]@{record.offset} has timestamp type "
            f"{record.timestamp_type}, not LogAppendTime ({SPARK_LOG_APPEND_TIME}): the topic's "
            f"declared contract is broken, and arrival times would be a producer's clock"
        )
    arrived = logged_at(record.logged_at_us)
    headerless = not record.session_ids and not record.seqs
    if headerless and record.topic == IDENTITY_EVENTS_V1 and not _is_gateway(record.producer):
        return None
    stamp = written_at(record.ingested_at)
    if stamp is None:
        return Observed(session_id=None, seq=None, logged_at=arrived, written_at=arrived)
    if headerless:
        return Observed(session_id=None, seq=None, logged_at=arrived, written_at=stamp)
    return Observed(
        session_id=_session_id(record.session_ids),
        seq=_seq(record.seqs),
        logged_at=arrived,
        written_at=stamp,
    )


def high_water_arrival(
    newest: Mapping[tuple[str, int], int], consumed: Iterable[tuple[str, int]]
) -> dt.datetime | None:
    """The arrival time before which Bronze holds every record of the consumed partitions.

    For each consumed partition, the newest arrival time Bronze holds (in microseconds); the high
    water is the earliest of those, since a later record on the slowest partition may be unread.
    None when a partition the checkpoint reads holds no record at all -- including one that has
    simply never received any -- because nothing in Bronze bounds what it has not yet delivered.
    Records AT the returned time may still be unread (module docstring), so the bound is strict.
    Relies on LogAppendTime not decreasing along a partition, which a broker clock stepping
    backwards would break.
    """
    partitions = sorted(set(consumed))
    if not partitions or any(key not in newest for key in partitions):
        return None
    return logged_at(min(newest[key] for key in partitions))


@dataclass(frozen=True, slots=True)
class BronzeCoverage:
    """The rule's verdict over Bronze, with the inputs that fix what it can vouch for."""

    coverage: Coverage
    bronze_high_water: dt.datetime | None
    """Bronze holds every record that arrived strictly before this. None: nothing is vouched for."""
    log_read_through: dt.datetime | None
    """What the rule was told the log is read through: one microsecond before the mark."""
    ledger_read_at: dt.datetime
    observations: int
    not_observations: int
    """Identity events from other producers: read, and not the gateway's."""
    beyond_high_water: int
    """Observations at or after the mark, left out: the log is not known to be read that far."""
    table_versions: Mapping[str, int]
    high_water_withheld: tuple[str, ...] = ()
    """Why the mark was withheld, when it was."""


def coverage_from_bronze(
    records: Iterable[BronzeRecord],
    ledger_rows: Iterable[SessionRow],
    *,
    bronze_high_water: dt.datetime | None,
    ledger_read_at: dt.datetime,
    clock_margin_s: float,
    lease_s: float = LEASE_S,
    takeover_margin_s: float = TAKEOVER_MARGIN_S,
    table_versions: Mapping[str, int] | None = None,
    high_water_withheld: tuple[str, ...] = (),
) -> BronzeCoverage:
    """The single call site of `assess` for Bronze. Bronze rows and ledger rows in, coverage out.

    `lease_s` and `takeover_margin_s` default to the gateway writer's own (ADR-0051 §2), so the
    bound the rule puts on an unclosed session is the one the writer actually obeys.
    """
    for name, moment in (
        ("ledger_read_at", ledger_read_at),
        ("bronze_high_water", bronze_high_water),
    ):
        if moment is not None and (moment.tzinfo is None or moment.utcoffset() is None):
            raise NaiveDatetimeError(f"{name} must be timezone-aware UTC, got {moment!r}")
    if bronze_high_water is not None and high_water_withheld:
        raise LakeContractError("a withheld high-water mark cannot also be given")
    read_through = None if bronze_high_water is None else bronze_high_water - _ONE_MICROSECOND
    counts = {"observations": 0, "not_observations": 0, "beyond_high_water": 0}

    def observed() -> Iterator[Observed]:
        for record in records:
            item = observation_from_record(record)
            if item is None:
                counts["not_observations"] += 1
            elif read_through is not None and item.logged_at > read_through:
                counts["beyond_high_water"] += 1
            else:
                counts["observations"] += 1
                yield item

    coverage = assess(
        ledger_rows,
        observed(),
        ledger_read_at=ledger_read_at,
        log_read_through=_UNBOUNDED if read_through is None else read_through,
        lease_s=lease_s,
        takeover_margin_s=takeover_margin_s,
        clock_margin_s=clock_margin_s,
    )
    if read_through is None:
        # Nothing bounds what Bronze has yet to deliver: the gaps are reported, nothing is vouched.
        coverage = replace(coverage, through=_EPOCH)
    return BronzeCoverage(
        coverage=coverage,
        bronze_high_water=bronze_high_water,
        log_read_through=read_through,
        ledger_read_at=ledger_read_at,
        observations=counts["observations"],
        not_observations=counts["not_observations"],
        beyond_high_water=counts["beyond_high_water"],
        table_versions=MappingProxyType(dict(table_versions or {})),
        high_water_withheld=high_water_withheld,
    )


# ------------------------------------------------------------------- ledger ---


class _Rows(Protocol):
    def fetchall(self) -> list[Any]: ...


class LedgerConnection(Protocol):
    """The part of a psycopg connection the ledger read uses (as `trace_stream`, SELECT only)."""

    def execute(self, query: LiteralString, params: tuple[object, ...]) -> _Rows: ...


def _aware(value: Any, what: str) -> dt.datetime:
    if not isinstance(value, dt.datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise NaiveDatetimeError(f"{what} must be a timezone-aware timestamp, got {value!r}")
    return value.astimezone(dt.UTC)


def read_ledger(
    connection: LedgerConnection, *, producer: str = GATEWAY_PRODUCER
) -> tuple[tuple[SessionRow, ...], dt.datetime]:
    """Every session of `producer`, and the database time of the read."""
    rows = connection.execute(LEDGER_QUERY, (producer,)).fetchall()
    if not rows:
        raise LakeContractError("the ledger read returned no row, not even its read time")
    read_at = _aware(rows[0][0], "ledger read time")
    sessions = tuple(
        SessionRow(
            session_id=str(session_id),
            started_at=_aware(started_at, "started_at"),
            heartbeat_at=_aware(heartbeat_at, "heartbeat_at"),
            closed_at=None if closed_at is None else _aware(closed_at, "closed_at"),
            last_seq=None if last_seq is None else int(last_seq),
        )
        for _, session_id, started_at, heartbeat_at, closed_at, last_seq in rows
        if session_id is not None
    )
    return sessions, read_at


# -------------------------------------------------------------------- Spark ---


def _bronze_records(
    spark: SparkSession, lake: LakeConfig, topic: str, version: int
) -> Iterator[BronzeRecord]:
    from pyspark.sql import functions as F  # noqa: N812

    path = bronze_topic(topic).table.local_path(lake)
    headers = F.col("kafka_headers")

    def values_of(name: str) -> Any:
        return F.transform(
            F.filter(headers, lambda h: h["key"] == F.lit(name)), lambda h: h["value"]
        )

    # Extracted on the executors: a value that is not JSON (or not UTF-8) yields nulls there, and no
    # value reaches the driver.
    text = F.col("kafka_value").cast("string")
    frame = (
        spark.read.format("delta")
        .option("versionAsOf", str(version))
        .load(str(path))
        .select(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            F.unix_micros("kafka_timestamp").alias("logged_at_us"),
            "kafka_timestamp_type",
            values_of(SESSION_HEADER).alias("session_ids"),
            values_of(SEQ_HEADER).alias("seqs"),
            F.get_json_object(text, "$.envelope.producer").alias("producer"),
            F.get_json_object(text, "$.envelope.ingested_at").alias("ingested_at"),
        )
    )
    for row in frame.toLocalIterator():
        yield BronzeRecord(
            topic=str(row["kafka_topic"]),
            partition=int(row["kafka_partition"]),
            offset=int(row["kafka_offset"]),
            logged_at_us=int(row["logged_at_us"]),
            timestamp_type=int(row["kafka_timestamp_type"]),
            session_ids=tuple(None if v is None else bytes(v) for v in row["session_ids"] or ()),
            seqs=tuple(None if v is None else bytes(v) for v in row["seqs"] or ()),
            producer=None if row["producer"] is None else str(row["producer"]),
            ingested_at=None if row["ingested_at"] is None else str(row["ingested_at"]),
        )


def assess_bronze_coverage(
    spark: SparkSession,
    lake: LakeConfig,
    ledger: LedgerConnection,
    *,
    clock_margin_s: float,
    lease_s: float = LEASE_S,
    takeover_margin_s: float = TAKEOVER_MARGIN_S,
) -> BronzeCoverage:
    """Read Bronze (pinned), then the ledger, then apply the rule once.

    `clock_margin_s` has no default: it stands for the measured broker-to-PostgreSQL clock offset
    (PHASE3_PLAN §4.1 point 5), and a default would be an unmeasured number."""
    from pyspark.sql import functions as F  # noqa: N812

    versions: dict[str, int] = {}
    newest: dict[tuple[str, int], int] = {}
    consumed: set[tuple[str, int]] = set()
    withheld: list[str] = []
    for topic in COVERED_TOPICS:
        spec = bronze_topic(topic)
        state = checkpoints.read_state(lake, spec.query)
        path = spec.table.local_path(lake)
        facts = snapshot_facts(spark, path)
        if state.identity is None or facts is None:
            raise LakeContractError(
                f"{spec.table} or its checkpoint does not exist; coverage cannot be read from a "
                f"Bronze table that was never ingested"
            )
        require_no_drift(
            bronze_declaration(topic), describe_live_table(spark, spec.table.path_identifier(lake))
        )
        versions[topic] = facts.version
        directory = checkpoints.query_directory(lake, spec.query) / f"v{state.identity.version}"
        ranges = consumed_ranges(
            topic=topic,
            checkpoint_id=state.identity.app_id,
            directory=directory,
            progress=state.progress,
            written_batch=facts.transactions.get(state.identity.app_id),
        )
        withheld.extend(f"{topic}: {problem}" for problem in ranges.problems)
        if not ranges.ranges:
            withheld.append(
                f"{topic}: the checkpoint records no partition it reads, so nothing bounds what "
                f"Bronze has yet to deliver"
            )
        consumed |= {(topic, partition) for partition in ranges.ranges}
        for row in (
            spark.read.format("delta")
            .option("versionAsOf", str(facts.version))
            .load(str(path))
            .groupBy("kafka_partition")
            .agg(F.max(F.unix_micros("kafka_timestamp")).alias("newest_us"))
            .collect()
        ):
            newest[(topic, int(row["kafka_partition"]))] = int(row["newest_us"])
    high_water = None if withheld else high_water_arrival(newest, consumed)
    sessions, read_at = read_ledger(ledger)  # after Bronze's high water is fixed
    records = (
        record
        for topic in COVERED_TOPICS
        for record in _bronze_records(spark, lake, topic, versions[topic])
    )
    result = coverage_from_bronze(
        records,
        sessions,
        bronze_high_water=high_water,
        ledger_read_at=read_at,
        clock_margin_s=clock_margin_s,
        lease_s=lease_s,
        takeover_margin_s=takeover_margin_s,
        table_versions=versions,
        high_water_withheld=tuple(withheld),
    )
    _log.info(
        "bronze_coverage_assessed",
        table_versions=dict(versions),
        bronze_high_water=None if high_water is None else high_water.isoformat(),
        high_water_withheld=list(withheld),
        ledger_read_at=read_at.isoformat(),
        through=result.coverage.through.isoformat(),
        sessions=len(sessions),
        observations=result.observations,
        not_observations=result.not_observations,
        beyond_high_water=result.beyond_high_water,
        gaps=len(result.coverage.gaps),
        anomalies=len(result.coverage.anomalies),
    )
    return result


__all__ = [
    "COVERED_TOPICS",
    "LEDGER_QUERY",
    "BronzeCoverage",
    "BronzeRecord",
    "LedgerConnection",
    "assess_bronze_coverage",
    "coverage_from_bronze",
    "high_water_arrival",
    "logged_at",
    "observation_from_record",
    "read_ledger",
    "written_at",
]
