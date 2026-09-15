"""Silver's decisions, without Spark: what each Bronze record becomes (ADR-0053 §2).

`trace_core.stream.silver` applies these to every Bronze row of a micro-batch. They live here, apart
from Spark, so their unit tests hold them to the ADR without a JVM.

**Admission, in order, for one Bronze record:**
1. No value: quarantined, `null_value`.
2. Not stamped with LogAppendTime: quarantined, `not_log_append_time`. Its arrival time would be a
   producer's clock, so lateness and future skew could not be judged.
3. Not a valid event of its topic, by the producers' own strict generated model
   (`model_validate_json`; `extra="forbid"`, enums and patterns): quarantined, `invalid_event`. A
   value outside a released enum is invalid, because adding one is a breaking change. The detail
   holds only model-declared path segments and pydantic's error types: a key the input supplied is
   written `<extra>`, and no input value is ever echoed, because the input is untrusted.
4. An integer outside the signed 64-bit range of its column: quarantined, `unrepresentable`. The
   detail names the field path, never the value.
5. Future skew (`trace_core.stream.timing`): quarantined, `future_skew`.
6. Otherwise admitted, with its identity, content digest, arrival delay, `is_late` (judged on the
   stored whole milliseconds, so the two never disagree) and its preference fields.

**Identity and content.** The identity is the topic's declared dedup identity
(`deploy/kafka/topics.yaml`, cross-checked by a unit test). The content digest covers the validated
event except what a producer retry may legitimately change:
- the envelope's `ingested_at` and `trace_id`, and `event_id` unless it is the identity itself;
- the producer's version: the digest keeps the producer's name only, so a retry across a redeploy is
  not a conflict;
- for `tx.scored.v1`, the scoring results a retry recomputes: `observe_outcome`, `store_position`,
  `store_epoch`, `decision_summary`, `served_features`, and the envelope's `idempotency_key` (a
  content hash over that payload). The digest is then the transaction's own content.

**Which record is canonical** (`classify`), per identity, by one total order (`order_key`) used
in-batch and across batches alike, so the canonical row does not depend on where micro-batches
fall:
- the order is `observe_outcome == RECORDED` first, then `(store_epoch, store_position)` ascending
  with nulls last, then arrival (LogAppendTime, partition, offset), then topic id. Only
  `tx.scored.v1` records carry the first three, so every other topic is ordered by arrival alone.
  A redelivery never moves the store's counter, so it sorts after the delivery the store recorded;
  more than one `RECORDED` per identity is possible (a store restart starts a new epoch, and an
  expired observation identity lets a retry be recorded again), and the earliest wins;
- with no canonical row yet, the first record in that order is admitted, and each later one is a
  duplicate when its digest matches, otherwise a conflict;
- with a canonical row already committed:
  - a record at its own Kafka coordinates is a replay of the batch that admitted it (`replayed`),
    never a duplicate of itself. Only a committed row is recognised this way: a Bronze coordinate
    repeated within one batch is a duplicate, as Spark classifies it;
  - `tx.scored.v1` only: the first record with the same digest and an earlier key replaces it
    (`supersede`), and the replaced row becomes a `superseded` duplicate. Every other topic's
    canonical row is never replaced (insert-only);
  - otherwise a duplicate when its digest matches the canonical row's, else a conflict
    (quarantined, `identity_conflict`).
"""

from __future__ import annotations

import datetime as dt
import functools
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Final, Literal

from pydantic import ValidationError

from trace_core.contracts.canonical_json import canonical_bytes, content_hash
from trace_core.contracts.publish import _models
from trace_core.contracts.topics import (
    DEVICE_EVENTS_V1,
    IDENTITY_EVENTS_V1,
    INVESTIGATION_REQUESTED_V1,
    PARTITION_KEY_FIELD,
    TX_AUTHORIZATION_V1,
    TX_RAW_V1,
    TX_SCORED_V1,
)
from trace_core.domain.errors import ContractError
from trace_core.stream import timing
from trace_core.stream.bronze import SPARK_LOG_APPEND_TIME, bronze_table_name
from trace_core.stream.lake import Tier, require_identifier
from trace_core.stream.tables import TableRef

SILVER_JOB: Final = "silver_transform"

DEDUP_IDENTITY: Final[Mapping[str, str]] = MappingProxyType(
    {
        TX_RAW_V1: "payload.transaction_id",
        IDENTITY_EVENTS_V1: "envelope.event_id",
        DEVICE_EVENTS_V1: "envelope.event_id",
        INVESTIGATION_REQUESTED_V1: "envelope.idempotency_key",
        TX_AUTHORIZATION_V1: "payload.transaction_id",
        TX_SCORED_V1: "payload.transaction_id",
    }
)
"""Each released topic's dedup identity, as `deploy/kafka/topics.yaml` declares it."""
if set(DEDUP_IDENTITY) != set(PARTITION_KEY_FIELD):
    raise ContractError(
        f"Silver declares identities for {sorted(DEDUP_IDENTITY)}, but the released topics are "
        f"{sorted(PARTITION_KEY_FIELD)}"
    )

RETRY_VARIABLE_ENVELOPE_FIELDS: Final = frozenset({"ingested_at", "trace_id"})
"""Envelope fields a producer retry may change without changing what the event says."""

SUPERSEDABLE_TOPIC: Final = TX_SCORED_V1
"""The one topic whose canonical row a same-digest record earlier in the order can replace."""
RECORDED: Final = "RECORDED"
SCORED_RETRY_VARIABLE_PAYLOAD_FIELDS: Final = frozenset(
    {"observe_outcome", "store_position", "store_epoch", "decision_summary", "served_features"}
)
SCORED_RETRY_VARIABLE_ENVELOPE_FIELDS: Final = frozenset({"idempotency_key"})

INT64_MIN: Final = -(2**63)
INT64_MAX: Final = 2**63 - 1

LATE_EVENTS: Final = TableRef(Tier.SILVER, "late_events")
QUARANTINE: Final = TableRef(Tier.SILVER, "quarantine")
DUPLICATES: Final = TableRef(Tier.SILVER, "duplicates")
SHARED_TABLES: Final = (LATE_EVENTS, QUARANTINE, DUPLICATES)

SILVER_COLUMNS: Final = (
    "silver_identity",
    "content_digest",
    "is_late",
    "arrival_delay_ms",
    "is_backfill",
    "trust_tier",
    "kafka_topic",
    "kafka_topic_id",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "bronze_batch_id",
    "bronze_checkpoint_id",
    "silver_batch_id",
    "silver_checkpoint_id",
    "silver_admitted_at",
)
"""Columns every canonical table carries beside the event's own."""


@dataclass(frozen=True, slots=True)
class SilverTopic:
    """A released topic, its canonical Silver table, its identity, and the query that writes it."""

    topic: str
    table: TableRef
    query: str
    identity: str

    @property
    def identity_section(self) -> str:
        return self.identity.split(".", 1)[0]

    @property
    def identity_field(self) -> str:
        return self.identity.split(".", 1)[1]


def _registry() -> Mapping[str, SilverTopic]:
    shared = {table.name for table in SHARED_TABLES}
    entries: dict[str, SilverTopic] = {}
    for topic in sorted(PARTITION_KEY_FIELD):
        name = bronze_table_name(topic)
        if name in shared:
            raise ContractError(f"{topic} maps to {name}, a shared Silver table's name")
        query = require_identifier("query name", f"{SILVER_JOB}_{name}")
        entries[topic] = SilverTopic(
            topic, TableRef(Tier.SILVER, name), query, DEDUP_IDENTITY[topic]
        )
    return MappingProxyType(entries)


SILVER_TOPICS: Final[Mapping[str, SilverTopic]] = _registry()


def silver_topic(topic: str) -> SilverTopic:
    try:
        return SILVER_TOPICS[topic]
    except KeyError:
        raise ContractError(
            f"{topic!r} is not a released topic, so it has no Silver table "
            f"({sorted(SILVER_TOPICS)})"
        ) from None


# ------------------------------------------------------------------- columns ---

ColumnKind = Literal["string", "long", "double", "boolean", "timestamp", "json"]


@dataclass(frozen=True, slots=True)
class ColumnSpec:
    """One canonical column, derived from the released contract model."""

    name: str
    section: str
    field: str
    kind: ColumnKind
    nullable: bool


def _resolve(node: Mapping[str, Any], defs: Mapping[str, Any]) -> Mapping[str, Any]:
    while "$ref" in node:
        target = defs[str(node["$ref"]).rsplit("/", 1)[-1]]
        node = {**target, **{k: v for k, v in node.items() if k != "$ref"}}
    return node


def _kind(prop: Mapping[str, Any], defs: Mapping[str, Any]) -> tuple[ColumnKind, bool]:
    node = _resolve(prop, defs)
    null_allowed = False
    if "anyOf" in node:
        options = [_resolve(option, defs) for option in node["anyOf"]]
        rest = [option for option in options if option.get("type") != "null"]
        null_allowed = len(rest) != len(options)
        if len(rest) != 1:
            return "json", null_allowed
        node = rest[0]
    kind = node.get("type")
    if isinstance(kind, list):
        null_allowed = null_allowed or "null" in kind
        remaining = [t for t in kind if t != "null"]
        kind = remaining[0] if len(remaining) == 1 else None
    if kind == "string" or (kind is None and "enum" in node):
        return ("timestamp" if node.get("format") == "date-time" else "string"), null_allowed
    if kind == "integer":
        return "long", null_allowed
    if kind == "number":
        return "double", null_allowed
    if kind == "boolean":
        return "boolean", null_allowed
    return "json", null_allowed


@functools.cache
def event_columns(topic: str) -> tuple[ColumnSpec, ...]:
    """The event's own columns, from its released model: required and non-null means NOT NULL, and
    objects and arrays are kept whole as canonical JSON (`<field>_json`)."""
    schema = _models()[silver_topic(topic).topic].model_json_schema(mode="validation")
    defs = schema.get("$defs", {})
    columns: list[ColumnSpec] = []
    for section in ("envelope", "payload"):
        node = _resolve(schema["properties"][section], defs)
        required = set(node.get("required", ()))
        for field, prop in node["properties"].items():
            kind, null_allowed = _kind(prop, defs)
            name = f"{field}_json" if kind == "json" else field
            columns.append(
                ColumnSpec(name, section, field, kind, field not in required or null_allowed)
            )
    names = [column.name for column in columns]
    repeated = {name for name in names if names.count(name) > 1}
    clashes = sorted(repeated | (set(names) & set(SILVER_COLUMNS)))
    if clashes:
        raise ContractError(f"{topic}: canonical column names clash: {clashes}")
    return tuple(columns)


@functools.cache
def declared_names(topic: str) -> frozenset[str]:
    """Every property name the topic's released model declares, at any depth."""
    schema = _models()[silver_topic(topic).topic].model_json_schema(mode="validation")
    names: set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            properties = node.get("properties")
            if isinstance(properties, dict):
                names.update(str(key) for key in properties)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(schema)
    return frozenset(names)


def column_value(spec: ColumnSpec, event: Mapping[str, Any]) -> Any:
    """The typed value of one canonical column from a validated, JSON-mode event."""
    raw = event[spec.section].get(spec.field)
    if raw is None:
        return None
    if spec.kind == "timestamp":
        return dt.datetime.fromisoformat(str(raw))
    if spec.kind == "long":
        return int(raw)
    if spec.kind == "double":
        return float(raw)
    if spec.kind == "boolean":
        return bool(raw)
    if spec.kind == "json":
        return canonical_bytes(raw).decode()
    return str(raw)


# ----------------------------------------------------------------- admission ---


class Outcome(StrEnum):
    ADMITTED = "admitted"
    QUARANTINED = "quarantined"


class QuarantineReason(StrEnum):
    NULL_VALUE = "null_value"
    NOT_LOG_APPEND_TIME = "not_log_append_time"
    INVALID_EVENT = "invalid_event"
    UNREPRESENTABLE = "unrepresentable"
    FUTURE_SKEW = "future_skew"
    IDENTITY_CONFLICT = "identity_conflict"


@dataclass(frozen=True, slots=True, order=True)
class Coordinates:
    """Where a record sits in Kafka; the topic id tells a recreated topic's offsets apart."""

    topic_id: str
    partition: int
    offset: int


@dataclass(frozen=True, slots=True)
class BronzeRecord:
    topic: str
    coordinates: Coordinates
    logged_at: dt.datetime
    timestamp_type: int
    value: bytes | None
    headers: tuple[tuple[str, bytes | None], ...] = ()


@dataclass(frozen=True, slots=True)
class Admission:
    outcome: Outcome
    reason: QuarantineReason | None = None
    detail: str | None = None
    identity: str | None = None
    digest: str | None = None
    event: Mapping[str, Any] | None = None
    """The validated event in JSON mode, when it validated."""
    occurred_at: dt.datetime | None = None
    ingested_at: dt.datetime | None = None
    is_late: bool | None = None
    arrival_delay_ms: int | None = None
    backfill: bool = False
    recorded: bool = False
    """`tx.scored.v1` only: the store recorded this delivery (`observe_outcome == RECORDED`)."""
    store_epoch_us: int | None = None
    store_position: int | None = None


MAX_DETAIL_CHARS: Final = 1000
EXTRA_SEGMENT: Final = "<extra>"
_EPOCH: Final = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)


def _micros(moment: dt.datetime) -> int:
    return (moment - _EPOCH) // dt.timedelta(microseconds=1)


def _validation_detail(topic: str, exc: ValidationError) -> str:
    """Each error's location and type, from declared names only (module docstring)."""
    declared = declared_names(topic)
    parts: list[str] = []
    for error in exc.errors(include_url=False, include_context=False, include_input=False):
        location = list(error["loc"])
        segments: list[str] = []
        for index, segment in enumerate(location):
            supplied_key = error["type"] == "extra_forbidden" and index == len(location) - 1
            if isinstance(segment, int) and not isinstance(segment, bool):
                segments.append(str(segment))
            elif not supplied_key and isinstance(segment, str) and segment in declared:
                segments.append(segment)
            else:
                segments.append(EXTRA_SEGMENT)
        parts.append(f"{'.'.join(segments) or '<value>'}: {error['type']}")
    return "; ".join(parts)[:MAX_DETAIL_CHARS]


def content_digest(topic: str, event: Mapping[str, Any]) -> str:
    """`sha256:` over the event without what a producer retry may change (module docstring)."""
    excluded = set(RETRY_VARIABLE_ENVELOPE_FIELDS)
    if silver_topic(topic).identity != "envelope.event_id":
        excluded.add("event_id")
    payload = dict(event["payload"])
    if topic == SUPERSEDABLE_TOPIC:
        excluded |= SCORED_RETRY_VARIABLE_ENVELOPE_FIELDS
        payload = {
            k: v for k, v in payload.items() if k not in SCORED_RETRY_VARIABLE_PAYLOAD_FIELDS
        }
    envelope = {k: v for k, v in event["envelope"].items() if k not in excluded}
    if isinstance(envelope.get("producer"), str):
        envelope["producer"] = envelope["producer"].split("@", 1)[0]
    return content_hash({"envelope": envelope, "payload": payload})


def identity_of(topic: str, event: Mapping[str, Any]) -> str:
    spec = silver_topic(topic)
    return str(event[spec.identity_section][spec.identity_field])


def _unrepresentable(topic: str, event: Mapping[str, Any]) -> str | None:
    for column in event_columns(topic):
        if column.kind != "long":
            continue
        raw = event[column.section].get(column.field)
        if isinstance(raw, int) and not INT64_MIN <= raw <= INT64_MAX:
            return f"{column.section}.{column.field}: outside the signed 64-bit range of its column"
    return None


def admit(record: BronzeRecord) -> Admission:
    """What one Bronze record becomes (module docstring)."""
    topic = silver_topic(record.topic).topic
    model = _models().get(topic)
    if model is None:
        raise ContractError(f"no released contract model for {record.topic!r}")
    if record.value is None:
        return Admission(
            Outcome.QUARANTINED, QuarantineReason.NULL_VALUE, "the record has no value"
        )
    if record.timestamp_type != SPARK_LOG_APPEND_TIME:
        return Admission(
            Outcome.QUARANTINED,
            QuarantineReason.NOT_LOG_APPEND_TIME,
            f"timestamp type {record.timestamp_type}, not LogAppendTime: its arrival time would be "
            f"a producer's clock",
        )
    try:
        parsed = model.model_validate_json(record.value)
    except ValidationError as exc:
        return Admission(
            Outcome.QUARANTINED, QuarantineReason.INVALID_EVENT, _validation_detail(topic, exc)
        )
    event = parsed.model_dump(mode="json", by_alias=True)
    too_large = _unrepresentable(topic, event)
    if too_large is not None:
        return Admission(Outcome.QUARANTINED, QuarantineReason.UNREPRESENTABLE, too_large)
    envelope = parsed.envelope
    occurred, ingested = envelope.occurred_at, envelope.ingested_at
    identity = identity_of(topic, event)
    digest = content_digest(topic, event)
    backfill = timing.is_backfill(record.headers)
    if timing.future_skewed(
        occurred_at=occurred,
        producer=envelope.producer,
        ingested_at=ingested,
        logged_at=record.logged_at,
    ):
        return Admission(
            Outcome.QUARANTINED,
            QuarantineReason.FUTURE_SKEW,
            f"occurred_at is beyond the v{timing.TIMING_SEMANTICS_VERSION} future-skew limit",
            identity=identity,
            digest=digest,
            event=event,
            occurred_at=occurred,
            ingested_at=ingested,
            backfill=backfill,
        )
    delay_ms = timing.arrival_delay_ms(logged_at=record.logged_at, occurred_at=occurred)
    recorded, epoch_us, position = False, None, None
    if topic == SUPERSEDABLE_TOPIC:
        payload = event["payload"]
        recorded = payload.get("observe_outcome") == RECORDED
        epoch = payload.get("store_epoch")
        epoch_us = None if epoch is None else _micros(dt.datetime.fromisoformat(str(epoch)))
        position = payload.get("store_position")
    return Admission(
        Outcome.ADMITTED,
        identity=identity,
        digest=digest,
        event=event,
        occurred_at=occurred,
        ingested_at=ingested,
        is_late=timing.is_late_ms(delay_ms, backfill=backfill),
        arrival_delay_ms=delay_ms,
        backfill=backfill,
        recorded=recorded,
        store_epoch_us=epoch_us,
        store_position=None if position is None else int(position),
    )


# ------------------------------------------------------------ classification ---

KEY_NULL: Final = INT64_MAX
"""Where a null store epoch or position sorts: after every real value (Spark's Long.MaxValue)."""

OrderKey = tuple[int, int, int, int, int, int, str]


def order_key(
    *,
    recorded: bool,
    store_epoch_us: int | None,
    store_position: int | None,
    logged_at_us: int,
    coordinates: Coordinates,
) -> OrderKey:
    """The one total order of an identity's records, used in-batch and across batches alike:
    RECORDED first, then (store_epoch, store_position) with nulls last, then arrival
    (LogAppendTime, partition, offset), then topic id. Only `tx.scored.v1` records carry the first
    three, so every other topic is ordered by arrival alone."""
    return (
        0 if recorded else 1,
        KEY_NULL if store_epoch_us is None else store_epoch_us,
        KEY_NULL if store_position is None else store_position,
        logged_at_us,
        coordinates.partition,
        coordinates.offset,
        coordinates.topic_id,
    )


class Disposition(StrEnum):
    ADMIT = "admit"
    SUPERSEDE = "supersede"
    REPLAYED = "replayed"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


class DuplicateKind(StrEnum):
    """What a `silver.duplicates` row is."""

    DUPLICATE = "duplicate"
    SUPERSEDED = "superseded"
    """A canonical row replaced by a record earlier in the order, which it now points to."""


@dataclass(frozen=True, slots=True)
class Candidate:
    """An admitted record of one micro-batch."""

    identity: str
    digest: str
    coordinates: Coordinates
    logged_at: dt.datetime
    recorded: bool = False
    store_epoch_us: int | None = None
    store_position: int | None = None

    @property
    def key(self) -> OrderKey:
        return order_key(
            recorded=self.recorded,
            store_epoch_us=self.store_epoch_us,
            store_position=self.store_position,
            logged_at_us=_micros(self.logged_at),
            coordinates=self.coordinates,
        )


@dataclass(frozen=True, slots=True)
class Canonical:
    """The canonical row an identity has."""

    coordinates: Coordinates
    digest: str
    key: OrderKey


@dataclass(frozen=True, slots=True)
class Classified:
    candidate: Candidate
    disposition: Disposition
    canonical: Canonical
    """For a duplicate or conflict, the row it is judged against; for an admit or supersede, the
    candidate itself; for a replay, the committed row."""
    replaces: Canonical | None = None
    """For a supersede, the committed row it replaces."""


def candidate(admission: Admission, record: BronzeRecord) -> Candidate:
    """The classification input of an admitted record."""
    if admission.identity is None or admission.digest is None:
        raise ContractError("only an admitted record is a candidate")
    return Candidate(
        identity=admission.identity,
        digest=admission.digest,
        coordinates=record.coordinates,
        logged_at=record.logged_at,
        recorded=admission.recorded,
        store_epoch_us=admission.store_epoch_us,
        store_position=admission.store_position,
    )


def _as_canonical(item: Candidate) -> Canonical:
    return Canonical(item.coordinates, item.digest, item.key)


def classify(
    candidates: Iterable[Candidate],
    existing: Mapping[str, Canonical],
    *,
    supersedable: bool,
) -> tuple[Classified, ...]:
    """Every admitted record of a batch, against the canonical rows its identities already have.

    With `supersedable` (`tx.scored.v1`), a record with the committed row's digest and an earlier
    key replaces it, so the canonical row is the first of the order however the batches fall.
    Otherwise a committed row is never replaced (insert-only)."""
    groups: dict[str, list[Candidate]] = {}
    for item in candidates:
        groups.setdefault(item.identity, []).append(item)
    result: list[Classified] = []
    for identity in sorted(groups):
        ordered = sorted(groups[identity], key=lambda c: c.key)
        prior = existing.get(identity)
        superseder: Candidate | None = None
        if prior is not None and supersedable:
            superseder = next(
                (
                    c
                    for c in ordered
                    if c.digest == prior.digest
                    and c.coordinates != prior.coordinates
                    and c.key < prior.key
                ),
                None,
            )
        canonical = prior if superseder is None else _as_canonical(superseder)
        for item in ordered:
            if canonical is None:
                canonical = _as_canonical(item)
                result.append(Classified(item, Disposition.ADMIT, canonical))
            elif prior is not None and item.coordinates == prior.coordinates:
                result.append(Classified(item, Disposition.REPLAYED, prior))
            elif item is superseder:
                result.append(Classified(item, Disposition.SUPERSEDE, canonical, replaces=prior))
            elif item.digest == canonical.digest:
                result.append(Classified(item, Disposition.DUPLICATE, canonical))
            else:
                result.append(Classified(item, Disposition.CONFLICT, canonical))
    return tuple(result)


__all__ = [
    "DEDUP_IDENTITY",
    "DUPLICATES",
    "EXTRA_SEGMENT",
    "INT64_MAX",
    "INT64_MIN",
    "KEY_NULL",
    "LATE_EVENTS",
    "QUARANTINE",
    "RECORDED",
    "RETRY_VARIABLE_ENVELOPE_FIELDS",
    "SCORED_RETRY_VARIABLE_ENVELOPE_FIELDS",
    "SCORED_RETRY_VARIABLE_PAYLOAD_FIELDS",
    "SHARED_TABLES",
    "SILVER_COLUMNS",
    "SILVER_JOB",
    "SILVER_TOPICS",
    "SUPERSEDABLE_TOPIC",
    "Admission",
    "BronzeRecord",
    "Candidate",
    "Canonical",
    "Classified",
    "ColumnSpec",
    "Coordinates",
    "Disposition",
    "DuplicateKind",
    "OrderKey",
    "Outcome",
    "QuarantineReason",
    "SilverTopic",
    "admit",
    "candidate",
    "classify",
    "column_value",
    "content_digest",
    "declared_names",
    "event_columns",
    "identity_of",
    "order_key",
    "silver_topic",
]
