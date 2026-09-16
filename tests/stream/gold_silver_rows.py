"""Silver-shaped canonical rows from feature-semantics observations, for Gold's stream tests.

Gold reads Silver's canonical tables (ADR-0055). These helpers write them directly, from the
`features.observation.Event`s the literal fixtures are written in, the way Silver would leave them:
one canonical row per identity, the first delivery in arrival order (ADR-0053 §2), in the table
the observation's stream arrives on. Everything a canonical row carries that Gold must not read is
filled in too -- a transaction's own `authorization_outcome`, `is_late`, the Kafka coordinates -- so
a Gold that read it would be caught by the fixtures rather than by an absent column.

An optional prefix renames every entity id and observation id, so many fixture logs can share one
lake and one Gold build without meeting: ids only ever compare for equality, or after an identical
prefix, so no order a fixture depends on changes. `ContextRows` read back are un-prefixed before a
context is built from them.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Final

from trace_core.contracts.topics import IDENTITY_EVENTS_V1, TX_AUTHORIZATION_V1, TX_SCORED_V1
from trace_core.features.observation import Event
from trace_core.features.semantics import Stream
from trace_core.stream.gold_plan import ContextRows
from trace_core.stream.lake import LakeConfig
from trace_core.stream.silver import canonical_schema, create_silver_tables
from trace_core.stream.silver_rules import silver_topic

TOPIC_OF_STREAM: Final[Mapping[Stream, str]] = {
    Stream.TRANSACTION: TX_SCORED_V1,
    Stream.IDENTITY_FAILED_LOGIN: IDENTITY_EVENTS_V1,
    Stream.IDENTITY_CHANGE: IDENTITY_EVENTS_V1,
    Stream.AUTHORIZATION_OUTCOME: TX_AUTHORIZATION_V1,
}
IDENTITY_TYPE_OF_STREAM: Final[Mapping[Stream, str]] = {
    Stream.IDENTITY_FAILED_LOGIN: "LOGIN_FAILED",
    Stream.IDENTITY_CHANGE: "PASSWORD_CHANGE",
}
GOLD_SILVER_TOPICS: Final = (TX_SCORED_V1, IDENTITY_EVENTS_V1, TX_AUTHORIZATION_V1)
ARRIVED: Final = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
ADMITTED: Final = dt.datetime(2026, 9, 15, tzinfo=dt.UTC)
SHA: Final = "0" * 40


def first_deliveries(log: Iterable[Event]) -> list[tuple[int, Event]]:
    """Each identity's first delivery with its arrival position, as Silver keeps it."""
    seen: set[str] = set()
    firsts: list[tuple[int, Event]] = []
    for arrival, event in enumerate(log):
        if event.identity not in seen:
            seen.add(event.identity)
            firsts.append((arrival, event))
    return firsts


def _prefixed(prefix: str, value: str | None) -> str | None:
    return None if value is None else prefix + value


GATEWAY_PRODUCER: Final = "trace-gateway@0.1.0"
GENERATOR_PRODUCER: Final = "trace-generator@1.0.0"
"""A directly produced identity event: the online store never saw it, so Gold must not read it."""


def silver_row(
    event: Event,
    *,
    arrival: int,
    prefix: str = "",
    is_late: bool | None = False,
    identity_type: str | None = None,
    producer: str = GATEWAY_PRODUCER,
    correlation_id: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """`(topic, canonical row)` for one observation's first delivery."""
    topic = TOPIC_OF_STREAM[event.stream]
    event_id = prefix + event.event_id
    account = prefix + event.account_id
    common: dict[str, Any] = {
        "event_type": topic.removesuffix(".v1"),
        "schema_version": 1,
        "occurred_at": event.occurred_at,
        "ingested_at": event.occurred_at,
        "producer": producer,
        "trace_id": "0" * 32,
        "idempotency_key": "sha256:" + "0" * 64,
        "content_digest": "sha256:" + "1" * 64,
        "is_late": is_late,
        "arrival_delay_ms": 0,
        "is_backfill": False,
        "trust_tier": "UNTRUSTED",
        "kafka_topic": topic,
        "kafka_topic_id": "gold-fixture-topic",
        "kafka_partition": 0,
        "kafka_offset": arrival,
        "kafka_timestamp": ARRIVED + dt.timedelta(milliseconds=arrival),
        "bronze_batch_id": 0,
        "bronze_checkpoint_id": "gold-fixture-bronze",
        "silver_batch_id": 0,
        "silver_checkpoint_id": "gold-fixture-silver",
        "silver_admitted_at": ADMITTED,
    }
    if topic == TX_SCORED_V1:
        row = {
            **common,
            "event_id": f"evt-{event_id}",
            "correlation_id": event_id,
            "silver_identity": event_id,
            "transaction_id": event_id,
            "account_id": account,
            "card_id": _prefixed(prefix, event.card_id),
            "device_id": _prefixed(prefix, event.device_id),
            "merchant_id": _prefixed(prefix, event.merchant_id),
            "ip_id": _prefixed(prefix, event.ip_id),
            "amount_minor": event.amount_minor,
            "currency": event.currency,
            "channel": None if event.channel is None else event.channel.value,
            "entry_mode": None,
            "merchant_mcc": event.merchant_mcc,
            "merchant_country": event.merchant_country,
            "merchant_name": None,
            "latitude": event.latitude,
            "longitude": event.longitude,
            "user_agent": None,
            "memo": None,
            # Carried, never read by Gold (ADR-0049 §3): fixtures set DECLINED here to prove it.
            "authorization_outcome": (
                None if event.authorization_outcome is None else event.authorization_outcome.value
            ),
            "field_coverage_json": "[]",
            "decision_summary_json": "{}",
            "served_features_json": "[]",
            "observe_outcome": "RECORDED",
            "store_position": arrival + 1,
            "store_epoch": None,
        }
    elif topic == IDENTITY_EVENTS_V1:
        # Silver identifies a gateway identity event by its envelope id; the online store, and so
        # Gold, by the observation id it carries as `correlation_id` (ADR-0055 §9).
        row = {
            **common,
            "event_id": event_id,
            "correlation_id": correlation_id or event_id,
            "silver_identity": event_id,
            "account_id": account,
            "identity_event_type": identity_type or IDENTITY_TYPE_OF_STREAM[event.stream],
            "device_id": _prefixed(prefix, event.device_id),
            "ip_id": _prefixed(prefix, event.ip_id),
            "user_agent": None,
        }
    else:
        assert event.authorization_outcome is not None
        row = {
            **common,
            "event_id": f"evt-outcome-{event_id}",
            "correlation_id": event_id,
            "silver_identity": event_id,
            "transaction_id": event_id,
            "account_id": account,
            "authorization_outcome": event.authorization_outcome.value,
            "transaction_occurred_at": event.occurred_at,
        }
    return topic, row


def write_silver(
    spark: Any, lake: LakeConfig, rows: Mapping[str, Sequence[Mapping[str, Any]]]
) -> None:
    """Create Gold's Silver sources from their declarations and append `rows` as one commit each."""
    for topic in GOLD_SILVER_TOPICS:
        create_silver_tables(spark, lake, topic, git_sha=SHA, dirty_worktree=False)
        topic_rows = rows.get(topic, ())
        if not topic_rows:
            continue
        schema = canonical_schema(topic)
        tuples = [tuple(row[f.name] for f in schema.fields) for row in topic_rows]
        frame = spark.createDataFrame(tuples, schema)
        frame.write.format("delta").mode("append").save(
            str(silver_topic(topic).table.local_path(lake))
        )


def rows_for_log(
    log: Sequence[Event], *, prefix: str = "", is_late: bool | None = False
) -> dict[str, list[dict[str, Any]]]:
    rows: dict[str, list[dict[str, Any]]] = {}
    for arrival, event in first_deliveries(log):
        topic, row = silver_row(event, arrival=arrival, prefix=prefix, is_late=is_late)
        rows.setdefault(topic, []).append(row)
    return rows


def unprefixed(rows: ContextRows, prefix: str) -> ContextRows:
    """The context rows with every prefixed id restored."""
    if not prefix:
        return rows

    def strip(value: str) -> str:
        assert value.startswith(prefix), (value, prefix)
        return value[len(prefix) :]

    profile = rows.profile
    if profile is not None:
        profile = dataclasses.replace(
            profile,
            account_id=strip(profile.account_id),
            habitual_merchants=frozenset(strip(m) for m in profile.habitual_merchants),
            known_devices=frozenset(strip(d) for d in profile.known_devices),
        )
    return ContextRows(
        transaction_id=strip(rows.transaction_id),
        as_of_ms=rows.as_of_ms,
        windows=tuple(dataclasses.replace(w, entity_id=strip(w.entity_id)) for w in rows.windows),
        profile=profile,
        previous=tuple(dataclasses.replace(p, entity_id=strip(p.entity_id)) for p in rows.previous),
    )


def comparable(rows: ContextRows) -> tuple[Any, ...]:
    """Context rows in a canonical order, for equality between builds."""
    return (
        rows.transaction_id,
        rows.as_of_ms,
        sorted(
            (w.entity.value, w.entity_id, w.stream.value, w.window_label, repr(w))
            for w in rows.windows
        ),
        repr(rows.profile),
        sorted(repr(p) for p in rows.previous),
    )
