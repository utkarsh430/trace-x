"""Silver end to end: a real broker, then Bronze, then Silver (ADR-0053; `P3.event-time`).

Events go to a throwaway broker through the producer factory, and one poison value through a raw
client. The Bronze query ingests them, and the Silver query reads Bronze. Beyond the Delta-only
stream tests, this proves:
- the broker's LogAppendTime drives lateness;
- a producer retry of an outcome carries a new envelope `event_id` and still collapses;
- out-of-order and late events are admitted, and only the late one is tagged and copied;
- a backfill-headered event is admitted with `is_late` null;
- future skew, an identity conflict and a poison value are quarantined;
- conservation holds from Bronze into Silver for both topics.
"""

from __future__ import annotations

import datetime as dt
import uuid
from pathlib import Path
from typing import Any

import pytest
from tests.integration.test_bronze_kafka import (  # noqa: F401 -- `spark` is a fixture
    _fresh,
    _ingest,
    _produce_raw,
    _publish,
    spark,
)
from tests.integration.test_kafka_platform import Broker, broker  # noqa: F401 -- a fixture

from trace_core.contracts import authorization
from trace_core.contracts.envelope import build_event
from trace_core.contracts.topics import (
    DEVICE_EVENTS_V1,
    IDENTITY_EVENTS_V1,
    INVESTIGATION_REQUESTED_V1,
    TX_AUTHORIZATION_V1,
    TX_RAW_V1,
)
from trace_core.domain.time import event_time
from trace_core.stream import silver_rules as rules
from trace_core.stream.bronze import Trigger
from trace_core.stream.lake import LakeConfig
from trace_core.stream.silver import start_silver_query
from trace_core.stream.silver_conservation import check_silver_conservation
from trace_core.stream.timing import BACKFILL_MODE, REPLAY_MODE_HEADER

pytestmark = [pytest.mark.integration, pytest.mark.stream]

SHA = "5" * 40


def _outcome(transaction: int, base_ms: int, outcome: str = "DECLINED") -> dict[str, Any]:
    """Same transaction and times, same content; a new envelope on every call."""
    return authorization.build_event(
        transaction_id=f"tx_{transaction:012d}",
        account_id=f"acct_{transaction:06d}",
        authorization_outcome=outcome,
        decided_ms=base_ms - 1_000,
        transaction_occurred_ms=base_ms - 2_000,
        transaction_occurred_at=authorization.iso_millis(base_ms - 2_000),
        producer="trace-gateway@0.1.0",
        trace_id=uuid.uuid4().hex,
        correlation_id=f"tx_{transaction:012d}",
        ingested_ms=base_ms - 900,
    )


def _identity(occurred: dt.datetime) -> dict[str, Any]:
    return build_event(
        event_type="identity.events",
        occurred_at=event_time(occurred),
        payload={"account_id": "acct_000001", "identity_event_type": "PASSWORD_CHANGE"},
        producer="trace-generator@1.0.0",
        trace_id=uuid.uuid4().hex,
        correlation_id="idev_" + uuid.uuid4().hex,
    )


def _silver(spark: Any, lake: LakeConfig, topic: str) -> None:  # noqa: F811
    handle = start_silver_query(
        spark,
        lake,
        topic,
        git_sha=SHA,
        dirty_worktree=False,
        now=dt.datetime.now(dt.UTC),
        trigger=Trigger(available_now=True),
    )
    handle.query.awaitTermination()
    assert handle.query.exception() is None, handle.query.exception()


def _rows(spark: Any, lake: LakeConfig, ref: Any, topic: str | None = None) -> list[Any]:  # noqa: F811
    frame = spark.read.format("delta").load(str(ref.local_path(lake)))
    if topic is not None:
        frame = frame.filter(frame.silver_topic == topic)
    return frame.collect()


def test_silver_admits_each_event_once_and_accounts_for_every_other_record(
    broker: Broker,  # noqa: F811
    spark: Any,  # noqa: F811
    tmp_path: Path,
) -> None:
    _fresh(broker, TX_AUTHORIZATION_V1, IDENTITY_EVENTS_V1)
    now = dt.datetime.now(dt.UTC)
    base_ms = int(now.timestamp() * 1000)
    first, retry = _outcome(1, base_ms), _outcome(1, base_ms)
    assert first["envelope"]["event_id"] != retry["envelope"]["event_id"]
    on_time = _identity(now)
    out_of_order = _identity(now - dt.timedelta(seconds=30))
    late = _identity(now - dt.timedelta(minutes=20))
    backfill = _identity(now - dt.timedelta(days=30))
    skewed = _identity(now + dt.timedelta(days=2))
    _publish(
        broker,
        [
            (TX_AUTHORIZATION_V1, first, ()),
            (TX_AUTHORIZATION_V1, retry, ()),
            (TX_AUTHORIZATION_V1, _outcome(1, base_ms, "APPROVED"), ()),
            (TX_AUTHORIZATION_V1, _outcome(2, base_ms), ()),
            (IDENTITY_EVENTS_V1, on_time, ()),
            (IDENTITY_EVENTS_V1, out_of_order, ()),
            (IDENTITY_EVENTS_V1, late, ()),
            (IDENTITY_EVENTS_V1, backfill, ((REPLAY_MODE_HEADER, BACKFILL_MODE),)),
            (IDENTITY_EVENTS_V1, skewed, ()),
        ],
    )
    _produce_raw(broker, TX_AUTHORIZATION_V1, [(b"acct_000009", b"not json", ())])

    lake = LakeConfig.at(tmp_path / "lake")
    for topic in (TX_AUTHORIZATION_V1, IDENTITY_EVENTS_V1):
        _ingest(spark, lake, broker, topic)
        _silver(spark, lake, topic)

    outcomes = rules.silver_topic(TX_AUTHORIZATION_V1)
    canonical = {str(r["silver_identity"]): r for r in _rows(spark, lake, outcomes.table)}
    assert set(canonical) == {"tx_000000000001", "tx_000000000002"}
    kept = canonical["tx_000000000001"]
    assert kept["event_id"] == first["envelope"]["event_id"], "the first delivery is the row kept"
    assert kept["authorization_outcome"] == "DECLINED"
    duplicates = _rows(spark, lake, rules.DUPLICATES, TX_AUTHORIZATION_V1)
    assert [str(r["silver_identity"]) for r in duplicates] == ["tx_000000000001"]
    reasons = sorted(
        str(r["reason"]) for r in _rows(spark, lake, rules.QUARANTINE, TX_AUTHORIZATION_V1)
    )
    assert reasons == ["identity_conflict", "invalid_event"]

    identities = rules.silver_topic(IDENTITY_EVENTS_V1)
    admitted = {str(r["event_id"]): r for r in _rows(spark, lake, identities.table)}
    ids = {name: event["envelope"]["event_id"] for name, event in (
        ("on_time", on_time), ("out_of_order", out_of_order), ("late", late),
        ("backfill", backfill), ("skewed", skewed),
    )}  # fmt: skip
    assert set(admitted) == {ids["on_time"], ids["out_of_order"], ids["late"], ids["backfill"]}
    assert admitted[ids["on_time"]]["is_late"] is False
    assert admitted[ids["out_of_order"]]["is_late"] is False, "out of order is not late"
    assert admitted[ids["late"]]["is_late"] is True
    assert admitted[ids["late"]]["arrival_delay_ms"] > 600_000
    assert admitted[ids["backfill"]]["is_late"] is None
    assert admitted[ids["backfill"]]["is_backfill"] is True
    late_rows = _rows(spark, lake, rules.LATE_EVENTS, IDENTITY_EVENTS_V1)
    assert [str(r["silver_identity"]) for r in late_rows] == [ids["late"]]
    quarantined = _rows(spark, lake, rules.QUARANTINE, IDENTITY_EVENTS_V1)
    assert [(str(r["reason"]), str(r["silver_identity"])) for r in quarantined] == [
        ("future_skew", ids["skewed"])
    ]

    expected = {TX_AUTHORIZATION_V1: (5, 2, 1, 2), IDENTITY_EVENTS_V1: (5, 4, 0, 1)}
    for topic, counts in expected.items():
        report = check_silver_conservation(spark, lake, topic)
        assert report.conserved, report.summary()
        assert (
            report.bronze_rows,
            report.canonical,
            report.duplicates,
            report.quarantined,
        ) == counts


def _raw(occurred: dt.datetime, transaction: str) -> dict[str, Any]:
    return build_event(
        event_type="tx.raw",
        occurred_at=event_time(occurred),
        payload={
            "transaction_id": transaction,
            "account_id": "acct_000000001",
            "card_id": "card_000000001",
            "device_id": "dev_000000001",
            "merchant_id": "mrch_000001",
            "ip_id": "ip_0000001",
            "amount_minor": 501,
            "currency": "EUR",
            "channel": "CARD_PRESENT",
            "entry_mode": "CHIP",
            "merchant_mcc": "5812",
            "merchant_country": "DE",
            "latitude": 52.52,
            "longitude": 13.4,
            "authorization_outcome": "APPROVED",
        },
        producer="trace-generator@1.0.0",
        trace_id=uuid.uuid4().hex,
        correlation_id=transaction,
    )


def _device(occurred: dt.datetime, device: str) -> dict[str, Any]:
    return build_event(
        event_type="device.event",
        occurred_at=event_time(occurred),
        payload={
            "device_id": device,
            "account_id": "acct_000000001",
            "device_event_type": "FIRST_SEEN",
            "platform": "android",
        },
        producer="trace-generator@1.0.0",
        trace_id=uuid.uuid4().hex,
        correlation_id="dev_" + uuid.uuid4().hex,
    )


def _investigation(occurred: dt.datetime, case: int) -> dict[str, Any]:
    return build_event(
        event_type="investigation.requested",
        occurred_at=event_time(occurred),
        payload={
            "case_id": f"case_{case:032x}",
            "transaction_id": "tx_it_0000000001",
            "account_id": "acct_000000001",
            "risk_band": "CRITICAL",
            "score": 0.91,
            "rule_pack_id": "core",
            "rule_pack_digest": "sha256:" + "c" * 64,
            "threshold_config_digest": "sha256:" + "d" * 64,
            "feature_set_version": "1.0.0",
            "feature_source": "ONLINE_ONLY",
            "degraded": False,
            "fired_rule_ids": ["R002_amount"],
        },
        producer="trace-gateway@0.1.0",
        trace_id=uuid.uuid4().hex,
        correlation_id=f"case_{case:032x}",
    )


def test_the_three_topics_with_no_runtime_producer_reach_silver_across_a_real_broker(
    broker: Broker,  # noqa: F811
    spark: Any,  # noqa: F811
    tmp_path: Path,
) -> None:
    """D23: `tx.raw.v1`, `device.events.v1` and `investigation.requested.v1` had been produced
    into Silver only from hand-written Bronze rows, so a Kafka-to-Silver defect specific to one of
    them would not have been caught. Phase 3 has no runtime producer for them, so the events are
    published by the generator's own factory.

    Each topic's declared identity is exercised on the path that would break:
    `payload.transaction_id` for `tx.raw.v1`, `envelope.event_id` for `device.events.v1`, and
    `envelope.idempotency_key` -- a content hash, so a fresh envelope over the same content
    collapses -- for `investigation.requested.v1`.
    """
    _fresh(broker, TX_RAW_V1, DEVICE_EVENTS_V1, INVESTIGATION_REQUESTED_V1)
    now = dt.datetime.now(dt.UTC)
    raw = _raw(now, "tx_it_0000000001")
    raw_retry = _raw(now, "tx_it_0000000001")
    assert raw["envelope"]["event_id"] != raw_retry["envelope"]["event_id"]
    device = _device(now, "dev_000000001")
    device_late = _device(now - dt.timedelta(minutes=20), "dev_000000002")
    investigation = _investigation(now, 1)
    investigation_retry = _investigation(now, 1)
    assert (
        investigation["envelope"]["idempotency_key"]
        == investigation_retry["envelope"]["idempotency_key"]
    ), "the idempotency key is a content hash, so the retry carries the same identity"
    _publish(
        broker,
        [
            (TX_RAW_V1, raw, ()),
            (TX_RAW_V1, raw_retry, ()),
            (DEVICE_EVENTS_V1, device, ()),
            (DEVICE_EVENTS_V1, device, ()),
            (DEVICE_EVENTS_V1, device_late, ()),
            (INVESTIGATION_REQUESTED_V1, investigation, ()),
            (INVESTIGATION_REQUESTED_V1, investigation_retry, ()),
        ],
    )
    _produce_raw(broker, TX_RAW_V1, [(b"acct_000000001", b"{not json", ())])

    lake = LakeConfig.at(tmp_path / "lake")
    for topic in (TX_RAW_V1, DEVICE_EVENTS_V1, INVESTIGATION_REQUESTED_V1):
        _ingest(spark, lake, broker, topic)
        _silver(spark, lake, topic)

    raws = _rows(spark, lake, rules.silver_topic(TX_RAW_V1).table)
    assert [str(r["silver_identity"]) for r in raws] == ["tx_it_0000000001"]
    assert raws[0]["event_id"] == raw["envelope"]["event_id"], "the first delivery is kept"
    assert raws[0]["amount_minor"] == 501, "the payload crossed the broker intact"
    assert [str(r["reason"]) for r in _rows(spark, lake, rules.QUARANTINE, TX_RAW_V1)] == [
        "invalid_event"
    ]

    device_rows = _rows(spark, lake, rules.silver_topic(DEVICE_EVENTS_V1).table)
    devices = {str(row["silver_identity"]): row for row in device_rows}
    assert set(devices) == {
        device["envelope"]["event_id"],
        device_late["envelope"]["event_id"],
    }
    assert devices[device["envelope"]["event_id"]]["is_late"] is False
    assert devices[device_late["envelope"]["event_id"]]["is_late"] is True
    assert [
        str(r["silver_identity"]) for r in _rows(spark, lake, rules.LATE_EVENTS, DEVICE_EVENTS_V1)
    ] == [device_late["envelope"]["event_id"]]

    cases = _rows(spark, lake, rules.silver_topic(INVESTIGATION_REQUESTED_V1).table)
    assert [str(r["silver_identity"]) for r in cases] == [
        investigation["envelope"]["idempotency_key"]
    ]
    assert cases[0]["case_id"] == f"case_{1:032x}"

    expected = {
        TX_RAW_V1: (3, 1, 1, 1),
        DEVICE_EVENTS_V1: (3, 2, 1, 0),
        INVESTIGATION_REQUESTED_V1: (2, 1, 1, 0),
    }
    for topic, counts in expected.items():
        report = check_silver_conservation(spark, lake, topic)
        assert report.conserved, report.summary()
        assert (
            report.bronze_rows,
            report.canonical,
            report.duplicates,
            report.quarantined,
        ) == counts
