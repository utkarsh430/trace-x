"""`P3.resource-bounds` end to end: a real broker, the Bronze query, Silver, the retention floor,
then more traffic read through the delete (ADR-0052 amendment 1).

Also carries, under this capability's name, the evidence that a Kafka byte cap which evicts unread
segments stops Bronze loudly (`failOnDataLoss=true`, PHASE3_PLAN §4.1 point 8): the Bronze suite's
trimmed-offsets test, which removes unread records the same way retention does, by advancing the
log start offset.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pytest
from tests.integration import test_bronze_kafka as bronze_suite
from tests.integration.test_bronze_kafka import (  # noqa: F401 -- `spark` is a fixture
    Headers,
    _fresh,
    _identity_for,
    _ingest,
    _one_account_per_partition,
    _publish,
    spark,
)
from tests.integration.test_kafka_platform import Broker, broker  # noqa: F401 -- a fixture

from trace_core.contracts.topics import IDENTITY_EVENTS_V1
from trace_core.stream.bronze import Trigger, fetch_topic_identity
from trace_core.stream.bronze_conservation import check_conservation
from trace_core.stream.lake import LakeConfig
from trace_core.stream.maintenance import RETENTION_AUDIT, advance_retention
from trace_core.stream.silver import start_silver_query
from trace_core.stream.silver_conservation import check_silver_conservation
from trace_core.stream.silver_rules import silver_topic

pytestmark = [pytest.mark.integration, pytest.mark.stream]

SHA = "7" * 40
TOPIC = IDENTITY_EVENTS_V1


def _silver(spark: Any, lake: LakeConfig) -> None:  # noqa: F811
    handle = start_silver_query(
        spark,
        lake,
        TOPIC,
        git_sha=SHA,
        dirty_worktree=False,
        now=dt.datetime.now(dt.UTC),
        trigger=Trigger(available_now=True),
    )
    handle.query.awaitTermination()
    assert handle.query.exception() is None, handle.query.exception()


def _events(start: int, count: int) -> list[tuple[str, dict[str, Any], Headers]]:
    accounts = _one_account_per_partition(TOPIC)
    return [
        (TOPIC, _identity_for(i, accounts[i % len(accounts)]), ())
        for i in range(start, start + count)
    ]


def test_resource_bounds_silver_reads_through_bronze_retention_against_a_real_broker(
    broker: Broker,  # noqa: F811
    spark: Any,  # noqa: F811
    tmp_path: Path,
) -> None:
    _fresh(broker, TOPIC)
    _publish(broker, _events(0, 12))
    lake = LakeConfig.at(tmp_path / "lake")
    _ingest(spark, lake, broker, TOPIC)
    _silver(spark, lake)
    topic_id = fetch_topic_identity(broker.bootstrap, TOPIC).topic_id

    report = advance_retention(
        spark,
        lake,
        TOPIC,
        requested_floors=None,
        broker_topic_id=topic_id,
        git_sha=SHA,
        dirty_worktree=False,
        now=dt.datetime.now(dt.UTC),
    )
    assert dict(report.floors_after) == {(topic_id, p): 4 for p in range(3)}, report.summary()
    assert report.rows_deleted == 12 and report.files_deleted >= 1

    _publish(broker, _events(12, 12))
    _ingest(spark, lake, broker, TOPIC)
    _silver(spark, lake)

    bronze = check_conservation(spark, lake, TOPIC, broker_topic_id=topic_id)
    assert bronze.conserved, bronze.summary()
    assert all((v.missing, v.retired_present, v.floor) == (0, 0, 4) for v in bronze.partitions)
    silver = check_silver_conservation(spark, lake, TOPIC)
    assert silver.conserved, silver.summary()
    assert silver.bronze_rows == 12
    canonical = spark.read.format("delta").load(str(silver_topic(TOPIC).table.local_path(lake)))
    assert canonical.count() == 24
    audit = spark.read.format("delta").load(str(RETENTION_AUDIT.local_path(lake))).collect()
    assert sorted(r["event"] for r in audit) == ["floor_advanced"] * 3 + ["rows_deleted"]


def test_resource_bounds_a_kafka_cap_that_evicts_unread_records_stops_bronze_loudly(
    broker: Broker,  # noqa: F811
    spark: Any,  # noqa: F811
    tmp_path: Path,
) -> None:
    bronze_suite.test_trimmed_unread_offsets_stop_bronze_loudly_and_trimmed_read_ones_do_not(
        broker, spark, tmp_path
    )
