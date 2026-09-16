"""Bronze, Silver and a Gold build on real Spark and Delta for a parity run, and what parity reads.

Each stage runs the production entrypoints the stream services run (`start_bronze_query`,
`start_silver_query`, `build_gold`), available-now, one topic at a time. What Spark wrote is counted
by Spark afterwards; that count, and the Gold build record, are the run's evidence that Spark
executed (`guard.SparkEvidence`).
"""

from __future__ import annotations

import datetime as dt
import importlib.metadata
from collections.abc import Mapping
from typing import Any, Final

from eval.parity.guard import SparkEvidence
from eval.parity.history import (
    IDENTITY_COLUMNS,
    OCCURRED_US,
    OUTCOME_COLUMNS,
    TRANSACTION_COLUMNS,
    identity_event,
    outcome_event,
    transaction_event,
)
from eval.parity.served import ServedScore, parse_scored

from trace_core.contracts.topics import IDENTITY_EVENTS_V1, TX_AUTHORIZATION_V1, TX_SCORED_V1
from trace_core.features.observation import Event
from trace_core.stream.gold import BuildRecord
from trace_core.stream.gold_plan import ContextRows
from trace_core.stream.lake import LakeConfig

LAKE_TOPICS: Final = (TX_SCORED_V1, IDENTITY_EVENTS_V1, TX_AUTHORIZATION_V1)


class MedallionError(RuntimeError):
    """A Bronze or Silver query failed, or Gold could not build."""


def _finish(handle: Any, stage: str, topic: str) -> None:
    handle.query.awaitTermination()
    error = handle.query.exception()
    if error is not None:
        raise MedallionError(f"{stage} {topic} failed: {error}")


def _rows(spark: Any, path: Any, version: int | None = None) -> Any:
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", str(version))
    return reader.load(str(path))


def run_medallion(
    spark: Any, lake: LakeConfig, *, bootstrap: str, git_sha: str, dirty_worktree: bool
) -> tuple[BuildRecord, SparkEvidence]:
    from trace_core.stream.bronze import Trigger, bronze_topic, start_bronze_query
    from trace_core.stream.gold import build_gold
    from trace_core.stream.silver import start_silver_query
    from trace_core.stream.silver_rules import silver_topic

    for topic in LAKE_TOPICS:
        handle = start_bronze_query(
            spark,
            lake,
            topic,
            bootstrap_servers=bootstrap,
            git_sha=git_sha,
            dirty_worktree=dirty_worktree,
            now=dt.datetime.now(dt.UTC),
            trigger=Trigger(available_now=True),
        )
        _finish(handle, "bronze", topic)
    for topic in LAKE_TOPICS:
        silver = start_silver_query(
            spark,
            lake,
            topic,
            git_sha=git_sha,
            dirty_worktree=dirty_worktree,
            now=dt.datetime.now(dt.UTC),
            trigger=Trigger(available_now=True),
        )
        _finish(silver, "silver", topic)
    result = build_gold(
        spark, lake, git_sha=git_sha, dirty_worktree=dirty_worktree, now=dt.datetime.now(dt.UTC)
    )
    evidence = SparkEvidence(
        spark_version=str(spark.version),
        bronze_rows={
            t: int(_rows(spark, bronze_topic(t).table.local_path(lake)).count())
            for t in LAKE_TOPICS
        },
        silver_rows={
            t: int(_rows(spark, silver_topic(t).table.local_path(lake)).count())
            for t in LAKE_TOPICS
        },
        gold_build_id=result.record.build_id,
        gold_rows={t.table: t.rows for t in result.record.targets},
    )
    return result.record, evidence


def served_deliveries(spark: Any, lake: LakeConfig) -> list[ServedScore]:
    """Every `tx.scored.v1` delivery Bronze holds, redeliveries included."""
    from trace_core.stream.bronze import bronze_topic

    frame = _rows(spark, bronze_topic(TX_SCORED_V1).table.local_path(lake)).select(
        "kafka_value", "kafka_headers"
    )
    scores: list[ServedScore] = []
    for row in frame.toLocalIterator():
        headers = [
            (str(h["key"]), None if h["value"] is None else bytes(h["value"]))
            for h in row["kafka_headers"] or []
        ]
        scores.append(parse_scored(bytes(row["kafka_value"]), headers))
    return scores


def complete_history(spark: Any, lake: LakeConfig, record: BuildRecord) -> list[Event]:
    """The observations of Silver's canonical tables at exactly the versions the Gold build read."""
    from trace_core.stream.silver_rules import silver_topic

    pins: Mapping[str, int] = {pin.table: pin.version for pin in record.sources}

    def table(topic: str, columns: tuple[str, ...]) -> list[dict[str, Any]]:
        ref = silver_topic(topic).table
        frame = _rows(spark, ref.local_path(lake), pins[str(ref)])
        selected = frame.selectExpr(*columns, f"unix_micros(occurred_at) AS {OCCURRED_US}")
        return [row.asDict() for row in selected.toLocalIterator()]

    events: list[Event] = [transaction_event(r) for r in table(TX_SCORED_V1, TRANSACTION_COLUMNS)]
    for row in table(IDENTITY_EVENTS_V1, IDENTITY_COLUMNS):
        if (event := identity_event(row)) is not None:
            events.append(event)
    for row in table(TX_AUTHORIZATION_V1, OUTCOME_COLUMNS):
        if (event := outcome_event(row)) is not None:
            events.append(event)
    return events


def gold_context_rows(spark: Any, lake: LakeConfig, record: BuildRecord) -> dict[str, ContextRows]:
    from trace_core.stream.gold import read_context_rows

    return read_context_rows(spark, lake, record)


def toolchain(spark: Any) -> dict[str, str]:
    jvm = spark.sparkContext._jvm
    return {
        "spark": str(spark.version),
        "delta": importlib.metadata.version("delta-spark"),
        "hadoop": str(jvm.org.apache.hadoop.util.VersionInfo.getVersion()),
        "scala": str(jvm.scala.util.Properties.versionNumberString()),
        "java": str(jvm.java.lang.System.getProperty("java.version")),
    }
