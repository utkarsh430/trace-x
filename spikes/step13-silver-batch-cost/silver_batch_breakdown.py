"""Diagnostic probe (NOT acceptance evidence): where one Silver micro-batch spends its time.

`--batches` consecutive micro-batches, one Bronze file each, over a canonical table seeded to
`--seed-rows`. Separates cold-start cost (batch 0) from steady state (batches 1..n) and breaks
`_write_batch` into its own phases.

  python silver_batch_breakdown.py --seed-rows 0 --batches 8 --out b0.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

TOPIC = "tx.scored.v1"
EXTRA: tuple[tuple[str, str], ...] = ()
SHA = "0" * 40
TOPIC_ID = "probe-topic-id"
NOW = dt.datetime(2026, 9, 20, 0, 0, tzinfo=dt.UTC)


def _column(field: Any, big: Any) -> Any:
    from pyspark.sql import functions as F  # noqa: N812

    name, kind = field.name, field.dataType.simpleString()
    if name == "silver_identity":
        return F.concat(F.lit("seed:"), F.col("id").cast("string")).alias(name)
    if name == "content_digest":
        return F.sha2(F.col("id").cast("string"), 256).alias(name)
    if name == "kafka_offset":
        return (F.col("id") + F.lit(10_000_000)).alias(name)
    if name.endswith("_json"):
        return big.alias(name)
    if kind == "string":
        return F.concat(F.lit(name[:4]), F.col("id").cast("string")).alias(name)
    if kind == "timestamp":
        return F.timestamp_micros(F.lit(int(NOW.timestamp() * 1e6)) + F.col("id")).alias(name)
    if kind in {"bigint", "int"}:
        return F.col("id").cast(kind).alias(name)
    if kind == "double":
        return F.col("id").cast("double").alias(name)
    if kind == "boolean":
        return F.lit(False).alias(name)
    raise AssertionError(f"unhandled {name}: {kind}")


def _seed_canonical(spark: Any, lake: Any, rows: int, files: int) -> None:
    from pyspark.sql import functions as F  # noqa: N812

    from trace_core.stream.silver import canonical_schema, silver_topic

    if rows == 0:
        return
    schema = canonical_schema(TOPIC)
    path = str(silver_topic(TOPIC).table.local_path(lake))
    per = max(1, rows // files)
    big = F.concat_ws("", *[F.lit("x" * 64)] * 16)
    start = 0
    while start < rows:
        count = min(per, rows - start)
        spark.range(start, start + count).select(
            *[_column(field, big) for field in schema.fields]
        ).write.format("delta").mode("append").save(path)
        start += count


def _bronze_commits(spark: Any, lake: Any, batches: int, rows: int, nonce: str) -> None:
    """`batches` Bronze commits of `rows` real scored events, one file each."""
    from benchmarks.stream_throughput.events import EventFactory, score_templates

    from trace_core.stream.bronze import bronze_schema, bronze_topic

    factory = EventFactory(
        templates=score_templates(8, 20260920),
        run_nonce=nonce,
        worker=0,
        account_pool=100_000,
        seed=20260920,
    )
    path = str(bronze_topic(TOPIC).table.local_path(lake))
    now_ms = int(NOW.timestamp() * 1000)
    offset = 0
    for batch in range(batches):
        tuples = []
        for _ in range(rows):
            _key, value = factory.build(TOPIC, now_ms + offset)
            tuples.append(
                (
                    TOPIC,
                    TOPIC_ID,
                    offset % 6,
                    offset,
                    NOW,
                    1,
                    b"k",
                    value,
                    None,
                    "UNTRUSTED",
                    NOW,
                    batch,
                    "bronze-writer",
                )
            )
            offset += 1
        frame = spark.createDataFrame(tuples, bronze_schema()).repartition(1)
        frame.write.format("delta").mode("append").save(path)


def _instrument() -> dict[str, list[float]]:
    from trace_core.stream import checkpoints, silver

    timings: dict[str, list[float]] = {}

    def wrap(label: str, fn: Any) -> Any:
        timings.setdefault(label, [])

        def run(*args: Any, **kwargs: Any) -> Any:
            started = time.monotonic()
            try:
                return fn(*args, **kwargs)
            finally:
                timings[label].append(round(time.monotonic() - started, 3))

        return run

    silver.classify_frame = wrap("classify", silver.classify_frame)
    silver._write_batch = wrap("write_batch", silver._write_batch)
    silver._assert_unique = wrap("assert_unique", silver._assert_unique)
    silver.batch_merge_bounds = wrap("bounds", silver.batch_merge_bounds)
    silver._merge_late_events = wrap("late_events", silver._merge_late_events)
    checkpoints.OpenedCheckpoint.merge = wrap("ckpt_merge", checkpoints.OpenedCheckpoint.merge)
    checkpoints.OpenedCheckpoint.append = wrap("ckpt_append", checkpoints.OpenedCheckpoint.append)
    return timings


def _jobs(events: Path) -> dict[str, Any]:
    """Job and stage counts from the event log: how many Spark jobs one micro-batch costs."""
    jobs: list[dict[str, Any]] = []
    stages: dict[int, dict[str, Any]] = {}
    for path in events.rglob("*"):
        if not path.is_file():
            continue
        for line in path.read_text(errors="replace").splitlines():
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if not isinstance(record, dict):
                continue
            kind = record.get("Event", "")
            if kind == "SparkListenerJobStart":
                jobs.append({"id": record["Job ID"], "at": record["Submission Time"]})
            elif kind == "SparkListenerStageCompleted":
                info = record["Stage Info"]
                stages[info["Stage ID"]] = {
                    "tasks": info["Number of Tasks"],
                    "ms": int(info.get("Completion Time", 0)) - int(info.get("Submission Time", 0)),
                }
    total_ms = sum(s["ms"] for s in stages.values())
    return {
        "job_count": len(jobs),
        "stage_count": len(stages),
        "task_count": sum(s["tasks"] for s in stages.values()),
        "stage_ms_total": total_ms,
        "slowest_stages_ms": sorted((s["ms"] for s in stages.values()), reverse=True)[:10],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed-rows", type=int, default=0)
    parser.add_argument("--seed-files", type=int, default=128)
    parser.add_argument("--batches", type=int, default=8)
    parser.add_argument("--batch-rows", type=int, default=7_000)
    parser.add_argument("--out", required=True)
    parser.add_argument("--conf", action="append", default=[], help="k=v Spark settings")
    args = parser.parse_args()

    root = Path(tempfile.mkdtemp(prefix="silver-breakdown-"))
    os.environ["TRACE_DELTA_ROOT"] = str(root)
    from trace_core.observability import configure_logging
    from trace_core.stream.bronze import Trigger, bronze_declaration, bronze_topic
    from trace_core.stream.lake import LakeConfig
    from trace_core.stream.session import build_session
    from trace_core.stream.silver import create_silver_tables, start_silver_query
    from trace_core.stream.tables import CommitProvenance, create_table

    configure_logging()
    events = Path(tempfile.mkdtemp(prefix="silver-breakdown-events-"))
    spark = build_session(
        "silver-batch-breakdown",
        master="local[4]",
        shuffle_partitions=2,
        driver_memory="2g",
        extra_conf={
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": events.as_uri(),
            **dict(c.split("=", 1) for c in args.conf),
        },
    )
    try:
        lake = LakeConfig.from_env(log=False)
        create_table(
            spark,
            bronze_declaration(TOPIC),
            lake,
            CommitProvenance(SHA, False, bronze_topic(TOPIC).query),
        )
        create_silver_tables(spark, lake, TOPIC, git_sha=SHA, dirty_worktree=False)
        print("  seeding", flush=True)
        _seed_canonical(spark, lake, args.seed_rows, args.seed_files)
        print("  bronze", flush=True)
        _bronze_commits(spark, lake, args.batches, args.batch_rows, uuid.uuid4().hex[:8])
        timings = _instrument()
        print("  silver", flush=True)
        started = time.monotonic()
        handle = start_silver_query(
            spark,
            lake,
            TOPIC,
            git_sha=SHA,
            dirty_worktree=False,
            now=dt.datetime.now(dt.UTC),
            trigger=Trigger(available_now=True),
            max_files_per_trigger=1,
        )
        handle.query.awaitTermination()
        assert handle.query.exception() is None, handle.query.exception()
        elapsed = time.monotonic() - started
        progress = [
            {
                "batch_id": p["batchId"],
                "rows": p["numInputRows"],
                "duration_ms": p["batchDuration"],
            }
            for p in handle.query.recentProgress
        ]
        result = {
            "seed_rows": args.seed_rows,
            "batches": args.batches,
            "batch_rows": args.batch_rows,
            "total_s": round(elapsed, 2),
            "phases": timings,
            "progress": progress,
        }
        Path(args.out).write_text(json.dumps(result, indent=2) + "\n")
        try:
            result["jobs"] = _jobs(events)
        except Exception as exc:  # diagnostics only; the phase timings are already written
            result["jobs"] = {"error": f"{type(exc).__name__}: {exc}"}
        print(json.dumps(result, indent=2), flush=True)
        Path(args.out).write_text(json.dumps(result, indent=2) + "\n")
    finally:
        spark.stop()
        shutil.rmtree(root, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
