"""Diagnostic probe (NOT acceptance evidence): how one Silver micro-batch's cost scales.

Two controlled variables, measured independently:

  A. canonical row count, with the file count held fixed
  B. canonical file count, with the row count held fixed

For each point: a fresh lake, a seeded canonical table, then ONE real Bronze batch of
`--batch-rows` rows driven through the real Silver query (availableNow), with the three
whole-table phases timed separately.

Usage:
  python silver_scaling_probe.py --mode rows  --points 0,250000,1000000,2000000
  python silver_scaling_probe.py --mode files --rows 1000000 --points 8,64,256,1024
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
SHA = "0" * 40
TOPIC_ID = "probe-topic-id"
NOW = dt.datetime(2026, 9, 20, 0, 0, tzinfo=dt.UTC)


def _heap(spark: Any) -> dict[str, int]:
    rt = spark.sparkContext._jvm.java.lang.Runtime.getRuntime()
    total, free, mx = int(rt.totalMemory()), int(rt.freeMemory()), int(rt.maxMemory())
    return {"heap_used_mb": (total - free) // 2**20, "heap_max_mb": mx // 2**20}


def _seed_canonical(spark: Any, lake: Any, rows: int, files: int) -> None:
    """`rows` synthetic canonical rows in `files` Delta commits, identities all distinct."""
    from pyspark.sql import functions as F  # noqa: N812

    from trace_core.stream.silver import canonical_schema, silver_topic

    if rows == 0:
        return
    schema = canonical_schema(TOPIC)
    path = str(silver_topic(TOPIC).table.local_path(lake))
    per = max(1, rows // files)
    big = F.concat_ws("", *[F.lit("x" * 64)] * 16)  # ~1 KiB, as the json columns are
    start = 0
    commit = 0
    while start < rows:
        count = min(per, rows - start)
        frame = spark.range(start, start + count).select(
            *[_column(field, big) for field in schema.fields]
        )
        frame.write.format("delta").mode("append").save(path)
        start += count
        commit += 1
    print(f"    seeded {rows} rows in {commit} commits", flush=True)


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


def _bronze_batch(spark: Any, lake: Any, rows: int, nonce: str) -> None:
    """One Bronze commit of `rows` real scored events, as a Bronze micro-batch makes it."""
    from benchmarks.stream_throughput.events import EventFactory, score_templates

    from trace_core.stream.bronze import bronze_schema, bronze_topic

    factory = EventFactory(
        templates=score_templates(8, 20260920),
        run_nonce=nonce,
        worker=0,
        account_pool=100_000,
        seed=20260920,
    )
    now_ms = int(NOW.timestamp() * 1000)
    tuples = []
    for offset in range(rows):
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
                1,
                "bronze-writer",
            )
        )
    path = str(bronze_topic(TOPIC).table.local_path(lake))
    spark.createDataFrame(tuples, bronze_schema()).write.format("delta").mode("append").save(path)


def _instrument() -> dict[str, list[float]]:
    """Time the three whole-table phases by wrapping the sink's module globals."""
    from trace_core.stream import silver

    timings: dict[str, list[float]] = {"classify": [], "write_batch": [], "assert_unique": []}
    originals = {
        "classify": silver.classify_frame,
        "write_batch": silver._write_batch,
        "assert_unique": silver._assert_unique,
    }

    def wrap(label: str, fn: Any) -> Any:
        def run(*args: Any, **kwargs: Any) -> Any:
            started = time.monotonic()
            try:
                return fn(*args, **kwargs)
            finally:
                timings[label].append(time.monotonic() - started)

        return run

    silver.classify_frame = wrap("classify", originals["classify"])
    silver._write_batch = wrap("write_batch", originals["write_batch"])
    silver._assert_unique = wrap("assert_unique", originals["assert_unique"])
    return timings


def _point(spark: Any, rows: int, files: int, batch_rows: int) -> dict[str, Any]:
    from trace_core.stream.bronze import Trigger, bronze_declaration, bronze_topic
    from trace_core.stream.lake import LakeConfig
    from trace_core.stream.silver import create_silver_tables, start_silver_query
    from trace_core.stream.tables import CommitProvenance, create_table

    root = Path(tempfile.mkdtemp(prefix="silver-probe-"))
    os.environ["TRACE_DELTA_ROOT"] = str(root)
    try:
        lake = LakeConfig.from_env(log=False)
        create_table(
            spark,
            bronze_declaration(TOPIC),
            lake,
            CommitProvenance(SHA, False, bronze_topic(TOPIC).query),
        )
        create_silver_tables(spark, lake, TOPIC, git_sha=SHA, dirty_worktree=False)
        _seed_canonical(spark, lake, rows, files)
        _bronze_batch(spark, lake, batch_rows, uuid.uuid4().hex[:8])
        timings = _instrument()
        started = time.monotonic()
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
        elapsed = time.monotonic() - started
        return {
            "seed_rows": rows,
            "seed_files": files,
            "batch_rows": batch_rows,
            "batch_s": round(elapsed, 2),
            "classify_s": round(sum(timings["classify"]), 2),
            "write_batch_s": round(sum(timings["write_batch"]), 2),
            "assert_unique_s": round(sum(timings["assert_unique"]), 2),
            **_heap(spark),
        }
    finally:
        shutil.rmtree(root, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["rows", "files"], required=True)
    parser.add_argument("--points", required=True)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--files", type=int, default=128)
    parser.add_argument("--batch-rows", type=int, default=7_000)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    os.environ.setdefault("TRACE_DELTA_ROOT", tempfile.mkdtemp(prefix="silver-probe-root-"))
    from trace_core.observability import configure_logging
    from trace_core.stream.session import build_session

    configure_logging()
    spark = build_session(
        "silver-scaling-probe", master="local[4]", shuffle_partitions=2, driver_memory="2g"
    )
    results = []
    try:
        for value in [int(p) for p in args.points.split(",")]:
            rows = value if args.mode == "rows" else args.rows
            files = args.files if args.mode == "rows" else value
            print(f"  point {args.mode}={value}", flush=True)
            result = _point(spark, rows, max(1, files), args.batch_rows)
            print("   ", json.dumps(result), flush=True)
            results.append(result)
    finally:
        spark.stop()
    Path(args.out).write_text(json.dumps(results, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
