"""Run the Delta layout benchmark (ADR-0015, `P3.layout-benchmark`): `make bench-layout`.

    make bench-layout                                  # 10,000,000 rows, 5 repetitions
    make bench-layout ARGS="--rows 200000 --reps 2"    # smoke: a run that is never published

A run, in order (every step refuses rather than records a wrong or partial number):

1. **Stage.** The generator's rows (`generator.py`) are appended in bounded batches to a staging
   Delta table, created from DDL so every declared NOT NULL holds. Its row count, key uniqueness
   and content digest are checked.
2. **Build.** Each layout variant (`spec.VARIANTS`) is created empty from DDL and filled by the
   MERGE Gold's writer issues (`gold.replace_table`: update what changed, insert, delete by
   source), so each table has the file structure Gold's own writer would give it; then the
   variant's `OPTIMIZE`, if any. Files, sizes, protocol and timings are recorded, and every
   variant's content digest must equal the staging table's.
3. **Correctness pass.** Every (variant, shape, repetition) is executed once and its row count and
   order-independent content digest recorded. Any difference between layouts, or from what the
   generator guarantees, stops the run (`LayoutChangedResultsError`). This pass is also the warm-up.
4. **Measured pass.** Every execution again, through `tables.measure_scans` (ADR-0048 §8: files
   and bytes selected from the executed plan's scan metrics, bytes and records read from Spark
   task metrics; a missing metric is refused, never zero), variants interleaved in an order that
   rotates each repetition.
5. **Rank** by the rule declared in `spec.RANKING_RULE`, and write the record, the results and the
   report. A run of at least ten million rows writes `eval/manifest/<run_id>.json`,
   `benchmarks/delta_layout/results/<run_id>.json` and `benchmarks/delta_layout/REPORT.md`; a
   smaller (smoke) run writes all three under its own lake directory and nothing into the
   repository.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import functools
import hashlib
import importlib
import importlib.metadata
import json
import os
import platform
import shutil
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final
from urllib.parse import unquote, urlparse

ROOT: Final = Path(__file__).resolve().parents[2]
# As the other benchmarks do: this checkout's trace_core and data, never another installed one.
for _path in (ROOT / "packages", ROOT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from benchmarks.delta_layout import generator, report  # noqa: E402
from benchmarks.delta_layout.generator import (  # noqa: E402
    COLUMNS,
    GeneratorSpec,
    Vocabulary,
    batches,
    split,
    timestamp,
)
from benchmarks.delta_layout.spec import (  # noqa: E402
    CONTROL,
    MIN_PUBLISHABLE_ROWS,
    RANKING_RULE,
    SHAPES,
    SUBJECT,
    TABLE_KEY,
    VARIANTS,
    BenchmarkRefusedError,
    QueryParams,
    QueryRun,
    ResultCheck,
    Variant,
    create_table_sql,
    extract_scan_metrics,
    missing_manifest_fields,
    optimize_sql,
    parameters,
    publishable,
    rank,
    require_complete_runs,
    require_identical_results,
    shape_leaders,
    summarize,
    vocabulary,
)
from data.generator.record import env_lock_digest, git_commit_sha, is_dirty  # noqa: E402

from trace_core.observability import get_logger  # noqa: E402

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

_log = get_logger("benchmarks.delta_layout")

MANIFEST_DIR: Final = ROOT / "eval" / "manifest"
BENCH_DIR: Final = Path(__file__).resolve().parent
RESULTS_DIR: Final = BENCH_DIR / "results"
REPORT_PATH: Final = BENCH_DIR / "REPORT.md"
DEFAULT_LAKE: Final = ROOT / "data" / "bench" / "delta_layout"
STAGING: Final = Variant("staging", "the generated rows, appended in bounded batches")

WARM_COLD_POLICY: Final = (
    "The page cache is not flushed (that needs root), so every measurement is warm-cache. Each "
    "(variant, shape, repetition) is executed once, unmeasured, by the correctness pass before "
    "any timing; the measured pass interleaves the variants, rotating their order each "
    "repetition. Files and bytes selected and bytes read are cache-independent; wall time is "
    "not, and understates cold I/O. Each execution resolves its table at the version the build "
    "recorded, as Gold readers do (gold._at), inside the timed region."
)

RECORDED_CONF: Final = (
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "spark.sql.files.maxPartitionBytes",
    "spark.sql.files.openCostInBytes",
    "spark.sql.parquet.compression.codec",
    "spark.sql.parquet.filterPushdown",
    "spark.sql.shuffle.partitions",
    "spark.databricks.delta.optimize.maxFileSize",
    "spark.databricks.delta.optimize.minFileSize",
    "spark.databricks.delta.merge.repartitionBeforeWrite.enabled",
    "spark.databricks.delta.stats.skipping",
    "spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols",
)
"""ADR-0048 correction 5: what clustering, file sizes and row-group skipping depend on."""


# ------------------------------------------------------------------- helpers ---


def _conf(spark: SparkSession) -> dict[str, str]:
    """Each recorded setting as the session resolves it; an unresolvable one says so."""
    out: dict[str, str] = {}
    for key in RECORDED_CONF:
        try:
            out[key] = str(spark.conf.get(key))
        except Exception as exc:  # py4j/Spark raise their own types for an unknown key
            out[key] = f"<unset: {type(exc).__name__}>"
    context: Any = spark.sparkContext
    block = context._jsc.hadoopConfiguration().get("parquet.block.size")
    if block is None:
        try:
            default = context._jvm.org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE
            block = f"<unset: parquet default {int(default)}>"
        except Exception as exc:  # the field is read through py4j; any failure is recorded
            block = f"<unset: default unreadable, {type(exc).__name__}>"
    out["parquet.block.size"] = str(block)
    return out


def _toolchain(spark: SparkSession) -> dict[str, str]:
    from trace_core.stream import toolchain
    from trace_core.stream.session import class_locations, running_versions

    versions = running_versions(spark)
    delta_jar = next(
        (j for j in toolchain.load_lock() if j.artifact.startswith("delta-spark")), None
    )
    return {
        **versions,
        "delta": importlib.metadata.version("delta-spark"),
        "delta_jar": ""
        if delta_jar is None
        else f"{delta_jar.coordinate} sha256:{delta_jar.sha256}",
        "delta_extension_loaded_from": class_locations(spark).get(
            "io.delta.sql.DeltaSparkSessionExtension", ""
        ),
        "pyspark": importlib.metadata.version("pyspark"),
        "python": platform.python_version(),
    }


def _machine(lake: Path) -> dict[str, Any]:
    try:
        memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError):
        memory = -1
    return {
        "platform": platform.platform(),
        "processor": platform.processor() or platform.machine(),
        "cpu_count": os.cpu_count(),
        "memory_bytes": memory,
        "lake_disk_free_bytes_at_start": shutil.disk_usage(lake).free,
    }


def _digest_of(frame: DataFrame, columns: Sequence[str]) -> tuple[int, str]:
    """Row count and an order-independent content digest: the exact sum of each row's xxhash64
    over `columns` (DECIMAL(38,0), so ANSI mode fails rather than wraps on overflow)."""
    from pyspark.sql import functions as F  # noqa: N812

    row = frame.select(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.xxhash64(*[F.col(c) for c in columns]).cast("decimal(38,0)")).alias("h"),
    ).collect()[0]
    rows = int(row["n"])
    return rows, f"{rows}:{row['h']}"


def _fields() -> list[tuple[str, str, bool]]:
    from trace_core.stream.gold_features import observations_schema

    schema = observations_schema()
    names = tuple(f.name for f in schema.fields)
    if names != COLUMNS:
        raise BenchmarkRefusedError(
            f"gold.observations is now {names}; the generator emits {COLUMNS}"
        )
    return [(f.name, f.dataType.simpleString(), f.nullable) for f in schema.fields]


def _local_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme not in ("", "file"):
        raise BenchmarkRefusedError(f"a benchmark table file is not local: {uri}")
    return Path(unquote(parsed.path))


# ------------------------------------------------------------------- staging ---


def stage(
    spark: SparkSession,
    spec: GeneratorSpec,
    vocab: Vocabulary,
    path: Path,
    fields: Sequence[tuple[str, str, bool]],
    batch_rows: int,
) -> dict[str, Any]:
    """Append the generated rows in bounded batches; check count, key uniqueness and digest."""
    from pyspark.sql import functions as F  # noqa: N812

    from trace_core.stream.gold_features import observations_schema

    # Spark pickles the generator into its Python workers by value, so they need nothing on
    # their path but the standard library.
    cloudpickle: Any = importlib.import_module("pyspark.cloudpickle")
    cloudpickle.register_pickle_by_value(generator)

    spark.sql(create_table_sql(STAGING, str(path), fields))
    context: Any = spark.sparkContext
    parallelism = max(1, int(context.defaultParallelism)) * 2
    schema = observations_schema()
    produce = functools.partial(generator.rows_for_slice, spec, vocab)
    started = time.perf_counter()
    plan = batches(spec.rows, batch_rows)
    for index, (lo, hi) in enumerate(plan):
        slices = split(lo, hi, parallelism)
        rdd = context.parallelize(slices, len(slices)).flatMap(produce)
        spark.createDataFrame(rdd, schema, verifySchema=True).write.format("delta").mode(
            "append"
        ).save(str(path))
        _log.info("bench_layout_staged_batch", batch=index + 1, of=len(plan), rows=hi - lo)
    write_s = time.perf_counter() - started

    table = spark.read.format("delta").load(str(path))
    rows, digest = _digest_of(table, COLUMNS)
    distinct = int(table.select(F.countDistinct(*TABLE_KEY).alias("k")).collect()[0]["k"])
    if rows != spec.rows or distinct != spec.rows:
        raise BenchmarkRefusedError(
            f"staging holds {rows} rows with {distinct} distinct keys; the generator made "
            f"{spec.rows} unique rows"
        )
    return {"rows": rows, "batches": len(plan), "write_s": write_s, "digest": digest}


# --------------------------------------------------------------------- build ---


def _merge(spark: SparkSession, source: DataFrame, path: Path) -> None:
    """The statement `gold.replace_table` builds: the target becomes exactly `source`, by key."""
    from delta.tables import DeltaTable

    values = [c for c in source.columns if c not in TABLE_KEY]
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in TABLE_KEY)
    changed = " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in values)
    target: Any = DeltaTable.forPath(spark, str(path))
    (
        target.alias("t")
        .merge(source.alias("s"), on)
        .whenMatchedUpdateAll(condition=changed)
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )


def build(
    spark: SparkSession,
    variant: Variant,
    staging: Path,
    path: Path,
    fields: Sequence[tuple[str, str, bool]],
    staging_digest: str,
) -> dict[str, Any]:
    from pyspark.sql import functions as F  # noqa: N812

    spark.sql(create_table_sql(variant, str(path), fields))
    source = spark.read.format("delta").load(str(staging))
    for column in variant.generated:
        source = source.withColumn(column.name, F.expr(column.expression))
    started = time.perf_counter()
    _merge(spark, source, path)
    write_s = time.perf_counter() - started

    optimize_s: float | None = None
    optimize_metrics: dict[str, Any] = {}
    if statement := optimize_sql(variant, str(path)):
        started = time.perf_counter()
        result = spark.sql(statement).collect()
        optimize_s = time.perf_counter() - started
        if result and "metrics" in result[0].asDict():
            optimize_metrics = result[0]["metrics"].asDict(recursive=True)

    detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0].asDict(recursive=True)
    table = spark.read.format("delta").load(str(path))
    sizes = sorted(_local_path(uri).stat().st_size for uri in table.inputFiles())
    if len(sizes) != int(detail["numFiles"]) or sum(sizes) != int(detail["sizeInBytes"]):
        raise BenchmarkRefusedError(
            f"{variant.name}: the snapshot lists {len(sizes)} files of {sum(sizes)} bytes, "
            f"DESCRIBE DETAIL {detail['numFiles']} of {detail['sizeInBytes']}"
        )
    rows, digest = _digest_of(table, COLUMNS)
    if digest != staging_digest:
        raise BenchmarkRefusedError(
            f"{variant.name} holds {digest}, staging {staging_digest}: the layouts do not hold "
            f"the identical data"
        )
    version = int(spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1").collect()[0]["version"])
    partitions = (
        table.select(*variant.partition_by).distinct().count() if variant.partition_by else 1
    )
    return {
        "variant": variant.name,
        "path": str(path),
        "version": version,
        "rows": rows,
        "num_files": len(sizes),
        "size_bytes": sum(sizes),
        "file_bytes_min": sizes[0] if sizes else 0,
        "file_bytes_median": sizes[len(sizes) // 2] if sizes else 0,
        "file_bytes_max": sizes[-1] if sizes else 0,
        "partitions": partitions,
        "partition_columns": list(detail.get("partitionColumns") or []),
        "clustering_columns": list(detail.get("clusteringColumns") or []),
        "min_reader_version": int(detail["minReaderVersion"]),
        "min_writer_version": int(detail["minWriterVersion"]),
        "table_features": sorted(detail.get("tableFeatures") or []),
        "write_s": write_s,
        "optimize_s": optimize_s,
        "optimize_metrics": optimize_metrics,
        "content_digest": digest,
    }


# ------------------------------------------------------------------- queries ---


def query(
    spark: SparkSession, build_record: dict[str, Any], p: QueryParams, vocab: Vocabulary
) -> DataFrame:
    """The shape's query against one variant, at the version its build recorded."""
    from pyspark.sql import functions as F  # noqa: N812

    frame = (
        spark.read.format("delta")
        .option("versionAsOf", str(build_record["version"]))
        .load(build_record["path"])
    )
    shape = next(s for s in SHAPES if s.name == p.shape)
    if p.shape == "context_lookup":
        frame = frame.filter(F.col("stream") == vocab.stream_transaction).filter(
            F.col("event_id").isin(*p.event_ids)
        )
    elif p.shape == "context_extract":
        frame = frame.filter(F.col("stream") == vocab.stream_transaction)
    elif p.shape == "account_history":
        frame = frame.filter(F.col("account_id") == p.account_id)
    elif p.shape == "time_range":
        frame = frame.filter(
            (F.col("occurred_at") >= F.lit(timestamp(p.lo_ms)))
            & (F.col("occurred_at") < F.lit(timestamp(p.hi_ms)))
        )
    else:
        raise BenchmarkRefusedError(f"no query for shape {p.shape!r}")
    return frame.select(*shape.projection)


def correctness_pass(
    spark: SparkSession,
    builds: dict[str, dict[str, Any]],
    params: Sequence[QueryParams],
    vocab: Vocabulary,
) -> list[ResultCheck]:
    checks = []
    for p in params:
        projection = next(s for s in SHAPES if s.name == p.shape).projection
        for name, record in builds.items():
            rows, digest = _digest_of(query(spark, record, p, vocab), projection)
            checks.append(ResultCheck(name, p.shape, p.rep, rows, digest))
    return checks


def measured_pass(
    spark: SparkSession,
    builds: dict[str, dict[str, Any]],
    params: Sequence[QueryParams],
    vocab: Vocabulary,
) -> tuple[list[QueryRun], dict[str, dict[str, str]]]:
    from trace_core.stream.tables import measure_scans

    names = list(builds)
    runs: list[QueryRun] = []
    plans: dict[str, dict[str, str]] = {name: {} for name in names}
    for p in params:
        shift = p.rep % len(names)
        for name in names[shift:] + names[:shift]:
            started = time.perf_counter()
            frame = query(spark, builds[name], p, vocab)
            measurement = measure_scans(frame)
            wall_s = time.perf_counter() - started
            numbers = extract_scan_metrics(dataclasses.asdict(measurement))
            runs.append(QueryRun(name, p.shape, p.rep, wall_s, numbers))
            if p.rep == 0:
                executed: Any = frame._jdf.queryExecution().executedPlan()
                plans[name][p.shape] = str(executed.toString())[:6000]
            _log.info(
                "bench_layout_measured",
                variant=name,
                shape=p.shape,
                rep=p.rep,
                wall_s=round(wall_s, 4),
                selected_files=numbers.selected_files,
                input_bytes=numbers.input_bytes,
            )
    return runs, plans


# ---------------------------------------------------------------------- main ---


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bench-layout", description="The Delta layout benchmark backing ADR-0015."
    )
    parser.add_argument("--rows", type=int, default=MIN_PUBLISHABLE_ROWS)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--batch-rows", type=int, default=1_000_000)
    parser.add_argument("--master", default="local[4]")
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--shuffle-partitions", type=int, default=4)
    parser.add_argument(
        "--optimize-max-file-size",
        type=int,
        default=None,
        help="spark.databricks.delta.optimize.maxFileSize in bytes; unset keeps Delta's default",
    )
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE)
    parser.add_argument(
        "--keep-tables",
        action="store_true",
        help="keep the run's Delta tables (a failed run always keeps them)",
    )
    return parser


def _write_json(path: Path, payload: Any) -> str:
    text = json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x") as handle:
        handle.write(text)
    return "sha256:" + hashlib.sha256(text.encode()).hexdigest()


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    spec = GeneratorSpec.for_rows(args.rows, args.seed, days=args.days)
    spec.validate()
    if args.reps < 1:
        raise SystemExit("--reps must be positive")
    vocab = vocabulary()
    params = parameters(spec, args.reps)
    fields = _fields()
    started = dt.datetime.now(dt.UTC)
    sha, dirty = git_commit_sha(), is_dirty()
    full = spec.rows >= MIN_PUBLISHABLE_ROWS
    mode = "full" if full else "smoke"
    run_id = f"{'bench' if full else 'smoke'}-{started:%Y%m%d-%H%M%S}-delta-layout-{sha[:8]}"
    lake = args.lake.resolve()
    run_dir = lake / run_id
    if run_dir.exists():
        raise SystemExit(f"{run_dir} exists; refusing to reuse another run's tables")
    run_dir.mkdir(parents=True)
    _log.info("bench_layout_started", run_id=run_id, rows=spec.rows, reps=args.reps, mode=mode)

    from trace_core.stream.session import build_session

    extra: dict[str, str] = {}
    if args.optimize_max_file_size is not None:
        extra["spark.databricks.delta.optimize.maxFileSize"] = str(args.optimize_max_file_size)
    spark = build_session(
        "trace-x-bench-layout",
        master=args.master,
        shuffle_partitions=args.shuffle_partitions,
        driver_memory=args.driver_memory,
        extra_conf=extra,
    )
    try:
        toolchain = _toolchain(spark)
        spark_conf = _conf(spark)
        machine = _machine(lake)
        staging_path = run_dir / "staging"
        staging = stage(spark, spec, vocab, staging_path, fields, args.batch_rows)
        builds: dict[str, dict[str, Any]] = {}
        for variant in VARIANTS:
            builds[variant.name] = build(
                spark, variant, staging_path, run_dir / variant.name, fields, staging["digest"]
            )
            _log.info(
                "bench_layout_built",
                variant=variant.name,
                files=builds[variant.name]["num_files"],
                write_s=round(builds[variant.name]["write_s"], 2),
            )
        names = list(builds)
        checks = correctness_pass(spark, builds, params, vocab)
        require_identical_results(checks, names, params, spec)
        runs, plans = measured_pass(spark, builds, params, vocab)
        require_complete_runs(runs, checks, names, params)
    finally:
        spark.stop()

    shape_names = [s.name for s in SHAPES]
    summaries = summarize(runs)
    ranking = rank(summaries, names, shape_names, CONTROL)
    finished = dt.datetime.now(dt.UTC)
    results = {
        "run_id": run_id,
        "params": [p.summary() for p in params],
        "builds": list(builds.values()),
        "checks": [dataclasses.asdict(c) for c in checks],
        "runs": [r.summary() for r in runs],
        "plans": plans,
    }
    results_path = (RESULTS_DIR if full else run_dir) / (
        f"{run_id}.json" if full else "results.json"
    )
    results_digest = _write_json(results_path, results)
    record: dict[str, Any] = {
        "run_id": run_id,
        "record_type": "BENCHMARK",
        "track": "SYNTHETIC",
        "subject": SUBJECT,
        "tool": "spark+delta",
        "tool_version": f"spark {toolchain['spark']} / delta {toolchain['delta']}",
        "git_commit_sha": sha,
        "dirty_worktree": dirty,
        "env_lock_digest": env_lock_digest(),
        "python_version": platform.python_version(),
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "mode": mode,
        "publishable": publishable(rows=spec.rows, reps=args.reps, dirty_worktree=dirty),
        "seed": spec.seed,
        "row_count": spec.rows,
        "generator_version": generator.GENERATOR_VERSION,
        "generator_spec": dataclasses.asdict(spec),
        "generator_digest": spec.digest(vocab),
        "dataset_digest": staging["digest"],
        "toolchain": toolchain,
        "session": {
            "master": args.master,
            "driver_memory": args.driver_memory,
            "shuffle_partitions": args.shuffle_partitions,
            "reps": args.reps,
            "batch_rows": args.batch_rows,
            "optimize_max_file_size": args.optimize_max_file_size,
        },
        "spark_conf": spark_conf,
        "machine": machine,
        "variants": [v.summary() for v in VARIANTS],
        "query_shapes": [
            {
                "name": s.name,
                "provenance": s.provenance,
                "source": s.source,
                "description": s.description,
                "projection": list(s.projection),
            }
            for s in SHAPES
        ],
        "warm_cold_policy": WARM_COLD_POLICY,
        "ranking_rule": RANKING_RULE,
        "measured": {
            "staging": staging,
            "builds": [
                {k: v for k, v in b.items() if k not in ("path", "optimize_metrics")}
                for b in builds.values()
            ],
            "summaries": [dataclasses.asdict(s) for s in summaries.values()],
            "ranking": [dataclasses.asdict(r) for r in ranking],
            "shape_leaders": {s: shape_leaders(summaries, names, s) for s in shape_names},
            "executions": len(runs),
            "results_file": str(results_path.relative_to(ROOT) if full else results_path),
            "results_digest": results_digest,
        },
    }
    if missing := missing_manifest_fields(record):
        raise BenchmarkRefusedError(f"the run record is incomplete: {missing}")
    manifest_path = MANIFEST_DIR / f"{run_id}.json" if full else run_dir / "manifest.json"
    _write_json(manifest_path, record)
    report_path = REPORT_PATH if full else run_dir / "REPORT.md"
    report_path.write_text(report.render(record) + "\n")
    if not args.keep_tables:
        for name in ("staging", *names):
            shutil.rmtree(run_dir / name)

    print(f"run_id: {run_id}")
    print(f"record: {manifest_path}")
    print(f"results: {results_path}")
    print(f"report: {report_path}")
    for r in ranking:
        print(f"  rank {r.rank}: {r.variant:<24} bytes-read score {r.bytes_score:.3f}")
    if not record["publishable"]:
        print("NOTE: not publishable (smoke rows, too few repetitions, or a dirty worktree).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
