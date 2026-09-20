# ruff: noqa: E501, S608 -- a diagnostic spike (Phase A evidence), not production code
"""Phase 3 Step 11, Phase A: the Delta 4.0.1 retention and maintenance spike. NOT production code.

Every experiment runs on the pinned toolchain through `build_session` and records what Delta DID
into `out/observations.json`. Nothing here is a benchmark: no timing or size is recorded as a
result, and there is no run_id.

Questions (the Step 11 brief, Phase A):
  X1  a real Bronze declaration: DELETE under appendOnly; a floor table property; drift.
  X2  a Silver-like reader (the checkpoint convention) at a rewrite DELETE: running and restarted;
      today's refusals; a checkpoint reset past the delete commit.
  X3  skipChangeCommits at a rewrite DELETE; fresh readers from version 0 before/after VACUUM.
  X4  a file-aligned DELETE: is its commit removes-only; ignoreDeletes; ignoreDeletes at a rewrite;
      a reset at the first unretired version; behaviour after VACUUM.
  X5  change data feed: protocol, cost on appends, a rewrite and a whole-file delete, an UPDATE,
      running and restarted readers, a fresh reader after VACUUM.
  X6  floor property commits (and appendOnly toggles) under a running and restarted reader; reading
      the floor and the rows from one snapshot.
  X7  durability through log cleanup: a table property versus commitInfo.userMetadata.
  X8  Delta domain metadata: public API, and an internal commit on a (1, 2) table.
  X9  OPTIMIZE on an appendOnly Bronze-like source under running and restarted readers; a reader
      behind it after VACUUM.
  X10 VACUUM facts: session default, appendOnly, DRY RUN output, LITE.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
import traceback
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trace_core.stream import checkpoints, tables
from trace_core.stream.bronze import bronze_declaration
from trace_core.stream.checkpoints import DeltaSourceStart
from trace_core.stream.lake import LakeConfig, Tier
from trace_core.stream.tables import CommitProvenance, TableDeclaration, TableRef

SHA = hashlib.sha1(b"trace-x step11 retention spike", usedforsecurity=False).hexdigest()
NOW = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
RETENTION_CHECK = "spark.databricks.delta.retentionDurationCheck.enabled"
USER_METADATA_CONF = "spark.databricks.delta.commitInfo.userMetadata"
TID = "hB5M5d1ZQpW-lA3_T0Hn1w"  # shaped like a Kafka topic id (base64url, mixed case, - and _)
COLUMNS = (
    "kafka_topic_id STRING NOT NULL, kafka_partition INT NOT NULL, kafka_offset BIGINT NOT NULL, "
    "bronze_batch_id BIGINT NOT NULL, payload STRING"
)
ROW_SCHEMA = (
    "kafka_topic_id STRING, kafka_partition INT, kafka_offset BIGINT, bronze_batch_id BIGINT, "
    "payload STRING"
)
OUT = Path(__file__).with_name("out") / "observations.json"
OBS: dict[str, Any] = {}


def log(message: str) -> None:
    sys.stderr.write(f"[spike {datetime.now(UTC).strftime('%H:%M:%S')}] {message}\n")
    sys.stderr.flush()


def save() -> None:
    OUT.write_text(json.dumps(OBS, indent=2, sort_keys=True, default=str))


def error_summary(exc: BaseException) -> dict[str, Any]:
    text = str(exc)
    classes = sorted(set(re.findall(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]", text)))
    return {"exception": type(exc).__name__, "error_classes": classes, "message": text[:700]}


# ------------------------------------------------------------------ table helpers ---


def latest_version(path: Path) -> int:
    return max(int(p.name[:20]) for p in (path / "_delta_log").glob("*.json"))


def commit_actions(path: Path, version: int) -> dict[str, Any]:
    file = path / "_delta_log" / f"{version:020d}.json"
    counts: Counter[str] = Counter()
    info: dict[str, Any] = {}
    added_rows = 0
    for line in file.read_text().splitlines():
        if not line.strip():
            continue
        action = json.loads(line)
        for kind, body in action.items():
            if kind == "add":
                counts[f"add(dataChange={body.get('dataChange')})"] += 1
                if body.get("stats"):
                    added_rows += int(json.loads(body["stats"]).get("numRecords", 0))
            elif kind == "remove":
                counts[f"remove(dataChange={body.get('dataChange')})"] += 1
            elif kind == "commitInfo":
                metrics = body.get("operationMetrics") or {}
                info = {
                    "operation": body.get("operation"),
                    "userMetadata": body.get("userMetadata"),
                    "metrics": {
                        k: v
                        for k, v in metrics.items()
                        if k
                        in {
                            "numAddedFiles",
                            "numRemovedFiles",
                            "numDeletedRows",
                            "numCopiedRows",
                            "numAddedChangeFiles",
                            "numOutputRows",
                            "numUpdatedRows",
                        }
                    },
                }
            elif kind == "metaData":
                counts["metaData"] += 1
                info["configuration"] = body.get("configuration")
            else:
                counts[kind] += 1
    return {"version": version, "actions": dict(counts), "rows_in_adds": added_rows, **info}


def physical_files(path: Path) -> dict[str, int]:
    data = [
        p
        for p in path.rglob("*.parquet")
        if "_delta_log" not in p.parts and "_change_data" not in p.parts
    ]
    cdc = (
        list((path / "_change_data").rglob("*.parquet")) if (path / "_change_data").exists() else []
    )
    return {"data_parquet_files": len(data), "change_data_files": len(cdc)}


def detail(spark: Any, path: Path) -> dict[str, Any]:
    live = tables.describe_live_table(spark, f"delta.`{path}`")
    return {
        "protocol": [live.min_reader_version, live.min_writer_version],
        "features": sorted(live.table_features),
        "properties": dict(sorted(live.properties.items())),
    }


def create_source(spark: Any, path: Path, props: dict[str, str]) -> None:
    clause = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
    spark.sql(
        f"CREATE TABLE delta.`{path}` ({COLUMNS}) USING delta"
        + (f" TBLPROPERTIES ({clause})" if clause else "")
    )


def append(
    spark: Any, path: Path, batch: int, ranges: dict[int, tuple[int, int]], files: int = 1
) -> int:
    rows = [
        (TID, p, o, batch, f"b{batch}") for p, (s, e) in sorted(ranges.items()) for o in range(s, e)
    ]
    frame = spark.createDataFrame(rows, ROW_SCHEMA)
    frame = frame.coalesce(1) if files == 1 else frame.repartitionByRange(files, "kafka_partition")
    frame.write.format("delta").mode("append").save(str(path))
    return latest_version(path)


def delete_below(spark: Any, path: Path, floors: dict[int, int]) -> int:
    condition = " OR ".join(
        f"(kafka_topic_id = '{TID}' AND kafka_partition = {p} AND kafka_offset < {f})"
        for p, f in sorted(floors.items())
    )
    spark.sql(f"DELETE FROM delta.`{path}` WHERE {condition}")
    return latest_version(path)


def set_properties(spark: Any, path: Path, props: dict[str, str]) -> int:
    clause = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
    spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ({clause})")
    return latest_version(path)


def vacuum_everything(spark: Any, path: Path) -> dict[str, Any]:
    """SPIKE ONLY: disables the retention check to reproduce what an unsafe VACUUM does."""
    spark.conf.set(RETENTION_CHECK, "false")
    try:
        dry = spark.sql(f"VACUUM delta.`{path}` RETAIN 0 HOURS DRY RUN").collect()
        spark.sql(f"VACUUM delta.`{path}` RETAIN 0 HOURS").collect()
    finally:
        spark.conf.set(RETENTION_CHECK, "true")
    return {"dry_run_would_delete": len(dry), "after": physical_files(path)}


def snapshot_offsets(spark: Any, path: Path) -> dict[str, list[int]]:
    rows = spark.read.format("delta").load(str(path)).select("kafka_partition", "kafka_offset")
    out: dict[str, list[int]] = {}
    for row in rows.collect():
        out.setdefault(str(row["kafka_partition"]), []).append(int(row["kafka_offset"]))
    return {p: sorted(v) for p, v in sorted(out.items())}


def summarise(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by: dict[str, list[int]] = {}
    keys: Counter[tuple[Any, ...]] = Counter()
    for row in rows:
        kind = row.get("_change_type", "row")
        by.setdefault(f"{kind}:p{row['kafka_partition']}", []).append(int(row["kafka_offset"]))
        keys[(kind, row["kafka_partition"], row["kafka_offset"])] += 1
    return {
        "count": len(rows),
        "offsets": {k: sorted(v) for k, v in sorted(by.items())},
        "repeated_coordinates": sum(n - 1 for n in keys.values() if n > 1),
        "batches": sorted({int(r["batch_id"]) for r in rows}),
    }


def overloads(target: Any, name: str) -> list[list[str]]:
    return sorted(
        [str(k.getName()) for k in m.getParameterTypes()]
        for m in target.getClass().getMethods()
        if str(m.getName()) == name
    )


def call_filling_options(jvm: Any, target: Any, name: str, *args: Any) -> Any:
    """Call the overload whose parameters after `args` are all scala.Option, passing None for each."""
    for params in sorted(overloads(target, name), key=len, reverse=True):
        rest = params[len(args) :]
        if len(params) >= len(args) and all(kind == "scala.Option" for kind in rest):
            return getattr(target, name)(*args, *[jvm.scala.Option.empty() for _ in rest])
    raise RuntimeError(
        f"no {name} overload takes {len(args)} args then only Options: {overloads(target, name)}"
    )


def call_with_defaults(target: Any, name: str, *args: Any) -> Any:
    methods = [m for m in target.getClass().getMethods() if str(m.getName()) == name]
    counts = sorted({int(m.getParameterCount()) for m in methods})
    count = max(counts)
    defaults = [getattr(target, f"{name}$default${i}")() for i in range(len(args) + 1, count + 1)]
    return getattr(target, name)(*args, *defaults)


# ------------------------------------------------------------------------ reader ---


class Reader:
    """A streaming reader of a Bronze-like source that records every row it delivers.

    `convention=True` reads through `OpenedCheckpoint.delta_source` exactly as Silver does
    (failOnDataLoss=true, startingVersion from the checkpoint, refusals applied). Otherwise a raw
    `readStream` with failOnDataLoss=true plus `options` -- used only for options the convention
    refuses today."""

    def __init__(
        self,
        spark: Any,
        lake: LakeConfig,
        source: TableRef,
        name: str,
        *,
        options: dict[str, str] | None = None,
        convention: bool = False,
        starting_version: int | None = 0,
        cdf: bool = False,
    ) -> None:
        self.spark, self.lake, self.source, self.name = spark, lake, source, name
        self.options = dict(options or {})
        self.convention, self.starting_version, self.cdf = convention, starting_version, cdf
        self.seen: list[dict[str, Any]] = []
        self.query: Any = None
        self.checkpoint: Path = lake.root / "_spike_checkpoints" / name
        if convention:
            from pyspark.sql.types import LongType, StructField, StructType

            self.target = TableRef(Tier.SILVER, f"{name}_target")
            tables.create_table(
                spark,
                TableDeclaration(
                    ref=self.target, schema=StructType([StructField("id", LongType())])
                ),
                lake,
                CommitProvenance(SHA, False, name),
            )

    def _open(self) -> Any:
        opened = checkpoints.open_checkpoint(
            self.spark,
            self.lake,
            self.name,
            targets=[self.target],
            sources=[DeltaSourceStart(self.source, self.starting_version)],
            git_sha=SHA,
            dirty_worktree=False,
            now=NOW,
        )
        self.checkpoint = opened.directory
        return opened

    def probe(self, options: dict[str, str]) -> dict[str, Any]:
        try:
            self._open().delta_source(self.spark, self.source, options)
            return {"refused": False}
        except Exception as exc:
            return {"refused": True, **error_summary(exc)}

    def reset(self, starting_version: int, reason: str) -> None:
        self.starting_version = starting_version
        checkpoints.reset_checkpoint(
            self.spark,
            self.lake,
            self.name,
            targets=[self.target],
            sources=[DeltaSourceStart(self.source, starting_version)],
            reason=reason,
            now=NOW,
        )

    def _frame(self) -> Any:
        if self.convention:
            return self._open().delta_source(self.spark, self.source, self.options)
        reader = self.spark.readStream.format("delta").option("failOnDataLoss", "true")
        if self.starting_version is not None:
            reader = reader.option("startingVersion", str(self.starting_version))
        for key, value in self.options.items():
            reader = reader.option(key, value)
        return reader.load(str(self.source.local_path(self.lake)))

    def _sink(self, frame: Any, batch_id: int) -> None:
        columns = ["kafka_partition", "kafka_offset", "bronze_batch_id"]
        if self.cdf:
            columns += ["_change_type", "_commit_version"]
        for row in frame.select(*columns).collect():
            self.seen.append({"batch_id": batch_id, **row.asDict()})

    def _start(self, available_now: bool) -> Any:
        writer = (
            self._frame()
            .writeStream.foreachBatch(self._sink)
            .option("checkpointLocation", str(self.checkpoint))
        )
        if available_now:
            writer = writer.trigger(availableNow=True)
        else:
            writer = writer.trigger(processingTime="500 milliseconds")
        return writer.start()

    def run_available_now(self) -> dict[str, Any]:
        before = len(self.seen)
        try:
            query = self._start(True)
            query.awaitTermination()
            outcome: dict[str, Any] = {"outcome": "completed"}
        except Exception as exc:
            outcome = {"outcome": "failed", **error_summary(exc)}
        return {**outcome, "delivered": summarise(self.seen[before:])}

    def start_running(self) -> dict[str, Any]:
        before = len(self.seen)
        self.query = self._start(False)
        self.query.processAllAvailable()
        return {"outcome": "running", "delivered": summarise(self.seen[before:])}

    def drain(self) -> dict[str, Any]:
        before = len(self.seen)
        try:
            self.query.processAllAvailable()
            outcome: dict[str, Any] = {"outcome": "completed", "still_active": self.query.isActive}
        except Exception as exc:
            outcome = {"outcome": "failed", **error_summary(exc)}
        return {**outcome, "delivered": summarise(self.seen[before:])}

    def stop(self) -> None:
        if self.query is not None and self.query.isActive:
            self.query.stop()
        self.query = None

    def total(self) -> dict[str, Any]:
        """Every row this reader delivered over its whole life. A processingTime query can deliver
        rows between two observation windows, so only this total proves completeness and no
        repeats for a running reader."""
        return summarise(self.seen)

    def recorded_offsets(self) -> dict[str, Any]:
        directory = Path(self.checkpoint)
        offsets: dict[str, Any] = {}
        if (directory / "offsets").is_dir():
            for entry in sorted(
                (p for p in (directory / "offsets").iterdir() if p.name.isdigit()),
                key=lambda p: int(p.name),
            ):
                lines = entry.read_text().splitlines()
                offsets[entry.name] = json.loads(lines[2]) if len(lines) > 2 else lines
        committed = (
            sorted(int(p.name) for p in (directory / "commits").iterdir() if p.name.isdigit())
            if (directory / "commits").is_dir()
            else []
        )
        return {"offsets": offsets, "committed_batches": committed}


# ------------------------------------------------------------------- experiments ---

THREE_BATCHES = ({0: (0, 4), 1: (0, 4)}, {0: (4, 8), 1: (4, 8)}, {0: (8, 12), 1: (8, 12)})
REWRITE_FLOORS = {0: 6, 1: 2}  # cuts through batch 0 (p1) and batch 1 (p0): files are rewritten
ALIGNED_FLOORS = {0: 8, 1: 8}  # exactly the rows of batches 0 and 1


def x1_append_only_and_floor_property(spark: Any, root: Path) -> dict[str, Any]:
    lake = LakeConfig.at(root / "lake")
    declaration = bronze_declaration("tx.raw.v1")
    path = declaration.ref.local_path(lake)
    tables.create_table(
        spark, declaration, lake, CommitProvenance(SHA, False, "bronze_ingest_tx_raw_v1")
    )
    out: dict[str, Any] = {"created": detail(spark, path)}
    try:
        spark.sql(f"DELETE FROM delta.`{path}` WHERE kafka_offset < 1")
        out["delete_under_append_only"] = {"refused": False, "version": latest_version(path)}
    except Exception as exc:
        out["delete_under_append_only"] = {"refused": True, **error_summary(exc)}
    key3 = f"trace_x.retention_floor.{TID}.3"
    try:
        version = set_properties(spark, path, {key3: "42"})
        live = tables.describe_live_table(spark, declaration.ref.path_identifier(lake))
        out["floor_property_on_append_only"] = {
            "accepted": True,
            "commit": commit_actions(path, version),
            "detail": detail(spark, path),
            "key_case_preserved": key3 in live.properties,
            "drift_against_declaration": [str(d) for d in tables.check_drift(declaration, live)],
        }
    except Exception as exc:
        out["floor_property_on_append_only"] = {"accepted": False, **error_summary(exc)}
    key4 = f"trace_x.retention_floor.{TID}.4"
    try:
        version = set_properties(spark, path, {"delta.appendOnly": "false", key4: "7"})
        out["append_only_off_and_floor_in_one_alter"] = {
            "commit": commit_actions(path, version),
            "detail": detail(spark, path),
        }
    except Exception as exc:
        out["append_only_off_and_floor_in_one_alter"] = error_summary(exc)
    try:
        spark.sql(
            f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('delta.retentionFloorSpike' = '1')"
        )
        out["unknown_delta_prefixed_key"] = {"accepted": True}
    except Exception as exc:
        out["unknown_delta_prefixed_key"] = {"accepted": False, **error_summary(exc)}
    return out


def x2_rewrite_delete_under_the_convention_reader(spark: Any, root: Path) -> dict[str, Any]:
    lake = LakeConfig.at(root / "lake")
    out: dict[str, Any] = {}
    for mode in ("running", "restart"):
        source = TableRef(Tier.BRONZE, f"rewrite_{mode}")
        path = source.local_path(lake)
        create_source(spark, path, {"delta.appendOnly": "false"})
        versions = [append(spark, path, i, r) for i, r in enumerate(THREE_BATCHES)]
        reader = Reader(spark, lake, source, f"conv_rewrite_{mode}", convention=True)
        section: dict[str, Any] = {"append_versions": versions}
        if mode == "running":
            section["initial"] = reader.start_running()
            d = delete_below(spark, path, REWRITE_FLOORS)
            section["append_after_delete"] = append(spark, path, 3, {0: (12, 16), 1: (12, 16)})
            section["at_delete"] = reader.drain()
            reader.stop()
            section["total"] = reader.total()
        else:
            section["initial"] = reader.run_available_now()
            d = delete_below(spark, path, REWRITE_FLOORS)
            section["append_after_delete"] = append(spark, path, 3, {0: (12, 16), 1: (12, 16)})
            section["restart_at_delete"] = reader.run_available_now()
            section["second_restart"] = reader.run_available_now()
            section["checkpoint_before_reset"] = reader.recorded_offsets()
            reader.reset(d + 1, "spike: start past the retention delete commit")
            section["reset_past_delete_commit"] = reader.run_available_now()
        section["delete_commit"] = commit_actions(path, d)
        section["files"] = physical_files(path)
        section["checkpoint"] = reader.recorded_offsets()
        section["snapshot"] = snapshot_offsets(spark, path)
        out[mode] = section
    probe_source = TableRef(Tier.BRONZE, "rewrite_running")
    probe = Reader(spark, lake, probe_source, "conv_probe_options", convention=True)
    out["refusals_today"] = {
        name: probe.probe({name: "true"})
        for name in ("ignoreDeletes", "skipChangeCommits", "ignoreChanges", "readChangeFeed")
    }
    return out


def x3_skip_change_commits(spark: Any, root: Path) -> dict[str, Any]:
    lake = LakeConfig.at(root / "lake")
    skip = {"skipChangeCommits": "true"}
    out: dict[str, Any] = {}
    for mode in ("running", "restart"):
        source = TableRef(Tier.BRONZE, f"skip_{mode}")
        path = source.local_path(lake)
        create_source(spark, path, {"delta.appendOnly": "false"})
        versions = [append(spark, path, i, r) for i, r in enumerate(THREE_BATCHES)]
        reader = Reader(spark, lake, source, f"skip_{mode}", options=skip)
        section: dict[str, Any] = {"append_versions": versions}
        if mode == "running":
            section["initial"] = reader.start_running()
            d = delete_below(spark, path, REWRITE_FLOORS)
            section["append_after_delete"] = append(spark, path, 3, {0: (12, 16), 1: (12, 16)})
            section["at_delete"] = reader.drain()
            reader.stop()
        else:
            section["initial"] = reader.run_available_now()
            d = delete_below(spark, path, REWRITE_FLOORS)
            section["append_after_delete"] = append(spark, path, 3, {0: (12, 16), 1: (12, 16)})
            section["restart_at_delete"] = reader.run_available_now()
        section["total"] = reader.total()
        section["delete_commit"] = commit_actions(path, d)
        section["checkpoint"] = reader.recorded_offsets()
        out[mode] = section

    source = TableRef(Tier.BRONZE, "skip_history")
    path = source.local_path(lake)
    create_source(spark, path, {"delta.appendOnly": "false"})
    history: dict[str, Any] = {"b0": append(spark, path, 0, THREE_BATCHES[0])}
    behind = Reader(spark, lake, source, "skip_behind", options=skip)
    history["behind_initial"] = behind.run_available_now()
    history["b1"] = append(spark, path, 1, THREE_BATCHES[1])
    history["b2"] = append(spark, path, 2, THREE_BATCHES[2])
    caught = Reader(spark, lake, source, "skip_caught", options=skip)
    history["caught_initial"] = caught.run_available_now()
    history["delete"] = delete_below(spark, path, REWRITE_FLOORS)
    history["b3"] = append(spark, path, 3, {0: (12, 16), 1: (12, 16)})
    history["snapshot_after_delete"] = snapshot_offsets(spark, path)
    history["fresh_v0_before_vacuum"] = Reader(
        spark, lake, source, "skip_fresh_before", options=skip
    ).run_available_now()
    history["fresh_at_delete_plus_1_before_vacuum"] = Reader(
        spark,
        lake,
        source,
        "skip_fresh_after_delete",
        options=skip,
        starting_version=history["delete"] + 1,
    ).run_available_now()
    history["fresh_snapshot_start"] = Reader(
        spark, lake, source, "skip_fresh_snapshot", options=skip, starting_version=None
    ).run_available_now()
    history["vacuum"] = vacuum_everything(spark, path)
    history["behind_after_vacuum"] = behind.run_available_now()
    history["caught_after_vacuum"] = caught.run_available_now()
    history["fresh_v0_after_vacuum"] = Reader(
        spark, lake, source, "skip_fresh_after", options=skip
    ).run_available_now()
    out["history"] = history
    return out


def x4_file_aligned_delete(spark: Any, root: Path) -> dict[str, Any]:
    lake = LakeConfig.at(root / "lake")
    ignore = {"ignoreDeletes": "true"}
    out: dict[str, Any] = {}
    for mode in ("running", "restart"):
        source = TableRef(Tier.BRONZE, f"aligned_{mode}")
        path = source.local_path(lake)
        create_source(spark, path, {"delta.appendOnly": "false"})
        versions = [
            append(spark, path, 0, THREE_BATCHES[0]),
            append(spark, path, 1, THREE_BATCHES[1], files=2),
            append(spark, path, 2, THREE_BATCHES[2]),
        ]
        section: dict[str, Any] = {
            "append_versions": versions,
            "batch1_commit": commit_actions(path, versions[1]),
            "files_before_delete": physical_files(path),
        }
        default = Reader(spark, lake, source, f"conv_aligned_{mode}", convention=True)
        tolerant = Reader(spark, lake, source, f"ignore_aligned_{mode}", options=ignore)
        if mode == "running":
            section["default_initial"] = default.start_running()
            section["tolerant_initial"] = tolerant.start_running()
            d = delete_below(spark, path, ALIGNED_FLOORS)
            section["append_after_delete"] = append(spark, path, 3, {0: (12, 16), 1: (12, 16)})
            section["default_at_delete"] = default.drain()
            section["tolerant_at_delete"] = tolerant.drain()
            default.stop()
            tolerant.stop()
            section["default_total"] = default.total()
            section["tolerant_total"] = tolerant.total()
        else:
            section["default_initial"] = default.run_available_now()
            section["tolerant_initial"] = tolerant.run_available_now()
            d = delete_below(spark, path, ALIGNED_FLOORS)
            section["append_after_delete"] = append(spark, path, 3, {0: (12, 16), 1: (12, 16)})
            section["default_restart_at_delete"] = default.run_available_now()
            section["tolerant_restart_at_delete"] = tolerant.run_available_now()
        section["delete_commit"] = commit_actions(path, d)
        section["files_after_delete"] = physical_files(path)
        section["tolerant_checkpoint"] = tolerant.recorded_offsets()
        section["snapshot"] = snapshot_offsets(spark, path)
        out[mode] = section

    source = TableRef(Tier.BRONZE, "ignore_on_rewrite")
    path = source.local_path(lake)
    create_source(spark, path, {"delta.appendOnly": "false"})
    for i, r in enumerate(THREE_BATCHES):
        append(spark, path, i, r)
    reader = Reader(spark, lake, source, "ignore_on_rewrite", options=ignore)
    rewrite: dict[str, Any] = {"initial": reader.run_available_now()}
    d = delete_below(spark, path, REWRITE_FLOORS)
    append(spark, path, 3, {0: (12, 16), 1: (12, 16)})
    rewrite["delete_commit"] = commit_actions(path, d)
    rewrite["restart_at_rewrite"] = reader.run_available_now()
    out["ignore_deletes_at_a_rewrite"] = rewrite

    source = TableRef(Tier.BRONZE, "aligned_history")
    path = source.local_path(lake)
    create_source(spark, path, {"delta.appendOnly": "false"})
    history: dict[str, Any] = {"b0": append(spark, path, 0, THREE_BATCHES[0])}
    behind = Reader(spark, lake, source, "aligned_behind", options=ignore)
    history["behind_initial"] = behind.run_available_now()
    history["b1"] = append(spark, path, 1, THREE_BATCHES[1])
    history["b2"] = append(spark, path, 2, THREE_BATCHES[2])
    history["delete"] = delete_below(spark, path, ALIGNED_FLOORS)
    history["delete_commit"] = commit_actions(path, history["delete"])
    history["b3"] = append(spark, path, 3, {0: (12, 16), 1: (12, 16)})
    history["fresh_first_unretired_before_vacuum"] = Reader(
        spark,
        lake,
        source,
        "aligned_first_unretired",
        options=ignore,
        starting_version=history["b2"],
    ).run_available_now()
    history["fresh_v0_before_vacuum"] = Reader(
        spark, lake, source, "aligned_fresh_before", options=ignore
    ).run_available_now()
    history["vacuum"] = vacuum_everything(spark, path)
    history["behind_after_vacuum"] = behind.run_available_now()
    history["fresh_v0_after_vacuum"] = Reader(
        spark, lake, source, "aligned_fresh_after", options=ignore
    ).run_available_now()
    history["fresh_first_unretired_after_vacuum"] = Reader(
        spark,
        lake,
        source,
        "aligned_first_unretired_after",
        options=ignore,
        starting_version=history["b2"],
    ).run_available_now()
    conv = Reader(
        spark,
        lake,
        source,
        "conv_aligned_first_unretired",
        convention=True,
        starting_version=history["b2"],
    )
    history["convention_first_unretired_after_vacuum_default_options"] = conv.run_available_now()
    out["history"] = history
    return out


def x5_change_data_feed(spark: Any, root: Path) -> dict[str, Any]:
    lake = LakeConfig.at(root / "lake")
    cdf = {"readChangeFeed": "true"}
    source = TableRef(Tier.BRONZE, "cdf_source")
    path = source.local_path(lake)
    create_source(spark, path, {"delta.appendOnly": "false", "delta.enableChangeDataFeed": "true"})
    out: dict[str, Any] = {"created": detail(spark, path)}
    versions = [append(spark, path, i, r) for i, r in enumerate(THREE_BATCHES)]
    out["append_commit"] = commit_actions(path, versions[0])
    out["files_after_appends"] = physical_files(path)
    running = Reader(spark, lake, source, "cdf_running", options=cdf, cdf=True)
    restarted = Reader(spark, lake, source, "cdf_restarted", options=cdf, cdf=True)
    out["running_initial"] = running.start_running()
    out["restarted_initial"] = restarted.run_available_now()
    d = delete_below(spark, path, REWRITE_FLOORS)
    out["rewrite_delete_commit"] = commit_actions(path, d)
    out["files_after_rewrite_delete"] = physical_files(path)
    append(spark, path, 3, {0: (12, 16), 1: (12, 16)})
    out["running_at_rewrite_delete"] = running.drain()
    out["restarted_at_rewrite_delete"] = restarted.run_available_now()
    d2 = delete_below(spark, path, ALIGNED_FLOORS)
    out["aligned_delete_commit"] = commit_actions(path, d2)
    out["files_after_aligned_delete"] = physical_files(path)
    append(spark, path, 4, {0: (16, 20), 1: (16, 20)})
    out["running_at_aligned_delete"] = running.drain()
    out["restarted_at_aligned_delete"] = restarted.run_available_now()
    spark.sql(
        f"UPDATE delta.`{path}` SET payload = 'changed' WHERE kafka_partition = 0 AND kafka_offset = 12"
    )
    out["update_commit"] = commit_actions(path, latest_version(path))
    out["running_at_update"] = running.drain()
    running.stop()
    out["running_total"] = running.total()
    out["restarted_at_update"] = restarted.run_available_now()
    out["restarted_total"] = restarted.total()
    out["fresh_v0_before_vacuum"] = Reader(
        spark, lake, source, "cdf_fresh_before", options=cdf, cdf=True
    ).run_available_now()
    out["vacuum"] = vacuum_everything(spark, path)
    out["fresh_v0_after_vacuum"] = Reader(
        spark, lake, source, "cdf_fresh_after", options=cdf, cdf=True
    ).run_available_now()
    return out


def snapshot_at(spark: Any, path: Path, version: int | None) -> dict[str, Any]:
    jvm: Any = spark.sparkContext._jvm
    delta_log = jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark._jsparkSession, str(path))
    if version is None:
        snapshot = delta_log.update(False, jvm.scala.Option.empty(), jvm.scala.Option.empty())
    else:
        snapshot = call_filling_options(jvm, delta_log, "getSnapshotAt", version)
    configuration = snapshot.metadata().configuration()
    keys = [str(k) for k in jvm.scala.jdk.javaapi.CollectionConverters.asJava(configuration.keys())]
    return {
        "version": int(snapshot.version()),
        "floor_properties": {
            k: str(configuration.apply(k)) for k in sorted(keys) if k.startswith("trace_x.")
        },
    }


def x6_floor_property_commits_under_readers(spark: Any, root: Path) -> dict[str, Any]:
    lake = LakeConfig.at(root / "lake")
    source = TableRef(Tier.BRONZE, "floor_props")
    path = source.local_path(lake)
    create_source(spark, path, {"delta.appendOnly": "true"})
    out: dict[str, Any] = {"b0": append(spark, path, 0, THREE_BATCHES[0])}
    running = Reader(spark, lake, source, "conv_floor_running", convention=True)
    restarted = Reader(spark, lake, source, "conv_floor_restarted", convention=True)
    out["running_initial"] = running.start_running()
    out["restarted_initial"] = restarted.run_available_now()
    floor_keys = {f"trace_x.retention_floor.{TID}.{p}": "2" for p in (0, 1)}
    v_floor = set_properties(spark, path, floor_keys)
    out["floor_commit"] = commit_actions(path, v_floor)
    out["b1"] = append(spark, path, 1, THREE_BATCHES[1])
    out["running_after_floor_commit"] = running.drain()
    v_off = set_properties(spark, path, {"delta.appendOnly": "false"})
    v_on = set_properties(spark, path, {"delta.appendOnly": "true"})
    out["append_only_toggle_commits"] = [commit_actions(path, v_off), commit_actions(path, v_on)]
    out["b2"] = append(spark, path, 2, THREE_BATCHES[2])
    out["running_after_toggles"] = running.drain()
    running.stop()
    v_floor2 = set_properties(spark, path, dict.fromkeys(floor_keys, "6"))
    out["b3"] = append(spark, path, 3, {0: (12, 16), 1: (12, 16)})
    out["restarted_after_floor_toggle_floor"] = restarted.run_available_now()
    out["restarted_checkpoint"] = restarted.recorded_offsets()
    out["running_total"] = running.total()
    out["restarted_total"] = restarted.total()
    jvm: Any = spark.sparkContext._jvm
    delta_log = jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark._jsparkSession, str(path))
    out["getSnapshotAt_overloads"] = overloads(delta_log, "getSnapshotAt")
    try:
        latest = snapshot_at(spark, path, None)
        rows = (
            spark.read.format("delta")
            .option("versionAsOf", str(latest["version"]))
            .load(str(path))
            .count()
        )
        out["latest_snapshot_and_rows_at_its_version"] = {**latest, "rows": int(rows)}
    except Exception as exc:
        out["latest_snapshot_error"] = error_summary(exc)
    for label, version in (
        ("before_floor", v_floor - 1),
        ("at_floor", v_floor),
        ("at_floor2", v_floor2),
    ):
        try:
            out[f"snapshot_{label}"] = snapshot_at(spark, path, version)
        except Exception as exc:
            out[f"snapshot_{label}_error"] = error_summary(exc)
    return out


def x7_durability_through_log_cleanup(spark: Any, root: Path) -> dict[str, Any]:
    from delta.tables import DeltaTable

    lake = LakeConfig.at(root / "lake")
    source = TableRef(Tier.BRONZE, "floor_durability")
    path = source.local_path(lake)
    create_source(
        spark,
        path,
        {
            "delta.appendOnly": "false",
            "delta.checkpointInterval": "3",
            "delta.logRetentionDuration": "interval 1 hours",
        },
    )
    append(spark, path, 0, THREE_BATCHES[0])
    marker = json.dumps({"trace_x_spike_audit": "floor advanced", "partition": 0, "floor": 4})
    spark.conf.set(USER_METADATA_CONF, marker)
    try:
        v_floor = set_properties(spark, path, {f"trace_x.retention_floor.{TID}.0": "4"})
    finally:
        spark.conf.unset(USER_METADATA_CONF)
    out: dict[str, Any] = {"floor_commit": commit_actions(path, v_floor)}

    def audit_in_history() -> list[int]:
        rows = (
            DeltaTable.forPath(spark, str(path))
            .history()
            .select("version", "userMetadata")
            .collect()
        )
        return sorted(int(r["version"]) for r in rows if r["userMetadata"] == marker)

    out["history_versions_with_marker_before"] = audit_in_history()
    for i in range(1, 10):
        append(spark, path, i, {0: (100 * i, 100 * i + 2)})
    aged = time.time() - 3 * 86400
    cutoff = latest_version(path) - 2
    for entry in (path / "_delta_log").iterdir():
        if entry.name[:20].isdigit() and int(entry.name[:20]) < cutoff:
            os.utime(entry, (aged, aged))
    append(spark, path, 10, {0: (1000, 1002)})
    retained = tables.retained_log(path)
    out["retained_log_after_cleanup"] = (
        None if retained is None else [retained.earliest, retained.latest]
    )
    out["history_versions_with_marker_after"] = audit_in_history()
    out["detail_after_cleanup"] = detail(spark, path)
    checkpoints_found = sorted((path / "_delta_log").glob("*.checkpoint.parquet"))
    out["checkpoint_files"] = [p.name for p in checkpoints_found]
    if checkpoints_found:
        frame = spark.read.parquet(str(checkpoints_found[-1]))
        columns = frame.columns
        configuration = [
            r["metaData"]["configuration"]
            for r in frame.where("metaData IS NOT NULL").select("metaData").collect()
        ]
        out["latest_checkpoint"] = {
            "columns": columns,
            "has_commitInfo_column": "commitInfo" in columns,
            "metaData_configuration": configuration,
        }
    return out


def x8_domain_metadata(spark: Any, root: Path) -> dict[str, Any]:
    import delta.tables as python_tables

    jvm: Any = spark.sparkContext._jvm
    out: dict[str, Any] = {
        "python_DeltaTable_members_with_domain": [
            n for n in dir(python_tables.DeltaTable) if "omain" in n
        ],
    }
    klass = jvm.org.apache.spark.util.Utils.classForName("io.delta.tables.DeltaTable", False, False)
    out["jvm_DeltaTable_methods_with_domain"] = sorted(
        {str(m.getName()) for m in klass.getMethods() if "omain" in str(m.getName())}
    )
    lake = LakeConfig.at(root / "lake")
    source = TableRef(Tier.BRONZE, "domain_spike")
    path = source.local_path(lake)
    create_source(spark, path, {"delta.appendOnly": "true"})
    out["created"] = detail(spark, path)
    try:
        delta_log = jvm.org.apache.spark.sql.delta.DeltaLog.forTable(
            spark._jsparkSession, str(path)
        )
        out["startTransaction_overloads"] = overloads(delta_log, "startTransaction")
        txn = call_filling_options(jvm, delta_log, "startTransaction")
        out["commit_overloads"] = overloads(txn, "commit")
        action = jvm.org.apache.spark.sql.delta.actions.DomainMetadata(
            "trace_x.retention", '{"floor": {"0": 4}}', False
        )
        actions = jvm.java.util.ArrayList()
        actions.add(action)
        seq = jvm.scala.jdk.javaapi.CollectionConverters.asScala(actions).toList()
        op_class = jvm.org.apache.spark.util.Utils.classForName(
            "org.apache.spark.sql.delta.DeltaOperations$ManualUpdate$", False, False
        )
        operation = op_class.getField("MODULE$").get(None)
        version = txn.commit(seq, operation)
        out["internal_commit"] = {
            "committed_version": int(version),
            "detail_after": detail(spark, path),
        }
    except Exception as exc:
        out["internal_commit"] = {"failed": True, **error_summary(exc)}
    return out


def x9_optimize(spark: Any, root: Path) -> dict[str, Any]:
    lake = LakeConfig.at(root / "lake")
    source = TableRef(Tier.BRONZE, "optimize_source")
    path = source.local_path(lake)
    create_source(spark, path, {"delta.appendOnly": "true"})
    out: dict[str, Any] = {"b0": append(spark, path, 0, THREE_BATCHES[0])}
    behind = Reader(spark, lake, source, "conv_opt_behind", convention=True)
    out["behind_initial"] = behind.run_available_now()
    out["b1"] = append(spark, path, 1, THREE_BATCHES[1])
    out["b2"] = append(spark, path, 2, THREE_BATCHES[2])
    running = Reader(spark, lake, source, "conv_opt_running", convention=True)
    restarted = Reader(spark, lake, source, "conv_opt_restarted", convention=True)
    out["running_initial"] = running.start_running()
    out["restarted_initial"] = restarted.run_available_now()
    try:
        spark.sql(f"OPTIMIZE delta.`{path}`").collect()
        v_opt = latest_version(path)
        out["optimize_commit"] = commit_actions(path, v_opt)
        adds = []
        for line in (path / "_delta_log" / f"{v_opt:020d}.json").read_text().splitlines():
            action = json.loads(line) if line.strip() else {}
            if "add" in action and action["add"].get("stats"):
                stats = json.loads(action["add"]["stats"])
                adds.append(
                    {
                        "rows": stats["numRecords"],
                        "min_offset": stats["minValues"].get("kafka_offset"),
                        "max_offset": stats["maxValues"].get("kafka_offset"),
                        "min_batch": stats["minValues"].get("bronze_batch_id"),
                        "max_batch": stats["maxValues"].get("bronze_batch_id"),
                    }
                )
        out["optimized_files"] = adds
    except Exception as exc:
        out["optimize_commit"] = {"failed": True, **error_summary(exc)}
    out["b3"] = append(spark, path, 3, {0: (12, 16), 1: (12, 16)})
    out["running_after_optimize"] = running.drain()
    running.stop()
    out["running_total"] = running.total()
    out["restarted_after_optimize"] = restarted.run_available_now()
    out["restarted_total"] = restarted.total()
    out["vacuum"] = vacuum_everything(spark, path)
    out["behind_after_optimize_and_vacuum"] = behind.run_available_now()
    return out


def x10_vacuum_facts(spark: Any, root: Path) -> dict[str, Any]:
    lake = LakeConfig.at(root / "lake")
    source = TableRef(Tier.BRONZE, "vacuum_facts")
    path = source.local_path(lake)
    create_source(spark, path, {"delta.appendOnly": "true"})
    append(spark, path, 0, THREE_BATCHES[0])
    out: dict[str, Any] = {"session_retention_check": spark.conf.get(RETENTION_CHECK, "<unset>")}
    try:
        frame = spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS DRY RUN")
        out["dry_run_on_append_only"] = {
            "accepted": True,
            "columns": frame.columns,
            "rows": len(frame.collect()),
        }
    except Exception as exc:
        out["dry_run_on_append_only"] = {"accepted": False, **error_summary(exc)}
    try:
        frame = spark.sql(f"VACUUM delta.`{path}` LITE RETAIN 168 HOURS DRY RUN")
        out["lite_dry_run"] = {"accepted": True, "rows": len(frame.collect())}
    except Exception as exc:
        out["lite_dry_run"] = {"accepted": False, **error_summary(exc)}
    try:
        spark.sql(f"VACUUM delta.`{path}` RETAIN 0 HOURS DRY RUN").collect()
        out["below_floor_with_check_on"] = {"refused": False}
    except Exception as exc:
        out["below_floor_with_check_on"] = {"refused": True, **error_summary(exc)}
    return out


def append_rows(spark: Any, path: Path, rows: list[tuple[int, int, int]]) -> int:
    """(partition, offset, batch) rows, in one file."""
    frame = spark.createDataFrame([(TID, pa, o, b, f"b{b}") for pa, o, b in rows], ROW_SCHEMA)
    frame.coalesce(1).write.format("delta").mode("append").save(str(path))
    return latest_version(path)


def x11_idempotence_and_structural_alignment(spark: Any, root: Path) -> dict[str, Any]:
    lake = LakeConfig.at(root / "lake")
    out: dict[str, Any] = {}
    source = TableRef(Tier.BRONZE, "idempotence")
    path = source.local_path(lake)
    create_source(spark, path, {"delta.appendOnly": "false"})
    for i, r in enumerate(THREE_BATCHES):
        append(spark, path, i, r)
    key = f"trace_x.retention_floor.{TID}.0"
    v1 = set_properties(spark, path, {key: "8"})
    v2 = set_properties(spark, path, {key: "8"})
    out["same_floor_property_twice"] = {
        "first_version": v1,
        "second_version": v2,
        "second_commit": commit_actions(path, v2) if v2 != v1 else None,
    }
    d1 = delete_below(spark, path, ALIGNED_FLOORS)
    d2 = delete_below(spark, path, ALIGNED_FLOORS)
    out["same_delete_twice"] = {
        "first_commit": commit_actions(path, d1),
        "second_version": d2,
        "second_commit": commit_actions(path, d2) if d2 != d1 else None,
    }

    # A later batch holds re-read offsets below the floor mixed with new ones (a reset duplicate).
    def mixed(name: str) -> Path:
        ref = TableRef(Tier.BRONZE, name)
        table = ref.local_path(lake)
        create_source(spark, table, {"delta.appendOnly": "false"})
        for i, r in enumerate(THREE_BATCHES):
            append(spark, table, i, r)
        append_rows(
            spark, table, [(0, o, 3) for o in (6, 7, 12, 13)] + [(1, o, 3) for o in (12, 13)]
        )
        return table

    offset_only = mixed("mixed_offset_only")
    v = delete_below(spark, offset_only, ALIGNED_FLOORS)
    out["offset_only_predicate_over_a_mixed_batch"] = {
        "commit": commit_actions(offset_only, v),
        "snapshot": snapshot_offsets(spark, offset_only),
    }
    conjunct = mixed("mixed_batch_conjunct")
    condition = " OR ".join(
        f"(kafka_topic_id = '{TID}' AND kafka_partition = {p} AND kafka_offset < {f} "
        f"AND bronze_batch_id IN (0, 1))"
        for p, f in sorted(ALIGNED_FLOORS.items())
    )
    spark.sql(f"DELETE FROM delta.`{conjunct}` WHERE {condition}")
    v = latest_version(conjunct)
    out["batch_conjunct_predicate_over_a_mixed_batch"] = {
        "commit": commit_actions(conjunct, v),
        "snapshot": snapshot_offsets(spark, conjunct),
    }
    return out


def x12_pinned_batch_reads(spark: Any, root: Path) -> dict[str, Any]:
    """A Gold-like consumer: a batch read of a table at a pinned version, after VACUUM removed
    files that version needs, and after log cleanup removed the version's commit."""
    lake = LakeConfig.at(root / "lake")
    out: dict[str, Any] = {}

    def read_at(path: Path, version: int) -> dict[str, Any]:
        try:
            rows = spark.read.format("delta").option("versionAsOf", str(version)).load(str(path))
            return {"outcome": "read", "rows": len(rows.select("kafka_offset").collect())}
        except Exception as exc:
            return {"outcome": "failed", **error_summary(exc)}

    source = TableRef(Tier.SILVER, "pinned_vacuum")
    path = source.local_path(lake)
    create_source(spark, path, {"delta.appendOnly": "false"})
    for i, r in enumerate(THREE_BATCHES):
        pinned = append(spark, path, i, r)
    out["pinned_version"] = pinned
    out["read_before"] = read_at(path, pinned)
    d = delete_below(spark, path, REWRITE_FLOORS)
    out["delete_commit"] = commit_actions(path, d)
    out["read_after_delete_before_vacuum"] = read_at(path, pinned)
    out["vacuum"] = vacuum_everything(spark, path)
    out["read_after_vacuum"] = read_at(path, pinned)
    out["read_latest_after_vacuum"] = read_at(path, latest_version(path))

    source = TableRef(Tier.SILVER, "pinned_log_cleanup")
    path = source.local_path(lake)
    create_source(
        spark,
        path,
        {"delta.checkpointInterval": "3", "delta.logRetentionDuration": "interval 1 hours"},
    )
    pinned = append(spark, path, 0, THREE_BATCHES[0])
    for i in range(1, 11):
        append(spark, path, i, {0: (100 * i, 100 * i + 2)})
    aged = time.time() - 3 * 86400
    cutoff = latest_version(path) - 2
    for entry in (path / "_delta_log").iterdir():
        if entry.name[:20].isdigit() and int(entry.name[:20]) < cutoff:
            os.utime(entry, (aged, aged))
    append(spark, path, 11, {0: (1100, 1102)})
    retained = tables.retained_log(path)
    out["log_cleanup"] = {
        "pinned_version": pinned,
        "retained": None if retained is None else [retained.earliest, retained.latest],
        "read_at_pinned": read_at(path, pinned),
        "read_at_retained_earliest": None if retained is None else read_at(path, retained.earliest),
    }
    return out


EXPERIMENTS: dict[str, Callable[[Any, Path], dict[str, Any]]] = {
    "x1_append_only_and_floor_property": x1_append_only_and_floor_property,
    "x2_rewrite_delete_under_the_convention_reader": x2_rewrite_delete_under_the_convention_reader,
    "x3_skip_change_commits": x3_skip_change_commits,
    "x4_file_aligned_delete": x4_file_aligned_delete,
    "x5_change_data_feed": x5_change_data_feed,
    "x6_floor_property_commits_under_readers": x6_floor_property_commits_under_readers,
    "x7_durability_through_log_cleanup": x7_durability_through_log_cleanup,
    "x8_domain_metadata": x8_domain_metadata,
    "x9_optimize": x9_optimize,
    "x10_vacuum_facts": x10_vacuum_facts,
    "x11_idempotence_and_structural_alignment": x11_idempotence_and_structural_alignment,
    "x12_pinned_batch_reads": x12_pinned_batch_reads,
}


def main() -> int:
    from trace_core.stream.session import build_session, running_versions

    root = Path(os.environ["TRACE_DELTA_ROOT"])
    selected = sys.argv[1:] or list(EXPERIMENTS)
    spark = build_session("trace-x-step11-retention-spike")
    OBS["environment"] = {
        "running_versions": running_versions(spark),
        "started_at": datetime.now(UTC).isoformat(),
        "selected": selected,
    }
    save()
    failures = 0
    try:
        for name in selected:
            log(f"start {name}")
            started = time.monotonic()
            try:
                OBS[name] = EXPERIMENTS[name](spark, root / name)
            except Exception as exc:
                failures += 1
                OBS[name] = {
                    "SECTION_ERROR": error_summary(exc),
                    "trace": traceback.format_exc()[-3000:],
                }
            finally:
                for query in spark.streams.active:
                    query.stop()
            log(f"end {name} ({time.monotonic() - started:.0f}s)")
            save()
    finally:
        spark.stop()
    OBS["environment"]["finished_at"] = datetime.now(UTC).isoformat()
    save()
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
