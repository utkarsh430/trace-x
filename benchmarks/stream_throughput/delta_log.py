"""Silver's commits, read from the Delta log and the files each commit added. No JVM.

The consumer lag sampler must not perturb the system it measures, and must not need a hook in the
production queries. Every Silver commit is a `_delta_log/<version>.json` file, so the harness reads
those, in version order, and for each one the Parquet files its `add` actions name (three or four
columns, with pyarrow, a locked dependency).

- **Commit time.** The later of the commit's own `commitInfo.timestamp` and the log file's
  modification time: when the commit became visible, never earlier. Both are the host's clock, the
  same clock the Spark driver and the harness read. Choosing the later one can only raise a lag.
- **Rewrites.** A MERGE that rewrites a file (`silver.tx_scored_v1`'s supersede) re-adds rows that
  were already committed. The tracker takes maxima, so a re-added older row changes nothing.
- **Shared tables.** `silver.duplicates` and `silver.quarantine` hold every topic's rows; a row's
  topic is its `silver_topic`.
- **A missing version** (log cleanup, or a version skipped) is an error: the sampler would otherwise
  miss commits and still report a plausible series.
"""

from __future__ import annotations

import json
import urllib.parse
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from benchmarks.stream_throughput.lag import CommitObservation, PartitionKey, PartitionProgress

LOG_DIR: Final = "_delta_log"
_COLUMNS: Final = ("kafka_partition", "kafka_offset", "kafka_timestamp")


class DeltaLogError(RuntimeError):
    """The Delta log cannot be read as a complete, ordered sequence of commits."""


@dataclass(frozen=True, slots=True)
class WatchedTable:
    """A Silver table and how its rows name their topic: fixed, or by `silver_topic`."""

    label: str
    directory: Path
    topic: str | None
    """The canonical table's own topic; None for a shared table (`silver_topic` per row)."""
    topics: frozenset[str]
    """Rows of other topics are ignored."""


def _version_file(directory: Path, version: int) -> Path:
    return directory / LOG_DIR / f"{version:020d}.json"


def _actions(path: Path) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            parsed = json.loads(line)
            if not isinstance(parsed, dict):
                raise DeltaLogError(f"{path}: a log line is not an object")
            actions.append(parsed)
    return actions


def _epoch_ms(values: Any) -> list[int]:
    """Timestamp column values as epoch milliseconds, whatever unit the file stored."""
    import pyarrow as pa
    import pyarrow.compute as pc

    # Spark writes UTC instants, as INT96 or as microseconds; either way the stored value is
    # counted from the epoch, so casting within the same time zone setting keeps it.
    as_us = pc.cast(values, pa.timestamp("us", tz=values.type.tz))
    micros = pc.cast(as_us, pa.int64()).to_pylist()
    return [int(v) // 1000 for v in micros if v is not None]


def read_added_rows(
    table: WatchedTable, add_paths: Iterable[str]
) -> dict[PartitionKey, PartitionProgress]:
    """Per partition, the newest LogAppendTime and highest offset among the added rows."""
    import pyarrow.parquet as pq

    newest: dict[PartitionKey, PartitionProgress] = {}
    columns = [*_COLUMNS] + ([] if table.topic is not None else ["silver_topic"])
    for relative in add_paths:
        path = table.directory / urllib.parse.unquote(relative)
        data = pq.read_table(path, columns=columns)
        partitions = data.column("kafka_partition").to_pylist()
        offsets = data.column("kafka_offset").to_pylist()
        stamps = _epoch_ms(data.column("kafka_timestamp"))
        if len(stamps) != len(partitions):
            raise DeltaLogError(f"{path}: a row has no kafka_timestamp")
        topics = (
            [table.topic] * len(partitions)
            if table.topic is not None
            else data.column("silver_topic").to_pylist()
        )
        for topic, partition, offset, stamp in zip(
            topics, partitions, offsets, stamps, strict=True
        ):
            if topic not in table.topics:
                continue
            key = PartitionKey(str(topic), int(partition))
            seen = newest.get(key)
            newest[key] = PartitionProgress(
                max(stamp, seen.newest_log_append_ms) if seen else stamp,
                max(int(offset), seen.max_offset) if seen else int(offset),
            )
    return newest


def read_commit(table: WatchedTable, version: int) -> CommitObservation:
    path = _version_file(table.directory, version)
    actions = _actions(path)
    info = next((a["commitInfo"] for a in actions if "commitInfo" in a), None)
    stamp = int(info["timestamp"]) if isinstance(info, dict) and "timestamp" in info else 0
    visible = path.stat().st_mtime_ns // 1_000_000
    adds = [str(a["add"]["path"]) for a in actions if "add" in a]
    return CommitObservation(
        table=table.label,
        version=version,
        committed_ms=max(stamp, visible),
        partitions=read_added_rows(table, adds),
    )


class CommitReader:
    """Reads a table's commits incrementally, in version order, from version 0."""

    def __init__(self, table: WatchedTable) -> None:
        self.table = table
        self.next_version = 0

    def poll(self) -> list[CommitObservation]:
        found: list[CommitObservation] = []
        if not (self.table.directory / LOG_DIR).is_dir():
            return found
        while _version_file(self.table.directory, self.next_version).is_file():
            found.append(read_commit(self.table, self.next_version))
            self.next_version += 1
        later = self._later_versions()
        # Re-checked: the next version may have landed between the loop and the listing.
        if later and not _version_file(self.table.directory, self.next_version).is_file():
            raise DeltaLogError(
                f"{self.table.label}: version {self.next_version} is missing but {later[:3]} "
                f"exist; the sampler would miss commits"
            )
        return found

    def _later_versions(self) -> list[int]:
        versions = []
        for entry in (self.table.directory / LOG_DIR).glob("*.json"):
            stem = entry.stem
            if stem.isdigit() and int(stem) > self.next_version:
                versions.append(int(stem))
        return sorted(versions)


def watched_tables(lake_root: Path, topics: Iterable[str]) -> tuple[WatchedTable, ...]:
    """Every Silver table a mix topic's records are committed to."""
    from trace_core.stream.lake import LakeConfig
    from trace_core.stream.silver_rules import DUPLICATES, QUARANTINE, silver_topic

    lake = LakeConfig(root=lake_root)
    chosen = frozenset(topics)
    tables = [
        WatchedTable(
            str(silver_topic(topic).table),
            silver_topic(topic).table.local_path(lake),
            topic,
            frozenset({topic}),
        )
        for topic in sorted(chosen)
    ]
    for shared in (DUPLICATES, QUARANTINE):
        tables.append(WatchedTable(str(shared), shared.local_path(lake), None, chosen))
    return tuple(tables)


def read_all(tables: Iterable[WatchedTable]) -> list[CommitObservation]:
    commits: list[CommitObservation] = []
    for table in tables:
        commits.extend(CommitReader(table).poll())
    return commits
