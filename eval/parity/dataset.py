"""A frozen partition's events from the eval-v2 dataset, verified before use (ADR-0056 §4).

Refuses a dataset whose manifest digest or any stream file's SHA-256 differs from the manifest, and
a file carrying anything but `envelope` and `payload`, so no label field can reach the gateway
(CLAUDE.md §11).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

from eval.parity.partition import Partition
from eval.parity.served import parse_time

REPLAY_ORDER: Final[Mapping[str, int]] = {
    "identity.events.v1": 1,
    "tx.raw.v1": 2,
    "tx.authorization.v1": 3,
}
"""At one instant, as `eval.replay.gateway_replay.REPLAY_ORDER` merges them (ADR-0049 §7)."""

Stream = list[tuple[str, dict[str, Any]]]


class DatasetRefusedError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_dataset(dataset_dir: Path, manifest_path: Path, partition: Partition) -> dict[str, Any]:
    raw = manifest_path.read_bytes()
    manifest = json.loads(raw)
    if manifest.get("dataset_digest") != partition.dataset_digest:
        raise DatasetRefusedError(
            f"{manifest_path} declares {manifest.get('dataset_digest')}, the partition was frozen "
            f"on {partition.dataset_digest}"
        )
    files: dict[str, str] = {}
    for stream in partition.streams:
        name = f"{stream}.parquet"
        want = manifest["files"][name]["sha256"]
        got = _sha256(dataset_dir / name)
        if got != want:
            raise DatasetRefusedError(
                f"{dataset_dir / name} is sha256:{got}, the manifest says {want}"
            )
        files[name] = f"sha256:{got}"
    return {
        "dataset_version": manifest.get("dataset_version"),
        "dataset_digest": manifest["dataset_digest"],
        "eval_v2_manifest_digest": "sha256:" + hashlib.sha256(raw).hexdigest(),
        "eval_v2_manifest": partition.dataset_manifest,
        "files": files,
    }


def load_partition(dataset_dir: Path, partition: Partition) -> tuple[Stream, Stream]:
    """`(warm-up, slice)`, each merged in event-time order."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    low = partition.warmup_start
    starts, ends = partition.start, partition.end
    bounds = (f"{low:%Y-%m-%dT%H:%M:%S}", f"{ends:%Y-%m-%dT%H:%M:%S.999}Z")
    warmup: list[tuple[Any, int, int, str, dict[str, Any]]] = []
    in_slice: list[tuple[Any, int, int, str, dict[str, Any]]] = []
    for stream in partition.streams:
        handle = pq.ParquetFile(dataset_dir / f"{stream}.parquet")
        if set(handle.schema_arrow.names) != {"envelope", "payload"}:
            raise DatasetRefusedError(f"{stream} carries {handle.schema_arrow.names}")
        for batch in handle.iter_batches(batch_size=50_000):
            occurred = pc.struct_field(batch.column("envelope"), "occurred_at")
            keep = pc.and_(
                pc.greater_equal(occurred, bounds[0]), pc.less_equal(occurred, bounds[1])
            )
            for index, row in enumerate(batch.filter(keep).to_pylist()):
                moment = parse_time(row["envelope"]["occurred_at"])
                entry = (moment, REPLAY_ORDER[stream], index, stream, row)
                if low <= moment < starts:
                    warmup.append(entry)
                elif starts <= moment < ends:
                    in_slice.append(entry)
    warmup.sort(key=lambda e: e[:3])
    in_slice.sort(key=lambda e: e[:3])
    return [(e[3], e[4]) for e in warmup], [(e[3], e[4]) for e in in_slice]
