"""The PARITY run record (ADR-0056 §6).

One JSON document per run, in `eval/manifest/` for a measured run. It carries enough to reproduce
the run and to attribute every number: the commit and its dirty flag, the eval-v2 manifest digest,
the overlay version and seed, the lateness model's digest, the Spark, Delta, Hadoop, Scala, Java and
Python versions, the gateway, counts per feature and stratum, the results and the guard. A
diagnostic record is never publishable.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

RECORD_TYPE: Final = "PARITY"
REQUIRED_FIELDS: Final = (
    "record_type",
    "run_id",
    "mode",
    "publishable",
    "track",
    "git_commit_sha",
    "dirty_worktree",
    "env_lock_digest",
    "python_version",
    "started_at",
    "finished_at",
    "parity_semantics_version",
    "feature_set_version",
    "partition",
    "dataset",
    "overlay",
    "lateness_model",
    "lateness_model_digest",
    "toolchain",
    "gateway",
    "counts",
    "results",
    "guard",
    "verdict",
)
MEASURED_ONLY: Final = ("eval_v2_manifest_digest", "gateway_image_digest")
"""Null is allowed in a diagnostic record, never in a measured one."""


def run_id_for(name: str, started: dt.datetime, git_sha: str) -> str:
    return f"parity-{started.astimezone(dt.UTC):%Y%m%d-%H%M%S}-{name}-{git_sha[:8]}"


def missing_fields(record: Mapping[str, Any]) -> list[str]:
    missing = [
        f for f in REQUIRED_FIELDS if f not in record or record[f] is None or record[f] == ""
    ]
    if record.get("record_type") != RECORD_TYPE:
        missing.append("record_type must be PARITY")
    if record.get("mode") == "measured":
        dataset = record.get("dataset") or {}
        gateway = record.get("gateway") or {}
        if not dataset.get("eval_v2_manifest_digest"):
            missing.append("dataset.eval_v2_manifest_digest")
        if not gateway.get("image_digest"):
            missing.append("gateway.image_digest")
    if record.get("publishable") and (
        record.get("mode") != "measured" or record.get("dirty_worktree")
    ):
        missing.append("publishable requires a measured run on a clean worktree")
    return missing


def write_record(record: Mapping[str, Any], directory: Path) -> Path:
    if problems := missing_fields(record):
        raise ValueError(f"refusing to write an incomplete PARITY record: {problems}")
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{record['run_id']}.json"
    if path.exists():
        raise FileExistsError(f"{path} exists; a run record is never overwritten")
    path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
