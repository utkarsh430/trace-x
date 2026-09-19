"""`bronze_maintenance`: the lake maintenance entrypoint (`python -m services.stream.maintenance`).

Thin by design: every decision lives in `trace_core.stream.maintenance` (ADR-0052 amendment 1).
Local development only.

    retention      advance one topic's Bronze retention floors -- to the given
                   --floor PARTITION=OFFSET values, or as far as every consumer allows -- and
                   delete the whole batches they retire
    vacuum         VACUUM one declared table (--table tier.name), refused while a consumer still
                   needs its removed files, or below its declared retention
    optimize       OPTIMIZE one declared table; refused on Bronze
    silver-reset   reset one topic's Silver checkpoint to Bronze's retention start

Exit codes: 0 done; 2 refused before the next commit (every blocker is logged); 3 a maintenance
commit contradicted its plan (loud: a Silver reader stops at it, and the drift check refuses a
table left without appendOnly).

Provenance is required, never invented (`services.stream.bronze.provenance`).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from collections.abc import Sequence
from typing import Any, Final

from services.stream.bronze import (
    EXIT_OK,
    EXIT_REFUSED,
    KAFKA_BOOTSTRAP_ENV,
    ProvenanceUnavailableError,
    provenance,
)
from trace_core.domain.errors import ContractError, LakeContractError, TraceXError
from trace_core.observability import configure_logging, get_logger
from trace_core.stream.lake import LakeConfig, Tier
from trace_core.stream.tables import TableRef

EXIT_DEFECT: Final = 3

_log = get_logger("services.stream.maintenance")


def parse_floors(values: Sequence[str] | None) -> dict[int, int] | None:
    """`["0=120", "3=77"]` -> `{0: 120, 3: 77}`; None when no floor was given."""
    if not values:
        return None
    floors: dict[int, int] = {}
    for value in values:
        partition, sep, offset = value.partition("=")
        if not sep or not partition.isdigit() or not offset.isdigit():
            raise ContractError(f"--floor {value!r} is not PARTITION=OFFSET")
        if int(partition) in floors:
            raise ContractError(f"--floor names partition {partition} twice")
        floors[int(partition)] = int(offset)
    return floors


def parse_table(value: str) -> TableRef:
    """`bronze.tx_raw_v1` -> the table reference."""
    tier, sep, name = value.partition(".")
    if not sep:
        raise ContractError(f"--table {value!r} is not tier.name")
    try:
        return TableRef(Tier(tier), name)
    except ValueError as exc:
        raise ContractError(f"--table {value!r}: {exc}") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m services.stream.maintenance")
    parser.add_argument("--driver-memory", default="1g")
    commands = parser.add_subparsers(dest="command", required=True)
    retention = commands.add_parser("retention", help="advance a topic's Bronze retention floor")
    retention.add_argument("--topic", required=True)
    retention.add_argument("--bootstrap", default=os.environ.get(KAFKA_BOOTSTRAP_ENV, ""))
    retention.add_argument("--floor", action="append", metavar="PARTITION=OFFSET")
    vacuum = commands.add_parser("vacuum", help="VACUUM one declared table, guarded")
    vacuum.add_argument("--table", required=True)
    vacuum.add_argument("--retain-hours", type=int, default=None)
    optimize = commands.add_parser("optimize", help="OPTIMIZE one declared table (not Bronze)")
    optimize.add_argument("--table", required=True)
    reset = commands.add_parser("silver-reset", help="reset Silver to Bronze's retention start")
    reset.add_argument("--topic", required=True)
    reset.add_argument("--reason", required=True)
    return parser


def _retention(spark: Any, lake: LakeConfig, args: argparse.Namespace) -> dict[str, Any]:
    from trace_core.stream.bronze import fetch_topic_identity
    from trace_core.stream.maintenance import advance_retention

    if not args.bootstrap.strip():
        raise LakeContractError(
            f"retention judges Bronze conservation against the broker's topic id; set --bootstrap "
            f"or {KAFKA_BOOTSTRAP_ENV}"
        )
    git_sha, dirty = provenance()
    report = advance_retention(
        spark,
        lake,
        args.topic,
        requested_floors=parse_floors(args.floor),
        broker_topic_id=fetch_topic_identity(args.bootstrap.strip(), args.topic).topic_id,
        git_sha=git_sha,
        dirty_worktree=dirty,
        now=dt.datetime.now(dt.UTC),
    )
    return report.summary()


def _vacuum(spark: Any, lake: LakeConfig, args: argparse.Namespace) -> dict[str, Any]:
    from trace_core.stream.maintenance import vacuum_table

    report = vacuum_table(spark, lake, parse_table(args.table), retain_hours=args.retain_hours)
    return {
        "table": report.table,
        "retain_hours": report.retain_hours,
        "removal_version": report.removal_version,
        "files_deleted": report.files_deleted,
        "consumers": {c.name: c.version for c in report.consumers},
    }


def _optimize(spark: Any, lake: LakeConfig, args: argparse.Namespace) -> dict[str, Any]:
    from trace_core.stream.maintenance import optimize_table

    ref = parse_table(args.table)
    return {"table": str(ref), "version": optimize_table(spark, lake, ref)}


def _silver_reset(spark: Any, lake: LakeConfig, args: argparse.Namespace) -> dict[str, Any]:
    from trace_core.stream.maintenance import reset_silver

    identity = reset_silver(
        spark, lake, args.topic, reason=args.reason, now=dt.datetime.now(dt.UTC)
    )
    return {
        "query": identity.query,
        "checkpoint_version": identity.version,
        "sources": {key: record.start for key, record in identity.sources.items()},
    }


def main(argv: Sequence[str] | None = None) -> int:
    from trace_core.stream.maintenance import MaintenanceDefectError

    args = _parser().parse_args(argv)
    configure_logging()
    try:
        lake = LakeConfig.from_env()
        from trace_core.stream.session import build_session

        spark = build_session(
            f"trace-x-maintenance-{args.command}", driver_memory=args.driver_memory
        )
    except (LakeContractError, TraceXError) as exc:
        _log.error("maintenance_refused", error=str(exc))
        return EXIT_REFUSED
    handlers = {
        "retention": _retention,
        "vacuum": _vacuum,
        "optimize": _optimize,
        "silver-reset": _silver_reset,
    }
    try:
        summary = handlers[args.command](spark, lake, args)
    except MaintenanceDefectError as exc:
        _log.error("maintenance_defect", command=args.command, error=str(exc))
        return EXIT_DEFECT
    except (LakeContractError, ContractError, ProvenanceUnavailableError) as exc:
        _log.error("maintenance_refused", command=args.command, error=str(exc))
        return EXIT_REFUSED
    finally:
        spark.stop()
    sys.stdout.write(json.dumps(summary, sort_keys=True) + "\n")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
