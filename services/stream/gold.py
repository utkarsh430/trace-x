"""`gold_build`: the Gold job's entrypoint (`python -m services.stream.gold`).

Thin by design: every decision lives in `trace_core.stream.gold`, `gold_plan` and `gold_features`
(ADR-0055).

    build   run the next Gold build (or replay a planned one) and print its record as JSON
    check   judge the latest committed build against its record and Silver at its pins;
            exit 1 unless consistent

Exit codes, as the Bronze and Silver jobs': 0 ok; 1 not consistent; 2 refused before anything was
written (a lake, checkpoint, plan or contract refusal, a missing Silver table, or no provenance);
3 the build failed while running on Spark.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from collections.abc import Sequence
from typing import Any, Final

from services.stream.bronze import (
    EXIT_FAILED_CHECK,
    EXIT_OK,
    EXIT_QUERY_FAILED,
    EXIT_REFUSED,
    ProvenanceUnavailableError,
    provenance,
)
from trace_core.domain.errors import ContractError, LakeContractError, TraceXError
from trace_core.observability import configure_logging, get_logger
from trace_core.stream.lake import LakeConfig

EXIT_BUILD_FAILED: Final = EXIT_QUERY_FAILED

_log = get_logger(__name__)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m services.stream.gold")
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build", help="run the next Gold build")
    build.add_argument("--driver-memory", default="2g")
    commands.add_parser("check", help="judge the latest Gold build")
    return parser


def _build(spark: Any, lake: LakeConfig, _args: argparse.Namespace) -> int:
    from pyspark.errors import PySparkException

    from trace_core.stream.gold import build_gold

    sha, dirty = provenance()
    try:
        result = build_gold(
            spark, lake, git_sha=sha, dirty_worktree=dirty, now=dt.datetime.now(dt.UTC)
        )
    except PySparkException as exc:
        _log.error("gold_build_failed", error=f"{type(exc).__name__}: {str(exc)[:2000]}")
        return EXIT_BUILD_FAILED
    summary = {**result.record.summary(), "replayed": result.replayed}
    sys.stdout.write(json.dumps(summary, sort_keys=True) + "\n")
    return EXIT_OK


def _check(spark: Any, lake: LakeConfig, _args: argparse.Namespace) -> int:
    from trace_core.stream.gold import check_gold

    report = check_gold(spark, lake)
    sys.stdout.write(json.dumps(report.summary(), sort_keys=True) + "\n")
    return EXIT_OK if report.consistent else EXIT_FAILED_CHECK


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    configure_logging()
    try:
        lake = LakeConfig.from_env()
        from trace_core.stream.session import build_session

        spark = build_session(
            f"trace-x-{args.command}-gold", driver_memory=getattr(args, "driver_memory", "2g")
        )
    except (LakeContractError, TraceXError) as exc:
        _log.error("gold_refused", error=str(exc))
        return EXIT_REFUSED
    try:
        handler = {"build": _build, "check": _check}
        return handler[args.command](spark, lake, args)
    except (LakeContractError, ContractError, ProvenanceUnavailableError) as exc:
        _log.error("gold_refused", command=args.command, error=str(exc))
        return EXIT_REFUSED
    finally:
        spark.stop()


if __name__ == "__main__":
    from trace_core.stream.session import run_driver

    run_driver(main)
