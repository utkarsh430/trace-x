"""`silver_transform`: the Silver job's entrypoint (`python -m services.stream.silver`).

Thin by design: every decision lives in `trace_core.stream.silver`, `silver_rules` and
`silver_conservation` (ADR-0053).

    run            start one query per released topic (or per --topic), each reading its Bronze
                   table, and keep them running, or drain what Bronze holds and stop
                   (--available-now)
    conservation   judge every Silver topic against its Bronze table; exit 1 unless all conserved

Exit codes, as the Bronze job's: 0 ok; 1 not conserved; 2 refused before anything ran (a lake,
checkpoint or contract refusal, a missing Bronze table, or no provenance); 3 a running query failed,
including a uniqueness assertion. Queries are supervised by the Bronze job's `_supervise`, so a
failure that races a stop or a clean termination is still reported.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import signal
import sys
from collections.abc import Callable, Sequence
from typing import Any, Final

from services.stream.bronze import (
    EXIT_FAILED_CHECK,
    EXIT_OK,
    EXIT_REFUSED,
    ProvenanceUnavailableError,
    _supervise,
    provenance,
)
from trace_core.domain.errors import ContractError, LakeContractError, TraceXError
from trace_core.observability import configure_logging, get_logger
from trace_core.stream.lake import LakeConfig

FAILURE_EVENT: Final = "silver_query_failed"

_log = get_logger(__name__)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m services.stream.silver")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="start the Silver queries")
    run.add_argument("--topic", action="append", help="a released topic (default: all)")
    mode = run.add_mutually_exclusive_group(required=True)
    mode.add_argument("--available-now", action="store_true")
    mode.add_argument("--interval-s", type=float)
    run.add_argument("--driver-memory", default="1g")
    check = commands.add_parser("conservation", help="judge Silver against Bronze")
    check.add_argument("--topic", action="append")
    return parser


def supervise(
    streams: Any,
    handles: Sequence[Any],
    *,
    stop_requested: Callable[[], bool],
    query_failure: type[BaseException],
) -> int:
    """The Silver queries under the Bronze job's supervision loop: EXIT_OK once every query has
    stopped without failing, EXIT_QUERY_FAILED once any has failed."""
    return _supervise(
        streams,
        handles,
        stop_requested=stop_requested,
        progress=lambda _handle: None,
        query_failure=query_failure,
        failure_event=FAILURE_EVENT,
    )


def _run(spark: Any, lake: LakeConfig, args: argparse.Namespace) -> int:
    from pyspark.errors import StreamingQueryException

    from trace_core.stream.bronze import Trigger, released_topics
    from trace_core.stream.silver import start_silver_query

    sha, dirty = provenance()
    trigger = (
        Trigger(available_now=True) if args.available_now else Trigger(interval_s=args.interval_s)
    )
    handles = [
        start_silver_query(
            spark,
            lake,
            topic,
            git_sha=sha,
            dirty_worktree=dirty,
            now=dt.datetime.now(dt.UTC),
            trigger=trigger,
        )
        for topic in released_topics(args.topic)
    ]
    stopping = {"requested": False}

    def request_stop(signum: int, _frame: object) -> None:
        stopping["requested"] = True
        _log.warning("silver_stop_requested", signal=signum)

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    return supervise(
        spark.streams,
        handles,
        stop_requested=lambda: stopping["requested"],
        query_failure=StreamingQueryException,
    )


def _conservation(spark: Any, lake: LakeConfig, args: argparse.Namespace) -> int:
    from trace_core.stream.bronze import released_topics
    from trace_core.stream.silver_conservation import check_silver_conservation

    reports = [
        check_silver_conservation(spark, lake, topic) for topic in released_topics(args.topic)
    ]
    sys.stdout.write(json.dumps([r.summary() for r in reports], sort_keys=True) + "\n")
    return EXIT_OK if all(r.conserved for r in reports) else EXIT_FAILED_CHECK


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    configure_logging()
    try:
        lake = LakeConfig.from_env()
        from trace_core.stream.session import build_session

        spark = build_session(
            f"trace-x-{args.command}-silver",
            driver_memory=getattr(args, "driver_memory", "1g"),
        )
    except (LakeContractError, TraceXError) as exc:
        _log.error("silver_refused", error=str(exc))
        return EXIT_REFUSED
    try:
        handler = {"run": _run, "conservation": _conservation}
        return handler[args.command](spark, lake, args)
    except (LakeContractError, ContractError, ProvenanceUnavailableError) as exc:
        _log.error("silver_refused", command=args.command, error=str(exc))
        return EXIT_REFUSED
    finally:
        spark.stop()


if __name__ == "__main__":
    from trace_core.stream.session import run_driver

    run_driver(main)
