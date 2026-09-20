"""The consumer under test: the real Bronze and Silver queries, in one JVM (PHASE3_PLAN B8).

`python -m benchmarks.stream_throughput.consumer --topic ... --bronze-trigger-s ...`

Nothing here decides anything about the data. It builds the session through the project's only
session factory (`build_session`, with the benchmark's declared master, driver memory and shuffle
partitions), starts `start_bronze_query` for every topic, then `start_silver_query` for every topic,
each on a `processingTime` trigger -- never `availableNow` (PHASE3_PLAN §4.3) -- and supervises all
of them with the Bronze job's own `_supervise`, so a failed query ends the process with exit 3
exactly as `services.stream.bronze` would. SIGTERM stops every query and exits 0: the outage.

Why its own entrypoint rather than `services.stream.bronze run` and `services.stream.silver run`:
those build `local[2]` sessions with fixed shuffle partitions, two JVMs; the benchmark must declare
and record the resources the consumer ran on, on a laptop that cannot afford a spare JVM.

Exit codes, as the stream jobs': 0 stopped cleanly; 2 refused before anything ran; 3 a query failed.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import signal
import sys
from collections.abc import Sequence
from typing import Any, Final

from services.stream.bronze import (
    EXIT_OK,
    EXIT_REFUSED,
    KAFKA_BOOTSTRAP_ENV,
    ProvenanceUnavailableError,
    _supervise,
    provenance,
)

from trace_core.domain.errors import ContractError, LakeContractError, TraceXError
from trace_core.observability import configure_logging, get_logger
from trace_core.stream.lake import LakeConfig

FAILURE_EVENT: Final = "load_stream_query_failed"
APP_NAME: Final = "trace-x-load-stream-consumer"

_log = get_logger(__name__)
_STOPPING: dict[str, bool] = {"requested": False}


def _request_stop(signum: int, _frame: object) -> None:
    """Installed before the session is built, so a stop during start-up is honoured too."""
    _STOPPING["requested"] = True
    _log.warning("load_stream_consumer_stop_requested", signal=signum)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.stream_throughput.consumer")
    parser.add_argument("--topic", action="append", required=True)
    parser.add_argument("--bootstrap", default=os.environ.get(KAFKA_BOOTSTRAP_ENV, ""))
    parser.add_argument("--bronze-trigger-s", type=float, required=True)
    parser.add_argument("--silver-trigger-s", type=float, required=True)
    parser.add_argument("--max-offsets-per-trigger", type=int)
    parser.add_argument("--max-files-per-trigger", type=int)
    parser.add_argument("--max-bytes-per-trigger", type=int)
    parser.add_argument("--master", required=True)
    parser.add_argument("--driver-memory", required=True)
    parser.add_argument("--shuffle-partitions", type=int, required=True)
    return parser


def _run(spark: Any, lake: LakeConfig, args: argparse.Namespace) -> int:
    from pyspark.errors import StreamingQueryException

    from trace_core.stream.bronze import BronzeQuery, Trigger, record_progress, start_bronze_query
    from trace_core.stream.silver import start_silver_query

    sha, dirty = provenance()
    bronze = [
        start_bronze_query(
            spark,
            lake,
            topic,
            bootstrap_servers=args.bootstrap,
            git_sha=sha,
            dirty_worktree=dirty,
            now=dt.datetime.now(dt.UTC),
            trigger=Trigger(interval_s=args.bronze_trigger_s),
            max_offsets_per_trigger=args.max_offsets_per_trigger,
        )
        for topic in args.topic
    ]
    silver = [
        start_silver_query(
            spark,
            lake,
            topic,
            git_sha=sha,
            dirty_worktree=dirty,
            now=dt.datetime.now(dt.UTC),
            trigger=Trigger(interval_s=args.silver_trigger_s),
            max_files_per_trigger=args.max_files_per_trigger,
            max_bytes_per_trigger=args.max_bytes_per_trigger,
        )
        for topic in args.topic
    ]
    seen: dict[str, set[int]] = {h.spec.topic: set() for h in bronze}

    def progress(handle: Any) -> None:
        if isinstance(handle, BronzeQuery):
            record_progress(handle, seen[handle.spec.topic])

    _log.info("load_stream_consumer_started", topics=list(args.topic), queries=len(bronze) * 2)
    return _supervise(
        spark.streams,
        [*bronze, *silver],
        stop_requested=lambda: _STOPPING["requested"],
        progress=progress,
        query_failure=StreamingQueryException,
        failure_event=FAILURE_EVENT,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    configure_logging()
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    if not args.bootstrap.strip():
        _log.error(
            "load_stream_consumer_refused", error=f"set --bootstrap or {KAFKA_BOOTSTRAP_ENV}"
        )
        return EXIT_REFUSED
    try:
        lake = LakeConfig.from_env()
        from trace_core.stream.session import build_session

        spark = build_session(
            APP_NAME,
            master=args.master,
            shuffle_partitions=args.shuffle_partitions,
            driver_memory=args.driver_memory,
        )
    except (LakeContractError, TraceXError) as exc:
        _log.error("load_stream_consumer_refused", error=str(exc))
        return EXIT_REFUSED
    try:
        if _STOPPING["requested"]:
            return EXIT_OK
        return _run(spark, lake, args)
    except (LakeContractError, ContractError, ProvenanceUnavailableError, TraceXError) as exc:
        _log.error("load_stream_consumer_refused", error=str(exc))
        return EXIT_REFUSED
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
