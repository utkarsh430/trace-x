"""`bronze_ingest`: the Bronze job's entrypoint (`python -m services.stream.bronze`).

Thin by design: every decision lives in `trace_core.stream.bronze`, `bronze_conservation` and
`bronze_coverage`.

    run            start one query per released topic (or per --topic) and keep them running, or
                   drain what is published and stop (--available-now)
    conservation   judge every Bronze table against its checkpoint; exit 1 unless all conserved
    coverage       apply the observation log's coverage rule to Bronze and the session ledger

Exit codes: 0 ok; 1 not conserved, or a coverage gap; 2 refused before anything ran (a lake,
checkpoint or contract refusal, or no provenance); 3 a running query failed -- including Kafka data
loss under failOnDataLoss=true, which stops every query rather than letting the others run on.

Provenance is required, never invented: the checkout's `git rev-parse HEAD` and `git status
--porcelain`, or `TRACE_GIT_SHA` with `TRACE_DIRTY_WORKTREE` set explicitly (for an image without a
checkout).
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import json
import os
import shutil
import signal
import subprocess
import sys
from collections.abc import Sequence
from typing import Any, Final

from trace_core.domain.errors import ContractError, LakeContractError, TraceXError
from trace_core.observability import configure_logging, get_logger
from trace_core.stream.lake import LakeConfig, source_checkout_root
from trace_core.stream.tables import require_git_sha

EXIT_OK: Final = 0
EXIT_FAILED_CHECK: Final = 1
EXIT_REFUSED: Final = 2
EXIT_QUERY_FAILED: Final = 3
GIT_SHA_ENV: Final = "TRACE_GIT_SHA"
DIRTY_ENV: Final = "TRACE_DIRTY_WORKTREE"
KAFKA_BOOTSTRAP_ENV: Final = "TRACE_KAFKA_BOOTSTRAP"

_log = get_logger("services.stream.bronze")


class ProvenanceUnavailableError(TraceXError):
    """No commit id can be attributed to the writes this job would make."""


def _git(*args: str) -> str:
    root = source_checkout_root()
    binary = shutil.which("git")
    if root is None or binary is None:
        raise ProvenanceUnavailableError(
            f"no source checkout or git binary; set {GIT_SHA_ENV} and {DIRTY_ENV}"
        )
    done = subprocess.run(  # noqa: S603 -- literal arguments, resolved binary
        [binary, *args], cwd=root, capture_output=True, text=True, timeout=30, check=False
    )
    if done.returncode != 0:
        raise ProvenanceUnavailableError(f"git {' '.join(args)} failed: {done.stderr.strip()}")
    return done.stdout


def provenance(environ: dict[str, str] | None = None) -> tuple[str, bool]:
    env = dict(os.environ) if environ is None else environ
    if GIT_SHA_ENV in env:
        dirty = env.get(DIRTY_ENV, "").strip().lower()
        if dirty not in {"true", "false"}:
            raise ProvenanceUnavailableError(
                f"{GIT_SHA_ENV} is set, so {DIRTY_ENV} must be 'true' or 'false', not {dirty!r}"
            )
        return require_git_sha(env[GIT_SHA_ENV].strip()), dirty == "true"
    return require_git_sha(_git("rev-parse", "HEAD").strip()), bool(_git("status", "--porcelain"))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m services.stream.bronze")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="start the Bronze queries")
    run.add_argument("--bootstrap", default=os.environ.get(KAFKA_BOOTSTRAP_ENV, ""))
    run.add_argument("--topic", action="append", help="a released topic (default: all)")
    mode = run.add_mutually_exclusive_group(required=True)
    mode.add_argument("--available-now", action="store_true")
    mode.add_argument("--interval-s", type=float)
    run.add_argument("--max-offsets-per-trigger", type=int)
    run.add_argument("--driver-memory", default="1g")
    check = commands.add_parser("conservation", help="judge Bronze against its checkpoints")
    check.add_argument("--bootstrap", default=os.environ.get(KAFKA_BOOTSTRAP_ENV, ""))
    check.add_argument("--topic", action="append")
    coverage = commands.add_parser("coverage", help="apply the coverage rule to Bronze")
    coverage.add_argument("--clock-margin-s", type=float, required=True)
    # Default: the gateway writer's own lease and takeover margin (ADR-0051 §2).
    coverage.add_argument("--lease-s", type=float, default=None)
    coverage.add_argument("--takeover-margin-s", type=float, default=None)
    return parser


def _run(spark: Any, lake: LakeConfig, args: argparse.Namespace) -> int:
    from pyspark.errors import StreamingQueryException

    from trace_core.stream.bronze import (
        Trigger,
        record_progress,
        released_topics,
        start_bronze_query,
    )

    sha, dirty = provenance()
    trigger = (
        Trigger(available_now=True) if args.available_now else Trigger(interval_s=args.interval_s)
    )
    handles = [
        start_bronze_query(
            spark,
            lake,
            topic,
            bootstrap_servers=args.bootstrap,
            git_sha=sha,
            dirty_worktree=dirty,
            now=dt.datetime.now(dt.UTC),
            trigger=trigger,
            max_offsets_per_trigger=args.max_offsets_per_trigger,
        )
        for topic in released_topics(args.topic)
    ]
    seen: dict[str, set[int]] = {h.spec.topic: set() for h in handles}
    stopping = {"requested": False}

    def request_stop(signum: int, _frame: object) -> None:
        stopping["requested"] = True
        _log.warning("bronze_stop_requested", signal=signum)

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        while True:
            for handle in handles:
                record_progress(handle, seen[handle.spec.topic])
                failure = handle.query.exception()
                if failure is not None:
                    _log.error(
                        "bronze_query_failed", topic=handle.spec.topic, error=str(failure)[:2000]
                    )
                    return EXIT_QUERY_FAILED
            if all(not h.query.isActive for h in handles) or stopping["requested"]:
                return EXIT_OK
            # A failed query rethrows here; it is recorded on its query and reported above.
            with contextlib.suppress(StreamingQueryException):
                spark.streams.awaitAnyTermination(5)
            spark.streams.resetTerminated()
    finally:
        for handle in handles:
            if handle.query.isActive:
                handle.query.stop()


def _conservation(spark: Any, lake: LakeConfig, args: argparse.Namespace) -> int:
    from trace_core.stream.bronze import fetch_topic_identity, released_topics
    from trace_core.stream.bronze_conservation import check_conservation

    if not args.bootstrap.strip():
        raise LakeContractError(
            f"conservation compares each checkpoint's recorded topic id with the broker's; set "
            f"--bootstrap or {KAFKA_BOOTSTRAP_ENV}"
        )
    reports = [
        check_conservation(
            spark,
            lake,
            topic,
            broker_topic_id=fetch_topic_identity(args.bootstrap.strip(), topic).topic_id,
        )
        for topic in released_topics(args.topic)
    ]
    sys.stdout.write(json.dumps([r.summary() for r in reports], sort_keys=True) + "\n")
    return EXIT_OK if all(r.conserved for r in reports) else EXIT_FAILED_CHECK


def _coverage(spark: Any, lake: LakeConfig, args: argparse.Namespace) -> int:
    import psycopg

    from trace_core.observation.supervisor import LEASE_S, TAKEOVER_MARGIN_S
    from trace_core.stream.bronze_coverage import assess_bronze_coverage

    dsn = "postgresql://{u}:{p}@{h}:{port}/{db}".format(
        u=os.environ["TRACE_STREAM_DB_USER"],
        p=os.environ["TRACE_STREAM_DB_PASSWORD"],
        h=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        db=os.environ.get("POSTGRES_DB", "tracex"),
    )
    with psycopg.connect(dsn, autocommit=True) as connection:
        result = assess_bronze_coverage(
            spark,
            lake,
            connection,
            clock_margin_s=args.clock_margin_s,
            lease_s=LEASE_S if args.lease_s is None else args.lease_s,
            takeover_margin_s=(
                TAKEOVER_MARGIN_S if args.takeover_margin_s is None else args.takeover_margin_s
            ),
        )
    gaps = result.coverage.gaps
    sys.stdout.write(
        json.dumps(
            {
                "gap_free": result.coverage.gap_free,
                "gaps": len(gaps),
                "open_gaps": sum(1 for gap in gaps if gap.end is None),
                "anomalies": len(result.coverage.anomalies),
                "observations": result.observations,
                "beyond_high_water": result.beyond_high_water,
                "high_water_withheld": list(result.high_water_withheld),
                "through": result.coverage.through.isoformat(),
                "bronze_high_water": None
                if result.bronze_high_water is None
                else result.bronze_high_water.isoformat(),
                "ledger_read_at": result.ledger_read_at.isoformat(),
                "table_versions": dict(result.table_versions),
            },
            sort_keys=True,
        )
        + "\n"
    )
    # Exit 0 only when Bronze vouches for everything it read: a mark, no gap, no anomaly. A live
    # gateway's session always has an open tail, so this exits 1 while a writer runs, by design;
    # `open_gaps` tells a live tail from a bounded gap.
    vouched = result.coverage.gap_free and result.bronze_high_water is not None
    return EXIT_OK if vouched else EXIT_FAILED_CHECK


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    configure_logging()
    try:
        lake = LakeConfig.from_env()
        from trace_core.stream.session import build_session

        spark = build_session(
            f"trace-x-{args.command}-bronze",
            driver_memory=getattr(args, "driver_memory", "1g"),
        )
    except (LakeContractError, TraceXError) as exc:
        _log.error("bronze_refused", error=str(exc))
        return EXIT_REFUSED
    try:
        handler = {"run": _run, "conservation": _conservation, "coverage": _coverage}
        return handler[args.command](spark, lake, args)
    except (LakeContractError, ContractError, ProvenanceUnavailableError) as exc:
        _log.error("bronze_refused", command=args.command, error=str(exc))
        return EXIT_REFUSED
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
