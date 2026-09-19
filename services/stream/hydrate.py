"""`hydrate`: rebuild the online feature store from history (`python -m services.stream.hydrate`).

Thin by design: every decision lives in `trace_core.stream.hydration` (ADR-0057).

    run      take the writer fence, replay history through the store's own write path, and claim
             completeness when the evidence allows it
    status   print the store's epoch, its observation counter and any hydration marker

While `run` holds the fence no gateway is ready, which is the accepted PostgreSQL-down behaviour
(`docs/ARCHITECTURE.md` §18). The store it rebuilds must be one nothing else has written.

Configuration comes from the environment the gateway reads -- `REDIS_HOST`, `REDIS_PORT`,
`REDIS_DB`, `TRACE_APP_DB_USER`, `TRACE_APP_DB_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_PORT`,
`POSTGRES_DB` -- and `TRACE_DELTA_ROOT` for the lake. `--clock-margin-s` has no default: it stands
for the measured broker-to-PostgreSQL clock offset, and a default would be an unmeasured number.

Exit codes, as the Bronze, Silver and Gold jobs': 0 claimed, or already claimed; 1 nothing is
claimable, and the store is left vouching only from hydration's own withdrawal; 2 refused before
anything was written; 3 the run failed part-way.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Sequence
from typing import Any, Final

from services.stream.bronze import EXIT_FAILED_CHECK, EXIT_OK, EXIT_QUERY_FAILED, EXIT_REFUSED
from trace_core.domain.errors import ContractError, LakeContractError, TraceXError
from trace_core.observability import configure_logging, get_logger
from trace_core.observation.session import WRITER_LOCK_KEY
from trace_core.observation.supervisor import WriterSupervisor
from trace_core.repositories.postgres_completeness import PostgresHoleLedger
from trace_core.repositories.postgres_sessions import writer_connection
from trace_core.repositories.redis_features import RedisOnlineFeatureStore
from trace_core.stream.hydration import (
    BATCH_OBSERVATIONS,
    HYDRATION_PRODUCER,
    HydrationFailedError,
    HydrationRefusedError,
    Hydrator,
)
from trace_core.stream.lake import LakeConfig

NAMESPACE_ENV: Final = "TRACE_FEATURE_NAMESPACE"
FENCE_TIMEOUT_S: Final = 30.0
"""How long `run` waits to become the writer before refusing."""

_log = get_logger("services.stream.hydrate")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m services.stream.hydrate")
    parser.add_argument("--namespace", default=os.environ.get(NAMESPACE_ENV, "f"))
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="rebuild the store and claim what the evidence allows")
    run.add_argument(
        "--clock-margin-s",
        type=float,
        required=True,
        help="the measured broker-to-PostgreSQL clock offset, in seconds",
    )
    run.add_argument("--batch-size", type=int, default=BATCH_OBSERVATIONS)
    run.add_argument("--lock-key", type=int, default=WRITER_LOCK_KEY)
    run.add_argument("--driver-memory", default="2g")
    run.add_argument(
        "--discard-unfinished",
        action="store_true",
        help="delete what an unfinished hydration wrote and rebuild it from an empty store",
    )
    commands.add_parser("status", help="print the store's epoch, counter and hydration marker")
    return parser


def _redis_client() -> Any:
    import redis

    return redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", "6389")),
        db=int(os.environ.get("REDIS_DB", "0")),
    )


def _dsn() -> str:
    return "postgresql://{user}:{password}@{host}:{port}/{db}".format(
        user=os.environ.get("TRACE_APP_DB_USER", "trace_app"),
        password=os.environ.get("TRACE_APP_DB_PASSWORD", ""),
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5442"),
        db=os.environ.get("POSTGRES_DB", "tracex"),
    )


def _status(args: argparse.Namespace) -> int:
    store = RedisOnlineFeatureStore(_redis_client(), namespace=args.namespace)
    sys.stdout.write(
        json.dumps(
            {
                "namespace": args.namespace,
                "epoch_ms": store.stored_epoch_ms(),
                "position": store.stored_position(),
                "marker": store.hydration_marker(),
            },
            sort_keys=True,
        )
        + "\n"
    )
    return EXIT_OK


def _run(args: argparse.Namespace) -> int:
    import psycopg
    import psycopg_pool
    from pyspark.errors import PySparkException

    from trace_core.stream.session import build_session

    lake = LakeConfig.from_env()
    store = RedisOnlineFeatureStore(_redis_client(), namespace=args.namespace)
    dsn = _dsn()
    spark = build_session("trace-x-hydrate", driver_memory=args.driver_memory)
    ledger = psycopg.connect(dsn, autocommit=True)
    pool = psycopg_pool.ConnectionPool(dsn, min_size=1, max_size=2, open=True)
    writer = WriterSupervisor(
        connect=lambda: writer_connection(dsn),
        producer=HYDRATION_PRODUCER,
        instance_id=f"hydrate@{os.uname().nodename}",
        lock_key=args.lock_key,
    )
    writer.start()
    try:
        deadline = time.monotonic() + FENCE_TIMEOUT_S
        while not writer.ready and time.monotonic() < deadline:
            time.sleep(0.1)
        if not writer.ready:
            raise HydrationRefusedError(f"never became the online store's writer: {writer.status}")
        hydrator = Hydrator(
            store=store,
            spark=spark,
            lake=lake,
            writer=writer,
            holes=PostgresHoleLedger(pool),
            ledger=ledger,
            clock_margin_s=args.clock_margin_s,
            batch_size=args.batch_size,
            discard_unfinished=args.discard_unfinished,
        )
        report = hydrator.run()
    except PySparkException as exc:
        _log.error("hydration_failed", error=f"{type(exc).__name__}: {str(exc)[:2000]}")
        return EXIT_QUERY_FAILED
    finally:
        writer.stop(confirmed=True)
        pool.close()
        ledger.close()
        spark.stop()
    sys.stdout.write(json.dumps(report.summary(), sort_keys=True) + "\n")
    return EXIT_OK if report.claimed else EXIT_FAILED_CHECK


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    configure_logging()
    try:
        return {"run": _run, "status": _status}[args.command](args)
    except HydrationRefusedError as exc:
        _log.error("hydration_refused", command=args.command, error=str(exc))
        return EXIT_REFUSED
    except HydrationFailedError as exc:
        _log.error("hydration_failed", command=args.command, error=str(exc))
        return EXIT_QUERY_FAILED
    except (LakeContractError, ContractError, TraceXError) as exc:
        _log.error("hydration_refused", command=args.command, error=str(exc))
        return EXIT_REFUSED


if __name__ == "__main__":
    from trace_core.stream.session import run_driver

    run_driver(main)
