"""A hydration run in a child process, killed for real part-way through its replay.

`tests/integration/test_redis_hydration.py` runs this as
`python -m tests.integration.hydration_harness`. It composes the production pieces the service
composes -- the fenced writer, the Redis store, the Spark session and `Hydrator` -- and sends
itself SIGKILL after `--kill-after` observations. The lock then ends with the backend and the
Redis socket drops, exactly as when a hydration process dies.

No production code carries a hook for this. The kill point sits between two `observe` calls, which
is where a crash is interesting: the marker's cursor is behind what the store already holds.
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import time
from typing import Any

from trace_core.observability import configure_logging, get_logger

_log = get_logger("tests.integration.hydration_harness")


class _KillAfter:
    """The production store, with a kill point between two of its writes."""

    def __init__(self, store: Any, kill_after: int) -> None:
        self._store = store
        self._kill_after = kill_after
        self._written = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._store, name)

    def observe(self, event: Any) -> Any:
        receipt = self._store.observe(event)
        self._written += 1
        if self._written >= self._kill_after:
            _log.info("harness_kill_point", written=self._written)
            os.kill(os.getpid(), signal.SIGKILL)
        return receipt


def _parse(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m tests.integration.hydration_harness")
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--lake-root", required=True)
    parser.add_argument("--clock-margin-s", type=float, required=True)
    parser.add_argument("--kill-after", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=2)
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", type=int, default=6389)
    parser.add_argument("--redis-db", type=int, default=15)
    parser.add_argument("--lock-key", type=int, required=True)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = _parse(argv)
    configure_logging()
    import psycopg
    import psycopg_pool
    import redis

    from trace_core.observation.supervisor import WriterSupervisor
    from trace_core.repositories.postgres_completeness import PostgresHoleLedger
    from trace_core.repositories.postgres_sessions import writer_connection
    from trace_core.repositories.redis_features import RedisOnlineFeatureStore
    from trace_core.stream.hydration import HYDRATION_PRODUCER, Hydrator
    from trace_core.stream.lake import LakeConfig
    from trace_core.stream.session import build_session

    store = RedisOnlineFeatureStore(
        redis.Redis(host=args.redis_host, port=args.redis_port, db=args.redis_db),
        namespace=args.namespace,
    )
    writer = WriterSupervisor(
        connect=lambda: writer_connection(args.dsn),
        producer=HYDRATION_PRODUCER,
        instance_id=f"harness-{args.namespace}",
        lock_key=args.lock_key,
    )
    writer.start()
    deadline = time.monotonic() + 30
    while not writer.ready and time.monotonic() < deadline:
        time.sleep(0.05)
    if not writer.ready:
        _log.error("harness_not_the_writer", status=writer.status)
        return 2
    spark = build_session("trace-x-hydrate-harness")
    ledger = psycopg.connect(args.dsn, autocommit=True)
    pool = psycopg_pool.ConnectionPool(args.dsn, min_size=1, max_size=2, open=True)
    hydrator = Hydrator(
        store=_KillAfter(store, args.kill_after) if args.kill_after else store,  # type: ignore[arg-type]
        spark=spark,
        lake=LakeConfig.at(args.lake_root),
        writer=writer,
        holes=PostgresHoleLedger(pool),
        ledger=ledger,
        clock_margin_s=args.clock_margin_s,
        batch_size=args.batch_size,
    )
    try:
        report = hydrator.run()
    finally:
        writer.stop(confirmed=True)
        pool.close()
        ledger.close()
        spark.stop()
    _log.info("harness_finished", **report.summary())
    return 0 if report.claimed else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
