"""trace-worker's outbox relay (ADR-0051 §7, option B; ADR-0049 §4).

The pre-registered hot-path A/B ruled out option A, the relay on a gateway thread
(`benchmarks/gateway/observation-log-ab.md`). So `app.outbox` is drained here, in its own process:
- its own PostgreSQL pool as `trace_app`, with acquiring a connection and every statement bounded,
  as the gateway's pool is;
- its own producer, from the one factory (`trace_core.contracts.publish`);
- `OutboxRelay` unchanged: SKIP LOCKED batches, rows marked only on a confirmed flush, and the
  authorization delivery watermark advanced in the marking transaction.

It refuses to start without a broker or database credentials: a relay with nowhere to publish would
only claim rows to fail them. It exits non-zero if the relay thread dies, so the container's restart
policy brings it back instead of leaving the outbox silently undrained.
"""

from __future__ import annotations

import os
import signal
import threading
import time
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Final

from trace_core.domain.errors import TraceXError
from trace_core.observability.logging import configure_logging, get_logger
from trace_core.observation.outbox_relay import MESSAGE_TIMEOUT_MS, OutboxRelay, RowOutcome

log = get_logger(__name__)

KAFKA_BOOTSTRAP_ENV: Final = "TRACE_WORKER_KAFKA_BOOTSTRAP"
POSTGRES_TIMEOUT_S: Final = 2.0
"""Acquiring a connection and each statement, as the gateway bounds its own pool."""
CONNECT_TIMEOUT_S: Final = 2
POOL_OPEN_TIMEOUT_S: Final = 30.0
CLOSE_FLUSH_S: Final = 10.0
LIVENESS_CHECK_S: Final = 1.0
ROWS_LOG_INTERVAL_S: Final = 60.0
EXIT_OK: Final = 0
EXIT_RELAY_DIED: Final = 1
EXIT_REFUSED: Final = 2


class WorkerConfigError(TraceXError):
    """The worker cannot run correctly with this configuration, so it refuses to start."""


@dataclass(frozen=True, slots=True)
class WorkerSettings:
    postgres_dsn: str
    kafka_bootstrap_servers: str
    pool_max_size: int
    client_id: str

    @classmethod
    def from_environment(cls, env: Mapping[str, str]) -> WorkerSettings:
        bootstrap = env.get(KAFKA_BOOTSTRAP_ENV, "").strip()
        if not bootstrap:
            raise WorkerConfigError(
                f"{KAFKA_BOOTSTRAP_ENV} is empty: the relay has nowhere to publish, and claiming "
                f"outbox rows only to fail them would churn the queue"
            )
        password = env.get("TRACE_APP_DB_PASSWORD", "")
        if not password:
            raise WorkerConfigError(
                "TRACE_APP_DB_PASSWORD is empty: the relay connects as trace_app"
            )
        dsn = "postgresql://{user}:{password}@{host}:{port}/{db}".format(
            user=env.get("TRACE_APP_DB_USER", "trace_app"),
            password=password,
            host=env.get("POSTGRES_HOST", "localhost"),
            port=env.get("POSTGRES_PORT", "5442"),
            db=env.get("POSTGRES_DB", "tracex"),
        )
        return cls(
            postgres_dsn=dsn,
            kafka_bootstrap_servers=bootstrap,
            pool_max_size=int(env.get("TRACE_WORKER_PG_POOL_MAX", "2")),
            client_id=env.get("TRACE_WORKER_CLIENT_ID", "trace-worker-relay"),
        )


def open_pool(settings: WorkerSettings, pool_class: Any) -> Any:
    """The relay's pool: one connection is enough for one relay thread; every wait is bounded."""
    return pool_class(
        settings.postgres_dsn,
        min_size=1,
        max_size=settings.pool_max_size,
        open=False,
        timeout=POSTGRES_TIMEOUT_S,
        kwargs={
            "connect_timeout": CONNECT_TIMEOUT_S,
            "options": f"-c statement_timeout={round(POSTGRES_TIMEOUT_S * 1000)}",
        },
    )


def run(
    settings: WorkerSettings,
    *,
    stop: threading.Event,
    pool_class: Any,
    connect_publisher: Callable[..., Any],
    relay_factory: Callable[..., Any] = OutboxRelay,
    liveness_check_s: float = LIVENESS_CHECK_S,
    rows_log_interval_s: float = ROWS_LOG_INTERVAL_S,
) -> int:
    """Relay until `stop` is set (EXIT_OK) or the relay thread dies (EXIT_RELAY_DIED)."""
    rows: Counter[tuple[str, str]] = Counter()

    def record(topic: str, outcome: RowOutcome, count: int) -> None:
        rows[(topic, outcome.value)] += count

    pool = open_pool(settings, pool_class)
    pool.open(wait=True, timeout=POOL_OPEN_TIMEOUT_S)
    publisher = None
    relay = None
    try:
        publisher = connect_publisher(
            settings.kafka_bootstrap_servers,
            client_id=settings.client_id,
            message_timeout_ms=MESSAGE_TIMEOUT_MS,
        )
        relay = relay_factory(pool=pool, publisher=publisher, record=record)
        relay.start()
        log.info("worker_outbox_relay_started", client_id=settings.client_id)
        logged: Counter[tuple[str, str]] = Counter()
        last_log = time.monotonic()
        while not stop.wait(liveness_check_s):
            if not relay.running:
                log.error("worker_outbox_relay_thread_died")
                return EXIT_RELAY_DIED
            if rows != logged and time.monotonic() - last_log >= rows_log_interval_s:
                log.info(
                    "worker_outbox_relay_rows",
                    rows={f"{topic}:{outcome}": count for (topic, outcome), count in rows.items()},
                )
                logged, last_log = Counter(rows), time.monotonic()
        log.info("worker_outbox_relay_stopping")
        return EXIT_OK
    finally:
        if relay is not None:
            relay.stop()
        if publisher is not None:
            try:
                publisher.close(CLOSE_FLUSH_S)
            except Exception as exc:  # unconfirmed rows stay unpublished and are relayed again
                log.warning("worker_publisher_close_unconfirmed", error=type(exc).__name__)
        pool.close()


def main(env: Mapping[str, str] | None = None) -> int:
    configure_logging()
    try:
        settings = WorkerSettings.from_environment(os.environ if env is None else env)
    except WorkerConfigError as exc:
        log.error("worker_refused_to_start", reason=str(exc))
        return EXIT_REFUSED
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    from psycopg_pool import ConnectionPool

    from trace_core.contracts.publish import EventPublisher

    return run(
        settings, stop=stop, pool_class=ConnectionPool, connect_publisher=EventPublisher.connect
    )


__all__ = [
    "EXIT_OK",
    "EXIT_REFUSED",
    "EXIT_RELAY_DIED",
    "KAFKA_BOOTSTRAP_ENV",
    "WorkerConfigError",
    "WorkerSettings",
    "main",
    "open_pool",
    "run",
]
