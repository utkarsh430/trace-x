"""trace-worker's outbox relay process (ADR-0051 §7, option B), with fakes for its three resources.

The relay itself is proved against PostgreSQL and a real broker in tests/integration; here, the
process around it: what it refuses, what it bounds, and that it always releases what it opened.
"""

from __future__ import annotations

import secrets
import threading
from typing import Any

import pytest
from services.worker import relay as worker

pytestmark = pytest.mark.unit

ENV = {
    "TRACE_WORKER_KAFKA_BOOTSTRAP": "kafka:19092",
    "TRACE_APP_DB_PASSWORD": secrets.token_hex(8),  # generated: no credential in the source
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
}


class FakePool:
    def __init__(self, dsn: str, **kwargs: Any) -> None:
        self.dsn, self.kwargs = dsn, kwargs
        self.events: list[str] = []

    def open(self, *, wait: bool, timeout: float) -> None:
        self.events.append("open")

    def close(self) -> None:
        self.events.append("close")


class FakePublisher:
    def __init__(self) -> None:
        self.closed_with: float | None = None

    def close(self, timeout_s: float) -> None:
        self.closed_with = timeout_s


class FakeRelay:
    def __init__(self, *, alive: bool = True, **_: Any) -> None:
        self.alive, self.started, self.stopped = alive, False, False

    @property
    def running(self) -> bool:
        return self.started and self.alive and not self.stopped

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True


def _run(stop: threading.Event, *, alive: bool) -> tuple[int, FakePool, FakePublisher, FakeRelay]:
    pools: list[FakePool] = []
    publisher = FakePublisher()
    relays: list[FakeRelay] = []

    def pool_class(dsn: str, **kwargs: Any) -> FakePool:
        pools.append(FakePool(dsn, **kwargs))
        return pools[-1]

    def relay_factory(**kwargs: Any) -> FakeRelay:
        relays.append(FakeRelay(alive=alive, **kwargs))
        return relays[-1]

    code = worker.run(
        worker.WorkerSettings.from_environment(ENV),
        stop=stop,
        pool_class=pool_class,
        connect_publisher=lambda *args, **kwargs: publisher,
        relay_factory=relay_factory,
        liveness_check_s=0.01,
    )
    return code, pools[0], publisher, relays[0]


@pytest.mark.parametrize("missing", ["TRACE_WORKER_KAFKA_BOOTSTRAP", "TRACE_APP_DB_PASSWORD"])
def test_the_worker_refuses_to_start_without_a_broker_or_database_credentials(missing: str) -> None:
    with pytest.raises(worker.WorkerConfigError, match=missing):
        worker.WorkerSettings.from_environment({**ENV, missing: ""})
    assert worker.main({**ENV, missing: ""}) == worker.EXIT_REFUSED


def test_the_pool_connects_as_trace_app_and_bounds_every_wait() -> None:
    settings = worker.WorkerSettings.from_environment(ENV)
    assert settings.postgres_dsn.startswith("postgresql://trace_app:")
    pool = worker.open_pool(settings, FakePool)
    assert pool.kwargs["timeout"] == worker.POSTGRES_TIMEOUT_S and pool.kwargs["open"] is False
    assert pool.kwargs["kwargs"] == {
        "connect_timeout": worker.CONNECT_TIMEOUT_S,
        "options": f"-c statement_timeout={round(worker.POSTGRES_TIMEOUT_S * 1000)}",
    }


def test_the_worker_relays_until_stopped_and_releases_everything() -> None:
    stop = threading.Event()
    timer = threading.Timer(0.05, stop.set)
    timer.start()
    code, pool, publisher, relay = _run(stop, alive=True)
    assert code == worker.EXIT_OK
    assert relay.started and relay.stopped
    assert publisher.closed_with == worker.CLOSE_FLUSH_S
    assert pool.events == ["open", "close"]


def test_a_dead_relay_thread_exits_non_zero_so_the_container_restarts_it() -> None:
    code, pool, publisher, relay = _run(threading.Event(), alive=False)
    assert code == worker.EXIT_RELAY_DIED
    assert relay.stopped and publisher.closed_with is not None and pool.events == ["open", "close"]
