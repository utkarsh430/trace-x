"""The relay's thread outlives a failed pass, and says when it is not running.

Critic re-review finding 2: a failed pass raised inside the loop's own error handling, the daemon
thread died silently, and readiness went on reporting the relay as running.
"""

from __future__ import annotations

import time
from typing import Any

import pytest
from fastapi.testclient import TestClient
from services.gateway.app import GatewayState, create_app
from services.gateway.config import GatewaySettings
from services.gateway.pipeline import ScoringPipeline

from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.observability.metrics import HotPathMetrics
from trace_core.observation.outbox_relay import OutboxRelay
from trace_core.rules.loader import default_loader
from trace_core.scoring.banding import load_thresholds
from trace_core.security.service_tokens import MIN_SECRET_LENGTH, ServiceTokenVerifier

pytestmark = pytest.mark.unit


class Unreachable:
    """A pool whose every connection fails, as PostgreSQL does when it is down."""

    def __init__(self) -> None:
        self.attempts = 0

    def connection(self) -> Any:
        self.attempts += 1
        raise OSError("PostgreSQL unreachable")


def test_a_failed_pass_never_kills_the_relay_thread() -> None:
    pool = Unreachable()
    relay = OutboxRelay(pool=pool, publisher=object(), idle_interval_s=0.01)  # type: ignore[arg-type]
    assert not relay.running, "not started"
    relay.start()
    try:
        deadline = time.monotonic() + 5.0
        while pool.attempts < 3 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert pool.attempts >= 3, "the thread went on retrying after failed passes"
        assert relay.running
    finally:
        relay.stop(timeout_s=2.0)
    assert not relay.running, "stopped"


def test_readiness_reports_a_relay_whose_thread_is_not_running() -> None:
    relay = OutboxRelay(pool=Unreachable(), publisher=object(), idle_interval_s=0.01)  # type: ignore[arg-type]
    loader = default_loader(frozenset(ONLINE_FEATURES.ids))
    state = GatewayState(
        settings=GatewaySettings.from_environment({}),
        verifier=ServiceTokenVerifier({"psp-one": "s" * MIN_SECRET_LENGTH}),
        loader=loader,
        pipeline=ScoringPipeline(
            pack=loader.load(), thresholds=load_thresholds(), feature_store=None
        ),
        metrics=HotPathMetrics(),
        outbox_relay=relay,
    )
    with TestClient(create_app(state)) as client:
        running = client.get("/readyz").json()["checks"]["outbox_relay"]
        relay.stop(timeout_s=2.0)
        stopped = client.get("/readyz").json()["checks"]["outbox_relay"]
    assert running == "running"
    assert stopped == "stopped: the relay thread is not running"
