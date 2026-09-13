"""The routes run on the event loop, and blocking them is only safe if bounded.

**This file replaces a guard that asserted the opposite, and the story is the
point.** Every repository call in the scoring route is a synchronous socket
round trip. That is the textbook case for moving a handler off the event loop,
so the routes were made `def` and dispatched to Starlette's worker threadpool.
A controlled A/B on identical store state then measured:

| variant                       | achieved TPS | scoring core p50 | gateway CPU |
|-------------------------------|--------------|------------------|-------------|
| routes on the event loop      | **342.1**    | **0.81 ms**      | 76.7%       |
| routes in a 64-thread pool    | 279.9        | 39.26 ms         | 122.7%      |

CPU rose 60% while throughput fell 18%: the work is GIL-bound, not I/O-bound, so
threads bought contention rather than overlap. ADR-0039 records it.

So the invariant worth testing is NOT "these handlers are async" -- that would be
asserting a coincidence of the current design, and the next person to write a
genuinely awaitable handler would be told off by a test for no reason. The
invariant is the one that makes blocking the loop survivable at all: **every
synchronous dependency the hot path touches is bounded in time.** A single
unbounded call on the event loop stalls every in-flight request, which is the
21.8 s request the chaos suite found before the circuit breaker existed
(ADR-0035).
"""

from __future__ import annotations

import pytest
from services.gateway.config import (
    POSTGRES_TIMEOUT_S,
    REDIS_MAX_CONNECTIONS,
    REDIS_TIMEOUT_S,
)

from trace_core.repositories.circuit_breaker import COOLDOWN_S, FAILURE_THRESHOLD

pytestmark = pytest.mark.contract

HOT_PATH_BUDGET_MS = 100.0
"""ROADMAP Phase 2: p99 < 100 ms."""


def test_redis_timeout_is_a_small_fraction_of_the_budget() -> None:
    """A blocking call on the event loop must not be able to eat the budget."""
    assert 0 < REDIS_TIMEOUT_S * 1000 <= HOT_PATH_BUDGET_MS / 4, (
        f"Redis socket timeout is {REDIS_TIMEOUT_S * 1000} ms against a "
        f"{HOT_PATH_BUDGET_MS} ms p99 budget. The hot path makes several Redis calls "
        f"per request and they run on the event loop, so this bound is multiplied by "
        f"the call count AND blocks every concurrent request while it waits."
    )


def test_the_breaker_bounds_an_outage_rather_than_each_call() -> None:
    """Timeouts bound one call; only the breaker bounds the system.

    Without it an outage costs one timeout per call per request forever. With
    it, it costs one probe per cooldown.
    """
    assert FAILURE_THRESHOLD >= 1
    worst_case_learning_ms = FAILURE_THRESHOLD * REDIS_TIMEOUT_S * 1000
    assert worst_case_learning_ms <= HOT_PATH_BUDGET_MS, (
        f"discovering an outage costs {worst_case_learning_ms} ms "
        f"({FAILURE_THRESHOLD} failures x {REDIS_TIMEOUT_S * 1000} ms), which exceeds "
        f"the {HOT_PATH_BUDGET_MS} ms budget it is meant to protect."
    )
    assert COOLDOWN_S > 0, "an open circuit that never probes has turned an outage permanent"


def test_postgres_timeout_is_bounded_even_though_it_is_allowed_to_be_slower() -> None:
    """Triage may cost more than a feature read, but not without limit.

    It runs on the event loop too, so an unbounded triage write would stall the
    gateway rather than just the request that caused it.
    """
    assert 0 < POSTGRES_TIMEOUT_S <= 5.0, (
        f"Postgres timeout is {POSTGRES_TIMEOUT_S}s. Triage is a small write and this "
        f"bound is what stops a hung one from holding the event loop."
    )


def test_the_redis_connection_pool_is_bounded() -> None:
    """redis-py's default is effectively unbounded.

    A stalled Redis would then be answered by opening sockets until the file
    descriptor limit decides the outcome, which converts a dependency slowdown
    into resource exhaustion.
    """
    assert 0 < REDIS_MAX_CONNECTIONS <= 256, (
        f"Redis pool ceiling is {REDIS_MAX_CONNECTIONS}; it must be a real bound."
    )
