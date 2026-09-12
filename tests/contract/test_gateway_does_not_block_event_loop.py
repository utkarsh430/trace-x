"""The routes must not run their blocking I/O on the event loop.

**This test exists because the absence of it cost a phase-gate miss.** Every
repository the gateway calls -- the rate limiter, the replay cache, the feature
store, the triage store -- is synchronous, and the scoring route was declared
`async def`. Starlette runs an `async def` endpoint directly on the event loop,
so a handler that blocks on a socket blocks *every* other in-flight request for
the duration. One replica therefore served requests strictly one at a time, no
matter that ~95% of each request was spent waiting rather than computing.

What that cost, measured: the canonical 500 TPS load test achieved **305.34
TPS** with p50 **2,519 ms** and p99 **5,252 ms** against a 100 ms budget, while
a full-path decomposition of the same code put the handler body at **4.654 ms
mean / 8.653 ms p99**. The gap was queueing, not work -- the same run's fastest
request was 0.86 ms.

A `def` endpoint is dispatched to Starlette's worker threadpool instead, so the
waiting overlaps. The fix is one keyword per route, which is exactly why it needs
a test: nothing about `async def score_transaction(...)` looks wrong, it is what
most FastAPI code in the world says, and a future edit that restores it would
reintroduce a 16x throughput regression that no functional test would notice.

Structural rather than behavioural on purpose. A timing test for this would be a
flaky one, and the property is not "it was fast on the machine that ran CI" but
"this handler is not on the event loop".
"""

from __future__ import annotations

import inspect

import pytest
from fastapi.routing import APIRoute
from services.gateway.app import create_app

pytestmark = pytest.mark.contract

BLOCKING_ROUTES: frozenset[tuple[str, str]] = frozenset(
    {
        ("POST", "/v1/transactions"),
        ("POST", "/v1/events/identity"),
        ("POST", "/v1/events/device"),
        ("GET", "/readyz"),
    }
)
"""Routes that reach a synchronous repository.

`/v1/transactions` scores and triages; the two ingress routes write to the
online store; `/readyz` runs a Postgres `SELECT 1` and a Redis `PING`. Readiness
is on the list for a reason that is easy to miss: a probe that blocked the loop
against a hung database would stall every request in flight and take a servable
instance out of rotation by making it look unhealthy.
"""

NON_BLOCKING_ROUTES: frozenset[tuple[str, str]] = frozenset(
    {("GET", "/healthz")},
)
"""Routes that touch no dependency and may stay on the loop.

Liveness deliberately checks nothing -- a liveness probe that failed on a
database blip would restart a healthy process -- so it has nothing to block on.
"""


def _endpoints() -> dict[tuple[str, str], object]:
    app = create_app(state=object())  # type: ignore[arg-type]
    found: dict[tuple[str, str], object] = {}
    for route in app.routes:
        if isinstance(route, APIRoute):
            for method in route.methods or ():
                found[(method, route.path)] = route.endpoint
    return found


def test_every_blocking_route_is_declared_sync() -> None:
    endpoints = _endpoints()
    missing = BLOCKING_ROUTES - set(endpoints)
    assert not missing, (
        f"routes named by this test no longer exist: {sorted(missing)}. If a route was "
        f"renamed, update the list; if it was deleted, delete its entry -- do not let the "
        f"list quietly stop covering anything."
    )
    on_the_loop = sorted(
        f"{method} {path}"
        for (method, path) in BLOCKING_ROUTES
        if inspect.iscoroutinefunction(endpoints[(method, path)])
    )
    assert not on_the_loop, (
        f"declared `async def` but calls synchronous, blocking repositories: {on_the_loop}. "
        f"Starlette runs an async endpoint ON the event loop, so every socket wait inside "
        f"one of these blocks every other in-flight request; the measured cost was 305 TPS "
        f"against a 500 TPS target with a p50 of 2.5 s. Declare it `def` so Starlette "
        f"dispatches it to the worker threadpool, or make every call inside it genuinely "
        f"awaitable -- but do not leave blocking calls in an async handler."
    )


def test_dependency_free_routes_may_stay_async() -> None:
    """The rule is about blocking calls, not a blanket ban on `async def`.

    Without this, the obvious reading of the test above is "sync endpoints are
    the house style", and the next handler that genuinely is async -- one that
    awaits an async client -- gets written the slow way to match.
    """
    endpoints = _endpoints()
    for method, path in NON_BLOCKING_ROUTES:
        assert inspect.iscoroutinefunction(endpoints[(method, path)]), (
            f"{method} {path} touches no dependency, so it has nothing to block on and "
            f"belongs on the event loop. Moving it to the threadpool spends a thread hop "
            f"on a health check."
        )
