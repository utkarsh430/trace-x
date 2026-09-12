"""Rate limiting against a real Redis, including what happens when it is gone.

The sliding window and the fail-open behaviour are both properties of the
interaction with Redis, so both are established here rather than against a fake.

The asymmetry under test is the one CLAUDE.md §3.7 states: **scoring fails open,
actions fail closed.** A rate limiter is on the scoring path and protects
TRACE-X, not the customer's money — so when it cannot be consulted, traffic
passes and every such request is counted. The action path in Phase 8 does the
opposite, and that difference is deliberate rather than an inconsistency.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from typing import Any

import pytest

from trace_core.repositories.redis_ratelimit import RedisRateLimiter

pytestmark = pytest.mark.integration

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6389"))


@pytest.fixture
def redis_client() -> Iterator[Any]:
    redis = pytest.importorskip("redis", reason="the `db` extra provides the Redis client")
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        client.ping()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no Redis at {REDIS_HOST}:{REDIS_PORT} ({exc}). "
            f"Run `make up`; the sliding window is a property of Redis and is never "
            f"mocked here (docs/TESTING.md §4)."
        )
    client.flushdb()
    yield client
    client.flushdb()


def _token() -> str:
    return f"psp-{uuid.uuid4().hex[:8]}"


def test_requests_within_budget_are_allowed(redis_client: Any) -> None:
    limiter = RedisRateLimiter(redis_client, limit=5, window_s=60)
    token = _token()
    for expected_remaining in (4, 3, 2, 1, 0):
        decision = limiter.check(token, request_id=uuid.uuid4().hex)
        assert decision.allowed
        assert decision.remaining == expected_remaining
        assert not decision.degraded


def test_the_request_over_budget_is_refused_with_a_retry_after(redis_client: Any) -> None:
    limiter = RedisRateLimiter(redis_client, limit=3, window_s=60)
    token = _token()
    for _ in range(3):
        assert limiter.check(token, request_id=uuid.uuid4().hex).allowed

    refused = limiter.check(token, request_id=uuid.uuid4().hex)
    assert not refused.allowed
    assert refused.remaining == 0
    assert 1 <= refused.retry_after_s <= 61, (
        "Retry-After must be a real number so a client can back off correctly; a "
        "constant would either stampede or overwait"
    )


def test_budgets_are_per_token(redis_client: Any) -> None:
    """One noisy caller must not throttle another."""
    limiter = RedisRateLimiter(redis_client, limit=2, window_s=60)
    noisy, quiet = _token(), _token()
    for _ in range(3):
        limiter.check(noisy, request_id=uuid.uuid4().hex)
    assert not limiter.check(noisy, request_id=uuid.uuid4().hex).allowed
    assert limiter.check(quiet, request_id=uuid.uuid4().hex).allowed


def test_two_requests_in_the_same_millisecond_both_count(redis_client: Any) -> None:
    """The sorted-set member is unique per request.

    If it were the timestamp alone, requests arriving in the same millisecond
    would collapse into one entry and a caller could exceed its budget simply by
    sending fast enough -- which is precisely the caller a limiter exists for.
    """
    limiter = RedisRateLimiter(redis_client, limit=100, window_s=60)
    token = _token()
    first = limiter.check(token, request_id="req-a")
    second = limiter.check(token, request_id="req-b")
    assert first.remaining == 99
    assert second.remaining == 98, "two same-millisecond requests collapsed into one"


def test_the_window_slides(redis_client: Any) -> None:
    """A fixed window lets a caller send a full budget either side of a boundary
    -- twice the limit in two seconds. Entries older than the window are trimmed
    rather than reset in bulk."""
    limiter = RedisRateLimiter(redis_client, limit=2, window_s=1)
    token = _token()
    key = f"rl:{token}"

    assert limiter.check(token, request_id="req-a").allowed
    assert limiter.check(token, request_id="req-b").allowed
    assert not limiter.check(token, request_id="req-c").allowed

    # Age every recorded request past the window, rather than sleeping: the
    # behaviour under test is the trim, not the clock.
    redis_client.delete(key)
    for offset, request_id in ((-5_000, "old-a"), (-4_000, "old-b")):
        import time

        redis_client.zadd(
            key,
            {f"{int(time.time() * 1000) + offset}:{request_id}": int(time.time() * 1000) + offset},
        )

    allowed = limiter.check(token, request_id="req-d")
    assert allowed.allowed, "requests outside the window were not trimmed"


def test_a_refused_request_still_consumes_its_attempt(redis_client: Any) -> None:
    """Otherwise a caller could poll the limiter for free and discover the exact
    moment its budget frees up."""
    limiter = RedisRateLimiter(redis_client, limit=1, window_s=60)
    token = _token()
    limiter.check(token, request_id="req-a")
    limiter.check(token, request_id="req-b")
    assert redis_client.zcard(f"rl:{token}") == 2


def test_the_key_never_contains_the_secret(redis_client: Any) -> None:
    """Rate-limit keys are keyed by token ID, never by the presented credential.

    Keying by the whole token would write secrets into Redis and into any dump
    of it (docs/SECURITY.md §10).
    """
    limiter = RedisRateLimiter(redis_client, limit=5, window_s=60)
    limiter.check("psp-one", request_id="req-a")
    keys = list(redis_client.scan_iter("rl:*"))
    assert keys == ["rl:psp-one"]


# --- the fail-open asymmetry ---------------------------------------------------


def test_an_unreachable_limiter_allows_and_says_so() -> None:
    """CLAUDE.md §3.7: scoring fails OPEN, and every fail-open is counted.

    Declining all traffic is worse than missing fraud for a few minutes -- but a
    fail-open nobody counts is indistinguishable from a limiter that is working,
    so the decision carries the reason for the caller to record.
    """
    redis = pytest.importorskip("redis")
    # A port nothing listens on: a real connection failure, not a simulated one.
    unreachable = redis.Redis(
        host="127.0.0.1", port=1, socket_connect_timeout=0.05, socket_timeout=0.05
    )
    limiter = RedisRateLimiter(unreachable, limit=1, window_s=60)

    decision = limiter.check("psp-one", request_id="req-a")
    assert decision.allowed, "a limiter outage must not become a scoring outage"
    assert decision.degraded
    assert decision.reason == "rate_limit_unavailable"


def test_the_limiter_never_raises_into_the_request_path() -> None:
    """An exception here would turn a Redis blip into a 500 on the hot path."""
    redis = pytest.importorskip("redis")
    unreachable = redis.Redis(
        host="127.0.0.1", port=1, socket_connect_timeout=0.05, socket_timeout=0.05
    )
    limiter = RedisRateLimiter(unreachable, limit=1, window_s=60)
    for _ in range(3):
        assert limiter.check("psp-one", request_id=uuid.uuid4().hex).allowed
