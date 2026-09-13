"""Per-token rate limiting, and why it fails OPEN.

**The asymmetry is deliberate and is the whole design.** CLAUDE.md §3.7 makes
fail-safe direction explicit: scoring fails open, actions fail closed. A rate
limiter sits on the scoring path and protects *TRACE-X* -- not the customer's
money, not the integrity of a decision. If Redis is unreachable the choice is
between refusing traffic we could have scored and accepting traffic we cannot
meter, and refusing is the worse outcome: declining all traffic is worse than
missing fraud for a few minutes.

So a limiter outage lets requests through, **and every one of them is counted**
(`degraded_mode_total{reason="rate_limit_unavailable"}`). A fail-open nobody
counts is indistinguishable from a limiter that is working.

The action path in Phase 8 does the opposite, for the opposite reason: an action
executed under uncertainty moves real money.

**A sliding window, not a fixed one.** A fixed window lets a caller send a full
budget at 0:59 and another at 1:01 -- twice the limit in two seconds, right at
the boundary. The sorted set here is trimmed by processing time, which is correct
precisely because this is NOT a business measurement: a rate limit is about load
on this process now, so the wall clock is the right clock. It is the one place in
this system where processing time is the honest choice, and saying so beside the
places where it would be wrong is cheaper than the alternative.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:  # pragma: no cover - typing only
    from redis import Redis

DEFAULT_LIMIT: Final = 1_000
DEFAULT_WINDOW_S: Final = 60
"""1,000 requests per minute per token by default.

Comfortably above the 500 TPS the ROADMAP load target drives through a single
token, so a benchmark does not spend its time being throttled, and low enough
that one misbehaving client cannot saturate the process. Overridable per token.
"""


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    """The outcome of one rate-limit check."""

    allowed: bool
    remaining: int
    retry_after_s: int
    degraded: bool = False
    """True when the limiter could not be consulted and the request was let
    through. Carried so the caller counts it rather than reporting a clean
    allow."""

    @property
    def reason(self) -> str | None:
        return "rate_limit_unavailable" if self.degraded else None


class RedisRateLimiter:
    """A sliding-window limiter over a Redis sorted set, one key per token."""

    def __init__(
        self,
        client: Redis,
        *,
        namespace: str = "rl",
        limit: int = DEFAULT_LIMIT,
        window_s: int = DEFAULT_WINDOW_S,
    ) -> None:
        self._redis = client
        self._ns = namespace
        self._limit = limit
        self._window_s = window_s

    @property
    def limit(self) -> int:
        return self._limit

    def _key(self, token_id: str) -> str:
        return f"{self._ns}:{token_id}"

    def check(self, token_id: str, *, request_id: str) -> RateLimitDecision:
        """Consume one unit of budget, or report that it is exhausted.

        A Redis failure returns an ALLOWED decision marked `degraded`. It does
        not raise: the caller is on the hot path, and an exception here would
        turn a limiter outage into a scoring outage.
        """
        key = self._key(token_id)
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - self._window_s * 1000
        try:
            pipe = self._redis.pipeline(transaction=False)
            pipe.zremrangebyscore(key, "-inf", cutoff)
            # The member must be unique per request: two requests in the same
            # millisecond would otherwise collapse into one and a caller could
            # exceed its budget by sending fast enough.
            pipe.zadd(key, {f"{now_ms}:{request_id}": now_ms})
            pipe.zcard(key)
            pipe.expire(key, self._window_s + 1)
            used = int(pipe.execute()[2])
        except Exception:
            # Fail OPEN and say so. See the module docstring: this limiter
            # protects TRACE-X, not the money.
            return RateLimitDecision(
                allowed=True, remaining=self._limit, retry_after_s=0, degraded=True
            )

        if used > self._limit:
            # Over budget. The request has already been recorded, which is
            # correct: a rejected request still consumed the attempt, and not
            # recording it would let a caller poll the limiter for free.
            return RateLimitDecision(
                allowed=False, remaining=0, retry_after_s=self._retry_after(key, now_ms)
            )
        return RateLimitDecision(
            allowed=True, remaining=max(0, self._limit - used), retry_after_s=0
        )

    def _retry_after(self, key: str, now_ms: int) -> int:
        """Seconds until the oldest request leaves the window.

        A real number rather than a fixed guess: `Retry-After` exists so a client
        can back off correctly, and a constant would either stampede or overwait.
        """
        try:
            oldest = self._redis.zrange(key, 0, 0, withscores=True)
        except Exception:
            return self._window_s
        if not oldest:
            return 1
        expires_at_ms = float(oldest[0][1]) + self._window_s * 1000
        return max(1, int((expires_at_ms - now_ms) / 1000) + 1)
