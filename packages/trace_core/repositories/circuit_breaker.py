"""A circuit breaker for the online store, and the measurement that demanded it.

**Found by the chaos test, not by design review.** With a real Redis paused, a
single scored request took **21.8 seconds** against a configured 20 ms timeout.
Two independent causes, both invisible until the failure was actually produced:

1. `redis-py` applies a default retry policy with exponential backoff, so the
   configured `socket_timeout` bounded one *attempt*, not one *call*. A single
   `GET` with a 50 ms timeout measured **4.10 s**; with retries disabled,
   **0.053 s**.
2. Even bounded, one request makes several Redis calls -- rate limit, replay
   lookup, feature snapshot, observe, replay store -- so an outage costs the sum
   of them. At 500 TPS that is not a degradation, it is a collapse: callers time
   out, retry, and the gateway is serving double the load it already cannot
   answer.

Disabling retries fixes (1). This fixes (2): after a few consecutive failures the
breaker opens and every subsequent call returns immediately without touching the
socket, so an outage costs **one probe per cooldown** rather than one timeout per
call per request. `docs/ARCHITECTURE.md` §18 pairs the 20 ms timeout with a
health probe for exactly this reason; the timeout alone bounds a call, and only a
breaker bounds the system.

**Failing open is the point.** Every method here returns rather than raises, and
an open breaker means "do not ask, assume unavailable". The caller degrades and
counts it (CLAUDE.md §3.7). A breaker that raised would convert the outage it
exists to contain into the 5xx §18 forbids.
"""

from __future__ import annotations

import threading
import time
from enum import StrEnum
from typing import Final

FAILURE_THRESHOLD: Final = 3
"""Consecutive failures before the circuit opens.

Not one: a single timeout is as likely to be a GC pause or a slow disk as an
outage, and opening on it would make the gateway degrade over noise. Not ten:
during a real outage each failure costs a full timeout on the hot path, so ten
would spend 200 ms of a 100 ms budget discovering something three had already
established.
"""

COOLDOWN_S: Final = 5.0
"""How long the circuit stays open before probing again.

Long enough that a restarting Redis is not hammered while it loads its AOF;
short enough that recovery is measured in seconds rather than minutes. Recovery
must be automatic -- a gateway that degrades correctly and stays degraded has
turned a transient outage into a permanent one.
"""


class CircuitState(StrEnum):
    CLOSED = "CLOSED"
    """Healthy: calls go through."""

    OPEN = "OPEN"
    """Failing: calls are skipped without touching the socket."""

    HALF_OPEN = "HALF_OPEN"
    """Cooldown elapsed: the next call is a probe. Success closes the circuit,
    failure reopens it for another cooldown."""


class CircuitBreaker:
    """Tracks a dependency's health. Thread-safe; one instance per dependency.

    Deliberately not a decorator or a context manager: the call sites need to
    distinguish "skipped because the circuit is open" from "attempted and
    failed", because only the first is free and both must be counted.
    """

    __slots__ = ("_cooldown_s", "_failures", "_lock", "_name", "_opened_at", "_threshold")

    def __init__(
        self,
        name: str,
        *,
        threshold: int = FAILURE_THRESHOLD,
        cooldown_s: float = COOLDOWN_S,
    ) -> None:
        self._name = name
        self._threshold = threshold
        self._cooldown_s = cooldown_s
        self._lock = threading.Lock()
        self._failures = 0
        self._opened_at: float | None = None

    @property
    def name(self) -> str:
        return self._name

    def state(self, *, now: float | None = None) -> CircuitState:
        moment = now if now is not None else time.monotonic()
        with self._lock:
            if self._opened_at is None:
                return CircuitState.CLOSED
            if moment - self._opened_at >= self._cooldown_s:
                return CircuitState.HALF_OPEN
            return CircuitState.OPEN

    def allows(self, *, now: float | None = None) -> bool:
        """Whether a call should be attempted at all.

        `HALF_OPEN` allows exactly the probe that decides the next cooldown.
        """
        return self.state(now=now) is not CircuitState.OPEN

    def record_success(self) -> None:
        """Close the circuit. A single success is enough.

        Requiring several would leave the gateway degraded for several requests
        after the dependency had already recovered, and those requests would be
        scored on fewer features for no reason.
        """
        with self._lock:
            self._failures = 0
            self._opened_at = None

    def record_failure(self, *, now: float | None = None) -> None:
        moment = now if now is not None else time.monotonic()
        with self._lock:
            self._failures += 1
            if self._failures >= self._threshold:
                self._opened_at = moment

    @property
    def consecutive_failures(self) -> int:
        with self._lock:
            return self._failures
