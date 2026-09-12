"""Idempotency, in two tiers that guarantee different things (ADR-0035).

`docs/API_CONTRACTS.md` §5 asks for two behaviours that are easy to conflate:

* **replay** — the same key and the same payload returns the original *response*;
* **one side effect** — the same transaction produces one case, whatever happens.

They need different mechanisms, because they fail differently.

**Postgres is authoritative for the effect.** `cases.trigger_transaction_id` is
UNIQUE, so "duplicate transaction_id yields one effect" holds under concurrency,
across restarts, and with the cache cold or gone. That is a database guarantee.

**Redis is a response cache, and only that.** It makes a replay byte-identical
and cheap. It is not authoritative for anything: if it is empty the request is
re-scored, and the answer may differ because the features moved in between. That
is a real and stated limitation, not a bug -- and the response says so, because a
caller that believed a replay was byte-identical when it was not would reconcile
against the wrong decision.

**Two keys on one endpoint, and they answer different questions.**
`X-Idempotency-Key` is the client's handle on one HTTP attempt; `transaction_id`
is the business identity of the payment. A retrying processor legitimately
regenerates the header, so a *new* header with an already-scored transaction and
an identical payload must replay rather than conflict. The header conflicts only
with itself: same key, different payload is a client bug and is surfaced as 409
rather than absorbed (§5).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final

from trace_core.contracts.canonical_json import content_hash

if TYPE_CHECKING:  # pragma: no cover - typing only
    from redis import Redis

REPLAY_TTL_S: Final = 24 * 3600
"""24 hours, per docs/API_CONTRACTS.md §5.

Long enough to cover any realistic client retry window, short enough that the
cache is bounded. Action-execution keys are retained permanently in Postgres
instead, because those gate real financial effects (Phase 8).
"""


class ReplayVerdict(StrEnum):
    """What the cache says about a request."""

    FRESH = "FRESH"
    """No record of this key. Score it."""

    REPLAY = "REPLAY"
    """Same key, same payload: return the stored response, take no new action."""

    CONFLICT = "CONFLICT"
    """Same key, DIFFERENT payload. A client bug, surfaced as 409 rather than
    absorbed: silently answering the first request's decision to a second,
    different request would be worse than an error."""


@dataclass(frozen=True, slots=True)
class ReplayLookup:
    verdict: ReplayVerdict
    response: str | None = None
    """The stored response body, present only on REPLAY."""


class RedisIdempotencyCache:
    """Stores one response per idempotency key, for replay.

    Deliberately small: it holds the fingerprint of the request that produced a
    response and the response itself. Anything more would make it look like a
    system of record, which it must not become -- it can be flushed at any moment
    and the system must still be correct.
    """

    def __init__(self, client: Redis, *, namespace: str = "idem") -> None:
        self._redis = client
        self._ns = namespace

    def _key(self, idempotency_key: str) -> str:
        return f"{self._ns}:{idempotency_key}"

    @staticmethod
    def fingerprint(payload: object) -> str:
        """A stable hash of the request, for detecting a reused key.

        Canonical JSON, so key order and whitespace do not make an identical
        request look different -- which would turn every retry into a 409.
        """
        return content_hash(payload)

    def lookup(self, idempotency_key: str, payload: object) -> ReplayLookup:
        """Classify a request against what this key has been used for before."""
        stored = self._redis.hgetall(self._key(idempotency_key))
        if not stored:
            return ReplayLookup(ReplayVerdict.FRESH)
        fields = {_text(k): _text(v) for k, v in stored.items()}
        if fields.get("fingerprint") != self.fingerprint(payload):
            return ReplayLookup(ReplayVerdict.CONFLICT)
        return ReplayLookup(ReplayVerdict.REPLAY, response=fields.get("response"))

    def remember(self, idempotency_key: str, payload: object, response: str) -> None:
        """Store a response for replay. Best effort by design.

        A failure here loses a replay, never a side effect: the effect is already
        committed in Postgres under its own constraint. So this must not raise
        into the request path -- refusing to answer a caller because a cache write
        failed would turn a cache outage into an outage.
        """
        key = self._key(idempotency_key)
        pipe = self._redis.pipeline(transaction=False)
        pipe.hset(key, mapping={"fingerprint": self.fingerprint(payload), "response": response})
        pipe.expire(key, REPLAY_TTL_S)
        pipe.execute()


def _text(value: object) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)
