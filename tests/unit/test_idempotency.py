"""Two-tier idempotency: what each tier guarantees, and what it does not.

Declared acceptance command for `P2.idempotency`:
`pytest tests/unit/test_idempotency.py`.

The distinction this module exists to pin is that **replay and "one side effect"
are different guarantees with different mechanisms**, and only one of them
survives Redis being gone. Conflating them is how a system ends up claiming
exactly-once because it has a cache.

The Postgres half -- one case per transaction under concurrency -- is a database
guarantee and is proven against a real database in
`tests/integration/test_triage_store.py`. Asserting it here with a fake would
prove only that the fake behaves.
"""

from __future__ import annotations

from typing import Any

import pytest

from trace_core.repositories.redis_idempotency import (
    REPLAY_TTL_S,
    RedisIdempotencyCache,
    ReplayVerdict,
)

pytestmark = pytest.mark.unit


class _FakeRedis:
    """A hash store with TTLs. Unit layer: everything outside the unit may be
    faked (docs/TESTING.md §4), and the unit here is the cache's LOGIC.

    The same cache runs against a real Redis in the integration suite, which is
    where the wire behaviour is actually established.
    """

    def __init__(self) -> None:
        self.data: dict[str, dict[str, str]] = {}
        self.ttl: dict[str, int] = {}

    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.data.get(key, {}))

    def pipeline(self, transaction: bool = True) -> _FakeRedis:
        del transaction
        return self

    def hset(self, key: str, mapping: dict[str, Any]) -> None:
        self.data.setdefault(key, {}).update({k: str(v) for k, v in mapping.items()})

    def expire(self, key: str, seconds: int) -> None:
        self.ttl[key] = seconds

    def execute(self) -> None:
        return None


@pytest.fixture
def fake() -> _FakeRedis:
    return _FakeRedis()


@pytest.fixture
def cache(fake: _FakeRedis) -> RedisIdempotencyCache:
    return RedisIdempotencyCache(fake)  # type: ignore[arg-type]


PAYLOAD = {"transaction_id": "tx_1", "amount_minor": 1999, "currency": "GBP"}


# --- the three verdicts --------------------------------------------------------


def test_an_unseen_key_is_fresh(cache: RedisIdempotencyCache) -> None:
    assert cache.lookup("idem-1", PAYLOAD).verdict is ReplayVerdict.FRESH


def test_the_same_key_and_payload_replays_the_original_response(
    cache: RedisIdempotencyCache,
) -> None:
    """docs/API_CONTRACTS.md §5: the original response, and no new side effect."""
    cache.remember("idem-1", PAYLOAD, '{"decision":"APPROVE"}')
    result = cache.lookup("idem-1", PAYLOAD)
    assert result.verdict is ReplayVerdict.REPLAY
    assert result.response == '{"decision":"APPROVE"}'


def test_the_same_key_with_a_different_payload_conflicts(
    cache: RedisIdempotencyCache,
) -> None:
    """Surfaced as 409, never absorbed. Answering the first request's decision to
    a second, different request would be worse than an error."""
    cache.remember("idem-1", PAYLOAD, '{"decision":"APPROVE"}')
    changed = {**PAYLOAD, "amount_minor": 500_000}
    assert cache.lookup("idem-1", changed).verdict is ReplayVerdict.CONFLICT


def test_a_different_key_is_independent(cache: RedisIdempotencyCache) -> None:
    cache.remember("idem-1", PAYLOAD, '{"decision":"APPROVE"}')
    assert cache.lookup("idem-2", PAYLOAD).verdict is ReplayVerdict.FRESH


# --- the fingerprint -----------------------------------------------------------


def test_key_order_does_not_make_an_identical_request_look_different(
    cache: RedisIdempotencyCache,
) -> None:
    """Otherwise every retry whose client serialised its JSON differently would
    come back 409, and the endpoint would be unusable."""
    cache.remember("idem-1", PAYLOAD, "{}")
    reordered = dict(reversed(list(PAYLOAD.items())))
    assert cache.lookup("idem-1", reordered).verdict is ReplayVerdict.REPLAY


@pytest.mark.parametrize(
    "changed",
    [
        {"amount_minor": 2000},
        {"currency": "EUR"},
        {"transaction_id": "tx_2"},
    ],
)
def test_any_material_change_is_detected(
    cache: RedisIdempotencyCache, changed: dict[str, Any]
) -> None:
    cache.remember("idem-1", PAYLOAD, "{}")
    assert cache.lookup("idem-1", {**PAYLOAD, **changed}).verdict is ReplayVerdict.CONFLICT


def test_the_fingerprint_is_a_content_hash() -> None:
    """Stored rather than the payload itself: the request carries attacker-
    controlled strings and may carry PII, and a cache is a poor place for either."""
    digest = RedisIdempotencyCache.fingerprint(PAYLOAD)
    assert digest.startswith("sha256:")
    assert "tx_1" not in digest


# --- what the cache stores, and what it must not -------------------------------


def test_the_entry_expires(cache: RedisIdempotencyCache, fake: _FakeRedis) -> None:
    """24 h per §5. An unbounded cache would become a system of record by
    accident, which is exactly what it must not be."""
    cache.remember("idem-1", PAYLOAD, "{}")
    assert fake.ttl["idem:idem-1"] == REPLAY_TTL_S
    assert REPLAY_TTL_S == 24 * 3600


def test_the_cache_stores_only_a_fingerprint_and_a_response(
    cache: RedisIdempotencyCache, fake: _FakeRedis
) -> None:
    """Anything more would make it look authoritative. It can be flushed at any
    moment and the system must still be correct."""
    cache.remember("idem-1", PAYLOAD, '{"decision":"APPROVE"}')
    stored = fake.data["idem:idem-1"]
    assert set(stored) == {"fingerprint", "response"}
    assert "1999" not in stored["fingerprint"]


# --- the boundary between the two tiers ----------------------------------------


def test_a_cold_cache_reports_fresh_rather_than_asserting_novelty(
    cache: RedisIdempotencyCache, fake: _FakeRedis
) -> None:
    """The tier boundary, stated as a test.

    FRESH means "this cache has no record", NOT "this transaction is new". The
    caller must still let Postgres decide whether a case already exists --
    `cases.trigger_transaction_id` is UNIQUE, and that is the guarantee that
    survives a flushed or absent Redis. A caller that treated FRESH as proof of
    novelty would create a second side effect after any cache eviction.
    """
    cache.remember("idem-1", PAYLOAD, '{"decision":"APPROVE"}')
    assert cache.lookup("idem-1", PAYLOAD).verdict is ReplayVerdict.REPLAY

    fake.data.clear()
    assert cache.lookup("idem-1", PAYLOAD).verdict is ReplayVerdict.FRESH, (
        "a flushed cache must report FRESH -- and the caller must then rely on the "
        "database constraint rather than on this answer"
    )


def test_remembering_is_best_effort_and_never_raises_into_the_request_path() -> None:
    """A cache write failure loses a replay, never a side effect -- the effect is
    already committed under its own constraint. Refusing to answer a caller
    because a cache write failed would turn a cache blip into an outage.
    """

    class _Failing(_FakeRedis):
        def execute(self) -> None:
            raise ConnectionError("redis is gone")

    failing = RedisIdempotencyCache(_Failing())  # type: ignore[arg-type]
    with pytest.raises(ConnectionError):
        failing.remember("idem-1", PAYLOAD, "{}")
    # The guard belongs at the call site, which is the gateway's degraded path:
    # this test pins that the cache itself does NOT silently swallow the error,
    # so the caller can count it (`degraded_mode_total{reason=...}`) rather than
    # discovering later that replays quietly stopped working.
