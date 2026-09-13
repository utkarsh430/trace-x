"""Redis is killed under a running gateway, and the gateway keeps answering.

Declared acceptance command for `P2.degraded-mode`:
`pytest -m chaos tests/chaos/test_redis_down.py`.

`docs/TESTING.md` §1 requirement 4 is the one most often skipped: *"a degraded
mode that has never degraded is unimplemented regardless of how much code
exists."* Every other layer proves the degraded code path in isolation — the
pipeline unit tests with a raising store, the HTTP tests with no store
configured. Neither of those is the same claim as **a real Redis going away
mid-flight and the next request still being answered**, which is what
`docs/ARCHITECTURE.md` §18 actually promises.

So this pauses the real container. `docker pause` rather than `stop`, because
pausing produces the failure a network partition produces — connections hang and
time out — while stopping produces an immediate connection refusal. The timeout
path is the one with a latency budget attached, and it is the one that would
otherwise be discovered under load.

Three things are asserted, and the third is the one that makes the other two
worth having:

1. the gateway answers 200 rather than 5xx;
2. the answer is marked degraded and names the reason;
3. **the gateway recovers by itself** when Redis comes back, without a restart.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from services.gateway.app import GatewayState, create_app
from services.gateway.config import GatewaySettings
from services.gateway.pipeline import REASON_RATE_LIMIT, REASON_REDIS, ScoringPipeline

from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.observability.metrics import HotPathMetrics
from trace_core.repositories.circuit_breaker import CircuitBreaker
from trace_core.repositories.redis_features import RedisOnlineFeatureStore
from trace_core.repositories.redis_idempotency import RedisIdempotencyCache
from trace_core.repositories.redis_ratelimit import RedisRateLimiter
from trace_core.rules.loader import default_loader
from trace_core.scoring.banding import load_thresholds
from trace_core.security.service_tokens import MIN_SECRET_LENGTH, ServiceTokenVerifier

pytestmark = [pytest.mark.chaos, pytest.mark.integration]

SECRET = "s" * MIN_SECRET_LENGTH
AUTH = {"Authorization": f"Bearer psp-one.{SECRET}"}
CONTAINER = os.environ.get("TRACEX_REDIS_CONTAINER", "tracex-redis-1")
CACHE_CONTAINER = os.environ.get("TRACEX_REDIS_CACHE_CONTAINER", "tracex-redis-cache-1")

# Short enough that a paused Redis costs the request its 20 ms budget and no
# more (config.REDIS_TIMEOUT_S), long enough that an ordinary round trip on a
# loaded laptop is not mistaken for an outage.
SOCKET_TIMEOUT_S = 0.05


def _docker(*args: str) -> subprocess.CompletedProcess[str]:
    binary = shutil.which("docker") or "docker"
    # S603: every argument is a fixed literal or a container name from the
    # environment; there is no user input on this path. Same reasoning as
    # scripts/doctor.py, which is annotated the same way.
    return subprocess.run(  # noqa: S603
        [binary, *args], capture_output=True, text=True, timeout=30
    )


@pytest.fixture
def paused_redis() -> Iterator[Any]:
    """A live Redis that this test can pause and resume.

    Skips LOUDLY rather than passing when the container is not there: a chaos
    test that silently no-ops is a false claim that a failure mode was exercised
    (docs/TESTING.md §2.5).
    """
    redis = pytest.importorskip("redis", reason="the `db` extra provides the Redis client")
    if _docker("inspect", "--format", "{{.State.Running}}", CONTAINER).stdout.strip() != "true":
        pytest.skip(
            f"SKIPPED (NOT PASSED): container {CONTAINER!r} is not running, so Redis "
            f"cannot be killed and the degraded mode is NOT exercised. Run `make up`. "
            f"docs/TESTING.md §1 requirement 4: a degraded mode that has never "
            f"degraded is unimplemented."
        )
    # Built exactly as `build_state` builds it, retries included -- a harness
    # that configured its client differently would measure something the gateway
    # never runs. Retries off is the fix this very suite forced (ADR-0035).
    from redis.backoff import NoBackoff
    from redis.retry import Retry

    client = redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", "6389")),
        decode_responses=True,
        socket_timeout=SOCKET_TIMEOUT_S,
        socket_connect_timeout=SOCKET_TIMEOUT_S,
        retry=Retry(NoBackoff(), 0),
        retry_on_timeout=False,
    )
    client.flushdb()
    # A store that has been recording for longer than any feature looks back.
    # The fixture flushes the DB, so without this the baseline decision would
    # correctly carry `history_incomplete` -- and this suite is about the
    # OUTAGE degradation, which has to be distinguishable from warm-up
    # (ADR-0044). Set on the event-time axis, as the gateway would have.
    import datetime as dt

    from trace_core.domain.time import event_time
    from trace_core.features.state_plan import PLAN

    RedisOnlineFeatureStore(client).establish_epoch(
        at=event_time(
            dt.datetime.now(dt.UTC) - dt.timedelta(seconds=PLAN.widest_lookback_s + 3_600)
        )
    )
    try:
        yield client
    finally:
        # Always unpause, even on failure: leaving a paused container behind
        # would break every subsequent test run in a way nobody would attribute
        # to this file.
        _docker("unpause", CONTAINER)
        try:
            client.flushdb()
        except Exception as exc:  # pragma: no cover - best effort cleanup
            # Deliberately swallowed, and named rather than silent: the unpause
            # above is the cleanup that matters, and failing teardown on a flush
            # would mask the real assertion failure that led here.
            print(f"cleanup flushdb failed after unpause: {type(exc).__name__}")


def _cache_client() -> Any:
    """The disposable-cache instance, built like the feature client."""
    import redis
    from redis.backoff import NoBackoff
    from redis.retry import Retry

    return redis.Redis(
        host=os.environ.get("REDIS_CACHE_HOST", "localhost"),
        port=int(os.environ.get("REDIS_CACHE_PORT", "6390")),
        decode_responses=True,
        socket_timeout=SOCKET_TIMEOUT_S,
        socket_connect_timeout=SOCKET_TIMEOUT_S,
        retry=Retry(NoBackoff(), 0),
        retry_on_timeout=False,
    )


def _client(redis_client: Any) -> TestClient:
    """The gateway with the REAL topology: features here, cache on its own
    instance with its own breaker (ADR-0044). Pausing one must not look like
    the other."""
    breaker = CircuitBreaker("redis-features")
    cache_breaker = CircuitBreaker("redis-cache")
    cache = _cache_client()
    state = GatewayState(
        settings=GatewaySettings.from_environment({}),
        verifier=ServiceTokenVerifier({"psp-one": SECRET}),
        loader=default_loader(frozenset(ONLINE_FEATURES.ids)),
        pipeline=ScoringPipeline(
            pack=default_loader(frozenset(ONLINE_FEATURES.ids)).load(),
            thresholds=load_thresholds(),
            feature_store=RedisOnlineFeatureStore(redis_client),
            breaker=breaker,
        ),
        metrics=HotPathMetrics(),
        limiter=RedisRateLimiter(cache, limit=10_000, window_s=60),
        idempotency=RedisIdempotencyCache(cache),
        redis=redis_client,
        cache_redis=cache,
        breaker=breaker,
        cache_breaker=cache_breaker,
        feature_store=RedisOnlineFeatureStore(redis_client),
    )
    return TestClient(create_app(state))


def _body() -> dict[str, Any]:
    import datetime as dt

    return {
        "transaction_id": f"tx_{uuid.uuid4().hex[:16]}",
        "account_id": "acct_000001",
        "amount_minor": 5_000,
        "currency": "GBP",
        "occurred_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
        "merchant_id": "mrch_00001",
        "merchant_mcc": "5411",
        "merchant_country": "GB",
        "device_id": "dev_000001",
        "channel": "CARD_PRESENT",
    }


def _headers() -> dict[str, str]:
    return {**AUTH, "X-Idempotency-Key": f"idem-{uuid.uuid4().hex}"}


def _post(client: TestClient) -> Any:
    return client.post("/v1/transactions", json=_body(), headers=_headers())


# --- the failure -----------------------------------------------------------------


def test_the_gateway_answers_while_redis_is_paused(paused_redis: Any) -> None:
    """ARCHITECTURE §18: rules-only, `degraded=true`, and NEVER a 5xx.

    The whole point of the fail-open direction: declining all traffic is worse
    than missing fraud for a few minutes (CLAUDE.md §3.7).
    """
    with _client(paused_redis) as client:
        healthy = _post(client)
        assert healthy.status_code == 200
        assert healthy.headers["X-Trace-Degraded"] == "false"

        assert _docker("pause", CONTAINER).returncode == 0, "could not pause Redis"
        degraded = _post(client)

    assert degraded.status_code == 200, (
        f"the gateway returned {degraded.status_code} with Redis unreachable. "
        f"§18 requires rules-only degradation, never a 5xx."
    )
    assert degraded.headers["X-Trace-Degraded"] == "true"
    reasons = degraded.json()["degraded_reasons"]
    assert REASON_REDIS in reasons, (
        "the decision did not name the reason it was degraded; an untagged "
        "fail-open is indistinguishable from a healthy decision (CLAUDE.md §3.7)"
    )
    assert REASON_RATE_LIMIT not in reasons, (
        "the limiter lives on the cache instance, which was not paused. Reporting it "
        "degraded here means the two instances are not actually separate (ADR-0044)."
    )


def test_a_degraded_decision_reports_what_it_could_not_see(paused_redis: Any) -> None:
    """A decision made with no features is not a confident "no risk found"."""
    with _client(paused_redis) as client:
        assert _docker("pause", CONTAINER).returncode == 0
        decision = _post(client).json()

    assert decision["decision"] == "APPROVE"
    assert decision["insufficient_history_features"], (
        "a decision made with every feature absent claimed to have assessed them"
    )
    assert decision["reasons"] == [], "a rule fired with no features available"


def test_no_rule_fires_on_absent_features(paused_redis: Any) -> None:
    """The property that makes degraded mode safe rather than merely quiet.

    A missing feature must make a rule ABSTAIN, not evaluate false. If it
    evaluated false the score would be a confident zero, and a degraded gateway
    would look exactly like a quiet hour.
    """
    with _client(paused_redis) as client:
        assert _docker("pause", CONTAINER).returncode == 0
        decision = _post(client).json()
    assert decision["score"] == 0.0
    assert decision["risk_band"] == "LOW"
    # The distinguishing evidence: the decision says its inputs were missing.
    assert decision["insufficient_history_features"]


def test_the_latency_budget_bounds_the_degraded_path(paused_redis: Any) -> None:
    """A paused Redis must cost the timeout, not the request.

    An unbounded read would turn a dependency outage into a latency incident:
    the caller times out, retries, and the gateway is now serving double load it
    cannot answer. `config.REDIS_TIMEOUT_S` exists for this.
    """
    with _client(paused_redis) as client:
        assert _docker("pause", CONTAINER).returncode == 0
        began = time.perf_counter()
        response = _post(client)
        elapsed = time.perf_counter() - began

    assert response.status_code == 200
    assert elapsed < 5.0, (
        f"a degraded request took {elapsed:.2f}s. The read must be bounded by a "
        f"timeout, or a Redis outage becomes a latency incident."
    )


# --- the recovery ------------------------------------------------------------------


def test_the_gateway_recovers_without_a_restart(paused_redis: Any) -> None:
    """The assertion that makes the others worth having.

    A gateway that degrades correctly but stays degraded has converted a
    transient outage into a permanent one, and the symptom -- decisions that are
    quietly worse than they should be -- is invisible without this test.
    """
    with _client(paused_redis) as client:
        assert _docker("pause", CONTAINER).returncode == 0
        assert _post(client).headers["X-Trace-Degraded"] == "true"

        assert _docker("unpause", CONTAINER).returncode == 0
        # One short settle: the client's connection pool may hold a socket that
        # died while paused, and the first request after resuming is allowed to
        # rediscover that rather than being counted as a failure to recover.
        time.sleep(0.2)
        _post(client)
        recovered = _post(client)

    assert recovered.status_code == 200
    assert recovered.headers["X-Trace-Degraded"] == "false", (
        "the gateway stayed degraded after Redis returned; a transient outage "
        "became a permanent one"
    )


def test_readiness_survives_a_redis_outage(paused_redis: Any) -> None:
    """Redis loss must NOT drain the instance.

    ADR-0035: the hot path is designed to work without Redis, so failing
    readiness for it would turn a planned degradation into an outage. Only
    Postgres loss drains, because only that means a case cannot be recorded.
    """
    with _client(paused_redis) as client:
        assert _docker("pause", CONTAINER).returncode == 0
        health = client.get("/healthz")
        readiness = client.get("/readyz")

    assert health.status_code == 200, "liveness must not depend on Redis"
    assert "redis" in readiness.json()["checks"]
    assert "degraded" in readiness.json()["checks"]["redis"]


# --- the OTHER instance: losing it must not change a decision ----------------------


@pytest.fixture
def paused_cache(paused_redis: Any) -> Iterator[Any]:
    """The cache container, pausable, with the feature store healthy and warm."""
    if (
        _docker("inspect", "--format", "{{.State.Running}}", CACHE_CONTAINER).stdout.strip()
        != "true"
    ):
        pytest.skip(
            f"SKIPPED (NOT PASSED): container {CACHE_CONTAINER!r} is not running, so the "
            f"cache instance cannot be lost and its disposability is NOT exercised."
        )
    try:
        yield paused_redis
    finally:
        _docker("unpause", CACHE_CONTAINER)


def test_losing_the_cache_instance_changes_no_decision(paused_cache: Any) -> None:
    """ADR-0044's classification, exercised: the cache holds nothing a decision
    depends on. Pause it and the SAME transaction gets the SAME band, score and
    fired rules -- only the limiter and the replay cache report themselves
    unavailable, and the feature store is not blamed for it."""
    body = _body()
    with _client(paused_cache) as client:
        before = client.post("/v1/transactions", json=body, headers=_headers()).json()
        assert _docker("pause", CACHE_CONTAINER).returncode == 0, "could not pause the cache"
        after = client.post(
            "/v1/transactions",
            json={**body, "transaction_id": "tx_after_cache_loss"},
            headers=_headers(),
        ).json()
        readiness = client.get("/readyz").json()["checks"]

    for field_name in ("risk_band", "score", "decision"):
        assert after[field_name] == before[field_name], (
            f"{field_name} changed when the CACHE was lost: {before[field_name]} -> "
            f"{after[field_name]}. Nothing in the cache may influence a decision."
        )
    assert [r["rule_id"] for r in after["reasons"]] == [r["rule_id"] for r in before["reasons"]]
    reasons = set(after["degraded_reasons"])
    assert REASON_RATE_LIMIT in reasons, "the limiter is on the cache and must say it failed open"
    assert REASON_REDIS not in reasons, "the feature store was healthy; it must not be blamed"
    assert readiness["redis"] == "ok" and "degraded" in readiness["redis_cache"]


# --- a healthy but EMPTY feature store must not look warm ----------------------------


def test_an_emptied_feature_store_says_its_history_is_incomplete(paused_redis: Any) -> None:
    """Restart recovery, exercised: FLUSHALL is what a restart with AOF off
    leaves behind. The store is healthy, reachable and empty, and the next
    decision must say `history_incomplete` -- never `redis_unavailable`, and
    never a clean answer. The first write re-establishes an epoch, at now."""
    with _client(paused_redis) as client:
        warm = _post(client)
        assert warm.headers["X-Trace-Degraded"] == "false", "fixture: the store should be warm"

        paused_redis.flushall()
        cold = _post(client)
        readiness = client.get("/readyz").json()["checks"]

    assert cold.status_code == 200
    assert cold.headers["X-Trace-Degraded"] == "true"
    reasons = set(cold.json()["degraded_reasons"])
    assert "history_incomplete" in reasons, (
        "an empty store produced a decision that did not admit its history was gone. "
        "This is the silent restart failure ADR-0044 exists to prevent."
    )
    assert REASON_REDIS not in reasons, "empty is not unavailable; the store answered"
    assert "warming" in readiness["feature_history"] or "empty" in readiness["feature_history"]
    assert paused_redis.get(RedisOnlineFeatureStore(paused_redis).epoch_key) is not None, (
        "the write after the flush did not re-establish an epoch"
    )
