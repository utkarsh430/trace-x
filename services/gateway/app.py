"""`trace-gateway` — the synchronous scoring surface.

A thin entrypoint over `trace_core` (CLAUDE.md §4). Everything that decides
anything lives in the package; this module wires it to HTTP, and its job is the
things HTTP adds: authentication, rate limiting, idempotent replay, trace
propagation, and turning outcomes into the status codes
`docs/API_CONTRACTS.md` §4 specifies.

**The order of the checks is part of the design.** Authenticate before rate
limiting, because an unauthenticated caller must not be able to consume a real
token's budget. Refuse before rate limiting when this instance is not the online
store's fenced writer, because such a refusal must not consume a budget either
(ADR-0051). Rate limit before scoring, because the limiter exists to protect
the scoring path. Check the replay cache before scoring, because a replay must
cost nothing. Score before triage, because triage needs the decision. Each step
can degrade or refuse, and which one it does is the substance of ADR-0035.

**Dependencies are resolved once at start-up**, not per request: a rule pack
parsed per request would be both slow and a way for a reload to land halfway
through a decision.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import random
import time
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, Any, Final

import structlog
from fastapi import Depends, FastAPI, Header, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import ValidationError

from services.gateway.config import (
    REDIS_MAX_CONNECTIONS,
    REDIS_TIMEOUT_S,
    SERVICE_NAME,
    SERVICE_VERSION,
    GatewaySettings,
)
from services.gateway.errors import classify, describe, problem
from services.gateway.pipeline import (
    REASON_RATE_LIMIT,
    REASON_REDIS,
    REASON_WRITE_FAILED,
    ScoringOutcome,
    ScoringPipeline,
    TriageUnavailableError,
    absent_feature_reasons,
)
from trace_core.contracts import authorization
from trace_core.contracts.api.decision import RiskDecision
from trace_core.contracts.api.events_ingress import (
    AcceptedResponse,
    AuthorizationOutcomeRequest,
    DeviceEventRequest,
    IdentityEventRequest,
)
from trace_core.contracts.api.problem import ErrorType
from trace_core.contracts.api.transaction import TransactionRequest
from trace_core.contracts.envelope import ZERO_TRACE_ID, build_event
from trace_core.contracts.publish import EventPublisher
from trace_core.contracts.topics import IDENTITY_EVENTS_V1, TX_SCORED_V1
from trace_core.domain.enums import AuthorizationOutcome
from trace_core.domain.errors import FeatureWriteFailedError
from trace_core.domain.identifiers import uuid7
from trace_core.domain.time import event_time, from_millis, to_millis, utc_now
from trace_core.features.completeness import CompletenessGuard, HoleReason
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.observation import Event, Verification, authorization_observation
from trace_core.features.semantics import Stream, identity_stream
from trace_core.observability.logging import configure_logging
from trace_core.observability.metrics import HotPathMetrics, register_online_store_gauges
from trace_core.observability.telemetry import configure_telemetry, current_trace_id
from trace_core.observation.log import DELIVERY_TIMEOUT_MS, ObservationLog, Sequenced
from trace_core.observation.outbox_relay import MESSAGE_TIMEOUT_MS as RELAY_MESSAGE_TIMEOUT_MS
from trace_core.observation.outbox_relay import OutboxRelay
from trace_core.observation.scored_event import build_scored_event
from trace_core.observation.session import WriterSessionError
from trace_core.observation.supervisor import WriterSupervisor
from trace_core.repositories.circuit_breaker import CircuitBreaker
from trace_core.repositories.postgres_authorization import (
    AuthorizationOutcomeRecord,
    Delivery,
    PostgresAuthorizationStore,
    validate_event,
)
from trace_core.repositories.postgres_completeness import PostgresHoleLedger
from trace_core.repositories.postgres_sessions import writer_connection
from trace_core.repositories.postgres_triage import PostgresTriageStore, new_case_id
from trace_core.repositories.redis_idempotency import RedisIdempotencyCache, ReplayVerdict
from trace_core.repositories.redis_ratelimit import RedisRateLimiter
from trace_core.repositories.triage_event import (
    build_investigation_requested,
    outbox_row,
    producer_string,
)
from trace_core.rules.loader import RulePackLoader, default_loader
from trace_core.scoring.banding import load_thresholds
from trace_core.security.service_tokens import ServiceToken, ServiceTokenVerifier

HEADER_REQUEST_ID: Final = "X-Request-Id"
HEADER_IDEMPOTENCY: Final = "X-Idempotency-Key"
HEADER_DEGRADED: Final = "X-Trace-Degraded"
HEADER_FEATURE_SOURCE: Final = "X-Feature-Source"
HEADER_AUTHORIZATION: Final = "Authorization"
BEARER: Final = "Bearer "

log = structlog.get_logger(__name__)


@dataclass
class GatewayState:
    """Everything resolved at start-up and shared by every request."""

    settings: GatewaySettings
    verifier: ServiceTokenVerifier
    loader: RulePackLoader
    pipeline: ScoringPipeline
    metrics: HotPathMetrics
    limiter: RedisRateLimiter | None = None
    idempotency: RedisIdempotencyCache | None = None
    triage: PostgresTriageStore | None = None
    authorizations: PostgresAuthorizationStore | None = None
    """The system of record for authorization outcomes (ADR-0049 §4), on triage's pool."""
    redis: Any = None
    """The feature-store client (ADR-0044: `noeviction`)."""
    cache_redis: Any = None
    """The disposable-cache client (ADR-0044: `allkeys-lru`)."""
    pool: Any = None
    breaker: CircuitBreaker | None = None
    """Breaker for the FEATURE store. One per instance, not one per
    collaborator: the two collaborators on the cache instance share
    `cache_breaker` below, so an outage there is learned once, not twice --
    and it is a different outage from the feature store's, with a different
    consequence (a retry re-scored versus a decision made blind)."""
    cache_breaker: CircuitBreaker | None = None
    """Breaker for the disposable-cache store, shared by the limiter and the
    replay cache."""
    feature_store: Any = None
    """Kept on the state so readiness can report how warm the store is."""
    completeness: CompletenessGuard | None = None
    """The same guard the pipeline holds (ADR-0046 §5); readiness reports a pending hole."""
    writer: WriterSupervisor | None = None
    """The online store's fenced writer session (ADR-0051 §2). While it is not ready, neither is
    the instance, and every request that would write online state is refused. None when no fence
    is configured: in tests, and without a system of record, which readiness refuses anyway."""
    observation_log: ObservationLog | None = None
    """The durable observation log (ADR-0051 §3): every write that changes online state is sequenced
    in the writer's session and published. None when no fence is configured."""
    outbox_relay: OutboxRelay | None = None
    """The in-process outbox relay (ADR-0051 §7, option A); None unless enabled with a broker."""

    def ready(self) -> tuple[bool, dict[str, str]]:
        """Readiness, per ADR-0035.

        Postgres unreachable means a CRITICAL transaction's investigation cannot
        be durably recorded, so the instance reports NOT ready and a load
        balancer drains it. Redis unreachable does NOT fail readiness: the hot
        path is designed to work without it, and draining would turn a planned
        degradation into an outage.

        Not holding the online store's writer fence is NOT ready either (ADR-0051
        §2): another instance is, or may still be, writing the store, and this one
        refuses every request that would write it.
        """
        checks: dict[str, str] = {}
        healthy = True
        if self.pool is None:
            checks["postgres"] = "not configured"
            healthy = False
        else:
            try:
                with self.pool.connection() as conn:
                    conn.execute("SELECT 1")
                checks["postgres"] = "ok"
            except Exception as exc:
                checks["postgres"] = f"unreachable: {type(exc).__name__}"
                healthy = False
        try:
            checks["redis"] = "ok" if self.redis is not None and self.redis.ping() else "degraded"
        except Exception as exc:
            # Degraded, not unhealthy. §18: the hot path is fully functional on
            # rules alone, and it says so in every response it returns.
            checks["redis"] = f"degraded: {type(exc).__name__}"
        try:
            checks["redis_cache"] = (
                "ok" if self.cache_redis is not None and self.cache_redis.ping() else "degraded"
            )
        except Exception as exc:
            checks["redis_cache"] = f"degraded: {type(exc).__name__}"
        if self.writer is None:
            checks["writer_session"] = "not configured: online writes are not fenced"
        else:
            writer_ready = self.writer.ready
            checks["writer_session"] = self.writer.status
            healthy = healthy and writer_ready
        if self.observation_log is not None:
            # Reported, never gated on: a broker outage costs history coverage, never a decision.
            checks["observation_log"] = self.observation_log.status
        # Reported, never gated on: an undrained outbox delays events, it never loses them.
        checks["outbox_relay"] = "running" if self.outbox_relay is not None else "disabled"
        checks["feature_history"] = self._history_status()
        return healthy, checks

    def _history_status(self) -> str:
        """How much of the declared lookback the feature store can vouch for.

        Reported, never gated on: a warming store is a correct store that has
        not been running long enough, and failing readiness on it would take a
        healthy instance out of rotation for a day after every restart.
        Operators read it here; every decision carries it as
        `history_incomplete` until it clears (ADR-0044).
        """
        if self.completeness is not None and self.completeness.pending:
            return "withdrawn: an unrecorded observation is pending; no completeness is claimed"
        store = self.feature_store
        if store is None or not hasattr(store, "epoch_key") or self.redis is None:
            return "unknown"
        try:
            stored = self.redis.get(store.epoch_key)
        except Exception as exc:
            return f"unknown: {type(exc).__name__}"
        if stored is None:
            return "empty: no epoch; complete for nothing until the first write"
        from trace_core.domain.time import from_millis
        from trace_core.features.state_plan import PLAN

        since = from_millis(int(stored))
        warm_at = since + dt.timedelta(seconds=PLAN.widest_lookback_s)
        now = dt.datetime.now(dt.UTC)
        if now >= warm_at:
            return f"complete since {since.isoformat()}"
        return (
            f"warming since {since.isoformat()}; "
            f"complete for every feature at {warm_at.isoformat()}"
        )


def _prepare_online_state(completeness: CompletenessGuard | None, store: Any) -> None:
    """The start-up writes to online state, run only by the fenced writer (ADR-0051 §2).

    A hole a previous process recorded and never withdrew is inherited, and withdrawn before the
    epoch is (re)established (ADR-0046 §5). The epoch is set NX, so a store that has been running
    for a day is not re-dated by a restart. Until the store has warmed for the widest declared
    lookback every decision carries `history_incomplete`; readiness reports when that clears
    (ADR-0044).
    """
    if completeness is not None:
        completeness.resume()
        completeness.reconcile()
    if store is not None and hasattr(store, "establish_epoch"):
        try:
            since = store.establish_epoch()
            log.info("feature_store_epoch", complete_since=since.isoformat())
        except Exception as exc:
            log.warning("feature_store_epoch_unavailable", error=type(exc).__name__)


def _observation_publisher(settings: GatewaySettings, client_id: str) -> EventPublisher | None:
    """The observation log's producer, or None when no broker is configured (ADR-0051 §3).

    Building one contacts no broker: its topics are verified at start, off the request path.
    """
    if not settings.kafka_bootstrap_servers:
        log.warning("observation_log_not_configured", detail="no writer session will close")
        return None
    try:
        return EventPublisher.connect(
            settings.kafka_bootstrap_servers,
            client_id=client_id,
            message_timeout_ms=DELIVERY_TIMEOUT_MS,
        )
    except Exception as exc:  # a missing client library: scoring continues, nothing is logged
        log.error("observation_log_unavailable", error=type(exc).__name__)
        return None


def _outbox_relay(
    settings: GatewaySettings, pool: Any, client_id: str, metrics: HotPathMetrics
) -> OutboxRelay | None:
    """The in-gateway outbox relay (ADR-0051 §7, option A), or None.

    Off unless enabled, and never without a broker: an enabled relay with nowhere to publish would
    claim rows only to fail them.
    """
    if not settings.outbox_relay or pool is None:
        return None
    if not settings.kafka_bootstrap_servers:
        log.warning("outbox_relay_not_started", detail="enabled without a broker; nothing relayed")
        return None
    try:
        publisher = EventPublisher.connect(
            settings.kafka_bootstrap_servers,
            client_id=client_id,
            message_timeout_ms=RELAY_MESSAGE_TIMEOUT_MS,
        )
    except Exception as exc:  # a missing client library: the outbox waits, nothing is lost
        log.error("outbox_relay_unavailable", error=type(exc).__name__)
        return None
    return OutboxRelay(
        pool=pool,
        publisher=publisher,
        record=lambda topic, outcome, count: metrics.outbox_relay_rows.add(
            count, {"topic": topic, "outcome": outcome.value}
        ),
    )


def build_state(settings: GatewaySettings | None = None) -> GatewayState:
    """Resolve dependencies. Raises if the gateway cannot serve correctly.

    Service tokens and the rule pack are both fatal when absent, for the reason
    ARCHITECTURE §18 gives about a corrupt model artifact: a gateway that serves
    traffic it cannot authenticate or cannot score is worse than one that is
    down, because it looks like it is working.
    """
    resolved = settings or GatewaySettings.from_environment()
    verifier = ServiceTokenVerifier.from_environment()
    loader = default_loader(frozenset(ONLINE_FEATURES.ids))
    pack = loader.load()
    thresholds = load_thresholds()

    redis_client: Any = None
    cache_client: Any = None
    limiter: RedisRateLimiter | None = None
    idempotency: RedisIdempotencyCache | None = None
    feature_store: Any = None
    try:
        import redis as redis_module
        from redis.backoff import NoBackoff
        from redis.retry import Retry

        from trace_core.repositories.redis_features import RedisOnlineFeatureStore

        def _client(url: str) -> Any:
            return redis_module.Redis.from_url(
                url,
                decode_responses=True,
                socket_timeout=REDIS_TIMEOUT_S,
                socket_connect_timeout=REDIS_TIMEOUT_S,
                retry=Retry(NoBackoff(), 0),
                retry_on_timeout=False,
                max_connections=REDIS_MAX_CONNECTIONS,
            )

        # Retries DISABLED, deliberately and with a measurement behind it.
        # redis-py applies a default retry policy with exponential backoff, so
        # `socket_timeout` bounds one ATTEMPT rather than one call: a chaos run
        # measured a single GET against a paused Redis at 4.10 s under a 50 ms
        # timeout, and 0.053 s with retries off. On a path with a 100 ms budget a
        # retry is not resilience -- the caller has already given up, and the
        # retry is load the gateway adds to an outage (ADR-0035).
        # Two instances with opposite contracts (ADR-0044). Redis eviction
        # policy is instance-wide, so the only way for feature state to be
        # `noeviction` while the replay cache is `allkeys-lru` is two servers.
        redis_client = _client(resolved.redis_url)
        cache_client = _client(resolved.redis_cache_url)
        feature_store = RedisOnlineFeatureStore(redis_client)
        limiter = RedisRateLimiter(
            cache_client, limit=resolved.rate_limit, window_s=resolved.rate_limit_window_s
        )
        idempotency = RedisIdempotencyCache(cache_client)
    except ModuleNotFoundError:
        # Not fatal: a Redis-less gateway is the documented degraded mode, and
        # every response it returns says so.
        log.warning("redis_client_unavailable", detail="scoring will run rules-only")

    pool: Any = None
    triage: PostgresTriageStore | None = None
    authorizations: PostgresAuthorizationStore | None = None
    try:
        from psycopg_pool import ConnectionPool

        pool = ConnectionPool(
            resolved.postgres_dsn,
            min_size=resolved.pool_min_size,
            max_size=resolved.pool_max_size,
            open=False,
        )
        triage = PostgresTriageStore(pool)
        authorizations = PostgresAuthorizationStore(pool)
    except ModuleNotFoundError:  # pragma: no cover - the db extra is required
        log.error("postgres_pool_unavailable", detail="triage cannot be recorded")

    breaker = CircuitBreaker("redis-features")
    instance_id = f"gateway-{uuid.uuid4().hex[:12]}"
    completeness = (
        CompletenessGuard(
            feature_store,
            PostgresHoleLedger(pool) if pool is not None else None,
            instance_id=instance_id,
        )
        if feature_store is not None
        else None
    )
    # One process writes online state at a time (ADR-0051 §2). The fence needs the system of
    # record; without one the gateway is not ready anyway.
    writer = (
        WriterSupervisor(
            connect=lambda: writer_connection(resolved.postgres_dsn),
            producer=producer_string(SERVICE_VERSION),
            instance_id=instance_id,
            on_acquired=lambda: _prepare_online_state(completeness, feature_store),
        )
        if pool is not None
        else None
    )
    metrics = HotPathMetrics()
    observation_log = (
        ObservationLog(
            writer=writer,
            publisher=_observation_publisher(resolved, instance_id),
            topics=(TX_SCORED_V1, IDENTITY_EVENTS_V1),
            record=lambda topic, outcome: metrics.observation_log.add(
                1, {"topic": topic, "outcome": outcome.value}
            ),
        )
        if writer is not None
        else None
    )
    outbox_relay = _outbox_relay(resolved, pool, f"{instance_id}-relay", metrics)
    cache_breaker = CircuitBreaker("redis-cache")
    return GatewayState(
        settings=resolved,
        verifier=verifier,
        loader=loader,
        pipeline=ScoringPipeline(
            pack=pack,
            thresholds=thresholds,
            feature_store=feature_store,
            producer_version=SERVICE_VERSION,
            breaker=breaker,
            completeness=completeness,
        ),
        metrics=metrics,
        limiter=limiter,
        idempotency=idempotency,
        triage=triage,
        authorizations=authorizations,
        redis=redis_client,
        cache_redis=cache_client,
        pool=pool,
        breaker=breaker,
        completeness=completeness,
        cache_breaker=cache_breaker,
        feature_store=feature_store,
        writer=writer,
        observation_log=observation_log,
        outbox_relay=outbox_relay,
    )


def create_app(state: GatewayState | None = None) -> FastAPI:
    """Build the ASGI application."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging()
        configure_telemetry(SERVICE_NAME, prometheus=True)

        resolved = state or build_state()
        app.state.gateway = resolved

        # Opened before anything reads the database, the completeness guard's hole ledger
        # first: a pool that is still closed reads as an unreadable ledger, which cannot vouch
        # for the absence of a hole (ADR-0046 §5).
        if resolved.pool is not None:
            try:
                resolved.pool.open(wait=True, timeout=10)
            except Exception as exc:
                # Not fatal at start-up: Postgres may come up after the gateway,
                # and readiness already reports the instance as not ready. A
                # crash loop here would make a slow database into an outage.
                log.warning("postgres_pool_not_ready", error=type(exc).__name__)

        # The start-up writes to online state run only in the fenced writer (ADR-0051 §2): the
        # supervisor acquires the session, runs them, and only then reports ready. Its first step
        # runs here, so a gateway with no predecessor starts ready; later steps run on its thread.
        if resolved.writer is not None:
            resolved.writer.start()
        else:
            _prepare_online_state(resolved.completeness, resolved.feature_store)
        if resolved.observation_log is not None:
            resolved.observation_log.start()
        if resolved.outbox_relay is not None:
            resolved.outbox_relay.start()

        # Each Redis instance's own memory and eviction counters, as gauges
        # labelled by store. Registered here rather than in `build_state`
        # because it needs the meter provider `configure_telemetry` installed.
        def _store_info(client: Any) -> Callable[[], dict[str, int]]:
            def read() -> dict[str, int]:
                """Never raises. A failed callback fails the whole /metrics
                scrape, which would hide every other metric to report one."""
                if client is None:
                    return {}
                try:
                    info = client.info("memory") | client.info("stats")
                except Exception:
                    return {}
                return {
                    key: int(info[key])
                    for key in ("used_memory", "evicted_keys")
                    if isinstance(info.get(key), int | str)
                }

            return read

        register_online_store_gauges(
            SERVICE_NAME,
            {"features": _store_info(resolved.redis), "cache": _store_info(resolved.cache_redis)},
        )
        yield
        if resolved.writer is not None:
            # The session closes only on a confirmed flush of everything it sequenced (ADR-0051
            # §4). Without a log nothing was published, so it is left unclosed: a gap, not a claim.
            confirmed = resolved.observation_log is not None and resolved.observation_log.close()
            resolved.writer.stop(confirmed=confirmed)
        if resolved.outbox_relay is not None:
            resolved.outbox_relay.stop()
        if resolved.pool is not None:
            resolved.pool.close()

    app = FastAPI(
        title="TRACE-X Gateway",
        version=SERVICE_VERSION,
        summary="Synchronous fraud scoring and triage.",
        description=(
            "The hot path: one transaction in, one banded, explainable RiskDecision out, "
            "within a 100 ms p99 budget. Every decision cites the rules that fired, the "
            "feature values they read, and the digests of the rule pack and thresholds "
            "that produced it."
        ),
        lifespan=lifespan,
        openapi_url="/openapi.json",
        docs_url=None,
        redoc_url=None,
    )
    _register_middleware(app)
    _register_handlers(app)
    _register_routes(app)
    return app


def _register_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def _observe_request(request: Request, call_next: Any) -> Response:
        """One structured log line per request, and the trace context to pivot on.

        **What is logged is a closed list, not "the request".** Every field below
        is either computed by us or an identifier the redaction pipeline already
        recognises; the body is never logged. Request fields are
        attacker-controlled and may carry PII (docs/SECURITY.md §10), and
        `PIIRedactingProcessor` is a backstop rather than a licence -- the cheapest
        way to keep PII out of logs is not to put it there.

        `X-Request-Id` is echoed on EVERY response including errors, because the
        identifier a caller quotes in a support ticket has to exist on the
        response that went wrong.
        """
        request_id = _request_id(request)
        started = time.perf_counter()
        response: Response = await call_next(request)
        response.headers.setdefault(HEADER_REQUEST_ID, request_id)
        if request.url.path not in _QUIET_PATHS:
            log.info(
                "http_request",
                method=request.method,
                path=request.url.path,
                status=response.status_code,
                duration_ms=round((time.perf_counter() - started) * 1000, 3),
                request_id=request_id,
            )
        return response


_QUIET_PATHS: Final[frozenset[str]] = frozenset({"/healthz", "/readyz", "/metrics"})
"""Probe endpoints are not logged.

A Kubernetes liveness probe every second is 86,400 log lines a day that say
nothing, and they are the lines that push the useful ones out of a retention
window."""


# --------------------------------------------------------------- plumbing ---


def _gateway(request: Request) -> GatewayState:
    state: GatewayState = request.app.state.gateway
    return state


def _request_id(request: Request) -> str:
    supplied = request.headers.get(HEADER_REQUEST_ID)
    return supplied or f"req_{uuid.uuid4().hex}"


def _trace_id() -> str:
    return current_trace_id() or ZERO_TRACE_ID


def _problem_response(
    request: Request,
    error: ErrorType,
    *,
    detail: str,
    errors: list[str] | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    document = problem(
        error,
        detail=detail,
        instance=str(request.url.path),
        trace_id=_trace_id(),
        request_id=_request_id(request),
        errors=errors,
    )
    response_headers = {HEADER_REQUEST_ID: document.request_id}
    response_headers.update(headers or {})
    return JSONResponse(
        status_code=document.status,
        content=document.model_dump(mode="json"),
        media_type="application/problem+json",
        headers=response_headers,
    )


def _register_handlers(app: FastAPI) -> None:
    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = list(exc.errors())
        return _problem_response(
            request,
            classify(errors),
            detail="The request could not be accepted as submitted.",
            errors=describe(errors),
        )

    @app.exception_handler(ValidationError)
    async def _model_validation(request: Request, exc: ValidationError) -> JSONResponse:
        errors = list(exc.errors())
        return _problem_response(
            request,
            classify(errors),
            detail="The request could not be accepted as submitted.",
            errors=describe(errors),
        )

    @app.exception_handler(TriageUnavailableError)
    async def _triage_unavailable(request: Request, exc: TriageUnavailableError) -> JSONResponse:
        # The exception's own message is NOT returned: it may carry a DSN, a
        # constraint name or a driver error, and an error response is a place
        # people paste into tickets (docs/SECURITY.md §10).
        del exc
        # ADR-0035: the decision was reached, but the investigation it demands
        # cannot be durably recorded. Approving and losing the case would be
        # worse than refusing, so this is the one dependency failure that is NOT
        # a degraded success.
        return _problem_response(
            request,
            ErrorType.SERVICE_UNAVAILABLE,
            detail=(
                "The decision could not be durably recorded, so it was not returned. "
                "Retry with the same X-Idempotency-Key."
            ),
            headers={"Retry-After": "5"},
        )


def _authenticate(request: Request) -> ServiceToken:
    state = _gateway(request)
    header = request.headers.get(HEADER_AUTHORIZATION, "")
    presented = header[len(BEARER) :] if header.startswith(BEARER) else None
    token = state.verifier.verify(presented)
    if token is None:
        state.metrics.unauthenticated.add(1)
        raise _UnauthenticatedError
    return token


class _UnauthenticatedError(Exception):
    """Raised by the auth dependency; converted to a 401 problem document."""


PROBLEM_MEDIA_TYPE: Final = "application/problem+json"
PROBLEM_REF: Final = "#/components/schemas/Problem"

_PROBLEM_STATUSES: Final[dict[int, str]] = {
    400: "Malformed request: the body does not match the schema (§6.1).",
    401: "Unauthenticated: no valid service token was presented.",
    409: (
        "Conflict: an idempotency key reused with a different payload (§5); a different "
        "authorization outcome already recorded for the transaction; or an outcome naming another "
        "account than its transaction (ADR-0049)."
    ),
    422: "Semantically invalid: the shape is right and a value cannot be (§6.6).",
    429: "Rate limited. Carries Retry-After.",
    503: (
        "Unavailable; carries Retry-After. The decision or event could not be durably recorded, so "
        "it was not returned (ADR-0035, ADR-0049), or this instance is not the online store's "
        "fenced writer (ADR-0051)."
    ),
}

_PROBLEM_RESPONSES: Final[dict[int | str, dict[str, Any]]] = {
    status: {
        "description": description,
        # `content` only, with no `model`: passing a model ALSO registers an
        # `application/json` variant, and the spec would then advertise a media
        # type this service never serves. The `$ref` keeps one definition of the
        # schema in `components`, registered by `PROBLEM_SCHEMA_ROUTE` below.
        "content": {PROBLEM_MEDIA_TYPE: {"schema": {"$ref": PROBLEM_REF}}},
    }
    for status, description in _PROBLEM_STATUSES.items()
}


def _register_routes(app: FastAPI) -> None:
    @app.exception_handler(_UnauthenticatedError)
    async def _unauthenticated(request: Request, exc: _UnauthenticatedError) -> JSONResponse:
        del exc  # the signature is FastAPI's; the reply is deliberately generic
        return _problem_response(
            request,
            ErrorType.UNAUTHENTICATED,
            detail="A valid service token is required.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        """Liveness: is the process running. Never touches a dependency -- a
        liveness probe that fails on a database blip restarts a healthy process."""
        return {"status": "ok", "service": SERVICE_NAME, "version": SERVICE_VERSION}

    @app.get("/readyz", include_in_schema=False)
    async def readyz(request: Request, response: Response) -> dict[str, Any]:
        # Sync for the same reason as the scoring route: `ready()` runs a
        # Postgres `SELECT 1` and a Redis `PING`, both blocking. On the event
        # loop a readiness probe against a hung dependency would stall every
        # in-flight scored request -- the probe would take the instance out of
        # service by making it unhealthy.
        healthy, checks = _gateway(request).ready()
        response.status_code = 200 if healthy else 503
        return {"ready": healthy, "checks": checks}

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> PlainTextResponse:
        """Prometheus exposition of the instruments the application writes.

        Served from the same meter provider, so there is one definition of every
        counter. Available with the `obs` profile down (ARCHITECTURE §14): a
        scrape endpoint needs no collector.
        """
        from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

        return PlainTextResponse(generate_latest().decode(), media_type=CONTENT_TYPE_LATEST)

    @app.post(
        "/v1/transactions",
        response_model=RiskDecision,
        # Every error response is documented with the `Problem` schema and the
        # `application/problem+json` media type it is actually served as.
        # API_CONTRACTS §4 requires RFC 9457 "always" and says the error `type`
        # URIs are enumerated in the spec -- a document that described FastAPI's
        # default HTTPValidationError instead would be describing a shape this
        # service never returns, and a generated client would be built to parse it.
        responses=_PROBLEM_RESPONSES,
        summary="Score a transaction synchronously",
    )
    async def score_transaction(
        request: Request,
        response: Response,
        body: TransactionRequest,
        token: Annotated[ServiceToken, Depends(_authenticate)],
        idempotency_key: Annotated[str | None, Header(alias=HEADER_IDEMPOTENCY)] = None,
    ) -> Any:
        # `async def`, and the reason is a measurement that contradicted the
        # obvious reading of the code. Every repository call below is a
        # SYNCHRONOUS socket round trip, which is the textbook case for moving a
        # handler off the event loop -- and doing exactly that made throughput
        # WORSE. A controlled A/B on identical store state measured 342.1 TPS on
        # the event loop against 279.9 TPS in a 64-thread pool, with the scoring
        # core going from 0.81 ms to 39.26 ms p50 and CPU rising 60% while
        # throughput fell 18%. The work here is GIL-bound rather than I/O-bound,
        # so threads add contention and buy no overlap; serialising is closer to
        # optimal. ADR-0039 records the experiment and supersedes ADR-0038's
        # decision on this point.
        #
        # What makes it safe to block this loop is not that the calls are fast
        # but that they are BOUNDED: 20 ms socket timeouts with retries off, and
        # a circuit breaker that stops asking a dead dependency (ADR-0035).
        # Without those, one hung Redis would stall every in-flight request --
        # which is exactly the 21.8 s request the chaos suite found before the
        # breaker existed.
        # The request clock starts HERE, not after the replay lookup and not
        # before scoring: `gateway_request_latency_seconds` is meant to answer
        # "how long did this server take", so it has to include the rate limit,
        # the replay lookup, triage and the replay store. The scoring-only
        # number is a separate instrument (`tx_score_latency_seconds`), because
        # a single histogram that quietly spans some of the work is worse than
        # two that each say what they cover.
        request_began = time.perf_counter()
        state = _gateway(request)
        request_id = _request_id(request)
        response.headers[HEADER_REQUEST_ID] = request_id

        if not idempotency_key:
            # §5: every mutating POST requires one. Rejecting is better than
            # inventing one, which would make a retry a new request.
            return _problem_response(
                request,
                ErrorType.MALFORMED_REQUEST,
                detail=f"{HEADER_IDEMPOTENCY} is required on every mutating POST.",
            )

        if (refusal := _not_the_writer(request, surface="transaction")) is not None:
            return refusal

        degraded_reasons: list[str] = []
        limit_decision = None
        # The limiter and the replay cache live on the cache instance, so it is
        # the CACHE breaker that decides whether to ask them (ADR-0044). The
        # feature store has its own breaker, consulted inside the pipeline.
        redis_usable = state.cache_breaker is None or state.cache_breaker.allows()
        if state.limiter is not None and not redis_usable:
            # The circuit is open: skip rather than pay the timeout to re-learn
            # what it already knows. Fails OPEN and is counted, per CLAUDE.md §3.7.
            degraded_reasons.append(REASON_RATE_LIMIT)
            state.metrics.degraded.add(1, {"reason": REASON_RATE_LIMIT})
        elif state.limiter is not None:
            limit_decision = state.limiter.check(token.token_id, request_id=request_id)
            if limit_decision.degraded:
                degraded_reasons.append(REASON_RATE_LIMIT)
                state.metrics.degraded.add(1, {"reason": REASON_RATE_LIMIT})
            if not limit_decision.allowed:
                state.metrics.rate_limited.add(1, {"token": token.token_id})
                return _problem_response(
                    request,
                    ErrorType.RATE_LIMITED,
                    detail="The request budget for this service token is exhausted.",
                    headers={"Retry-After": str(limit_decision.retry_after_s)},
                )

        payload = body.model_dump(mode="json", exclude_none=True)
        if state.idempotency is not None and redis_usable:
            try:
                lookup = state.idempotency.lookup(idempotency_key, payload)
            except Exception:
                lookup = None
            if lookup is not None and lookup.verdict is ReplayVerdict.CONFLICT:
                return _problem_response(
                    request,
                    ErrorType.IDEMPOTENCY_CONFLICT,
                    detail=(
                        f"{HEADER_IDEMPOTENCY} was already used for a different payload. "
                        f"Use a new key for a new request."
                    ),
                )
            if lookup is not None and lookup.verdict is ReplayVerdict.REPLAY and lookup.response:
                state.metrics.replays.add(1)
                # Parsed back through the contract rather than returned as raw
                # bytes: a cached response that no longer satisfies the current
                # RiskDecision schema is a contract change that happened while
                # entries were live, and serving it would be quietly answering
                # today's caller with yesterday's shape. JSON mode, because the
                # stored form IS json (trace_core.contracts.base documents why
                # the two validation modes differ).
                replayed = RiskDecision.model_validate_json(lookup.response)
                state.metrics.request_latency.record(time.perf_counter() - request_began)
                response.headers[HEADER_DEGRADED] = str(replayed.degraded).lower()
                response.headers[HEADER_FEATURE_SOURCE] = replayed.feature_source.value
                return replayed

        # Sequence, write online, produce (ADR-0051 §3). The clock is checked before a number is
        # assigned, so a refused transaction consumes none; a fence lost since the readiness check
        # assigns nothing and writes nothing.
        observation_log = state.observation_log
        sequenced: Sequenced | None = None
        if observation_log is not None:
            if (problem := _future_skew_problem(request, body.occurred_at)) is not None:
                return problem
            try:
                sequenced = observation_log.sequence()
            except WriterSessionError:
                return _writer_refusal(request, surface="transaction")
        try:
            outcome = state.pipeline.score(body, extra_degraded=tuple(degraded_reasons))
        except BaseException:
            if observation_log is not None and sequenced is not None:
                observation_log.unpublished(sequenced, TX_SCORED_V1)
            raise
        decision = outcome.decision

        try:
            if state.pipeline.opens_investigation(decision):
                decision = _triage(state, outcome, body)
        finally:
            # Published even when triage refuses the request: the online store has recorded the
            # transaction, so the log must too, with the decision the pipeline reached.
            if observation_log is not None and sequenced is not None:
                _publish_scored(observation_log, sequenced, outcome, decision)

        # The transaction was recorded in the online store by the scoring read itself, before
        # this response: an answered transaction is never missing from the store (plan §4.1).
        _record(state, outcome, decision)

        if state.idempotency is not None:
            try:
                state.idempotency.remember(idempotency_key, payload, decision.model_dump_json())
            except Exception:
                # A lost replay, never a lost effect: the case is already
                # committed under its own constraint.
                state.metrics.degraded.add(1, {"reason": "idempotency_cache_unavailable"})

        state.metrics.request_latency.record(time.perf_counter() - request_began)
        response.headers[HEADER_DEGRADED] = str(decision.degraded).lower()
        response.headers[HEADER_FEATURE_SOURCE] = decision.feature_source.value
        return decision

    # The caller's token is taken as a parameter because an optional `X-Idempotency-Key` is
    # scoped to it: the event's identity is derived from (token, key), so a retry is recognised
    # and two callers' keys can never collide (plan §3 Q2, ADR-0046 §1).
    @app.post(
        "/v1/events/identity",
        status_code=202,
        summary="Ingest an identity event",
        response_model=AcceptedResponse,
        responses={status: _PROBLEM_RESPONSES[status] for status in (400, 401, 409, 422, 503)},
    )
    async def ingest_identity(
        request: Request,
        body: IdentityEventRequest,
        token: Annotated[ServiceToken, Depends(_authenticate)],
        idempotency_key: Annotated[str | None, Header(alias=HEADER_IDEMPOTENCY)] = None,
    ) -> Any:
        # Sync: `_ingest` writes to Redis. See `score_transaction`.
        if (problem := _future_skew_problem(request, body.occurred_at)) is not None:
            return problem
        return _ingest(
            request,
            token_id=token.token_id,
            idempotency_key=idempotency_key,
            payload=body.model_dump(mode="json", exclude_none=True),
            account_id=body.account_id,
            occurred_at=body.occurred_at,
            # Which stream a type feeds is declared beside the features (ADR-0046 §4), where
            # the offline implementation reads the same table. A successful login or an
            # enrolled second factor feeds none: counting them as identity changes reset
            # `hours_since_identity_change` on ordinary account activity.
            stream=identity_stream(body.identity_event_type.value),
            device_id=body.device_id,
            ip_id=body.ip_id,
        )

    @app.post(
        "/v1/events/device",
        status_code=202,
        summary="Ingest a device event",
        response_model=AcceptedResponse,
        responses={status: _PROBLEM_RESPONSES[status] for status in (400, 401, 409, 422)},
    )
    async def ingest_device(
        request: Request,
        body: DeviceEventRequest,
        token: Annotated[ServiceToken, Depends(_authenticate)],
        idempotency_key: Annotated[str | None, Header(alias=HEADER_IDEMPOTENCY)] = None,
    ) -> Any:
        # Sync: `_ingest` writes to Redis. See `score_transaction`.
        if (problem := _future_skew_problem(request, body.occurred_at)) is not None:
            return problem
        return _ingest(
            request,
            token_id=token.token_id,
            idempotency_key=idempotency_key,
            payload=body.model_dump(mode="json", exclude_none=True),
            account_id=body.account_id,
            occurred_at=body.occurred_at,
            stream=None,
            device_id=body.device_id,
            ip_id=body.ip_id,
        )

    @app.post(
        "/v1/events/authorization",
        status_code=202,
        summary="Ingest an authorization outcome",
        response_model=AcceptedResponse,
        responses={status: _PROBLEM_RESPONSES[status] for status in (400, 401, 409, 422, 503)},
    )
    async def ingest_authorization(
        request: Request,
        body: AuthorizationOutcomeRequest,
        token: Annotated[ServiceToken, Depends(_authenticate)],
    ) -> Any:
        # Sync: `_record_authorization` writes PostgreSQL, bounded by POSTGRES_TIMEOUT_S. See
        # `score_transaction`. The token authenticates; identity is the transaction id (ADR-0049
        # §4).
        del token
        if (problem := _future_skew_problem(request, body.decided_at)) is not None:
            return problem
        return _record_authorization(request, body)


def _future_skew_problem(request: Request, occurred_at: dt.datetime) -> Any:
    """A 422 for an event dated beyond the accepted future clock skew, as for transactions.

    Identity and device events had no future bound. That broke more than hygiene: an event dated
    days ahead and lost during a feature-store outage lies beyond the 24 h margin by which the
    completeness guard moves the epoch forward, so a later window could claim completeness over
    it (ADR-0046 §5). One bound for every recorded stream keeps the margin sufficient.
    """
    try:
        ScoringPipeline.check_clock(occurred_at, now=dt.datetime.now(dt.UTC))
    except ValueError as exc:
        return _problem_response(request, ErrorType.INVALID_REQUEST, detail=str(exc))
    return None


def _identity_event_id(token_id: str, idempotency_key: str | None) -> str:
    """The observation's id: derived from the caller's key when there is one.

    Deterministic from `(token, key)`, so a retry under the same key is the same observation
    and counts once. Without a key nothing distinguishes a retry from a repeat, so every
    delivery is new -- the limit ADR-0046 §1 states.
    """
    if not idempotency_key:
        return f"idev_{uuid.uuid4().hex}"
    digest = hashlib.sha256(f"{token_id}\x00{idempotency_key}".encode()).hexdigest()
    return f"idev_{digest[:32]}"


def _ingest(
    request: Request,
    *,
    account_id: str,
    occurred_at: dt.datetime,
    stream: Stream | None,
    token_id: str,
    idempotency_key: str | None,
    payload: dict[str, object],
    device_id: str | None = None,
    ip_id: str | None = None,
) -> Any:
    """202: update online state for later transactions, return nothing to score.

    `stream` is None for an event no released feature reads -- a successful login, an
    enrolled second factor, every device event today -- and such an event changes no
    online state (ADR-0046 §4). Otherwise best effort: these events sharpen a later
    decision, and failing the caller because the store is unavailable would make an
    optional signal into a required dependency. An observation the store did not record
    still withdraws its completeness (ADR-0046 §5).

    An event that feeds a stream is refused (503) while this instance is not the online store's
    fenced writer (ADR-0051 §2); an event that changes no online state is not.

    A key reused for a different payload is refused (409), as for transactions; without
    the replay cache that check is skipped and the first delivery is the observation.
    """
    if stream is not None and (refusal := _not_the_writer(request, surface="identity")) is not None:
        return refusal
    state = _gateway(request)
    request_id = _request_id(request)
    event_id = _identity_event_id(token_id, idempotency_key)
    cache_key = f"event:{token_id}:{idempotency_key}" if idempotency_key else None
    cache_usable = state.cache_breaker is None or state.cache_breaker.allows()
    if cache_key is not None and state.idempotency is not None and cache_usable:
        try:
            lookup = state.idempotency.lookup(cache_key, payload)
        except Exception:
            lookup = None
        if lookup is not None and lookup.verdict is ReplayVerdict.CONFLICT:
            return _problem_response(
                request,
                ErrorType.IDEMPOTENCY_CONFLICT,
                detail=(
                    f"{HEADER_IDEMPOTENCY} was already used for a different event. "
                    f"Use a new key for a new event."
                ),
            )
    # Sequenced before the online write, published after it whatever the store did (ADR-0051 §3).
    observation_log = state.observation_log if stream is not None else None
    sequenced: Sequenced | None = None
    if observation_log is not None:
        try:
            sequenced = observation_log.sequence()
        except WriterSessionError:
            return _writer_refusal(request, surface="identity")
    if stream is not None and state.pipeline.feature_store is not None:
        event = Event(
            stream=stream,
            occurred_at=event_time(occurred_at),
            account_id=account_id,
            device_id=device_id,
            ip_id=ip_id,
            event_id=event_id,
        )
        guard = state.pipeline.completeness
        if guard is not None:
            guard.reconcile()
        try:
            state.pipeline.feature_store.observe(event)
        except FeatureWriteFailedError:
            if guard is not None:
                guard.observation_unrecorded(HoleReason.REFUSED)
            state.metrics.degraded.add(1, {"reason": REASON_WRITE_FAILED})
        except Exception as exc:
            log.warning("feature_store_observe_failed", error=type(exc).__name__)
            if guard is not None:
                guard.observation_unrecorded(HoleReason.UNREACHABLE)
            state.metrics.degraded.add(1, {"reason": REASON_REDIS})
    if observation_log is not None and sequenced is not None:
        _publish_identity(
            observation_log,
            sequenced,
            payload=payload,
            token_id=token_id,
            idempotency_key=idempotency_key,
            observation_id=event_id,
            occurred_at=occurred_at,
        )
    accepted = AcceptedResponse(accepted=True, event_id=event_id, request_id=request_id)
    if cache_key is not None and state.idempotency is not None and cache_usable:
        with contextlib.suppress(Exception):
            state.idempotency.remember(cache_key, payload, accepted.model_dump_json())
    return accepted


_NOT_APPLIED: Final = "NOT_APPLIED"
"""The verification label of a delivery the online store did not apply: a conflict, no store, or a
store failure."""


def _record_authorization(request: Request, body: AuthorizationOutcomeRequest) -> Any:
    """Record an authorization outcome in the system of record, and only then acknowledge it.

    ADR-0049 §4, in order:
    - an outcome decided before its transaction occurred cannot observe it: 422;
    - this instance is not the online store's fenced writer: 503, before anything is recorded
      (ADR-0051 §2);
    - no system of record, an invalid event or a failed write: 503, and nothing is acknowledged;
    - the first delivery for the transaction is recorded with its outbox row: 202;
    - an identical redelivery changes nothing: 202, answered with the recorded event's id;
    - different content under the same transaction id is a conflict: 409, and the first delivery
      stays the observation.

    - then a recorded or duplicate delivery is applied online (ADR-0049 §6): one naming another
      account than the transaction the store holds is refused with 409 and never reaches a feature;
      a store failure is still 202, because the durable record has the outcome.

    Both times are compared and published at millisecond precision, the precision of the event.
    """
    state = _gateway(request)
    decided_ms = to_millis(event_time(body.decided_at))
    occurred_ms = to_millis(event_time(body.transaction_occurred_at))
    if decided_ms < occurred_ms:
        return _problem_response(
            request,
            ErrorType.INVALID_REQUEST,
            detail=(
                "decided_at precedes transaction_occurred_at: an outcome cannot be decided before "
                "its transaction (ADR-0049 §2)."
            ),
        )
    if (refusal := _not_the_writer(request, surface="authorization")) is not None:
        return refusal
    if state.authorizations is None:
        return _authorization_unavailable(request)
    outcome = body.authorization_outcome.value
    try:
        event = authorization.build_event(
            transaction_id=body.transaction_id,
            account_id=body.account_id,
            authorization_outcome=outcome,
            decided_ms=decided_ms,
            transaction_occurred_ms=occurred_ms,
            transaction_occurred_at=authorization.iso_millis(occurred_ms),
            producer=producer_string(SERVICE_VERSION),
            trace_id=_trace_id(),
            correlation_id=body.transaction_id,
            ingested_ms=to_millis(utc_now()),
        )
        validate_event(event)
    except Exception as exc:
        # A contract failure is not an outage, but it must not be acknowledged either.
        log.error("authorization_event_invalid", error=type(exc).__name__)
        return _authorization_unavailable(request)
    record = AuthorizationOutcomeRecord(
        transaction_id=body.transaction_id,
        account_id=body.account_id,
        authorization_outcome=outcome,
        decided_at=from_millis(decided_ms),
        transaction_occurred_at=from_millis(occurred_ms),
        event_id=str(event["envelope"]["event_id"]),
    )
    try:
        receipt = state.authorizations.record(record, event)
    except Exception as exc:
        log.warning("authorization_record_failed", error=type(exc).__name__)
        return _authorization_unavailable(request)
    log.info("authorization_outcome_delivered", delivery=receipt.delivery.value)
    applied = (
        None
        if receipt.delivery is Delivery.CONFLICT
        else _apply_authorization(state, body, decided_ms)
    )
    state.metrics.authorization_outcomes.add(
        1,
        {
            "delivery": receipt.delivery.value,
            "verification": _NOT_APPLIED if applied is None else applied.value,
        },
    )
    if receipt.delivery is Delivery.CONFLICT:
        return _problem_response(
            request,
            ErrorType.AUTHORIZATION_CONFLICT,
            detail=(
                "A different authorization outcome is already recorded for this transaction. The "
                "first delivery stays the observation (ADR-0049 §2)."
            ),
        )
    if applied is Verification.REJECTED:
        return _problem_response(
            request,
            ErrorType.AUTHORIZATION_ACCOUNT_MISMATCH,
            detail=(
                "The outcome names another account than the transaction it reports. It is recorded "
                "as reported and never reaches a feature (ADR-0049 §4)."
            ),
        )
    return AcceptedResponse(
        accepted=True, event_id=receipt.recorded.event_id, request_id=_request_id(request)
    )


def _apply_authorization(
    state: GatewayState, body: AuthorizationOutcomeRequest, decided_ms: int
) -> Verification | None:
    """Apply a durably recorded outcome to the online store; None when nothing was applied.

    Best effort, as for identity events: the durable record already holds the outcome. A store
    failure is counted as degraded and withdraws completeness, so no window claims to be complete
    over an outcome the store never saw (ADR-0049 §4; ADR-0046 §5).
    """
    store = state.pipeline.feature_store
    if store is None:
        return None
    observation = authorization_observation(
        transaction_id=body.transaction_id,
        account_id=body.account_id,
        authorization_outcome=AuthorizationOutcome(body.authorization_outcome.value),
        decided_at=event_time(from_millis(decided_ms)),
    )
    guard = state.pipeline.completeness
    if guard is not None:
        guard.reconcile()
    try:
        receipt = store.observe(observation)
    except FeatureWriteFailedError:
        if guard is not None:
            guard.observation_unrecorded(HoleReason.REFUSED)
        state.metrics.degraded.add(1, {"reason": REASON_WRITE_FAILED})
        return None
    except Exception as exc:
        log.warning("feature_store_observe_failed", error=type(exc).__name__)
        if guard is not None:
            guard.observation_unrecorded(HoleReason.UNREACHABLE)
        state.metrics.degraded.add(1, {"reason": REASON_REDIS})
        return None
    return receipt.verification


def _not_the_writer(request: Request, *, surface: str) -> JSONResponse | None:
    """503 while this instance is not the online store's fenced writer (ADR-0051 §2); else None.

    Checked before anything that writes online state, and before the rate limiter and the replay
    cache, so a refusal consumes no budget. Scoring records the transaction in the store, so it is
    refused; so are identity events a released feature reads, and authorization outcomes. Events
    that change no online state are not.
    """
    writer = _gateway(request).writer
    if writer is None or writer.ready:
        return None
    return _writer_refusal(request, surface=surface)


def _writer_refusal(request: Request, *, surface: str) -> JSONResponse:
    """The 503 for a request this instance may not serve as the online store's writer."""
    _gateway(request).metrics.writer_refused.add(1, {"surface": surface})
    return _problem_response(
        request,
        ErrorType.SERVICE_UNAVAILABLE,
        detail=(
            "This instance is not the fenced writer of the online store, so it neither scores nor "
            "records online state. Retry; a ready instance serves the request."
        ),
        headers={"Retry-After": "5"},
    )


IDENTITY_EVENT_TYPE: Final = "identity.events"
IDENTITY_EVENT_FIELDS: Final = (
    "account_id",
    "identity_event_type",
    "device_id",
    "ip_id",
    "user_agent",
)


def _publish_scored(
    observation_log: ObservationLog,
    sequenced: Sequenced,
    outcome: ScoringOutcome,
    decision: RiskDecision,
) -> None:
    """Produce the scored observation, whatever the store did with it (ADR-0051 §3). Never raises:
    an event that cannot be built leaves its number unpublished, and so its session unclosable."""
    try:
        event = build_scored_event(
            canonical=outcome.canonical,
            decision=decision,
            features=outcome.features,
            context=outcome.context,
            observe_outcome=outcome.observe_outcome.value,
            store_position=outcome.observe_position,
            store_epoch_ms=outcome.store_epoch_ms,
            producer=producer_string(SERVICE_VERSION),
            trace_id=_trace_id(),
        )
    except Exception as exc:
        log.error("scored_event_unbuildable", error=type(exc).__name__)
        observation_log.unpublished(sequenced, TX_SCORED_V1)
        return
    observation_log.publish(sequenced, TX_SCORED_V1, event)


def _identity_envelope_id(
    token_id: str, idempotency_key: str | None, occurred_ms: int
) -> uuid.UUID:
    """The published event's `event_id`, its topic's dedup identity (PHASE3_PLAN §3 Q2).

    Derived from `(token, key)` like the observation's own id, so a retry under the same key is the
    same event on Kafka as it is the same observation in the store. Without a key, every delivery is
    new in both places.
    """
    if not idempotency_key:
        return uuid7(millis=occurred_ms)
    digest = hashlib.sha256(f"{token_id}\x00{idempotency_key}".encode()).digest()
    return uuid7(millis=occurred_ms, rng=random.Random(int.from_bytes(digest, "big")))  # noqa: S311


def _publish_identity(
    observation_log: ObservationLog,
    sequenced: Sequenced,
    *,
    payload: dict[str, object],
    token_id: str,
    idempotency_key: str | None,
    observation_id: str,
    occurred_at: dt.datetime,
) -> None:
    """Produce an identity event that changed online state (ADR-0051 §1). Never raises."""
    moment = event_time(occurred_at)
    try:
        event = build_event(
            event_type=IDENTITY_EVENT_TYPE,
            occurred_at=moment,
            payload={key: payload[key] for key in IDENTITY_EVENT_FIELDS if key in payload},
            producer=producer_string(SERVICE_VERSION),
            trace_id=_trace_id(),
            correlation_id=observation_id,
            event_id=_identity_envelope_id(token_id, idempotency_key, to_millis(moment)),
        )
    except Exception as exc:
        log.error("identity_event_unbuildable", error=type(exc).__name__)
        observation_log.unpublished(sequenced, IDENTITY_EVENTS_V1)
        return
    observation_log.publish(sequenced, IDENTITY_EVENTS_V1, event)


def _authorization_unavailable(request: Request) -> JSONResponse:
    """503: the outcome was not durably recorded, so it is not acknowledged (ADR-0049 §4)."""
    return _problem_response(
        request,
        ErrorType.SERVICE_UNAVAILABLE,
        detail=(
            "The authorization outcome could not be durably recorded, so it was not accepted. "
            "Retry the delivery."
        ),
        headers={"Retry-After": "5"},
    )


def _triage(state: GatewayState, outcome: Any, body: TransactionRequest) -> RiskDecision:
    """Open a case, or refuse the request.

    The event is built and validated BEFORE the transaction, so a contract
    failure never rolls back a case for a reason unrelated to the database.
    """
    if state.triage is None:
        raise TriageUnavailableError("no system of record is configured")
    case_id = new_case_id()
    try:
        event = build_investigation_requested(
            decision=outcome.decision,
            evaluation=outcome.evaluation,
            case_id=case_id,
            account_id=body.account_id,
            occurred_at=event_time(body.occurred_at),
            producer=producer_string(SERVICE_VERSION),
            trace_id=_trace_id(),
            correlation_id=body.transaction_id,
        )
        topic, key, idem, payload = outbox_row(event)
        result = state.triage.open_case(
            case_id=case_id,
            decision=outcome.decision,
            account_id=body.account_id,
            occurred_at=body.occurred_at,
            outbox_topic=topic,
            outbox_partition_key=key,
            outbox_idempotency_key=idem,
            outbox_payload=payload,
        )
    except Exception as exc:
        raise TriageUnavailableError(str(exc)) from exc
    if result.created:
        state.metrics.triaged.add(1, {"band": outcome.decision.risk_band.value})
    return outcome.decision.model_copy(update={"case_id": result.case_id})


def _record(
    state: GatewayState,
    outcome: Any,
    decision: RiskDecision,
) -> None:
    """Emit the §13 metric set for one scored transaction."""
    metrics = state.metrics
    # From the decision's own measurement, not from a clock in the route. The
    # route's clock had been started before scoring and read after triage and
    # the observe-write, so `tx_score_latency_seconds` was reporting a Postgres
    # transaction as scoring time on the ~92% of load-profile traffic that
    # triages. `latency_ms` is what the caller is told, and the metric now
    # agrees with it by construction rather than by coincidence.
    metrics.latency.record(decision.latency_ms / 1000.0)
    metrics.feature_read_latency.record(outcome.feature_read_seconds)
    metrics.scored.add(1, {"band": decision.risk_band.value})
    for reason in decision.degraded_reasons:
        metrics.degraded.add(1, {"reason": reason})
    for fired in outcome.evaluation.fired:
        metrics.rules_fired.add(1, {"rule": fired.rule_id})
    for abstained in outcome.evaluation.abstained:
        metrics.abstained.add(1, {"rule": abstained.rule_id})
    for feature_id, reason in absent_feature_reasons(outcome.features).items():
        metrics.feature_unavailable.add(1, {"feature": feature_id, "reason": reason})


app_factory: Final[Callable[[], FastAPI]] = create_app
