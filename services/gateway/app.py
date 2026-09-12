"""`trace-gateway` — the synchronous scoring surface.

A thin entrypoint over `trace_core` (CLAUDE.md §4). Everything that decides
anything lives in the package; this module wires it to HTTP, and its job is the
things HTTP adds: authentication, rate limiting, idempotent replay, trace
propagation, and turning outcomes into the status codes
`docs/API_CONTRACTS.md` §4 specifies.

**The order of the checks is part of the design.** Authenticate before rate
limiting, because an unauthenticated caller must not be able to consume a real
token's budget. Rate limit before scoring, because the limiter exists to protect
the scoring path. Check the replay cache before scoring, because a replay must
cost nothing. Score before triage, because triage needs the decision. Each step
can degrade or refuse, and which one it does is the substance of ADR-0035.

**Dependencies are resolved once at start-up**, not per request: a rule pack
parsed per request would be both slow and a way for a reload to land halfway
through a decision.
"""

from __future__ import annotations

import datetime as dt
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
    REDIS_TIMEOUT_S,
    SERVICE_NAME,
    SERVICE_VERSION,
    GatewaySettings,
)
from services.gateway.errors import classify, describe, problem
from services.gateway.pipeline import (
    REASON_RATE_LIMIT,
    ScoringPipeline,
    TriageUnavailableError,
    absent_feature_reasons,
)
from trace_core.contracts.api.decision import RiskDecision
from trace_core.contracts.api.events_ingress import (
    AcceptedResponse,
    DeviceEventRequest,
    IdentityEventRequest,
)
from trace_core.contracts.api.problem import ErrorType
from trace_core.contracts.api.transaction import TransactionRequest
from trace_core.contracts.envelope import ZERO_TRACE_ID
from trace_core.domain.time import event_time
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.reference import Event
from trace_core.features.semantics import Stream
from trace_core.observability.logging import configure_logging
from trace_core.observability.metrics import HotPathMetrics
from trace_core.observability.telemetry import configure_telemetry, current_trace_id
from trace_core.repositories.circuit_breaker import CircuitBreaker
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
    redis: Any = None
    pool: Any = None
    breaker: CircuitBreaker | None = None
    """One breaker for the whole Redis dependency, shared by the feature store,
    the limiter and the replay cache. Per-collaborator breakers would each have
    to learn the outage separately, which is three timeouts instead of one."""

    def ready(self) -> tuple[bool, dict[str, str]]:
        """Readiness, per ADR-0035.

        Postgres unreachable means a CRITICAL transaction's investigation cannot
        be durably recorded, so the instance reports NOT ready and a load
        balancer drains it. Redis unreachable does NOT fail readiness: the hot
        path is designed to work without it, and draining would turn a planned
        degradation into an outage.
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
        return healthy, checks


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
    limiter: RedisRateLimiter | None = None
    idempotency: RedisIdempotencyCache | None = None
    feature_store: Any = None
    try:
        import redis as redis_module
        from redis.backoff import NoBackoff
        from redis.retry import Retry

        from trace_core.repositories.redis_features import RedisOnlineFeatureStore

        # Retries DISABLED, deliberately and with a measurement behind it.
        # redis-py applies a default retry policy with exponential backoff, so
        # `socket_timeout` bounds one ATTEMPT rather than one call: a chaos run
        # measured a single GET against a paused Redis at 4.10 s under a 50 ms
        # timeout, and 0.053 s with retries off. On a path with a 100 ms budget a
        # retry is not resilience -- the caller has already given up, and the
        # retry is load the gateway adds to an outage (ADR-0035).
        redis_client = redis_module.Redis.from_url(
            resolved.redis_url,
            decode_responses=True,
            socket_timeout=REDIS_TIMEOUT_S,
            socket_connect_timeout=REDIS_TIMEOUT_S,
            retry=Retry(NoBackoff(), 0),
            retry_on_timeout=False,
        )
        feature_store = RedisOnlineFeatureStore(redis_client)
        limiter = RedisRateLimiter(
            redis_client, limit=resolved.rate_limit, window_s=resolved.rate_limit_window_s
        )
        idempotency = RedisIdempotencyCache(redis_client)
    except ModuleNotFoundError:
        # Not fatal: a Redis-less gateway is the documented degraded mode, and
        # every response it returns says so.
        log.warning("redis_client_unavailable", detail="scoring will run rules-only")

    pool: Any = None
    triage: PostgresTriageStore | None = None
    try:
        from psycopg_pool import ConnectionPool

        pool = ConnectionPool(
            resolved.postgres_dsn,
            min_size=resolved.pool_min_size,
            max_size=resolved.pool_max_size,
            open=False,
        )
        triage = PostgresTriageStore(pool)
    except ModuleNotFoundError:  # pragma: no cover - the db extra is required
        log.error("postgres_pool_unavailable", detail="triage cannot be recorded")

    breaker = CircuitBreaker("redis")
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
        ),
        metrics=HotPathMetrics(),
        limiter=limiter,
        idempotency=idempotency,
        triage=triage,
        redis=redis_client,
        pool=pool,
        breaker=breaker,
    )


def create_app(state: GatewayState | None = None) -> FastAPI:
    """Build the ASGI application."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging()
        configure_telemetry(SERVICE_NAME, prometheus=True)
        resolved = state or build_state()
        app.state.gateway = resolved
        if resolved.pool is not None:
            try:
                resolved.pool.open(wait=True, timeout=10)
            except Exception as exc:
                # Not fatal at start-up: Postgres may come up after the gateway,
                # and readiness already reports the instance as not ready. A
                # crash loop here would make a slow database into an outage.
                log.warning("postgres_pool_not_ready", error=type(exc).__name__)
        yield
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
        responses={
            400: {"description": "Malformed request"},
            401: {"description": "Unauthenticated"},
            409: {"description": "Idempotency key reused with a different payload"},
            422: {"description": "Semantically invalid request"},
            429: {"description": "Rate limited"},
            503: {"description": "The decision could not be durably recorded"},
        },
        summary="Score a transaction synchronously",
    )
    async def score_transaction(
        request: Request,
        response: Response,
        body: TransactionRequest,
        token: Annotated[ServiceToken, Depends(_authenticate)],
        idempotency_key: Annotated[str | None, Header(alias=HEADER_IDEMPOTENCY)] = None,
    ) -> Any:
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

        degraded_reasons: list[str] = []
        limit_decision = None
        redis_usable = state.breaker is None or state.breaker.allows()
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
                response.headers[HEADER_DEGRADED] = str(replayed.degraded).lower()
                response.headers[HEADER_FEATURE_SOURCE] = replayed.feature_source.value
                return replayed

        started = time.perf_counter()
        outcome = state.pipeline.score(body, extra_degraded=tuple(degraded_reasons))
        decision = outcome.decision

        if state.pipeline.opens_investigation(decision):
            decision = _triage(state, outcome, body)

        observe_reason = state.pipeline.observe(outcome.canonical)
        _record(state, outcome, decision, started, observe_reason=observe_reason)

        if state.idempotency is not None:
            try:
                state.idempotency.remember(idempotency_key, payload, decision.model_dump_json())
            except Exception:
                # A lost replay, never a lost effect: the case is already
                # committed under its own constraint.
                state.metrics.degraded.add(1, {"reason": "idempotency_cache_unavailable"})

        response.headers[HEADER_DEGRADED] = str(decision.degraded).lower()
        response.headers[HEADER_FEATURE_SOURCE] = decision.feature_source.value
        return decision

    # `dependencies=` rather than a parameter: these endpoints need the caller
    # to be AUTHENTICATED but do not need to know who it is. Taking the token as
    # an argument and ignoring it would read as an oversight.
    @app.post(
        "/v1/events/identity",
        status_code=202,
        summary="Ingest an identity event",
        dependencies=[Depends(_authenticate)],
    )
    async def ingest_identity(
        request: Request,
        body: IdentityEventRequest,
    ) -> AcceptedResponse:
        return _ingest(
            request,
            account_id=body.account_id,
            occurred_at=body.occurred_at,
            stream=(
                Stream.IDENTITY_FAILED_LOGIN
                if body.identity_event_type.value == "LOGIN_FAILED"
                else Stream.IDENTITY_CHANGE
            ),
            device_id=body.device_id,
            ip_id=body.ip_id,
        )

    @app.post(
        "/v1/events/device",
        status_code=202,
        summary="Ingest a device event",
        dependencies=[Depends(_authenticate)],
    )
    async def ingest_device(
        request: Request,
        body: DeviceEventRequest,
    ) -> AcceptedResponse:
        return _ingest(
            request,
            account_id=body.account_id,
            occurred_at=body.occurred_at,
            stream=Stream.TRANSACTION,
            device_id=body.device_id,
            ip_id=body.ip_id,
            observe=False,
        )


def _ingest(
    request: Request,
    *,
    account_id: str,
    occurred_at: dt.datetime,
    stream: Stream,
    device_id: str | None = None,
    ip_id: str | None = None,
    observe: bool = True,
) -> AcceptedResponse:
    """202: update online state for later transactions, return nothing to score.

    Best effort. These events sharpen a later decision; failing the caller
    because the store is unavailable would make an optional signal into a
    required dependency.
    """
    state = _gateway(request)
    request_id = _request_id(request)
    event = Event(
        stream=stream,
        occurred_at=event_time(occurred_at),
        account_id=account_id,
        device_id=device_id,
        ip_id=ip_id,
        event_id=request_id,
    )
    if observe and state.pipeline.feature_store is not None:
        try:
            state.pipeline.feature_store.observe(event)
        except Exception:
            state.metrics.degraded.add(1, {"reason": "redis_unavailable"})
    return AcceptedResponse(accepted=True, event_id=request_id, request_id=request_id)


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
    started: float,
    *,
    observe_reason: str | None,
) -> None:
    """Emit the §13 metric set for one scored transaction."""
    metrics = state.metrics
    metrics.latency.record(time.perf_counter() - started)
    metrics.feature_read_latency.record(outcome.feature_read_seconds)
    metrics.scored.add(1, {"band": decision.risk_band.value})
    for reason in decision.degraded_reasons:
        metrics.degraded.add(1, {"reason": reason})
    if observe_reason is not None:
        metrics.degraded.add(1, {"reason": observe_reason})
    for fired in outcome.evaluation.fired:
        metrics.rules_fired.add(1, {"rule": fired.rule_id})
    for abstained in outcome.evaluation.abstained:
        metrics.abstained.add(1, {"rule": abstained.rule_id})
    for feature_id, reason in absent_feature_reasons(outcome.features).items():
        metrics.feature_unavailable.add(1, {"feature": feature_id, "reason": reason})


app_factory: Final[Callable[[], FastAPI]] = create_app
