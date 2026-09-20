"""Authorization outcomes against a real PostgreSQL (ADR-0049 §4).

Fixtures F4, F5, F14 and F15 where they are decided: in the system of record, and through the
gateway route that writes it.
- **F4.** An identical redelivery is a duplicate: nothing is written, and it is answered with the
  first delivery's event id.
- **F5.** Different content under the same transaction id is a conflict, and the first delivery
  stays the observation.
- **F14.** The database decides both. No cache is involved at all.
- **F15.** A gateway whose system of record is unreachable acknowledges nothing.

The application role cannot rewrite a recorded outcome: it has no UPDATE and no DELETE.

Then applied online, against a real Redis (ADR-0049 §6):
- **Verified.** An outcome for a held transaction of the same account counts.
- **F9.** One naming another account is refused with 409, recorded as reported, and never counted.
- **F10, F13.** While its transaction is not held, an outcome, its duplicate and a conflicting
  delivery count nothing; once the transaction is recorded the first delivery counts, once.
- **F14.** After the online store is flushed, PostgreSQL still decides the duplicate and the
  conflict.
- **A store that cannot apply it.** The outcome is still acknowledged, because the durable record
  holds it, and completeness is withdrawn.
"""

from __future__ import annotations

import datetime as dt
import os
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from tests.conformance.feature_semantics_suite import transaction

from trace_core.contracts import authorization
from trace_core.contracts.topics import TX_AUTHORIZATION_V1
from trace_core.domain.time import event_time, from_millis
from trace_core.features.context import WindowState
from trace_core.features.observation import transaction_observation
from trace_core.features.semantics import ONE_HOUR, Entity, Stream
from trace_core.repositories.postgres_authorization import (
    AuthorizationOutcomeRecord,
    Delivery,
    PostgresAuthorizationStore,
    validate_event,
)

pytestmark = pytest.mark.integration

OCCURRED_MS = 1_789_200_000_123
TRACE_ID = "0" * 32
SECRET = "s" * 32
HEADERS = {"Authorization": f"Bearer psp-one.{SECRET}"}
REDIS_DB = int(os.environ.get("REDIS_AUTHORIZATION_TEST_DB", "13"))
"""Not the live gateway's database, nor the semantics suites' (15): these tests flush it."""


def _dsn(user_env: str, password_env: str, *, port: str | None = None) -> str:
    return "postgresql://{u}:{p}@{h}:{port}/{db}".format(
        u=os.environ.get(user_env, ""),
        p=os.environ.get(password_env, ""),
        h=os.environ.get("POSTGRES_HOST", "localhost"),
        port=port or os.environ.get("POSTGRES_PORT", "5442"),
        db=os.environ.get("POSTGRES_DB", "tracex"),
    )


def _truncate() -> None:
    """As the owner: `trace_app` has no DELETE, and granting it one for a fixture would weaken
    the control this module tests."""
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(
        _dsn("POSTGRES_SUPERUSER", "POSTGRES_SUPERUSER_PASSWORD"), autocommit=True
    ) as owner:
        owner.execute("TRUNCATE app.authorization_outcomes, app.outbox")


@pytest.fixture
def pool() -> Iterator[Any]:
    psycopg_pool = pytest.importorskip("psycopg_pool", reason="the `db` extra provides it")
    psycopg = pytest.importorskip("psycopg")
    try:
        with psycopg.connect(_dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD")) as probe:
            table = probe.execute("SELECT to_regclass('app.authorization_outcomes')").fetchone()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no PostgreSQL reachable as trace_app ({exc}). Run `make up`; "
            f"the outcome record is a database guarantee and is never mocked (docs/TESTING.md §4)."
        )
    if table is None or table[0] is None:  # pragma: no cover - environment dependent
        pytest.skip(
            "SKIPPED (NOT PASSED): app.authorization_outcomes does not exist. Run `make migrate`."
        )
    _truncate()
    created = psycopg_pool.ConnectionPool(
        _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD"), min_size=1, max_size=4, open=True
    )
    yield created
    created.close()
    _truncate()


def _delivery(
    transaction_id: str,
    *,
    outcome: str = "DECLINED",
    decided_offset_ms: int = 340,
    account: str = "acct_000001",
) -> tuple[AuthorizationOutcomeRecord, dict[str, Any]]:
    decided = OCCURRED_MS + decided_offset_ms
    event = authorization.build_event(
        transaction_id=transaction_id,
        account_id=account,
        authorization_outcome=outcome,
        decided_ms=decided,
        transaction_occurred_ms=OCCURRED_MS,
        transaction_occurred_at=authorization.iso_millis(OCCURRED_MS),
        producer="trace-gateway@0.0.0",
        trace_id=TRACE_ID,
        correlation_id=transaction_id,
        ingested_ms=decided + 60,
    )
    validate_event(event)
    record = AuthorizationOutcomeRecord(
        transaction_id=transaction_id,
        account_id=account,
        authorization_outcome=outcome,
        decided_at=from_millis(decided),
        transaction_occurred_at=from_millis(OCCURRED_MS),
        event_id=str(event["envelope"]["event_id"]),
    )
    return record, event


def _outbox_rows(pool: Any, transaction_id: str) -> int:
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT count(*) FROM app.outbox "
            "WHERE topic = %s AND payload -> 'payload' ->> 'transaction_id' = %s",
            (TX_AUTHORIZATION_V1, transaction_id),
        ).fetchone()
    return int(row[0])


def test_the_first_delivery_is_recorded_with_its_outbox_row(pool: Any) -> None:
    store = PostgresAuthorizationStore(pool)
    record, event = _delivery("tx_it_first")
    receipt = store.record(record, event)
    assert receipt.delivery is Delivery.RECORDED
    assert store.recorded("tx_it_first") == record
    assert _outbox_rows(pool, "tx_it_first") == 1


def test_an_identical_redelivery_is_a_duplicate_and_writes_nothing(pool: Any) -> None:
    """F4. A producer retry carries a new event id and the same content."""
    store = PostgresAuthorizationStore(pool)
    record, event = _delivery("tx_it_duplicate")
    store.record(record, event)
    again, again_event = _delivery("tx_it_duplicate")
    assert again.event_id != record.event_id
    receipt = store.record(again, again_event)
    assert receipt.delivery is Delivery.DUPLICATE
    assert receipt.recorded.event_id == record.event_id
    assert _outbox_rows(pool, "tx_it_duplicate") == 1


@pytest.mark.parametrize(
    "change",
    [{"outcome": "APPROVED"}, {"decided_offset_ms": 341}, {"account": "acct_000002"}],
    ids=["outcome", "decided-time", "account"],
)
def test_different_content_is_a_conflict_and_never_overwrites(
    pool: Any, change: dict[str, Any]
) -> None:
    """F5 and F14: the first delivery stays the observation, decided by the database alone."""
    store = PostgresAuthorizationStore(pool)
    record, event = _delivery("tx_it_conflict")
    store.record(record, event)
    other, other_event = _delivery("tx_it_conflict", **change)
    receipt = store.record(other, other_event)
    assert receipt.delivery is Delivery.CONFLICT
    assert receipt.recorded == record
    assert store.recorded("tx_it_conflict") == record
    assert _outbox_rows(pool, "tx_it_conflict") == 1


def test_the_application_role_cannot_rewrite_a_recorded_outcome(pool: Any) -> None:
    psycopg = pytest.importorskip("psycopg")
    store = PostgresAuthorizationStore(pool)
    record, event = _delivery("tx_it_immutable")
    store.record(record, event)
    for statement in (
        "UPDATE app.authorization_outcomes SET authorization_outcome = 'APPROVED' "
        "WHERE transaction_id = 'tx_it_immutable'",
        "DELETE FROM app.authorization_outcomes WHERE transaction_id = 'tx_it_immutable'",
    ):
        with pool.connection() as conn, pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute(statement)
    assert store.recorded("tx_it_immutable") == record


# --- through the gateway route ---------------------------------------------------


def _client(authorizations: Any, *, feature_store: Any = None, completeness: Any = None) -> Any:
    from fastapi.testclient import TestClient
    from services.gateway.app import GatewayState, create_app
    from services.gateway.config import GatewaySettings
    from services.gateway.pipeline import ScoringPipeline

    from trace_core.features.definitions import ONLINE_FEATURES
    from trace_core.observability.metrics import HotPathMetrics
    from trace_core.rules.loader import default_loader
    from trace_core.scoring.banding import load_thresholds
    from trace_core.security.service_tokens import ServiceTokenVerifier

    loader = default_loader(frozenset(ONLINE_FEATURES.ids))
    state = GatewayState(
        settings=GatewaySettings.from_environment({}),
        verifier=ServiceTokenVerifier({"psp-one": SECRET}),
        loader=loader,
        pipeline=ScoringPipeline(
            pack=loader.load(),
            thresholds=load_thresholds(),
            feature_store=feature_store,
            completeness=completeness,
        ),
        metrics=HotPathMetrics(),
        authorizations=authorizations,
    )
    return TestClient(create_app(state))


def _body(transaction_id: str, outcome: str = "DECLINED") -> dict[str, Any]:
    occurred = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=5)
    return {
        "transaction_id": transaction_id,
        "account_id": "acct_000001",
        "authorization_outcome": outcome,
        "decided_at": (occurred + dt.timedelta(milliseconds=340))
        .isoformat()
        .replace("+00:00", "Z"),
        "transaction_occurred_at": occurred.isoformat().replace("+00:00", "Z"),
    }


def test_the_route_records_deduplicates_and_refuses_a_conflict(pool: Any) -> None:
    headers = {"Authorization": f"Bearer psp-one.{SECRET}"}
    transaction_id = f"tx_{uuid.uuid4().hex[:16]}"
    body = _body(transaction_id)
    with _client(PostgresAuthorizationStore(pool)) as client:
        first = client.post("/v1/events/authorization", json=body, headers=headers)
        assert first.status_code == 202, first.text
        again = client.post("/v1/events/authorization", json=body, headers=headers)
        assert again.status_code == 202, again.text
        assert again.json()["event_id"] == first.json()["event_id"]
        conflicting = {**body, "authorization_outcome": "APPROVED"}
        refused = client.post("/v1/events/authorization", json=conflicting, headers=headers)
        assert refused.status_code == 409, refused.text
        assert refused.json()["type"].endswith("authorization-conflict")
    recorded = PostgresAuthorizationStore(pool).recorded(transaction_id)
    assert recorded is not None and recorded.authorization_outcome == "DECLINED"
    assert _outbox_rows(pool, transaction_id) == 1


def test_the_route_acknowledges_nothing_when_the_system_of_record_is_unreachable() -> None:
    """F15, with a real pool that cannot reach a database."""
    psycopg_pool = pytest.importorskip("psycopg_pool", reason="the `db` extra provides it")
    unreachable = psycopg_pool.ConnectionPool(
        _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD", port="1"), open=False, timeout=1.0
    )
    headers = {"Authorization": f"Bearer psp-one.{SECRET}"}
    with _client(PostgresAuthorizationStore(unreachable)) as client:
        response = client.post(
            "/v1/events/authorization", json=_body(f"tx_{uuid.uuid4().hex[:16]}"), headers=headers
        )
    assert response.status_code == 503, response.text


# --- applied online (ADR-0049 §4, §6) ----------------------------------------------


@pytest.fixture
def redis_store() -> Iterator[tuple[Any, Any]]:
    redis = pytest.importorskip("redis", reason="the `db` extra provides the Redis client")
    from trace_core.repositories.redis_features import RedisOnlineFeatureStore

    client = redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", "6389")),
        db=REDIS_DB,
        decode_responses=True,
    )
    try:
        client.ping()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no Redis reachable ({exc}). Run `make up`; the online store is "
            f"never faked (docs/TESTING.md §4)."
        )
    client.flushdb()
    yield client, RedisOnlineFeatureStore(client)
    client.flushdb()


def _occurred() -> dt.datetime:
    return (dt.datetime.now(dt.UTC) - dt.timedelta(seconds=30)).replace(microsecond=0)


def _hold(store: Any, transaction_id: str, account: str, occurred: dt.datetime) -> None:
    """Record the transaction as the gateway's scoring path does."""
    store.observe(
        transaction_observation(
            transaction(
                occurred_at=event_time(occurred), transaction_id=transaction_id, account_id=account
            )
        )
    )


def _outcome_body(
    transaction_id: str,
    occurred: dt.datetime,
    *,
    outcome: str = "DECLINED",
    account: str = "acct_000001",
) -> dict[str, Any]:
    return {
        "transaction_id": transaction_id,
        "account_id": account,
        "authorization_outcome": outcome,
        "decided_at": (occurred + dt.timedelta(milliseconds=340))
        .isoformat()
        .replace("+00:00", "Z"),
        "transaction_occurred_at": occurred.isoformat().replace("+00:00", "Z"),
    }


def _outcomes(store: Any, account: str, occurred: dt.datetime) -> WindowState | None:
    """The account's verified outcomes as a score ten seconds after the transaction reads them."""
    context = store.snapshot(
        as_of=event_time(occurred + dt.timedelta(seconds=10)), account_id=account, currency="GBP"
    )
    window: WindowState | None = context.windows.get(
        (Entity.ACCOUNT, account, Stream.AUTHORIZATION_OUTCOME, ONE_HOUR.label)
    )
    return window


def test_an_outcome_for_a_held_transaction_of_the_same_account_counts(
    pool: Any, redis_store: tuple[Any, Any]
) -> None:
    _, store = redis_store
    transaction_id, occurred = f"tx_{uuid.uuid4().hex[:16]}", _occurred()
    _hold(store, transaction_id, "acct_000001", occurred)
    with _client(PostgresAuthorizationStore(pool), feature_store=store) as client:
        response = client.post(
            "/v1/events/authorization",
            json=_outcome_body(transaction_id, occurred),
            headers=HEADERS,
        )
    assert response.status_code == 202, response.text
    state = _outcomes(store, "acct_000001", occurred)
    assert state is not None
    assert (state.outcome_known_count, state.declined_count) == (1, 1)


def test_an_outcome_naming_another_account_is_refused_recorded_and_never_counted(
    pool: Any, redis_store: tuple[Any, Any]
) -> None:
    """F9."""
    _, store = redis_store
    transaction_id, occurred = f"tx_{uuid.uuid4().hex[:16]}", _occurred()
    _hold(store, transaction_id, "acct_000001", occurred)
    body = _outcome_body(transaction_id, occurred, account="acct_000002")
    with _client(PostgresAuthorizationStore(pool), feature_store=store) as client:
        response = client.post("/v1/events/authorization", json=body, headers=HEADERS)
    assert response.status_code == 409, response.text
    assert response.json()["type"].endswith("authorization-account-mismatch")
    recorded = PostgresAuthorizationStore(pool).recorded(transaction_id)
    assert recorded is not None and recorded.account_id == "acct_000002"
    assert _outcomes(store, "acct_000002", occurred) is None
    assert _outcomes(store, "acct_000001", occurred) is None


def test_a_pending_outcome_its_duplicate_and_a_conflict_count_nothing_until_the_transaction(
    pool: Any, redis_store: tuple[Any, Any]
) -> None:
    """F10 and F13 through the route, then F11 when the transaction is recorded."""
    _, store = redis_store
    transaction_id, occurred = f"tx_{uuid.uuid4().hex[:16]}", _occurred()
    body = _outcome_body(transaction_id, occurred)
    with _client(PostgresAuthorizationStore(pool), feature_store=store) as client:
        first = client.post("/v1/events/authorization", json=body, headers=HEADERS)
        assert first.status_code == 202, first.text
        again = client.post("/v1/events/authorization", json=body, headers=HEADERS)
        assert again.status_code == 202, again.text
        assert again.json()["event_id"] == first.json()["event_id"]
        conflicting = {**body, "authorization_outcome": "APPROVED"}
        refused = client.post("/v1/events/authorization", json=conflicting, headers=HEADERS)
        assert refused.status_code == 409, refused.text
        assert refused.json()["type"].endswith("authorization-conflict")
    assert _outcomes(store, "acct_000001", occurred) is None
    _hold(store, transaction_id, "acct_000001", occurred)
    state = _outcomes(store, "acct_000001", occurred)
    assert state is not None
    assert (state.outcome_known_count, state.declined_count) == (1, 1)


def test_the_system_of_record_decides_after_the_online_store_is_flushed(
    pool: Any, redis_store: tuple[Any, Any]
) -> None:
    """F14."""
    redis_client, store = redis_store
    transaction_id, occurred = f"tx_{uuid.uuid4().hex[:16]}", _occurred()
    _hold(store, transaction_id, "acct_000001", occurred)
    body = _outcome_body(transaction_id, occurred)
    with _client(PostgresAuthorizationStore(pool), feature_store=store) as client:
        first = client.post("/v1/events/authorization", json=body, headers=HEADERS)
        assert first.status_code == 202, first.text
        redis_client.flushdb()
        again = client.post("/v1/events/authorization", json=body, headers=HEADERS)
        assert again.status_code == 202, again.text
        assert again.json()["event_id"] == first.json()["event_id"]
        conflicting = {**body, "authorization_outcome": "APPROVED"}
        refused = client.post("/v1/events/authorization", json=conflicting, headers=HEADERS)
        assert refused.status_code == 409, refused.text
        assert refused.json()["type"].endswith("authorization-conflict")
    assert _outbox_rows(pool, transaction_id) == 1


def test_an_outcome_the_online_store_cannot_apply_is_accepted_and_withdraws_completeness(
    pool: Any,
) -> None:
    redis = pytest.importorskip("redis", reason="the `db` extra provides the Redis client")
    from trace_core.features.completeness import CompletenessGuard
    from trace_core.repositories.redis_features import RedisOnlineFeatureStore

    unreachable = RedisOnlineFeatureStore(
        redis.Redis(host="localhost", port=1, socket_connect_timeout=0.5, socket_timeout=0.5)
    )
    guard = CompletenessGuard(unreachable, None, instance_id="authorization-integration")
    transaction_id, occurred = f"tx_{uuid.uuid4().hex[:16]}", _occurred()
    with _client(
        PostgresAuthorizationStore(pool), feature_store=unreachable, completeness=guard
    ) as client:
        response = client.post(
            "/v1/events/authorization",
            json=_outcome_body(transaction_id, occurred),
            headers=HEADERS,
        )
    assert response.status_code == 202, response.text
    assert guard.pending, "an outcome the online store never saw must withdraw its completeness"
    assert PostgresAuthorizationStore(pool).recorded(transaction_id) is not None
