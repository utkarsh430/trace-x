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
"""

from __future__ import annotations

import datetime as dt
import os
import uuid
from collections.abc import Iterator
from typing import Any

import pytest

from trace_core.contracts import authorization
from trace_core.contracts.topics import TX_AUTHORIZATION_V1
from trace_core.domain.time import from_millis
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


def _client(authorizations: Any) -> Any:
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
        pipeline=ScoringPipeline(pack=loader.load(), thresholds=load_thresholds()),
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
