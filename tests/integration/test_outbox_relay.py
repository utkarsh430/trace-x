"""The outbox relay against a real PostgreSQL, with a fake producer (ADR-0051 §7; migration 0007).

The claim, the marking and the refusal are database behaviour, so they run against the database.
The producer is a fake that honours the callbacks the factory binds, so the verdicts are the
relay's.
The same drain against a real broker is tests/integration/test_outbox_relay_kafka.py.
"""

from __future__ import annotations

import json
import os
import threading
from collections.abc import Iterator
from typing import Any

import pytest
from tests.unit.test_observation_log_sequencing import FakeMetadata, FakeProducer, FakeTopic

from trace_core.contracts import authorization
from trace_core.contracts.publish import DeliveryLedger, EventPublisher, producer_config
from trace_core.contracts.topics import INVESTIGATION_REQUESTED_V1, TX_AUTHORIZATION_V1
from trace_core.domain.errors import ContractError
from trace_core.observation.outbox_relay import REFUSED_PREFIX, OutboxRelay, RowOutcome
from trace_core.repositories.postgres_authorization import outbox_row

pytestmark = pytest.mark.integration

OUTBOX_TOPICS = (TX_AUTHORIZATION_V1, INVESTIGATION_REQUESTED_V1)


def _dsn(user_env: str, password_env: str) -> str:
    return "postgresql://{u}:{p}@{h}:{port}/{db}".format(
        u=os.environ.get(user_env, ""),
        p=os.environ.get(password_env, ""),
        h=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5442"),
        db=os.environ.get("POSTGRES_DB", "tracex"),
    )


def _truncate() -> None:
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(
        _dsn("POSTGRES_SUPERUSER", "POSTGRES_SUPERUSER_PASSWORD"), autocommit=True
    ) as owner:
        owner.execute("TRUNCATE app.outbox")


@pytest.fixture
def pool() -> Iterator[Any]:
    psycopg_pool = pytest.importorskip("psycopg_pool", reason="the `db` extra provides it")
    psycopg = pytest.importorskip("psycopg")
    dsn = _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD")
    try:
        with psycopg.connect(dsn) as probe:
            guarded = probe.execute(
                "SELECT 1 FROM pg_trigger WHERE tgname = 'outbox_guard' AND NOT tgisinternal"
            ).fetchone()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no PostgreSQL reachable as trace_app ({exc}). Run `make up "
            f"&& make migrate`; the outbox is a database guarantee and is never mocked."
        )
    if guarded is None:  # pragma: no cover - environment dependent
        pytest.skip("SKIPPED (NOT PASSED): migration 0007 is not applied. Run `make migrate`.")
    _truncate()
    created = psycopg_pool.ConnectionPool(dsn, min_size=1, max_size=4, open=True)
    try:
        yield created
    finally:
        created.close()
        _truncate()


class OutboxProducer(FakeProducer):
    """The observation-log fake, answering metadata for the outbox's topics."""

    def __init__(self, config: dict[str, Any], *, outcome: str = "deliver") -> None:
        super().__init__(config, outcome=outcome)
        self.topics: set[str] = set(OUTBOX_TOPICS)

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> FakeMetadata:
        return FakeMetadata({name: FakeTopic() for name in self.topics})


def _relay(pool: Any, **fake: Any) -> tuple[OutboxRelay, OutboxProducer, list[Any]]:
    ledger = DeliveryLedger()
    config = producer_config(bootstrap_servers="127.0.0.1:9", client_id="relay-it", ledger=ledger)
    producer = OutboxProducer(config, **fake)
    publisher = EventPublisher(producer, ledger, bootstrap_servers="127.0.0.1:9")
    tallies: list[Any] = []
    relay = OutboxRelay(
        pool=pool,
        publisher=publisher,
        batch_size=100,
        flush_timeout_s=1.0,
        record=lambda topic, outcome, count: tallies.append((topic, outcome, count)),
    )
    return relay, producer, tallies


def _event(i: int, account: str = "acct_000001") -> dict[str, Any]:
    occurred = 1_757_000_000_000 + i * 1_000
    return authorization.build_event(
        transaction_id=f"tx_{i:012d}",
        account_id=account,
        authorization_outcome="DECLINED",
        decided_ms=occurred + 340,
        transaction_occurred_ms=occurred,
        transaction_occurred_at=authorization.iso_millis(occurred),
        producer="trace-gateway@0.1.0",
        trace_id="0" * 32,
        correlation_id=f"tx_{i:012d}",
        ingested_ms=occurred + 400,
    )


def _insert(pool: Any, event: dict[str, Any], *, key: str | None = None) -> int:
    """A row as the store writes it, or with `key` stored in place of the one the event names.

    The store refuses to write an unkeyed event at all, so an unkeyed row -- the poison a relay must
    survive -- is written here by hand.
    """
    try:
        topic, stored_key, idempotency, payload = outbox_row(event)
    except ContractError:
        topic, stored_key = TX_AUTHORIZATION_V1, ""
        idempotency = str(event["envelope"]["idempotency_key"])
        payload = json.dumps(event, sort_keys=True, separators=(",", ":"))
    with pool.connection() as conn:
        row = conn.execute(
            "INSERT INTO app.outbox (topic, partition_key, idempotency_key, payload) "
            "VALUES (%s, %s, %s, %s) RETURNING outbox_id",
            (topic, key or stored_key, idempotency, payload),
        ).fetchone()
    assert row is not None
    return int(row[0])


def _rows(pool: Any) -> dict[int, tuple[Any, int, str | None]]:
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT outbox_id, published_at, attempts, last_error FROM app.outbox"
        ).fetchall()
    return {int(r[0]): (r[1], int(r[2]), r[3]) for r in rows}


def test_confirmed_rows_are_published_and_marked_exactly_once(pool: Any) -> None:
    ids = [_insert(pool, _event(i)) for i in range(3)]
    relay, producer, tallies = _relay(pool)
    result = relay.run_once()
    assert (result.claimed, result.published, result.failed, result.refused) == (3, 3, 0, 0)
    assert all(
        published is not None and attempts == 0 for published, attempts, _ in _rows(pool).values()
    )
    assert [call["key"] for call in producer.produced] == [b"acct_000001"] * 3
    assert relay.run_once().claimed == 0, "a published row is never claimed again"
    assert tallies == [(TX_AUTHORIZATION_V1, RowOutcome.PUBLISHED, 3)]
    assert set(_rows(pool)) == set(ids)


def test_the_published_bytes_are_the_event_the_transaction_committed(pool: Any) -> None:
    event = _event(7)
    _insert(pool, event)
    relay, producer, _ = _relay(pool)
    relay.run_once()
    assert json.loads(producer.produced[0]["value"]) == event


def test_an_unconfirmed_flush_marks_nothing_and_counts_an_attempt(pool: Any) -> None:
    for i in range(3):
        _insert(pool, _event(i))
    failing, _, _ = _relay(pool, outcome="fail")
    result = failing.run_once()
    assert (result.published, result.failed, result.refused) == (0, 3, 0)
    for published, attempts, error in _rows(pool).values():
        assert published is None and attempts == 1
        assert error is not None and not error.startswith(REFUSED_PREFIX), (
            "a delivery failure retries"
        )
    healthy, producer, _ = _relay(pool)
    assert healthy.run_once().published == 3, "at least once: the batch is redelivered"
    assert len(producer.produced) == 3


def test_a_row_that_can_never_be_published_is_refused_once_and_never_blocks_the_queue(
    pool: Any,
) -> None:
    unkeyed = _event(1)
    del unkeyed["payload"]["account_id"]
    poison = _insert(pool, unkeyed, key="acct_000001")
    mismatched = _insert(pool, _event(2), key="acct_999999")
    good = [_insert(pool, _event(i)) for i in (3, 4)]
    relay, producer, _ = _relay(pool)
    result = relay.run_once()
    assert (result.claimed, result.published, result.refused) == (4, 2, 2)
    rows = _rows(pool)
    for refused in (poison, mismatched):
        published, attempts, error = rows[refused]
        assert published is None and attempts == 1
        assert error is not None and error.startswith(REFUSED_PREFIX)
    assert all(rows[i][0] is not None for i in good)
    assert relay.run_once().claimed == 0, "a refused row is never claimed again"
    assert len(producer.produced) == 2


def test_a_missing_topic_is_retried_not_refused(pool: Any) -> None:
    outbox_id = _insert(pool, _event(1))
    relay, producer, _ = _relay(pool)
    producer.topics.discard(TX_AUTHORIZATION_V1)
    assert relay.run_once().failed == 1
    published, attempts, error = _rows(pool)[outbox_id]
    assert published is None and attempts == 1
    assert error is not None and not error.startswith(REFUSED_PREFIX)
    retry, _, _ = _relay(pool)
    assert retry.run_once().published == 1


def test_two_relays_in_concurrent_passes_never_publish_the_same_row(pool: Any) -> None:
    for i in range(6):
        _insert(pool, _event(i))
    first, first_producer, _ = _relay(pool)
    second, second_producer, _ = _relay(pool)
    for relay in (first, second):
        relay._batch_size = 3
    barrier = threading.Barrier(2)

    def run(relay: OutboxRelay) -> None:
        barrier.wait()
        relay.run_once()

    threads = [threading.Thread(target=run, args=(relay,)) for relay in (first, second)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    while first.run_once().claimed:
        pass

    def transactions(producer: OutboxProducer) -> list[str]:
        return [
            json.loads(call["value"])["payload"]["transaction_id"] for call in producer.produced
        ]

    mine, theirs = transactions(first_producer), transactions(second_producer)
    assert not set(mine) & set(theirs)
    assert sorted(mine + theirs) == sorted(f"tx_{i:012d}" for i in range(6))
    assert all(published is not None for published, _, _ in _rows(pool).values())
