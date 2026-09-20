"""The outbox relay advancing the authorization delivery watermark, against PostgreSQL.

ADR-0051 §5.

A fake producer that honours the factory's callbacks, as in tests/integration/test_outbox_relay.py,
so the verdicts are the relay's. The same path against a real broker, with the marking commit lost
after a confirmed delivery, is tests/chaos/test_outbox_watermark.py.
"""

from __future__ import annotations

import datetime as dt
import json
import os
from collections.abc import Iterator
from types import MappingProxyType
from typing import Any

import pytest
from tests.integration.test_outbox_relay import (  # noqa: F401 -- `pool` is a fixture
    OutboxProducer,
    _event,
    _insert,
    _relay,
    _rows,
    pool,
)

from trace_core.contracts.publish import DeliveryLedger, EventPublisher, producer_config
from trace_core.observation import outbox_watermark
from trace_core.observation.coverage import Coverage
from trace_core.observation.history_completeness import assess_history
from trace_core.observation.outbox_relay import OutboxRelay

pytestmark = pytest.mark.integration


def _dsn(user_env: str, password_env: str) -> str:
    return "postgresql://{u}:{p}@{h}:{port}/{db}".format(
        u=os.environ.get(user_env, ""),
        p=os.environ.get(password_env, ""),
        h=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5442"),
        db=os.environ.get("POSTGRES_DB", "tracex"),
    )


@pytest.fixture
def database(pool: Any) -> Iterator[Any]:  # noqa: F811
    psycopg = pytest.importorskip("psycopg")
    owner = _dsn("POSTGRES_SUPERUSER", "POSTGRES_SUPERUSER_PASSWORD")
    with psycopg.connect(owner, autocommit=True) as conn:
        conn.execute("TRUNCATE app.outbox_delivery_watermark")
    yield pool
    with psycopg.connect(owner, autocommit=True) as conn:
        conn.execute("TRUNCATE app.outbox_delivery_watermark")


def _watermark(database: Any) -> dt.datetime | None:
    with database.connection() as conn:
        return outbox_watermark.read(conn)


def _created(database: Any, outbox_id: int) -> dt.datetime:
    with database.connection() as conn:
        return conn.execute(
            "SELECT created_at FROM app.outbox WHERE outbox_id = %s", (outbox_id,)
        ).fetchone()[0]


def _require_no_older_open_transaction(database: Any, than: dt.datetime) -> None:
    with database.connection() as conn:
        row = conn.execute(
            "SELECT min(xact_start) FROM pg_stat_activity WHERE usename = current_user "
            "AND datname = current_database() AND pid <> pg_backend_pid() "
            "AND xact_start IS NOT NULL"
        ).fetchone()
    if row is not None and row[0] is not None and row[0] <= than:
        pytest.skip(
            "SKIPPED (NOT PASSED): another trace_app transaction older than the rows is open, so "
            "forward progress cannot be asserted. Re-run with no other PostgreSQL clients."
        )


class FullAfter(OutboxProducer):
    """Accepts `accept` messages, then reports its queue full: a retryable failure mid-batch."""

    def __init__(self, config: dict[str, Any], *, accept: int) -> None:
        super().__init__(config)
        self._remaining = accept

    def produce(self, topic: str, **kwargs: Any) -> None:
        if self._remaining <= 0:
            raise BufferError("Local: Queue full")
        self._remaining -= 1
        super().produce(topic, **kwargs)


def test_the_watermark_advances_only_after_a_confirmed_pass(database: Any) -> None:
    ids = [_insert(database, _event(i)) for i in range(3)]
    oldest = _created(database, ids[0])
    failing, _, _ = _relay(database, outcome="fail")
    failing.run_once()
    held = _watermark(database)
    assert held is not None and held <= oldest, "an unconfirmed pass never moves it past a row"
    _require_no_older_open_transaction(database, _created(database, ids[-1]))
    healthy, _, _ = _relay(database)
    assert healthy.run_once().published == 3
    moved = _watermark(database)
    assert moved is not None and moved > _created(database, ids[-1])


def _shedding_relay(database: Any, *, accept: int) -> OutboxRelay:
    """A relay whose producer accepts `accept` messages and then reports its queue full."""
    ledger = DeliveryLedger()
    config = producer_config(bootstrap_servers="127.0.0.1:9", client_id="relay-wm", ledger=ledger)
    publisher = EventPublisher(
        FullAfter(config, accept=accept), ledger, bootstrap_servers="127.0.0.1:9"
    )
    return OutboxRelay(pool=database, publisher=publisher, flush_timeout_s=1.0)


def test_a_partial_batch_stops_the_watermark_at_the_first_unconfirmed_row(database: Any) -> None:
    first, second = _insert(database, _event(1)), _insert(database, _event(2))
    _shedding_relay(database, accept=1).run_once()
    rows = _rows(database)
    assert rows[first][0] is not None, "the row confirmed before the failure is marked"
    assert rows[second][0] is None, "the shed row is not"
    second_at = _created(database, second)
    watermark = _watermark(database)
    assert watermark is not None and watermark <= second_at, "never past the unconfirmed row"
    _require_no_older_open_transaction(database, second_at)
    assert watermark == second_at, "and exactly at it, once the confirmed row is marked"


def test_extra_records_in_kafka_never_move_the_watermark(database: Any) -> None:
    outbox_id = _insert(database, _event(1))
    failing, producer, _ = _relay(database, outcome="fail")
    for _ in range(3):
        failing.run_once()  # three handed-over deliveries of the same row, none confirmed
    assert len(producer.produced) == 3
    watermark = _watermark(database)
    assert watermark is not None and watermark <= _created(database, outbox_id)


def test_a_restarted_relay_continues_from_the_stored_watermark(database: Any) -> None:
    _insert(database, _event(1))
    first, _, _ = _relay(database)
    first.run_once()
    before = _watermark(database)
    assert before is not None
    restarted, _, _ = _relay(database)
    restarted.run_once()
    after = _watermark(database)
    assert after is not None and after >= before, "a restart never moves it back"


def test_a_refused_row_keeps_every_later_horizon_incomplete(database: Any) -> None:
    unkeyed = _event(1)
    del unkeyed["payload"]["account_id"]
    refused = _insert(database, unkeyed, key="acct_000001")
    later = _insert(database, _event(2))
    relay, _, _ = _relay(database)
    relay.run_once()
    relay.run_once()
    watermark = _watermark(database)
    refused_at = _created(database, refused)
    assert watermark is not None and watermark <= refused_at
    horizon_end = _created(database, later) + dt.timedelta(seconds=1)
    verdict = assess_history(
        horizon_start=refused_at - dt.timedelta(hours=1),
        horizon_end=horizon_end,
        observation_coverage=Coverage(
            sessions=MappingProxyType({}), unknown=(), through=horizon_end + dt.timedelta(minutes=5)
        ),
        authorization_delivered_through=watermark,
        clock_margin_s=1.0,
    )
    assert not verdict.complete
    assert any("authorization outcomes are delivered only through" in r for r in verdict.reasons)


def test_a_row_confirmed_before_a_mid_batch_failure_is_never_delivered_again(database: Any) -> None:
    """Critic finding B3: the rows handed over before a retryable failure were marked failed and
    re-produced on every pass. They are now confirmed and marked, so a later pass skips them."""
    first, second = _insert(database, _event(1)), _insert(database, _event(2))
    third = _insert(database, _event(3))
    shedding = _shedding_relay(database, accept=1)
    result = shedding.run_once()
    # Every claimed row is one outcome: 1 confirmed, 2 shed, 3 never attempted after it.
    assert (result.claimed, result.published, result.failed, result.deferred) == (3, 1, 1, 1)
    assert _rows(database)[third][1] == 0, "a deferred row carries no attempt"
    healthy, producer, _ = _relay(database)
    assert healthy.run_once().published == 2
    produced = [
        json.loads(call["value"])["payload"]["transaction_id"] for call in producer.produced
    ]
    assert produced == ["tx_000000000002", "tx_000000000003"], "only rows 2 and 3 are delivered"
    rows = _rows(database)
    assert all(rows[i][0] is not None for i in (first, second, third))
