"""Chaos: a confirmed delivery whose marking commit is lost (ADR-0051 §5; user decision 2026-09-15).

Produced, not simulated, against a throwaway real broker and the local PostgreSQL:
1. A relay pass claims three authorization rows, hands them over, and waits in its flush while the
   broker is paused.
2. PostgreSQL terminates the relay's backend. The broker is resumed, so the deliveries are
   confirmed, and the relay's marking commit fails.
3. The rows are in Kafka, yet unmarked. The watermark must not have moved, and feature history over
   those rows must be INCOMPLETE.
4. A fresh relay re-delivers them. The duplicates must be harmless, deduplicated by transaction id.
   Only now does the watermark advance, and history over the rows becomes COMPLETE.
"""

from __future__ import annotations

import datetime as dt
import json
import threading
import time
from collections import Counter
from types import MappingProxyType
from typing import Any

import pytest
from tests.integration.test_kafka_platform import Broker, _consume, _docker, _watermarks
from tests.integration.test_outbox_relay import (  # noqa: F401
    _event,
    _insert,
    _rows,
    _truncate,
    pool,
)
from tests.integration.test_outbox_relay_kafka import broker, outbox_topic  # noqa: F401 -- fixtures

from trace_core.contracts.publish import EventPublisher
from trace_core.contracts.topics import TX_AUTHORIZATION_V1
from trace_core.observation import outbox_watermark
from trace_core.observation.coverage import Coverage
from trace_core.observation.history_completeness import assess_history
from trace_core.observation.outbox_relay import OutboxRelay

pytestmark = [pytest.mark.chaos, pytest.mark.integration]


def _relay(target: Broker, database: Any, client_id: str) -> OutboxRelay:
    publisher = EventPublisher.connect(
        target.bootstrap, client_id=client_id, message_timeout_ms=30_000
    )
    return OutboxRelay(pool=database, publisher=publisher, flush_timeout_s=40.0)


def _verdict(database: Any, horizon_end: dt.datetime) -> Any:
    with database.connection() as conn:
        delivered = outbox_watermark.read(conn)
    return assess_history(
        horizon_start=horizon_end - dt.timedelta(hours=1),
        horizon_end=horizon_end,
        observation_coverage=Coverage(
            sessions=MappingProxyType({}), unknown=(), through=horizon_end + dt.timedelta(minutes=5)
        ),
        authorization_delivered_through=delivered,
        clock_margin_s=1.0,
    )


def _relay_backend(owner: Any) -> int | None:
    row = owner.execute(
        "SELECT pid FROM pg_stat_activity WHERE usename = %s AND state = 'idle in transaction' "
        "AND query ILIKE %s ORDER BY xact_start LIMIT 1",
        ("trace_app", "%app.outbox%"),
    ).fetchone()
    return None if row is None else int(row[0])


def test_a_lost_marking_commit_keeps_history_incomplete_until_the_duplicates_are_marked(
    outbox_topic: Broker,  # noqa: F811
    pool: Any,  # noqa: F811
) -> None:
    psycopg = pytest.importorskip("psycopg")
    from tests.integration.test_outbox_relay import _dsn

    owner = psycopg.connect(
        _dsn("POSTGRES_SUPERUSER", "POSTGRES_SUPERUSER_PASSWORD"), autocommit=True
    )
    owner.execute("TRUNCATE app.outbox_delivery_watermark")
    try:
        ids = [_insert(pool, _event(i, account=f"acct_{200_000 + i:06d}")) for i in range(3)]
        with pool.connection() as conn:
            newest = conn.execute(
                "SELECT max(created_at) FROM app.outbox WHERE outbox_id = ANY(%s)", (ids,)
            ).fetchone()[0]
        start = _watermarks(outbox_topic, TX_AUTHORIZATION_V1)
        failed: list[BaseException] = []
        relay = _relay(outbox_topic, pool, "chaos-watermark-first")

        def pass_that_loses_its_commit() -> None:
            try:
                relay.run_once()
            except BaseException as exc:  # the terminated backend surfaces here
                failed.append(exc)

        assert _docker("pause", outbox_topic.name).returncode == 0
        worker = threading.Thread(target=pass_that_loses_its_commit)
        try:
            worker.start()
            deadline = time.monotonic() + 30
            pid = None
            while pid is None and time.monotonic() < deadline:
                pid = _relay_backend(owner)
                time.sleep(0.2)
            assert pid is not None, "the relay's pass never reached its flush"
            owner.execute("SELECT pg_terminate_backend(%s)", (pid,))
        finally:
            _docker("unpause", outbox_topic.name)
        worker.join(timeout=90)
        assert not worker.is_alive()
        assert failed, "the marking commit should have failed on the terminated backend"

        rows = _rows(pool)
        assert all(rows[i][0] is None for i in ids), "delivered, but never marked"
        delivered_once = Counter(
            json.loads(r.value)["payload"]["transaction_id"]
            for r in _consume(outbox_topic, TX_AUTHORIZATION_V1, start)
        )
        assert set(delivered_once) == {f"tx_{i:012d}" for i in range(3)}, "the broker has them"
        verdict = _verdict(pool, newest)
        assert not verdict.complete, "records in Kafka are not marks; history stays incomplete"
        with pool.connection() as conn:
            held = outbox_watermark.read(conn)
            oldest = conn.execute(
                "SELECT min(created_at) FROM app.outbox WHERE outbox_id = ANY(%s)", (ids,)
            ).fetchone()[0]
        assert held is None or held <= oldest, "the watermark never passed an unmarked row"

        healthy = _relay(outbox_topic, pool, "chaos-watermark-second")
        assert healthy.run_once().published == 3
        everything = Counter(
            json.loads(r.value)["payload"]["transaction_id"]
            for r in _consume(outbox_topic, TX_AUTHORIZATION_V1, start)
        )
        assert set(everything) == set(delivered_once), "deduplicated, the same three outcomes"
        assert all(count >= 2 for count in everything.values()), "at least once: duplicates exist"
        with pool.connection() as conn:
            moved = outbox_watermark.read(conn)
        assert moved is not None and moved >= newest, "marked rows no longer hold it back"
        # COMPLETE needs the watermark past the horizon by the clock margin, and no pass moves it
        # past its own start: wait on the database clock, then an empty pass carries it on.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            with pool.connection() as conn:
                past = conn.execute(
                    "SELECT clock_timestamp() > %s + interval '1.2 seconds'", (newest,)
                ).fetchone()[0]
            if past:
                break
            time.sleep(0.1)
        assert healthy.run_once().published == 0
        assert _verdict(pool, newest).complete, "marked: history over the rows is complete"
    finally:
        owner.close()
