"""The outbox relay end to end: PostgreSQL to a real broker (ADR-0051 §7; ADR-0049 §4).

One throwaway broker, started as tests/integration/test_kafka_platform.py starts it, with
`tx.authorization.v1` created from its declaration, and the local migrated PostgreSQL. What the
fake-producer tests cannot show:
- librdkafka delivers the committed event, keyed and stamped with LogAppendTime, and the relay marks
  it published only after that delivery;
- a paused broker fails the batch without marking it, and a later pass delivers it: at least once,
  and never lost.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from tests.integration.test_kafka_platform import (  # noqa: F401 -- `broker` is a fixture
    DECLARATION,
    Broker,
    _consume,
    _create,
    _docker,
    _watermarks,
    broker,
)
from tests.integration.test_outbox_relay import (  # noqa: F401 -- `pool` is a fixture
    _event,
    _insert,
    _rows,
    pool,
)

from trace_core.contracts.publish import EventPublisher
from trace_core.contracts.topics import TX_AUTHORIZATION_V1
from trace_core.observation.outbox_relay import MESSAGE_TIMEOUT_MS, OutboxRelay

pytestmark = pytest.mark.integration

LOG_APPEND_TIME = 2


@pytest.fixture(scope="module")
def outbox_topic(broker: Broker) -> Broker:  # noqa: F811
    declared = DECLARATION.topics[TX_AUTHORIZATION_V1]
    _create(broker, TX_AUTHORIZATION_V1, declared.partitions, dict(declared.applied_config))
    return broker


def _relay(target: Any, database: Any, *, client_id: str, message_timeout_ms: int) -> OutboxRelay:
    publisher = EventPublisher.connect(
        target.bootstrap, client_id=client_id, message_timeout_ms=message_timeout_ms
    )
    return OutboxRelay(pool=database, publisher=publisher, flush_timeout_s=15.0)


def test_the_relay_delivers_the_committed_event_and_marks_it_only_then(
    outbox_topic: Broker,
    pool: Any,  # noqa: F811
) -> None:
    events = [_event(i, account=f"acct_{100_000 + i:06d}") for i in range(5)]
    ids = [_insert(pool, event) for event in events]
    start = _watermarks(outbox_topic, TX_AUTHORIZATION_V1)
    relay = _relay(
        outbox_topic, pool, client_id="it-outbox-relay", message_timeout_ms=MESSAGE_TIMEOUT_MS
    )
    result = relay.run_once()
    assert (result.claimed, result.published, result.failed, result.refused) == (5, 5, 0, 0)
    rows = _rows(pool)
    assert all(rows[i][0] is not None for i in ids)
    records = _consume(outbox_topic, TX_AUTHORIZATION_V1, start)
    assert sorted(json.loads(r.value)["payload"]["transaction_id"] for r in records) == sorted(
        event["payload"]["transaction_id"] for event in events
    )
    by_transaction = {event["payload"]["transaction_id"]: event for event in events}
    for record in records:
        decoded = json.loads(record.value)
        assert decoded == by_transaction[decoded["payload"]["transaction_id"]]
        assert record.key == decoded["payload"]["account_id"].encode()
        assert record.timestamp_type == LOG_APPEND_TIME
    assert relay.run_once().claimed == 0


def test_a_paused_broker_fails_the_batch_unmarked_and_a_later_pass_delivers_it(
    outbox_topic: Broker,
    pool: Any,  # noqa: F811
) -> None:
    ids = [_insert(pool, _event(i)) for i in range(3)]
    paused_relay = _relay(
        outbox_topic, pool, client_id="it-outbox-relay-paused", message_timeout_ms=3_000
    )
    paused = _docker("pause", outbox_topic.name)
    assert paused.returncode == 0, paused.stderr
    try:
        result = paused_relay.run_once()
    finally:
        _docker("unpause", outbox_topic.name)
    assert (result.published, result.failed) == (0, 3)
    rows = _rows(pool)
    assert all(rows[i][0] is None and rows[i][1] == 1 for i in ids), "unmarked, one attempt each"
    start = _watermarks(outbox_topic, TX_AUTHORIZATION_V1)
    healthy = _relay(
        outbox_topic, pool, client_id="it-outbox-relay-after", message_timeout_ms=MESSAGE_TIMEOUT_MS
    )
    assert healthy.run_once().published == 3
    delivered = {
        json.loads(r.value)["payload"]["transaction_id"]
        for r in _consume(outbox_topic, TX_AUTHORIZATION_V1, start)
    }
    assert delivered == {f"tx_{i:012d}" for i in range(3)}
