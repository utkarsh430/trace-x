"""The observation log against a real broker (ADR-0051 §3-4; plan §4.1).

One throwaway broker for this module, started exactly as tests/integration/test_kafka_platform.py
starts it, with the two covered topics created from their declarations. What a unit test cannot
show:
- librdkafka delivers what the log handed over: keyed, stamped with LogAppendTime, and carrying both
  session headers. A consumer reads every sequence number from 1 to `last_seq`, across both topics,
  with none missing;
- a session closes only on a flush the broker confirmed. With the broker paused, the same log
  refuses to let its session close.
"""

from __future__ import annotations

import datetime as dt
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
from tests.unit.test_observation_log import _scored, _writer

from trace_core.contracts.envelope import build_event
from trace_core.contracts.events.identity_events_v1 import IdentityEventV1
from trace_core.contracts.events.tx_scored_v1 import TxScoredV1
from trace_core.contracts.publish import EventPublisher
from trace_core.contracts.topics import IDENTITY_EVENTS_V1, TX_SCORED_V1
from trace_core.domain.time import event_time
from trace_core.observation.log import (
    DELIVERY_TIMEOUT_MS,
    SEQ_HEADER,
    SESSION_HEADER,
    LogOutcome,
    ObservationLog,
)

pytestmark = pytest.mark.integration

TOPICS = (TX_SCORED_V1, IDENTITY_EVENTS_V1)
LOG_APPEND_TIME = 2
"""confluent_kafka.TIMESTAMP_LOG_APPEND_TIME: every declared topic stamps at the broker."""


@pytest.fixture(scope="module")
def topics(broker: Broker) -> Broker:  # noqa: F811
    for name in TOPICS:
        declared = DECLARATION.topics[name]
        _create(broker, name, declared.partitions, dict(declared.applied_config))
    return broker


def _identity_event(i: int) -> dict[str, Any]:
    return build_event(
        event_type="identity.events",
        occurred_at=event_time(
            dt.datetime(2026, 3, 1, 12, 0, tzinfo=dt.UTC) + dt.timedelta(seconds=i)
        ),
        payload={"account_id": "acct_000001", "identity_event_type": "PASSWORD_CHANGE"},
        producer="trace-gateway@0.1.0",
        trace_id="0" * 32,
        correlation_id=f"idev_{i:032d}",
    )


def _observation_log(
    target: Broker, *, client_id: str, message_timeout_ms: int
) -> tuple[Any, ObservationLog]:
    writer = _writer()
    publisher = EventPublisher.connect(
        target.bootstrap, client_id=client_id, message_timeout_ms=message_timeout_ms
    )
    observation_log = ObservationLog(
        writer=writer, publisher=publisher, topics=TOPICS, retry_s=0.5, check_timeout_s=10.0
    )
    observation_log.start()
    assert observation_log.status == "ok", observation_log.status
    return writer, observation_log


def test_every_handed_over_number_arrives_keyed_stamped_and_contiguous(topics: Broker) -> None:
    starts = {name: _watermarks(topics, name) for name in TOPICS}
    writer, observation_log = _observation_log(
        topics, client_id="it-observation-log", message_timeout_ms=DELIVERY_TIMEOUT_MS
    )
    count = 25
    for i in range(1, count + 1):
        sequenced = observation_log.sequence()
        topic, event = (
            (IDENTITY_EVENTS_V1, _identity_event(i)) if i % 5 == 0 else (TX_SCORED_V1, _scored(i))
        )
        assert observation_log.publish(sequenced, topic, event) is LogOutcome.HANDED_OVER
    assert observation_log.close(timeout_s=30.0), (
        "every delivery confirmed, every number handed over"
    )

    session = writer.session
    assert session is not None and session.session_id is not None
    records = [record for name in TOPICS for record in _consume(topics, name, starts[name])]
    assert len(records) == count
    assert sorted(int(dict(r.headers)[SEQ_HEADER]) for r in records) == list(range(1, count + 1))
    assert {dict(r.headers)[SESSION_HEADER] for r in records} == {session.session_id.encode()}
    assert session.last_seq == count
    for record in records:
        assert record.timestamp_type == LOG_APPEND_TIME
        assert record.key == b"acct_000001"
        model = TxScoredV1 if record.topic == TX_SCORED_V1 else IdentityEventV1
        model.model_validate_json(record.value)
        assert b"tracex" not in record.value, "session identity never enters the payload"
    assert sum(1 for r in records if r.topic == IDENTITY_EVENTS_V1) == count // 5


def test_a_session_does_not_close_on_a_flush_the_broker_did_not_confirm(topics: Broker) -> None:
    _, observation_log = _observation_log(
        topics, client_id="it-observation-log-paused", message_timeout_ms=3_000
    )
    paused = _docker("pause", topics.name)
    assert paused.returncode == 0, paused.stderr
    try:
        sequenced = observation_log.sequence()
        assert (
            observation_log.publish(sequenced, TX_SCORED_V1, _scored(1)) is LogOutcome.HANDED_OVER
        )
        assert not observation_log.close(timeout_s=10.0), "an unconfirmed delivery closes nothing"
    finally:
        _docker("unpause", topics.name)
