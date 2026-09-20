"""One undeliverable row blocks its own key, and nothing else.

The relay claims oldest-first and used to abandon the pass at the first transient failure, so a
single undeliverable row starved every later row, across topics, for as long as it kept failing --
and it was retried forever without a log line. Kafka only orders within a partition key, so that is
the only ordering a relay owes: a failed row blocks its own lane, and every other lane drains.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from trace_core.contracts.publish import DeliveryReport
from trace_core.observation import outbox_relay
from trace_core.observation.outbox_relay import OutboxRelay

pytestmark = pytest.mark.unit

TOPIC = "tx.authorization.v1"


class FakeConn:
    """The claim query, and the marks the relay writes back."""

    def __init__(self, rows: list[tuple[int, str, str, Any]]) -> None:
        self.rows = rows
        self.published: list[int] = []
        self.marked: list[tuple[list[int], str]] = []

    def execute(self, sql: str, params: Any = None) -> Any:
        if "SELECT outbox_id" in sql:
            return self
        if "published_at = now()" in sql:
            self.published.extend(params[0])
        elif "attempts = attempts + 1" in sql:
            self.marked.append((list(params[1]), str(params[0])))
        return self

    def fetchall(self) -> list[tuple[int, str, str, Any]]:
        return self.rows

    def fetchone(self) -> None:
        return None

    def __enter__(self) -> FakeConn:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def transaction(self) -> FakeConn:
        return self


class FakePool:
    def __init__(self, conn: FakeConn) -> None:
        self._conn = conn

    def connection(self) -> FakeConn:
        return self._conn


class FakePublisher:
    """Accepts every publish except for one key, which fails as a missing topic does."""

    def __init__(self, undeliverable_key: str) -> None:
        self.undeliverable_key = undeliverable_key
        self.published: list[str] = []

    def report(self) -> DeliveryReport:
        counts = {TOPIC: len(self.published)}
        return DeliveryReport(
            accepted=counts,
            delivered=counts,
            failed={},
            shed={},
            refused={},
            outstanding=0,
            fatal=None,
            error_samples=(),
        )

    def publish(self, topic: str, payload: bytes, *, block: bool = False) -> bool:
        key = json.loads(payload)["payload"]["account_id"]
        if key == self.undeliverable_key:
            raise RuntimeError("UNKNOWN_TOPIC_OR_PART")
        self.published.append(key)
        return True

    def flush(self, timeout: float) -> DeliveryReport:
        return self.report()


def _row(outbox_id: int, account: str) -> tuple[int, str, str, Any]:
    return (outbox_id, TOPIC, account, {"payload": {"account_id": account}})


def _relay(rows: list[tuple[int, str, str, Any]], undeliverable: str) -> tuple[Any, FakeConn, Any]:
    conn = FakeConn(rows)
    publisher = FakePublisher(undeliverable)
    relay = OutboxRelay(pool=FakePool(conn), publisher=publisher)  # type: ignore[arg-type]
    return relay, conn, publisher


@pytest.fixture(autouse=True)
def _identity_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    """The key each event names, without building released events: lanes, not contracts."""
    monkeypatch.setattr(
        outbox_relay, "partition_key", lambda topic, payload: payload["payload"]["account_id"]
    )
    monkeypatch.setattr(
        outbox_relay, "canonical_bytes", lambda payload: json.dumps(payload).encode()
    )
    monkeypatch.setattr(outbox_relay.outbox_watermark, "advance", lambda conn: None)


def test_a_blocked_lane_never_starves_another_lane() -> None:
    rows = [_row(1, "acct_stuck"), _row(2, "acct_other"), _row(3, "acct_third")]
    relay, conn, publisher = _relay(rows, undeliverable="acct_stuck")
    result = relay.run_once()
    assert publisher.published == ["acct_other", "acct_third"], "other lanes drained"
    assert (result.claimed, result.published, result.failed, result.deferred) == (3, 2, 1, 0)
    assert conn.published == [2, 3]
    assert conn.marked and conn.marked[0][0] == [1]


def test_a_later_row_on_the_blocked_lane_waits_rather_than_overtaking() -> None:
    rows = [_row(1, "acct_stuck"), _row(2, "acct_other"), _row(3, "acct_stuck")]
    relay, conn, publisher = _relay(rows, undeliverable="acct_stuck")
    result = relay.run_once()
    assert publisher.published == ["acct_other"], "the blocked lane's later row never overtakes"
    assert (result.claimed, result.published, result.failed, result.deferred) == (3, 1, 1, 1)
    assert 3 not in conn.published
    assert all(3 not in ids for ids, _ in conn.marked), "a deferred row is not marked failed"


def test_every_claimed_row_is_exactly_one_outcome() -> None:
    rows = [_row(1, "acct_stuck"), _row(2, "acct_other"), _row(3, "acct_stuck"), _row(4, "acct_x")]
    relay, _, _ = _relay(rows, undeliverable="acct_stuck")
    result = relay.run_once()
    assert result.claimed == result.published + result.failed + result.refused + result.deferred
