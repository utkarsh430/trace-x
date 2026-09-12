"""What `case_id` is and is not, for `investigation.requested.v1`.

The topic is keyed by `case_id`, and the reason it is safe to do so rests on four
distinctions that are easy to lose later. A key is routing and ordering; it is
not identity, and treating it as identity is how a topic silently loses history.

1. **`case_id` is the partition key, not the event id.** It decides which
   partition a message lands on and therefore what is ordered with respect to
   what. It does not identify a message.
2. **Every event carries its own `event_id`**, unique per event, UUIDv7, and the
   deduplication key in both Redis and Spark.
3. **Retrying the same logical request is idempotent** -- one case, one queue
   entry, one outbox row -- and that is enforced by database constraints, not by
   the key.
4. **The topic must never be compacted.** Compaction keeps only the newest record
   per key. Since the key is `case_id`, compaction would erase every historical
   request for a case but the last -- and a case CAN be requested more than once:
   Phase 5 exposes `/v1/investigations/{id}/reopen`. The record of why an
   investigation was opened the first time is exactly the record an audit needs.

The integration suite proves (3) against a real database. This module pins the
contract-level halves, which is where they would otherwise be assumed.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from trace_core.contracts.api.decision import RiskDecision
from trace_core.contracts.topics import INVESTIGATION_REQUESTED_V1, partition_key
from trace_core.domain.enums import FeatureSource, RiskBand
from trace_core.domain.identifiers import uuid7_millis
from trace_core.domain.time import event_time
from trace_core.repositories.triage_event import (
    build_investigation_requested,
    outbox_row,
    producer_string,
)
from trace_core.rules.engine import Evaluation

pytestmark = pytest.mark.contract

ROOT = Path(__file__).resolve().parents[2]
LEDGER = ROOT / "docs" / "contracts" / "RELEASED.json"
OCCURRED = event_time(dt.datetime(2026, 3, 1, 12, 0, 0, tzinfo=dt.UTC))
CASE_ID = "case_" + "a" * 32


@pytest.fixture(scope="module")
def ledger() -> dict[str, Any]:
    return json.loads(LEDGER.read_text())


@pytest.fixture(scope="module")
def entry(ledger: dict[str, Any]) -> dict[str, Any]:
    return next(e for e in ledger["released"] if e["topic"] == INVESTIGATION_REQUESTED_V1)


def _decision(transaction_id: str) -> RiskDecision:
    return RiskDecision(
        transaction_id=transaction_id,
        decision="FLAG",
        risk_band=RiskBand.HIGH,
        score=0.8,
        reasons=[],
        feature_source=FeatureSource.ONLINE_ONLY,
        degraded=False,
        degraded_reasons=[],
        unavailable_features=[],
        insufficient_history_features=[],
        rule_pack_id="core",
        rule_pack_digest="sha256:" + "a" * 64,
        threshold_config_digest="sha256:" + "b" * 64,
        feature_set_version="1.0.0",
        case_id=None,
        scored_at=dt.datetime(2026, 3, 1, 12, tzinfo=dt.UTC),
        latency_ms=4.0,
    )


def _event(transaction_id: str = "tx_1", *, case_id: str = CASE_ID, occurred: Any = None):
    return build_investigation_requested(
        decision=_decision(transaction_id),
        evaluation=Evaluation(
            pack_id="core",
            pack_version="1.0.0",
            pack_digest="sha256:" + "a" * 64,
        ),
        case_id=case_id,
        account_id="acct_000001",
        occurred_at=occurred or OCCURRED,
        producer=producer_string("0.1.0"),
        trace_id="0" * 32,
        correlation_id=f"corr_{transaction_id}",
    )


# --- (1) the key routes and orders; it does not identify ----------------------


def test_the_partition_key_is_case_id(entry: dict[str, Any]) -> None:
    assert entry["key"] == "case_id"
    assert partition_key(INVESTIGATION_REQUESTED_V1, _event()) == CASE_ID


def test_the_key_is_not_the_event_id() -> None:
    """If they were the same field, every message would be its own partition and
    nothing would be ordered with respect to anything."""
    event = _event()
    assert event["envelope"]["event_id"] != partition_key(INVESTIGATION_REQUESTED_V1, event)


def test_events_for_one_case_share_a_key_and_are_therefore_ordered() -> None:
    """The property keying on `case_id` buys: requests about one case land on one
    partition, so a consumer sees them in the order they were produced."""
    first = _event("tx_1")
    later = _event("tx_1", occurred=event_time(dt.datetime(2026, 3, 1, 13, tzinfo=dt.UTC)))
    assert partition_key(INVESTIGATION_REQUESTED_V1, first) == partition_key(
        INVESTIGATION_REQUESTED_V1, later
    )


def test_events_for_different_cases_do_not_share_a_key() -> None:
    other = "case_" + "b" * 32
    assert partition_key(INVESTIGATION_REQUESTED_V1, _event(case_id=other)) == other


# --- (2) every event has its own stable identity ------------------------------


def test_every_event_carries_a_unique_uuid7_event_id() -> None:
    """Identity is `event_id`, and it is the deduplication key in both Redis and
    Spark -- which is why it must be unique even when the routing key is not."""
    ids = {
        _event("tx_1")["envelope"]["event_id"],
        _event("tx_1")["envelope"]["event_id"],
        _event("tx_2")["envelope"]["event_id"],
    }
    assert len(ids) == 3, "two events shared an event_id; dedup would drop a real message"
    for raw in ids:
        assert UUID(raw).version == 7


def test_the_event_id_is_time_ordered() -> None:
    """UUIDv7: the prefix is the event-time millisecond, which is what keeps the
    dedup key sortable and Delta index locality sane."""
    from trace_core.domain.time import to_millis

    event = _event()
    assert uuid7_millis(UUID(event["envelope"]["event_id"])) == to_millis(OCCURRED)


def test_two_requests_for_one_case_are_distinguishable() -> None:
    """A reopened case (Phase 5) produces a second request. It shares the routing
    key with the first and must still be a distinct, retained message."""
    later = event_time(dt.datetime(2026, 3, 1, 13, tzinfo=dt.UTC))
    first, second = _event("tx_1"), _event("tx_1", occurred=later)
    assert partition_key(INVESTIGATION_REQUESTED_V1, first) == partition_key(
        INVESTIGATION_REQUESTED_V1, second
    )
    assert first["envelope"]["event_id"] != second["envelope"]["event_id"]
    assert first["envelope"]["idempotency_key"] != second["envelope"]["idempotency_key"]


# --- (3) a retry of the SAME logical request is idempotent --------------------


def test_a_byte_identical_retry_produces_the_same_idempotency_key() -> None:
    """ "Equal keys mean equal meaning": a retry of the same request must be
    recognisable as one, which is what makes the outbox's UNIQUE constraint
    collapse it to a single durable effect."""
    assert (
        _event("tx_1")["envelope"]["idempotency_key"]
        == (_event("tx_1")["envelope"]["idempotency_key"])
    )
    assert outbox_row(_event("tx_1"))[2] == outbox_row(_event("tx_1"))[2]


def test_the_outbox_row_carries_the_key_and_the_identity_separately() -> None:
    topic, key, idempotency_key, payload = outbox_row(_event("tx_1"))
    assert topic == INVESTIGATION_REQUESTED_V1
    assert key == CASE_ID, "the partition key is stored, not re-derived by the relay"
    assert idempotency_key.startswith("sha256:")
    assert key != idempotency_key
    assert json.loads(payload)["envelope"]["event_id"] not in {key, idempotency_key}


# --- (4) the topic must never be compacted ------------------------------------


def test_the_topic_is_delete_retention_never_compacted(entry: dict[str, Any]) -> None:
    """The constraint that makes keying on `case_id` safe.

    Compaction keeps only the newest record per key. With `case_id` as the key,
    it would erase every request for a case but the last -- and a case can be
    requested more than once once Phase 5 exposes reopen. The record of why an
    investigation was first opened is exactly what an audit needs.
    """
    assert entry["cleanup"] == "delete", (
        "investigation.requested.v1 must not be compacted: the key is case_id, so "
        "compaction would silently erase historical requests for a reopened case"
    )
    reason = entry.get("cleanup_reason", "")
    assert "compact" in reason.lower(), (
        "the ledger records the policy but not why; a flag with no reason is one a "
        "future reader will change to save disk"
    )
    assert len(reason) > 80, "the reason must survive as more than a restatement"


def test_no_keyed_non_identity_topic_is_compacted(ledger: dict[str, Any]) -> None:
    """Held across every released topic, not just this one.

    Compaction is only ever safe where the key IS the identity of the thing and
    older records are genuinely superseded. Every topic released so far keys on
    an entity that recurs -- an account, a device, a case -- so for all of them
    compaction would delete history rather than deduplicate it.
    """
    for released in ledger["released"]:
        assert released["cleanup"] == "delete", (
            f"{released['topic']} is keyed by {released['key']!r}, which recurs. "
            f"Compaction would keep only the newest record per key and erase the rest."
        )


def test_the_ledger_and_the_document_agree_on_cleanup(ledger: dict[str, Any]) -> None:
    """Phase 3 writes `deploy/kafka/topics.yaml` from this; two sources that
    disagree would let the wrong policy reach a broker."""
    text = (ROOT / "docs" / "EVENT_CONTRACTS.md").read_text()
    for released in ledger["released"]:
        row = next(
            (line for line in text.splitlines() if line.startswith(f"| `{released['topic']}`")),
            None,
        )
        assert row is not None, f"{released['topic']} has no row in EVENT_CONTRACTS.md §3"
        assert f"| {released['cleanup']} |" in row, (
            f"{released['topic']}: ledger says cleanup={released['cleanup']}, "
            f"EVENT_CONTRACTS.md §3 says otherwise"
        )
