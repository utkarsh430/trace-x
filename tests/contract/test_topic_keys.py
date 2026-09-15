"""The partition-key table and the release ledger must agree, both ways.

`docs/contracts/RELEASED.json` records each released topic's key *and the stated
reason for it*. `trace_core.contracts.topics` mirrors that table so a deployed
artifact -- which does not ship `docs/` -- can still resolve a key. Two copies of
a control can drift, so this test diffs them edge for edge, exactly as
`test_state_machines` diffs the investigation table against its mermaid block.

It also covers the Phase 1 defect that motivated the module: a publisher that
produces without a key silently round-robins the partitions.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from trace_core.contracts.topics import PARTITION_KEY_FIELD, partition_key
from trace_core.domain.errors import ContractError, UnreleasedTopicError

pytestmark = pytest.mark.contract

ROOT = Path(__file__).resolve().parents[2]
LEDGER = ROOT / "docs" / "contracts" / "RELEASED.json"


@pytest.fixture(scope="module")
def ledger() -> dict[str, list[dict[str, str]]]:
    return json.loads(LEDGER.read_text())


def test_every_released_topic_declares_a_partition_key(
    ledger: dict[str, list[dict[str, str]]],
) -> None:
    for entry in ledger["released"]:
        assert entry["topic"] in PARTITION_KEY_FIELD, (
            f"{entry['topic']} is RELEASED but declares no partition key in "
            f"trace_core.contracts.topics. A publisher would have no key to use."
        )


def test_no_unreleased_topic_declares_a_partition_key(
    ledger: dict[str, list[dict[str, str]]],
) -> None:
    """The other direction: a key here implies a producer, which implies release."""
    released = {entry["topic"] for entry in ledger["released"]}
    extra = sorted(set(PARTITION_KEY_FIELD) - released)
    assert not extra, (
        f"{extra} declare a partition key but are not RELEASED in the ledger. "
        f"Release the schema in the phase that introduces its producer (ADR-0028)."
    )


def test_the_key_field_matches_the_ledger(ledger: dict[str, list[dict[str, str]]]) -> None:
    for entry in ledger["released"]:
        assert PARTITION_KEY_FIELD[entry["topic"]] == entry["key"], (
            f"{entry['topic']}: ledger says key={entry['key']!r}, code says "
            f"{PARTITION_KEY_FIELD[entry['topic']]!r}. Changing a key repartitions the "
            f"topic and silently reorders history (docs/EVENT_CONTRACTS.md §4)."
        )


def test_every_key_has_a_stated_reason(ledger: dict[str, list[dict[str, str]]]) -> None:
    """A key chosen for load balancing is a bug; the ledger must say why each was chosen."""
    for entry in ledger["released"]:
        assert entry.get("key_reason", "").strip(), (
            f"{entry['topic']} declares a key with no key_reason. The rule is that the key "
            f"is the entity whose ORDERING matters -- a key with no stated reason cannot be "
            f"reviewed against it."
        )


def test_partition_key_is_extracted_from_the_payload() -> None:
    event: dict[str, Any] = {
        "envelope": {"event_type": "tx.raw"},
        "payload": {"account_id": "acct_000123", "device_id": "dev_000456"},
    }
    assert partition_key("tx.raw.v1", event) == "acct_000123"
    assert partition_key("device.events.v1", event) == "dev_000456"


def test_an_unknown_topic_refuses_rather_than_defaulting() -> None:
    with pytest.raises(UnreleasedTopicError):
        partition_key("audit.v1", {"payload": {"entity_id": "acct_000123"}})


def test_a_missing_key_field_refuses_rather_than_publishing_unkeyed() -> None:
    """The Phase 1 defect, in test form: no key must be an error, not a silent round robin."""
    with pytest.raises(ContractError):
        partition_key("tx.raw.v1", {"payload": {"device_id": "dev_000456"}})
    with pytest.raises(ContractError):
        partition_key("tx.raw.v1", {"payload": {"account_id": ""}})
    with pytest.raises(ContractError):
        partition_key("tx.raw.v1", {"envelope": {}})
