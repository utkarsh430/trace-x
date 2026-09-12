"""Topic partition keys, mirrored from the release ledger.

**The defect this exists to prevent, found in Phase 1 code.**
`data/generator/emit.py` published with `producer.produce(topic, value=payload)`
and no `key=`. A null key means round-robin partitioning, so the per-partition
ordering guarantee `docs/contracts/RELEASED.json` asserts -- *"per-account
velocity is order-sensitive"* -- did not actually hold. Nothing had noticed,
because no consumer existed yet (PROGRESS.md D13). Phase 3 would have found it
as unexplained velocity drift, which is among the hardest classes of bug to
attribute.

So the key is not a per-call-site argument. It is a property of the topic,
declared once here and **diffed against the ledger by a contract test** in both
directions -- the same technique that pins the investigation state machine
against its mermaid diagram (ADR-0027). A publisher that cannot resolve a key
refuses to publish rather than silently load-balancing.

The table is mirrored rather than read from `docs/` at runtime because a
deployed artifact does not ship the documentation tree, and a control that
works in the repo but not in production is not a control.
"""

from __future__ import annotations

from typing import Any, Final

from trace_core.domain.errors import ContractError, UnreleasedTopicError

TX_RAW_V1: Final = "tx.raw.v1"
IDENTITY_EVENTS_V1: Final = "identity.events.v1"
DEVICE_EVENTS_V1: Final = "device.events.v1"
INVESTIGATION_REQUESTED_V1: Final = "investigation.requested.v1"

PARTITION_KEY_FIELD: Final[dict[str, str]] = {
    # Keyed by the entity whose ORDERING matters, never for load balancing
    # (docs/EVENT_CONTRACTS.md §3). The reasons are recorded in the ledger and
    # asserted against this table by tests/contract/test_topic_keys.py.
    TX_RAW_V1: "account_id",
    IDENTITY_EVENTS_V1: "account_id",
    DEVICE_EVENTS_V1: "device_id",
    # One message per case, and the case is the entity that exists at produce
    # time -- an investigation_id is minted later by the worker that leases it
    # (ADR-0027), so keying on one would mean keying on something that does
    # not yet exist.
    INVESTIGATION_REQUESTED_V1: "case_id",
}
"""Released topic -> the PAYLOAD field whose value is the partition key.

Only RELEASED topics appear. Adding a topic here before its schema is released
would let code publish against a contract nobody has frozen, which
`tests/contract/test_topic_release_gate.py` refuses in both directions.
"""


def partition_key(topic: str, event: dict[str, Any]) -> str:
    """The partition key for `event` on `topic`.

    Raises rather than returning `None`. A missing key is not a degraded
    publish, it is a silently reordered topic: every consumer that assumed
    per-key ordering keeps working and keeps producing subtly wrong answers.
    """
    try:
        field = PARTITION_KEY_FIELD[topic]
    except KeyError:
        raise UnreleasedTopicError(
            f"no partition key is declared for topic {topic!r}. A topic is declared here "
            f"only once its schema is released (docs/contracts/RELEASED.json); publishing "
            f"without a key would round-robin the partitions and break the ordering "
            f"guarantee the ledger states."
        ) from None

    payload = event.get("payload")
    if not isinstance(payload, dict):
        raise ContractError(
            f"event on {topic!r} has no payload object, so its partition key "
            f"({field!r}) cannot be resolved"
        )
    value = payload.get(field)
    if not isinstance(value, str) or not value:
        raise ContractError(
            f"event on {topic!r} is missing its partition key field {field!r}. "
            f"The key is required, not optional: see docs/EVENT_CONTRACTS.md §3."
        )
    return value
