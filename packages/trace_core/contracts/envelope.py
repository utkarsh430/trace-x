"""Building the mandatory event envelope (docs/EVENT_CONTRACTS.md §2, ADR-0026).

Every produced event carries the same nine fields, and two of them are easy to
get subtly wrong in ways nothing downstream can detect.

**`idempotency_key` is a content hash, not entropy.** The released schema says
*"Deterministic hash of the semantic content. Equal keys mean equal meaning."*
The Phase 1 generator emits `rng.getrandbits(256)` there instead, so two
byte-identical replays of the same transaction carry different keys. That is a
real divergence from the contract, and it is **deliberately not fixed in the
generator**: the key is inside the canonical row JSON the dataset digest covers,
so changing it would move `eval-v1`'s digest and silently invalidate every
result that cites it (ADR-0029, docs/EVALUATION.md §2). It is recorded in
`docs/PROGRESS.md` as frozen into `eval-v1` rather than left for a reader to
assume the contract holds everywhere. Every Phase 2 producer uses this module,
where the key is what the schema says it is.

**`occurred_at` and `ingested_at` are never the same clock.** Event time comes
from the event; processing time is stamped here, by the process that observed
it. `build_envelope` takes `EventTime` and reads the wall clock itself, so a
caller cannot accidentally pass one where the other belongs -- mypy rejects it.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Final
from uuid import UUID

from trace_core.contracts.canonical_json import content_hash
from trace_core.domain.errors import ContractError
from trace_core.domain.identifiers import uuid7
from trace_core.domain.time import EventTime, ProcessingTime, to_millis, utc_now

SCHEMA_VERSION: Final = 1
TRACE_ID_LENGTH: Final = 32
CORRELATION_ID_MAX: Final = 64
_HEX: Final = frozenset("0123456789abcdef")

ZERO_TRACE_ID: Final = "0" * 32
"""What `trace_id` becomes when no span is active.

A valid-shaped placeholder rather than a random one: a random id would look like
a real trace that simply cannot be found, while an all-zero id is the W3C
"invalid" value and says plainly that the event was produced outside a trace.
"""


def _iso(moment: dt.datetime) -> str:
    """RFC 3339 with a `Z` suffix, matching the released schemas' examples."""
    return moment.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


def semantic_content(
    *, event_type: str, occurred_at: EventTime, payload: dict[str, Any]
) -> dict[str, Any]:
    """The part of an event that determines whether two events mean the same.

    Deliberately excludes `event_id` (random by construction), `ingested_at`
    (when *we* saw it, not what happened), `trace_id`, `correlation_id` and
    `producer` -- all of which differ between two deliveries of the same fact.
    Includes `occurred_at`, because the same payload at a different event time is
    a different event, not a duplicate.
    """
    return {
        "event_type": event_type,
        "schema_version": SCHEMA_VERSION,
        "occurred_at": _iso(occurred_at),
        "payload": payload,
    }


def build_envelope(
    *,
    event_type: str,
    occurred_at: EventTime,
    payload: dict[str, Any],
    producer: str,
    trace_id: str,
    correlation_id: str,
    ingested_at: ProcessingTime | None = None,
    event_id: UUID | None = None,
) -> dict[str, Any]:
    """One envelope, with every field the released schema requires.

    `ingested_at` defaults to now and is typed `ProcessingTime`, so the two
    clocks cannot be transposed by accident (ADR-0026).
    """
    if len(trace_id) != TRACE_ID_LENGTH or not _HEX.issuperset(trace_id):
        raise ContractError(
            f"trace_id must be {TRACE_ID_LENGTH} lowercase hex characters (W3C), got {trace_id!r}"
        )
    if not correlation_id or len(correlation_id) > CORRELATION_ID_MAX:
        raise ContractError(
            f"correlation_id must be 1..{CORRELATION_ID_MAX} characters, got {len(correlation_id)}"
        )

    observed = ingested_at if ingested_at is not None else utc_now()
    return {
        "event_id": str(event_id or uuid7(millis=to_millis(occurred_at))),
        "event_type": event_type,
        "schema_version": SCHEMA_VERSION,
        "occurred_at": _iso(occurred_at),
        "ingested_at": _iso(observed),
        "producer": producer,
        "trace_id": trace_id,
        "correlation_id": correlation_id,
        "idempotency_key": content_hash(
            semantic_content(event_type=event_type, occurred_at=occurred_at, payload=payload)
        ),
    }


def build_event(
    *,
    event_type: str,
    occurred_at: EventTime,
    payload: dict[str, Any],
    producer: str,
    trace_id: str,
    correlation_id: str,
    ingested_at: ProcessingTime | None = None,
    event_id: UUID | None = None,
) -> dict[str, Any]:
    """A complete `{envelope, payload}` event, ready to validate and publish."""
    return {
        "envelope": build_envelope(
            event_type=event_type,
            occurred_at=occurred_at,
            payload=payload,
            producer=producer,
            trace_id=trace_id,
            correlation_id=correlation_id,
            ingested_at=ingested_at,
            event_id=event_id,
        ),
        "payload": payload,
    }
