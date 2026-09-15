"""A seeded, deterministic fault overlay over released events, with a publish schedule it keeps.

Silver (Phase 3 Step 6) and the parity framework (Step 8) must be tested against
the stream they will actually receive. `eval-v1` contains no duplicate, late or
out-of-order event (docs/PHASE3_PLAN.md §1), so a test fed only a clean dataset
passes vacuously on exactly the paths that matter. This module takes a sequence
of released `{envelope, payload}` events and returns the same stream with a
requested number of each fault class injected, a schedule for publishing it, and
provenance that makes the overlay reproducible and citable.

**Fault classes**

| class | what is published |
|---|---|
| `reordered` | an event held back until just after the next event for its key |
| `exact_duplicate` | the same bytes again, shortly after the original (same `event_id`) |
| `retry_new_event_id` | a transaction again: NEW envelope `event_id`, same `transaction_id` |
| `conflicting_duplicate` | the same dedup identity (§3 Q2) with different content |
| `late_exact_duplicate` | the same bytes again, arriving LATE (the original was on time) |
| `late_retry_new_event_id` | a transaction retry (new `event_id`) arriving LATE |
| `late` | an event whose arrival delay exceeds the late threshold |
| `future_within_24h` | an event dated ahead of its arrival, inside the future-skew bound |
| `future_beyond_24h` | an event dated ahead of its arrival, beyond the bound and its margin |
| `malformed_json` | a truncated copy of an event's bytes, keyed by that event |
| `wrong_schema_version` | a copy whose envelope claims the next schema version |
| `unknown_enum` | a copy with an enum field outside the released schema |

**Rules the overlay keeps**

* *Exact counts.* A request that cannot be met raises; it never injects fewer.
* *At most one fault per base event*, so every injected record belongs to exactly
  one class. The combinations that matter are classes of their own
  (`late_exact_duplicate`, `late_retry_new_event_id`). The invalid copies carry
  fresh identities under every declared dedup identity, so they collide with nothing.
* *Deterministic.* Every selection and every derived value comes from SHA-256 over
  the overlay version, the seed and the base position -- not from `random`.
* *Identities come from unshifted content.* Shifting or re-timing never changes an
  `event_id`, `transaction_id` or `idempotency_key` (§4.3).
* *`wrong_schema_version` records VALIDATE against the released models*: the
  envelope schema only requires `schema_version >= 1`. A consumer catches them only
  by comparing `schema_version` with the topic's version suffix.
* *No ground-truth FIELDS.* Every base record is validated against its released
  schema with extra fields forbidden, so no label, fraud pattern or scenario field
  can enter the overlay or its provenance. That does not remove label PROXIES a
  base may already carry -- `eval-v1` emits identity and device events only inside
  fraud scenarios (PHASE3_PLAN §1) -- and the overlay neither adds nor removes
  them. Choose the base accordingly.

**Time: a replay has a schedule, and a paced replay keeps it.** Lateness is arrival
delay, the broker's `LogAppendTime` minus `occurred_at` (§4.3). Every record carries
`schedule_offset_s` from the replay's anchor (the publish start):

* *Base-timed records* -- every class except `late` and the two `future_*` -- are
  published at their base `ingested_at` moved to the anchor. In `Timing.PACED` both
  `occurred_at` and `ingested_at` move by one shift, `anchor - earliest occurred_at`,
  so each record arrives after its event time by its recorded ingestion lag.
  Displaced copies arrive `duplicate_delay_s` after their source; a reordered event
  arrives just after its key's next event, and only pairs at most
  `reorder_max_gap_s` apart are reordered. The build refuses a paced base whose
  planned on-time delays would exceed `ON_TIME_CEILING_S`.
* *`late_exact_duplicate` and `late_retry_new_event_id`* are held back: same content,
  scheduled at the source's event time plus the late delay. That is a true arrival
  delay and costs real wall-clock time.
* *`late` and `future_*` records are stamped*: `occurred_at = scheduled - delay` (or
  `+ lead`) and `ingested_at = scheduled`. In a paced replay the scheduled time is
  `anchor + schedule_offset_s`, so their bytes are a function of the overlay and the
  anchor; they keep their identity and place in arrival order and their event time
  moves, because holding them back would need waits longer than the late threshold.

`publish_overlay` sleeps to each record's scheduled time, raises
`ReplayScheduleError` when it falls more than `MAX_SCHEDULE_SLIP_S` behind, and asks
`render(published_at=...)` to confirm each record arrives in its intended class. It
returns a `PublishManifest` -- anchor, shift, every scheduled and actual hand-over
time, every rendered `occurred_at`/`ingested_at` and value digest -- so what was
published can be reproduced and audited. `Timing.BACKFILL` (a compressed replay)
does not pace or shift, marks every record `REPLAY_MODE_HEADER: backfill`, refuses
every late class because `is_late` is null there, and stamps future-dated records at
the actual hand-over time, which the manifest records.

Future skew for records the overlay publishes follows the *other-producer* rule:
more than `FUTURE_SKEW_BOUND_S + OTHER_PRODUCER_CLOCK_MARGIN_S` ahead of
`LogAppendTime`. Gateway-observed events are judged instead against the gateway
receipt time carried in the event (§4.3); no released topic carries one, and the
overlay never sets it.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import itertools
import json
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final, Protocol
from uuid import UUID

from pydantic import ValidationError

from trace_core.contracts.api.transaction import MAX_CLOCK_SKEW_FUTURE_S
from trace_core.contracts.canonical_json import canonical_bytes, content_hash
from trace_core.contracts.envelope import SCHEMA_VERSION, semantic_content
from trace_core.contracts.topics import PARTITION_KEY_FIELD, partition_key
from trace_core.domain.errors import SchemaValidationError, UnreleasedTopicError
from trace_core.domain.identifiers import uuid7_millis
from trace_core.domain.time import event_time

OVERLAY_VERSION: Final = "fault-overlay-v2"

# Timing semantics v1 (docs/PHASE3_PLAN.md §4.3). The future-skew bound is the API's
# own constant. The late threshold and the other-producer clock margin have no home
# in code until Step 6 lands the canonical timing constants; until then they are
# mirrored here and pinned to their literal values by tests/unit/test_faults_overlay.py.
TIMING_SEMANTICS_VERSION: Final = 1
IS_LATE_THRESHOLD_S: Final = 600
FUTURE_SKEW_BOUND_S: Final = MAX_CLOCK_SKEW_FUTURE_S
OTHER_PRODUCER_CLOCK_MARGIN_S: Final = 300

ARRIVAL_MARGIN_S: Final = 300
"""The overlay's own distance from every threshold, so transport latency and broker
clock offset cannot move a record from one arrival class into another."""
MAX_SCHEDULE_SLIP_S: Final = 60
"""How far behind its schedule a paced replay may fall before it stops."""
ON_TIME_CEILING_S: Final = IS_LATE_THRESHOLD_S - ARRIVAL_MARGIN_S - MAX_SCHEDULE_SLIP_S
"""The largest planned arrival delay an on-time record may have."""
REORDER_EPSILON_S: Final = 0.001

REPLAY_MODE_HEADER: Final = "trace-replay-mode"
BACKFILL_HEADER_VALUE: Final = b"backfill"

DEDUP_IDENTITY: Final[Mapping[str, tuple[str, str]]] = {
    # PHASE3_PLAN §3 Q2, mirrored from deploy/kafka/topics.yaml and diffed against
    # it by tests/unit/test_faults_overlay.py.
    "tx.raw.v1": ("payload", "transaction_id"),
    "identity.events.v1": ("envelope", "event_id"),
    "device.events.v1": ("envelope", "event_id"),
    "investigation.requested.v1": ("envelope", "idempotency_key"),
    "tx.authorization.v1": ("payload", "transaction_id"),
    "tx.scored.v1": ("payload", "transaction_id"),
}

_ENUM_FIELD: Final[Mapping[str, str]] = {
    "tx.raw.v1": "channel",
    "identity.events.v1": "identity_event_type",
    "device.events.v1": "device_event_type",
    "investigation.requested.v1": "feature_source",
    "tx.authorization.v1": "authorization_outcome",
    "tx.scored.v1": "observe_outcome",
}
UNKNOWN_ENUM_VALUE: Final = "OVERLAY_UNSEEN_VALUE"

_MAX_AMOUNT_MINOR: Final = 100_000_000_000
_MAX_TRANSACTION_ID: Final = 64
_TRANSACTION_TOPIC: Final = "tx.raw.v1"


class FaultClass(StrEnum):
    """Declaration order is selection order: the most constrained class goes first."""

    REORDERED = "reordered"
    EXACT_DUPLICATE = "exact_duplicate"
    RETRY_NEW_EVENT_ID = "retry_new_event_id"
    CONFLICTING_DUPLICATE = "conflicting_duplicate"
    LATE_EXACT_DUPLICATE = "late_exact_duplicate"
    LATE_RETRY_NEW_EVENT_ID = "late_retry_new_event_id"
    LATE = "late"
    FUTURE_WITHIN_24H = "future_within_24h"
    FUTURE_BEYOND_24H = "future_beyond_24h"
    MALFORMED_JSON = "malformed_json"
    WRONG_SCHEMA_VERSION = "wrong_schema_version"
    UNKNOWN_ENUM = "unknown_enum"


class Arrival(StrEnum):
    """The arrival class a record is meant to land in."""

    ON_TIME = "on_time"
    LATE = "late"
    FUTURE_WITHIN_24H = "future_within_24h"
    FUTURE_BEYOND_24H = "future_beyond_24h"


class Timing(StrEnum):
    PACED = "paced"
    BACKFILL = "backfill"


_CLASS_ORDER: Final = {fault: index for index, fault in enumerate(FaultClass)}
STAMPED_CLASSES: Final = frozenset(
    {FaultClass.LATE, FaultClass.FUTURE_WITHIN_24H, FaultClass.FUTURE_BEYOND_24H}
)
HELD_BACK_CLASSES: Final = frozenset(
    {FaultClass.LATE_EXACT_DUPLICATE, FaultClass.LATE_RETRY_NEW_EVENT_ID}
)
INVALID_CLASSES: Final = frozenset(
    {FaultClass.MALFORMED_JSON, FaultClass.WRONG_SCHEMA_VERSION, FaultClass.UNKNOWN_ENUM}
)
ADDITIVE_CLASSES: Final = frozenset(
    {
        FaultClass.EXACT_DUPLICATE,
        FaultClass.RETRY_NEW_EVENT_ID,
        FaultClass.CONFLICTING_DUPLICATE,
        *HELD_BACK_CLASSES,
        *INVALID_CLASSES,
    }
)
"""Classes that ADD a record; the rest alter an original where it stands."""
PACED_ONLY_CLASSES: Final = frozenset({FaultClass.LATE, *HELD_BACK_CLASSES})
_TRANSACTION_ONLY: Final = frozenset(
    {FaultClass.RETRY_NEW_EVENT_ID, FaultClass.LATE_RETRY_NEW_EVENT_ID}
)
_ARRIVAL_OF: Final[Mapping[FaultClass, Arrival]] = {
    FaultClass.LATE: Arrival.LATE,
    FaultClass.LATE_EXACT_DUPLICATE: Arrival.LATE,
    FaultClass.LATE_RETRY_NEW_EVENT_ID: Arrival.LATE,
    FaultClass.FUTURE_WITHIN_24H: Arrival.FUTURE_WITHIN_24H,
    FaultClass.FUTURE_BEYOND_24H: Arrival.FUTURE_BEYOND_24H,
}


class ReplayScheduleError(RuntimeError):
    """A record would not arrive in the arrival class the overlay intends for it.

    Raised rather than published: a fault-free record that arrives late, or a late
    record that arrives on time, would make a lateness test pass or fail for reasons
    unrelated to the code under test.
    """


@dataclass(frozen=True)
class FaultPlan:
    """What to inject, and how the replay is timed."""

    seed: int
    counts: Mapping[FaultClass, int]
    timing: Timing = Timing.PACED
    late_arrival_delay_s: tuple[int, int] = (1_800, 7_200)
    future_within_24h_lead_s: tuple[int, int] = (3_600, 43_200)
    future_beyond_24h_lead_s: tuple[int, int] = (93_600, 259_200)
    duplicate_delay_s: tuple[int, int] = (1, 120)
    """How long after its source an on-time displaced copy arrives."""
    reorder_max_gap_s: float = 120.0
    """Only consecutive same-key events at most this far apart in event time swap."""

    def __post_init__(self) -> None:
        for fault, count in self.counts.items():
            if not isinstance(fault, FaultClass):
                raise ValueError(f"unknown fault class {fault!r}; use FaultClass members")
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise ValueError(f"{fault}: count must be a non-negative integer, got {count!r}")
        late_lo, late_hi = self.late_arrival_delay_s
        if not IS_LATE_THRESHOLD_S + ARRIVAL_MARGIN_S < late_lo <= late_hi:
            raise ValueError(
                f"late arrival delays {self.late_arrival_delay_s} must start beyond "
                f"{IS_LATE_THRESHOLD_S + ARRIVAL_MARGIN_S}s (the late threshold plus the margin)"
            )
        within_lo, within_hi = self.future_within_24h_lead_s
        floor = ARRIVAL_MARGIN_S + MAX_SCHEDULE_SLIP_S
        if not floor <= within_lo <= within_hi < FUTURE_SKEW_BOUND_S - ARRIVAL_MARGIN_S:
            raise ValueError(
                f"future-within-24h leads {self.future_within_24h_lead_s} must sit inside "
                f"[{floor}, {FUTURE_SKEW_BOUND_S - ARRIVAL_MARGIN_S})"
            )
        beyond_lo, beyond_hi = self.future_beyond_24h_lead_s
        beyond_floor = (
            FUTURE_SKEW_BOUND_S
            + OTHER_PRODUCER_CLOCK_MARGIN_S
            + ARRIVAL_MARGIN_S
            + MAX_SCHEDULE_SLIP_S
        )
        if not beyond_floor < beyond_lo <= beyond_hi:
            raise ValueError(
                f"future-beyond-24h leads {self.future_beyond_24h_lead_s} must start beyond "
                f"{beyond_floor}s (the bound, the other-producer clock margin, the margin and "
                f"the allowed schedule slip)"
            )
        dup_lo, dup_hi = self.duplicate_delay_s
        if not 0 <= dup_lo <= dup_hi < ON_TIME_CEILING_S:
            raise ValueError(
                f"duplicate_delay_s {self.duplicate_delay_s} must satisfy "
                f"0 <= lo <= hi < {ON_TIME_CEILING_S}"
            )
        if not 0 < self.reorder_max_gap_s < ON_TIME_CEILING_S:
            raise ValueError(f"reorder_max_gap_s must be in (0, {ON_TIME_CEILING_S})")
        if self.timing is Timing.BACKFILL:
            refused = sorted(f.value for f in PACED_ONLY_CLASSES if self.counts.get(f, 0))
            if refused:
                raise ValueError(
                    f"{refused} cannot be injected into a backfill: a compressed replay has no "
                    f"meaningful arrival delay, so is_late is null there (PHASE3_PLAN §4.3)"
                )


# ---------------------------------------------------------------- helpers ----


def _rank(seed: int, *parts: object) -> int:
    material = "|".join([OVERLAY_VERSION, str(seed), *(str(p) for p in parts)])
    return int.from_bytes(hashlib.sha256(material.encode()).digest()[:8], "big")


def _draw(seed: int, band: tuple[int, int], *parts: object) -> int:
    lo, hi = band
    return lo + _rank(seed, *parts) % (hi - lo + 1)


def _parse_time(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise SchemaValidationError(f"timestamp {value!r} carries no timezone")
    return parsed


def _format_time(moment: dt.datetime) -> str:
    return moment.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


def _require_aware(moment: dt.datetime, name: str) -> None:
    if moment.utcoffset() is None:
        raise ValueError(f"{name} must be a timezone-aware UTC datetime")


def _model(topic: str) -> Any:
    from trace_core.contracts.events import (
        device_events_v1,
        identity_events_v1,
        investigation_requested_v1,
        tx_authorization_v1,
        tx_raw_v1,
        tx_scored_v1,
    )

    models: dict[str, Any] = {
        "tx.raw.v1": tx_raw_v1.TxRawV1,
        "identity.events.v1": identity_events_v1.IdentityEventV1,
        "device.events.v1": device_events_v1.DeviceEventV1,
        "investigation.requested.v1": investigation_requested_v1.InvestigationRequestedV1,
        "tx.authorization.v1": tx_authorization_v1.TxAuthorizationV1,
        "tx.scored.v1": tx_scored_v1.TxScoredV1,
    }
    return models[topic]


def _validated_copy(topic: str, event: Mapping[str, Any], index: int) -> dict[str, Any]:
    if topic not in PARTITION_KEY_FIELD or topic not in DEDUP_IDENTITY:
        raise UnreleasedTopicError(f"base event {index} is on {topic!r}, which is not released")
    if not isinstance(event, Mapping) or set(event) != {"envelope", "payload"}:
        found = sorted(event) if isinstance(event, Mapping) else type(event).__name__
        raise SchemaValidationError(
            f"base event {index} on {topic} carries {found}. The overlay accepts released events "
            f"only -- envelope and payload, nothing else -- so no label, fraud pattern or "
            f"scenario field can ride along into a test stream (CLAUDE.md §11)."
        )
    encoded = canonical_bytes(event)
    try:
        _model(topic).model_validate_json(encoded)
    except ValidationError as exc:
        raise SchemaValidationError(
            f"base event {index} on {topic} is not a valid released event, so it cannot be a "
            f"base for faults (extra fields are forbidden): {exc}"
        ) from exc
    copied: dict[str, Any] = json.loads(encoded)
    return copied


def _copy(event: Mapping[str, Any]) -> dict[str, Any]:
    copied: dict[str, Any] = json.loads(canonical_bytes(event))
    return copied


def _identity(topic: str, event: Mapping[str, Any]) -> Any:
    section, name = DEDUP_IDENTITY[topic]
    return event[section][name]


def _derived_event_id(seed: int, fault: FaultClass, index: int, source: Mapping[str, Any]) -> str:
    """A UUIDv7 with the source's millisecond prefix and deterministic randomness."""
    base_id = str(source["envelope"]["event_id"])
    millis = uuid7_millis(UUID(base_id))
    material = f"{OVERLAY_VERSION}|{seed}|{fault}|event_id|{index}".encode()
    entropy = int.from_bytes(hashlib.sha256(material).digest(), "big")
    rand_a = (entropy >> 62) & 0xFFF
    rand_b = entropy & ((1 << 62) - 1)
    value = (millis << 80) | (7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
    derived = str(UUID(int=value))
    if derived == base_id:
        raise RuntimeError(f"derived event_id collided with its source at base event {index}")
    return derived


def _refresh_content_key(event: dict[str, Any]) -> None:
    envelope = event["envelope"]
    envelope["idempotency_key"] = content_hash(
        semantic_content(
            event_type=envelope["event_type"],
            occurred_at=event_time(_parse_time(envelope["occurred_at"])),
            payload=event["payload"],
        )
    )


def _retry(event: dict[str, Any], seed: int, index: int, fault: FaultClass) -> dict[str, Any]:
    variant = _copy(event)
    variant["envelope"]["event_id"] = _derived_event_id(seed, fault, index, event)
    return variant


def _conflict(topic: str, event: dict[str, Any], seed: int, index: int) -> dict[str, Any]:
    """Same dedup identity, different content."""
    variant = _copy(event)
    envelope, payload = variant["envelope"], variant["payload"]
    rank = _rank(seed, FaultClass.CONFLICTING_DUPLICATE, "content", index)
    if topic == _TRANSACTION_TOPIC:
        amount = int(payload["amount_minor"])
        delta = 1 + rank % 997
        payload["amount_minor"] = (
            amount + delta if amount + delta <= _MAX_AMOUNT_MINOR else amount - delta
        )
        envelope["event_id"] = _derived_event_id(
            seed, FaultClass.CONFLICTING_DUPLICATE, index, event
        )
        _refresh_content_key(variant)
    elif topic in ("identity.events.v1", "device.events.v1"):
        candidate = f"ip_{10_000 + rank % 90_000:05d}"
        if candidate == payload.get("ip_id"):
            candidate = f"ip_{10_000 + (rank + 1) % 90_000:05d}"
        payload["ip_id"] = candidate
        _refresh_content_key(variant)  # event_id is the identity and stays
    else:
        score = float(payload["score"])
        payload["score"] = round(score + 0.01, 6) if score + 0.01 <= 1 else round(score - 0.01, 6)
        envelope["event_id"] = _derived_event_id(
            seed, FaultClass.CONFLICTING_DUPLICATE, index, event
        )  # idempotency_key is the identity and stays
    if _identity(topic, variant) != _identity(topic, event):
        raise RuntimeError(f"conflicting duplicate of base event {index} lost its identity")
    if canonical_bytes(variant) == canonical_bytes(event):
        raise RuntimeError(f"conflicting duplicate of base event {index} has identical content")
    return variant


def _fresh_identity(
    topic: str, event: dict[str, Any], seed: int, fault: FaultClass, index: int
) -> dict[str, Any]:
    """A copy that collides with nothing under any declared dedup identity."""
    variant = _copy(event)
    envelope = variant["envelope"]
    envelope["event_id"] = _derived_event_id(seed, fault, index, event)
    material = f"{OVERLAY_VERSION}|{seed}|{fault}|{index}|{envelope['idempotency_key']}"
    envelope["idempotency_key"] = "sha256:" + hashlib.sha256(material.encode()).hexdigest()
    if topic == _TRANSACTION_TOPIC:
        payload = variant["payload"]
        suffix = f"~{_rank(seed, fault, 'transaction_id', index) % 16**6:06x}"
        payload["transaction_id"] = (
            str(payload["transaction_id"])[: _MAX_TRANSACTION_ID - len(suffix)] + suffix
        )
    return variant


def _truncate(value: bytes, rank: int) -> bytes:
    cut = max(1, len(value) // 2 + rank % max(1, len(value) // 4))
    truncated = value[:cut]
    try:
        json.loads(truncated)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return truncated
    raise RuntimeError("a truncated JSON object decoded; the malformed record would be valid")


def _distribution(values: Sequence[float]) -> dict[str, float] | None:
    if not values:
        return None
    ordered = sorted(values)

    def rank(q: float) -> float:
        return ordered[min(len(ordered) - 1, max(0, round(q * len(ordered)) - 1))]

    return {
        "count": float(len(ordered)),
        "min": ordered[0],
        "p50": rank(0.50),
        "p95": rank(0.95),
        "max": ordered[-1],
    }


# ---------------------------------------------------------------- records ----


@dataclass(frozen=True)
class OverlayRecord:
    """One record in publish order. Treat `event` and `key_event` as read-only."""

    position: int
    topic: str
    fault: FaultClass | None
    base_index: int
    key_event: dict[str, Any]
    event: dict[str, Any]
    """The content before any shift or stamping (for malformed JSON, its source)."""
    malformed_rank: int | None
    arrival: Arrival
    stamp_delay_s: int | None
    """Stamped records only: + seconds late, - seconds of future lead."""
    schedule_offset_s: float
    timing: Timing
    base_start: dt.datetime
    headers: tuple[tuple[str, bytes], ...]

    @property
    def key(self) -> str:
        return partition_key(self.topic, self.key_event)

    @property
    def stamped(self) -> bool:
        return self.stamp_delay_s is not None

    def scheduled_at(self, anchor: dt.datetime) -> dt.datetime:
        _require_aware(anchor, "anchor")
        if self.timing is Timing.BACKFILL:
            return anchor
        return anchor + dt.timedelta(seconds=self.schedule_offset_s)

    def render(self, anchor: dt.datetime, published_at: dt.datetime | None = None) -> bytes:
        """The value bytes for a replay anchored at `anchor`.

        With `published_at`, also confirms the record arrives in its intended class at
        that hand-over time, and raises `ReplayScheduleError` if it would not.
        """
        _require_aware(anchor, "anchor")
        if published_at is not None:
            _require_aware(published_at, "published_at")
        content = _copy(self.event)
        envelope = content["envelope"]
        if self.stamp_delay_s is not None:
            if self.timing is Timing.PACED:
                stamp = self.scheduled_at(anchor)
            elif published_at is None:
                raise ValueError(
                    f"record {self.position} ({self.fault}) in a backfill is stamped at its "
                    f"hand-over time; render it with published_at"
                )
            else:
                stamp = published_at
            envelope["occurred_at"] = _format_time(stamp - dt.timedelta(seconds=self.stamp_delay_s))
            envelope["ingested_at"] = _format_time(stamp)
        elif self.timing is Timing.PACED:
            shift = anchor - self.base_start
            for field in ("occurred_at", "ingested_at"):
                envelope[field] = _format_time(_parse_time(envelope[field]) + shift)
        if published_at is not None:
            self._check_arrival(_parse_time(envelope["occurred_at"]), published_at)
        value = canonical_bytes(content)
        if self.malformed_rank is not None:
            return _truncate(value, self.malformed_rank)
        return value

    def _check_arrival(self, occurred: dt.datetime, published_at: dt.datetime) -> None:
        delay = (published_at - occurred).total_seconds()
        if self.arrival is Arrival.ON_TIME:
            if self.timing is Timing.BACKFILL:
                return
            ok = -ARRIVAL_MARGIN_S < delay < IS_LATE_THRESHOLD_S - ARRIVAL_MARGIN_S
        elif self.arrival is Arrival.LATE:
            ok = delay > IS_LATE_THRESHOLD_S + ARRIVAL_MARGIN_S
        elif self.arrival is Arrival.FUTURE_WITHIN_24H:
            ok = ARRIVAL_MARGIN_S <= -delay < FUTURE_SKEW_BOUND_S - ARRIVAL_MARGIN_S
        else:
            ok = -delay > FUTURE_SKEW_BOUND_S + OTHER_PRODUCER_CLOCK_MARGIN_S + ARRIVAL_MARGIN_S
        if not ok:
            raise ReplayScheduleError(
                f"record {self.position} ({self.fault or 'fault-free'}) would reach the broker "
                f"{delay:+.1f}s after its event time, outside the {self.arrival} arrival class. "
                f"A replay that does not keep its schedule turns fault-free records late (or "
                f"late records on time) and every lateness test built on it vacuous."
            )


@dataclass(frozen=True)
class OverlayProvenance:
    overlay_version: str
    timing_semantics_version: int
    seed: int
    base_dataset_ref: str
    base_digest: str
    base_events_by_topic: Mapping[str, int]
    base_start: str
    base_event_span_s: float
    schedule_span_s: float
    """The paced replay's publish duration, from anchor to its last record."""
    timing: str
    bands: Mapping[str, Any]
    planned_fault_free_delay_s: Mapping[str, float] | None
    """Planned arrival delay of fault-free records (paced replays only)."""
    max_planned_on_time_delay_s: float | None
    requested: Mapping[str, int]
    injected: Mapping[str, int]
    injected_by_topic: Mapping[str, Mapping[str, int]]
    output_records: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "overlay_version": self.overlay_version,
            "timing_semantics_version": self.timing_semantics_version,
            "seed": self.seed,
            "base_dataset_ref": self.base_dataset_ref,
            "base_digest": self.base_digest,
            "base_events_by_topic": dict(self.base_events_by_topic),
            "base_start": self.base_start,
            "base_event_span_s": self.base_event_span_s,
            "schedule_span_s": self.schedule_span_s,
            "timing": self.timing,
            "bands": {k: list(v) if isinstance(v, tuple) else v for k, v in self.bands.items()},
            "planned_fault_free_delay_s": (
                dict(self.planned_fault_free_delay_s) if self.planned_fault_free_delay_s else None
            ),
            "max_planned_on_time_delay_s": self.max_planned_on_time_delay_s,
            "requested": dict(self.requested),
            "injected": dict(self.injected),
            "injected_by_topic": {k: dict(v) for k, v in self.injected_by_topic.items()},
            "output_records": self.output_records,
        }


@dataclass(frozen=True)
class Overlay:
    records: tuple[OverlayRecord, ...]
    provenance: OverlayProvenance


@dataclass
class _Draft:
    topic: str
    fault: FaultClass | None
    base_index: int
    key_event: dict[str, Any]
    event: dict[str, Any]
    malformed_rank: int | None
    arrival: Arrival
    stamp_delay_s: int | None
    schedule_offset_s: float
    lane: int


def _pick(eligible: Sequence[int], wanted: int, seed: int, fault: FaultClass) -> list[int]:
    if len(eligible) < wanted:
        raise ValueError(
            f"{fault}: {wanted} requested but only {len(eligible)} eligible base events remain "
            f"(each base event carries at most one fault). Use a larger base or fewer faults; "
            f"the overlay never injects fewer than requested."
        )
    return sorted(eligible, key=lambda i: (_rank(seed, fault, i), i))[:wanted]


# ------------------------------------------------------------------ build ----


def build_overlay(
    base: Sequence[tuple[str, Mapping[str, Any]]], plan: FaultPlan, *, base_dataset_ref: str
) -> Overlay:
    """The base stream with `plan.counts` faults injected, scheduled and in publish order."""
    if not base_dataset_ref:
        raise ValueError("base_dataset_ref is required: an overlay must name what it overlays")
    events = [
        (topic, _validated_copy(topic, event, index)) for index, (topic, event) in enumerate(base)
    ]
    if not events:
        raise ValueError("the base stream is empty")

    digest = hashlib.sha256()
    for topic, event in events:
        digest.update(topic.encode() + b"\n" + canonical_bytes(event) + b"\n")

    occurred = [_parse_time(e["envelope"]["occurred_at"]) for _, e in events]
    ingested = [_parse_time(e["envelope"]["ingested_at"]) for _, e in events]
    base_start = min(occurred)
    event_off = [(o - base_start).total_seconds() for o in occurred]
    ingest_off = [(g - base_start).total_seconds() for g in ingested]

    free = set(range(len(events)))
    originals: dict[int, _Draft] = {
        i: _Draft(topic, None, i, event, event, None, Arrival.ON_TIME, None, ingest_off[i], 0)
        for i, (topic, event) in enumerate(events)
    }
    copies: list[_Draft] = []

    # Reordering first: it needs PAIRS of free events, the most constrained choice.
    wanted_pairs = plan.counts.get(FaultClass.REORDERED, 0)
    if wanted_pairs:
        by_key: dict[tuple[str, str], list[int]] = {}
        for index, (topic, event) in enumerate(events):
            by_key.setdefault((topic, partition_key(topic, event)), []).append(index)
        pairs = [
            (a, b)
            for indices in by_key.values()
            for a, b in itertools.pairwise(sorted(indices, key=lambda i: (event_off[i], i)))
            if 0 <= event_off[b] - event_off[a] <= plan.reorder_max_gap_s
        ]
        pairs.sort(key=lambda p: (_rank(plan.seed, FaultClass.REORDERED, p[0], p[1]), p))
        chosen: list[tuple[int, int]] = []
        for a, b in pairs:
            if len(chosen) == wanted_pairs:
                break
            if a in free and b in free:
                chosen.append((a, b))
                free.difference_update((a, b))
        if len(chosen) < wanted_pairs:
            raise ValueError(
                f"reordered: {wanted_pairs} pairs requested but only {len(chosen)} disjoint pairs "
                f"of consecutive same-key events within {plan.reorder_max_gap_s}s exist"
            )
        for a, b in chosen:
            held = originals[a]
            held.fault = FaultClass.REORDERED
            held.schedule_offset_s = max(ingest_off[a], ingest_off[b]) + REORDER_EPSILON_S

    for fault in FaultClass:
        wanted = plan.counts.get(fault, 0)
        if fault is FaultClass.REORDERED or not wanted:
            continue
        eligible = [
            i
            for i in sorted(free)
            if fault not in _TRANSACTION_ONLY or events[i][0] == _TRANSACTION_TOPIC
        ]
        for index in _pick(eligible, wanted, plan.seed, fault):
            free.discard(index)
            topic, event = events[index]
            if fault in STAMPED_CLASSES:
                original = originals[index]
                original.fault = fault
                original.arrival = _ARRIVAL_OF[fault]
                if fault is FaultClass.LATE:
                    delay = _draw(plan.seed, plan.late_arrival_delay_s, fault, "delay", index)
                elif fault is FaultClass.FUTURE_WITHIN_24H:
                    delay = -_draw(plan.seed, plan.future_within_24h_lead_s, fault, "lead", index)
                else:
                    delay = -_draw(plan.seed, plan.future_beyond_24h_lead_s, fault, "lead", index)
                original.stamp_delay_s = delay
            elif fault in (
                FaultClass.EXACT_DUPLICATE,
                FaultClass.RETRY_NEW_EVENT_ID,
                FaultClass.CONFLICTING_DUPLICATE,
            ):
                if fault is FaultClass.EXACT_DUPLICATE:
                    variant = event
                elif fault is FaultClass.RETRY_NEW_EVENT_ID:
                    variant = _retry(event, plan.seed, index, fault)
                else:
                    variant = _conflict(topic, event, plan.seed, index)
                gap = _draw(plan.seed, plan.duplicate_delay_s, fault, "gap", index)
                copies.append(
                    _Draft(
                        topic,
                        fault,
                        index,
                        variant,
                        variant,
                        None,
                        Arrival.ON_TIME,
                        None,
                        ingest_off[index] + gap,
                        2,
                    )
                )
            elif fault in HELD_BACK_CLASSES:
                variant = (
                    event
                    if fault is FaultClass.LATE_EXACT_DUPLICATE
                    else _retry(event, plan.seed, index, fault)
                )
                delay = _draw(plan.seed, plan.late_arrival_delay_s, fault, "delay", index)
                copies.append(
                    _Draft(
                        topic,
                        fault,
                        index,
                        variant,
                        variant,
                        None,
                        Arrival.LATE,
                        None,
                        event_off[index] + delay,
                        2,
                    )
                )
            elif fault is FaultClass.MALFORMED_JSON:
                rank = _rank(plan.seed, fault, "cut", index)
                copies.append(
                    _Draft(
                        topic,
                        fault,
                        index,
                        event,
                        event,
                        rank,
                        Arrival.ON_TIME,
                        None,
                        ingest_off[index],
                        1,
                    )
                )
            else:
                variant = _fresh_identity(topic, event, plan.seed, fault, index)
                if fault is FaultClass.WRONG_SCHEMA_VERSION:
                    variant["envelope"]["schema_version"] = SCHEMA_VERSION + 1
                else:
                    variant["payload"][_ENUM_FIELD[topic]] = UNKNOWN_ENUM_VALUE
                copies.append(
                    _Draft(
                        topic,
                        fault,
                        index,
                        variant,
                        variant,
                        None,
                        Arrival.ON_TIME,
                        None,
                        ingest_off[index],
                        1,
                    )
                )

    drafts = [*originals.values(), *copies]
    planned_on_time: list[tuple[float, _Draft]] = [
        (d.schedule_offset_s - event_off[d.base_index], d)
        for d in drafts
        if d.arrival is Arrival.ON_TIME
    ]
    if plan.timing is Timing.PACED:
        offending = [
            (delay, d) for delay, d in planned_on_time if not 0 <= delay + 1e-9 <= ON_TIME_CEILING_S
        ]
        if offending:
            worst_delay, worst = max(offending, key=lambda item: abs(item[0]))
            raise ValueError(
                f"{len(offending)} on-time record(s) would arrive outside "
                f"[0, {ON_TIME_CEILING_S}]s of their event time in a paced replay (worst: "
                f"{worst_delay:.3f}s, base event "
                f"{worst.base_index}, {worst.fault or 'fault-free'}). The base's recorded "
                f"ingestion lag, or the duplicate or reorder bands, would make fault-free "
                f"records late or early; the overlay refuses rather than mislabel them."
            )

    drafts.sort(
        key=lambda d: (
            d.schedule_offset_s,
            d.lane,
            -1 if d.fault is None else _CLASS_ORDER[d.fault],
            d.base_index,
        )
    )
    headers: tuple[tuple[str, bytes], ...] = (
        ((REPLAY_MODE_HEADER, BACKFILL_HEADER_VALUE),) if plan.timing is Timing.BACKFILL else ()
    )
    records = tuple(
        OverlayRecord(
            position=position,
            topic=d.topic,
            fault=d.fault,
            base_index=d.base_index,
            key_event=d.key_event,
            event=d.event,
            malformed_rank=d.malformed_rank,
            arrival=d.arrival,
            stamp_delay_s=d.stamp_delay_s,
            schedule_offset_s=d.schedule_offset_s,
            timing=plan.timing,
            base_start=base_start,
            headers=headers,
        )
        for position, d in enumerate(drafts)
    )

    injected: Counter[str] = Counter()
    by_topic: dict[str, Counter[str]] = {}
    for record in records:
        if record.fault is not None:
            injected[record.fault.value] += 1
            by_topic.setdefault(record.fault.value, Counter())[record.topic] += 1
    for fault in FaultClass:
        if injected.get(fault.value, 0) != plan.counts.get(fault, 0):
            raise RuntimeError(
                f"overlay invariant broken: {fault} injected {injected.get(fault.value, 0)}, "
                f"requested {plan.counts.get(fault, 0)}"
            )

    fault_free = [delay for delay, d in planned_on_time if d.fault is None]
    paced = plan.timing is Timing.PACED
    provenance = OverlayProvenance(
        overlay_version=OVERLAY_VERSION,
        timing_semantics_version=TIMING_SEMANTICS_VERSION,
        seed=plan.seed,
        base_dataset_ref=base_dataset_ref,
        base_digest="sha256:" + digest.hexdigest(),
        base_events_by_topic=dict(Counter(topic for topic, _ in events)),
        base_start=_format_time(base_start),
        base_event_span_s=max(event_off),
        schedule_span_s=max(d.schedule_offset_s for d in drafts) if paced else 0.0,
        timing=plan.timing.value,
        bands={
            "late_arrival_delay_s": plan.late_arrival_delay_s,
            "future_within_24h_lead_s": plan.future_within_24h_lead_s,
            "future_beyond_24h_lead_s": plan.future_beyond_24h_lead_s,
            "duplicate_delay_s": plan.duplicate_delay_s,
            "reorder_max_gap_s": plan.reorder_max_gap_s,
            "on_time_ceiling_s": ON_TIME_CEILING_S,
            "max_schedule_slip_s": MAX_SCHEDULE_SLIP_S,
        },
        planned_fault_free_delay_s=_distribution(fault_free) if paced else None,
        max_planned_on_time_delay_s=max(d for d, _ in planned_on_time) if paced else None,
        requested={fault.value: plan.counts.get(fault, 0) for fault in FaultClass},
        injected={fault.value: injected.get(fault.value, 0) for fault in FaultClass},
        injected_by_topic={k: dict(v) for k, v in sorted(by_topic.items())},
        output_records=len(records),
    )
    return Overlay(records=records, provenance=provenance)


# ---------------------------------------------------------------- publish ----


class OverlayPublisher(Protocol):
    """What `publish_overlay` needs: `trace_core.contracts.publish.EventPublisher` fits."""

    allow_invalid_events: bool

    def publish(
        self,
        topic: str,
        value: bytes,
        *,
        key_event: dict[str, Any] | None = None,
        headers: Sequence[tuple[str, bytes]] | None = None,
        block: bool = True,
    ) -> bool: ...


@dataclass(frozen=True)
class PublishedRecord:
    record: OverlayRecord
    value: bytes
    scheduled_at: dt.datetime
    published_at: dt.datetime
    occurred_at: str | None
    ingested_at: str | None

    @property
    def value_sha256(self) -> str:
        return "sha256:" + hashlib.sha256(self.value).hexdigest()


@dataclass(frozen=True)
class PublishManifest:
    """What was handed to the producer, and when -- enough to reproduce and audit it."""

    overlay_version: str
    base_digest: str
    timing: str
    anchor: dt.datetime
    shift_s: float | None
    records: tuple[PublishedRecord, ...]
    max_schedule_slip_s: float
    fault_free_delay_at_handover_s: Mapping[str, float] | None
    """Host-clock hand-over time minus occurred_at, for fault-free records (paced only)."""

    def as_dict(self) -> dict[str, Any]:
        return {
            "overlay_version": self.overlay_version,
            "base_digest": self.base_digest,
            "timing": self.timing,
            "anchor": _format_time(self.anchor),
            "shift_s": self.shift_s,
            "max_schedule_slip_s": self.max_schedule_slip_s,
            "fault_free_delay_at_handover_s": (
                dict(self.fault_free_delay_at_handover_s)
                if self.fault_free_delay_at_handover_s
                else None
            ),
            "records": [
                {
                    "position": p.record.position,
                    "topic": p.record.topic,
                    "fault": p.record.fault.value if p.record.fault else None,
                    "scheduled_at": _format_time(p.scheduled_at),
                    "published_at": _format_time(p.published_at),
                    "occurred_at": p.occurred_at,
                    "ingested_at": p.ingested_at,
                    "value_sha256": p.value_sha256,
                }
                for p in self.records
            ],
        }


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def publish_overlay(
    overlay: Overlay,
    publisher: OverlayPublisher,
    *,
    anchor: dt.datetime | None = None,
    clock: Callable[[], dt.datetime] = _utc_now,
    sleep: Callable[[float], None] = time.sleep,
    start_lead_s: float = 1.0,
) -> PublishManifest:
    """Publish every record on its schedule through the producer factory.

    A paced replay sleeps until each record's scheduled time and stops with
    `ReplayScheduleError` when it cannot keep up. The caller closes the publisher;
    that close is the verdict on delivery, and the returned manifest records what
    was handed over.
    """
    faults = {record.fault for record in overlay.records}
    if faults & INVALID_CLASSES and not publisher.allow_invalid_events:
        raise ValueError(
            "this overlay contains schema-invalid records; publish it with an EventPublisher "
            "built with allow_invalid_events=True, which only a loopback broker accepts"
        )
    begin = anchor if anchor is not None else clock() + dt.timedelta(seconds=start_lead_s)
    _require_aware(begin, "anchor")
    timing = overlay.records[0].timing if overlay.records else Timing.PACED
    base_start = overlay.records[0].base_start if overlay.records else begin

    published: list[PublishedRecord] = []
    delays: list[float] = []
    max_slip = 0.0
    for record in overlay.records:
        scheduled = record.scheduled_at(begin)
        if timing is Timing.PACED:
            while (remaining := (scheduled - clock()).total_seconds()) > 0:
                sleep(min(remaining, 0.5))
        now = clock()
        slip = (now - scheduled).total_seconds()
        if timing is Timing.PACED:
            max_slip = max(max_slip, slip)
            if slip > MAX_SCHEDULE_SLIP_S:
                raise ReplayScheduleError(
                    f"record {record.position} was due at {_format_time(scheduled)} and is being "
                    f"handed over {slip:.1f}s late, beyond the {MAX_SCHEDULE_SLIP_S}s the replay "
                    f"may slip; it can no longer keep its schedule"
                )
        value = record.render(begin, published_at=now)
        publisher.publish(
            record.topic, value, key_event=record.key_event, headers=record.headers or None
        )
        occurred_at = ingested_at = None
        if record.malformed_rank is None:
            envelope = json.loads(value)["envelope"]
            occurred_at, ingested_at = envelope["occurred_at"], envelope["ingested_at"]
            if record.fault is None and timing is Timing.PACED:
                delays.append((now - _parse_time(occurred_at)).total_seconds())
        published.append(
            PublishedRecord(
                record=record,
                value=value,
                scheduled_at=scheduled,
                published_at=now,
                occurred_at=occurred_at,
                ingested_at=ingested_at,
            )
        )
    return PublishManifest(
        overlay_version=OVERLAY_VERSION,
        base_digest=overlay.provenance.base_digest,
        timing=timing.value,
        anchor=begin,
        shift_s=(begin - base_start).total_seconds() if timing is Timing.PACED else None,
        records=tuple(published),
        max_schedule_slip_s=max_slip,
        fault_free_delay_at_handover_s=_distribution(delays),
    )
