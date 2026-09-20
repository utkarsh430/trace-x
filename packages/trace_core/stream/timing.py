"""Timing semantics v1 (`docs/PHASE3_PLAN.md` §4.3; ADR-0053 §3).

Frozen before the first run that depends on them. Changing a value means a new version and an ADR,
never a response to a result.

- **Late.** Arrival delay is Kafka's LogAppendTime minus `occurred_at`. An event is late when that
  delay is strictly greater than 600 s. The value is inherited from the contract's 10-minute
  watermark, but the quantity is new: arrival delay includes producer buffering. As-served
  features, arrival skew and quarantine never read `is_late`.
- **Backfill.** A compressed replay marks every record with the backfill header. Its `occurred_at`
  is not shifted, so arrival delay says nothing about it, and `is_late` is null.
- **Future skew**, quarantined:
  - an event the gateway produced, when `occurred_at` is more than 86,400 s after the gateway's
    own receipt time (its envelope `ingested_at`, the clock its acceptance check used);
  - any other producer's event, when `occurred_at` is more than 86,400 s plus a 300 s clock margin
    after LogAppendTime.
- There is no watermarked dedup pre-filter in v1: exact uniqueness comes from ADR-0053 alone.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from typing import Final

from trace_core.domain.errors import NaiveDatetimeError
from trace_core.repositories.triage_event import PRODUCER_NAME as GATEWAY_PRODUCER

TIMING_SEMANTICS_VERSION: Final = 1
LATE_AFTER: Final = dt.timedelta(seconds=600)
"""Late when arrival delay is strictly greater than this."""
FUTURE_SKEW_LIMIT: Final = dt.timedelta(seconds=86_400)
OTHER_PRODUCER_CLOCK_MARGIN: Final = dt.timedelta(seconds=300)
"""Added for producers whose own receipt clock Silver cannot see: it covers broker clock skew."""
REPLAY_MODE_HEADER: Final = "trace-replay-mode"
BACKFILL_MODE: Final = b"backfill"
"""The header and value a compressed replay puts on every record (`eval/replay/faults.py`)."""

_MS: Final = dt.timedelta(milliseconds=1)


def _aware(name: str, moment: dt.datetime) -> dt.datetime:
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise NaiveDatetimeError(f"{name} must be timezone-aware, got {moment!r}")
    return moment


def arrival_delay(*, logged_at: dt.datetime, occurred_at: dt.datetime) -> dt.timedelta:
    """LogAppendTime minus `occurred_at`. Negative when the event claims a later time."""
    return _aware("logged_at", logged_at) - _aware("occurred_at", occurred_at)


def arrival_delay_ms(*, logged_at: dt.datetime, occurred_at: dt.datetime) -> int:
    """The arrival delay in whole milliseconds, floored, as Silver stores it."""
    return arrival_delay(logged_at=logged_at, occurred_at=occurred_at) // _MS


def is_backfill(headers: Iterable[tuple[str, bytes | None]]) -> bool:
    """Whether the record carries the backfill header with the backfill value."""
    return any(key == REPLAY_MODE_HEADER and value == BACKFILL_MODE for key, value in headers)


def is_late_ms(delay_ms: int, *, backfill: bool) -> bool | None:
    """Late when the arrival delay, in the whole milliseconds Silver stores, exceeds 600,000;
    None for a backfill record.

    Judged at millisecond precision, LogAppendTime's own, so a stored `is_late` and its stored
    `arrival_delay_ms` never disagree. Before this, a sub-millisecond `occurred_at` could make a
    600.0005 s delay late while storing 600,000 ms (critic finding C2). The threshold is unchanged,
    so this is still timing semantics v1.
    """
    if backfill:
        return None
    return delay_ms > LATE_AFTER // _MS


def is_late(*, logged_at: dt.datetime, occurred_at: dt.datetime, backfill: bool) -> bool | None:
    """True when arrival delay, in whole milliseconds, is strictly greater than 600,000 ms."""
    delay_ms = arrival_delay_ms(logged_at=logged_at, occurred_at=occurred_at)
    return is_late_ms(delay_ms, backfill=backfill)


def future_skewed(
    *,
    occurred_at: dt.datetime,
    producer: str,
    ingested_at: dt.datetime,
    logged_at: dt.datetime,
) -> bool:
    """Whether the event is too far in the future to admit (module docstring)."""
    occurred = _aware("occurred_at", occurred_at)
    received = _aware("ingested_at", ingested_at)
    arrived = _aware("logged_at", logged_at)
    if producer.split("@", 1)[0] == GATEWAY_PRODUCER:
        return occurred - received > FUTURE_SKEW_LIMIT
    return occurred - arrived > FUTURE_SKEW_LIMIT + OTHER_PRODUCER_CLOCK_MARGIN


__all__ = [
    "BACKFILL_MODE",
    "FUTURE_SKEW_LIMIT",
    "LATE_AFTER",
    "OTHER_PRODUCER_CLOCK_MARGIN",
    "REPLAY_MODE_HEADER",
    "TIMING_SEMANTICS_VERSION",
    "arrival_delay",
    "arrival_delay_ms",
    "future_skewed",
    "is_backfill",
    "is_late",
    "is_late_ms",
]
