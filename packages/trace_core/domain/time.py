"""Time handling: always timezone-aware UTC, with event time distinct from
processing time.

ADR-0026 names confusing event time with processing time as the single most
damaging shortcut available in this system: it silently corrupts every windowed
aggregate, and the corruption is invisible until someone recomputes by hand.

The two are therefore distinct types. `EventTime` is when the thing happened and
drives all windowing and watermarking; `ProcessingTime` is when we saw it and is
used only to measure lag. mypy rejects passing one where the other is expected,
so the mistake is caught at type-check time rather than in a Gold table.
"""

from __future__ import annotations

import datetime as dt
from typing import Final, NewType

from trace_core.domain.errors import NaiveDatetimeError

UTC: Final = dt.UTC

EventTime = NewType("EventTime", dt.datetime)
"""When the event actually occurred. Drives windowing and watermarks."""

ProcessingTime = NewType("ProcessingTime", dt.datetime)
"""When TRACE-X observed the event. Lag measurement only — never business logic."""


def require_aware_utc(value: dt.datetime, *, field: str = "datetime") -> dt.datetime:
    """Return `value` normalised to UTC, rejecting naive datetimes.

    A naive datetime is ambiguous, and the ambiguity resolves differently on a
    developer laptop and a UTC container — so the same code produces different
    windows in different places. Rejecting is the only safe option; guessing
    "probably UTC" is how the bug ships.
    """
    if not isinstance(value, dt.datetime):
        raise NaiveDatetimeError(f"{field} must be a datetime, got {type(value).__name__}")
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise NaiveDatetimeError(
            f"{field} is timezone-naive. Time is always timezone-aware UTC "
            f"(CLAUDE.md §6); an offset is never assumed."
        )
    return value.astimezone(UTC)


def event_time(value: dt.datetime) -> EventTime:
    """Tag an aware datetime as event time."""
    return EventTime(require_aware_utc(value, field="occurred_at"))


def processing_time(value: dt.datetime) -> ProcessingTime:
    """Tag an aware datetime as processing time."""
    return ProcessingTime(require_aware_utc(value, field="ingested_at"))


def utc_now() -> ProcessingTime:
    """Current processing time.

    Returns `ProcessingTime` on purpose: the wall clock can only ever tell you
    when you observed something. Event time comes from the event.
    """
    return ProcessingTime(dt.datetime.now(UTC))


_EPOCH: Final = dt.datetime(1970, 1, 1, tzinfo=UTC)
_ONE_MILLI: Final = dt.timedelta(milliseconds=1)


def to_millis(value: dt.datetime) -> int:
    """Whole milliseconds since the Unix epoch, UTC. Exact.

    Deliberately not `int(value.timestamp() * 1000)`. A float timestamp has
    ~15-16 significant digits, and epoch milliseconds already need 13, so the
    multiply leaves under three digits of headroom. Found by a property test:
    2038-02-01T00:00:00.022Z came back as .021, because the product was
    2148595200021.9998 and `int()` truncates.

    Milliseconds here become the UUIDv7 prefix and feed watermark arithmetic, so
    an off-by-one-millisecond is an off-by-one in a dedup key. `timedelta //
    timedelta` is integer division over integer-backed fields: exact, always.
    """
    return (require_aware_utc(value) - _EPOCH) // _ONE_MILLI


def from_millis(millis: int) -> dt.datetime:
    """An aware UTC datetime from Unix milliseconds. Exact, for the same reason."""
    return _EPOCH + dt.timedelta(milliseconds=millis)
