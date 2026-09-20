"""The broker-to-host clock offset, bounded from delivery reports. Pure: no I/O.

Consumer lag subtracts a broker clock (LogAppendTime) from a host clock (the Silver commit), so
the offset between the two is recorded with every run (PHASE3_PLAN §4.3).

The broker stamps a record at append, which happens after the producer sent it and before the
producer's delivery callback ran. With `offset = broker clock - host clock`:

    sent_ms <= append (host clock) <= acked_ms
    log_append_ms <= append + offset < log_append_ms + 1
    =>   log_append_ms - acked_ms  <=  offset  <  log_append_ms + 1 - sent_ms

(LogAppendTime is truncated to the millisecond, hence the + 1.)

Every delivered record gives such an interval, whatever its batching or callback delay; the true
offset lies in all of them, so it lies in their intersection. The intersection narrows as records
accumulate. An empty intersection means one of the clocks stepped during the run, and the offset
is unknown.

**A record whose acknowledgement reads earlier than its send is discarded, not fatal.** Both are
host wall-clock readings, and a wall clock can step backwards -- an NTP correction mid-run put an
acknowledgement 93.5 ms before its send and killed a producer worker, which made a 17-minute run
INVALID on the harness rather than on the pipeline
(`bench-20260920-062507-stream-throughput-4231d90e`). The inequality the arithmetic above rests on
does not hold for that pair, so its interval says nothing and is dropped; `discarded` counts them,
and the run is judged on how many (`enough_measured`).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class OffsetBounds:
    lower_ms: float | None = None
    upper_ms: float | None = None
    samples: int = 0
    min_lower_ms: float | None = None
    """The smallest per-record lower bound: no record's offset can be below it."""
    max_upper_ms: float | None = None
    """The largest per-record upper bound: no record's offset can be above it.

    `lower_ms`/`upper_ms` intersect every record's interval, which assumes one constant offset for
    the whole run. Docker Desktop's VM clock drifts against the host, so the intersection can
    invert by a millisecond or two (720,784 reports, 2026-09-18) while every record is far inside
    the tolerance. The tolerance is judged on this envelope instead (`within`); the intersection
    is kept as a diagnostic (`drift_ms`)."""
    discarded: int = 0
    """Delivery reports whose host-clock readings inverted, so their interval said nothing.

    Last, and keyword-only in practice, so every positional construction in the harness and its
    tests keeps meaning what it meant."""

    def __post_init__(self) -> None:
        # Bounds built without an envelope (older records, direct construction) take the
        # intersection as their envelope, which is exactly what they claimed.
        if self.min_lower_ms is None and self.lower_ms is not None:
            object.__setattr__(self, "min_lower_ms", self.lower_ms)
        if self.max_upper_ms is None and self.upper_ms is not None:
            object.__setattr__(self, "max_upper_ms", self.upper_ms)

    def observe(self, *, sent_ms: float, acked_ms: float, log_append_ms: float) -> OffsetBounds:
        if acked_ms < sent_ms:
            # A host wall clock that stepped backwards between the send and the callback. The
            # inequality this arithmetic rests on does not hold for that pair, so its interval is
            # dropped rather than believed -- and counted, because a clock that steps often is
            # itself evidence against the run (`enough_measured`).
            return OffsetBounds(
                self.lower_ms,
                self.upper_ms,
                self.samples,
                self.min_lower_ms,
                self.max_upper_ms,
                self.discarded + 1,
            )
        lower = log_append_ms - acked_ms
        upper = log_append_ms + 1 - sent_ms
        return OffsetBounds(
            lower if self.lower_ms is None else max(self.lower_ms, lower),
            upper if self.upper_ms is None else min(self.upper_ms, upper),
            self.samples + 1,
            lower if self.min_lower_ms is None else min(self.min_lower_ms, lower),
            upper if self.max_upper_ms is None else max(self.max_upper_ms, upper),
            self.discarded,
        )

    def merge(self, other: OffsetBounds) -> OffsetBounds:
        """Two sets of intervals over the same host clock intersect the same way."""
        lowers = [v for v in (self.lower_ms, other.lower_ms) if v is not None]
        uppers = [v for v in (self.upper_ms, other.upper_ms) if v is not None]
        min_lowers = [v for v in (self.min_lower_ms, other.min_lower_ms) if v is not None]
        max_uppers = [v for v in (self.max_upper_ms, other.max_upper_ms) if v is not None]
        return OffsetBounds(
            max(lowers) if lowers else None,
            min(uppers) if uppers else None,
            self.samples + other.samples,
            min(min_lowers) if min_lowers else None,
            max(max_uppers) if max_uppers else None,
            self.discarded + other.discarded,
        )

    @property
    def consistent(self) -> bool:
        return (
            self.lower_ms is not None
            and self.upper_ms is not None
            and self.lower_ms <= self.upper_ms
        )

    @property
    def midpoint_ms(self) -> float | None:
        if not self.consistent or self.lower_ms is None or self.upper_ms is None:
            return None
        return (self.lower_ms + self.upper_ms) / 2

    @property
    def drift_ms(self) -> float | None:
        """How far the constant-offset intersection inverted (0 when consistent)."""
        if self.lower_ms is None or self.upper_ms is None:
            return None
        return max(0.0, self.lower_ms - self.upper_ms)

    def enough_measured(self, max_discarded_fraction: float) -> bool:
        """Whether few enough reports were discarded for the rest to bound the offset.

        A wall clock that steps once in a run costs a handful of reports out of millions. One that
        steps repeatedly leaves an offset nobody should trust, and the run is judged on that
        rather than on the survivors."""
        seen = self.samples + self.discarded
        return seen > 0 and self.discarded <= seen * max_discarded_fraction

    def within(self, tolerance_ms: float) -> bool:
        """Every record's offset interval lies within +-tolerance, whatever the drift between
        records: the envelope of the per-record intervals, not their intersection."""
        return (
            self.min_lower_ms is not None
            and self.max_upper_ms is not None
            and -tolerance_ms <= self.min_lower_ms
            and self.max_upper_ms <= tolerance_ms
        )

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> OffsetBounds:
        lower, upper = record.get("lower_ms"), record.get("upper_ms")
        min_lower, max_upper = record.get("min_lower_ms"), record.get("max_upper_ms")
        return cls(
            None if lower is None else float(lower),
            None if upper is None else float(upper),
            int(record.get("samples", 0)),
            None if min_lower is None else float(min_lower),
            None if max_upper is None else float(max_upper),
            int(record.get("discarded", 0)),
        )

    def as_record(self) -> dict[str, float | int | bool | None]:
        return {
            "lower_ms": self.lower_ms,
            "upper_ms": self.upper_ms,
            "midpoint_ms": self.midpoint_ms,
            "samples": self.samples,
            "consistent": self.consistent,
            "min_lower_ms": self.min_lower_ms,
            "max_upper_ms": self.max_upper_ms,
            "drift_ms": self.drift_ms,
            "discarded": self.discarded,
        }


def fleet_bounds(
    finals: Mapping[int, Mapping[str, Any]], streamed: Mapping[int, Mapping[str, Any]]
) -> OffsetBounds:
    """The fleet's bound: per worker, the most complete bound it sent -- its final report's, or
    the latest it streamed if the final is missing or covers fewer reports. A worker's bound only
    narrows as reports accumulate, so one worker's snapshots are never merged with each other
    (that would count its reports twice). Workers that sent neither contribute nothing; the count
    of reports says how much evidence there is."""
    bounds = OffsetBounds()
    for worker in sorted(set(finals) | set(streamed)):
        final = finals.get(worker)
        clock = final.get("clock") if final is not None else None
        candidates = [
            OffsetBounds.from_record(r) for r in (clock, streamed.get(worker)) if r is not None
        ]
        if candidates:
            bounds = bounds.merge(max(candidates, key=lambda b: b.samples))
    return bounds
