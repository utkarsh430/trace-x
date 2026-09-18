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

    def observe(self, *, sent_ms: float, acked_ms: float, log_append_ms: float) -> OffsetBounds:
        if acked_ms < sent_ms:
            raise ValueError(f"acknowledged at {acked_ms} before sent at {sent_ms}")
        lower = log_append_ms - acked_ms
        upper = log_append_ms + 1 - sent_ms
        return OffsetBounds(
            lower if self.lower_ms is None else max(self.lower_ms, lower),
            upper if self.upper_ms is None else min(self.upper_ms, upper),
            self.samples + 1,
        )

    def merge(self, other: OffsetBounds) -> OffsetBounds:
        """Two sets of intervals over the same host clock intersect the same way."""
        lowers = [v for v in (self.lower_ms, other.lower_ms) if v is not None]
        uppers = [v for v in (self.upper_ms, other.upper_ms) if v is not None]
        return OffsetBounds(
            max(lowers) if lowers else None,
            min(uppers) if uppers else None,
            self.samples + other.samples,
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

    def within(self, tolerance_ms: float) -> bool:
        """The whole interval lies within +-tolerance: whatever the true offset, it is small."""
        return (
            self.consistent
            and self.lower_ms is not None
            and self.upper_ms is not None
            and -tolerance_ms <= self.lower_ms
            and self.upper_ms <= tolerance_ms
        )

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> OffsetBounds:
        lower, upper = record.get("lower_ms"), record.get("upper_ms")
        return cls(
            None if lower is None else float(lower),
            None if upper is None else float(upper),
            int(record.get("samples", 0)),
        )

    def as_record(self) -> dict[str, float | int | bool | None]:
        return {
            "lower_ms": self.lower_ms,
            "upper_ms": self.upper_ms,
            "midpoint_ms": self.midpoint_ms,
            "samples": self.samples,
            "consistent": self.consistent,
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
