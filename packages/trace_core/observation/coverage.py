"""The observation log's coverage rule: which spans the log vouches for (ADR-0051 §5; plan §4.1).

Given the producer-session ledger and the observations read back from the log, the rule decides
what the log can vouch for and what is a gap. Bronze applies it (Step 5), and the chaos tests apply
it now, so the rule is written once.

Per session:
- **Closed.** Covered when every sequence number from 1 to `max(seen, last_seq)` is present. A run
  of missing numbers is a gap between the observations either side of it. A missing tail is a gap
  from the last present observation to the close.
- **Unclosed.** The session certifies only its contiguous prefix. Everything after it is one gap:
  from the prefix's last observation, or the session's start, to its last heartbeat plus the
  heartbeat interval, the producer's delivery timeout and the clock margin. After that point the
  fenced process could no longer write or produce, so the gap is bounded.
- **Unknown.** An observation that names a session the ledger does not hold, or carries no usable
  session headers, is a gap at its own time.

Session times are the database clock and observation times are the broker's LogAppendTime. Every gap
edge is widened by the declared clock margin between them.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType


@dataclass(frozen=True, slots=True)
class SessionRow:
    session_id: str
    started_at: dt.datetime
    heartbeat_at: dt.datetime
    closed_at: dt.datetime | None
    last_seq: int | None


@dataclass(frozen=True, slots=True)
class Observed:
    """One observation read back from the log: its session headers and its arrival time."""

    session_id: str | None
    seq: int | None
    logged_at: dt.datetime


@dataclass(frozen=True, slots=True)
class Gap:
    """A span of arrival time the log cannot vouch for."""

    session_id: str | None
    start: dt.datetime
    end: dt.datetime
    missing: tuple[int, ...] = ()
    """The sequence numbers known to be missing inside it; empty when the extent is unknown."""


@dataclass(frozen=True, slots=True)
class SessionCoverage:
    session_id: str
    closed: bool
    certified_through: int
    """Every number from 1 to this one is present, and the session vouches for them."""
    gaps: tuple[Gap, ...]

    @property
    def covered(self) -> bool:
        return not self.gaps


@dataclass(frozen=True, slots=True)
class Coverage:
    sessions: Mapping[str, SessionCoverage]
    unknown: tuple[Gap, ...]

    @property
    def gaps(self) -> tuple[Gap, ...]:
        return (*self.unknown, *(gap for s in self.sessions.values() for gap in s.gaps))

    @property
    def gap_free(self) -> bool:
        return not self.gaps


def _runs(numbers: list[int]) -> list[tuple[int, int]]:
    runs: list[tuple[int, int]] = []
    for number in numbers:
        if runs and runs[-1][1] == number - 1:
            runs[-1] = (runs[-1][0], number)
        else:
            runs.append((number, number))
    return runs


def assess(
    ledger: Iterable[SessionRow],
    observed: Iterable[Observed],
    *,
    heartbeat_interval_s: float,
    delivery_timeout_s: float,
    clock_margin_s: float,
) -> Coverage:
    """Apply the coverage rule to a ledger and the observations read back from the log."""
    margin = dt.timedelta(seconds=clock_margin_s)
    rows = {row.session_id: row for row in ledger}
    arrivals: dict[str, dict[int, dt.datetime]] = {session_id: {} for session_id in rows}
    unknown: list[Gap] = []
    for item in observed:
        if item.session_id is None or item.session_id not in rows or not item.seq or item.seq < 1:
            unknown.append(Gap(item.session_id, item.logged_at - margin, item.logged_at + margin))
            continue
        times = arrivals[item.session_id]
        earliest = times.get(item.seq)
        times[item.seq] = item.logged_at if earliest is None else min(earliest, item.logged_at)

    sessions: dict[str, SessionCoverage] = {}
    for session_id, row in rows.items():
        times = arrivals[session_id]
        prefix = 0
        while prefix + 1 in times:
            prefix += 1
        seen_max = max(times, default=0)

        def at(
            seq: int, fallback: dt.datetime, times: dict[int, dt.datetime] = times
        ) -> dt.datetime:
            return times.get(seq, fallback)

        gaps: list[Gap] = []
        if row.closed_at is not None:
            upper = max(seen_max, row.last_seq or 0)
            missing = [n for n in range(1, upper + 1) if n not in times]
            for first, last in _runs(missing):
                start = at(first - 1, row.started_at)
                end = at(last + 1, row.closed_at)
                gaps.append(
                    Gap(session_id, start - margin, end + margin, tuple(range(first, last + 1)))
                )
            certified = prefix if missing else upper
        else:
            bound = row.heartbeat_at + dt.timedelta(
                seconds=heartbeat_interval_s + delivery_timeout_s
            )
            start = at(prefix, row.started_at) if prefix else row.started_at
            known_missing = tuple(n for n in range(prefix + 1, seen_max + 1) if n not in times)
            gaps.append(Gap(session_id, start - margin, bound + margin, known_missing))
            certified = prefix
        sessions[session_id] = SessionCoverage(
            session_id=session_id,
            closed=row.closed_at is not None,
            certified_through=certified,
            gaps=tuple(gaps),
        )
    return Coverage(sessions=MappingProxyType(sessions), unknown=tuple(unknown))


__all__ = ["Coverage", "Gap", "Observed", "SessionCoverage", "SessionRow", "assess"]
