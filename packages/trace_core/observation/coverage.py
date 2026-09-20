"""The observation log's coverage rule: which writes the log vouches for (ADR-0051 §5; plan §4.1).

Given one read of the producer-session ledger and the observations read back from the log, the rule
decides what the log can vouch for and what is a gap. Bronze applies it (Step 5), and the chaos
tests apply it now, so the rule is written once.

**What a gap means.** A lost observation is one the online store recorded and the log does not
hold. Its *write time* is when the writer wrote the online store. Every lost write before
`Coverage.through` lies inside a reported gap of its own session, and nothing written at or after
`through` is vouched for at all. Every edge is widened by the clock margin, every gap has
`start <= end`, and `end is None` means the gap is open, because the session may still be writing.

**Premises.** The bounds are derived from these, and from nothing else:
1. *Serial writer.* One process writes a session, and handles one observation at a time. It assigns
   the number, stamps the record's `written_at` (its envelope `ingested_at`), writes the online
   store and produces the record, all before it starts the next. The gateway serialises on its
   event loop (ADR-0039). The order of steps within one observation does not matter; what matters is
   that observations never interleave. So for numbers `k < n < j`,
   `written_at(k) <= write(n) <= written_at(j)`.
2. *Lease.* A write happens only while the writer is ready, and only for the session that numbered
   it: within `lease_s` of the start of a heartbeat that committed before it, plus at most
   `takeover_margin_s` between the final readiness check and the store write (ADR-0051 §2). A
   session the same process acquires later never writes a predecessor's numbers.
3. *Clocks.* `written_at` is the writer's host clock, as the write is. Session times come from
   PostgreSQL, within `clock_margin_s` of it.
4. *Snapshot.* `ledger_read_at` is PostgreSQL's `now()` for the ledger read, so the read sees every
   session opened and every heartbeat committed before it.
5. *High-water mark.* `observed` holds every record the log has with an arrival (`logged_at`) at or
   before `log_read_through`, and none after it. A record not yet read is treated as lost.

**Why not arrival times.** A record's arrival does not bound when it was produced. It can wait in
the producer's buffer, and a produce request in flight to a frozen broker is appended when the
broker resumes, after the producer has already reported it failed. Bounds from `logged_at` minus
the delivery timeout would put such a loss outside its gap; the writer's own stamp cannot.

**The rule, per session in the ledger.**
- *A run of missing numbers* starts at the later of the session's start and the latest `written_at`
  of a number present below it. It ends at the earliest `written_at` of a number present above it,
  and for a closed session no later than the close.
- *Closed* (`closed_at` and `last_seq`): every number from 1 to `last_seq` must be present, and a
  missing tail ends at the close. A number above `last_seq` means a write after the close: an
  anomaly, and the session is judged as unclosed, with an open tail.
- *Unclosed:* the session certifies only its contiguous prefix. Its unknown tail starts as a run
  does, above the highest number present. A heartbeat committed after the ledger read is invisible
  to it, so the tail is bounded only when the read came after the writer's lease could have run
  out: `heartbeat_at + lease_s + takeover_margin_s + 2 * clock_margin_s < ledger_read_at`. It then
  ends at `heartbeat_at + lease_s + takeover_margin_s + clock_margin_s`. Otherwise, or when a
  record shows the writer outlived that bound, the tail is open.
- *Inconsistent times* contradict the premises. The bounds mix host stamps with PostgreSQL times,
  each within the clock margin of the other, so they may cross by up to twice the margin and still
  be consistent. A run whose start follows its end by more than that is an anomaly, and that run's
  gap starts at the session's start.

**Outside the ledger.**
- A session id the ledger does not hold is an open gap. It starts at the ledger read when the
  session could have opened after it. Otherwise the ledger should hold it: an anomaly, and the gap
  starts at its first record.
- A record with no usable session headers is a gap from its stamp to its arrival.
- A session opened after the ledger read whose every record was lost leaves nothing to see. All its
  writes fall at or after `through`, which is why `through` never passes the ledger read.

`Coverage.vouches(start, end)` is the question a reader asks: whether the log vouches for every
write in that span. It never does while any anomaly stands, because an anomaly means a premise the
bounds rest on does not hold.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from trace_core.domain.errors import TraceXError


class CoverageInputError(TraceXError):
    """An observation arrived after the high-water mark the log was said to be read through."""


@dataclass(frozen=True, slots=True)
class SessionRow:
    session_id: str
    started_at: dt.datetime
    heartbeat_at: dt.datetime
    closed_at: dt.datetime | None
    last_seq: int | None


@dataclass(frozen=True, slots=True)
class Observed:
    """One observation read back from the log."""

    session_id: str | None
    seq: int | None
    logged_at: dt.datetime
    """Its arrival: the broker's LogAppendTime."""
    written_at: dt.datetime
    """The writer's own stamp, taken while handling it: the envelope's `ingested_at`."""


@dataclass(frozen=True, slots=True)
class Gap:
    """A span of write time the log cannot vouch for."""

    session_id: str | None
    start: dt.datetime
    end: dt.datetime | None
    """None: the gap is open, because the session may still be writing."""
    missing: tuple[int, ...] = ()
    """The sequence numbers known to be missing inside it; empty when the extent is unknown."""

    def contains(self, moment: dt.datetime) -> bool:
        return self.start <= moment and (self.end is None or moment <= self.end)

    def overlaps(self, start: dt.datetime, end: dt.datetime) -> bool:
        return self.start <= end and (self.end is None or start <= self.end)


@dataclass(frozen=True, slots=True)
class SessionCoverage:
    session_id: str
    closed: bool
    """Whether the ledger row is closed. A closed row with an anomaly is judged as unclosed."""
    certified_through: int
    """Every number from 1 to this one is present, and the session vouches for them."""
    gaps: tuple[Gap, ...]
    anomalies: tuple[str, ...] = ()
    """What contradicts the premises: a write after the close, or times no serial writer stamps."""

    @property
    def covered(self) -> bool:
        return not self.gaps and not self.anomalies


@dataclass(frozen=True, slots=True)
class Coverage:
    sessions: Mapping[str, SessionCoverage]
    unknown: tuple[Gap, ...]
    through: dt.datetime
    """Nothing written at or after this time is vouched for, whatever the gaps say."""
    anomalies: tuple[str, ...] = ()

    @property
    def gaps(self) -> tuple[Gap, ...]:
        return (*self.unknown, *(gap for s in self.sessions.values() for gap in s.gaps))

    @property
    def consistent(self) -> bool:
        """No anomaly anywhere: every premise the bounds rest on held for what was read."""
        return not self.anomalies and all(not s.anomalies for s in self.sessions.values())

    @property
    def gap_free(self) -> bool:
        """No gap and no anomaly anywhere. Says nothing about writes at or after `through`."""
        return not self.gaps and self.consistent

    def vouches(self, start: dt.datetime, end: dt.datetime) -> bool:
        """Whether the log vouches for every write in `[start, end]`: never with an anomaly."""
        if start > end or end >= self.through or not self.consistent:
            return False
        return not any(gap.overlaps(start, end) for gap in self.gaps)


def _prefix(present: list[int]) -> int:
    prefix = 0
    for number in present:
        if number != prefix + 1:
            break
        prefix = number
    return prefix


def _assess_session(
    row: SessionRow,
    stamps: Mapping[int, dt.datetime],
    *,
    margin: dt.timedelta,
    fence: dt.timedelta,
    ledger_read_at: dt.datetime,
    conflicts: set[int],
) -> SessionCoverage:
    present = sorted(stamps)
    seen_max = present[-1] if present else 0
    anomalies: list[str] = [
        f"sequence {number} carries two different stamps" for number in sorted(conflicts)
    ]
    closed = row.closed_at is not None
    if closed and row.last_seq is None:
        anomalies.append("closed without a last_seq")
    elif closed and row.last_seq is not None and seen_max > row.last_seq:
        anomalies.append(
            f"sequence {seen_max} is above last_seq {row.last_seq}: a write after the close"
        )
    close_bound = row.closed_at if closed and not anomalies else None

    # after[i]: every write numbered above present[i] happened at or after it (premise 1).
    after: list[dt.datetime] = []
    latest = row.started_at
    for number in present:
        latest = max(latest, stamps[number])
        after.append(latest)
    # before[i]: every write numbered below present[i] happened at or before it (premise 1).
    before = [stamps[number] for number in present]
    for i in range(len(before) - 2, -1, -1):
        before[i] = min(before[i], before[i + 1])

    gaps: list[Gap] = []

    def run(first: int, last: int, lower: dt.datetime, upper: dt.datetime) -> None:
        # Host stamps against PostgreSQL times: consistent while they cross by at most two margins.
        if lower - margin > upper + margin:
            anomalies.append(
                f"numbers {first}-{last}: no serial writer stamps these times, so the gap starts "
                f"at the session's start"
            )
            lower = min(row.started_at, upper)
        missing = tuple(range(first, last + 1))
        gaps.append(Gap(row.session_id, lower - margin, upper + margin, missing))

    previous = 0
    for i, number in enumerate(present):
        if number > previous + 1:
            lower = after[i - 1] if i else row.started_at
            upper = before[i] if close_bound is None else min(before[i], close_bound)
            run(previous + 1, number - 1, lower, upper)
        previous = number
    tail_lower = after[-1] if present else row.started_at

    if close_bound is not None and row.last_seq is not None:
        if row.last_seq > seen_max:
            run(seen_max + 1, row.last_seq, tail_lower, close_bound)
        certified = row.last_seq if not gaps else _prefix(present)
    else:
        end: dt.datetime | None = None
        lease_ran_out = row.heartbeat_at + fence + 2 * margin < ledger_read_at
        if lease_ran_out and not anomalies:
            bound = row.heartbeat_at + fence + margin
            if tail_lower - margin <= bound:
                end = bound
        gaps.append(Gap(row.session_id, tail_lower - margin, end, ()))
        certified = _prefix(present)
    return SessionCoverage(
        session_id=row.session_id,
        closed=closed,
        certified_through=certified,
        gaps=tuple(gaps),
        anomalies=tuple(anomalies),
    )


def assess(
    ledger: Iterable[SessionRow],
    observed: Iterable[Observed],
    *,
    ledger_read_at: dt.datetime,
    log_read_through: dt.datetime,
    lease_s: float,
    takeover_margin_s: float,
    clock_margin_s: float,
) -> Coverage:
    """Apply the coverage rule to one ledger read and the log read through its high-water mark.

    Raises `CoverageInputError` for an observation that arrived after `log_read_through`.
    """
    margin = dt.timedelta(seconds=clock_margin_s)
    rows = {row.session_id: row for row in ledger}
    stamps: dict[str, dict[int, dt.datetime]] = {session_id: {} for session_id in rows}
    strangers: dict[str, list[dt.datetime]] = {}
    conflicts: dict[str, set[int]] = {}
    unknown: list[Gap] = []
    anomalies: list[str] = []
    for item in observed:
        if item.logged_at > log_read_through:
            raise CoverageInputError(
                f"an observation arrived at {item.logged_at.isoformat()}, after the log's "
                f"high-water mark {log_read_through.isoformat()}"
            )
        if item.session_id is not None and item.session_id not in rows:
            strangers.setdefault(item.session_id, []).append(item.written_at)
            continue
        if item.session_id is None or item.seq is None or item.seq < 1:
            low, high = sorted((item.written_at, item.logged_at))
            unknown.append(Gap(item.session_id, low - margin, high + margin))
            continue
        session = stamps[item.session_id]
        known = session.get(item.seq)
        if known is not None and known != item.written_at:
            # A redelivery carries its record's own stamp; two stamps mean two records under one
            # number. The earlier is kept, which widens both bounds.
            conflicts.setdefault(item.session_id, set()).add(item.seq)
        session[item.seq] = item.written_at if known is None else min(known, item.written_at)

    for session_id, written in sorted(strangers.items()):
        first = min(written)
        if first + margin < ledger_read_at:
            anomalies.append(f"session {session_id} is in the log but not in the ledger")
        unknown.append(Gap(session_id, min(ledger_read_at, first) - margin, None))

    fence = dt.timedelta(seconds=lease_s + takeover_margin_s)
    sessions = {
        session_id: _assess_session(
            row,
            stamps[session_id],
            margin=margin,
            fence=fence,
            ledger_read_at=ledger_read_at,
            conflicts=conflicts.get(session_id, set()),
        )
        for session_id, row in rows.items()
    }
    return Coverage(
        sessions=MappingProxyType(sessions),
        unknown=tuple(unknown),
        through=min(log_read_through, ledger_read_at) - margin,
        anomalies=tuple(anomalies),
    )


__all__ = [
    "Coverage",
    "CoverageInputError",
    "Gap",
    "Observed",
    "SessionCoverage",
    "SessionRow",
    "assess",
]
