"""Whether feature history is complete over a horizon: both durable paths must vouch (ADR-0051 §5).

Online state has two durable histories, and history-derived state may claim COMPLETE only where
both hold:
1. **Writer-session observation coverage** (`trace_core.observation.coverage`). The coverage must
   vouch for every write in the horizon: the horizon ends before `Coverage.through`, no gap,
   including an open one, overlaps it, and no anomaly was found. An anomaly contradicts the
   premises the gaps rest on, so it keeps every horizon incomplete.
2. **Authorization-outbox delivery coverage** (`trace_core.observation.outbox_watermark`). Every
   authorization outcome recorded before the horizon's end must have a confirmed delivery.

Anything else is INCOMPLETE, with every reason that applies, so an unresolved gap on either path
keeps the horizon incomplete. Times are write and record times on the host and database clocks;
the margin covers their offset. Mapping a horizon to event time is plan §4.1 point 6, applied by
the caller.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from enum import StrEnum

from trace_core.observation.coverage import Coverage


class HistoryCompleteness(StrEnum):
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"


@dataclass(frozen=True, slots=True)
class HistoryVerdict:
    completeness: HistoryCompleteness
    reasons: tuple[str, ...]

    @property
    def complete(self) -> bool:
        return self.completeness is HistoryCompleteness.COMPLETE


def assess_history(
    *,
    horizon_start: dt.datetime,
    horizon_end: dt.datetime,
    observation_coverage: Coverage,
    authorization_delivered_through: dt.datetime | None,
    clock_margin_s: float,
) -> HistoryVerdict:
    """COMPLETE only when both durable paths vouch for the whole horizon."""
    if horizon_end < horizon_start:
        raise ValueError(f"horizon ends at {horizon_end} before it starts at {horizon_start}")
    margin = dt.timedelta(seconds=clock_margin_s)
    coverage = observation_coverage
    reasons: list[str] = []
    if horizon_end >= coverage.through:
        reasons.append(f"the observation log vouches for nothing at or after {coverage.through}")
    for gap in coverage.gaps:
        if gap.overlaps(horizon_start, horizon_end):
            end = "open" if gap.end is None else str(gap.end)
            reasons.append(
                f"observation gap {gap.start}..{end} (session {gap.session_id}) overlaps"
            )
    anomalies = [
        *coverage.anomalies,
        *(anomaly for session in coverage.sessions.values() for anomaly in session.anomalies),
    ]
    reasons.extend(f"observation coverage anomaly: {anomaly}" for anomaly in anomalies)
    if authorization_delivered_through is None:
        reasons.append("authorization outcomes have no delivery watermark")
    elif authorization_delivered_through - margin < horizon_end:
        reasons.append(
            f"authorization outcomes are delivered only through {authorization_delivered_through}"
        )
    return HistoryVerdict(
        completeness=HistoryCompleteness.INCOMPLETE if reasons else HistoryCompleteness.COMPLETE,
        reasons=tuple(reasons),
    )


__all__ = ["HistoryCompleteness", "HistoryVerdict", "assess_history"]
