"""Pass, fail or invalid. Pure: every input is a measurement, every output a recorded verdict.

Two kinds of verdict, never mixed:

- **integrity**: did the run measure what it claims? The rate was offered, the clocks agree, the
  samples are anchored to records the broker actually held. Any failure makes the run INVALID: its
  numbers are kept as evidence and never published as a pass *or* a fail of the target.
- **target**: the ROADMAP Phase 3 targets and PHASE3_PLAN §4.3 Gold freshness, against the frozen
  values in `spec`. Any failure makes the run FAIL, recorded and published as found.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from benchmarks.stream_throughput.clock import OffsetBounds
from benchmarks.stream_throughput.lag import (
    LagSample,
    PartitionKey,
    Recovery,
    WindowStats,
    percentile,
)
from benchmarks.stream_throughput.spec import (
    CLOCK_OFFSET_TOLERANCE_MS,
    GOLD_MAX_LAG_MS,
    GOLD_P95_LAG_MS,
    GOLD_STALL_MS,
    LAG_TARGET_MS,
    MAX_SCHEDULE_LAG_S,
    MIN_SAMPLES_PER_WINDOW,
    OFFERED_RATE_TOLERANCE,
    OUTAGE_S,
    TARGET_RATE_EVENTS_PER_S,
    THROUGHPUT_QUANTISATION_TOLERANCE,
)

INTEGRITY: Final = "integrity"
TARGET: Final = "target"
OUTAGE_TIMING_TOLERANCE_S: Final = 1.0
SAMPLE_TIME_SLACK_MS: Final = 60_000


class Status(StrEnum):
    PASS = "PASS"  # noqa: S105 -- a verdict, not a credential
    FAIL = "FAIL"
    INVALID = "INVALID"


@dataclass(frozen=True, slots=True)
class Verdict:
    name: str
    kind: str
    passed: bool
    detail: str


def overall(verdicts: Sequence[Verdict]) -> Status:
    """INVALID if any integrity check failed; else FAIL if any target failed; else PASS. An empty
    list is INVALID: a run that checked nothing proved nothing."""
    if not verdicts or any(v.kind == INTEGRITY and not v.passed for v in verdicts):
        return Status.INVALID
    if not any(v.kind == TARGET for v in verdicts):
        return Status.INVALID
    if any(v.kind == TARGET and not v.passed for v in verdicts):
        return Status.FAIL
    return Status.PASS


# ------------------------------------------------------------------- offered ---


@dataclass(frozen=True, slots=True)
class OfferedLoad:
    """What the producers offered over a window: the rate over the whole one-second buckets inside
    it, and the furthest any worker ever fell behind its schedule."""

    rate_events_per_s: float | None
    buckets: int
    max_behind_s: float


def offered_load(
    buckets: Mapping[float, int],
    behind: Mapping[float, float],
    start_s: float,
    end_s: float,
    *,
    bucket_s: float = 1.0,
) -> OfferedLoad:
    """`buckets` maps a bucket's start (epoch seconds) to the events produced in it, summed over
    workers; only buckets wholly inside `[start_s, end_s)` count."""
    inside = [b for b in buckets if start_s <= b and b + bucket_s <= end_s]
    total = sum(buckets[b] for b in inside)
    rate = total / (len(inside) * bucket_s) if inside else None
    lagging = [v for b, v in behind.items() if start_s <= b < end_s]
    return OfferedLoad(rate, len(inside), max(lagging, default=0.0))


def offered_verdicts(phase: str, offered: OfferedLoad, rate: int) -> list[Verdict]:
    floor = rate * (1 - OFFERED_RATE_TOLERANCE)
    measured = offered.rate_events_per_s
    return [
        Verdict(
            f"{phase}_offered_rate",
            INTEGRITY,
            measured is not None and measured >= floor,
            f"offered {measured if measured is None else round(measured, 1)} events/s over "
            f"{offered.buckets} one-second buckets, against the fixed rate {rate} "
            f"(at least {floor:.0f} required for the run to have offered it)",
        ),
        Verdict(
            f"{phase}_schedule_kept",
            INTEGRITY,
            offered.max_behind_s <= MAX_SCHEDULE_LAG_S,
            f"the producers were at most {offered.max_behind_s:.3f} s behind schedule "
            f"(limit {MAX_SCHEDULE_LAG_S} s)",
        ),
    ]


# ---------------------------------------------------------------- throughput ---


def throughput_verdicts(stats: WindowStats) -> list[Verdict]:
    floor = TARGET_RATE_EVENTS_PER_S * (1 - THROUGHPUT_QUANTISATION_TOLERANCE)
    rate = stats.throughput_events_per_s
    lag_ok = (
        stats.undefined_samples == 0
        and stats.lag_max_ms is not None
        and stats.lag_max_ms < LAG_TARGET_MS
        and stats.widest_gap_ms is not None
        and stats.widest_gap_ms < LAG_TARGET_MS
    )
    # Few or no samples are not an integrity problem to hide behind INVALID: a consumer that
    # commits rarely leaves gaps, and a gap of the target or more fails the lag verdict below.
    return [
        Verdict(
            "throughput_sustained",
            TARGET,
            rate is not None and rate >= floor,
            f"Kafka -> Silver committed {rate if rate is None else round(rate, 1)} events/s "
            f"({stats.committed_records} records over {stats.committed_span_ms} ms); target "
            f"{TARGET_RATE_EVENTS_PER_S} events/s, at least {floor:.0f} after the declared "
            f"{THROUGHPUT_QUANTISATION_TOLERANCE:.0%} commit-quantisation tolerance",
        ),
        Verdict(
            "throughput_lag_below_target",
            TARGET,
            lag_ok,
            f"{stats.samples} Silver commits sampled; consumer lag max {stats.lag_max_ms} ms, "
            f"p99 {stats.lag_p99_ms} ms, widest commit gap "
            f"{stats.widest_gap_ms} ms, {stats.undefined_samples} undefined sample(s) "
            f"(missing: {list(stats.missing_partitions)}); every sample and every gap must be "
            f"below {LAG_TARGET_MS} ms for the rate to count as sustained",
        ),
    ]


# -------------------------------------------------------------------- outage ---


@dataclass(frozen=True, slots=True)
class OutageTiming:
    """The outage as it happened, on the host clock.

    The stop is *signalled* first; the consumer process has *exited* only once its queries have
    stopped (up to `consumer_stop_timeout_s`, plus a kill), and Silver may keep committing until
    then. So the outage runs from the exit to the *restart*, never from the signal."""

    signalled_ms: int
    exited_ms: int
    restarted_ms: int

    @property
    def stop_s(self) -> float:
        """How long the consumer took to exit after it was signalled."""
        return (self.exited_ms - self.signalled_ms) / 1000

    @property
    def down_s(self) -> float:
        """How long no consumer process existed: the outage."""
        return (self.restarted_ms - self.exited_ms) / 1000

    def as_record(self) -> dict[str, float | int]:
        return {
            "signalled_at_ms": self.signalled_ms,
            "exited_at_ms": self.exited_ms,
            "restarted_at_ms": self.restarted_ms,
            "stop_s": self.stop_s,
            "consumer_down_s": self.down_s,
        }


def restart_due_ms(exited_ms: int) -> int:
    """When the consumer is restarted: `OUTAGE_S` after its process actually exited."""
    return exited_ms + OUTAGE_S * 1000


def commits_while_down(timing: OutageTiming, commit_ms: Iterable[int]) -> list[int]:
    """Silver commit times strictly inside (exited, restarted): none can exist if the consumer was
    really down, because no process was left to write them."""
    return sorted(t for t in commit_ms if timing.exited_ms < t < timing.restarted_ms)


def outage_timing_verdicts(timing: OutageTiming, silver_commit_ms: Iterable[int]) -> list[Verdict]:
    """Integrity of the outage itself: it lasted `OUTAGE_S` from the consumer's real exit, and
    Silver did not advance while the consumer was supposedly down."""
    ordered = timing.signalled_ms <= timing.exited_ms <= timing.restarted_ms
    inside = commits_while_down(timing, silver_commit_ms)
    return [
        Verdict(
            "outage_duration",
            INTEGRITY,
            ordered and abs(timing.down_s - OUTAGE_S) <= OUTAGE_TIMING_TOLERANCE_S,
            f"the consumer exited {timing.stop_s:.3f} s after the stop was signalled and was "
            f"restarted {timing.down_s:.3f} s after it exited; the outage is {OUTAGE_S} s "
            f"(+-{OUTAGE_TIMING_TOLERANCE_S} s), measured from the exit"
            + ("" if ordered else "; the signal, exit and restart times are out of order"),
        ),
        Verdict(
            "outage_consumer_down",
            INTEGRITY,
            not inside,
            (
                f"{len(inside)} Silver commit(s) inside the down window "
                f"({timing.exited_ms}, {timing.restarted_ms}) ms, e.g. {inside[:3]}: the "
                f"consumer was not down, so no outage was measured"
            )
            if inside
            else "no Silver commit between the consumer's exit and its restart",
        ),
    ]


def outage_verdicts(
    recovery: Recovery | None,
    *,
    timing: OutageTiming | None,
    silver_commit_ms: Iterable[int],
    consumer_failure: str | None,
    bound_s: int,
) -> list[Verdict]:
    verdicts: list[Verdict] = []
    if timing is not None:
        # Only an outage that happened can have the wrong length. One that never happened, because
        # the consumer had already failed, is that failure's target verdict below.
        verdicts.extend(outage_timing_verdicts(timing, silver_commit_ms))
    if consumer_failure is not None:
        verdicts.append(Verdict("lag_recovers_after_outage", TARGET, False, consumer_failure))
        return verdicts
    if recovery is None:
        verdicts.append(
            Verdict(
                "lag_recovers_after_outage",
                TARGET,
                False,
                "the outage phase did not complete, so recovery was never observed",
            )
        )
        return verdicts
    if recovery.recovery_ms is None:
        detail = (
            f"consumer lag did not recover below {LAG_TARGET_MS} ms within the observed "
            f"{(recovery.observed_until_ms - recovery.restored_ms) / 1000:.0f} s after the "
            f"restart (peak {recovery.peak_lag_ms} ms, {recovery.samples_after_restore} samples)"
        )
    else:
        detail = (
            f"consumer lag recovered below {LAG_TARGET_MS} ms {recovery.recovery_ms / 1000:.1f} s "
            f"after the restart and held; peak {recovery.peak_lag_ms} ms; bound {bound_s} s"
        )
    verdicts.append(Verdict("lag_recovers_after_outage", TARGET, recovery.within_bound, detail))
    return verdicts


# ---------------------------------------------------------------------- Gold ---


@dataclass(frozen=True, slots=True)
class GoldBuild:
    started_ms: int
    finished_ms: int | None
    """None when the harness gave up waiting (`abandoned_ms`)."""
    exit_code: int | None
    lag_ms: int | None
    """The build's own record (`gold.builds.lag_ms`): its finish minus the newest Silver commit
    it pinned (PHASE3_PLAN §4.3)."""
    build_id: int | None = None
    abandoned_ms: int | None = None

    @property
    def succeeded(self) -> bool:
        return self.exit_code == 0 and self.lag_ms is not None and self.finished_ms is not None

    @property
    def lag_lower_bound_ms(self) -> int | None:
        """An unfinished build pinned Silver before it started, so its lag is at least its age."""
        if self.finished_ms is None and self.abandoned_ms is not None:
            return self.abandoned_ms - self.started_ms
        return None


def gold_relevant(builds: Iterable[GoldBuild], start_ms: int, end_ms: int) -> list[GoldBuild]:
    """Builds that finished inside the window, or were running when it ended."""
    chosen = []
    for build in builds:
        end = build.finished_ms if build.finished_ms is not None else build.abandoned_ms
        finished_inside = end is not None and start_ms <= end <= end_ms
        running_at_end = build.started_ms <= end_ms and (end is None or end > end_ms)
        if finished_inside or running_at_end:
            chosen.append(build)
    return chosen


def gold_verdicts(
    builds: Sequence[GoldBuild],
    *,
    enabled: bool,
    loop_started_ms: int | None,
    start_ms: int,
    end_ms: int,
    evaluated_until_ms: int,
    silver_advanced: bool,
) -> list[Verdict]:
    if not enabled or loop_started_ms is None:
        return [
            Verdict(
                "gold_freshness",
                TARGET,
                False,
                "Gold was not run, so the Gold freshness target is unevaluated",
            )
        ]
    relevant = gold_relevant(builds, start_ms, end_ms)
    failed = [b for b in relevant if b.finished_ms is not None and not b.succeeded]
    lags = [b.lag_ms for b in relevant if b.succeeded and b.lag_ms is not None]
    bounds = [b.lag_lower_bound_ms for b in relevant if b.lag_lower_bound_ms is not None]
    worst = max([*lags, *bounds], default=None)
    p95 = percentile(lags, 0.95) if lags else None

    finishes = sorted(b.finished_ms for b in builds if b.succeeded and b.finished_ms is not None)
    before = [f for f in finishes if f <= start_ms]
    anchor = before[-1] if before else loop_started_ms
    inside = [f for f in finishes if start_ms < f <= end_ms]
    after = [f for f in finishes if f > end_ms]
    closing = after[0] if after else evaluated_until_ms
    points = [anchor, *inside, closing]
    stall = max(b - a for a, b in itertools.pairwise(points))
    return [
        Verdict(
            "gold_builds_succeeded",
            TARGET,
            not failed and bool(lags),
            f"{len(relevant)} Gold build(s) finished in or spanned the measured window, "
            f"{len(lags)} succeeded, {len(failed)} failed (exit codes "
            f"{[b.exit_code for b in failed]})",
        ),
        Verdict(
            "gold_p95_lag",
            TARGET,
            p95 is not None and p95 <= GOLD_P95_LAG_MS and not bounds,
            f"p95 Gold build lag {p95} ms over {len(lags)} build(s); target {GOLD_P95_LAG_MS} ms"
            + (f"; unfinished build(s) at least {bounds} ms" if bounds else ""),
        ),
        Verdict(
            "gold_max_lag",
            TARGET,
            worst is not None and worst <= GOLD_MAX_LAG_MS,
            f"worst Gold build lag {worst} ms; target {GOLD_MAX_LAG_MS} ms",
        ),
        Verdict(
            "gold_no_stall",
            TARGET,
            not (silver_advanced and stall >= GOLD_STALL_MS),
            f"longest interval without a Gold build committing, around the window: {stall} ms "
            f"while Silver {'advanced' if silver_advanced else 'did not advance'}; a stall is "
            f"{GOLD_STALL_MS} ms",
        ),
    ]


# -------------------------------------------------------------- integrity ---


def clock_verdict(bounds: OffsetBounds, *, required: bool = True) -> Verdict:
    """The offset must be bounded within tolerance whenever a lag the run judges depends on it
    (`required`: a completed measured window, or an observed recovery). When nothing judged
    depends on it and no delivery report arrived, there is nothing to bound and nothing it could
    invalidate. Any report that did arrive is still judged: a clock outside tolerance or one that
    stepped is evidence against the whole run, needed or not."""
    if not required and bounds.samples == 0:
        return Verdict(
            "clock_offset_bounded",
            INTEGRITY,
            True,
            "not required: no lag measurement this run judges depends on the broker-to-host "
            "clock offset, and no delivery report bounded it (0 reports)",
        )
    return Verdict(
        "clock_offset_bounded",
        INTEGRITY,
        bounds.within(CLOCK_OFFSET_TOLERANCE_MS),
        f"broker-to-host clock offset in [{bounds.lower_ms}, {bounds.upper_ms}] ms from "
        f"{bounds.samples} delivery reports (consistent: {bounds.consistent}); must lie within "
        f"+-{CLOCK_OFFSET_TOLERANCE_MS} ms",
    )


def delivery_verdicts(
    *,
    problems: Sequence[str],
    not_log_append_time: int,
    delivered_partitions: Iterable[PartitionKey],
    expected: Iterable[PartitionKey],
    worker_errors: Sequence[str],
) -> list[Verdict]:
    uncovered = sorted(set(expected) - set(delivered_partitions))
    return [
        Verdict(
            "producers_healthy",
            INTEGRITY,
            not worker_errors,
            f"{len(worker_errors)} producer worker error(s): {list(worker_errors)[:3]}",
        ),
        Verdict(
            "deliveries_confirmed",
            INTEGRITY,
            not problems,
            f"delivery problems: {list(problems)[:5]}" if problems else "every record delivered",
        ),
        Verdict(
            "log_append_time",
            INTEGRITY,
            not_log_append_time == 0,
            f"{not_log_append_time} record(s) stamped with something other than LogAppendTime",
        ),
        Verdict(
            "partitions_offered",
            INTEGRITY,
            not uncovered,
            f"partitions that received no benchmark record: {[str(p) for p in uncovered]}",
        ),
    ]


def authenticity_verdicts(
    samples: Sequence[LagSample],
    *,
    committed_offsets: Mapping[PartitionKey, int],
    committed_newest: Mapping[PartitionKey, int],
    broker_end_offsets: Mapping[PartitionKey, int],
    run_started_ms: int,
    run_finished_ms: int,
    consumer_failed: bool = False,
) -> list[Verdict]:
    """The samples must describe records the broker actually held during this run.

    When the consumer failed (`consumer_failed`), coverage cannot be expected of it -- it may die
    before its first commit, or before every partition committed -- so "no sample" and "a
    partition never or only stale-committed" are recorded in the detail but do not fail the check;
    that failure is the consumer's, and a target verdict already says so. Everything that detects
    a *fabricated* sample (outside the run, before the broker stamp, beyond the broker's end
    offset, identical values) is judged exactly as always.

    A lag series cannot be fabricated past these: every committed offset must be below the
    broker's end offset for its partition; every partition must have committed a record appended
    during the run; no sample may fall outside the run, or show a record committed before the
    broker stamped it (beyond the clock tolerance); and a series of identical values is not a
    measurement."""
    problems: list[str] = []
    coverage: list[str] = []
    if not samples:
        coverage.append("no Silver commit was sampled")
    outside = [
        s.at_ms
        for s in samples
        if not run_started_ms - SAMPLE_TIME_SLACK_MS
        <= s.at_ms
        <= run_finished_ms + SAMPLE_TIME_SLACK_MS
    ]
    if outside:
        problems.append(f"{len(outside)} sample(s) outside the run, e.g. {outside[:3]}")
    negative = [
        s.lag_ms for s in samples if s.lag_ms is not None and s.lag_ms < -CLOCK_OFFSET_TOLERANCE_MS
    ]
    if negative:
        problems.append(f"{len(negative)} sample(s) committed before the broker stamped them")
    for key, offset in sorted(committed_offsets.items()):
        end = broker_end_offsets.get(key)
        if end is None or offset >= end:
            problems.append(f"{key}: committed offset {offset}, but the broker's end is {end}")
    stale = sorted(str(k) for k, v in committed_newest.items() if v < run_started_ms)
    missing = sorted(str(k) for k in broker_end_offsets if k not in committed_newest)
    if stale or missing:
        coverage.append(
            f"partitions whose newest committed record predates the run: {stale}; never "
            f"committed: {missing}"
        )
    defined = [s.lag_ms for s in samples if s.lag_ms is not None]
    if len(defined) >= MIN_SAMPLES_PER_WINDOW and len(set(defined)) == 1:
        problems.append(f"all {len(defined)} lag samples are identical ({defined[0]} ms)")
    if not consumer_failed:
        problems = [*coverage, *problems]
    if problems:
        detail = "; ".join(problems)
    else:
        detail = "every sample is anchored to broker records"
        if coverage:
            detail += "; not held against the run because the consumer failed: " + "; ".join(
                coverage
            )
    return [Verdict("samples_authentic", INTEGRITY, not problems, detail)]
