"""Consumer lag exactly as PHASE3_PLAN §4.3 defines it, from Silver's commits. Pure: no I/O.

    freshness_lag(t) = t - min over partitions p of (newest LogAppendTime committed to Silver
                                                      for p by t)

sampled at each Silver commit. "Committed to Silver" covers every table a Silver micro-batch writes
a record's coordinates to (the canonical table, `silver.duplicates`, `silver.quarantine`), so a
partition whose newest records were duplicates or rejects has still been consumed. A sample is taken
after its commit is applied: "committed ... by t" includes the commit at t.

**Partitions are declared, not discovered.** The expected set is every partition of every topic in
the mix, read from the broker before the run. A partition with nothing committed yet makes the
sample undefined (`lag_ms` None, the partition listed as missing), never silently ignored: one
stuck or never-read partition cannot hide behind the others.

**Gaps.** `freshness_lag` is defined at every instant, and between two commits it only grows: at any
t, lag(t) >= t - (the last commit before t), because nothing committed by then is newer than that
commit. So a window with a gap of at least the target between consecutive commits (or between a
window edge and its nearest commit) provably reached the target, even when every sample taken at a
commit is below it. `window_stats` reports the widest gap and the verdict counts it.
"""

from __future__ import annotations

import itertools
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType


@dataclass(frozen=True, slots=True, order=True)
class PartitionKey:
    topic: str
    partition: int

    def __str__(self) -> str:
        return f"{self.topic}[{self.partition}]"


@dataclass(frozen=True, slots=True)
class PartitionProgress:
    """What one commit carried for one partition."""

    newest_log_append_ms: int
    max_offset: int


@dataclass(frozen=True, slots=True)
class CommitObservation:
    """One Silver commit: when it became visible, and per partition what it carried."""

    table: str
    version: int
    committed_ms: int
    partitions: Mapping[PartitionKey, PartitionProgress]


@dataclass(frozen=True, slots=True)
class LagSample:
    """`freshness_lag` at one Silver commit."""

    at_ms: int
    lag_ms: int | None
    """None while any expected partition has nothing committed (`missing`)."""
    missing: tuple[PartitionKey, ...]
    committed_total: int
    """Sum over committed partitions of (max committed offset + 1): a watermark whose difference
    between two samples with no missing partition is the records Silver committed between them."""
    table: str
    version: int
    pre_commit_lag_ms: int | None = None
    """The lag just before this commit was applied (the sawtooth's peak); diagnostic only."""


class UnexpectedPartitionError(ValueError):
    """A commit carried a partition outside the declared set: the broker changed under the run."""


class LagTracker:
    """Applies commits in commit order and samples `freshness_lag` after each."""

    def __init__(self, expected: Iterable[PartitionKey]) -> None:
        self._expected = frozenset(expected)
        if not self._expected:
            raise ValueError("no expected partitions: consumer lag would be undefined")
        self._newest: dict[PartitionKey, int] = {}
        self._offset: dict[PartitionKey, int] = {}

    @property
    def expected(self) -> frozenset[PartitionKey]:
        return self._expected

    def _lag(self, at_ms: int) -> tuple[int | None, tuple[PartitionKey, ...]]:
        missing = tuple(sorted(self._expected - self._newest.keys()))
        if missing:
            return None, missing
        return at_ms - min(self._newest.values()), ()

    def observe(self, commit: CommitObservation) -> LagSample:
        unexpected = sorted(set(commit.partitions) - self._expected)
        if unexpected:
            raise UnexpectedPartitionError(
                f"{commit.table} v{commit.version} carried {[str(p) for p in unexpected]}, which "
                f"are not in the declared partitions"
            )
        before, _ = self._lag(commit.committed_ms)
        for key, progress in commit.partitions.items():
            self._newest[key] = max(
                self._newest.get(key, progress.newest_log_append_ms), progress.newest_log_append_ms
            )
            self._offset[key] = max(self._offset.get(key, progress.max_offset), progress.max_offset)
        lag, missing = self._lag(commit.committed_ms)
        return LagSample(
            at_ms=commit.committed_ms,
            lag_ms=lag,
            missing=missing,
            committed_total=sum(offset + 1 for offset in self._offset.values()),
            table=commit.table,
            version=commit.version,
            pre_commit_lag_ms=before,
        )

    def newest(self) -> Mapping[PartitionKey, int]:
        return MappingProxyType(dict(self._newest))

    def offsets(self) -> Mapping[PartitionKey, int]:
        return MappingProxyType(dict(self._offset))


def commit_order(commit: CommitObservation) -> tuple[int, str, int]:
    return (commit.committed_ms, commit.table, commit.version)


def lag_samples(
    commits: Iterable[CommitObservation], expected: Iterable[PartitionKey]
) -> tuple[list[LagSample], LagTracker]:
    """Every commit, in commit order, through one tracker."""
    tracker = LagTracker(expected)
    samples = [tracker.observe(c) for c in sorted(commits, key=commit_order)]
    return samples, tracker


def percentile(values: Sequence[int], q: float) -> int:
    """Nearest-rank percentile of a non-empty sequence, as the other benchmarks compute it."""
    if not values:
        raise ValueError("percentile of no values")
    if not 0 < q <= 1:
        raise ValueError(f"q must be in (0, 1], got {q}")
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


@dataclass(frozen=True, slots=True)
class WindowStats:
    """Consumer lag and Kafka -> Silver throughput over `[start_ms, end_ms)`."""

    start_ms: int
    end_ms: int
    samples: int
    undefined_samples: int
    """Samples with a missing partition: each one is a sample that is not below the target."""
    lag_p50_ms: int | None
    lag_p95_ms: int | None
    lag_p99_ms: int | None
    lag_max_ms: int | None
    widest_gap_ms: int | None
    """The widest interval with no commit, window edges included; None with no sample at all."""
    committed_records: int | None
    committed_span_ms: int | None
    throughput_events_per_s: float | None
    """Records committed between the first and last defined samples of the window, over the time
    between those commits. None with fewer than two defined samples."""
    missing_partitions: tuple[str, ...] = field(default_factory=tuple)


def window_stats(samples: Sequence[LagSample], start_ms: int, end_ms: int) -> WindowStats:
    if end_ms <= start_ms:
        raise ValueError(f"empty window [{start_ms}, {end_ms})")
    ordered = sorted(samples, key=lambda s: (s.at_ms, s.table, s.version))
    inside = [s for s in ordered if start_ms <= s.at_ms < end_ms]
    defined = [s for s in inside if s.lag_ms is not None]
    lags = [s.lag_ms for s in defined if s.lag_ms is not None]
    gap: int | None = None
    if inside:
        before = [s.at_ms for s in ordered if s.at_ms < start_ms]
        edge = before[-1] if before else start_ms
        points = [edge, *(s.at_ms for s in inside), end_ms]
        gap = max(b - a for a, b in itertools.pairwise(points))
    committed = span = None
    rate: float | None = None
    if len(defined) >= 2 and defined[-1].at_ms > defined[0].at_ms:
        committed = defined[-1].committed_total - defined[0].committed_total
        span = defined[-1].at_ms - defined[0].at_ms
        rate = committed / (span / 1000)
    missing = sorted({str(p) for s in inside for p in s.missing})
    return WindowStats(
        start_ms=start_ms,
        end_ms=end_ms,
        samples=len(inside),
        undefined_samples=len(inside) - len(defined),
        lag_p50_ms=percentile(lags, 0.50) if lags else None,
        lag_p95_ms=percentile(lags, 0.95) if lags else None,
        lag_p99_ms=percentile(lags, 0.99) if lags else None,
        lag_max_ms=max(lags) if lags else None,
        widest_gap_ms=gap,
        committed_records=committed,
        committed_span_ms=span,
        throughput_events_per_s=rate,
        missing_partitions=tuple(missing),
    )


@dataclass(frozen=True, slots=True)
class Recovery:
    restored_ms: int
    recovered_at_ms: int | None
    recovery_ms: int | None
    """From the restore (the consumer restarted) to the first commit of the held recovery."""
    within_bound: bool
    peak_lag_ms: int | None
    """The highest defined sample after the restore, up to the recovery (or the observation end)."""
    samples_after_restore: int
    observed_until_ms: int


def detect_recovery(
    samples: Sequence[LagSample],
    *,
    restored_ms: int,
    observed_until_ms: int,
    target_ms: int,
    hold_ms: int,
    bound_ms: int,
) -> Recovery:
    """The first commit at or after the restore from which lag stays below the target for
    `hold_ms`, with the hold fully observed and no commit gap of `target_ms` or more inside it.

    An undefined sample (a missing partition) is not below the target. Recovered only when that
    commit comes no later than `bound_ms` after the restore."""
    after = sorted(
        (s for s in samples if restored_ms <= s.at_ms <= observed_until_ms),
        key=lambda s: (s.at_ms, s.table, s.version),
    )

    def below(sample: LagSample) -> bool:
        return sample.lag_ms is not None and sample.lag_ms < target_ms

    recovered: int | None = None
    for i, candidate in enumerate(after):
        if not below(candidate):
            continue
        hold_end = candidate.at_ms + hold_ms
        if hold_end > observed_until_ms:
            break
        held = [s for s in after[i:] if s.at_ms <= hold_end]
        points = [s.at_ms for s in held] + [hold_end]
        widest = max((b - a for a, b in itertools.pairwise(points)), default=0)
        if all(below(s) for s in held) and widest < target_ms:
            recovered = candidate.at_ms
            break
    until = recovered if recovered is not None else observed_until_ms
    peaks = [s.lag_ms for s in after if s.at_ms <= until and s.lag_ms is not None]
    recovery_ms = None if recovered is None else recovered - restored_ms
    return Recovery(
        restored_ms=restored_ms,
        recovered_at_ms=recovered,
        recovery_ms=recovery_ms,
        within_bound=recovery_ms is not None and recovery_ms <= bound_ms,
        peak_lag_ms=max(peaks) if peaks else None,
        samples_after_restore=len(after),
        observed_until_ms=observed_until_ms,
    )
