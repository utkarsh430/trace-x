"""The stream benchmark's decisions, without a JVM or a broker (`make load-stream`, Step 13).

The benchmark itself runs Spark against a real broker. Everything that turns what it observes into
a number or a verdict is pure, and is held here: consumer lag from per-partition Silver commits, the
Delta-log reader over real Parquet files, recovery detection, the clock-offset bound, pass / fail /
invalid, the rate that is never lowered, the manifest's completeness, and the guard that fabricated
samples cannot pass.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import os
from pathlib import Path
from typing import Any

import pytest
from benchmarks.stream_throughput import delta_log, record, verdict
from benchmarks.stream_throughput.clock import OffsetBounds
from benchmarks.stream_throughput.lag import (
    CommitObservation,
    LagSample,
    LagTracker,
    PartitionKey,
    PartitionProgress,
    UnexpectedPartitionError,
    detect_recovery,
    lag_samples,
    percentile,
    window_stats,
)
from benchmarks.stream_throughput.spec import (
    DEFAULT_MIX,
    GOLD_MAX_LAG_MS,
    GOLD_P95_LAG_MS,
    GOLD_STALL_MS,
    LAG_TARGET_MS,
    OUTAGE_S,
    TARGET_RATE_EVENTS_PER_S,
    RunConfig,
)
from benchmarks.stream_throughput.verdict import GoldBuild, Status, Verdict

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
A0, A1, B0 = PartitionKey("a", 0), PartitionKey("a", 1), PartitionKey("b", 0)
T0 = 1_800_000_000_000


def _commit(
    at: int, table: str = "t", version: int = 0, **parts: tuple[int, int]
) -> CommitObservation:
    keys = {"a0": A0, "a1": A1, "b0": B0}
    return CommitObservation(
        table,
        version,
        at,
        {keys[name]: PartitionProgress(stamp, offset) for name, (stamp, offset) in parts.items()},
    )


def _sample(at: int, lag: int | None, total: int = 0, version: int = 0) -> LagSample:
    return LagSample(at, lag, () if lag is not None else (A0,), total, "t", version)


# ----------------------------------------------------------------- the lag ---


def test_lag_is_measured_against_the_slowest_partition() -> None:
    tracker = LagTracker([A0, A1])
    tracker.observe(_commit(T0, a0=(T0 - 1_000, 10), a1=(T0 - 1_000, 10)))
    sample = tracker.observe(_commit(T0 + 5_000, version=1, a0=(T0 + 4_900, 20)))
    # a1 is stuck at T0 - 1,000: the fast partition cannot hide it.
    assert sample.lag_ms == 6_000
    assert sample.pre_commit_lag_ms == 6_000


def test_a_partition_with_nothing_committed_makes_the_sample_undefined() -> None:
    tracker = LagTracker([A0, A1])
    sample = tracker.observe(_commit(T0, a0=(T0 - 10, 5)))
    assert sample.lag_ms is None
    assert sample.missing == (A1,)


def test_a_partition_outside_the_declared_set_is_refused() -> None:
    with pytest.raises(UnexpectedPartitionError):
        LagTracker([A0]).observe(_commit(T0, a0=(T0, 1), b0=(T0, 1)))


def test_no_declared_partition_is_refused() -> None:
    with pytest.raises(ValueError, match="no expected partitions"):
        LagTracker([])


def test_a_rewritten_older_row_never_moves_a_partition_backwards() -> None:
    tracker = LagTracker([A0])
    tracker.observe(_commit(T0, a0=(T0 - 100, 50)))
    # A supersede MERGE re-adds an older row: it is not newer data.
    sample = tracker.observe(_commit(T0 + 1_000, version=1, a0=(T0 - 9_000, 3)))
    assert sample.lag_ms == 1_100
    assert tracker.offsets()[A0] == 50


def test_commits_are_applied_in_commit_order_whatever_order_they_are_read_in() -> None:
    late = _commit(T0 + 2_000, "x", 0, a0=(T0 + 1_900, 20))
    early = _commit(T0 + 1_000, "y", 0, a0=(T0 + 900, 10))
    samples, _ = lag_samples([late, early], [A0])
    assert [s.at_ms for s in samples] == [T0 + 1_000, T0 + 2_000]
    assert [s.lag_ms for s in samples] == [100, 100]


def test_percentile_is_nearest_rank() -> None:
    values = list(range(1, 101))
    assert percentile(values, 0.5) == 50
    assert percentile(values, 0.95) == 95
    assert percentile(values, 1.0) == 100
    with pytest.raises(ValueError, match="no values"):
        percentile([], 0.5)


def test_window_throughput_is_records_committed_between_commits_over_their_time() -> None:
    samples = [_sample(T0 + i * 1_000, 500, total=i * 5_000, version=i) for i in range(11)]
    stats = window_stats(samples, T0, T0 + 10_500)
    assert stats.committed_records == 50_000
    assert stats.committed_span_ms == 10_000
    assert stats.throughput_events_per_s == pytest.approx(5_000.0)
    assert stats.widest_gap_ms == 1_000
    assert stats.samples == 11


def test_a_commit_gap_as_long_as_the_target_fails_the_window_even_when_every_sample_is_low() -> (
    None
):
    samples = [_sample(T0 + i * 1_000, 300, total=i * 5_000, version=i) for i in range(5)]
    samples.append(_sample(T0 + 4_000 + LAG_TARGET_MS, 300, total=80_000, version=5))
    stats = window_stats(samples, T0, T0 + 20_000)
    assert stats.lag_max_ms == 300
    assert stats.widest_gap_ms == LAG_TARGET_MS
    lag = next(v for v in verdict.throughput_verdicts(stats) if v.name.endswith("below_target"))
    assert not lag.passed


def test_a_consumer_that_rarely_commits_fails_rather_than_invalidating_the_run() -> None:
    samples = [_sample(T0 + i * 180_000, 900, total=i * 900_000, version=i) for i in range(4)]
    result = verdict.throughput_verdicts(window_stats(samples, T0, T0 + 600_000))
    assert verdict.overall(result) is Status.FAIL


def test_the_window_edge_counts_as_a_gap() -> None:
    samples = [_sample(T0 + 1_000 * i, 100, total=i, version=i) for i in range(5)]
    stats = window_stats(samples, T0, T0 + 30_000)
    assert stats.widest_gap_ms == 26_000


def test_undefined_samples_fail_the_lag_verdict() -> None:
    samples = [_sample(T0 + i * 500, 100, total=i * 2_500, version=i) for i in range(30)]
    samples[10] = _sample(T0 + 5_000, None, total=0, version=10)
    stats = window_stats(samples, T0, T0 + 15_000)
    assert stats.undefined_samples == 1
    assert stats.missing_partitions == ("a[0]",)
    lag = next(v for v in verdict.throughput_verdicts(stats) if v.name.endswith("below_target"))
    assert not lag.passed


def test_throughput_just_under_the_quantisation_floor_fails() -> None:
    floor = TARGET_RATE_EVENTS_PER_S * 0.99
    samples = [
        _sample(T0 + i * 1_000, 200, total=int(i * (floor - 1)), version=i) for i in range(60)
    ]
    sustained = next(
        v
        for v in verdict.throughput_verdicts(window_stats(samples, T0, T0 + 60_000))
        if v.name == "throughput_sustained"
    )
    assert not sustained.passed


# ---------------------------------------------------------------- recovery ---


def _recovery(samples: list[LagSample], observed: int = T0 + 900_000) -> Any:
    return detect_recovery(
        samples,
        restored_ms=T0,
        observed_until_ms=observed,
        target_ms=LAG_TARGET_MS,
        hold_ms=60_000,
        bound_ms=900_000,
    )


def _series(start: int, end: int, lag: int, step: int = 2_000) -> list[LagSample]:
    return [_sample(t, lag, version=t) for t in range(start, end, step)]


def test_recovery_is_the_first_commit_from_which_lag_stays_below_the_target() -> None:
    samples = _series(T0 + 30_000, T0 + 120_000, 90_000) + _series(
        T0 + 120_000, T0 + 300_000, 4_000
    )
    found = _recovery(samples)
    assert found.recovered_at_ms == T0 + 120_000
    assert found.recovery_ms == 120_000
    assert found.within_bound
    assert found.peak_lag_ms == 90_000


def test_a_dip_that_bounces_back_is_not_recovery() -> None:
    samples = [
        *_series(T0 + 30_000, T0 + 100_000, 50_000),
        _sample(T0 + 100_000, 5_000, version=1),
        *_series(T0 + 102_000, T0 + 140_000, 30_000),
        *_series(T0 + 140_000, T0 + 400_000, 3_000),
    ]
    assert _recovery(samples).recovered_at_ms == T0 + 140_000


def test_a_hold_that_was_not_fully_observed_is_not_recovery() -> None:
    samples = _series(T0 + 10_000, T0 + 50_000, 2_000)
    found = _recovery(samples, observed=T0 + 50_000)
    assert found.recovered_at_ms is None
    assert not found.within_bound


def test_a_commit_gap_inside_the_hold_is_not_recovery() -> None:
    samples = [_sample(T0 + 10_000, 2_000, version=1), _sample(T0 + 25_000, 2_000, version=2)]
    samples += _series(T0 + 25_000, T0 + 200_000, 2_000)
    assert _recovery(samples).recovered_at_ms == T0 + 25_000


def test_recovery_after_the_bound_is_a_failure() -> None:
    samples = _series(T0, T0 + 950_000, 60_000) + _series(T0 + 950_000, T0 + 1_100_000, 1_000)
    found = _recovery(samples, observed=T0 + 1_100_000)
    assert found.recovered_at_ms == T0 + 950_000
    assert not found.within_bound
    outage = verdict.outage_verdicts(
        found, timing=_timing(), silver_commit_ms=[], consumer_failure=None, bound_s=900
    )
    assert verdict.overall([*outage, _passing_target()]) is Status.FAIL


def test_an_undefined_sample_is_not_below_the_target() -> None:
    samples = [_sample(T0 + 1_000, None, version=1), *_series(T0 + 3_000, T0 + 100_000, 1_000)]
    assert _recovery(samples).recovered_at_ms == T0 + 3_000


# --------------------------------------------------------------- verdicts ---


def _passing_target() -> Verdict:
    return Verdict("x", verdict.TARGET, True, "")


def test_overall_orders_invalid_over_fail_over_pass() -> None:
    ok = _passing_target()
    bad_target = Verdict("t", verdict.TARGET, False, "")
    bad_integrity = Verdict("i", verdict.INTEGRITY, False, "")
    assert verdict.overall([ok]) is Status.PASS
    assert verdict.overall([ok, bad_target]) is Status.FAIL
    assert verdict.overall([ok, bad_target, bad_integrity]) is Status.INVALID
    assert verdict.overall([]) is Status.INVALID
    assert verdict.overall([Verdict("i", verdict.INTEGRITY, True, "")]) is Status.INVALID


def test_offered_load_counts_only_whole_buckets_inside_the_window() -> None:
    buckets = {1000.25 + i: 5_000 for i in range(10)}
    buckets[1000.25 + 3] = 4_000
    offered = verdict.offered_load(buckets, {1003.25: 0.2}, 1001.0, 1008.0)
    assert offered.buckets == 6
    assert offered.rate_events_per_s == pytest.approx((5 * 5_000 + 4_000) / 6)
    assert offered.max_behind_s == 0.2


def test_a_rate_the_harness_did_not_offer_makes_the_run_invalid_never_a_pass() -> None:
    low = verdict.OfferedLoad(4_900.0, 600, 0.0)
    assert verdict.overall([*verdict.offered_verdicts("p", low, 5_000), _passing_target()]) is (
        Status.INVALID
    )
    behind = verdict.OfferedLoad(5_000.0, 600, 1.5)
    assert verdict.overall([*verdict.offered_verdicts("p", behind, 5_000), _passing_target()]) is (
        Status.INVALID
    )
    kept = verdict.OfferedLoad(5_000.0, 600, 0.05)
    assert verdict.overall([*verdict.offered_verdicts("p", kept, 5_000), _passing_target()]) is (
        Status.PASS
    )


def test_a_consumer_that_died_is_a_failure_not_an_invalid_run() -> None:
    died = verdict.outage_verdicts(
        None, timing=None, silver_commit_ms=[], consumer_failure="exit 3", bound_s=900
    )
    assert verdict.overall([*died, _passing_target()]) is Status.FAIL


def test_the_first_error_line_names_the_out_of_memory_error_under_spark_s_wrappers() -> None:
    from benchmarks.stream_throughput.run import first_error_line

    log = [
        "26/09/18 13:35:12 INFO SparkContext: Running Spark version 4.0.1\n",
        "[Stage 799:>   (0 + 4) / 5]\r26/09/18 13:37:39 WARN BlockManager: Putting block failed\n",
        "26/09/18 13:37:40 ERROR Executor: Exception in task 0.0 in stage 799.0 (TID 8990)\n",
        "\tat org.apache.spark.sql.errors.QueryExecutionErrors.cannotReadFilesError(x.scala:856)\n",
        "Caused by: java.lang.OutOfMemoryError: Java heap space\n",
    ]
    assert first_error_line(log) == "Caused by: java.lang.OutOfMemoryError: Java heap space"
    assert first_error_line(log[:4]) == (
        "26/09/18 13:37:40 ERROR Executor: Exception in task 0.0 in stage 799.0 (TID 8990)"
    )
    assert first_error_line(log[:2]) is None


class _StubFleet:
    """What `_measure` reads from the producer fleet, as a run whose consumer died leaves it."""

    def __init__(
        self, finals: dict[int, dict[str, Any]], streamed: dict[int, dict[str, Any]]
    ) -> None:
        self.finals, self.clocks = finals, streamed
        self.processes = [object()] * max(len(finals), len(streamed), 1)

    def snapshot(self) -> tuple[dict[float, int], dict[float, float], list[str]]:
        return {}, {}, []

    def reports(self) -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]]]:
        return dict(self.finals), dict(self.clocks)


def _worker_final(
    worker: int, clock: OffsetBounds, partitions: list[PartitionKey]
) -> dict[str, Any]:
    return {
        "worker": worker,
        "clock": clock.as_record(),
        "delivery_problems": [],
        "not_log_append_time": 0,
        "delivered_by_partition": {str(p): 10 for p in partitions},
    }


def _record_crashed_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet: _StubFleet
) -> tuple[int, dict[str, Any]]:
    """`_evaluate_and_record` for a run whose consumer died before the measured window: a real,
    empty lake (the consumer committed nothing), no broker (its end offsets are stubbed), a clean
    worktree."""
    import argparse

    from benchmarks.stream_throughput import run
    from benchmarks.stream_throughput.spec import RunConfig

    lake = tmp_path / "lake"
    lake.mkdir(parents=True)
    log = tmp_path / "consumer-1.log"
    log.write_text(
        "26/09/18 13:37:40 ERROR Executor: Exception in task 0.0\n"
        "Caused by: java.lang.OutOfMemoryError: Java heap space\n"
    )
    crash = run.crash_record(1, log, generation=1)
    monkeypatch.setattr(run, "watermarks", lambda _b, keys: dict.fromkeys(keys, 100))
    monkeypatch.setattr(run.record, "git_facts", lambda: ("a" * 40, False, "sha256:lock"))
    started = dt.datetime.fromtimestamp(T0 / 1000, dt.UTC)
    code = run._evaluate_and_record(
        config=RunConfig(),
        args=argparse.Namespace(bootstrap="localhost:9092", record_dir=tmp_path / "records"),
        run_id="bench-test-crash",
        started=started,
        finished=started + dt.timedelta(minutes=3),
        lake_root=lake,
        logs=tmp_path,
        topics=["tx.scored.v1"],
        expected=[PartitionKey("tx.scored.v1", 0)],
        broker={},
        starting_low={},
        starting_end={},
        phases={"producers_started_ms": T0, "observed_until_ms": T0 + 150_000},
        fleet=fleet,  # type: ignore[arg-type]
        consumer=None,
        gold=None,
        sampler=None,
        consumer_failure=run.crash_detail(crash),
        consumer_crash=crash,
        outage_timing=None,
        harness_error=None,
        notes=[],
        toolchain_record=[],
        provenance_at_start=("a" * 40, False, "sha256:lock"),
    )
    written = json.loads((tmp_path / "records" / "bench-test-crash.json").read_text())
    return code, written


def test_a_consumer_crash_with_every_evaluable_integrity_check_passing_is_a_recorded_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from benchmarks.stream_throughput import run

    partitions = [PartitionKey("tx.scored.v1", 0)]
    clock = OffsetBounds().observe(sent_ms=1_000.0, acked_ms=1_004.0, log_append_ms=1_002.0)
    fleet = _StubFleet({0: _worker_final(0, clock, partitions)}, {0: clock.as_record()})
    code, written = _record_crashed_run(tmp_path, monkeypatch, fleet)
    assert written["status"] == "FAIL" and written["publishable"] is True
    assert code == run.EXIT_TARGET_MISSED == 1
    failed = {v["name"]: v for v in written["verdicts"] if not v["passed"]}
    assert failed and all(v["kind"] == verdict.TARGET for v in failed.values())
    survived = failed["consumer_survived"]["detail"]
    assert "exited with code 1" in survived
    assert "java.lang.OutOfMemoryError: Java heap space" in survived
    assert written["measured"]["consumer_crash"]["exit_code"] == 1
    assert "OutOfMemoryError" in written["measured"]["consumer_crash"]["first_error_line"]
    assert "OutOfMemoryError" in failed["throughput_sustained"]["detail"]
    assert written["measured"]["clock_offset"]["samples"] == 1


def test_a_consumer_crash_does_not_hide_a_genuine_integrity_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A clock outside tolerance still makes the run INVALID, crash or not; so does a worker
    that never confirmed its deliveries."""
    partitions = [PartitionKey("tx.scored.v1", 0)]
    skewed = OffsetBounds().observe(sent_ms=1_000.0, acked_ms=1_005.0, log_append_ms=3_002.0)
    fleet = _StubFleet({0: _worker_final(0, skewed, partitions)}, {})
    _code, written = _record_crashed_run(tmp_path / "skew", monkeypatch, fleet)
    assert written["status"] == "INVALID"
    assert _failed_integrity(written) == ["clock_offset_bounded"]
    fine = OffsetBounds().observe(sent_ms=1_000.0, acked_ms=1_004.0, log_append_ms=1_002.0)
    no_final = _StubFleet({}, {0: fine.as_record()})
    _code, written = _record_crashed_run(tmp_path / "nofinal", monkeypatch, no_final)
    assert written["status"] == "INVALID"
    assert _failed_integrity(written) == ["deliveries_confirmed", "partitions_offered"]
    assert written["measured"]["clock_offset"]["samples"] == 1, "the streamed bound is kept"


def _failed_integrity(written: dict[str, Any]) -> list[str]:
    return [
        v["name"] for v in written["verdicts"] if v["kind"] == verdict.INTEGRITY and not v["passed"]
    ]


def test_a_crashed_consumer_s_missing_coverage_is_noted_but_fabrication_is_still_caught() -> None:
    lenient = verdict.authenticity_verdicts(
        [],
        committed_offsets={},
        committed_newest={},
        broker_end_offsets={A0: 100},
        run_started_ms=T0,
        run_finished_ms=T0 + 600_000,
        consumer_failed=True,
    )[0]
    assert lenient.passed and "consumer failed" in lenient.detail
    strict = _authentic([], committed_offsets={}, committed_newest={})
    assert not strict.passed
    beyond = verdict.authenticity_verdicts(
        [_sample(T0 + 20_000, 400)],
        committed_offsets={A0: 5_000},
        committed_newest={A0: T0 + 10_000},
        broker_end_offsets={A0: 100},
        run_started_ms=T0,
        run_finished_ms=T0 + 600_000,
        consumer_failed=True,
    )[0]
    assert not beyond.passed and "broker's end" in beyond.detail


def _timing(*, stop_ms: int = 5_000, restarted: int = T0) -> verdict.OutageTiming:
    """An outage the harness can produce: signalled, exited `stop_ms` later, and restarted when
    `restart_due_ms` says, ending at `restarted` (the recovery's `restored_ms`)."""
    exited = restarted - OUTAGE_S * 1000
    assert verdict.restart_due_ms(exited) == restarted
    return verdict.OutageTiming(exited - stop_ms, exited, restarted)


def _outage(timing: verdict.OutageTiming, commits: list[int]) -> list[Verdict]:
    found = _recovery(_series(T0 + 2_000, T0 + 200_000, 1_000))
    return verdict.outage_verdicts(
        found, timing=timing, silver_commit_ms=commits, consumer_failure=None, bound_s=900
    )


def test_the_outage_is_measured_from_the_exit_so_a_slow_stop_still_yields_the_full_outage() -> None:
    # The stop was signalled, and the consumer took 60 s to exit (its queries finishing, Silver
    # committing throughout). The restart is due OUTAGE_S after the exit, not after the signal.
    signalled = T0 - 180_000
    exited = signalled + 60_000
    restarted = verdict.restart_due_ms(exited)
    timing = verdict.OutageTiming(signalled, exited, restarted)
    assert timing.stop_s == 60.0
    assert timing.down_s == float(OUTAGE_S) == 120.0
    # Silver committed during the slow stop (before the exit) and after the restart: legitimate.
    commits = [signalled + 10_000, exited - 1, exited, restarted, restarted + 5_000]
    result = _outage(timing, commits)
    assert all(v.passed for v in result), [v for v in result if not v.passed]
    assert verdict.overall(result) is Status.PASS
    record = timing.as_record()
    assert (record["signalled_at_ms"], record["exited_at_ms"], record["restarted_at_ms"]) == (
        signalled,
        exited,
        restarted,
    )
    assert record["consumer_down_s"] == 120.0 and record["stop_s"] == 60.0


def test_a_silver_commit_while_the_consumer_was_down_makes_the_run_invalid() -> None:
    timing = _timing(stop_ms=60_000)
    inside = timing.exited_ms + 30_000
    result = _outage(timing, [timing.exited_ms - 1_000, inside, timing.restarted_ms + 1_000])
    down = next(v for v in result if v.name == "outage_consumer_down")
    assert not down.passed and str(inside) in down.detail
    assert verdict.commits_while_down(timing, [inside, timing.exited_ms]) == [inside]
    assert verdict.overall([*result, _passing_target()]) is Status.INVALID


def test_an_outage_outside_the_timing_tolerance_fails_the_timing_verdict() -> None:
    exited = T0 - 200_000
    tolerance_ms = int(verdict.OUTAGE_TIMING_TOLERANCE_S * 1000)
    for down_ms, ok in [
        (OUTAGE_S * 1000 + tolerance_ms, True),
        (OUTAGE_S * 1000 - tolerance_ms, True),
        (OUTAGE_S * 1000 + tolerance_ms + 1, False),
        (OUTAGE_S * 1000 - tolerance_ms - 1, False),
        (60_000, False),  # the old defect: 120 s after the signal, 60 s after a slow exit
    ]:
        timing = verdict.OutageTiming(exited - 60_000, exited, exited + down_ms)
        duration = next(v for v in _outage(timing, []) if v.name == "outage_duration")
        assert duration.passed is ok, (down_ms, duration.detail)
        assert verdict.overall(_outage(timing, [])) is (Status.PASS if ok else Status.INVALID)


def test_out_of_order_outage_times_are_invalid() -> None:
    backwards = verdict.OutageTiming(T0, T0 - 1_000, T0 - 1_000 + OUTAGE_S * 1000)
    assert verdict.overall(_outage(backwards, [])) is Status.INVALID


def test_the_consumer_stop_reports_the_real_exit_not_the_signal(tmp_path: Path) -> None:
    """A real child process that takes a while to exit on SIGTERM, as the consumer's graceful
    query stop does: the exit time is after the child exited, not when it was signalled."""
    import subprocess
    import sys

    from benchmarks.stream_throughput.run import ConsumerProcess

    script = (
        "import signal, sys, time\n"
        "signal.signal(signal.SIGTERM, lambda *_: (time.sleep(0.6), sys.exit(0)))\n"
        "print('ready', flush=True)\n"
        "time.sleep(60)\n"
    )
    consumer = ConsumerProcess(RunConfig(), bootstrap="x:1", lake_root=tmp_path, logs=tmp_path)
    consumer.process = subprocess.Popen(  # noqa: S603 -- this interpreter, a literal script
        [sys.executable, "-c", script], stdout=subprocess.PIPE, start_new_session=True
    )
    assert consumer.process.stdout is not None
    assert consumer.process.stdout.readline().strip() == b"ready"
    stopped = consumer.stop(timeout_s=30)
    assert stopped.exit_code == 0 and not stopped.killed
    assert stopped.exited_ms - stopped.signalled_ms >= 600
    assert consumer.events[-1]["exited_ms"] == stopped.exited_ms


# -------------------------------------------------------------- exit codes ---


def test_exit_codes_distinguish_a_harness_failure_from_a_missed_target() -> None:
    from benchmarks.stream_throughput import run

    assert run.exit_code_for(Status.PASS, publishable=True) == run.EXIT_PASS == 0
    assert run.exit_code_for(Status.FAIL, publishable=True) == run.EXIT_TARGET_MISSED == 1
    assert run.exit_code_for(Status.INVALID, publishable=False) == run.EXIT_HARNESS == 2
    assert run.exit_code_for(Status.INVALID, publishable=True) == run.EXIT_HARNESS
    assert run.exit_code_for(Status.PASS, publishable=False) == run.EXIT_UNPUBLISHABLE == 3
    assert run.exit_code_for(Status.FAIL, publishable=False) == run.EXIT_UNPUBLISHABLE


@pytest.mark.parametrize("raised", [RuntimeError("boom"), KeyError("k"), KeyboardInterrupt()])
def test_an_unexpected_exception_exits_as_a_harness_error_never_as_a_missed_target(
    monkeypatch: pytest.MonkeyPatch, raised: BaseException
) -> None:
    from benchmarks.stream_throughput import run

    logged: list[tuple[str, dict[str, Any]]] = []

    class _Log:
        def exception(self, event: str, **kw: Any) -> None:
            logged.append((event, kw))

    def crash(_argv: Any) -> int:
        raise raised

    monkeypatch.setattr(run, "_run", crash)
    monkeypatch.setattr(run, "_log", _Log())
    assert run.main([]) == run.EXIT_HARNESS
    assert logged == [("load_stream_harness_error", {"error": type(raised).__name__})]


# ------------------------------------------------------------------- Gold ---

W0, W1 = T0, T0 + 600_000


def _gold(
    builds: list[GoldBuild], *, enabled: bool = True, until: int = W1 + 60_000
) -> list[Verdict]:
    return verdict.gold_verdicts(
        builds,
        enabled=enabled,
        loop_started_ms=W0 - 300_000,
        start_ms=W0,
        end_ms=W1,
        evaluated_until_ms=until,
        silver_advanced=True,
    )


def _build(start: int, end: int, lag: int, code: int = 0) -> GoldBuild:
    return GoldBuild(started_ms=start, finished_ms=end, exit_code=code, lag_ms=lag, build_id=start)


def test_fresh_gold_builds_pass() -> None:
    builds = [_build(W0 + i * 120_000, W0 + (i + 1) * 120_000, 130_000) for i in range(5)]
    assert all(v.passed for v in _gold(builds))


def test_gold_p95_over_fifteen_minutes_fails() -> None:
    builds = [
        _build(W0 + i * 100_000, W0 + (i + 1) * 100_000, GOLD_P95_LAG_MS + 1) for i in range(5)
    ]
    failed = {v.name for v in _gold(builds) if not v.passed}
    assert failed == {"gold_p95_lag"}


def test_a_failed_gold_build_fails() -> None:
    builds = [_build(W0, W0 + 100_000, 100_000), _build(W0 + 100_000, W0 + 200_000, 0, code=3)]
    failed = {v.name for v in _gold(builds) if not v.passed}
    assert "gold_builds_succeeded" in failed


def test_an_abandoned_gold_build_counts_at_least_its_age() -> None:
    abandoned = GoldBuild(
        started_ms=W1 - 1_000,
        finished_ms=None,
        exit_code=None,
        lag_ms=None,
        abandoned_ms=W1 - 1_000 + GOLD_MAX_LAG_MS + 5_000,
    )
    builds = [_build(W0, W0 + 100_000, 100_000), abandoned]
    failed = {v.name for v in _gold(builds, until=abandoned.abandoned_ms or 0) if not v.passed}
    assert {"gold_max_lag", "gold_p95_lag"} <= failed


def test_thirty_minutes_without_a_gold_commit_is_a_stall() -> None:
    build = _build(W0 - GOLD_STALL_MS, W0 - GOLD_STALL_MS + 60_000, 60_000)
    late = _build(W0, W1 + GOLD_STALL_MS, 120_000)
    names = {v.name for v in _gold([build, late], until=W1 + GOLD_STALL_MS) if not v.passed}
    assert "gold_no_stall" in names


def test_gold_not_run_is_a_failure_of_the_target() -> None:
    result = _gold([], enabled=False)
    assert verdict.overall(result) is Status.FAIL


# ---------------------------------------------------------------- clocks ---


def test_the_offset_is_the_intersection_of_every_delivery_interval() -> None:
    bounds = OffsetBounds()
    bounds = bounds.observe(sent_ms=1_000.0, acked_ms=1_010.0, log_append_ms=1_004.0)
    bounds = bounds.observe(sent_ms=2_000.0, acked_ms=2_003.0, log_append_ms=2_002.0)
    assert bounds.lower_ms == pytest.approx(-1.0)
    assert bounds.upper_ms == pytest.approx(3.0)
    assert bounds.consistent and bounds.within(500.0)


def test_a_skewed_broker_clock_is_outside_the_tolerance() -> None:
    bounds = OffsetBounds().observe(sent_ms=1_000.0, acked_ms=1_005.0, log_append_ms=3_002.0)
    assert bounds.consistent
    assert not bounds.within(500.0)
    assert not verdict.clock_verdict(bounds).passed


def test_a_clock_that_stepped_leaves_no_consistent_offset() -> None:
    """A ~900 ms step: no single offset fits both records (drift reported), and the step is judged
    by the envelope -- inside a 10 s tolerance, outside the benchmark's 500 ms one."""
    bounds = OffsetBounds().observe(sent_ms=1_000.0, acked_ms=1_002.0, log_append_ms=1_001.0)
    bounds = bounds.observe(sent_ms=2_000.0, acked_ms=2_002.0, log_append_ms=2_900.0)
    assert not bounds.consistent
    assert bounds.drift_ms == 896.0  # intersection [898, 2]
    assert (bounds.min_lower_ms, bounds.max_upper_ms) == (-1.0, 901.0)
    assert bounds.within(10_000.0)
    assert not bounds.within(500.0)


def test_millisecond_drift_between_records_does_not_invalidate_the_offset() -> None:
    """The 2026-09-18 run: 720,784 reports whose intersection inverted by ~1.4 ms (LogAppendTime is
    in whole milliseconds, and Docker Desktop's VM clock drifts) while every record sat within a
    few milliseconds of the host clock. That is a bounded offset, not an integrity failure."""
    bounds = OffsetBounds().observe(sent_ms=1_000.0, acked_ms=1_000.4, log_append_ms=1_000.0)
    bounds = bounds.observe(sent_ms=2_002.5, acked_ms=2_003.0, log_append_ms=2_001.0)
    assert not bounds.consistent and bounds.drift_ms is not None and bounds.drift_ms > 0
    assert bounds.within(500.0)
    assert verdict.clock_verdict(bounds).passed
    merged = OffsetBounds(-1.0, 2.0, 5).merge(OffsetBounds(-3.0, 1.0, 5))
    assert (merged.lower_ms, merged.upper_ms, merged.samples) == (-1.0, 1.0, 10)


def test_the_fleet_offset_comes_from_a_partial_set_of_reports() -> None:
    """A worker that sent its final report contributes it; one that only streamed contributes its
    latest streamed bound; one that sent nothing contributes nothing. No worker is counted twice."""
    from benchmarks.stream_throughput.clock import fleet_bounds

    finals = {0: {"clock": OffsetBounds(-2.0, 4.0, 1_000).as_record()}}
    streamed = {
        0: OffsetBounds(-3.0, 6.0, 400).as_record(),  # older than worker 0's final: not reused
        1: OffsetBounds(-1.0, 5.0, 250).as_record(),
    }
    bounds = fleet_bounds(finals, streamed)
    assert (bounds.lower_ms, bounds.upper_ms, bounds.samples) == (-1.0, 4.0, 1_250)
    assert verdict.clock_verdict(bounds).passed
    only_streamed = fleet_bounds({}, {1: OffsetBounds(-1.0, 5.0, 250).as_record()})
    assert only_streamed.samples == 250 and only_streamed.within(500.0)
    # A final that somehow covers fewer reports than the worker already streamed loses to it.
    stale_final = fleet_bounds({2: {"clock": OffsetBounds().as_record()}}, {2: streamed[1]})
    assert stale_final.samples == 250
    assert fleet_bounds({}, {}).samples == 0


def test_zero_reports_invalidate_a_run_only_when_a_judged_lag_needs_the_offset() -> None:
    empty = OffsetBounds()
    needed = verdict.clock_verdict(empty, required=True)
    assert not needed.passed and needed.kind == verdict.INTEGRITY
    assert verdict.overall([needed, _passing_target()]) is Status.INVALID
    unneeded = verdict.clock_verdict(empty, required=False)
    assert unneeded.passed and "not required" in unneeded.detail
    # Reports that arrived are judged whether or not a lag needed them.
    skewed = OffsetBounds().observe(sent_ms=1_000.0, acked_ms=1_005.0, log_append_ms=3_002.0)
    assert not verdict.clock_verdict(skewed, required=False).passed


# ---------------------------------------------------- the rate is never lowered ---


def test_the_rate_is_never_below_the_target() -> None:
    with pytest.raises(ValueError, match="never lowered"):
        RunConfig(rate_events_per_s=TARGET_RATE_EVENTS_PER_S - 1)
    assert RunConfig().rate_events_per_s == TARGET_RATE_EVENTS_PER_S
    assert RunConfig(rate_events_per_s=6_000).rate_events_per_s == 6_000


def test_the_outage_length_is_not_configurable() -> None:
    assert OUTAGE_S == 120
    assert not any(name == "outage_s" for name in RunConfig.model_fields)
    with pytest.raises(ValueError, match=r"(?i)extra"):
        RunConfig.model_validate({"outage_s": 60})


def test_the_mix_is_declared_and_released() -> None:
    with pytest.raises(ValueError, match="may name only"):
        RunConfig(mix={"tx.raw.v1": 1})
    with pytest.raises(ValueError, match="scored transaction"):
        RunConfig(mix={"tx.authorization.v1": 1})
    assert RunConfig().mix == DEFAULT_MIX


def test_the_consumer_runs_processing_time_triggers_never_available_now() -> None:
    from benchmarks.stream_throughput.consumer import _parser
    from benchmarks.stream_throughput.run import ConsumerProcess

    command = ConsumerProcess(
        RunConfig(), bootstrap="localhost:9092", lake_root=Path("/x"), logs=Path("/y")
    ).command()
    assert "--bronze-trigger-s" in command and "--silver-trigger-s" in command
    assert not any("available" in part for part in command)
    parsed = _parser().parse_args(command[3:])
    assert parsed.bronze_trigger_s == 2.0 and parsed.master == "local[4]"
    source = (ROOT / "benchmarks" / "stream_throughput" / "consumer.py").read_text()
    assert "available_now=True" not in source


# ----------------------------------------------------- fabricated samples ---


def _authentic(samples: list[LagSample], **overrides: Any) -> Verdict:
    kwargs: dict[str, Any] = {
        "committed_offsets": {A0: 99},
        "committed_newest": {A0: T0 + 10_000},
        "broker_end_offsets": {A0: 100},
        "run_started_ms": T0,
        "run_finished_ms": T0 + 3_600_000,
    }
    kwargs.update(overrides)
    (only,) = verdict.authenticity_verdicts(samples, **kwargs)
    return only


def test_a_genuine_looking_series_is_accepted() -> None:
    samples = [_sample(T0 + 20_000 + i * 1_000, 400 + (i % 7) * 13, version=i) for i in range(30)]
    assert _authentic(samples).passed


def test_fabricated_samples_cannot_pass() -> None:
    """The constant-predictor guard: a series that is not anchored to broker records is INVALID,
    however good its numbers look, and so is the run that carries it."""
    good_looking = [_sample(T0 + 20_000 + i * 1_000, 500, version=i) for i in range(30)]
    constant = _authentic(good_looking)
    assert not constant.passed and "identical" in constant.detail
    varied = [_sample(T0 + 20_000 + i * 1_000, 400 + i, version=i) for i in range(30)]
    beyond = _authentic(varied, committed_offsets={A0: 5_000})
    assert not beyond.passed and "broker's end" in beyond.detail
    stale = _authentic(varied, committed_newest={A0: T0 - 1})
    assert not stale.passed and "predates the run" in stale.detail
    never = _authentic(varied, committed_newest={}, committed_offsets={})
    assert not never.passed and "never committed" in never.detail
    outside = [_sample(T0 - 3_600_000 + i * 1_000, 400 + i, version=i) for i in range(30)]
    assert not _authentic(outside).passed
    negative = [_sample(T0 + 20_000 + i * 1_000, -5_000 + i, version=i) for i in range(30)]
    assert not _authentic(negative).passed
    assert not _authentic([]).passed

    stats = window_stats(good_looking, T0, T0 + 60_000)
    targets = verdict.throughput_verdicts(
        dataclasses.replace(stats, throughput_events_per_s=5_000.0)
    )
    assert verdict.overall([*targets, constant]) is Status.INVALID


# ------------------------------------------------------------- Delta log ---


def _write_commit(
    table: Path, version: int, rows: list[dict[str, Any]], *, stamp_ms: int, int96: bool = False
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    log = table / delta_log.LOG_DIR
    log.mkdir(parents=True, exist_ok=True)
    actions: list[dict[str, Any]] = [{"commitInfo": {"timestamp": stamp_ms, "operation": "WRITE"}}]
    if rows:
        name = f"part-{version:05d} with space.parquet"
        data = pa.table(
            {
                "kafka_partition": pa.array([r["partition"] for r in rows], pa.int32()),
                "kafka_offset": pa.array([r["offset"] for r in rows], pa.int64()),
                "kafka_timestamp": pa.array(
                    [dt.datetime.fromtimestamp(r["ts"] / 1000, dt.UTC) for r in rows],
                    pa.timestamp("us", tz="UTC"),
                ),
                "silver_topic": pa.array([r.get("topic", "a") for r in rows], pa.string()),
            }
        )
        pq.write_table(data, table / name, use_deprecated_int96_timestamps=int96)
        actions.append({"add": {"path": name.replace(" ", "%20"), "dataChange": True}})
    path = log / f"{version:020d}.json"
    path.write_text("\n".join(json.dumps(a) for a in actions) + "\n")
    os.utime(path, ns=(stamp_ms * 1_000_000 - 5_000_000, stamp_ms * 1_000_000 - 5_000_000))


@pytest.mark.parametrize("int96", [False, True])
def test_the_delta_log_reader_reads_each_commit_and_the_rows_it_added(
    tmp_path: Path, int96: bool
) -> None:
    table = delta_log.WatchedTable("silver.a", tmp_path / "a", "a", frozenset({"a"}))
    _write_commit(table.directory, 0, [], stamp_ms=T0)
    rows = [
        {"partition": 0, "offset": 7, "ts": T0 + 100},
        {"partition": 0, "offset": 8, "ts": T0 + 250},
        {"partition": 1, "offset": 3, "ts": T0 + 90},
    ]
    _write_commit(table.directory, 1, rows, stamp_ms=T0 + 2_000, int96=int96)
    reader = delta_log.CommitReader(table)
    commits = reader.poll()
    assert [c.version for c in commits] == [0, 1]
    assert commits[0].partitions == {}
    assert commits[1].committed_ms == T0 + 2_000
    assert commits[1].partitions == {
        A0: PartitionProgress(T0 + 250, 8),
        A1: PartitionProgress(T0 + 90, 3),
    }
    assert reader.poll() == []


def test_a_shared_table_attributes_rows_by_silver_topic_and_ignores_other_topics(
    tmp_path: Path,
) -> None:
    table = delta_log.WatchedTable("silver.duplicates", tmp_path / "d", None, frozenset({"a"}))
    rows = [
        {"partition": 0, "offset": 1, "ts": T0, "topic": "a"},
        {"partition": 0, "offset": 9, "ts": T0 + 999, "topic": "other"},
    ]
    _write_commit(table.directory, 0, rows, stamp_ms=T0 + 50)
    (commit,) = delta_log.CommitReader(table).poll()
    assert commit.partitions == {A0: PartitionProgress(T0, 1)}


def test_the_later_of_commit_time_and_file_time_is_the_commit_time(tmp_path: Path) -> None:
    table = delta_log.WatchedTable("silver.a", tmp_path / "a", "a", frozenset({"a"}))
    _write_commit(table.directory, 0, [], stamp_ms=T0)
    path = table.directory / delta_log.LOG_DIR / f"{0:020d}.json"
    later = (T0 + 7_000) * 1_000_000
    os.utime(path, ns=(later, later))
    (commit,) = delta_log.CommitReader(table).poll()
    assert commit.committed_ms == T0 + 7_000


def test_a_missing_log_version_is_an_error_not_a_silent_gap(tmp_path: Path) -> None:
    table = delta_log.WatchedTable("silver.a", tmp_path / "a", "a", frozenset({"a"}))
    _write_commit(table.directory, 0, [], stamp_ms=T0)
    _write_commit(table.directory, 2, [], stamp_ms=T0 + 10)
    with pytest.raises(delta_log.DeltaLogError, match="missing"):
        delta_log.CommitReader(table).poll()


def test_the_watched_tables_are_the_canonical_and_shared_silver_tables(tmp_path: Path) -> None:
    tables = delta_log.watched_tables(tmp_path, ["tx.scored.v1", "identity.events.v1"])
    labels = [t.label for t in tables]
    assert labels == [
        "silver.identity_events_v1",
        "silver.tx_scored_v1",
        "silver.duplicates",
        "silver.quarantine",
    ]
    assert tables[2].topic is None and tables[2].topics == {"tx.scored.v1", "identity.events.v1"}


# ------------------------------------------------------------ the record ---


def _record(**overrides: Any) -> record.StreamBenchmarkRecord:
    fields: dict[str, Any] = {
        "run_id": record.new_run_id(dt.datetime(2026, 9, 18, 12, 0, 0, tzinfo=dt.UTC), "a" * 40),
        "started_at": "2026-09-18T12:00:00+00:00",
        "finished_at": "2026-09-18T12:40:00+00:00",
        "status": "PASS",
        "config": json.loads(RunConfig().model_dump_json()),
        "broker": {"bootstrap": "localhost:9092", "topics": {}},
        "toolchain": record.toolchain_facts([]),
        "host": record.host_facts(),
        "lake_root": "/lake",
        "logs": "/logs",
        "verdicts": [dataclasses.asdict(_passing_target())],
        "measured": {"throughput_window": {"samples": 300}},
        "git_commit_sha": "a" * 40,
        "dirty_worktree": False,
        "env_lock_digest": "sha256:" + "0" * 64,
    }
    fields.update(overrides)
    return record.StreamBenchmarkRecord(**fields)


def _check_claims() -> Any:
    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location(
        "check_claims", ROOT / "scripts" / "check_claims.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_claims"] = module
    spec.loader.exec_module(module)
    return module


def test_the_record_carries_every_field_the_claim_linter_requires() -> None:
    manifest = dataclasses.asdict(_record())
    assert manifest["record_type"] == "BENCHMARK"
    assert _check_claims().incomplete_fields(manifest) == []
    pins = manifest["toolchain"]["pins"]
    assert set(pins) == {"java_major", "spark", "delta", "hadoop_line", "scala_line"}
    assert manifest["targets"]["rate_events_per_s"] == TARGET_RATE_EVENTS_PER_S
    assert manifest["targets"]["lag_target_ms"] == LAG_TARGET_MS
    assert manifest["config"]["driver_memory"] and manifest["config"]["shuffle_partitions"]
    assert manifest["timing_semantics_version"] >= 1
    assert manifest["run_id"].startswith("bench-20260918-120000-stream-throughput-aaaaaaaa")


def test_a_record_is_never_overwritten(tmp_path: Path) -> None:
    run = _record()
    run.write(tmp_path)
    with pytest.raises(record.RecordError, match="never overwritten"):
        run.write(tmp_path)


def test_only_a_valid_run_on_a_clean_worktree_is_publishable() -> None:
    assert _record().publishable
    assert _record(status="FAIL").publishable
    assert not _record(status="INVALID").publishable
    assert not _record(dirty_worktree=True).publishable
    assert not _record(status="FAIL", dirty_worktree=True).publishable


@pytest.mark.parametrize(
    ("status", "dirty", "expected"),
    [
        ("PASS", False, True),
        ("FAIL", False, True),  # a missed target is evidence: publishable, never hidden
        ("INVALID", False, False),
        ("INVALID", True, False),
        ("PASS", True, False),
    ],
)
def test_every_written_record_carries_its_publishable_flag(
    tmp_path: Path, status: str, dirty: bool, expected: bool
) -> None:
    written = json.loads(_record(status=status, dirty_worktree=dirty).write(tmp_path).read_text())
    assert written["publishable"] is expected


def test_the_publishable_flag_cannot_disagree_with_the_status_it_is_written_beside(
    tmp_path: Path,
) -> None:
    run = _record(status="PASS")
    run.status = "INVALID"  # e.g. judged after construction
    assert json.loads(run.write(tmp_path).read_text())["publishable"] is False


def test_every_report_section_is_scoped_to_the_run_id() -> None:
    run = _record(
        measured={
            "throughput_window": {"throughput_events_per_s": 5012.5, "lag_max_ms": 4200},
            "throughput_offered": {"rate_events_per_s": 5000.0},
            "gold_builds": [{"build_id": 0, "started_ms": 1, "finished_ms": 2, "exit_code": 0}],
            "resources": {
                "consumer": {"peak_rss_mib": 1.0, "mean_cpu_percent": 2.0, "max_cpu_percent": 3.0}
            },
        }
    )
    text = record.render_report(run)
    headings = [line for line in text.splitlines() if line.startswith("## ")]
    assert headings
    assert all(f"run_id: {run.run_id}" in heading for heading in headings)


# ---------------------------------------------------------- the producers ---


class _Delivered:
    """A delivery report as confluent-kafka hands one to `on_delivery` (its read-only accessors).

    Like the real one it carries the value but NOT the headers: confluent-kafka 2.15.1 returns
    None from `headers()` in every delivery report (see the real-client test below). An earlier
    fake returned the produced headers, which is how a send time carried in a header passed here
    and left every real report unstamped (bench-20260918-133511)."""

    def __init__(self, *, kind: int, stamp: int, value: bytes = b"v0", partition: int = 0) -> None:
        self._kind, self._stamp, self._partition, self._value = kind, stamp, partition, value

    def topic(self) -> str:
        return "tx.scored.v1"

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return 41

    def value(self) -> bytes:
        return self._value

    def timestamp(self) -> tuple[int, int]:
        return self._kind, self._stamp

    def headers(self) -> None:
        return None


def test_a_delivery_report_bounds_the_clock_and_records_the_partition() -> None:
    import time

    from benchmarks.stream_throughput.producer import LOG_APPEND_TIME_TYPE, TimedLedger

    ledger = TimedLedger()
    now_ms = time.time_ns() // 1_000_000
    ledger.expect(b"v0", (now_ms - 5) * 1000)
    ledger.on_delivery(None, _Delivered(kind=LOG_APPEND_TIME_TYPE, stamp=now_ms, value=b"v0"))
    assert ledger.bounds.samples == 1
    assert ledger.bounds.upper_ms == pytest.approx(6.0)
    assert ledger.delivered_by_partition == {"tx.scored.v1[0]": 1}
    assert ledger.max_offset == {"tx.scored.v1[0]": 41}
    assert ledger.awaiting == 0
    ledger.expect(b"v1", now_ms * 1000)
    ledger.on_delivery(None, _Delivered(kind=1, stamp=now_ms, value=b"v1"))
    assert ledger.not_log_append_time == 1
    assert ledger.awaiting == 0, "a report of any kind releases its send time"
    ledger.on_delivery(None, _Delivered(kind=LOG_APPEND_TIME_TYPE, stamp=now_ms, value=b"nope"))
    assert ledger.unstamped == 1


def test_every_report_is_stamped_although_reports_carry_no_headers() -> None:
    """The regression: a stream of header-less reports, as the real client delivers them, all
    bound the clock."""
    import time

    from benchmarks.stream_throughput.producer import LOG_APPEND_TIME_TYPE, TimedLedger

    ledger = TimedLedger()
    now_ms = time.time_ns() // 1_000_000
    for i in range(100):
        ledger.expect(f"event-{i}".encode(), (now_ms - 3) * 1000)
    for i in range(100):
        ledger.on_delivery(
            None, _Delivered(kind=LOG_APPEND_TIME_TYPE, stamp=now_ms, value=f"event-{i}".encode())
        )
    assert (ledger.bounds.samples, ledger.unstamped, ledger.awaiting) == (100, 0, 0)
    assert ledger.bounds.within(500.0)
    ledger.expect(b"event-0", now_ms * 1000)
    ledger.expect(b"event-0", now_ms * 1000 + 7)
    assert ledger.duplicate_values == 1 and ledger.awaiting == 1


def test_a_failed_delivery_releases_its_send_time_without_bounding_the_clock() -> None:
    from benchmarks.stream_throughput.producer import LOG_APPEND_TIME_TYPE, TimedLedger

    class _Err:
        def name(self) -> str:
            return "_MSG_TIMED_OUT"

        def str(self) -> str:
            return "timed out"

    ledger = TimedLedger()
    ledger.expect(b"v0", 1_000_000)
    ledger.on_delivery(_Err(), _Delivered(kind=LOG_APPEND_TIME_TYPE, stamp=2_000, value=b"v0"))
    assert (ledger.awaiting, ledger.bounds.samples) == (0, 0)


def test_the_real_client_hands_back_the_value_the_send_time_is_found_by() -> None:
    """Against confluent-kafka itself, no broker: a record to an unreachable address times out,
    and its delivery report still carries the exact value produced -- what `TimedLedger` looks
    the send time up by. (Its `headers()` is None, which is why no header is relied on.)"""
    confluent_kafka = pytest.importorskip("confluent_kafka", reason="confluent-kafka not installed")
    reports: list[tuple[Any, Any, Any]] = []

    def on_delivery(err: Any, msg: Any) -> None:
        reports.append((err, msg.value(), msg.headers()))

    producer = confluent_kafka.Producer(
        {
            "bootstrap.servers": "127.0.0.1:1",
            "message.timeout.ms": 200,
            "log_level": 0,
            "on_delivery": on_delivery,
        }
    )
    producer.produce("t", value=b"the-value", key=b"k", headers=[("h", b"1")])
    assert producer.flush(10) == 0
    ((err, value, _headers),) = reports
    assert err is not None and value == b"the-value"


def test_the_events_are_valid_released_events_that_refer_to_each_other() -> None:
    from benchmarks.stream_throughput.events import EventFactory, mix_pattern, score_templates

    from trace_core.contracts.publish import _models

    factory = EventFactory(
        templates=score_templates(4, seed=7),
        seed=7,
        worker=1,
        run_nonce="0123456789abcdef",
        account_pool=1_000,
    )
    # With nothing scored yet, an outcome slot produces a scored transaction instead, counted.
    topic, first = factory.build("tx.authorization.v1", T0)
    assert topic == "tx.scored.v1" and factory.substituted == 1
    pattern = mix_pattern(DEFAULT_MIX, 7)
    assert sorted(pattern) == sorted(t for t, n in DEFAULT_MIX.items() for _ in range(n))
    payload = json.loads(first)["payload"]
    scored: dict[str, dict[str, Any]] = {payload["transaction_id"]: payload}
    seen_ids: set[str] = {payload["transaction_id"]}
    for k in range(200):
        topic, value = factory.build(pattern[k % len(pattern)], T0 + k)
        event = json.loads(value)
        _models()[topic].model_validate_json(value)
        assert event["envelope"]["producer"].startswith("trace-load-stream@")
        payload = event["payload"]
        if topic == "tx.scored.v1":
            assert payload["transaction_id"] not in seen_ids
            seen_ids.add(payload["transaction_id"])
            scored[payload["transaction_id"]] = payload
        elif topic == "tx.authorization.v1":
            source = scored[payload["transaction_id"]]
            assert payload["account_id"] == source["account_id"]
    assert factory.substituted == 1
