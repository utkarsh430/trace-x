"""`make load-stream`: the Phase 3 stream throughput and outage benchmark (`P3.stream-throughput`).

**Phases**, every length declared in `RunConfig` before the run and recorded:

1. *Start.* Preflight the broker (topics exist, stamp LogAppendTime, partition counts), start the
   producer workers at the fixed rate, then the consumer (real Bronze and Silver queries, one JVM),
   then Gold builds back to back (`services.stream.gold build`, the production entrypoint).
2. *Throughput.* A warm-up of at least `warmup_min_s`, ending at the first Silver commit whose lag
   is below the target, and at most `warmup_max_s`; then the measured window of `measured_s`. The
   window runs whatever the lag, so a pipeline that never reaches steady state is measured failing.
3. *Outage.* `pre_outage_s` of steady traffic, then the consumer is stopped (SIGTERM, its queries
   stop cleanly) and restarted from its checkpoints exactly `OUTAGE_S` after the stop, while the
   producers keep offering the rate. Observed until lag has recovered and held, or until
   `recovery_bound_s` plus the hold has passed.
4. *Evaluation.* The lag series is recomputed from the complete Delta logs after the run -- the
   live monitor only decides when the window starts and when observation may end -- and judged
   (`verdict`). A record is written to `eval/manifest/`, and the report only when the run is valid
   and the worktree clean.

**Exit codes.** 0 every target met; 1 a target missed (recorded and reported as found); 2 the
harness could not measure (refused preflight, a producer or consumer refusal, an integrity failure
-- the run is INVALID and no report is written); 3 a valid run on a dirty worktree (recorded, not
publishable, no report).

Nothing here needs more than the local `streaming` profile: `make up-streaming`, then
`make kafka-topics`.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import multiprocessing as mp
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any, Final

from benchmarks.stream_throughput import delta_log, record, verdict
from benchmarks.stream_throughput.clock import OffsetBounds
from benchmarks.stream_throughput.lag import (
    LagSample,
    LagTracker,
    PartitionKey,
    commit_order,
    detect_recovery,
    lag_samples,
    window_stats,
)
from benchmarks.stream_throughput.spec import LAG_TARGET_MS, OUTAGE_S, RunConfig
from benchmarks.stream_throughput.verdict import GoldBuild, Verdict

ROOT: Final = Path(__file__).resolve().parents[2]
EXIT_PASS: Final = 0
EXIT_TARGET_MISSED: Final = 1
EXIT_HARNESS: Final = 2
EXIT_UNPUBLISHABLE: Final = 3
KAFKA_BOOTSTRAP_ENV: Final = "TRACE_KAFKA_BOOTSTRAP"
LAKE_ROOT_ENV: Final = "TRACE_DELTA_ROOT"
READY_TIMEOUT_S: Final = 180.0
RESOURCE_EVERY_S: Final = 5.0
GOLD_EXIT_REFUSED: Final = 2


class HarnessError(RuntimeError):
    """The harness could not measure what it claims to: the run is not evidence of anything."""


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _say(message: str) -> None:
    print(f"  [{dt.datetime.now(dt.UTC):%H:%M:%S}] {message}", flush=True)


# ------------------------------------------------------------------- broker ---


def describe_broker(bootstrap: str, topics: Sequence[str]) -> dict[str, dict[str, Any]]:
    """Per topic: partition count and timestamp type. Refused unless every topic exists and is
    stamped with LogAppendTime (PHASE3_PLAN Q3): with CreateTime the lag would be a producer's
    clock minus the host's."""
    from confluent_kafka.admin import AdminClient, ConfigResource

    admin: Any = AdminClient({"bootstrap.servers": bootstrap})
    metadata = admin.list_topics(timeout=15)
    described: dict[str, dict[str, Any]] = {}
    for topic in topics:
        meta = metadata.topics.get(topic)
        if meta is None or meta.error is not None or not meta.partitions:
            raise HarnessError(
                f"topic {topic} is not on the broker at {bootstrap}; run `make kafka-topics`"
            )
        resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
        entries = admin.describe_configs([resource], request_timeout=15)[resource].result(
            timeout=15
        )
        stamp = entries["message.timestamp.type"].value
        if stamp != "LogAppendTime":
            raise HarnessError(f"{topic} stamps {stamp}, not LogAppendTime (PHASE3_PLAN Q3)")
        described[topic] = {
            "partitions": len(meta.partitions),
            "message.timestamp.type": stamp,
            "retention.bytes": entries["retention.bytes"].value,
        }
    return described


def offset_range(
    bootstrap: str, partitions: Sequence[PartitionKey]
) -> dict[PartitionKey, tuple[int, int]]:
    """Each partition's (earliest retained, end) offsets; the end is the next offset the broker
    will assign. Read without joining a group or committing anything."""
    from confluent_kafka import Consumer, TopicPartition

    client: Any = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"trace-load-stream-watermarks-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    try:
        found: dict[PartitionKey, tuple[int, int]] = {}
        for key in partitions:
            low, high = client.get_watermark_offsets(
                TopicPartition(key.topic, key.partition), timeout=15
            )
            found[key] = (int(low), int(high))
        return found
    finally:
        client.close()


def watermarks(bootstrap: str, partitions: Sequence[PartitionKey]) -> dict[PartitionKey, int]:
    return {key: end for key, (_low, end) in offset_range(bootstrap, partitions).items()}


# --------------------------------------------------------------- processes ---


class ConsumerProcess:
    """The consumer under test, one generation per start. Stopped with SIGTERM (its queries stop
    cleanly); killed with its whole process group, JVM included, only if it will not stop."""

    def __init__(self, config: RunConfig, *, bootstrap: str, lake_root: Path, logs: Path) -> None:
        self.config = config
        self.bootstrap = bootstrap
        self.lake_root = lake_root
        self.logs = logs
        self.process: subprocess.Popen[bytes] | None = None
        self.generation = 0
        self.events: list[dict[str, Any]] = []

    def command(self) -> list[str]:
        c = self.config
        args = [
            sys.executable,
            "-m",
            "benchmarks.stream_throughput.consumer",
            "--bootstrap",
            self.bootstrap,
            "--bronze-trigger-s",
            str(c.bronze_trigger_s),
            "--silver-trigger-s",
            str(c.silver_trigger_s),
            "--master",
            c.master,
            "--driver-memory",
            c.driver_memory,
            "--shuffle-partitions",
            str(c.shuffle_partitions),
        ]
        if c.max_offsets_per_trigger is not None:
            args += ["--max-offsets-per-trigger", str(c.max_offsets_per_trigger)]
        for topic in sorted(c.mix):
            args += ["--topic", topic]
        return args

    def start(self) -> int:
        self.generation += 1
        log = (self.logs / f"consumer-{self.generation}.log").open("ab")
        env = {
            **os.environ,
            LAKE_ROOT_ENV: str(self.lake_root),
            KAFKA_BOOTSTRAP_ENV: self.bootstrap,
        }
        self.process = subprocess.Popen(  # noqa: S603 -- this interpreter, our own modules
            self.command(),
            cwd=ROOT,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        log.close()
        started = _now_ms()
        self.events.append({"event": "start", "generation": self.generation, "at_ms": started})
        return started

    def exit_code(self) -> int | None:
        return None if self.process is None else self.process.poll()

    def stop(self, timeout_s: float) -> tuple[int, int | None, bool]:
        """(signalled at, exit code, killed)."""
        if self.process is None:
            raise HarnessError("the consumer was never started")
        signalled = _now_ms()
        killed = False
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=timeout_s)
            except subprocess.TimeoutExpired:
                killed = True
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=30)
        code = self.process.returncode
        self.events.append(
            {
                "event": "stop",
                "generation": self.generation,
                "signalled_ms": signalled,
                "exited_ms": _now_ms(),
                "exit_code": code,
                "killed": killed,
            }
        )
        return signalled, code, killed


class GoldLoop(threading.Thread):
    """`services.stream.gold build`, back to back, each build a fresh JVM as in production."""

    def __init__(self, config: RunConfig, *, lake_root: Path, logs: Path) -> None:
        super().__init__(name="gold-loop", daemon=True)
        self.config = config
        self.lake_root = lake_root
        self.logs = logs
        self.builds: list[GoldBuild] = []
        self.started_ms: int | None = None
        self.stop_requested = threading.Event()
        self.current: subprocess.Popen[bytes] | None = None
        self.current_started_ms: int | None = None
        self.refused: str | None = None
        self._abandoned: set[int] = set()
        self._lock = threading.Lock()

    def run(self) -> None:
        self.started_ms = _now_ms()
        index = 0
        while not self.stop_requested.is_set():
            index += 1
            started = _now_ms()
            out_path = self.logs / f"gold-{index}.out"
            with out_path.open("wb") as out, (self.logs / f"gold-{index}.log").open("wb") as err:
                env = {**os.environ, LAKE_ROOT_ENV: str(self.lake_root)}
                process = subprocess.Popen(  # noqa: S603 -- this interpreter, our own modules
                    [
                        sys.executable,
                        "-m",
                        "services.stream.gold",
                        "build",
                        "--driver-memory",
                        self.config.gold_driver_memory,
                    ],
                    cwd=ROOT,
                    env=env,
                    stdout=out,
                    stderr=err,
                    start_new_session=True,
                )
                with self._lock:
                    self.current, self.current_started_ms = process, started
                code = process.wait()
            finished = _now_ms()
            with self._lock:
                self.current = self.current_started_ms = None
                if process.pid in self._abandoned:
                    return  # `abandon` recorded it
            summary = _last_json_line(out_path)
            lag = summary.get("lag_ms") if summary else None
            build_id = summary.get("build_id") if summary else None
            self.builds.append(
                GoldBuild(
                    started_ms=started,
                    finished_ms=finished,
                    exit_code=code,
                    lag_ms=int(lag) if isinstance(lag, int) else None,
                    build_id=int(build_id) if isinstance(build_id, int) else None,
                )
            )
            if code == GOLD_EXIT_REFUSED:
                self.refused = f"gold build {index} was refused (exit 2); see {out_path.parent}"
                return
            wait_until = time.time() + self.config.gold_min_interval_s
            while time.time() < wait_until and not self.stop_requested.is_set():
                time.sleep(0.5)

    def running_since(self) -> int | None:
        with self._lock:
            return self.current_started_ms

    def abandon(self) -> None:
        """Kill the build in flight, recorded as abandoned (its lag is at least its age)."""
        with self._lock:
            process, started = self.current, self.current_started_ms
        if process is not None and started is not None and process.poll() is None:
            with self._lock:
                self._abandoned.add(process.pid)
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=30)
            self.builds.append(
                GoldBuild(
                    started_ms=started,
                    finished_ms=None,
                    exit_code=None,
                    lag_ms=None,
                    abandoned_ms=_now_ms(),
                )
            )


def _last_json_line(path: Path) -> dict[str, Any] | None:
    try:
        lines = [line for line in path.read_text().splitlines() if line.strip().startswith("{")]
    except OSError:
        return None
    for line in reversed(lines):
        try:
            parsed = json.loads(line)
        except ValueError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


class LiveMonitor(threading.Thread):
    """Samples lag while the run is live, only to decide when the window starts and when
    observation may end. The published series is recomputed from the complete log afterwards."""

    def __init__(
        self,
        tables: Sequence[delta_log.WatchedTable],
        expected: Sequence[PartitionKey],
        every_s: float,
    ) -> None:
        super().__init__(name="lag-monitor", daemon=True)
        self.readers = [delta_log.CommitReader(t) for t in tables]
        self.tracker = LagTracker(expected)
        self.every_s = every_s
        self.samples: list[LagSample] = []
        self.error: str | None = None
        self.halt = threading.Event()
        self._lock = threading.Lock()

    def run(self) -> None:
        while not self.halt.is_set():
            try:
                commits = [c for reader in self.readers for c in reader.poll()]
                fresh = [self.tracker.observe(c) for c in sorted(commits, key=commit_order)]
            except Exception as exc:  # recorded; the run is invalid without its sampler
                self.error = f"{type(exc).__name__}: {exc}"
                return
            with self._lock:
                self.samples.extend(fresh)
            self.halt.wait(self.every_s)

    def since(self, at_ms: int) -> list[LagSample]:
        with self._lock:
            return [s for s in self.samples if s.at_ms >= at_ms]


class ProducerFleet:
    """The worker processes and the thread that drains their results."""

    def __init__(self, config: RunConfig, *, bootstrap: str, run_nonce: str) -> None:
        self.config = config
        self.context = mp.get_context("spawn")
        self.queue: Any = self.context.Queue()
        self.start_at: Any = self.context.Value("d", 0.0)
        self.stop_event: Any = self.context.Event()
        self.processes = [
            self.context.Process(
                target=_worker_entry,
                args=(
                    w,
                    config.model_dump_json(),
                    bootstrap,
                    run_nonce,
                    self.start_at,
                    self.stop_event,
                    self.queue,
                ),
                name=f"load-stream-producer-{w}",
                daemon=True,
            )
            for w in range(config.producer_workers)
        ]
        self.ready: set[int] = set()
        self.buckets: dict[float, int] = {}
        self.behind: dict[float, float] = {}
        self.finals: dict[int, dict[str, Any]] = {}
        self.errors: list[str] = []
        self._drainer = threading.Thread(target=self._drain, name="producer-drain", daemon=True)
        self._lock = threading.Lock()
        self._draining = True

    def start(self) -> None:
        for process in self.processes:
            process.start()
        self._drainer.start()

    def _drain(self) -> None:
        import queue as queue_module

        while self._draining or not self.queue.empty():
            try:
                message = self.queue.get(timeout=0.5)
            except queue_module.Empty:
                continue
            kind, worker = message[0], message[1]
            with self._lock:
                if kind == "ready":
                    self.ready.add(worker)
                elif kind == "stats":
                    bucket, counts, behind = float(message[2]), message[3], float(message[4])
                    key = round(bucket, 3)
                    self.buckets[key] = self.buckets.get(key, 0) + sum(counts.values())
                    self.behind[key] = max(self.behind.get(key, 0.0), behind)
                elif kind == "final":
                    self.finals[worker] = message[2]
                elif kind == "error":
                    self.errors.append(f"worker {worker}: {message[2]}")

    def wait_ready(self, timeout_s: float) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            with self._lock:
                if self.errors:
                    raise HarnessError(f"a producer worker failed to start: {self.errors[0]}")
                if len(self.ready) == len(self.processes):
                    return
            time.sleep(0.2)
        raise HarnessError(f"producer workers not ready within {timeout_s} s")

    def go(self) -> float:
        t0 = time.time() + 1.0
        self.start_at.value = t0
        return t0

    def stop(self, timeout_s: float = 300.0) -> None:
        self.stop_event.set()
        deadline = time.time() + timeout_s
        for process in self.processes:
            process.join(timeout=max(1.0, deadline - time.time()))
        while time.time() < deadline:
            with self._lock:
                if len(self.finals) + len(self.errors) >= len(self.processes):
                    break
            time.sleep(0.2)
        self._draining = False
        self._drainer.join(timeout=10)
        for process in self.processes:
            if process.is_alive():
                process.kill()
                self.errors.append(f"{process.name} did not exit and was killed")

    def snapshot(self) -> tuple[dict[float, int], dict[float, float], list[str]]:
        with self._lock:
            return dict(self.buckets), dict(self.behind), list(self.errors)


def _worker_entry(*args: Any) -> None:
    from benchmarks.stream_throughput.producer import worker_main

    worker_main(*args)


class ResourceSampler(threading.Thread):
    """Peak resident memory and CPU of each role, JVM children included, from `ps`."""

    def __init__(self, roles: Callable[[], Mapping[str, Sequence[int]]]) -> None:
        super().__init__(name="resource-sampler", daemon=True)
        self.roles = roles
        self.peak_rss_mib: dict[str, float] = {}
        self.cpu_samples: dict[str, list[float]] = {}
        self.halt = threading.Event()

    def run(self) -> None:
        binary = shutil.which("ps")
        if binary is None:
            return
        while not self.halt.is_set():
            done = subprocess.run(  # noqa: S603 -- resolved binary, literal arguments
                [binary, "-A", "-o", "pid=,ppid=,rss=,%cpu="],
                capture_output=True,
                text=True,
                check=False,
            )
            table: dict[int, tuple[int, int, float]] = {}
            for line in done.stdout.splitlines():
                parts = line.split()
                if len(parts) == 4:
                    try:
                        table[int(parts[0])] = (int(parts[1]), int(parts[2]), float(parts[3]))
                    except ValueError:
                        continue
            children: dict[int, list[int]] = {}
            for pid, (ppid, _rss, _cpu) in table.items():
                children.setdefault(ppid, []).append(pid)
            for role, roots in self.roles().items():
                rss = cpu = 0.0
                stack = list(roots)
                seen: set[int] = set()
                while stack:
                    pid = stack.pop()
                    if pid in seen or pid not in table:
                        continue
                    seen.add(pid)
                    rss += table[pid][1] / 1024
                    cpu += table[pid][2]
                    stack.extend(children.get(pid, []))
                if seen:
                    self.peak_rss_mib[role] = max(self.peak_rss_mib.get(role, 0.0), rss)
                    self.cpu_samples.setdefault(role, []).append(cpu)
            self.halt.wait(RESOURCE_EVERY_S)

    def summary(self) -> dict[str, dict[str, float]]:
        return {
            role: {
                "peak_rss_mib": round(self.peak_rss_mib.get(role, 0.0), 1),
                "mean_cpu_percent": round(sum(v) / len(v), 1) if v else 0.0,
                "max_cpu_percent": round(max(v), 1) if v else 0.0,
            }
            for role, v in self.cpu_samples.items()
        }


# -------------------------------------------------------------- the phases ---


def _sleep_until(deadline_ms: int, *, check: Callable[[], None]) -> None:
    while _now_ms() < deadline_ms:
        check()
        time.sleep(min(1.0, max(0.0, (deadline_ms - _now_ms()) / 1000)))


def _first_below(samples: Sequence[LagSample]) -> LagSample | None:
    return next((s for s in samples if s.lag_ms is not None and s.lag_ms < LAG_TARGET_MS), None)


def _parser() -> argparse.ArgumentParser:
    defaults = RunConfig()
    parser = argparse.ArgumentParser(
        prog="make load-stream ARGS=...", description=__doc__.split("\n\n")[0]
    )
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get(KAFKA_BOOTSTRAP_ENV)
        or f"localhost:{os.environ.get('KAFKA_PORT', '9092')}",
    )
    parser.add_argument(
        "--lake-parent",
        type=Path,
        default=Path(os.environ.get("TMPDIR", "/tmp")) / "trace-x-load-stream",  # noqa: S108
        help="a fresh lake is created under it per run (default: outside the repository)",
    )
    parser.add_argument("--record-dir", type=Path, default=None)
    for name, field in RunConfig.model_fields.items():
        if name == "mix":
            continue
        flag = "--" + name.replace("_", "-")
        default = getattr(defaults, name)
        if isinstance(default, bool):
            parser.add_argument(flag, action=argparse.BooleanOptionalAction, default=default)
        elif name == "max_offsets_per_trigger":
            parser.add_argument(flag, type=int, default=default)
        else:
            parser.add_argument(flag, type=type(default), default=default, help=field.description)
    parser.add_argument(
        "--mix",
        default=None,
        help='JSON {topic: weight}, e.g. \'{"tx.scored.v1": 14, "tx.authorization.v1": 5}\'',
    )
    return parser


def config_from_args(args: argparse.Namespace) -> RunConfig:
    values: dict[str, Any] = {
        name: getattr(args, name) for name in RunConfig.model_fields if name != "mix"
    }
    if args.mix is not None:
        values["mix"] = json.loads(args.mix)
    return RunConfig.model_validate(values)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config = config_from_args(args)
    except ValueError as exc:
        print(f"load-stream: refused: {exc}", file=sys.stderr)
        return EXIT_HARNESS
    from data.generator.record import git_commit_sha

    from trace_core.domain.errors import NonConformantFeatureSetError
    from trace_core.features.spec import require_served_conformance

    try:
        require_served_conformance("A stream benchmark record")
    except NonConformantFeatureSetError as exc:
        print(f"load-stream: refused: {exc}", file=sys.stderr)
        return EXIT_HARNESS

    if config.gold and (missing := _gold_sources_outside(config.mix)):
        print(
            f"load-stream: refused: Gold reads {missing}, which the mix does not produce; add "
            f"them or run --no-gold (and the Gold target is then unevaluated)",
            file=sys.stderr,
        )
        return EXIT_HARNESS

    started = dt.datetime.now(dt.UTC)
    provenance_at_start = record.git_facts()
    run_id = record.new_run_id(started, git_commit_sha())
    run_nonce = uuid.uuid4().hex
    lake_root = (args.lake_parent / run_id).resolve()
    logs = lake_root.parent / f"{run_id}-logs"
    topics = sorted(config.mix)
    notes: list[str] = []
    consumer: ConsumerProcess | None = None
    fleet: ProducerFleet | None = None
    gold: GoldLoop | None = None
    monitor: LiveMonitor | None = None
    sampler: ResourceSampler | None = None
    phases: dict[str, int | None] = {}
    consumer_failure: str | None = None
    consumer_down_s: float | None = None
    harness_error: str | None = None
    broker: dict[str, dict[str, Any]] = {}
    expected: list[PartitionKey] = []
    starting_low: dict[PartitionKey, int] = {}
    starting_end: dict[PartitionKey, int] = {}
    toolchain_record: list[dict[str, Any]] = []
    print(f"load-stream {run_id}")
    try:
        from trace_core.stream import toolchain

        findings = toolchain.inspect_toolchain()
        toolchain_record = [asdict(f) for f in findings]
        broken = [f for f in findings if not f.ok]
        if broken:
            raise HarnessError(
                "the Phase 3 toolchain does not match its pins: "
                + "; ".join(f"{f.component}: {f.detail}" for f in broken)
            )
        broker = describe_broker(args.bootstrap, topics)
        expected = [PartitionKey(t, p) for t in topics for p in range(int(broker[t]["partitions"]))]
        starting = offset_range(args.bootstrap, expected)
        starting_low = {key: low for key, (low, _end) in starting.items()}
        starting_end = {key: end for key, (_low, end) in starting.items()}
        preexisting = sum(starting_end[k] - starting_low[k] for k in expected)
        _say(
            f"broker {args.bootstrap}: {len(expected)} partitions over {topics}; "
            f"{preexisting} retained records predate the run (Bronze reads from earliest)"
        )
        if lake_root.exists():
            raise HarnessError(f"{lake_root} already exists; a run always starts on a fresh lake")
        lake_root.mkdir(parents=True)
        logs.mkdir(parents=True, exist_ok=True)
        _say(f"lake {lake_root}; logs {logs}")

        tables = delta_log.watched_tables(lake_root, topics)
        monitor = LiveMonitor(tables, expected, config.poll_interval_s)
        fleet = ProducerFleet(config, bootstrap=args.bootstrap, run_nonce=run_nonce)
        consumer = ConsumerProcess(config, bootstrap=args.bootstrap, lake_root=lake_root, logs=logs)
        gold = GoldLoop(config, lake_root=lake_root, logs=logs) if config.gold else None

        def roles() -> dict[str, list[int]]:
            # Each role is a process and all its descendants (a JVM is a child of its Python
            # driver), so `all` is the harness plus everything it started.
            found: dict[str, list[int]] = {"all": [os.getpid()]}
            if fleet is not None:
                found["producers"] = [p.pid for p in fleet.processes if p.pid is not None]
            if consumer is not None and consumer.process is not None:
                found["consumer"] = [consumer.process.pid]
            if gold is not None and gold.current is not None:
                found["gold"] = [gold.current.pid]
            return found

        sampler = ResourceSampler(roles)
        sampler.start()

        _say(f"starting {config.producer_workers} producer workers (scoring templates) ...")
        fleet.start()
        fleet.wait_ready(READY_TIMEOUT_S)
        t0 = fleet.go()
        phases["producers_started_ms"] = int(t0 * 1000)
        _say(f"offering {config.rate_events_per_s} events/s from t0")
        consumer_started = consumer.start()
        phases["consumer_started_ms"] = consumer_started
        monitor.start()

        def check() -> None:
            nonlocal consumer_failure
            _, _, errors = fleet.snapshot() if fleet is not None else ({}, {}, [])
            if errors:
                raise HarnessError(f"a producer worker failed: {errors[0]}")
            if monitor is not None and monitor.error is not None:
                raise HarnessError(f"the lag sampler failed: {monitor.error}")
            code = consumer.exit_code() if consumer is not None else None
            if code is not None:
                if code == _consumer_refused_code():
                    raise HarnessError(f"the consumer refused to start (exit {code}); see {logs}")
                consumer_failure = (
                    f"the consumer exited with code {code} while it should have been running "
                    f"(a failed query exits 3); see {logs}"
                )
                raise _ConsumerDiedError(consumer_failure)
            if gold is not None and gold.refused is not None:
                raise HarnessError(gold.refused)

        # Gold starts once Silver has created the tables it reads.
        if gold is not None:
            _wait_for_gold_sources(lake_root, check)
            gold.start()
            _say("Gold builds running back to back")

        # --- phase 1: throughput ---
        warm_min = consumer_started + config.warmup_min_s * 1000
        warm_max = consumer_started + config.warmup_max_s * 1000
        _sleep_until(warm_min, check=check)
        while _now_ms() < warm_max:
            check()
            if _first_below(monitor.since(warm_min)) is not None:
                break
            time.sleep(1.0)
        steady = _first_below(monitor.since(warm_min))
        measured_start = _now_ms()
        phases["measured_start_ms"] = measured_start
        phases["warmup_reached_steady_state"] = int(steady is not None)
        _say(
            "measured window starts"
            + ("" if steady is not None else " WITHOUT steady state (warm-up maximum reached)")
        )
        _sleep_until(measured_start + config.measured_s * 1000, check=check)
        measured_end = _now_ms()
        phases["measured_end_ms"] = measured_end
        _say("measured window ends")

        # --- phase 2: outage ---
        _sleep_until(measured_end + config.pre_outage_s * 1000, check=check)
        signalled, code, killed = consumer.stop(config.consumer_stop_timeout_s)
        phases["outage_start_ms"] = signalled
        if code not in (0, None):
            notes.append(f"the consumer exited {code} when stopped for the outage")
        if killed:
            notes.append("the consumer did not stop within its timeout and was killed")
        _say(f"OUTAGE: consumer stopped (exit {code}); restarting in {OUTAGE_S} s")
        restart_at = signalled + OUTAGE_S * 1000

        def outage_check() -> None:
            _, _, errors = fleet.snapshot() if fleet is not None else ({}, {}, [])
            if errors:
                raise HarnessError(f"a producer worker failed: {errors[0]}")

        _sleep_until(restart_at, check=outage_check)
        restored = consumer.start()
        phases["restored_ms"] = restored
        consumer_down_s = (restored - signalled) / 1000
        _say("consumer restarted from its checkpoints; observing recovery")
        observe_until = restored + (config.recovery_bound_s + config.recovery_hold_s) * 1000
        while _now_ms() < observe_until:
            check()
            live = detect_recovery(
                monitor.since(restored),
                restored_ms=restored,
                observed_until_ms=_now_ms(),
                target_ms=LAG_TARGET_MS,
                hold_ms=config.recovery_hold_s * 1000,
                bound_ms=config.recovery_bound_s * 1000,
            )
            if live.recovered_at_ms is not None:
                # A margin past the hold, so the published recomputation sees the same commits.
                _sleep_until(_now_ms() + int(config.poll_interval_s * 2000) + 5000, check=check)
                break
            time.sleep(2.0)
        phases["observed_until_ms"] = _now_ms()
    except _ConsumerDiedError:
        notes.append(consumer_failure or "the consumer died")
        phases.setdefault("observed_until_ms", _now_ms())
    except HarnessError as exc:
        harness_error = str(exc)
    except KeyboardInterrupt:
        harness_error = "interrupted"
    finally:
        _say("stopping ...")
        if gold is not None and gold.is_alive():
            gold.stop_requested.set()
            _finish_gold(gold, phases)
        if fleet is not None:
            fleet.stop()
        if consumer is not None and consumer.exit_code() is None and consumer.process is not None:
            consumer.stop(config.consumer_stop_timeout_s)
        if monitor is not None:
            monitor.halt.set()
        if sampler is not None:
            sampler.halt.set()
    finished = dt.datetime.now(dt.UTC)

    return _evaluate_and_record(
        config=config,
        args=args,
        run_id=run_id,
        started=started,
        finished=finished,
        lake_root=lake_root,
        logs=logs,
        topics=topics,
        expected=expected,
        broker=broker,
        starting_low=starting_low,
        starting_end=starting_end,
        phases=phases,
        fleet=fleet,
        consumer=consumer,
        gold=gold,
        sampler=sampler,
        consumer_failure=consumer_failure,
        consumer_down_s=consumer_down_s,
        harness_error=harness_error,
        notes=notes,
        toolchain_record=toolchain_record,
        provenance_at_start=provenance_at_start,
    )


def _gold_sources_outside(mix: Mapping[str, int]) -> list[str]:
    from trace_core.stream.gold_plan import GOLD_SOURCES
    from trace_core.stream.silver_rules import silver_topic

    produced = {str(silver_topic(topic).table) for topic in mix}
    return sorted(str(ref) for ref in GOLD_SOURCES if str(ref) not in produced)


class _ConsumerDiedError(Exception):
    """The consumer exited while it should have been running: a recorded FAIL, not a harness
    error."""


def _consumer_refused_code() -> int:
    from services.stream.bronze import EXIT_REFUSED

    return int(EXIT_REFUSED)


def _wait_for_gold_sources(lake_root: Path, check: Callable[[], None]) -> None:
    from trace_core.stream.gold_plan import GOLD_SOURCES
    from trace_core.stream.lake import LakeConfig

    lake = LakeConfig(root=lake_root)
    logs = [ref.local_path(lake) / delta_log.LOG_DIR / f"{0:020d}.json" for ref in GOLD_SOURCES]
    deadline = time.time() + 600
    while not all(path.is_file() for path in logs):
        check()
        if time.time() > deadline:
            raise HarnessError("Silver did not create Gold's source tables within 600 s")
        time.sleep(1.0)


def _finish_gold(gold: GoldLoop, phases: Mapping[str, int | None]) -> None:
    """Let a build that started by the end of the measured window finish (its lag is part of the
    window's freshness) until it is certainly over the 60-minute maximum; abandon any other."""
    from benchmarks.stream_throughput.spec import GOLD_MAX_LAG_MS

    window_end = phases.get("measured_end_ms")
    since = gold.running_since()
    if since is not None and window_end is not None and since <= window_end:
        _say("waiting for the Gold build that spans the measured window ...")
        deadline = since + GOLD_MAX_LAG_MS + 1000
        while gold.running_since() == since and _now_ms() < deadline:
            time.sleep(1.0)
    gold.abandon()
    gold.join(timeout=60)


def _evaluate_and_record(
    *,
    config: RunConfig,
    args: argparse.Namespace,
    run_id: str,
    started: dt.datetime,
    finished: dt.datetime,
    lake_root: Path,
    logs: Path,
    topics: Sequence[str],
    expected: Sequence[PartitionKey],
    broker: Mapping[str, Any],
    starting_low: Mapping[PartitionKey, int],
    starting_end: Mapping[PartitionKey, int],
    phases: Mapping[str, int | None],
    fleet: ProducerFleet | None,
    consumer: ConsumerProcess | None,
    gold: GoldLoop | None,
    sampler: ResourceSampler | None,
    consumer_failure: str | None,
    consumer_down_s: float | None,
    harness_error: str | None,
    notes: list[str],
    toolchain_record: list[dict[str, Any]],
    provenance_at_start: tuple[str, bool, str],
) -> int:
    verdicts: list[Verdict] = []
    measured: dict[str, Any] = {"phases": dict(phases), "notes": notes}
    if harness_error is not None:
        verdicts.append(Verdict("harness", verdict.INTEGRITY, False, harness_error))
    try:
        if expected and lake_root.exists():
            tables = delta_log.watched_tables(lake_root, topics)
            commits = delta_log.read_all(tables)
            samples, tracker = lag_samples(commits, expected)
            measured.update(
                _measure(
                    config=config,
                    args=args,
                    samples=samples,
                    tracker=tracker,
                    expected=expected,
                    phases=phases,
                    fleet=fleet,
                    gold=gold,
                    consumer_failure=consumer_failure,
                    consumer_down_s=consumer_down_s,
                    started=started,
                    finished=finished,
                    verdicts=verdicts,
                    commits=len(commits),
                )
            )
    except Exception as exc:  # the evaluation itself failed: the run cannot be judged
        verdicts.append(
            Verdict("evaluation", verdict.INTEGRITY, False, f"{type(exc).__name__}: {exc}")
        )
    status = verdict.overall(verdicts)
    start_sha, start_dirty, start_lock = provenance_at_start
    end_sha, end_dirty, _ = record.git_facts()
    if end_sha != start_sha or end_dirty != start_dirty:
        notes.append(f"the worktree changed during the run: {start_sha} -> {end_sha}")
    lake_bytes = _tree_bytes(lake_root)
    measured["lake_bytes"] = lake_bytes
    measured["resources"] = sampler.summary() if sampler is not None else {}
    measured["consumer_events"] = consumer.events if consumer is not None else []
    measured["preexisting_records"] = {
        str(k): starting_end[k] - starting_low.get(k, 0) for k in starting_end
    }
    run = record.StreamBenchmarkRecord(
        run_id=run_id,
        started_at=started.isoformat(),
        finished_at=finished.isoformat(),
        status=status.value,
        config=json.loads(config.model_dump_json()),
        broker={"bootstrap": args.bootstrap, "topics": dict(broker)},
        toolchain=record.toolchain_facts(toolchain_record),
        host=record.host_facts(),
        lake_root=str(lake_root),
        logs=str(logs),
        verdicts=[asdict(v) for v in verdicts],
        measured=measured,
        git_commit_sha=start_sha,
        # Dirty if the tree was dirty at the start or the end, or moved to another commit during
        # the run: then the recorded commit does not describe everything that was measured.
        dirty_worktree=start_dirty or end_dirty or end_sha != start_sha,
        env_lock_digest=start_lock,
    )
    path = run.write(args.record_dir)
    print()
    for v in verdicts:
        mark = "ok  " if v.passed else "FAIL"
        print(f"  {mark} [{v.kind}] {v.name}: {v.detail}")
    print(f"\n  status: {status.value}")
    print(f"  record: {path}")
    print(f"  lake:   {lake_root} ({lake_bytes / 2**30:.2f} GiB, kept as evidence)")
    if status is verdict.Status.INVALID:
        print("  INVALID: the run did not measure what it claims; no report is written.")
        return EXIT_HARNESS
    if not run.publishable:
        print("  dirty worktree: recorded, NOT publishable, no report written. Commit and re-run.")
        return EXIT_UNPUBLISHABLE
    if args.record_dir is None:
        record.REPORT.write_text(record.render_report(run))
        print(f"  report: {record.REPORT.relative_to(ROOT)}")
    if status is verdict.Status.FAIL:
        print("  a Phase 3 target was missed: recorded and reported as found (CLAUDE.md §17).")
        return EXIT_TARGET_MISSED
    return EXIT_PASS


def _measure(
    *,
    config: RunConfig,
    args: argparse.Namespace,
    samples: Sequence[LagSample],
    tracker: LagTracker,
    expected: Sequence[PartitionKey],
    phases: Mapping[str, int | None],
    fleet: ProducerFleet | None,
    gold: GoldLoop | None,
    consumer_failure: str | None,
    consumer_down_s: float | None,
    started: dt.datetime,
    finished: dt.datetime,
    verdicts: list[Verdict],
    commits: int,
) -> dict[str, Any]:
    out: dict[str, Any] = {"silver_commits": commits}
    buckets, behind, errors = fleet.snapshot() if fleet is not None else ({}, {}, [])
    finals = list(fleet.finals.values()) if fleet is not None else []

    # Producers and clocks.
    bounds = OffsetBounds()
    problems: list[str] = []
    not_lat = 0
    delivered_partitions: set[PartitionKey] = set()
    for final in finals:
        clock = final.get("clock", {})
        bounds = bounds.merge(
            OffsetBounds(clock.get("lower_ms"), clock.get("upper_ms"), int(clock.get("samples", 0)))
        )
        problems.extend(final.get("delivery_problems", []))
        not_lat += int(final.get("not_log_append_time", 0))
        for key in final.get("delivered_by_partition", {}):
            topic, partition = key.rsplit("[", 1)
            delivered_partitions.add(PartitionKey(topic, int(partition.rstrip("]"))))
    if fleet is not None and len(finals) < len(fleet.processes):
        problems.append(f"{len(fleet.processes) - len(finals)} worker(s) sent no final report")
    verdicts.append(verdict.clock_verdict(bounds))
    verdicts.extend(
        verdict.delivery_verdicts(
            problems=problems,
            not_log_append_time=not_lat,
            delivered_partitions=delivered_partitions,
            expected=expected,
            worker_errors=errors,
        )
    )
    out["clock_offset"] = bounds.as_record()
    out["producers"] = finals

    # Phase 1.
    start, end = phases.get("measured_start_ms"), phases.get("measured_end_ms")
    if start is not None and end is not None:
        stats = window_stats(samples, start, end)
        offered = verdict.offered_load(buckets, behind, start / 1000, end / 1000)
        verdicts.extend(verdict.offered_verdicts("throughput", offered, config.rate_events_per_s))
        verdicts.extend(verdict.throughput_verdicts(stats))
        out["throughput_window"] = asdict(stats)
        out["throughput_offered"] = asdict(offered)
    else:
        verdicts.append(
            Verdict(
                "throughput_sustained",
                verdict.TARGET,
                False,
                "the measured window never completed"
                + (f": {consumer_failure}" if consumer_failure else ""),
            )
        )

    # Phase 2.
    restored, observed = phases.get("restored_ms"), phases.get("observed_until_ms")
    outage_start = phases.get("outage_start_ms")
    recovery = None
    if restored is not None and observed is not None:
        recovery = detect_recovery(
            samples,
            restored_ms=restored,
            observed_until_ms=observed,
            target_ms=LAG_TARGET_MS,
            hold_ms=config.recovery_hold_s * 1000,
            bound_ms=config.recovery_bound_s * 1000,
        )
        out["recovery"] = asdict(recovery)
    if outage_start is not None and observed is not None:
        offered = verdict.offered_load(buckets, behind, outage_start / 1000, observed / 1000)
        verdicts.extend(verdict.offered_verdicts("outage", offered, config.rate_events_per_s))
        out["outage_offered"] = asdict(offered)
    verdicts.extend(
        verdict.outage_verdicts(
            recovery,
            consumer_down_s=consumer_down_s,
            consumer_failure=consumer_failure,
            bound_s=config.recovery_bound_s,
        )
    )

    # Gold.
    if start is not None and end is not None:
        evaluated_until = phases.get("observed_until_ms") or _now_ms()
        silver_advanced = any(start <= s.at_ms < end for s in samples)
        verdicts.extend(
            verdict.gold_verdicts(
                gold.builds if gold is not None else [],
                enabled=config.gold,
                loop_started_ms=gold.started_ms if gold is not None else None,
                start_ms=start,
                end_ms=end,
                evaluated_until_ms=max(evaluated_until, _now_ms()),
                silver_advanced=silver_advanced,
            )
        )
    out["gold_builds"] = [asdict(b) for b in (gold.builds if gold is not None else [])]

    # Authenticity: anchored to the broker's records.
    ends = watermarks(args.bootstrap, expected) if expected else {}
    verdicts.extend(
        verdict.authenticity_verdicts(
            samples,
            committed_offsets=tracker.offsets(),
            committed_newest=tracker.newest(),
            broker_end_offsets=ends,
            run_started_ms=int(started.timestamp() * 1000),
            run_finished_ms=int(finished.timestamp() * 1000),
        )
    )
    out["broker_end_offsets"] = {str(k): v for k, v in ends.items()}
    out["lag_series"] = [
        [s.at_ms, s.lag_ms, s.pre_commit_lag_ms, s.table, s.version] for s in samples
    ]
    return out


def _tree_bytes(path: Path) -> int:
    total = 0
    for dirpath, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += (Path(dirpath) / name).lstat().st_size
            except OSError:
                continue
    return total


if __name__ == "__main__":
    raise SystemExit(main())
