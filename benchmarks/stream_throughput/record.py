"""The run record (`eval/manifest/<run_id>.json`) and the report (`REPORT.md` beside this file).

The record is a `BENCHMARK` record in the shape `scripts/check_claims.py` resolves, plus everything
CLAUDE.md §13 asks a run to carry that applies to a stream benchmark: Spark, Delta, Hadoop, Java,
Scala and Python versions (the pins `build_session` enforces on the running JVM, and the toolchain
checks' own findings), the env-lock digest, the jar-lock digest, the broker image, the git SHA and
the dirty-worktree flag, the whole configuration, the frozen targets, the broker-to-host clock
offset, and every verdict. It is written for every run, valid or not, because a measurement is
evidence even when it cannot be cited; it is never overwritten.

The report is rendered only from a valid run on a clean worktree, and every number in it sits
under a heading that names the `run_id`, which is how the claim linter scopes a citation.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import platform
import re
import subprocess
from dataclasses import asdict, dataclass, field
from importlib import metadata
from pathlib import Path
from typing import Any, Final

from benchmarks.stream_throughput.spec import (
    HARNESS_VERSION,
    LAG_TARGET_MS,
    OUTAGE_S,
    SUBJECT,
    TARGET_RATE_EVENTS_PER_S,
    TOOL,
    targets,
)

ROOT: Final = Path(__file__).resolve().parents[2]
MANIFEST_DIR: Final = ROOT / "eval" / "manifest"
REPORT: Final = Path(__file__).resolve().parent / "REPORT.md"
COMPOSE: Final = ROOT / "deploy" / "compose.yml"


class RecordError(RuntimeError):
    """A record could not be written without losing evidence."""


def new_run_id(started: dt.datetime, sha: str) -> str:
    """Timestamp-prefixed (chronological, unique per second on one commit), commit-suffixed."""
    return f"bench-{started:%Y%m%d-%H%M%S}-stream-throughput-{sha[:8]}"


def git_facts() -> tuple[str, bool, str]:
    from data.generator.record import env_lock_digest, git_commit_sha, is_dirty

    return git_commit_sha(), is_dirty(), env_lock_digest()


def _sha256(path: Path) -> str | None:
    try:
        return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def _version(distribution: str) -> str | None:
    try:
        return metadata.version(distribution)
    except metadata.PackageNotFoundError:
        return None


def kafka_image() -> str | None:
    """The broker image the compose file pins (tag and digest)."""
    try:
        match = re.search(r"image:\s*(apache/kafka:\S+)", COMPOSE.read_text())
    except OSError:
        return None
    return match.group(1) if match else None


def toolchain_facts(findings: list[dict[str, Any]]) -> dict[str, Any]:
    from trace_core.stream import toolchain

    return {
        "pins": {
            "java_major": toolchain.JAVA_MAJOR,
            "spark": toolchain.SPARK_VERSION,
            "delta": toolchain.DELTA_VERSION,
            "hadoop_line": toolchain.HADOOP_LINE,
            "scala_line": toolchain.SCALA_LINE,
        },
        "enforced_on_running_jvm": (
            "build_session refuses a JVM whose Spark, Java, Scala or Hadoop contradict the pins "
            "(ADR-0045), so the consumer and every Gold build ran on exactly these"
        ),
        "findings": findings,
        "java_home": os.environ.get("JAVA_HOME"),
        "jars_lock_digest": _sha256(Path(toolchain.LOCK_PATH)),
        "installed": {
            name: _version(name)
            for name in ("pyspark", "delta-spark", "confluent-kafka", "pyarrow")
        },
        "kafka_image": kafka_image(),
    }


def _physical_memory_bytes() -> int | None:
    if platform.system() == "Darwin":
        done = subprocess.run(
            ["/usr/sbin/sysctl", "-n", "hw.memsize"],
            capture_output=True,
            text=True,
            check=False,
        )
        return int(done.stdout.strip()) if done.stdout.strip().isdigit() else None
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    except OSError:
        return None
    return None


def host_facts() -> dict[str, Any]:
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "physical_memory_bytes": _physical_memory_bytes(),
    }


@dataclass
class StreamBenchmarkRecord:
    run_id: str
    started_at: str
    finished_at: str
    status: str
    config: dict[str, Any]
    broker: dict[str, Any]
    toolchain: dict[str, Any]
    host: dict[str, Any]
    lake_root: str
    logs: str
    verdicts: list[dict[str, Any]]
    measured: dict[str, Any]
    targets: dict[str, float | int] = field(default_factory=targets)
    record_type: str = "BENCHMARK"
    track: str = "SYNTHETIC"
    subject: str = SUBJECT
    tool: str = TOOL
    tool_version: str = HARNESS_VERSION
    capability: str = "P3.stream-throughput"
    python_version: str = field(default_factory=platform.python_version)
    git_commit_sha: str = ""
    dirty_worktree: bool = True
    env_lock_digest: str = ""
    timing_semantics_version: int = 0
    feature_set_version: str = ""
    publishable: bool = field(init=False, default=False)
    """True only for a PASS or FAIL on a clean worktree; see `derive_publishable`."""

    def __post_init__(self) -> None:
        from trace_core.features.spec import FEATURE_SET_VERSION
        from trace_core.stream.timing import TIMING_SEMANTICS_VERSION

        if not self.git_commit_sha:
            self.git_commit_sha, self.dirty_worktree, self.env_lock_digest = git_facts()
        self.timing_semantics_version = TIMING_SEMANTICS_VERSION
        self.feature_set_version = FEATURE_SET_VERSION
        self.publishable = self.derive_publishable()

    def derive_publishable(self) -> bool:
        """Only a valid run (PASS or FAIL: a missed target is evidence, never hidden) from a
        worktree that was clean at the start and at the end on the same commit, which the caller
        folds into `dirty_worktree` (docs/EVALUATION.md §8 rule 4). INVALID, including a harness
        error, is never citable as a measurement."""
        return not self.dirty_worktree and self.status in {"PASS", "FAIL"}

    def write(self, directory: Path | None = None) -> Path:
        # Re-derived at write time, so the recorded flag can never disagree with the status and
        # worktree it is recorded beside.
        self.publishable = self.derive_publishable()
        target = (directory or MANIFEST_DIR) / f"{self.run_id}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            with target.open("x") as handle:
                handle.write(json.dumps(asdict(self), indent=2, sort_keys=True) + "\n")
        except FileExistsError as exc:
            raise RecordError(f"{target} exists; a run record is never overwritten") from exc
        return target


# ------------------------------------------------------------------ report ---


def _ms(value: Any) -> str:
    return "—" if value is None else f"{int(value):,} ms"


def _rate(value: Any) -> str:
    return "—" if value is None else f"{float(value):,.1f}"


def render_report(run: StreamBenchmarkRecord) -> str:
    rid = run.run_id
    m = run.measured
    window = m.get("throughput_window", {})
    offered = m.get("throughput_offered", {})
    outage_offered = m.get("outage_offered", {})
    recovery = m.get("recovery", {})
    clock = m.get("clock_offset", {})
    config = run.config
    lines = [
        "# Stream throughput and outage recovery",
        "",
        "> Written by `make load-stream`, never by hand. Every figure below comes from the run",
        f"> recorded as `run_id: {rid}`, which `make check-claims` resolves.",
        ">",
        "> An operational benchmark on synthetic traffic, on one laptop. It measures the Kafka ->",
        "> Bronze -> Silver stream and Gold's freshness; it says nothing about fraud-detection",
        "> quality and predicts nothing about a cloud deployment.",
        "",
        f"## Verdict — `run_id: {rid}`",
        "",
        f"**{run.status}** (`P3.stream-throughput`). Targets (ROADMAP Phase 3, PHASE3_PLAN §4.3):",
        f"the {TARGET_RATE_EVENTS_PER_S} events/s rate offered in both phases and never lowered; "
        f"consumer lag below {LAG_TARGET_MS} ms throughout the measured window and recovering "
        f"below it after a {OUTAGE_S} s consumer outage; Gold freshness.",
        "",
        "| verdict | kind | result | detail |",
        "|---|---|---|---|",
    ]
    for v in run.verdicts:
        detail = str(v["detail"]).replace("|", "/")
        lines.append(
            f"| `{v['name']}` | {v['kind']} | {'pass' if v['passed'] else 'FAIL'} | {detail} |"
        )
    lines += [
        "",
        f"## Run — `run_id: {rid}`",
        "",
        "| field | value |",
        "|---|---|",
        f"| commit | `{run.git_commit_sha}` (dirty: {run.dirty_worktree}) |",
        f"| host | {run.host.get('platform')}, {run.host.get('cpu_count')} CPUs, "
        f"{(run.host.get('physical_memory_bytes') or 0) / 2**30:.0f} GiB |",
        f"| toolchain pins | Spark {run.toolchain['pins']['spark']}, Delta "
        f"{run.toolchain['pins']['delta']}, Java {run.toolchain['pins']['java_major']}, Hadoop "
        f"{run.toolchain['pins']['hadoop_line']}.x, Scala {run.toolchain['pins']['scala_line']} |",
        f"| broker | `{run.toolchain.get('kafka_image')}` |",
        f"| consumer | `{config['master']}`, driver {config['driver_memory']}, "
        f"{config['shuffle_partitions']} shuffle partitions, triggers "
        f"{config['bronze_trigger_s']} s / {config['silver_trigger_s']} s, maxOffsetsPerTrigger "
        f"{config['max_offsets_per_trigger']} |",
        f"| Gold | {'on' if config['gold'] else 'off'}, driver {config['gold_driver_memory']} |",
        f"| mix | {config['mix']} ({config['producer_workers']} producer workers) |",
        f"| windows | warm-up {config['warmup_min_s']} to {config['warmup_max_s']} s, measured "
        f"{config['measured_s']} s, pre-outage {config['pre_outage_s']} s, recovery bound "
        f"{config['recovery_bound_s']} s + hold {config['recovery_hold_s']} s |",
        f"| clock offset (broker minus host) | [{clock.get('lower_ms')}, "
        f"{clock.get('upper_ms')}] ms from {clock.get('samples')} delivery reports |",
        "",
        f"## Throughput window — `run_id: {rid}`",
        "",
        "| measure | value |",
        "|---|---|",
        f"| offered (events/s) | {_rate(offered.get('rate_events_per_s'))} |",
        f"| Kafka → Silver committed (events/s) | {_rate(window.get('throughput_events_per_s'))} |",
        f"| Silver commits sampled | {window.get('samples')} |",
        f"| consumer lag p50 / p95 / p99 / max | {_ms(window.get('lag_p50_ms'))} / "
        f"{_ms(window.get('lag_p95_ms'))} / {_ms(window.get('lag_p99_ms'))} / "
        f"{_ms(window.get('lag_max_ms'))} |",
        f"| widest interval without a Silver commit | {_ms(window.get('widest_gap_ms'))} |",
        "",
        f"## Outage — `run_id: {rid}`",
        "",
        "| measure | value |",
        "|---|---|",
        f"| offered during the outage phase (events/s) | "
        f"{_rate(outage_offered.get('rate_events_per_s'))} |",
        f"| recovery after the restart | {_ms(recovery.get('recovery_ms'))} |",
        f"| peak consumer lag after the restart | {_ms(recovery.get('peak_lag_ms'))} |",
        "",
        f"## Gold builds — `run_id: {rid}`",
        "",
        "| build | started (ms) | finished (ms) | exit | lag |",
        "|---|---|---|---|---|",
    ]
    for build in m.get("gold_builds", []):
        lines.append(
            f"| {build.get('build_id')} | {build.get('started_ms')} | {build.get('finished_ms')} "
            f"| {build.get('exit_code')} | {_ms(build.get('lag_ms'))} |"
        )
    lines += [
        "",
        f"## Resources — `run_id: {rid}`",
        "",
        "| role | peak RSS (MiB) | mean CPU % | max CPU % |",
        "|---|---|---|---|",
    ]
    for role, facts in sorted(m.get("resources", {}).items()):
        lines.append(
            f"| {role} | {facts['peak_rss_mib']} | {facts['mean_cpu_percent']} | "
            f"{facts['max_cpu_percent']} |"
        )
    lines += [
        "",
        f"The full lag series, every verdict and the configuration are in "
        f"`eval/manifest/{rid}.json`.",
        "",
    ]
    return "\n".join(lines)
