#!/usr/bin/env python3
"""What one score costs as an account's raw history deepens (Phase 3 Step 12).

**Why.** A score records the transaction and reads its context in one Lua script (ADR-0046 §5). The
read fetches the account's raw sets for the last 25 hours and `MGET`s every observation's JSON, and
the gateway then decodes each one and reduces them in Python. So the cost of a score grows with the
account's transactions in 25 hours, a number an adversary controls (docs/PROGRESS.md, Step 1b known
limits). This measures that growth with a `run_id`, so the fallback design, pre-aggregating account
windows inside the script, is chosen or rejected on evidence.

**What this does.**
- It starts a throwaway Redis of the image the compose feature store runs, and refuses any other.
  Nothing else writes to it.
- For each depth, it writes that many transactions for one account within the last 24 hours, then
  times `RedisOnlineFeatureStore.score` for a fixed number of new transactions on that account.
- Per depth it records wall-clock p50, p95, p99 and max per score, and the Redis script's own
  microseconds per call from `INFO commandstats`, so Redis time and client-side decoding are
  separated.

It measures one host and one Redis, not the gateway under load: the load gate measures that.
Writes a `BENCHMARK` record (`eval/manifest/`) and `benchmarks/features/SCORE_LATENCY.md`.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import platform
import re
import shutil
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Final

ROOT: Final = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "packages"))
sys.path.insert(0, str(ROOT))

from data.generator.record import (  # noqa: E402
    env_lock_digest,
    git_commit_sha,
    is_dirty,
)

from trace_core.domain.time import event_time  # noqa: E402
from trace_core.features.observation import Event  # noqa: E402
from trace_core.features.semantics import Stream  # noqa: E402

MANIFEST_DIR: Final = ROOT / "eval" / "manifest"
REPORT: Final = Path(__file__).resolve().parent / "SCORE_LATENCY.md"
COMPOSE: Final = ROOT / "deploy" / "compose.yml"
STORE_IMAGE: Final = "redis:7-alpine"
NAMESPACE: Final = "sl"
DEPTHS: Final = (1, 64, 512, 2_048, 8_192)
"""Transactions one account holds in its raw window before the timed scores."""
SCORES_PER_DEPTH: Final = 200
WARMUP_SCORES: Final = 20
DAY_MS: Final = 86_400_000


def percentile(samples: Sequence[float], q: float) -> float:
    """Nearest-rank percentile: the smallest sample with at least `q` of all samples at or below."""
    if not samples:
        raise ValueError("no samples")
    if not 0 < q <= 1:
        raise ValueError(f"q must be in (0, 1], got {q}")
    ordered = sorted(samples)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def history_times_ms(depth: int, now_ms: int) -> list[int]:
    """`depth` event times spread evenly across the 24 hours before `now_ms`, oldest first."""
    if depth <= 0:
        return []
    step = DAY_MS // (depth + 1)
    return [now_ms - DAY_MS + step * (i + 1) for i in range(depth)]


def _tx(account: str, i: int, at_ms: int) -> Event:
    return Event(
        stream=Stream.TRANSACTION,
        occurred_at=event_time(dt.datetime.fromtimestamp(at_ms / 1000, tz=dt.UTC)),
        account_id=account,
        event_id=f"tx_{account}_{i:08d}",
        currency="GBP",
        amount_minor=1_000 + i % 5_000,
        card_id="card_000000001",
        device_id="dev_000000001",
        merchant_id=f"mrch_{i % 8:06d}",
        ip_id="ip_0000001",
        merchant_mcc="5411",
        merchant_country="GB",
        latitude=51.5,
        longitude=-0.12,
    )


def _script_usec(client: Any) -> tuple[int, int]:
    """(calls, usec) summed over the script commands Redis has run."""
    calls = usec = 0
    for name, stats in client.info("commandstats").items():
        if name in {"cmdstat_evalsha", "cmdstat_eval", "cmdstat_fcall"}:
            calls += int(stats["calls"])
            usec += int(stats["usec"])
    return calls, usec


@dataclass
class DepthResult:
    depth: int
    scores: int
    wall_ms_p50: float
    wall_ms_p95: float
    wall_ms_p99: float
    wall_ms_max: float
    redis_usec_per_call: float


def measure_depth(client: Any, store: Any, depth: int, now_ms: int) -> DepthResult:
    client.flushall()
    account = "acct_000000001"
    for i, at_ms in enumerate(history_times_ms(depth, now_ms - 60_000)):
        store.observe(_tx(account, i, at_ms))
    for w in range(WARMUP_SCORES):
        store.score(_tx(account, depth + w, now_ms - 50_000 + w))
    client.config_resetstat()
    samples: list[float] = []
    for j in range(SCORES_PER_DEPTH):
        event = _tx(account, depth + WARMUP_SCORES + j, now_ms - 30_000 + j)
        began = time.perf_counter_ns()
        store.score(event)
        samples.append((time.perf_counter_ns() - began) / 1e6)
    calls, usec = _script_usec(client)
    return DepthResult(
        depth=depth,
        scores=len(samples),
        wall_ms_p50=percentile(samples, 0.50),
        wall_ms_p95=percentile(samples, 0.95),
        wall_ms_p99=percentile(samples, 0.99),
        wall_ms_max=max(samples),
        redis_usec_per_call=usec / calls if calls else 0.0,
    )


@dataclass
class BenchmarkRunRecord:
    run_id: str
    record_type: str = "BENCHMARK"
    track: str = "SYNTHETIC"
    subject: str = "online-feature-store-score-latency-by-depth"
    git_commit_sha: str = field(default_factory=git_commit_sha)
    dirty_worktree: bool = field(default_factory=is_dirty)
    env_lock_digest: str = field(default_factory=env_lock_digest)
    python_version: str = field(default_factory=platform.python_version)
    started_at: str = ""
    finished_at: str = ""
    tool: str = "redis"
    tool_version: str = ""
    measured: dict[str, Any] = field(default_factory=dict)

    def write(self) -> Path:
        target = MANIFEST_DIR / f"{self.run_id}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("x") as handle:
            handle.write(json.dumps(asdict(self), indent=2, sort_keys=True) + "\n")
        return target


def render(record_id: str, results: Sequence[DepthResult], redis_version: str) -> str:
    rows = [
        "| depth | scores | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | Redis script (µs/call) |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in results:
        rows.append(
            f"| {r.depth:,} | {r.scores} | {r.wall_ms_p50:.3f} | {r.wall_ms_p95:.3f} | "
            f"{r.wall_ms_p99:.3f} | {r.wall_ms_max:.3f} | {r.redis_usec_per_call:,.0f} |"
        )
    return "\n".join(
        [
            "# Online feature store — score latency by account depth",
            "",
            "> Written by `benchmarks/features/score_latency.py`, never by hand. Every figure",
            "> comes",
            f"> from the run recorded as `run_id: {record_id}`, which `make check-claims`",
            "> resolves.",
            "",
            f"One account, one throwaway `{STORE_IMAGE}` (Redis {redis_version}), nothing else",
            "writing.",
            "Depth is the transactions the account holds in its raw window before the timed",
            "scores.",
            "Wall time covers the script and the client's decoding and reductions.",
            "",
            f"## Per-score cost — `run_id: {record_id}`",
            "",
            *rows,
            "",
        ]
    )


def compose_store_image(compose_text: str) -> str:
    match = re.search(r"^  redis:\n(?:    .*\n)*?    image: (\S+)", compose_text, re.MULTILINE)
    if match is None:
        raise SystemExit("deploy/compose.yml has no `redis` service image")
    return match.group(1)


def _docker(*args: str) -> subprocess.CompletedProcess[str]:
    binary = shutil.which("docker") or "docker"
    return subprocess.run([binary, *args], capture_output=True, text=True, timeout=120)  # noqa: S603


def main() -> int:
    argparse.ArgumentParser(description=__doc__).parse_args()
    image = compose_store_image(COMPOSE.read_text())
    if image != STORE_IMAGE:
        raise SystemExit(f"the compose feature store runs {image}, not {STORE_IMAGE}")
    import redis

    from trace_core.repositories.redis_features import RedisOnlineFeatureStore

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = int(probe.getsockname()[1])
    name = f"tracex-score-latency-{uuid.uuid4().hex[:8]}"
    started_container = _docker(
        "run", "-d", "--rm", "--name", name, "-p", f"127.0.0.1:{port}:6379",
        STORE_IMAGE, "redis-server", "--appendonly", "no", "--save", "",
    )  # fmt: skip
    if started_container.returncode != 0:
        raise SystemExit(f"could not start {STORE_IMAGE}: {started_container.stderr[:300]}")
    started = dt.datetime.now(dt.UTC)
    try:
        client = redis.Redis(host="127.0.0.1", port=port, decode_responses=True, socket_timeout=30)
        for _ in range(100):
            try:
                if client.ping():
                    break
            except Exception:
                time.sleep(0.1)
        redis_version = str(client.info("server")["redis_version"])
        store = RedisOnlineFeatureStore(client, namespace=NAMESPACE)
        now_ms = int(time.time() * 1000)
        results = [measure_depth(client, store, depth, now_ms) for depth in DEPTHS]
    finally:
        _docker("rm", "-f", name)

    record_id = f"bench-{started:%Y%m%d-%H%M%S}-score-latency-{git_commit_sha()[:8]}"
    record = BenchmarkRunRecord(
        run_id=record_id,
        started_at=started.isoformat(),
        finished_at=dt.datetime.now(dt.UTC).isoformat(),
        tool_version=redis_version,
        measured={
            "store_image": STORE_IMAGE,
            "scores_per_depth": SCORES_PER_DEPTH,
            "warmup_scores": WARMUP_SCORES,
            "results": [asdict(r) for r in results],
        },
    )
    path = record.write()
    REPORT.write_text(render(record_id, results, redis_version))
    print(f"run_id: {record_id}")
    print(f"record: {path.relative_to(ROOT)}")
    for r in results:
        print(f"depth {r.depth:>6,}: p50 {r.wall_ms_p50:.3f} ms, p99 {r.wall_ms_p99:.3f} ms, "
              f"script {r.redis_usec_per_call:,.0f} µs/call")  # fmt: skip
    if record.dirty_worktree:
        print("NOTE: dirty worktree -- the record is not publishable until run on a clean commit.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
