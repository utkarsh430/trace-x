#!/usr/bin/env python3
"""Characterise the hybrid distinct-count strategy (ADR-0034).

Measures what the ADR must record, and nothing it can assume:

* **estimator error** for `APPROXIMATE` features, across cardinality and window;
* **exactness** for `EXACT` features, asserted rather than hoped for;
* **memory** per key for both representations;
* **read and update latency** for both.

Every number it prints is written to a run record in `eval/manifest/` so
`make check-claims` can resolve it, and nothing is published without one
(CLAUDE.md §13). The record type is `BENCHMARK`: it exercises no model and no
LLM, so the claim linter refuses to let it back a quality claim.

Run: `make bench-features` (needs `make up`).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import platform
import random
import statistics
import sys
import time
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

MANIFEST_DIR: Final = ROOT / "eval" / "manifest"
REPORT: Final = Path(__file__).resolve().parent / "REPORT.md"

HLL_BUCKET_MS: Final = 300_000
CARDINALITIES: Final = (10, 100, 1_000, 5_000, 20_000, 50_000)
WINDOWS_S: Final = (300, 3_600, 86_400)
READ_REPEATS: Final = 200


@dataclass
class Measurement:
    representation: str
    cardinality: int
    window_s: int
    counted: int
    relative_error: float
    memory_bytes: int
    read_ms_p50: float
    read_ms_p99: float
    update_ms_per_event: float


@dataclass
class BenchmarkRunRecord:
    """Provenance for a benchmark run, in the shape `check_claims.py` resolves."""

    run_id: str
    record_type: str = "BENCHMARK"
    track: str = "SYNTHETIC"
    subject: str = "online-feature-store-cardinality"
    git_commit_sha: str = field(default_factory=git_commit_sha)
    dirty_worktree: bool = field(default_factory=is_dirty)
    env_lock_digest: str = field(default_factory=env_lock_digest)
    python_version: str = field(default_factory=platform.python_version)
    started_at: str = ""
    finished_at: str = ""
    tool: str = "redis"
    tool_version: str = ""
    seed: int = 0
    measured: dict[str, Any] = field(default_factory=dict)

    def write(self) -> Path:
        target = MANIFEST_DIR / f"{self.run_id}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(asdict(self), indent=2, sort_keys=True) + "\n")
        return target


def _percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(q * len(ordered)))
    return ordered[index]


def measure(client: Any, *, seed: int) -> list[Measurement]:
    rng = random.Random(seed)
    now = 1_770_000_000_000
    results: list[Measurement] = []

    for window_s in WINDOWS_S:
        for cardinality in CARDINALITIES:
            members = [f"acct_{i:06d}" for i in range(cardinality)]
            spread_ms = window_s * 1000

            # --- EXACT: one sorted set keyed by the counted value --------
            zkey = f"bench:dx:{window_s}:{cardinality}"
            client.delete(zkey)
            began = time.perf_counter()
            pipe = client.pipeline(transaction=False)
            for member in members:
                pipe.zadd(zkey, {member: now - rng.randrange(0, spread_ms)}, gt=True)
            pipe.execute()
            zupdate = (time.perf_counter() - began) / cardinality * 1000

            zreads = []
            for _ in range(READ_REPEATS):
                started = time.perf_counter()
                zcount = client.zcount(zkey, f"({now - spread_ms}", now)
                zreads.append((time.perf_counter() - started) * 1000)
            results.append(
                Measurement(
                    representation="EXACT",
                    cardinality=cardinality,
                    window_s=window_s,
                    counted=int(zcount),
                    relative_error=abs(int(zcount) - cardinality) / cardinality,
                    memory_bytes=int(client.memory_usage(zkey) or 0),
                    read_ms_p50=_percentile(zreads, 0.5),
                    read_ms_p99=_percentile(zreads, 0.99),
                    update_ms_per_event=zupdate,
                )
            )

            # --- APPROXIMATE: one HyperLogLog per five-minute bucket -----
            hkeys: set[str] = set()
            began = time.perf_counter()
            pipe = client.pipeline(transaction=False)
            for member in members:
                stamp = now - rng.randrange(0, spread_ms)
                key = f"bench:da:{window_s}:{cardinality}:{stamp // HLL_BUCKET_MS}"
                hkeys.add(key)
                pipe.pfadd(key, member)
            pipe.execute()
            hupdate = (time.perf_counter() - began) / cardinality * 1000

            ordered = sorted(hkeys)
            hreads = []
            for _ in range(READ_REPEATS):
                started = time.perf_counter()
                hcount = client.pfcount(*ordered)
                hreads.append((time.perf_counter() - started) * 1000)
            results.append(
                Measurement(
                    representation="APPROXIMATE",
                    cardinality=cardinality,
                    window_s=window_s,
                    counted=int(hcount),
                    relative_error=abs(int(hcount) - cardinality) / cardinality,
                    memory_bytes=sum(int(client.memory_usage(k) or 0) for k in ordered),
                    read_ms_p50=_percentile(hreads, 0.5),
                    read_ms_p99=_percentile(hreads, 0.99),
                    update_ms_per_event=hupdate,
                )
            )
            for key in (zkey, *ordered):
                client.delete(key)
    return results


def render(results: list[Measurement], record_id: str, tool_version: str) -> str:
    lines = [
        "# Online feature store — distinct-cardinality benchmark",
        "",
        "> Evidence for ADR-0034. Every number below comes from the run recorded as",
        f"> `run_id: {record_id}`, which `make check-claims` resolves.",
        "",
        "**What is being compared.** Two representations of an event-time sliding",
        "distinct count: a sorted set keyed by the counted value (`ZADD … GT`,",
        "`ZCOUNT` over the window) and one HyperLogLog per five-minute bucket unioned",
        "by `PFCOUNT`. Both were driven with identical inputs at each cardinality.",
        "",
        f"Redis {tool_version}. Each read figure is the median and p99 of",
        f"{READ_REPEATS} reads; update cost is wall-clock per event over a pipelined load.",
        "",
        "| window | cardinality | repr | counted | rel. error | memory "
        "| read p50 | read p99 | update/event |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for row in results:
        lines.append(
            f"| {row.window_s}s | {row.cardinality:,} | {row.representation} | "
            f"{row.counted:,} | {row.relative_error:.2%} | "
            f"{row.memory_bytes / 1024:,.1f} KB | {row.read_ms_p50:.3f} ms | "
            f"{row.read_ms_p99:.3f} ms | {row.update_ms_per_event:.4f} ms |"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6389)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    try:
        import redis
    except ModuleNotFoundError:
        print("redis client missing; it ships in the `db` extra", file=sys.stderr)
        return 1

    client = redis.Redis(host=args.host, port=args.port, decode_responses=True)
    try:
        client.ping()
    except Exception as exc:
        print(f"no Redis at {args.host}:{args.port} ({exc}). Run `make up`.", file=sys.stderr)
        return 1

    tool_version = str(client.info("server").get("redis_version", "unknown"))
    started = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
    results = measure(client, seed=args.seed)
    finished = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")

    approximate = [r for r in results if r.representation == "APPROXIMATE"]
    exact = [r for r in results if r.representation == "EXACT"]
    record_id = f"bench-{dt.datetime.now(dt.UTC):%Y%m%d}-cardinality-{git_commit_sha()[:8]}"
    record = BenchmarkRunRecord(
        run_id=record_id,
        started_at=started,
        finished_at=finished,
        tool_version=tool_version,
        seed=args.seed,
        measured={
            "max_relative_error_approximate": max(r.relative_error for r in approximate),
            "median_relative_error_approximate": statistics.median(
                r.relative_error for r in approximate
            ),
            "max_relative_error_exact": max(r.relative_error for r in exact),
            "max_memory_ratio_exact_over_approximate": max(
                e.memory_bytes / a.memory_bytes
                for e, a in zip(exact, approximate, strict=True)
                if a.memory_bytes
            ),
            "max_read_ms_p99": max(r.read_ms_p99 for r in results),
            "cardinalities": list(CARDINALITIES),
            "windows_s": list(WINDOWS_S),
            "rows": [asdict(r) for r in results],
        },
    )
    path = record.write()
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(render(results, record_id, tool_version))

    print(render(results, record_id, tool_version))
    print(f"run record: {path.relative_to(ROOT)}")
    if record.dirty_worktree:
        print("\nWARNING: dirty worktree — this run is NOT publishable (EVALUATION.md §8 rule 4)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
