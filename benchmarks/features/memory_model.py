#!/usr/bin/env python3
"""A memory model for the online feature store, derived from the released features.

**Why a model and not a measurement of the first ten minutes.** ADR-0041 sized
the store from a sample taken while it was already evicting, and was wrong by
2.8x. ADR-0042 corrected the number but still only knew what ten minutes of
traffic cost. Neither says what the store needs at steady state, because a
ten-minute run never sees a key expire and never sees a hot merchant's bucket
hash reach its retention. This does: it measures the bytes each storage
primitive actually costs in Redis -- base plus per-member, by writing synthetic
keys and asking `MEMORY USAGE` -- and then projects every primitive the released
`StatePlan` declares across the population, rate and retention it will see.

**Everything projected here is traceable to a declaration or a measurement.**
The primitives, their retention and which entities get them come from
`trace_core.features.state_plan.PLAN`. Bytes per key come from Redis. The
population and per-account rate are the representative profile's, which are
themselves derived from the frozen `eval-v1` manifest (ADR-0040). Nothing is a
guess, and the report says which of the two kinds each number is.

Two projections, because they answer different questions:

* **the ten-minute acceptance run** -- what the gate needs, so it can run with
  `noeviction` and zero refused writes;
* **steady state at 500 TPS** -- what the store needs to serve the target rate
  indefinitely with the declared retention, which is the honest capacity
  requirement and is NOT expected to fit a laptop.

Writes a `BENCHMARK` record (`eval/manifest/`) and `benchmarks/features/MEMORY.md`,
so every figure it publishes carries a `run_id` that `make check-claims` resolves.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import platform
import sys
import uuid
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

from trace_core.features.semantics import Entity, Stream  # noqa: E402
from trace_core.features.state_plan import PLAN  # noqa: E402

MANIFEST_DIR: Final = ROOT / "eval" / "manifest"
REPORT: Final = Path(__file__).resolve().parent / "MEMORY.md"

# --- the representative profile (ADR-0040), restated from the k6 derivation ---
TARGET_TPS: Final = 500
CANONICAL_WINDOW_S: Final = 600
EVAL_V1_TX_PER_ACCOUNT_PER_DAY: Final = 0.43
RETENTION_25H_S: Final = 25 * 3_600
HISTORY_DEPTH: Final = EVAL_V1_TX_PER_ACCOUNT_PER_DAY * RETENTION_25H_S / 86_400
ACCOUNTS: Final = round(TARGET_TPS * CANONICAL_WINDOW_S / HISTORY_DEPTH)
MERCHANTS: Final = max(3_000, round(ACCOUNTS * 3_000 / 40_000))
DEVICES: Final = max(48_000, round(ACCOUNTS * 48_000 / 40_000))
IPS: Final = max(20_000, round(ACCOUNTS * 20_000 / 40_000))
CARDS: Final = ACCOUNTS  # cards_per_account = 1 in the frozen manifest
IDENTITY_EVENT_FRACTION: Final = 865 / 1_000_000
"""identity.events.v1 rows per tx.raw.v1 row in eval-v1."""
HABITUAL_MERCHANT_RATIO: Final = 0.75
HABITUAL_MERCHANTS_PER_ACCOUNT: Final = 8  # randrange(4, 13) mean

POPULATION: Final[dict[Entity, int]] = {
    Entity.ACCOUNT: ACCOUNTS,
    Entity.CARD: CARDS,
    Entity.DEVICE: DEVICES,
    Entity.MERCHANT: MERCHANTS,
    Entity.IP: IPS,
}
"""Uniform draw over each pool, as the representative profile does (no hot
skew; the skew was what made the old profile adversarial). Merchant choice is
75% from an account's habitual set, so merchant arrivals are less uniform than
the pool size suggests; treated as uniform here, which OVERSTATES merchant key
count and is therefore conservative."""


# ------------------------------------------------------------- measurement ---


@dataclass(frozen=True)
class Fit:
    """bytes(key) ~= base + per_member * members, from two measured points."""

    primitive: str
    base_bytes: int
    per_member_bytes: float
    points: tuple[tuple[int, int], ...]

    def bytes_for(self, members: float) -> float:
        if len(self.points) > 2:
            # A measured curve: step up to the first point at or above `members`,
            # which rounds a sketch UP to the next measured size (conservative).
            for n, size in self.points:
                if members <= n:
                    return float(size)
            return float(self.points[-1][1])
        return self.base_bytes + self.per_member_bytes * members


def _usage(client: Any, key: str) -> int:
    usage = client.memory_usage(key)
    return int(usage or 0)


def measure_primitives(client: Any) -> dict[str, Fit]:
    """Write synthetic keys of each shape at two sizes and fit base + slope.

    Under a throwaway namespace on the live feature store; every key is deleted
    afterwards. Member shapes mirror `RedisOnlineFeatureStore` exactly: velocity
    members are dedup keys scored by ms, bucket fields are `{minute}:{kind}`,
    exact-distinct members are entity ids, HLL is a sketch, the profile hash
    carries one counter per distinct merchant/mcc/device, amounts are
    `{ms}:{dedup}:{amount}`.
    """
    ns = f"mm:{uuid.uuid4().hex[:8]}"
    fits: dict[str, Fit] = {}
    now_ms = 1_790_000_000_000

    def _fit(name: str, write: Any, sizes: tuple[int, int]) -> None:
        points = []
        for n in sizes:
            key = f"{ns}:{name}:{n}"
            write(key, n)
            points.append((n, _usage(client, key)))
            client.delete(key)
        (n0, b0), (n1, b1) = points
        slope = (b1 - b0) / (n1 - n0)
        fits[name] = Fit(name, max(0, round(b0 - slope * n0)), slope, tuple(points))

    def _fixed(name: str, write: Any, n: int) -> None:
        """A primitive whose size does not grow with members: one point, slope 0."""
        key = f"{ns}:{name}:{n}"
        write(key, n)
        size = _usage(client, key)
        client.delete(key)
        fits[name] = Fit(name, size, 0.0, ((n, size),))

    def velocity(key: str, n: int) -> None:
        client.zadd(key, {f"evt-{uuid.uuid4().hex}": now_ms + i for i in range(n)})

    def buckets(key: str, n: int) -> None:
        mapping = {}
        for i in range(n):
            minute = now_ms // 60_000 + i
            mapping[f"{minute}:c"] = 3
            mapping[f"{minute}:s"] = 12_345
            mapping[f"{minute}:q"] = 152_399_025
            mapping[f"{minute}:k"] = 3
            mapping[f"{minute}:d"] = 0
        client.hset(key, mapping=mapping)

    def exact_distinct(key: str, n: int) -> None:
        client.zadd(key, {f"dev_{i:09d}": now_ms + i for i in range(n)})

    def hll(key: str, n: int) -> None:
        client.pfadd(key, *[f"acct_{i:09d}" for i in range(n)])

    def previous(key: str, n: int) -> None:
        del n  # one hash of four fields, whatever the history
        client.hset(
            key,
            mapping={
                "occurred_ms": now_ms,
                "latitude": 51.5074,
                "longitude": -0.1278,
                "card_present": 1,
            },
        )

    def profile(key: str, n: int) -> None:
        mapping: dict[str, Any] = {
            "first_seen_ms": now_ms,
            "observations": n,
            "lat": 51.5,
            "lon": -0.1,
        }
        for i in range(n):
            mapping[f"m:mrch_{i:06d}"] = 2
            mapping[f"c:{5000 + (i % 40)}"] = 2
            mapping[f"d:dev_{i:09d}"] = 2
        client.hset(key, mapping=mapping)

    def amounts(key: str, n: int) -> None:
        client.zadd(
            key,
            {f"{now_ms + i}:evt-{uuid.uuid4().hex[:12]}:{1000 + i}": now_ms + i for i in range(n)},
        )

    _fit("velocity", velocity, (1, 64))
    _fit("buckets", buckets, (1, 120))  # n = active minutes
    _fit("exact_distinct", exact_distinct, (1, 32))
    # A HyperLogLog is SPARSE at low cardinality -- tens of bytes for a handful
    # of members -- and converts to a dense ~12 KiB block only once enough
    # registers are set. Almost every real bucket holds a few accounts per five
    # minutes, so sizing every sketch at the dense figure overstated the store
    # by 5x on the first run of this model. Measured as a curve instead, and
    # looked up by each entity's expected cardinality per bucket.
    hll_points: list[tuple[int, int]] = []
    for n in (1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1_024, 5_000):
        key = f"{ns}:hll:{n}"
        hll(key, n)
        hll_points.append((n, _usage(client, key)))
        client.delete(key)
    fits["hll"] = Fit("hll", hll_points[0][1], 0.0, tuple(hll_points))
    _fixed("previous", previous, 1)
    _fit("profile", profile, (1, 12))  # n = distinct merchants ~ devices
    _fit("amounts", amounts, (1, 128))
    return fits


# -------------------------------------------------------------- projection ---


@dataclass
class Line:
    primitive: str
    scope: str
    retention_s: int
    keys: float
    members_per_key: float
    bytes_per_key: float
    total_bytes: float
    basis: str


def _active_entities(pool: int, arrivals_per_s: float, horizon_s: float) -> float:
    """How many distinct entities from `pool` appear within `horizon_s` of
    uniform arrivals: pool * (1 - exp(-arrivals*horizon/pool))."""
    expected = arrivals_per_s * horizon_s
    return pool * (1.0 - math.exp(-expected / pool))


def _members(pool: int, arrivals_per_s: float, retention_s: float) -> float:
    """Observations per active entity inside its retention window."""
    active = _active_entities(pool, arrivals_per_s, retention_s)
    return (arrivals_per_s * retention_s) / max(active, 1.0)


def _line(
    primitive: str,
    scope: str,
    retention: int,
    keys: float,
    members: float,
    per_key: float,
    basis: str,
) -> Line:
    return Line(primitive, scope, retention, keys, members, per_key, keys * per_key, basis)


def project(fits: dict[str, Fit], *, horizon_s: float, tps: float) -> list[Line]:
    """Every primitive the plan declares, over `horizon_s` of traffic at `tps`.

    Retention caps the horizon per primitive: a ten-minute run keeps everything
    it wrote, steady state keeps `retention` seconds of it.
    """
    lines: list[Line] = []
    identity_tps = tps * IDENTITY_EVENT_FRACTION

    for (entity, stream), _windows in PLAN.velocity.items():
        retention = PLAN.velocity_retention(entity, stream).seconds
        rate = identity_tps if stream is not Stream.TRANSACTION else tps
        span = min(horizon_s, retention)
        keys = _active_entities(POPULATION[entity], rate, span)
        members = _members(POPULATION[entity], rate, span)
        per_key = fits["velocity"].bytes_for(members)
        lines.append(
            Line(
                "velocity",
                f"{entity.value}/{stream.value}",
                retention,
                keys,
                members,
                per_key,
                keys * per_key,
                "measured fit x plan retention",
            )
        )

    for entity, _windows in PLAN.buckets.items():
        retention = PLAN.bucket_retention(entity).seconds
        span = min(horizon_s, retention)
        keys = _active_entities(POPULATION[entity], tps, span)
        obs = _members(POPULATION[entity], tps, span)
        active_minutes = min(
            obs, span / 60.0
        )  # one field set per active minute, at most one per minute
        per_key = fits["buckets"].bytes_for(active_minutes)
        lines.append(
            Line(
                "buckets",
                entity.value,
                retention,
                keys,
                active_minutes,
                per_key,
                keys * per_key,
                "measured fit x plan retention; trimmed per write",
            )
        )

    for (entity, dimension), _w in PLAN.exact_distinct.items():
        retention = PLAN.exact_distinct_retention(entity, dimension).seconds
        span = min(horizon_s, retention)
        keys = _active_entities(POPULATION[entity], tps, span)
        obs = _members(POPULATION[entity], tps, span)
        # Distinct values per entity are bounded by the entity's own behaviour
        # (ADR-0034): habitual merchants ~8, devices 1-3, countries ~1-2.
        cap = {
            "MERCHANT": HABITUAL_MERCHANTS_PER_ACCOUNT,
            "DEVICE": 2.0,
            "COUNTRY": 1.5,
            "MCC": 4.0,
            "ACCOUNT": obs,
        }[dimension.value]
        members = min(obs, cap) if entity is Entity.ACCOUNT else obs
        per_key = fits["exact_distinct"].bytes_for(members)
        lines.append(
            Line(
                "exact_distinct",
                f"{entity.value}.{dimension.value}",
                retention,
                keys,
                members,
                per_key,
                keys * per_key,
                "measured fit x plan retention; members capped by ADR-0034 bound",
            )
        )

    for (entity, dimension), _windows in PLAN.approx_distinct.items():
        retention = PLAN.approx_distinct_retention(entity, dimension).seconds
        span = min(horizon_s, retention)
        active = _active_entities(POPULATION[entity], tps, span)
        obs = _members(POPULATION[entity], tps, span)
        # One HLL key per active 5-minute bucket; its cardinality is the number
        # of observations that landed in that bucket.
        buckets_per_entity = min(obs, span / 300.0)
        keys = active * buckets_per_entity
        per_bucket_cardinality = obs / max(buckets_per_entity, 1.0)
        per_key = fits["hll"].bytes_for(per_bucket_cardinality)
        lines.append(
            Line(
                "hll",
                f"{entity.value}.{dimension.value}",
                retention,
                keys,
                1,
                per_key,
                keys * per_key,
                "measured sketch size x active 5-min buckets",
            )
        )

    for (entity, stream), _lb in PLAN.previous.items():
        retention = PLAN.previous_retention(entity, stream).seconds
        rate = identity_tps if stream is not Stream.TRANSACTION else tps
        span = min(horizon_s, retention)
        keys = _active_entities(POPULATION[entity], rate, span)
        per_key = fits["previous"].bytes_for(1)
        lines.append(
            Line(
                "previous",
                f"{entity.value}/{stream.value}",
                retention,
                keys,
                1,
                per_key,
                keys * per_key,
                "measured x plan retention",
            )
        )

    for entity, _h in PLAN.profiles.items():
        retention = PLAN.profile_retention(entity).seconds
        span = min(horizon_s, retention)
        keys = _active_entities(POPULATION[entity], tps, span)
        obs = _members(POPULATION[entity], tps, span)
        distinct_counters = min(obs, HABITUAL_MERCHANTS_PER_ACCOUNT + 3)
        per_key = fits["profile"].bytes_for(distinct_counters)
        lines.append(
            Line(
                "profile",
                entity.value,
                retention,
                keys,
                distinct_counters,
                per_key,
                keys * per_key,
                "measured fit x plan horizon",
            )
        )
        sample = min(obs, 128)
        per_key = fits["amounts"].bytes_for(sample)
        lines.append(
            Line(
                "amounts",
                entity.value,
                retention,
                keys,
                sample,
                per_key,
                keys * per_key,
                "measured fit; sample capped at 128",
            )
        )

    lines.append(Line("epoch", "store", 0, 1, 1, 64, 64, "one key"))
    return lines


# ------------------------------------------------------------------ record ---


@dataclass
class BenchmarkRunRecord:
    run_id: str
    record_type: str = "BENCHMARK"
    track: str = "SYNTHETIC"
    subject: str = "online-feature-store-memory-model"
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
        target.write_text(json.dumps(asdict(self), indent=2, sort_keys=True) + "\n")
        return target


def _mib(b: float) -> str:
    return f"{b / 1_048_576:,.1f} MiB"


def render(
    record_id: str,
    fits: dict[str, Fit],
    ten_min: list[Line],
    steady: list[Line],
    limits: dict[str, Any],
) -> str:
    def table(lines: list[Line]) -> list[str]:
        rows = [
            "| primitive | scope | retention | keys | members/key | bytes/key | total | basis |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for ln in sorted(lines, key=lambda x: -x.total_bytes):
            rows.append(
                f"| `{ln.primitive}` | {ln.scope} | {ln.retention_s:,}s | {ln.keys:,.0f} | "
                f"{ln.members_per_key:,.1f} | {ln.bytes_per_key:,.0f} | "
                f"**{_mib(ln.total_bytes)}** | {ln.basis} |"
            )
        rows.append(f"| | | | | | | **{_mib(sum(x.total_bytes for x in lines))}** | |")
        return rows

    out = [
        "# Online feature store — memory model",
        "",
        "> Written by `benchmarks/features/memory_model.py`, never by hand. Every figure comes",
        f"> from the run recorded as `run_id: {record_id}`, which `make check-claims` resolves.",
        "",
        "Derived from the released `StatePlan` (what is stored, for how long) and from Redis's",
        "own `MEMORY USAGE` on synthetic keys of each shape. Population and rate are the",
        f"representative profile's (ADR-0040): {ACCOUNTS:,} accounts, {MERCHANTS:,} merchants,",
        f"{DEVICES:,} devices, {IPS:,} IPs, {TARGET_TPS} TPS, uniform arrivals.",
        "",
        f"## Measured bytes per primitive — `run_id: {record_id}`",
        "",
        "| primitive | base bytes | bytes per member | measured points (members, bytes) |",
        "|---|---|---|---|",
    ]
    for f in fits.values():
        points = ", ".join(f"({n}, {b:,})" for n, b in f.points)
        out.append(f"| `{f.primitive}` | {f.base_bytes:,} | {f.per_member_bytes:,.1f} | {points} |")
    out += [
        "",
        f"## The ten-minute acceptance run — `run_id: {record_id}`",
        "",
        *table(ten_min),
        "",
        f"**Configured feature-store limit: {limits['feature_maxmemory']}** — the projection above",
        f"with {limits['headroom_factor']}x headroom for fragmentation and for the model's own",
        "error, rounded up to 64 MiB. The gate runs with `noeviction`, so exceeding it would be a",
        "refused write and a failed run, not a silent loss.",
        "",
        f"## Steady state at {TARGET_TPS} TPS with the declared retention — `run_id: {record_id}`",
        "",
        *table(steady),
        "",
        "This is the honest capacity requirement for serving the target rate indefinitely. It",
        "does not fit a laptop, and the local budget does not pretend to: at the configured limit",
        f"the local store holds **{limits['local_minutes_at_target']:,.0f} minutes** of",
        f"{TARGET_TPS} TPS before it refuses writes -- loudly, with `feature_write_failed` on",
        "every",
        "decision and the completeness epoch withdrawn (ADR-0044). The largest lines above are",
        "where any reduction would have to come from, and each is a feature-semantics decision",
        "rather than a tuning one.",
        "",
    ]
    return "\n".join(out) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6389)
    parser.add_argument(
        "--headroom",
        type=float,
        default=1.25,
        help=(
            "multiplier over the ten-minute projection for the configured limit. 1.25 keeps the "
            "core profile inside ARCHITECTURE §14's 2.7 GB budget with room for api/worker/ui; the "
            "acceptance run records the store's real end-of-run memory, which is what validates it."
        ),
    )
    args = parser.parse_args()

    import redis

    client = redis.Redis(host=args.host, port=args.port, decode_responses=True)
    started = dt.datetime.now(dt.UTC)
    tool_version = str(client.info("server")["redis_version"])
    fits = measure_primitives(client)

    ten_min = project(fits, horizon_s=CANONICAL_WINDOW_S, tps=TARGET_TPS)
    steady = project(fits, horizon_s=RETENTION_25H_S * 30, tps=TARGET_TPS)
    ten_total = sum(x.total_bytes for x in ten_min)
    steady_total = sum(x.total_bytes for x in steady)

    # The configured limit: ten-minute working set with headroom, rounded up to 64 MiB.
    needed = ten_total * args.headroom
    limit_mib = int(math.ceil(needed / (64 * 1_048_576)) * 64)
    bytes_per_second_at_target = ten_total / CANONICAL_WINDOW_S
    local_minutes = (limit_mib * 1_048_576) / bytes_per_second_at_target / 60

    record_id = f"bench-{started:%Y%m%d}-memory-model-{git_commit_sha()[:8]}"
    limits = {
        "feature_maxmemory": f"{limit_mib}mb",
        "headroom_factor": args.headroom,
        "local_minutes_at_target": local_minutes,
    }
    record = BenchmarkRunRecord(
        run_id=record_id,
        started_at=started.isoformat(),
        finished_at=dt.datetime.now(dt.UTC).isoformat(),
        tool_version=tool_version,
        measured={
            "fits": {k: asdict(v) for k, v in fits.items()},
            "population": {e.value: n for e, n in POPULATION.items()},
            "target_tps": TARGET_TPS,
            "ten_minute_bytes": ten_total,
            "ten_minute_lines": [asdict(x) for x in ten_min],
            "steady_state_bytes": steady_total,
            "steady_state_lines": [asdict(x) for x in steady],
            "configured_feature_maxmemory_mib": limit_mib,
            "headroom_factor": args.headroom,
            "local_minutes_at_target_tps": local_minutes,
        },
    )
    path = record.write()
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(render(record_id, fits, ten_min, steady, limits))

    print(f"run_id: {record_id}")
    print(f"record: {path.relative_to(ROOT)}")
    print(f"report: {REPORT.relative_to(ROOT)}")
    print(
        f"ten-minute run  : {_mib(ten_total)}   -> configured limit {limit_mib} MiB "
        f"({args.headroom}x headroom)"
    )
    print(f"steady state    : {_mib(steady_total)}  ({steady_total / 1_073_741_824:.2f} GiB)")
    print(f"local store holds ~{local_minutes:,.0f} min of {TARGET_TPS} TPS before refusing writes")
    if record.dirty_worktree:
        print("NOTE: dirty worktree -- record written, but not publishable until committed clean.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
