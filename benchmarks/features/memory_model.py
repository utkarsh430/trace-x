#!/usr/bin/env python3
"""The online feature store's memory model, measured through the store itself (Phase 3 Step 12).

**Why it was rewritten.** The Phase 2 model (`run_id: bench-20260913-memory-model-5c770259`) wrote
synthetic keys of the Phase 2 shapes and fitted each with a straight line through two small sizes.
The Step 1b store (ADR-0046 §5) no longer has those shapes. Every observation is now one string key
for the raw window, beside per-account raw sets and a folded profile prefix. And a straight line
through two small sizes cannot see a sorted set leave its compact encoding above 128 members.

**What this does.**
- It starts a throwaway Redis of the image the compose feature store runs, and refuses any other.
- It drives `RedisOnlineFeatureStore` itself, the production Lua scripts, through controlled
  scenarios, so every key shape is the store's by construction, never restated here.
- After each scenario it reads Redis's exact `MEMORY USAGE` (SAMPLES 0) for every key and sums it by
  key family. Nothing else writes to that Redis (ADR-0042 §3).
- Each family becomes a curve over its size driver, measured on both sides of the encoding
  threshold, and is projected over the representative profile's population and rate (ADR-0040),
  unchanged from the Phase 2 model so that the correction isolates the layout, with the store's own
  retentions (`LAYOUT`).

Two projections, as before: the ten-minute acceptance run, which sizes the configured limit, and
steady state at the target rate, which is the honest capacity requirement and is not expected to fit
a laptop.

Writes a `BENCHMARK` record (`eval/manifest/`) and `benchmarks/features/MEMORY.md`, so every
published figure carries the run's `run_id`.
"""

from __future__ import annotations

import argparse
import datetime as dt
import itertools
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
from collections.abc import Callable, Iterable, Mapping
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

from trace_core.domain.enums import AuthorizationOutcome  # noqa: E402
from trace_core.domain.time import event_time  # noqa: E402
from trace_core.features.observation import Event, authorization_observation  # noqa: E402
from trace_core.features.semantics import Stream  # noqa: E402

MANIFEST_DIR: Final = ROOT / "eval" / "manifest"
REPORT: Final = Path(__file__).resolve().parent / "MEMORY.md"
COMPOSE: Final = ROOT / "deploy" / "compose.yml"

# --- the representative profile (ADR-0040), restated from the k6 derivation, as in Phase 2 ---
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
HABITUAL_MERCHANTS_PER_ACCOUNT: Final = 8

STORE_IMAGE: Final = "redis:7-alpine"
NAMESPACE: Final = "mm"
MIB: Final = 1_048_576
SIZES: Final = (1, 16, 64, 128, 129, 256, 1_024)
"""Members per key, on both sides of Redis's 128-entry compact sorted-set encoding."""
FOLDED_SIZES: Final = (1, 20, 128, 512)
HLL_SIZES: Final = (1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1_024)
MINUTE_SIZES: Final = (1, 16, 128, 512, 1_440)
OUTCOME_SIZES: Final = (1, 16, 128, 129)


# ------------------------------------------------------------- measurement ---


@dataclass(frozen=True)
class Curve:
    """Bytes of one key of a family against its size driver, as measured."""

    family: str
    driver: str
    points: tuple[tuple[float, float], ...]
    """(size, bytes per key), sorted by size."""

    def bytes_for(self, size: float) -> float:
        """Piecewise linear between measured sizes; beyond the largest, the last segment's slope."""
        if size <= 0:
            return 0.0
        points = self.points
        if size <= points[0][0] or len(points) == 1:
            return points[0][1] if size <= points[0][0] else points[-1][1]
        for (x0, y0), (x1, y1) in itertools.pairwise(points):
            if size <= x1:
                return y0 + (y1 - y0) * (size - x0) / (x1 - x0)
        (x0, y0), (x1, y1) = points[-2], points[-1]
        return y1 + (y1 - y0) / (x1 - x0) * (size - x1)


def family_of(key: str, namespace: str = NAMESPACE) -> str:
    """`mm:obs:transaction:evt-1` -> `obs`: the store's key family, its first segment."""
    prefix = f"{namespace}:"
    if not key.startswith(prefix):
        raise ValueError(f"{key!r} is not in namespace {namespace!r}")
    return key[len(prefix) :].split(":", 1)[0]


def family_usage(client: Any, namespace: str = NAMESPACE) -> dict[str, tuple[int, int]]:
    """Every key's exact `MEMORY USAGE`, summed by family: {family: (keys, bytes)}."""
    totals: dict[str, list[int]] = {}
    for key in client.scan_iter(match=f"{namespace}:*", count=1_000):
        size = int(client.memory_usage(key, samples=0) or 0)
        entry = totals.setdefault(family_of(key, namespace), [0, 0])
        entry[0] += 1
        entry[1] += size
    return {family: (keys, size) for family, (keys, size) in totals.items()}


def _at(ms: int) -> Any:
    return event_time(dt.datetime.fromtimestamp(ms / 1000, tz=dt.UTC))


def _tx(
    i: int,
    *,
    account: str,
    at_ms: int,
    merchant: str = "mrch_000001",
    device: str = "dev_000000001",
    card: str = "card_000000001",
    ip: str = "ip_0000001",
    mcc: str = "5411",
) -> Event:
    return Event(
        stream=Stream.TRANSACTION,
        occurred_at=_at(at_ms),
        account_id=account,
        event_id=f"tx_{account}_{i:08d}",
        currency="GBP",
        amount_minor=1_000 + i,
        card_id=card,
        device_id=device,
        merchant_id=merchant,
        ip_id=ip,
        merchant_mcc=mcc,
        merchant_country="GB",
        latitude=51.5 + (i % 7) * 0.01,
        longitude=-0.12,
    )


def measure(client: Any, store: Any, now_ms: int) -> dict[str, Curve]:
    """Every family's curve, one scenario at a time on an otherwise empty Redis."""
    points: dict[str, list[tuple[float, float]]] = {}

    def run(scenario: Callable[[], None]) -> dict[str, tuple[int, int]]:
        client.flushall()
        scenario()
        return family_usage(client)

    def per_key(usage: Mapping[str, tuple[int, int]], family: str) -> float:
        keys, size = usage.get(family, (0, 0))
        return size / keys if keys else 0.0

    def add(name: str, size: float, value: float) -> None:
        points.setdefault(name, []).append((float(size), float(value)))

    for n in SIZES:  # one account's raw transactions on one card, device and merchant, 1 s apart

        def raw(n: int = n) -> None:
            for i in range(n):
                store.observe(_tx(i, account="acct_000000001", at_ms=now_ms - (n - i) * 1_000))

        usage = run(raw)
        add("obs", 1, per_key(usage, "obs"))
        for family in ("tx", "card", "dev"):
            add(family, n, usage.get(family, (0, 0))[1])
        for family in ("epoch", "position", "hw"):
            add(family, 1, per_key(usage, family))

    for n in FOLDED_SIZES:  # a fully folded history: n transactions two days back, then one now

        def folded(n: int = n) -> None:
            start = now_ms - 3 * 86_400_000
            for i in range(n):
                store.observe(
                    _tx(
                        i,
                        account="acct_000000002",
                        at_ms=start + i * 60_000,
                        merchant=f"mrch_{i % HABITUAL_MERCHANTS_PER_ACCOUNT:06d}",
                        device=f"dev_{i % 2:09d}",
                        mcc=f"{5400 + i % 4}",
                    )
                )
            store.observe(_tx(n, account="acct_000000002", at_ms=now_ms))

        usage = run(folded)
        for family in ("pf", "pfh", "pfa", "pfl", "pfc"):
            add(family, n, usage.get(family, (0, 0))[1])

    for k in HLL_SIZES:  # k accounts at one IP and one merchant inside one five-minute bucket
        bucket = (now_ms // 300_000) * 300_000

        def sketch(k: int = k, bucket: int = bucket) -> None:
            for i in range(k):
                store.observe(
                    _tx(
                        0,
                        account=f"acct_{i + 10:09d}",
                        at_ms=bucket + i,
                        card=f"card_{i:09d}",
                        device=f"dev_{i:09d}",
                    )
                )

        add("hll", k, per_key(run(sketch), "hll"))

    for m in MINUTE_SIZES:  # one merchant and currency, one transaction per minute

        def minutes(m: int = m) -> None:
            for i in range(m):
                store.observe(
                    _tx(
                        i,
                        account=f"acct_{i % 50 + 100:09d}",
                        at_ms=now_ms - (m - i) * 60_000,
                        card=f"card_{i:09d}",
                        device=f"dev_{i:09d}",
                    )
                )

        add("mcv", m, run(minutes).get("mcv", (0, 0))[1])

    for n in SIZES:  # one account's identity changes

        def identity(n: int = n) -> None:
            for i in range(n):
                store.observe(
                    Event(
                        stream=Stream.IDENTITY_CHANGE,
                        occurred_at=_at(now_ms - (n - i) * 1_000),
                        account_id="acct_000000003",
                        event_id=f"ie_{i:08d}",
                    )
                )

        usage = run(identity)
        add("obs_identity", 1, per_key(usage, "obs"))
        add("ie", n, usage.get("ie", (0, 0))[1])

    for n in OUTCOME_SIZES:  # one account's transactions, each then declined a second later

        def outcomes(n: int = n) -> dict[str, tuple[int, int]]:
            for i in range(n):
                store.observe(_tx(i, account="acct_000000004", at_ms=now_ms - (n - i) * 2_000))
            before = family_usage(client)
            for i in range(n):
                store.observe(
                    authorization_observation(
                        transaction_id=f"tx_acct_000000004_{i:08d}",
                        account_id="acct_000000004",
                        authorization_outcome=AuthorizationOutcome.DECLINED,
                        decided_at=_at(now_ms - (n - i) * 2_000 + 1_000),
                    )
                )
            return before

        client.flushall()
        before = outcomes(n)
        after = family_usage(client)
        added_obs = after.get("obs", (0, 0))[1] - before.get("obs", (0, 0))[1]
        add("obs_outcome", 1, added_obs / n)
        add("aov", 1, per_key(after, "aov"))
        for family in ("ao", "aod"):
            add(family, n, after.get(family, (0, 0))[1])

    drivers = {
        "obs": "one key",
        "obs_identity": "one key",
        "obs_outcome": "one outcome",
        "aov": "one key",
        "epoch": "one key",
        "position": "one key",
        "hw": "one key",
        "hll": "accounts in the bucket",
        "mcv": "minutes held",
        "pf": "folded observations",
        "pfh": "folded observations",
        "pfa": "folded observations",
        "pfl": "folded observations",
        "pfc": "folded observations",
    }
    return {
        name: Curve(name, drivers.get(name, "members"), tuple(sorted(set(values))))
        for name, values in points.items()
    }


# -------------------------------------------------------------- projection ---


@dataclass
class Line:
    family: str
    scope: str
    retention_s: float
    keys: float
    size: float
    bytes_per_key: float
    total_bytes: float
    basis: str


def active_entities(pool: int, arrivals_per_s: float, span_s: float) -> float:
    """Distinct entities of `pool` seen in `span_s` of uniform arrivals."""
    if span_s <= 0 or arrivals_per_s <= 0:
        return 0.0
    return pool * (1.0 - math.exp(-arrivals_per_s * span_s / pool))


def retentions_s() -> dict[str, float]:
    """The store's own retentions (`LAYOUT`), in seconds."""
    from trace_core.repositories.redis_features import LAYOUT

    config = json.loads(LAYOUT.config_json())
    return {
        name: config[f"{name}_ms"] / 1000
        for name in ("raw_tx", "raw_ie", "profile", "card", "dev", "hll", "cv", "ao")
    } | {"hll_bucket": config["hll_bucket_ms"] / 1000, "minute": config["minute_ms"] / 1000}


def project(
    curves: Mapping[str, Curve],
    retention: Mapping[str, float],
    *,
    horizon_s: float,
    tps: float = TARGET_TPS,
) -> list[Line]:
    """Every family the store writes, over `horizon_s` of traffic at `tps`."""
    lines: list[Line] = []

    def line(
        family: str, scope: str, keep: str, keys: float, size: float, per_key: float, basis: str
    ) -> None:
        lines.append(
            Line(family, scope, retention[keep], keys, size, per_key, keys * per_key, basis)
        )

    raw = min(horizon_s, retention["raw_tx"])
    line(
        "obs",
        "transaction",
        "raw_tx",
        tps * raw,
        1,
        curves["obs"].bytes_for(1),
        "one string key per observation, for the raw window",
    )
    accounts = active_entities(ACCOUNTS, tps, raw)
    members = tps * raw / max(accounts, 1.0)
    line(
        "tx",
        "account",
        "raw_tx",
        accounts,
        members,
        curves["tx"].bytes_for(members),
        "measured curve at the active accounts' raw depth",
    )

    span = min(horizon_s, retention["profile"])
    folded_accounts = active_entities(ACCOUNTS, tps, span)
    folded = tps * max(0.0, span - retention["raw_tx"]) / max(folded_accounts, 1.0)
    if folded > 0:
        per_key = sum(curves[f].bytes_for(folded) for f in ("pf", "pfh", "pfa", "pfl", "pfc"))
        line(
            "pf..pfc",
            "account",
            "profile",
            folded_accounts,
            folded,
            per_key,
            "folded profile prefix, measured through the store's own fold",
        )

    for family, pool, keep in (("card", CARDS, "card"), ("dev", DEVICES, "dev")):
        window = min(horizon_s, retention[keep])
        keys = active_entities(pool, tps, window)
        size = tps * window / max(keys, 1.0)
        line(
            family,
            family,
            keep,
            keys,
            size,
            curves[family].bytes_for(size),
            "measured curve at the active entities' depth",
        )

    window = min(horizon_s, retention["hll"])
    for scope, pool in (("IP", IPS), ("MERCHANT", MERCHANTS)):
        entities = active_entities(pool, tps, window)
        per_entity = tps * window / max(entities, 1.0)
        buckets = max(1.0, min(per_entity, window / retention["hll_bucket"]))
        cardinality = per_entity / buckets
        line(
            "hll",
            scope,
            "hll",
            entities * buckets,
            cardinality,
            curves["hll"].bytes_for(cardinality),
            "measured sketch at its bucket's cardinality x active buckets",
        )

    window = min(horizon_s, retention["cv"])
    merchants = active_entities(MERCHANTS, tps, window)
    minutes = min(tps * window / max(merchants, 1.0), window / retention["minute"])
    line(
        "mcv",
        "merchant/GBP",
        "cv",
        merchants,
        minutes,
        curves["mcv"].bytes_for(minutes),
        "measured curve at the minutes each merchant holds",
    )

    identity_rate = tps * IDENTITY_EVENT_FRACTION
    window = min(horizon_s, retention["raw_ie"])
    line(
        "obs",
        "identity",
        "raw_ie",
        identity_rate * window,
        1,
        curves["obs_identity"].bytes_for(1),
        "one string key per identity event, for the raw window",
    )
    identity_accounts = active_entities(ACCOUNTS, identity_rate, window)
    size = identity_rate * window / max(identity_accounts, 1.0)
    line(
        "ie",
        "account",
        "raw_ie",
        identity_accounts,
        size,
        curves["ie"].bytes_for(size),
        "measured curve at the active accounts' depth",
    )

    window = min(horizon_s, retention["ao"])
    outcomes = tps * window
    line(
        "obs+aov",
        "authorization outcome",
        "ao",
        outcomes,
        1,
        curves["obs_outcome"].bytes_for(1) + curves["aov"].bytes_for(1),
        "ASSUMED one outcome per transaction, the upper bound",
    )
    outcome_accounts = active_entities(ACCOUNTS, tps, window)
    size = outcomes / max(outcome_accounts, 1.0)
    line(
        "ao+aod",
        "account",
        "ao",
        outcome_accounts,
        size,
        curves["ao"].bytes_for(size) + curves["aod"].bytes_for(size),
        "ASSUMED one outcome per transaction; measured curve at the accounts' depth",
    )

    for family in ("epoch", "position", "hw"):
        lines.append(
            Line(
                family,
                "store",
                0,
                1,
                1,
                curves[family].bytes_for(1),
                curves[family].bytes_for(1),
                "one key",
            )
        )
    return lines


def configured_limit_mib(ten_minute_bytes: float, headroom: float) -> int:
    """The ten-minute working set with headroom, rounded up to 64 MiB."""
    return int(math.ceil(ten_minute_bytes * headroom / (64 * MIB)) * 64)


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
        with target.open("x") as handle:
            handle.write(json.dumps(asdict(self), indent=2, sort_keys=True) + "\n")
        return target


def _mib(b: float) -> str:
    return f"{b / MIB:,.1f} MiB"


def render(
    record_id: str,
    curves: Mapping[str, Curve],
    ten_min: Iterable[Line],
    steady: Iterable[Line],
    limits: Mapping[str, Any],
    redis_version: str,
) -> str:
    ten_min, steady = list(ten_min), list(steady)

    def table(lines: list[Line]) -> list[str]:
        rows = [
            "| family | scope | retention | keys | size | bytes/key | total | basis |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for ln in sorted(lines, key=lambda x: -x.total_bytes):
            rows.append(
                f"| `{ln.family}` | {ln.scope} | {ln.retention_s:,.0f}s | {ln.keys:,.0f} | "
                f"{ln.size:,.1f} | {ln.bytes_per_key:,.0f} | "
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
        "Measured through `RedisOnlineFeatureStore` itself, the Step 1b layout (ADR-0046 §5), on a",
        f"throwaway `{STORE_IMAGE}` (Redis {redis_version}), with Redis's exact",
        "`MEMORY USAGE` summed by",
        "key family. Population and rate are the representative profile's (ADR-0040), unchanged",
        f"from Phase 2: {ACCOUNTS:,} accounts, {MERCHANTS:,} merchants, {DEVICES:,} devices,",
        f"{IPS:,} IPs, {TARGET_TPS} TPS, uniform arrivals. Retentions are the store's `LAYOUT`.",
        "It supersedes the Phase 2 model (`run_id: bench-20260913-memory-model-5c770259`), which",
        "measured the Phase 2 key shapes.",
        "",
        f"## Measured curves — `run_id: {record_id}`",
        "",
        "| family | size driver | measured points (size, bytes per key) |",
        "|---|---|---|",
    ]
    for curve in curves.values():
        pts = ", ".join(f"({s:,.0f}, {b:,.0f})" for s, b in curve.points)
        out.append(f"| `{curve.family}` | {curve.driver} | {pts} |")
    out += [
        "",
        f"## The ten-minute acceptance run — `run_id: {record_id}`",
        "",
        *table(ten_min),
        "",
        f"**Configured feature-store limit: {limits['feature_maxmemory']}**: the projection above",
        f"with {limits['headroom_factor']}x headroom for fragmentation and for the model's",
        "own error,",
        "rounded up to 64 MiB. The store runs with `noeviction`, so exceeding it is a refused",
        "write",
        "and a failed run, not a silent loss.",
        "",
        f"## Steady state at {TARGET_TPS} TPS with the declared retention — `run_id: {record_id}`",
        "",
        *table(steady),
        "",
        "This is the capacity requirement for serving the target rate indefinitely, and it is not",
        "expected to fit a laptop. Lines marked ASSUMED rest on a stated upper bound, not on the",
        "profile.",
        "",
    ]
    return "\n".join(out) + "\n"


# ------------------------------------------------------------- operations ---


def compose_store_image(compose_text: str) -> str:
    """The image of the compose `redis` service: the feature store."""
    match = re.search(r"^  redis:\n(?:    .*\n)*?    image: (\S+)", compose_text, re.MULTILINE)
    if match is None:
        raise SystemExit("deploy/compose.yml has no `redis` service image")
    return match.group(1)


def _docker(*args: str) -> subprocess.CompletedProcess[str]:
    binary = shutil.which("docker") or "docker"
    return subprocess.run([binary, *args], capture_output=True, text=True, timeout=120)  # noqa: S603


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--headroom", type=float, default=1.25)
    args = parser.parse_args()

    image = compose_store_image(COMPOSE.read_text())
    if image != STORE_IMAGE:
        raise SystemExit(
            f"the compose feature store runs {image}, not {STORE_IMAGE}: "
            "refusing to measure another"
        )
    import redis

    from trace_core.repositories.redis_features import RedisOnlineFeatureStore

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = int(probe.getsockname()[1])
    name = f"tracex-memory-model-{uuid.uuid4().hex[:8]}"
    started_container = _docker(
        "run",
        "-d",
        "--rm",
        "--name",
        name,
        "-p",
        f"127.0.0.1:{port}:6379",
        STORE_IMAGE,
        "redis-server",
        "--appendonly",
        "no",
        "--save",
        "",
    )
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
        curves = measure(client, store, int(time.time() * 1000))
    finally:
        _docker("rm", "-f", name)

    retention = retentions_s()
    ten_min = project(curves, retention, horizon_s=CANONICAL_WINDOW_S)
    steady = project(curves, retention, horizon_s=retention["profile"])
    ten_total = sum(x.total_bytes for x in ten_min)
    steady_total = sum(x.total_bytes for x in steady)
    limit_mib = configured_limit_mib(ten_total, args.headroom)
    record_id = f"bench-{started:%Y%m%d-%H%M%S}-memory-model-{git_commit_sha()[:8]}"
    limits = {"feature_maxmemory": f"{limit_mib}mb", "headroom_factor": args.headroom}
    record = BenchmarkRunRecord(
        run_id=record_id,
        started_at=started.isoformat(),
        finished_at=dt.datetime.now(dt.UTC).isoformat(),
        tool_version=redis_version,
        measured={
            "store_image": STORE_IMAGE,
            "curves": {k: asdict(v) for k, v in curves.items()},
            "retention_s": retention,
            "population": {
                "accounts": ACCOUNTS,
                "cards": CARDS,
                "devices": DEVICES,
                "merchants": MERCHANTS,
                "ips": IPS,
            },
            "target_tps": TARGET_TPS,
            "ten_minute_bytes": ten_total,
            "ten_minute_lines": [asdict(x) for x in ten_min],
            "steady_state_bytes": steady_total,
            "steady_state_lines": [asdict(x) for x in steady],
            "configured_feature_maxmemory_mib": limit_mib,
            "headroom_factor": args.headroom,
        },
    )
    path = record.write()
    REPORT.write_text(render(record_id, curves, ten_min, steady, limits, redis_version))
    print(f"run_id: {record_id}")
    print(f"record: {path.relative_to(ROOT)}")
    print(f"report: {REPORT.relative_to(ROOT)}")
    print(
        f"ten-minute run : {_mib(ten_total)} -> configured limit {limit_mib} MiB ({args.headroom}x)"
    )
    print(f"steady state   : {_mib(steady_total)} ({steady_total / 1_073_741_824:.2f} GiB)")
    if record.dirty_worktree:
        print("NOTE: dirty worktree -- the record is not publishable until run on a clean commit.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
