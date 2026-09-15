#!/usr/bin/env python3
"""The observation log's controlled hot-path A/B, as ADR-0051 §7 pre-registers it.

    python scripts/observation_log_ab.py run --record-dir /tmp/tracex-ab     # the six runs
    python scripts/observation_log_ab.py report --record-dir /tmp/tracex-ab  # the comparison

One gateway image, three arms that differ only in configuration, run in the pre-registered order.
Before every run this script:
- stops the gateway;
- flushes both Redis instances;
- truncates the `app` tables a run writes;
- deletes the covered topics and recreates them from their declarations.

It verifies each is empty, then starts the gateway with the arm's configuration and waits until
`/readyz` is ready and its checks match the arm. Only then does it run `scripts/load_gateway.py` in
experiment mode, which writes the record outside the repository so every run stays publishable.

`report` applies the pre-registered rule to the records and renders the comparison, with every
numeric row citing its `run_id`. The records are copied into `eval/manifest/` in the same step, so
`make check-claims` resolves them.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

ROOT: Final = Path(__file__).resolve().parent.parent
MANIFEST_DIR: Final = ROOT / "eval" / "manifest"
REPORT: Final = ROOT / "benchmarks" / "gateway" / "observation-log-ab.md"
EXPERIMENT: Final = "observation-log-ab"
BROKER_IN_NETWORK: Final = "kafka:19092"
BROKER_ON_HOST: Final = "localhost:9092"
ARMS: Final[Mapping[str, Mapping[str, str]]] = {
    "log-off": {"TRACE_GATEWAY_KAFKA_BOOTSTRAP": "", "TRACE_GATEWAY_OUTBOX_RELAY": "false"},
    "log-on": {
        "TRACE_GATEWAY_KAFKA_BOOTSTRAP": BROKER_IN_NETWORK,
        "TRACE_GATEWAY_OUTBOX_RELAY": "false",
    },
    "log-relay": {
        "TRACE_GATEWAY_KAFKA_BOOTSTRAP": BROKER_IN_NETWORK,
        "TRACE_GATEWAY_OUTBOX_RELAY": "true",
    },
}
EXPECTED_CHECKS: Final[Mapping[str, Mapping[str, str]]] = {
    "log-off": {"observation_log": "not configured", "outbox_relay": "disabled"},
    "log-on": {"observation_log": "ok", "outbox_relay": "disabled"},
    "log-relay": {"observation_log": "ok", "outbox_relay": "running"},
}
"""A check's value must START with the expected text; the writer session must be active."""
ORDER: Final = ("log-off", "log-on", "log-relay", "log-relay", "log-on", "log-off")
TOPICS: Final = (
    "tx.scored.v1",
    "identity.events.v1",
    "investigation.requested.v1",
    "tx.authorization.v1",
)
TABLES: Final = (
    "app.case_transitions",
    "app.investigation_queue",
    "app.outbox",
    "app.authorization_outcomes",
    "app.cases",
    "app.feature_store_holes",
)
REDIS_CONTAINERS: Final = ("tracex-redis-1", "tracex-redis-cache-1")
COMPOSE: Final = ("docker", "compose", "-f", "deploy/compose.yml", "--env-file", ".env")
GATEWAY_IMAGE: Final = "tracex-gateway:dev"
IMAGE_RECORD: Final = "gateway-image.json"
"""Beside the run records: the one image every run of an experiment used, and its commit."""
METRICS: Final = (
    ("server_p99_ms", "scoring-core p99 (ms)"),
    ("achieved_tps", "achieved rate (req/s)"),
)


class ExperimentError(RuntimeError):
    """The experiment cannot proceed without producing an unattributable number."""


# ------------------------------------------------------------------ analysis --


@dataclass(frozen=True)
class Result:
    arm: str
    run_id: str
    started_at: str
    server_p99_ms: float
    achieved_tps: float
    client_p99_ms: float
    dropped_iterations: int
    http_5xx: int
    gateway_checks: Mapping[str, str]
    git_commit_sha: str
    dirty_worktree: bool


@dataclass(frozen=True)
class Comparison:
    metric: str
    left: str
    right: str
    left_mean: float
    right_mean: float
    difference: float
    noise: float

    @property
    def within_noise(self) -> bool:
        return self.difference <= self.noise


@dataclass(frozen=True)
class Decision:
    relay: tuple[Comparison, ...]
    publisher: tuple[Comparison, ...]

    @property
    def keep_option_a(self) -> bool:
        return all(comparison.within_noise for comparison in self.relay)


def load_results(record_dir: Path) -> list[Result]:
    """Every record of this experiment in `record_dir`, in the order the runs started."""
    results: list[Result] = []
    for path in sorted(record_dir.glob("load-*.json")):
        record = json.loads(path.read_text())
        if record.get("experiment") != EXPERIMENT:
            continue
        measured = record["measured"]
        results.append(
            Result(
                arm=str(record["arm"]),
                run_id=str(record["run_id"]),
                started_at=str(record["started_at"]),
                server_p99_ms=float(measured["server_p99_ms"]),
                achieved_tps=float(measured["achieved_tps"]),
                client_p99_ms=float(measured["client_p99_ms"]),
                dropped_iterations=int(measured["dropped_iterations"]),
                http_5xx=int(measured["http_5xx"]),
                gateway_checks=dict(record.get("gateway_checks") or {}),
                git_commit_sha=str(record["git_commit_sha"]),
                dirty_worktree=bool(record["dirty_worktree"]),
            )
        )
    return sorted(results, key=lambda result: result.started_at)


def load_image(record_dir: Path) -> dict[str, str]:
    path = record_dir / IMAGE_RECORD
    if not path.exists():
        raise ExperimentError(f"{path} is missing: the runs' gateway image is unknown")
    record = json.loads(path.read_text())
    return {"git_commit_sha": str(record["git_commit_sha"]), "image_id": str(record["image_id"])}


def check_image(results: Sequence[Result], image: Mapping[str, str]) -> None:
    """Refuse runs whose commit is not the one the pinned gateway image was built from."""
    if any(result.git_commit_sha != image["git_commit_sha"] for result in results):
        raise ExperimentError(
            f"the runs' commit is not the pinned image's ({image['git_commit_sha']}): the numbers "
            f"would be attributed to code the gateway did not run"
        )


def check_arms(results: Sequence[Result]) -> None:
    """Refuse a comparison the pre-registration does not describe."""
    if tuple(result.arm for result in results) != ORDER:
        raise ExperimentError(
            f"the runs are {[r.arm for r in results]}, not the pre-registered order {list(ORDER)}"
        )
    if len({result.git_commit_sha for result in results}) != 1:
        raise ExperimentError("the runs were recorded at different commits")
    if any(result.dirty_worktree for result in results):
        raise ExperimentError("a run was recorded on a dirty worktree; it cannot be published")
    for result in results:
        for check, expected in EXPECTED_CHECKS[result.arm].items():
            if not result.gateway_checks.get(check, "").startswith(expected):
                raise ExperimentError(
                    f"{result.run_id} is labelled {result.arm} but its gateway reported "
                    f"{check}={result.gateway_checks.get(check)!r}"
                )


def _values(results: Iterable[Result], arm: str, metric: str) -> list[float]:
    return [float(getattr(result, metric)) for result in results if result.arm == arm]


def compare(results: Sequence[Result], left: str, right: str, metric: str) -> Comparison:
    """The difference of two arms' means against the larger of their own run-to-run spreads."""
    lefts, rights = _values(results, left, metric), _values(results, right, metric)
    if len(lefts) != 2 or len(rights) != 2:
        raise ExperimentError(f"{metric}: expected two runs of {left} and of {right}")
    return Comparison(
        metric=metric,
        left=left,
        right=right,
        left_mean=statistics.fmean(lefts),
        right_mean=statistics.fmean(rights),
        difference=abs(statistics.fmean(rights) - statistics.fmean(lefts)),
        noise=max(max(lefts) - min(lefts), max(rights) - min(rights)),
    )


def decide(results: Sequence[Result]) -> Decision:
    """ADR-0051 §7's pre-registered rule, applied to the records."""
    check_arms(results)
    return Decision(
        relay=tuple(compare(results, "log-on", "log-relay", metric) for metric, _ in METRICS),
        publisher=tuple(compare(results, "log-off", "log-on", metric) for metric, _ in METRICS),
    )


def render(results: Sequence[Result], decision: Decision, *, image_id: str) -> str:
    """The comparison. Every line carrying a measured number cites the run_id(s) it comes from."""
    runs = {arm: [r.run_id for r in results if r.arm == arm] for arm in ARMS}
    lines = [
        "# trace-gateway — observation log hot-path A/B",
        "",
        "> Written by `scripts/observation_log_ab.py report`, never by hand. The arms, the",
        "> workload, the state reset and the rule were pre-registered in ADR-0051 §7 before the",
        "> first run.",
        ">",
        "> These are `LOADTEST` records: they exercised the service, not a model. They can",
        "> substantiate latency and throughput, never a quality claim (`docs/EVALUATION.md` §8",
        "> rule 2).",
        "",
        "## Method",
        "",
        "- One gateway image; the arms differ only in configuration, read back from each gateway's",
        "  own `/readyz` checks into its record.",
        "- `log-off`: no broker configured. `log-on`: the observation log publishing.",
        "  `log-relay`: the observation log publishing and the outbox relay in the gateway.",
        "- Workload `representative`, its seed, the same offered rate and window for every run,",
        "  from flushed stores, truncated tables and recreated topics, verified empty before each",
        "  run.",
        f"- Order: {', '.join(f'`{arm}`' for arm in ORDER)}.",
        f"- Commit `{results[0].git_commit_sha}`, clean worktree for every run.",
        f"- Gateway image `{image_id}`, built once from that commit and checked before every run.",
        "",
        "## Runs",
        "",
        "| # | arm | run | scoring-core p99 (ms) | achieved rate (req/s) | client p99 (ms) "
        "| dropped | 5xx |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for index, result in enumerate(results, start=1):
        lines.append(
            f"| {index} | `{result.arm}` | `run_id: {result.run_id}` | {result.server_p99_ms:.3f} "
            f"| {result.achieved_tps:.2f} | {result.client_p99_ms:.2f} "
            f"| {result.dropped_iterations} | {result.http_5xx} |"
        )
    for title, comparisons in (
        ("The relay's process (the pre-registered rule)", decision.relay),
        ("The publisher's cost (reported, no threshold)", decision.publisher),
    ):
        lines += [
            "",
            f"## {title}",
            "",
            "| metric | arms | means | difference | noise | within noise |",
            "|---|---|---|---|---|---|",
        ]
        for c in comparisons:
            cited = ", ".join(f"`run_id: {rid}`" for rid in (*runs[c.left], *runs[c.right]))
            label = dict(METRICS)[c.metric]
            lines.append(
                f"| {label} | `{c.left}` vs `{c.right}` | {c.left_mean:.3f} vs {c.right_mean:.3f} "
                f"| {c.difference:.3f} | {c.noise:.3f} | {'yes' if c.within_noise else 'no'} "
                f"| {cited} |"
            )
    verdict = (
        "**Option A is kept:** the relay stays in the gateway, because `log-relay` stayed within "
        "the no-relay control's run-to-run noise on both metrics."
        if decision.keep_option_a
        else "**Option A is rejected:** the relay moves to option B, because `log-relay` left the "
        "no-relay control's run-to-run noise on at least one metric. ADR-0049 is updated to match."
    )
    lines += ["", "## Decision", "", verdict, ""]
    return "\n".join(lines)


# --------------------------------------------------------------- operations --


def _run(
    command: Sequence[str], *, env: Mapping[str, str] | None = None, check: bool = True
) -> str:
    done = subprocess.run(  # noqa: S603
        list(command),
        cwd=ROOT,
        env=dict(env) if env is not None else None,
        capture_output=True,
        text=True,
        timeout=600,
    )
    if check and done.returncode != 0:
        raise ExperimentError(f"{' '.join(command)} exited {done.returncode}: {done.stderr[-800:]}")
    return done.stdout


def _dotenv() -> dict[str, str]:
    values: dict[str, str] = {}
    for line in (ROOT / ".env").read_text().splitlines():
        if line and not line.lstrip().startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def reset_stores(env: Mapping[str, str]) -> None:
    """Stop the gateway, then empty and verify every store a run writes."""
    import psycopg
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient
    from psycopg import sql

    _run([*COMPOSE, "stop", "gateway"])
    for container in REDIS_CONTAINERS:
        _run(["docker", "exec", container, "redis-cli", "FLUSHALL"])
        if _run(["docker", "exec", container, "redis-cli", "DBSIZE"]).strip() != "0":
            raise ExperimentError(f"{container} is not empty after FLUSHALL")
    dsn = "postgresql://{u}:{p}@localhost:{port}/{db}".format(
        u=env["POSTGRES_SUPERUSER"],
        p=env["POSTGRES_SUPERUSER_PASSWORD"],
        port=env.get("POSTGRES_PORT", "5442"),
        db=env.get("POSTGRES_DB", "tracex"),
    )
    identifiers = [sql.Identifier(*table.split(".")) for table in TABLES]
    with psycopg.connect(dsn, autocommit=True) as owner:
        owner.execute(sql.SQL("TRUNCATE {}").format(sql.SQL(", ").join(identifiers)))
        for table, identifier in zip(TABLES, identifiers, strict=True):
            row = owner.execute(sql.SQL("SELECT count(*) FROM {}").format(identifier)).fetchone()
            if row is None or row[0] != 0:
                raise ExperimentError(f"{table} is not empty after TRUNCATE")
    admin = AdminClient({"bootstrap.servers": BROKER_ON_HOST})
    existing = set(admin.list_topics(timeout=10).topics)
    doomed: list[str] = [topic for topic in TOPICS if topic in existing]
    for future in admin.delete_topics(doomed, operation_timeout=30).values():
        future.result(timeout=60)
    deadline = time.monotonic() + 60
    while set(TOPICS) & set(admin.list_topics(timeout=10).topics):
        if time.monotonic() > deadline:
            raise ExperimentError("the covered topics were not deleted within 60 s")
        time.sleep(1)
    time.sleep(2)
    _run([sys.executable, "scripts/kafka_topics.py", "apply", "--environment", "local"])
    consumer = Consumer({"bootstrap.servers": BROKER_ON_HOST, "group.id": "observation-log-ab"})
    try:
        for topic in TOPICS:
            for partition in consumer.list_topics(topic, timeout=10).topics[topic].partitions:
                high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)[
                    1
                ]
                if high != 0:
                    raise ExperimentError(f"{topic}[{partition}] is not empty after recreation")
    finally:
        consumer.close()


def build_gateway_image(record_dir: Path, head: str) -> str:
    """Build the gateway image from the committed tree once, and pin it for every run.

    Compose starts the gateway without building, so an image left over from an older tree would
    run silently, and every number would be attributed to a commit it did not come from. The
    record is created exclusively: one experiment, one image.
    """
    _run([*COMPOSE, "--profile", "core", "build", "gateway"])
    image_id = _run(["docker", "image", "inspect", "--format", "{{.Id}}", GATEWAY_IMAGE]).strip()
    if not image_id.startswith("sha256:"):
        raise ExperimentError(f"{GATEWAY_IMAGE} has no image id after the build: {image_id!r}")
    with (record_dir / IMAGE_RECORD).open("x") as handle:
        json.dump({"git_commit_sha": head, "image_id": image_id}, handle, sort_keys=True)
        handle.write("\n")
    return image_id


def require_pinned(head: str, image_id: str) -> None:
    """Before a run: the tree is still that commit, clean, and the gateway runs the pinned image."""
    moved = _run(["git", "rev-parse", "HEAD"]).strip() != head
    if moved or _run(["git", "status", "--porcelain"]).strip():
        raise ExperimentError("the worktree moved or became dirty during the experiment")
    container = _run([*COMPOSE, "ps", "-q", "gateway"]).strip()
    running = (
        _run(["docker", "inspect", "--format", "{{.Image}}", container]).strip()
        if container
        else ""
    )
    if running != image_id:
        raise ExperimentError(
            f"the gateway runs image {running or 'none'}, not the pinned {image_id}"
        )


def start_gateway(arm: str, base_url: str) -> dict[str, str]:
    """Start the gateway with the arm's configuration; return its checks once they match."""
    _run(
        [*COMPOSE, "--profile", "core", "up", "-d", "--no-deps", "--force-recreate", "gateway"],
        env={**os.environ, **ARMS[arm]},
    )
    deadline = time.monotonic() + 120
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{base_url}/readyz", timeout=5) as response:  # noqa: S310  # nosec B310
                last = json.loads(response.read())
        except Exception as exc:  # not up yet, or 503 while not ready
            last = {"error": type(exc).__name__}
        checks = last.get("checks") if isinstance(last, dict) else None
        if isinstance(checks, dict) and last.get("ready") is True:
            matches = str(checks.get("writer_session", "")).startswith("active") and all(
                str(checks.get(k, "")).startswith(v) for k, v in EXPECTED_CHECKS[arm].items()
            )
            if matches:
                return {str(k): str(v) for k, v in checks.items()}
        time.sleep(1)
    raise ExperimentError(f"the gateway never became ready as {arm}: {last}")


def run_all(record_dir: Path, *, duration_s: int, target_tps: int, base_url: str) -> None:
    if _run(["git", "status", "--porcelain"]).strip():
        raise ExperimentError(
            "the worktree is dirty: commit before the runs, or none is publishable"
        )
    head = _run(["git", "rev-parse", "HEAD"]).strip()
    env = {**_dotenv(), **os.environ}
    record_dir.mkdir(parents=True, exist_ok=True)
    image_id = build_gateway_image(record_dir, head)
    _run([*COMPOSE, "--profile", "streaming", "up", "-d", "--wait", "kafka"])
    for index, arm in enumerate(ORDER, start=1):
        before = set(record_dir.glob("load-*.json"))
        print(f"[{index}/{len(ORDER)}] {arm}: resetting stores", flush=True)
        reset_stores(env)
        checks = start_gateway(arm, base_url)
        require_pinned(head, image_id)
        print(f"[{index}/{len(ORDER)}] {arm}: gateway ready {checks}", flush=True)
        k6_dir = record_dir / f"k6-{index}-{arm}"
        done = subprocess.run(  # noqa: S603
            [
                sys.executable,
                "scripts/load_gateway.py",
                "--experiment",
                EXPERIMENT,
                "--arm",
                arm,
                "--record-dir",
                str(record_dir),
                "--duration-s",
                str(duration_s),
                "--target-tps",
                str(target_tps),
                "--out-dir",
                str(k6_dir),
                "--base-url",
                base_url,
            ],
            cwd=ROOT,
            env=env,
            text=True,
            timeout=duration_s + 900,
        )
        written = set(record_dir.glob("load-*.json")) - before
        if len(written) != 1:
            raise ExperimentError(
                f"run {index} ({arm}) wrote {len(written)} records (exit {done.returncode}); the "
                f"pre-registration says a failed run is recorded and repeated, never dropped: "
                f"investigate, then re-run the experiment"
            )
        print(f"[{index}/{len(ORDER)}] {arm}: recorded {next(iter(written)).name}", flush=True)


def publish(record_dir: Path) -> Decision:
    results = load_results(record_dir)
    image = load_image(record_dir)
    decision = decide(results)
    check_image(results, image)
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    for result in results:
        shutil.copy2(record_dir / f"{result.run_id}.json", MANIFEST_DIR / f"{result.run_id}.json")
    REPORT.write_text(render(results, decision, image_id=image["image_id"]))
    return decision


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("command", choices=("run", "report"))
    parser.add_argument("--record-dir", type=Path, required=True)
    parser.add_argument("--duration-s", type=int, default=180)
    parser.add_argument("--target-tps", type=int, default=500)
    parser.add_argument("--base-url", default="http://localhost:8010")
    args = parser.parse_args(argv)
    # The one scheme check `start_gateway`'s urlopen relies on (bandit B310).
    if urllib.parse.urlparse(args.base_url).scheme not in {"http", "https"}:
        print(f"refused: --base-url {args.base_url!r} is not an HTTP URL", file=sys.stderr)
        return 2
    resolved = args.record_dir.resolve()
    if resolved == ROOT or ROOT in resolved.parents:
        print("refused: --record-dir must be outside the repository", file=sys.stderr)
        return 2
    try:
        if args.command == "run":
            run_all(
                resolved,
                duration_s=args.duration_s,
                target_tps=args.target_tps,
                base_url=args.base_url,
            )
            print(f"done at {dt.datetime.now(dt.UTC).isoformat()}; now run `report`")
            return 0
        decision = publish(resolved)
        print(f"report: {REPORT.relative_to(ROOT)}; option A kept: {decision.keep_option_a}")
        return 0
    except ExperimentError as exc:
        print(f"observation-log-ab: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
