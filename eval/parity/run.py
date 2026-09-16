"""A parity run: a partition through the gateway and Kafka, then Bronze, Silver and Gold on Spark,
then every comparison, the guard and a PARITY record (ADR-0056 §5).

    python -m eval.parity.run --partition representative --mode measured \\
        --dataset-dir data/generated/eval-v2 --gateway-url http://localhost:8010 \\
        --gateway-container tracex-gateway-1 --bootstrap localhost:9092 --lake /tmp/parity-lake

**Diagnostic** mode is for development. It runs on any worktree, needs `--output-dir`, and its
record says NOT publishable. **Measured** mode is for acceptance. It refuses a dirty worktree, a
dataset that does not verify against the eval-v2 manifest, observation topics that already hold
records, an existing lake, a feature store holding anything, and a gateway whose image digest
cannot be read. It writes to `eval/manifest/`.

What a run needs from its environment, and why: a gateway that is the online store's only writer
and scores nothing else during the run, because the as-served order is the driver's order
(`driver`); empty observation topics, because the complete history is everything Bronze reads; and
the outbox relayed to Kafka, because Gold reads authorization outcomes from `tx.authorization.v1`.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import platform
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

from data.generator.record import env_lock_digest, git_commit_sha, is_dirty
from eval.parity.comparator import Pairing, ParityTally
from eval.parity.dataset import load_partition, verify_dataset
from eval.parity.driver import (
    IDENTITY_EVENTS,
    TRANSACTIONS,
    Applied,
    GatewayDriver,
    HttpClient,
    Posted,
    iso_millis,
    shifted,
)
from eval.parity.evaluate import (
    Evaluation,
    HistoryMismatchError,
    LinkError,
    OrderEvidenceError,
    evaluate,
)
from eval.parity.guard import GuardViolation, SparkEvidence, collection_violations
from eval.parity.lake import (
    LAKE_TOPICS,
    complete_history,
    gold_context_rows,
    run_medallion,
    served_deliveries,
    toolchain,
)
from eval.parity.partition import PARITY_SEMANTICS_VERSION, PARTITIONS, LatenessModel
from eval.parity.record import RECORD_TYPE, run_id_for, write_record
from eval.parity.served import parse_time
from eval.parity.skew import SkewTally, SkewVerdict
from eval.replay.faults import OVERLAY_VERSION, ReplayScheduleError, build_overlay, publish_overlay
from eval.replay.gateway_replay import vouch

from trace_core.contracts.topics import IDENTITY_EVENTS_V1, TX_AUTHORIZATION_V1, TX_SCORED_V1
from trace_core.domain.time import to_millis
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.semantics import ParityComparison
from trace_core.features.spec import FEATURE_SET_VERSION, require_served_conformance
from trace_core.stream.lake import LakeConfig

ROOT: Final = Path(__file__).resolve().parents[2]
MANIFEST_DIR: Final = ROOT / "eval" / "manifest"
MIN_WARMUP_POSTS_PER_S: Final = 20.0
"""The slowest warm-up posting rate the anchor allows for; a slower gateway stops the run."""
ANCHOR_MARGIN_S: Final = 10.0
DELIVERY_TIMEOUT_S: Final = 300.0


class Mode(StrEnum):
    DIAGNOSTIC = "diagnostic"
    MEASURED = "measured"


class RunRefusedError(RuntimeError):
    """Refused before posting anything."""


@dataclass(frozen=True)
class RunPlan:
    name: str
    gated: bool
    base_ref: str
    warmup: Sequence[tuple[str, Mapping[str, Any]]]
    slice_events: Sequence[tuple[str, Mapping[str, Any]]]
    lateness: LatenessModel
    partition: Mapping[str, Any]
    dataset: Mapping[str, Any]


@dataclass
class Services:
    client: HttpClient
    token: str
    feature_redis: Any
    open_holes: Callable[[], int]
    bootstrap: str
    spark: Any
    lake: LakeConfig
    gateway: Mapping[str, Any]
    stop_gateway: Callable[[], None] | None = None


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def approximate_features() -> list[str]:
    return sorted(s.feature_id for s in ONLINE_FEATURES if s.parity is ParityComparison.APPROXIMATE)


def topic_offsets(bootstrap: str) -> dict[str, int]:
    """The sum of each observation topic's high watermarks."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {"bootstrap.servers": bootstrap, "group.id": "trace-x-parity", "enable.auto.commit": False}
    )
    try:
        totals: dict[str, int] = {}
        for topic in LAKE_TOPICS:
            metadata = consumer.list_topics(topic, timeout=10).topics.get(topic)
            if metadata is None or metadata.error is not None or not metadata.partitions:
                raise RunRefusedError(f"{topic} does not exist on {bootstrap}")
            totals[topic] = sum(
                consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
                for p in metadata.partitions
            )
        return totals
    finally:
        consumer.close()


def expected_deliveries(posts: Sequence[Posted]) -> dict[str, int]:
    """What the gateway must have published for these posts (ADR-0051 §3; ADR-0049 §4)."""
    return {
        TX_SCORED_V1: sum(
            1 for p in posts if p.topic == TRANSACTIONS and p.applied is Applied.SCORED
        ),
        IDENTITY_EVENTS_V1: sum(
            1 for p in posts if p.topic == IDENTITY_EVENTS and p.applied is Applied.OBSERVED
        ),
        TX_AUTHORIZATION_V1: len(
            {
                str(p.request["transaction_id"])
                for p in posts
                if p.topic == TX_AUTHORIZATION_V1 and p.applied is Applied.OBSERVED
            }
        ),
    }


def wait_for_deliveries(
    bootstrap: str,
    before: Mapping[str, int],
    expected: Mapping[str, int],
    *,
    timeout_s: float = DELIVERY_TIMEOUT_S,
    clock: Callable[[], dt.datetime] = _utc_now,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, int]:
    """Wait until every expected record is on Kafka. The outbox relay delivers at least once, so
    outcomes may exceed; any other excess means another producer shares the topics."""
    deadline = clock() + dt.timedelta(seconds=timeout_s)
    while True:
        now = topic_offsets(bootstrap)
        arrived = {topic: now[topic] - before[topic] for topic in LAKE_TOPICS}
        excess = [t for t in (TX_SCORED_V1, IDENTITY_EVENTS_V1) if arrived[t] > expected[t]]
        if excess:
            raise RunRefusedError(
                f"{excess} hold more records than this run published ({arrived} for {expected}): "
                f"another producer shares the topics"
            )
        if all(arrived[t] >= expected[t] for t in LAKE_TOPICS):
            return arrived
        if clock() > deadline:
            raise TimeoutError(f"after {timeout_s}s Kafka holds {arrived} of {expected}")
        sleep(1.0)


def verdict_of(
    evaluation: Evaluation | None, violations: Sequence[GuardViolation], *, mode: Mode, gated: bool
) -> str:
    if evaluation is None:
        return "ERROR"
    if mode is Mode.DIAGNOSTIC:
        return "DIAGNOSTIC"
    skew = evaluation.skew.verdict(gated=gated, measured=True)
    failed = (
        evaluation.as_served.divergent_total
        or evaluation.complete.divergent_total
        or evaluation.as_served.approximate_failures()
        or violations
        or skew in (SkewVerdict.FAIL, SkewVerdict.NO_DATA)
    )
    return "FAIL" if failed else "PASS"


def exit_code(
    evaluation: Evaluation | None, violations: Sequence[GuardViolation], verdict: str
) -> int:
    if verdict in ("PASS", "FAIL"):
        return 0 if verdict == "PASS" else 1
    if evaluation is None:
        return 1
    broken = (
        evaluation.as_served.divergent_total
        or evaluation.complete.divergent_total
        or evaluation.as_served.approximate_failures()
        or any(v.vacuity for v in violations)
    )
    return 1 if broken else 0


def execute(
    plan: RunPlan,
    services: Services,
    *,
    mode: Mode,
    output_dir: Path,
    clock: Callable[[], dt.datetime] = _utc_now,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[dict[str, Any], Path, int]:
    require_served_conformance("the parity run")
    started = clock()
    sha, dirty = git_commit_sha(), is_dirty()
    if mode is Mode.MEASURED:
        problems = [
            *(["the worktree is dirty"] if dirty else []),
            *(
                []
                if plan.dataset.get("eval_v2_manifest_digest")
                else ["no verified eval-v2 dataset"]
            ),
            *(
                []
                if services.gateway.get("image_digest")
                else ["the gateway image digest is unknown"]
            ),
            *(["the lake already exists"] if Path(str(services.lake.root)).exists() else []),
        ]
        if problems:
            raise RunRefusedError(f"a measured run refuses: {problems}")

    overlay = build_overlay(
        list(plan.slice_events),
        plan.lateness.plan(len(plan.slice_events)),
        base_dataset_ref=plan.base_ref,
    )
    base_start = parse_time(overlay.provenance.base_start)
    lead_s = ANCHOR_MARGIN_S + len(plan.warmup) / MIN_WARMUP_POSTS_PER_S
    anchor = clock() + dt.timedelta(seconds=lead_s)
    shift = anchor - base_start
    before = topic_offsets(services.bootstrap)
    if mode is Mode.MEASURED and any(before.values()):
        raise RunRefusedError(f"the observation topics already hold records: {before}")
    earliest = min(
        parse_time(str(event["envelope"]["occurred_at"]))
        for _topic, event in (*plan.warmup, *plan.slice_events)
    )
    epoch_ms = to_millis(earliest + shift)
    vouch(services.feature_redis, epoch_ms=epoch_ms, open_holes=services.open_holes())

    driver = GatewayDriver(services.client, token=services.token, shift=shift)
    for topic, event in plan.warmup:
        driver.post(topic, shifted(event, shift))
    if clock() > anchor - dt.timedelta(seconds=1):
        raise ReplayScheduleError(
            f"the warm-up of {len(plan.warmup)} events ran until {iso_millis(clock())}, past the "
            f"slice's anchor {iso_millis(anchor)}"
        )
    driver.phase = "slice"
    published = publish_overlay(overlay, driver, anchor=anchor, clock=clock, sleep=sleep)
    driver.label_faults(
        [p.record.fault.value if p.record.fault is not None else None for p in published.records]
    )
    expected = expected_deliveries(driver.posts)
    arrived = wait_for_deliveries(services.bootstrap, before, expected, clock=clock, sleep=sleep)
    if services.stop_gateway is not None:
        services.stop_gateway()

    build, evidence = run_medallion(
        services.spark,
        services.lake,
        bootstrap=services.bootstrap,
        git_sha=sha,
        dirty_worktree=dirty,
    )
    evaluation: Evaluation | None = None
    error: str | None = None
    try:
        evaluation = evaluate(
            driver.posts,
            served_deliveries(services.spark, services.lake),
            complete_history(services.spark, services.lake, build),
            gold_context_rows(services.spark, services.lake, build),
        )
    except (LinkError, OrderEvidenceError, HistoryMismatchError) as exc:
        error = f"{type(exc).__name__}: {exc}"

    approximate = approximate_features()
    violations = (
        collection_violations(
            tallies=(evaluation.as_served, evaluation.complete),
            approximate_features=approximate,
            skew=evaluation.skew,
            spark=evidence,
            gated=plan.gated,
        )
        if evaluation is not None
        else []
    )
    verdict = verdict_of(evaluation, violations, mode=mode, gated=plan.gated)
    measured = mode is Mode.MEASURED
    results = (
        evaluation.as_dict(approximate_features=approximate, gated=plan.gated, measured=measured)
        if evaluation is not None
        else {"error": error}
    )
    record: dict[str, Any] = {
        "record_type": RECORD_TYPE,
        "run_id": run_id_for(plan.name, started, sha),
        "mode": mode.value,
        "publishable": verdict == "PASS" and measured and not dirty,
        "track": "SYNTHETIC",
        "git_commit_sha": sha,
        "dirty_worktree": dirty,
        "env_lock_digest": env_lock_digest(),
        "python_version": platform.python_version(),
        "started_at": iso_millis(started),
        "finished_at": iso_millis(clock()),
        "parity_semantics_version": PARITY_SEMANTICS_VERSION,
        "feature_set_version": FEATURE_SET_VERSION,
        "partition": dict(plan.partition),
        "dataset": dict(plan.dataset),
        "overlay": {
            "version": OVERLAY_VERSION,
            "seed": plan.lateness.seed,
            "provenance": overlay.provenance.as_dict(),
            "anchor": iso_millis(anchor),
            "shift_s": shift.total_seconds(),
            "max_schedule_slip_s": published.max_schedule_slip_s,
        },
        "lateness_model": plan.lateness.as_dict(),
        "lateness_model_digest": plan.lateness.digest,
        "toolchain": {**toolchain(services.spark), "python": platform.python_version()},
        "gateway": dict(services.gateway),
        "counts": {
            "posts": {} if evaluation is None else evaluation.deliveries,
            "kafka_expected": expected,
            "kafka_arrived": arrived,
            "history": {} if evaluation is None else evaluation.history,
            "spark": evidence.as_dict(),
            "gold_build": build.summary(),
        },
        "results": results,
        "measured": {
            "as_served_divergent": None
            if evaluation is None
            else evaluation.as_served.divergent_total,
            "event_time_complete_divergent": (
                None if evaluation is None else evaluation.complete.divergent_total
            ),
            "arrival_skew_fraction": None if evaluation is None else evaluation.skew.fraction(),
        },
        "guard": {"violations": [v.as_dict() for v in violations]},
        "verdict": verdict,
    }
    path = write_record(record, output_dir)
    return record, path, exit_code(evaluation, violations, verdict)


def gateway_image_digest(container: str) -> str | None:
    completed = subprocess.run(  # noqa: S603 -- fixed argv; the container name is an operator flag
        ["docker", "inspect", "--format", "{{.Image}}", container],  # noqa: S607
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    digest = completed.stdout.strip()
    return digest if completed.returncode == 0 and digest.startswith("sha256:") else None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m eval.parity.run")
    parser.add_argument("--partition", choices=sorted(PARTITIONS), required=True)
    parser.add_argument("--mode", choices=[m.value for m in Mode], required=True)
    parser.add_argument("--dataset-dir", type=Path, required=True)
    parser.add_argument("--gateway-url", default="http://localhost:8010")
    parser.add_argument("--gateway-container", default="")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--lake", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--driver-memory", default="4g")
    args = parser.parse_args(argv)

    import httpx
    import psycopg
    import redis
    from eval.replay.gateway_replay import _service_token
    from services.gateway.config import GatewaySettings

    from trace_core.observability import configure_logging
    from trace_core.stream.session import build_session

    configure_logging()
    mode = Mode(args.mode)
    partition = PARTITIONS[args.partition]
    if mode is Mode.MEASURED:
        if args.output_dir is not None and args.output_dir.resolve() != MANIFEST_DIR.resolve():
            parser.error("a measured run writes its record to eval/manifest/ only")
        output_dir = MANIFEST_DIR
    elif args.output_dir is None:
        parser.error("a diagnostic run needs --output-dir; its records never go to eval/manifest/")
    else:
        output_dir = args.output_dir

    dataset = verify_dataset(args.dataset_dir, ROOT / partition.dataset_manifest, partition)
    warmup, slice_events = load_partition(args.dataset_dir, partition)
    plan = RunPlan(
        name=partition.name,
        gated=partition.gated,
        base_ref=f"{partition.dataset_version}:{partition.slice_start}/{partition.slice_end}",
        warmup=warmup,
        slice_events=slice_events,
        lateness=partition.lateness,
        partition={**partition.as_dict(), "digest": partition.digest},
        dataset=dataset,
    )
    settings = GatewaySettings.from_environment()

    def open_holes() -> int:
        with psycopg.connect(settings.postgres_dsn) as conn:
            row = conn.execute(
                "SELECT count(*) FROM app.feature_store_holes WHERE cleared_at IS NULL"
            ).fetchone()
        return int(row[0]) if row is not None else 0

    container = args.gateway_container or None
    spark = build_session("trace-x-parity", driver_memory=args.driver_memory)
    try:
        with httpx.Client(base_url=args.gateway_url, timeout=30.0) as client:
            services = Services(
                client=client,
                token=_service_token(),
                feature_redis=redis.Redis.from_url(settings.redis_url),
                open_holes=open_holes,
                bootstrap=args.bootstrap,
                spark=spark,
                lake=LakeConfig.at(args.lake),
                gateway={
                    "target": args.gateway_url,
                    "container": container,
                    "image_digest": None if container is None else gateway_image_digest(container),
                },
            )
            record, path, code = execute(plan, services, mode=mode, output_dir=output_dir)
    finally:
        spark.stop()
    sys.stdout.write(
        json.dumps({"run_id": record["run_id"], "record": str(path), "verdict": record["verdict"]})
        + "\n"
    )
    return code


# Kept importable for the tallies the record summarises.
__all__ = [
    "Mode",
    "Pairing",
    "ParityTally",
    "RunPlan",
    "Services",
    "SkewTally",
    "SparkEvidence",
    "execute",
    "main",
]

if __name__ == "__main__":
    sys.exit(main())
