"""Paced producer workers on the project's own `EventPublisher` (validated, keyed, idempotent).

One worker process per share of the rate, because building and validating a scored event costs a
Python process a fraction of a millisecond and 5,000 of them a second do not fit one interpreter.

**Pacing is a fixed schedule, never adaptive.** Worker `w` of `W` owes event `k` at
`t0 + (k * W + w) / rate`. It publishes each event as soon as it is due, and when it falls behind it
catches up rather than skipping: the offered rate is never lowered. How far behind schedule a worker
ever was is reported, and the run is INVALID if that exceeded `MAX_SCHEDULE_LAG_S`.

**Delivery reports are evidence.** Each record carries its send time (`trace-bench-sent-us`
header); the delivery callback bounds the broker-to-host clock offset from it (`clock`), confirms
the broker stamped LogAppendTime, and tracks per partition how many records were delivered and the
highest offset. `EventPublisher.close` vouches for everything accepted, or the worker reports its
problems.

Messages on the result queue, as tuples:
    ("ready", worker)
    ("stats", worker, bucket_start_s, {topic: produced}, max_behind_s)
    ("final", worker, WorkerFinal-as-dict)
    ("error", worker, message)
"""

from __future__ import annotations

import json
import time
import traceback
from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any, Final

from benchmarks.stream_throughput.clock import OffsetBounds
from benchmarks.stream_throughput.events import EventFactory, mix_pattern, score_templates
from benchmarks.stream_throughput.spec import RunConfig

from trace_core.contracts.publish import DeliveryLedger, EventPublisher, build_producer

SENT_HEADER: Final = "trace-bench-sent-us"
LOG_APPEND_TIME_TYPE: Final = 2
"""confluent-kafka's `TIMESTAMP_LOG_APPEND_TIME`."""
STATS_EVERY_S: Final = 1.0
CLOSE_TIMEOUT_S: Final = 180.0


class TimedLedger(DeliveryLedger):
    """The contract ledger, plus what each delivery report says about clocks and partitions."""

    def __init__(self) -> None:
        super().__init__()
        self.bounds = OffsetBounds()
        self.not_log_append_time = 0
        self.unstamped = 0
        self.delivered_by_partition: Counter[str] = Counter()
        self.max_offset: dict[str, int] = {}
        self.max_log_append_ms: dict[str, int] = {}
        self.max_delivery_ms = 0.0

    def on_delivery(self, err: Any, msg: Any) -> None:
        super().on_delivery(err, msg)
        if err is not None:
            return
        acked_ms = time.time_ns() / 1_000_000
        kind, stamp = msg.timestamp()
        if kind != LOG_APPEND_TIME_TYPE:
            self.not_log_append_time += 1
            return
        key = f"{msg.topic()}[{msg.partition()}]"
        self.delivered_by_partition[key] += 1
        self.max_offset[key] = max(self.max_offset.get(key, -1), int(msg.offset()))
        self.max_log_append_ms[key] = max(self.max_log_append_ms.get(key, 0), int(stamp))
        sent = dict(msg.headers() or ()).get(SENT_HEADER)
        if sent is None:
            self.unstamped += 1
            return
        sent_ms = int(sent) / 1000
        self.max_delivery_ms = max(self.max_delivery_ms, acked_ms - sent_ms)
        self.bounds = self.bounds.observe(
            sent_ms=sent_ms, acked_ms=acked_ms, log_append_ms=float(stamp)
        )


@dataclass
class WorkerFinal:
    worker: int
    produced: dict[str, int] = field(default_factory=dict)
    substituted: int = 0
    max_behind_s: float = 0.0
    delivery_problems: list[str] = field(default_factory=list)
    delivered: dict[str, int] = field(default_factory=dict)
    failed: dict[str, int] = field(default_factory=dict)
    not_log_append_time: int = 0
    unstamped: int = 0
    delivered_by_partition: dict[str, int] = field(default_factory=dict)
    max_offset: dict[str, int] = field(default_factory=dict)
    max_log_append_ms: dict[str, int] = field(default_factory=dict)
    max_delivery_ms: float = 0.0
    clock: dict[str, Any] = field(default_factory=dict)


def worker_main(
    worker: int,
    config_json: str,
    bootstrap: str,
    run_nonce: str,
    start_at: Any,
    stop: Any,
    out: Any,
) -> None:
    """The worker process. `start_at` is a shared double (0 until the harness sets t0), `stop` an
    event, `out` the result queue."""
    try:
        _run_worker(worker, config_json, bootstrap, run_nonce, start_at, stop, out)
    except Exception as exc:  # reported to the harness, which fails the run on it
        out.put(("error", worker, f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"))


def _run_worker(
    worker: int,
    config_json: str,
    bootstrap: str,
    run_nonce: str,
    start_at: Any,
    stop: Any,
    out: Any,
) -> None:
    from trace_core.observability import configure_logging

    configure_logging()
    config = RunConfig.model_validate_json(config_json)
    factory = EventFactory(
        templates=score_templates(config.scored_templates, config.seed),
        seed=config.seed,
        worker=worker,
        run_nonce=run_nonce,
        account_pool=config.account_pool,
    )
    pattern = mix_pattern(config.mix, config.seed)
    ledger = TimedLedger()
    producer = build_producer(
        bootstrap_servers=bootstrap, client_id=f"trace-load-stream-{worker}", ledger=ledger
    )
    publisher = EventPublisher(producer, ledger, bootstrap_servers=bootstrap)
    unconfirmed = publisher.check_topics(sorted(config.mix), timeout_s=10.0)
    if unconfirmed:
        out.put(("error", worker, f"the broker does not confirm topics {unconfirmed}"))
        return
    out.put(("ready", worker))
    while start_at.value <= 0 and not stop.is_set():
        time.sleep(0.01)
    t0 = float(start_at.value)
    period = config.producer_workers / config.rate_events_per_s
    offset = worker / config.rate_events_per_s
    produced: Counter[str] = Counter()
    second: Counter[str] = Counter()
    max_behind = second_behind = 0.0
    k = 0
    next_stats = t0 + STATS_EVERY_S
    while not stop.is_set():
        now = time.time()
        while now >= next_stats:
            # Each bucket is [next_stats - 1 s, next_stats): flushed before an event past it.
            out.put(("stats", worker, next_stats - STATS_EVERY_S, dict(second), second_behind))
            second.clear()
            second_behind = 0.0
            next_stats += STATS_EVERY_S
        due = t0 + k * period + offset
        if due > now:
            time.sleep(min(due - now, 0.005))
            continue
        behind = now - due
        second_behind = max(second_behind, behind)
        max_behind = max(max_behind, behind)
        topic, value = factory.build(pattern[k % len(pattern)], int(now * 1000))
        publisher.publish(
            topic, value, headers=[(SENT_HEADER, str(time.time_ns() // 1000).encode())]
        )
        produced[topic] += 1
        second[topic] += 1
        k += 1
    out.put(("stats", worker, next_stats - STATS_EVERY_S, dict(second), second_behind))
    final = WorkerFinal(worker, dict(produced), factory.substituted, max_behind)
    try:
        report = publisher.close(CLOSE_TIMEOUT_S)
        final.delivered, final.failed = dict(report.delivered), dict(report.failed)
    except Exception as exc:  # EventPublishError: recorded as the worker's delivery problems
        final.delivery_problems = [f"{type(exc).__name__}: {exc}"]
        partial = ledger.report(outstanding=0)
        final.delivered, final.failed = dict(partial.delivered), dict(partial.failed)
    final.not_log_append_time = ledger.not_log_append_time
    final.unstamped = ledger.unstamped
    final.delivered_by_partition = dict(ledger.delivered_by_partition)
    final.max_offset = dict(ledger.max_offset)
    final.max_log_append_ms = dict(ledger.max_log_append_ms)
    final.max_delivery_ms = ledger.max_delivery_ms
    final.clock = ledger.bounds.as_record()
    out.put(("final", worker, json.loads(json.dumps(asdict(final)))))
