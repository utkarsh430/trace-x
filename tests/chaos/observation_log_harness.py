"""A gateway's observation path in a child process, killed for real at a named point (chaos only).

`tests/chaos/test_observation_log.py` runs this as `python -m tests.chaos.observation_log_harness`.
It composes the production pieces the gateway composes, with nothing faked:
- the fenced writer supervisor on PostgreSQL;
- the scoring pipeline over the real Redis feature store;
- the observation log over the one producer factory, against a real broker.

At `--kill-at` it sends itself SIGKILL. The lock then ends with the backend, the sockets drop and
librdkafka's buffer is lost, exactly as when a gateway dies. No production code carries a hook for
this; the kill points sit between the calls the gateway makes.

`--shed` produces real shedding. The broker is paused and the producer's queue bounded to two
messages, so a third observation is shed; the broker is then resumed, and a fourth is delivered
after it.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from typing import Any

KILL_POINTS = (
    "none",
    "before_session",
    "after_session",
    "after_sequence",
    "after_online_write",
    "after_produce",
    "after_ack",
    "before_close",
)


def _parse(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kill-at", choices=KILL_POINTS, default="none")
    parser.add_argument("--kill-on", type=int, default=1)
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", type=int, default=6389)
    parser.add_argument("--redis-db", type=int, default=15)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--lock-key", type=int, required=True)
    parser.add_argument("--instance-id", required=True)
    parser.add_argument("--interval-s", type=float, default=0.5)
    parser.add_argument("--shed-container", default="")
    return parser.parse_args(argv)


def _die_if(args: argparse.Namespace, point: str, index: int | None = None) -> None:
    if args.kill_at == point and (index is None or index == args.kill_on):
        print(json.dumps({"killed_at": point, "index": index}), flush=True)
        os.kill(os.getpid(), signal.SIGKILL)


def _docker(*argv: str) -> None:
    subprocess.run(["docker", *argv], check=True, capture_output=True, timeout=60)  # noqa: S603


def main(argv: list[str]) -> int:
    args = _parse(argv)
    import redis
    from confluent_kafka import Producer
    from services.gateway.pipeline import ScoringPipeline

    from trace_core.contracts.api.transaction import TransactionRequest
    from trace_core.contracts.publish import DeliveryLedger, EventPublisher, producer_config
    from trace_core.contracts.topics import TX_SCORED_V1
    from trace_core.features.definitions import ONLINE_FEATURES
    from trace_core.observation.log import DELIVERY_TIMEOUT_MS, ObservationLog
    from trace_core.observation.scored_event import build_scored_event
    from trace_core.observation.supervisor import WriterSupervisor
    from trace_core.repositories.postgres_sessions import writer_connection
    from trace_core.repositories.redis_features import RedisOnlineFeatureStore
    from trace_core.rules.loader import default_loader
    from trace_core.scoring.banding import load_thresholds

    _die_if(args, "before_session")
    writer = WriterSupervisor(
        connect=lambda: writer_connection(args.dsn),
        producer="trace-gateway@0.1.0",
        instance_id=args.instance_id,
        interval_s=args.interval_s,
        lease_s=3 * args.interval_s,
        takeover_grace_s=3 * args.interval_s + 1.0,
        lock_key=args.lock_key,
    )
    writer.start()
    deadline = time.monotonic() + 30.0
    while not writer.ready:
        if time.monotonic() > deadline:
            print(json.dumps({"error": f"never became the writer: {writer.status}"}), flush=True)
            return 2
        time.sleep(0.05)
    _die_if(args, "after_session")

    store = RedisOnlineFeatureStore(
        redis.Redis(host=args.redis_host, port=args.redis_port, db=args.redis_db),
        namespace=args.namespace,
    )
    pipeline = ScoringPipeline(
        pack=default_loader(frozenset(ONLINE_FEATURES.ids)).load(),
        thresholds=load_thresholds(),
        feature_store=store,
    )
    ledger = DeliveryLedger()
    config: dict[str, Any] = producer_config(
        bootstrap_servers=args.bootstrap,
        client_id=args.instance_id,
        ledger=ledger,
        message_timeout_ms=DELIVERY_TIMEOUT_MS,
    )
    if args.shed_container:
        config["queue.buffering.max.messages"] = 2
    publisher = EventPublisher(Producer(config), ledger, bootstrap_servers=args.bootstrap)
    observation_log = ObservationLog(
        writer=writer, publisher=publisher, topics=(TX_SCORED_V1,), check_timeout_s=10.0
    )
    observation_log.start()
    if observation_log.status != "ok":
        print(json.dumps({"error": observation_log.status}), flush=True)
        return 2

    outcomes: list[str] = []
    paused = False
    if args.shed_container:
        _docker("pause", args.shed_container)
        paused = True
    try:
        for index in range(1, args.count + 1):
            if paused and index == 4:
                _docker("unpause", args.shed_container)
                paused = False
                publisher.flush(30.0)
            sequenced = observation_log.sequence()
            _die_if(args, "after_sequence", index)
            now = dt.datetime.now(dt.UTC)
            request = TransactionRequest.model_validate(
                {
                    "transaction_id": f"tx_{uuid.uuid4().hex[:16]}",
                    "account_id": f"acct_{index % 3:06d}",
                    "amount_minor": 1_000 + index,
                    "currency": "GBP",
                    "occurred_at": now.isoformat().replace("+00:00", "Z"),
                }
            )
            outcome = pipeline.score(request, now=now)
            _die_if(args, "after_online_write", index)
            event = build_scored_event(
                canonical=outcome.canonical,
                decision=outcome.decision,
                features=outcome.features,
                context=outcome.context,
                observe_outcome=outcome.observe_outcome.value,
                store_position=outcome.observe_position,
                store_epoch_ms=outcome.store_epoch_ms,
                producer="trace-gateway@0.1.0",
                trace_id="0" * 32,
            )
            outcomes.append(observation_log.publish(sequenced, TX_SCORED_V1, event).value)
            _die_if(args, "after_produce", index)
            if args.kill_at == "after_ack" and index == args.kill_on:
                report = publisher.flush(30.0)
                print(json.dumps({"flushed_outstanding": report.outstanding}), flush=True)
                _die_if(args, "after_ack", index)
    finally:
        if paused:
            _docker("unpause", args.shed_container)
    _die_if(args, "before_close")
    confirmed = observation_log.close(timeout_s=30.0)
    session = writer.session
    closed = writer.stop(confirmed=confirmed)
    print(
        json.dumps(
            {
                "session_id": None if session is None else session.session_id,
                "last_seq": None if session is None else session.last_seq,
                "confirmed": confirmed,
                "closed": closed,
                "outcomes": outcomes,
            }
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
