"""Parity end to end, diagnostic: a small partition through a real gateway, Kafka, Bronze, Silver
and Gold on Spark and Delta (ADR-0056 §6).

Everything the run touches is its own:
- a throwaway broker, started as `tests.integration.test_kafka_platform` starts one;
- a throwaway PostgreSQL migrated by the real Alembic migrations;
- a throwaway Redis, feature store on database 0 and cache on database 1.

The gateway is `services.gateway.app.build_state`'s production composition over them: fenced writer,
observation log, completeness guard and in-process outbox relay. It is driven through its ASGI app.
Nothing is mocked, and the shared local stack is never touched.

The partition is synthetic, because the frozen eval-v2 dataset is not available in CI:
- four accounts sharing merchants, devices and IPs, with identity events and one outcome per
  transaction;
- one account with more than `SCORE_READ_CAP` transactions in its warm-up, so ADR-0046 §8 caps its
  reads on both implementations and arrival skew must exclude them;
- the overlay's late band kept below the online store's shortest non-account retention margin
  (ADR-0056 §7).

Every declared Kafka topic is created, because the gateway's triage shares the outbox with the
observation log: an undeliverable `investigation.requested.v1` row blocks every outcome behind it.

The run is DIAGNOSTIC and its record is NOT publishable. Set `PARITY_IT_RECORDS` to keep the record.
"""

from __future__ import annotations

import datetime as dt
import os
import random
import secrets
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from eval.parity.driver import iso_millis
from eval.parity.partition import LatenessModel
from eval.parity.run import Mode, RunPlan, Services, execute
from eval.replay.faults import FaultClass, Timing
from tests.integration.test_kafka_platform import (  # noqa: F401 -- `broker` is a fixture
    DECLARATION,
    Broker,
    _create,
    broker,
)

from trace_core.contracts import authorization
from trace_core.contracts.envelope import build_event
from trace_core.domain.time import event_time, processing_time, to_millis
from trace_core.stream.lake import LakeConfig

pytestmark = [pytest.mark.integration, pytest.mark.stream, pytest.mark.parity]

ROOT = Path(__file__).resolve().parents[2]
DB, OWNER = "tracex", "tracex_owner"
# Throwaway credentials for the disposable container this module starts and removes again:
# generated per run, never written to disk, and discarded with the container. None of them is a
# stored credential, which is why none of them is a literal -- a literal here is indistinguishable
# to a scanner (and to a reader) from a real one that leaked (CLAUDE.md section 9).
OWNER_PW = secrets.token_hex(16)
ROLE_PASSWORDS = {
    f"TRACE_{role}_DB_PASSWORD": secrets.token_hex(16)
    for role in ("APP", "STREAM", "EVAL", "AUDITOR", "GENERATOR")
}
TOKEN_ID, TOKEN_SECRET = "parity", secrets.token_hex(16)
START = dt.datetime(2026, 3, 2, 12, 0, tzinfo=dt.UTC)
ACCOUNTS = ("acct_000001", "acct_000002", "acct_000003", "acct_000004")
DEEP = "acct_000009"
REQUESTED = {
    FaultClass.REORDERED: 2,
    FaultClass.EXACT_DUPLICATE: 3,
    FaultClass.RETRY_NEW_EVENT_ID: 2,
    FaultClass.LATE: 2,
    FaultClass.FUTURE_WITHIN_24H: 1,
    FaultClass.FUTURE_BEYOND_24H: 1,
}


def _docker(*args: str, timeout: float = 180) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 -- fixed arguments
        ["docker", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _port(name: str, port: int) -> int:
    lines = _docker("port", name, f"{port}/tcp").stdout.strip().splitlines()
    assert lines, f"{name} published no port for {port}"
    return int(lines[0].rsplit(":", 1)[1])


@pytest.fixture(scope="module")
def postgres() -> Iterator[dict[str, str]]:
    name = f"tracex-parity-pg-{uuid.uuid4().hex[:8]}"
    started = _docker(
        "run", "-d", "--rm", "--name", name,
        "-e", f"POSTGRES_DB={DB}", "-e", f"POSTGRES_USER={OWNER}",
        "-e", f"POSTGRES_PASSWORD={OWNER_PW}",
        "-e", "POSTGRES_INITDB_ARGS=--locale=C --encoding=UTF8",
        "-p", "127.0.0.1::5432", "postgres:16",
    )  # fmt: skip
    if started.returncode != 0:
        pytest.fail(f"could not start PostgreSQL: {started.stderr}")
    try:
        for _ in range(90):
            if _docker("exec", name, "pg_isready", "-U", OWNER, "-d", DB).returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail(f"PostgreSQL never became ready: {_docker('logs', name).stderr[-2000:]}")
        time.sleep(2)
        env = {
            "POSTGRES_SUPERUSER": OWNER,
            "POSTGRES_SUPERUSER_PASSWORD": OWNER_PW,
            "POSTGRES_HOST": "127.0.0.1",
            "POSTGRES_PORT": str(_port(name, 5432)),
            "POSTGRES_DB": DB,
            "TRACE_APP_DB_USER": "trace_app",
            **ROLE_PASSWORDS,
        }
        migrate_env = {**os.environ, **env}
        migrate_env.pop("TRACE_DATABASE_URL", None)
        migrated = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=ROOT,
            env=migrate_env,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )
        assert migrated.returncode == 0, f"alembic failed:\n{migrated.stdout}\n{migrated.stderr}"
        yield env
    finally:
        _docker("rm", "-f", "-v", name)


@pytest.fixture(scope="module")
def redis_port() -> Iterator[int]:
    import redis

    name = f"tracex-parity-redis-{uuid.uuid4().hex[:8]}"
    started = _docker(
        "run", "-d", "--rm", "--name", name, "-p", "127.0.0.1::6379", "redis:7-alpine"
    )
    if started.returncode != 0:
        pytest.fail(f"could not start Redis: {started.stderr}")
    try:
        port = _port(name, 6379)
        for _ in range(60):
            try:
                if redis.Redis(host="127.0.0.1", port=port).ping():
                    break
            except redis.RedisError:
                time.sleep(0.5)
        else:
            pytest.fail("Redis never answered")
        yield port
    finally:
        _docker("rm", "-f", "-v", name)


@pytest.fixture(scope="module")
def spark() -> Iterator[Any]:
    from trace_core.stream.session import build_session

    session = build_session("trace-x-parity-it", driver_memory="2g")
    try:
        yield session
    finally:
        session.stop()


def _transaction(
    index: int, at: dt.datetime, account: str, *, device: str, merchant: str, ip: str, amount: int
) -> dict[str, Any]:
    payload = {
        "transaction_id": f"tx_parity_{index:05d}",
        "account_id": account,
        "card_id": "card_" + account[-6:],
        "device_id": device,
        "merchant_id": merchant,
        "ip_id": ip,
        "amount_minor": amount,
        "currency": "GBP",
        "channel": "CARD_NOT_PRESENT",
        "entry_mode": "ECOMMERCE",
        "merchant_mcc": "5411",
        "merchant_country": "GB",
        "latitude": 51.5,
        "longitude": -0.12,
        "authorization_outcome": "UNKNOWN",
    }
    return build_event(
        event_type="tx.raw",
        occurred_at=event_time(at),
        payload=payload,
        producer="trace-generator@1.0.0",
        trace_id=uuid.uuid4().hex,
        correlation_id=f"corr_{uuid.uuid4().hex}",
        ingested_at=processing_time(at + dt.timedelta(milliseconds=50)),
    )


def _outcome(event: dict[str, Any], outcome: str) -> dict[str, Any]:
    occurred_ms = to_millis(
        dt.datetime.fromisoformat(event["envelope"]["occurred_at"].replace("Z", "+00:00"))
    )
    return authorization.build_event(
        transaction_id=event["payload"]["transaction_id"],
        account_id=event["payload"]["account_id"],
        authorization_outcome=outcome,
        decided_ms=occurred_ms + 300,
        transaction_occurred_ms=occurred_ms,
        transaction_occurred_at=authorization.iso_millis(occurred_ms),
        producer="trace-generator@1.0.0",
        trace_id=uuid.uuid4().hex,
        correlation_id=event["envelope"]["correlation_id"],
        ingested_ms=occurred_ms + 320,
    )


def _identity(at: dt.datetime, account: str, kind: str, ip: str) -> dict[str, Any]:
    return build_event(
        event_type="identity.events",
        occurred_at=event_time(at),
        payload={"account_id": account, "identity_event_type": kind, "ip_id": ip},
        producer="trace-generator@1.0.0",
        trace_id=uuid.uuid4().hex,
        correlation_id=f"corr_{uuid.uuid4().hex}",
        ingested_at=processing_time(at + dt.timedelta(milliseconds=40)),
    )


def _ordered(events: list[tuple[str, dict[str, Any]]]) -> list[tuple[str, dict[str, Any]]]:
    rank = {"identity.events.v1": 1, "tx.raw.v1": 2, "tx.authorization.v1": 3}
    return sorted(events, key=lambda e: (e[1]["envelope"]["occurred_at"], rank[e[0]]))


def synthetic_partition() -> tuple[
    list[tuple[str, dict[str, Any]]], list[tuple[str, dict[str, Any]]]
]:
    rng = random.Random(20260915)
    warmup: list[tuple[str, dict[str, Any]]] = []
    for k in range(520):
        at = START - dt.timedelta(seconds=600) + dt.timedelta(milliseconds=1_000 * k)
        warmup.append(
            (
                "tx.raw.v1",
                _transaction(
                    k,
                    at,
                    DEEP,
                    device="dev_000050",
                    merchant=f"mrch_{50 + k % 2:05d}",
                    ip="ip_00050",
                    amount=700 + k % 13,
                ),
            )
        )
    for k in range(20):
        at = START - dt.timedelta(seconds=590) + dt.timedelta(seconds=25 * k)
        account = ACCOUNTS[k % len(ACCOUNTS)]
        event = _transaction(
            1_000 + k,
            at,
            account,
            device=f"dev_00000{k % len(ACCOUNTS) + 1}",
            merchant=f"mrch_{1 + k % 2:05d}",
            ip=f"ip_{1 + k % 3:05d}",
            amount=rng.choice([500, 1_200, 4_000]),
        )
        warmup += [("tx.raw.v1", event), ("tx.authorization.v1", _outcome(event, "APPROVED"))]
    for k in range(4):
        warmup.append(
            (
                "identity.events.v1",
                _identity(
                    START - dt.timedelta(seconds=300 - 60 * k),
                    ACCOUNTS[k],
                    "LOGIN_FAILED",
                    "ip_00002",
                ),
            )
        )

    in_slice: list[tuple[str, dict[str, Any]]] = []
    for k in range(60):
        at = START + dt.timedelta(milliseconds=1_300 * k)
        deep = k % 10 == 0
        account = DEEP if deep else rng.choice(ACCOUNTS)
        event = _transaction(
            2_000 + k,
            at,
            account,
            device="dev_000050" if deep else rng.choice(["dev_000001", "dev_000002", "dev_000099"]),
            merchant="mrch_00050" if deep else rng.choice(["mrch_00001", "mrch_00002"]),
            ip="ip_00050" if deep else rng.choice(["ip_00001", "ip_00002", "ip_00003"]),
            amount=rng.choice([500, 900, 1_200, 25_000]),
        )
        in_slice += [
            ("tx.raw.v1", event),
            (
                "tx.authorization.v1",
                _outcome(event, rng.choice(["APPROVED", "APPROVED", "DECLINED"])),
            ),
        ]
    kinds = ("LOGIN_FAILED", "LOGIN_FAILED", "PASSWORD_CHANGE", "LOGIN_SUCCEEDED")
    for k in range(16):
        at = START + dt.timedelta(milliseconds=5_000 * k + 200)
        in_slice.append(
            (
                "identity.events.v1",
                _identity(
                    at, rng.choice(ACCOUNTS), kinds[k % 4], rng.choice(["ip_00001", "ip_00003"])
                ),
            )
        )
    return _ordered(warmup), _ordered(in_slice)


def _lateness(base_events: int) -> LatenessModel:
    rates = {fault: round(count * 10_000 / base_events) for fault, count in REQUESTED.items()}
    model = LatenessModel(
        name="integration-synthetic",
        seed=7,
        timing=Timing.PACED,
        basis_points=rates,
        late_arrival_delay_s=(1_000, 3_000),
    )
    assert model.counts(base_events) == REQUESTED
    return model


def test_a_diagnostic_parity_run_compares_every_scored_transaction_without_divergence(
    broker: Broker,  # noqa: F811
    postgres: dict[str, str],
    redis_port: int,
    spark: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg
    import redis
    from fastapi.testclient import TestClient
    from services.gateway.app import build_state, create_app
    from services.gateway.config import GatewaySettings

    # Every declared topic, not only the three the medallion reads: the gateway's triage writes
    # `investigation.requested.v1` rows to the same outbox, and the relay claims oldest-first and
    # stops its batch at the first hand-over failure. One undeliverable row therefore defers every
    # outcome behind it, on every pass, with nothing logged (ADR-0056 §8, F2).
    for topic, declared in DECLARATION.topics.items():
        _create(broker, topic, declared.partitions, dict(declared.applied_config))
    monkeypatch.setenv(f"TRACE_SERVICE_TOKEN_{TOKEN_ID.upper()}", TOKEN_SECRET)
    settings = GatewaySettings.from_environment(
        {
            **postgres,
            "REDIS_HOST": "127.0.0.1",
            "REDIS_PORT": str(redis_port),
            "REDIS_DB": "0",
            "REDIS_CACHE_HOST": "127.0.0.1",
            "REDIS_CACHE_PORT": str(redis_port),
            "REDIS_CACHE_DB": "1",
            "TRACE_RATE_LIMIT_PER_MINUTE": "1000000",
            "TRACE_GATEWAY_KAFKA_BOOTSTRAP": broker.bootstrap,
            "TRACE_GATEWAY_OUTBOX_RELAY": "true",
            "TRACE_LOG_FORMAT": "console",
        }
    )
    state = build_state(settings)
    gateway = TestClient(create_app(state))
    client = gateway.__enter__()
    open_gateway = [True]

    def stop_gateway() -> None:
        if open_gateway[0]:
            gateway.__exit__(None, None, None)
            open_gateway[0] = False

    try:
        deadline = time.monotonic() + 60
        while client.get("/readyz").status_code != 200:
            assert time.monotonic() < deadline, client.get("/readyz").json()
            time.sleep(0.5)

        def open_holes() -> int:
            with psycopg.connect(settings.postgres_dsn) as conn:
                row = conn.execute(
                    "SELECT count(*) FROM app.feature_store_holes WHERE cleared_at IS NULL"
                ).fetchone()
            return 0 if row is None else int(row[0])

        warmup, in_slice = synthetic_partition()
        plan = RunPlan(
            name="integration-synthetic",
            gated=True,
            base_ref="synthetic:tests/integration/test_parity_run.py",
            warmup=warmup,
            slice_events=in_slice,
            lateness=_lateness(len(in_slice)),
            partition={"name": "integration-synthetic", "gated": True, "digest": None},
            dataset={"base_ref": "synthetic", "eval_v2_manifest_digest": None},
        )
        services = Services(
            client=client,
            token=f"{TOKEN_ID}.{TOKEN_SECRET}",
            feature_redis=redis.Redis(host="127.0.0.1", port=redis_port, db=0),
            open_holes=open_holes,
            bootstrap=broker.bootstrap,
            spark=spark,
            lake=LakeConfig.at(tmp_path / "lake"),
            gateway={"target": "in-process build_state", "image_digest": None},
            stop_gateway=stop_gateway,
        )
        output = Path(os.environ.get("PARITY_IT_RECORDS") or tmp_path / "records")
        try:
            record, path, code = execute(plan, services, mode=Mode.DIAGNOSTIC, output_dir=output)
        except TimeoutError as exc:
            # A stalled relay or log leaves the outbox and the relay's own state as the evidence;
            # a bare timeout says only that Kafka was short (ADR-0056 §7).
            with psycopg.connect(settings.postgres_dsn) as conn:
                grouped = conn.execute(
                    "SELECT published_at IS NULL AS pending, "
                    "left(coalesce(last_error, '<none>'), 120) AS error, count(*), max(attempts) "
                    "FROM app.outbox GROUP BY 1, 2 ORDER BY 3 DESC"
                ).fetchall()
            relay = state.outbox_relay
            pytest.fail(
                f"{exc}\noutbox: {grouped}\n"
                f"relay running: {None if relay is None else relay.running}\n"
                f"readyz: {client.get('/readyz').json()}",
                pytrace=False,
            )
    finally:
        stop_gateway()

    sys.stdout.write(f"\nPARITY record (diagnostic, NOT publishable): {path}\n")
    results = record["results"]
    assert "error" not in results, results
    served, complete, skew = (
        results["as_served"],
        results["event_time_complete"],
        results["arrival_skew"],
    )
    assert served["divergent"] == 0, served["divergences"][:10]
    assert complete["divergent"] == 0, complete["divergences"][:10]
    assert served["compared"] > 0 and complete["compared"] > 0
    assert skew["denominator"] > 0 and skew["capped_excluded"] > 0, skew
    vacuous = {"NOTHING_COMPARED", "ALL_SKIPPED", "SPARK_NOT_EXECUTED", "NO_ARRIVAL_SKEW"}
    assert not vacuous & {v["kind"] for v in record["guard"]["violations"]}
    counts = record["counts"]
    assert counts["spark"]["executed"], counts["spark"]
    for topic in ("tx.scored.v1", "identity.events.v1"):
        assert counts["kafka_arrived"][topic] == counts["kafka_expected"][topic]
    provenance = record["overlay"]["provenance"]
    assert provenance["injected"] == provenance["requested"]
    assert counts["posts"]["slice:tx.raw.v1:SCORED"] > 0
    assert record["verdict"] == "DIAGNOSTIC" and record["publishable"] is False
    assert code == 0
    assert iso_millis(START)  # the partition's anchor is rendered at millisecond precision
