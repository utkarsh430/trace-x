"""Reconstruction reaches completeness only through evidence, never across a gap.

The acceptance evidence for `P3.redis-hydration` (ADR-0057).

Nothing here is faked. A real gateway writer, on a real PostgreSQL, writes a real Redis store and
publishes to a real broker; Bronze and Silver ingest it on a real JVM; and the hydrator rebuilds a
second store from that history and claims what the evidence allows.

**Isolation.** Each module run gets its own migrated database inside the compose PostgreSQL. The
coverage rule reads every gateway session in one ledger, and the writer lock is per database, so a
shared one would mix the live compose gateway's open session into this evidence. The database is
dropped at the end; no compose service is reconfigured.
"""

from __future__ import annotations

import datetime as dt
import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

import pytest
from tests.integration.test_bronze_kafka import _fresh, _ingest, _publish
from tests.integration.test_kafka_platform import Broker, _create, broker  # noqa: F401

from trace_core.contracts import authorization
from trace_core.contracts.api.transaction import MAX_CLOCK_SKEW_FUTURE_S
from trace_core.contracts.envelope import build_event
from trace_core.contracts.publish import EventPublisher
from trace_core.contracts.topics import IDENTITY_EVENTS_V1, TX_AUTHORIZATION_V1, TX_SCORED_V1
from trace_core.domain.enums import AuthorizationOutcome
from trace_core.domain.time import EventTime, event_time, from_millis, to_millis
from trace_core.features.completeness import CompletenessGuard, HoleReason
from trace_core.features.context import FeatureContext
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.observation import Event, authorization_observation
from trace_core.features.semantics import Stream, identity_stream
from trace_core.observation.log import LogOutcome, ObservationLog
from trace_core.observation.scored_event import build_scored_event
from trace_core.observation.session import WRITER_LOCK_KEY
from trace_core.observation.supervisor import WriterSupervisor
from trace_core.repositories.postgres_authorization import (
    AuthorizationOutcomeRecord,
    PostgresAuthorizationStore,
)
from trace_core.repositories.postgres_completeness import PostgresHoleLedger
from trace_core.repositories.postgres_sessions import writer_connection
from trace_core.repositories.redis_features import RedisOnlineFeatureStore
from trace_core.stream.bronze import Trigger
from trace_core.stream.hydration import (
    HYDRATION_PRODUCER,
    HydrationRefusedError,
    Hydrator,
    read_evidence,
)
from trace_core.stream.lake import LakeConfig
from trace_core.stream.silver import create_silver_tables, start_silver_query

pytestmark = [pytest.mark.integration, pytest.mark.stream]

SHA: Final = "9" * 40
GATEWAY: Final = "trace-gateway@0.1.0"
IDENTITY_EVENT_TYPE: Final = "LOGIN_FAILED"
"""A released identity event type that feeds a stream, so the online store records it."""
CLOCK_MARGIN_S: Final = 1.0
ROOT: Final = Path(__file__).resolve().parents[2]
REDIS_HOST: Final = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT: Final = int(os.environ.get("REDIS_PORT", "6389"))
REDIS_DB: Final = int(os.environ.get("REDIS_TEST_DB", "15"))
BASE: Final = dt.datetime.now(dt.UTC).replace(microsecond=0) - dt.timedelta(hours=40)
"""The history starts 40 hours back, so the 25-hour fold horizon is crossed by event time."""


# ------------------------------------------------------------------ fixtures ---


def _dsn(user_env: str, password_env: str, database: str) -> str:
    return "postgresql://{u}:{p}@{h}:{port}/{db}".format(
        u=os.environ.get(user_env, ""),
        p=os.environ.get(password_env, ""),
        h=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5442"),
        db=database,
    )


@pytest.fixture(scope="module")
def database() -> Iterator[str]:
    """A migrated database of this run's own: the ledger it reads must hold only its sessions."""
    psycopg = pytest.importorskip("psycopg")
    name = f"tracex_hydration_{uuid.uuid4().hex[:8]}"
    admin = _dsn(
        "POSTGRES_SUPERUSER", "POSTGRES_SUPERUSER_PASSWORD", os.environ.get("POSTGRES_DB", "tracex")
    )
    try:
        with psycopg.connect(admin, autocommit=True) as owner:
            owner.execute(f'CREATE DATABASE "{name}"')
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no PostgreSQL superuser at {admin.split('@')[-1]} ({exc}). "
            f"Run `make up`; this suite migrates a database of its own and never mocks the ledger."
        )
    migrated = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=ROOT,
        env={
            **os.environ,
            # Alembic drives SQLAlchemy, whose plain `postgresql://` dialect is psycopg2.
            # `migrations/env.py` names psycopg 3 explicitly, and so must this override.
            "TRACE_DATABASE_URL": _dsn(
                "POSTGRES_SUPERUSER", "POSTGRES_SUPERUSER_PASSWORD", name
            ).replace("postgresql://", "postgresql+psycopg://", 1),
            "PYTHONPATH": f"{ROOT / 'packages'}:{ROOT}",
        },
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert migrated.returncode == 0, f"alembic upgrade failed: {migrated.stderr[-2000:]}"
    yield name
    with psycopg.connect(admin, autocommit=True) as owner:
        owner.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s", (name,)
        )
        owner.execute(f'DROP DATABASE IF EXISTS "{name}"')


@pytest.fixture(scope="module")
def redis_client() -> Iterator[Any]:
    redis = pytest.importorskip("redis", reason="the `db` extra provides the Redis client")
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=False)
    try:
        client.ping()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no Redis at {REDIS_HOST}:{REDIS_PORT} ({exc}). Run `make up` "
            f"-- reconstruction is never tested against a fake store (docs/TESTING.md §4)."
        )
    client.flushdb()
    yield client
    client.flushdb()


@pytest.fixture(scope="module")
def spark() -> Iterator[Any]:
    from trace_core.stream.session import build_session

    session = build_session("trace-x-hydration")
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture(scope="module")
def topics(broker: Broker) -> Broker:  # noqa: F811
    from tests.integration.test_kafka_platform import DECLARATION

    for name in (TX_SCORED_V1, IDENTITY_EVENTS_V1):
        declared = DECLARATION.topics[name]
        _create(broker, name, declared.partitions, dict(declared.applied_config))
    return broker


# -------------------------------------------------------------------- history ---


@dataclass
class Live:
    """One live gateway's history: what it wrote online, and what it published."""

    namespace: str
    lake: LakeConfig
    store: RedisOnlineFeatureStore
    events: list[Any] = field(default_factory=list)
    """The canonical transactions the gateway scored, in the order it scored them."""
    lost_at: dt.datetime | None = None
    """When an observation went unrecorded and unpublished, on this host's clock."""


def _pool(database: str) -> Any:
    import psycopg_pool

    return psycopg_pool.ConnectionPool(
        _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD", database),
        min_size=1,
        max_size=3,
        open=True,
    )


def _transaction_request(index: int, account: str, occurred: dt.datetime) -> Any:
    from trace_core.contracts.api.transaction import TransactionRequest

    return TransactionRequest.model_validate(
        {
            "transaction_id": f"tx_{index:012d}",
            "account_id": account,
            "amount_minor": 1_500 + 37 * index,
            "currency": "GBP",
            "occurred_at": occurred.isoformat().replace("+00:00", "Z"),
            "card_id": f"card_{index % 2:06d}",
            "device_id": f"dev_{index % 3:06d}",
            "merchant_id": f"mrch_{index % 4:05d}",
            "ip_id": f"ip_{index % 2:06d}",
            "merchant_mcc": "5411",
            "merchant_country": "GB",
            "latitude": 51.5 + index / 1000,
            "longitude": -0.12 - index / 1000,
            "channel": "CARD_PRESENT",
        }
    )


def _write_history(
    target: Broker,
    database: str,
    redis_client: Any,
    lake: LakeConfig,
    *,
    namespace: str,
    transactions: int = 14,
    lose_one: bool = False,
    first_index: int = 1,
) -> Live:
    """A gateway session that scores, observes and publishes, exactly as the gateway does."""
    stream = identity_stream(IDENTITY_EVENT_TYPE)
    assert stream is not None, IDENTITY_EVENT_TYPE
    from services.gateway.pipeline import ScoringPipeline

    from trace_core.rules.loader import default_loader
    from trace_core.scoring.banding import load_thresholds

    live = Live(
        namespace=namespace,
        lake=lake,
        store=RedisOnlineFeatureStore(redis_client, namespace=namespace),
    )
    dsn = _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD", database)
    writer = WriterSupervisor(
        connect=lambda: writer_connection(dsn),
        producer=GATEWAY,
        instance_id=f"live-{namespace}",
        lock_key=WRITER_LOCK_KEY,
    )
    writer.start()
    deadline = time.monotonic() + 30
    while not writer.ready and time.monotonic() < deadline:
        time.sleep(0.05)
    assert writer.ready, writer.status
    publisher = EventPublisher.connect(target.bootstrap, client_id=f"live-{uuid.uuid4().hex[:8]}")
    log = ObservationLog(
        writer=writer,
        publisher=publisher,
        topics=(TX_SCORED_V1, IDENTITY_EVENTS_V1),
        check_timeout_s=30.0,
    )
    log.start()
    assert log.status == "ok", log.status
    pool = _pool(database)
    pipeline = ScoringPipeline(
        pack=default_loader(frozenset(ONLINE_FEATURES.ids)).load(),
        thresholds=load_thresholds(),
        feature_store=live.store,
        writer=writer,
    )
    guard = CompletenessGuard(live.store, PostgresHoleLedger(pool), instance_id=f"live-{namespace}")
    try:
        for index in range(first_index, first_index + transactions):
            occurred = BASE + dt.timedelta(minutes=170 * (index - first_index))
            sequenced = log.sequence()
            request = _transaction_request(index, f"acct_{index % 3:06d}", occurred)
            outcome = pipeline.score(
                request, now=dt.datetime.now(dt.UTC), session_id=sequenced.session_id
            )
            live.events.append(outcome.canonical)
            event = build_scored_event(
                canonical=outcome.canonical,
                decision=outcome.decision,
                features=outcome.features,
                context=outcome.context,
                observe_outcome=outcome.observe_outcome.value,
                store_position=outcome.observe_position,
                store_epoch_ms=outcome.store_epoch_ms,
                producer=GATEWAY,
                trace_id=uuid.uuid4().hex,
            )
            assert log.publish(sequenced, TX_SCORED_V1, event) is LogOutcome.HANDED_OVER

            if index % 4 == 0:  # an identity event the gateway observes and publishes
                identity_id = f"idev_{uuid.uuid4().hex}"
                when = occurred + dt.timedelta(minutes=3)
                sequenced = log.sequence()
                live.store.observe(
                    Event(
                        stream=stream,
                        occurred_at=event_time(when),
                        account_id=request.account_id,
                        event_id=identity_id,
                        device_id=request.device_id,
                        ip_id=request.ip_id,
                    )
                )
                envelope = build_event(
                    event_type="identity.events",
                    occurred_at=event_time(when),
                    payload={
                        "account_id": request.account_id,
                        "identity_event_type": IDENTITY_EVENT_TYPE,
                        "device_id": request.device_id,
                        "ip_id": request.ip_id,
                    },
                    producer=GATEWAY,
                    trace_id=uuid.uuid4().hex,
                    correlation_id=identity_id,
                )
                published = log.publish(sequenced, IDENTITY_EVENTS_V1, envelope)
                assert published is LogOutcome.HANDED_OVER

            if index % 5 == 0:  # an authorization outcome: durable first, then applied online
                decided = occurred + dt.timedelta(minutes=1)
                envelope = authorization.build_event(
                    transaction_id=request.transaction_id,
                    account_id=request.account_id,
                    authorization_outcome="DECLINED",
                    decided_ms=to_millis(decided),
                    transaction_occurred_ms=to_millis(occurred),
                    transaction_occurred_at=authorization.iso_millis(to_millis(occurred)),
                    producer=GATEWAY,
                    trace_id=uuid.uuid4().hex,
                    correlation_id=request.transaction_id,
                    ingested_ms=to_millis(dt.datetime.now(dt.UTC)),
                )
                PostgresAuthorizationStore(pool).record(
                    AuthorizationOutcomeRecord(
                        transaction_id=request.transaction_id,
                        account_id=request.account_id,
                        authorization_outcome="DECLINED",
                        decided_at=decided,
                        transaction_occurred_at=occurred,
                        event_id=str(envelope["envelope"]["event_id"]),
                    ),
                    envelope,
                )
                live.store.observe(
                    authorization_observation(
                        transaction_id=request.transaction_id,
                        account_id=request.account_id,
                        authorization_outcome=AuthorizationOutcome.DECLINED,
                        decided_at=EventTime(decided),
                    )
                )

            if lose_one and index == first_index + transactions // 2:
                # The store refuses the write and the record is never produced: a hole in the
                # ledger AND a missing sequence number. Nothing records this observation anywhere.
                sequenced = log.sequence()
                live.lost_at = dt.datetime.now(dt.UTC)
                guard.observation_unrecorded(HoleReason.REFUSED)
                log.unpublished(sequenced, IDENTITY_EVENTS_V1)
    finally:
        confirmed = log.close(timeout_s=30.0)
        writer.stop(confirmed=confirmed and not lose_one)
        pool.close()
    return live


def _medallion(spark: Any, lake: LakeConfig, target: Broker, topics: tuple[str, ...]) -> None:
    for topic in topics:
        _ingest(spark, lake, target, topic)
    for topic in topics:
        handle = start_silver_query(
            spark,
            lake,
            topic,
            git_sha=SHA,
            dirty_worktree=False,
            now=dt.datetime.now(dt.UTC),
            trigger=Trigger(available_now=True),
        )
        handle.query.awaitTermination()
        assert handle.query.exception() is None, handle.query.exception()
    # Gold's projection reads all three canonical tables; the outcome table is Postgres's here.
    create_silver_tables(spark, lake, TX_AUTHORIZATION_V1, git_sha=SHA, dirty_worktree=False)


@dataclass
class Rebuilt:
    hydrator: Hydrator
    store: RedisOnlineFeatureStore
    writer: WriterSupervisor
    namespace: str


@contextmanager
def _hydrator(
    spark: Any,
    lake: LakeConfig,
    database: str,
    redis_client: Any,
    namespace: str,
    *,
    ready_timeout_s: float = 30.0,
    **options: Any,
) -> Iterator[Rebuilt]:
    import psycopg

    dsn = _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD", database)
    store = RedisOnlineFeatureStore(redis_client, namespace=namespace)
    writer = WriterSupervisor(
        connect=lambda: writer_connection(dsn),
        producer=HYDRATION_PRODUCER,
        instance_id=f"hydrate-{namespace}",
        lock_key=WRITER_LOCK_KEY,
    )
    writer.start()
    deadline = time.monotonic() + ready_timeout_s
    while not writer.ready and time.monotonic() < deadline:
        time.sleep(0.05)
    ledger = psycopg.connect(dsn, autocommit=True)
    pool = _pool(database)
    try:
        yield Rebuilt(
            hydrator=Hydrator(
                store=store,
                spark=spark,
                lake=lake,
                writer=writer,
                holes=PostgresHoleLedger(pool),
                ledger=ledger,
                clock_margin_s=CLOCK_MARGIN_S,
                **options,
            ),
            store=store,
            writer=writer,
            namespace=namespace,
        )
    finally:
        writer.stop(confirmed=True)
        pool.close()
        ledger.close()


def _probe_contexts(store: RedisOnlineFeatureStore, live: Live) -> dict[str, FeatureContext]:
    """Every entity the history touched, read after its newest observation."""
    newest = max(to_millis(event.occurred_at) for event in live.events)
    as_of = EventTime(from_millis(newest + 60_000))
    contexts: dict[str, FeatureContext] = {}
    for transaction in live.events:
        key = "|".join(
            str(value)
            for value in (
                transaction.account_id,
                transaction.card_id,
                transaction.device_id,
                transaction.merchant_id,
                transaction.ip_id,
            )
        )
        contexts[key] = store.snapshot(
            as_of=as_of,
            account_id=transaction.account_id,
            currency=transaction.currency,
            card_id=transaction.card_id,
            device_id=transaction.device_id,
            merchant_id=transaction.merchant_id,
            ip_id=transaction.ip_id,
        )
    return contexts


def _served(context: FeatureContext) -> tuple[Any, ...]:
    """What a read serves, completeness aside: the two stores date their epochs differently."""
    return (
        tuple(sorted((str(key), value) for key, value in context.windows.items())),
        tuple(sorted((str(key), value) for key, value in context.profiles.items())),
        tuple(sorted((str(key), value) for key, value in context.previous.items())),
    )


@pytest.fixture(scope="module")
def clean(
    topics: Broker, database: str, redis_client: Any, spark: Any, tmp_path_factory: Any
) -> Live:
    """One clean history: nothing lost, every session closed, Bronze and Silver caught up."""
    _fresh(topics, TX_SCORED_V1, IDENTITY_EVENTS_V1)
    lake = LakeConfig.at(tmp_path_factory.mktemp("clean-lake"))
    live = _write_history(topics, database, redis_client, lake, namespace="live-clean")
    _medallion(spark, lake, topics, (TX_SCORED_V1, IDENTITY_EVENTS_V1))
    return live


# ----------------------------------------------------------------------- tests ---


def test_a_hydrated_store_answers_as_the_live_store_did(
    clean: Live, database: str, redis_client: Any, spark: Any, record_property: Any
) -> None:
    """B1: primitives rebuilt through the store's own path answer every read as the live store."""
    before = int(redis_client.info("memory")["used_memory"])
    with _hydrator(spark, clean.lake, database, redis_client, "hyd-clean") as rebuilt:
        report = rebuilt.hydrator.run()
    assert report.claimed, report.summary()
    assert report.progress.recorded > 0 and report.progress.redeliveries == 0
    after = int(redis_client.info("memory")["used_memory"])
    peak = int(redis_client.info("memory")["used_memory_peak"])
    hydrated_keys = sum(1 for _ in redis_client.scan_iter(match="hyd-clean:*", count=1000))
    live_keys = sum(1 for _ in redis_client.scan_iter(match=f"{clean.namespace}:*", count=1000))
    record_property("redis_used_memory_before", before)
    record_property("redis_used_memory_after", after)
    record_property("redis_used_memory_delta", after - before)
    # Instance-wide since the server started: it does not isolate this run, and is recorded as
    # the diagnostic it is. The delta and the key counts are what this run owns.
    record_property("redis_used_memory_peak_instance_wide", peak)
    record_property("hydrated_namespace_keys", hydrated_keys)
    record_property("live_namespace_keys", live_keys)
    assert peak >= after
    assert hydrated_keys > 0

    live_contexts = _probe_contexts(clean.store, clean)
    rebuilt_contexts = _probe_contexts(rebuilt.store, clean)
    assert set(live_contexts) == set(rebuilt_contexts)
    for key, served in live_contexts.items():
        assert _served(rebuilt_contexts[key]) == _served(served), key

    since = report.claim.since_ms
    assert since is not None and since < report.begin_epoch_ms
    assert rebuilt.store.stored_epoch_ms() == since
    assert rebuilt.store.hydration_marker()["state"] == "claimed"


def test_hydrating_twice_equals_hydrating_once(
    clean: Live, database: str, redis_client: Any, spark: Any
) -> None:
    with _hydrator(spark, clean.lake, database, redis_client, "hyd-twice") as first:
        once = first.hydrator.run()
        contexts = _probe_contexts(first.store, clean)
        position, epoch = first.store.stored_position(), first.store.stored_epoch_ms()
    assert once.claimed
    with _hydrator(spark, clean.lake, database, redis_client, "hyd-twice") as again:
        twice = again.hydrator.run()
        assert _probe_contexts(again.store, clean) == contexts
        assert (again.store.stored_position(), again.store.stored_epoch_ms()) == (position, epoch)
    assert twice.claimed and twice.outcome == "already_claimed"
    assert twice.progress.replayed == 0, "a claimed store is never replayed into again"


def test_a_store_another_writer_has_touched_is_refused(
    clean: Live, database: str, redis_client: Any, spark: Any
) -> None:
    """(d): an epoch hydration did not set is never moved earlier."""
    foreign = RedisOnlineFeatureStore(redis_client, namespace="hyd-foreign")
    established = foreign.establish_epoch()
    with (
        _hydrator(spark, clean.lake, database, redis_client, "hyd-foreign") as rebuilt,
        pytest.raises(HydrationRefusedError, match="already holds an epoch"),
    ):
        rebuilt.hydrator.prepare()
    assert foreign.stored_epoch_ms() == to_millis(established)
    assert foreign.hydration_marker() == {}, "a refused run leaves no marker"


def test_a_held_fence_refuses_before_anything_is_written(
    clean: Live, database: str, redis_client: Any, spark: Any
) -> None:
    """Hydration never writes online state while another process holds the writer fence."""
    dsn = _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD", database)
    blocker = WriterSupervisor(
        connect=lambda: writer_connection(dsn),
        producer=GATEWAY,
        instance_id="blocker",
        lock_key=WRITER_LOCK_KEY,
    )
    blocker.start()
    deadline = time.monotonic() + 30
    while not blocker.ready and time.monotonic() < deadline:
        time.sleep(0.05)
    assert blocker.ready, blocker.status
    try:
        with _hydrator(
            spark, clean.lake, database, redis_client, "hyd-fenced", ready_timeout_s=3.0
        ) as rebuilt:
            with pytest.raises(HydrationRefusedError, match="not the fenced writer"):
                rebuilt.hydrator.prepare()
            assert rebuilt.store.stored_epoch_ms() is None
            assert rebuilt.store.stored_position() == 0
    finally:
        blocker.stop(confirmed=True)


def test_a_withdrawal_during_the_run_makes_the_claim_fail_closed(
    clean: Live, database: str, redis_client: Any, spark: Any
) -> None:
    """The epoch is no longer hydration's own `B`, so the compare-and-set does not apply."""
    with _hydrator(spark, clean.lake, database, redis_client, "hyd-withdrawn") as rebuilt:
        rebuilt.hydrator.prepare()
        evidence = read_evidence(
            spark,
            clean.lake,
            ledger=rebuilt.hydrator.ledger,
            clock_margin_s=CLOCK_MARGIN_S,
            now=dt.datetime.now(dt.UTC),
        )
        progress = rebuilt.hydrator.replay(evidence)
        moved = from_millis(rebuilt.store.stored_epoch_ms() or 0) + dt.timedelta(hours=1)
        rebuilt.store.withdraw_completeness(resume_at=EventTime(moved))
        report = rebuilt.hydrator.claim(evidence, progress)
    assert not report.claimed and report.outcome == "epoch"
    assert rebuilt.store.stored_epoch_ms() == to_millis(moved)


def test_a_foreign_write_during_the_run_makes_the_claim_fail_closed(
    clean: Live, database: str, redis_client: Any, spark: Any
) -> None:
    """The store's own counter moved, so another writer recorded an observation."""
    with _hydrator(spark, clean.lake, database, redis_client, "hyd-raced") as rebuilt:
        rebuilt.hydrator.prepare()
        evidence = read_evidence(
            spark,
            clean.lake,
            ledger=rebuilt.hydrator.ledger,
            clock_margin_s=CLOCK_MARGIN_S,
            now=dt.datetime.now(dt.UTC),
        )
        progress = rebuilt.hydrator.replay(evidence)
        RedisOnlineFeatureStore(redis_client, namespace=rebuilt.namespace).observe(
            Event(
                stream=Stream.IDENTITY_CHANGE,
                occurred_at=event_time(dt.datetime.now(dt.UTC)),
                account_id="acct_000001",
                event_id=f"idev_{uuid.uuid4().hex}",
            )
        )
        report = rebuilt.hydrator.claim(evidence, progress)
    assert not report.claimed and report.outcome == "position"
    assert rebuilt.store.stored_epoch_ms() == report.begin_epoch_ms, "left at its own withdrawal"


def test_a_crashed_run_resumes_and_converges_on_the_same_store(
    clean: Live, database: str, redis_client: Any, spark: Any
) -> None:
    """A crash mid-replay claims nothing, and a re-run converges on a clean hydration's store."""
    namespace = "hyd-crash"
    killed = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-m",
            "tests.integration.hydration_harness",
            "--namespace",
            namespace,
            "--dsn",
            _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD", database),
            "--lake-root",
            str(clean.lake.root),
            "--clock-margin-s",
            str(CLOCK_MARGIN_S),
            "--kill-after",
            "5",
            "--batch-size",
            "2",
            "--redis-host",
            REDIS_HOST,
            "--redis-port",
            str(REDIS_PORT),
            "--redis-db",
            str(REDIS_DB),
            "--lock-key",
            str(WRITER_LOCK_KEY),
        ],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": f"{ROOT / 'packages'}:{ROOT}"},
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert killed.returncode == -signal.SIGKILL, (killed.returncode, killed.stdout, killed.stderr)
    crashed = RedisOnlineFeatureStore(redis_client, namespace=namespace)
    marker = crashed.hydration_marker()
    assert marker["state"] == "replaying", marker
    assert crashed.stored_epoch_ms() == int(marker["begin_epoch_ms"]), (
        "a crash claims nothing: the store vouches only from hydration's own withdrawal"
    )
    assert 0 < int(marker["count"]) < crashed.stored_position() + 1

    with _hydrator(spark, clean.lake, database, redis_client, namespace) as resumed:
        report = resumed.hydrator.run()
    assert report.claimed and report.progress.resumed_from > 0
    with _hydrator(spark, clean.lake, database, redis_client, "hyd-control") as control:
        control_report = control.hydrator.run()
    assert control_report.claimed
    assert report.claim.since_ms == control_report.claim.since_ms
    assert resumed.store.stored_position() == control.store.stored_position()
    assert _probe_contexts(resumed.store, clean) == _probe_contexts(control.store, clean)


# -- histories of their own -------------------------------------------------


@pytest.fixture(scope="module")
def with_a_loss(
    topics: Broker, database: str, redis_client: Any, spark: Any, tmp_path_factory: Any
) -> Live:
    """A history with one observation the store never recorded and the log never carried."""
    _fresh(topics, TX_SCORED_V1, IDENTITY_EVENTS_V1)
    lake = LakeConfig.at(tmp_path_factory.mktemp("loss-lake"))
    live = _write_history(
        topics, database, redis_client, lake, namespace="live-loss", transactions=10, lose_one=True
    )
    _medallion(spark, lake, topics, (TX_SCORED_V1, IDENTITY_EVENTS_V1))
    return live


def test_a_lost_observation_absent_from_history_keeps_the_claim_after_it(
    with_a_loss: Live, database: str, redis_client: Any, spark: Any
) -> None:
    """(c) and D5: the hole is withdrawn as the guard withdraws it, and the loss still moves `T`.

    The lost observation is in neither the store nor the log, so history cannot rebuild it. Its
    sequence number is missing, which the coverage rule reports as a gap, and the claim must start
    after the latest event time that lost write could have carried.
    """
    pool = _pool(database)
    try:
        assert PostgresHoleLedger(pool).open_holes() == 1, "the live gateway recorded its loss"
        with _hydrator(spark, with_a_loss.lake, database, redis_client, "hyd-loss") as rebuilt:
            report = rebuilt.hydrator.run()
        assert PostgresHoleLedger(pool).open_holes() == 0, (
            "hydration withdrew it, as the guard does"
        )
    finally:
        pool.close()
    assert report.claimed, report.summary()
    assert with_a_loss.lost_at is not None
    reach = to_millis(with_a_loss.lost_at + dt.timedelta(seconds=MAX_CLOCK_SKEW_FUTURE_S))
    assert report.claim.since_ms is not None and report.claim.since_ms > reach, (
        "the claim must not cross the event time the lost observation could have carried"
    )
    assert report.claim.components["coverage_gaps"] == report.claim.since_ms


@pytest.fixture(scope="module")
def with_a_stranger(
    topics: Broker, database: str, redis_client: Any, spark: Any, tmp_path_factory: Any
) -> Live:
    """A history plus a record whose session the ledger does not hold."""
    from tests.integration.test_bronze_kafka import _session_headers

    _fresh(topics, TX_SCORED_V1, IDENTITY_EVENTS_V1)
    lake = LakeConfig.at(tmp_path_factory.mktemp("stranger-lake"))
    live = _write_history(
        topics, database, redis_client, lake, namespace="live-stranger", transactions=6
    )
    envelope = build_event(
        event_type="identity.events",
        occurred_at=event_time(BASE + dt.timedelta(hours=1)),
        payload={"account_id": "acct_000001", "identity_event_type": IDENTITY_EVENT_TYPE},
        producer=GATEWAY,
        trace_id=uuid.uuid4().hex,
        correlation_id=f"idev_{uuid.uuid4().hex}",
    )
    _publish(
        topics,
        [(IDENTITY_EVENTS_V1, envelope, _session_headers(f"unknown-{uuid.uuid4().hex[:8]}", 1))],
    )
    _medallion(spark, lake, topics, (TX_SCORED_V1, IDENTITY_EVENTS_V1))
    return live


def test_an_unknown_session_is_an_open_gap_and_nothing_is_claimed(
    with_a_stranger: Live, database: str, redis_client: Any, spark: Any
) -> None:
    """A session the ledger does not hold is open-ended, so no claim may cross it."""
    with _hydrator(spark, with_a_stranger.lake, database, redis_client, "hyd-stranger") as rebuilt:
        report = rebuilt.hydrator.run()
        epoch = rebuilt.store.stored_epoch_ms()
    assert not report.claimed and report.outcome == "not_claimable"
    assert any("open gap" in reason for reason in report.claim.reasons), report.claim.reasons
    assert epoch == report.begin_epoch_ms, "the store is left vouching from the withdrawal alone"
    assert report.progress.recorded > 0, "the primitives are still rebuilt; only the claim is not"


def test_silver_behind_the_evidence_is_refused(
    topics: Broker, database: str, redis_client: Any, spark: Any, tmp_path_factory: Any
) -> None:
    """(b): Silver must hold every observation the coverage counted, or nothing is claimed."""
    _fresh(topics, TX_SCORED_V1, IDENTITY_EVENTS_V1)
    lake = LakeConfig.at(tmp_path_factory.mktemp("lag-lake"))
    _write_history(topics, database, redis_client, lake, namespace="live-lag", transactions=4)
    _medallion(spark, lake, topics, (TX_SCORED_V1, IDENTITY_EVENTS_V1))
    # More history reaches Bronze, and Silver is not run again.
    _write_history(
        topics, database, redis_client, lake, namespace="live-lag", transactions=3, first_index=90
    )
    for topic in (TX_SCORED_V1, IDENTITY_EVENTS_V1):
        _ingest(spark, lake, topics, topic)
    with _hydrator(spark, lake, database, redis_client, "hyd-lag") as rebuilt:
        rebuilt.hydrator.prepare()
        with pytest.raises(HydrationRefusedError, match="below the version"):
            read_evidence(
                spark,
                lake,
                ledger=rebuilt.hydrator.ledger,
                clock_margin_s=CLOCK_MARGIN_S,
                now=dt.datetime.now(dt.UTC),
            )
        assert rebuilt.store.stored_position() == 0, "nothing is replayed onto unproven evidence"
