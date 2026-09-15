"""Chaos: every loss the observation log can suffer is a detectable gap (ADR-0051; plan §4.1).

Acceptance command for `P3.observation-log`. The failures are produced, not simulated. A child
process (`tests/chaos/observation_log_harness.py`) composes the production pieces: the fenced writer
on PostgreSQL, the scoring pipeline over the real Redis feature store, and the observation log
against a real broker. It kills itself with SIGKILL at each point of plan §4.1's loss table, or
sheds for real against a paused broker.

After each run the test reads back three independent records:
- what the log holds, from the broker;
- what the ledger says, from PostgreSQL;
- how many observations the online store recorded, from Redis.

It then applies the coverage rule (`trace_core.observation.coverage`), and asserts two things:
- the certified prefix is really in the log;
- every observation the store recorded is either in the log or inside a gap the rule reports. A loss
  nobody can see is the one failure this capability exists to rule out.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from tests.integration.test_kafka_platform import Broker, _consume, _watermarks
from tests.integration.test_observation_log_kafka import (  # noqa: F401 -- fixtures
    broker,
    topics,
)

from trace_core.contracts.topics import TX_SCORED_V1
from trace_core.observation.coverage import Coverage, Observed, SessionRow, assess
from trace_core.observation.log import DELIVERY_TIMEOUT_MS, SEQ_HEADER, SESSION_HEADER
from trace_core.observation.session import WRITER_LOCK_KEY

pytestmark = [pytest.mark.chaos, pytest.mark.integration]

ROOT = Path(__file__).resolve().parents[2]
LOCK_KEY = WRITER_LOCK_KEY + 3
"""Not the gateway's key, so a running gateway is neither disturbed nor contended with."""
INTERVAL_S = 0.5
CLOCK_MARGIN_S = 1.0
"""Broker and PostgreSQL share this host's clock here; the margin stands in for the measured
offset."""
INSTANCE_PREFIX = "gw-chaos-log-"
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6389"))
REDIS_DB = int(os.environ.get("REDIS_TEST_DB", "15"))
SIGKILLED = -9


def _dsn(user_env: str, password_env: str) -> str:
    return "postgresql://{u}:{p}@{h}:{port}/{db}".format(
        u=os.environ.get(user_env, ""),
        p=os.environ.get(password_env, ""),
        h=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5442"),
        db=os.environ.get("POSTGRES_DB", "tracex"),
    )


@pytest.fixture
def owner() -> Iterator[Any]:
    psycopg = pytest.importorskip("psycopg")
    try:
        connection = psycopg.connect(
            _dsn("POSTGRES_SUPERUSER", "POSTGRES_SUPERUSER_PASSWORD"), autocommit=True
        )
        connection.execute("SELECT 1 FROM app.producer_sessions LIMIT 1")
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no migrated PostgreSQL reachable as the owner ({exc}). Run "
            f"`make up && make migrate`; the ledger is a database guarantee and is never mocked."
        )
    yield connection
    connection.execute(
        "DELETE FROM app.producer_sessions WHERE instance_id LIKE %s", (f"{INSTANCE_PREFIX}%",)
    )
    connection.close()


@pytest.fixture
def redis_client() -> Iterator[Any]:
    redis = pytest.importorskip("redis", reason="the `db` extra provides the Redis client")
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    try:
        client.ping()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no Redis at {REDIS_HOST}:{REDIS_PORT} ({exc}). Run `make up`; "
            f"the online store is never faked."
        )
    yield client
    client.flushdb()


@dataclass(frozen=True)
class Run:
    returncode: int
    stdout: str
    stderr: str
    sessions: list[SessionRow]
    observed: list[Observed]
    logged_positions: set[int]
    store_position: int
    coverage: Coverage

    @property
    def summary(self) -> dict[str, Any]:
        lines = [line for line in self.stdout.splitlines() if line.startswith("{")]
        return json.loads(lines[-1]) if lines else {}


def _run(
    target: Broker,
    owner: Any,
    redis_client: Any,
    *,
    kill_at: str,
    kill_on: int = 3,
    count: int = 5,
    shed: bool = False,
) -> Run:
    instance_id = f"{INSTANCE_PREFIX}{uuid.uuid4().hex[:8]}"
    namespace = f"chaos-{instance_id}"
    start = _watermarks(target, TX_SCORED_V1)
    command = [
        sys.executable,
        "-m",
        "tests.chaos.observation_log_harness",
        "--kill-at",
        kill_at,
        "--kill-on",
        str(kill_on),
        "--count",
        str(count),
        "--bootstrap",
        target.bootstrap,
        "--dsn",
        _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD"),
        "--redis-host",
        REDIS_HOST,
        "--redis-port",
        str(REDIS_PORT),
        "--redis-db",
        str(REDIS_DB),
        "--namespace",
        namespace,
        "--lock-key",
        str(LOCK_KEY),
        "--instance-id",
        instance_id,
        "--interval-s",
        str(INTERVAL_S),
    ]
    if shed:
        command += ["--shed-container", target.name]
    done = subprocess.run(  # noqa: S603
        command,
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": str(ROOT)},
        capture_output=True,
        text=True,
        timeout=240,
    )
    sessions = [
        SessionRow(
            session_id=str(row[0]),
            started_at=row[1],
            heartbeat_at=row[2],
            closed_at=row[3],
            last_seq=None if row[4] is None else int(row[4]),
        )
        for row in owner.execute(
            "SELECT session_id, started_at, heartbeat_at, closed_at, last_seq "
            "FROM app.producer_sessions WHERE instance_id = %s",
            (instance_id,),
        ).fetchall()
    ]
    session_ids = {session.session_id for session in sessions}
    observed: list[Observed] = []
    logged: set[int] = set()
    for record in _consume(target, TX_SCORED_V1, start):
        headers = dict(record.headers)
        session_id = headers[SESSION_HEADER].decode() if SESSION_HEADER in headers else None
        if session_id not in session_ids:
            continue
        observed.append(
            Observed(
                session_id=session_id,
                seq=int(headers[SEQ_HEADER]),
                logged_at=dt.datetime.fromtimestamp(record.timestamp_ms / 1000, tz=dt.UTC),
            )
        )
        position = json.loads(record.value)["payload"]["store_position"]
        if position is not None:
            logged.add(int(position))
    raw_position = redis_client.get(f"{namespace}:position")
    coverage = assess(
        sessions,
        observed,
        heartbeat_interval_s=INTERVAL_S,
        delivery_timeout_s=DELIVERY_TIMEOUT_MS / 1000,
        clock_margin_s=CLOCK_MARGIN_S,
    )
    return Run(
        returncode=done.returncode,
        stdout=done.stdout,
        stderr=done.stderr,
        sessions=sessions,
        observed=observed,
        logged_positions=logged,
        store_position=0 if raw_position is None else int(raw_position),
        coverage=coverage,
    )


def _assert_no_invisible_loss(run: Run) -> None:
    for session in run.sessions:
        verdict = run.coverage.sessions[session.session_id]
        present = {o.seq for o in run.observed if o.session_id == session.session_id}
        assert set(range(1, verdict.certified_through + 1)) <= present, "certified but absent"
        for gap in verdict.gaps:
            assert gap.end >= gap.start
            assert all(
                o.logged_at <= gap.end for o in run.observed if o.session_id == session.session_id
            )
    unlogged = set(range(1, run.store_position + 1)) - run.logged_positions
    if unlogged:
        assert not run.coverage.gap_free, (
            f"the store recorded positions {sorted(unlogged)} that the log lacks, and no gap "
            f"says so"
        )


def test_a_clean_run_is_covered_end_to_end(topics: Broker, owner: Any, redis_client: Any) -> None:  # noqa: F811
    run = _run(topics, owner, redis_client, kill_at="none", count=5)
    assert run.returncode == 0, run.stderr
    (session,) = run.sessions
    assert session.closed_at is not None and session.last_seq == 5
    assert (
        run.coverage.gap_free and run.coverage.sessions[session.session_id].certified_through == 5
    )
    assert run.store_position == 5 and run.logged_positions == {1, 2, 3, 4, 5}
    _assert_no_invisible_loss(run)


def test_a_process_killed_before_its_session_opens_serves_and_records_nothing(
    topics: Broker,  # noqa: F811
    owner: Any,
    redis_client: Any,
) -> None:
    run = _run(topics, owner, redis_client, kill_at="before_session")
    assert run.returncode == SIGKILLED, run.stderr
    assert run.sessions == [] and run.observed == [] and run.store_position == 0


@pytest.mark.parametrize(
    "kill_at",
    ["after_session", "after_sequence", "after_online_write", "after_produce", "before_close"],
)
def test_a_process_killed_mid_flight_leaves_a_bounded_gap_and_no_invisible_loss(
    kill_at: str,
    topics: Broker,  # noqa: F811
    owner: Any,
    redis_client: Any,
) -> None:
    run = _run(topics, owner, redis_client, kill_at=kill_at, kill_on=3, count=5)
    assert run.returncode == SIGKILLED, run.stderr
    (session,) = run.sessions
    assert session.closed_at is None, "a killed session is never closed"
    verdict = run.coverage.sessions[session.session_id]
    assert not verdict.covered
    (gap,) = verdict.gaps
    bound = session.heartbeat_at + dt.timedelta(
        seconds=INTERVAL_S + DELIVERY_TIMEOUT_MS / 1000 + CLOCK_MARGIN_S
    )
    assert gap.end == bound, "the gap ends where the fenced process could no longer write"
    expected_position = {
        "after_session": 0,
        "after_sequence": 2,
        "after_online_write": 3,
        "after_produce": 3,
        "before_close": 5,
    }[kill_at]
    assert run.store_position == expected_position
    if kill_at == "after_online_write":
        assert 3 not in run.logged_positions, "recorded online, never produced"
    _assert_no_invisible_loss(run)


def test_a_process_killed_after_the_broker_acknowledged_certifies_what_arrived(
    topics: Broker,  # noqa: F811
    owner: Any,
    redis_client: Any,
) -> None:
    run = _run(topics, owner, redis_client, kill_at="after_ack", kill_on=3, count=5)
    assert run.returncode == SIGKILLED, run.stderr
    (session,) = run.sessions
    assert session.closed_at is None
    verdict = run.coverage.sessions[session.session_id]
    assert verdict.certified_through == 3, "every acknowledged number is certified"
    assert run.logged_positions == {1, 2, 3} and run.store_position == 3
    assert not verdict.covered, "the session never closed, so its tail is still a gap"
    _assert_no_invisible_loss(run)


def test_a_shed_observation_is_a_missing_number_and_the_session_never_closes(
    topics: Broker,  # noqa: F811
    owner: Any,
    redis_client: Any,
) -> None:
    run = _run(topics, owner, redis_client, kill_at="none", count=4, shed=True)
    assert run.returncode == 0, run.stderr
    assert run.summary["outcomes"] == ["handed_over", "handed_over", "shed", "handed_over"]
    assert run.summary["confirmed"] is False
    (session,) = run.sessions
    assert session.closed_at is None, "a session that shed can never close"
    assert sorted(o.seq or 0 for o in run.observed) == [1, 2, 4]
    verdict = run.coverage.sessions[session.session_id]
    assert verdict.certified_through == 2
    (gap,) = verdict.gaps
    assert gap.missing == (3,)
    assert run.store_position == 4 and run.logged_positions == {1, 2, 4}
    _assert_no_invisible_loss(run)
