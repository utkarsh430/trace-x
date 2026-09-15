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
import tempfile
import time
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
from trace_core.observation.log import SEQ_HEADER, SESSION_HEADER
from trace_core.observation.session import WRITER_LOCK_KEY

pytestmark = [pytest.mark.chaos, pytest.mark.integration]

ROOT = Path(__file__).resolve().parents[2]
LOCK_KEY = WRITER_LOCK_KEY + 3
"""Not the gateway's key, so a running gateway is neither disturbed nor contended with."""
INTERVAL_S = 0.5
LEASE_S = 3 * INTERVAL_S
TAKEOVER_MARGIN_S = 1.0
"""The harness's lease and the margin its takeover grace adds (grace = lease + 1 s)."""
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
    written: dict[int, tuple[int, dt.datetime]]
    """Store position -> (sequence number, a time at or after its online write)."""
    ledger_read_at: dt.datetime

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
    early_ledger_after_seq: int = 0,
    pause_after: tuple[int, ...] = (),
    pause_s: float = 0.0,
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
    for seq in pause_after:
        command += ["--pause-after-seq", str(seq)]
    if pause_after:
        command += ["--pause-s", str(pause_s)]
    early: tuple[dt.datetime, list[SessionRow]] | None = None
    lines: list[str] = []
    with tempfile.TemporaryFile("w+") as stderr_file:
        process = subprocess.Popen(  # noqa: S603
            command,
            cwd=ROOT,
            env={**os.environ, "PYTHONPATH": str(ROOT)},
            stdout=subprocess.PIPE,
            stderr=stderr_file,
            text=True,
        )
        assert process.stdout is not None
        for line in process.stdout:
            lines.append(line)
            if (
                early_ledger_after_seq
                and early is None
                and line.startswith('{"written"')
                and json.loads(line)["written"]["seq"] == early_ledger_after_seq
            ):
                # Past the clock margin, so the writes before the read are inside `through`.
                time.sleep(2 * CLOCK_MARGIN_S)
                early = _read_ledger(owner, instance_id)
        returncode = process.wait(timeout=240)
        stderr_file.seek(0)
        stderr = stderr_file.read()
    exited = time.monotonic()
    stdout = "".join(lines)
    log_through = dt.datetime.now(dt.UTC)
    records = [
        record
        for record in _consume(target, TX_SCORED_V1, start)
        if dt.datetime.fromtimestamp(record.timestamp_ms / 1000, tz=dt.UTC) <= log_through
    ]
    if early is None and returncode != 0:
        # A killed writer's lease runs out after its last heartbeat, which precedes its exit. Read
        # the ledger once that has passed, so the rule's bounded tail is what is exercised.
        settle = LEASE_S + TAKEOVER_MARGIN_S + 2 * CLOCK_MARGIN_S + 0.5
        time.sleep(max(0.0, exited + settle - time.monotonic()))
    ledger_read_at, sessions = early if early is not None else _read_ledger(owner, instance_id)
    session_ids = {session.session_id for session in sessions}
    observed: list[Observed] = []
    logged: set[int] = set()
    for record in records:
        headers = dict(record.headers)
        session_id = headers[SESSION_HEADER].decode() if SESSION_HEADER in headers else None
        if session_id not in session_ids:
            continue
        value = json.loads(record.value)
        observed.append(
            Observed(
                session_id=session_id,
                seq=int(headers[SEQ_HEADER]),
                logged_at=dt.datetime.fromtimestamp(record.timestamp_ms / 1000, tz=dt.UTC),
                written_at=dt.datetime.fromisoformat(value["envelope"]["ingested_at"]),
            )
        )
        position = value["payload"]["store_position"]
        if position is not None:
            logged.add(int(position))
    raw_position = redis_client.get(f"{namespace}:position")
    coverage = assess(
        sessions,
        observed,
        ledger_read_at=ledger_read_at,
        log_read_through=log_through,
        lease_s=LEASE_S,
        takeover_margin_s=TAKEOVER_MARGIN_S,
        clock_margin_s=CLOCK_MARGIN_S,
    )
    written: dict[int, tuple[int, dt.datetime]] = {}
    for line in lines:
        if line.startswith('{"written"'):
            entry = json.loads(line)["written"]
            if entry["position"] is not None:
                written[int(entry["position"])] = (
                    int(entry["seq"]),
                    dt.datetime.fromisoformat(entry["at"]),
                )
    return Run(
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
        sessions=sessions,
        observed=observed,
        logged_positions=logged,
        store_position=0 if raw_position is None else int(raw_position),
        coverage=coverage,
        written=written,
        ledger_read_at=ledger_read_at,
    )


def _read_ledger(owner: Any, instance_id: str) -> tuple[dt.datetime, list[SessionRow]]:
    """The ledger as one read sees it, with that read's time taken first: everything committed
    before the returned time is in the rows."""
    read_at = owner.execute("SELECT clock_timestamp()").fetchone()[0]
    rows = owner.execute(
        "SELECT session_id, started_at, heartbeat_at, closed_at, last_seq "
        "FROM app.producer_sessions WHERE instance_id = %s",
        (instance_id,),
    ).fetchall()
    return read_at, [
        SessionRow(
            session_id=str(row[0]),
            started_at=row[1],
            heartbeat_at=row[2],
            closed_at=row[3],
            last_seq=None if row[4] is None else int(row[4]),
        )
        for row in rows
    ]


def _assert_no_invisible_loss(run: Run) -> None:
    """The capability's invariant, checked against what the store recorded, not against the rule.

    Every store position the log lacks was written at a recorded time. That time lies inside a gap
    of its own session, or at or after `Coverage.through`, beyond which nothing is vouched for.
    And the log never vouches for a span containing it.
    """
    coverage = run.coverage
    for gap in coverage.gaps:
        assert gap.end is None or gap.start <= gap.end, gap
    for session in run.sessions:
        verdict = coverage.sessions[session.session_id]
        present = {o.seq for o in run.observed if o.session_id == session.session_id}
        assert set(range(1, verdict.certified_through + 1)) <= present, "certified but absent"
    session_ids = {session.session_id for session in run.sessions}
    for position in range(1, run.store_position + 1):
        if position in run.logged_positions:
            continue
        assert position in run.written, f"position {position} has no recorded write time"
        _, at = run.written[position]
        assert not coverage.vouches(at, at), f"the log vouches for lost position {position}"
        if at >= coverage.through:
            continue
        assert any(gap.contains(at) for gap in coverage.gaps if gap.session_id in session_ids), (
            f"position {position}, written at {at}, lies outside every gap"
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
    # Accounts rotate over partitions, so a kill can drop an earlier record while a later one on
    # another partition was delivered: that is a run gap of its own. The unknown tail is always one.
    (tail,) = [gap for gap in verdict.gaps if not gap.missing]
    fence = dt.timedelta(seconds=LEASE_S + TAKEOVER_MARGIN_S)
    margin = dt.timedelta(seconds=CLOCK_MARGIN_S)
    assert session.heartbeat_at + fence + 2 * margin < run.ledger_read_at, "the lease ran out first"
    assert tail.end == session.heartbeat_at + fence + margin, (
        "the tail ends where the fenced process could no longer write"
    )
    logged_seqs = {o.seq for o in run.observed}
    for gap in verdict.gaps:
        assert not set(gap.missing) & logged_seqs, "a run gap names only numbers the log lacks"
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
    # The missing number is a run between the stamps of 2 and 4; the unclosed session also keeps an
    # unknown tail above 4.
    (gap, tail) = verdict.gaps
    assert gap.missing == (3,) and gap.end is not None
    assert tail.missing == () and tail.start > gap.start
    assert run.store_position == 4 and run.logged_positions == {1, 2, 4}
    _assert_no_invisible_loss(run)


def test_a_ledger_read_before_the_losses_never_vouches_for_them(
    topics: Broker,  # noqa: F811
    owner: Any,
    redis_client: Any,
) -> None:
    """Critic finding A1: the ledger is read mid-run, before a later write that is lost.

    Writes 1-2, a pause, writes 3-4, a pause during which the ledger is read, then write 5, which
    reaches the store and never the log. The pauses outlast delivery and the clock margin, so what
    the rule may vouch for is fixed: 1-2 lie more than the margin before the tail, and 5 follows the
    read.
    """
    run = _run(
        topics,
        owner,
        redis_client,
        kill_at="after_online_write",
        kill_on=5,
        count=6,
        early_ledger_after_seq=4,
        pause_after=(2, 4),
        pause_s=4 * CLOCK_MARGIN_S,
    )
    assert run.returncode == SIGKILLED, run.stderr
    (session,) = run.sessions
    assert session.closed_at is None and session.last_seq is None
    assert run.logged_positions == {1, 2, 3, 4}, "the pauses let 1-4 be delivered"
    assert run.store_position == 5, "5 was written online, never logged"
    _, lost_at = run.written[5]
    assert lost_at > run.ledger_read_at, "the loss follows the ledger read"
    assert not run.coverage.vouches(lost_at, lost_at)
    first, second = run.written[1][1], run.written[2][1]
    assert run.coverage.vouches(first, second), (
        "writes long before the read, all logged, are vouched"
    )
    _assert_no_invisible_loss(run)
