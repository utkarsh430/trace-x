"""GROUND-TRUTH ISOLATION -- RELEASE BLOCKER (ADR-0004).

The generator injects fraud with known labels and causal_evidence_keys. If any
of that reaches an agent, EVERY metric in this project is silently invalid.

The control is a database grant, not a naming convention, because the failure
mode is a code bug -- so the control must sit below the code. These tests assert
the grant, against a real PostgreSQL instance. They are never mocked.

If this file fails, the release is blocked. Do not skip it, and do not "fix" it
by granting trace_app access.
"""

from __future__ import annotations

import os
import subprocess
import time
import uuid

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.security]

IMAGE = "postgres:16"
DB, OWNER, OWNER_PW = "tracex", "tracex_owner", "isolation_test_pw"
ROLE_PW = "change_me_locally"  # matches deploy/postgres/init


def _docker(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        ["docker", *args],
        capture_output=True,
        text=True,
        timeout=120,
    )


@pytest.fixture(scope="module")
def pg(repo_root, docker_available):
    if not docker_available:
        pytest.skip(
            "SKIPPED (NOT PASSED): Docker unavailable. The ground-truth isolation "
            "control is UNVERIFIED. Start Docker and re-run before any release."
        )
    name = f"tracex-isolation-{uuid.uuid4().hex[:8]}"
    init = repo_root / "deploy" / "postgres" / "init"
    assert init.is_dir(), "deploy/postgres/init must exist"

    r = _docker(
        "run",
        "-d",
        "--rm",
        "--name",
        name,
        "-e",
        f"POSTGRES_DB={DB}",
        "-e",
        f"POSTGRES_USER={OWNER}",
        "-e",
        f"POSTGRES_PASSWORD={OWNER_PW}",
        "-e",
        "POSTGRES_INITDB_ARGS=--locale=C --encoding=UTF8",
        # Role passwords are injected from the environment, never committed
        # (CLAUDE.md §9). The init script reads them via psql \getenv and
        # refuses to create passwordless roles.
        "-e",
        f"TRACE_APP_DB_PASSWORD={ROLE_PW}",
        "-e",
        f"TRACE_STREAM_DB_PASSWORD={ROLE_PW}",
        "-e",
        f"TRACE_EVAL_DB_PASSWORD={ROLE_PW}",
        "-e",
        f"TRACE_AUDITOR_DB_PASSWORD={ROLE_PW}",
        "-v",
        f"{init}:/docker-entrypoint-initdb.d:ro",
        "-P",
        IMAGE,
    )
    assert r.returncode == 0, f"could not start postgres: {r.stderr}"
    try:
        for _ in range(60):
            if _docker("exec", name, "pg_isready", "-U", OWNER, "-d", DB).returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail(f"postgres never became ready: {_docker('logs', name).stderr[-2000:]}")
        time.sleep(2)  # let the init scripts finish after the socket opens
        yield name
    finally:
        _docker("rm", "-f", name)


def _as(container: str, role: str, sql: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PGPASSWORD": ROLE_PW if role != OWNER else OWNER_PW}
    return subprocess.run(  # noqa: S603
        [
            "docker",
            "exec",
            "-e",
            f"PGPASSWORD={env['PGPASSWORD']}",
            container,
            "psql",
            "-U",
            role,
            "-h",
            "127.0.0.1",
            "-d",
            DB,
            "-tAc",
            sql,
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )


@pytest.fixture(scope="module")
def seeded(pg: str) -> str:
    """Create a ground-truth table with a real label, as the owner."""
    r = _as(
        pg,
        OWNER,
        (
            "CREATE TABLE IF NOT EXISTS groundtruth.labels("
            " tx_id text primary key, is_fraud bool, fraud_pattern text,"
            " causal_evidence_keys text[]);"
            "INSERT INTO groundtruth.labels VALUES"
            " ('tx_1', true, 'DEVICE_FARM', ARRAY['DEVICE_SHARING','GRAPH_CLUSTER'])"
            " ON CONFLICT DO NOTHING;"
            "CREATE TABLE IF NOT EXISTS app.cases(id text primary key);"
        ),
    )
    assert r.returncode == 0, r.stderr
    return pg


def test_init_script_creates_all_five_schemas(seeded: str) -> None:
    r = _as(
        seeded,
        OWNER,
        "select nspname from pg_namespace "
        "where nspname in ('app','audit','groundtruth','eval','external') order by 1",
    )
    assert sorted(r.stdout.split()) == ["app", "audit", "eval", "external", "groundtruth"]


def test_init_script_creates_all_service_roles(seeded: str) -> None:
    r = _as(seeded, OWNER, "select rolname from pg_roles where rolname like 'trace\\_%' order by 1")
    assert sorted(r.stdout.split()) == ["trace_app", "trace_auditor", "trace_eval", "trace_stream"]


# ---------------------------------------------------------------------------
# THE RELEASE-BLOCKING ASSERTIONS
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("role", ["trace_app", "trace_stream", "trace_auditor"])
@pytest.mark.parametrize(
    "sql",
    [
        "select count(*) from groundtruth.labels",
        "select * from groundtruth.labels limit 1",
        "select causal_evidence_keys from groundtruth.labels",
    ],
)
def test_ground_truth_is_unreachable_by_non_eval_roles(seeded: str, role: str, sql: str) -> None:
    """The application, agents, Spark and auditors must ALL be denied.

    Anything other than a permission denial here means ground truth can reach
    agent reasoning, which silently invalidates every metric in the project.
    """
    r = _as(seeded, role, sql)
    assert r.returncode != 0, (
        f"SECURITY FAILURE: {role} successfully read groundtruth. "
        f"Output: {r.stdout!r}. See ADR-0004."
    )
    assert "permission denied for schema groundtruth" in r.stderr, (
        f"{role} was denied, but not by the expected schema grant. Got: {r.stderr!r}"
    )


def test_ground_truth_labels_never_appear_in_denied_output(seeded: str) -> None:
    """Not even a leaked value in an error message."""
    r = _as(seeded, "trace_app", "select fraud_pattern from groundtruth.labels")
    combined = r.stdout + r.stderr
    assert "DEVICE_FARM" not in combined
    assert "DEVICE_SHARING" not in combined


def test_eval_role_can_read_ground_truth(seeded: str) -> None:
    """The isolation must not be so broad that the harness cannot measure anything."""
    r = _as(seeded, "trace_eval", "select tx_id, is_fraud from groundtruth.labels")
    assert r.returncode == 0, f"trace_eval must read groundtruth: {r.stderr}"
    assert "tx_1|t" in r.stdout


def test_app_role_can_still_use_the_app_schema(seeded: str) -> None:
    """Isolation must not break the application."""
    r = _as(seeded, "trace_app", "select count(*) from app.cases")
    assert r.returncode == 0, f"trace_app must read app schema: {r.stderr}"
    assert r.stdout.strip() == "0"


def test_app_role_cannot_create_objects_in_ground_truth(seeded: str) -> None:
    """No write path either -- an agent must not be able to stage a copy."""
    r = _as(seeded, "trace_app", "create table groundtruth.sneaky(x int)")
    assert r.returncode != 0
    assert "permission denied" in r.stderr
