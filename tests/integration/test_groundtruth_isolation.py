"""GROUND-TRUTH ISOLATION -- RELEASE BLOCKER (ADR-0004).

The generator injects fraud with known labels and causal_evidence_keys. If any
of that reaches an agent, EVERY metric in this project is silently invalid.

The control is a database grant, not a naming convention, because the failure
mode is a code bug -- so the control must sit below the code. These tests run
the real Alembic migration against a real PostgreSQL instance and assert the
grants it produces. Nothing here is mocked.

If this file fails, the release is blocked. Do not skip it, and do not "fix" it
by granting trace_app access to groundtruth.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.security]

REPO_ROOT = Path(__file__).resolve().parents[2]

IMAGE = "postgres:16"
DB, OWNER, OWNER_PW = "tracex", "tracex_owner", "isolation_owner_pw"

# Injected into the migration via the environment; never stored in a file.
ROLE_PWS = {
    "trace_app": "isolation_app_pw",
    "trace_stream": "isolation_stream_pw",
    "trace_eval": "isolation_eval_pw",
    "trace_auditor": "isolation_auditor_pw",
    # Added by migration 0002 (ADR-0031). The migration refuses to create a role
    # without a password, so omitting this fails the whole upgrade -- which is
    # the intended behaviour, and is why it is listed here rather than defaulted.
    "trace_generator": "isolation_generator_pw",
}
ENV_VARS = {
    "trace_app": "TRACE_APP_DB_PASSWORD",
    "trace_stream": "TRACE_STREAM_DB_PASSWORD",
    "trace_eval": "TRACE_EVAL_DB_PASSWORD",
    "trace_auditor": "TRACE_AUDITOR_DB_PASSWORD",
    "trace_generator": "TRACE_GENERATOR_DB_PASSWORD",
}


def _docker(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        ["docker", *args], capture_output=True, text=True, timeout=180
    )


def _free_port(container: str) -> str:
    out = _docker("port", container, "5432/tcp").stdout.strip().splitlines()
    assert out, "container published no port"
    return out[0].rsplit(":", 1)[1]


@pytest.fixture(scope="module")
def migrated_db(repo_root, docker_available):
    """A real PostgreSQL with the real migration applied."""
    if not docker_available:
        pytest.skip(
            "SKIPPED (NOT PASSED): Docker unavailable. The ground-truth isolation "
            "control is UNVERIFIED. Start Docker and re-run before any release."
        )
    name = f"tracex-isolation-{uuid.uuid4().hex[:8]}"
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
        "-P",
        IMAGE,
    )
    assert r.returncode == 0, f"could not start postgres: {r.stderr}"
    try:
        for _ in range(90):
            if _docker("exec", name, "pg_isready", "-U", OWNER, "-d", DB).returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail(f"postgres never became ready: {_docker('logs', name).stderr[-2000:]}")
        time.sleep(1)

        port = _free_port(name)
        env = {
            **os.environ,
            "POSTGRES_SUPERUSER": OWNER,
            "POSTGRES_SUPERUSER_PASSWORD": OWNER_PW,
            "POSTGRES_HOST": "127.0.0.1",
            "POSTGRES_PORT": port,
            "POSTGRES_DB": DB,
            **{ENV_VARS[role]: pw for role, pw in ROLE_PWS.items()},
        }
        env.pop("TRACE_DATABASE_URL", None)

        mig = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=repo_root,
            env=env,
            capture_output=True,
            text=True,
            timeout=180,
        )
        assert mig.returncode == 0, f"alembic upgrade failed:\n{mig.stdout}\n{mig.stderr}"
        yield name, env
    finally:
        _docker("rm", "-f", name)


def _as(container: str, role: str, sql: str) -> subprocess.CompletedProcess[str]:
    pw = OWNER_PW if role == OWNER else ROLE_PWS[role]
    return subprocess.run(  # noqa: S603
        [
            "docker",
            "exec",
            "-e",
            f"PGPASSWORD={pw}",
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
def seeded(migrated_db) -> str:
    """Ground-truth and application tables, created by the owner."""
    container, _ = migrated_db
    r = _as(
        container,
        OWNER,
        (
            "CREATE TABLE IF NOT EXISTS groundtruth.labels("
            " tx_id text primary key, is_fraud bool, fraud_pattern text,"
            " causal_evidence_keys text[]);"
            "INSERT INTO groundtruth.labels VALUES"
            " ('tx_1', true, 'DEVICE_FARM', ARRAY['DEVICE_SHARING','GRAPH_CLUSTER'])"
            " ON CONFLICT DO NOTHING;"
            "CREATE TABLE IF NOT EXISTS app.cases(id text primary key);"
            "CREATE TABLE IF NOT EXISTS audit.events(seq bigserial primary key, payload text);"
        ),
    )
    assert r.returncode == 0, r.stderr
    return container


# --------------------------------------------------------------- migration --


def test_migration_creates_all_five_schemas(seeded: str) -> None:
    r = _as(
        seeded,
        OWNER,
        "select nspname from pg_namespace "
        "where nspname in ('app','audit','groundtruth','eval','external') order by 1",
    )
    assert sorted(r.stdout.split()) == ["app", "audit", "eval", "external", "groundtruth"]


def test_migration_creates_all_service_roles(seeded: str) -> None:
    r = _as(seeded, OWNER, "select rolname from pg_roles where rolname like 'trace\\_%' order by 1")
    assert sorted(r.stdout.split()) == [
        "trace_app",
        "trace_auditor",
        "trace_eval",
        "trace_generator",
        "trace_stream",
    ]


def test_migration_is_recorded_in_the_version_table(seeded: str) -> None:
    """Pinned to the current head, so adding a migration is a deliberate edit
    here rather than something that slips through unnoticed."""
    r = _as(seeded, OWNER, "select version_num from app.alembic_version")
    assert r.stdout.strip() == "0008"


def test_ground_truth_schema_carries_an_explanatory_comment(seeded: str) -> None:
    """A future engineer must find out WHY before trying to add a grant."""
    r = _as(seeded, OWNER, "select obj_description('groundtruth'::regnamespace, 'pg_namespace')")
    assert "trace_app MUST NOT" in r.stdout
    assert "ADR-0004" in r.stdout


# ------------------------------------------------ THE RELEASE-BLOCKING SET --


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
    agent reasoning, silently invalidating every metric in the project.
    """
    r = _as(seeded, role, sql)
    assert r.returncode != 0, (
        f"SECURITY FAILURE: {role} successfully read groundtruth. "
        f"Output: {r.stdout!r}. See ADR-0004."
    )
    assert "permission denied for schema groundtruth" in r.stderr, (
        f"{role} was denied, but not by the expected schema grant. Got: {r.stderr!r}"
    )


def test_ground_truth_values_never_leak_through_error_output(seeded: str) -> None:
    r = _as(seeded, "trace_app", "select fraud_pattern from groundtruth.labels")
    combined = r.stdout + r.stderr
    assert "DEVICE_FARM" not in combined
    assert "DEVICE_SHARING" not in combined


def test_app_role_cannot_create_objects_in_ground_truth(seeded: str) -> None:
    """No write path either -- an agent must not be able to stage a copy."""
    r = _as(seeded, "trace_app", "create table groundtruth.sneaky(x int)")
    assert r.returncode != 0
    assert "permission denied" in r.stderr


def test_eval_role_can_read_ground_truth(seeded: str) -> None:
    """Isolation must not be so broad that the harness cannot measure anything."""
    r = _as(seeded, "trace_eval", "select tx_id, is_fraud from groundtruth.labels")
    assert r.returncode == 0, f"trace_eval must read groundtruth: {r.stderr}"
    assert "tx_1|t" in r.stdout


def test_app_role_can_still_use_the_app_schema(seeded: str) -> None:
    """Isolation must not break the application."""
    r = _as(seeded, "trace_app", "select count(*) from app.cases")
    assert r.returncode == 0, f"trace_app must read app schema: {r.stderr}"
    assert r.stdout.strip() == "0"


# ------------------------------------------------- audit log is append-only --


def test_app_can_append_to_the_audit_log(seeded: str) -> None:
    r = _as(seeded, "trace_app", "insert into audit.events(payload) values ('audited')")
    assert r.returncode == 0, f"trace_app must be able to append audit events: {r.stderr}"


@pytest.mark.parametrize(
    "sql",
    [
        "select * from audit.events",
        "update audit.events set payload = 'tampered'",
        "delete from audit.events",
    ],
)
def test_app_cannot_read_modify_or_delete_the_audit_log(seeded: str, sql: str) -> None:
    """Append-only at the database level, not by application convention (ADR-0020)."""
    r = _as(seeded, "trace_app", sql)
    assert r.returncode != 0, f"SECURITY FAILURE: trace_app could run {sql!r}"
    assert "permission denied" in r.stderr


def test_auditor_can_read_what_the_app_appended(seeded: str) -> None:
    r = _as(seeded, "trace_auditor", "select payload from audit.events")
    assert r.returncode == 0, r.stderr
    assert "audited" in r.stdout


# ------------------------------------------------------- migration lifecycle --


def test_migration_round_trip_removes_and_restores_everything(migrated_db) -> None:
    """down then up must be clean -- a downgrade that half-works is worse than none."""
    container, env = migrated_db
    # Drop objects created by the tests so the roles own nothing.
    _as(
        container, OWNER, "DROP TABLE IF EXISTS audit.events, app.cases, groundtruth.labels CASCADE"
    )

    def alembic(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603
            [sys.executable, "-m", "alembic", *args],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=180,
        )

    down = alembic("downgrade", "base")
    assert down.returncode == 0, f"downgrade failed:\n{down.stdout}\n{down.stderr}"

    r = _as(
        container,
        OWNER,
        "select count(*) from pg_namespace "
        "where nspname in ('audit','groundtruth','eval','external')",
    )
    assert r.stdout.strip() == "0", "downgrade left schemas behind"
    r = _as(container, OWNER, "select count(*) from pg_roles where rolname like 'trace\\_%'")
    assert r.stdout.strip() == "0", "downgrade left roles behind (DROP OWNED BY missing?)"

    up = alembic("upgrade", "head")
    assert up.returncode == 0, f"re-upgrade failed:\n{up.stdout}\n{up.stderr}"
    r = _as(
        container,
        OWNER,
        "select count(*) from pg_namespace "
        "where nspname in ('app','audit','groundtruth','eval','external')",
    )
    assert r.stdout.strip() == "5", "re-upgrade did not restore all schemas"
