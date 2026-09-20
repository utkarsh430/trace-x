"""The ground-truth WRITE path, against real PostgreSQL (ADR-0031).

Declared acceptance command for `P1.causal-evidence`.

`test_groundtruth_isolation.py` proves nothing can *read* ground truth. This file
proves the complementary half: exactly one role can *write* it, and that role
cannot read back what it wrote.

That second property is the one worth stating plainly. `trace_generator` holds
INSERT on the label tables and SELECT on the dataset registry only, so **no
credential used in ordinary development can read ground truth** -- not the
application's, and not the generator's either. If the generator could read its
own labels, a careless debugging session or a future feature could surface them,
and every metric in the project would be quietly invalid.

Nothing here is mocked. Real container, real migration, real grants.
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
DB, OWNER, OWNER_PW = "tracex", "tracex_owner", "writepath_owner_pw"

ROLE_PWS = {
    "trace_app": "writepath_app_pw",
    "trace_stream": "writepath_stream_pw",
    "trace_eval": "writepath_eval_pw",
    "trace_auditor": "writepath_auditor_pw",
    "trace_generator": "writepath_generator_pw",
}
ENV_VARS = {
    "trace_app": "TRACE_APP_DB_PASSWORD",
    "trace_stream": "TRACE_STREAM_DB_PASSWORD",
    "trace_eval": "TRACE_EVAL_DB_PASSWORD",
    "trace_auditor": "TRACE_AUDITOR_DB_PASSWORD",
    "trace_generator": "TRACE_GENERATOR_DB_PASSWORD",
}

WRITE_ONLY_TABLES = ("transaction_labels", "causal_evidence", "scenario_instances")


def _docker(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        ["docker", *args], capture_output=True, text=True, timeout=180
    )


@pytest.fixture(scope="module")
def migrated_db(repo_root, docker_available):
    if not docker_available:
        pytest.skip(
            "SKIPPED (NOT PASSED): Docker unavailable. The ground-truth WRITE path is "
            "UNVERIFIED -- P1.causal-evidence cannot be marked PASS. Start Docker and re-run."
        )
    name = f"tracex-writepath-{uuid.uuid4().hex[:8]}"
    started = _docker(
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
    assert started.returncode == 0, f"could not start postgres: {started.stderr}"
    try:
        for _ in range(90):
            if _docker("exec", name, "pg_isready", "-U", OWNER, "-d", DB).returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail(f"postgres never became ready: {_docker('logs', name).stderr[-2000:]}")
        time.sleep(1)

        ports = _docker("port", name, "5432/tcp").stdout.strip().splitlines()
        assert ports, "container published no port"
        port = ports[0].rsplit(":", 1)[1]

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

        migration = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=repo_root,
            env=env,
            capture_output=True,
            text=True,
            timeout=180,
        )
        assert migration.returncode == 0, (
            f"alembic upgrade failed:\n{migration.stdout}\n{migration.stderr}"
        )
        yield name, env, port
    finally:
        _docker("rm", "-f", "-v", name)


def _sql(container: str, role: str, statement: str) -> subprocess.CompletedProcess[str]:
    password = OWNER_PW if role == OWNER else ROLE_PWS[role]
    return subprocess.run(  # noqa: S603
        [
            "docker",
            "exec",
            "-e",
            f"PGPASSWORD={password}",
            container,
            "psql",
            "-U",
            role,
            "-h",
            "127.0.0.1",
            "-d",
            DB,
            "-tAc",
            statement,
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )


@pytest.fixture(scope="module")
def written(migrated_db):
    """A real dataset written by the real writer, as trace_generator."""
    container, _env, port = migrated_db

    from data.generator.config import GENERATOR_VERSION, GeneratorConfig
    from data.generator.digest import digest_of
    from data.generator.engine import generate_dataset
    from data.generator.groundtruth import DatasetRecord, write_dataset

    config = GeneratorConfig(
        row_count=1200,
        account_count=120,
        merchant_count=60,
        device_count=180,
        ip_count=90,
        fraud_rate=0.02,
    )
    rows = list(generate_dataset(config))
    digest, count = digest_of(r.event for r in rows)
    labels = [r.label for r in rows if r.label is not None]
    instances = {
        r.scenario_instance.instance_id: r.scenario_instance
        for r in rows
        if r.scenario_instance is not None
    }

    dsn = (
        f"host=127.0.0.1 port={port} dbname={DB} "
        f"user=trace_generator password={ROLE_PWS['trace_generator']}"
    )
    dataset_id = write_dataset(
        DatasetRecord(
            dataset_version="writepath-v1",
            seed=config.seed,
            generator_version=GENERATOR_VERSION,
            fraud_scenario_config_digest=config.digest(),
            dataset_digest=digest,
            row_count=count,
        ),
        labels,
        instances.values(),
        dsn=dsn,
    )
    return container, dsn, dataset_id, labels


# --------------------------------------------------------------- writing ----


def test_the_generator_can_write_ground_truth(written) -> None:
    container, _dsn, dataset_id, labels = written
    out = _sql(
        container,
        OWNER,
        f"SELECT count(*) FROM groundtruth.transaction_labels WHERE dataset_id={dataset_id}",
    )
    assert out.returncode == 0, out.stderr
    assert int(out.stdout.strip()) == len(labels)


def test_causal_evidence_was_written_for_every_fraud_label(written) -> None:
    """These rows are what evidence precision and recall are computed against."""
    container, _dsn, dataset_id, labels = written
    expected = sum(len(label.causal_evidence_keys) for label in labels)
    out = _sql(
        container,
        OWNER,
        f"SELECT count(*) FROM groundtruth.causal_evidence WHERE dataset_id={dataset_id}",
    )
    assert int(out.stdout.strip()) == expected
    assert expected > 0, "no causal evidence written; Track A would be unscoreable"


def test_scenario_instances_were_recorded(written) -> None:
    container, _dsn, dataset_id, _labels = written
    out = _sql(
        container,
        OWNER,
        f"SELECT count(*) FROM groundtruth.scenario_instances WHERE dataset_id={dataset_id}",
    )
    assert int(out.stdout.strip()) >= 10, "every pattern should have contributed an instance"


# ------------------------------------------- the writer cannot read back ----


@pytest.mark.parametrize("table", WRITE_ONLY_TABLES)
def test_the_generator_cannot_read_the_labels_it_wrote(written, table: str) -> None:
    """The property that makes this role worth having.

    No credential used in ordinary development can read ground truth -- not the
    application's, and not the generator's own.
    """
    container, _dsn, _dataset_id, _labels = written
    out = _sql(container, "trace_generator", f"SELECT count(*) FROM groundtruth.{table}")
    assert out.returncode != 0, f"trace_generator could SELECT {table}"
    assert "permission denied" in out.stderr.lower(), out.stderr


def test_the_generator_may_read_only_the_dataset_registry(written) -> None:
    """It must be able to check whether a version already exists, and no more."""
    container, _dsn, _dataset_id, _labels = written
    out = _sql(container, "trace_generator", "SELECT count(*) FROM groundtruth.datasets")
    assert out.returncode == 0, out.stderr
    assert int(out.stdout.strip()) >= 1


def test_the_generator_has_no_access_to_application_data(written) -> None:
    """Least privilege: the writer's blast radius is ground truth alone."""
    container, _dsn, _dataset_id, _labels = written
    out = _sql(
        container,
        "trace_generator",
        "SELECT 1 FROM information_schema.tables WHERE table_schema='app'",
    )
    # `app` has no tables yet, so the meaningful assertion is on the grant.
    usage = _sql(
        container,
        OWNER,
        "SELECT has_schema_privilege('trace_generator','app','USAGE')",
    )
    assert usage.stdout.strip() == "f", "trace_generator should have no USAGE on app"
    assert out.returncode == 0  # information_schema is world-readable by design


# ---------------------------------- isolation still holds WITH tables -------


@pytest.mark.parametrize("role", ["trace_app", "trace_stream", "trace_auditor"])
@pytest.mark.parametrize("table", ["transaction_labels", "causal_evidence", "datasets"])
def test_application_roles_are_still_denied_now_that_tables_exist(
    written, role: str, table: str
) -> None:
    """RELEASE BLOCKER.

    The Phase 0 test asserted the denial when `groundtruth` held no tables. The
    grant is at schema level, so it should still deny -- but "should" is not
    evidence, and this is the highest-severity control in the project.
    """
    container, _dsn, _dataset_id, _labels = written
    out = _sql(container, role, f"SELECT count(*) FROM groundtruth.{table}")
    assert out.returncode != 0, f"{role} could read groundtruth.{table}"
    assert "permission denied for schema groundtruth" in out.stderr.lower(), out.stderr


@pytest.mark.parametrize("table", ["transaction_labels", "causal_evidence", "datasets"])
def test_the_eval_role_can_read_ground_truth(written, table: str) -> None:
    """The harness must be able to measure, or none of this is usable."""
    container, _dsn, _dataset_id, _labels = written
    out = _sql(container, "trace_eval", f"SELECT count(*) FROM groundtruth.{table}")
    assert out.returncode == 0, out.stderr


# ----------------------------------------------------------- re-seeding ----


def test_reseeding_an_existing_dataset_version_is_refused(written) -> None:
    """`eval-v1` must never be regenerated in place (docs/EVALUATION.md §2).

    The refusal is a UNIQUE constraint, not an application check, so it holds
    regardless of which writer runs.
    """
    from data.generator.groundtruth import DatasetRecord, write_dataset

    _container, dsn, _dataset_id, _labels = written
    duplicate = DatasetRecord(
        dataset_version="writepath-v1",
        seed=1,
        generator_version="1.0.0",
        fraud_scenario_config_digest="sha256:" + "0" * 64,
        dataset_digest="sha256:" + "1" * 64,
        row_count=1,
    )
    with pytest.raises(Exception, match=r"duplicate key|unique"):
        write_dataset(duplicate, [], dsn=dsn)


def test_a_failed_write_leaves_no_partial_dataset(written) -> None:
    """All or nothing.

    A partially-written dataset would let the harness compute metrics over a
    subset while believing it had the whole -- a silently wrong number, which is
    the category this project treats as worse than a crash.
    """

    from data.generator.groundtruth import DatasetRecord, write_dataset
    from data.generator.labels import TransactionLabel

    container, dsn, _dataset_id, _labels = written
    before = _sql(container, OWNER, "SELECT count(*) FROM groundtruth.datasets").stdout.strip()

    # A label whose transaction_id is duplicated violates the primary key, so the
    # transaction must roll the dataset row back with it.
    doomed = [
        TransactionLabel(transaction_id="tx_dup", is_fraud=False),
        TransactionLabel(transaction_id="tx_dup", is_fraud=False),
    ]
    with pytest.raises(Exception, match=r"duplicate key|unique"):
        write_dataset(
            DatasetRecord(
                dataset_version="partial-v1",
                seed=2,
                generator_version="1.0.0",
                fraud_scenario_config_digest="sha256:" + "0" * 64,
                dataset_digest="sha256:" + "2" * 64,
                row_count=2,
            ),
            doomed,
            dsn=dsn,
        )

    after = _sql(container, OWNER, "SELECT count(*) FROM groundtruth.datasets").stdout.strip()
    assert before == after, "a failed write left a dataset row behind"
    orphan = _sql(
        container,
        OWNER,
        "SELECT count(*) FROM groundtruth.datasets WHERE dataset_version='partial-v1'",
    )
    assert orphan.stdout.strip() == "0"


def test_a_fraud_label_without_a_pattern_is_refused_by_the_database(written) -> None:
    """The consistency rule lives in the schema as well as in Python, because the
    table outlives any particular writer."""
    container, _dsn, dataset_id, _labels = written
    out = _sql(
        container,
        OWNER,
        f"INSERT INTO groundtruth.transaction_labels "
        f"(dataset_id, transaction_id, is_fraud, fraud_pattern) "
        f"VALUES ({dataset_id}, 'tx_bad', true, NULL)",
    )
    assert out.returncode != 0
    assert "label_pattern_consistency" in out.stderr
