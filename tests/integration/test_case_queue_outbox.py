"""The triage tables, against a real PostgreSQL.

Three things are checked here that cannot be checked anywhere else, because all
three are database guarantees rather than application behaviour:

1. **`trigger_transaction_id` is UNIQUE**, so "duplicate transaction_id yields
   one effect" (docs/SECURITY.md §9) holds under genuine concurrency, not just
   when the application remembers to look first.
2. **`SELECT … FOR UPDATE SKIP LOCKED` hands one row to one worker** (ADR-0007).
   Two connections claiming simultaneously must not both get the same case.
3. **The new tables did not widen `trace_app`'s reach.** Adding tables to `app`
   must leave the `groundtruth` denial untouched -- the Phase 0 control is a
   release blocker, and every migration is an opportunity to weaken it by
   accident.
"""

from __future__ import annotations

import datetime as dt
import os
import uuid
from collections.abc import Iterator
from typing import Any

import pytest

pytestmark = pytest.mark.integration

DSN_TEMPLATE = "postgresql://{user}:{password}@{host}:{port}/{db}"


def _dsn(user_env: str, password_env: str) -> str:
    return DSN_TEMPLATE.format(
        user=os.environ.get(user_env, ""),
        password=os.environ.get(password_env, ""),
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5442"),
        db=os.environ.get("POSTGRES_DB", "tracex"),
    )


def _truncate() -> None:
    """Reset the triage tables between tests, AS THE OWNER.

    Deliberately not as `trace_app`: that role has no DELETE grant, which
    `test_trace_app_cannot_delete_a_case` asserts. Granting it one so a fixture
    could tidy up would weaken a real control for test convenience -- so cleanup
    connects as the migration owner instead, which is what an administrative
    operation should use anyway.
    """
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(
        _dsn("POSTGRES_SUPERUSER", "POSTGRES_SUPERUSER_PASSWORD"), autocommit=True
    ) as owner:
        owner.execute(
            "TRUNCATE app.outbox, app.investigation_queue, app.case_transitions, app.cases"
        )


@pytest.fixture
def app_conn() -> Iterator[Any]:
    psycopg = pytest.importorskip("psycopg", reason="the `db` extra provides psycopg")
    dsn = _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD")
    try:
        conn = psycopg.connect(dsn, autocommit=True)
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no PostgreSQL reachable as trace_app ({exc}). "
            f"Run `make up`; these are database guarantees and are never mocked "
            f"(docs/TESTING.md §4)."
        )
    _truncate()
    yield conn
    conn.close()
    _truncate()


def _case_id() -> str:
    return f"case_{uuid.uuid4().hex}"


def _insert_case(conn: Any, *, transaction_id: str, case_id: str | None = None) -> str:
    case_id = case_id or _case_id()
    conn.execute(
        """
        INSERT INTO app.cases (
            case_id, trigger_transaction_id, account_id, status, risk_band, score,
            rule_pack_id, rule_pack_digest, threshold_config_digest,
            feature_set_version, feature_source, degraded, occurred_at
        ) VALUES (%s, %s, %s, 'OPEN', 'HIGH', 0.7, 'core', %s, %s, '1.0.0',
                  'ONLINE_ONLY', false, %s)
        """,
        (
            case_id,
            transaction_id,
            "acct_000001",
            "sha256:" + "a" * 64,
            "sha256:" + "b" * 64,
            dt.datetime(2026, 3, 1, 12, tzinfo=dt.UTC),
        ),
    )
    return case_id


# --- idempotency is a database guarantee -------------------------------------


def test_a_duplicate_trigger_transaction_is_refused_by_the_database(app_conn: Any) -> None:
    """Not by an application check, and not by a cache -- Redis may be down.

    The same reasoning that makes re-seeding a dataset version impossible in
    migration 0002: the constraint holds regardless of which writer runs.
    """
    psycopg = pytest.importorskip("psycopg")
    _insert_case(app_conn, transaction_id="tx_duplicate")
    with pytest.raises(psycopg.errors.UniqueViolation):
        _insert_case(app_conn, transaction_id="tx_duplicate")


def test_concurrent_duplicates_produce_exactly_one_case(app_conn: Any) -> None:
    """The real shape of the risk: two gateway workers scoring a retried
    transaction at the same instant."""
    psycopg = pytest.importorskip("psycopg")
    dsn = _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD")
    connections = [psycopg.connect(dsn, autocommit=True) for _ in range(4)]
    try:
        for conn in connections:
            conn.execute(
                """
                    INSERT INTO app.cases (
                        case_id, trigger_transaction_id, account_id, status, risk_band,
                        score, rule_pack_id, rule_pack_digest, threshold_config_digest,
                        feature_set_version, feature_source, degraded, occurred_at
                    ) VALUES (%s, 'tx_concurrent', 'acct_000001', 'OPEN', 'HIGH', 0.7,
                              'core', %s, %s, '1.0.0', 'ONLINE_ONLY', false, now())
                    ON CONFLICT (trigger_transaction_id) DO NOTHING
                    """,
                (_case_id(), "sha256:" + "a" * 64, "sha256:" + "b" * 64),
            )
    finally:
        for conn in connections:
            conn.close()
    count = app_conn.execute(
        "SELECT count(*) FROM app.cases WHERE trigger_transaction_id = 'tx_concurrent'"
    ).fetchone()[0]
    assert count == 1, f"{count} cases created for one transaction"


def test_one_queue_entry_per_case(app_conn: Any) -> None:
    """Without UNIQUE, a retry enqueues a second entry and two workers spend the
    same investigation's budget."""
    psycopg = pytest.importorskip("psycopg")
    case_id = _insert_case(app_conn, transaction_id="tx_queue")
    app_conn.execute("INSERT INTO app.investigation_queue (case_id) VALUES (%s)", (case_id,))
    with pytest.raises(psycopg.errors.UniqueViolation):
        app_conn.execute("INSERT INTO app.investigation_queue (case_id) VALUES (%s)", (case_id,))


# --- the whole triage write is one transaction --------------------------------


def test_case_queue_and_outbox_commit_together(app_conn: Any) -> None:
    """ADR-0007's requirement, exercised: a failure anywhere leaves nothing."""
    psycopg = pytest.importorskip("psycopg")
    dsn = _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD")
    conn = psycopg.connect(dsn, autocommit=False)
    try:
        case_id = _case_id()
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO app.cases (
                    case_id, trigger_transaction_id, account_id, status, risk_band, score,
                    rule_pack_id, rule_pack_digest, threshold_config_digest,
                    feature_set_version, feature_source, degraded, occurred_at
                ) VALUES (%s, 'tx_atomic', 'acct_000001', 'OPEN', 'HIGH', 0.7, 'core',
                          %s, %s, '1.0.0', 'ONLINE_ONLY', false, now())
                """,
                (case_id, "sha256:" + "a" * 64, "sha256:" + "b" * 64),
            )
            conn.execute("INSERT INTO app.investigation_queue (case_id) VALUES (%s)", (case_id,))
            conn.execute(
                """
                INSERT INTO app.outbox (topic, partition_key, idempotency_key, payload)
                VALUES ('investigation.requested.v1', 'acct_000001', %s, %s)
                """,
                ("sha256:" + "c" * 64, "{}"),
            )
    finally:
        conn.close()

    assert (
        app_conn.execute(
            "SELECT count(*) FROM app.cases WHERE trigger_transaction_id = 'tx_atomic'"
        ).fetchone()[0]
        == 1
    )
    assert app_conn.execute("SELECT count(*) FROM app.outbox").fetchone()[0] == 1


def test_a_failed_triage_leaves_no_partial_state(app_conn: Any) -> None:
    """The failure that ADR-0007 names: a case whose queue entry was never
    written is a silently stuck investigation."""
    psycopg = pytest.importorskip("psycopg")
    dsn = _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD")
    conn = psycopg.connect(dsn, autocommit=False)
    try:
        with pytest.raises(psycopg.errors.ForeignKeyViolation), conn.transaction():
            conn.execute(
                """
                INSERT INTO app.cases (
                    case_id, trigger_transaction_id, account_id, status, risk_band, score,
                    rule_pack_id, rule_pack_digest, threshold_config_digest,
                    feature_set_version, feature_source, degraded, occurred_at
                ) VALUES (%s, 'tx_rollback', 'acct_000001', 'OPEN', 'HIGH', 0.7, 'core',
                          %s, %s, '1.0.0', 'ONLINE_ONLY', false, now())
                """,
                (_case_id(), "sha256:" + "a" * 64, "sha256:" + "b" * 64),
            )
            # A queue row for a case that does not exist: the FK rejects it, and
            # the case inserted a moment ago must go with it.
            conn.execute("INSERT INTO app.investigation_queue (case_id) VALUES ('case_missing')")
    finally:
        conn.close()
    assert (
        app_conn.execute(
            "SELECT count(*) FROM app.cases WHERE trigger_transaction_id = 'tx_rollback'"
        ).fetchone()[0]
        == 0
    )


# --- SKIP LOCKED leasing (ADR-0007) -------------------------------------------


def test_skip_locked_gives_one_row_to_one_worker(app_conn: Any) -> None:
    """Two workers claiming at the same instant must get different cases.

    The consuming worker is Phase 6, but the queue's correctness is a Phase 2
    deliverable and is provable without it.
    """
    psycopg = pytest.importorskip("psycopg")
    dsn = _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD")
    for i in range(2):
        case_id = _insert_case(app_conn, transaction_id=f"tx_lease_{i}")
        app_conn.execute("INSERT INTO app.investigation_queue (case_id) VALUES (%s)", (case_id,))

    claim = """
        SELECT queue_id, case_id FROM app.investigation_queue
        WHERE leased_by IS NULL AND available_at <= now()
        ORDER BY priority DESC, available_at
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    """
    first = psycopg.connect(dsn, autocommit=False)
    second = psycopg.connect(dsn, autocommit=False)
    try:
        claimed_first = first.execute(claim).fetchone()
        claimed_second = second.execute(claim).fetchone()
        assert claimed_first is not None and claimed_second is not None
        assert claimed_first[0] != claimed_second[0], (
            "two workers claimed the same queue row; the investigation's budget "
            "would be spent twice"
        )
    finally:
        first.rollback()
        second.rollback()
        first.close()
        second.close()


def test_a_third_worker_gets_nothing_rather_than_blocking(app_conn: Any) -> None:
    """SKIP LOCKED, not FOR UPDATE: a worker with no work must return, not wait."""
    psycopg = pytest.importorskip("psycopg")
    dsn = _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD")
    case_id = _insert_case(app_conn, transaction_id="tx_only_one")
    app_conn.execute("INSERT INTO app.investigation_queue (case_id) VALUES (%s)", (case_id,))

    claim = """
        SELECT queue_id FROM app.investigation_queue
        WHERE leased_by IS NULL
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    """
    holder = psycopg.connect(dsn, autocommit=False)
    other = psycopg.connect(dsn, autocommit=False)
    try:
        assert holder.execute(claim).fetchone() is not None
        assert other.execute(claim).fetchone() is None
    finally:
        holder.rollback()
        other.rollback()
        holder.close()
        other.close()


# --- the outbox ----------------------------------------------------------------


def test_the_same_event_cannot_be_enqueued_twice(app_conn: Any) -> None:
    """The envelope's content hash is the key, so a retry of the same semantic
    event produces one row rather than a duplicate a consumer must dedupe."""
    psycopg = pytest.importorskip("psycopg")
    key = "sha256:" + "d" * 64
    for _ in range(1):
        app_conn.execute(
            """
            INSERT INTO app.outbox (topic, partition_key, idempotency_key, payload)
            VALUES ('investigation.requested.v1', 'acct_000001', %s, '{}')
            """,
            (key,),
        )
    with pytest.raises(psycopg.errors.UniqueViolation):
        app_conn.execute(
            """
            INSERT INTO app.outbox (topic, partition_key, idempotency_key, payload)
            VALUES ('investigation.requested.v1', 'acct_000001', %s, '{}')
            """,
            (key,),
        )


# --- the control every migration could weaken ----------------------------------


def test_the_new_tables_did_not_widen_trace_app(app_conn: Any) -> None:
    """Adding tables to `app` must leave the `groundtruth` denial untouched.

    The Phase 0 isolation test is a release blocker; this asserts the same thing
    from the migration that most plausibly could have broken it, because a grant
    added for convenience here would be invisible until a metric was already
    invalid (ADR-0004, CLAUDE.md §11).
    """
    psycopg = pytest.importorskip("psycopg")
    with pytest.raises(psycopg.errors.InsufficientPrivilege) as exc:
        app_conn.execute("SELECT 1 FROM groundtruth.transaction_labels LIMIT 1")
    assert "permission denied for schema groundtruth" in str(exc.value)


def test_trace_app_cannot_delete_a_case(app_conn: Any) -> None:
    """Cases are the audit surface an analyst sees. The application may open and
    advance them; removing history is not an application operation."""
    psycopg = pytest.importorskip("psycopg")
    case_id = _insert_case(app_conn, transaction_id="tx_nodelete")
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        app_conn.execute("DELETE FROM app.cases WHERE case_id = %s", (case_id,))
