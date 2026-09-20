"""The online-store hole ledger, against a real PostgreSQL (ADR-0046 §5).

The guard's state machine is unit-tested with fakes; this proves the durable half: that a hole
survives a process, that the database refuses what the ledger must never hold, and that `trace_app`
can clear a hole but never erase the record that it happened.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

import pytest

from trace_core.domain.time import EventTime
from trace_core.features.completeness import CompletenessGuard, HoleReason
from trace_core.repositories.postgres_completeness import PostgresHoleLedger

pytestmark = pytest.mark.integration


def _dsn(user_env: str, password_env: str) -> str:
    return "postgresql://{u}:{p}@{h}:{port}/{db}".format(
        u=os.environ.get(user_env, ""),
        p=os.environ.get(password_env, ""),
        h=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5442"),
        db=os.environ.get("POSTGRES_DB", "tracex"),
    )


def _truncate() -> None:
    """As the owner: `trace_app` deliberately has no DELETE on the ledger."""
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(
        _dsn("POSTGRES_SUPERUSER", "POSTGRES_SUPERUSER_PASSWORD"), autocommit=True
    ) as owner:
        owner.execute("TRUNCATE app.feature_store_holes RESTART IDENTITY")


@pytest.fixture
def pool() -> Iterator[Any]:
    psycopg_pool = pytest.importorskip("psycopg_pool", reason="the `db` extra provides it")
    psycopg = pytest.importorskip("psycopg")
    try:
        with psycopg.connect(_dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD")) as probe:
            probe.execute("SELECT 1 FROM app.feature_store_holes LIMIT 1")
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no migrated PostgreSQL reachable as trace_app ({exc}). "
            f"Run `make up && make migrate`; the ledger is a database guarantee and is never "
            f"mocked (docs/TESTING.md §4)."
        )
    _truncate()
    created = psycopg_pool.ConnectionPool(
        _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD"), min_size=1, max_size=2, open=True
    )
    yield created
    created.close()
    _truncate()


class _Store:
    def __init__(self) -> None:
        self.withdrawals: list[EventTime] = []

    def withdraw_completeness(self, *, resume_at: EventTime) -> None:
        self.withdrawals.append(resume_at)


def test_a_hole_is_recorded_counted_and_cleared_but_kept(pool: Any) -> None:
    ledger = PostgresHoleLedger(pool)
    assert ledger.open_holes() == 0
    ledger.record_hole(reason=HoleReason.UNREACHABLE, instance_id="gw-test")
    assert ledger.open_holes() == 1
    through = ledger.latest_open_hole()
    assert through is not None
    ledger.record_hole(reason=HoleReason.BREAKER_OPEN, instance_id="gw-later")
    assert ledger.clear_holes(through_hole_id=through) == 1, "a later hole is not cleared"
    assert ledger.open_holes() == 1
    assert ledger.clear_holes(through_hole_id=through + 1) == 1
    assert ledger.open_holes() == 0
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT reason, instance_id, cleared_at IS NOT NULL FROM app.feature_store_holes"
        ).fetchall()
    assert rows == [("UNREACHABLE", "gw-test", True), ("BREAKER_OPEN", "gw-later", True)], (
        "a cleared hole stays on the record"
    )


def test_trace_app_cannot_erase_a_hole(pool: Any) -> None:
    psycopg = pytest.importorskip("psycopg")
    PostgresHoleLedger(pool).record_hole(reason=HoleReason.REFUSED, instance_id="gw-test")
    with pytest.raises(psycopg.errors.InsufficientPrivilege), pool.connection() as conn:
        conn.execute("DELETE FROM app.feature_store_holes")


def test_the_database_refuses_an_undeclared_reason(pool: Any) -> None:
    psycopg = pytest.importorskip("psycopg")
    with pytest.raises(psycopg.errors.CheckViolation), pool.connection() as conn:
        conn.execute(
            "INSERT INTO app.feature_store_holes (reason, instance_id) VALUES ('MAYBE', 'gw')"
        )


def test_a_restarted_process_inherits_a_hole_from_postgres(pool: Any) -> None:
    store = _Store()
    CompletenessGuard(store, PostgresHoleLedger(pool), instance_id="gw-a").observation_unrecorded(
        HoleReason.UNREACHABLE
    )
    restarted = CompletenessGuard(store, PostgresHoleLedger(pool), instance_id="gw-b")
    restarted.resume()
    assert restarted.pending
    assert restarted.reconcile()
    assert len(store.withdrawals) == 1
    assert PostgresHoleLedger(pool).open_holes() == 0


def test_trace_app_cannot_rewrite_or_unclear_a_hole(pool: Any) -> None:
    psycopg = pytest.importorskip("psycopg")
    ledger = PostgresHoleLedger(pool)
    ledger.record_hole(reason=HoleReason.UNREACHABLE, instance_id="gw-test")
    with pytest.raises(psycopg.errors.InsufficientPrivilege), pool.connection() as conn:
        conn.execute("UPDATE app.feature_store_holes SET reason = 'REFUSED'")
    through = ledger.latest_open_hole()
    assert through is not None
    ledger.clear_holes(through_hole_id=through)
    with pytest.raises(psycopg.errors.RaiseException), pool.connection() as conn:
        conn.execute("UPDATE app.feature_store_holes SET cleared_at = NULL")
    with pytest.raises(psycopg.errors.RaiseException), pool.connection() as conn:
        conn.execute("UPDATE app.feature_store_holes SET cleared_at = now()")


def test_trace_app_records_a_hole_only_by_its_reason_and_instance(pool: Any) -> None:
    """Its id, opening time and clearing are the database's: an already-cleared or back-dated hole
    cannot be inserted by the process it describes."""
    psycopg = pytest.importorskip("psycopg")
    for statement in (
        "INSERT INTO app.feature_store_holes (reason, instance_id, cleared_at) "
        "VALUES ('UNREACHABLE', 'gw', now())",
        "INSERT INTO app.feature_store_holes (reason, instance_id, opened_at) "
        "VALUES ('UNREACHABLE', 'gw', now() - interval '1 day')",
        "INSERT INTO app.feature_store_holes (hole_id, reason, instance_id) "
        "VALUES (999999, 'UNREACHABLE', 'gw')",
    ):
        with pytest.raises(psycopg.errors.InsufficientPrivilege), pool.connection() as conn:
            conn.execute(statement)
