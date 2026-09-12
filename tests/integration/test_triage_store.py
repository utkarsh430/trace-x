"""The triage write, against a real PostgreSQL.

Declared acceptance evidence for the triage half of `P2.idempotency`.

The previous integration module proved the *schema* enforces what it should.
This one proves the *store* uses it correctly: that `open_case` writes all four
rows or none, that a duplicate returns the existing case rather than raising or
duplicating, that the case moves through its state machine rather than having a
status written directly at it, and that a lease is exclusive.
"""

from __future__ import annotations

import datetime as dt
import os
import uuid
from collections.abc import Iterator
from typing import Any

import pytest

from trace_core.contracts.api.decision import RiskDecision
from trace_core.domain.enums import FeatureSource, RiskBand
from trace_core.domain.state_machines.case import CaseStatus
from trace_core.domain.time import event_time
from trace_core.repositories.postgres_triage import PostgresTriageStore
from trace_core.repositories.triage_event import (
    build_investigation_requested,
    outbox_row,
    producer_string,
)
from trace_core.rules.engine import Evaluation, RuleOutcome
from trace_core.rules.grammar import Truth

pytestmark = pytest.mark.integration

OCCURRED = dt.datetime(2026, 3, 1, 12, 0, 0, tzinfo=dt.UTC)
TRACE_ID = "0" * 32


def _dsn(user_env: str, password_env: str) -> str:
    return "postgresql://{u}:{p}@{h}:{port}/{db}".format(
        u=os.environ.get(user_env, ""),
        p=os.environ.get(password_env, ""),
        h=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5442"),
        db=os.environ.get("POSTGRES_DB", "tracex"),
    )


@pytest.fixture
def pool() -> Iterator[Any]:
    psycopg_pool = pytest.importorskip("psycopg_pool", reason="the `db` extra provides it")
    psycopg = pytest.importorskip("psycopg")
    try:
        with psycopg.connect(_dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD")) as probe:
            probe.execute("SELECT 1")
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no PostgreSQL reachable as trace_app ({exc}). "
            f"Run `make up`; the triage transaction is a database guarantee and is "
            f"never mocked (docs/TESTING.md §4)."
        )
    _truncate()
    created = psycopg_pool.ConnectionPool(
        _dsn("TRACE_APP_DB_USER", "TRACE_APP_DB_PASSWORD"), min_size=1, max_size=4, open=True
    )
    yield created
    created.close()
    _truncate()


def _truncate() -> None:
    """As the owner: `trace_app` has no DELETE, and granting it one so a fixture
    could tidy up would weaken a real control for test convenience."""
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(
        _dsn("POSTGRES_SUPERUSER", "POSTGRES_SUPERUSER_PASSWORD"), autocommit=True
    ) as owner:
        owner.execute(
            "TRUNCATE app.outbox, app.investigation_queue, app.case_transitions, app.cases"
        )


def _evaluation(*rule_ids: str, abstained: int = 0) -> Evaluation:
    outcomes = [
        RuleOutcome(
            rule_id=rid,
            rule_version="1.0.0",
            description="A rule fired during triage in this integration test.",
            result=Truth.TRUE,
            weight=0.8,
            band_floor=None,
            features_read={"account_tx_count_5m": 12.0},
        )
        for rid in rule_ids
    ]
    outcomes += [
        RuleOutcome(
            rule_id=f"R9{i:02d}_abstained",
            rule_version="1.0.0",
            description="A rule that could not reach a verdict in this test.",
            result=Truth.UNKNOWN,
            weight=0.5,
            band_floor=None,
            features_read={},
        )
        for i in range(abstained)
    ]
    return Evaluation(
        pack_id="core",
        pack_version="1.0.0",
        pack_digest="sha256:" + "a" * 64,
        outcomes=tuple(outcomes),
    )


def _decision(transaction_id: str, *, band: RiskBand = RiskBand.HIGH) -> RiskDecision:
    return RiskDecision(
        transaction_id=transaction_id,
        decision="FLAG",
        risk_band=band,
        score=0.8,
        reasons=[],
        feature_source=FeatureSource.ONLINE_ONLY,
        degraded=False,
        degraded_reasons=[],
        unavailable_features=[],
        insufficient_history_features=[],
        rule_pack_id="core",
        rule_pack_digest="sha256:" + "a" * 64,
        threshold_config_digest="sha256:" + "b" * 64,
        feature_set_version="1.0.0",
        case_id=None,
        scored_at=OCCURRED,
        latency_ms=4.0,
    )


def _open(store: PostgresTriageStore, transaction_id: str, **over: Any) -> Any:
    decision = over.pop("decision", None) or _decision(transaction_id)
    evaluation = over.pop("evaluation", None) or _evaluation("R005_velocity_spike_5m")
    case_id = over.pop("case_id", None) or f"case_{uuid.uuid4().hex}"
    event = build_investigation_requested(
        decision=decision,
        evaluation=evaluation,
        case_id=case_id,
        account_id="acct_000001",
        occurred_at=event_time(OCCURRED),
        producer=producer_string("0.1.0"),
        trace_id=TRACE_ID,
        correlation_id=f"corr_{transaction_id}",
    )
    topic, key, idem, payload = outbox_row(event)
    return store.open_case(
        decision=decision,
        account_id="acct_000001",
        occurred_at=OCCURRED,
        outbox_topic=topic,
        outbox_partition_key=key,
        outbox_idempotency_key=idem,
        outbox_payload=payload,
    )


# --- the write ----------------------------------------------------------------


def test_opening_a_case_writes_case_queue_transition_and_outbox(pool: Any) -> None:
    store = PostgresTriageStore(pool)
    result = _open(store, "tx_open_1")
    assert result.created

    with pool.connection() as conn:
        case = conn.execute(
            "SELECT status, risk_band, rule_pack_digest FROM app.cases WHERE case_id = %s",
            (result.case_id,),
        ).fetchone()
        assert case is not None
        # TRIAGED, not OPEN: the case went through the state machine, and the
        # queue entry exists, so the investigation is not silently stuck.
        assert case[0] == CaseStatus.TRIAGED.value
        assert (
            conn.execute(
                "SELECT count(*) FROM app.investigation_queue WHERE case_id = %s", (result.case_id,)
            ).fetchone()[0]
            == 1
        )
        assert (
            conn.execute(
                "SELECT count(*) FROM app.case_transitions WHERE case_id = %s", (result.case_id,)
            ).fetchone()[0]
            == 1
        )
        assert conn.execute("SELECT count(*) FROM app.outbox").fetchone()[0] == 1


def test_the_outbox_row_carries_a_publishable_event(pool: Any) -> None:
    """Validated against the released schema BEFORE the transaction opens, so a
    contract failure never rolls back a case for an unrelated reason."""
    import json

    store = PostgresTriageStore(pool)
    _open(store, "tx_outbox_1")
    with pool.connection() as conn:
        topic, key, payload = conn.execute(
            "SELECT topic, partition_key, payload FROM app.outbox"
        ).fetchone()
    assert topic == "investigation.requested.v1"
    assert key.startswith("case_"), "keyed by case_id, the entity that exists at produce time"
    event = payload if isinstance(payload, dict) else json.loads(payload)
    assert event["payload"]["risk_band"] == "HIGH"
    assert event["payload"]["fired_rule_ids"] == ["R005_velocity_spike_5m"]
    assert event["envelope"]["idempotency_key"].startswith("sha256:")


def test_a_duplicate_transaction_returns_the_existing_case(pool: Any) -> None:
    """The idempotent path: a successful replay, not a failure. Decided by a
    UNIQUE constraint rather than by a prior read, which would be a race."""
    store = PostgresTriageStore(pool)
    first = _open(store, "tx_dup")
    second = _open(store, "tx_dup")
    assert first.created and not second.created
    assert first.case_id == second.case_id
    with pool.connection() as conn:
        assert conn.execute("SELECT count(*) FROM app.cases").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM app.investigation_queue").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM app.outbox").fetchone()[0] == 1


def test_a_low_band_is_refused_by_the_contract_and_by_the_store(pool: Any) -> None:
    """Two independent refusals, and the outer one fires first.

    The released schema's `risk_band` enum admits only HIGH and CRITICAL, so a
    LOW-band event is unpublishable before the store is ever reached -- an
    invalid message is never published (EVENT_CONTRACTS §6.1). The store keeps
    its own guard anyway: it is reachable by a caller that builds an outbox row
    some other way, and "opening a case for every transaction is not triage"
    should not depend on a schema several layers out.
    """
    from trace_core.domain.errors import SchemaValidationError

    store = PostgresTriageStore(pool)

    # 1. the contract, at build time
    with pytest.raises(SchemaValidationError, match="risk_band"):
        _open(store, "tx_low", decision=_decision("tx_low", band=RiskBand.LOW))

    # 2. the store, called directly with an already-built row
    with pytest.raises(ValueError, match="does not open an investigation"):
        store.open_case(
            decision=_decision("tx_low", band=RiskBand.LOW),
            account_id="acct_000001",
            occurred_at=OCCURRED,
            outbox_topic="investigation.requested.v1",
            outbox_partition_key="case_" + "0" * 32,
            outbox_idempotency_key="sha256:" + "0" * 64,
            outbox_payload="{}",
        )

    with pool.connection() as conn:
        assert conn.execute("SELECT count(*) FROM app.cases").fetchone()[0] == 0


def test_a_critical_case_is_claimed_before_a_high_one(pool: Any) -> None:
    """A burst of HIGH cases must not delay a CRITICAL one behind it."""
    store = PostgresTriageStore(pool)
    _open(store, "tx_high", decision=_decision("tx_high", band=RiskBand.HIGH))
    critical = _open(
        store, "tx_critical", decision=_decision("tx_critical", band=RiskBand.CRITICAL)
    )
    leased = store.lease(worker_id="worker-1")
    assert leased is not None
    assert leased.case_id == critical.case_id


# --- leasing -------------------------------------------------------------------


def test_a_lease_moves_the_case_and_is_exclusive(pool: Any) -> None:
    store = PostgresTriageStore(pool)
    opened = _open(store, "tx_lease")
    leased = store.lease(worker_id="worker-1")
    assert leased is not None and leased.case_id == opened.case_id
    assert leased.attempts == 1

    with pool.connection() as conn:
        status = conn.execute(
            "SELECT status FROM app.cases WHERE case_id = %s", (opened.case_id,)
        ).fetchone()[0]
    assert status == CaseStatus.INVESTIGATING.value

    assert store.lease(worker_id="worker-2") is None, "a leased case was handed out twice"


def test_an_empty_queue_returns_none_rather_than_blocking(pool: Any) -> None:
    assert PostgresTriageStore(pool).lease(worker_id="worker-1") is None


def test_an_expired_lease_returns_the_case_to_triaged_not_open(pool: Any) -> None:
    """WORKER_LOST returns the case to TRIAGED: the checkpoint survives a worker
    crash, so re-triaging would discard completed work and re-spend its budget
    (ADR-0007, ARCHITECTURE §19.1)."""
    store = PostgresTriageStore(pool)
    opened = _open(store, "tx_expire")
    leased = store.lease(worker_id="worker-1")
    assert leased is not None

    with pool.connection() as conn:
        conn.execute(
            "UPDATE app.investigation_queue SET lease_expires_at = now() - interval '1 minute' "
            "WHERE case_id = %s",
            (opened.case_id,),
        )
    assert store.release_expired_leases() == 1

    with pool.connection() as conn:
        status, leased_by = conn.execute(
            """
            SELECT c.status, q.leased_by FROM app.cases c
            JOIN app.investigation_queue q ON q.case_id = c.case_id
            WHERE c.case_id = %s
            """,
            (opened.case_id,),
        ).fetchone()
    assert status == CaseStatus.TRIAGED.value
    assert leased_by is None
    assert store.lease(worker_id="worker-2") is not None


def test_a_live_lease_is_not_reclaimed(pool: Any) -> None:
    """Reclaiming under a working worker would put two workers on one
    investigation -- which is why the lease outlasts the investigation budget."""
    store = PostgresTriageStore(pool)
    _open(store, "tx_live")
    assert store.lease(worker_id="worker-1") is not None
    assert store.release_expired_leases() == 0


def test_the_transition_history_records_every_move(pool: Any) -> None:
    """A silently-dropped transition produces a plausible-looking case history,
    which is far harder to find than a stack trace (ADR-0027)."""
    store = PostgresTriageStore(pool)
    opened = _open(store, "tx_history")
    store.lease(worker_id="worker-1")
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT from_status, to_status, event FROM app.case_transitions "
            "WHERE case_id = %s ORDER BY transition_id",
            (opened.case_id,),
        ).fetchall()
    assert [(r[0], r[1], r[2]) for r in rows] == [
        ("OPEN", "TRIAGED", "TRIAGE"),
        ("TRIAGED", "INVESTIGATING", "LEASE"),
    ]
