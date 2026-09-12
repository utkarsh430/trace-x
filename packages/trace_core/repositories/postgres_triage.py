"""Triage persistence: the case, its queue entry and its outbox row (ADR-0007).

**The atomicity is in the API, not in the caller.** ADR-0007's requirement — the
case row and the queue row move in one transaction — could be met by three
repositories and a caller that remembers to wrap them. It would be met right up
until someone added a fourth write, or handled an exception one level too high.
So `open_case` performs the whole write itself and there is no way to write a
case without its queue entry. A case marked triaged whose queue entry was never
written is a silently stuck investigation, and stuck is the failure mode nobody
notices.

**The duplicate check is `ON CONFLICT DO NOTHING`, not a prior `SELECT`.** Reading
first and inserting second is a race with a window exactly as wide as the round
trip, and a retrying payment processor is precisely the traffic that finds it.
The database decides, once, and tells us which of the two things happened.

**The outbox exists because publishing is not transactional.** A broker write
inside a database transaction commits independently of it: publish before commit
and you announce a case that may never exist; publish after and a crash loses it.
The row is committed with the state it describes, and Phase 3's relay drains it.
"""

from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from trace_core.contracts.api.decision import RiskDecision
from trace_core.domain.enums import RiskBand
from trace_core.domain.state_machines.case import CASE, CaseEvent, CaseStatus

if TYPE_CHECKING:  # pragma: no cover - typing only
    from psycopg_pool import ConnectionPool

LEASE_SECONDS: Final = 300
"""How long a worker holds a claimed case before the lease expires.

Longer than the 120-second investigation budget (`TRACE_MAX_WALLCLOCK_S`) with
room for start-up, so a lease never expires under a worker that is still
working -- which would put two workers on one investigation. Phase 6 owns the
consuming side; the value lives here because the queue enforces it.
"""

ACTOR_SYSTEM: Final = "SYSTEM"


def new_case_id() -> str:
    """`case_<32 hex>`, matching the `RiskDecision.case_id` pattern."""
    return f"case_{uuid.uuid4().hex}"


@dataclass(frozen=True, slots=True)
class TriageResult:
    """What `open_case` did.

    `created` distinguishes "this call opened the case" from "a case already
    existed for this transaction" -- which the caller needs, because the second
    is a successful idempotent replay rather than a failure, and the two must be
    counted separately or a retry storm looks like fraud volume.
    """

    case_id: str
    created: bool


@dataclass(frozen=True, slots=True)
class LeasedCase:
    """A queue entry claimed by a worker."""

    queue_id: int
    case_id: str
    attempts: int


class PostgresTriageStore:
    """The `app` schema's triage surface, as `trace_app`."""

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    # -- the one write that matters -----------------------------------------

    def open_case(
        self,
        *,
        decision: RiskDecision,
        account_id: str,
        occurred_at: dt.datetime,
        outbox_topic: str,
        outbox_partition_key: str,
        outbox_idempotency_key: str,
        outbox_payload: str,
    ) -> TriageResult:
        """Open a case, enqueue it and record its event, atomically.

        Returns the existing case when one is already open for this transaction,
        without writing anything. That is the idempotent path, and it is decided
        by a UNIQUE constraint rather than by a prior read.
        """
        if not _opens_investigation(decision.risk_band):
            raise ValueError(
                f"{decision.risk_band} does not open an investigation; triage must not be "
                f"called for it. Opening a case for every transaction is not triage."
            )
        case_id = new_case_id()
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute(
                """
                INSERT INTO app.cases (
                    case_id, trigger_transaction_id, account_id, status, risk_band, score,
                    rule_pack_id, rule_pack_digest, threshold_config_digest,
                    feature_set_version, feature_source, degraded, occurred_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trigger_transaction_id) DO NOTHING
                RETURNING case_id
                """,
                (
                    case_id,
                    decision.transaction_id,
                    account_id,
                    CaseStatus.OPEN.value,
                    decision.risk_band.value,
                    decision.score,
                    decision.rule_pack_id,
                    decision.rule_pack_digest,
                    decision.threshold_config_digest,
                    decision.feature_set_version,
                    decision.feature_source.value,
                    decision.degraded,
                    occurred_at,
                ),
            ).fetchone()

            if row is None:
                # Someone else opened it -- possibly in the microsecond between
                # our read and our write, which is exactly why there was no read.
                existing = conn.execute(
                    "SELECT case_id FROM app.cases WHERE trigger_transaction_id = %s",
                    (decision.transaction_id,),
                ).fetchone()
                assert existing is not None, "ON CONFLICT fired but no row exists"
                return TriageResult(case_id=str(existing[0]), created=False)

            # OPEN -> TRIAGED, through the state machine rather than by writing
            # the target status directly: an illegal transition must raise here
            # rather than produce a plausible-looking case history (ADR-0027).
            triaged = CASE.transition(CaseStatus.OPEN, CaseEvent.TRIAGE)
            conn.execute(
                "UPDATE app.cases SET status = %s, updated_at = now() WHERE case_id = %s",
                (triaged.value, case_id),
            )
            conn.execute(
                """
                INSERT INTO app.case_transitions (case_id, from_status, to_status, event, actor)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    case_id,
                    CaseStatus.OPEN.value,
                    triaged.value,
                    CaseEvent.TRIAGE.value,
                    ACTOR_SYSTEM,
                ),
            )
            conn.execute(
                "INSERT INTO app.investigation_queue (case_id, priority) VALUES (%s, %s)",
                (case_id, _priority(decision.risk_band)),
            )
            conn.execute(
                """
                INSERT INTO app.outbox (topic, partition_key, idempotency_key, payload)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (topic, idempotency_key) DO NOTHING
                """,
                (outbox_topic, outbox_partition_key, outbox_idempotency_key, outbox_payload),
            )
        return TriageResult(case_id=case_id, created=True)

    # -- reads ---------------------------------------------------------------

    def case_for_transaction(self, transaction_id: str) -> str | None:
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT case_id FROM app.cases WHERE trigger_transaction_id = %s",
                (transaction_id,),
            ).fetchone()
        return None if row is None else str(row[0])

    def pending_outbox_count(self) -> int:
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT count(*) FROM app.outbox WHERE published_at IS NULL"
            ).fetchone()
        return 0 if row is None else int(row[0])

    # -- leasing (ADR-0007); the consuming worker is Phase 6 -----------------

    def lease(self, *, worker_id: str, lease_seconds: int = LEASE_SECONDS) -> LeasedCase | None:
        """Claim one case, or return None.

        `SKIP LOCKED`, not `FOR UPDATE`: a worker with no work must return
        immediately rather than queue behind another worker's row.
        """
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute(
                """
                SELECT queue_id, case_id, attempts
                FROM app.investigation_queue
                WHERE leased_by IS NULL AND available_at <= now()
                ORDER BY priority DESC, available_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            queue_id, case_id, attempts = int(row[0]), str(row[1]), int(row[2])
            conn.execute(
                """
                UPDATE app.investigation_queue
                SET leased_by = %s,
                    lease_expires_at = now() + make_interval(secs => %s),
                    attempts = attempts + 1
                WHERE queue_id = %s
                """,
                (worker_id, lease_seconds, queue_id),
            )
            self._advance(conn, case_id, CaseEvent.LEASE, actor=worker_id)
        return LeasedCase(queue_id=queue_id, case_id=case_id, attempts=attempts + 1)

    def release_expired_leases(self) -> int:
        """Return expired leases to the queue. Returns how many were reclaimed.

        The case goes back to TRIAGED, never to OPEN: the LangGraph checkpoint
        survives a worker crash, so re-triaging would discard completed work and
        re-spend its budget (ADR-0007, ARCHITECTURE §19.1).
        """
        with self._pool.connection() as conn, conn.transaction():
            rows = conn.execute(
                """
                SELECT queue_id, case_id FROM app.investigation_queue
                WHERE leased_by IS NOT NULL AND lease_expires_at <= now()
                FOR UPDATE SKIP LOCKED
                """
            ).fetchall()
            for queue_id, case_id in rows:
                conn.execute(
                    """
                    UPDATE app.investigation_queue
                    SET leased_by = NULL, lease_expires_at = NULL, available_at = now()
                    WHERE queue_id = %s
                    """,
                    (queue_id,),
                )
                self._advance(conn, str(case_id), CaseEvent.WORKER_LOST, actor=ACTOR_SYSTEM)
        return len(rows)

    # -- state transitions ---------------------------------------------------

    def _advance(self, conn: Any, case_id: str, event: CaseEvent, *, actor: str) -> CaseStatus:
        """Move a case through the state machine, recording the transition.

        The machine decides the target status; an illegal transition raises
        `IllegalTransitionError` and rolls the surrounding transaction back. A
        silently-dropped transition produces a plausible-looking case history,
        which is far harder to find than a stack trace (ADR-0027).
        """
        row = conn.execute(
            "SELECT status FROM app.cases WHERE case_id = %s FOR UPDATE", (case_id,)
        ).fetchone()
        if row is None:
            raise LookupError(f"no case {case_id!r}")
        current = CaseStatus(str(row[0]))
        target = CASE.transition(current, event)
        conn.execute(
            "UPDATE app.cases SET status = %s, updated_at = now() WHERE case_id = %s",
            (target.value, case_id),
        )
        conn.execute(
            """
            INSERT INTO app.case_transitions (case_id, from_status, to_status, event, actor)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (case_id, current.value, target.value, event.value, actor),
        )
        return target


def _opens_investigation(band: RiskBand) -> bool:
    return band in {RiskBand.HIGH, RiskBand.CRITICAL}


def _priority(band: RiskBand) -> int:
    """CRITICAL is claimed before HIGH.

    The queue is FIFO within a priority, so a burst of HIGH cases cannot delay a
    CRITICAL one behind it.
    """
    return 1 if band is RiskBand.CRITICAL else 0
