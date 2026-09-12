"""Case lifecycle — a fraud case from triage to closure.

**This machine is newly authored.** `docs/ROADMAP.md` pointed at
`ARCHITECTURE.md` §13 for it, but §13 is Observability and §12 is Action
execution; no case lifecycle was specified anywhere in the control plane. Rather
than invent one, every state and edge below is derived from an existing
authority, cited inline. ADR-0027 records the decision and ARCHITECTURE.md §19
now carries the specification.

The case is the unit an analyst sees and the unit the audit chain is keyed on,
so the states are the ones a human needs to distinguish when answering "what
happened to this case, and who decided?" — not the internal steps of the
investigation, which are `InvestigationState`'s job.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from trace_core.domain.state_machines.machine import StateMachine


class CaseStatus(StrEnum):
    OPEN = "OPEN"
    """Created by triage from a HIGH or CRITICAL score (ARCHITECTURE.md §3)."""

    TRIAGED = "TRIAGED"
    """On the durable work queue, not yet leased (ADR-0007)."""

    INVESTIGATING = "INVESTIGATING"
    """Leased by a worker. A worker crash returns the case here, not to OPEN,
    because LangGraph resumes from a checkpoint rather than from scratch
    (ARCHITECTURE.md §18)."""

    PENDING_DECISION = "PENDING_DECISION"
    """Evidence gathering finished or the budget ran out; the Decision agent
    has not yet recorded a verdict."""

    DECIDED = "DECIDED"
    """A verdict is recorded with cited evidence_ids (CLAUDE.md §10.5)."""

    HUMAN_REVIEW = "HUMAN_REVIEW"
    """INSUFFICIENT_EVIDENCE went to the human queue. A valid recorded outcome,
    never a failure (CLAUDE.md §10.4)."""

    PENDING_APPROVAL = "PENDING_APPROVAL"
    """A MEDIUM or HIGH risk action awaits a human (SECURITY.md §7)."""

    EXECUTING = "EXECUTING"
    """A ValidatedAction is with the idempotent executor. Distinct from
    ACTIONED because execution and verification can fail, and conflating the
    two would make 'approved' indistinguishable from 'actually happened'."""

    ACTIONED = "ACTIONED"
    """Executed and verified by read-back (ARCHITECTURE.md §12)."""

    COMPENSATING = "COMPENSATING"
    """Verification mismatched; the compensating action is running."""

    COMPENSATED = "COMPENSATED"
    """Rolled back successfully (SECURITY.md §7)."""

    ESCALATED = "ESCALATED"
    """Needs a human now: compensation failed, execution failed, or an approval
    expired (SECURITY.md §7: approvals expire to ESCALATED rather than
    lingering)."""

    REJECTED = "REJECTED"
    """The action was refused — by an approver, or by policy as PROHIBITED."""

    CLOSED = "CLOSED"
    """Terminal. Every case ends here, so 'still open' is always answerable."""


class CaseEvent(StrEnum):
    TRIAGE = "TRIAGE"
    LEASE = "LEASE"
    WORKER_LOST = "WORKER_LOST"
    EVIDENCE_COMPLETE = "EVIDENCE_COMPLETE"
    BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
    RECORD_VERDICT = "RECORD_VERDICT"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    ANALYST_DECIDE = "ANALYST_DECIDE"
    DISMISS = "DISMISS"
    NO_ACTION_REQUIRED = "NO_ACTION_REQUIRED"
    PROPOSE_ACTION = "PROPOSE_ACTION"
    AUTO_EXECUTE = "AUTO_EXECUTE"
    POLICY_PROHIBITED = "POLICY_PROHIBITED"
    APPROVE = "APPROVE"
    REJECT = "REJECT"
    APPROVAL_EXPIRED = "APPROVAL_EXPIRED"
    VERIFY_OK = "VERIFY_OK"
    VERIFY_MISMATCH = "VERIFY_MISMATCH"
    EXECUTION_FAILED = "EXECUTION_FAILED"
    COMPENSATE_OK = "COMPENSATE_OK"
    COMPENSATE_FAILED = "COMPENSATE_FAILED"
    CLOSE = "CLOSE"


_S = CaseStatus
_E = CaseEvent

CASE: Final = StateMachine[CaseStatus, CaseEvent](
    name="case",
    initial=_S.OPEN,
    terminal=frozenset({_S.CLOSED}),
    transitions={
        _S.OPEN: {_E.TRIAGE: _S.TRIAGED},
        _S.TRIAGED: {_E.LEASE: _S.INVESTIGATING},
        _S.INVESTIGATING: {
            _E.EVIDENCE_COMPLETE: _S.PENDING_DECISION,
            _E.BUDGET_EXHAUSTED: _S.PENDING_DECISION,
            # A lost lease returns the case to the queue. It does NOT go to
            # OPEN: the investigation checkpoint survives, so re-triaging would
            # discard completed work and re-spend its budget (ADR-0007).
            _E.WORKER_LOST: _S.TRIAGED,
        },
        _S.PENDING_DECISION: {
            _E.RECORD_VERDICT: _S.DECIDED,
            _E.INSUFFICIENT_EVIDENCE: _S.HUMAN_REVIEW,
        },
        _S.HUMAN_REVIEW: {
            _E.ANALYST_DECIDE: _S.DECIDED,
            _E.DISMISS: _S.CLOSED,
        },
        _S.DECIDED: {
            # Risk classification decides which of these fires (SECURITY.md §7).
            _E.AUTO_EXECUTE: _S.EXECUTING,  # LOW
            _E.PROPOSE_ACTION: _S.PENDING_APPROVAL,  # MEDIUM / HIGH
            _E.POLICY_PROHIBITED: _S.REJECTED,  # PROHIBITED
            _E.NO_ACTION_REQUIRED: _S.CLOSED,  # verdict needs no side effect
        },
        _S.PENDING_APPROVAL: {
            _E.APPROVE: _S.EXECUTING,
            _E.REJECT: _S.REJECTED,
            _E.APPROVAL_EXPIRED: _S.ESCALATED,
        },
        _S.EXECUTING: {
            _E.VERIFY_OK: _S.ACTIONED,
            _E.VERIFY_MISMATCH: _S.COMPENSATING,
            _E.EXECUTION_FAILED: _S.ESCALATED,
        },
        _S.COMPENSATING: {
            _E.COMPENSATE_OK: _S.COMPENSATED,
            _E.COMPENSATE_FAILED: _S.ESCALATED,
        },
        _S.ACTIONED: {_E.CLOSE: _S.CLOSED},
        _S.COMPENSATED: {_E.CLOSE: _S.CLOSED},
        _S.ESCALATED: {_E.CLOSE: _S.CLOSED},
        _S.REJECTED: {_E.CLOSE: _S.CLOSED},
    },
)

SIDE_EFFECT_STATES: Final = frozenset({_S.EXECUTING, _S.ACTIONED, _S.COMPENSATING, _S.COMPENSATED})
"""States in which a real-world side effect may have occurred.

Reaching any of these without passing through DECIDED means an action executed
without a recorded, evidence-cited verdict. A test asserts that is impossible.
"""

APPROVAL_REQUIRED_PREDECESSORS: Final = frozenset({_S.DECIDED, _S.PENDING_APPROVAL})
"""The only states EXECUTING may be entered from.

DECIDED covers LOW-risk auto-execution; PENDING_APPROVAL covers MEDIUM and HIGH
after a human approves. There is no third way in, which is the machine-level
expression of "a HIGH-risk action never executes without approval"
(ROADMAP Phase 8: a hard zero, not a percentage).
"""
