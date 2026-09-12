"""Investigation orchestration lifecycle (ARCHITECTURE.md §8).

Transcribed from the `stateDiagram-v2` block in §8, with **two added edges that
the published diagram does not contain**. They are listed explicitly below
rather than folded in quietly, because ARCHITECTURE.md is authoritative for
design and a silent divergence between it and this table would be exactly the
drift CLAUDE.md §14 forbids. ADR-0027 records the decision; §19 of
ARCHITECTURE.md documents the resulting machine.

**The gap.** ROUTE's only exit in §8 is `AGENT_SELECTED`. ADR-0019 defines
routing as

    eligible(a) ⟺ a.produces_evidence ∩ open_gaps ≠ ∅
                ∧ a.required_evidence ⊆ collected_kinds
                ∧ a.invocations < a.max_invocations
                ∧ budget_remaining(steps, tokens, usd, wall_clock)

and that predicate is routinely empty — every eligible agent may already be at
its invocation cap, or the budget may be spent. §8 offers ROUTE no edge for
that case, so an implementation reaching it has nowhere legal to go. Since
CLAUDE.md §10.4 requires budget exhaustion to produce a recorded
INSUFFICIENT_EVIDENCE rather than a hang, ROUTE gains two exits to DECIDE.

Note that the literal §8 table is *structurally* valid — DONE is reachable from
ROUTE by way of the agent path — so this is a modelling gap, not a dead end the
validator would have caught. It was found by reading ADR-0019 against the
diagram, which is why it is written down here.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from trace_core.domain.state_machines.machine import StateMachine


class InvestigationState(StrEnum):
    INIT = "INIT"
    PLAN = "PLAN"
    ROUTE = "ROUTE"
    """The deterministic evidence-gap router. No LLM runs here (ADR-0019)."""

    INVOKE_AGENT = "INVOKE_AGENT"
    VALIDATE_OUTPUT = "VALIDATE_OUTPUT"
    RETRY_OR_ABSTAIN = "RETRY_OR_ABSTAIN"
    """Exactly one reprompt with the validation error, then ABSTAIN (CLAUDE.md §10.7)."""

    INTEGRATE_EVIDENCE = "INTEGRATE_EVIDENCE"
    UPDATE_HYPOTHESES = "UPDATE_HYPOTHESES"
    SKEPTIC_GATE = "SKEPTIC_GATE"
    """Where a challenge can reopen the graph by creating new gaps."""

    DECIDE = "DECIDE"
    """The Decision agent runs here with no retrieval tools, so the decision is
    reproducible from the recorded ledger alone (ARCHITECTURE.md §8)."""

    PROPOSE_ACTIONS = "PROPOSE_ACTIONS"
    HUMAN_QUEUE = "HUMAN_QUEUE"
    DONE = "DONE"


class InvestigationEvent(StrEnum):
    START = "START"
    PLANNED = "PLANNED"
    AGENT_SELECTED = "AGENT_SELECTED"
    AGENT_RETURNED = "AGENT_RETURNED"
    SCHEMA_INVALID = "SCHEMA_INVALID"
    SCHEMA_VALID = "SCHEMA_VALID"
    REPROMPTED = "REPROMPTED"
    EVIDENCE_INTEGRATED = "EVIDENCE_INTEGRATED"
    HYPOTHESES_UPDATED = "HYPOTHESES_UPDATED"
    CHALLENGE_RAISED = "CHALLENGE_RAISED"
    GAPS_CLOSED = "GAPS_CLOSED"
    NO_ELIGIBLE_AGENT = "NO_ELIGIBLE_AGENT"
    """Added edge: the router's eligible set is empty (ADR-0019)."""

    BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
    """Added edge: a termination bound was hit while routing (CLAUDE.md §10.4)."""

    DECIDED = "DECIDED"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    ACTIONS_PROPOSED = "ACTIONS_PROPOSED"
    QUEUED = "QUEUED"


_S = InvestigationState
_E = InvestigationEvent

#: Edges present here but not in the ARCHITECTURE.md §8 diagram. A test asserts
#: this set is exactly the difference, so neither can drift without the other.
ADDED_EDGES: Final[frozenset[tuple[str, str, str]]] = frozenset(
    {
        (_S.ROUTE.value, _E.NO_ELIGIBLE_AGENT.value, _S.DECIDE.value),
        (_S.ROUTE.value, _E.BUDGET_EXHAUSTED.value, _S.DECIDE.value),
    }
)

INVESTIGATION: Final = StateMachine[InvestigationState, InvestigationEvent](
    name="investigation",
    initial=_S.INIT,
    terminal=frozenset({_S.DONE}),
    transitions={
        _S.INIT: {_E.START: _S.PLAN},
        _S.PLAN: {_E.PLANNED: _S.ROUTE},
        _S.ROUTE: {
            _E.AGENT_SELECTED: _S.INVOKE_AGENT,
            # --- the two added edges ---
            _E.NO_ELIGIBLE_AGENT: _S.DECIDE,
            _E.BUDGET_EXHAUSTED: _S.DECIDE,
        },
        _S.INVOKE_AGENT: {_E.AGENT_RETURNED: _S.VALIDATE_OUTPUT},
        _S.VALIDATE_OUTPUT: {
            _E.SCHEMA_INVALID: _S.RETRY_OR_ABSTAIN,
            _E.SCHEMA_VALID: _S.INTEGRATE_EVIDENCE,
        },
        _S.RETRY_OR_ABSTAIN: {_E.REPROMPTED: _S.ROUTE},
        _S.INTEGRATE_EVIDENCE: {_E.EVIDENCE_INTEGRATED: _S.UPDATE_HYPOTHESES},
        _S.UPDATE_HYPOTHESES: {_E.HYPOTHESES_UPDATED: _S.SKEPTIC_GATE},
        _S.SKEPTIC_GATE: {
            _E.CHALLENGE_RAISED: _S.ROUTE,
            _E.GAPS_CLOSED: _S.DECIDE,
        },
        _S.DECIDE: {
            _E.DECIDED: _S.PROPOSE_ACTIONS,
            _E.INSUFFICIENT_EVIDENCE: _S.HUMAN_QUEUE,
        },
        _S.PROPOSE_ACTIONS: {_E.ACTIONS_PROPOSED: _S.DONE},
        _S.HUMAN_QUEUE: {_E.QUEUED: _S.DONE},
    },
)

CYCLE_STATES: Final = frozenset(
    {
        _S.ROUTE,
        _S.INVOKE_AGENT,
        _S.VALIDATE_OUTPUT,
        _S.RETRY_OR_ABSTAIN,
        _S.INTEGRATE_EVIDENCE,
        _S.UPDATE_HYPOTHESES,
        _S.SKEPTIC_GATE,
    }
)
"""The routing loop.

Termination is **not** a property of this graph — the loop is deliberately
cyclic, which is what makes the investigation path vary with the evidence. It is
guaranteed instead by the four independent budget bounds of CLAUDE.md §10.4,
which is why `BUDGET_EXHAUSTED` must have an edge out of ROUTE.
"""
