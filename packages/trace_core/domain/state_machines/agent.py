"""Agent invocation lifecycle.

One agent invocation, from selection to a recorded outcome. The outcome set is
taken from the `on_failure` column of the agent roster in ARCHITECTURE.md §8:
FAIL, DEGRADE and ABSTAIN are the three declared behaviours, and budget
exhaustion is the fourth outcome CLAUDE.md §10.4 requires to be recorded rather
than thrown away.

Every terminal state is a *recorded* result. There is deliberately no "gave up
quietly" state: an agent that produced nothing still produces a row, because the
Phase 9 metrics count invocations by outcome (`agent_invocations_total`).
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from trace_core.domain.state_machines.machine import StateMachine


class AgentInvocationState(StrEnum):
    PENDING = "PENDING"
    """Selected by the router, not yet started."""

    RUNNING = "RUNNING"
    """In flight, holding budget."""

    SUCCEEDED = "SUCCEEDED"
    """Produced its declared evidence."""

    DEGRADED = "DEGRADED"
    """Produced partial evidence after a dependency failed (roster: DEGRADE).

    Recorded as reduced evidence, never as a silent gap: an evidence kind that
    is missing for a known reason routes differently from one nobody attempted.
    """

    ABSTAINED = "ABSTAINED"
    """Declined to answer (roster: ABSTAIN).

    Reached after exactly one reprompt on invalid structured output
    (CLAUDE.md §10.7). Abstaining is a legitimate answer; loose parsing is not.
    """

    FAILED = "FAILED"
    """Errored (roster: FAIL). Includes an out-of-allow-list tool call."""

    BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
    """Halted mid-flight by the budget manager; partial evidence is retained."""


class AgentInvocationEvent(StrEnum):
    INVOKE = "INVOKE"
    COMPLETE = "COMPLETE"
    DEGRADE = "DEGRADE"
    ABSTAIN = "ABSTAIN"
    FAIL = "FAIL"
    EXHAUST_BUDGET = "EXHAUST_BUDGET"


_S = AgentInvocationState
_E = AgentInvocationEvent

AGENT_INVOCATION: Final = StateMachine[AgentInvocationState, AgentInvocationEvent](
    name="agent_invocation",
    initial=_S.PENDING,
    terminal=frozenset({_S.SUCCEEDED, _S.DEGRADED, _S.ABSTAINED, _S.FAILED, _S.BUDGET_EXHAUSTED}),
    transitions={
        _S.PENDING: {
            _E.INVOKE: _S.RUNNING,
            # An agent can be cut before it starts: the budget may be spent by
            # the time the router reaches it.
            _E.EXHAUST_BUDGET: _S.BUDGET_EXHAUSTED,
        },
        _S.RUNNING: {
            _E.COMPLETE: _S.SUCCEEDED,
            _E.DEGRADE: _S.DEGRADED,
            _E.ABSTAIN: _S.ABSTAINED,
            _E.FAIL: _S.FAILED,
            _E.EXHAUST_BUDGET: _S.BUDGET_EXHAUSTED,
        },
    },
)
"""Terminal states are all *recorded outcomes*; none of them is a hang."""

PRODUCED_EVIDENCE: Final = frozenset({_S.SUCCEEDED, _S.DEGRADED, _S.BUDGET_EXHAUSTED})
"""Outcomes after which some evidence may exist and must be integrated.

BUDGET_EXHAUSTED is in this set deliberately: an agent halted mid-flight keeps
the evidence it already produced (ARCHITECTURE.md §18, "partial evidence
retained"). Dropping it would discard work that was already paid for.
"""
