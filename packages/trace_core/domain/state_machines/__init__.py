"""The three state machines (ARCHITECTURE.md §19).

`case`          — what an analyst and the audit chain see.
`investigation` — the orchestration loop of ARCHITECTURE.md §8.
`agent`         — one agent invocation and its recorded outcome.

All three are immutable transition tables validated at import time by
`machine.StateMachine`, so a structurally broken table cannot ship.
"""

from trace_core.domain.state_machines.agent import (
    AGENT_INVOCATION,
    PRODUCED_EVIDENCE,
    AgentInvocationEvent,
    AgentInvocationState,
)
from trace_core.domain.state_machines.case import (
    APPROVAL_REQUIRED_PREDECESSORS,
    CASE,
    SIDE_EFFECT_STATES,
    CaseEvent,
    CaseStatus,
)
from trace_core.domain.state_machines.investigation import (
    ADDED_EDGES,
    CYCLE_STATES,
    INVESTIGATION,
    InvestigationEvent,
    InvestigationState,
)
from trace_core.domain.state_machines.machine import MalformedMachineError, StateMachine

__all__ = [
    "ADDED_EDGES",
    "AGENT_INVOCATION",
    "APPROVAL_REQUIRED_PREDECESSORS",
    "CASE",
    "CYCLE_STATES",
    "INVESTIGATION",
    "PRODUCED_EVIDENCE",
    "SIDE_EFFECT_STATES",
    "AgentInvocationEvent",
    "AgentInvocationState",
    "CaseEvent",
    "CaseStatus",
    "InvestigationEvent",
    "InvestigationState",
    "MalformedMachineError",
    "StateMachine",
]
