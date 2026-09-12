"""State machine invariants.

This is the acceptance test for `P1.domain-model`, declared in
`tests/acceptance/status.json` as `pytest -m property tests/unit/test_state_machines.py`
-- hence the module-level `property` marker.

The invariants worth stating plainly, because each corresponds to a way the
system could fail silently rather than loudly:

* An illegal transition raises. A silently-ignored transition produces a
  plausible-looking case history, which is far worse than a stack trace.
* No state is a dead end. A state with no path to a terminal state is a place a
  worker enters and never leaves -- the hang CLAUDE.md §10.4 forbids.
* A side effect cannot happen without a recorded verdict, and EXECUTING has
  exactly two ways in. That is the machine-level form of "a HIGH-risk action
  never executes without approval".
* The investigation table and the ARCHITECTURE.md §8 diagram agree, edge for
  edge, except for a difference that is itself asserted.
"""

from __future__ import annotations

import re
from enum import StrEnum
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from trace_core.domain.errors import IllegalTransitionError
from trace_core.domain.state_machines import (
    ADDED_EDGES,
    AGENT_INVOCATION,
    APPROVAL_REQUIRED_PREDECESSORS,
    CASE,
    INVESTIGATION,
    PRODUCED_EVIDENCE,
    SIDE_EFFECT_STATES,
    AgentInvocationEvent,
    AgentInvocationState,
    CaseEvent,
    CaseStatus,
    InvestigationEvent,
    InvestigationState,
    MalformedMachineError,
    StateMachine,
)

pytestmark = pytest.mark.property

ROOT = Path(__file__).resolve().parents[2]

AnyMachine = StateMachine[Any, Any]

MACHINES: list[AnyMachine] = [CASE, INVESTIGATION, AGENT_INVOCATION]
MACHINE_EVENTS: dict[str, type[StrEnum]] = {
    "case": CaseEvent,
    "investigation": InvestigationEvent,
    "agent_invocation": AgentInvocationEvent,
}


def _ids(machine: AnyMachine) -> str:
    return machine.name


# ------------------------------------------------------- universal laws ----


@pytest.mark.parametrize("machine", MACHINES, ids=_ids)
def test_every_illegal_transition_raises(machine: AnyMachine) -> None:
    """Exhaustive over the full state x event product -- no sampling needed."""
    events = MACHINE_EVENTS[machine.name]
    checked = 0
    for state in machine.states:
        legal = machine.legal_events(state)
        for event in events:
            if event in legal:
                assert machine.transition(state, event) in machine.states
                continue
            checked += 1
            with pytest.raises(IllegalTransitionError) as exc:
                machine.transition(state, event)
            assert exc.value.state == str(state)
            assert exc.value.event == str(event)
    assert checked > 0, "a machine where everything is legal is not a state machine"


@pytest.mark.parametrize("machine", MACHINES, ids=_ids)
def test_terminal_states_absorb(machine: AnyMachine) -> None:
    for state in machine.terminal:
        assert machine.legal_events(state) == frozenset()
        assert machine.is_terminal(state)


@pytest.mark.parametrize("machine", MACHINES, ids=_ids)
def test_every_state_is_reachable_from_the_initial_state(machine: AnyMachine) -> None:
    """An unreachable state is dead code that looks like coverage."""
    assert machine.states - machine.reachable_from(machine.initial) - {machine.initial} == set()


@pytest.mark.parametrize("machine", MACHINES, ids=_ids)
def test_a_terminal_state_is_reachable_from_everywhere(machine: AnyMachine) -> None:
    """No dead ends: every state can still finish."""
    for state in machine.states:
        if machine.is_terminal(state):
            continue
        assert machine.reachable_from(state) & machine.terminal, f"{state} can never finish"


@pytest.mark.parametrize("machine", MACHINES, ids=_ids)
def test_transitions_are_deterministic(machine: AnyMachine) -> None:
    """Same state, same event, same result -- every time. Replay depends on it."""
    for state in machine.states:
        for event in machine.legal_events(state):
            first = machine.transition(state, event)
            assert all(machine.transition(state, event) is first for _ in range(5))


@pytest.mark.parametrize("machine", MACHINES, ids=_ids)
def test_transition_never_mutates_the_machine(machine: AnyMachine) -> None:
    before = {s: machine.legal_events(s) for s in machine.states}
    for state in machine.states:
        for event in machine.legal_events(state):
            machine.transition(state, event)
    assert {s: machine.legal_events(s) for s in machine.states} == before


# ------------------------------------------------------- random walking ----


@given(
    seed=st.integers(min_value=0, max_value=10_000), steps=st.integers(min_value=1, max_value=60)
)
def test_a_random_legal_walk_never_leaves_the_declared_state_space(seed: int, steps: int) -> None:
    """Whatever sequence of legal events occurs, the machine stays well defined."""
    import random

    rng = random.Random(seed)
    for machine in MACHINES:
        state = machine.initial
        for _ in range(steps):
            legal = sorted(machine.legal_events(state), key=str)
            if not legal:
                assert machine.is_terminal(state)
                break
            state = machine.transition(state, rng.choice(legal))
            assert state in machine.states


@pytest.mark.parametrize("machine", [CASE, AGENT_INVOCATION], ids=_ids)
def test_acyclic_machines_always_terminate(machine: AnyMachine) -> None:
    """The case and agent machines have no unbounded loop, so every walk ends.

    The investigation machine is excluded on purpose: its routing loop is
    deliberately cyclic (that is what makes the path vary with the evidence),
    and its termination comes from budgets, not from the graph.
    """
    import random

    rng = random.Random(7)
    for _ in range(200):
        state, seen = machine.initial, 0
        while not machine.is_terminal(state):
            legal = sorted(machine.legal_events(state), key=str)
            state = machine.transition(state, rng.choice(legal))
            seen += 1
            assert seen <= 4 * len(machine.states), f"{machine.name} failed to converge"


# ------------------------------------------------------ case invariants ----


def test_no_side_effect_without_a_recorded_verdict() -> None:
    """Reaching a side-effecting state must require passing through DECIDED.

    Proven by deleting DECIDED from the graph and checking the side-effect
    states become unreachable, rather than by inspecting paths by eye.
    """
    without_decided: dict[CaseStatus, dict[CaseEvent, CaseStatus]] = {
        state: {e: t for e, t in edges.items() if t is not CaseStatus.DECIDED}
        for state, edges in CASE.transitions.items()
        if state is not CaseStatus.DECIDED
    }
    reachable: set[CaseStatus] = set()
    frontier = [CaseStatus.OPEN]
    while frontier:
        current = frontier.pop()
        for target in without_decided.get(current, {}).values():
            if target not in reachable:
                reachable.add(target)
                frontier.append(target)
    leaked = reachable & SIDE_EFFECT_STATES
    assert not leaked, (
        f"a side effect is reachable without a verdict: {sorted(str(s) for s in leaked)}"
    )


def test_executing_has_exactly_two_ways_in() -> None:
    """LOW auto-executes from DECIDED; MEDIUM/HIGH arrive from PENDING_APPROVAL.

    A third entry point would be a path by which an unapproved HIGH-risk action
    could execute (ROADMAP Phase 8 requires a hard zero there).
    """
    entries = {
        state for state, edges in CASE.transitions.items() if CaseStatus.EXECUTING in edges.values()
    }
    assert entries == APPROVAL_REQUIRED_PREDECESSORS


def test_a_lost_worker_returns_the_case_to_the_queue_not_to_open() -> None:
    """Re-triaging would discard the checkpoint and re-spend the budget (ADR-0007)."""
    assert CASE.transition(CaseStatus.INVESTIGATING, CaseEvent.WORKER_LOST) is CaseStatus.TRIAGED


def test_every_case_can_reach_closed() -> None:
    """'Is this case still open?' must always be answerable."""
    for state in CASE.states:
        assert state is CaseStatus.CLOSED or CaseStatus.CLOSED in CASE.reachable_from(state)


def test_an_expired_approval_escalates_rather_than_lingering() -> None:
    """SECURITY.md §7: approvals expire to ESCALATED after 30 minutes."""
    assert (
        CASE.transition(CaseStatus.PENDING_APPROVAL, CaseEvent.APPROVAL_EXPIRED)
        is CaseStatus.ESCALATED
    )


# --------------------------------------------- investigation invariants ----


def _diagram_edges() -> set[tuple[str, str]]:
    """(from, to) pairs in the stateDiagram-v2 block of ARCHITECTURE.md §8."""
    text = (ROOT / "docs" / "ARCHITECTURE.md").read_text()
    section = text.split("## 8. Agent architecture", 1)[1].split("\n## 9.", 1)[0]
    block = section.split("stateDiagram-v2", 1)[1].split("```", 1)[0]
    edges: set[tuple[str, str]] = set()
    for line in block.splitlines():
        m = re.match(r"\s*(\[\*\]|\w+)\s*-->\s*(\[\*\]|\w+)", line)
        if not m:
            continue
        src, dst = m.group(1), m.group(2)
        if src == "[*]":  # entry marker, not a transition
            continue
        edges.add((src, "DONE" if dst == "[*]" else dst))
    return edges


def _table_edges() -> set[tuple[str, str]]:
    return {
        (str(state), str(target))
        for state, edges in INVESTIGATION.transitions.items()
        for target in edges.values()
    }


def test_the_diagram_was_parsed_at_all() -> None:
    """Guards against a doc edit silently turning the comparison into a no-op."""
    assert len(_diagram_edges()) >= 12


def test_the_table_contains_every_edge_the_diagram_declares() -> None:
    """CLAUDE.md §14: code and ARCHITECTURE.md disagreeing is a bug in one of them."""
    missing = _diagram_edges() - _table_edges()
    assert not missing, f"ARCHITECTURE.md §8 has edges the table lacks: {sorted(missing)}"


def test_the_only_extra_edges_are_the_declared_ones() -> None:
    """The two added ROUTE exits are documented in ADR-0027, and nothing else is."""
    extra = _table_edges() - _diagram_edges()
    declared = {(src, dst) for src, _event, dst in ADDED_EDGES}
    assert extra == declared, (
        f"undeclared divergence from ARCHITECTURE.md §8: {sorted(extra - declared)}. "
        f"Add the edge to the diagram or record it in ADDED_EDGES."
    )


def test_an_empty_eligible_set_still_terminates() -> None:
    """ADR-0019's eligible(a) can be empty; §8 gave ROUTE no exit for that.

    This is the gap the added edges close. Without them a router that finds no
    eligible agent has nowhere legal to go.
    """
    state = INVESTIGATION.transition(InvestigationState.ROUTE, InvestigationEvent.NO_ELIGIBLE_AGENT)
    state = INVESTIGATION.transition(state, InvestigationEvent.INSUFFICIENT_EVIDENCE)
    state = INVESTIGATION.transition(state, InvestigationEvent.QUEUED)
    assert state is InvestigationState.DONE


def test_budget_exhaustion_reaches_a_recorded_outcome_not_a_hang() -> None:
    """CLAUDE.md §10.4: budget exhaustion is a valid recorded outcome."""
    state = INVESTIGATION.transition(InvestigationState.ROUTE, InvestigationEvent.BUDGET_EXHAUSTED)
    assert state is InvestigationState.DECIDE
    assert InvestigationState.HUMAN_QUEUE in INVESTIGATION.reachable_from(state)


def test_invalid_output_reaches_abstain_and_reroutes() -> None:
    """CLAUDE.md §10.7: one reprompt, then ABSTAIN -- never a loose parse."""
    state = INVESTIGATION.transition(
        InvestigationState.VALIDATE_OUTPUT, InvestigationEvent.SCHEMA_INVALID
    )
    assert state is InvestigationState.RETRY_OR_ABSTAIN
    assert (
        INVESTIGATION.transition(state, InvestigationEvent.REPROMPTED) is InvestigationState.ROUTE
    )


# ----------------------------------------------------- agent invariants ----


def test_every_agent_outcome_is_recorded_not_silent() -> None:
    """`agent_invocations_total{agent,outcome}` needs an outcome for every path."""
    assert AGENT_INVOCATION.terminal == frozenset(
        {
            AgentInvocationState.SUCCEEDED,
            AgentInvocationState.DEGRADED,
            AgentInvocationState.ABSTAINED,
            AgentInvocationState.FAILED,
            AgentInvocationState.BUDGET_EXHAUSTED,
        }
    )


def test_budget_exhaustion_retains_partial_evidence() -> None:
    """ARCHITECTURE.md §18: 'agent halted mid-flight, partial evidence retained'."""
    assert AgentInvocationState.BUDGET_EXHAUSTED in PRODUCED_EVIDENCE
    assert AgentInvocationState.FAILED not in PRODUCED_EVIDENCE


def test_an_agent_can_be_cut_before_it_starts() -> None:
    assert (
        AGENT_INVOCATION.transition(
            AgentInvocationState.PENDING, AgentInvocationEvent.EXHAUST_BUDGET
        )
        is AgentInvocationState.BUDGET_EXHAUSTED
    )


# ---------------------------------------- the validator actually bites -----


class _S(StrEnum):
    A = "A"
    B = "B"
    END = "END"


class _E(StrEnum):
    GO = "GO"
    BACK = "BACK"


def _build(**kwargs: object) -> AnyMachine:
    defaults: dict[str, object] = {
        "name": "probe",
        "initial": _S.A,
        "terminal": frozenset({_S.END}),
        "transitions": {_S.A: {_E.GO: _S.END}},
    }
    defaults.update(kwargs)
    return StateMachine(**defaults)  # type: ignore[arg-type]


def test_a_well_formed_probe_machine_builds() -> None:
    assert _build().transition(_S.A, _E.GO) is _S.END


def test_dead_end_state_is_refused() -> None:
    with pytest.raises(MalformedMachineError, match="no terminal state is reachable"):
        _build(transitions={_S.A: {_E.GO: _S.B}, _S.B: {_E.BACK: _S.B}})


def test_terminal_state_with_outgoing_edges_is_refused() -> None:
    with pytest.raises(MalformedMachineError, match="terminal state"):
        _build(transitions={_S.A: {_E.GO: _S.END}, _S.END: {_E.BACK: _S.A}})


def test_unreachable_state_is_refused() -> None:
    with pytest.raises(MalformedMachineError, match="unreachable"):
        _build(
            transitions={_S.A: {_E.GO: _S.END}, _S.B: {_E.GO: _S.END}},
        )


def test_machine_with_no_terminal_state_is_refused() -> None:
    with pytest.raises(MalformedMachineError, match="terminal"):
        _build(terminal=frozenset(), transitions={_S.A: {_E.GO: _S.A}})
