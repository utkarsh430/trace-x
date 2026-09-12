"""A transition-table state machine that validates itself at construction.

Every machine in TRACE-X is an explicit table, not a pile of `if` statements.
Three things follow, and all three are requirements rather than conveniences:

* **Illegal transitions raise.** A case that slips into an impossible state has
  lost its audit meaning, and the audit chain is the product (SECURITY.md §8).
* **The table is data,** so it can be property-tested exhaustively and rendered
  into documentation without re-reading the implementation.
* **Structural defects fail at import time.** The checks below run when a table
  is defined, so a dead-end state — a state from which no terminal state is
  reachable — cannot ship. In the investigation machine that is not a
  theoretical concern: it is the difference between a bounded investigation and
  a worker that hangs forever (CLAUDE.md §10.4).
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from trace_core.domain.errors import IllegalTransitionError


class MalformedMachineError(Exception):
    """A transition table is structurally invalid.

    Not a `TraceXError`: this is a programming error in a literal table, caught
    at import time, not a runtime condition any caller could handle.
    """


@dataclass(frozen=True, slots=True)
class StateMachine[S: StrEnum, E: StrEnum]:
    """An immutable, validated transition table.

    `transitions[state][event] -> next_state`. A state absent from `transitions`
    has no outgoing edges and must therefore be declared terminal.
    """

    name: str
    initial: S
    terminal: frozenset[S]
    transitions: Mapping[S, Mapping[E, S]]

    def __post_init__(self) -> None:
        self._validate()

    # ----------------------------------------------------------- queries --

    def legal_events(self, state: S) -> frozenset[E]:
        """Events accepted in `state`. Empty for terminal states."""
        return frozenset(self.transitions.get(state, {}))

    def is_terminal(self, state: S) -> bool:
        return state in self.terminal

    def transition(self, state: S, event: E) -> S:
        """Apply `event` in `state`, or raise.

        Raising rather than returning the unchanged state is deliberate: a
        silently-ignored transition is a bug that produces a plausible-looking
        case history, which is far harder to find than a stack trace.
        """
        allowed = self.transitions.get(state, {})
        if event not in allowed:
            raise IllegalTransitionError(
                machine=self.name,
                state=str(state),
                event=str(event),
                allowed=frozenset(str(e) for e in allowed),
            )
        return allowed[event]

    def reachable_from(self, state: S) -> frozenset[S]:
        """States reachable from `state`, excluding `state` unless it is in a cycle."""
        seen: set[S] = set()
        queue: deque[S] = deque(self.transitions.get(state, {}).values())
        while queue:
            current = queue.popleft()
            if current in seen:
                continue
            seen.add(current)
            queue.extend(self.transitions.get(current, {}).values())
        return frozenset(seen)

    @property
    def states(self) -> frozenset[S]:
        """Every state named anywhere in the table, plus the initial state."""
        named: set[S] = {self.initial, *self.terminal, *self.transitions}
        for edges in self.transitions.values():
            named.update(edges.values())
        return frozenset(named)

    # -------------------------------------------------------- validation --

    def _validate(self) -> None:
        problems: list[str] = []

        for state in self.terminal:
            if self.transitions.get(state):
                problems.append(
                    f"terminal state {state} has outgoing transitions "
                    f"{sorted(str(e) for e in self.transitions[state])}"
                )

        for state in self.states:
            if state not in self.terminal and not self.transitions.get(state):
                problems.append(f"non-terminal state {state} has no outgoing transition")

        if not self.terminal:
            problems.append("machine declares no terminal state, so nothing can ever finish")

        unreachable = self.states - self.reachable_from(self.initial) - {self.initial}
        if unreachable:
            problems.append(
                f"unreachable from {self.initial}: {sorted(str(s) for s in unreachable)}"
            )

        # The important one. A state with no path to a terminal state is a
        # place an investigation can enter and never leave -- exactly the hang
        # that CLAUDE.md §10.4 forbids.
        for state in self.states:
            if state in self.terminal:
                continue
            if not (self.reachable_from(state) & self.terminal):
                problems.append(
                    f"no terminal state is reachable from {state}; "
                    f"anything entering it can never finish"
                )

        if problems:
            raise MalformedMachineError(
                f"{self.name} transition table is invalid:\n  - " + "\n  - ".join(problems)
            )
