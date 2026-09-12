# ADR-0027: Case, investigation and agent state machines as validated transition tables

- **Status:** Accepted
- **Date:** 2026-09-12
- **Phase:** 1

## Context
Phase 1 must deliver "both state machines ... as pure functions with an explicit legal-transition
table" (`docs/ROADMAP.md` § Phase 1). Two problems surfaced immediately.

**First, one of the two machines did not exist.** The roadmap cites `docs/ARCHITECTURE.md` §13 for the
case lifecycle and §12 for the agent lifecycle, but §12 is *Action execution architecture* and §13 is
*Observability*. The only state diagram in the document is in §8 and describes the investigation
orchestration loop. **No case lifecycle was specified anywhere in the control plane.** It had to be
authored, and authoring a lifecycle for the unit the audit chain is keyed on is an architectural
decision, not a transcription task.

**Second, the §8 diagram has a modelling gap.** `ROUTE`'s only exit is `AGENT_SELECTED`. ADR-0019
defines routing through a predicate

```
eligible(a) ⟺ a.produces_evidence ∩ open_gaps ≠ ∅
            ∧ a.required_evidence ⊆ collected_kinds
            ∧ a.invocations < a.max_invocations
            ∧ budget_remaining(steps, tokens, usd, wall_clock)
```

which is routinely empty — every candidate agent may sit at its invocation cap, or the budget may be
spent. §8 offers `ROUTE` no edge for that case, so an implementation that reaches it has nowhere legal
to go. CLAUDE.md §10.4 requires budget exhaustion to yield a recorded `INSUFFICIENT_EVIDENCE`, never a
hang, so the gap has to be closed somewhere.

Worth stating precisely: the literal §8 table is *structurally* valid — `DONE` is reachable from
`ROUTE` by way of the agent path — so this was not a dead end that automated validation would have
caught. It was found by reading ADR-0019 against the diagram.

## Decision
**Three machines**, each an immutable transition table validated at import time:

| Machine | Scope | Source |
|---|---|---|
| `case` | What an analyst and the audit chain see, triage to closure | **Newly authored**; every edge derived from `ARCHITECTURE.md` §12 and `SECURITY.md` §7, cited inline |
| `investigation` | The orchestration loop | `ARCHITECTURE.md` §8, plus two declared edges |
| `agent_invocation` | One agent invocation and its recorded outcome | The `on_failure` column of the §8 agent roster, plus CLAUDE.md §10.4 |

**Tables are data, and the table validates itself when it is defined.** `StateMachine.__post_init__`
rejects: a terminal state with outgoing edges, a non-terminal state with none, a state unreachable
from the initial state, a machine with no terminal state, and — the one that matters — **any state
from which no terminal state is reachable**. A structurally broken machine therefore fails at import,
not in production.

**Illegal transitions raise `IllegalTransitionError`**, carrying the machine, the state, the event and
the legal alternatives. They are never warnings and never silently ignored.

**Two edges are added to the §8 diagram, and declared as added**: `ROUTE --NO_ELIGIBLE_AGENT--> DECIDE`
and `ROUTE --BUDGET_EXHAUSTED--> DECIDE`. They are listed in `ADDED_EDGES`, and a test parses the
mermaid block out of `ARCHITECTURE.md` §8, diffs its edges against the table, and fails unless the
difference is *exactly* that declared set. Divergence in either direction breaks the build.

`ARCHITECTURE.md` gains **§19** carrying all three specifications, and the `ROADMAP.md` cross-reference
is corrected to point at §19 and §8.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Model state transitions with `if`/`elif` in the services that own them | The legality rules end up scattered across the worker, the API and the policy engine, where they drift. A table can be property-tested exhaustively over the full state × event product, which is what these tests do |
| A third-party state machine library (`transitions`, `python-statemachine`) | A dependency requiring an ADR (CLAUDE.md §17) in exchange for roughly sixty lines. Neither library performs the check that actually earns its place here — that a terminal state is reachable from every state |
| Return the unchanged state on an illegal transition instead of raising | Produces a plausible-looking case history from a real bug. A case silently stuck in `PENDING_APPROVAL` looks exactly like one legitimately awaiting a human, and the audit chain is the product (`SECURITY.md` §8) |
| Quietly add the missing `ROUTE` exits without recording them | The cheapest option and the most corrosive: `ARCHITECTURE.md` is authoritative for design, so an undocumented divergence is precisely the drift CLAUDE.md §14 forbids. Declaring them keeps the disagreement visible and testable |
| Amend the §8 diagram instead of declaring added edges | Considered, and partially done — §19 documents the full machine. The `ADDED_EDGES` mechanism is retained regardless, because it is what keeps any *future* divergence from going unnoticed |
| Collapse `EXECUTING` into `ACTIONED` in the case machine | Would make "approved" indistinguishable from "actually happened". Execution and verification can both fail, and Phase 8 must be able to prove a HIGH-risk action never executed without approval |

## Consequences
**Positive.** Termination and reachability are structural properties checked at import, not hopes.
`EXECUTING` provably has exactly two entry points, which is the machine-level form of "no unapproved
HIGH-risk execution". A side effect provably cannot occur without passing `DECIDED`, proven by deleting
`DECIDED` from the graph and showing the side-effecting states become unreachable. The investigation
table and `ARCHITECTURE.md` §8 cannot drift apart silently.

**Negative.** Three machines mean three vocabularies, and a reader must know which one answers their
question. Every new lifecycle step is now a table edit plus a test, which is friction — intended
friction, but real. The `ADDED_EDGES` mechanism is extra machinery that exists solely to keep a
document and a table honest. The case machine has fourteen states, which is more than a first sketch
would produce; the extra states are the failure paths, which is exactly where a shorter machine would
have been lying.

**Risks.** The case machine is authored ahead of the code that will drive it (Phases 2, 5 and 8), so
some edge may prove wrong in practice. Mitigated by deriving every edge from an existing authority
rather than from imagination, and by the fact that a wrong edge surfaces as an
`IllegalTransitionError` in an integration test rather than as silent corruption. A second risk is the
diagram-parsing test becoming a no-op if someone reformats the mermaid block; guarded by asserting the
parse finds at least twelve edges before comparing.

## Status
Accepted
