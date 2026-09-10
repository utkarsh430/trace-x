# ADR-0019: Evidence-gap routing — no LLM selects the next agent

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 7

## Context
The product requires that the investigation path is **not predetermined** — a device discovery should
be able to pull in graph analysis, which pulls in historical cases, which the skeptic can challenge,
creating new lines of enquiry.

Two obvious designs both fail:

- **Fixed pipeline.** Every investigation runs the same agents in the same order. Cheap and auditable,
  but it is not investigating — it is a report generator.
- **Agents calling agents / an LLM supervisor choosing freely.** Genuinely dynamic, but unbounded and
  unauditable: the reachable state space is whatever the model imagines, cost is unpredictable, and two
  runs on identical input can diverge for no recorded reason.

## Decision
Agents never call each other, and **no LLM selects the next agent.** Routing is a deterministic function
of an **evidence-gap ledger**:

```
open_gaps = ⋃ h.required_evidence  for h in hypotheses if h.status == OPEN
          ∪ ⋃ c.required_refutation_evidence  for c in open_challenges

eligible(a) ⟺ a.produces_evidence ∩ open_gaps ≠ ∅
            ∧ a.required_evidence ⊆ collected_kinds
            ∧ a.invocations < a.max_invocations
            ∧ budget_remaining(steps, tokens, usd, wall_clock)

next = argmax_{a ∈ eligible} priority(a, open_gaps)   # deterministic tie-break by agent_id
```

The **LLM** proposes and revises *hypotheses* and interprets evidence. The **router** performs the
mechanical map from gaps to agents. The Skeptic reopens the graph by emitting challenges that create
new gaps — which is what makes the path genuinely non-deterministic in a principled way.

Termination is guaranteed by **four independent bounds**: max steps, wall-clock, cost, and monotonic
per-agent invocation caps. Budget exhaustion yields `INSUFFICIENT_EVIDENCE` → human queue: a valid
recorded outcome, never a hang.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Fixed pipeline | Not an investigation. Fails the core product requirement |
| LLM supervisor picks the next agent | Unbounded reachable state space, unpredictable cost, unauditable, and irreproducible run to run |
| Agents call each other directly | Same problems plus no central budget enforcement and no single audit point |
| Blackboard with agents self-selecting | Closer, but without hypothesis-derived gaps there is no principled stopping condition |

## Consequences
**Positive.** The path genuinely varies with the evidence, while the reachable state space stays finite,
auditable and replayable. Termination is provable rather than hoped for. Routing decisions are
explainable: "the Graph agent ran because `GRAPH_CLUSTER` was an open gap."
**Negative.** Agents must declare `produces_evidence` and `required_evidence` accurately; a wrong
declaration produces a permanent gap or a never-eligible agent. Expressiveness is bounded by the
evidence taxonomy.
**Risks.** An agent that cannot actually produce its declared evidence causes routing thrash. Mitigated
by per-agent invocation caps and a property test asserting convergence from adversarially generated
states. Verified by the Phase 7 path-variability test, which requires two fraud patterns to visit
provably different agent sequences.

## Status
Accepted
