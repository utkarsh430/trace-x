# ADR-0010: LangGraph as the agent orchestration engine

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 6

## Context
The investigation runtime needs: typed graph state, durable checkpointing after every node (so a worker
crash resumes rather than restarting a paid LLM run), conditional edges for dynamic routing, step-level
replay for debugging, and a runtime that works identically on a laptop and in the cloud.

## Decision
LangGraph, with `PostgresSaver` checkpointing into the same database as the case and queue state
(ADR-0007). The graph topology is ours: an evidence-gap router (ADR-0019), not a prebuilt agent
pattern. LangGraph provides state management, checkpointing and edge control — nothing about *how*
agents are selected.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Hand-rolled state machine | Would work, but re-implements checkpointing, resumption and replay — the parts most likely to be subtly wrong |
| CrewAI / AutoGen | Built around agents conversing with each other, which is precisely the pattern this architecture rejects (ADR-0019). Fighting the framework's grain |
| Bedrock AgentCore Runtime | Cloud-hosted; would break the local-first requirement for the entire agent tier. See ADR-0014 |
| LangChain agents | Higher-level abstraction with weaker state and checkpoint control, and less visibility into the step boundary we need for auditing |

## Consequences
**Positive.** Crash resumption without re-spending LLM budget. Step-level replay gives a free audit
timeline. Runs identically everywhere, which is what makes local-first viable for the agent tier.
**Negative.** A framework dependency in the core loop; LangGraph API changes could force migration.
Checkpoint rows add write volume per investigation step.
**Risks.** Framework lock-in. Mitigated by keeping routing, agent specs, tool middleware and budget
enforcement in `trace_core` — LangGraph orchestrates our components, not the reverse. A migration would
replace the graph runner, not the domain.

## Status
Accepted
