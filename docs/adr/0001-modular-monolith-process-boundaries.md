# ADR-0001: Modular monolith with four justified process boundaries

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 0

## Context
TRACE-X spans ingestion, feature computation, scoring, agentic investigation, action execution and a
dashboard. The reflexive design is a microservice per capability. On a solo project with an 8 GB Docker
ceiling, each network boundary costs a deployment unit, a serialization format, a failure mode, a trace
hop and a set of integration tests — and buys nothing unless something genuinely differs across it.

Three things genuinely differ here: **latency SLO** (100 ms scoring vs 120 s investigations),
**scaling axis** (TPS vs analyst concurrency vs queue depth), and **runtime** (Python vs JVM vs Node).

## Decision
One Python package, `trace_core`, holds all domain logic. It is deployed under four entrypoints:
`trace-gateway` (hot path), `trace-api` (control plane), `trace-worker` (investigations),
`trace-stream` (Spark), plus `trace-ui` (Next.js) and three MCP servers (ADR-0013).

Each boundary's justification is recorded in `docs/ARCHITECTURE.md` §2. **Any new network boundary
requires an ADR.** `trace-gateway` and `trace-api` share a codebase and are collapsible into one
process by `TRACE_SINGLE_PROCESS=1` for laptop development.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Microservice per capability | No differing SLO, scaling axis or runtime between most of them. Pure overhead, and 8+ containers do not fit the RAM budget |
| Single process for everything | A 60-second LLM investigation would share a thread pool, connection pool and GC pause with a 100 ms scoring path. That is the one thing the architecture must prevent |
| Serverless functions per capability | Cold starts are fatal to a 100 ms p99; Spark and long-running LangGraph workers do not fit the model |

## Consequences
**Positive.** One domain model, one test suite, one refactor surface. The gateway/api split is a
deployment flag, not a rewrite. Latency isolation is real where it matters.
**Negative.** A shared package can accumulate hidden coupling; module boundaries must be enforced by
review rather than by the network. All Python processes share a dependency set, so an upgrade affects
everything at once.
**Risks.** If `trace_core` grows god-modules, the "modular" claim becomes false. Signal: import cycles,
or a change to `features/` requiring a change to `agents/`.

## Status
Accepted
