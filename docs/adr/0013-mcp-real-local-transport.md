# ADR-0013: MCP as a real local transport boundary, from Phase 5

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 5
- **Supersedes:** an earlier draft position that deferred MCP entirely to AWS AgentCore Gateway (Phase 12)

## Context
The earlier position was that tools would be plain in-process functions locally, with MCP appearing
only when AgentCore Gateway exposed them in the cloud. That is wrong for two reasons: it makes Phase 12
a rewrite rather than a configuration change, and it means the protocol boundary — including its
authorization model — would first be exercised in the most expensive, least debuggable environment.

Honest counter-argument, recorded because it is real: **functionally, in-process calls would work.**
MCP adds a process boundary, serialization cost and a failure mode. It earns its place through genuine
interoperability, a real auth boundary, and being the thing AgentCore federates later.

## Decision
Three real MCP servers exist locally from Phase 5: `fraud-intelligence-mcp`, `identity-mcp`,
`graph-mcp`. Transport is **stdio by default** (spawned by the worker — a real protocol boundary at
**zero container cost**) and streamable HTTP with OAuth 2.1 under the `mcp-http` profile. SSE transport
is deprecated and not implemented.

`ToolSpec` remains the internal abstraction, and `InProcessAdapter` is **deliberately retained** for
latency-sensitive feature reads and Decision-agent ledger reads, where a protocol hop is not justified.

**The critical invariant: behaviour is transport-independent.** Authorization, capability tokens,
`EvidenceEnvelope` validation, rate limits, timeouts, row caps and audit live in **one middleware
chain**. An MCP server is a thin shell that registers the middleware-wrapped functions — **there is no
unwrapped entrypoint to expose**, and a test asserts it. `ToolTransportParitySuite` asserts identical
envelopes and identical denial/limit/timeout/truncation/audit behaviour across transports.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| In-process only; MCP in Phase 12 | Makes the cloud step a rewrite and defers the auth boundary to the worst place to debug it |
| MCP for everything, no in-process adapter | Puts a protocol hop on latency-sensitive reads for no benefit, and leaves the abstraction with one transport — untestable by comparison |
| Separate authz per transport | The dual-path security bug this ADR is built to prevent: a check enforced on one path and forgotten on the other |
| HTTP-only MCP locally | Three more containers in an 8 GB budget. stdio gives the same protocol boundary for free |

## Consequences
**Positive.** The protocol boundary is exercised, tested and debugged locally. Phase 12 adds a transport
and an auth mode, not a protocol. Tools are usable by any MCP client, not only our worker.
**Negative.** Serialization and process-spawn latency on every tool call over MCP — **measured, not
waved away**: the Phase 5 target explicitly budgets 200 ms → 350 ms p95 for the hop. A second place for
things to go wrong.
**Risks.** (1) Authorization divergence between transports — mitigated structurally by the single
middleware chain and the no-unwrapped-entrypoint test. (2) Measured overhead proving unacceptable on a
hot-adjacent tool — in which case that tool stays `InProcess`, which is exactly why both adapters exist.

## Status
Accepted
