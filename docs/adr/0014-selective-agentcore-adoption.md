# ADR-0014: Selective adoption of AWS Bedrock AgentCore

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 12

## Context
Bedrock AgentCore became generally available in October 2025 and offers composable services: Runtime,
Gateway, Memory, Browser, Code Interpreter, Identity and Observability. The brief asks to use AgentCore
"where useful."

The temptation is to adopt all of it because it exists. Each component must be evaluated against a
requirement this project actually has.

## Decision
**Adopt:**
- **Gateway** — it federates the three MCP servers that already exist and already pass the transport
  parity suite (ADR-0013), adding IAM/SigV4 authentication and VPC reach. Real gain, low risk, because
  it adds a transport rather than a protocol.
- **Observability** — native agent tracing that complements OpenTelemetry.

**Reject, with reasons:**
- **Runtime** — a cloud-hosted agent runtime would break the local-first requirement for the entire
  agent tier. LangGraph runs identically on a laptop and in EKS; that portability is worth more than a
  managed runtime.
- **Memory** — investigation and case memory must be **SQL-queryable by the evaluation harness**.
  Metrics such as evidence precision are computed by joining evidence records to ground truth. A managed
  memory service that cannot be joined in SQL would break the benchmark.
- **Identity** — the capability-token model (per investigation, per agent, scoped to `allowed_tools`
  and an entity set) is finer-grained than what Identity provides and is already audited end to end.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Adopt all of AgentCore | Breaks local-first (Runtime) and the evaluation harness (Memory). Adoption because a service exists is not a decision |
| Adopt none | Forfeits genuine value from Gateway's IAM-authenticated MCP federation, and the brief explicitly asks for AgentCore where useful |
| Gateway only, no Observability | Marginal; Observability is low-cost and complements existing tracing |

## Consequences
**Positive.** Cloud-native tool federation with IAM. Local-first preserved. The evaluation harness keeps
full SQL access to case memory. Each adoption and rejection is defensible in an interview.
**Negative.** A partially-adopted managed platform means two mental models — some agent infrastructure
is ours, some is AWS's. Gateway is an additional cloud dependency in the Phase 12 window.
**Risks.** AgentCore evolving such that Runtime becomes compelling. Signal: a local-capable runtime
mode. That would warrant a superseding ADR, not a silent change.

## Status
Accepted
