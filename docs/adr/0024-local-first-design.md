# ADR-0024: Local-first design with profiled compose

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 0

## Context
The requirement is that the complete core product runs locally, with paid cloud resources needed only
for cloud validation. The reference machine measured at **7 GB free disk** and a **7.7 GB Docker RAM
ceiling**. The full stack — Kafka, Spark, Neo4j, Postgres, Redis, MLflow, Prometheus, Grafana, Jaeger,
Ollama, Next.js — is roughly 18–22 GB of images and would not start, let alone run.

"Just run `docker compose up`" is therefore not achievable for the whole stack on this hardware, and
pretending otherwise would produce a repository that does not work on the machine it was written on.

## Decision
**Profiled compose, where `core` is a complete working product on its own.**

| Profile | Contents | Degraded behaviour when off |
|---|---|---|
| `core` (default) | postgres, redis, gateway, api, worker, ui, 3 MCP servers over stdio | — |
| `streaming` | kafka, spark | Online features only; `X-Feature-Source: ONLINE_ONLY` |
| `graph` | neo4j | `PostgresGraphStore` fallback, reduced and recorded confidence |
| `ml` | mlflow | Serving unaffected (digest-pinned artifacts); training unavailable |
| `obs` | otel, prometheus, grafana, jaeger | `/metrics` still exposed; no dashboards |
| `llm` | ollama + 3B model | Falls back to `DEV`/`CI`; otherwise `make demo` says so plainly |
| `mcp-http` | MCP servers as HTTP services | stdio transport used instead |

Supporting decisions: **MCP over stdio by default** — a real protocol boundary at zero container cost
(ADR-0013); the **`SMOKE` LLM tier** so no API key is needed (ADR-0016); non-standard ports (5442, 6389)
to avoid collisions with other local projects; and `make doctor` checking disk, RAM and ports before
Docker fails confusingly.

**Every degraded mode is visible, never silent.** A decision made without reconciled features says so
in a response header.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Single compose file, everything always up | Does not fit in 7 GB disk / 7.7 GB RAM. Would fail on the reference machine |
| Cloud-only development | Contradicts the local-first requirement and imposes a bill for ordinary development |
| Drop Neo4j and Spark locally, keep them cloud-only | Fits comfortably, but forfeits local development of the two most technically interesting components |
| Local Kubernetes (kind) as the primary environment | More faithful to production, but heavier and slower to iterate on, and another large download. Kept as an optional Phase 11 extra |
| Devcontainer with everything preinstalled | Does not reduce the runtime footprint, which is the actual constraint |

## Consequences
**Positive.** The repository works on the machine it was written on. A contributor runs `make up` and
has a functioning product in minutes with no cloud account and no API key. Degraded modes are designed,
documented and tested rather than discovered.
**Negative.** More compose files and profile combinations to maintain and test. Every optional
dependency needs a real fallback path — genuine extra work, though it also produces genuinely better
resilience. The full profile still needs ~15 GB free, so full-stack E2E cannot run on the reference
machine today.
**Risks.** Profile drift, where a service works only in the full profile and the degraded path silently
rots. Mitigated by CI exercising the `core` profile on every PR and the full profile nightly.

## Status
Accepted
