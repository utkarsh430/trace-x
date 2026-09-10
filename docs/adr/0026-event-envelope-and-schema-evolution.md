# ADR-0026: Event envelope, keying, and backward-only schema evolution

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 0

## Context
Events cross language and runtime boundaries: Python producers, Spark (JVM) consumers, a possible
future Go gateway, and a replay path used by the evaluation harness. Three failures are common and
expensive:

- **Event time confused with processing time**, which silently corrupts every windowed aggregate.
- **A released schema mutated in place**, which breaks consumers replaying historical data.
- **Partition keys chosen for load balancing**, which destroys the ordering guarantees velocity
  features depend on.

## Decision
**A mandatory envelope on every event:** `event_id` (UUIDv7), `event_type`, `schema_version`,
`occurred_at` (**event time**), `ingested_at` (**processing time**), `producer` (`service@semver`),
`trace_id`, `correlation_id`, `idempotency_key`, and `payload`.

Rules:
- **JSON Schema is the source of truth; Pydantic models are generated from it** — never the reverse.
  A schema is a cross-language contract, so deriving it from one language's type system would privilege
  that language. CI regenerates and fails on drift. (APIs run the opposite direction, ADR-0001 area —
  see `docs/API_CONTRACTS.md`.)
- **`occurred_at` drives all windowing and watermarking. `ingested_at` is never used for business
  logic** — only for lag measurement.
- **Keys are the entity whose ordering matters**, never a load-balancing choice. Transactions key on
  `account_id`; device events on `device_id`.
- **Backward-compatible evolution only.** A breaking change means a new `.vN+1` topic, a dual-write
  window, consumer migration, verification of zero lag and zero DLQ for a full retention period, then
  retirement. Released schema files are immutable.
- Changing a topic key or partition count is **breaking** — it alters key→partition affinity and
  silently reorders history.
- `trace_id` is propagated as a Kafka **header and** in the body: headers survive Spark's
  transformations, bodies survive header-stripping intermediaries.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Avro + Confluent Schema Registry | Excellent compatibility enforcement, but another service in a constrained RAM budget, and JSON Schema files in git give the same guarantee with a reviewable diff |
| Protobuf | Efficient and strongly typed, but a worse fit for Spark JSON ingestion and less readable in a git diff during review |
| Pydantic as the source of truth | Privileges Python; a Spark or Go consumer would be a second-class citizen deriving from a Python type system |
| One timestamp field | The single most damaging shortcut available here. Event-time correctness is the entire justification for the warm path (ADR-0002) |
| Forward or full compatibility | Backward compatibility is what replay requires: every consumer must read everything ever written |

## Consequences
**Positive.** Event-time correctness is structural. Schema changes are reviewable diffs with an
automated compatibility gate. Replay works across schema versions. Distributed tracing survives the
whole pipeline.
**Negative.** Envelope overhead on every message. A breaking change is genuinely expensive — a new
topic and a dual-write window — which is intended, but it does slow legitimate evolution. Two
generation directions (schema→Pydantic for events, Pydantic→OpenAPI for APIs) must be explained.
**Risks.** Generated models drifting from schemas if the regeneration check is bypassed. Signal: the
CI drift check. Partition-count changes being treated as configuration rather than as breaking —
mitigated by documenting it explicitly in `docs/EVENT_CONTRACTS.md`.

## Status
Accepted
