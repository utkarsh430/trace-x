# ADR-0006: Kafka (KRaft) as the event backbone

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 0

## Context
The event backbone must provide: replay from offset (so an evaluation run can be reproduced against the
exact event sequence), per-key ordering (per-account velocity is order-sensitive), partitioned parallel
consumption, and a first-class Spark Structured Streaming source. It must also fit in a constrained
local RAM budget.

## Decision
Apache Kafka in **KRaft mode** — no ZooKeeper — single broker locally, MSK in the cloud. Topic
configuration, keying and evolution rules are in `docs/EVENT_CONTRACTS.md`.

Keys are chosen by the entity whose **ordering** matters, never for load balancing.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| **Redpanda** | Genuinely lighter (~300 MB vs ~600 MB), no JVM, Kafka-API compatible, lower latency. Rejected **only** because Spark's Kafka connector is the canonical, best-tested path and MSK is the cloud target. **This remains the documented fallback if the RAM ceiling binds** — the switch is a compose change, since the API is compatible |
| RabbitMQ / SQS | No replay from offset, no partition ordering, no Spark streaming source. Replay is a hard requirement for reproducible evaluation |
| Postgres as a queue for events too | Used for the *work queue* (ADR-0007), but cannot serve high-throughput event streaming or Spark consumption |
| Kafka with ZooKeeper | Deprecated; an extra container and extra RAM for no benefit |

## Consequences
**Positive.** Replay makes evaluation reproducible. Per-key ordering is guaranteed. Canonical Spark
integration. Cloud path is MSK with no code change.
**Negative.** JVM memory footprint is significant in an 8 GB VM. Partition-count changes are breaking
(they alter key→partition affinity), so capacity must be planned up front.
**Risks.** RAM pressure making the `streaming` profile unusable locally. Signal: OOM-killed containers.
Mitigation: the Redpanda swap is pre-analysed and API-compatible.

## Status
Accepted
