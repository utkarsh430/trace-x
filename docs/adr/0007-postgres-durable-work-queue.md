# ADR-0007: PostgreSQL durable work queue for investigations

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 0

## Context
Investigations run 30–120 s, are LLM-bound, cost real money per run, and must survive worker crashes.
The critical requirement: **the case row and the queue row must move in one transaction.** A case
marked `QUEUED` whose queue entry was never written is a silently stuck investigation; a queue entry
without a case is an orphan that will fail repeatedly.

## Decision
A Postgres-backed durable queue consumed with `SELECT … FOR UPDATE SKIP LOCKED`, with lease expiry for
crash recovery. LangGraph checkpoints (`PostgresSaver`) live in the same database, so a resumed worker
picks up mid-investigation rather than restarting — which matters because restarting costs LLM spend.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Celery + Redis | Enqueue is not transactional with the case-state write. Requires an outbox pattern to be correct — at which point Postgres alone is simpler |
| Redis Streams with consumer groups | Same transactional gap. Also weaker durability guarantees than the case data demands |
| Kafka topic as the work queue | Used to *signal* (`investigation.requested.v1`), but Kafka offers no per-message lease, no ad-hoc requeue, and no SQL-inspectable state — all of which operations needs |
| SQS | Cloud-only; breaks the local-first requirement |

## Consequences
**Positive.** Exactly the transactional guarantee needed. The queue is inspectable with the same SQL as
the audit trail — an operator can answer "what is stuck and why" in one query. No extra infrastructure.
**Negative.** Does not scale to very high throughput; polling has latency. Neither matters at
investigation volume (bounded by LLM cost long before Postgres).
**Risks.** Queue-table bloat under high churn. Signal: autovacuum lag. Mitigation: partitioned queue
table with periodic archival.

## Status
Accepted
