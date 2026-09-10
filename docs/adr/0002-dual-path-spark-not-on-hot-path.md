# ADR-0002: Dual-path architecture — Spark Structured Streaming is not on the hot path

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 0

## Context
An authorization decision must return in tens of milliseconds. Spark Structured Streaming micro-batch
latency is 100 ms–seconds, and materially worse on a single-node local Spark inside an 8 GB VM.

But event-time correctness — watermarking, exact deduplication, late-data handling, out-of-order
aggregation — is exactly what Spark is good at and what a Redis counter is bad at. Redis sliding-window
counters are fast and *approximate*: they drift under duplicate, late and out-of-order events.

Conflating the two produces either a slow product or wrong features. This is the most common failure in
systems shaped like this one.

## Decision
Split into three paths with distinct budgets:

- **Hot** (sync, p99 < 100 ms): gateway → Redis online features → rules → ML → ensemble. No Spark, no LLM.
- **Warm** (streaming, seconds): Kafka → Spark → Bronze/Silver/Gold Delta, event-time correct, and
  **reconciles the Redis online store**.
- **Cold** (async, 30–120 s): investigations via LangGraph.

Divergence between online and offline features is a **monitored metric** (`feature_parity_drift`) with
an automated parity test — not a hidden bug.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Spark on the hot path | Cannot meet a 100 ms p99. Would require abandoning the synchronous decision product |
| Redis only, no Spark | Loses event-time correctness, replay, training-data lineage and dedup guarantees. Counters drift silently with no way to detect it |
| Flink instead of Spark | True low-latency streaming, but loses Delta/Databricks integration and the PySpark/Databricks skill demonstration that is a project goal |
| Kafka Streams | JVM-only; would fragment the codebase and duplicate feature definitions in a third language |

## Consequences
**Positive.** Each path meets its actual budget. Spark owns correctness and has a real job rather than a
decorative one. Feature drift becomes observable.
**Negative.** **Two implementations of the same feature logic** — a genuine, accepted cost. Mitigated by
a single feature *definition* module and an automated parity test, never by discipline alone.
**Risks.** The two implementations drift apart. Signal: `feature_parity_drift` exceeding tolerance, or a
parity test that needs its tolerance widened to pass.

## Status
Accepted
