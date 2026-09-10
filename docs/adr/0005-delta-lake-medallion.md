# ADR-0005: Delta Lake medallion architecture

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 0

## Context
The warm path needs a storage layer that supports ACID writes from a streaming job, schema enforcement,
time travel for reproducible training sets, and identical semantics locally and on Databricks. Training
sets must be reconstructible **exactly** — a benchmark whose training data cannot be reproduced is not
a benchmark.

## Decision
Delta Lake 4.0.1 with a three-tier medallion:

| Tier | Content | Guarantees |
|---|---|---|
| **Bronze** | Raw events plus envelope, append-only | Nothing dropped, nothing altered; the replayable record |
| **Silver** | Deduplicated (`event_id`), watermarked, typed, validated, PII-tokenized | Event-time correct; late events routed to `late_events`, never lost |
| **Gold** | Event-time windowed aggregates, entity profiles, training features | Authoritative feature values; reconciles Redis |

Local storage is the filesystem; cloud is S3. The Spark code is identical (ADR-0002). Table **layout**
is deliberately not decided here — see ADR-0015.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Parquet without a table format | No ACID, no schema enforcement, no time travel. Concurrent stream writes corrupt it |
| Apache Iceberg | Technically comparable and arguably better catalog-side, but Databricks is a stated project target and Delta is its native format |
| Apache Hudi | Strong for upserts; weaker Databricks integration and a smaller local-dev story |
| Postgres for everything | Does not scale to the training-set volume and gives no columnar scan performance or time travel |

## Consequences
**Positive.** Reproducible training sets by table version. Schema enforcement catches producer drift at
write time. Identical local and cloud semantics.
**Negative.** Requires the JVM and a strict Spark/Delta/Hadoop version alignment (ADR-0018) — a real
source of opaque failures. Small-file accumulation needs active maintenance locally.
**Risks.** Version drift producing `NoSuchMethodError`. Mitigated by `make doctor` asserting the pin
matrix before any Spark job starts.

## Status
Accepted
