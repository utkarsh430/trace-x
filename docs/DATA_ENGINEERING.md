# TRACE-X — Data Engineering Specification

> Authoritative for the medallion pipeline, event-time semantics, the version pin matrix, the
> `SourceAdapter` contract, and Delta table layout.
>
> Data engineering is a **core deliverable** of this project, not an enhancement. Phases 3, 4 and 4B are
> on the mandatory completion path.

---

## 1. Version pin matrix (ADR-0018)

| Component | Pin | Why it is pinned |
|---|---|---|
| Python | 3.12 | Dependency lock target |
| **Java** | **Temurin 17** | **Spark 4.0 supports Java 17/21 only.** Java 25 fails with `UnsupportedClassVersionError` |
| Spark | 4.0.1 | |
| Delta | 4.0.1 | Requires Hadoop 3.4.x with Spark 4.0.1 |
| Hadoop | 3.4.x | Mismatch surfaces as an opaque `NoSuchMethodError` |
| Scala | 2.13 | Spark 4.x baseline |

`make doctor` asserts all six **before any Spark job starts**. Every pin is recorded in every run
manifest; changing one invalidates prior benchmark comparability and requires a manifest-diff note
(ADR-0017).

---

## 2. Medallion architecture

| Tier | Content | Write mode | Guarantees |
|---|---|---|---|
| **Bronze** | Raw event plus envelope, untransformed | append | Nothing dropped, nothing altered. The replayable record of what actually arrived |
| **Silver** | Deduplicated on `event_id`, watermarked, typed, validated, PII-tokenized | append + late routing | Event-time correct. Late events routed, never lost |
| **Gold** | Event-time windowed aggregates, entity profiles, training features | upsert | Authoritative feature values. Reconciles the Redis online store |

**Bronze is never rewritten.** A parsing bug is fixed by reprocessing Bronze into Silver, not by editing
Bronze — that is the entire point of keeping it.

### Silver transformations
- Validate against the event JSON Schema; failures route to `<topic>.dlq` with the full envelope.
- `dropDuplicatesWithinWatermark(["event_id"])` — exactly-once semantics within the watermark.
- Type coercion to `CanonicalTransaction`, with `field_coverage` propagated from the source adapter.
- PII tokenization per the field policy; `trust_tier` stamped and thereafter immutable.
- Late-arrival tagging: events beyond the watermark go to `late_events` **and are counted**.

### Gold aggregates
Event-time windowed velocity (1m/5m/1h/24h), amount statistics per account, distinct-entity counts,
merchant risk aggregates, and entity profiles for the graph sync job.

---

## 3. Event-time semantics

The single most consequential rule: **`occurred_at` drives all business logic. `ingested_at` is used
only to measure lag.** Confusing them silently corrupts every windowed aggregate.

| Concern | Implementation |
|---|---|
| Watermark | `withWatermark("occurred_at", "10 minutes")` on every stateful stage |
| Deduplication | `dropDuplicatesWithinWatermark(["event_id"])` |
| Out-of-order | Normal and expected. All aggregation is event-time windowed |
| Late data | Beyond the watermark → `late_events` Delta table + counter. **Never silently dropped** |
| Stateful ops | `flatMapGroupsWithState` with explicit TTL for session and velocity state |
| Checkpoints | Per query under `_checkpoints/{query}/` |
| Replay | `availableNow` trigger for bounded batch reprocessing from any offset |
| Local sizing | `spark.sql.shuffle.partitions=8`, driver 2g, executor 2g — sized for an 8 GB VM |

**A late event that vanishes is indistinguishable from a bug.** That is why `late_events` is a table
with a metric rather than a dropped record.

---

## 4. Online / offline feature parity

Redis (hot) and Spark (warm) compute the same features by two different mechanisms — an accepted cost of
ADR-0002. The risk is silent divergence, so it is measured rather than trusted:

- A **single feature definition module** declares each feature's semantics and `required_fields`.
- The Gold → Redis **reconciliation job** writes authoritative values back to the online store.
- **`feature_parity_drift{feature}`** is a first-class monitored metric.
- An automated **parity test** (`pytest -m parity`) replays a 100 k-event stream through both paths and
  asserts agreement within tolerance. Tolerance accounts for HyperLogLog's ~0.81% error on
  cardinality features and is documented per feature.
- **Widening a tolerance to make the test pass is prohibited** — it converts a detected bug into a
  hidden one. Investigate the divergence instead.

When the `streaming` profile is off, responses carry `X-Feature-Source: ONLINE_ONLY` so degradation is
visible rather than silent.

---

## 5. SourceAdapter contract (ADR-0022)

Every inbound dataset implements:

```python
describe()      -> SourceProfile { name, version, field_coverage, row_count, digest }
to_canonical(b) -> DataFrame[CanonicalTransaction]
label_column()  -> str
```

`CanonicalTransaction` carries `source_dataset`, `source_row_id` and **`field_coverage`** — the set of
canonical fields this source actually supplies.

**Every feature declares `required_fields`. A feature whose inputs are not covered evaluates to
`UNAVAILABLE` and propagates as null/absent — never imputed to zero, never defaulted.**

| Adapter | Coverage | Notes |
|---|---|---|
| `GeneratorAdapter` | Full | Track A |
| `IeeeCisAdapter` | **Partial** — no true merchant id, no lat/lon, obfuscated timedeltas | Track B. `V*/C*/D*/M*` carried as a typed `opaque_features` map |

Both pass `SourceAdapterConformanceSuite`, which asserts declared coverage matches actually-produced
columns.

**Track B reuses the medallion unmodified.** IEEE-CIS loads through the *same* Bronze→Silver→Gold code
in `availableNow` batch mode. No parallel pipeline exists — and Phase 4B proves it with an assertion
that `git diff` of the stream package across the phase is **empty**.

---

## 6. Delta table layout (ADR-0015)

**No layout is prescribed.** Layout is table DDL, chosen per environment by measurement, and local and
Databricks answers are permitted to differ because the runtimes differ in capability.

| Option | Local OSS Delta 4.0.1 | Databricks |
|---|---|---|
| `PARTITIONED BY (event_date)` | viable; small-file skew risk | discouraged for new tables |
| Partition + `ZORDER BY` | viable; manual `OPTIMIZE` | superseded by liquid clustering |
| `CLUSTER BY (…)` liquid | **supported**; manual `OPTIMIZE` | supported |
| `CLUSTER BY AUTO` + predictive optimization | **not available** (needs Unity Catalog) | **candidate default** |

`benchmarks/delta_layout/` runs the **real Gold query mix** — point lookup by `account_id`, time-range
scan, training-set extract — at ≥ 10 M rows, recording **files scanned, bytes scanned, wall-clock and
`OPTIMIZE` cost**. ADR-0015 is finalized from those numbers and cites them. A CI check asserts the ADR
references non-empty benchmark output, so it cannot be accepted before its evidence exists.

Nothing in the streaming code hardcodes a layout.

---

## 7. Data quality and lineage

| Control | Where |
|---|---|
| Schema validation at produce time | Producer — an invalid message is never published |
| Schema validation on receipt | Consumer — the network and peer versions are not trusted |
| Delta schema enforcement | Write time — producer drift fails the write |
| Dedup | `event_id`, Redis (hot) and Spark (warm) |
| Late tracking | `late_events` table + `late_events_total` |
| Poison messages | `<topic>.dlq` after 3 failures, with full context |
| Lineage | `source_dataset`, `source_row_id`, `producer` (`service@semver`), `trace_id` on every row |
| Reproducibility | Delta time travel by table version, recorded in the run manifest |

---

## 8. Databricks parity

The Spark code is **identical** to local. Notebooks are thin entrypoints calling `trace_core.stream.*` —
this is the guard against "works on Databricks, unrunnable locally", which would defeat the local-first
requirement and make the pipeline untestable in CI.

Jobs: `bronze_ingest` (continuous), `silver_transform`, `gold_features`, `graph_sync`,
`training_pipeline`, `drift_monitor`, `external_validation` (Track B).

Unity Catalog: `tracex.{bronze,silver,gold,ml,external}`, with `groundtruth` in a **separately-granted**
catalog mirroring the local Postgres isolation (ADR-0004). Table layout is re-decided per ADR-0015
rather than inherited.
