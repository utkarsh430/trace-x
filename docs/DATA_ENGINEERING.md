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

`make doctor` asserts all six, and from Phase 3 each is a **required** check: Java by running the JDK
Spark's launcher would use, pyspark and Delta from installed package metadata, and Hadoop and Scala from
the jar names pyspark actually bundles. The Delta and Kafka-connector jars are pinned by SHA-256 and
verified before a session may use them, and `trace_core.stream.session.build_session` repeats every
check before a JVM starts, then asks the running JVM for its versions (ADR-0045). Every pin is recorded
in every run manifest; changing one invalidates prior benchmark comparability and requires a manifest-diff note
(ADR-0017).

---

## 2. Medallion architecture

> **Being superseded in Phase 3.** The approved plan (`docs/PHASE3_PLAN.md` §2–§4) replaces parts of
> §2–§4 below: Silver keeps every accepted event (late events are tagged and copied to `late_events`,
> not removed) and is exactly deduplicated by a declared identity; Spark rejects go to a Delta
> quarantine table; Gold is computed in batch; and reconstruction rebuilds the online store's
> primitives rather than writing feature values back. Each section is rewritten by the step that
> implements it, and until then the plan is authoritative where the two differ.

| Tier | Content | Write mode | Guarantees |
|---|---|---|---|
| **Bronze** | Raw event plus envelope, untransformed | append | Nothing dropped, nothing altered. The replayable record of what actually arrived |
| **Silver** | Deduplicated, typed, validated, `trust_tier` carried | append | Event-time correct. Late events kept and tagged, never lost |
| **Gold** | Event-time windowed aggregates, entity profiles, training features | upsert | Authoritative feature values. Reconciles the Redis online store |

**Bronze is never rewritten.** A parsing bug is fixed by reprocessing Bronze into Silver, not by editing
Bronze — that is the entire point of keeping it.

### Silver transformations
- Validate against the event JSON Schema; failures route to `<topic>.dlq` with the full envelope.
- `dropDuplicatesWithinWatermark(["event_id"])` — exactly-once semantics within the watermark.
- Type coercion to `CanonicalTransaction`, with `field_coverage` propagated from the source adapter.
- `trust_tier` stamped and thereafter immutable. **No PII tokenization is performed or claimed:** every
  current source carries synthetic or already-anonymised identities, and tokenizing them would break joins
  with the online store's keys. An explicit tokenization and privacy design is required before any source
  containing real PII is admitted (`docs/PHASE3_PLAN.md` §3, Q9).
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
  From Phase 2 that declaration is machine-readable (ADR-0032): every feature carries a `semantics`
  object naming its entity, stream, window and aggregation, so Phase 3 compiles the offline version
  from the declaration rather than re-deriving the intent from prose. A shared module that shared only
  Python closures would not have helped — a closure over a Redis client cannot be run by Spark.
- The Gold → Redis **reconciliation job** writes authoritative values back to the online store.
- **`feature_parity_drift{feature}`** is a first-class monitored metric.
- An automated **parity test** (`pytest -m parity`) replays a 100 k-event stream through both paths and
  asserts agreement within tolerance. Tolerance accounts for HyperLogLog's ~0.81% error on
  cardinality features and is documented per feature.
- **Widening a tolerance to make the test pass is prohibited** — it converts a detected bug into a
  hidden one. Investigate the divergence instead.
- **Most features are compared by equality, not tolerance.** ADR-0034 stores distinct counts exactly
  wherever cardinality is bounded by one entity's own behaviour, so five of the seven distinct counts
  have a parity tolerance of *zero*; only the two whose cardinality is bounded by a sharing
  population are estimates. The robust z-score is exact since ADR-0046 declared its bounded sample to
  be the definition. How each feature is compared -- equality, a `1e-9` floating-point tolerance, or
  the per-stratum bound for an estimate -- is declared on the feature (`FeatureSpec.parity`), never a
  global allowance.

When the `streaming` profile is off, responses carry `X-Feature-Source: ONLINE_ONLY` so degradation is
visible rather than silent.

### The backfill contract (ADR-0044)

The online store holds **only** the state the released features declare — `trace_core.features.state_plan`
derives what is written, for which entities, and with what retention from every `FeatureSpec.semantics`.
Nothing is retained for a feature that has not been released, so a feature released later must not
pretend its history exists:

```
new feature → required state declared in FeatureSpec.semantics
            → PLAN derives its primitives and retention
            → backfill (Phase 3: durable replay from Bronze/Silver into the store) or warm-up
            → INSUFFICIENT_HISTORY, decisions carry history_incomplete, until the lookback is complete
            → /readyz reports "complete since …" → feature enabled
```

The store records a **completeness epoch**; a window that began before it is incomplete and reads as
`INSUFFICIENT_HISTORY`, one that began after it is complete and an absent key is a measured zero. The
Gold → Redis reconciliation job is what makes backfill possible: it is the only path by which a store
younger than a feature's lookback can become complete without waiting the lookback out.

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

## 5b. The Track A generator (Phase 1)

`data/generator/` produces the synthetic dataset the `GeneratorAdapter` consumes. Three properties
matter downstream.

**Determinism (ADR-0029).** Stdlib `random.Random`, not NumPy: NumPy reserves the right to change its
stream between versions, and a dataset digest that moves on a dependency bump is indistinguishable
from a fabricated one. Randomness is drawn from **named substreams** seeded by BLAKE2b of
`(seed, namespace, key)`, so generation order is not part of the contract — which is what lets fraud
scenarios be injected without reshuffling the legitimate rows around them.

**The digest is over canonical row JSON in emission order**, never over output bytes. A file digest
would change with a compression setting or a pyarrow upgrade and present an encoding change as a data
change, training everyone to ignore digest mismatches.

**A realistic baseline, because fraud is only detectable as a departure.** Diurnal and weekly volume
curves with a genuine overnight trough, Zipf merchant popularity, geography clustered on population
centres, lognormal amounts with a long right tail, and per-account habitual merchants and devices. A
flat baseline would make every injected scenario trivially separable and every reported metric
meaningless.

Output topics are the three released ingress contracts: `tx.raw.v1`, plus `identity.events.v1` and
`device.events.v1`, which exist because two fraud scenarios are *defined* by non-transaction events
(see `docs/FRAUD_SCENARIOS.md`). Sinks are JSONL, Parquet, Kafka or none; a row is encoded exactly
once and those same bytes are validated, digested and written.

`eval-v1` is frozen by `eval/track_a/eval-v1.manifest.json`. The dataset itself is gitignored —
datasets are never committed — so the manifest carries the seed, the full config, the generator
version and the digest the combination must produce. `pytest -m slow` regenerates it in full and
asserts the digest.

Measured results, with resolvable run ids: `benchmarks/generator/REPORT.md`.

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
