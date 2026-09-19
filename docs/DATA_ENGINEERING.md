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
> quarantine table; every table is created from its declaration before a query writes it (ADR-0048); Gold
> is computed in batch; and reconstruction rebuilds the online store's
> primitives rather than writing feature values back. Each section is rewritten by the step that
> implements it, and until then the plan is authoritative where the two differ.

| Tier | Content | Write mode | Guarantees |
|---|---|---|---|
| **Bronze** | Raw event plus envelope, untransformed | append | Nothing dropped, nothing altered. The replayable record of what actually arrived |
| **Silver** | Validated, typed, exactly deduplicated by each topic's identity, `trust_tier` carried | MERGE on the identity | One row per real event. Late events kept and tagged, never lost. Every Bronze row accounted for (ADR-0053) |
| **Gold** | Event-time-complete point-in-time contexts per scored transaction, and primitive state shaped like the online store's (ADR-0055) | batch build: one MERGE per table from pinned Silver versions | Exact to the declared semantics in event-time-complete mode (three-way conformance). The oracle for parity (Step 8) and the source for Redis reconstruction (Step 9) |

**Bronze is never rewritten.** A parsing bug is fixed by reprocessing Bronze into Silver, not by editing
Bronze — that is the entire point of keeping it.

### Silver transformations (ADR-0053)
- **Validation.** Each Bronze value is validated with the topic's generated contract model, the one
  producers validate with (strict, `extra="forbid"`). A record Silver cannot admit goes to
  `silver.quarantine` with its raw bytes and a reason: `null_value`, `not_log_append_time`,
  `invalid_event`, `unrepresentable`, `future_skew` or `identity_conflict`. Nothing goes to a
  `.dlq` topic.
- **Exact deduplication**, by each topic's declared identity. It is deterministic within the
  micro-batch, then a MERGE on the identity, then a uniqueness assertion; no watermark bounds it.
  - A delivery with the same content is recorded in `silver.duplicates`.
  - One with different content is an identity conflict.
  - `tx.scored.v1` keeps the recorded delivery, not a retry that happened to arrive first.
- `trust_tier` stamped and thereafter immutable. **No PII tokenization is performed or claimed:** every
  current source carries synthetic or already-anonymised identities, and tokenizing them would break joins
  with the online store's keys. An explicit tokenization and privacy design is required before any source
  containing real PII is admitted (`docs/PHASE3_PLAN.md` §3, Q9).
- **Lateness is a stored fact** (timing semantics v1, `trace_core.stream.timing`).
  - `arrival_delay_ms` is LogAppendTime − `occurred_at`.
  - `is_late` is true when that exceeds 600,000 ms, and null for a backfill replay.
  - A late event stays canonical; `silver.late_events` holds exactly the canonical rows that are late.
- **Conservation.** Every Bronze row a Silver checkpoint consumed is a canonical row, a recorded
  duplicate or a quarantine row (`python -m services.stream.silver conservation`).

### Gold (ADR-0055)
- **What a build reads.** `silver.tx_scored_v1`, `silver.identity_events_v1` and
  `silver.tx_authorization_v1`, each at one pinned Delta version. Every transaction the gateway
  accepted counts, including those the online store failed to record. `silver.tx_raw_v1` is not a
  source: reading it as well would count a replayed transaction twice.
- **What it writes.**
  - Primitive state, shaped by the online store's plan: `gold.observations`, `gold.minute_buckets`
    and `gold.distinct_buckets`.
  - Point-in-time contexts: `gold.tx_windows`, `gold.tx_profiles` and `gold.tx_previous`.
  - Build records: `gold.builds`, append-only, with each build's lag behind the Silver commits it
    covers.
- **Contexts, not finished values.** Gold stores what a feature context holds, and the shared feature
  definitions evaluate it, so there is one meaning across the reference, Redis and Gold. Gold claims
  no completeness itself.
- **How a build is replayed.** Its plan is recorded in the `gold_build` checkpoint before any table is
  touched, so a crashed build replays with the same pinned versions, and each table's MERGE is
  idempotent.
- **Cost.** Every build is a full rebuild, a recorded Negative consequence. Freshness is measured in
  Step 13.
- **Later phases, not built:** training features and graph-sync profiles.

---

## 3. Event-time semantics

The single most consequential rule: **`occurred_at` drives all business logic. `ingested_at` is used
only to measure lag.** Confusing them silently corrupts every windowed aggregate.

| Concern | Implementation |
|---|---|
| Watermark | None in Silver, whose deduplication is bounded by the table, not a watermark (ADR-0053). A stateful streaming stage declares its own when one is added |
| Deduplication | By each topic's declared identity (`deploy/kafka/topics.yaml`): deterministically within each micro-batch, then an insert-only MERGE through the query's checkpoint. Both are needed: Delta inserts every duplicate a MERGE source carries (ADR-0048) |
| Out-of-order | Normal and expected. All aggregation is event-time windowed |
| Late data | Arrival delay over 600 s (timing semantics v1) → `is_late` on the canonical row, which `silver.late_events` also holds. **Never dropped** |
| Stateful ops | `flatMapGroupsWithState` with explicit TTL for session and velocity state |
| Checkpoints | `<lake root>/_checkpoints/<query>/v<N>/`, beside the tiers: each version owns its Delta app id and is the only way its query writes (ADR-0048) |
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

`benchmarks/delta_layout/` (`make bench-layout`) runs the **real Gold query mix** at ≥ 10 M rows
shaped like `gold.observations`: the two `gold.read_context_rows` reads (one transaction's subject row
by id, and the full context extract parity and training read) and ADR-0015's two further shapes that
no Gold reader issues yet (point lookup by `account_id`, 24-hour time-range scan). It builds a no-layout
control, a compacted control, `event_date` partitioning (a generated column, so readers keep filtering
on `occurred_at`), partitioning plus `ZORDER BY (account_id)`, `stream` partitioning and liquid
`CLUSTER BY (account_id, occurred_at)`, each filled by the MERGE Gold's writer issues. It records
**files and bytes selected, bytes and records read (ADR-0048 §8), wall-clock, and write and
`OPTIMIZE` cost**, refuses a missing metric or any difference in results between layouts, and ranks
by a rule declared before measurement (`benchmarks/delta_layout/spec.py`). ADR-0015 is finalized from
those numbers and cites them. A unit test keeps the ADR Proposed until `benchmarks/delta_layout/REPORT.md`
cites a committed, publishable run of at least ten million rows.

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

## 8. Databricks strategy (documented in Phase 3, deployed in Phase 12)

Nothing is deployed in Phase 3. ADR-0025 governs the one funded Phase 12 window: complete Terraform and
Databricks Asset Bundles, applied once, then destroyed. This section records how what Phase 3 built maps
onto Databricks, so that window adds deployment, not redesign. Decisions that need a Databricks runtime to
settle stay open here and get their own ADRs in Phase 12.

**The Spark code is identical to local.** Jobs are thin entrypoints over `trace_core.stream.*`, the same
modules `services/stream/*` call. That is the guard against "works on Databricks, unrunnable locally",
which would defeat the local-first requirement and make the pipeline untestable in CI.

### What maps directly
- **Jobs**, named as the local pipelines (snake_case, ADR-0048 U9):
  - `bronze_ingest`: one continuous query per released topic;
  - `silver_transform`: one query per topic, `silver_transform_<topic_name>`;
  - `gold_build`: a batch build over pinned Silver versions (ADR-0055).
  - The Redis reconstruction job arrives with Step 9.
  - Planned for later phases: `graph_sync`, `training_pipeline`, `drift_monitor`, `external_validation`
    (Track B).
- **Tables**, with the same identifiers as local, under Unity Catalog `tracex.<tier>.<table>`:
  - `tracex.bronze.<topic_name>`;
  - `tracex.silver.<topic_name>`, `tracex.silver.duplicates`, `tracex.silver.late_events` and
    `tracex.silver.quarantine`;
  - `tracex.gold.*` (ADR-0055);
  - `groundtruth` in a separately granted catalog, mirroring the local PostgreSQL isolation (ADR-0004).
- **Declarations and checkpoints.** The ADR-0048 conventions are unchanged:
  - every table is created from its declaration before a query writes it, and written only through
    `OpenedCheckpoint`;
  - checkpoints live per query and version (`_checkpoints/<query>/v<N>/`) on the job's storage
    location.
- **Loss refusals are identical.** `failOnDataLoss=true`, and every loss-tolerant reader option and
  session setting is refused.
- **Correctness-bearing layout travels as is.** `silver.late_events` is partitioned by `silver_topic`
  because concurrent topic queries must not conflict (ADR-0053 §1), whatever the layout benchmark
  decides.
- **Dependencies outside the lake.** The observation-log coverage rule needs the `producer_sessions`
  table (PostgreSQL, RDS on AWS) and Bronze. Kafka is MSK.

### What does not travel, or is decided in Phase 12
- **Local-only retention.** Step 11's audited retention floor, and Silver's `ignoreDeletes` allowance on
  Bronze sources, are local-development overrides (Q8). Production Bronze stays append-only, the
  production retention contract (`docs/EVENT_CONTRACTS.md`) is unchanged, and no Databricks job enables
  the retention path.
- **Table layout** is re-decided per environment (ADR-0015). Candidates are liquid clustering, and
  `CLUSTER BY AUTO` with predictive optimization where the runtime supports it. The local benchmark
  result is not inherited.
- **Table protocol and managed properties.** Locally, tables use the minimum Delta protocol with an
  allow-list of properties (ADR-0048 (k)). A Databricks runtime may set managed properties or defaults,
  such as deletion vectors or row tracking, which the drift check refuses until they are explicitly
  allowed. Phase 12 decides each allowance with evidence.
- **Runtime version.** The local pins are Spark 4.0.1, Delta 4.0.1, Scala 2.13 and Java 17
  (ADR-0018). Phase 12 selects a Databricks runtime matching them, and records any difference as a
  manifest-diff note, as CLAUDE.md §5 requires.
