# ADR-0048: What Delta 4.0.1 actually does, and the lake conventions built on it

- **Status:** Proposed (Phase 3 Step 3; revised after critic review, integrated by the lead). The naming decision below was made by the user on 2026-09-14 (U9).
- **Date:** 2026-09-13
- **Phase:** 3 (Step 3 of `docs/PHASE3_PLAN.md`)
- **Supersedes / Superseded by:** — . Refines how ADR-0005's tier guarantees are enforced; it does
  not change the tiers. Leaves ADR-0015 open: this spike records what the local runtime *can* do,
  not which layout performs best.

## Context

Bronze, Silver and Gold have not been built yet, and several of their designs rest on Delta
behaviours the plan took from documentation:
- idempotent `foreachBatch` writes;
- exact canonical deduplication by insert-only MERGE (§4.2);
- append-only Bronze;
- retention;
- the layout options;
- Q6's "minimum protocol features".

The plan's rule (§1) is that a claim resting only on documentation is re-verified by the step that
depends on it. Step 3 does that for Delta before anything depends on it. It also fixes the
conventions every table and every streaming query will follow.

Everything below was executed on the pinned toolchain through `build_session` (ADR-0045), at
`local[2]` with one session per test module, and is asserted by
`tests/stream/test_delta_capabilities.py` (marker `stream`). No performance was measured: none of
these runs has a `run_id`, and nothing here is a benchmark. The counts the tests compare are either
derived from how the test data was built or read back from Delta or Spark itself.

The first draft of this spike was reviewed by the critic. Several of its claims were wrong or
overstated, and are corrected here, each with a new or strengthened test:
- **(g):** a *running* query can silently lose rows.
- **(j):** `filesSize` is the size of the files selected, not bytes read.
- **(k):** constraints can be set at creation.
- **Baseline features:** they are not universal.

Where a statement is inferred rather than observed, it says so.

### Observed behaviour of Delta 4.0.1

| Item | Observed | Test |
|---|---|---|
| (a) `delta.appendOnly=true` | `DELETE`, `UPDATE`, and even `DELETE ... WHERE false`, fail with `DELTA_CANNOT_MODIFY_APPEND_ONLY` and commit nothing. Insert-only MERGE is **allowed**. | `test_a_append_only_refuses_delete_and_update_but_admits_insert_only_merge` |
| (b) `txnAppId` / `txnVersion` | A write whose version is at or below the highest recorded for its app id is skipped with no commit, including a *lower* version. A new app id writes. | `test_b_a_replayed_transaction_version_is_skipped_and_a_new_app_id_writes` |
| (b) new checkpoint, old app id | **Silent loss.** The new checkpoint's batch 0 is reported as a completed batch with zero input rows and no error, and the new rows never reach the target. Resuming the original checkpoint writes them. | `test_a_new_checkpoint_reusing_an_old_app_id_silently_loses_its_batches` |
| (b) two writes, one batch | A second write to the same table with the same app id and batch id is skipped silently. | `test_b_a_second_idempotent_commit_to_one_table_in_one_batch_is_skipped_and_refused` |
| (b) crash windows | Losing only `commits/<n>`, with new rows already in the source, is safe: Spark replays batch `n` from its recorded offsets, Delta skips it, and batch `n+1` takes the new rows. Losing `offsets/<n>` too is not: Spark re-plans batch `n` over the wider range and Delta skips all of it. | `test_b_a_lost_commit_marker_replays_safely_and_lost_offsets_are_refused` |
| (b) native streaming sink | Delta's own sink uses the checkpoint's persistent query id (`<checkpoint>/metadata`) as its app id and honours the `userMetadata` writer option. A new checkpoint over the same target **writes again**, duplicating rows in an append sink; it does not skip them. | `test_b_the_native_delta_sink_keys_idempotency_on_the_checkpoint_query_id` |
| (b) MERGE idempotency | MERGE (and, per the critic, a plain append) reads its transaction identity from session configuration (`spark.databricks.delta.write.txnAppId` / `txnVersion`). **While `write.txnVersion.autoReset.enabled` is off (the default)** Delta leaves both keys set after the commit, and a later, different MERGE on the session is silently skipped. With the flag on, `txnVersion` is cleared after the commit but `txnAppId` is not. | `test_b_merge_idempotency_rides_on_session_conf_that_delta_leaves_set` |
| (b) the `foreachBatch` session | The session a batch function receives is not the session that started the query (observed). That it is a separate clone per query is Spark's documented design, inferred rather than observed. | `test_b_the_checkpoint_convention_pairs_the_app_id_with_its_checkpoint` |
| (b) a target that lost delivered rows | `RESTORE TABLE ... TO VERSION AS OF` keeps both the table id and the transaction identifiers, so a checkpoint's batch ids still look delivered while the rows are gone; only the history's `RESTORE` commit shows it. A table deleted and recreated at the same path gets a new table id. | `test_b_a_target_that_lost_delivered_rows_refuses_the_resume` |
| (c) insert-only MERGE across batches | Deduplicates against rows already in the table; the first write wins. | `test_c_insert_only_merge_deduplicates_across_batches_and_the_first_write_wins` |
| (d) duplicate keys in a MERGE source | **Contradicts the plan.** An insert-only MERGE inserts *every* duplicate of a new key, without error. Only a MERGE with a matched clause fails (`DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`), and only when the duplicates match an existing target row. | `test_d_duplicate_source_keys_are_inserted_twice_by_an_insert_only_merge` |
| (e) NOT NULL / CHECK violation | The whole write fails, whether an append or a MERGE, and no commit is made. | `test_e_a_constraint_violation_fails_the_whole_write` |
| (f) VACUUM retention | With `retentionDurationCheck` enabled (Delta's default, which the session keeps), VACUUM refuses a retention below 168 hours. **That floor is the table's `delta.deletedFileRetentionDuration`**, not a constant: set it to one hour and `RETAIN 1 HOURS` is accepted. | `test_f_vacuum_refuses_retention_below_the_tables_own_retention` |
| (g) a **running** query, log cleaned past its next version | **Silent loss with `failOnDataLoss=false`.** The query completes without error and rows present in the source before it started never arrive. With `failOnDataLoss=true` it fails with `DELTA_MISSING_FILES_UNEXPECTED_VERSION`. No pre-start check can see this case. | `test_g_a_running_query_with_fail_on_data_loss_false_silently_loses_rows` |
| (g) a **restart**, log cleaned past its checkpoint | The query fails with `DELTA_LOG_FILE_NOT_FOUND_FOR_STREAMING_SOURCE`, and `failOnDataLoss=false` does not change that at restart. Delta's message advises deleting the checkpoint. | `test_g_a_restart_behind_the_retained_log_fails_loudly_and_the_guard_refuses_first` |
| (g) data files vacuumed | By default the query fails loudly (`FAILED_READ_FILE.FILE_NOT_EXIST`). **With `spark.sql.files.ignoreMissingFiles=true` it completes, advances its checkpoint past the lost versions and never delivers their rows**; a later run with the setting off does not recover them. | `test_g_ignore_missing_files_advances_the_checkpoint_past_vacuumed_rows_for_good` |
| (h) liquid `CLUSTER BY` | **Nothing is clustered on write**: appended files and `CREATE TABLE ... CLUSTER BY ... AS SELECT` carry no `clusteringProvider`, and each spans the whole key range. `OPTIMIZE` rewrites files tagged `liquid` whose key ranges overlap only at neighbouring boundaries, when it writes more than one file. With the default `maxFileSize` a small table compacts into one file. | `test_h_liquid_clustering_is_applied_by_optimize_not_on_write` |
| (h) clustering with partitioning | Refused in SQL, by the table builder (`DELTA_CLUSTER_BY_WITH_PARTITIONED_BY`) and by `ALTER TABLE ... CLUSTER BY` on a partitioned table. `OPTIMIZE ... ZORDER BY` on a clustered table is refused too. | `test_h_clustering_cannot_be_combined_with_partitioning` |
| (i) `OPTIMIZE ... ZORDER BY` on a partitioned table | With a partition predicate, only that partition's files are rewritten, and once several files are written their key ranges separate. Z-ordering by a partition column is refused. The protocol is unchanged. | `test_i_optimize_zorder_by_rewrites_only_the_selected_partition` |
| (j) what a scan selected | `FileSourceScanExec`'s `numFiles` and `filesSize` are the files a scan **selected** after partition pruning and data skipping, and their on-disk size. For a full scan they equal DESCRIBE DETAIL's `numFiles` and `sizeInBytes`; a predicate reduces them to exactly the files whose statistics overlap it. **They are not bytes read:** they do not change under column pruning, and a point lookup in a file with many row groups "selects" the whole file while its scan outputs a fraction of the rows. | `test_j_selection_and_reads_are_measured_from_the_plan_that_ran`, `test_j_selected_bytes_are_blind_to_column_pruning_and_row_group_skipping` |
| (j) what a scan read | Spark task metrics (`inputBytes`, `inputRecords`) for the stages that executed a scan's input RDD, found through a job group and the application status store, are populated with the UI disabled. They are smaller than the selected size for the row-group lookup, include Parquet footers, and exclude Delta's log reads during planning, which run as separate stages. `groupBy().count()` is answered from log statistics and reads no file. | `test_j_selection_and_reads_are_measured_from_the_plan_that_ran`, `test_j_selected_bytes_are_blind_to_column_pruning_and_row_group_skipping` |
| (j) plans that hide reads | With exchange reuse, a self-join's final plan holds one scan and a `ReusedExchange` (without reuse, two scans and twice the records read). Adaptive execution **drops finished stages from its final plan** around a join with an empty side, so a plan walk sees no scan although data was read. A top-level `LIMIT` executes through `CollectLimitExec`, whose `collect()` path differs from `toRdd()`. | `test_j_reused_exchanges_count_once_and_an_unattributable_plan_is_refused` |
| (k) protocol per property | See the table below. Every CREATE is one commit. | `test_k_the_protocol_and_features_each_table_property_produces`, `test_k_a_protocol_property_yields_a_protocol_set_by_not_null_not_by_the_api` |
| (k) allow-listed properties | `appendOnly`, `checkpointInterval`, `logRetentionDuration`, `deletedFileRetentionDuration`, `dataSkippingNumIndexedCols` and `dataSkippingStatsColumns`, set together on a created table, leave it at (1, 2) with only the baseline features. | `test_k_allowed_properties_add_no_protocol_feature` |
| (k) nullability of a table created implicitly | A table created by a DataFrame `save()`, by `writeTo().create()` or by a streaming sink's first batch stores **every column as nullable**, even when the writing schema declares a column non-nullable. `NOT NULL` survives only DDL or the table builder, and a later append does not loosen it. | `test_k_a_table_created_implicitly_stores_every_column_as_nullable` |
| (k) creation from a declaration | The builder accepts `delta.constraints.<name>` at creation: **one commit**, protocol (1, 3), and a violating write is refused afterwards. Session settings under `spark.databricks.delta.properties.defaults.` are copied into a new table (critic: `enableDeletionVectors` produced a deletion-vector table). | `test_k_a_table_is_created_from_its_declaration_in_one_commit_and_drift_is_refused` |
| (l) RocksDB state store, changelog checkpointing | State is checkpointed as `.changelog` files. With every state store unloaded from the JVM, a restart reads only the new rows and yields exact counts. | `test_l_rocksdb_changelog_state_resumes_from_its_checkpoint_without_double_counting` |

**(k) Protocol versions and table features reported by DESCRIBE DETAIL.** A table at (1, 2) reports
`appendOnly` and `invariants` whether or not either is enabled: they are the legacy
writer-version-2 features. **Other protocols report other sets**, so "every table reports them" is
false.

| Table (SQL `CREATE TABLE` unless noted) | (reader, writer) | `tableFeatures` |
|---|---|---|
| no properties; `appendOnly`; a `NOT NULL` column; `PARTITIONED BY` | (1, 2) | `appendOnly`, `invariants` |
| a CHECK constraint set at creation | (1, 3) | + `checkConstraints` |
| `CLUSTER BY` | (1, 7) | + `clustering`, `domainMetadata` |
| `delta.enableChangeDataFeed=true` | (1, 7) | + `changeDataFeed` |
| `delta.enableRowTracking=true` | (1, 7) | + `domainMetadata`, `rowTracking` (and two generated properties) |
| `delta.enableDeletionVectors=true`, or `delta.feature.deletionVectors=supported` | (3, 7) | + `deletionVectors` (the second leaves no property behind) |
| `delta.columnMapping.mode=name` | (2, 7) | + `columnMapping` |
| `delta.enableTypeWidening=true` | (3, 7) | + `typeWidening` |
| `delta.minWriterVersion=7`, every column nullable (SQL or the builder) | (1, 1) | none |
| `delta.minWriterVersion=7`, a `NOT NULL` column (SQL or the builder) | (1, 7) | `invariants` only |

## Decision

**1. One lake root, one layout.** (`trace_core.stream.lake`)
- **Root.** `TRACE_DELTA_ROOT` (default `./data/lake`) is resolved once, symlinks resolved.
  - **A relative value is anchored to the source checkout, never to the working directory**, so a
    job started from `services/stream/` cannot write an unignored second lake.
  - Installed without a checkout, a relative value is refused.
  - The resolved root is logged (`lake_root_resolved`).
  - A blank value is refused, and so is a URI until Phase 12.
- **Layout.** Tables live at `<root>/<tier>/<name>/`. Checkpoints live at
  `<root>/_checkpoints/<query>/v<N>/`: beside the tiers, never inside a table. Anything Spark manages
  without a path, such as a managed table, lives under `<root>/_warehouse/`; the session factory sets
  it and no caller may override it.

**2. `TableRef` is the only table address.** It resolves one logical name to the local path and to
`tracex.<tier>.<name>`. Per ADR-0015, layout is the only DDL allowed to differ between environments.

**3. `TableDeclaration` states what a table may be.** It carries a `StructType` schema, a layout
(partitioned or clustered, never both), named CHECK constraints, and properties from an
**allow-list**.
- **Allow-list:** the six properties observed to add no feature.
- **Opt-in only:** `enableDeletionVectors`, `columnMapping.mode` and `enableTypeWidening`.
- **Refused outright:**
  - `delta.feature.*` and `delta.min(Reader|Writer)Version`, which set the protocol directly and
    were observed to produce a protocol that depends on unrelated schema details (NOT NULL);
  - `delta.constraints.*`;
  - `delta.setTransactionRetentionDuration`, which expires the transaction identifiers idempotent
    writes depend on.
- **Anything unlisted** is refused, including `enableChangeDataFeed` and `enableRowTracking`, which
  were observed to add features.
- **Required features** are derived from the declaration: `invariants` for any non-nullable field,
  `checkConstraints`, `clustering` and `domainMetadata`, and any opted-in feature. **Allowed features**
  are those plus the baseline pair.

**4. A table is created from its declaration, in one commit, before any query runs.**
- **Creation.** `create_table` uses the builder with the declared properties and one
  `delta.constraints.<name>` per CHECK constraint, stamped with provenance.
- **Session defaults.** It refuses to create while the session carries
  `spark.databricks.delta.properties.defaults.*`.
- **Existing tables.** A table that exists is compared, not altered. `check_drift` reports every
  difference in one error:
  - format;
  - a feature beyond those allowed, or a required feature absent;
  - a protocol above what the allowed features need;
  - properties, where an undeclared property is drift;
  - constraints;
  - partition and clustering columns;
  - schema, compared as Spark's JSON without field metadata, nested nullability and column order
    included.
- **A table that drifted is refused, never repaired or deleted:** the rows written meanwhile are
  evidence.

**5. Every commit carries provenance.**
- **Format.** Canonical JSON in `userMetadata`: `git_sha` (full 40-hex), `dirty_worktree`, `query`,
  `checkpoint_id` and `batch_id`.
  - `checkpoint_id` must be an app id of that query.
  - It is null for commits made outside a checkpoint (table creation, batch jobs), which are never
    attributed to one.
- **Transport.** DataFrame writes carry it as a writer option. MERGE and DDL carry it through
  `stamped_commits`, which refuses an existing value and always unsets its own.
- **Reading.** Foreign metadata is not ours; malformed metadata carrying our marker is an error.

**6. A checkpoint owns its app id, and is the only way a query writes its targets.**
(`trace_core.stream.checkpoints`)
- **Identity.** `v<N>/trace-checkpoint.json` records:
  - the app id `trace-x:<query>:v<N>:<random uuid4>`; an app id whose nonce is not a random UUID is
    not attributed to any query;
  - every target's table id and version when the version was opened;
  - every source's kind, start position and (for Delta) table id;
  - what a reset superseded, and why.

  It is published once, by renaming a staging directory, and read back on restart. **No public
  function or method accepts an app id.**
- **Writing.** `OpenedCheckpoint.append` and `.merge` make **exactly one idempotent, stamped commit per
  target per batch**, and refuse a second. `.merge` builds the MERGE on the target itself, and sets
  the session transaction keys only around its single `execute()`, inside the `try`, always
  unsetting them.
- **Starting.** `open_checkpoint` takes the session and the `TableRef` of every target and the start
  of every source, **reads their evidence itself**, and refuses (`CheckpointRefusedError`) when:
  - no target or no source is named;
  - there is no checkpoint, but a target holds the query's commits, found by parsed app ids or by
    stamped `userMetadata`;
  - a version has no identity, or the directory holds unexpected entries;
  - a target holds commits from a newer, recreated or unsuperseded checkpoint version;
  - a target recorded a batch for this checkpoint that is neither committed nor exactly the next
    planned batch; this also applies to Spark's query id, for the native sink;
  - **a target's table id changed, its version went backwards, or the retained history shows a
    RESTORE after the version was opened**;
  - the set of targets or sources changed, a source's start position changed, or a Delta source's
    table id changed.
- **Reading sources.** Delta sources are read only through `OpenedCheckpoint.delta_source`, which:
  - **always sets `failOnDataLoss=true`**;
  - takes the start position from the checkpoint;
  - refuses `startingVersion`, `startingTimestamp` and `failOnDataLoss` from the caller;
  - refuses loss-tolerant reader options (`ignoreMissingFiles`, `ignoreCorruptFiles`,
    `skipChangeCommits`, `ignoreChanges`, `ignoreDeletes`) and session settings
    (`spark.sql.files.ignoreMissingFiles`, `ignoreCorruptFiles`);
  - on a restart, refuses a checkpoint whose offset needs log versions the source no longer retains.

  Kafka readers take `startingOffsets` and `failOnDataLoss=true` from `kafka_options`.
- **Resetting.** `reset_checkpoint` publishes `v<N+1>`, where `N` is the highest version on disk or in
  any target. It records the present targets, each source's start position and a mandatory reason,
  and deletes nothing. **A Kafka source may never start at `latest` or at an unspecified position**
  (§4.1 point 8).

**7. The pre-start retention guard covers restarts only.** A running query is protected by
`failOnDataLoss=true`, which the reader forces; nothing checked before start can see a query fall
behind while it runs.

**8. Scans are measured from the plan that ran, and refused when that is impossible.**
`measure_scans(dataframe)`:
- executes the query's own `QueryExecution` under a unique job group and drains the listener bus;
- returns `selected_files` and `selected_bytes` (the scans' SQL metrics) separately from
  `input_bytes` and `input_records` (task metrics of the stages that executed the scans' input RDDs),
  plus `scan_output_rows`;
- walks the plan by JVM reference identity;
- refuses (`ScanMeasurementError`) when:
  - an adaptive final plan lacks a scan its initial plan had, compared by semantic hash, so exchange
    reuse is not refused;
  - a scan that selected files has no completed stage;
  - the root is a top-level limit (`CollectLimitExec`, `CollectTailExec`,
    `TakeOrderedAndProjectExec`);
  - a metric name changed.

**9. Lake errors are typed in `trace_core.domain.errors`:** `LakeContractError` and its subclasses.

### Corrections this spike proposes to the plan

1. **§4.2 and Step 6: deduplicate each micro-batch before the insert-only MERGE (item d).** Delta
   inserts duplicates *within* a batch. Canonical uniqueness is two mechanisms, both required:
   - a deterministic in-batch deduplication by the declared identity;
   - the insert-only MERGE, through `OpenedCheckpoint.merge`.

   Step 6 should add a post-write uniqueness assertion.
2. **Q6, "minimum protocol features": say what the minimum costs.**
   - A CHECK constraint raises the writer to version 3.
   - Clustering raises it to version 7 and adds `domainMetadata`.
   - "Clustering only if the layout benchmark earns it" should weigh that protocol cost as well as
     scan performance.
3. **Step 11 and Q8: the VACUUM floor is per table (item f).** Local retention must be declared per
   table, which the drift check enforces. The resource-bounds guard should relate VACUUM and log
   retention to the longest expected consumer outage.
4. **Item (g) needs two guards, not one.** A running query silently loses rows under
   `failOnDataLoss=false`, which only the reader can prevent; a restart is refused by Delta and, with
   a better remedy, by the pre-start guard. Vacuumed files are silently skipped under
   `ignoreMissingFiles`. §4.1 point 8's `failOnDataLoss=true` for Kafka is consistent with this.
5. **Step 14, the layout benchmark:**
   - It must report selected files and bytes separately from bytes read, never as one "bytes
     scanned".
   - It must not use `count(*)` as a scan query.
   - It must record `spark.databricks.delta.optimize.maxFileSize`, `parquet.block.size` and adaptive
     settings in the run manifest, because clustering and row-group skipping depend on them.
   - It must treat a refused measurement as a failed run, not as zero.
6. **Steps 5 to 7: create every table from its declaration before its first query runs**, and write
   only through `OpenedCheckpoint`. A table a query creates loses every `NOT NULL` (item k), and a
   target that does not exist is refused at open.
7. **`docs/DATA_ENGINEERING.md` §3** documents checkpoints at `_checkpoints/{query}/`; the convention
   is `_checkpoints/<query>/v<N>/`.

### Decided: data-platform identifiers are snake_case (U9)

CLAUDE.md §6 says file paths are kebab-case. Table and query names here are **lowercase snake_case**
(`silver.late_events`, `bronze_ingest`) for three reasons:
- a table name is also an unquoted Unity Catalog identifier, where a hyphen needs backquoting in
  every statement;
- it is part of a Delta transaction app id and of Databricks job names, which
  `docs/DATA_ENGINEERING.md` §8 already writes in snake_case;
- one spelling everywhere avoids a mapping between two that could silently diverge.

**Recommendation:** keep snake_case for these identifiers and record them as an exception to the
file-path rule, since the directory name is derived from an identifier rather than chosen as a path.
The alternative is kebab-case directories with snake_case catalog names and an explicit, tested
mapping. Changing CLAUDE.md needed the user's approval.

**Decision (the user, 2026-09-14): snake_case**, the canonical spelling of every data-platform
identifier:
- Delta and Unity Catalog schema and table identifiers, and the physical lake directories derived
  from them (`silver.late_events`, `silver/late_events/`);
- Structured Streaming query names and checkpoint logical identifiers (`bronze_ingest`);
- Delta transaction app ids;
- Databricks job and task identifiers derived from pipeline names;
- metric and manifest identifiers that represent the same logical name (`gold_tx_features`).

CLAUDE.md §6 is amended narrowly: kebab-case applies to human-authored repository filesystem paths.
No snake_case to kebab-case mapping layer is introduced to preserve the repository convention.
`tests/unit/test_lake_identifier_consistency.py` asserts that a canonical identifier and the
directory, catalog name, checkpoint path and app id derived from it cannot silently diverge.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| A fixed app id per query (for example the query name) | This is the trap in item (b): the first new checkpoint silently loses its batches |
| An app id supplied by configuration or by the caller | A restored or copied configuration reintroduces reuse, and nothing ties the value to the checkpoint that must own it |
| Spark's persistent query id (`<checkpoint>/metadata`) as the `foreachBatch` app id | The file appears only when the query first starts; reading it inside the batch couples the sink to an internal file format; and it cannot be attributed to a query by name once the checkpoint is gone |
| A context manager inside which callers make their own idempotent writes | Observed: a second write inside such a block is skipped silently, and the block cannot count commits. One call per commit makes "one commit per target per batch" structural |
| Callers pass the evidence (target lists, reader options) the guards read | A caller that omits a target or an option skips every check silently; the functions now read the evidence themselves and refuse an empty list |
| Reset by deleting or overwriting the checkpoint | It destroys the evidence of what was processed and why it was reset, and it is Delta's own advice that turns a loud failure into loss or duplication |
| Detect a lost checkpoint from `DESCRIBE HISTORY` alone | History is bounded by log retention; transaction identifiers live in the snapshot and survive it. History is kept as the second source, and for RESTOREs |
| Read transaction identifiers by parsing `_delta_log` in Python | It would re-implement log replay and work only on a local filesystem. `DeltaLog` is Delta's own replay; its risk is being an internal API, bounded by the pin and the stream tests |
| Rely on the pre-start retention guard alone for Delta sources | It cannot see a running query fall behind (item g); only `failOnDataLoss=true` on the reader can |
| Create tables with the builder and then `ADD CONSTRAINT` | Two commits and a crash window in which the table exists without its constraint; the builder accepts constraints at creation |
| Accept any `delta.*` property and rely on drift after creation | The refused table is then already on disk, and feature-adding properties (`delta.feature.*`, `minWriterVersion`, CDF, row tracking) were observed to be accepted by Delta |
| Report `filesSize` as bytes read | Observed to be blind to column pruning and row-group skipping; task metrics measure reads |
| Measure reads with Hadoop `FileSystem` statistics | They are process-wide, include Delta's own log reads, and were not consistent with task metrics on the empty-side join |
| Treat a scan missing from an adaptive final plan as zero | Observed: the dropped stage had read data. A partial measurement published as a number would be a plausible, wrong one |
| Declare schemas as DDL strings | A type-name comparison loses nested nullability; `StructType` builds without a JVM and compares as Spark's own JSON |
| Repair drift automatically | Rows written while the table drifted may violate the declaration; whether to accept them is an operator's decision |

## Consequences

**Positive.**
- Every Delta behaviour a later step depends on is asserted on the pinned version, including the
  critic's corrections, so a version that behaves differently fails a named test.
- The silent-loss states found here are refused or made loud: a reused app id, a second write in a
  batch, a checkpoint behind or ahead of its target, a cleaned log in a running or restarting query,
  skipped missing files, and a reset to `latest`. Each refusal carries a remedy that does not create a
  new one.
- Tables are created in one commit from declarations that cannot quietly add protocol features.
- Scan measurements separate selection from reading and refuse rather than under-report.

**Negative.**
- `snapshot_facts` calls Delta's internal `DeltaLog` API, and `measure_scans` Spark's internal status
  store and plan classes, through py4j. An upgrade may break either; the stream tests will say so.
- Provenance-based attribution (for native-sink writers) and RESTORE detection read only the most
  recent `history_limit` retained commits. An unstamped native-sink writer whose checkpoint is lost,
  or a RESTORE older than that window, is not detected.
- A target replaced by an older copy at a version at or above the recorded one is not detected.
- A reset reprocesses from the recorded start; for a plain append sink that duplicates rows, which is
  why it needs a reason.
- The checks run before a query starts and assume one writer per query (as §4.1 does for the
  gateway); a concurrent writer could change a target between the check and the start. Session
  settings changed after `delta_source` is called are not seen.
- The claim of one idempotent commit per target per batch is enforced per `OpenedCheckpoint` object;
  two objects opened for the same checkpoint in one process could each claim a batch.
- The source-retention guard and the lake root are local-only until Phase 12, and undeclared
  Databricks-managed properties will need an explicit allowance then.
- Measurement ignores Delta log I/O by design, and footers make read bytes exceed selected bytes on
  small files.

**Risks.**
- The spike ran at laptop scale. Behaviours that depend on size are recorded as conditional, not as
  results: whether OPTIMIZE writes several files, whether a count is answered from metadata, and how
  far row-group skipping reduces reads.
- Field metadata is ignored by the schema comparison, so a changed column comment is not reported.

## Status

Proposed
