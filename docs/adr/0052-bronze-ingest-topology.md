# ADR-0052: Bronze ingest topology and its no-skip guards

## Context
PHASE3_PLAN §3 Q6 makes Bronze the durable local record: raw Kafka value bytes plus transport
metadata, never parsed. §4.1 point 8 requires that Bronze never skip unread data: `failOnDataLoss`
is always on, and offsets are never reset to latest. `P3.kafka-ingest` requires conservation into
Bronze. ADR-0048 fixed the lake conventions (snake_case identifiers, deterministic checkpoints, the
table registry). This ADR records how Step 5 meets those requirements, and the guards the
implementation found it needed.

## Decision

### 1. One table and one query per released topic
- `bronze.<topic with dots as underscores>` (for example `bronze.tx_raw_v1`), written by the query
  `bronze_ingest_<same>`, each with its own checkpoint.
- **Why not one query over every topic:** a checkpoint records its sources. Releasing a new topic
  would then force a checkpoint reset, which re-reads every topic and duplicates every row in an
  append-only table. Per-topic queries also give one commit per batch, and let conservation be
  judged per checkpoint.

### 2. The row
- Raw key and value bytes, never parsed; Bronze accepts a poison value and blocks nothing on it.
- Every header, in delivery order, as an array rather than a map, because a Kafka header name can
  repeat. The array includes `tracex-session-id` and `tracex-seq`.
- Topic, partition, offset, and the Kafka timestamp and its type. Spark numbers LogAppendTime 1,
  where confluent-kafka numbers it 2.
- `kafka_topic_id`: the broker's topic id that the writing checkpoint version recorded reading. A
  deleted and recreated topic starts its offsets again at 0; without the id, its offsets would be
  indistinguishable from the old topic's, and a correct reset over a recreated topic would count as
  duplicates forever (critic finding B4).
- `trust_tier = UNTRUSTED`, the ingest time, and the batch id and app id that wrote the row.
- The table is created from its declaration before its query starts: `delta.appendOnly = true`,
  protocol (1, 2), no layout.

### 3. Never skip unread data
- `failOnDataLoss = true` always. A new checkpoint starts at `earliest`, and a resume starts where
  the checkpoint recorded. Reader options cannot override either.
- Explicit starting offsets that contain `-1`, Spark's spelling of latest, are refused.
- **A recreated topic.** Spark alone does not detect a topic that was deleted, recreated and
  refilled past the checkpoint's offset; against a real broker, the first records of the new topic
  silently never reached Bronze. Each checkpoint version therefore records the broker's topic id in
  a sidecar file before Spark records anything, and a different id is refused:
  - before the query starts. A version holding planned batches or Spark's initial offsets without
    a sidecar is refused too: nothing proves which topic it read;
  - by every micro-batch, before it is written and again after, before Spark records it as done
    (critic finding B3). A topic can be recreated under a running query, which the start-time
    check never sees.
- **A reset may not skip.** A reset starting a Kafka source at an explicit offset above where the
  superseded version stopped consuming it is refused: the offsets in between would be read by no
  version. `earliest` cannot be checked at reset time, because only the broker knows where earliest
  is; conservation judges it once the new version has read (§4).

### 4. Conservation
- `check_conservation` judges **every checkpoint version** of the query, not only the current one
  (critic finding A1). Judged alone, a version forgets a loss inside an earlier version, and the
  offsets between two versions that neither read. That is exactly the aftermath of a data-loss
  stop: its only remedy, a reset at `earliest`, begins above the hole. Per partition:
  - a version's consumed range runs from where it started to the end of the later of Spark's last
    committed batch and the table's last recorded batch for its app id;
  - every offset in that range appears exactly once in the rows the version wrote, none outside it,
    and all under the topic id it recorded;
  - a version starting above the highest end of the earlier versions of the same topic id skipped
    the offsets in between. They are reported as `skipped`, and the query is never conserved again:
    the table is append-only, and the loss is real;
  - no (topic id, offset) appears twice under any checkpoint.
- The current version's recorded topic id is compared with the broker's; the service always passes
  it. A mismatch is a problem: the broker's offsets are no longer the ones the checkpoint consumed.
- **Read order:** Spark's commits, then the table snapshot, then Spark's planned batches, then the
  rows at that snapshot. A committed batch missing from the table is a loss, never a race, and a
  batch the snapshot holds is never reported as never planned by a race either (critic finding B2).
- It assumes contiguous offsets, which holds for the platform's idempotent, non-transactional
  producers. A transactional producer would surface as "missing", never as a silent pass.

### 5. Coverage from Bronze
`coverage_from_bronze` is the single call site of `trace_core.observation.coverage.assess`
(ADR-0051 §5).
- It counts every `tx.scored.v1` record, and every `identity.events.v1` record carrying session
  headers.
- A `tx.scored.v1` record without session headers is a gap whoever produced it, because only the
  gateway produces that topic. An `identity.events.v1` record without them is a gap only if its
  envelope producer is `trace-gateway`, because the generator also produces that topic.
- The high-water mark is the earliest, across partitions, of each partition's newest arrival. It is
  `None` if any partition holds no rows.
- The ledger is read after the high-water mark is established, and the read time is recorded.

## Alternatives Considered
| Alternative | Why not |
|---|---|
| One multi-topic query | A new topic forces a checkpoint reset and re-reads every topic, duplicating rows |
| Headers as a map | A repeated header name would lose values |
| Parsing in Bronze | A poison value would block the partition, and Bronze would stop being the raw record |
| Trusting Spark's offsets alone | A recreated topic silently skips records; observed against a real broker |

## Consequences
**Positive.**
- Loss, duplication and a recreated topic are all detectable at Bronze.
- The observation-log coverage rule has one Bronze entry point.

**Negative.**
- Six queries and six checkpoints instead of one.
- `delta.appendOnly` conflicts with Step 11's local retention, which must change the declaration
  explicitly.

**Risks.**
- The per-batch topic check cannot attribute a batch whose records were read across a recreation
  that completed inside that one batch: the check after the write stops the query before Spark
  records the batch, but its rows are already in the table under the old id.
- A topic deleted with records Bronze had not read loses them with no trace in any checkpoint.
- Coverage collects Bronze rows into driver memory, O(rows).
- The high-water mark assumes LogAppendTime does not decrease along a partition.
- The initial-offsets file format is a Spark internal, pinned by an integration test.

## Open questions
- **Step 11 retention (critic finding B6): decided by the user on 2026-09-15; implemented in Step 11,
  not before.** Q8 bounds local lake retention, and Bronze conservation would otherwise count every
  deleted row as lost. The decision is an audited local Bronze retention floor.
  - **Where the authority lives.** The retirement state must be atomically coupled to the Bronze
    table itself, or derivable from it. Delta's atomicity is per table, so a Bronze delete and a row
    written to a separate audit table are never one commit, and the design must not assume they are.
    The smallest Delta-native form is preferred: a same-table retention marker, or metadata on the
    Bronze delete commit itself, so the Bronze table and its commit log are authoritative.
  - **Required semantics:**
    - local development only; the production Bronze contract stays append-only;
    - one floor per (topic id, partition);
    - only rows strictly below the committed retirement floor may be deleted;
    - conservation treats offsets below that floor as RETIRED, never as LOST;
    - the maintenance command refuses to advance a floor past any required consumer or checkpoint
      position, and never deletes data still needed for replay or recovery;
    - a crash at any point must leave conservation neither believing deleted rows still exist nor
      treating unretired rows as retired;
    - maintenance is idempotent;
    - every floor advancement leaves an auditable record;
    - no generalised retention framework.

## Implementation status
- Implemented by the Spark agent, integrated by the lead:
  - `packages/trace_core/stream/bronze.py`, `bronze_conservation.py` and `bronze_coverage.py`;
  - `services/stream/bronze.py`, with `run`, `conservation` and `coverage` commands;
  - unit, stream and integration tests.
- **Critic review (2026-09-15).** An independent read-only critic reviewed the step. The lead
  reproduced the Category A findings before any fix.
  - **A1, fixed.** After a reset, conservation judged only the current checkpoint version. A hole
    left between versions therefore read as conserved. Conservation now judges every version. A
    version that starts above the earlier versions' end counts as skipped, and the query is never
    conserved again. A reset with an explicit start above the superseded end is refused.
  - **A2, fixed.** The coverage high-water mark silently left out partitions that were never read.
    The mark is now withheld when a covered checkpoint reports a problem or records no partition.
  - **B1, fixed.** The mark is strict. Broker timestamps are milliseconds, so rows at the mark are
    left out.
  - **B2, fixed.** Commits are now read before the table snapshot, and planned batches after it.
  - **B3, fixed.** The topic id is checked on every batch, around each append. A checkpoint with
    initial offsets but no sidecar is refused.
  - **B4, fixed.** Rows carry `kafka_topic_id`, and duplicates are counted per topic id.
  - **B5, fixed.** The envelope producer and writer stamp are extracted in Spark. No value is parsed
    on the driver, so hostile nesting cannot raise there.
  - **B6, decided (2026-09-15), implemented in Step 11.** Step 11's bounded retention needs an audited
    local retention floor that conservation respects; the user's decision and its required semantics
    are in the open questions.
  - **C, addressed.**
    - The integration oracle keeps a null header value.
    - A coverage case now spreads records over every partition.
    - A checkpoint that starts above its predecessor's end has unit tests.
  - **C, still open.** CI provisioning of PostgreSQL for the live coverage test.
  - **`coverage` exit status.** A live gateway's session always has an open tail, so `coverage`
    exits 1 while a writer runs, by design. It prints `open_gaps`, so a live tail can be told
    apart from a bounded gap.
- `P3.kafka-ingest` is not PASS before the step's exit evidence.

## Amendment 1 (Step 11): the audited local Bronze retention floor

- **Status of this amendment:** Proposed. The Delta agent proposed it in Phase A. The user decided
  Option A on 2026-09-15: file-aligned retention deletes, plus `ignoreDeletes` on Bronze sources
  only. Every other loss-tolerant reader option and session setting stays refused. The Delta agent
  implemented it in Phase B (see *Implementation status*).
- **This amendment relaxes a loss-refusal rule**, by the user's decision: Silver's reader of a Bronze
  topic table sets `ignoreDeletes=true` (point 5).

### Context

The user's B6 decision (open questions, above) needs rows to be deleted from Bronze. Silver reads
Bronze as a Delta streaming source through `OpenedCheckpoint.delta_source`, which refuses
`ignoreDeletes`, `skipChangeCommits` and `ignoreChanges`. Bronze is unpartitioned, and a floor per
(topic id, partition) cuts across the files one micro-batch writes.

**Evidence.** Every statement below was executed by `spikes/step11-maintenance/delta_retention_spike.py`
on Spark 4.0.1, Delta 4.0.1, Hadoop 3.4.1, Scala 2.13.16 and Temurin 17, at `local[2]`, through
`build_session`. The recorded outputs are `out/observations-run1.json` (X1-X10),
`out/observations-x11.json`, `out/observations-run2.json` (X6, X8, X12, re-run) and
`out/observations-run3.json` (X3, X4, X5, X9, re-run with whole-life reader totals). Nothing here
is a benchmark, and no run has a `run_id`. "Silver's reader" means `OpenedCheckpoint.delta_source`
at `startingVersion=0`. "Running" means a `processingTime` query that was already running when the
commit landed. "Restart" means an `availableNow` run on the same checkpoint afterwards.

| # | Observed on Delta 4.0.1 | Key |
|---|---|---|
| O1 | On the real Bronze declaration, `DELETE` fails with `DELTA_CANNOT_MODIFY_APPEND_ONLY`. | X1 |
| O2 | A floor that cuts files makes `DELETE` rewrite them: 2 removes and 2 adds, all `dataChange=true`. Silver's reader, running and on every restart, fails at that version with `DELTA_SOURCE_TABLE_IGNORE_CHANGES`. Its checkpoint stays before the delete. A reset to `startingVersion = delete + 1` delivers only later appends. | X2 |
| O3 | With `skipChangeCommits`, running and restarted readers pass the rewrite and deliver later appends. **A fresh reader starting after the rewrite completes without error, having never delivered the 16 unretired rows that exist only in the rewrite's files.** A fresh reader from version 0, before VACUUM, delivers the 8 deleted rows again. A snapshot start delivers exactly the unretired rows. | X3 |
| O4 | A `DELETE` whose predicate covers whole files commits **removes only**: 0 adds and `numCopiedRows=0`, including for a batch written as two files. It also leaves two new files on disk that no commit references. Silver's reader, running and restarted, fails with `DELTA_SOURCE_IGNORE_DELETE`. With `ignoreDeletes`, running and restarted readers pass it and deliver later appends (whole-life totals in run 3: all 32 appended coordinates, each exactly once). **`ignoreDeletes` does not pass a rewrite**: it still fails with `DELTA_SOURCE_TABLE_IGNORE_CHANGES`. | X4 |
| O5 | After a whole-file delete, a fresh reader with `ignoreDeletes`, starting at the first version whose files are all live, delivers exactly the unretired rows, before and after VACUUM. With default options (Silver's reader as it is today) it fails with `DELTA_SOURCE_IGNORE_DELETE`. From version 0 it delivers the deleted rows again before VACUUM, and fails with `FAILED_READ_FILE.FILE_NOT_EXIST` after. A reader positioned before the delete fails the same way after VACUUM. | X4 |
| O6 | Over a file that mixes offsets below the floor with offsets above it, an offset-only predicate rewrites (1 add, 3 removes). Adding a batch-identity conjunct makes the commit removes-only and leaves that file's below-floor rows present. The same `DELETE` run twice commits nothing the second time. The same property value set twice **commits a new version** the second time. | X11 |
| O7 | Change data feed puts the table at (1, 7) with `changeDataFeed`. Appends write no change files and commit only `add` actions. A rewrite delete and a whole-file delete each write change files. Readers with `readChangeFeed` pass both deletes and an `UPDATE`: they emit `delete` rows for exactly the deleted coordinates, inserts only for new appends, and pre- and post-images for the update (whole-life totals in run 3, the same for running and restarted readers: 40 inserts, 16 deletes and 2 update images, none repeated). From version 0 they emit inserts for deleted rows before VACUUM, and fail with `FILE_NOT_EXIST` after. `delta_source` does not refuse `readChangeFeed` today. | X5, X2 |
| O8 | A `trace_x.retention_floor.<topic id>.<partition>` property on an appendOnly (1, 2) table is one `metaData`-only commit. The protocol and features are unchanged and the key's case is preserved. `check_drift` reports it as an undeclared property. An unknown `delta.` key is refused with `DELTA_UNKNOWN_CONFIGURATION`. Running and restarted readers pass floor commits and `appendOnly` toggles, and deliver every later row. Delta's own snapshot at a version, read through `DeltaLog.getSnapshotAt`, returns the floor as of that version: none at version 1, `2` at the floor commit (version 2), and `6` at the second floor commit (version 7). The rows read with `versionAsOf` the same snapshot's version match it. | X1, X6 |
| O9 | After log cleanup (log retained from version 9 to 12), the floor property is still in `DESCRIBE DETAIL` and in the checkpoint's `metaData.configuration`. The floor commit's `commitInfo.userMetadata` is gone from history, and the checkpoint has no `commitInfo` column. | X7 |
| O10 | Neither the Python nor the JVM `DeltaTable` has a member naming domains. An internal `OptimisticTransaction` commit of a `DomainMetadata` action on a (1, 2) table is refused with `DELTA_DOMAIN_METADATA_NOT_SUPPORTED` ("DomainMetadataTableFeature is not enabled"). ADR-0048 (k) records that feature only at writer version 7. | X8 |
| O11 | `OPTIMIZE` is allowed on an appendOnly table and commits `dataChange=false`. Running and restarted readers pass it (whole-life totals in run 3: all 32 appended coordinates, each exactly once). It merged batches 0-2 into one file. After VACUUM, a reader positioned before it fails with `FILE_NOT_EXIST`. | X9 |
| O12 | The session keeps `retentionDurationCheck` on, and `RETAIN 0 HOURS` is refused. `DRY RUN` works on an appendOnly table and returns one `path` column. `LITE` is accepted. ADR-0048 (f) already records that the floor is the table's `delta.deletedFileRetentionDuration`. | X10 |
| O13 | A batch read at a pinned version, as Gold reads Silver, still reads that version after a rewrite `DELETE` removed its files. After VACUUM it fails with `FAILED_READ_FILE.FILE_NOT_EXIST`, while the latest version reads. After log cleanup it fails with "Cannot time travel Delta table to version 1. Available versions: [9, 12]". The earliest retained version still reads. | X12 |

### Decision

**1. The floor lives in Bronze's own table properties.**
- **Key.** One property per (Kafka topic id, partition): `trace_x.retention_floor.<topic id>.<partition>`,
  a non-negative integer offset (O8). The topic id is spelled exactly as Bronze's `kafka_topic_id`
  records it: confluent-kafka's `str(Uuid)`, which is standard base64 (`+`, `/`), not Kafka's URL-safe
  form. The segment therefore admits `[A-Za-z0-9+/_-]`, none of which can end a SQL string literal or
  the key's `.`-separated segments. (Found on 2026-09-18 when a real broker's id carried a `+` and the
  first key accepted only the URL-safe alphabet; about half of all topic ids do.)
- **Authority.** It is authoritative from the commit that sets it: every offset below it is RETIRED,
  whether or not its row still exists. It survives log cleanup and checkpoints (O9).
- **Floor and rows from one snapshot.** Bronze conservation reads the floors and the rows from the
  same Bronze snapshot (`SnapshotFacts.properties`). Silver conservation reads the floors from a
  Bronze snapshot taken after the checkpoint's offsets, so that snapshot is at or after every
  version it judges, and a floor only rises.
- **Never lowered, never re-set.** A floor is committed only when it changes, because a same-value
  commit is not a no-op (O6).
- **Drift.** `bronze_declaration` sets `retention_floors=True`. On such a table `check_drift`
  accepts well-formed floor properties and reports a malformed one; on any other table a floor
  property is undeclared, and is drift. A malformed floor never reads as no floor
  (`tables.retention_floors` raises).

**2. Conservation.**
- **Bronze.**
  - An offset below its floor is never missing, a duplicate or out of range, present or not. A row
    still present below it is counted as `retired_present`.
  - Skips between checkpoint versions stay reported.
  - A table whose only drift is `delta.appendOnly=false` is judged, but never conserved until
    maintenance runs again (`append_only_lifted`).
- **Silver.** Coordinates below the floor are left out of the Bronze side and the accounted side
  alike. A checkpoint that starts at `version:N` with N above 0 is judged only once Bronze has
  floors (`silver_conservation.start_problem`).
- **Coverage** (`bronze_coverage`, the observation log's rule, ADR-0051 §5). A retired offset is
  neither present nor a gap: its row is left out of the observations and counted, whether it still
  exists or not, with the floors read from the same snapshot as the rows.
  - **A retired region is not evidence FOR completeness.** Every retired row arrived at or before
    the oldest row above its floor, so the verdict carries one gap from the epoch to that arrival.
    No session vouches across it, and hydration (`history_completeness`) refuses to claim before the
    floor with no change of its own.
  - The gaps that lie entirely inside the region are reported apart (`retired_gaps`), never as
    losses.
  - A floored partition with no row above its floor leaves the region open, and the log then
    vouches for nothing.

**3. A floor never passes a required consumer position.** `plan_retention` refuses, naming every
blocker, and nothing is committed while one exists. Requested floors are refused, never clamped.
- **a. Bronze's own checkpoint.** The floor may not pass the highest consumed end, over Bronze's
  checkpoint versions, of that (topic id, partition).
- **b. Bronze conservation is clean** at the snapshot judged. Retirement never launders a missing,
  skipped or duplicated offset.
- **c. The current Silver checkpoint** of the topic:
  - it is conserved, and has read at least one whole Bronze version T;
  - the floor may not pass one past the highest offset Bronze holds at T;
  - a batch is deleted only when all its rows are present at the lowest such T.
- **d. No Silver checkpoint** refuses any advance: retired rows would be removed before Silver reads
  them.
- **e. Gold** reads Silver as batch snapshots at pinned versions (ADR-0055), never Bronze. It places
  no bound on a Bronze floor. Its pins bound VACUUM on Silver (point 7).
- **f. Other refusals.** A partition Bronze never read has no floor, and a floor is never lowered.
- **Automatic mode.** With no floors requested, each floor advances to the lower of (a) and (c).

**4. Deletion is file-aligned, and appendOnly is lifted only for the delete.**
- **One run per table,** by a lock directory `<lake>/_maintenance/<tier>_<name>.lock`. A lock left by
  a process that no longer exists on the same host is taken over, loudly.
- **Deletable batches.** Only whole Bronze batches `(bronze_checkpoint_id, bronze_batch_id)` whose
  rows are all below their partitions' floors, and that Silver has read whole (point 3c).
- **The predicate** names those batches AND bounds each partition's offset below its floor. Only rows
  strictly below the floor can match, and the commit is removes-only (O4, O6). Every literal is
  validated first: checkpoint ids must be TRACE-X app ids, and topic ids must be floor-key segments.
- **Verification.** After the DELETE, a data-changing `add`, or a deleted-row count other than the
  plan's, raises `MaintenanceDefectError`, loudly.
- **Sequence, one commit each:**
  1. the floor properties;
  2. `delta.appendOnly=false`, only when a batch is deletable;
  3. the DELETE;
  4. `delta.appendOnly=true`;
  5. the audit reconciliation (point 9), after the last Bronze commit.
- **Crash windows.**
  - Before (1): nothing changed.
  - After (1): offsets are retired, rows are present, and the table has not drifted.
  - After (2) or (3): `start_bronze_query` refuses on drift, loudly, and conservation is not
    conserved, until maintenance runs again. That run restores appendOnly, even when it refuses to
    advance anything.
  - After (4): only the audit is missing.
- **Re-run.** A re-run is idempotent at every point. The floor is already committed, a repeated DELETE
  has nothing left to delete, and the audit writes each key at most once.

**5. Silver's Bronze reader sets `ignoreDeletes=true`. This relaxes a refusal, by the user's decision.**
- **Scope.** `OpenedCheckpoint.delta_source` sets the option itself, and only on a released topic's
  Bronze table (`checkpoints.is_bronze_topic_table`, from Bronze's own registry). A caller may pass
  only `true`.
- **Still refused:**
  - `skipChangeCommits` and `ignoreChanges`;
  - `failOnDataLoss=false`, `ignoreMissingFiles` and `ignoreCorruptFiles`;
  - the loss-tolerant session settings;
  - `ignoreDeletes` on any other table.
- **Why it skips no appended row (O4, O5):**
  - it passes only removes-only commits: a rewrite still stops the reader;
  - a delete never retracts the `add` actions a reader has not yet reached, so a reader behind a
    delete still receives those rows, and after VACUUM it fails loudly instead.

**6. A Silver reset after retention starts at the first live version; version 0 is refused.**
- **Start.** `tables.retention_start` gives version 0 without floors. With floors it gives the
  lowest version that added a data file still live, read from one snapshot. It refuses when a live
  file was added before the retained log, or by a `dataChange=false` rewrite.
- **Resuming.** `silver.silver_start_version` keeps a checkpoint's recorded start; a new checkpoint
  uses `retention_start`.
- **Resetting.** `maintenance.reset_silver` publishes the new version at that start.
- **The guard.** `delta_source` refuses a Bronze reader that has committed no batch, when floors exist
  and its start is any other version. So version 0 is refused, and so is a start that would skip
  live rows.
- **Elsewhere.** This changes ADR-0053 §7, which the lead amends.

**7. Retention declarations and VACUUM.**
- **Declared per table.** Every declaration has an effective retention: its own
  `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration`, else `DECLARED_RETENTION`.
  - The defaults are 30 days of log and 7 days of removed files, **chosen, not derived**: no longest
    expected consumer outage is declared anywhere. They are Delta 4.0.1's own defaults.
  - `create_table` writes both properties.
  - The drift check reads an absent property as Delta's default, observed to equal the declared
    value, so a table created earlier behaves as declared.
  - A declaration whose removed-file retention exceeds its log retention is refused, because the
    VACUUM guard reads removals from the retained log.
- **VACUUM tooling** (`maintenance.vacuum_table`), for declared tables only, under the lock, refuses:
  - a retention below the declared removed-file retention;
  - `retentionDurationCheck` disabled, or any loss-tolerant session setting;
  - a drifted table, including a Bronze table left with appendOnly lifted;
  - a Bronze topic table whose current Silver checkpoint has not read whole the newest retained
    commit that removed a file (before its first batch, its start minus 1 counts as read);
  - a Silver table Gold reads, when the latest committed build's pin, or a planned uncommitted
    build's pin, is below that commit;
  - Gold's own tables: their readers are not modelled.

  It runs with Delta's check on, and never `LITE`.

**8. OPTIMIZE** (`maintenance.optimize_table`) is refused on every Bronze-tier table: it merges
batches into one file (O11), which the file-aligned delete can then never remove. It is permitted on
declared Silver and Gold tables under the lock.

**9. The audit** is the declared append-only table `bronze.retention_audit`, non-authoritative and
never assumed atomic with Bronze.
- **Rows.**
  - `floor_advanced`: topic id, partition, previous floor, new floor, and the Bronze version of the
    floor commit (from retained history when reconciled).
  - `rows_deleted`: the Bronze version, files and rows, from the DELETE commit's metrics.
  - Every row also carries: `positions_checked` (JSON of the evidence), `reconciled`, `recorded_at`,
    the git SHA and the dirty-worktree flag.
- **Key.** Each row is keyed by `audit_key`: (table id, topic id, partition, floor), or (table id,
  delete version). It is written by an insert-only MERGE, so a re-run or a race writes each key at
  most once.
- **Reconciliation.** Every run reconciles the rows missing for the current floors and for every
  retained maintenance DELETE. Conservation never reads the audit.

**10. Kafka byte caps are unchanged.** The Bronze suite's trimmed-offsets test remains the evidence
that evicted unread records stop Bronze loudly. It also runs under this capability's name.

**11. Concurrency.** The lock serialises maintenance runs. See *Implementation status* for what a Bronze
commit racing maintenance was observed to do.

### Alternatives Considered

| Option | Evidence | Verdict |
|---|---|---|
| **A. File-aligned deletes; Bronze sources set `ignoreDeletes`** (this amendment) | O4-O6: removes-only commits pass, rewrites still stop the reader, and resets start at the first live version. No protocol change. | **Decided by the user (2026-09-15).** Relaxes a refusal (point 5). |
| B. Rewrite deletes at any floor; Bronze sources set `skipChangeCommits` | O3: a reader starting after a rewrite silently never delivers 16 unretired rows. The option skips a whole commit, its adds included. That it would also skip an `UPDATE` with the same action shape is inferred, not observed. | Rejected: a reset becomes silently incomplete. Also relaxes a refusal. |
| C. Silver reads Bronze's change data feed and admits inserts | O7: no refusal changes, and readers pass rewrites, deletes and updates. Silver would see each delete. But Bronze moves to (1, 7) with `changeDataFeed`, beyond Q6's minimum. Silver's source schema and offsets change, reopening Step 6 and Silver conservation, and each delete writes change files. Appends were observed to write no change files. | Runner-up. |
| D. Delete, then reset each Silver checkpoint to `delete + 1` | O2: the reader stops loudly, and a reset after the delete delivers later rows. But after a rewrite, a start past it silently misses unretired rows (O3). Bronze and Silver must both be stopped, and every run adds a checkpoint version. | Rejected. |
| E. Retire offsets without deleting rows | Disk is never bounded. | Rejected: Q8 bounds local lake retention. |
| Floor in Delta domain metadata | O10: no public API, and refused unless the `domainMetadata` feature is enabled, which moves Bronze beyond Q6's minimum protocol. | Rejected. |
| Floor in `commitInfo.userMetadata` only | O9: gone after log cleanup, and absent from checkpoints. | Rejected as the authority. Commits still carry provenance. |
| Floor as marker rows in Bronze | Silver would read them as data, and Bronze conservation would count them as foreign rows. O8 shows a property commit disturbs no reader. | Rejected. |
| Audit only in Bronze's commit history | History does not survive log cleanup (O9), and the user requires a durable audit. | Rejected. |
| Maintenance without a lock, verified after each commit | Two concurrent runs could each commit a floor from a stale read, and a lower floor could land after a higher one. The verification would catch it only after the commit. | Rejected. |

### Consequences

**Positive.**
- The floor, the delete, and the readers' behaviour all rest on observed Delta 4.0.1 behaviour. The
  authority is Bronze's own snapshot, and it survives log cleanup.
- Every crash boundary leaves either retired rows present, or a loud drift refusal until the next run,
  which heals it. A test proves each boundary by conservation.
- Silver's reader, offsets, protocol and conservation model are unchanged, apart from
  `ignoreDeletes`, the floor filter and the reset start.
- Every floor advance and delete leaves a durable, reconciled audit row.

**Negative.**
- **A loss-refusal rule is relaxed.**
  - Silver's Bronze reader no longer stops at a removes-only commit, so a removes-only commit that is
    not maintenance goes unnoticed by Silver.
  - Bronze conservation above the floor, and the Bronze checkpoint's RESTORE guard, remain the
    detectors.
- `delta.appendOnly` is lifted for the length of each delete. A crash inside that window refuses
  every Bronze start until maintenance runs again.
- **A topic that ever failed Bronze conservation can never be retained.** A skip is permanent, so its
  disk use is unbounded locally.
- **Coverage loses its gap-free verdict once anything is retired.** The retired region is a gap, so
  `Coverage.gap_free` is false for all time before the floor, and hydration can never claim
  completeness there. That is the honest reading: the records are gone.
- **Bronze loses OPTIMIZE and Gold loses VACUUM.**
  - OPTIMIZE is refused on Bronze, so small files accumulate until they are retired.
  - Gold tables cannot be VACUUMed by this tooling.
- **Retired rows can stay on disk.** A batch with any unretired row keeps all its retired rows
  (`retired_present`) until a later floor covers the whole batch.
- **Silver resets.** Once retention has run they no longer start at version 0, which changes
  ADR-0053 §7. A Silver checkpoint must exist before any floor advances.
- **Existing local tables.** Tables created before this change carry no retention properties; the
  drift check reads absence as Delta's default, which is observed to equal the declared value.
- **Internal APIs.** Floors at a version and live files are read through Delta's internal `DeltaLog`
  API via py4j, pinned to Delta 4.0.1.
- **A crashed run's lock** is taken over automatically only on the same host.

**Risks.**
- **A maintenance-side Delta conflict is not constructed deterministically.** What was observed is
  the other direction: a Bronze commit whose transaction began before maintenance's metadata commits
  fails when it commits, and maintenance completes. That is the safe direction -- the appended rows
  are simply not committed, and the Bronze query restarts idempotently -- but a DELETE losing to a
  concurrent append is inferred from Delta's conflict rules, not executed here.
- **Gold's own tables are refused by the VACUUM tooling rather than vacuumed.** Their readers (a
  build's recorded target versions) are not modelled, so the tooling declines rather than guess.
  Gold's disk is therefore not bounded by this step.
- **The Spark half of the coverage rule is proved with a stub ledger.** The retirement rule itself is
  unit-tested, and the rule against a real PostgreSQL ledger is the Bronze integration suite's
  coverage test; what the stub stands in for is only the ledger read beside the Delta read.
- The retention durations are chosen, not derived from a declared outage bound.
- Point 3c assumes Silver conservation at T proves Silver holds every unretired row at T.
- Point 3e assumes a completed Gold build needs nothing further from Silver's older versions.

### Implementation status (amendment 1)

Implemented by the Delta agent in Phase B, after the user's decision. Not yet integrated or
committed by the lead.

**Code.**

| File | What it carries |
|---|---|
| `stream/tables.py` | floor keys, parsing and `floor_column`; declared retention and its hours; drift that accepts only well-formed floors on a `retention_floors` declaration and reads an absent retention property as Delta's default; `SnapshotFacts.properties`, so floors and rows come from one snapshot; `RETENTION_SOURCE_OPTIONS`; `retained_adds`, `latest_removal_version` and `retention_start` |
| `stream/checkpoints.py` | `is_bronze_topic_table`; `delta_source` sets `ignoreDeletes=true` on a Bronze topic table and nowhere else, and refuses a fresh Bronze start that is not the first live version once floors exist |
| `stream/bronze.py` | `bronze_declaration(retention_floors=True)` |
| `stream/bronze_conservation.py` | retired offsets, `retired_present`, `append_only_lifted`, floors from the judged snapshot |
| `stream/silver.py` | `silver_sources(topic, starting_version)` and `silver_start_version` |
| `stream/silver_conservation.py` | `start_problem` and the floor filter on both sides |
| `stream/bronze_coverage.py` | retired offsets in the coverage rule: neither present nor gaps, the retired span, and `retired_gaps` |
| `stream/maintenance.py` (new) | evidence, `plan_retention`, `delete_predicate`, the lock, `advance_retention` with its step hooks, the audit table and its reconciliation, `reset_silver`, `vacuum_table`, `optimize_table` |
| `services/stream/maintenance.py` (new) | the `retention`, `vacuum`, `optimize` and `silver-reset` commands |

**Each required semantic, and the test that holds it.** Every test name carries `resource_bounds`;
the acceptance command is `pytest -m 'unit or integration' -k resource_bounds`.

| Semantic | Test |
|---|---|
| Only rows strictly below the floor are deleted, in whole batches | `..._floors_advance_only_as_far_as_every_consumer_has_read`, `..._the_delete_names_whole_batches_and_bounds_offsets_below_floors` (unit); `..._silver_reads_through_a_retention_delete_and_every_row_stays_accounted` (stream) |
| Offsets below the floor are RETIRED, never LOST | `..._offsets_below_the_floor_are_retired_whether_present_or_not`, `..._retirement_never_hides_a_loss_a_duplicate_or_another_topic_s_rows` (unit) |
| The floor never passes a required consumer position | `..._a_floor_past_any_required_position_is_refused_naming_it` (unit); `..._maintenance_never_passes_a_consumer_and_commits_nothing_when_refused` (stream) |
| A crash at any step leaves conservation right | `..._a_crash_at_each_maintenance_step_is_judged_correctly_by_conservation` (stream, one case per step boundary) |
| A crash leaving appendOnly off refuses a Bronze start until the next run restores it | the same test's lifted cases, through `start_bronze_query` |
| Maintenance is idempotent | the re-run and third-run assertions in the crash test and in `..._silver_reads_through_a_retention_delete...` |
| Every advance leaves a durable audit record, reconciled on re-run | the audit assertions in the crash test (`reconciled` per row) and in the stream and integration paths |
| `ignoreDeletes` on Bronze sources only | `..._ignore_deletes_is_the_only_option_a_bronze_source_is_granted`, `..._only_released_topic_tables_are_bronze_retention_sources` (unit); `..._ignore_deletes_is_granted_to_bronze_topic_sources_only` (stream) |
| A Silver reset starts at the first live version; version 0 is refused | `..._a_silver_reset_after_retention_starts_at_the_first_live_version` (stream) |
| Declared retention, and VACUUM and OPTIMIZE guards | `..._retention_is_declared_per_table_and_absent_means_delta_s_default`, `..._vacuum_is_refused_below_retention_without_the_check_or_behind_a_reader`, `..._optimize_is_refused_on_bronze_before_touching_the_table` (unit); `..._delta_default_retention_is_the_declared_retention`, `..._every_declared_table_is_created_with_its_declared_retention`, `..._vacuum_waits_for_silver_and_retention_and_optimize_skips_bronze` (stream) |
| A floor commit racing a Bronze append | `..._an_append_in_flight_across_maintenance_fails_loudly_and_state_holds` (stream) |
| Coverage treats retired offsets as neither present nor gaps | `..._a_row_is_retired_only_below_its_own_floor`, `..._retired_numbers_are_neither_present_nor_gaps` (still present and deleted), `..._a_session_cannot_vouch_across_a_retired_span`, `..._an_unbounded_retired_region_vouches_for_nothing` (unit); `..._coverage_reads_the_floors_from_the_rows_own_snapshot` (stream) |
| Kafka byte caps still stop Bronze loudly | `..._a_kafka_cap_that_evicts_unread_records_stops_bronze_loudly` (integration) |
| The end-to-end path against a real broker | `..._silver_reads_through_bronze_retention_against_a_real_broker` (integration) |

**Evidence (Phase B, on this toolchain).**
- **Offline, executed:**
  - the whole unit suite, after the coverage change: 2139 passed, 1 skipped (the skip is loud and
    environmental: the worktree has no `.venv`, so the toolchain test's venv-preference half cannot
    be observed);
  - the acceptance selection `pytest -m 'unit or integration' -k resource_bounds` collects 44 tests,
    and its unit half passes 42;
  - `ruff format --check .` and `ruff check .` clean over 433 files; `mypy` clean over 340 source
    files (the project's own configuration, which covers `tests`, `services` and `packages`);
  - the stream and integration fixtures validated without a JVM or a broker: the values both
    covered topics carry, the observation-log header names, the checkpoint offset files and the
    topic-id sidecar parsed by the real readers, and the ledger row shape.
- **On real Delta, a real broker and real PostgreSQL, executed** (each run held the shared
  heavy-suite lock alone, took it by pid and released it immediately):
  - the acceptance command, `pytest -m 'unit or integration' -k resource_bounds`: **44 passed**, 61 s;
  - the `resource_bounds` stream suite on Delta 4.0.1 and Temurin 17: **14 passed**, 379 s -- the
    crash case at every step boundary, the allowance, the reset start, the refusals, VACUUM and
    OPTIMIZE, the floor-versus-append race, and coverage reading the floors from the rows' own
    snapshot;
  - the Bronze and Silver Kafka integration suites as a regression: **9 passed**, 134 s.
  - One defect was found by these runs and it was in the test, not the code: the coverage stream
    test asserted that every Bronze row is an observation, ignoring that rows at or after the
    high-water mark are held back by design (measured: 12 observations, 18 beyond the mark). The
    assertion now states the rule's own accounting, and the suite passes as one run.
- **Two earlier runs are discarded, not carried forward.** They ran inside a window in which the
  lock was briefly held by two agents at once (this agent removed a lock it did not own; the fix is
  that `release()` now removes only a lock whose owner names its own process). Two heavy runs could
  therefore have overlapped, which is the likeliest cause of the py4j gateway socket failures and of
  a Gold test refusing because Silver was absent. Nothing measured in that window is cited as
  evidence here:
  - a `resource_bounds` stream run that passed 13 of 13, before the coverage test existed;
  - a stream regression reporting 4 failed, 130 passed and 21 errors. One cause was real and is
    fixed (Bronze's exact-properties assertion now expects the declared retention); the rest are
    unexplained and stay unexplained until a solo run says otherwise.

**Not done, and why.**
- A maintenance-side Delta conflict (its DELETE losing to a concurrent Bronze append) is not
  constructed deterministically. The observed direction is the other one: the in-flight Bronze
  commit fails.
- Gold's own tables are refused by the VACUUM tooling rather than vacuumed: their readers are not
  modelled.
- `bronze_coverage`'s Spark half is proved with a stub ledger; the rule against a real PostgreSQL
  ledger stays the Bronze integration suite's coverage test.

## Status
Proposed
