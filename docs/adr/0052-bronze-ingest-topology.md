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
- **Step 11 retention (critic finding B6).** Bounded local lake retention needs DELETE, which means
  lifting `delta.appendOnly`. Every row deleted inside a consumed range would then count as missing,
  for good. Before Step 11 starts, decide a declared, audited retention floor per partition that
  conservation respects.

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
  - **B6, open.** Step 11's bounded retention needs a declared retention floor that conservation
    respects (see the open questions).
  - **C, addressed.**
    - The integration oracle keeps a null header value.
    - A coverage case now spreads records over every partition.
    - A checkpoint that starts above its predecessor's end has unit tests.
  - **C, still open.** CI provisioning of PostgreSQL for the live coverage test.
  - **`coverage` exit status.** A live gateway's session always has an open tail, so `coverage`
    exits 1 while a writer runs, by design. It prints `open_gaps`, so a live tail can be told
    apart from a bounded gap.
- `P3.kafka-ingest` is not PASS before the step's exit evidence.

## Status
Proposed
