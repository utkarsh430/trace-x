# Step 13 spike — what one Silver micro-batch actually costs

**Diagnostics, not acceptance evidence.** Nothing here produces a `RunManifest`, so no figure in
this directory may be cited as a benchmark result. `P3.stream-throughput`'s evidence is the
`make load-stream` record it names. These probes exist to answer one question the Step 13 failures
kept raising and never settled: *what does a Silver micro-batch spend its time on, and what does
that cost grow with?*

ADR-0053 Amendment 2 is written from these two measurements.

## The probes

Both build the project's own Spark session (`local[4]`, 2 GiB, 2 shuffle partitions -- the stream
benchmark's declared consumer) and drive the **real** Silver query over a real Bronze table. Only
the canonical table's seeded rows are synthetic, and they are written through the declared schema.

| Probe | Question | Controls |
|---|---|---|
| `silver_scaling_probe.py` | does a batch cost more against a bigger table? | one batch per point, fresh lake per point; `--mode rows` varies the canonical rows with the file count fixed, `--mode files` varies the file count with the rows fixed |
| `silver_batch_breakdown.py` | where does a batch's time go, cold and in steady state? | one lake, `--batches` consecutive micro-batches of one Bronze file each (`maxFilesPerTrigger=1`), with `classify_frame`, `_write_batch`, `_assert_unique`, `batch_merge_bounds`, `_merge_late_events` and the checkpoint's `merge`/`append` timed separately |

```sh
export PYTHONPATH=$PWD; set -a; . ./.env; set +a
.venv/bin/python spikes/step13-silver-batch-cost/silver_scaling_probe.py \
  --mode rows --points 0,250000,1000000,2000000,3000000 --files 128 --batch-rows 7000 \
  --out results/scaling-rows-before.json
.venv/bin/python spikes/step13-silver-batch-cost/silver_batch_breakdown.py \
  --seed-rows 0 --batches 8 --out results/breakdown-seed0-before.json
```

## What they found

- **A batch's cost is fixed, not proportional.** 7,000 rows took 7.46 s against an empty canonical
  table and 7.41 s against 2,000,000 rows; over 0 to 3,000,000 rows the whole batch grew by about
  1 s. Steady state over eight consecutive batches: 5.20 s per batch at 0 rows, 5.90 s at
  2,000,000.
- **The fixed part was mostly Spark jobs, not Spark work.** Of the 5.20 s: canonical MERGE 0.98 s,
  `late_events` re-derivation 0.80 s, uniqueness assertion 0.74 s, bounds aggregate 0.05 s, and
  about 2.3 s on the disposition counts and the four `isEmpty()` probes -- eleven jobs per batch in
  a local session where a job costs about 0.2 s before it does anything.
- **So D29 was mis-ranked.** The whole-table reads it names are real, and about 0.35 s per million
  rows; they were not why the consumer could not keep up at a 2-second trigger.

## What the amended sink measures

Same machine, same seeds, same session settings, steady state over eight consecutive batches:

| canonical rows | before | after | after, `snapshotPartitions=4` |
|---|---|---|---|
| 0 | 5.20 s | 3.80 s | 3.52 s |
| 2,000,000 | 5.90 s | 4.70 s | — |

`breakdown-seed0-after-snapshot4.json` is why `LOCAL_DELTA_CONF` exists: Delta replays a table's
log in 50 partitions by default, which on a one-executor session is 50 task launches per read and
per commit. Four took about 7% off the batch and about a fifth off its MERGE.

`breakdown-seed0-after-21k-rows.json` measures the marginal cost of rows, and carries a caveat
that matters: each probe batch admits **one** Bronze file, so the admission UDF runs in a single
task. 7,000 rows cost 3.80 s and 21,000 cost 6.60 s, so 0.20 ms per row **on one core**. A real
Bronze commit writes one file per Kafka partition, so the same work spreads across tasks; the
number bounds the per-row cost from above, it does not predict the benchmark.

## Files

`*-before.json` are the measurements ADR-0053 Amendment 2 was written from; `*-after.json` are the
same probes on the amended sink.
