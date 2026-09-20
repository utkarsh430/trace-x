# ADR-0015: Delta data layout — local OSS and Databricks decided separately, by benchmark

- **Status:** **Proposed** — may not be Accepted until the Phase 3 benchmark produces committed output
- **Date:** 2026-09-10
- **Phase:** 3

## Context
"Partition by date, Z-order by the high-cardinality key" was the correct Delta default around 2022. It
is no longer automatically correct, and the local and cloud answers now genuinely differ:

- **Databricks:** liquid clustering is GA (DBR 15.2+) and **replaces** both partitioning and Z-ordering.
  `CLUSTER BY AUTO` with predictive optimization (Unity Catalog managed tables, DBR 15.4 LTS+)
  additionally removes manual key selection by adapting to observed query patterns.
- **Open-source Delta 4.0.1:** supports liquid clustering via explicit `CLUSTER BY`, but **not** `AUTO`
  and **not** predictive optimization — those are Databricks-managed capabilities requiring Unity
  Catalog.

Different capability sets mean these are two questions, not one. Answering them once — in either
direction — would be prescription rather than engineering.

## Decision
**No layout is prescribed in advance.** Layout is table DDL, configured per environment, and chosen by
measurement.

`benchmarks/delta_layout/` runs the project's **actual Gold query mix** — point lookup by `account_id`,
time-range scan, and the training-set extract — across each viable option at ≥ 10 M rows, recording
**files scanned, bytes scanned, wall-clock, and `OPTIMIZE` cost**.

| Option | Local OSS Delta 4.0.1 | Databricks |
|---|---|---|
| `PARTITIONED BY (event_date)` | viable; small-file skew risk at local volumes | discouraged for new tables |
| Partition + `ZORDER BY (account_id)` | viable; needs manual `OPTIMIZE` scheduling | superseded by liquid clustering |
| `CLUSTER BY (account_id, event_date)` liquid | **supported in OSS 4.0.1**; needs manual `OPTIMIZE` | supported |
| `CLUSTER BY AUTO` + predictive optimization | **not available** | **candidate default** |

**This ADR is finalized after those numbers exist and cites them.** If the local benchmark favours one
option while Databricks favours another, the ADR records the divergence and its cause rather than
forcing artificial consistency — layout is DDL, so divergence costs zero application code.

A CI check asserts this ADR references non-empty files under `benchmarks/delta_layout/`, so it cannot be
finalized before its evidence exists.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Prescribe partition + Z-order everywhere | The conventional answer, and outdated on Databricks where liquid clustering supersedes both. Carrying both forward is actively harmful |
| Prescribe liquid clustering everywhere | Better-informed, but still unmeasured — and `AUTO` plus predictive optimization simply does not exist in OSS Delta |
| Decide once and port the local choice to Databricks | Ignores that the runtimes differ in capability. Consistency for its own sake |
| Write the ADR now, back-fill the benchmark | Exactly the failure this process guards against: the conventional answer with justification added afterwards |

## Consequences
**Positive.** The decision is evidence-backed and environment-appropriate. The benchmark harness is
reusable when volumes or query patterns change.
**Negative.** Phase 3 carries additional benchmark work before its exit gate. A local/cloud divergence
will invite "why is it different?" — answered by this ADR.
**Risks.** The benchmark is run at a scale unrepresentative of production, making the conclusion
misleading. Mitigation: ≥ 10 M rows and the real query mix, with the scale recorded alongside the
result.

## Measured result (local OSS Delta) — `run_id: bench-20260918-135350-delta-layout-36816cc7`

Full report: `benchmarks/delta_layout/REPORT.md`; machine-readable results under
`benchmarks/delta_layout/results/`.
- **Scale and setup:** 10,000,000 rows shaped like `gold.observations`, 5 repetitions, on a
  clean tree.
- **Options compared:** six layouts, including two controls, all filled by Gold's own MERGE.
- **Queries:** four shapes. Results were identical across layouts on every shape.
- **Declared before measurement:** the ranking rule, and an OPTIMIZE max file size of 128 MiB,
  chosen from table size alone. At the 1 GiB default the 0.6 GB table packs into one file, which
  would compare file counts rather than layouts.

**Ranking by the declared rule** (geometric mean over the four shapes of bytes read relative to
the no-layout control; lower is better):

| Rank | Layout | Score | `context_lookup` | `context_extract` | `account_history` | `time_range` |
|---|---|---|---|---|---|---|
| 1 | `PARTITIONED BY (event_date)` + `ZORDER BY (account_id)` | 0.499 | 2.601 | 1.424 | 0.493 | 0.034 |
| 2 | `PARTITIONED BY (event_date)` | 0.499 | 2.601 | 1.424 | 0.493 | 0.034 |
| 3 | `CLUSTER BY (account_id, occurred_at)` liquid | 0.807 | 2.101 | 1.049 | 0.455 | 0.424 |
| 4 | no layout, compacted | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| 5 | no layout (today's `gold.observations`) | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| 6 | `PARTITIONED BY (stream)` | 1.169 | 2.160 | 0.579 | 1.221 | 1.221 |

What the numbers say, in the same run (`run_id: bench-20260918-135350-delta-layout-36816cc7`):
- **The rule's winner is date partitioning.**
  - It wins on the two shapes this ADR named but no code yet issues: `time_range` and
    `account_history`.
  - It **loses on both shapes Gold's code issues today**: `context_lookup` and
    `context_extract`, from `gold.read_context_rows` and the parity/training extract.
  - On those two shapes it reads more bytes than no layout at all.
- **Z-order added nothing measurable.** Files and bytes were identical to plain date partitioning:
  one small file per date partition leaves nothing to reorder. It ranks first only on the rule's
  last tie-break, wall time, whose ranges overlap. It costs an OPTIMIZE per partition.
- **Liquid clustering is the middle option.** It is second-best on `account_history`, needs
  table features (1, 7) with clustering and domainMetadata, and cannot prune `time_range` by
  date the way partitioning can.
- **Partitioning by stream** helps only the full extract, which reads one stream.
- **Compaction alone changed nothing:** the control's files were already large.

## Decision (local), proposed for acceptance at Phase 3 exit

By the declared rule, the local layout for `gold.observations` is **`PARTITIONED BY (event_date)`**,
with `event_date` generated from `occurred_at`.
- It is chosen **without** `ZORDER BY (account_id)`. The rule's tie-break picked Z-order on
  overlapping wall-time noise. The two are identical on every declared metric, so the simpler
  option is recorded and the choice is disclosed here.
- The table needs protocol (1, 7) with generatedColumns.
- The cost is stated plainly: until the account and time-range readers exist (Phase 5
  investigation tools), today's two Gold reads pay 2.6× and 1.4× the bytes of no layout. The
  user may instead defer the layout change until those readers land. That is a legitimate
  acceptance-time choice, and the numbers above are what it rests on.

Databricks is decided separately in Phase 12, per the Context above. Nothing here predicts it.

## Status
Proposed — Accepted only on Phase 3 exit, citing committed benchmark output.
