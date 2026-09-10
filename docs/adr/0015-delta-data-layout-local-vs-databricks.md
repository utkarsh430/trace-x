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

## Status
Proposed — Accepted only on Phase 3 exit, citing committed benchmark output.
