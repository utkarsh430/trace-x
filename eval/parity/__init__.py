"""Feature parity for `P3.feature-parity` (ADR-0056).

Two questions, answered separately (PHASE3_PLAN §2 B7):

* **Implementation parity on identical ordered input.** What the online store served must equal the
  reference's AS_SERVED evaluation of the same observations in the store's order, and Gold must
  equal the reference's EVENT_TIME_COMPLETE evaluation of the complete history.
* **Arrival skew.** How often what was served differs from what the complete history says, on
  windowed counters. A separate metric, gated only on the representative partition.

Modules: `partition` (the frozen partitions and lateness model), `comparator`, `skew`, `guard`,
`replay` (the reference over a long stream), `served` and `history` (reading both sides), `driver`
(the gateway), `lake` (Bronze, Silver and Gold on Spark), `run` (orchestration and the CLI),
`record` and `report`.
"""
