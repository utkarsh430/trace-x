"""The Delta layout benchmark backing ADR-0015 (Phase 3 Step 14, `P3.layout-benchmark`).

- `generator`: a deterministic, seeded row generator shaped like `gold.observations`; pure stdlib.
- `spec`: the layout variants, the query shapes and where each comes from, the ranking rule
  declared before measurement, and the validation every result passes; pure.
- `report`: the markdown report and the run record; pure.
- `run`: the Spark driver (`make bench-layout`).
"""
