"""Data-side packages (CLAUDE.md §4).

`data.generator` is the Track A synthetic transaction generator; `data.adapters`
holds `SourceAdapter` implementations (ADR-0022). Neither is part of the
installed `trace_core` distribution: they are build- and evaluation-time tools,
so they depend on `trace_core`, never the reverse.

`data/external/`, `data/generated/` and `data/lake/` hold datasets and are
gitignored — a dataset is never committed.
"""
