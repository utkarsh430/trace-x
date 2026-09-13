"""Adapters over the stores (CLAUDE.md §4: ports and adapters).

Driver imports live here and nowhere else. `features/`, `rules/` and `services/`
see domain types -- a `FeatureContext`, a `Case` -- and never a Redis client or a
SQL connection, so the choice of store stays a decision this layer owns rather
than one diffused through the business logic.
"""
