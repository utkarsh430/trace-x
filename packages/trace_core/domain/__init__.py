"""Domain layer: entities, value objects, enumerations, errors and state machines.

Pure functions and immutable values only. Nothing here performs I/O — that lives
in `trace_core.repositories` (CLAUDE.md §6), which is what lets the whole domain
be property-tested without a container.
"""
