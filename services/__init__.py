"""Thin entrypoints over `trace_core` (CLAUDE.md §4).

Each service is a deployment shape, not a second codebase: every one of them is
a few dozen lines that wire `trace_core` to a runtime. A process boundary exists
here only where `docs/ARCHITECTURE.md` §2 states a technical reason for it.
"""
