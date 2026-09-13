"""Combining signals into a banded, explainable decision.

Phase 2 is rules-only: the model tiers of `docs/ARCHITECTURE.md` §7 arrive in
Phase 4, and the combination function is the seam they plug into. Nothing here
involves an LLM, and nothing here reads a label.
"""

from trace_core.scoring.banding import (
    DEFAULT_CONFIG,
    ThresholdConfig,
    band,
    combine,
    load_thresholds,
)
from trace_core.scoring.decision import APPROVE, FLAG, build_decision

__all__ = [
    "APPROVE",
    "DEFAULT_CONFIG",
    "FLAG",
    "ThresholdConfig",
    "band",
    "build_decision",
    "combine",
    "load_thresholds",
]
