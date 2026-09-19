"""The shared feature-semantics suite against the naive reference, in both evaluation modes.

The suite lives in `feature_semantics_suite.py` and is implementation agnostic. The Redis store
runs the as-served half; Phase 3's replay and Gold run the halves that match what they claim,
each subclassing the unmodified suite (ADR-0046).
"""

from __future__ import annotations

import pytest
from tests.conformance.feature_semantics_suite import (
    ReferenceAsServedConformanceTest,
    ReferenceEventTimeCompleteConformanceTest,
)

pytestmark = pytest.mark.conformance


class TestReferenceAsServed(ReferenceAsServedConformanceTest):
    """The naive store must pass the literal fixtures, as served."""


class TestReferenceEventTimeComplete(ReferenceEventTimeCompleteConformanceTest):
    """The naive implementation must pass the literal fixtures over a complete history."""
