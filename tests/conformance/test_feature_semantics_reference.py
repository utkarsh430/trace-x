"""Runs the shared feature-semantics suite against the naive reference store.

The suite itself lives in `feature_semantics_suite.py` and is implementation
agnostic. Phase 2 adds a second subject (the Redis store) and Phase 3 a third
(Spark Gold), each subclassing the **unmodified** suite -- which is what makes
"online and offline agree" a checked property rather than an assurance.
"""

from __future__ import annotations

import pytest
from tests.conformance.feature_semantics_suite import ReferenceStoreConformanceTest

pytestmark = pytest.mark.conformance


class TestReferenceFeatureStore(ReferenceStoreConformanceTest):
    """The naive implementation must pass the suite it defines the meaning of."""
