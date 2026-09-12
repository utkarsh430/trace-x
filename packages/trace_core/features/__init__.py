"""Feature definitions — the single source of truth for feature semantics.

One definition per feature, read by both the online (Redis) and offline (Spark)
paths, so the two cannot drift in meaning. Divergence in their *implementations*
is measured as `feature_parity_drift` rather than assumed away
(docs/DATA_ENGINEERING.md §4).
"""

from trace_core.features.spec import (
    REFERENCE_FEATURES,
    FeatureRegistry,
    FeatureSpec,
    FeatureValue,
)

__all__ = [
    "REFERENCE_FEATURES",
    "FeatureRegistry",
    "FeatureSpec",
    "FeatureValue",
]
