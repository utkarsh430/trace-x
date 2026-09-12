"""Feature definitions — the single source of truth for feature semantics.

One definition per feature, read by both the online (Redis) and offline (Spark)
paths, so the two cannot drift in meaning. Divergence in their *implementations*
is measured as `feature_parity_drift` rather than assumed away
(docs/DATA_ENGINEERING.md §4, ADR-0032).
"""

from trace_core.features.context import (
    INSUFFICIENT_HISTORY,
    FeatureContext,
    Observation,
    Profile,
    WindowState,
)
from trace_core.features.definitions import FEATURE_COUNT, ONLINE_FEATURES
from trace_core.features.reference_features import REFERENCE_FEATURES
from trace_core.features.spec import (
    FEATURE_SET_VERSION,
    FeatureRegistry,
    FeatureSpec,
    FeatureState,
    FeatureValue,
)

__all__ = [
    "FEATURE_COUNT",
    "FEATURE_SET_VERSION",
    "INSUFFICIENT_HISTORY",
    "ONLINE_FEATURES",
    "REFERENCE_FEATURES",
    "FeatureContext",
    "FeatureRegistry",
    "FeatureSpec",
    "FeatureState",
    "FeatureValue",
    "Observation",
    "Profile",
    "WindowState",
]
