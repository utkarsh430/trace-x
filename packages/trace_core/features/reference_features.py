"""Features whose only job is to exercise the UNAVAILABLE mechanism.

Deliberately trivial, and deliberately kept out of the production registry.
`docs/ROADMAP.md` Phase 1 task 5 requires the mechanism to be tested *before any
real feature depends on it*, and that test is worth keeping afterwards: it
exercises the coverage check on features simple enough that a failure can only
be the mechanism's fault, never the feature's arithmetic.

`geo_precision` is the canonical case. It needs latitude and longitude, which
IEEE-CIS does not supply, so on Track B it must evaluate to `UNAVAILABLE` rather
than to a plausible-looking `0.0`.

Production features live in `trace_core.features.definitions`.
"""

from __future__ import annotations

from typing import Final

from trace_core.contracts.canonical import CanonicalField, CanonicalTransaction
from trace_core.features.context import FeatureContext
from trace_core.features.semantics import RowLocal
from trace_core.features.spec import FeatureRegistry, FeatureSpec

REFERENCE_FEATURES: Final = FeatureRegistry()


def _amount_magnitude(tx: CanonicalTransaction, ctx: FeatureContext) -> float:
    del ctx
    # Square root of the absolute value: defined at zero, and a refund's
    # magnitude is its size, not its sign.
    return float(float(abs(tx.amount_minor)) ** 0.5)


def _merchant_context_present(tx: CanonicalTransaction, ctx: FeatureContext) -> float:
    del ctx
    return float(tx.merchant_id is not None and tx.merchant_mcc is not None)


def _geo_precision(tx: CanonicalTransaction, ctx: FeatureContext) -> float:
    del ctx
    return abs(tx.latitude or 0.0) + abs(tx.longitude or 0.0)


REFERENCE_FEATURES.register(
    FeatureSpec(
        feature_id="amount_magnitude",
        description="Order of magnitude of the transaction amount in minor units.",
        required_fields=frozenset({CanonicalField.AMOUNT_MINOR}),
        semantics=RowLocal(),
        compute=_amount_magnitude,
    )
)

REFERENCE_FEATURES.register(
    FeatureSpec(
        feature_id="merchant_context_present",
        description="Whether both merchant identity and MCC accompany the transaction.",
        required_fields=frozenset({CanonicalField.MERCHANT_ID, CanonicalField.MERCHANT_MCC}),
        semantics=RowLocal(),
        compute=_merchant_context_present,
    )
)

REFERENCE_FEATURES.register(
    FeatureSpec(
        feature_id="geo_precision",
        description=(
            "Coordinate precision of the transaction location. The canonical example of a "
            "feature that is UNAVAILABLE rather than zero on a source without geography."
        ),
        required_fields=frozenset({CanonicalField.LATITUDE, CanonicalField.LONGITUDE}),
        semantics=RowLocal(),
        compute=_geo_precision,
    )
)
