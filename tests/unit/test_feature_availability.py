"""The UNAVAILABLE mechanism (ADR-0022).

`docs/TESTING.md` §5 lists "silent zero-imputation" among the risks that would be
severe and silent: an uncovered feature imputed to 0 makes cross-dataset transfer
results meaningless, and nothing about the output looks wrong. These tests are
that risk's countermeasure, so they assert the *type* refuses the mistake rather
than that callers remember not to make it.
"""

from __future__ import annotations

import datetime as dt

import pytest

from trace_core.contracts.canonical import (
    ALWAYS_REQUIRED,
    CanonicalField,
    CanonicalTransaction,
)
from trace_core.domain.errors import FeatureUnavailableError
from trace_core.domain.time import event_time
from trace_core.features import (
    REFERENCE_FEATURES,
    FeatureContext,
    FeatureRegistry,
    FeatureSpec,
    FeatureValue,
)
from trace_core.features.semantics import RowLocal

pytestmark = pytest.mark.unit

# The reference features are row-local, so an empty snapshot is the whole
# context they need. Phase 2 features read state; these deliberately do not,
# so a failure here can only be the coverage mechanism's fault.
EMPTY = FeatureContext(as_of=event_time(dt.datetime(2026, 1, 1, tzinfo=dt.UTC)))

GEO_FIELDS = frozenset({CanonicalField.LATITUDE, CanonicalField.LONGITUDE})


def _tx(coverage: frozenset[CanonicalField], **extra: object) -> CanonicalTransaction:
    return CanonicalTransaction(
        source_dataset="probe",
        source_row_id="row-1",
        field_coverage=coverage,
        transaction_id="tx_1",
        account_id="acct_000001",
        amount_minor=1999,
        currency="USD",
        occurred_at=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
        ingested_at=dt.datetime(2026, 1, 1, 0, 0, 1, tzinfo=dt.UTC),
        **extra,  # type: ignore[arg-type]
    )


FULL = frozenset(CanonicalField)
MINIMAL = frozenset(ALWAYS_REQUIRED)


# ------------------------------------------------- the central property ----


def test_an_uncovered_feature_is_unavailable_not_zero() -> None:
    """The headline risk of ADR-0022, stated directly.

    A geo feature on a source with no geography must not report 0.0 -- zero is a
    measurement, and it is indistinguishable from a real one downstream.
    """
    spec = REFERENCE_FEATURES.get("geo_precision")
    result = spec.evaluate(_tx(MINIMAL), EMPTY)

    assert not result.is_available
    assert result.or_none() is None
    assert result.or_none() != 0.0
    assert result._value != 0
    assert result.missing_fields == GEO_FIELDS


def test_reading_the_value_of_an_unavailable_feature_raises() -> None:
    """Not a default, not a NaN: an exception. A caller that assumed
    availability finds out here rather than scoring on a fabricated number."""
    result = REFERENCE_FEATURES.get("geo_precision").evaluate(_tx(MINIMAL), EMPTY)
    with pytest.raises(FeatureUnavailableError, match="never as 0"):
        _ = result.value


def test_the_error_names_the_fields_that_were_missing() -> None:
    """'IEEE-CIS supplies no latitude' is actionable; 'null' is not."""
    result = REFERENCE_FEATURES.get("geo_precision").evaluate(_tx(MINIMAL), EMPTY)
    with pytest.raises(FeatureUnavailableError, match="latitude"):
        _ = result.value


def test_a_covered_feature_computes_normally() -> None:
    spec = REFERENCE_FEATURES.get("geo_precision")
    result = spec.evaluate(_tx(FULL, latitude=51.5, longitude=-0.1), EMPTY)
    assert result.is_available
    assert result.value == pytest.approx(51.6)


def test_truthiness_tracks_availability() -> None:
    """So `if feature: use(feature.value)` is correct rather than a trap."""
    assert bool(REFERENCE_FEATURES.get("amount_magnitude").evaluate(_tx(MINIMAL), EMPTY))
    assert not bool(REFERENCE_FEATURES.get("geo_precision").evaluate(_tx(MINIMAL), EMPTY))


def test_unavailable_cannot_be_constructed_without_a_reason() -> None:
    """An UNAVAILABLE with no missing fields is indistinguishable from a bug."""
    with pytest.raises(ValueError, match="requires the fields that were missing"):
        FeatureValue.unavailable("x", frozenset())


def test_feature_values_are_immutable() -> None:
    result = REFERENCE_FEATURES.get("amount_magnitude").evaluate(_tx(MINIMAL), EMPTY)
    with pytest.raises(AttributeError):
        result._value = 0.0  # type: ignore[misc]


def test_a_feature_value_is_not_a_number() -> None:
    """No __float__, __int__ or arithmetic, so it cannot silently become 0.0 in
    an expression that forgot to check availability."""
    result = REFERENCE_FEATURES.get("geo_precision").evaluate(_tx(MINIMAL), EMPTY)
    for dunder in ("__float__", "__int__", "__add__", "__radd__", "__index__"):
        assert not hasattr(result, dunder), f"FeatureValue exposes {dunder}"


# ----------------------------------------------------- spec discipline ----


def test_a_feature_must_declare_required_fields() -> None:
    """A feature needing nothing can never be UNAVAILABLE, so it silently claims
    to work on every source regardless of coverage."""
    with pytest.raises(ValueError, match="declares no required_fields"):
        FeatureSpec(
            feature_id="claims_everything",
            description="x",
            required_fields=frozenset(),
            semantics=RowLocal(),
            compute=lambda tx, ctx: 1.0,
        )


def test_every_registered_feature_declares_required_fields() -> None:
    assert len(REFERENCE_FEATURES) >= 3
    for spec in REFERENCE_FEATURES:
        assert spec.required_fields
        assert spec.required_fields <= frozenset(CanonicalField)
        assert spec.description.strip()


def test_duplicate_registration_is_refused() -> None:
    registry = FeatureRegistry()
    spec = FeatureSpec(
        feature_id="dup",
        description="x",
        required_fields=frozenset({CanonicalField.AMOUNT_MINOR}),
        semantics=RowLocal(),
        compute=lambda tx, ctx: 1.0,
    )
    registry.register(spec)
    with pytest.raises(ValueError, match="already registered"):
        registry.register(spec)


# ------------------------------------------------ coverage reporting ------


def test_computable_on_reports_the_intersecting_subset() -> None:
    """ADR-0021: every transfer metric is reported alongside the size of the
    intersecting feature subset, so a thin overlap invalidates it visibly."""
    on_full = REFERENCE_FEATURES.computable_on(FULL)
    on_minimal = REFERENCE_FEATURES.computable_on(MINIMAL)
    assert on_minimal < on_full
    assert "amount_magnitude" in on_minimal
    assert "geo_precision" not in on_minimal
    assert "geo_precision" in on_full


def test_evaluate_all_reports_absence_rather_than_omitting_it() -> None:
    """A caller must be able to tell "not computed" from "not in the registry"."""
    results = REFERENCE_FEATURES.evaluate_all(_tx(MINIMAL), EMPTY)
    assert set(results) == {s.feature_id for s in REFERENCE_FEATURES}
    assert not results["geo_precision"].is_available
    assert results["amount_magnitude"].is_available


# ------------------------------------- end to end through an adapter ------


def test_a_partial_coverage_source_yields_unavailable_end_to_end() -> None:
    """The Phase 4B shape, rehearsed now.

    IEEE-CIS has no merchant id and no lat/lon. A source declaring that coverage
    must produce UNAVAILABLE geo features rather than zeros, all the way from the
    adapter's declaration to the feature value.
    """
    partial = frozenset(ALWAYS_REQUIRED | {CanonicalField.CARD_ID})
    transaction = _tx(partial, card_id="card_000001")

    results = REFERENCE_FEATURES.evaluate_all(transaction, EMPTY)
    assert not results["geo_precision"].is_available
    assert not results["merchant_context_present"].is_available
    assert results["amount_magnitude"].is_available

    for feature_id, value in results.items():
        if not value.is_available:
            assert value.or_none() is None, f"{feature_id} imputed a value"


def test_the_generator_adapter_can_compute_every_reference_feature() -> None:
    """Track A has full coverage, so nothing is UNAVAILABLE there. The contrast
    with the partial-coverage case above is what the mechanism measures."""
    from data.adapters.generator_adapter import FULL_COVERAGE

    assert REFERENCE_FEATURES.computable_on(FULL_COVERAGE) == frozenset(
        s.feature_id for s in REFERENCE_FEATURES
    )
