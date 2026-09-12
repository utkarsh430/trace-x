"""The online feature set, checked against the things that justify it.

A feature set is easy to pad and hard to audit. These tests hold it to three
claims that would otherwise be assertions in a docstring:

1. **Every feature is declared well enough for Phase 3 to reimplement it.** It
   names its inputs, its semantics and its approximation status, so the offline
   version can be compiled from the declaration rather than re-derived from prose
   (ADR-0032).
2. **Every documented fraud signature has something on the hot path that could
   surface it.** A scenario with no corresponding feature is a signature the
   gateway is structurally blind to, and Phase 9's per-pattern metrics would
   report that blindness as a model failure rather than a missing input.
3. **No feature reads anything the canonical contract does not supply.** A
   feature reaching around `CanonicalTransaction` would work on Track A and
   fabricate signal on Track B.
"""

from __future__ import annotations

import datetime as dt

import pytest

from trace_core.contracts.canonical import ALWAYS_REQUIRED, CanonicalField, CanonicalTransaction
from trace_core.domain.enums import FraudPattern
from trace_core.domain.time import event_time
from trace_core.features import FEATURE_COUNT, FEATURE_SET_VERSION, FeatureContext
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.semantics import (
    Aggregation,
    CardinalityStorage,
    PairwiseWithPrevious,
    ProfileAttribute,
    RowLocal,
    WindowedAggregate,
)

pytestmark = pytest.mark.unit

EMPTY = FeatureContext(as_of=event_time(dt.datetime(2026, 3, 1, tzinfo=dt.UTC)))

# ROADMAP Phase 2 asks for "~20 features". The count is pinned because a feature
# quietly added or removed changes what every recorded decision means.
MINIMUM_FEATURES = 20


def test_the_feature_set_meets_its_declared_size() -> None:
    assert len(ONLINE_FEATURES) == FEATURE_COUNT
    assert len(ONLINE_FEATURES) >= MINIMUM_FEATURES


def test_the_feature_set_version_is_semantic() -> None:
    """Recorded on every decision and in every run manifest: a number produced
    by a different feature set is not comparable to one produced by this one."""
    parts = FEATURE_SET_VERSION.split(".")
    assert len(parts) == 3 and all(p.isdigit() for p in parts)


def test_every_feature_declares_required_fields() -> None:
    """A feature that needs nothing can never be UNAVAILABLE, which means it
    silently claims to work on every source regardless of coverage (ADR-0022)."""
    for spec in ONLINE_FEATURES:
        assert spec.required_fields, spec.feature_id


def test_every_feature_declares_a_description() -> None:
    """Descriptions reach an analyst through the decision's reasons; a blank one
    makes an explanation unexplainable."""
    for spec in ONLINE_FEATURES:
        assert len(spec.description) > 30, f"{spec.feature_id} is barely described"


def test_no_production_feature_is_row_local() -> None:
    """Fraud is a departure from a baseline, and a baseline is state.

    A row-local production feature would be one computed without reference to
    any history, which cannot express "unusual for this account" -- the shape
    every scenario in docs/FRAUD_SCENARIOS.md takes.
    """
    for spec in ONLINE_FEATURES:
        assert not isinstance(spec.semantics, RowLocal), spec.feature_id


def test_every_feature_has_a_known_offline_translation() -> None:
    """The property Phase 3 depends on: three shapes, three known translations."""
    for spec in ONLINE_FEATURES:
        assert isinstance(
            spec.semantics, WindowedAggregate | ProfileAttribute | PairwiseWithPrevious
        ), f"{spec.feature_id} has no declared offline translation"


def test_exactly_the_declared_features_are_approximate() -> None:
    """Approximation is a property to be declared, not discovered later.

    Only three features are estimates, and each for a stated reason (ADR-0034):
    two distinct counts whose cardinality is bounded by the population sharing an
    entity rather than by one entity's own behaviour, and a robust z-score whose
    online median is computed over a bounded recent sample. Everything else must
    agree EXACTLY with the offline computation, and a parity tolerance above zero
    anywhere else would be hiding a bug (docs/DATA_ENGINEERING.md §4).
    """
    approximate = {s.feature_id for s in ONLINE_FEATURES if s.approximate}
    expected = {
        "ip_distinct_accounts_1h",
        "merchant_distinct_accounts_1h",
        "amount_zscore_vs_account",
    }
    assert approximate == expected


def test_every_distinct_count_declares_a_storage_class() -> None:
    """Declared, never inferred. A feature that chose its representation from
    observed cardinality would change its own error characteristics under load --
    exactly when a reader most needs to know what a value means."""
    for spec in ONLINE_FEATURES:
        semantics = spec.semantics
        if isinstance(semantics, WindowedAggregate) and (
            semantics.aggregation is Aggregation.DISTINCT_COUNT
        ):
            assert semantics.storage is not None, spec.feature_id
            assert spec.approximate == (semantics.storage is CardinalityStorage.APPROXIMATE)


def test_the_storage_class_split_follows_the_stated_criterion() -> None:
    """EXACT where cardinality is bounded by one entity's own behaviour;
    APPROXIMATE where it is bounded by the population sharing that entity.

    Pinned because the split is the whole substance of ADR-0034: a feature moved
    across it silently changes whether its parity comparison is an equality or a
    tolerance.
    """
    exact: set[str] = set()
    approximate: set[str] = set()
    for spec in ONLINE_FEATURES:
        semantics = spec.semantics
        if isinstance(semantics, WindowedAggregate) and semantics.storage is not None:
            target = approximate if semantics.storage is CardinalityStorage.APPROXIMATE else exact
            target.add(spec.feature_id)
    assert exact == {
        "account_distinct_merchants_1h",
        "account_distinct_mcc_5m",
        "account_distinct_devices_24h",
        "account_distinct_countries_24h",
        "device_distinct_accounts_24h",
    }
    assert approximate == {"ip_distinct_accounts_1h", "merchant_distinct_accounts_1h"}


def test_amount_aggregates_require_currency() -> None:
    """Summing across currencies would invent a number: no FX source exists."""
    for spec in ONLINE_FEATURES:
        semantics = spec.semantics
        money = isinstance(semantics, WindowedAggregate) and semantics.aggregation in {
            Aggregation.AMOUNT_SUM,
            Aggregation.AMOUNT_CV,
        }
        if money or spec.feature_id == "amount_zscore_vs_account":
            assert CanonicalField.CURRENCY in spec.required_fields, (
                f"{spec.feature_id} aggregates money without requiring a currency"
            )


def test_features_reading_geography_require_both_coordinates() -> None:
    """Half a coordinate pair is not a location, and the UNAVAILABLE decision has
    to be all-or-nothing for IEEE-CIS to be handled correctly."""
    for spec in ONLINE_FEATURES:
        has_lat = CanonicalField.LATITUDE in spec.required_fields
        has_lon = CanonicalField.LONGITUDE in spec.required_fields
        assert has_lat == has_lon, f"{spec.feature_id} requires only one coordinate"


# --- the correspondence with documented fraud signatures --------------------

# Each scenario maps to features that could plausibly surface its published
# SIGNATURE (docs/FRAUD_SCENARIOS.md §3). Derived from the signature, which is a
# property of the data -- never from the label, which lives in a schema the
# application role cannot read at all (ADR-0004).
SIGNATURE_FEATURES: dict[FraudPattern, set[str]] = {
    FraudPattern.ACCOUNT_TAKEOVER: {
        "hours_since_identity_change",
        "device_is_known_for_account",
        "amount_zscore_vs_account",
        "merchant_is_habitual",
    },
    FraudPattern.CARD_TESTING: {
        "account_distinct_mcc_5m",
        "card_tx_count_5m",
        "declined_ratio_1h",
        "account_tx_count_5m",
    },
    FraudPattern.IMPOSSIBLE_TRAVEL: {
        "implied_speed_kmh_from_last",
        "geo_distance_from_last_km",
    },
    FraudPattern.VELOCITY_ATTACK: {
        "account_tx_count_1m",
        "account_tx_count_5m",
        "account_tx_count_1h",
    },
    FraudPattern.DEVICE_FARM: {
        "device_distinct_accounts_24h",
        "device_is_known_for_account",
    },
    FraudPattern.FRAUD_RING: {
        "device_distinct_accounts_24h",
        "ip_distinct_accounts_1h",
    },
    FraudPattern.MERCHANT_COLLUSION: {
        "merchant_amount_cv_24h",
        "merchant_distinct_accounts_1h",
    },
    FraudPattern.CREDENTIAL_STUFFING: {
        "failed_logins_1h",
        "ip_distinct_accounts_1h",
    },
    FraudPattern.ANOMALOUS_HIGH_VALUE: {
        "amount_zscore_vs_account",
        "mcc_is_habitual_for_account",
    },
    FraudPattern.UNUSUAL_LOCATION_DEVICE: {
        "device_is_known_for_account",
        "distance_from_account_home_km",
    },
}


def test_every_fraud_pattern_has_at_least_one_hot_path_feature() -> None:
    """A scenario with no feature is one the gateway cannot see at all.

    Phase 9 reports per-pattern metrics; a structurally blind pattern would show
    up there as a model failure rather than as a missing input, which is the
    wrong diagnosis and an expensive one.
    """
    assert set(SIGNATURE_FEATURES) == set(FraudPattern), (
        "a fraud pattern has no declared hot-path features. Every pattern in "
        "docs/FRAUD_SCENARIOS.md needs something the gateway can measure."
    )
    for pattern, features in SIGNATURE_FEATURES.items():
        assert features, pattern


def test_every_named_feature_actually_exists() -> None:
    """Otherwise the mapping above would drift into fiction."""
    for pattern, features in SIGNATURE_FEATURES.items():
        for feature_id in features:
            assert feature_id in ONLINE_FEATURES, (
                f"{pattern} names {feature_id!r}, which is not a registered feature"
            )


def test_every_feature_serves_at_least_one_documented_signature() -> None:
    """The other direction: a feature nobody's scenario motivates cannot be
    evaluated, and pads the set without adding signal."""
    used = set().union(*SIGNATURE_FEATURES.values())
    unused = set(ONLINE_FEATURES.ids) - used
    # Supporting features are legitimate -- a rule may combine a signature
    # feature with context -- but each one is named here deliberately rather
    # than accumulating unnoticed.
    supporting = {
        "account_tx_count_24h",
        "account_amount_sum_1h",
        "account_distinct_merchants_1h",
        "account_distinct_devices_24h",
        "account_distinct_countries_24h",
        "account_tenure_days",
        "seconds_since_last_transaction",
    }
    assert unused == supporting, (
        f"unaccounted features: {sorted(unused - supporting)}. Either name the "
        f"signature a feature serves, or list it as supporting context."
    )


# --- nothing is read outside the canonical contract -------------------------


def test_no_feature_produces_a_value_from_an_empty_context() -> None:
    """The invariant that outranks the rest: no history means no number.

    One `.get(key, 0)` anywhere turns a cold store into a confident "no risk"
    on every transaction, and nothing about the output looks wrong.
    """
    tx = CanonicalTransaction(
        source_dataset="probe",
        source_row_id="row-1",
        field_coverage=frozenset(CanonicalField),
        transaction_id="tx_1",
        account_id="acct_000001",
        amount_minor=5_000,
        currency="GBP",
        occurred_at=dt.datetime(2026, 3, 1, tzinfo=dt.UTC),
        ingested_at=dt.datetime(2026, 3, 1, 0, 0, 1, tzinfo=dt.UTC),
    )
    for spec in ONLINE_FEATURES:
        value = spec.evaluate(tx, EMPTY)
        assert not value.is_available, (
            f"{spec.feature_id} produced {value.or_none()} with no observations"
        )


def test_features_are_unavailable_on_a_minimal_coverage_source() -> None:
    """IEEE-CIS supplies far less than the generator. Features that need what it
    lacks must say UNAVAILABLE, and the intersecting subset is what a transfer
    metric may be computed over (ADR-0021)."""
    minimal = frozenset(ALWAYS_REQUIRED)
    computable = ONLINE_FEATURES.computable_on(minimal)
    assert computable, "no feature survives minimal coverage; the set is unusable on Track B"
    assert len(computable) < len(ONLINE_FEATURES), (
        "every feature is computable on the irreducible minimum, which means none of "
        "them actually declares the inputs it needs"
    )
    for feature_id in ("geo_distance_from_last_km", "distance_from_account_home_km"):
        assert feature_id not in computable, f"{feature_id} claims to work without geography"
