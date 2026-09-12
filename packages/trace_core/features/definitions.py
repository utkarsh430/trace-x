"""The production online feature set (ROADMAP Phase 2: "~20 features").

Twenty-six features, each declaring three things: the canonical fields it needs
(so an uncovered source yields `UNAVAILABLE` rather than a fabricated zero), its
`semantics` (so Phase 3 compiles the same meaning into an event-time Spark
aggregate instead of re-deriving it), and a compute function over an already-read
context snapshot.

**Every feature is grounded in a documented fraud signature.** The ten Track A
scenarios (`docs/FRAUD_SCENARIOS.md`) each declare `causal_evidence_keys`, and
each key needs something on the hot path that could plausibly surface it. A
feature nobody's scenario motivates is a feature nobody can evaluate, and a
scenario with no feature is a signature the hot path is structurally blind to.
`tests/unit/test_feature_definitions.py` asserts the correspondence in both
directions.

To be explicit about what that does *not* mean: these are derived from each
scenario's published **signature**, which is a property of the data, never from
its label. Labels live in the `groundtruth` schema, which the application role
cannot read at all (ADR-0004).

**Currency is a partition, never a conversion.** Windowed sums and robust
z-scores are computed per `(entity, currency)`. No FX source exists in this
system, so converting would invent a number; a currency the account does not
normally use is itself a signal, and it shows up as an absent window rather than
as a silently-merged total.
"""

from __future__ import annotations

import math
from typing import Final

from trace_core.contracts.canonical import CanonicalField as F
from trace_core.contracts.canonical import CanonicalTransaction
from trace_core.domain.enums import TransactionChannel
from trace_core.domain.geo import GeoPoint, haversine_km, implied_speed_kmh
from trace_core.features.context import (
    INSUFFICIENT_HISTORY,
    MIN_OBSERVATIONS_FOR_ROBUST_Z,
    FeatureContext,
    InsufficientHistory,
    Observation,
    Profile,
)
from trace_core.features.semantics import (
    FIVE_MINUTES as W5M,
)
from trace_core.features.semantics import (
    ONE_DAY as W24H,
)
from trace_core.features.semantics import (
    ONE_HOUR as W1H,
)
from trace_core.features.semantics import (
    ONE_MINUTE as W1M,
)
from trace_core.features.semantics import (
    Aggregation,
    Dimension,
    Entity,
    PairwiseMetric,
    PairwiseWithPrevious,
    ProfileAttribute,
    ProfileMetric,
    Stream,
    WindowedAggregate,
)
from trace_core.features.spec import Compute, FeatureRegistry, FeatureSpec

ONLINE_FEATURES: Final = FeatureRegistry()

MAD_TO_SIGMA: Final = 1.4826
"""Scale factor making MAD a consistent estimator of sigma for a normal
distribution. Named rather than inlined because the constant is what makes a
robust z-score comparable to an ordinary one."""

ROBUST_Z_CAP: Final = 50.0
"""Z-scores are capped, not because large ones are wrong but because a
degenerate MAD makes them unbounded, and an unbounded feature makes a bounded
score impossible to reason about. The cap is far outside any rule threshold."""


# --------------------------------------------------------------- helpers ---


def _entity_id(tx: CanonicalTransaction, entity: Entity) -> str | None:
    return {
        Entity.ACCOUNT: tx.account_id,
        Entity.CARD: tx.card_id,
        Entity.DEVICE: tx.device_id,
        Entity.MERCHANT: tx.merchant_id,
        Entity.IP: tx.ip_id,
    }[entity]


def _count(spec: WindowedAggregate) -> Compute:
    def compute(tx: CanonicalTransaction, ctx: FeatureContext) -> float | InsufficientHistory:
        state = ctx.window(spec.entity, _entity_id(tx, spec.entity), spec.stream, spec.window.label)
        if state is None:
            return INSUFFICIENT_HISTORY
        return float(state.count)

    return compute


def _amount_sum(spec: WindowedAggregate) -> Compute:
    def compute(tx: CanonicalTransaction, ctx: FeatureContext) -> float | InsufficientHistory:
        state = ctx.window(spec.entity, _entity_id(tx, spec.entity), spec.stream, spec.window.label)
        if state is None:
            return INSUFFICIENT_HISTORY
        return float(state.amount_sum_minor)

    return compute


def _distinct(spec: WindowedAggregate) -> Compute:
    def compute(tx: CanonicalTransaction, ctx: FeatureContext) -> float | InsufficientHistory:
        state = ctx.window(spec.entity, _entity_id(tx, spec.entity), spec.stream, spec.window.label)
        if state is None or spec.dimension is None:
            return INSUFFICIENT_HISTORY
        value = state.distinct.get(spec.dimension)
        return INSUFFICIENT_HISTORY if value is None else float(value)

    return compute


def _declined_ratio(spec: WindowedAggregate) -> Compute:
    def compute(tx: CanonicalTransaction, ctx: FeatureContext) -> float | InsufficientHistory:
        state = ctx.window(spec.entity, _entity_id(tx, spec.entity), spec.stream, spec.window.label)
        if state is None or state.outcome_known_count == 0:
            # Zero known outcomes is NOT a zero ratio. Dividing by the wrong
            # denominator would report a ratio that was never measured.
            return INSUFFICIENT_HISTORY
        return state.declined_count / state.outcome_known_count

    return compute


def _amount_cv(spec: WindowedAggregate) -> Compute:
    def compute(tx: CanonicalTransaction, ctx: FeatureContext) -> float | InsufficientHistory:
        state = ctx.window(spec.entity, _entity_id(tx, spec.entity), spec.stream, spec.window.label)
        if state is None or state.count < 2:
            return INSUFFICIENT_HISTORY
        mean = state.amount_sum_minor / state.count
        if mean == 0:
            return INSUFFICIENT_HISTORY
        variance = state.amount_sum_squares / state.count - mean * mean
        # Floating error can make a genuinely-zero variance very slightly
        # negative; clamping is correct, and a real negative is impossible.
        return math.sqrt(max(variance, 0.0)) / abs(mean)

    return compute


def _profile(tx: CanonicalTransaction, ctx: FeatureContext, entity: Entity) -> Profile | None:
    return ctx.profile(entity, _entity_id(tx, entity))


# ------------------------------------------------------- velocity (1-7) ---


def _register_window(
    feature_id: str,
    description: str,
    required: frozenset[F],
    semantics: WindowedAggregate,
    compute: Compute,
    *,
    higher_is_riskier: bool = True,
) -> None:
    ONLINE_FEATURES.register(
        FeatureSpec(
            feature_id=feature_id,
            description=description,
            required_fields=required,
            semantics=semantics,
            compute=compute,
            higher_is_riskier=higher_is_riskier,
        )
    )


_ACCOUNT_TX = frozenset({F.ACCOUNT_ID, F.OCCURRED_AT})

for _window in (W1M, W5M, W1H, W24H):
    _spec = WindowedAggregate(Entity.ACCOUNT, _window, Aggregation.COUNT)
    _register_window(
        f"account_tx_count_{_window.label}",
        f"Transactions on this account in the last {_window.label}.",
        _ACCOUNT_TX,
        _spec,
        _count(_spec),
    )

_spec = WindowedAggregate(Entity.ACCOUNT, W1H, Aggregation.AMOUNT_SUM)
_register_window(
    "account_amount_sum_1h",
    "Total amount, in minor units, on this account in the last hour and currency.",
    _ACCOUNT_TX | {F.AMOUNT_MINOR, F.CURRENCY},
    _spec,
    _amount_sum(_spec),
)

_spec = WindowedAggregate(Entity.CARD, W5M, Aggregation.COUNT)
_register_window(
    "card_tx_count_5m",
    "Transactions on this card in the last 5 minutes. Card testing probes one card.",
    frozenset({F.CARD_ID, F.OCCURRED_AT}),
    _spec,
    _count(_spec),
)

_spec = WindowedAggregate(Entity.ACCOUNT, W1H, Aggregation.DECLINED_RATIO)
_register_window(
    "declined_ratio_1h",
    "Share of this account's authorised-or-declined transactions in the last hour "
    "that were declined. A high ratio is the card-testing tell that survives when "
    "amounts are deliberately small.",
    _ACCOUNT_TX | {F.AUTHORIZATION_OUTCOME},
    _spec,
    _declined_ratio(_spec),
)


# --------------------------------------------- distinct cardinality (8-14) ---

_DISTINCTS: Final = (
    (
        "account_distinct_merchants_1h",
        "Distinct merchants this account touched in the last hour.",
        Entity.ACCOUNT,
        W1H,
        Dimension.MERCHANT,
        _ACCOUNT_TX | {F.MERCHANT_ID},
    ),
    (
        "account_distinct_mcc_5m",
        "Distinct merchant categories in 5 minutes. Card testing sprays across "
        "categories; ordinary spending does not.",
        Entity.ACCOUNT,
        W5M,
        Dimension.MCC,
        _ACCOUNT_TX | {F.MERCHANT_MCC},
    ),
    (
        "account_distinct_devices_24h",
        "Distinct devices this account transacted from in 24 hours.",
        Entity.ACCOUNT,
        W24H,
        Dimension.DEVICE,
        _ACCOUNT_TX | {F.DEVICE_ID},
    ),
    (
        "account_distinct_countries_24h",
        "Distinct merchant countries in 24 hours. Geographic dispersion without "
        "the travel time to explain it.",
        Entity.ACCOUNT,
        W24H,
        Dimension.COUNTRY,
        _ACCOUNT_TX | {F.MERCHANT_COUNTRY},
    ),
    (
        "device_distinct_accounts_24h",
        "Distinct accounts transacting from this device in 24 hours. The device-farm "
        "signal: one fingerprint, many unrelated accounts.",
        Entity.DEVICE,
        W24H,
        Dimension.ACCOUNT,
        frozenset({F.DEVICE_ID, F.ACCOUNT_ID, F.OCCURRED_AT}),
    ),
    (
        "ip_distinct_accounts_1h",
        "Distinct accounts transacting from this IP in an hour. Credential stuffing "
        "originates from a small pool of addresses across many accounts.",
        Entity.IP,
        W1H,
        Dimension.ACCOUNT,
        frozenset({F.IP_ID, F.ACCOUNT_ID, F.OCCURRED_AT}),
    ),
    (
        "merchant_distinct_accounts_1h",
        "Distinct accounts paying this merchant in an hour.",
        Entity.MERCHANT,
        W1H,
        Dimension.ACCOUNT,
        frozenset({F.MERCHANT_ID, F.ACCOUNT_ID, F.OCCURRED_AT}),
    ),
)

for _fid, _desc, _entity, _window, _dimension, _required in _DISTINCTS:
    _spec = WindowedAggregate(_entity, _window, Aggregation.DISTINCT_COUNT, dimension=_dimension)
    _register_window(_fid, _desc, _required, _spec, _distinct(_spec))


# --------------------------------------------------- merchant shape (15) ---

_spec = WindowedAggregate(Entity.MERCHANT, W24H, Aggregation.AMOUNT_CV)
_register_window(
    "merchant_amount_cv_24h",
    "Coefficient of variation of this merchant's amounts over 24 hours. Genuine "
    "spend at one merchant is dispersed; laundering through one is unusually "
    "uniform, so a LOW value is the signal here.",
    frozenset({F.MERCHANT_ID, F.AMOUNT_MINOR, F.CURRENCY, F.OCCURRED_AT}),
    _spec,
    _amount_cv(_spec),
    higher_is_riskier=False,
)


# ------------------------------------------------------- profile (16-20) ---


def _robust_z(tx: CanonicalTransaction, ctx: FeatureContext) -> float | InsufficientHistory:
    profile = _profile(tx, ctx, Entity.ACCOUNT)
    if profile is None or profile.observation_count < MIN_OBSERVATIONS_FOR_ROBUST_Z:
        return INSUFFICIENT_HISTORY
    if profile.amount_median_minor is None or profile.amount_mad_minor is None:
        return INSUFFICIENT_HISTORY
    scale = MAD_TO_SIGMA * profile.amount_mad_minor
    deviation = abs(tx.amount_minor) - profile.amount_median_minor
    if scale <= 0:
        # An account that has always spent exactly the same amount. Any
        # departure is maximally anomalous; an identical amount is not anomalous
        # at all. Returning 0/0 or infinity would be worse than either.
        return 0.0 if deviation == 0 else ROBUST_Z_CAP
    return max(-ROBUST_Z_CAP, min(ROBUST_Z_CAP, deviation / scale))


def _tenure_days(tx: CanonicalTransaction, ctx: FeatureContext) -> float | InsufficientHistory:
    profile = _profile(tx, ctx, Entity.ACCOUNT)
    if profile is None or profile.first_seen_at is None:
        return INSUFFICIENT_HISTORY
    return max(0.0, (ctx.as_of - profile.first_seen_at).total_seconds() / 86_400.0)


def _membership(metric: ProfileMetric) -> Compute:
    def compute(tx: CanonicalTransaction, ctx: FeatureContext) -> float | InsufficientHistory:
        profile = _profile(tx, ctx, Entity.ACCOUNT)
        if profile is None:
            return INSUFFICIENT_HISTORY
        if metric is ProfileMetric.MERCHANT_IS_HABITUAL:
            known, subject = profile.habitual_merchants, tx.merchant_id
        elif metric is ProfileMetric.MCC_IS_HABITUAL:
            known, subject = profile.habitual_mccs, tx.merchant_mcc
        else:
            known, subject = profile.known_devices, tx.device_id
        if not known:
            # An empty set is "we have never seen this account use anything",
            # not "this is unfamiliar". Reporting 0.0 would make every new
            # account's first transaction look like a novel device.
            return INSUFFICIENT_HISTORY
        return float(subject is not None and subject in known)

    return compute


def _distance_from_home(
    tx: CanonicalTransaction, ctx: FeatureContext
) -> float | InsufficientHistory:
    profile = _profile(tx, ctx, Entity.ACCOUNT)
    if profile is None or profile.home_latitude is None or profile.home_longitude is None:
        return INSUFFICIENT_HISTORY
    if tx.latitude is None or tx.longitude is None:
        return INSUFFICIENT_HISTORY
    return haversine_km(
        GeoPoint(profile.home_latitude, profile.home_longitude),
        GeoPoint(tx.latitude, tx.longitude),
    )


_PROFILES: Final = (
    (
        "amount_zscore_vs_account",
        "Robust z-score of this amount against the account's OWN amount history "
        "(median/MAD). Defined per account rather than globally, because the "
        "baseline has a long right tail and a global threshold would fire on "
        "every wealthy customer.",
        ProfileMetric.AMOUNT_ROBUST_Z,
        frozenset({F.ACCOUNT_ID, F.AMOUNT_MINOR, F.CURRENCY}),
        _robust_z,
        True,
    ),
    (
        "account_tenure_days",
        "Days since this account was first observed.",
        ProfileMetric.TENURE_DAYS,
        frozenset({F.ACCOUNT_ID, F.OCCURRED_AT}),
        _tenure_days,
        False,
    ),
    (
        "merchant_is_habitual",
        "1.0 if this merchant is one the account uses habitually.",
        ProfileMetric.MERCHANT_IS_HABITUAL,
        frozenset({F.ACCOUNT_ID, F.MERCHANT_ID}),
        _membership(ProfileMetric.MERCHANT_IS_HABITUAL),
        False,
    ),
    (
        "mcc_is_habitual_for_account",
        "1.0 if this merchant category is one the account uses habitually.",
        ProfileMetric.MCC_IS_HABITUAL,
        frozenset({F.ACCOUNT_ID, F.MERCHANT_MCC}),
        _membership(ProfileMetric.MCC_IS_HABITUAL),
        False,
    ),
    (
        "device_is_known_for_account",
        "1.0 if the account has transacted from this device before. Device novelty "
        "is the first half of an account takeover.",
        ProfileMetric.DEVICE_IS_KNOWN,
        frozenset({F.ACCOUNT_ID, F.DEVICE_ID}),
        _membership(ProfileMetric.DEVICE_IS_KNOWN),
        False,
    ),
    (
        "distance_from_account_home_km",
        "Great-circle distance from the account's habitual location.",
        ProfileMetric.DISTANCE_FROM_HOME_KM,
        frozenset({F.ACCOUNT_ID, F.LATITUDE, F.LONGITUDE}),
        _distance_from_home,
        True,
    ),
)

for _fid, _desc, _metric, _required, _compute, _riskier in _PROFILES:
    ONLINE_FEATURES.register(
        FeatureSpec(
            feature_id=_fid,
            description=_desc,
            required_fields=_required,
            semantics=ProfileAttribute(Entity.ACCOUNT, _metric),
            compute=_compute,
            higher_is_riskier=_riskier,
        )
    )


# ------------------------------------------------------ pairwise (21-23) ---


def _previous(tx: CanonicalTransaction, ctx: FeatureContext) -> Observation | None:
    return ctx.previous_observation(Entity.ACCOUNT, tx.account_id, Stream.TRANSACTION)


def _geo_distance_from_last(
    tx: CanonicalTransaction, ctx: FeatureContext
) -> float | InsufficientHistory:
    previous = _previous(tx, ctx)
    if previous is None or previous.latitude is None or previous.longitude is None:
        return INSUFFICIENT_HISTORY
    if tx.latitude is None or tx.longitude is None:
        return INSUFFICIENT_HISTORY
    return haversine_km(
        GeoPoint(previous.latitude, previous.longitude), GeoPoint(tx.latitude, tx.longitude)
    )


def _implied_speed(tx: CanonicalTransaction, ctx: FeatureContext) -> float | InsufficientHistory:
    """Speed implied by the gap to the previous transaction.

    Reported as 0.0 unless BOTH legs are card-present. A card-not-present leg has
    an innocent explanation -- buying online from a hotel room is not travel --
    and `IMPOSSIBLE_TRAVEL` is defined on two card-present legs
    (docs/FRAUD_SCENARIOS.md §3.3). Returning the raw speed anyway would make the
    rule fire on ordinary e-commerce.
    """
    previous = _previous(tx, ctx)
    if previous is None or previous.latitude is None or previous.longitude is None:
        return INSUFFICIENT_HISTORY
    if tx.latitude is None or tx.longitude is None:
        return INSUFFICIENT_HISTORY
    here_present = tx.channel is TransactionChannel.CARD_PRESENT
    if not (here_present and previous.card_present):
        return 0.0
    seconds = (ctx.as_of - previous.occurred_at).total_seconds()
    if seconds <= 0:
        # Same instant, or out-of-order arrival. An infinite speed is not a
        # measurement; the window aggregates already carry that signal.
        return 0.0
    return implied_speed_kmh(
        GeoPoint(previous.latitude, previous.longitude),
        GeoPoint(tx.latitude, tx.longitude),
        seconds,
    )


def _seconds_since(stream: Stream) -> Compute:
    def compute(tx: CanonicalTransaction, ctx: FeatureContext) -> float | InsufficientHistory:
        previous = ctx.previous_observation(Entity.ACCOUNT, tx.account_id, stream)
        if previous is None:
            return INSUFFICIENT_HISTORY
        return max(0.0, (ctx.as_of - previous.occurred_at).total_seconds())

    return compute


_GEO_REQUIRED = frozenset({F.ACCOUNT_ID, F.LATITUDE, F.LONGITUDE, F.OCCURRED_AT})

ONLINE_FEATURES.register(
    FeatureSpec(
        feature_id="geo_distance_from_last_km",
        description="Great-circle distance from this account's previous transaction.",
        required_fields=_GEO_REQUIRED,
        semantics=PairwiseWithPrevious(Entity.ACCOUNT, PairwiseMetric.DISTANCE_KM),
        compute=_geo_distance_from_last,
    )
)

ONLINE_FEATURES.register(
    FeatureSpec(
        feature_id="implied_speed_kmh_from_last",
        description=(
            "Speed implied by the distance and elapsed time since the previous "
            "transaction, counted only when both legs are card-present."
        ),
        required_fields=_GEO_REQUIRED | {F.CHANNEL},
        semantics=PairwiseWithPrevious(Entity.ACCOUNT, PairwiseMetric.IMPLIED_SPEED_KMH),
        compute=_implied_speed,
    )
)

ONLINE_FEATURES.register(
    FeatureSpec(
        feature_id="seconds_since_last_transaction",
        description="Elapsed event time since this account's previous transaction.",
        required_fields=frozenset({F.ACCOUNT_ID, F.OCCURRED_AT}),
        semantics=PairwiseWithPrevious(Entity.ACCOUNT, PairwiseMetric.SECONDS_SINCE),
        compute=_seconds_since(Stream.TRANSACTION),
        higher_is_riskier=False,
    )
)


# ------------------------------------------------------ identity (24-25) ---


def _hours_since_identity_change(
    tx: CanonicalTransaction, ctx: FeatureContext
) -> float | InsufficientHistory:
    previous = ctx.previous_observation(Entity.ACCOUNT, tx.account_id, Stream.IDENTITY_CHANGE)
    if previous is None:
        return INSUFFICIENT_HISTORY
    return max(0.0, (ctx.as_of - previous.occurred_at).total_seconds() / 3_600.0)


ONLINE_FEATURES.register(
    FeatureSpec(
        feature_id="hours_since_identity_change",
        description=(
            "Hours since this account's last credential or identity change. An account "
            "takeover BEGINS with one, so recency is the signal and a low value is the "
            "risky one."
        ),
        required_fields=frozenset({F.ACCOUNT_ID, F.OCCURRED_AT}),
        semantics=PairwiseWithPrevious(
            Entity.ACCOUNT, PairwiseMetric.SECONDS_SINCE, stream=Stream.IDENTITY_CHANGE
        ),
        compute=_hours_since_identity_change,
        higher_is_riskier=False,
    )
)

_spec = WindowedAggregate(
    Entity.ACCOUNT, W1H, Aggregation.COUNT, stream=Stream.IDENTITY_FAILED_LOGIN
)
_register_window(
    "failed_logins_1h",
    "Failed logins against this account in the last hour. Credential stuffing IS a "
    "burst of these, so a transaction alone would only show the aftermath.",
    frozenset({F.ACCOUNT_ID, F.OCCURRED_AT}),
    _spec,
    _count(_spec),
)


# The declared, published size of the online feature set. Asserted by a test:
# a feature quietly added or removed changes what every recorded decision means,
# so it is a version bump (FEATURE_SET_VERSION), not an edit.
FEATURE_COUNT: Final = 26

__all__ = ["FEATURE_COUNT", "ONLINE_FEATURES"]
