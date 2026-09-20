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
    HISTORY_DEPTH_CAPPED,
    INSUFFICIENT_HISTORY,
    LIFETIME_UNOBSERVED,
    MIN_OBSERVATIONS_FOR_ROBUST_Z,
    Completeness,
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
    CardinalityStorage,
    CurrentObservation,
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
        if state.content_capped:
            return HISTORY_DEPTH_CAPPED  # the sum needs content past the cap (ADR-0046 §8)
        return float(state.amount_sum_minor)

    return compute


def _distinct(spec: WindowedAggregate) -> Compute:
    def compute(tx: CanonicalTransaction, ctx: FeatureContext) -> float | InsufficientHistory:
        state = ctx.window(spec.entity, _entity_id(tx, spec.entity), spec.stream, spec.window.label)
        if state is None or spec.dimension is None:
            return INSUFFICIENT_HISTORY
        if state.content_capped and spec.storage is CardinalityStorage.EXACT:
            return HISTORY_DEPTH_CAPPED  # the members past the cap are not read (ADR-0046 §8)
        value = state.distinct.get(spec.dimension)
        return INSUFFICIENT_HISTORY if value is None else float(value)

    return compute


def _declined_ratio(spec: WindowedAggregate) -> Compute:
    def compute(tx: CanonicalTransaction, ctx: FeatureContext) -> float | InsufficientHistory:
        state = ctx.window(spec.entity, _entity_id(tx, spec.entity), spec.stream, spec.window.label)
        if state is None or state.outcome_known_count == 0:
            # Zero known outcomes is NOT a zero ratio. Dividing by the wrong
            # denominator would report a ratio that was never measured. The scored
            # transaction's own outcome is never among them: it is decided after
            # TRACE-X answers (observation.POST_DECISION_FIELDS, ADR-0046 §7).
            return INSUFFICIENT_HISTORY
        return state.declined_count / state.outcome_known_count

    return compute


def _amount_cv(spec: WindowedAggregate) -> Compute:
    def compute(tx: CanonicalTransaction, ctx: FeatureContext) -> float | InsufficientHistory:
        state = ctx.window(spec.entity, _entity_id(tx, spec.entity), spec.stream, spec.window.label)
        if state is None or state.aligned_count < 2:
            return INSUFFICIENT_HISTORY
        # Same-currency count over same-currency sums, both minute-aligned (ADR-0046 §2).
        # The previous form divided same-currency sums by the all-currency count, in BOTH
        # implementations, so a mixed-currency merchant read as uniform or dispersed at
        # random and implementation parity could not see it.
        mean = state.aligned_amount_sum_minor / state.aligned_count
        if mean == 0:
            return INSUFFICIENT_HISTORY
        variance = state.aligned_amount_sum_squares / state.aligned_count - mean * mean
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

_spec = WindowedAggregate(
    Entity.ACCOUNT,
    W1H,
    Aggregation.DECLINED_RATIO,
    stream=Stream.AUTHORIZATION_OUTCOME,
    current_observation=CurrentObservation.PRIOR_KNOWN,
)
_register_window(
    "declined_ratio_1h",
    "Share of this account's verified authorization outcomes decided in the last hour, and known "
    "before this transaction was scored, that were declines. A high ratio is the card-testing tell "
    "that survives when amounts are deliberately small.",
    _ACCOUNT_TX | {F.AUTHORIZATION_OUTCOME},
    _spec,
    _declined_ratio(_spec),
)


# --------------------------------------------- distinct cardinality (8-14) ---

# Storage class per distinct-count feature (ADR-0034, constraint: declared, never
# inferred at runtime). The criterion is whether the counted dimension's
# cardinality is bounded by ONE entity's own behaviour or by the population
# sharing that entity:
#
#   EXACT       -- an account touches tens of merchants, categories, devices and
#                  countries in a window, and one device is used by a household
#                  or, at the extreme the feature exists to catch, a farm of
#                  dozens. Bounded, so a sorted set costs single-digit KB and is
#                  exactly comparable to the offline computation.
#   APPROXIMATE -- an IP can be a carrier NAT and a popular merchant can take
#                  thousands of distinct accounts an hour. Unbounded in normal
#                  operation, where the measured 20x memory difference is
#                  material and bounded memory is worth an estimator error.
_DISTINCTS: Final = (
    (
        "account_distinct_merchants_1h",
        "Distinct merchants this account touched in the last hour.",
        Entity.ACCOUNT,
        W1H,
        Dimension.MERCHANT,
        _ACCOUNT_TX | {F.MERCHANT_ID},
        CardinalityStorage.EXACT,
    ),
    (
        "account_distinct_mcc_5m",
        "Distinct merchant categories in 5 minutes. Card testing sprays across "
        "categories; ordinary spending does not.",
        Entity.ACCOUNT,
        W5M,
        Dimension.MCC,
        _ACCOUNT_TX | {F.MERCHANT_MCC},
        CardinalityStorage.EXACT,
    ),
    (
        "account_distinct_devices_24h",
        "Distinct devices this account transacted from in 24 hours.",
        Entity.ACCOUNT,
        W24H,
        Dimension.DEVICE,
        _ACCOUNT_TX | {F.DEVICE_ID},
        CardinalityStorage.EXACT,
    ),
    (
        "account_distinct_countries_24h",
        "Distinct merchant countries in 24 hours. Geographic dispersion without "
        "the travel time to explain it.",
        Entity.ACCOUNT,
        W24H,
        Dimension.COUNTRY,
        _ACCOUNT_TX | {F.MERCHANT_COUNTRY},
        CardinalityStorage.EXACT,
    ),
    (
        "device_distinct_accounts_24h",
        "Distinct accounts transacting from this device in 24 hours. The device-farm "
        "signal: one fingerprint, many unrelated accounts.",
        Entity.DEVICE,
        W24H,
        Dimension.ACCOUNT,
        frozenset({F.DEVICE_ID, F.ACCOUNT_ID, F.OCCURRED_AT}),
        CardinalityStorage.EXACT,
    ),
    (
        "ip_distinct_accounts_1h",
        "Distinct accounts transacting from this IP in an hour. Credential stuffing "
        "originates from a small pool of addresses across many accounts.",
        Entity.IP,
        W1H,
        Dimension.ACCOUNT,
        frozenset({F.IP_ID, F.ACCOUNT_ID, F.OCCURRED_AT}),
        CardinalityStorage.APPROXIMATE,
    ),
    (
        "merchant_distinct_accounts_1h",
        "Distinct accounts paying this merchant in an hour.",
        Entity.MERCHANT,
        W1H,
        Dimension.ACCOUNT,
        frozenset({F.MERCHANT_ID, F.ACCOUNT_ID, F.OCCURRED_AT}),
        CardinalityStorage.APPROXIMATE,
    ),
)

for _fid, _desc, _entity, _window, _dimension, _required, _storage in _DISTINCTS:
    _spec = WindowedAggregate(
        _entity,
        _window,
        Aggregation.DISTINCT_COUNT,
        dimension=_dimension,
        storage=_storage,
    )
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


def _robust_z(spec: ProfileAttribute) -> Compute:
    del spec  # the estimator reads the profile as it is; no horizon gate

    def compute(tx: CanonicalTransaction, ctx: FeatureContext) -> float | InsufficientHistory:
        profile = _profile(tx, ctx, Entity.ACCOUNT)
        if profile is not None and profile.depth_capped and profile.amount_median_minor is None:
            return HISTORY_DEPTH_CAPPED  # the capped read does not hold the whole sample
        if profile is None or profile.observation_count < MIN_OBSERVATIONS_FOR_ROBUST_Z:
            return INSUFFICIENT_HISTORY
        if profile.amount_median_minor is None or profile.amount_mad_minor is None:
            return INSUFFICIENT_HISTORY
        scale = MAD_TO_SIGMA * profile.amount_mad_minor
        deviation = abs(tx.amount_minor) - profile.amount_median_minor
        if scale <= 0:
            # An account that has always spent exactly the same amount. Any
            # departure is maximally anomalous IN ITS OWN DIRECTION; an identical amount
            # is not anomalous at all. Phase 2 returned +50 for a LOWER amount too, so
            # R015, R009 and R016 read a small payment as a high-value anomaly
            # (ADR-0046 §3).
            return 0.0 if deviation == 0 else math.copysign(ROBUST_Z_CAP, deviation)
        return max(-ROBUST_Z_CAP, min(ROBUST_Z_CAP, deviation / scale))

    return compute


def _tenure_days(spec: ProfileAttribute) -> Compute:
    def compute(tx: CanonicalTransaction, ctx: FeatureContext) -> float | InsufficientHistory:
        profile = _profile(tx, ctx, Entity.ACCOUNT)
        if profile is None:
            return INSUFFICIENT_HISTORY
        if profile.first_seen_at is None:
            # A capped read knows the lifetime's start only when the folded prefix holds it.
            return HISTORY_DEPTH_CAPPED if profile.depth_capped else INSUFFICIENT_HISTORY
        # "First seen N days ago" is only tenure if the store has been watching
        # for at least its horizon: to a store that started last week, an
        # account opened last year and one opened last week look identical, and
        # reporting the second's age for the first is how a restart turns every
        # regular customer into a fresh account for the tenure rules.
        if ctx.completeness(spec.horizon.seconds) is not Completeness.COMPLETE:
            return INSUFFICIENT_HISTORY
        # ...and watching since before the lifetime began, not merely for the horizon: a store that
        # started INSIDE an ongoing lifetime would otherwise serve the time since it started, as
        # COMPLETE, turning every long-standing customer into a new account (ADR-0046 §3).
        if not ctx.vouched_lifetime_start(profile.first_seen_at, spec.horizon.seconds):
            return LIFETIME_UNOBSERVED
        return max(0.0, (ctx.as_of - profile.first_seen_at).total_seconds() / 86_400.0)

    return compute


def _membership(spec: ProfileAttribute) -> Compute:
    def compute(tx: CanonicalTransaction, ctx: FeatureContext) -> float | InsufficientHistory:
        profile = _profile(tx, ctx, Entity.ACCOUNT)
        if profile is None:
            return INSUFFICIENT_HISTORY
        if spec.metric is ProfileMetric.MERCHANT_IS_HABITUAL:
            known, subject = profile.habitual_merchants, tx.merchant_id
        elif spec.metric is ProfileMetric.MCC_IS_HABITUAL:
            known, subject = profile.habitual_mccs, tx.merchant_mcc
        else:
            known, subject = profile.known_devices, tx.device_id
        if profile.depth_capped:
            # ADR-0046 §8: a member the capped read saw is certain; a non-member may be one it did
            # not reach, so it is never called unfamiliar.
            return 1.0 if subject is not None and subject in known else HISTORY_DEPTH_CAPPED
        if not known:
            # An empty set is "we have never seen this account use anything",
            # not "this is unfamiliar". Reporting 0.0 would make every new
            # account's first transaction look like a novel device.
            return INSUFFICIENT_HISTORY
        if subject is not None and subject in known:
            # A positive is sound as soon as it is observed: the store saw this
            # account use this device, whatever it did not see before.
            return 1.0
        # A NEGATIVE is a claim about everything the account has ever done, and
        # a store that started recording last week has not seen everything. It
        # may only say "unknown device" once it has watched for its horizon;
        # before that the honest answer is that it cannot tell. This is the
        # asymmetry that keeps a Redis restart from making every returning
        # customer look like an account takeover in progress.
        if ctx.completeness(spec.horizon.seconds) is not Completeness.COMPLETE:
            return INSUFFICIENT_HISTORY
        # The same asymmetry runs one step deeper: a negative is a claim about the WHOLE lifetime,
        # so
        # the store must also have been watching before that lifetime began (ADR-0046 §3).
        if profile.first_seen_at is None or not ctx.vouched_lifetime_start(
            profile.first_seen_at, spec.horizon.seconds
        ):
            return LIFETIME_UNOBSERVED
        return 0.0

    return compute


def _distance_from_home(spec: ProfileAttribute) -> Compute:
    del spec  # home is a location the store observed; no horizon gate

    def compute(tx: CanonicalTransaction, ctx: FeatureContext) -> float | InsufficientHistory:
        profile = _profile(tx, ctx, Entity.ACCOUNT)
        if tx.latitude is None or tx.longitude is None:
            return INSUFFICIENT_HISTORY
        if profile is None or profile.home_latitude is None or profile.home_longitude is None:
            capped = profile is not None and profile.depth_capped
            return HISTORY_DEPTH_CAPPED if capped else INSUFFICIENT_HISTORY
        return haversine_km(
            GeoPoint(profile.home_latitude, profile.home_longitude),
            GeoPoint(tx.latitude, tx.longitude),
        )

    return compute


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
        _membership,
        False,
    ),
    (
        "mcc_is_habitual_for_account",
        "1.0 if this merchant category is one the account uses habitually.",
        ProfileMetric.MCC_IS_HABITUAL,
        frozenset({F.ACCOUNT_ID, F.MERCHANT_MCC}),
        _membership,
        False,
    ),
    (
        "device_is_known_for_account",
        "1.0 if the account has transacted from this device before. Device novelty "
        "is the first half of an account takeover.",
        ProfileMetric.DEVICE_IS_KNOWN,
        frozenset({F.ACCOUNT_ID, F.DEVICE_ID}),
        _membership,
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
    _semantics = ProfileAttribute(Entity.ACCOUNT, _metric)
    ONLINE_FEATURES.register(
        FeatureSpec(
            feature_id=_fid,
            description=_desc,
            required_fields=_required,
            semantics=_semantics,
            # Every profile compute is built from its spec. Tenure and the three
            # membership features read the declared horizon -- a negative ("not
            # a known device", "N days old") is only sound once the store has
            # watched that long. The others ignore it, uniformly shaped.
            compute=_compute(_semantics),
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
