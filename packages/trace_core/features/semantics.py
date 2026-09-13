"""The declarative vocabulary a feature's meaning is expressed in.

**Why a feature is declared and not merely implemented.** ADR-0002 accepts a
genuine cost: the same feature is computed twice, once from Redis on the hot path
and once from Spark on the warm path, and the two can drift. `docs/DATA_ENGINEERING.md`
§4 answers that with "a single feature definition module" plus a parity test --
but a shared module only helps if what it shares is the *meaning*, not a Python
function. A Python closure over a Redis client cannot be run by Spark, so a
module that shared only closures would leave Phase 3 re-reading prose and
re-deriving the intent, which is exactly how two implementations of "distinct
merchants in the last hour" come to disagree about whether the window is closed
at both ends.

So every feature carries a `semantics` object from this module: which entity,
which stream, which window, which aggregation. That is enough to compile the
feature into an event-time Spark aggregate in Phase 3 **without reopening the
question of what it means**, and enough for `feature_parity_drift` to compare
like with like.

The taxonomy is deliberately small -- three shapes cover all 25 features -- and
every shape has a known event-time translation:

| Shape | Online | Offline |
|---|---|---|
| `WindowedAggregate` | sorted set / HLL / hash | one window per scored transaction |
| `ProfileAttribute` | Redis hash | the account's lifetime before `as_of` |
| `PairwiseWithPrevious` | last-observation hash | the latest observation before `as_of` |
| `RowLocal` | nothing to read | a column expression |

ADR-0046 replaced the first plan's offline translations: an event-time `window()` produces
tumbling or sliding buckets rather than one window per scored transaction, and `lag()` over
`occurred_at` returns the previous row even when it is at the same millisecond or outside the
lookback. What each shape means precisely -- identity, order, ties, self-inclusion, currency
scope, lifetime -- is declared below and pinned by literal fixtures.

Adding a fifth shape is a real decision: it means Phase 3 must learn a fifth
translation, so it belongs in an ADR rather than in a feature definition.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Final


class Entity(StrEnum):
    """What a feature is computed *about*.

    This is the grouping key offline and the Redis key prefix online, so the two
    partition the data identically by construction.
    """

    ACCOUNT = "ACCOUNT"
    CARD = "CARD"
    DEVICE = "DEVICE"
    MERCHANT = "MERCHANT"
    IP = "IP"


class Stream(StrEnum):
    """Which event stream an aggregate is computed over.

    Transactions are not the only input: two of the ten fraud scenarios are
    *defined* by identity events (docs/FRAUD_SCENARIOS.md §2), so a credential
    stuffing signal that only looked at transactions would be measuring the
    aftermath rather than the attack.
    """

    TRANSACTION = "TRANSACTION"
    IDENTITY_FAILED_LOGIN = "IDENTITY_FAILED_LOGIN"
    IDENTITY_CHANGE = "IDENTITY_CHANGE"


class Aggregation(StrEnum):
    """How observations in a window are reduced to one number."""

    COUNT = "COUNT"
    AMOUNT_SUM = "AMOUNT_SUM"
    """Minor units, summed. Integer arithmetic throughout (CLAUDE.md §6)."""
    DECLINED_RATIO = "DECLINED_RATIO"
    DISTINCT_COUNT = "DISTINCT_COUNT"
    """Approximate online (HyperLogLog, ~0.81% error -- ADR-0003) and exact
    offline. The tolerance this implies is declared per feature, never widened
    to make a parity test pass (docs/DATA_ENGINEERING.md §4)."""
    AMOUNT_CV = "AMOUNT_CV"
    """Coefficient of variation of amounts: stddev / mean.

    The tell for MERCHANT_COLLUSION. Genuine spend at one merchant is dispersed;
    laundering through one is unusually uniform (docs/FRAUD_SCENARIOS.md §3.7),
    so a *low* value is the signal -- the only feature here where that is true.
    """


class CardinalityStorage(StrEnum):
    """How a distinct count is represented online (ADR-0034).

    **Declared per feature and never switched at runtime.** A feature that chose
    its representation from observed cardinality would change its own error
    characteristics under load -- exactly when a reader most needs to know what a
    value means -- and would make a recorded parity tolerance unattributable.

    The classification criterion is whether the counted dimension's cardinality
    is bounded by ONE entity's own behaviour, or by the size of the population
    sharing that entity. See ADR-0034 for the measurements behind it.
    """

    EXACT = "EXACT"
    """Sorted set keyed by the counted VALUE, scored by event time (`ZADD … GT`).

    `ZCOUNT` over the window is then exactly the distinct count: each value holds
    its latest observation, so a value last seen inside the window counts once and
    one last seen before it does not. Memory is O(distinct cardinality), which is
    why this is reserved for dimensions with a natural bound."""

    APPROXIMATE = "APPROXIMATE"
    """Bucketed HyperLogLog: one HLL per time bucket, unioned by `PFCOUNT`.

    Memory is bounded regardless of cardinality, at the cost of two compounding
    error sources -- HLL's own, and the bucket boundary. Used only where exact
    representation is materially more expensive."""


class Dimension(StrEnum):
    """What `DISTINCT_COUNT` counts."""

    MERCHANT = "MERCHANT"
    MCC = "MCC"
    DEVICE = "DEVICE"
    COUNTRY = "COUNTRY"
    ACCOUNT = "ACCOUNT"


class ProfileMetric(StrEnum):
    """An attribute of an entity's accumulated profile rather than of a window."""

    TENURE_DAYS = "TENURE_DAYS"
    AMOUNT_ROBUST_Z = "AMOUNT_ROBUST_Z"
    """(amount - median) / (1.4826 * MAD), against the entity's OWN history.

    Median and MAD rather than mean and standard deviation because the
    distribution has a long right tail by construction (ADR-0029): a single
    large transaction moves a mean enough to hide the next one. ADR-0012 names
    the same robust-z as the control an anomaly detector must beat."""
    MERCHANT_IS_HABITUAL = "MERCHANT_IS_HABITUAL"
    MCC_IS_HABITUAL = "MCC_IS_HABITUAL"
    DEVICE_IS_KNOWN = "DEVICE_IS_KNOWN"
    DISTANCE_FROM_HOME_KM = "DISTANCE_FROM_HOME_KM"


class PairwiseMetric(StrEnum):
    """A comparison between this observation and the entity's previous one."""

    DISTANCE_KM = "DISTANCE_KM"
    IMPLIED_SPEED_KMH = "IMPLIED_SPEED_KMH"
    SECONDS_SINCE = "SECONDS_SINCE"


class CurrentObservation(StrEnum):
    """Whether the transaction being scored is part of what its own feature reads (ADR-0046 §2).

    Declared per feature, never a side effect of when a store happens to write. Phase 2 read
    features before recording the scored transaction, so every served window excluded it while
    ADR-0032 declared the opposite: a global accident nobody chose. The decision recorded in
    ADR-0046 honours ADR-0032 for transactional windows and states the exception for baselines.
    """

    INCLUDED = "INCLUDED"
    """The window closes at the current observation: the scored transaction counts in its
    own counts, sums and distinct counts, because "how many in the last minute" describes the
    burst this transaction belongs to. A field the transaction carries that is not known when
    it is scored (`observation.POST_DECISION_FIELDS`) still never contributes from it."""

    EXCLUDED = "EXCLUDED"
    """Only observations strictly before `as_of` on the event-time axis. Baselines -- the
    robust z-score's amounts, the home location, the account's habits, the previous
    observation -- where a transaction entering its own baseline would hide exactly the
    departure the feature measures. Other observations at the same millisecond as `as_of` are
    excluded too, so a baseline never depends on arrival order at a tie."""


class ParityComparison(StrEnum):
    """How two implementations' values for one feature are compared (ADR-0046 §5).

    Declared by the feature's shape rather than chosen by whoever writes a comparison, so a
    parity report cannot quietly widen a tolerance for one feature.
    """

    EXACT = "EXACT"
    """Integer or set arithmetic. Two correct implementations agree bit for bit."""
    FLOAT = "FLOAT"
    """The value passes through floating-point arithmetic (a division, a square root, a
    trigonometric distance): relative tolerance `FLOAT_PARITY_RELATIVE_TOLERANCE`, which is
    arithmetic, not estimation."""
    APPROXIMATE = "APPROXIMATE"
    """An estimator: the per-cardinality-stratum bound frozen in plan §4.3."""


class EvaluationMode(StrEnum):
    """Which observations a feature value is computed over (ADR-0046 §1).

    Two modes, because two different questions are asked of one declaration: what did the
    online store serve, and what would a complete history have said.
    """

    AS_SERVED = "AS_SERVED"
    """The observations the online store had recorded when it served the value, in the order
    it recorded them, with the scored transaction as the current observation. An observation
    recorded after the scored transaction cannot contribute, even one at the same millisecond,
    which is why an offline replay of served values must follow the recorded observation order
    rather than a range over `occurred_at`."""

    EVENT_TIME_COMPLETE = "EVENT_TIME_COMPLETE"
    """Every observation, whatever order it arrived in, ordered by `(occurred_ms, identity)`.
    An INCLUDED window's upper edge is the scored transaction's own place in that order: an
    observation at the same millisecond counts if its identity sorts at or before the scored
    transaction's, and not otherwise."""


@dataclass(frozen=True, slots=True)
class Window:
    """A sliding event-time window.

    **Event time, always.** `occurred_at` drives every window; `ingested_at` is
    used only to measure lag (ADR-0026). Online this is the sorted-set score and
    offline it is the watermark column, which is what makes an out-of-order
    arrival land in the window it belongs to on both paths.

    Half-open `(t - seconds, t]` at millisecond precision: an observation exactly
    `seconds` old is outside it. Whether the transaction being scored is inside
    its own window is declared per feature (`CurrentObservation`), and which other
    observations at the same millisecond as `t` count depends on the
    `EvaluationMode`. Stated because every implementation must agree at the
    boundary, and a one-observation disagreement is exactly the kind of drift
    nobody attributes.
    """

    seconds: int
    label: str

    def __post_init__(self) -> None:
        if self.seconds <= 0:
            raise ValueError(f"window {self.label!r} must be positive, got {self.seconds}")


ONE_MINUTE: Final = Window(60, "1m")
FIVE_MINUTES: Final = Window(300, "5m")
ONE_HOUR: Final = Window(3_600, "1h")
ONE_DAY: Final = Window(86_400, "24h")

WINDOWS: Final[tuple[Window, ...]] = (ONE_MINUTE, FIVE_MINUTES, ONE_HOUR, ONE_DAY)

LATE_ARRIVAL_MARGIN_S: Final = 3_600
"""How long state is kept beyond the widest window that reads it.

A late-arriving event (up to `MAX_BACKDATE_S` is accepted) must still land in
a window that has not been trimmed out from under it. One hour is the existing
implementation's margin, stated here so retention is DERIVED from the widest
declared window plus this, per storage primitive -- rather than one number
applied to everything, which is what kept five-minute card velocity for
twenty-five hours."""

PROFILE_ACTIVITY_HORIZON: Final = Window(30 * 86_400, "30d")
"""How far back an entity's accumulated profile is trusted to reach.

A profile has no window: tenure, habitual merchants and known devices
accumulate for as long as the entity is active. But the online store cannot
keep them forever, and more importantly it cannot know what happened before it
began recording. This is the horizon it commits to: an entity inactive for
longer loses its profile and is treated as unknown (not new), and a store
younger than this cannot assert that an entity is new -- because an entity
older than the store's own age would look identical. Thirty days is the
existing profile TTL, now declared rather than implied."""


PROFILE_LIFETIME_GAP_S: Final = 30 * 86_400
"""A profile reaches back to the most recent inactivity gap of at least this long
(ADR-0046 §3, Q4b). Walking back through an account's observations strictly before
`as_of`, the profile ends before the first consecutive pair at least this far apart, and
it is empty when the latest observation is at least this old. The same number as the
completeness horizon, for a different reason: that one bounds what a store can vouch
for, this one bounds what an account's profile means."""

HABITUAL_MIN_VISITS: Final = 3
"""Distinct observations at one merchant or category before it counts as habitual.

A single visit is not a habit, and treating it as one would make `merchant_is_habitual`
true for exactly the merchants a takeover just used."""

AMOUNT_SAMPLE_SIZE: Final = 128
"""The robust z-score's baseline: the last this-many same-currency amounts strictly
before `as_of` within the profile lifetime (ADR-0046 §3, Q4d). Declared as the
definition, so every implementation computes the same median and MAD exactly."""

HOME_SAMPLE_SIZE: Final = 20
HOME_MIN_OBSERVATIONS: Final = 3
"""Home is the geodesic medoid of the last `HOME_SAMPLE_SIZE` located observations
strictly before `as_of` within the lifetime, and needs at least `HOME_MIN_OBSERVATIONS`
of them (ADR-0046 §3, Q4c). Three is the smallest set whose medoid survives one outlier."""

ALIGNED_MINUTE_MS: Final = 60_000
"""The bucket width of the declared minute-aligned window `AMOUNT_CV` reads."""

APPROXIMATE_BUCKET_MS: Final = 300_000
"""The bucket width of the approximate distinct counts' edge-inclusive estimand (ADR-0034)."""

FLOAT_PARITY_RELATIVE_TOLERANCE: Final = 1e-9
"""How far two implementations' floating-point distances may differ. Arithmetic, not
estimation: libm implementations differ in the last bits, and nothing else may."""

IDENTITY_CHANGE_EVENT_TYPES: Final = frozenset(
    {"PASSWORD_CHANGE", "EMAIL_CHANGE", "PHONE_CHANGE", "ADDRESS_CHANGE", "MFA_RESET"}
)
FAILED_LOGIN_EVENT_TYPES: Final = frozenset({"LOGIN_FAILED"})


def identity_stream(identity_event_type: str) -> Stream | None:
    """Which feature stream an `identity.events.v1` type feeds, if any (ADR-0046 §4, Q4e).

    Declared here, beside the streams, rather than in the HTTP handler that used to hold
    it: the offline implementation must derive exactly the same streams, and it cannot
    read a request handler. `LOGIN_SUCCEEDED`, `MFA_ENROLLED` and `UNKNOWN` feed nothing --
    a successful login is not a credential change, and enrolling a second factor makes an
    account safer, so neither may reset `hours_since_identity_change`.
    """
    if identity_event_type in FAILED_LOGIN_EVENT_TYPES:
        return Stream.IDENTITY_FAILED_LOGIN
    if identity_event_type in IDENTITY_CHANGE_EVENT_TYPES:
        return Stream.IDENTITY_CHANGE
    return None


@dataclass(frozen=True, slots=True)
class WindowedAggregate:
    """`aggregation` of `stream` observations for `entity` over `window`."""

    entity: Entity
    window: Window
    aggregation: Aggregation
    stream: Stream = Stream.TRANSACTION
    dimension: Dimension | None = None
    storage: CardinalityStorage | None = None
    """Required on DISTINCT_COUNT, forbidden elsewhere. Declared, never inferred."""
    current_observation: CurrentObservation = CurrentObservation.INCLUDED
    """Whether the scored transaction is inside its own window (ADR-0046 §2). Every released
    transactional window includes it, as ADR-0032 declared."""

    def __post_init__(self) -> None:
        needs_dimension = self.aggregation is Aggregation.DISTINCT_COUNT
        if needs_dimension and self.dimension is None:
            raise ValueError("DISTINCT_COUNT must say what it counts")
        if not needs_dimension and self.dimension is not None:
            raise ValueError(f"{self.aggregation} does not count a dimension")
        if needs_dimension and self.storage is None:
            raise ValueError(
                "DISTINCT_COUNT must declare its storage class. Whether a value is "
                "exact or estimated is part of what the feature MEANS, not an "
                "implementation detail a caller can be left to discover (ADR-0034)."
            )
        if not needs_dimension and self.storage is not None:
            raise ValueError(f"{self.aggregation} does not have a cardinality storage class")

    @property
    def parity(self) -> ParityComparison:
        if self.storage is CardinalityStorage.APPROXIMATE:
            return ParityComparison.APPROXIMATE
        if self.aggregation in (Aggregation.DECLINED_RATIO, Aggregation.AMOUNT_CV):
            return ParityComparison.FLOAT
        return ParityComparison.EXACT

    @property
    def is_approximate(self) -> bool:
        """Whether the ONLINE value is an estimate.

        A function of the declared storage class, not of the aggregation: an
        exact sorted-set distinct count is exact, and reporting it as approximate
        would put a tolerance on a comparison that should be equality."""
        return self.storage is CardinalityStorage.APPROXIMATE


@dataclass(frozen=True, slots=True)
class ProfileAttribute:
    """`metric` read from `entity`'s accumulated profile."""

    entity: Entity
    metric: ProfileMetric
    horizon: Window = PROFILE_ACTIVITY_HORIZON
    """How far back the profile must be complete for a NEGATIVE answer to be
    trusted. "This device is known" is sound the moment it is observed;
    "this device is NOT known" and "this account is N days old" are only sound
    if the store has watched at least this long, because an older device or
    account would look exactly the same to a younger store."""

    @property
    def current_observation(self) -> CurrentObservation:
        """A profile is a baseline: strictly before `as_of`, always (ADR-0046 §3)."""
        return CurrentObservation.EXCLUDED

    @property
    def parity(self) -> ParityComparison:
        membership = (
            ProfileMetric.MERCHANT_IS_HABITUAL,
            ProfileMetric.MCC_IS_HABITUAL,
            ProfileMetric.DEVICE_IS_KNOWN,
        )
        return ParityComparison.EXACT if self.metric in membership else ParityComparison.FLOAT

    @property
    def is_approximate(self) -> bool:
        """False for every profile attribute.

        Phase 2 declared the robust z-score approximate because the online store kept a
        bounded recent sample and the offline side would take an exact median over all
        history -- two estimands under one name. ADR-0046 (Q4d) declares the bounded sample
        AS the definition: the last `AMOUNT_SAMPLE_SIZE` same-currency amounts strictly
        before `as_of` within the lifetime. Every implementation then computes the same
        median and MAD exactly, and parity is an equality."""
        return False


@dataclass(frozen=True, slots=True)
class PairwiseWithPrevious:
    """`metric` between this observation and `entity`'s previous one on `stream`."""

    entity: Entity
    metric: PairwiseMetric
    stream: Stream = Stream.TRANSACTION
    lookback: Window = ONE_DAY
    """How far back a "previous observation" is looked for.

    One day, and it loses nothing the rules can use: `hours_since_identity_change`
    is compared against 24 h, and an implied speed over a gap longer than a day
    can never exceed 1,000 km/h, because half the Earth's circumference is about
    20,000 km. It was already the effective bound -- the previous-observation
    key expired after the global retention -- and is now stated where the
    retention is derived from it."""

    @property
    def current_observation(self) -> CurrentObservation:
        """The PREVIOUS observation is by definition not this one (ADR-0046 §4)."""
        return CurrentObservation.EXCLUDED

    @property
    def parity(self) -> ParityComparison:
        """Distances, speeds and elapsed seconds or hours all pass through floats."""
        return ParityComparison.FLOAT

    @property
    def is_approximate(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class RowLocal:
    """A pure function of the transaction row, reading no accumulated state.

    The degenerate shape, and the only one with no online/offline divergence to
    measure: both paths evaluate the same expression over the same row, so its
    parity tolerance is exactly zero. No production Phase 2 feature is row-local
    -- fraud is a departure from a baseline, and a baseline is state -- but the
    shape is real, and the features that exercise the UNAVAILABLE mechanism
    (`trace_core.features.reference_features`) are exactly this.
    """

    @property
    def current_observation(self) -> CurrentObservation:
        """A row-local feature reads nothing but the current observation."""
        return CurrentObservation.INCLUDED

    @property
    def parity(self) -> ParityComparison:
        return ParityComparison.EXACT

    @property
    def is_approximate(self) -> bool:
        return False


Semantics = WindowedAggregate | ProfileAttribute | PairwiseWithPrevious | RowLocal


def required_lookback_s(
    semantics: WindowedAggregate | ProfileAttribute | PairwiseWithPrevious | RowLocal,
) -> int:
    """How far before `as_of` the store must have been recording for this
    feature's answer to be complete.

    The number every completeness decision is made from, so it is defined once
    beside the declarations it reads rather than re-derived by each store.
    `RowLocal` reads no state and is complete by construction.
    """
    if isinstance(semantics, WindowedAggregate):
        return semantics.window.seconds
    if isinstance(semantics, ProfileAttribute):
        return semantics.horizon.seconds
    if isinstance(semantics, PairwiseWithPrevious):
        return semantics.lookback.seconds
    return 0
