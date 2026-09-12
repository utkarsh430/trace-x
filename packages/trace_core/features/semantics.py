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

| Shape | Online (Phase 2) | Offline (Phase 3) |
|---|---|---|
| `WindowedAggregate` | sorted set / HLL / bucketed hash | event-time `window()` + `groupBy` |
| `ProfileAttribute` | Redis hash | join against the Gold entity profile |
| `PairwiseWithPrevious` | hash with the last observation | `lag()` over `occurred_at` per entity |
| `RowLocal` | nothing to read | a column expression |

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


@dataclass(frozen=True, slots=True)
class Window:
    """A sliding event-time window.

    **Event time, always.** `occurred_at` drives every window; `ingested_at` is
    used only to measure lag (ADR-0026). Online this is the sorted-set score and
    offline it is the watermark column, which is what makes an out-of-order
    arrival land in the window it belongs to on both paths.

    Half-open `(t - seconds, t]`: the transaction being scored is inside its own
    window, and an observation exactly `seconds` old is outside it. Stated
    because the two implementations must agree at the boundary, and a
    one-observation disagreement is exactly the kind of drift nobody attributes.
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

    @property
    def is_approximate(self) -> bool:
        """True where the online estimator differs from the offline computation.

        A robust z-score needs a median, and an online store maintains an
        *estimate* of one; Spark computes it exactly over the window. The
        feature's MEANING is identical -- only the estimator differs, which is
        precisely what `feature_parity_drift` is for."""
        return self.metric is ProfileMetric.AMOUNT_ROBUST_Z


@dataclass(frozen=True, slots=True)
class PairwiseWithPrevious:
    """`metric` between this observation and `entity`'s previous one on `stream`."""

    entity: Entity
    metric: PairwiseMetric
    stream: Stream = Stream.TRANSACTION

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
    def is_approximate(self) -> bool:
        return False


Semantics = WindowedAggregate | ProfileAttribute | PairwiseWithPrevious | RowLocal
