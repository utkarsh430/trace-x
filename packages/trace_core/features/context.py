"""What a feature reads, and the third answer a feature can give.

**The context is a snapshot, not a live store.** Everything a transaction's 25
features need is read in one pass and frozen into a `FeatureContext`; features
are then pure functions of `(transaction, context)`. Three things follow, and
all three are the point:

* the hot path makes **one** round trip rather than 25, which is what a 100 ms
  p99 budget requires (ADR-0002);
* a feature is testable with a literal, with no store of any kind;
* the reference implementation and, in Phase 3, Spark can build the *same*
  snapshot from their own sources, so the parity test compares the feature
  logic rather than two different retrieval paths.

**`INSUFFICIENT_HISTORY` is not `UNAVAILABLE`, and conflating them is a real
error.** ADR-0022's `UNAVAILABLE` means *this source does not supply the input at
all* -- IEEE-CIS has no latitude, so every geo feature is undefined on it
forever, and a transfer metric must exclude them. "This account was opened four
hours ago, so it has no 24-hour baseline" is a different fact: the input exists,
the source supplies it, there is simply not enough of it yet. It resolves on its
own with time, it does not disqualify the feature from a cross-dataset
comparison, and it is common enough in normal traffic that treating it as a
coverage gap would make the coverage report wrong.

What they share is the thing that matters: neither is ever a number. Reading
either as a float raises, so a rule cannot quietly treat "no history" as zero and
report a confident "not fraud".
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

from trace_core.domain.enums import FeatureSource
from trace_core.domain.time import EventTime
from trace_core.features.semantics import Dimension, Entity, Stream


class InsufficientHistory:
    """Sentinel: computable in principle, not yet computable for this entity.

    A distinct type rather than `None` so mypy can tell a compute function that
    forgot to return from one that deliberately declined.
    """

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "INSUFFICIENT_HISTORY"


INSUFFICIENT_HISTORY: Final = InsufficientHistory()

MIN_OBSERVATIONS_FOR_ROBUST_Z: Final = 8
"""Below this, a median and MAD describe the sample rather than the account.

Eight is a judgement, not a measurement, and it is recorded here rather than
buried in a compute function so it can be revisited against Phase 3's offline
distribution. Too low and every new account looks anomalous on its second
transaction; too high and a genuine takeover on a young account is invisible.
"""


@dataclass(frozen=True, slots=True)
class WindowState:
    """What one entity did on one stream inside one window.

    Carries the raw sufficient statistics rather than the finished features, so
    several features share one read and the reduction stays in the feature
    definition where it can be compared across implementations.
    """

    count: int = 0
    amount_sum_minor: int = 0
    amount_sum_squares: int = 0
    """Sum of amount_minor squared, for the coefficient of variation.

    Integer arithmetic, so it is exact -- a running float variance loses
    precision at exactly the scale where uniform-amount laundering lives."""
    declined_count: int = 0
    outcome_known_count: int = 0
    """Denominator for DECLINED_RATIO. Distinct from `count` because a source
    may not supply `authorization_outcome` for every row, and dividing by the
    wrong denominator would report a ratio that was never measured."""
    distinct: dict[Dimension, int] = field(default_factory=dict)
    """Approximate distinct cardinality per dimension (HyperLogLog online)."""


@dataclass(frozen=True, slots=True)
class Observation:
    """The previous event for an entity, for pairwise comparison."""

    occurred_at: EventTime
    latitude: float | None = None
    longitude: float | None = None
    card_present: bool = False
    """Whether that observation was card-present.

    `IMPOSSIBLE_TRAVEL` requires BOTH legs card-present: a card-not-present leg
    has an innocent explanation, and a rule that ignored this would label ordinary
    online shopping as travelling faster than an aircraft
    (docs/FRAUD_SCENARIOS.md §3.3)."""


@dataclass(frozen=True, slots=True)
class Profile:
    """An entity's accumulated state.

    Every field is optional because a first-ever transaction has none of it, and
    the honest answer then is `INSUFFICIENT_HISTORY` rather than a default.
    """

    first_seen_at: EventTime | None = None
    observation_count: int = 0
    amount_median_minor: float | None = None
    amount_mad_minor: float | None = None
    """Median absolute deviation. Zero is legitimate (an account that always
    spends exactly the same amount) and is handled where the z-score is
    computed, not by pretending the value is missing."""
    habitual_merchants: frozenset[str] = frozenset()
    habitual_mccs: frozenset[str] = frozenset()
    known_devices: frozenset[str] = frozenset()
    home_latitude: float | None = None
    home_longitude: float | None = None


@dataclass(frozen=True, slots=True)
class FeatureContext:
    """Every value the online store returned for one transaction.

    Keyed by `(entity, entity_id, stream, window_label)` and
    `(entity, entity_id)`: a missing key means the store had nothing, which the
    feature turns into `INSUFFICIENT_HISTORY` -- never into a zero.
    """

    as_of: EventTime
    """The event time the snapshot describes. Not the wall clock: a replayed
    transaction must see the window it actually belonged to (ADR-0026)."""
    source: FeatureSource = FeatureSource.ONLINE_ONLY
    windows: dict[tuple[Entity, str, Stream, str], WindowState] = field(default_factory=dict)
    profiles: dict[tuple[Entity, str], Profile] = field(default_factory=dict)
    previous: dict[tuple[Entity, str, Stream], Observation] = field(default_factory=dict)

    def window(
        self, entity: Entity, entity_id: str | None, stream: Stream, window_label: str
    ) -> WindowState | None:
        if entity_id is None:
            return None
        return self.windows.get((entity, entity_id, stream, window_label))

    def profile(self, entity: Entity, entity_id: str | None) -> Profile | None:
        if entity_id is None:
            return None
        return self.profiles.get((entity, entity_id))

    def previous_observation(
        self, entity: Entity, entity_id: str | None, stream: Stream
    ) -> Observation | None:
        if entity_id is None:
            return None
        return self.previous.get((entity, entity_id, stream))
