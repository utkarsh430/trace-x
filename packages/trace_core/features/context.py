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

import datetime as dt
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Final

from trace_core.domain.enums import FeatureSource
from trace_core.domain.time import EventTime
from trace_core.features.semantics import WINDOWS, Dimension, Entity, Stream

_WINDOW_SECONDS: Final[dict[str, int]] = {w.label: w.seconds for w in WINDOWS}


class Completeness(StrEnum):
    """Whether the store can vouch for everything inside a lookback.

    The third answer a feature read can give, and the one that separates "this
    entity has done nothing" from "we do not know what this entity did". A
    missing key means the former only if the store has been recording for the
    whole period it is being asked about; otherwise it means nothing at all.
    """

    COMPLETE = "COMPLETE"
    """The store has recorded continuously since before the lookback began, so
    an absent key is a measured zero: the entity genuinely did nothing."""
    INCOMPLETE = "INCOMPLETE"
    """The store began recording inside the lookback. What it holds is a lower
    bound, and an absent key is not evidence of anything."""
    UNKNOWN = "UNKNOWN"
    """The store did not say when it began. Treated exactly as INCOMPLETE; it
    exists as a distinct value so that a store which never learned to report
    its epoch is visible as such rather than silently trusted."""


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
    """Declined observations with a known outcome. Never counts the current observation's
    outcome, which is not known when it is scored (`observation.POST_DECISION_FIELDS`)."""
    outcome_known_count: int = 0
    """Denominator for DECLINED_RATIO. Distinct from `count` because a source
    may not supply `authorization_outcome` for every row, and dividing by the
    wrong denominator would report a ratio that was never measured."""
    distinct: dict[Dimension, int] = field(default_factory=dict)
    """Distinct cardinality per dimension: exact for EXACT storage, the declared
    edge-inclusive five-minute-bucket estimand for APPROXIMATE storage (ADR-0046 §2)."""
    aligned_count: int = 0
    aligned_amount_sum_minor: int = 0
    aligned_amount_sum_squares: int = 0
    """Same-currency count, sum and sum of squares over the declared MINUTE-ALIGNED window
    (ADR-0046 §2): whole minutes strictly between the minute containing `as_of - W` and the
    minute containing `as_of`, plus the current observation when the feature declares it
    INCLUDED (it lies in the excluded minute containing `as_of`, so it is added once, by
    itself). Read only by `AMOUNT_CV`, and kept apart from the exact fields because the two
    windows differ at both edges -- and because the coefficient of variation must divide
    same-currency sums by a same-currency count, which the all-currency `count` is not."""


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
    `(entity, entity_id)`. A missing key means one of two things, and the
    context is what tells them apart: if the store has been recording for the
    whole window (`complete_since` is old enough), the entity genuinely did
    nothing and the window is a measured zero; if it has not, the store cannot
    know, and the feature reports `INSUFFICIENT_HISTORY` -- never a zero.

    **Why the distinction lives here and not in each store.** Both the reference
    implementation and Redis build this same object, so the rule "absent means
    zero only when complete" is written once and the conformance suite proves
    both stores produce the same answer. A store that forgot to report its epoch
    gets `Completeness.UNKNOWN`, which is treated as incomplete: the failure mode
    of a missing epoch is over-caution, never a fabricated zero.
    """

    as_of: EventTime
    """The event time the snapshot describes. Not the wall clock: a replayed
    transaction must see the window it actually belonged to (ADR-0026)."""
    source: FeatureSource = FeatureSource.ONLINE_ONLY
    windows: dict[tuple[Entity, str, Stream, str], WindowState] = field(default_factory=dict)
    profiles: dict[tuple[Entity, str], Profile] = field(default_factory=dict)
    previous: dict[tuple[Entity, str, Stream], Observation] = field(default_factory=dict)
    complete_since: EventTime | None = None
    """When the store began recording continuously, or None if it did not say.

    Compared against the START of each lookback: a window is complete if it
    began after the store did. Wall-clock at store start, interpreted on the
    event-time axis -- the honest approximation, since an event that arrives
    after the store started is recorded whatever its event time says, and one
    that happened before is gone."""
    distinct_dimensions: dict[Entity, tuple[Dimension, ...]] = field(default_factory=dict)
    """Which dimensions the released features count per entity, so that a
    complete-and-empty window can say "zero distinct merchants" rather than
    leave the dimension absent -- which `_distinct` would correctly read as
    "not declared" and refuse to answer."""

    def completeness(self, lookback_s: int) -> Completeness:
        """Can the store vouch for the whole of `(as_of - lookback_s, as_of]`?"""
        if self.complete_since is None:
            return Completeness.UNKNOWN
        began = self.as_of - dt.timedelta(seconds=lookback_s)
        return (
            Completeness.COMPLETE
            if began.timestamp() >= self.complete_since.timestamp()
            else Completeness.INCOMPLETE
        )

    def window(
        self, entity: Entity, entity_id: str | None, stream: Stream, window_label: str
    ) -> WindowState | None:
        """The window's state; a measured zero if absent from a complete store.

        The zero carries the declared distinct dimensions at 0, because a
        window with no observations has zero distinct anything -- and because
        `_distinct` must be able to tell that from a dimension nobody declared.
        """
        if entity_id is None:
            return None
        state = self.windows.get((entity, entity_id, stream, window_label))
        if state is not None:
            return state
        seconds = _WINDOW_SECONDS.get(window_label)
        if seconds is None or self.completeness(seconds) is not Completeness.COMPLETE:
            return None
        return WindowState(distinct=dict.fromkeys(self.distinct_dimensions.get(entity, ()), 0))

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
