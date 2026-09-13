"""What the online store must hold, derived from the released features and nothing else.

**The declarations decide what is stored; the store does not guess.** Every
released `FeatureSpec` carries a `semantics` object (ADR-0032) naming its entity,
stream, window or lookback, aggregation and storage class. This module folds
those into a `StatePlan`: for each storage primitive, which entities and streams
it is written for, which windows read it, how long it must be retained, and how
far back the store has to have been recording for a feature over it to be
trusted. The Redis store consumes the plan for both its reads and its writes;
Phase 3 can consume the same plan to size its offline state.

**Why the writes are derived too.** ADR-0038 pruned the reads to what the
features declare and deliberately left the writes alone, so that history would
accumulate ahead of a feature nobody had declared yet. Measured against a
representative 500 TPS workload that decision cost real memory for hypothetical
features: five-minute card velocity retained for twenty-five hours, previous
observations written for five entities and read for one, minute buckets for
entities with no bucket-derived feature at all. Worse, it cost the memory that
correctness-relevant state needed, and under `allkeys-lru` it was
correctness-relevant state that was evicted. ADR-0044 reverses it: a feature
that is not released has no claim on the store, and a feature released later
declares what it needs and warms up (the backfill contract).

**Retention is per primitive, from the widest window that reads it plus the
late-arrival margin.** One retention for everything -- the previous design --
means every primitive is kept as long as the widest window anywhere, which for a
five-minute window is a 300x overhang.

Nothing here changes what any feature computes. The conformance suite, run
unmodified against the reference implementation and against Redis, is the check
on that claim.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

from trace_core.features.semantics import (
    LATE_ARRIVAL_MARGIN_S,
    Aggregation,
    CardinalityStorage,
    Dimension,
    Entity,
    PairwiseWithPrevious,
    ProfileAttribute,
    Stream,
    Window,
    WindowedAggregate,
    required_lookback_s,
)
from trace_core.features.spec import FeatureRegistry

NEEDS_VELOCITY: Final = frozenset({Aggregation.COUNT, Aggregation.AMOUNT_CV})
"""Aggregations that read the exact per-observation count.

`COUNT` obviously. `AMOUNT_CV` less obviously: it divides by `WindowState.count`,
and the bucket merge supplies that count from the velocity set when one exists
and from the minute-rounded bucket total when one does not -- a different number
at the window boundary. Keeping the velocity set for CV is what keeps the online
and offline coefficient of variation equal rather than merely close."""

BUCKET_DERIVED: Final = frozenset(
    {Aggregation.AMOUNT_SUM, Aggregation.DECLINED_RATIO, Aggregation.AMOUNT_CV}
)
"""Aggregations answered from the minute-bucket hash.

Named positively, not as "everything that is not a count": a new aggregation
should have to say which primitive answers it, and a definition by exclusion
would silently adopt it into the bucket path."""


@dataclass(frozen=True, slots=True)
class Retention:
    """How long one storage primitive is kept, and why."""

    seconds: int
    widest_window: Window

    @classmethod
    def for_windows(cls, windows: tuple[Window, ...]) -> Retention:
        widest = max(windows, key=lambda w: w.seconds)
        return cls(seconds=widest.seconds + LATE_ARRIVAL_MARGIN_S, widest_window=widest)


@dataclass(frozen=True)
class StatePlan:
    """The complete storage contract implied by one feature registry."""

    velocity: dict[tuple[Entity, Stream], tuple[Window, ...]] = field(default_factory=dict)
    """Per-observation sorted sets: written and read for these entity/stream
    pairs, over these windows. An entity/stream pair absent here is NOT written,
    and no `ZCOUNT` is issued for it."""

    buckets: dict[Entity, tuple[Window, ...]] = field(default_factory=dict)
    """Minute-bucket hashes (TRANSACTION stream only, since only transactions
    carry amounts and outcomes), per entity, over these windows."""

    exact_distinct: dict[tuple[Entity, Dimension], tuple[Window, ...]] = field(default_factory=dict)
    approx_distinct: dict[tuple[Entity, Dimension], tuple[Window, ...]] = field(
        default_factory=dict
    )
    previous: dict[tuple[Entity, Stream], Window] = field(default_factory=dict)
    """Previous-observation hashes, with the lookback each is trusted over."""

    profiles: dict[Entity, Window] = field(default_factory=dict)
    """Accumulated profiles, with the activity horizon each is trusted over."""

    lookback_s: dict[str, int] = field(default_factory=dict)
    """`feature_id -> seconds`: how far back the store must have been recording
    for that feature's answer to be complete."""

    # -- derived views -------------------------------------------------------

    def velocity_retention(self, entity: Entity, stream: Stream) -> Retention:
        return Retention.for_windows(self.velocity[(entity, stream)])

    def bucket_retention(self, entity: Entity) -> Retention:
        return Retention.for_windows(self.buckets[entity])

    def exact_distinct_retention(self, entity: Entity, dimension: Dimension) -> Retention:
        return Retention.for_windows(self.exact_distinct[(entity, dimension)])

    def approx_distinct_retention(self, entity: Entity, dimension: Dimension) -> Retention:
        return Retention.for_windows(self.approx_distinct[(entity, dimension)])

    def previous_retention(self, entity: Entity, stream: Stream) -> Retention:
        return Retention.for_windows((self.previous[(entity, stream)],))

    def profile_retention(self, entity: Entity) -> Retention:
        # A profile has no window; its horizon IS its retention. No margin,
        # because there is no late arrival to a lifetime accumulation.
        window = self.profiles[entity]
        return Retention(seconds=window.seconds, widest_window=window)

    def storage(self, entity: Entity, dimension: Dimension) -> CardinalityStorage | None:
        if (entity, dimension) in self.exact_distinct:
            return CardinalityStorage.EXACT
        if (entity, dimension) in self.approx_distinct:
            return CardinalityStorage.APPROXIMATE
        return None

    def distinct_dimensions(self, entity: Entity) -> tuple[Dimension, ...]:
        """Every dimension any feature counts for this entity.

        Used to synthesise a complete-and-empty window: an entity the store has
        watched for the whole window and never seen has ZERO distinct anything,
        and the zero has to name the dimensions or `_distinct` cannot tell "zero"
        from "not declared"."""
        return tuple(
            sorted(
                {d for (e, d) in self.exact_distinct if e is entity}
                | {d for (e, d) in self.approx_distinct if e is entity},
                key=lambda d: d.value,
            )
        )

    @property
    def velocity_streams(self) -> frozenset[tuple[Entity, Stream]]:
        return frozenset(self.velocity)

    @property
    def bucket_entities(self) -> frozenset[Entity]:
        return frozenset(self.buckets)

    @property
    def widest_lookback_s(self) -> int:
        """The longest any feature needs the store to have been recording.

        This is how long a fresh store takes to warm completely, and therefore
        how long a Redis restart costs. Reported at readiness."""
        return max(self.lookback_s.values(), default=0)


def _append(table: dict, key: object, window: Window) -> None:  # type: ignore[type-arg]
    existing = table.get(key, ())
    if window not in existing:
        table[key] = (*existing, window)


def build_plan(registry: FeatureRegistry) -> StatePlan:
    """Fold a registry's declarations into a `StatePlan`.

    Raises at import if two features declare the same distinct dimension with
    different storage classes: one physical representation cannot serve both,
    and ADR-0034 forbids choosing at runtime.
    """
    velocity: dict[tuple[Entity, Stream], tuple[Window, ...]] = {}
    buckets: dict[Entity, tuple[Window, ...]] = {}
    exact: dict[tuple[Entity, Dimension], tuple[Window, ...]] = {}
    approx: dict[tuple[Entity, Dimension], tuple[Window, ...]] = {}
    previous: dict[tuple[Entity, Stream], Window] = {}
    profiles: dict[Entity, Window] = {}
    lookback: dict[str, int] = {}

    for spec in registry:
        semantics = spec.semantics
        lookback[spec.feature_id] = required_lookback_s(semantics)

        if isinstance(semantics, PairwiseWithPrevious):
            pair = (semantics.entity, semantics.stream)
            current = previous.get(pair)
            if current is None or semantics.lookback.seconds > current.seconds:
                previous[pair] = semantics.lookback
            continue

        if isinstance(semantics, ProfileAttribute):
            current_profile = profiles.get(semantics.entity)
            if current_profile is None or semantics.horizon.seconds > current_profile.seconds:
                profiles[semantics.entity] = semantics.horizon
            continue

        if not isinstance(semantics, WindowedAggregate):
            continue

        if semantics.aggregation in NEEDS_VELOCITY:
            _append(velocity, (semantics.entity, semantics.stream), semantics.window)
        if semantics.aggregation in BUCKET_DERIVED:
            if semantics.stream is not Stream.TRANSACTION:
                raise ValueError(
                    f"{spec.feature_id}: {semantics.aggregation} needs amounts, which only "
                    f"the TRANSACTION stream carries; got {semantics.stream}"
                )
            _append(buckets, semantics.entity, semantics.window)
        if semantics.aggregation is Aggregation.DISTINCT_COUNT:
            assert semantics.dimension is not None and semantics.storage is not None
            counted = (semantics.entity, semantics.dimension)
            other = approx if semantics.storage is CardinalityStorage.EXACT else exact
            if counted in other:
                raise ValueError(
                    f"{counted} is declared both EXACT and APPROXIMATE by different features; "
                    f"one physical representation cannot serve both"
                )
            target = exact if semantics.storage is CardinalityStorage.EXACT else approx
            _append(target, counted, semantics.window)

    return StatePlan(
        velocity=velocity,
        buckets=buckets,
        exact_distinct=exact,
        approx_distinct=approx,
        previous=previous,
        profiles=profiles,
        lookback_s=lookback,
    )


def online_plan() -> StatePlan:
    """The plan for the released online feature set. Imported lazily so this
    module can be imported by `definitions.py`'s neighbours without a cycle."""
    from trace_core.features.definitions import ONLINE_FEATURES

    return build_plan(ONLINE_FEATURES)


PLAN: Final = online_plan()
