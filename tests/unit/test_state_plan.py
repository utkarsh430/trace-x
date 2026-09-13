"""The online store holds what the released features declare, and nothing else.

ADR-0044. Before this the write path was a cross product -- every entity, every
stream, one retention for everything -- and measured against a representative
500 TPS workload it spent the memory that correctness-relevant state needed on
state nothing read. These tests pin the derivation: what is written, for how
long, and how far back each feature needs the store to have been recording.
Asserted as RELATIONSHIPS to the declarations rather than as a list of magic
values, so a feature added tomorrow changes the plan without changing the test.
"""

from __future__ import annotations

import pytest

from trace_core.contracts.canonical import CanonicalField as F
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.semantics import (
    LATE_ARRIVAL_MARGIN_S,
    Aggregation,
    Entity,
    PairwiseWithPrevious,
    ProfileAttribute,
    Stream,
    WindowedAggregate,
    required_lookback_s,
)
from trace_core.features.state_plan import BUCKET_DERIVED, NEEDS_VELOCITY, PLAN, build_plan

pytestmark = pytest.mark.unit


def test_every_released_feature_has_a_lookback_and_nothing_else_does() -> None:
    assert set(PLAN.lookback_s) == set(ONLINE_FEATURES.ids)
    for spec in ONLINE_FEATURES:
        assert PLAN.lookback_s[spec.feature_id] == required_lookback_s(spec.semantics)


def test_velocity_sets_exist_exactly_where_a_count_is_read() -> None:
    """DEVICE and IP have only distinct-count features, so no velocity set.

    Writing one for them was the largest single waste in the old write path:
    four sorted sets per request that no feature ever read.
    """
    expected = {
        (s.entity, s.stream)
        for f in ONLINE_FEATURES
        if isinstance((s := f.semantics), WindowedAggregate) and s.aggregation in NEEDS_VELOCITY
    }
    assert set(PLAN.velocity) == expected
    for entity in (Entity.DEVICE, Entity.IP):
        assert (entity, Stream.TRANSACTION) not in PLAN.velocity, (
            f"{entity} has no COUNT or AMOUNT_CV feature; a velocity set for it is "
            f"memory spent on a feature nobody released"
        )


def test_buckets_exist_exactly_where_an_amount_aggregate_is_read() -> None:
    expected = {
        s.entity
        for f in ONLINE_FEATURES
        if isinstance((s := f.semantics), WindowedAggregate) and s.aggregation in BUCKET_DERIVED
    }
    assert set(PLAN.buckets) == expected


def test_retention_is_the_widest_reader_plus_the_margin_per_primitive() -> None:
    """Card velocity is read over five minutes. It was kept for twenty-five hours."""
    for (entity, stream), windows in PLAN.velocity.items():
        widest = max(w.seconds for w in windows)
        assert PLAN.velocity_retention(entity, stream).seconds == widest + LATE_ARRIVAL_MARGIN_S
    for entity, windows in PLAN.buckets.items():
        widest = max(w.seconds for w in windows)
        assert PLAN.bucket_retention(entity).seconds == widest + LATE_ARRIVAL_MARGIN_S
    card = PLAN.velocity_retention(Entity.CARD, Stream.TRANSACTION).seconds
    account = PLAN.velocity_retention(Entity.ACCOUNT, Stream.TRANSACTION).seconds
    assert card < account, (
        "one retention for everything is what kept a 5-minute card window for a day"
    )


def test_previous_and_profile_retention_come_from_the_declarations() -> None:
    for f in ONLINE_FEATURES:
        s = f.semantics
        if isinstance(s, PairwiseWithPrevious):
            assert PLAN.previous[(s.entity, s.stream)].seconds >= s.lookback.seconds
            assert (
                PLAN.previous_retention(s.entity, s.stream).seconds
                == PLAN.previous[(s.entity, s.stream)].seconds + LATE_ARRIVAL_MARGIN_S
            )
        if isinstance(s, ProfileAttribute):
            assert PLAN.profiles[s.entity].seconds >= s.horizon.seconds
            assert PLAN.profile_retention(s.entity).seconds == PLAN.profiles[s.entity].seconds


def test_the_widest_lookback_is_the_profile_horizon() -> None:
    """How long a fresh store takes to warm completely -- and what a restart costs."""
    assert PLAN.widest_lookback_s == max(
        s.horizon.seconds
        for f in ONLINE_FEATURES
        if isinstance((s := f.semantics), ProfileAttribute)
    )


def test_distinct_dimensions_name_every_counted_dimension_per_entity() -> None:
    """A complete-and-empty window must say "zero distinct X" for every X that
    is counted, or `_distinct` cannot tell zero from not-declared."""
    for f in ONLINE_FEATURES:
        s = f.semantics
        if isinstance(s, WindowedAggregate) and s.aggregation is Aggregation.DISTINCT_COUNT:
            assert s.dimension in PLAN.distinct_dimensions(s.entity)


def test_conflicting_storage_declarations_are_refused_at_build() -> None:
    from trace_core.features.semantics import CardinalityStorage, Dimension, Window
    from trace_core.features.spec import FeatureRegistry, FeatureSpec

    def _spec(fid: str, storage: CardinalityStorage) -> FeatureSpec:
        return FeatureSpec(
            feature_id=fid,
            description="x",
            required_fields=frozenset({F.ACCOUNT_ID}),
            semantics=WindowedAggregate(
                entity=Entity.ACCOUNT,
                stream=Stream.TRANSACTION,
                window=Window(60, "1m"),
                aggregation=Aggregation.DISTINCT_COUNT,
                dimension=Dimension.MERCHANT,
                storage=storage,
            ),
            compute=lambda tx, ctx: 0.0,
        )

    registry = FeatureRegistry()
    registry.register(_spec("a", CardinalityStorage.EXACT))
    registry.register(_spec("b", CardinalityStorage.APPROXIMATE))
    with pytest.raises(ValueError, match="both EXACT and APPROXIMATE"):
        build_plan(registry)
