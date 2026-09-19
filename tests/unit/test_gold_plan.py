"""Gold's plan without a JVM: what it compiles, how it reads rows back, how it records progress.

ADR-0055. The Spark computation is held to the literal fixtures on a real JVM
(tests/stream/test_feature_semantics_gold.py); these hold the pure parts to their declarations.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
from pathlib import Path

import pytest

from trace_core.contracts.canonical import CanonicalField, CanonicalTransaction
from trace_core.contracts.events.identity_events_v1 import IdentityEventType
from trace_core.domain.enums import TransactionChannel
from trace_core.domain.time import EventTime, event_time, from_millis, to_millis
from trace_core.features.context import INSUFFICIENT_HISTORY, WindowState
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.observation import Event
from trace_core.features.semantics import (
    ONE_HOUR,
    ONE_MINUTE,
    Aggregation,
    CardinalityStorage,
    CurrentObservation,
    Dimension,
    Entity,
    Stream,
    WindowedAggregate,
    identity_stream,
)
from trace_core.features.spec import FeatureRegistry, FeatureSpec
from trace_core.stream import gold_plan as plan
from trace_core.stream.checkpoints import SparkProgress
from trace_core.stream.silver_rules import SHARED_TABLES

pytestmark = pytest.mark.unit

T0_MS = to_millis(dt.datetime(2026, 3, 1, 12, 0, tzinfo=dt.UTC))


def _pins() -> tuple[plan.SourcePin, ...]:
    return tuple(
        plan.SourcePin(str(ref), f"id-{i}", 3 + i, T0_MS + i * 1000)
        for i, ref in enumerate(plan.GOLD_SOURCES)
    )


# ----------------------------------------------------------------- compiling ---


def test_every_released_window_compiles_to_exactly_the_fields_its_features_read() -> None:
    assert plan.unsupported_declarations() == []
    specs = plan.compile_windows()
    declared = {
        (s.semantics.entity, s.semantics.stream, s.semantics.window)
        for s in ONLINE_FEATURES
        if isinstance(s.semantics, WindowedAggregate)
    }
    assert {(s.entity, s.stream, s.window) for s in specs} == declared
    state_fields = {f.name for f in dataclasses.fields(WindowState)}
    assert set(plan.SCALAR_FIELDS) <= state_fields
    for fields in plan.READS.values():
        assert fields <= set(plan.SCALAR_FIELDS)
    by_key = {(s.entity, s.stream, s.label): s for s in specs}
    cv = by_key[(Entity.MERCHANT, Stream.TRANSACTION, "24h")]
    assert cv.fields == plan.ALIGNED_FIELDS and not cv.needs_raw and cv.present_by_construction
    merchants = by_key[(Entity.MERCHANT, Stream.TRANSACTION, "1h")]
    assert merchants.approximate_dimensions == {Dimension.ACCOUNT} and not merchants.needs_raw
    logins = by_key[(Entity.ACCOUNT, Stream.IDENTITY_FAILED_LOGIN, "1h")]
    assert not logins.present_by_construction and "count" in logins.fields
    outcomes = by_key[(Entity.ACCOUNT, Stream.AUTHORIZATION_OUTCOME, "1h")]
    assert outcomes.current_observation is CurrentObservation.PRIOR_KNOWN
    assert outcomes.fields == {"count", "declined_count", "outcome_known_count"}
    account_hour = by_key[(Entity.ACCOUNT, Stream.TRANSACTION, "1h")]
    assert account_hour.fields == {"count", "amount_sum_minor"}
    assert account_hour.exact_dimensions == {Dimension.MERCHANT}


def _spec(feature_id: str, semantics: WindowedAggregate) -> FeatureSpec:
    return FeatureSpec(
        feature_id=feature_id,
        description="a declaration Gold must refuse",
        required_fields=frozenset({CanonicalField.ACCOUNT_ID}),
        semantics=semantics,
        compute=lambda _tx, _ctx: INSUFFICIENT_HISTORY,
    )


def test_a_declaration_gold_does_not_compile_is_refused_before_anything_runs() -> None:
    registry = FeatureRegistry()
    registry.register(
        _spec(
            "excluded_count",
            WindowedAggregate(
                Entity.ACCOUNT,
                ONE_MINUTE,
                Aggregation.COUNT,
                current_observation=CurrentObservation.EXCLUDED,
            ),
        )
    )
    registry.register(
        _spec(
            "identity_sketch",
            WindowedAggregate(
                Entity.ACCOUNT,
                ONE_HOUR,
                Aggregation.DISTINCT_COUNT,
                stream=Stream.IDENTITY_CHANGE,
                dimension=Dimension.DEVICE,
                storage=CardinalityStorage.APPROXIMATE,
            ),
        )
    )
    problems = plan.unsupported_declarations(registry)
    assert len(problems) == 2, problems
    with pytest.raises(plan.GoldPlanError, match="does not compile"):
        plan.compile_windows(registry)


def test_entity_and_dimension_columns_are_the_fields_an_event_is_read_by() -> None:
    probe = Event(
        stream=Stream.TRANSACTION,
        occurred_at=event_time(dt.datetime(2026, 3, 1, tzinfo=dt.UTC)),
        account_id="acct_000001",
        event_id="tx_probe",
        card_id="card_000001",
        device_id="dev_000001",
        merchant_id="mrch_00001",
        ip_id="ip_00001",
        merchant_mcc="5411",
        merchant_country="GB",
    )
    for entity, column in plan.ENTITY_COLUMNS.items():
        assert getattr(probe, column) == probe.entity_id(entity)
    for dimension, column in plan.DIMENSION_COLUMNS.items():
        assert getattr(probe, column) == probe.dimension_value(dimension)
    assert set(plan.ENTITY_COLUMNS) == set(Entity)
    assert set(plan.DIMENSION_COLUMNS) == set(Dimension)


def test_identity_event_types_feed_exactly_their_declared_streams() -> None:
    for member in IdentityEventType:
        assert plan.IDENTITY_TYPE_STREAMS.get(member.value) == identity_stream(member.value)
    assert "LOGIN_SUCCEEDED" not in plan.IDENTITY_TYPE_STREAMS
    assert "UNKNOWN" not in plan.IDENTITY_TYPE_STREAMS


def test_gold_reads_only_silver_canonical_observation_tables() -> None:
    assert [str(ref) for ref in plan.GOLD_SOURCES] == [
        "silver.tx_scored_v1",
        "silver.identity_events_v1",
        "silver.tx_authorization_v1",
    ]
    assert not set(plan.GOLD_SOURCES) & set(SHARED_TABLES)


def test_gold_tables_are_declared_with_not_null_keys_and_only_builds_append_only() -> None:
    from trace_core.stream.gold import gold_declarations

    declarations = {d.ref: d for d in gold_declarations()}
    assert list(declarations) == list(plan.GOLD_TARGETS)
    for ref in plan.REPLACED_TABLES:
        fields = {f.name: f for f in declarations[ref].schema.fields}
        for key in plan.TABLE_KEYS[ref]:
            assert fields[key].nullable is False, (ref, key)
        assert "delta.appendOnly" not in declarations[ref].properties
    assert declarations[plan.BUILDS].properties == {"delta.appendOnly": "true"}
    assert all(str(ref).startswith("gold.") for ref in declarations)


# ------------------------------------------------------------------ contexts ---


def test_rows_become_the_context_the_features_read() -> None:
    window = plan.WindowRow(
        entity=Entity.ACCOUNT,
        entity_id="acct_000001",
        stream=Stream.TRANSACTION,
        window_label="1h",
        count=3,
        amount_sum_minor=7_000,
        distinct={Dimension.MERCHANT: 2, Dimension.DEVICE: 0},
    )
    logins = plan.WindowRow(
        entity=Entity.ACCOUNT,
        entity_id="acct_000001",
        stream=Stream.IDENTITY_FAILED_LOGIN,
        window_label="1h",
        count=2,
    )
    profile = plan.ProfileRow(
        account_id="acct_000001",
        first_seen_ms=T0_MS - 86_400_000,
        observation_count=8,
        amount_median_minor=450.0,
        amount_mad_minor=200.0,
        habitual_merchants=frozenset({"mrch_00001"}),
        habitual_mccs=frozenset(),
        known_devices=frozenset({"dev_000001"}),
        home_latitude=None,
        home_longitude=None,
    )
    previous = plan.PreviousRow(
        entity=Entity.ACCOUNT,
        entity_id="acct_000001",
        stream=Stream.TRANSACTION,
        occurred_ms=T0_MS - 100_000,
        latitude=51.5,
        longitude=-0.12,
        card_present=True,
    )
    watched = EventTime(from_millis(T0_MS - 90 * 86_400_000))
    context = plan.ContextRows("tx_mmm", T0_MS, (window, logins), profile, (previous,)).context(
        complete_since=watched
    )
    assert context.complete_since == watched and to_millis(context.as_of) == T0_MS
    state = context.windows[(Entity.ACCOUNT, "acct_000001", Stream.TRANSACTION, "1h")]
    assert (state.count, state.amount_sum_minor, state.distinct) == (
        3,
        7_000,
        {Dimension.MERCHANT: 2},
    )
    subject = CanonicalTransaction(
        source_dataset="unit",
        source_row_id="tx_mmm",
        field_coverage=frozenset(CanonicalField),
        transaction_id="tx_mmm",
        account_id="acct_000001",
        amount_minor=1_000,
        currency="GBP",
        occurred_at=from_millis(T0_MS),
        ingested_at=from_millis(T0_MS),
        merchant_id="mrch_00001",
        merchant_mcc="5411",
        merchant_country="GB",
        device_id="dev_000001",
        card_id="card_000001",
        ip_id="ip_00001",
        latitude=51.5,
        longitude=-0.12,
        channel=TransactionChannel.CARD_PRESENT,
    )
    values = ONLINE_FEATURES.evaluate_all(subject, context)
    assert values["account_tx_count_1h"].value == 3.0
    assert values["account_distinct_merchants_1h"].value == 2.0
    assert values["failed_logins_1h"].value == 2.0
    assert values["merchant_is_habitual"].value == 1.0
    assert values["seconds_since_last_transaction"].value == 100.0
    assert values["account_tenure_days"].value == 1.0
    # A window Gold holds no row for is absent, as for the reference: a measured zero only on a
    # store watched over it (ADR-0044), and no value at all without a completeness claim.
    assert values["account_tx_count_1m"].value == 0.0
    unwatched = plan.ContextRows("tx_mmm", T0_MS, (window,)).context(complete_since=None)
    assert not ONLINE_FEATURES.get("account_tx_count_1m").evaluate(subject, unwatched)
    assert not ONLINE_FEATURES.get("account_tenure_days").evaluate(subject, unwatched)


# ------------------------------------------------------------------ progress ---


def test_a_plan_is_published_once_and_read_back_exactly(tmp_path: Path) -> None:
    build = plan.BuildPlan(0, T0_MS, _pins())
    plan.write_plan(tmp_path, build)
    assert plan.read_plan(tmp_path, 0) == build
    with pytest.raises(plan.GoldRefusedError, match="already exists"):
        plan.write_plan(tmp_path, build)
    plan.write_commit(tmp_path, 0)
    with pytest.raises(plan.GoldRefusedError, match="already exists"):
        plan.write_commit(tmp_path, 0)
    progress = SparkProgress.read(tmp_path)
    assert (progress.planned, progress.committed) == (frozenset({0}), frozenset({0}))
    assert not [p for p in tmp_path.rglob("*") if p.name.endswith(".tmp")]
    assert build.newest_commit_ms == T0_MS + 2000
    assert plan.lag_ms(T0_MS + 5000, build) == 3000


def test_a_plan_must_pin_exactly_the_gold_sources_in_order(tmp_path: Path) -> None:
    with pytest.raises(plan.GoldRefusedError, match="pins"):
        plan.BuildPlan(0, T0_MS, tuple(reversed(_pins())))
    garbage = tmp_path / plan.OFFSETS_DIR / "0"
    garbage.parent.mkdir(parents=True)
    garbage.write_text("v1\n{}\n")
    with pytest.raises(plan.GoldRefusedError, match="unreadable"):
        plan.read_plan(tmp_path, 0)


@pytest.mark.parametrize(
    ("planned", "committed", "expected"),
    [
        (set(), set(), (0, False)),
        ({0}, set(), (0, True)),
        ({0}, {0}, (1, False)),
        ({0, 1}, {0}, (1, True)),
    ],
)
def test_the_next_build_follows_the_last_commit_and_replays_a_planned_one(
    planned: set[int], committed: set[int], expected: tuple[int, bool]
) -> None:
    progress = SparkProgress(planned=frozenset(planned), committed=frozenset(committed))
    assert plan.next_build(progress) == expected


@pytest.mark.parametrize(
    ("planned", "committed"),
    [({1}, {1}), ({0, 3}, {0}), (set(), {0}), ({0, 1, 2}, {0})],
    ids=["gap", "planned-beyond-next", "committed-unplanned", "two-uncommitted"],
)
def test_progress_that_this_job_never_writes_is_refused(
    planned: set[int], committed: set[int]
) -> None:
    progress = SparkProgress(planned=frozenset(planned), committed=frozenset(committed))
    with pytest.raises(plan.GoldRefusedError):
        plan.next_build(progress)
