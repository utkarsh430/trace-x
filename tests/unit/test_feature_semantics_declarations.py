"""The declarations ADR-0046 adds, pinned literally.

Each is a statement a Spark implementation must read rather than re-derive, so each is
asserted as a literal table: a change to any of them is a change of meaning, and a
`FEATURE_SET_VERSION` bump, never a quiet edit.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
from typing import Final

import pytest

from trace_core.contracts.canonical import CanonicalField
from trace_core.domain.enums import AuthorizationOutcome, IdentityEventType
from trace_core.domain.time import event_time
from trace_core.features import FEATURE_SET_VERSION
from trace_core.features.context import INSUFFICIENT_HISTORY
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.observation import (
    NAMESPACES,
    POST_DECISION_FIELDS,
    Event,
    IdentityNamespace,
    ObserveReceipt,
)
from trace_core.features.profile_math import geodesic_medoid, robust_centre, whole_metres
from trace_core.features.reference import ReferenceFeatureStore
from trace_core.features.semantics import (
    ONE_MINUTE,
    Aggregation,
    CurrentObservation,
    Entity,
    Stream,
    WindowedAggregate,
    identity_stream,
)
from trace_core.features.spec import FeatureRegistry, FeatureSpec

pytestmark = pytest.mark.unit

T0: Final = event_time(dt.datetime(2026, 3, 1, 12, 0, tzinfo=dt.UTC))

IDENTITY_STREAMS: Final[dict[str, Stream | None]] = {
    "PASSWORD_CHANGE": Stream.IDENTITY_CHANGE,
    "EMAIL_CHANGE": Stream.IDENTITY_CHANGE,
    "PHONE_CHANGE": Stream.IDENTITY_CHANGE,
    "ADDRESS_CHANGE": Stream.IDENTITY_CHANGE,
    "MFA_RESET": Stream.IDENTITY_CHANGE,
    "LOGIN_FAILED": Stream.IDENTITY_FAILED_LOGIN,
    "LOGIN_SUCCEEDED": None,
    "MFA_ENROLLED": None,
    "UNKNOWN": None,
}
"""ADR-0046 §4 (Q4e). A successful login is not a credential change, and enrolling a second
factor makes an account safer; neither may reset `hours_since_identity_change`."""

EXCLUDES_THE_SCORED_TRANSACTION: Final = frozenset(
    {
        "amount_zscore_vs_account",
        "account_tenure_days",
        "merchant_is_habitual",
        "mcc_is_habitual_for_account",
        "device_is_known_for_account",
        "distance_from_account_home_km",
        "geo_distance_from_last_km",
        "implied_speed_kmh_from_last",
        "seconds_since_last_transaction",
        "hours_since_identity_change",
    }
)
"""Baselines and the previous observation (ADR-0046 §2-§4). Every other feature -- every
windowed count, sum, ratio, distinct count and dispersion -- includes it."""


def _tx(event_id: str, seconds: int) -> Event:
    return Event(
        stream=Stream.TRANSACTION,
        occurred_at=event_time(T0 + dt.timedelta(seconds=seconds)),
        account_id="acct_000001",
        event_id=event_id,
        currency="GBP",
        amount_minor=100,
    )


def test_the_feature_set_version_is_the_one_adr_0046_declares() -> None:
    assert FEATURE_SET_VERSION == "2.0.0"


def test_every_identity_event_type_has_a_declared_stream() -> None:
    assert {member.value for member in IdentityEventType} == set(IDENTITY_STREAMS)
    for event_type, stream in IDENTITY_STREAMS.items():
        assert identity_stream(event_type) is stream, event_type


def test_whether_the_scored_transaction_counts_is_declared_per_feature() -> None:
    excluded = {
        spec.feature_id
        for spec in ONLINE_FEATURES
        if spec.current_observation is CurrentObservation.EXCLUDED
    }
    assert excluded == EXCLUDES_THE_SCORED_TRANSACTION
    for spec in ONLINE_FEATURES:
        if isinstance(spec.semantics, WindowedAggregate):
            assert spec.current_observation is CurrentObservation.INCLUDED, spec.feature_id


def test_a_registry_refuses_two_readings_of_one_window() -> None:
    registry = FeatureRegistry()

    def spec(feature_id: str, current: CurrentObservation) -> FeatureSpec:
        return FeatureSpec(
            feature_id=feature_id,
            description="test",
            required_fields=frozenset({CanonicalField.ACCOUNT_ID}),
            semantics=WindowedAggregate(
                Entity.ACCOUNT, ONE_MINUTE, Aggregation.COUNT, current_observation=current
            ),
            compute=lambda tx, ctx: INSUFFICIENT_HISTORY,
        )

    registry.register(spec("included", CurrentObservation.INCLUDED))
    with pytest.raises(ValueError, match="disagree"):
        registry.register(spec("excluded", CurrentObservation.EXCLUDED))


def test_only_the_declined_ratio_reads_a_post_decision_field() -> None:
    assert frozenset({CanonicalField.AUTHORIZATION_OUTCOME}) == POST_DECISION_FIELDS
    readers = {s.feature_id for s in ONLINE_FEATURES if s.required_fields & POST_DECISION_FIELDS}
    assert readers == {"declined_ratio_1h"}


def test_an_observation_without_an_identity_cannot_be_constructed() -> None:
    with pytest.raises(ValueError, match="identity"):
        Event(stream=Stream.TRANSACTION, occurred_at=T0, account_id="acct_000001", event_id="")


def test_distances_are_rounded_half_up_to_whole_metres() -> None:
    assert whole_metres(0.0004999) == 0
    assert whole_metres(0.0005) == 1
    assert whole_metres(0.0015) == 2


def test_the_medoid_and_the_robust_centre_refuse_below_their_declared_minimums() -> None:
    assert geodesic_medoid([(1, "a", 0.0, 0.0), (2, "b", 0.0, 1.0)]) is None
    assert geodesic_medoid([(1, "a", 0.0, 0.0), (2, "b", 0.0, 1.0), (3, "c", 0.0, 10.0)]) == (
        0.0,
        1.0,
    )
    assert robust_centre([100] * 7) is None
    assert robust_centre([100, 200, 300, 400, 500, 600, 700, 800]) == (450.0, 200.0)


def test_the_reference_store_reports_positions_and_recognises_redeliveries() -> None:
    store = ReferenceFeatureStore()
    first, second = _tx("tx_1", -20), _tx("tx_2", -10)
    assert store.observe(first) == ObserveReceipt(position=1, recorded=True)
    assert store.observe(first) == ObserveReceipt(position=1, recorded=False)
    assert store.observe(second) == ObserveReceipt(position=2, recorded=True)
    assert store.score(first).receipt == ObserveReceipt(position=2, recorded=False)
    assert len(store.events) == 2


def test_a_redelivery_carrying_another_observation_is_reported_as_conflicting() -> None:
    """The first delivery stays the observation either way. The receipt says whether this one
    agreed with it, so a caller never evaluates its own payload against another's context."""
    store = ReferenceFeatureStore()
    first = _tx("tx_1", -20)
    store.observe(first)
    same_millisecond = dataclasses.replace(
        first, occurred_at=event_time(first.occurred_at + dt.timedelta(microseconds=400))
    )
    assert store.observe(same_millisecond) == ObserveReceipt(position=1, recorded=False)
    learned = dataclasses.replace(first, authorization_outcome=AuthorizationOutcome.REVERSED)
    assert not store.observe(learned).conflicting, "a post-decision field is not the observation"
    for changed in (
        dataclasses.replace(first, account_id="acct_000002"),
        dataclasses.replace(first, amount_minor=first.amount_minor + 1),
        dataclasses.replace(
            first, occurred_at=event_time(first.occurred_at + dt.timedelta(milliseconds=1))
        ),
    ):
        assert store.observe(changed) == ObserveReceipt(
            position=1, recorded=False, conflicting=True
        )
    assert store.events == (first,)


def test_a_read_only_snapshot_does_not_contain_the_transaction_it_would_have_scored() -> None:
    store = ReferenceFeatureStore()
    store.observe(_tx("tx_1", -10))
    subject = _tx("tx_subject", 0)
    snapshot = store.snapshot(as_of=subject.occurred_at, account_id="acct_000001", currency="GBP")
    key = (Entity.ACCOUNT, "acct_000001", Stream.TRANSACTION, "1m")
    assert snapshot.windows[key].count == 1
    assert store.score(subject).context.windows[key].count == 2


def test_the_identity_namespaces_and_their_order_are_pinned() -> None:
    """At one millisecond the namespace decides inclusion (ADR-0046 §1). A new namespace or a
    renamed one changes which same-millisecond observations a window holds, so it is a declared
    change, never a spelling accident."""
    assert sorted(ns.value for ns in IdentityNamespace) == ["identity_event", "transaction"]
    assert set(NAMESPACES) == set(Stream)
    assert NAMESPACES == {
        Stream.TRANSACTION: IdentityNamespace.TRANSACTION,
        Stream.IDENTITY_FAILED_LOGIN: IdentityNamespace.IDENTITY_EVENT,
        Stream.IDENTITY_CHANGE: IdentityNamespace.IDENTITY_EVENT,
    }
