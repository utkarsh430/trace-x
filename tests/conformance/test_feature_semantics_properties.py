"""Properties tying the two evaluation modes together, on the reference implementation.

These do not replace the literal fixtures: a property holds for a consistently wrong
implementation as easily as for a right one. They catch a different failure -- the two modes
drifting apart where ADR-0046 says they must coincide.

* Serving observations in `(occurred_ms, namespace, id)` order and scoring the latest is, by
  definition, the event-time-complete answer.
* The same holds with the scored transaction at ANY position in that order, with observations
  tied at its millisecond on both sides of it, for every exact feature.
* Redeliveries change nothing, whatever they carry, in either mode.

What a property cannot do: a rule broken identically in both modes -- the amount sample's size, the
home sample, a tie-break inside the medoid -- leaves the modes agreeing. Only the literal
fixtures and
their mutants catch those. The last property is built so that lifetime gaps on both sides of 30
days, robust z-scores and over-full home samples are reached by construction, and asserts that they
were, so mode agreement is at least exercised where the profile rules act.
"""

from __future__ import annotations

import datetime as dt
from typing import Final

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import event as label_case
from hypothesis import strategies as st
from tests.conformance.feature_semantics_suite import T0, at_ms, transaction

from trace_core.contracts.canonical import CanonicalTransaction
from trace_core.domain.enums import AuthorizationOutcome, TransactionChannel
from trace_core.domain.time import EventTime, event_time
from trace_core.features import FeatureContext, FeatureState
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.observation import Event, transaction_observation
from trace_core.features.reference import ReferenceFeatureStore, event_time_complete_context
from trace_core.features.semantics import ParityComparison, Stream

pytestmark = [pytest.mark.conformance, pytest.mark.property]

EPOCHS: Final[tuple[EventTime | None, ...]] = (
    None,
    event_time(T0 - dt.timedelta(days=90)),
    event_time(T0 - dt.timedelta(hours=1)),
)
OFFSETS_MS = st.one_of(
    st.integers(-3, 0),  # ties at and just before the scored millisecond
    st.integers(-400_000, 0),  # minute and bucket edges
    st.integers(-90_000_000, 0),  # across the 24 h windows and lookback
    st.integers(-40 * 86_400_000, 0),  # across a 30-day lifetime gap
)
POINTS = st.sampled_from([None, (0.0, 0.0), (0.0, 1.0), (10.0, 179.5), (10.0, -179.5)])
IDENTITY_STREAMS: Final = [Stream.IDENTITY_FAILED_LOGIN, Stream.IDENTITY_CHANGE]


@st.composite
def observations(draw: st.DrawFn, event_id: str, stream: Stream | None = None) -> Event:
    chosen = (
        stream
        if stream is not None
        else draw(st.sampled_from([Stream.TRANSACTION] * 4 + IDENTITY_STREAMS))
    )
    occurred = at_ms(draw(OFFSETS_MS))
    account = draw(st.sampled_from(["acct_000001", "acct_000002"]))
    if chosen is not Stream.TRANSACTION:
        return Event(stream=chosen, occurred_at=occurred, account_id=account, event_id=event_id)
    point = draw(POINTS)
    return Event(
        stream=chosen,
        occurred_at=occurred,
        account_id=account,
        event_id=event_id,
        currency=draw(st.sampled_from(["GBP", "EUR"])),
        amount_minor=draw(st.integers(-500, 5_000)),
        card_id=draw(st.sampled_from(["card_000001", "card_000002"])),
        device_id=draw(st.sampled_from([None, "dev_000001", "dev_000002"])),
        merchant_id=draw(st.sampled_from(["mrch_00001", "mrch_00002", "mrch_00003"])),
        ip_id=draw(st.sampled_from(["ip_00001", "ip_00002"])),
        merchant_mcc=draw(st.sampled_from(["5411", "5812"])),
        merchant_country=draw(st.sampled_from(["GB", "FR"])),
        latitude=None if point is None else point[0],
        longitude=None if point is None else point[1],
        channel=draw(
            st.sampled_from([TransactionChannel.CARD_PRESENT, TransactionChannel.CARD_NOT_PRESENT])
        ),
        authorization_outcome=draw(
            st.sampled_from([None, AuthorizationOutcome.APPROVED, AuthorizationOutcome.DECLINED])
        ),
    )


@st.composite
def rich_account(draw: st.DrawFn, start: int) -> list[Event]:
    """Enough same-currency, located history on one account to reach the robust z-score's
    minimum and the home sample's cap, with ties and sometimes a lifetime gap inside it."""
    events = []
    for i in range(draw(st.integers(8, 26))):
        point = draw(st.sampled_from([(0.0, 0.0), (0.0, 1.0), (0.0, 5.0), (10.0, -179.5)]))
        offset = draw(st.one_of(st.integers(-3, 0), st.integers(-45 * 86_400_000, 0)))
        events.append(
            Event(
                stream=Stream.TRANSACTION,
                occurred_at=at_ms(offset),
                account_id="acct_000001",
                event_id=f"e_{start + i:03d}",
                currency="GBP",
                amount_minor=draw(st.integers(1, 2_000)),
                card_id="card_000001",
                device_id=draw(st.sampled_from(["dev_000001", "dev_000002"])),
                merchant_id=draw(st.sampled_from(["mrch_00001", "mrch_00002"])),
                ip_id="ip_00001",
                merchant_mcc="5411",
                merchant_country="GB",
                latitude=point[0],
                longitude=point[1],
                channel=TransactionChannel.CARD_PRESENT,
                authorization_outcome=AuthorizationOutcome.APPROVED,
            )
        )
    return events


@st.composite
def histories(draw: st.DrawFn) -> list[Event]:
    events = [draw(observations(f"e_{i:03d}")) for i in range(draw(st.integers(0, 25)))]
    if draw(st.booleans()):
        events += draw(rich_account(len(events)))
    return events


@st.composite
def latest_subjects(draw: st.DrawFn, history: list[Event]) -> CanonicalTransaction:
    """At the latest millisecond in the history, with an identity sorting after all of it."""
    latest = max((e.occurred_at for e in history), default=T0)
    point = draw(st.sampled_from([(0.0, 0.0), (10.0, -179.5)]))
    return transaction(
        occurred_at=latest,
        transaction_id="tx_zzz",
        account_id=draw(st.sampled_from(["acct_000001", "acct_000002"])),
        amount_minor=draw(st.integers(1, 5_000)),
        currency=draw(st.sampled_from(["GBP", "EUR"])),
        merchant_id=draw(st.sampled_from(["mrch_00001", "mrch_00002"])),
        device_id=draw(st.sampled_from(["dev_000001", "dev_000003"])),
        latitude=point[0],
        longitude=point[1],
        authorization_outcome=draw(
            st.sampled_from([AuthorizationOutcome.APPROVED, AuthorizationOutcome.DECLINED])
        ),
    )


def features(
    subject: CanonicalTransaction, context: FeatureContext
) -> dict[str, tuple[FeatureState, float | None]]:
    return {
        feature_id: (value.state, value.or_none())
        for feature_id, value in ONLINE_FEATURES.evaluate_all(subject, context).items()
    }


def served(
    deliveries: list[Event], subject: Event, complete_since: EventTime | None
) -> FeatureContext:
    store = ReferenceFeatureStore(complete_since=complete_since)
    store.observe_all(deliveries)
    return store.score(subject).context


PROPERTY_SETTINGS = settings(
    max_examples=75, deadline=None, suppress_health_check=[HealthCheck.too_slow]
)


@PROPERTY_SETTINGS
@given(data=st.data())
def test_serving_in_event_time_order_is_the_event_time_complete_answer(data: st.DataObject) -> None:
    history = data.draw(histories())
    subject_tx = data.draw(latest_subjects(history))
    subject = transaction_observation(subject_tx)
    complete_since = data.draw(st.sampled_from(EPOCHS))
    in_order = served(sorted(history, key=lambda e: e.order_key), subject, complete_since)
    arrival = data.draw(st.permutations([*history, subject]))
    complete = event_time_complete_context(arrival, subject, complete_since=complete_since)
    assert features(subject_tx, in_order) == features(subject_tx, complete)


@PROPERTY_SETTINGS
@given(data=st.data())
def test_serving_up_to_any_observation_is_the_complete_answer_for_every_exact_feature(
    data: st.DataObject,
) -> None:
    """Approximate counts are excluded: the complete answer's last bucket also holds
    observations that sort after the scored transaction."""
    history = data.draw(histories())
    anchor = data.draw(st.sampled_from([e.occurred_at for e in history])) if history else T0
    subject_tx = transaction(
        occurred_at=anchor,
        transaction_id=data.draw(st.sampled_from(["e_", "e_050x", "e_999", "tx_zzz"])),
        account_id=data.draw(st.sampled_from(["acct_000001", "acct_000002"])),
        amount_minor=data.draw(st.integers(1, 5_000)),
        currency=data.draw(st.sampled_from(["GBP", "EUR"])),
        merchant_id=data.draw(st.sampled_from(["mrch_00001", "mrch_00002"])),
        device_id=data.draw(st.sampled_from(["dev_000001", "dev_000003"])),
        latitude=0.0,
        longitude=data.draw(st.sampled_from([0.0, 1.0, -179.5])),
    )
    subject = transaction_observation(subject_tx)
    complete_since = data.draw(st.sampled_from(EPOCHS))
    before = sorted(
        (e for e in history if e.order_key < subject.order_key), key=lambda e: e.order_key
    )
    arrival = data.draw(st.permutations([*history, subject]))
    exact = {s.feature_id for s in ONLINE_FEATURES if s.parity is not ParityComparison.APPROXIMATE}
    served_values = features(subject_tx, served(before, subject, complete_since))
    complete_values = features(
        subject_tx, event_time_complete_context(arrival, subject, complete_since=complete_since)
    )
    assert {k: v for k, v in served_values.items() if k in exact} == {
        k: v for k, v in complete_values.items() if k in exact
    }


@PROPERTY_SETTINGS
@given(data=st.data())
def test_redeliveries_change_nothing_in_either_mode(data: st.DataObject) -> None:
    """A redelivery keeps its id and its namespace -- anything else is a new observation."""
    history = data.draw(histories())
    subject_tx = data.draw(latest_subjects(history))
    subject = transaction_observation(subject_tx)
    complete_since = data.draw(st.sampled_from(EPOCHS))
    repeated = []
    for event in history:
        if data.draw(st.booleans()):
            stream = (
                Stream.TRANSACTION
                if event.stream is Stream.TRANSACTION
                else data.draw(st.sampled_from(IDENTITY_STREAMS))
            )
            repeated.append(data.draw(observations(event.event_id, stream=stream)))
    assert features(subject_tx, served([*history, *repeated], subject, complete_since)) == features(
        subject_tx, served(history, subject, complete_since)
    )
    assert features(
        subject_tx,
        event_time_complete_context(
            [*history, subject, *repeated], subject, complete_since=complete_since
        ),
    ) == features(
        subject_tx,
        event_time_complete_context([*history, subject], subject, complete_since=complete_since),
    )


DAY_MS: Final = 86_400_000


@st.composite
def two_eras(draw: st.DrawFn) -> list[Event]:
    """One account in GBP: an old era, a gap drawn within ten minutes either side of 30 days, and a
    recent era of 21 to 30 located transactions ending just before T0, with ties in time and place.
    A few other accounts' transactions and identity events share T0 itself."""
    gap = 30 * DAY_MS + draw(st.integers(-600_000, 600_000))
    recent_start = -draw(st.integers(DAY_MS, 3 * DAY_MS))
    points = [(0.0, 0.0), (0.0, 1.0), (0.0, 1.0), (10.0, -179.5)]

    def tx(
        event_id: str, offset: int, currency: str = "GBP", account: str = "acct_000001"
    ) -> Event:
        point = draw(st.sampled_from(points))
        return Event(
            stream=Stream.TRANSACTION,
            occurred_at=at_ms(offset),
            account_id=account,
            event_id=event_id,
            currency=currency,
            amount_minor=draw(st.integers(1, 2_000)),
            card_id="card_000001",
            device_id=draw(st.sampled_from(["dev_000001", "dev_000002"])),
            merchant_id=draw(st.sampled_from(["mrch_00001", "mrch_00002"])),
            ip_id="ip_00001",
            merchant_mcc=draw(st.sampled_from(["5411", "7995"])),
            merchant_country="GB",
            latitude=point[0],
            longitude=point[1],
            channel=TransactionChannel.CARD_PRESENT,
            authorization_outcome=AuthorizationOutcome.APPROVED,
        )

    recent_offsets = [recent_start, -1] + [
        draw(st.integers(recent_start, -1)) for _ in range(draw(st.integers(19, 28)))
    ]
    recent_offsets += recent_offsets[: draw(st.integers(0, 3))]  # ties in time
    events = [tx(f"r_{i:03d}", offset) for i, offset in enumerate(recent_offsets)]
    old_end = recent_start - gap
    old_offsets = [old_end] + [
        draw(st.integers(old_end - 2 * DAY_MS, old_end)) for _ in range(draw(st.integers(2, 7)))
    ]
    events += [tx(f"o_{i:03d}", offset) for i, offset in enumerate(old_offsets)]
    events += [
        tx(f"x_{i:03d}", draw(st.integers(recent_start, -1)), currency="EUR") for i in range(2)
    ]
    events += [tx("s_000", 0, account="acct_000002")]
    events += [
        Event(
            stream=Stream.IDENTITY_FAILED_LOGIN,
            occurred_at=at_ms(0),
            account_id="acct_000001",
            event_id="i_000",
        )
    ]
    label_case("gap under 30 days" if gap < 30 * DAY_MS else "gap of 30 days or more")
    return events


@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(data=st.data())
def test_modes_agree_where_the_profile_rules_act(data: st.DataObject) -> None:
    history = data.draw(two_eras())
    subject_tx = transaction(
        occurred_at=T0,
        transaction_id=data.draw(st.sampled_from(["r_", "r_500", "tx_zzz"])),
        account_id="acct_000001",
        amount_minor=data.draw(st.integers(1, 5_000)),
        currency="GBP",
        merchant_id=data.draw(st.sampled_from(["mrch_00001", "mrch_00002"])),
        device_id=data.draw(st.sampled_from(["dev_000001", "dev_000003"])),
        latitude=0.0,
        longitude=data.draw(st.sampled_from([0.0, 2.0])),
    )
    subject = transaction_observation(subject_tx)
    complete_since = data.draw(st.sampled_from(EPOCHS))
    before = sorted(
        (e for e in history if e.order_key < subject.order_key), key=lambda e: e.order_key
    )
    arrival = data.draw(st.permutations([*history, subject]))
    complete_values = features(
        subject_tx, event_time_complete_context(arrival, subject, complete_since=complete_since)
    )
    located_recent = sum(1 for e in history if e.event_id.startswith("r_"))
    assert located_recent > 20, "precondition: the home sample must overflow"
    assert complete_values["amount_zscore_vs_account"][0] is FeatureState.AVAILABLE, (
        "precondition: the robust z-score must be computed"
    )
    assert complete_values["distance_from_account_home_km"][0] is FeatureState.AVAILABLE
    exact = {s.feature_id for s in ONLINE_FEATURES if s.parity is not ParityComparison.APPROXIMATE}
    served_values = features(subject_tx, served(before, subject, complete_since))
    assert {k: v for k, v in served_values.items() if k in exact} == {
        k: v for k, v in complete_values.items() if k in exact
    }
