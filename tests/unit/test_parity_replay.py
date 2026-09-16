"""The indexed reference equals the unmodified reference, read for read (ADR-0056 §2).

`eval.parity.replay` evaluates the reference's own `build_context` over a subset of observations.
The subset is only an optimisation if it changes nothing, so these streams are built to reach every
place it could: shared devices, merchants and IPs across accounts, observations older than the
non-account horizon, redeliveries, conflicting redeliveries, pending and cross-account authorization
outcomes, identity events, out-of-order arrival, and an account deep enough to be capped.
"""

from __future__ import annotations

import random
from dataclasses import replace
from typing import Final

import pytest
from eval.parity.replay import NON_ACCOUNT_HORIZON_MS, AsServedReplay, CompleteHistory

from trace_core.domain.enums import AuthorizationOutcome, TransactionChannel
from trace_core.domain.time import EventTime, from_millis
from trace_core.features.observation import Event
from trace_core.features.reference import ReferenceFeatureStore, event_time_complete_context
from trace_core.features.semantics import Stream

pytestmark = pytest.mark.parity

BASE_MS: Final = 1_786_795_200_000
SEEDS: Final = range(24)


def _stream(seed: int) -> list[Event]:
    rng = random.Random(seed)
    accounts = [f"acct_{i:06d}" for i in range(3)]
    transactions: list[Event] = []
    events: list[Event] = []
    for index in range(80):
        cluster = rng.random() < 0.5
        at = BASE_MS + (
            rng.randrange(0, 7_200_000) if cluster else rng.randrange(0, 3 * 86_400_000)
        )
        account = rng.choice(accounts)
        roll = rng.random()
        if roll < 0.6:
            located = rng.random() < 0.8
            event = Event(
                stream=Stream.TRANSACTION,
                occurred_at=EventTime(from_millis(at)),
                account_id=account,
                event_id=f"tx_{index}",
                currency=rng.choice(["GBP", "GBP", "EUR"]),
                amount_minor=rng.choice([500, 500, 1_250, 90_000]),
                card_id=rng.choice([None, f"card_{account[-6:]}"]),
                device_id=rng.choice(["dev_000001", "dev_000002", None]),
                merchant_id=rng.choice(["mrch_00001", "mrch_00002", "mrch_00003"]),
                ip_id=rng.choice(["ip_00001", "ip_00002"]),
                merchant_mcc=rng.choice(["5411", "5812"]),
                merchant_country=rng.choice(["GB", "FR"]),
                latitude=rng.uniform(50, 52) if located else None,
                longitude=rng.uniform(-1, 1) if located else None,
                channel=rng.choice(list(TransactionChannel)),
            )
            transactions.append(event)
        elif roll < 0.8:
            event = Event(
                stream=rng.choice([Stream.IDENTITY_FAILED_LOGIN, Stream.IDENTITY_CHANGE]),
                occurred_at=EventTime(from_millis(at)),
                account_id=account,
                event_id=f"idev_{index}",
                device_id=rng.choice(["dev_000001", None]),
                ip_id=rng.choice(["ip_00001", None]),
            )
        else:
            target = rng.choice(transactions) if transactions and rng.random() < 0.7 else None
            event = Event(
                stream=Stream.AUTHORIZATION_OUTCOME,
                occurred_at=EventTime(from_millis(at)),
                account_id=target.account_id if target and rng.random() < 0.8 else account,
                event_id=target.event_id if target else f"tx_{index + 1_000}",
                authorization_outcome=rng.choice(
                    [AuthorizationOutcome.APPROVED, AuthorizationOutcome.DECLINED]
                ),
            )
        events.append(event)
    for event in rng.sample(events, 10):
        events.insert(rng.randrange(len(events) + 1), event)
    for event in rng.sample([e for e in events if e.stream is Stream.TRANSACTION], 3):
        conflicting = replace(event, amount_minor=event.amount_minor + 1)
        events.insert(rng.randrange(len(events) + 1), conflicting)
    return events


def test_the_streams_reach_beyond_the_non_account_horizon() -> None:
    spans = [
        max(e.occurred_ms for e in _stream(s)) - min(e.occurred_ms for e in _stream(s))
        for s in SEEDS
    ]
    assert min(spans) > NON_ACCOUNT_HORIZON_MS, "a stream that never trims proves nothing about it"


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("vouched", [True, False])
def test_as_served_reads_equal_the_reference_store(seed: int, vouched: bool) -> None:
    since = EventTime(from_millis(BASE_MS - 86_400_000)) if vouched else None
    reference = ReferenceFeatureStore(complete_since=since)
    replay = AsServedReplay()
    scored = 0
    for event in _stream(seed):
        if event.stream is Stream.TRANSACTION:
            expected = reference.score(event)
            receipt, context = replay.score(event, complete_since=since)
            assert receipt == expected.receipt
            assert context == expected.context, f"seed {seed}: {event.identity}"
            scored += 1
        else:
            assert replay.observe(event) == reference.observe(event)
    assert scored > 0
    assert set(replay.recorded) == {e.identity for e in reference.events}


@pytest.mark.parametrize("seed", SEEDS)
def test_event_time_complete_contexts_equal_the_reference(seed: int) -> None:
    stream = _stream(seed)
    history = CompleteHistory(stream)
    since = EventTime(from_millis(BASE_MS))
    subjects = [e for e in history.observations.values() if e.stream is Stream.TRANSACTION]
    assert subjects
    for subject in subjects:
        expected = event_time_complete_context(stream, subject, complete_since=since)
        assert history.context(subject.identity, complete_since=since) == expected


def test_a_subject_outside_the_history_is_refused() -> None:
    with pytest.raises(KeyError):
        CompleteHistory([]).context("transaction:tx_missing", complete_since=None)


def test_capped_as_served_reads_equal_the_reference_store() -> None:
    """An account deeper than `SCORE_READ_CAP` within its raw history, beside another account."""
    since = EventTime(from_millis(BASE_MS - 86_400_000))
    reference = ReferenceFeatureStore(complete_since=since)
    replay = AsServedReplay()
    events = [
        Event(
            stream=Stream.TRANSACTION,
            occurred_at=EventTime(from_millis(BASE_MS + index * 700)),
            account_id="acct_000001" if index % 11 else "acct_000002",
            event_id=f"tx_{index}",
            currency="GBP",
            amount_minor=100 + index % 7,
            device_id=f"dev_00000{index % 3}",
            merchant_id=f"mrch_0000{index % 5}",
            ip_id="ip_00001",
            merchant_mcc="5411",
            latitude=51.0 + index % 4 / 10,
            longitude=0.1,
        )
        for index in range(600)
    ]
    capped = 0
    for index, event in enumerate(events):
        if index < 590:
            assert replay.observe(event) == reference.observe(event)
            continue
        expected = reference.score(event)
        receipt, context = replay.score(event, complete_since=since)
        assert (receipt, context) == (expected.receipt, expected.context)
        capped += sum(1 for state in context.windows.values() if state.content_capped)
    assert capped > 0, "no read was capped, so the cap's equivalence was not exercised"
