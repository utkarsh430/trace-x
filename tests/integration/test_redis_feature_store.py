"""The Redis store's own obligations, beyond the literal fixtures (ADR-0046 §5).

The shared suite pins what each feature means on short, hand-derivable histories. It cannot reach
what makes this store different from the reference implementation: history folded into a bounded
prefix after 25 hours, identity checked across the whole store, and many clients writing at once.
Against a real Redis, never a fake (docs/TESTING.md §4).

* **Long histories agree with the reference, as served.** Seeded logs spanning months -- ties at
  one millisecond, arrivals out of order by up to the late-arrival margin, redeliveries carrying
  other payloads and other accounts, lifetime gaps on both sides of 30 days, amounts at the
  released bound -- are delivered to both, and every released feature of every scored transaction
  and of periodic read-only snapshots must agree. The one exception is a read further behind the
  newest observation than the late-arrival margin: there a feature may be absent, never different,
  where the store no longer holds the window, and the store must then stop vouching for it. Each
  run asserts that profiles were read through the folded prefix and across lifetime resets, and
  that the exception was exercised, so a store that never folds cannot pass by keeping everything
  raw.
* **Concurrent scores are serialisable in receipt order.** Threads score transactions on shared
  accounts, a card, a device and a merchant at once. Replaying the deliveries into the reference in
  the order of the positions the store returned must reproduce every context a thread was served;
  a read that interleaved with another client's write would not.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import itertools
import os
import random
import threading
from collections import Counter
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Final

import pytest
from tests.conformance.feature_semantics_suite import transaction

from trace_core.contracts.api.transaction import AMOUNT_MINOR_MAX
from trace_core.contracts.canonical import CanonicalTransaction
from trace_core.domain.enums import AuthorizationOutcome, TransactionChannel
from trace_core.domain.time import EventTime, event_time, to_millis
from trace_core.features import FeatureContext, FeatureState
from trace_core.features.context import Completeness
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.observation import (
    Event,
    ObserveReceipt,
    Verification,
    authorization_observation,
    transaction_observation,
)
from trace_core.features.reference import ReferenceFeatureStore
from trace_core.features.semantics import (
    LATE_ARRIVAL_MARGIN_S,
    PROFILE_LIFETIME_GAP_S,
    Entity,
    PairwiseWithPrevious,
    ParityComparison,
    ProfileAttribute,
    Stream,
    WindowedAggregate,
)
from trace_core.features.state_plan import PLAN
from trace_core.repositories.redis_features import LAYOUT, RedisOnlineFeatureStore

pytestmark = [pytest.mark.integration, pytest.mark.parity]

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6389"))
REDIS_DB = int(os.environ.get("REDIS_TEST_DB", "15"))
"""A database the live gateway does not use: these tests flush what they touch."""

BASE: Final = dt.datetime(2026, 1, 5, 9, 0, tzinfo=dt.UTC)
"""In the past, so every trim and fold follows event time rather than the clock."""
EPOCH: Final = event_time(BASE - dt.timedelta(days=400))
ACCOUNTS: Final = ("acct_000101", "acct_000102")
IDENTITY_STREAMS: Final = (Stream.IDENTITY_FAILED_LOGIN, Stream.IDENTITY_CHANGE)
HOUR_MS: Final = 3_600_000
DAY_MS: Final = 24 * HOUR_MS
GAP_MS: Final = PROFILE_LIFETIME_GAP_S * 1000
LATE_MS: Final = LATE_ARRIVAL_MARGIN_S * 1000
POINTS: Final = (None, (51.5, -0.12), (48.85, 2.35), (10.0, 179.5), (10.0, -179.5), (0.0, 0.0))
THREADS: Final = 8


@pytest.fixture
def redis_client() -> Iterator[Any]:
    redis = pytest.importorskip("redis", reason="the `db` extra provides the Redis client")
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    try:
        client.ping()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no Redis at {REDIS_HOST}:{REDIS_PORT} ({exc}). Run `make up` "
            f"-- this suite uses a real Redis and never a fake (docs/TESTING.md §4)."
        )
    client.flushdb()
    yield client
    client.flushdb()


@dataclass(frozen=True, slots=True)
class Delivery:
    event: Event
    subject: CanonicalTransaction | None
    """The transaction as the gateway maps it, which is scored; None for an identity event,
    which is observed."""


def _at(ms: int) -> EventTime:
    return event_time(BASE + dt.timedelta(milliseconds=ms))


def _compare(
    subject: CanonicalTransaction,
    served: FeatureContext,
    expected: FeatureContext,
    where: str,
    available: Counter[str],
    *,
    behind: Counter[str] | None = None,
) -> list[str]:
    """Every released feature, served against the reference, by its declared parity.

    APPROXIMATE features are compared exactly: no sketch here holds more than two accounts, where
    Redis's sparse HyperLogLog is exact, so a difference is a bucket-edge defect, not estimation.

    `behind` is given for a read further behind the store's newest observation than the
    late-arrival margin. There -- and only there -- a feature may be absent where the reference has
    a value, and then the store must have stopped vouching for the window; a number it does serve
    must still be the reference's (ADR-0046 §5).
    """
    problems = []
    for spec in ONLINE_FEATURES:
        got, want = spec.evaluate(subject, served), spec.evaluate(subject, expected)
        got_value, want_value = got.or_none(), want.or_none()
        if want_value is not None:
            available[spec.feature_id] += 1
        if got_value is not None and want_value is not None:
            if spec.parity is ParityComparison.FLOAT:
                agree = abs(got_value - want_value) <= max(abs(want_value) * 1e-9, 1e-9)
            else:
                agree = got_value == want_value
        else:
            agree = got_value is None and want_value is None and got.state is want.state
        if agree:
            continue
        if (
            behind is not None
            and got_value is None
            and got.state is FeatureState.INSUFFICIENT_HISTORY
        ):
            if (
                isinstance(spec.semantics, WindowedAggregate)
                and PLAN.completeness(spec.feature_id, served) is Completeness.COMPLETE
            ):
                problems.append(
                    f"{where} {spec.feature_id}: absent in a read behind the store, which still "
                    f"vouched for the window"
                )
            else:
                behind[spec.feature_id] += 1
            continue
        problems.append(
            f"{where} {spec.feature_id}: served {got.state} {got_value}, "
            f"reference {want.state} {want_value}"
        )
    return problems


def _snapshot(
    store: RedisOnlineFeatureStore | ReferenceFeatureStore, probe: CanonicalTransaction
) -> FeatureContext:
    return store.snapshot(
        as_of=event_time(probe.occurred_at),
        account_id=probe.account_id,
        currency=probe.currency,
        card_id=probe.card_id,
        device_id=probe.device_id,
        merchant_id=probe.merchant_id,
        ip_id=probe.ip_id,
    )


def _step_ms(rng: random.Random) -> int:
    roll = rng.random()
    if roll < 0.05:
        return 0  # a tie at one millisecond
    if roll < 0.15:
        return rng.randint(1, 999)
    if roll < 0.45:
        return rng.randint(1_000, 120_000)
    if roll < 0.72:
        return rng.randint(300_000, 3 * HOUR_MS)
    if roll < 0.92:
        return rng.randint(20 * HOUR_MS, 30 * HOUR_MS)  # across the 25-hour fold
    if roll < 0.96:
        return rng.choice((GAP_MS - 1, GAP_MS, GAP_MS + 1))  # both sides of a lifetime gap
    return rng.randint(2 * DAY_MS, 12 * DAY_MS)


def _transaction(
    rng: random.Random, transaction_id: str, account: str, occurred_ms: int
) -> CanonicalTransaction:
    roll = rng.random()
    if roll < 0.03:
        amount = -rng.randint(1, 5_000)
    elif roll < 0.05:
        amount = AMOUNT_MINOR_MAX  # its square needs every limb of the merchant sums
    else:
        amount = rng.randint(1, 20_000)
    point = rng.choice(POINTS)
    return transaction(
        occurred_at=_at(occurred_ms),
        transaction_id=transaction_id,
        account_id=account,
        amount_minor=amount,
        currency="EUR" if rng.random() < 0.15 else "GBP",
        merchant_id=rng.choice(("mrch_00101", "mrch_00102", "mrch_00103", "mrch_00104")),
        merchant_mcc=rng.choice(("5411", "5812")),
        merchant_country=rng.choice(("GB", "FR")),
        device_id=rng.choice((None, "dev_000101", "dev_000102", "dev_000103")),
        card_id=rng.choice(("card_000101", "card_000102")),
        ip_id=rng.choice(("ip_00101", "ip_00102")),
        latitude=None if point is None else point[0],
        longitude=None if point is None else point[1],
        channel=rng.choice((TransactionChannel.CARD_PRESENT, TransactionChannel.CARD_NOT_PRESENT)),
        authorization_outcome=rng.choice(
            (None, AuthorizationOutcome.APPROVED, AuthorizationOutcome.DECLINED)
        ),
    )


def _redelivery(rng: random.Random, original: Delivery) -> Delivery:
    """The same identity again: sometimes the same observation, usually something else. The first
    delivery is the observation (ADR-0046 §1), whatever this one says -- including another
    account."""
    if rng.random() < 0.3:
        return original  # a plain retry: the same observation again
    account = original.event.account_id
    if rng.random() < 0.5:
        account = next(a for a in ACCOUNTS if a != account)
    if original.event.stream is Stream.AUTHORIZATION_OUTCOME:
        other = (
            AuthorizationOutcome.APPROVED
            if original.event.authorization_outcome is AuthorizationOutcome.DECLINED
            else AuthorizationOutcome.DECLINED
        )
        return Delivery(
            dataclasses.replace(original.event, account_id=account, authorization_outcome=other),
            None,
        )
    if original.subject is None:
        stream = (
            Stream.IDENTITY_CHANGE
            if original.event.stream is Stream.IDENTITY_FAILED_LOGIN
            else Stream.IDENTITY_FAILED_LOGIN
        )
        return Delivery(
            dataclasses.replace(original.event, stream=stream, account_id=account), None
        )
    subject = original.subject.model_copy(
        update={
            "account_id": account,
            "amount_minor": original.subject.amount_minor + 1,
            "merchant_id": "mrch_00199",
            "occurred_at": original.subject.occurred_at
            + dt.timedelta(minutes=rng.randint(-10, 10)),
        }
    )
    return Delivery(transaction_observation(subject), subject)


def _outcome(rng: random.Random, subject: CanonicalTransaction, occurred_ms: int) -> Event:
    """An authorization outcome for `subject`, decided within seconds as DM-1 decides, now and then
    naming another account, which the store must reject."""
    account = subject.account_id
    if rng.random() < 0.05:
        account = next(a for a in ACCOUNTS if a != account)
    return authorization_observation(
        transaction_id=subject.transaction_id,
        account_id=account,
        authorization_outcome=(
            AuthorizationOutcome.DECLINED if rng.random() < 0.35 else AuthorizationOutcome.APPROVED
        ),
        decided_at=_at(occurred_ms + rng.randint(40, 2_000)),
    )


def _long_log(seed: int, count: int) -> list[Delivery]:
    """One clock for every entity, arrivals out of order by less than the late-arrival margin,
    and each redelivery sent before anything could fold its first delivery away -- the conditions
    under which the store promises the reference's answers exactly.

    Most transactions also get an authorization outcome, decided within seconds, so its transaction
    is still held whenever the outcome arrives: sometimes before its transaction (pending, then
    reconciled), sometimes naming another account (rejected), sometimes again with another outcome
    or account. Outcomes draw from their own generator, so the transactions and identity events a
    seed produced before outcomes existed are unchanged, and so is what they exercise.
    """
    rng = random.Random(seed)
    outcome_rng = random.Random(seed + 1_000_000)
    timeline: list[Delivery] = []
    outcomes: list[Delivery] = []
    ms = 0
    for i in range(count):
        ms += _step_ms(rng)
        account = rng.choice(ACCOUNTS)
        if rng.random() < 0.15:
            event = Event(
                stream=rng.choice(IDENTITY_STREAMS),
                occurred_at=_at(ms),
                account_id=account,
                event_id=f"ie_{i:05d}",
            )
            timeline.append(Delivery(event, None))
        else:
            subject = _transaction(rng, f"tx_{i:05d}", account, ms)
            timeline.append(Delivery(transaction_observation(subject), subject))
            if outcome_rng.random() < 0.7:
                outcomes.append(Delivery(_outcome(outcome_rng, subject, ms), None))
    lateness = [rng.randrange(LATE_MS) if rng.random() < 0.3 else 0 for _ in timeline]
    outcome_lateness = [
        outcome_rng.randrange(LATE_MS) if outcome_rng.random() < 0.3 else 0 for _ in outcomes
    ]
    arrivals = sorted(
        [
            (d.event.occurred_ms + late, 0, i, d)
            for i, (d, late) in enumerate(zip(timeline, lateness, strict=True))
        ]
        + [
            (d.event.occurred_ms + late, 1, i, d)
            for i, (d, late) in enumerate(zip(outcomes, outcome_lateness, strict=True))
        ],
        key=lambda arrival: arrival[:3],
    )
    log: list[Delivery] = []
    pending: list[tuple[int, Delivery, Delivery]] = []
    for delivery in (arrival[3] for arrival in arrivals):
        waiting = []
        for remaining, original, again in pending:
            elapsed = delivery.event.occurred_ms - original.event.occurred_ms
            if remaining <= 0 or elapsed >= 20 * HOUR_MS:
                log.append(again)
            else:
                waiting.append((remaining - 1, original, again))
        pending = waiting
        log.append(delivery)
        draw = outcome_rng if delivery.event.stream is Stream.AUTHORIZATION_OUTCOME else rng
        if draw.random() < 0.12:
            pending.append((draw.randint(0, 3), delivery, _redelivery(draw, delivery)))
    log.extend(again for _, _, again in pending)
    return log


@pytest.mark.parametrize("seed", [4601, 4602, 4603])
def test_long_histories_agree_with_the_reference_as_served(redis_client: Any, seed: int) -> None:
    log = _long_log(seed, count=260)
    store = RedisOnlineFeatureStore(redis_client)
    store.establish_epoch(at=EPOCH)
    reference = ReferenceFeatureStore(complete_since=EPOCH)
    first: dict[str, CanonicalTransaction] = {}
    earliest: dict[str, int] = {}
    latest_subject: CanonicalTransaction | None = None
    newest_ms = 0
    problems: list[str] = []
    reach: Counter[str] = Counter()
    available: Counter[str] = Counter()
    behind: Counter[str] = Counter()
    awaiting_transaction: dict[str, str] = {}
    for index, delivery in enumerate(log):
        event = delivery.event
        where = f"seed {seed}, delivery {index} ({event.identity})"
        newest_before, newest_ms = newest_ms, max(newest_ms, event.occurred_ms)
        if delivery.subject is None:
            got, want = store.observe(event), reference.observe(event)
            if got != want:
                problems.append(f"{where}: receipt {got}, reference {want}")
            if event.stream is Stream.AUTHORIZATION_OUTCOME and want.verification is not None:
                if not want.recorded:
                    reach["outcome redelivery"] += 1
                else:
                    reach[f"outcome {want.verification.value.lower()} on arrival"] += 1
                    if want.verification is Verification.PENDING:
                        awaiting_transaction[event.event_id] = event.account_id
        else:
            served, expected = store.score(event), reference.score(event)
            if served.receipt != expected.receipt:
                problems.append(f"{where}: receipt {served.receipt}, reference {expected.receipt}")
            subject = first.setdefault(event.identity, delivery.subject)
            if expected.receipt.recorded and event.event_id in awaiting_transaction:
                if awaiting_transaction.pop(event.event_id) == event.account_id:
                    reach["pending outcome verified by its transaction"] += 1
                else:
                    reach["pending outcome rejected by its transaction"] += 1
            if expected.receipt.recorded:
                account_earliest = earliest.get(subject.account_id, event.occurred_ms)
                earliest[subject.account_id] = min(account_earliest, event.occurred_ms)
                latest_subject = subject
            else:
                reach["transaction redelivery"] += 1
                if delivery.subject.account_id != subject.account_id:
                    reach["transaction redelivery naming another account"] += 1
                if not expected.receipt.conflicting:
                    reach["transaction redelivery carrying the same observation"] += 1
            # Evaluated for the first delivery: the context is its context (ADR-0046 §1).
            as_of_ms = to_millis(expected.context.as_of)
            is_behind = as_of_ms < newest_before - LATE_MS
            reach["read behind the late-arrival margin"] += is_behind
            problems += _compare(
                subject,
                served.context,
                expected.context,
                where,
                available,
                behind=behind if is_behind else None,
            )
            profile = expected.context.profiles.get((Entity.ACCOUNT, subject.account_id))
            if profile is not None and profile.first_seen_at is not None:
                first_seen_ms = to_millis(profile.first_seen_at)
                if first_seen_ms < to_millis(expected.context.as_of) - LAYOUT.raw_tx_ms:
                    reach["profile reaching into the folded prefix"] += 1
                if first_seen_ms > earliest[subject.account_id]:
                    reach["profile of a lifetime begun after a gap"] += 1
        if index % 20 == 19 and latest_subject is not None:
            probe = latest_subject.model_copy(
                update={"transaction_id": "tx_probe", "occurred_at": _at(newest_ms)}
            )
            problems += _compare(
                probe,
                _snapshot(store, probe),
                _snapshot(reference, probe),
                f"seed {seed}, snapshot after delivery {index}",
                available,
            )
            reach["read-only snapshot"] += 1

    assert not problems, f"{len(problems)} disagreements with the reference:\n" + "\n".join(
        problems[:40]
    )
    minimums = {
        "profile reaching into the folded prefix": 25,
        "profile of a lifetime begun after a gap": 3,
        "transaction redelivery": 5,
        "transaction redelivery naming another account": 2,
        "transaction redelivery carrying the same observation": 1,
        "read-only snapshot": 10,
        "read behind the late-arrival margin": 3,
        "outcome verified on arrival": 40,
        "outcome pending on arrival": 10,
        "outcome rejected on arrival": 2,
        "outcome redelivery": 3,
        "pending outcome verified by its transaction": 5,
    }
    unreached = {name: reach[name] for name, least in minimums.items() if reach[name] < least}
    assert not unreached, (
        f"seed {seed} no longer exercises {unreached} (reach: {dict(reach)}); a pass would say "
        f"nothing about them"
    )
    rare = {
        spec.feature_id: available[spec.feature_id]
        for spec in ONLINE_FEATURES
        if isinstance(spec.semantics, (ProfileAttribute, PairwiseWithPrevious))
        and available[spec.feature_id] < 3
    }
    assert not rare, f"profile and previous-observation features almost never available: {rare}"
    assert available["declined_ratio_1h"] >= 10, (
        f"seed {seed}: the declined ratio was almost never available "
        f"({available['declined_ratio_1h']}), so verified outcomes were barely compared"
    )
    assert behind, (
        f"seed {seed}: no read behind the margin had a window the store no longer held, so the "
        f"declared limit was never exercised"
    )
    for account in ACCOUNTS:
        assert redis_client.hget(f"f:pf:{account}", "folded_through_ms"), f"{account} never folded"


def test_concurrent_scores_are_serialisable_in_receipt_order(redis_client: Any) -> None:
    import redis

    rng = random.Random(4610)
    instants = sorted(rng.sample(range(600_000), 40))
    deliveries: list[Delivery] = []
    for i in range(320):
        ms = rng.choice(instants)
        account = rng.choice(ACCOUNTS)
        if rng.random() < 0.1:
            event = Event(
                stream=Stream.IDENTITY_FAILED_LOGIN,
                occurred_at=_at(ms),
                account_id=account,
                event_id=f"ie_{i:05d}",
            )
            deliveries.append(Delivery(event, None))
            continue
        subject = transaction(
            occurred_at=_at(ms),
            transaction_id=f"tx_{i:05d}",
            account_id=account,
            amount_minor=rng.randint(1, 20_000),
            merchant_id="mrch_00101",
            device_id="dev_000101",
            card_id="card_000101",
            ip_id="ip_00101",
        )
        deliveries.append(Delivery(transaction_observation(subject), subject))
    originals = [d for d in deliveries if d.subject is not None]
    # Sent by other threads, so a "redelivery" may well be recorded before its original.
    deliveries += [_redelivery(rng, d) for d in rng.sample(originals, 30)]
    rng.shuffle(deliveries)

    clients = [
        redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        for _ in range(THREADS)
    ]
    stores = [RedisOnlineFeatureStore(client) for client in clients]
    stores[0].establish_epoch(at=EPOCH)
    start = threading.Barrier(THREADS)

    def work(worker: int) -> list[tuple[int, Delivery, ObserveReceipt, FeatureContext | None]]:
        start.wait()
        done: list[tuple[int, Delivery, ObserveReceipt, FeatureContext | None]] = []
        for delivery in deliveries[worker::THREADS]:
            if delivery.subject is None:
                done.append((worker, delivery, stores[worker].observe(delivery.event), None))
            else:
                served = stores[worker].score(delivery.event)
                done.append((worker, delivery, served.receipt, served.context))
        return done

    try:
        with ThreadPoolExecutor(THREADS) as pool:
            results = [item for chunk in pool.map(work, range(THREADS)) for item in chunk]
    finally:
        for client in clients:
            client.close()

    ordered = sorted(results, key=lambda r: (r[2].position, not r[2].recorded))
    positions = [receipt.position for _, _, receipt, _ in ordered if receipt.recorded]
    assert positions == list(range(1, len(positions) + 1)), "a position was skipped or repeated"
    switches = sum(1 for a, b in itertools.pairwise(ordered) if a[0] != b[0])
    assert switches >= len(ordered) // 2, (
        f"threads interleaved only {switches} times in {len(ordered)} deliveries; this run did "
        f"not exercise concurrency"
    )

    reference = ReferenceFeatureStore(complete_since=EPOCH)
    first: dict[str, CanonicalTransaction] = {}
    problems: list[str] = []
    for worker, delivery, receipt, context in ordered:
        where = f"position {receipt.position} ({delivery.event.identity}, thread {worker})"
        if delivery.subject is None or context is None:
            if (expected_receipt := reference.observe(delivery.event)) != receipt:
                problems.append(f"{where}: receipt {receipt}, reference {expected_receipt}")
            continue
        expected = reference.score(delivery.event)
        if expected.receipt != receipt:
            problems.append(f"{where}: receipt {receipt}, reference {expected.receipt}")
        subject = first.setdefault(delivery.event.identity, delivery.subject)
        problems += _compare(subject, context, expected.context, where, Counter())
    assert not problems, (
        f"{len(problems)} served contexts are not the reference replayed in receipt order:\n"
        + "\n".join(problems[:40])
    )


def test_a_read_behind_what_the_store_holds_is_absent_not_a_lower_bound(redis_client: Any) -> None:
    """A redelivery is read at its first delivery's time (ADR-0046 §1). Three hours on, the card's
    members from then have been trimmed, and the count left behind -- zero -- would be a believable
    lower bound. The window must be absent and no longer vouched for; what the store still holds
    in full is still served."""
    store = RedisOnlineFeatureStore(redis_client)
    store.establish_epoch(at=EPOCH)
    reference = ReferenceFeatureStore(complete_since=EPOCH)
    first, second, later = (
        transaction(
            occurred_at=_at(ms),
            transaction_id=name,
            card_id="card_000101",
            device_id="dev_000101",
            merchant_id="mrch_00101",
            ip_id="ip_00101",
        )
        for ms, name in ((0, "tx_first"), (60_000, "tx_second"), (3 * HOUR_MS, "tx_later"))
    )
    for subject in (first, second, later):
        store.score(transaction_observation(subject))
        reference.score(transaction_observation(subject))

    again = transaction_observation(second.model_copy(update={"amount_minor": 1}))
    served, expected = store.score(again), reference.score(again)
    assert served.receipt == expected.receipt
    assert not served.receipt.recorded

    card = ONLINE_FEATURES.get("card_tx_count_5m")
    assert card.evaluate(second, expected.context).or_none() == 2.0
    got = card.evaluate(second, served.context)
    assert got.or_none() is None and got.state is FeatureState.INSUFFICIENT_HISTORY, (
        f"served {got.state} {got.or_none()} for a window the store no longer holds"
    )
    assert PLAN.completeness("card_tx_count_5m", served.context) is Completeness.INCOMPLETE

    # Per window, not per lookback length: the account's windows are held in full, so they are
    # served and still vouched for (ADR-0046 §5).
    hourly = ONLINE_FEATURES.get("account_tx_count_1h")
    assert hourly.evaluate(second, served.context).or_none() == 2.0
    assert hourly.evaluate(second, expected.context).or_none() == 2.0
    for feature_id in ("account_tx_count_5m", "account_tx_count_1h", "failed_logins_1h"):
        assert PLAN.completeness(feature_id, served.context) is Completeness.COMPLETE, feature_id
    logins = ONLINE_FEATURES.get("failed_logins_1h").evaluate(second, served.context)
    assert logins.or_none() == 0.0, f"served {logins.state} for a held, empty window"
