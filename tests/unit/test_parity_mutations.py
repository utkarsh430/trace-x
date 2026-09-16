"""The comparator is not vacuous: every planted divergence fails the comparison (ADR-0056 §3, §5).

A correct online store is emulated by the reference store itself, its served entries rendered by the
gateway's own `served_features`. Each mutant then changes only the implementation's answers -- never
the input the reference is given -- in exactly one declared way:
- a window edge off by one millisecond;
- a duplicate counted twice;
- a reordered pair;
- an approximate value outside the bound;
- an absent value served as 0.

The control, with nothing planted, must compare clean, so a mutant's failure is attributable to the
mutation. Gold's side is mutated the same way, arrival skew is shown to see a late arrival, and
ADR-0046 §8's declared situations are shown to excuse exactly the absences they declare.
"""

from __future__ import annotations

import datetime as dt
import json
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from typing import Final

import pytest
from eval.parity.comparator import Observed, Pairing, ParityTally
from eval.parity.driver import Applied, Posted
from eval.parity.evaluate import (
    Delivery,
    HistoryMismatchError,
    OrderEvidenceError,
    compare_as_served,
    compare_complete,
    verify_history,
)
from eval.parity.replay import CompleteHistory
from eval.parity.served import ServedScore
from eval.parity.skew import SkewTally

from trace_core.contracts.canonical import CanonicalTransaction
from trace_core.domain.time import EventTime, from_millis
from trace_core.features.context import FeatureContext
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.observation import Event
from trace_core.features.reference import ReferenceFeatureStore, event_time_complete_context
from trace_core.features.semantics import Stream
from trace_core.observation.scored_event import served_features

pytestmark = pytest.mark.parity

HOUR_MS: Final = 3_600_000
DAY_MS: Final = 24 * HOUR_MS
T0: Final = 1_786_795_200_000
VOUCHED: Final = T0 - 2 * DAY_MS
POSTED: Final = Posted(0, "tx.raw.v1", {}, None, 200, {}, Applied.SCORED, "slice")


def tx(event_id: str, at_ms: int, account: str = "acct_000001", ip: str = "ip_00001") -> Event:
    return Event(
        stream=Stream.TRANSACTION,
        occurred_at=EventTime(from_millis(at_ms)),
        account_id=account,
        event_id=event_id,
        currency="GBP",
        amount_minor=1_000,
        device_id="dev_000001",
        merchant_id="mrch_00001",
        ip_id=ip,
        merchant_mcc="5411",
        merchant_country="GB",
    )


def failed_login(event_id: str, at_ms: int) -> Event:
    return Event(
        stream=Stream.IDENTITY_FAILED_LOGIN,
        occurred_at=EventTime(from_millis(at_ms)),
        account_id="acct_000001",
        event_id=event_id,
    )


def canonical(event: Event) -> CanonicalTransaction:
    millis = event.occurred_ms
    at = f"{from_millis(millis):%Y-%m-%dT%H:%M:%S}.{millis % 1000:03d}Z"
    optional = {
        "device_id": event.device_id,
        "merchant_id": event.merchant_id,
        "ip_id": event.ip_id,
        "merchant_mcc": event.merchant_mcc,
        "merchant_country": event.merchant_country,
    }
    present = {k: v for k, v in optional.items() if v is not None}
    document = {
        "source_dataset": "gateway",
        "source_row_id": event.event_id,
        "field_coverage": [
            "transaction_id",
            "account_id",
            "amount_minor",
            "currency",
            "occurred_at",
            "ingested_at",
            *present,
        ],
        "transaction_id": event.event_id,
        "account_id": event.account_id,
        "amount_minor": event.amount_minor,
        "currency": event.currency,
        "occurred_at": at,
        "ingested_at": at,
        **present,
    }
    return CanonicalTransaction.model_validate_json(json.dumps(document))


def serve(
    log: Sequence[Event], *, epoch_ms: int | None = VOUCHED, scored: frozenset[str] | None = None
) -> list[Delivery]:
    """What a correct online store serves for `log`, in order, as the gateway publishes it.

    `scored`: the transactions scored; any other transaction is only recorded."""
    since = None if epoch_ms is None else EventTime(from_millis(epoch_ms))
    store = ReferenceFeatureStore(complete_since=since)
    deliveries: list[Delivery] = []
    for event in log:
        if event.stream is not Stream.TRANSACTION or (
            scored is not None and event.event_id not in scored
        ):
            store.observe(event)
            deliveries.append(Delivery(POSTED, event=event))
            continue
        read = store.score(event)
        transaction = canonical(event)
        values = ONLINE_FEATURES.evaluate_all(transaction, read.context)
        receipt = read.receipt
        outcome = (
            "RECORDED"
            if receipt.recorded
            else ("CONFLICT" if receipt.conflicting else "REDELIVERY")
        )
        entries = served_features(values, read.context)
        features = {str(e["feature_id"]): Observed.served(e) for e in entries}
        score = ServedScore(
            event.event_id, transaction, outcome, receipt.position, epoch_ms, (), features
        )
        deliveries.append(Delivery(POSTED, served=score))
    return deliveries


def implemented_by(truth: list[Delivery], implementation: list[Delivery]) -> list[Delivery]:
    """The true deliveries, each scored one carrying the implementation's answers instead."""
    answers: dict[str, list[dict[str, Observed]]] = defaultdict(list)
    for delivery in implementation:
        if delivery.served is not None:
            answers[delivery.served.transaction_id].append(dict(delivery.served.features))
    merged: list[Delivery] = []
    for delivery in truth:
        if delivery.served is not None:
            features = answers[delivery.served.transaction_id].pop(0)
            delivery = replace(delivery, served=replace(delivery.served, features=features))
        merged.append(delivery)
    return merged


def served_parity(deliveries: list[Delivery]) -> ParityTally:
    tally = ParityTally(Pairing.AS_SERVED)
    compare_as_served(deliveries, tally)
    assert tally.compared > 0
    return tally


def divergent_features(tally: ParityTally) -> set[str]:
    return {str(d["feature_id"]) for d in tally.divergences}


EDGE: Final = [tx("tx_a", T0), tx("tx_b", T0 + HOUR_MS)]
"""tx_a is exactly one hour older than tx_b: outside tx_b's half-open hour."""


def test_the_control_compares_clean() -> None:
    log = [*EDGE, failed_login("idev_1", T0 + HOUR_MS + 10), tx("tx_c", T0 + HOUR_MS + 20)]
    tally = served_parity(serve(log))
    assert (tally.divergent_total, tally.declared_total) == (0, 0)


def test_a_window_edge_off_by_one_millisecond_diverges() -> None:
    off_by_one = [tx("tx_a", T0 + 1), EDGE[1]]
    tally = served_parity(implemented_by(serve(EDGE), serve(off_by_one)))
    assert "account_tx_count_1h" in divergent_features(tally)


def test_a_duplicate_counted_twice_diverges() -> None:
    truth = [tx("tx_a", T0), tx("tx_a", T0), tx("tx_b", T0 + 1_000)]
    counted_twice = [tx("tx_a", T0), tx("tx_a~again", T0), tx("tx_b", T0 + 1_000)]
    renamed = [
        replace(d, served=replace(d.served, transaction_id="tx_a"))
        if d.served is not None and d.served.transaction_id == "tx_a~again"
        else d
        for d in serve(counted_twice)
    ]
    tally = served_parity(implemented_by(serve(truth), renamed))
    assert {"account_tx_count_1m", "account_tx_count_24h"} <= divergent_features(tally)


def test_a_reordered_pair_diverges() -> None:
    truth = [failed_login("idev_1", T0), tx("tx_a", T0 + 1_000)]
    reordered = [tx("tx_a", T0 + 1_000), failed_login("idev_1", T0)]
    tally = served_parity(implemented_by(serve(truth), serve(reordered)))
    assert "failed_logins_1h" in divergent_features(tally)


def _scale_approximate(deliveries: list[Delivery], factor: float) -> list[Delivery]:
    scaled: list[Delivery] = []
    for delivery in deliveries:
        if delivery.served is not None:
            features = dict(delivery.served.features)
            for feature_id in ("ip_distinct_accounts_1h", "merchant_distinct_accounts_1h"):
                value = features[feature_id]
                if value.value:
                    features[feature_id] = replace(value, value=value.value * factor)
            delivery = replace(delivery, served=replace(delivery.served, features=features))
        scaled.append(delivery)
    return scaled


def test_an_approximate_value_outside_the_bound_fails_its_stratum() -> None:
    stream = [tx(f"tx_{i}", T0 + i * 1_000, account=f"acct_{i % 40:06d}") for i in range(240)]
    truth = serve(stream)
    within = served_parity(_scale_approximate(truth, 1.005))
    assert within.divergent_total == 0 and within.approximate_failures() == []
    beyond = served_parity(_scale_approximate(truth, 1.02))
    assert beyond.divergent_total == 0
    failures = beyond.approximate_failures()
    assert any(f.startswith("ip_distinct_accounts_1h stratum 10-99") for f in failures), failures


def _absent_served(delivery: Delivery, feature_id: str, *, vouched: bool = True) -> Delivery:
    """Plant an absence. Vouched by default: an unvouched one is excluded by ADR-0046 §5's
    measurement rule before ADR-0046 §8's situations are considered (ADR-0056 §3)."""
    served = delivery.served
    assert served is not None
    absent = Observed("INSUFFICIENT_HISTORY", None, "COMPLETE" if vouched else "INCOMPLETE")
    return replace(
        delivery, served=replace(served, features={**served.features, feature_id: absent})
    )


def test_an_absent_value_served_as_zero_diverges() -> None:
    truth = serve([tx("tx_a", T0)], epoch_ms=None)
    served = truth[0].served
    assert served is not None
    absent = served.features["failed_logins_1h"]
    assert absent.value is None, "an unvouched store cannot say there were no failed logins"
    zero = Observed("AVAILABLE", 0.0, absent.lookback_completeness)
    planted = replace(served, features={**served.features, "failed_logins_1h": zero})
    tally = served_parity([replace(truth[0], served=planted)])
    assert [(d["feature_id"], d["reason"]) for d in tally.divergences] == [
        ("failed_logins_1h", "state")
    ]


def test_a_position_the_reference_cannot_reproduce_is_refused() -> None:
    truth = serve(EDGE)
    served = truth[1].served
    assert served is not None
    moved = [truth[0], replace(truth[1], served=replace(served, store_position=7))]
    with pytest.raises(OrderEvidenceError):
        served_parity(moved)


def test_an_absence_scored_ahead_of_the_wall_clock_is_a_declared_exception() -> None:
    truth = serve(EDGE)
    absent = _absent_served(truth[1], "account_tx_count_1h")
    assert absent.served is not None
    early_clock = absent.served.canonical.occurred_at - dt.timedelta(seconds=5)
    ahead = replace(
        absent,
        served=replace(
            absent.served,
            canonical=absent.served.canonical.model_copy(update={"ingested_at": early_clock}),
        ),
    )
    declared = served_parity([truth[0], ahead])
    assert (declared.divergent_total, declared.declared_total) == (0, 1)
    assert dict(declared.declared) == {"scored_ahead_of_wall_clock": 1}
    undeclared = served_parity([truth[0], absent])
    assert (undeclared.divergent_total, undeclared.declared_total) == (1, 0)


def test_a_late_read_after_a_fold_excuses_only_features_over_the_account_transactions() -> None:
    log = [
        tx("tx_old", T0 - 30 * 60_000),
        tx("tx_new", T0 + DAY_MS + 2 * HOUR_MS),
        tx("tx_late", T0),
    ]
    truth = serve(log)
    planted = _absent_served(
        _absent_served(truth[2], "account_tx_count_24h"), "ip_distinct_accounts_1h"
    )
    tally = served_parity([truth[0], truth[1], planted])
    assert dict(tally.declared) == {"late_read_after_fold": 1}
    assert divergent_features(tally) == {"ip_distinct_accounts_1h"}
    assert tally.divergences[0]["detail"]["late_by_ms"] == DAY_MS + 2 * HOUR_MS
    in_order = serve([log[0], log[2], log[1]])
    later = _absent_served(in_order[1], "account_tx_count_24h")
    assert served_parity([in_order[0], later, in_order[2]]).declared_total == 0


def test_an_unvouched_absence_is_excluded_and_counted_not_judged() -> None:
    """ADR-0046 §5 through the whole as-served path: a read the store stopped vouching for is
    excluded per feature and window, and never counted as a divergence (ADR-0056 §3)."""
    truth = serve(EDGE)
    planted = _absent_served(truth[1], "account_tx_count_1h", vouched=False)
    tally = served_parity([truth[0], planted])
    assert (tally.divergent_total, tally.declared_total) == (0, 0)
    assert tally.not_vouched_total == 1
    assert dict(tally.not_vouched) == {"account_tx_count_1h 1h": 1}
    fraction = tally.as_dict()["not_vouched_fraction"]
    assert fraction is not None and 0 < fraction < 1


@dataclass(frozen=True)
class _GoldFrom:
    """A Gold stand-in: the context an implementation computed over its own history."""

    log: Sequence[Event]
    subject: Event

    def context(self, *, complete_since: EventTime | None) -> FeatureContext:
        return event_time_complete_context(self.log, self.subject, complete_since=complete_since)


def _complete(
    truth: list[Event], gold_log: list[Event], pick: Callable[[str], Event]
) -> tuple[ParityTally, SkewTally]:
    deliveries = serve(truth)
    result = compare_as_served(deliveries, ParityTally(Pairing.AS_SERVED))
    subjects = [
        d.served
        for d in deliveries
        if d.served is not None and d.served.observe_outcome == "RECORDED"
    ]
    gold = {s.transaction_id: _GoldFrom(gold_log, pick(s.transaction_id)) for s in subjects}
    tally, skew = ParityTally(Pairing.EVENT_TIME_COMPLETE), SkewTally()
    compare_complete(
        CompleteHistory(truth),
        subjects,
        gold,
        complete_since=EventTime(from_millis(VOUCHED)),
        tally=tally,
        skew=skew,
        skew_subjects=frozenset(s.transaction_id for s in subjects),
        capped=result.capped,
    )
    return tally, skew


def _by_id(log: list[Event]) -> Callable[[str], Event]:
    return {e.event_id: e for e in log}.__getitem__


def test_gold_off_by_one_millisecond_or_counting_twice_diverges() -> None:
    control, _ = _complete(EDGE, EDGE, _by_id(EDGE))
    assert control.divergent_total == 0 and control.compared > 0
    shifted = [tx("tx_a", T0 + 1), EDGE[1]]
    edge, _ = _complete(EDGE, shifted, _by_id(shifted))
    assert "account_tx_count_1h" in divergent_features(edge)
    doubled, _ = _complete(EDGE, [*EDGE, tx("tx_a~again", T0)], _by_id(EDGE))
    assert "account_tx_count_24h" in divergent_features(doubled)


def test_arrival_skew_sees_a_late_arrival() -> None:
    late = [tx("tx_b", T0 + 60_000), tx("tx_a", T0)]
    _, skew = _complete(late, late, _by_id(late))
    fraction = skew.fraction()
    assert skew.different > 0 and fraction is not None and fraction > 0
    in_order = [late[1], late[0]]
    _, clean = _complete(in_order, in_order, _by_id(in_order))
    assert clean.different == 0 and clean.denominator > 0


def test_a_capped_read_matches_the_reference_and_is_excluded_from_arrival_skew() -> None:
    deep = [tx(f"tx_{i:04d}", T0 + i * 1_000) for i in range(520)]
    scored = frozenset({"tx_0519"})
    deliveries = serve(deep, scored=scored)
    as_served = ParityTally(Pairing.AS_SERVED)
    result = compare_as_served(deliveries, as_served)
    assert as_served.divergent_total == 0 and as_served.compared > 0
    assert ("tx_0519", "account_distinct_merchants_1h") in result.capped
    subjects = [d.served for d in deliveries if d.served is not None]
    tally, skew = ParityTally(Pairing.EVENT_TIME_COMPLETE), SkewTally()
    compare_complete(
        CompleteHistory(deep),
        subjects,
        {"tx_0519": _GoldFrom(deep, deep[-1])},
        complete_since=EventTime(from_millis(VOUCHED)),
        tally=tally,
        skew=skew,
        skew_subjects=scored,
        capped=result.capped,
    )
    assert tally.divergent_total == 0
    assert skew.capped >= 1 and skew.different == 0 and skew.denominator > 0


def test_a_complete_history_missing_a_recorded_observation_is_refused() -> None:
    log = [failed_login("idev_1", T0), tx("tx_a", T0 + 1)]
    result = compare_as_served(serve(log), ParityTally(Pairing.AS_SERVED))
    assert verify_history(result, CompleteHistory(log))["recorded_by_store"] == 2
    with pytest.raises(HistoryMismatchError):
        verify_history(result, CompleteHistory([log[1]]))
