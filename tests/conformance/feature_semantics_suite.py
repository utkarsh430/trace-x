"""The shared suite every feature implementation must pass, with literal fixtures as the oracle.

**What this is for.** ADR-0002 accepts that the same feature is computed more than once -- from
Redis on the hot path, and offline from the durable history -- and names silent divergence as the
risk. This file states the declared meaning (ADR-0032, ADR-0046) as *observations in, feature
values out*, with no reference to how any implementation works.

**The literals are the oracle; agreement is not.** Every expected number below is derived by hand
from the declaration and written as its arithmetic, so a reviewer can check it without running
anything. Two implementations that share a defect agree with each other; Phase 3 planning found
exactly that (the coefficient of variation divided same-currency sums by an all-currency count in
both). They cannot both agree with a literal that was derived independently.

**Two evaluation modes** (`EvaluationMode`, ADR-0046 §1). Every implementation is given the same
thing: the log of deliveries in the order they arrived, and the position in it of the scored
transaction's delivery.

* `AsServedConformanceSuite` -- what an online store serves. Observations delivered after the
  scored transaction cannot contribute. The Redis store and the reference store run it; Phase 3's
  as-served replay runs it too, which is what forbids that replay from being a range over
  `occurred_at`.
* `EventTimeCompleteConformanceSuite` -- what a complete history says. Arrival order is
  irrelevant except for which delivery of an identity came first; ties at the scored
  transaction's millisecond break by identity. Phase 3's Gold runs it.

Fixtures whose answer is the same in both modes live on the shared base class. Fixtures whose
answer depends on the mode live on the mode's class, and the pair is written side by side so the
difference is visible.

Phase 3 subclasses these classes **unmodified**. If proving parity required editing the suite, the
suite would be describing whatever the implementations happen to do rather than what the features
mean.
"""

from __future__ import annotations

import datetime as dt
import math
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

from trace_core.contracts.canonical import ALWAYS_REQUIRED, CanonicalField, CanonicalTransaction
from trace_core.domain.enums import AuthorizationOutcome, TransactionChannel
from trace_core.domain.time import EventTime, event_time
from trace_core.features import FeatureContext, FeatureState
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.observation import (
    Event,
    authorization_observation,
    transaction_observation,
)
from trace_core.features.semantics import ParityComparison, Stream

T0: Final = event_time(dt.datetime(2026, 3, 1, 12, 0, 0, tzinfo=dt.UTC))
"""A whole minute and a whole five-minute bucket, so bucket edges below are easy to read."""

WATCHED_LONG_ENOUGH: Final = event_time(T0 - dt.timedelta(days=90))
"""A store recording for longer than any feature looks back, and than a 30-day lifetime gap."""

WATCHED_ONE_HOUR: Final = event_time(T0 - dt.timedelta(hours=1, seconds=1))
"""Complete for the 1 m, 5 m and 1 h windows; incomplete for 24 h and the profile horizon."""

FULL_COVERAGE: Final = frozenset(CanonicalField)
ACCOUNT: Final = "acct_000001"
OTHER_ACCOUNT: Final = "acct_000002"
DEVICE: Final = "dev_000001"
MERCHANT: Final = "mrch_00001"
CARD: Final = "card_000001"
IP: Final = "ip_00001"
SUBJECT: Final = "tx_mmm"
"""The scored transaction's identity. Chosen to sort between `tx_a...` and `tx_z...`, so
tie-break fixtures can place observations on either side of it."""

EXACT: Final = 0.0
FLOAT_ABSOLUTE_FLOOR: Final = 1e-9
"""A relative tolerance on an expected zero is no tolerance at all; a FLOAT feature expected to be
exactly 0 is compared within this absolute margin instead."""
FLOAT: Final = 1e-9
"""Relative tolerance for values that pass through floating-point arithmetic (a square root,
a division, a trigonometric distance). Arithmetic, not estimation (ADR-0046 §5)."""

EARTH_RADIUS_KM: Final = 6371.0088
"""The IUGG mean radius ADR-0046 declares. Written here, not imported: if the declared radius
changed, these fixtures must fail rather than follow."""


def along_equator_km(degrees: float) -> float:
    """Great-circle distance between two equatorial points `degrees` of longitude apart.

    On the equator the haversine formula reduces to R * |delta longitude| in radians, which is
    what makes the geographic fixtures below derivable by hand.
    """
    return EARTH_RADIUS_KM * degrees * math.pi / 180.0


def at(seconds: float) -> EventTime:
    """An event time offset from T0 in seconds. Negative is in the past."""
    return event_time(T0 + dt.timedelta(seconds=seconds))


def at_ms(milliseconds: int, *, microseconds: int = 0) -> EventTime:
    return event_time(
        T0 + dt.timedelta(milliseconds=milliseconds) + dt.timedelta(microseconds=microseconds)
    )


def tx_event(
    event_id: str,
    *,
    occurred_at: EventTime,
    account_id: str = ACCOUNT,
    amount_minor: int = 5_000,
    currency: str = "GBP",
    merchant_id: str | None = MERCHANT,
    merchant_mcc: str | None = "5411",
    merchant_country: str | None = "GB",
    device_id: str | None = DEVICE,
    card_id: str | None = CARD,
    ip_id: str | None = IP,
    latitude: float | None = 51.5,
    longitude: float | None = -0.12,
    channel: TransactionChannel | None = TransactionChannel.CARD_PRESENT,
    authorization_outcome: AuthorizationOutcome | None = AuthorizationOutcome.APPROVED,
) -> Event:
    return Event(
        stream=Stream.TRANSACTION,
        occurred_at=occurred_at,
        account_id=account_id,
        event_id=event_id,
        currency=currency,
        amount_minor=amount_minor,
        card_id=card_id,
        device_id=device_id,
        merchant_id=merchant_id,
        ip_id=ip_id,
        merchant_mcc=merchant_mcc,
        merchant_country=merchant_country,
        latitude=latitude,
        longitude=longitude,
        channel=channel,
        authorization_outcome=authorization_outcome,
    )


def identity_event(
    event_id: str, *, stream: Stream, occurred_at: EventTime, account_id: str = ACCOUNT
) -> Event:
    return Event(stream=stream, occurred_at=occurred_at, account_id=account_id, event_id=event_id)


def outcome_event(
    transaction_id: str,
    *,
    decided_at: EventTime,
    outcome: AuthorizationOutcome = AuthorizationOutcome.DECLINED,
    account_id: str = ACCOUNT,
) -> Event:
    """An authorization outcome for `transaction_id`, decided at `decided_at` (ADR-0049)."""
    return authorization_observation(
        transaction_id=transaction_id,
        account_id=account_id,
        authorization_outcome=outcome,
        decided_at=decided_at,
    )


def transaction(
    *,
    occurred_at: EventTime = T0,
    transaction_id: str = SUBJECT,
    account_id: str = ACCOUNT,
    amount_minor: int = 5_000,
    currency: str = "GBP",
    coverage: frozenset[CanonicalField] = FULL_COVERAGE,
    **over: object,
) -> CanonicalTransaction:
    """The transaction being scored, as the gateway maps it."""
    fields: dict[str, object] = {
        "merchant_id": MERCHANT,
        "merchant_mcc": "5411",
        "merchant_country": "GB",
        "device_id": DEVICE,
        "card_id": CARD,
        "ip_id": IP,
        "latitude": 51.5,
        "longitude": -0.12,
        "channel": TransactionChannel.CARD_PRESENT,
        "entry_mode": None,
        "authorization_outcome": AuthorizationOutcome.APPROVED,
        "merchant_name": None,
        "user_agent": None,
        "memo": None,
    }
    fields.update(over)
    fields = {k: v for k, v in fields.items() if CanonicalField(k) in coverage or v is None}
    return CanonicalTransaction(
        source_dataset="conformance",
        source_row_id=transaction_id,
        field_coverage=coverage,
        transaction_id=transaction_id,
        account_id=account_id,
        amount_minor=amount_minor,
        currency=currency,
        occurred_at=occurred_at,
        ingested_at=occurred_at + dt.timedelta(milliseconds=40),
        **fields,  # type: ignore[arg-type]
    )


def located(event_id: str, *, hours_ago: float, longitude: float, latitude: float = 0.0) -> Event:
    return tx_event(
        event_id, occurred_at=at(-3_600 * hours_ago), latitude=latitude, longitude=longitude
    )


def unlocated(event_id: str, *, hours_ago: float) -> Event:
    return tx_event(event_id, occurred_at=at(-3_600 * hours_ago), latitude=None, longitude=None)


@dataclass(frozen=True, slots=True)
class Expectation:
    """One feature's expected outcome, with the tolerance it is judged at."""

    feature_id: str
    value: float | None
    """`None` means the feature must be absent -- never 0.0, which is the failure ADR-0022
    exists to prevent."""
    tolerance: float = EXACT
    state: FeatureState | None = None


INSUFFICIENT: Final = FeatureState.INSUFFICIENT_HISTORY

FAILURE_HEADER: Final = "feature expectations failed:"
FAILURE_BULLET: Final = "  - "


def failed_feature_ids(error: AssertionError) -> frozenset[str]:
    """The features a `check_log` failure names; empty for any other assertion."""
    text = str(error)
    if not text.startswith(FAILURE_HEADER):
        return frozenset()
    return frozenset(
        line[len(FAILURE_BULLET) :].split(":", 1)[0]
        for line in text.splitlines()[1:]
        if line.startswith(FAILURE_BULLET)
    )


def metres_east(metres: float) -> float:
    """The longitude, in degrees, of an equatorial point `metres` east of longitude 0."""
    return metres / (EARTH_RADIUS_KM * 1_000.0) * 180.0 / math.pi


class FeatureSemanticsConformanceSuite(ABC):
    """Fixtures whose answer is the same in both evaluation modes."""

    @abstractmethod
    def context_for(
        self,
        log: Sequence[Event],
        subject_index: int,
        subject: CanonicalTransaction,
        *,
        complete_since: EventTime | None,
    ) -> FeatureContext:
        """The context the implementation produces for the scored transaction.

        `log` is every delivery in the order it arrived, possibly with redeliveries;
        `log[subject_index]` is the delivery being scored. `complete_since` is when the store
        began recording, or None for a store that makes no completeness claim (ADR-0044).
        """

    # -- harness ------------------------------------------------------------

    def check_log(
        self,
        log: Sequence[Event],
        subject_index: int,
        subject: CanonicalTransaction,
        expectations: Sequence[Expectation],
        *,
        complete_since: EventTime | None = None,
    ) -> None:
        assert expectations, "a fixture that asserts nothing passes whatever the implementation"
        assert log[subject_index] == transaction_observation(subject), (
            "fixture bug: the delivery at subject_index is not the scored transaction"
        )
        context = self.context_for(log, subject_index, subject, complete_since=complete_since)
        failures: list[str] = []
        for expected in expectations:
            spec = ONLINE_FEATURES.get(expected.feature_id)
            assert expected.tolerance <= FLOAT, "fixture bug: a tolerance wider than arithmetic"
            assert spec.parity is not ParityComparison.EXACT or expected.tolerance == EXACT, (
                f"fixture bug: {expected.feature_id} is declared EXACT but given a tolerance"
            )
            actual = spec.evaluate(subject, context)
            if expected.value is None:
                if actual.is_available:
                    failures.append(
                        f"{expected.feature_id}: expected no value, got {actual.or_none()}. "
                        f"An absent feature must never be reported as a number (ADR-0022)."
                    )
                elif expected.state is not None and actual.state is not expected.state:
                    failures.append(
                        f"{expected.feature_id}: expected {expected.state}, got {actual.state}"
                    )
                continue
            if not actual.is_available:
                failures.append(
                    f"{expected.feature_id}: expected {expected.value}, got {actual.state}"
                )
                continue
            allowed = (
                max(abs(expected.value) * FLOAT, FLOAT_ABSOLUTE_FLOOR)
                if spec.parity is ParityComparison.FLOAT
                else abs(expected.value) * expected.tolerance
            )
            if abs(actual.value - expected.value) > allowed:
                failures.append(
                    f"{expected.feature_id}: expected {expected.value} +/- {allowed}, "
                    f"got {actual.value}"
                )
        if failures:
            raise AssertionError(
                FAILURE_HEADER + "".join(f"\n{FAILURE_BULLET}{line}" for line in failures)
            )

    def check(
        self,
        history: Sequence[Event],
        subject: CanonicalTransaction,
        expectations: Sequence[Expectation],
        *,
        complete_since: EventTime | None = None,
    ) -> None:
        """`history` delivered in order, then the scored transaction."""
        log = [*history, transaction_observation(subject)]
        self.check_log(log, len(history), subject, expectations, complete_since=complete_since)

    # -- transactional windows include the scored transaction (ADR-0046 §2) ---

    def test_the_scored_transaction_counts_in_its_own_windows(self) -> None:
        history = [
            tx_event("tx_h1", occurred_at=at(-30)),
            tx_event("tx_h2", occurred_at=at(-120)),
            tx_event("tx_h3", occurred_at=at(-1_800)),
            tx_event("tx_h4", occurred_at=at(-40_000)),
        ]
        self.check(
            history,
            transaction(),
            [
                Expectation("account_tx_count_1m", 1 + 1),  # h1 + scored
                Expectation("account_tx_count_5m", 2 + 1),  # h1, h2 + scored
                Expectation("account_tx_count_1h", 3 + 1),  # h1..h3 + scored
                Expectation("account_tx_count_24h", 4 + 1),  # h1..h4 + scored
                Expectation("card_tx_count_5m", 2 + 1),
            ],
        )

    def test_an_observation_exactly_a_window_old_is_outside_and_a_millisecond_younger_inside(
        self,
    ) -> None:
        history = [
            tx_event("tx_edge", occurred_at=at_ms(-60_000)),
            tx_event("tx_inside", occurred_at=at_ms(-59_999)),
        ]
        self.check(
            history,
            transaction(),
            [
                Expectation("account_tx_count_1m", 1 + 1),  # inside + scored; edge excluded
                Expectation("account_tx_count_5m", 2 + 1),
            ],
        )

    def test_event_time_is_floored_to_the_millisecond_before_any_comparison(self) -> None:
        """Scored at T0 + 0.9 ms, which floors to T0.

        `tx_floor` at T0 - 59 999.05 ms floors to T0 - 60 000 ms: exactly a minute old, outside.
        Compared unfloored it would be 59 999.95 ms old and inside. `tx_keep` at
        T0 - 59 999 ms is inside either way.
        """
        history = [
            tx_event("tx_floor", occurred_at=at_ms(-59_999, microseconds=-50)),
            tx_event("tx_keep", occurred_at=at_ms(-59_999)),
        ]
        self.check(
            history,
            transaction(occurred_at=at_ms(0, microseconds=900)),
            [Expectation("account_tx_count_1m", 1 + 1)],  # keep + scored
        )

    def test_arrival_order_among_earlier_observations_does_not_change_the_answer(self) -> None:
        events = [
            tx_event(f"tx_o{i}", occurred_at=at(-offset))
            for i, offset in enumerate((10, 200, 40, 100, 25))
        ]
        expectations = [
            Expectation("account_tx_count_1m", 3 + 1),  # -10, -40, -25 + scored
            Expectation("account_tx_count_5m", 5 + 1),
        ]
        self.check(events, transaction(), expectations)
        self.check(list(reversed(events)), transaction(), expectations)

    def test_an_older_observation_is_ignored_only_by_event_time_not_by_arrival(self) -> None:
        """A later-dated observation delivered first must not hide an earlier one."""
        history = [
            tx_event("tx_future", occurred_at=at(7_200)),
            tx_event("tx_now", occurred_at=at(-30)),
        ]
        self.check(
            history,
            transaction(),
            [
                Expectation("account_tx_count_1m", 1 + 1),  # now + scored; future excluded
                Expectation("account_tx_count_24h", 1 + 1),
            ],
        )

    def test_a_future_dated_observation_delivered_last_evicts_nothing(self) -> None:
        """Retention trims must never be driven by a future-dated event (ADR-0046 §5)."""
        history = [
            tx_event("tx_now", occurred_at=at(-30)),
            tx_event("tx_future", occurred_at=at(7_200)),
        ]
        self.check(
            history,
            transaction(),
            [Expectation("account_tx_count_1m", 1 + 1), Expectation("account_tx_count_5m", 1 + 1)],
        )

    # -- identity and redelivery (ADR-0046 §1) -------------------------------

    def test_a_redelivery_counts_once_and_the_first_delivery_is_the_observation(self) -> None:
        """The second delivery of `tx_dup` carries a different time and amount; neither counts."""
        history = [
            tx_event("tx_dup", occurred_at=at(-100), amount_minor=1_000),
            tx_event("tx_dup", occurred_at=at(-50), amount_minor=9_000),
        ]
        self.check(
            history,
            transaction(),
            [
                Expectation("account_tx_count_1m", 0 + 1),  # the first delivery is 100 s old
                Expectation("account_tx_count_5m", 1 + 1),
                Expectation("account_amount_sum_1h", 1_000 + 5_000),
            ],
        )

    def test_a_redelivered_scored_transaction_counts_once(self) -> None:
        subject = transaction()
        log = [
            transaction_observation(subject),
            tx_event("tx_between", occurred_at=at(-10)),
            transaction_observation(subject),
        ]
        self.check_log(
            log,
            2,
            subject,
            [
                Expectation("account_tx_count_1m", 1 + 1),  # between + scored, once
                Expectation("account_amount_sum_1h", 5_000 + 5_000),
            ],
        )

    def test_a_redelivered_identity_event_counts_once(self) -> None:
        login = identity_event("evt_fl1", stream=Stream.IDENTITY_FAILED_LOGIN, occurred_at=at(-60))
        self.check([login, login], transaction(), [Expectation("failed_logins_1h", 1)])

    # -- amounts, outcomes and currency ------------------------------------

    def test_the_account_amount_sum_is_the_scored_currency_and_includes_the_scored_amount(
        self,
    ) -> None:
        history = [
            tx_event("tx_gbp", occurred_at=at(-100), amount_minor=1_000, currency="GBP"),
            tx_event("tx_eur", occurred_at=at(-200), amount_minor=9_999, currency="EUR"),
        ]
        self.check(
            history,
            transaction(currency="GBP", amount_minor=5_000),
            [Expectation("account_amount_sum_1h", 1_000 + 5_000)],
        )
        self.check(
            history,
            transaction(currency="EUR", amount_minor=700),
            [Expectation("account_amount_sum_1h", 9_999 + 700)],
        )

    # -- the declined ratio: verified outcomes known before the score (ADR-0049 §5, §8) --------

    def test_the_declined_ratio_never_reads_the_scored_transactions_own_outcome(self) -> None:
        """F1. The scored transaction's own outcome is recorded before it is scored -- at its own
        millisecond, and in a variant dated before it. Counted, the ratio would read 2/3."""
        for own_decided_at in (T0, at_ms(-5)):
            history = [
                tx_event("tx_d1", occurred_at=at(-30), authorization_outcome=None),
                tx_event("tx_a1", occurred_at=at(-25), authorization_outcome=None),
                outcome_event("tx_d1", decided_at=at_ms(-19_700)),
                outcome_event("tx_a1", decided_at=at(-10), outcome=AuthorizationOutcome.APPROVED),
                outcome_event(SUBJECT, decided_at=own_decided_at),
            ]
            self.check(
                history,
                transaction(authorization_outcome=None),
                [Expectation("declined_ratio_1h", 1 / 2, tolerance=FLOAT)],
            )

    def test_an_outcome_known_before_a_later_score_counts(self) -> None:
        """F2."""
        history = [
            tx_event("tx_s0", occurred_at=at(-10), authorization_outcome=None),
            outcome_event("tx_s0", decided_at=at_ms(-9_700)),
        ]
        self.check(history, transaction(), [Expectation("declined_ratio_1h", 1.0, tolerance=FLOAT)])

    def test_a_duplicate_outcome_counts_once(self) -> None:
        """F4. A redelivery carries the same outcome and contributes nothing."""
        history = [
            tx_event("tx_d1", occurred_at=at(-30), authorization_outcome=None),
            tx_event("tx_a1", occurred_at=at(-25), authorization_outcome=None),
            outcome_event("tx_d1", decided_at=at(-20)),
            outcome_event("tx_d1", decided_at=at(-20)),
            outcome_event("tx_a1", decided_at=at(-15), outcome=AuthorizationOutcome.APPROVED),
        ]
        self.check(
            history, transaction(), [Expectation("declined_ratio_1h", 1 / 2, tolerance=FLOAT)]
        )

    def test_a_conflicting_outcome_never_replaces_the_first(self) -> None:
        """F5. The first delivery stays the observation, so the ratio reads the approval."""
        history = [
            tx_event("tx_x1", occurred_at=at(-30), authorization_outcome=None),
            outcome_event("tx_x1", decided_at=at(-20), outcome=AuthorizationOutcome.APPROVED),
            outcome_event("tx_x1", decided_at=at(-20), outcome=AuthorizationOutcome.DECLINED),
        ]
        self.check(history, transaction(), [Expectation("declined_ratio_1h", 0.0, tolerance=FLOAT)])

    def test_an_outcome_decided_at_the_scored_millisecond_is_not_yet_known(self) -> None:
        """F7. Neither the scored transaction's own outcome at T nor another's counts, in either
        recording order."""
        subject = transaction()
        known = tx_event("tx_y1", occurred_at=at(-5), authorization_outcome=None)
        theirs = outcome_event("tx_y1", decided_at=T0)
        own = outcome_event(SUBJECT, decided_at=T0)
        self.check(
            [known, theirs, own],
            subject,
            [Expectation("declined_ratio_1h", None, state=INSUFFICIENT)],
        )
        log = [known, transaction_observation(subject), theirs, own]
        self.check_log(
            log, 1, subject, [Expectation("declined_ratio_1h", None, state=INSUFFICIENT)]
        )

    def test_the_outcome_window_is_open_at_both_edges(self) -> None:
        """F8. Decided exactly a window before is outside, a millisecond later is inside, and at
        the scored millisecond is not yet known: only the declined outcome counts."""
        history = [
            tx_event("tx_e1", occurred_at=at(-7_200), authorization_outcome=None),
            tx_event("tx_e2", occurred_at=at(-7_100), authorization_outcome=None),
            tx_event("tx_e3", occurred_at=at(-10), authorization_outcome=None),
            outcome_event(
                "tx_e1", decided_at=at_ms(-3_600_000), outcome=AuthorizationOutcome.APPROVED
            ),
            outcome_event("tx_e2", decided_at=at_ms(-3_599_999)),
            outcome_event("tx_e3", decided_at=T0, outcome=AuthorizationOutcome.APPROVED),
        ]
        self.check(history, transaction(), [Expectation("declined_ratio_1h", 1.0, tolerance=FLOAT)])

    def test_a_pending_outcome_never_counts(self) -> None:
        """F10. No transaction for it is known."""
        self.check(
            [outcome_event("tx_p1", decided_at=at(-20))],
            transaction(),
            [Expectation("declined_ratio_1h", None, state=INSUFFICIENT)],
        )

    def test_a_pending_outcome_counts_once_its_transaction_is_known(self) -> None:
        """F11. Its verifying transaction never counts its own outcome; a later score does."""
        pending = outcome_event("tx_p1", decided_at=at(-20))
        promoted = tx_event("tx_p1", occurred_at=at(-25), authorization_outcome=None)
        self.check(
            [pending, promoted],
            transaction(),
            [Expectation("declined_ratio_1h", 1.0, tolerance=FLOAT)],
        )
        own = transaction(occurred_at=at(-25), transaction_id="tx_p1", authorization_outcome=None)
        self.check([pending], own, [Expectation("declined_ratio_1h", None, state=INSUFFICIENT)])

    def test_an_outcome_reported_for_another_account_never_counts(self) -> None:
        """F12. Its transaction is known with a different account, so it is rejected."""
        history = [
            outcome_event("tx_m1", decided_at=at(-20)),
            tx_event(
                "tx_m1", occurred_at=at(-25), account_id=OTHER_ACCOUNT, authorization_outcome=None
            ),
        ]
        self.check(
            history, transaction(), [Expectation("declined_ratio_1h", None, state=INSUFFICIENT)]
        )

    def test_the_transaction_field_is_never_read(self) -> None:
        """F16. Earlier transactions say DECLINED in their own field, and no outcome is known."""
        history = [
            tx_event(
                "tx_f1", occurred_at=at(-10), authorization_outcome=AuthorizationOutcome.DECLINED
            ),
            tx_event(
                "tx_f2", occurred_at=at(-20), authorization_outcome=AuthorizationOutcome.DECLINED
            ),
        ]
        self.check(
            history,
            transaction(authorization_outcome=AuthorizationOutcome.DECLINED),
            [Expectation("declined_ratio_1h", None, state=INSUFFICIENT)],
        )

    def test_the_declined_ratio_spans_currencies(self) -> None:
        """F17. Outcomes carry no currency; the ratio covers every transaction's."""
        history = [
            tx_event("tx_eur", occurred_at=at(-30), currency="EUR", authorization_outcome=None),
            tx_event("tx_gbp", occurred_at=at(-25), currency="GBP", authorization_outcome=None),
            outcome_event("tx_eur", decided_at=at(-10)),
            outcome_event("tx_gbp", decided_at=at(-20), outcome=AuthorizationOutcome.APPROVED),
        ]
        self.check(
            history,
            transaction(currency="GBP"),
            [Expectation("declined_ratio_1h", 1 / 2, tolerance=FLOAT)],
        )

    def test_the_declined_ratio_is_absent_when_no_earlier_outcome_is_known(self) -> None:
        """F17. No outcome in the window is not a zero ratio.

        Also from a store watched over the whole window, whose empty window is a measured zero:
        zero known outcomes is still no denominator."""
        history = [tx_event("tx_n1", occurred_at=at(-10), authorization_outcome=None)]
        for complete_since in (None, WATCHED_LONG_ENOUGH):
            self.check(
                history,
                transaction(),
                [Expectation("declined_ratio_1h", None, state=INSUFFICIENT)],
                complete_since=complete_since,
            )

    def test_merchant_cv_reads_same_currency_whole_minutes_plus_the_scored_transaction(
        self,
    ) -> None:
        """Scored at 12:00:30 for 2 000 GBP; the minute containing it (12:00) is excluded.

        In: 1 000 GBP at 11:58:20 and 3 000 GBP at 11:43:20, plus the scored 2 000.
        Out: 50 000 EUR (other currency) and 99 999 GBP at 12:00:10 (the as_of minute).
        c = 3, mean = 2 000, variance = (1e6 + 9e6 + 4e6) / 3 - 2 000^2 = 2e6 / 3,
        CV = sqrt(2e6 / 3) / 2 000 = 1 / sqrt(6).
        Without the scored transaction it would be 0.5; with 99 999 very different.
        """
        history = [
            tx_event("tx_c1", occurred_at=at(-100), account_id="acct_000011", amount_minor=1_000),
            tx_event("tx_c2", occurred_at=at(-1_000), account_id="acct_000012", amount_minor=3_000),
            tx_event(
                "tx_eur",
                occurred_at=at(-200),
                account_id="acct_000013",
                amount_minor=50_000,
                currency="EUR",
            ),
            tx_event("tx_min", occurred_at=at(10), account_id="acct_000014", amount_minor=99_999),
        ]
        self.check(
            history,
            transaction(occurred_at=at(30), amount_minor=2_000),
            [Expectation("merchant_amount_cv_24h", 1 / math.sqrt(6), tolerance=FLOAT)],
        )

    def test_merchant_cv_excludes_the_partial_minute_at_the_far_edge(self) -> None:
        """Scored at 12:00:30. The far minute is floor((as_of - 24 h) / 1 min), 12:00 yesterday.

        `tx_edge` at T0 - 86 369 s is 23 h 59 m 59 s old -- inside the exact 24 h window, so it
        counts for the account -- but it lies in that far minute, so CV excludes it.
        `tx_inner` at T0 - 86 340 s is in the next minute and counts. CV over 1 000 and the
        scored 1 000 is exactly 0; with the edge's 3 000 it would not be.
        """
        history = [
            tx_event("tx_edge", occurred_at=at(-86_369), amount_minor=3_000),
            tx_event("tx_inner", occurred_at=at(-86_340), amount_minor=1_000),
        ]
        self.check(
            history,
            transaction(occurred_at=at(30), amount_minor=1_000),
            [
                Expectation("merchant_amount_cv_24h", 0.0),
                Expectation("account_tx_count_24h", 2 + 1),
            ],
        )

    def test_merchant_cv_needs_two_same_currency_observations(self) -> None:
        self.check(
            [tx_event("tx_eur", occurred_at=at(-100), currency="EUR", amount_minor=1_000)],
            transaction(occurred_at=at(30), currency="GBP"),
            [Expectation("merchant_amount_cv_24h", None, state=INSUFFICIENT)],
        )

    # -- distinct counts ------------------------------------------------------

    def test_exact_distinct_counts_include_the_scored_transactions_value(self) -> None:
        history = [
            tx_event("tx_1", occurred_at=at(-10), merchant_id="mrch_00011"),
            tx_event("tx_2", occurred_at=at(-20), merchant_id="mrch_00011"),
            tx_event("tx_3", occurred_at=at(-30), merchant_id="mrch_00012"),
        ]
        self.check(
            history,
            transaction(merchant_id=MERCHANT),
            [Expectation("account_distinct_merchants_1h", 3)],  # 00011, 00012, 00001
        )
        self.check(
            history,
            transaction(merchant_id="mrch_00011"),
            [Expectation("account_distinct_merchants_1h", 2)],  # 00011, 00012
        )

    def test_an_exact_distinct_value_also_seen_later_in_event_time_still_counts(self) -> None:
        """`mrch_00011` occurs at +300 s (delivered first) and at -100 s. Only the second is
        inside the window, and it must count: a store that kept each value's LATEST time would
        see +300 s, fall outside the window, and report 1."""
        history = [
            tx_event("tx_later", occurred_at=at(300), merchant_id="mrch_00011"),
            tx_event("tx_inside", occurred_at=at(-100), merchant_id="mrch_00011"),
        ]
        self.check(
            history,
            transaction(merchant_id=MERCHANT),
            [Expectation("account_distinct_merchants_1h", 2)],
        )

    def test_device_distinct_accounts_include_the_scored_account(self) -> None:
        history = [
            tx_event(f"tx_d{i}", occurred_at=at(-100 * i), account_id=f"acct_00000{i}")
            for i in range(2, 7)
        ]
        self.check(
            history,
            transaction(account_id="acct_000001"),
            [Expectation("device_distinct_accounts_24h", 5 + 1)],
        )

    def test_approximate_distinct_counts_read_back_exactly_at_low_cardinality(self) -> None:
        """Low cardinality on purpose: HyperLogLog's sparse encoding is exact there, so this
        measures wiring rather than the estimator. The estimator's error is bounded per
        cardinality stratum by the parity framework (plan §4.3), not by a unit tolerance."""
        history = [
            tx_event(f"tx_a{i}", occurred_at=at(-60 * i), account_id=f"acct_00000{i + 1}")
            for i in range(1, 5)
        ]
        self.check(
            history,
            transaction(account_id="acct_000001"),
            [
                Expectation("merchant_distinct_accounts_1h", 4 + 1),
                Expectation("ip_distinct_accounts_1h", 4 + 1),
            ],
        )

    def test_approximate_distinct_counts_use_the_declared_edge_inclusive_buckets(self) -> None:
        """Scored at 12:02:30, in the 12:00 bucket. The window starts at 11:02:30, in the
        11:00 bucket; both edge buckets count whole (ADR-0046 §2).

        In: acct_000007 at 11:00:10 (11:00 bucket, 1 h 2 m 20 s old -- outside the exact window)
        and acct_000008 at 12:03:20 (12:00 bucket, after as_of, delivered earlier).
        Out: acct_000009 at 10:59:50 (the 10:55 bucket). Plus the scored acct_000001: 3.
        """
        history = [
            tx_event("tx_far", occurred_at=at(-3_590), account_id="acct_000007"),
            tx_event("tx_after", occurred_at=at(200), account_id="acct_000008"),
            tx_event("tx_before", occurred_at=at(-3_610), account_id="acct_000009"),
        ]
        self.check(
            history,
            transaction(occurred_at=at(150), account_id="acct_000001"),
            [
                Expectation("merchant_distinct_accounts_1h", 3),
                Expectation("ip_distinct_accounts_1h", 3),
            ],
        )

    def test_approximate_and_exact_values_declare_which_they_are(self) -> None:
        history = [
            tx_event(f"tx_a{i}", occurred_at=at(-60 * i), account_id=f"acct_00000{i + 1}")
            for i in range(1, 5)
        ]
        subject = transaction(account_id="acct_000001")
        log = [*history, transaction_observation(subject)]
        ctx = self.context_for(log, len(history), subject, complete_since=None)
        approximate = ONLINE_FEATURES.get("merchant_distinct_accounts_1h").evaluate(subject, ctx)
        exact = ONLINE_FEATURES.get("account_distinct_merchants_1h").evaluate(subject, ctx)
        assert approximate.is_available and approximate.approximate
        assert exact.is_available and not exact.approximate

    # -- baselines exclude the scored transaction (ADR-0046 §3) --------------

    def test_the_robust_z_baseline_excludes_the_scored_transaction_and_its_millisecond(
        self,
    ) -> None:
        """Baseline 100..800 GBP, one per hour. Median (400 + 500) / 2 = 450; deviations
        350 250 150 50 50 150 250 350, MAD (150 + 250) / 2 = 200; z = (1 000 - 450) / (1.4826 *
        200). Letting the scored 1 000 in -- or `tx_aaa`, 100 000 GBP at the same millisecond,
        delivered first -- makes the median 500 and z = 500 / (1.4826 * 200): not saturated,
        so the difference is visible."""
        history = [
            tx_event(f"tx_z{k}", occurred_at=at(-3_600 * (9 - k)), amount_minor=100 * k)
            for k in range(1, 9)
        ]
        history.append(tx_event("tx_aaa", occurred_at=T0, amount_minor=100_000))
        self.check(
            history,
            transaction(amount_minor=1_000),
            [Expectation("amount_zscore_vs_account", 550 / (1.4826 * 200), tolerance=FLOAT)],
        )

    def test_the_robust_z_needs_eight_amounts(self) -> None:
        seven = [
            tx_event(f"tx_z{k}", occurred_at=at(-3_600 * (9 - k)), amount_minor=100 * k)
            for k in range(1, 8)
        ]
        self.check(
            seven,
            transaction(amount_minor=1_000),
            [Expectation("amount_zscore_vs_account", None, state=INSUFFICIENT)],
        )
        eight = [*seven, tx_event("tx_z8", occurred_at=at(-3_600), amount_minor=800)]
        self.check(
            eight,
            transaction(amount_minor=1_000),
            [Expectation("amount_zscore_vs_account", 550 / (1.4826 * 200), tolerance=FLOAT)],
        )

    def test_the_robust_z_reads_only_the_last_128_same_currency_amounts(self) -> None:
        """128 GBP amounts alternating 100 and 300 (median 200, MAD 100), preceded by a 129th
        of 1 000 000 and followed by 777 EUR. The sample is the 128: z = (500 - 200) / (1.4826 *
        100). With the 129th: median 300, MAD 200, z = 200 / 296.52. Ignoring currency: median
        300, MAD 100, z = 200 / 148.26."""
        history = [tx_event("tx_s_old", occurred_at=at(-3_600 * 131), amount_minor=1_000_000)]
        history += [
            tx_event(
                f"tx_s{k:03d}",
                occurred_at=at(-3_600 * (130 - k)),
                amount_minor=100 if k % 2 == 0 else 300,
            )
            for k in range(128)
        ]
        history.append(
            tx_event("tx_s_eur", occurred_at=at(-1_800), amount_minor=777, currency="EUR")
        )
        self.check(
            history,
            transaction(amount_minor=500),
            [Expectation("amount_zscore_vs_account", 300 / (1.4826 * 100), tolerance=FLOAT)],
        )

    def test_a_profile_ends_at_an_inactivity_gap_of_thirty_days(self) -> None:
        """`tx_old` then `tx_recent` exactly 30 days later: the lifetime is `tx_recent` alone,
        so the old device is unknown and tenure is 10 days. One millisecond less than 30 days
        apart and the lifetime holds both."""
        subject = transaction(device_id="dev_000077")
        recent = tx_event("tx_recent", occurred_at=at(-10 * 86_400))
        self.check(
            [tx_event("tx_old", occurred_at=at(-40 * 86_400), device_id="dev_000077"), recent],
            subject,
            [
                Expectation("device_is_known_for_account", 0.0),
                Expectation("account_tenure_days", 10.0, tolerance=FLOAT),
            ],
            complete_since=WATCHED_LONG_ENOUGH,
        )
        self.check(
            [
                tx_event("tx_old", occurred_at=at_ms(-40 * 86_400_000 + 1), device_id="dev_000077"),
                recent,
            ],
            subject,
            [
                Expectation("device_is_known_for_account", 1.0),
                Expectation(
                    "account_tenure_days", (40 * 86_400_000 - 1) / 86_400_000, tolerance=FLOAT
                ),
            ],
            complete_since=WATCHED_LONG_ENOUGH,
        )

    def test_a_profile_is_empty_once_the_account_has_been_quiet_for_thirty_days(self) -> None:
        subject = transaction(device_id="dev_000077")
        self.check(
            [tx_event("tx_only", occurred_at=at(-30 * 86_400), device_id="dev_000077")],
            subject,
            [
                Expectation("device_is_known_for_account", None, state=INSUFFICIENT),
                Expectation("account_tenure_days", None, state=INSUFFICIENT),
            ],
            complete_since=WATCHED_LONG_ENOUGH,
        )
        self.check(
            [tx_event("tx_only", occurred_at=at_ms(-30 * 86_400_000 + 1), device_id="dev_000077")],
            subject,
            [
                Expectation("device_is_known_for_account", 1.0),
                Expectation(
                    "account_tenure_days", (30 * 86_400_000 - 1) / 86_400_000, tolerance=FLOAT
                ),
            ],
            complete_since=WATCHED_LONG_ENOUGH,
        )

    def test_habitual_needs_three_observations_and_a_redelivery_is_not_one(self) -> None:
        habit = [
            tx_event(f"tx_h{i}", occurred_at=at(-3_600 * (7 - i)), merchant_id="mrch_00099")
            for i in range(1, 4)
        ]
        visits = [
            tx_event("tx_v1", occurred_at=at(-3 * 3_600), merchant_id="mrch_00031"),
            tx_event("tx_v2", occurred_at=at(-2 * 3_600), merchant_id="mrch_00031"),
            tx_event("tx_v2", occurred_at=at(-1 * 3_600), merchant_id="mrch_00031"),
        ]
        subject = transaction(merchant_id="mrch_00031")
        self.check(
            [*habit, *visits],
            subject,
            [Expectation("merchant_is_habitual", 0.0)],
            complete_since=WATCHED_LONG_ENOUGH,
        )
        self.check(
            [*habit, *visits, tx_event("tx_v3", occurred_at=at(-1_800), merchant_id="mrch_00031")],
            subject,
            [Expectation("merchant_is_habitual", 1.0)],
            complete_since=WATCHED_LONG_ENOUGH,
        )

    # -- home location: geodesic medoid of the last 20 (ADR-0046 §3, Q4c) ----

    def test_home_needs_three_located_observations(self) -> None:
        """Unlocated observations are in the lifetime but not in the home sample."""
        subject = transaction(latitude=0.0, longitude=2.0)
        points = [
            located("tx_p0", hours_ago=3, longitude=0.0),
            located("tx_p1", hours_ago=2, longitude=1.0),
            located("tx_p10", hours_ago=1, longitude=10.0),
        ]
        padding = [unlocated(f"tx_u{i}", hours_ago=4 + i) for i in range(3)]
        for count in (0, 1, 2):
            self.check(
                [*padding, *points[:count]],
                subject,
                [Expectation("distance_from_account_home_km", None, state=INSUFFICIENT)],
            )
        # Three: sums of distances in degrees are 1 + 10 = 11, 1 + 9 = 10 and 10 + 9 = 19,
        # so home is (0, 1), one degree from the scored (0, 2).
        self.check(
            [*padding, *points],
            subject,
            [Expectation("distance_from_account_home_km", along_equator_km(1), tolerance=FLOAT)],
        )

    def test_home_ignores_an_outlier_and_is_a_point_the_account_visited(self) -> None:
        """Longitudes 0, 0.1, 0.3, 0.35 and a newest outlier at 90. Sums of distances, in
        degrees: 0 -> 90.75, 0.1 -> 90.45, 0.3 -> 90.25, 0.35 -> 90.3, 90 -> 359.25, so home is
        0.3 and the scored point at 1.3 is one degree away. The mean longitude (18.15) and the
        last-seen point (90) are both wrong."""
        history = [
            located("tx_p1", hours_ago=5, longitude=0.0),
            located("tx_p2", hours_ago=4, longitude=0.1),
            located("tx_p3", hours_ago=3, longitude=0.3),
            located("tx_p4", hours_ago=2, longitude=0.35),
            located("tx_p5", hours_ago=1, longitude=90.0),
        ]
        self.check(
            history,
            transaction(latitude=0.0, longitude=1.3),
            [Expectation("distance_from_account_home_km", along_equator_km(1), tolerance=FLOAT)],
        )

    def test_home_is_the_medoid_of_exactly_twenty(self) -> None:
        """Oldest first: ten at longitude 1, then ten at 5. Every point's sum is ten 4-degree
        distances, a tie, so the earliest -- longitude 1 -- is home, four degrees from the
        scored point at 5. A sample of 19 would drop that point and leave nine against ten, so
        home would be 5; so would "the last located observation"."""
        longitudes = [1.0] * 10 + [5.0] * 10
        history = [
            located(f"tx_p{i:02d}", hours_ago=30 - i, longitude=lon)
            for i, lon in enumerate(longitudes)
        ]
        self.check(
            history,
            transaction(latitude=0.0, longitude=5.0),
            [Expectation("distance_from_account_home_km", along_equator_km(4), tolerance=FLOAT)],
        )

    def test_home_uses_only_the_last_twenty_located_observations(self) -> None:
        """Oldest first: 1 | 5, nine at 1, nine at 5, 1 | then one unlocated. The last twenty
        located are ten at 5 and ten at 1, a tie that the earliest of them (5) wins, so the
        scored point at 6 is one degree from home. Counting the oldest as well makes eleven at
        1, which wins outright; counting the unlocated one as a slot drops the 5 that wins the
        tie; and the last located point is at 1."""
        longitudes = [1.0, 5.0] + [1.0] * 9 + [5.0] * 9 + [1.0]
        history = [
            located(f"tx_p{i:02d}", hours_ago=30 - i, longitude=lon)
            for i, lon in enumerate(longitudes)
        ]
        history.append(unlocated("tx_u", hours_ago=1))
        self.check(
            history,
            transaction(latitude=0.0, longitude=6.0),
            [Expectation("distance_from_account_home_km", along_equator_km(1), tolerance=FLOAT)],
        )

    def test_home_ties_go_to_the_earliest_time_then_the_smaller_identity(self) -> None:
        """Four points at longitudes 0 and 2, two of each: every sum is two 2-degree
        distances. The earliest is the pair at the same millisecond; of those `tx_pa` (at 2)
        sorts before `tx_pb` (at 0), though `tx_pb` was delivered first. The latest,
        `tx_pd`, is at 0 too, so a tie that went to the latest would also be caught."""
        history = [
            located("tx_pb", hours_ago=3, longitude=0.0),
            located("tx_pa", hours_ago=3, longitude=2.0),
            located("tx_pc", hours_ago=2, longitude=2.0),
            located("tx_pd", hours_ago=1, longitude=0.0),
        ]
        self.check(
            history,
            transaction(latitude=0.0, longitude=3.0),
            [Expectation("distance_from_account_home_km", along_equator_km(1), tolerance=FLOAT)],
        )

    def test_home_is_computed_on_the_sphere_across_the_antimeridian(self) -> None:
        """Longitudes 179, -179, -178.5, 178.5. Sums of shortest-arc distances: 5, 5, 6, 6;
        the tie goes to the earlier, 179. The scored -179.5 is 1.5 degrees away. A
        component-wise median of longitude would put home at 0, half a world away."""
        history = [
            located("tx_p1", hours_ago=4, longitude=179.0),
            located("tx_p2", hours_ago=3, longitude=-179.0),
            located("tx_p3", hours_ago=2, longitude=-178.5),
            located("tx_p4", hours_ago=1, longitude=178.5),
        ]
        self.check(
            history,
            transaction(latitude=0.0, longitude=-179.5),
            [Expectation("distance_from_account_home_km", along_equator_km(1.5), tolerance=FLOAT)],
        )

    def test_profiles_span_currencies(self) -> None:
        """Three EUR purchases at one merchant, on one device, at longitudes 0, 1 and 10. A
        GBP purchase there is habitual, on a known device, one degree from home (the medoid
        is 1), on an account first seen three hours ago. A profile keyed by currency would
        know nothing about this account."""
        history = [
            tx_event(
                f"tx_e{i}",
                occurred_at=at(-3_600 * (3 - i)),
                currency="EUR",
                merchant_id="mrch_00031",
                device_id="dev_000077",
                latitude=0.0,
                longitude=lon,
            )
            for i, lon in enumerate((0.0, 1.0, 10.0))
        ]
        self.check(
            history,
            transaction(
                currency="GBP",
                merchant_id="mrch_00031",
                device_id="dev_000077",
                latitude=0.0,
                longitude=2.0,
            ),
            [
                Expectation("merchant_is_habitual", 1.0),
                Expectation("device_is_known_for_account", 1.0),
                Expectation("distance_from_account_home_km", along_equator_km(1), tolerance=FLOAT),
                Expectation("account_tenure_days", 3 / 24, tolerance=FLOAT),
            ],
            complete_since=WATCHED_LONG_ENOUGH,
        )

    def test_a_profile_follows_event_time_not_arrival_order(self) -> None:
        """The 30-day-gap fixture delivered newest first gives the same answer."""
        self.check(
            [
                tx_event("tx_recent", occurred_at=at(-10 * 86_400)),
                tx_event("tx_old", occurred_at=at(-40 * 86_400), device_id="dev_000077"),
            ],
            transaction(device_id="dev_000077"),
            [
                Expectation("device_is_known_for_account", 0.0),
                Expectation("account_tenure_days", 10.0, tolerance=FLOAT),
            ],
            complete_since=WATCHED_LONG_ENOUGH,
        )

    def test_baselines_read_only_the_lifetime(self) -> None:
        """Two eras 50 days apart. Old: eight 100 000 GBP at `mrch_00031`, longitude 90.
        Recent: 100..800 GBP at the default merchant, longitude 0. Only the recent era is the
        lifetime: z = 550 / (1.4826 * 200), home is 0 so (0, 1) is one degree away, and
        `mrch_00031` is not habitual. Reading all history gives none of these."""
        old = [
            tx_event(
                f"tx_old{k}",
                occurred_at=at(-60 * 86_400 - 3_600 * k),
                amount_minor=100_000,
                merchant_id="mrch_00031",
                latitude=0.0,
                longitude=90.0,
            )
            for k in range(1, 9)
        ]
        recent = [
            tx_event(
                f"tx_new{k}",
                occurred_at=at(-10 * 86_400 - 3_600 * k),
                amount_minor=100 * k,
                latitude=0.0,
                longitude=0.0,
            )
            for k in range(1, 9)
        ]
        self.check(
            [*old, *recent],
            transaction(amount_minor=1_000, merchant_id="mrch_00031", latitude=0.0, longitude=1.0),
            [
                Expectation("amount_zscore_vs_account", 550 / (1.4826 * 200), tolerance=FLOAT),
                Expectation("distance_from_account_home_km", along_equator_km(1), tolerance=FLOAT),
                Expectation("merchant_is_habitual", 0.0),
            ],
            complete_since=WATCHED_LONG_ENOUGH,
        )

    def test_home_sums_distances_in_whole_metres_rounded_half_up(self) -> None:
        """Equatorial points 788.3, 1 597.47, 1 286.9, 1 292.33 and 1 291.67 m east, oldest
        first. Summed in whole metres rounded half up, the distances give 2315, 1731, 820, 815
        and 815: a tie the older point wins, so home is 1 292.33 m and the scored point at
        3 000 m is 1.70767 km away. Unrounded the sums are 2315.17, 1730.68, 819.37, 815.26 and
        814.60, and rounded down 2314, 1729, 817, 814 and 812: both pick 1 291.67 m instead,
        by a margin no floating-point noise can close."""
        history = [
            located(f"tx_m{i}", hours_ago=5 - i, longitude=metres_east(m))
            for i, m in enumerate((788.3, 1_597.47, 1_286.9, 1_292.33, 1_291.67))
        ]
        self.check(
            history,
            transaction(latitude=0.0, longitude=metres_east(3_000.0)),
            [
                Expectation(
                    "distance_from_account_home_km", (3_000.0 - 1_292.33) / 1_000, tolerance=FLOAT
                )
            ],
        )

    def test_transaction_counts_span_currencies(self) -> None:
        """A EUR purchase ten seconds before a GBP one still happened: the account's minute count
        and the card's five-minute count are both 2. A velocity set keyed by currency says 1."""
        self.check(
            [tx_event("tx_eur", occurred_at=at(-10), currency="EUR")],
            transaction(currency="GBP"),
            [Expectation("account_tx_count_1m", 2), Expectation("card_tx_count_5m", 2)],
        )

    def test_habitual_categories_read_only_the_lifetime(self) -> None:
        """Three visits in MCC 7995 sixty days ago, then three ordinary ones ten days ago. The gap
        ends the old era, so 7995 is not habitual now (5411 is, so the set is not empty)."""
        old = [
            tx_event(f"tx_bet{k}", occurred_at=at(-60 * 86_400 - 3_600 * k), merchant_mcc="7995")
            for k in range(1, 4)
        ]
        recent = [
            tx_event(f"tx_shop{k}", occurred_at=at(-10 * 86_400 - 3_600 * k)) for k in range(1, 4)
        ]
        self.check(
            [*old, *recent],
            transaction(merchant_mcc="7995"),
            [Expectation("mcc_is_habitual_for_account", 0.0)],
            complete_since=WATCHED_LONG_ENOUGH,
        )

    def test_exact_distinct_counts_span_currencies(self) -> None:
        self.check(
            [tx_event("tx_eur", occurred_at=at(-10), currency="EUR", merchant_id="mrch_00011")],
            transaction(currency="GBP", merchant_id=MERCHANT),
            [Expectation("account_distinct_merchants_1h", 2)],
        )

    def test_merchant_cv_is_absent_when_the_mean_is_zero(self) -> None:
        """A 1 000 GBP refund and a 1 000 GBP purchase: mean 0, no coefficient of variation."""
        self.check(
            [tx_event("tx_refund", occurred_at=at(-100), amount_minor=-1_000)],
            transaction(occurred_at=at(30), amount_minor=1_000),
            [Expectation("merchant_amount_cv_24h", None, state=INSUFFICIENT)],
        )

    def test_the_robust_z_is_capped_at_fifty(self) -> None:
        """Baseline 100..800 (median 450, MAD 200); 1 000 000 would be z = 3 370."""
        history = [
            tx_event(f"tx_z{k}", occurred_at=at(-3_600 * (9 - k)), amount_minor=100 * k)
            for k in range(1, 9)
        ]
        self.check(
            history,
            transaction(amount_minor=1_000_000),
            [Expectation("amount_zscore_vs_account", 50.0)],
        )

    def test_a_zero_mad_keeps_the_sign_of_the_deviation(self) -> None:
        """Eight 5 000 GBP amounts: MAD 0. A lower amount is -50, a higher one +50, the same
        amount 0. Phase 2 gave +50 to a lower amount, so high-value rules fired on it."""
        history = [tx_event(f"tx_z{k}", occurred_at=at(-3_600 * k)) for k in range(1, 9)]
        for amount, expected in ((100, -50.0), (9_000, 50.0), (5_000, 0.0)):
            self.check(
                history,
                transaction(amount_minor=amount),
                [Expectation("amount_zscore_vs_account", expected)],
            )

    def test_as_of_exactly_on_a_minute_and_bucket_boundary(self) -> None:
        """Scored at exactly 12:00:00.000 for 2 000 GBP at the default merchant.

        Buckets 11:00 through 12:00 count whole: A at exactly 1 h old (11:00 bucket), C at
        12:04:59.999 (12:00 bucket, after as_of), the 1 000 at -1 ms, `tx_aaa` at 0 and the
        scored account -- five accounts. B (10:59:59.999) and D (12:05:00.000) are outside.
        The CV reads whole minutes from 12:01 yesterday to 11:59, same currency, plus the
        scored amount: A 5 000, B 5 000, 3 000 at -23 h 59 m, 1 000 at -1 ms and 2 000.
        Excluded: 1 000 at exactly -24 h (the far minute), C and D (after), `tx_aaa` (the
        12:00 minute). Mean 3 200, variance 64e6 / 5 - 3 200^2 = 2.56e6, CV = 1 600 / 3 200.
        """
        history = [
            tx_event("tx_a", occurred_at=at_ms(-3_600_000), account_id="acct_000011"),
            tx_event("tx_b", occurred_at=at_ms(-3_600_001), account_id="acct_000012"),
            tx_event("tx_c", occurred_at=at_ms(299_999), account_id="acct_000013"),
            tx_event("tx_d", occurred_at=at_ms(300_000), account_id="acct_000014"),
            tx_event(
                "tx_far",
                occurred_at=at_ms(-86_400_000),
                account_id="acct_000015",
                amount_minor=1_000,
            ),
            tx_event(
                "tx_in",
                occurred_at=at_ms(-86_340_000),
                account_id="acct_000016",
                amount_minor=3_000,
            ),
            tx_event(
                "tx_near", occurred_at=at_ms(-1), account_id="acct_000017", amount_minor=1_000
            ),
            tx_event("tx_aaa", occurred_at=T0, account_id="acct_000018", amount_minor=9_000),
        ]
        self.check(
            history,
            transaction(account_id=ACCOUNT, amount_minor=2_000),
            [
                Expectation("merchant_distinct_accounts_1h", 5),
                Expectation("merchant_amount_cv_24h", 1_600 / 3_200, tolerance=FLOAT),
            ],
        )

    def test_a_redelivery_is_scored_as_its_first_delivery(self) -> None:
        """`tx_mmm` first arrived at -100 s for 1 000; it is redelivered at T0 for 5 000. The
        first delivery is the observation: the read is at -100 s, so the 1-minute window holds
        only itself, the hour holds `tx_h` (5 000) and 1 000, and the previous transaction is
        `tx_h`, 100 s earlier -- never itself."""
        subject = transaction(amount_minor=5_000)
        log = [
            tx_event("tx_h", occurred_at=at(-200)),
            tx_event(SUBJECT, occurred_at=at(-100), amount_minor=1_000),
            transaction_observation(subject),
        ]
        self.check_log(
            log,
            2,
            subject,
            [
                Expectation("account_tx_count_1m", 1),
                Expectation("account_tx_count_5m", 2),
                Expectation("account_amount_sum_1h", 5_000 + 1_000),
                Expectation("seconds_since_last_transaction", 100.0),
            ],
        )

    def test_an_identity_event_cannot_swallow_a_transaction_with_the_same_id(self) -> None:
        """Identities are per stream (plan §3 Q2): a failed login whose id is `tx_mmm` is not
        an earlier delivery of the transaction `tx_mmm`."""
        subject = transaction()
        log = [
            identity_event(SUBJECT, stream=Stream.IDENTITY_FAILED_LOGIN, occurred_at=at(-10)),
            transaction_observation(subject),
        ]
        self.check_log(
            log,
            1,
            subject,
            [Expectation("account_tx_count_1m", 1), Expectation("failed_logins_1h", 1)],
        )

    # -- the previous observation (ADR-0046 §4) ------------------------------

    def test_the_previous_observation_is_the_latest_and_an_older_arrival_does_not_replace_it(
        self,
    ) -> None:
        history = [
            located("tx_newer", hours_ago=100 / 3_600, longitude=1.0),
            located("tx_older", hours_ago=1, longitude=0.0),
        ]
        self.check(
            history,
            transaction(latitude=0.0, longitude=0.0),
            [
                Expectation("geo_distance_from_last_km", along_equator_km(1), tolerance=FLOAT),
                Expectation("seconds_since_last_transaction", 100.0),
                Expectation(
                    "implied_speed_kmh_from_last",
                    along_equator_km(1) / (100 / 3_600),
                    tolerance=FLOAT,
                ),
            ],
        )

    def test_an_observation_at_the_scored_millisecond_is_never_the_previous_one(self) -> None:
        history = [
            located("tx_before", hours_ago=100 / 3_600, longitude=1.0),
            tx_event("tx_aaa", occurred_at=T0, latitude=0.0, longitude=5.0),
        ]
        self.check(
            history,
            transaction(latitude=0.0, longitude=0.0),
            [
                Expectation("geo_distance_from_last_km", along_equator_km(1), tolerance=FLOAT),
                Expectation("seconds_since_last_transaction", 100.0),
            ],
        )

    def test_the_previous_observation_lookback_is_half_open(self) -> None:
        self.check(
            [tx_event("tx_edge", occurred_at=at_ms(-86_400_000))],
            transaction(),
            [Expectation("seconds_since_last_transaction", None, state=INSUFFICIENT)],
        )
        self.check(
            [tx_event("tx_inside", occurred_at=at_ms(-86_399_999))],
            transaction(),
            [Expectation("seconds_since_last_transaction", 86_399.999, tolerance=FLOAT)],
        )

    def test_previous_ties_at_one_millisecond_go_to_the_greater_identity(self) -> None:
        """`tx_pb` (at 2) wins over `tx_pa` (at 1) in either delivery order."""
        winner = located("tx_pb", hours_ago=100 / 3_600, longitude=2.0)
        loser = located("tx_pa", hours_ago=100 / 3_600, longitude=1.0)
        for history in ([winner, loser], [loser, winner]):
            self.check(
                history,
                transaction(latitude=0.0, longitude=0.0),
                [Expectation("geo_distance_from_last_km", along_equator_km(2), tolerance=FLOAT)],
            )

    def test_implied_speed_needs_both_legs_card_present(self) -> None:
        history = [
            tx_event(
                "tx_cnp",
                occurred_at=at(-3_600),
                channel=TransactionChannel.CARD_NOT_PRESENT,
                latitude=40.7,
                longitude=-74.0,
            )
        ]
        self.check(history, transaction(), [Expectation("implied_speed_kmh_from_last", 0.0)])

    def test_geo_features_are_unavailable_without_coverage_not_zero(self) -> None:
        coverage = frozenset(ALWAYS_REQUIRED) | {CanonicalField.CHANNEL}
        self.check(
            [tx_event("tx_h", occurred_at=at(-100))],
            transaction(coverage=coverage),
            [
                Expectation("geo_distance_from_last_km", None, state=FeatureState.UNAVAILABLE),
                Expectation("implied_speed_kmh_from_last", None, state=FeatureState.UNAVAILABLE),
                Expectation("distance_from_account_home_km", None, state=FeatureState.UNAVAILABLE),
            ],
        )

    # -- identity streams ------------------------------------------------------

    def test_failed_logins_are_counted_from_their_own_stream(self) -> None:
        history = [
            identity_event(
                f"evt_fl{i}", stream=Stream.IDENTITY_FAILED_LOGIN, occurred_at=at(-60 * i)
            )
            for i in range(1, 6)
        ]
        self.check(history, transaction(), [Expectation("failed_logins_1h", 5)])

    def test_an_identity_change_is_not_a_failed_login(self) -> None:
        history = [identity_event("evt_ic", stream=Stream.IDENTITY_CHANGE, occurred_at=at(-60))]
        self.check(
            history,
            transaction(),
            [
                Expectation("failed_logins_1h", None, state=INSUFFICIENT),
                Expectation("hours_since_identity_change", 60 / 3_600, tolerance=FLOAT),
            ],
        )

    # -- completeness (ADR-0044) ------------------------------------------------

    def test_a_window_the_store_watched_and_saw_nothing_in_is_a_measured_zero(self) -> None:
        history = [tx_event("tx_other", occurred_at=at(-60), account_id="acct_000777")]
        self.check(
            history,
            transaction(account_id=ACCOUNT),
            [
                Expectation("failed_logins_1h", 0.0),
                Expectation("account_tx_count_24h", 0 + 1),  # the scored transaction alone
            ],
            complete_since=WATCHED_LONG_ENOUGH,
        )

    def test_a_young_store_zeroes_only_the_windows_it_watched(self) -> None:
        """Complete for an hour. A window holding the scored transaction is served as what it
        holds -- a lower bound on an incomplete store (`Completeness.INCOMPLETE`)."""
        self.check(
            [tx_event("tx_other", occurred_at=at(-60), account_id="acct_000777")],
            transaction(account_id=ACCOUNT),
            [
                Expectation("failed_logins_1h", 0.0),
                Expectation("account_tx_count_24h", 1),
                Expectation("hours_since_identity_change", None, state=INSUFFICIENT),
            ],
            complete_since=WATCHED_ONE_HOUR,
        )

    def test_a_store_that_makes_no_completeness_claim_reports_no_absence_as_zero(self) -> None:
        self.check(
            [tx_event("tx_other", occurred_at=at(-60), account_id="acct_000777")],
            transaction(account_id=ACCOUNT),
            [
                Expectation("failed_logins_1h", None, state=INSUFFICIENT),
                Expectation("account_tx_count_1m", 1),
            ],
            complete_since=None,
        )

    def test_tenure_needs_the_store_to_have_watched_for_its_horizon(self) -> None:
        history = [tx_event("tx_h", occurred_at=at(-3 * 86_400))]
        self.check(
            history,
            transaction(),
            [Expectation("account_tenure_days", 3.0, tolerance=FLOAT)],
            complete_since=WATCHED_LONG_ENOUGH,
        )
        self.check(
            history,
            transaction(),
            [Expectation("account_tenure_days", None, state=INSUFFICIENT)],
            complete_since=WATCHED_ONE_HOUR,
        )

    def test_an_unknown_device_is_only_called_unknown_by_a_store_that_would_know(self) -> None:
        history = [tx_event("tx_h", occurred_at=at(-1_800), device_id="dev_000009")]
        self.check(
            history,
            transaction(device_id="dev_000123"),
            [Expectation("device_is_known_for_account", 0.0)],
            complete_since=WATCHED_LONG_ENOUGH,
        )
        self.check(
            history,
            transaction(device_id="dev_000123"),
            [Expectation("device_is_known_for_account", None, state=INSUFFICIENT)],
            complete_since=WATCHED_ONE_HOUR,
        )
        self.check(
            history,
            transaction(device_id="dev_000009"),
            [Expectation("device_is_known_for_account", 1.0)],
            complete_since=WATCHED_ONE_HOUR,
        )

    # -- the whole feature set on an empty store --------------------------------

    def test_on_an_empty_store_every_value_is_the_scored_transaction_alone(self) -> None:
        """Nothing is ever a fabricated zero, and nothing counts the transaction twice.

        One `.get(key, 0)` anywhere in an implementation turns a cold store into a confident
        "no risk" -- and one double-recorded subject turns every count into 2. Listed for all
        26 features, and asserted to be all of them, so a new feature cannot slip past it.
        """
        subject = transaction()
        expectations = {
            "account_tx_count_1m": 1.0,
            "account_tx_count_5m": 1.0,
            "account_tx_count_1h": 1.0,
            "account_tx_count_24h": 1.0,
            "account_amount_sum_1h": 5_000.0,
            "card_tx_count_5m": 1.0,
            "declined_ratio_1h": None,  # no outcome is known, and its own never counts
            "account_distinct_merchants_1h": 1.0,
            "account_distinct_mcc_5m": 1.0,
            "account_distinct_devices_24h": 1.0,
            "account_distinct_countries_24h": 1.0,
            "device_distinct_accounts_24h": 1.0,
            "ip_distinct_accounts_1h": 1.0,
            "merchant_distinct_accounts_1h": 1.0,
            "merchant_amount_cv_24h": None,  # one observation has no dispersion
            "amount_zscore_vs_account": None,
            "account_tenure_days": None,
            "merchant_is_habitual": None,
            "mcc_is_habitual_for_account": None,
            "device_is_known_for_account": None,
            "distance_from_account_home_km": None,
            "geo_distance_from_last_km": None,
            "implied_speed_kmh_from_last": None,
            "seconds_since_last_transaction": None,
            "hours_since_identity_change": None,
            "failed_logins_1h": None,
        }
        assert set(expectations) == set(ONLINE_FEATURES.ids)
        self.check(
            [],
            subject,
            [
                Expectation(fid, value, state=None if value is not None else INSUFFICIENT)
                for fid, value in expectations.items()
            ],
        )


class AsServedConformanceSuite(FeatureSemanticsConformanceSuite):
    """What an online store serves: nothing delivered after the scored transaction counts."""

    def test_an_outcome_recorded_after_the_score_cannot_change_it(self) -> None:
        """F3 as served: the outcome arrived after the read."""
        subject = transaction()
        known = tx_event("tx_x1", occurred_at=at(-10), authorization_outcome=None)
        late = outcome_event("tx_x1", decided_at=at_ms(-9_700))
        self.check_log(
            [known, transaction_observation(subject), late],
            1,
            subject,
            [Expectation("declined_ratio_1h", None, state=INSUFFICIENT)],
        )

    def test_an_outcome_verified_after_the_score_does_not_count_in_it(self) -> None:
        """F3 as served: the outcome was recorded first, its transaction only after the read."""
        subject = transaction()
        early = outcome_event("tx_x1", decided_at=at_ms(-9_700))
        late = tx_event("tx_x1", occurred_at=at(-10), authorization_outcome=None)
        self.check_log(
            [early, transaction_observation(subject), late],
            1,
            subject,
            [Expectation("declined_ratio_1h", None, state=INSUFFICIENT)],
        )

    def test_a_same_millisecond_observation_counts_only_if_it_was_recorded_first(self) -> None:
        """At T0: `tx_zzz` (1 000) delivered first, the scored `tx_mmm` (5 000), then `tx_aaa`
        (300). Served: 1 000 + 5 000. The event-time-complete answer is 300 + 5 000."""
        subject = transaction(amount_minor=5_000)
        log = [
            tx_event("tx_zzz", occurred_at=T0, amount_minor=1_000),
            transaction_observation(subject),
            tx_event("tx_aaa", occurred_at=T0, amount_minor=300),
        ]
        self.check_log(
            log,
            1,
            subject,
            [
                Expectation("account_amount_sum_1h", 1_000 + 5_000),
                Expectation("account_tx_count_1m", 2),
            ],
        )

    def test_an_observation_delivered_after_the_scored_transaction_cannot_leak_into_it(
        self,
    ) -> None:
        subject = transaction()
        log = [transaction_observation(subject), tx_event("tx_late", occurred_at=at(-30))]
        self.check_log(
            log,
            0,
            subject,
            [Expectation("account_tx_count_1m", 1), Expectation("account_amount_sum_1h", 5_000)],
        )

    def test_different_arrival_orders_at_one_millisecond_serve_different_counts(self) -> None:
        subject = transaction(transaction_id="tx_b")
        a = tx_event("tx_a", occurred_at=T0)
        b = transaction_observation(subject)
        c = tx_event("tx_c", occurred_at=T0)
        for log, index, served in (([c, b, a], 1, 2), ([a, c, b], 2, 3), ([b, a, c], 0, 1)):
            self.check_log(log, index, subject, [Expectation("account_tx_count_1m", served)])

    def test_a_failed_login_at_the_scored_millisecond_counts_if_recorded_first(self) -> None:
        subject = transaction()
        log = [
            identity_event("zz_fl", stream=Stream.IDENTITY_FAILED_LOGIN, occurred_at=T0),
            transaction_observation(subject),
        ]
        self.check_log(log, 1, subject, [Expectation("failed_logins_1h", 1)])

    def test_the_last_bucket_holds_only_what_was_recorded_before_the_read(self) -> None:
        subject = transaction(occurred_at=at(150), account_id="acct_000001")
        log = [
            transaction_observation(subject),
            tx_event("tx_after", occurred_at=at(200), account_id="acct_000008"),
        ]
        self.check_log(log, 0, subject, [Expectation("merchant_distinct_accounts_1h", 1)])


class EventTimeCompleteConformanceSuite(FeatureSemanticsConformanceSuite):
    """What a complete history says: arrival order only decides which delivery came first."""

    def test_an_outcome_recorded_after_the_score_counts_in_a_complete_history(self) -> None:
        """F3 complete: arrival order is irrelevant, so the late outcome counts."""
        subject = transaction()
        known = tx_event("tx_x1", occurred_at=at(-10), authorization_outcome=None)
        late = outcome_event("tx_x1", decided_at=at_ms(-9_700))
        self.check_log(
            [known, transaction_observation(subject), late],
            1,
            subject,
            [Expectation("declined_ratio_1h", 1.0, tolerance=FLOAT)],
        )

    def test_an_outcome_whose_transaction_arrived_later_counts_in_a_complete_history(self) -> None:
        """F3 complete: its transaction exists in the complete history, whatever the order."""
        subject = transaction()
        early = outcome_event("tx_x1", decided_at=at_ms(-9_700))
        late = tx_event("tx_x1", occurred_at=at(-10), authorization_outcome=None)
        self.check_log(
            [early, transaction_observation(subject), late],
            1,
            subject,
            [Expectation("declined_ratio_1h", 1.0, tolerance=FLOAT)],
        )

    def test_same_millisecond_ties_break_by_identity_whatever_the_arrival_order(self) -> None:
        """At T0: `tx_aaa` (300) sorts before the scored `tx_mmm` (5 000); `tx_zzz` (1 000)
        after it. 300 + 5 000, in every arrival order."""
        subject = transaction(amount_minor=5_000)
        a = tx_event("tx_aaa", occurred_at=T0, amount_minor=300)
        s = transaction_observation(subject)
        z = tx_event("tx_zzz", occurred_at=T0, amount_minor=1_000)
        for log in ([z, s, a], [s, a, z], [a, z, s]):
            self.check_log(
                log,
                log.index(s),
                subject,
                [
                    Expectation("account_amount_sum_1h", 300 + 5_000),
                    Expectation("account_tx_count_1m", 2),
                ],
            )

    def test_an_earlier_observation_delivered_after_the_scored_transaction_counts(self) -> None:
        subject = transaction()
        log = [transaction_observation(subject), tx_event("tx_late", occurred_at=at(-30))]
        self.check_log(
            log,
            0,
            subject,
            [
                Expectation("account_tx_count_1m", 2),
                Expectation("account_amount_sum_1h", 5_000 + 5_000),
            ],
        )

    def test_the_identity_tie_break_applies_only_at_the_scored_millisecond(self) -> None:
        """`tx_000` sorts before the scored identity but is exactly a minute old: outside."""
        self.check(
            [tx_event("tx_000", occurred_at=at_ms(-60_000))],
            transaction(),
            [Expectation("account_tx_count_1m", 1)],
        )

    def test_the_last_bucket_is_whole_whatever_arrived_after(self) -> None:
        subject = transaction(occurred_at=at(150), account_id="acct_000001")
        log = [
            transaction_observation(subject),
            tx_event("tx_after", occurred_at=at(200), account_id="acct_000008"),
        ]
        self.check_log(log, 0, subject, [Expectation("merchant_distinct_accounts_1h", 2)])

    def test_identity_events_at_the_scored_millisecond_sort_before_it(self) -> None:
        """The declared order is `(occurred_ms, namespace, id)`, and `identity_event` sorts
        before `transaction`: both failed logins at T0 count, whatever their ids and whatever
        order they arrived in."""
        subject = transaction()
        aa = identity_event("aa_fl", stream=Stream.IDENTITY_FAILED_LOGIN, occurred_at=T0)
        zz = identity_event("zz_fl", stream=Stream.IDENTITY_FAILED_LOGIN, occurred_at=T0)
        s = transaction_observation(subject)
        for log in ([zz, aa, s], [s, zz, aa]):
            self.check_log(log, log.index(s), subject, [Expectation("failed_logins_1h", 2)])


class ReferenceAsServedConformanceTest(AsServedConformanceSuite):
    """The naive store, recording deliveries in order and scoring the subject."""

    def context_for(
        self,
        log: Sequence[Event],
        subject_index: int,
        subject: CanonicalTransaction,
        *,
        complete_since: EventTime | None,
    ) -> FeatureContext:
        from trace_core.features.reference import ReferenceFeatureStore

        store = ReferenceFeatureStore(complete_since=complete_since)
        store.observe_all(log[:subject_index])
        served = store.score(log[subject_index])
        assert served.receipt.position == len({e.identity for e in log[: subject_index + 1]})
        # Later deliveries land after the read; the context already served must not move.
        store.observe_all(log[subject_index + 1 :])
        return served.context


class ReferenceEventTimeCompleteConformanceTest(EventTimeCompleteConformanceSuite):
    """The naive implementation over every delivery, whatever order they arrived in."""

    def context_for(
        self,
        log: Sequence[Event],
        subject_index: int,
        subject: CanonicalTransaction,
        *,
        complete_since: EventTime | None,
    ) -> FeatureContext:
        from trace_core.features.reference import event_time_complete_context

        return event_time_complete_context(log, log[subject_index], complete_since=complete_since)


__all__ = [
    "EXACT",
    "FLOAT",
    "SUBJECT",
    "T0",
    "AsServedConformanceSuite",
    "EventTimeCompleteConformanceSuite",
    "Expectation",
    "FeatureSemanticsConformanceSuite",
    "ReferenceAsServedConformanceTest",
    "ReferenceEventTimeCompleteConformanceTest",
    "along_equator_km",
    "at",
    "at_ms",
    "failed_feature_ids",
    "identity_event",
    "located",
    "metres_east",
    "transaction",
    "tx_event",
    "unlocated",
]

# No class name here begins with `Test`, so pytest collects them only through the concrete
# subclasses in `test_feature_semantics_*.py`, the arrangement `source_adapter_suite.py` uses.
