"""The shared suite every feature-store implementation must pass.

**What this is for.** ADR-0002 accepts that the same feature is computed twice --
once from Redis on the hot path, once from Spark on the warm path -- and names
silent divergence as the risk. `docs/DATA_ENGINEERING.md` §4 answers it with a
single feature definition plus a parity test. This file is the parity test's
reusable half: a set of scenarios stated in terms of *observations in, feature
values out*, with no reference to how any store works.

Three implementations run it:

| Implementation | Phase | How it builds a context |
|---|---|---|
| `ReferenceFeatureStore` | 2 | scans a list |
| `RedisOnlineFeatureStore` | 2 | sorted sets, HLL, hashes |
| Spark Gold | 3 | event-time windows over Delta |

Phase 3 adds the third by subclassing this file **unmodified** -- which is the
whole point. If proving parity required editing the suite, the suite would be
describing whatever the implementations happen to do rather than what the
features mean.

**Tolerances are declared per feature and are never widened to make a test
pass** (docs/DATA_ENGINEERING.md §4). A HyperLogLog distinct count is inexact by
construction (~0.81%, ADR-0003) and its tolerance says so; everything else is
exact, and an "almost equal" there is a bug being hidden.
"""

from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Final

from trace_core.contracts.canonical import ALWAYS_REQUIRED, CanonicalField, CanonicalTransaction
from trace_core.domain.enums import AuthorizationOutcome, TransactionChannel
from trace_core.domain.time import EventTime, event_time
from trace_core.features import FeatureContext, FeatureState
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.reference import Event
from trace_core.features.semantics import Stream

T0: Final = event_time(dt.datetime(2026, 3, 1, 12, 0, 0, tzinfo=dt.UTC))
FULL_COVERAGE: Final = frozenset(CanonicalField)
ACCOUNT: Final = "acct_000001"
OTHER_ACCOUNT: Final = "acct_000002"
DEVICE: Final = "dev_000001"
MERCHANT: Final = "mrch_00001"
CARD: Final = "card_000001"
IP: Final = "ip_00001"

EXACT: Final = 0.0
HLL_TOLERANCE: Final = 0.0081
"""HyperLogLog's documented relative error (ADR-0003). Applied to distinct
counts only, and stated as the ADR's number rather than as a value chosen to
make a run pass."""


def at(seconds: float) -> EventTime:
    """An event time offset from T0. Negative is in the past."""
    return event_time(T0 + dt.timedelta(seconds=seconds))


def tx_event(
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


def transaction(
    *,
    occurred_at: EventTime,
    account_id: str = ACCOUNT,
    amount_minor: int = 5_000,
    currency: str = "GBP",
    coverage: frozenset[CanonicalField] = FULL_COVERAGE,
    **over: object,
) -> CanonicalTransaction:
    """The transaction being scored, as the gateway would have mapped it."""
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
        source_row_id="row-1",
        field_coverage=coverage,
        transaction_id="tx_0000000001",
        account_id=account_id,
        amount_minor=amount_minor,
        currency=currency,
        occurred_at=occurred_at,
        ingested_at=occurred_at + dt.timedelta(milliseconds=40),
        **fields,  # type: ignore[arg-type]
    )


@dataclass(frozen=True, slots=True)
class Expectation:
    """One feature's expected outcome, with the tolerance it is judged at."""

    feature_id: str
    value: float | None
    """`None` means the feature must be absent -- INSUFFICIENT_HISTORY or
    UNAVAILABLE. Never 0.0, which is the failure ADR-0022 exists to prevent."""
    tolerance: float = EXACT
    state: FeatureState | None = None


class FeatureSemanticsConformanceSuite(ABC):
    """Subclass and implement `build_context` to test an implementation.

    Phase 3's Spark implementation subclasses this file unmodified.
    """

    @abstractmethod
    def build_context(
        self, history: list[Event], *, as_of: EventTime, subject: CanonicalTransaction
    ) -> FeatureContext:
        """Load `history` into the implementation and read a snapshot."""

    # -- harness ------------------------------------------------------------

    def check(
        self,
        history: list[Event],
        subject: CanonicalTransaction,
        expectations: list[Expectation],
    ) -> None:
        context = self.build_context(
            history, as_of=event_time(subject.occurred_at), subject=subject
        )
        for expected in expectations:
            spec = ONLINE_FEATURES.get(expected.feature_id)
            actual = spec.evaluate(subject, context)
            if expected.value is None:
                assert not actual.is_available, (
                    f"{expected.feature_id}: expected no value, got {actual.or_none()}. "
                    f"An absent feature must never be reported as a number (ADR-0022)."
                )
                if expected.state is not None:
                    assert actual.state is expected.state, (
                        f"{expected.feature_id}: expected {expected.state}, got {actual.state}. "
                        f"UNAVAILABLE and INSUFFICIENT_HISTORY have different causes and "
                        f"different remedies; conflating them misreports coverage."
                    )
                continue
            assert actual.is_available, (
                f"{expected.feature_id}: expected {expected.value}, got {actual.state}"
            )
            allowed = abs(expected.value) * expected.tolerance
            assert abs(actual.value - expected.value) <= allowed, (
                f"{expected.feature_id}: expected {expected.value} +/- {allowed}, "
                f"got {actual.value}"
            )

    # -- velocity -----------------------------------------------------------

    def test_window_counts_only_what_falls_inside_the_window(self) -> None:
        history = [
            tx_event(occurred_at=at(-30)),
            tx_event(occurred_at=at(-120)),
            tx_event(occurred_at=at(-1_800)),
            tx_event(occurred_at=at(-40_000)),
        ]
        self.check(
            history,
            transaction(occurred_at=T0),
            [
                Expectation("account_tx_count_1m", 1.0),
                Expectation("account_tx_count_5m", 2.0),
                Expectation("account_tx_count_1h", 3.0),
                Expectation("account_tx_count_24h", 4.0),
            ],
        )

    def test_the_window_boundary_is_half_open(self) -> None:
        """Exactly `seconds` old is OUTSIDE; both implementations must agree."""
        self.check(
            [tx_event(occurred_at=at(-60)), tx_event(occurred_at=at(-59))],
            transaction(occurred_at=T0),
            [Expectation("account_tx_count_1m", 1.0)],
        )

    def test_another_accounts_activity_is_not_counted(self) -> None:
        self.check(
            [tx_event(occurred_at=at(-10), account_id=OTHER_ACCOUNT)],
            transaction(occurred_at=T0),
            [Expectation("account_tx_count_1m", None)],
        )

    def test_out_of_order_arrival_does_not_change_the_answer(self) -> None:
        """Event time, not arrival order, decides the window (ADR-0026).

        The single most consequential property here: a Redis counter that trims
        by arrival order silently undercounts under replay, and the symptom is a
        velocity feature that quietly stops firing.
        """
        events = [tx_event(occurred_at=at(-o)) for o in (10, 200, 40, 100, 25)]
        subject = transaction(occurred_at=T0)
        expectations = [
            Expectation("account_tx_count_1m", 3.0),
            Expectation("account_tx_count_5m", 5.0),
        ]
        self.check(events, subject, expectations)
        self.check(list(reversed(events)), subject, expectations)

    # -- amounts ------------------------------------------------------------

    def test_amount_sums_are_partitioned_by_currency(self) -> None:
        """No FX source exists, so converting would invent a number."""
        history = [
            tx_event(occurred_at=at(-100), amount_minor=1_000, currency="GBP"),
            tx_event(occurred_at=at(-200), amount_minor=9_999, currency="EUR"),
        ]
        self.check(
            history,
            transaction(occurred_at=T0, currency="GBP"),
            [Expectation("account_amount_sum_1h", 1_000.0)],
        )

    def test_declined_ratio_uses_only_known_outcomes(self) -> None:
        history = [
            tx_event(occurred_at=at(-10), authorization_outcome=AuthorizationOutcome.DECLINED),
            tx_event(occurred_at=at(-20), authorization_outcome=AuthorizationOutcome.DECLINED),
            tx_event(occurred_at=at(-30), authorization_outcome=AuthorizationOutcome.APPROVED),
            tx_event(occurred_at=at(-40), authorization_outcome=None),
        ]
        self.check(
            history,
            transaction(occurred_at=T0),
            [Expectation("declined_ratio_1h", 2.0 / 3.0)],
        )

    def test_declined_ratio_is_absent_rather_than_zero_when_no_outcome_is_known(self) -> None:
        """Zero known outcomes is not a zero ratio -- it is an unmeasured one."""
        self.check(
            [tx_event(occurred_at=at(-10), authorization_outcome=None)],
            transaction(occurred_at=T0),
            [Expectation("declined_ratio_1h", None, state=FeatureState.INSUFFICIENT_HISTORY)],
        )

    def test_merchant_amount_uniformity_is_low_when_amounts_are_identical(self) -> None:
        """The MERCHANT_COLLUSION tell: uniform amounts, not large ones."""
        history = [tx_event(occurred_at=at(-i * 100), amount_minor=25_000) for i in range(1, 6)]
        self.check(
            history,
            transaction(occurred_at=T0),
            [Expectation("merchant_amount_cv_24h", 0.0, tolerance=1e-9)],
        )

    # -- distinct cardinality ----------------------------------------------

    def test_distinct_counts_deduplicate(self) -> None:
        history = [
            tx_event(occurred_at=at(-10), merchant_id="mrch_00001"),
            tx_event(occurred_at=at(-20), merchant_id="mrch_00001"),
            tx_event(occurred_at=at(-30), merchant_id="mrch_00002"),
        ]
        self.check(
            history,
            transaction(occurred_at=T0),
            [Expectation("account_distinct_merchants_1h", 2.0, tolerance=HLL_TOLERANCE)],
        )

    def test_a_device_shared_across_accounts_is_visible_from_the_device(self) -> None:
        """The device-farm signal is a property of the DEVICE, not the account."""
        history = [
            tx_event(occurred_at=at(-100 * i), account_id=f"acct_00000{i}") for i in range(1, 6)
        ]
        self.check(
            history,
            transaction(occurred_at=T0),
            [Expectation("device_distinct_accounts_24h", 5.0, tolerance=HLL_TOLERANCE)],
        )

    def test_an_approximately_stored_distinct_count_is_read_back(self) -> None:
        """The APPROXIMATE storage class, exercised positively (ADR-0034).

        Kept at low cardinality deliberately: HyperLogLog is exact in its sparse
        encoding there, so both implementations must agree EXACTLY and this test
        measures the wiring rather than the estimator. The estimator's error is
        characterised across the cardinality range by the benchmark, which is the
        right instrument for it -- a tolerance chosen to make a unit test pass
        would be a number nobody measured.
        """
        history = [
            tx_event(occurred_at=at(-60 * i), account_id=f"acct_00000{i}") for i in range(1, 5)
        ]
        self.check(
            history,
            transaction(occurred_at=T0),
            [
                Expectation("merchant_distinct_accounts_1h", 4.0),
                Expectation("ip_distinct_accounts_1h", 4.0),
            ],
        )

    def test_approximate_values_declare_themselves_approximate(self) -> None:
        """Callers, tests, observability and Phase 3 parity all need to know a
        value is an estimate. Carried on the VALUE, not on the response, so it
        survives being logged, stored and compared."""
        history = [
            tx_event(occurred_at=at(-60 * i), account_id=f"acct_00000{i}") for i in range(1, 5)
        ]
        subject = transaction(occurred_at=T0)
        ctx = self.build_context(history, as_of=T0, subject=subject)
        approximate = ONLINE_FEATURES.get("merchant_distinct_accounts_1h").evaluate(subject, ctx)
        exact = ONLINE_FEATURES.get("account_distinct_merchants_1h").evaluate(subject, ctx)
        assert approximate.is_available and approximate.approximate
        assert exact.is_available and not exact.approximate

    # -- profiles -----------------------------------------------------------

    def test_robust_z_is_absent_until_there_is_enough_history(self) -> None:
        """Not zero. A new account is not a normal-spending account."""
        history = [tx_event(occurred_at=at(-3_600 * i), amount_minor=5_000) for i in range(1, 4)]
        self.check(
            history,
            transaction(occurred_at=T0, amount_minor=900_000),
            [
                Expectation(
                    "amount_zscore_vs_account", None, state=FeatureState.INSUFFICIENT_HISTORY
                )
            ],
        )

    def test_robust_z_is_large_for_an_amount_far_outside_the_account_baseline(self) -> None:
        history = [
            tx_event(occurred_at=at(-3_600 * i), amount_minor=5_000 + i) for i in range(1, 13)
        ]
        context_tx = transaction(occurred_at=T0, amount_minor=900_000)
        ctx = self.build_context(history, as_of=T0, subject=context_tx)
        value = ONLINE_FEATURES.get("amount_zscore_vs_account").evaluate(context_tx, ctx)
        assert value.is_available
        assert value.value > 10.0, f"expected a large robust z, got {value.value}"

    def test_a_transaction_does_not_contribute_to_the_profile_it_is_scored_against(self) -> None:
        """Leakage, and it would make every transaction look normal vs itself."""
        history = [tx_event(occurred_at=at(-3_600 * i), amount_minor=5_000) for i in range(1, 13)]
        subject = transaction(occurred_at=T0, amount_minor=900_000)
        ctx = self.build_context(
            [*history, tx_event(occurred_at=T0, amount_minor=900_000)], as_of=T0, subject=subject
        )
        value = ONLINE_FEATURES.get("amount_zscore_vs_account").evaluate(subject, ctx)
        assert value.is_available
        assert value.value > 10.0, (
            "the scored transaction entered its own baseline, which hides exactly the "
            "anomaly the feature exists to detect"
        )

    def test_a_never_seen_device_is_not_known(self) -> None:
        history = [tx_event(occurred_at=at(-3_600), device_id="dev_000009")]
        self.check(
            history,
            transaction(occurred_at=T0, device_id="dev_000123"),
            [Expectation("device_is_known_for_account", 0.0)],
        )

    def test_device_novelty_is_absent_rather_than_zero_for_a_first_transaction(self) -> None:
        """With no history at all, "novel device" is not a measurement."""
        self.check(
            [],
            transaction(occurred_at=T0),
            [
                Expectation(
                    "device_is_known_for_account", None, state=FeatureState.INSUFFICIENT_HISTORY
                )
            ],
        )

    def test_a_repeatedly_used_merchant_becomes_habitual(self) -> None:
        history = [tx_event(occurred_at=at(-3_600 * i)) for i in range(1, 6)]
        self.check(
            history,
            transaction(occurred_at=T0),
            [Expectation("merchant_is_habitual", 1.0)],
        )

    # -- pairwise -----------------------------------------------------------

    def test_implied_speed_needs_both_legs_card_present(self) -> None:
        """A card-not-present leg has an innocent explanation; counting it would
        make ordinary e-commerce look like supersonic travel."""
        far_away = {"latitude": 40.7, "longitude": -74.0}
        history = [
            tx_event(
                occurred_at=at(-3_600),
                channel=TransactionChannel.CARD_NOT_PRESENT,
                **far_away,  # type: ignore[arg-type]
            )
        ]
        self.check(
            history,
            transaction(occurred_at=T0),
            [Expectation("implied_speed_kmh_from_last", 0.0)],
        )

    def test_implied_speed_is_high_for_two_distant_card_present_legs(self) -> None:
        history = [
            tx_event(
                occurred_at=at(-3_600),
                latitude=40.7,
                longitude=-74.0,
                channel=TransactionChannel.CARD_PRESENT,
            )
        ]
        subject = transaction(occurred_at=T0)
        ctx = self.build_context(history, as_of=T0, subject=subject)
        value = ONLINE_FEATURES.get("implied_speed_kmh_from_last").evaluate(subject, ctx)
        assert value.is_available
        assert value.value > 5_000, f"London to New York in an hour, got {value.value} km/h"

    def test_geo_features_are_unavailable_without_coverage_not_zero(self) -> None:
        """The ADR-0022 headline, on the production feature set.

        A source with no geography -- IEEE-CIS -- must produce UNAVAILABLE, which
        is a different state from "this account has no history yet".
        """
        coverage = frozenset(ALWAYS_REQUIRED) | {CanonicalField.CHANNEL}
        self.check(
            [tx_event(occurred_at=at(-100))],
            transaction(occurred_at=T0, coverage=coverage),
            [
                Expectation("geo_distance_from_last_km", None, state=FeatureState.UNAVAILABLE),
                Expectation("implied_speed_kmh_from_last", None, state=FeatureState.UNAVAILABLE),
                Expectation("distance_from_account_home_km", None, state=FeatureState.UNAVAILABLE),
            ],
        )

    # -- identity streams ---------------------------------------------------

    def test_failed_logins_are_counted_from_the_identity_stream(self) -> None:
        history = [
            Event(
                stream=Stream.IDENTITY_FAILED_LOGIN,
                occurred_at=at(-60 * i),
                account_id=ACCOUNT,
            )
            for i in range(1, 6)
        ]
        self.check(
            history,
            transaction(occurred_at=T0),
            [Expectation("failed_logins_1h", 5.0)],
        )

    def test_an_identity_change_is_not_counted_as_a_failed_login(self) -> None:
        """Distinct streams, because they mean different things."""
        history = [Event(stream=Stream.IDENTITY_CHANGE, occurred_at=at(-60), account_id=ACCOUNT)]
        self.check(
            history,
            transaction(occurred_at=T0),
            [
                Expectation("failed_logins_1h", None),
                Expectation("hours_since_identity_change", 1.0 / 60.0, tolerance=1e-6),
            ],
        )

    # -- the invariant that outranks all of the above -----------------------

    def test_no_feature_is_ever_silently_zero_on_an_empty_store(self) -> None:
        """With nothing observed, every feature must be ABSENT, not 0.0.

        This is the single assertion that would catch a whole class of quiet
        regressions: one `.get(key, 0)` anywhere in an implementation turns a
        cold store into a confident "no risk detected" on every transaction.
        """
        subject = transaction(occurred_at=T0)
        ctx = self.build_context([], as_of=T0, subject=subject)
        for spec in ONLINE_FEATURES:
            value = spec.evaluate(subject, ctx)
            assert not value.is_available, (
                f"{spec.feature_id} returned {value.or_none()} from an EMPTY store. "
                f"With no observations there is nothing to measure, and a number here "
                f"would be fabricated (ADR-0022)."
            )


class ReferenceStoreConformanceTest(FeatureSemanticsConformanceSuite):
    """The naive implementation, as the first subject of the shared suite."""

    def build_context(
        self, history: list[Event], *, as_of: EventTime, subject: CanonicalTransaction
    ) -> FeatureContext:
        from trace_core.features.reference import ReferenceFeatureStore

        store = ReferenceFeatureStore()
        store.observe_all(history)
        return store.snapshot(
            as_of=as_of,
            account_id=subject.account_id,
            currency=subject.currency,
            card_id=subject.card_id,
            device_id=subject.device_id,
            merchant_id=subject.merchant_id,
            ip_id=subject.ip_id,
        )


__all__ = [
    "EXACT",
    "HLL_TOLERANCE",
    "T0",
    "Expectation",
    "FeatureSemanticsConformanceSuite",
    "ReferenceStoreConformanceTest",
    "at",
    "transaction",
    "tx_event",
]

# Neither class name begins with `Test`, so pytest collects neither of them
# directly -- they are collected only through the concrete subclasses in
# `test_feature_semantics_*.py`, which is the same arrangement
# `source_adapter_suite.py` uses.
