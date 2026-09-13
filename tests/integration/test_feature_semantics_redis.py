# ruff: noqa: E501 -- the divergence table is keyed by verbatim fixture names, which cannot be wrapped
"""The Redis store against the shared feature-semantics suite, as served.

Declared acceptance command for `P2.online-features`, and part of `P3.semantics-hardening`.

Same suite file the naive reference runs, unmodified: a list scan and a mixture of sorted sets,
HyperLogLogs and hashes are asked identical questions and must give the literal answers
(ADR-0046). `integration`-marked: this uses a real Redis, never a fake -- a fake would agree with
the reference by construction and prove nothing about the store that runs (docs/TESTING.md §4).
It runs in CI's `test-integration` job, not in `make verify`.

**Not yet conformant, and said so precisely.** The fixtures the Phase 2 store fails are listed below
with the exact set of features each diverges on, and the reason:

* `store:` a defect of the Phase 2 store itself.
* `emulation:` an artefact of how this harness drives a store that has no atomic score-time read
  yet: it records the scored transaction and then reads, so profile and previous-observation reads
  include the transaction. Read before the write, as Phase 2 served, those fixtures fail differently
  or pass. The Step 1 store replaces the emulation with `score()`.

Each listed fixture is a STRICT expected failure. It still runs, a fixed one fails until its entry
is removed, and a failure on any feature set other than the recorded one -- a new divergence, or a
partial fix -- fails as `UnexpectedDivergenceError` instead of hiding inside the expected failure.
A fixture stops at its first failing check, so later checks in a listed fixture run only once it is
fixed.
"""

from __future__ import annotations

import dataclasses
import os
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import ClassVar

import pytest
from tests.conformance.feature_semantics_suite import (
    AsServedConformanceSuite,
    Expectation,
    failed_feature_ids,
)

from trace_core.contracts.canonical import CanonicalTransaction
from trace_core.domain.time import EventTime
from trace_core.features import FeatureContext
from trace_core.features.observation import Event
from trace_core.repositories.redis_features import RedisOnlineFeatureStore

pytestmark = [pytest.mark.integration, pytest.mark.parity]

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6389"))


class UnexpectedDivergenceError(Exception):
    """A divergence that is not the recorded one. Deliberately not an AssertionError, so a strict
    expected failure cannot absorb it."""


@dataclass(frozen=True, slots=True)
class Divergence:
    features: frozenset[str]
    reason: str


@pytest.fixture
def redis_client() -> Iterator[object]:
    redis = pytest.importorskip("redis", reason="the `db` extra provides the Redis client")
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        client.ping()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(
            f"SKIPPED (NOT PASSED): no Redis at {REDIS_HOST}:{REDIS_PORT} ({exc}). "
            f"Run `make up` -- this suite uses a real Redis and never a fake, because "
            f"a fake would agree with the reference implementation by construction "
            f"(docs/TESTING.md §4)."
        )
    yield client
    client.flushdb()


class TestRedisOnlineFeatureStore(AsServedConformanceSuite):
    """The Redis store must give the literal answers the reference gives."""

    NOT_YET_CONFORMANT: ClassVar[dict[str, Divergence]] = {
        "test_a_profile_ends_at_an_inactivity_gap_of_thirty_days": Divergence(
            frozenset(["account_tenure_days", "device_is_known_for_account"]),
            "store: profiles have no inactivity-gap lifetime",
        ),
        "test_a_profile_follows_event_time_not_arrival_order": Divergence(
            frozenset(["device_is_known_for_account"]),
            "store: profiles have no inactivity-gap lifetime",
        ),
        "test_a_profile_is_empty_once_the_account_has_been_quiet_for_thirty_days": Divergence(
            frozenset(["account_tenure_days", "device_is_known_for_account"]),
            "store: profiles have no inactivity-gap lifetime",
        ),
        "test_a_redelivered_scored_transaction_counts_once": Divergence(
            frozenset(["account_amount_sum_1h"]),
            "store: minute-bucket sums add every delivery of one identity",
        ),
        "test_a_redelivery_counts_once_and_the_first_delivery_is_the_observation": Divergence(
            frozenset(["account_amount_sum_1h", "account_tx_count_1m"]),
            "store: a redelivery moves the sorted-set member and adds to bucket sums",
        ),
        "test_a_redelivery_is_scored_as_its_first_delivery": Divergence(
            frozenset(["account_amount_sum_1h", "seconds_since_last_transaction"]),
            "store: sums add every delivery; emulation: the previous-observation hash holds the scored transaction",
        ),
        "test_an_exact_distinct_value_also_seen_later_in_event_time_still_counts": Divergence(
            frozenset(["account_distinct_merchants_1h"]),
            "store: an exact distinct set keeps only each value's latest time",
        ),
        "test_an_observation_at_the_scored_millisecond_is_never_the_previous_one": Divergence(
            frozenset(["geo_distance_from_last_km", "seconds_since_last_transaction"]),
            "store: the previous-observation hash holds the last write, here tx_aaa at T0",
        ),
        "test_an_unknown_device_is_only_called_unknown_by_a_store_that_would_know": Divergence(
            frozenset(["device_is_known_for_account"]),
            "emulation: profile reads include the scored transaction's device",
        ),
        "test_as_of_exactly_on_a_minute_and_bucket_boundary": Divergence(
            frozenset(["merchant_amount_cv_24h"]),
            "store: the minute-aligned same-currency CV statistics are not produced",
        ),
        "test_baselines_read_only_the_lifetime": Divergence(
            frozenset(
                [
                    "amount_zscore_vs_account",
                    "distance_from_account_home_km",
                    "merchant_is_habitual",
                ]
            ),
            "store: no lifetime; emulation: the samples include the scored transaction",
        ),
        "test_habitual_categories_read_only_the_lifetime": Divergence(
            frozenset(["mcc_is_habitual_for_account"]),
            "store: profiles have no inactivity-gap lifetime",
        ),
        "test_habitual_needs_three_observations_and_a_redelivery_is_not_one": Divergence(
            frozenset(["merchant_is_habitual"]),
            "store: visit counts include redeliveries; emulation: and the scored transaction",
        ),
        "test_home_ignores_an_outlier_and_is_a_point_the_account_visited": Divergence(
            frozenset(["distance_from_account_home_km"]),
            "store: home is the last located write, not the medoid",
        ),
        "test_home_is_computed_on_the_sphere_across_the_antimeridian": Divergence(
            frozenset(["distance_from_account_home_km"]),
            "store: home is the last located write, not the medoid",
        ),
        "test_home_is_the_medoid_of_exactly_twenty": Divergence(
            frozenset(["distance_from_account_home_km"]),
            "store: home is the last located write, not the medoid",
        ),
        "test_home_needs_three_located_observations": Divergence(
            frozenset(["distance_from_account_home_km"]),
            "store: home is the last located write, with no minimum",
        ),
        "test_home_sums_distances_in_whole_metres_rounded_half_up": Divergence(
            frozenset(["distance_from_account_home_km"]),
            "store: home is the last located write, not the medoid",
        ),
        "test_home_ties_go_to_the_earliest_time_then_the_smaller_identity": Divergence(
            frozenset(["distance_from_account_home_km"]),
            "store: home is the last located write, not the medoid",
        ),
        "test_home_uses_only_the_last_twenty_located_observations": Divergence(
            frozenset(["distance_from_account_home_km"]),
            "store: home is the last located write, not the medoid",
        ),
        "test_implied_speed_needs_both_legs_card_present": Divergence(
            frozenset(["implied_speed_kmh_from_last"]),
            "emulation: the previous-observation hash holds the scored transaction",
        ),
        "test_merchant_cv_excludes_the_partial_minute_at_the_far_edge": Divergence(
            frozenset(["merchant_amount_cv_24h"]),
            "store: the minute-aligned same-currency CV statistics are not produced",
        ),
        "test_merchant_cv_reads_same_currency_whole_minutes_plus_the_scored_transaction": Divergence(
            frozenset(["merchant_amount_cv_24h"]),
            "store: the minute-aligned same-currency CV statistics are not produced",
        ),
        "test_on_an_empty_store_every_value_is_the_scored_transaction_alone": Divergence(
            frozenset(
                [
                    "declined_ratio_1h",
                    "device_is_known_for_account",
                    "distance_from_account_home_km",
                ]
            ),
            "store: the scored transaction's own outcome is counted; emulation: its profile is read",
        ),
        "test_previous_ties_at_one_millisecond_go_to_the_greater_identity": Divergence(
            frozenset(["geo_distance_from_last_km"]),
            "store: the previous-observation hash holds the last write, not the latest",
        ),
        "test_profiles_span_currencies": Divergence(
            frozenset(
                ["account_tenure_days", "distance_from_account_home_km", "merchant_is_habitual"]
            ),
            "store: profiles are keyed by currency; emulation masks device_is_known_for_account, which joins the set once the store reads before recording",
        ),
        "test_the_declined_ratio_is_absent_when_no_earlier_outcome_is_known": Divergence(
            frozenset(["declined_ratio_1h"]),
            "store: the scored transaction's own outcome is counted",
        ),
        "test_the_declined_ratio_never_reads_the_scored_transactions_own_outcome": Divergence(
            frozenset(["declined_ratio_1h"]),
            "store: the scored transaction's own outcome is counted",
        ),
        "test_the_declined_ratio_spans_currencies": Divergence(
            frozenset(["declined_ratio_1h"]),
            "store: outcome buckets are keyed by the scored currency",
        ),
        "test_the_previous_observation_is_the_latest_and_an_older_arrival_does_not_replace_it": Divergence(
            frozenset(
                [
                    "geo_distance_from_last_km",
                    "implied_speed_kmh_from_last",
                    "seconds_since_last_transaction",
                ]
            ),
            "store: the previous-observation hash holds the last write, not the latest",
        ),
        "test_the_previous_observation_lookback_is_half_open": Divergence(
            frozenset(["seconds_since_last_transaction"]),
            "store: the previous-observation hash is not bounded by the declared half-open lookback",
        ),
        "test_the_robust_z_baseline_excludes_the_scored_transaction_and_its_millisecond": Divergence(
            frozenset(["amount_zscore_vs_account"]),
            "store: the sample includes the scored millisecond; emulation: and the scored transaction",
        ),
        "test_the_robust_z_needs_eight_amounts": Divergence(
            frozenset(["amount_zscore_vs_account"]),
            "emulation: the sample includes the scored transaction",
        ),
        "test_the_robust_z_reads_only_the_last_128_same_currency_amounts": Divergence(
            frozenset(["amount_zscore_vs_account"]),
            "emulation: the scored transaction displaces the oldest amount",
        ),
    }

    _expected: Divergence | None = None

    @pytest.fixture(autouse=True)
    def _store(self, redis_client: object, request: pytest.FixtureRequest) -> Iterator[None]:
        self._expected = self.NOT_YET_CONFORMANT.get(request.node.name)
        if self._expected is not None:
            request.applymarker(
                pytest.mark.xfail(
                    strict=True,
                    raises=AssertionError,
                    reason=f"ADR-0046, not yet conformant: {self._expected.reason}",
                )
            )
        redis_client.flushdb()  # type: ignore[attr-defined]
        self._client = redis_client
        yield
        redis_client.flushdb()  # type: ignore[attr-defined]

    def check_log(
        self,
        log: Sequence[Event],
        subject_index: int,
        subject: CanonicalTransaction,
        expectations: Sequence[Expectation],
        *,
        complete_since: EventTime | None = None,
    ) -> None:
        try:
            super().check_log(
                log, subject_index, subject, expectations, complete_since=complete_since
            )
        except AssertionError as exc:
            diverged = failed_feature_ids(exc)
            if self._expected is not None and diverged and diverged != self._expected.features:
                raise UnexpectedDivergenceError(
                    f"diverged on {sorted(diverged)}, but the recorded divergence is "
                    f"{sorted(self._expected.features)}:\n{exc}"
                ) from exc
            raise

    def context_for(
        self,
        log: Sequence[Event],
        subject_index: int,
        subject: CanonicalTransaction,
        *,
        complete_since: EventTime | None,
    ) -> FeatureContext:
        # A fresh store per build: fixtures check more than once with different completeness
        # claims, and the epoch is set with NX.
        self._client.flushdb()  # type: ignore[attr-defined]
        store = RedisOnlineFeatureStore(self._client)  # type: ignore[arg-type]
        if complete_since is not None:
            store.establish_epoch(at=complete_since)
        for event in log[:subject_index]:
            store.observe(event)
        current = log[subject_index]
        # Emulation, until the store has an atomic score-time read: record, then read. See the
        # module docstring for which divergences this produces.
        store.observe(current)
        context = store.snapshot(
            as_of=current.occurred_at,
            account_id=current.account_id,
            currency=current.currency,
            card_id=current.card_id,
            device_id=current.device_id,
            merchant_id=current.merchant_id,
            ip_id=current.ip_id,
        )
        if complete_since is None:
            # observe() stamped a wall-clock epoch; the suite asked for a store with no claim.
            return dataclasses.replace(context, complete_since=None)
        return context
