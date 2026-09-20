# ruff: noqa: E501 -- the mutant table quotes source lines verbatim; wrapping them would change what they match
"""Every declared rule has a literal fixture that fails when the rule is broken.

A suite that passes the reference proves little until it is shown to fail an implementation that
is wrong in exactly one declared way. Each mutant below breaks one rule of ADR-0046 in a copy of
the reference implementation, of its shared arithmetic (`profile_math`) or of the feature
definitions, and names the fixture that exists to catch it. The test runs that fixture against
the mutant and requires it to fail with an ASSERTION: a mutant that crashes a fixture has not
been caught by the rule the fixture pins, and surfaces as an error instead. A mutant whose target
text is no longer in the source fails loudly too, so the list cannot silently go stale.

Two further tests swap whole implementations: an as-served replay computed over a complete
history, or as a plain range over `occurred_at`, and a complete history computed as served --
the shortcuts a Phase 3 replay or Gold job is most likely to take.
"""

from __future__ import annotations

import itertools
import sys
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Final

import pytest
import tests.conformance.feature_semantics_suite as suite_module
from tests.conformance.feature_semantics_suite import (
    AsServedConformanceSuite,
    EventTimeCompleteConformanceSuite,
    failed_feature_ids,
)

import trace_core.features.reference as reference
from trace_core.contracts.canonical import CanonicalTransaction
from trace_core.domain.time import EventTime
from trace_core.features import FeatureContext
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.observation import Event

pytestmark = pytest.mark.conformance

FEATURES: Final = Path(reference.__file__).parent
_NAMES = itertools.count()

Build = Callable[[Sequence[Event], int, "EventTime | None"], FeatureContext]
Edit = tuple[str, str, str]


@dataclass(frozen=True, slots=True)
class Mutant:
    name: str
    edits: tuple[Edit, ...]
    """(module, exact text, replacement): module is `reference`, `profile_math` or `definitions`."""
    catches: str
    modes: tuple[str, ...] = ("served", "complete")


@dataclass(frozen=True, slots=True)
class Mutated:
    reference: Any
    registry: Any
    """The feature registry the fixtures evaluate through: the mutant's, if `definitions` was
    mutated, else the real one."""


MUTANTS: Final[tuple[Mutant, ...]] = (
    Mutant(
        "the scored transaction left out of its own windows",
        (
            (
                "reference",
                "        return event.occurred_ms <= read.as_of_ms\n",
                "        return event.occurred_ms <= read.as_of_ms and event.identity != read.identity\n",
            ),
            ("reference", "    return event.order_key <= key", "    return event.order_key < key"),
        ),
        "test_the_scored_transaction_counts_in_its_own_windows",
    ),
    Mutant(
        "the scored transaction's own authorization outcome counted",
        (
            (
                "reference",
                "        and e.event_id != own\n",
                "",
            ),
        ),
        "test_the_declined_ratio_never_reads_the_scored_transactions_own_outcome",
    ),
    Mutant(
        "an outcome reported for another account counted",
        (
            (
                "reference",
                "    if account != outcome.account_id:\n        return Verification.REJECTED\n",
                "",
            ),
        ),
        "test_an_outcome_reported_for_another_account_never_counts",
    ),
    Mutant(
        "a pending outcome counted",
        (
            (
                "reference",
                "        return Verification.PENDING\n",
                "        return Verification.VERIFIED\n",
            ),
        ),
        "test_a_pending_outcome_never_counts",
    ),
    Mutant(
        "an outcome decided at as_of counted",
        (
            (
                "reference",
                "        if lower_ms < e.occurred_ms < read.as_of_ms\n",
                "        if lower_ms < e.occurred_ms <= read.as_of_ms\n",
            ),
        ),
        "test_the_outcome_window_is_open_at_both_edges",
    ),
    Mutant(
        "an observation exactly a window old counted",
        (("reference", "if event.occurred_ms <= lower_ms:", "if event.occurred_ms < lower_ms:"),),
        "test_an_observation_exactly_a_window_old_is_outside_and_a_millisecond_younger_inside",
    ),
    Mutant(
        "as_of rounded to the millisecond instead of floored",
        (
            (
                "reference",
                "            as_of_ms=current.occurred_ms,",
                "            as_of_ms=round(current.occurred_at.timestamp() * 1000),",
            ),
            (
                "reference",
                "        as_of_ms=anchor.occurred_ms,",
                "        as_of_ms=round(anchor.occurred_at.timestamp() * 1000),",
            ),
        ),
        "test_event_time_is_floored_to_the_millisecond_before_any_comparison",
    ),
    Mutant(
        "the merchant CV reading the minute that contains as_of",
        (
            (
                "reference",
                "< e.occurred_ms // ALIGNED_MINUTE_MS < upper_minute",
                "< e.occurred_ms // ALIGNED_MINUTE_MS <= upper_minute",
            ),
        ),
        "test_merchant_cv_reads_same_currency_whole_minutes_plus_the_scored_transaction",
    ),
    Mutant(
        "the merchant CV reading the partial minute at the far edge",
        (("reference", "and lower_minute < e.occurred_ms", "and lower_minute <= e.occurred_ms"),),
        "test_merchant_cv_excludes_the_partial_minute_at_the_far_edge",
    ),
    Mutant(
        "the merchant CV without the scored transaction",
        (("reference", "        aligned.append(current)", "        pass"),),
        "test_merchant_cv_reads_same_currency_whole_minutes_plus_the_scored_transaction",
    ),
    Mutant(
        "the merchant CV across currencies",
        (
            (
                "reference",
                "        if e.currency == read.currency\n        and e.identity != read.identity",
                "        if e.identity != read.identity",
            ),
        ),
        "test_merchant_cv_reads_same_currency_whole_minutes_plus_the_scored_transaction",
    ),
    Mutant(
        "a merchant CV from one observation",
        (("definitions", "state.aligned_count < 2", "state.aligned_count < 1"),),
        "test_merchant_cv_needs_two_same_currency_observations",
    ),
    Mutant(
        "a merchant CV of zero when the mean is zero",
        (
            (
                "definitions",
                "        if mean == 0:\n            return INSUFFICIENT_HISTORY",
                "        if mean == 0:\n            return 0.0",
            ),
        ),
        "test_merchant_cv_is_absent_when_the_mean_is_zero",
    ),
    Mutant(
        "a declined ratio of zero when no outcome is known",
        (
            (
                "definitions",
                "        if state is None or state.outcome_known_count == 0:",
                "        if state is None:",
            ),
            (
                "definitions",
                "        return state.declined_count / state.outcome_known_count",
                "        return state.declined_count / state.outcome_known_count if state.outcome_known_count else 0.0",
            ),
        ),
        "test_the_declined_ratio_is_absent_when_no_earlier_outcome_is_known",
    ),
    Mutant(
        "approximate buckets exclusive at the far edge",
        (
            (
                "reference",
                "if first <= e.occurred_ms // APPROXIMATE_BUCKET_MS <= last",
                "if first < e.occurred_ms // APPROXIMATE_BUCKET_MS <= last",
            ),
        ),
        "test_approximate_distinct_counts_use_the_declared_edge_inclusive_buckets",
    ),
    Mutant(
        "approximate distinct counts over the exact window",
        (
            (
                "reference",
                "e for e in observations if first <= e.occurred_ms // APPROXIMATE_BUCKET_MS <= last\n",
                "e for e in members\n",
            ),
        ),
        "test_approximate_distinct_counts_use_the_declared_edge_inclusive_buckets",
    ),
    Mutant(
        "exact distinct counts confined to one currency",
        (
            (
                "reference",
                "        else:\n            pool = members",
                "        else:\n            pool = same_currency",
            ),
        ),
        "test_exact_distinct_counts_span_currencies",
    ),
    Mutant(
        "baselines that include the scored millisecond",
        (
            (
                "reference",
                "if e.occurred_ms < read.as_of_ms and e.identity != read.identity),",
                "if e.occurred_ms <= read.as_of_ms and e.identity != read.identity),",
            ),
        ),
        "test_the_robust_z_baseline_excludes_the_scored_transaction_and_its_millisecond",
    ),
    Mutant(
        "profiles keyed by the scored currency",
        (
            (
                "reference",
                "if e.occurred_ms < read.as_of_ms and e.identity != read.identity),",
                "if e.occurred_ms < read.as_of_ms and e.identity != read.identity and e.currency == read.currency),",
            ),
        ),
        "test_profiles_span_currencies",
    ),
    Mutant(
        "the lifetime walked in arrival order",
        (
            ("reference", "    history = sorted(\n", "    history = list(\n"),
            (
                "reference",
                "        key=lambda e: e.order_key,\n    )\n    if not history",
                "    )\n    if not history",
            ),
        ),
        "test_a_profile_follows_event_time_not_arrival_order",
    ),
    Mutant(
        "a lifetime broken only by a gap longer than 30 days",
        (
            (
                "reference",
                "history[index - 1].occurred_ms >= _LIFETIME_GAP_MS",
                "history[index - 1].occurred_ms > _LIFETIME_GAP_MS",
            ),
        ),
        "test_a_profile_ends_at_an_inactivity_gap_of_thirty_days",
    ),
    Mutant(
        "a profile surviving exactly 30 quiet days",
        (
            (
                "reference",
                "read.as_of_ms - history[-1].occurred_ms >= _LIFETIME_GAP_MS",
                "read.as_of_ms - history[-1].occurred_ms > _LIFETIME_GAP_MS",
            ),
        ),
        "test_a_profile_is_empty_once_the_account_has_been_quiet_for_thirty_days",
    ),
    Mutant(
        "the amount sample read over all history",
        (
            (
                "reference",
                "    amounts = [abs(e.amount_minor) for e in lifetime if e.currency == read.currency]",
                "    amounts = [abs(e.amount_minor) for e in history if e.currency == read.currency]",
            ),
        ),
        "test_baselines_read_only_the_lifetime",
    ),
    Mutant(
        "the home sample read over all history",
        (
            (
                "reference",
                "        for e in lifetime\n        if e.latitude is not None and e.longitude is not None",
                "        for e in history\n        if e.latitude is not None and e.longitude is not None",
            ),
        ),
        "test_baselines_read_only_the_lifetime",
    ),
    Mutant(
        "habitual merchants read over all history",
        (
            (
                "reference",
                "_habitual(e.merchant_id for e in lifetime)",
                "_habitual(e.merchant_id for e in history)",
            ),
        ),
        "test_baselines_read_only_the_lifetime",
    ),
    Mutant(
        "a robust z sample of 127",
        (("reference", "amounts[-AMOUNT_SAMPLE_SIZE:]", "amounts[-(AMOUNT_SAMPLE_SIZE - 1):]"),),
        "test_the_robust_z_reads_only_the_last_128_same_currency_amounts",
    ),
    Mutant(
        "a robust z sample of 129",
        (("reference", "amounts[-AMOUNT_SAMPLE_SIZE:]", "amounts[-(AMOUNT_SAMPLE_SIZE + 1):]"),),
        "test_the_robust_z_reads_only_the_last_128_same_currency_amounts",
    ),
    Mutant(
        "a robust z sample across currencies",
        (("reference", "for e in lifetime if e.currency == read.currency]", "for e in lifetime]"),),
        "test_the_robust_z_reads_only_the_last_128_same_currency_amounts",
    ),
    Mutant(
        "a robust z from seven amounts",
        (
            (
                "profile_math",
                "    if len(amounts) < MIN_OBSERVATIONS_FOR_ROBUST_Z:",
                "    if len(amounts) < MIN_OBSERVATIONS_FOR_ROBUST_Z - 1:",
            ),
            (
                "definitions",
                "profile.observation_count < MIN_OBSERVATIONS_FOR_ROBUST_Z",
                "profile.observation_count < MIN_OBSERVATIONS_FOR_ROBUST_Z - 1",
            ),
        ),
        "test_the_robust_z_needs_eight_amounts",
    ),
    Mutant(
        "an uncapped robust z",
        (
            (
                "definitions",
                "        return max(-ROBUST_Z_CAP, min(ROBUST_Z_CAP, deviation / scale))",
                "        return deviation / scale",
            ),
        ),
        "test_the_robust_z_is_capped_at_fifty",
    ),
    Mutant(
        "a zero MAD that loses the deviation's sign",
        (("definitions", "math.copysign(ROBUST_Z_CAP, deviation)", "ROBUST_Z_CAP"),),
        "test_a_zero_mad_keeps_the_sign_of_the_deviation",
    ),
    Mutant(
        "a home sample of 19",
        (("reference", "][-HOME_SAMPLE_SIZE:]", "][-(HOME_SAMPLE_SIZE - 1):]"),),
        "test_home_is_the_medoid_of_exactly_twenty",
    ),
    Mutant(
        "a home sample of 21",
        (("reference", "][-HOME_SAMPLE_SIZE:]", "][-(HOME_SAMPLE_SIZE + 1):]"),),
        "test_home_uses_only_the_last_twenty_located_observations",
    ),
    Mutant(
        "unlocated observations taking home sample slots",
        (
            (
                "reference",
                "        for e in lifetime\n        if e.latitude is not None and e.longitude is not None\n    ][-HOME_SAMPLE_SIZE:]",
                "        for e in lifetime[-HOME_SAMPLE_SIZE:]\n        if e.latitude is not None and e.longitude is not None\n    ]",
            ),
        ),
        "test_home_uses_only_the_last_twenty_located_observations",
    ),
    Mutant(
        "medoid ties going to the latest observation",
        (
            (
                "profile_math",
                "if best is None or key < best:",
                "if best is None or key[0] < best[0] or (key[0] == best[0] and key[1:] > best[1:]):",
            ),
        ),
        "test_home_ties_go_to_the_earliest_time_then_the_smaller_identity",
    ),
    Mutant(
        "a component-wise median as home",
        (
            (
                "profile_math",
                "    return located[chosen].latitude, located[chosen].longitude",
                "    return statistics.median(p[2] for p in points), statistics.median(p[3] for p in points)",
            ),
        ),
        "test_home_is_computed_on_the_sphere_across_the_antimeridian",
    ),
    Mutant(
        "home from two located observations",
        (
            (
                "profile_math",
                "    if len(points) < HOME_MIN_OBSERVATIONS:",
                "    if len(points) < HOME_MIN_OBSERVATIONS - 1:",
            ),
        ),
        "test_home_needs_three_located_observations",
    ),
    Mutant(
        "medoid distances summed unrounded",
        (
            (
                "profile_math",
                "whole_metres(haversine_km(located[i], located[j]))",
                "haversine_km(located[i], located[j])",
            ),
        ),
        "test_home_sums_distances_in_whole_metres_rounded_half_up",
    ),
    Mutant(
        "medoid distances rounded down",
        (
            (
                "profile_math",
                "    return math.floor(km * _METRES_PER_KM + 0.5)",
                "    return math.floor(km * _METRES_PER_KM)",
            ),
        ),
        "test_home_sums_distances_in_whole_metres_rounded_half_up",
    ),
    Mutant(
        "previous-observation ties going to the last delivered",
        (
            (
                "reference",
                "    latest = max(earlier, key=lambda e: e.order_key)",
                "    latest = max(reversed(earlier), key=lambda e: e.occurred_ms)",
            ),
        ),
        "test_previous_ties_at_one_millisecond_go_to_the_greater_identity",
    ),
    Mutant(
        "previous-observation ties going to the first delivered",
        (
            (
                "reference",
                "    latest = max(earlier, key=lambda e: e.order_key)",
                "    latest = max(earlier, key=lambda e: e.occurred_ms)",
            ),
        ),
        "test_previous_ties_at_one_millisecond_go_to_the_greater_identity",
    ),
    Mutant(
        "the last delivered as the previous observation",
        (
            (
                "reference",
                "    latest = max(earlier, key=lambda e: e.order_key)",
                "    latest = earlier[-1]",
            ),
        ),
        "test_the_previous_observation_is_the_latest_and_an_older_arrival_does_not_replace_it",
    ),
    Mutant(
        "a previous observation at the scored millisecond",
        (
            (
                "reference",
                "        if lower_ms < e.occurred_ms < read.as_of_ms and e.identity != read.identity\n",
                "        if lower_ms < e.occurred_ms <= read.as_of_ms and e.identity != read.identity\n",
            ),
        ),
        "test_an_observation_at_the_scored_millisecond_is_never_the_previous_one",
    ),
    Mutant(
        "a previous-observation lookback closed at the far end",
        (
            (
                "reference",
                "        if lower_ms < e.occurred_ms < read.as_of_ms and e.identity != read.identity\n",
                "        if lower_ms <= e.occurred_ms < read.as_of_ms and e.identity != read.identity\n",
            ),
        ),
        "test_the_previous_observation_lookback_is_half_open",
    ),
    Mutant(
        "a redelivery replacing the first delivery in the store",
        (
            (
                "reference",
                "            return ObserveReceipt(\n                position=len(self._log),\n                recorded=False,",
                "            self._log[self._log.index(self._recorded[event.identity])] = event\n            self._recorded[event.identity] = event\n            return ObserveReceipt(\n                position=len(self._log),\n                recorded=False,",
            ),
        ),
        "test_a_redelivery_counts_once_and_the_first_delivery_is_the_observation",
        modes=("served",),
    ),
    Mutant(
        "a redelivery replacing the first delivery in a complete history",
        (
            (
                "reference",
                "        first.setdefault(event.identity, event)",
                "        first[event.identity] = event",
            ),
        ),
        "test_a_redelivery_counts_once_and_the_first_delivery_is_the_observation",
        modes=("complete",),
    ),
    Mutant(
        "a store that does not deduplicate",
        (
            (
                "reference",
                "        if (first := self._recorded.get(event.identity)) is not None:\n",
                "        if (first := self._recorded.get(event.identity)) is not None and False:\n",
            ),
        ),
        "test_a_redelivery_counts_once_and_the_first_delivery_is_the_observation",
        modes=("served",),
    ),
    Mutant(
        "a redelivery read at its own time in the store",
        (
            (
                "reference",
                "            as_of_ms=current.occurred_ms,\n            currency=current.currency,\n            ids=_ids(current),",
                "            as_of_ms=event.occurred_ms,\n            currency=event.currency,\n            ids=_ids(event),",
            ),
        ),
        "test_a_redelivery_is_scored_as_its_first_delivery",
        modes=("served",),
    ),
    Mutant(
        "a redelivery read at its own time in a complete history",
        (
            (
                "reference",
                "    anchor = subject if current is None else current",
                "    anchor = subject",
            ),
        ),
        "test_a_redelivery_is_scored_as_its_first_delivery",
        modes=("complete",),
    ),
    Mutant(
        "identities shared across streams in the store",
        (
            (
                "reference",
                "        if (first := self._recorded.get(event.identity)) is not None:\n",
                "        if (first := self._recorded.get(event.event_id)) is not None:\n",
            ),
            (
                "reference",
                "        self._recorded[event.identity] = event\n",
                "        self._recorded[event.event_id] = event\n",
            ),
            (
                "reference",
                "        current = self._recorded[event.identity]\n",
                "        current = self._recorded[event.event_id]\n",
            ),
        ),
        "test_an_identity_event_cannot_swallow_a_transaction_with_the_same_id",
        modes=("served",),
    ),
    Mutant(
        "identities shared across streams in a complete history",
        (
            (
                "reference",
                "        first.setdefault(event.identity, event)",
                "        first.setdefault(event.event_id, event)",
            ),
            (
                "reference",
                "    current = first.get(subject.identity)",
                "    current = first.get(subject.event_id)",
            ),
        ),
        "test_an_identity_event_cannot_swallow_a_transaction_with_the_same_id",
        modes=("complete",),
    ),
    Mutant(
        "a complete history counting every observation at the scored millisecond",
        (
            (
                "reference",
                "    return event.order_key <= key",
                "    return event.occurred_ms <= read.as_of_ms",
            ),
        ),
        "test_same_millisecond_ties_break_by_identity_whatever_the_arrival_order",
        modes=("complete",),
    ),
    Mutant(
        "a complete history breaking ties the wrong way",
        (
            (
                "reference",
                "    return event.order_key <= key",
                "    return event.occurred_ms < read.as_of_ms or event.order_key >= key",
            ),
        ),
        "test_same_millisecond_ties_break_by_identity_whatever_the_arrival_order",
        modes=("complete",),
    ),
    Mutant(
        "habitual after two visits",
        (("reference", "if n >= HABITUAL_MIN_VISITS)", "if n >= HABITUAL_MIN_VISITS - 1)"),),
        "test_habitual_needs_three_observations_and_a_redelivery_is_not_one",
    ),
    Mutant(
        "known devices from all history rather than the lifetime",
        (
            (
                "reference",
                "known_devices=frozenset(e.device_id for e in lifetime",
                "known_devices=frozenset(e.device_id for e in history",
            ),
        ),
        "test_a_profile_ends_at_an_inactivity_gap_of_thirty_days",
    ),
    Mutant(
        "tenure from all history rather than the lifetime",
        (
            (
                "reference",
                "first_seen_at=EventTime(from_millis(lifetime[0].occurred_ms))",
                "first_seen_at=EventTime(from_millis(history[0].occurred_ms))",
            ),
        ),
        "test_a_profile_ends_at_an_inactivity_gap_of_thirty_days",
    ),
    Mutant(
        "transaction counts confined to the scored currency",
        (
            (
                "reference",
                "        count=len(members),\n        amount_sum_minor=",
                "        count=len([e for e in members if e.stream is not Stream.TRANSACTION or e.currency == read.currency]),\n        amount_sum_minor=",
            ),
        ),
        "test_transaction_counts_span_currencies",
    ),
    Mutant(
        "habitual categories read over all history",
        (
            (
                "reference",
                "_habitual(e.merchant_mcc for e in lifetime)",
                "_habitual(e.merchant_mcc for e in history)",
            ),
        ),
        "test_habitual_categories_read_only_the_lifetime",
    ),
    Mutant(
        "transactions sorting before identity events at one millisecond",
        (
            (
                "reference",
                "    return event.order_key <= key",
                '    return (event.occurred_ms, event.namespace.value == "identity_event", event.event_id) <= (key[0], key[1] == "identity_event", key[2])',
            ),
        ),
        "test_identity_events_at_the_scored_millisecond_sort_before_it",
        modes=("complete",),
    ),
    Mutant(
        "a redelivery read only up to its first delivery's position",
        (
            (
                "reference",
                "            self._log[: receipt.position],",
                "            self._log[: self._log.index(current) + 1],",
            ),
        ),
        "test_a_redelivered_scored_transaction_counts_once",
        modes=("served",),
    ),
)


def _load(name: str, source: str) -> ModuleType:
    module = ModuleType(name)
    module.__file__ = f"<{name}>"
    sys.modules[name] = module
    exec(compile(source, module.__file__, "exec"), module.__dict__)  # noqa: S102 -- our own source
    return module


@pytest.fixture
def mutate() -> Iterator[Callable[[tuple[Edit, ...]], Mutated]]:
    loaded: list[str] = []

    def build(edits: tuple[Edit, ...]) -> Mutated:
        sources = {
            name: (FEATURES / f"{name}.py").read_text()
            for name in ("reference", "profile_math", "definitions")
        }
        for module, old, new in edits:
            found = sources[module].count(old)
            assert found == 1, (
                f"mutation target appears {found} times in {module}.py, not once -- the source "
                f"moved and this mutant no longer breaks what it names: {old!r}"
            )
            sources[module] = sources[module].replace(old, new)
        suffix = next(_NAMES)
        math_name = f"_mutant_profile_math_{suffix}"
        ref_name = f"_mutant_reference_{suffix}"
        import_line = "from trace_core.features.profile_math import"
        assert sources["reference"].count(import_line) == 1
        loaded.extend([math_name, ref_name])
        _load(math_name, sources["profile_math"])
        mutated_reference = _load(
            ref_name, sources["reference"].replace(import_line, f"from {math_name} import")
        )
        registry: Any = ONLINE_FEATURES
        if any(target == "definitions" for target, _, _ in edits):
            definitions_name = f"_mutant_definitions_{suffix}"
            loaded.append(definitions_name)
            registry = _load(definitions_name, sources["definitions"]).ONLINE_FEATURES
        return Mutated(reference=mutated_reference, registry=registry)

    yield build
    for name in loaded:
        sys.modules.pop(name, None)


def _served_by(module: Any) -> Build:
    def build(log: Sequence[Event], index: int, complete_since: EventTime | None) -> FeatureContext:
        store = module.ReferenceFeatureStore(complete_since=complete_since)
        store.observe_all(log[:index])
        context: FeatureContext = store.score(log[index]).context
        return context

    return build


def _complete_by(module: Any) -> Build:
    def build(log: Sequence[Event], index: int, complete_since: EventTime | None) -> FeatureContext:
        context: FeatureContext = module.event_time_complete_context(
            log, log[index], complete_since=complete_since
        )
        return context

    return build


def _fails(suite: type[Any], build: Build, fixture: str, registry: Any) -> AssertionError | None:
    """Run one fixture of an abstract suite against `build`; return the assertion it raised."""

    class Subject(suite):
        def context_for(
            self,
            log: Sequence[Event],
            subject_index: int,
            subject: CanonicalTransaction,
            *,
            complete_since: EventTime | None,
        ) -> FeatureContext:
            return build(log, subject_index, complete_since)

    assert hasattr(Subject, fixture), f"{suite.__name__} has no fixture {fixture}"
    patch = pytest.MonkeyPatch()
    # The suite evaluates features through its module-level registry.
    patch.setattr(suite_module, "ONLINE_FEATURES", registry)
    try:
        getattr(Subject(), fixture)()
    except AssertionError as exc:
        if not failed_feature_ids(exc):
            # A harness or precondition assertion is not the rule's fixture catching the rule.
            raise
        return exc
    finally:
        patch.undo()
    return None


def _suite_for(mode: str, module: Any) -> tuple[type[Any], Build]:
    if mode == "served":
        return AsServedConformanceSuite, _served_by(module)
    return EventTimeCompleteConformanceSuite, _complete_by(module)


def test_the_unmutated_copy_passes_every_fixture_in_both_modes(
    mutate: Callable[[tuple[Edit, ...]], Mutated],
) -> None:
    """The control: the loader itself must not be what makes a mutant fail."""
    mutated = mutate(())
    for mode in ("served", "complete"):
        suite, build = _suite_for(mode, mutated.reference)
        fixtures = [name for name in dir(suite) if name.startswith("test_")]
        assert len(fixtures) > 50
        failures = {
            name: exc
            for name in fixtures
            if (exc := _fails(suite, build, name, mutated.registry)) is not None
        }
        assert not failures, failures


@pytest.mark.parametrize("mutant", MUTANTS, ids=[m.name for m in MUTANTS])
def test_breaking_one_declared_rule_fails_the_fixture_written_for_it(
    mutant: Mutant, mutate: Callable[[tuple[Edit, ...]], Mutated]
) -> None:
    mutated = mutate(mutant.edits)
    for mode in mutant.modes:
        suite, build = _suite_for(mode, mutated.reference)
        assert _fails(suite, build, mutant.catches, mutated.registry) is not None, (
            f"{mutant.name}: {mutant.catches} still passes in the {mode} mode, so no literal "
            f"fixture pins this rule"
        )


def test_a_replay_of_served_values_must_follow_the_recorded_order(
    mutate: Callable[[tuple[Edit, ...]], Mutated],
) -> None:
    """The shortcuts ADR-0046 forbids for an as-served replay."""
    complete = mutate(())
    for fixture in (
        "test_a_same_millisecond_observation_counts_only_if_it_was_recorded_first",
        "test_an_observation_delivered_after_the_scored_transaction_cannot_leak_into_it",
    ):
        assert (
            _fails(
                AsServedConformanceSuite, _complete_by(complete.reference), fixture, ONLINE_FEATURES
            )
            is not None
        ), fixture
    range_over_occurred_at = mutate(
        (
            (
                "reference",
                "    return event.order_key <= key",
                "    return event.occurred_ms <= read.as_of_ms",
            ),
        )
    )
    assert (
        _fails(
            AsServedConformanceSuite,
            _complete_by(range_over_occurred_at.reference),
            "test_a_same_millisecond_observation_counts_only_if_it_was_recorded_first",
            ONLINE_FEATURES,
        )
        is not None
    )


def test_a_complete_history_is_not_what_a_store_served(
    mutate: Callable[[tuple[Edit, ...]], Mutated],
) -> None:
    served = mutate(())
    assert (
        _fails(
            EventTimeCompleteConformanceSuite,
            _served_by(served.reference),
            "test_an_earlier_observation_delivered_after_the_scored_transaction_counts",
            ONLINE_FEATURES,
        )
        is not None
    )
