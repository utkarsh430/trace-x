"""`LPC-5` §4.6: availability from the reference implementation, for every TX row.

- **Equal to the whole-history context.** For sampled rows, every released feature -- value and
  state -- matches `reference.event_time_complete_context` over the whole generation, both with
  outcomes derived (eval-v1's shape) and under the eval-v2 gate (an outcome stream, identity
  events, households and trips). Dense generations, so the per-row bounds actually leave related
  observations out.
- **Mapped as the ingress maps.** A TX row is observed exactly as the generator adapter's canonical
  transaction is; an identity event feeds only its declared stream and needs an identity.
- **What S6 reads.** A value for each of the 26 features on every row, and a run with the provider
  is no longer invalid for want of availability.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
from collections import Counter
from typing import Any

import pytest
from data.adapters.generator_adapter import GeneratorAdapter
from data.generator.config import BaselineIdentityConfig, GeneratorConfig
from data.generator.engine import GeneratedRow, generate_dataset
from data.generator.lpc5 import declaration as d
from data.generator.lpc5 import run
from data.generator.lpc5.availability import (
    Index,
    canonical,
    identity_observation,
    observations,
    reference_availability,
)
from data.generator.lpc5.frame import Frame, build_frame, knowledge
from data.generator.population import build_universe

from trace_core.domain.time import event_time, from_millis
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.observation import transaction_observation
from trace_core.features.reference import build_context, event_time_complete_context
from trace_core.features.semantics import Entity, EvaluationMode, identity_stream
from trace_core.features.spec import FeatureState

pytestmark = pytest.mark.unit

START = dt.datetime(2026, 3, 2, tzinfo=dt.UTC)


def _config(**overrides: Any) -> GeneratorConfig:
    """Dense: a week, so card, device, IP and merchant histories reach well past a day. Forty
    merchants, because the eval-v2 gate's card-testing probes need merchants nobody else used."""
    base: dict[str, Any] = {
        "row_count": 2_000,
        "account_count": 90,
        "merchant_count": 40,
        "device_count": 108,
        "ip_count": 45,
        "fraud_rate": 0.01,
        "seed": 42,
        "start_at": START,
        "end_at": START + dt.timedelta(days=7),
    }
    base.update(overrides)
    return GeneratorConfig(**base)


@pytest.fixture(scope="module", params=["derived-outcomes", "outcome-stream"])
def generation(request: pytest.FixtureRequest) -> tuple[list[GeneratedRow], Frame, int]:
    gate = (
        BaselineIdentityConfig(coverage_floor_instances=1)
        if request.param == "outcome-stream"
        else None
    )
    config = _config(baseline_identity=gate)
    universe = build_universe(config)
    rows = list(generate_dataset(config, universe))
    frame = build_frame(rows, seed=config.seed)
    assert frame.outcomes_derived is (gate is None)
    return rows, frame, knowledge(universe).start_ms


def test_every_feature_equals_the_whole_history_context(
    generation: tuple[list[GeneratedRow], Frame, int],
) -> None:
    _, frame, start_ms = generation
    subjects, events, position = observations(frame)
    index = Index(events)
    complete_since = event_time(from_millis(start_ms))
    available: Counter[str] = Counter()
    bounded = 0
    sampled = subjects[::6]
    for subject in sampled:
        current_index = position[transaction_observation(subject).identity]
        current = events[current_index]
        visible = index.visible(current_index)
        ids = {entity: current.entity_id(entity) for entity in Entity}
        related = sum(
            1
            for event in events
            if any(value is not None and event.entity_id(e) == value for e, value in ids.items())
        )
        bounded += len(visible) < related
        read = build_context(
            visible,
            as_of_ms=current.occurred_ms,
            currency=current.currency,
            ids=ids,
            current=current,
            mode=EvaluationMode.EVENT_TIME_COMPLETE,
            complete_since=complete_since,
        )
        whole = event_time_complete_context(events, current, complete_since=complete_since)
        got = ONLINE_FEATURES.evaluate_all(subject, read)
        for feature, want in ONLINE_FEATURES.evaluate_all(subject, whole).items():
            assert (got[feature].state, got[feature].or_none()) == (want.state, want.or_none()), (
                f"{subject.transaction_id} {feature}: bounded {got[feature]}, whole {want}"
            )
            available[feature] += want.state is FeatureState.AVAILABLE
    assert bounded >= len(sampled) // 2, (
        f"the bounds left related observations out for only {bounded} of {len(sampled)} rows, so "
        f"agreement says little about them"
    )
    for feature in (
        "declined_ratio_1h",
        "merchant_distinct_accounts_1h",
        "merchant_amount_cv_24h",
        "device_distinct_accounts_24h",
        "ip_distinct_accounts_1h",
        "amount_zscore_vs_account",
        "seconds_since_last_transaction",
    ):
        assert available[feature], f"{feature} was never available in the rows compared"


def test_a_transaction_row_is_observed_as_the_ingress_observes_it(
    generation: tuple[list[GeneratedRow], Frame, int],
) -> None:
    rows, frame, _ = generation
    raw = [row.event for row in rows if row.topic == "tx.raw.v1"]
    assert len(raw) == len(frame.tx)
    adapter = GeneratorAdapter(row_count=len(raw))
    for event, tx in zip(raw, frame.tx, strict=True):
        expected = next(iter(adapter.to_canonical([event])))
        mapped = canonical(tx)
        assert mapped.field_coverage == expected.field_coverage
        assert transaction_observation(mapped) == transaction_observation(expected)


def test_an_identity_event_feeds_only_its_declared_stream_and_needs_an_identity(
    generation: tuple[list[GeneratedRow], Frame, int],
) -> None:
    _, frame, _ = generation
    for side in frame.ident:
        assert (identity_observation(side) is None) == (identity_stream(side.event_type) is None)
    feeding = next(s for s in frame.ident if identity_stream(s.event_type) is not None)
    anonymous = dataclasses.replace(
        feeding, envelope=dataclasses.replace(feeding.envelope, event_id=None)
    )
    with pytest.raises(ValueError, match="no event id"):
        identity_observation(anonymous)


def test_the_provider_answers_every_released_feature_for_every_row(
    generation: tuple[list[GeneratedRow], Frame, int],
) -> None:
    _, frame, start_ms = generation
    values = reference_availability(start_ms)(frame)
    assert list(values) == list(d.RELEASED_FEATURES)
    assert all(len(column) == len(frame.tx) for column in values.values())
    seen = {value for column in values.values() for value in column}
    assert seen <= {state.value for state in FeatureState}
    assert FeatureState.UNAVAILABLE.value not in seen, "a generated source covers every input"


def test_a_run_with_the_provider_judges_s6_and_is_not_invalid_for_it() -> None:
    config = _config(row_count=1_200)
    universe = build_universe(config)
    know = knowledge(universe)
    report = run.evaluate(
        generate_dataset(config, universe),
        know,
        availability=reference_availability(know.start_ms),
    )
    assert not any("availability" in reason for reason in report.invalid)
    s6a = report.checks["S6a"]
    assert s6a.judged > 0 and s6a.judged % len(d.RELEASED_FEATURES) == 0
    assert not s6a.findings
