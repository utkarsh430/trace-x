"""Fraud base rate and label integrity over a whole generated dataset.

ROADMAP Phase 1 automated test: "base rate within configured tolerance". The
tolerance exists because scenarios contribute whole *episodes*, not individual
rows -- a ring adds tens of transactions at once -- so the realised rate lands
near the target rather than on it. It is measured, never assumed.

The label-integrity tests here are the ones that matter most: a ground-truth
defect is invisible downstream and silently invalidates every metric computed
against it.
"""

from __future__ import annotations

import collections

import pytest
from data.generator.config import GeneratorConfig
from data.generator.engine import GeneratedRow, generate_dataset
from data.generator.scenarios import ALL_SCENARIOS

from trace_core.domain.enums import EvidenceKind, FraudPattern

pytestmark = pytest.mark.unit


def _config(**overrides: object) -> GeneratorConfig:
    base: dict[str, object] = {
        "row_count": 8000,
        "account_count": 500,
        "merchant_count": 150,
        "device_count": 700,
        "ip_count": 350,
        "fraud_rate": 0.01,
    }
    base.update(overrides)
    return GeneratorConfig(**base)  # type: ignore[arg-type]


@pytest.fixture(scope="module")
def dataset() -> list[GeneratedRow]:
    return list(generate_dataset(_config()))


@pytest.fixture(scope="module")
def transactions(dataset: list[GeneratedRow]) -> list[GeneratedRow]:
    return [r for r in dataset if r.topic == "tx.raw.v1"]


# ------------------------------------------------------------- base rate ----


def test_exactly_the_requested_number_of_transactions(transactions: list[GeneratedRow]) -> None:
    """Fraud replaces legitimate rows rather than adding to them, so a run of
    `row_count` is the same size whatever the fraud rate."""
    assert len(transactions) == _config().row_count


def test_realised_fraud_rate_is_near_the_target(transactions: list[GeneratedRow]) -> None:
    config = _config()
    fraud = [r for r in transactions if r.label and r.label.is_fraud]
    realised = len(fraud) / len(transactions)
    # Generous but real: episodes are indivisible, and ten mandatory instances
    # are seeded before the weighted mix runs.
    assert config.fraud_rate * 0.5 <= realised <= config.fraud_rate * 2.0, (
        f"realised {realised:.4f} against target {config.fraud_rate:.4f}"
    )


def test_a_zero_fraud_rate_produces_no_fraud() -> None:
    """The rules-only arm needs a clean baseline; a floor of mandatory instances
    would quietly contaminate it."""
    rows = list(generate_dataset(_config(row_count=500, fraud_rate=0.0)))
    assert rows
    assert not any(r.label and r.label.is_fraud for r in rows)


def _realised(row_count: int, fraud_rate: float) -> float:
    rows = [
        r
        for r in generate_dataset(_config(row_count=row_count, fraud_rate=fraud_rate))
        if r.topic == "tx.raw.v1"
    ]
    return sum(1 for r in rows if r.label and r.label.is_fraud) / len(rows)


def test_a_higher_rate_produces_more_fraud() -> None:
    """Exercised above the floor, where the weighted mix actually runs."""
    assert _realised(12_000, 0.02) > _realised(12_000, 0.005)


def test_below_the_floor_the_rate_is_set_by_mandatory_coverage() -> None:
    """A real and deliberate limitation, pinned so it cannot surprise anyone.

    Ten mandatory instances -- one per pattern -- are planted before the mix
    runs. On a dataset small enough that those ten already exceed the target,
    `fraud_rate` has no effect and the realised rate is whatever coverage costs.
    Coverage is worth more than an exact rate on a toy dataset, but the behaviour
    must be visible rather than discovered later as a bug.
    """
    assert _realised(4000, 0.005) == _realised(4000, 0.02)


# ------------------------------------------------------ pattern coverage ----


def test_every_pattern_appears(transactions: list[GeneratedRow]) -> None:
    """A dataset missing a pattern silently drops it from every per-pattern
    metric in Phase 9, and the absence would look like a modelling result rather
    than a sampling artefact."""
    present = {r.label.fraud_pattern for r in transactions if r.label and r.label.is_fraud}
    assert present == {s.pattern for s in ALL_SCENARIOS}


def test_every_pattern_appears_even_in_a_small_dataset() -> None:
    rows = [
        r
        for r in generate_dataset(_config(row_count=1200, fraud_rate=0.01))
        if r.topic == "tx.raw.v1"
    ]
    present = {r.label.fraud_pattern for r in rows if r.label and r.label.is_fraud}
    assert present == set(FraudPattern)


# ---------------------------------------------------- label integrity ------


def test_every_transaction_carries_a_label(transactions: list[GeneratedRow]) -> None:
    """Negatives are labelled too: without them the harness cannot compute a
    false-positive rate, which is half of what a fraud metric is."""
    assert all(r.label is not None for r in transactions)


def test_labels_match_their_transaction_ids(transactions: list[GeneratedRow]) -> None:
    for row in transactions:
        assert row.label is not None
        assert row.label.transaction_id == row.event["payload"]["transaction_id"]


def test_every_fraud_label_carries_causal_keys(transactions: list[GeneratedRow]) -> None:
    for row in transactions:
        if row.label and row.label.is_fraud:
            assert row.label.causal_evidence_keys
            assert row.label.causal_evidence_keys <= set(EvidenceKind)
            assert row.label.scenario_instance_id


def test_legitimate_labels_carry_no_pattern_or_keys(transactions: list[GeneratedRow]) -> None:
    """A negative row that looks partially explained would corrupt evidence
    precision for every arm."""
    for row in transactions:
        if row.label and not row.label.is_fraud:
            assert row.label.fraud_pattern is None
            assert row.label.causal_evidence_keys == frozenset()


def test_causal_keys_match_the_scenario_that_produced_them(
    transactions: list[GeneratedRow],
) -> None:
    from data.generator.scenarios import SCENARIOS_BY_PATTERN

    for row in transactions:
        if row.label and row.label.is_fraud:
            # Narrowed for mypy: TransactionLabel guarantees a pattern whenever
            # is_fraud, but that invariant lives in __post_init__ rather than the
            # type, so the check is made explicit here.
            assert row.label.fraud_pattern is not None
            scenario = SCENARIOS_BY_PATTERN[row.label.fraud_pattern]
            assert row.label.causal_evidence_keys == scenario.causal_evidence_keys()


# ------------------------------------------------ ground-truth isolation ----


def test_no_transaction_id_reveals_its_label(transactions: list[GeneratedRow]) -> None:
    """Ids are assigned by final emission position precisely so a fraudulent row
    is indistinguishable from a legitimate one (ADR-0004)."""
    for row in transactions:
        identifier = row.event["payload"]["transaction_id"].lower()
        assert not any(token in identifier for token in ("fraud", "fi_", "fs_", "scenario"))


def test_no_event_payload_carries_ground_truth(dataset: list[GeneratedRow]) -> None:
    """The event is what the application sees. A label reaching it would route
    ground truth straight into the feature path."""
    forbidden = {
        "is_fraud",
        "fraud_pattern",
        "causal_evidence_keys",
        "label",
        "scenario_instance_id",
    }
    for row in dataset:
        assert not forbidden & set(row.event["payload"])
        assert not forbidden & set(row.event["envelope"])


def test_fraudulent_and_legitimate_rows_are_structurally_identical(
    transactions: list[GeneratedRow],
) -> None:
    """Same field set, so nothing about the shape of a row betrays its label."""
    fraud = next(r for r in transactions if r.label and r.label.is_fraud)
    legit = next(r for r in transactions if r.label and not r.label.is_fraud)
    assert set(fraud.event["payload"]) == set(legit.event["payload"])
    assert set(fraud.event["envelope"]) == set(legit.event["envelope"])


# ------------------------------------------------------------- structure ----


def test_the_dataset_is_event_time_ordered(dataset: list[GeneratedRow]) -> None:
    """Compared as parsed milliseconds, never as strings.

    `_iso` drops the fractional part of a whole-second timestamp, and
    "...21Z" sorts after "...21.500000Z" as text although it is earlier in time.
    A string comparison can therefore hide a real misordering, or report a false
    one.
    """
    import datetime as dt

    from trace_core.domain.time import to_millis

    times = [
        to_millis(
            dt.datetime.fromisoformat(r.event["envelope"]["occurred_at"].replace("Z", "+00:00"))
        )
        for r in dataset
    ]
    assert times == sorted(times)


def test_side_events_use_released_topics_only(dataset: list[GeneratedRow]) -> None:
    """A topic without a released schema cannot be produced (ADR-0028)."""
    assert {r.topic for r in dataset} <= {
        "tx.raw.v1",
        "identity.events.v1",
        "device.events.v1",
    }


def test_identity_and_device_events_are_produced(dataset: list[GeneratedRow]) -> None:
    """Account takeover and credential stuffing are defined by them; if none are
    emitted, IDENTITY_CHANGE is an uncausal key for both."""
    topics = collections.Counter(r.topic for r in dataset)
    assert topics["identity.events.v1"] > 0
    assert topics["device.events.v1"] > 0


def test_side_events_carry_no_label(dataset: list[GeneratedRow]) -> None:
    """Labels are per transaction; labelling a login would make the positive
    count disagree with the transaction count."""
    for row in dataset:
        if row.topic != "tx.raw.v1":
            assert row.label is None


def test_the_dataset_is_reproducible() -> None:
    from data.generator.digest import digest_of

    config = _config(row_count=1500)
    first, n1 = digest_of(r.event for r in generate_dataset(config))
    second, n2 = digest_of(r.event for r in generate_dataset(config))
    assert first == second and n1 == n2
