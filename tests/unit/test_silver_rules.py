"""Silver's per-record decisions without a JVM (ADR-0053 §2).

Spark applies exactly these functions (`trace_core.stream.silver`); here they are held to the ADR on
hand-built records: the registry against the Kafka declaration, the canonical columns against the
released models, admission's every branch, the retry-tolerant digest, the one total order and every
classification disposition, including a replayed batch and a supersede. `tx.scored.v1` records are
built by the real scoring pipeline and `build_scored_event`, as the gateway builds them.
"""

from __future__ import annotations

import datetime as dt
import uuid
from pathlib import Path
from typing import Any

import pytest
import yaml
from services.gateway.pipeline import ScoringOutcome, ScoringPipeline

from trace_core.contracts import authorization
from trace_core.contracts.api.transaction import TransactionRequest
from trace_core.contracts.canonical_json import canonical_bytes
from trace_core.contracts.envelope import build_event
from trace_core.contracts.topics import (
    IDENTITY_EVENTS_V1,
    PARTITION_KEY_FIELD,
    TX_AUTHORIZATION_V1,
    TX_SCORED_V1,
)
from trace_core.domain.errors import ContractError
from trace_core.domain.time import event_time
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.reference import ReferenceFeatureStore
from trace_core.observation.scored_event import build_scored_event
from trace_core.rules.loader import default_loader
from trace_core.scoring.banding import load_thresholds
from trace_core.stream import silver_rules as rules
from trace_core.stream import timing

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
T = dt.datetime(2026, 9, 15, 12, 0, tzinfo=dt.UTC)
MS = dt.timedelta(milliseconds=1)
HERE = rules.Coordinates("topic-id-1", 0, 7)
ARRIVED = T + dt.timedelta(seconds=2)


def _identity_event(
    *,
    occurred: dt.datetime = T,
    producer: str = "trace-generator@1.0.0",
    kind: str = "PASSWORD_CHANGE",
) -> dict[str, Any]:
    return build_event(
        event_type="identity.events",
        occurred_at=event_time(occurred),
        payload={"account_id": "acct_000001", "identity_event_type": kind},
        producer=producer,
        trace_id="0" * 32,
        correlation_id="idev_" + "0" * 32,
    )


def _authorization(
    *,
    outcome: str = "DECLINED",
    decided_ms: int | None = None,
    ingested_ms: int | None = None,
    producer: str = "trace-gateway@0.1.0",
) -> dict[str, Any]:
    occurred_ms = int(T.timestamp() * 1000) - 5_000
    decided = occurred_ms + 340 if decided_ms is None else decided_ms
    return authorization.build_event(
        transaction_id="tx_000000000042",
        account_id="acct_000042",
        authorization_outcome=outcome,
        decided_ms=decided,
        transaction_occurred_ms=occurred_ms,
        transaction_occurred_at=authorization.iso_millis(occurred_ms),
        producer=producer,
        trace_id="1" * 32,
        correlation_id="tx_000000000042",
        ingested_ms=decided + 12 if ingested_ms is None else ingested_ms,
    )


def _pipeline() -> ScoringPipeline:
    return ScoringPipeline(
        pack=default_loader(frozenset(ONLINE_FEATURES.ids)).load(),
        thresholds=load_thresholds(),
        feature_store=ReferenceFeatureStore(),
    )


def _request(*, amount: int = 5_000, account: str = "acct_000777") -> TransactionRequest:
    return TransactionRequest.model_validate(
        {
            "transaction_id": "tx_000000000777",
            "account_id": account,
            "amount_minor": amount,
            "currency": "GBP",
            "occurred_at": T.isoformat().replace("+00:00", "Z"),
        }
    )


def _scored(outcome: ScoringOutcome, *, producer: str = "trace-gateway@0.1.0") -> dict[str, Any]:
    return build_scored_event(
        canonical=outcome.canonical,
        decision=outcome.decision,
        features=outcome.features,
        context=outcome.context,
        observe_outcome=outcome.observe_outcome.value,
        store_position=outcome.observe_position,
        store_epoch_ms=outcome.store_epoch_ms,
        producer=producer,
        trace_id=uuid.uuid4().hex,
    )


def _record(
    topic: str,
    value: bytes | None,
    *,
    logged_at: dt.datetime = ARRIVED,
    timestamp_type: int = 1,
    headers: tuple[tuple[str, bytes | None], ...] = (),
    coordinates: rules.Coordinates = HERE,
) -> rules.BronzeRecord:
    return rules.BronzeRecord(topic, coordinates, logged_at, timestamp_type, value, headers)


# ------------------------------------------------------------------ registry ---


def test_every_released_topic_has_a_silver_table_query_and_the_declared_identity() -> None:
    declared = yaml.safe_load((ROOT / "deploy" / "kafka" / "topics.yaml").read_text())["topics"]
    assert set(rules.SILVER_TOPICS) == set(PARTITION_KEY_FIELD) == set(declared)
    for topic, spec in rules.SILVER_TOPICS.items():
        assert spec.identity == declared[topic]["dedup_identity"], topic
        assert str(spec.table) == f"silver.{topic.replace('.', '_')}"
        assert spec.query == f"silver_transform_{topic.replace('.', '_')}"
    assert {str(t) for t in rules.SHARED_TABLES} == {
        "silver.late_events",
        "silver.quarantine",
        "silver.duplicates",
    }
    with pytest.raises(ContractError):
        rules.silver_topic("tx.unreleased.v1")


def test_canonical_columns_follow_the_released_model() -> None:
    columns = {c.name: c for c in rules.event_columns(TX_SCORED_V1)}
    assert columns["occurred_at"] == rules.ColumnSpec(
        "occurred_at", "envelope", "occurred_at", "timestamp", False
    )
    assert columns["amount_minor"].kind == "long" and not columns["amount_minor"].nullable
    assert columns["store_position"].kind == "long" and columns["store_position"].nullable
    assert columns["store_epoch"].kind == "timestamp" and columns["store_epoch"].nullable
    assert columns["latitude"].kind == "double" and columns["latitude"].nullable
    assert columns["channel"].kind == "string" and columns["channel"].nullable
    summary = columns["decision_summary_json"]
    assert summary.kind == "json" and not summary.nullable
    assert columns["served_features_json"].kind == "json"
    authorization_columns = {c.name: c for c in rules.event_columns(TX_AUTHORIZATION_V1)}
    assert authorization_columns["transaction_occurred_at"].kind == "timestamp"
    for topic, spec in rules.SILVER_TOPICS.items():
        by_field = {(c.section, c.field): c for c in rules.event_columns(topic)}
        identity = by_field[(spec.identity_section, spec.identity_field)]
        assert identity.kind == "string" and not identity.nullable, topic


# ----------------------------------------------------------------- admission ---


def test_a_valid_event_is_admitted_with_its_identity_timing_and_digest() -> None:
    event = _identity_event()
    admission = rules.admit(_record(IDENTITY_EVENTS_V1, canonical_bytes(event)))
    assert admission.outcome is rules.Outcome.ADMITTED and admission.reason is None
    assert admission.identity == event["envelope"]["event_id"]
    assert admission.digest is not None and admission.digest.startswith("sha256:")
    assert admission.is_late is False and admission.arrival_delay_ms == 2_000
    assert admission.occurred_at == T and admission.backfill is False
    assert (admission.recorded, admission.store_epoch_us, admission.store_position) == (
        False,
        None,
        None,
    )


@pytest.mark.parametrize(
    ("value", "timestamp_type", "reason"),
    [
        (None, 1, rules.QuarantineReason.NULL_VALUE),
        (b"{}", 0, rules.QuarantineReason.NOT_LOG_APPEND_TIME),
        (b"not json", 1, rules.QuarantineReason.INVALID_EVENT),
        (b"\xff\xfe", 1, rules.QuarantineReason.INVALID_EVENT),
        (b"[" * 5_000, 1, rules.QuarantineReason.INVALID_EVENT),
    ],
)
def test_a_record_that_is_not_a_judgeable_event_is_quarantined(
    value: bytes | None, timestamp_type: int, reason: rules.QuarantineReason
) -> None:
    admission = rules.admit(_record(IDENTITY_EVENTS_V1, value, timestamp_type=timestamp_type))
    assert (admission.outcome, admission.reason) == (rules.Outcome.QUARANTINED, reason)
    assert admission.identity is None and admission.event is None


def test_the_detail_holds_only_declared_path_segments_and_error_types() -> None:
    """Critic finding C1: an input-supplied key is written `<extra>`, and no value is echoed."""
    extra = _identity_event()
    extra["payload"]["ssn_123-45-6789"] = "123-45-6789"
    admission = rules.admit(_record(IDENTITY_EVENTS_V1, canonical_bytes(extra)))
    assert admission.reason is rules.QuarantineReason.INVALID_EVENT
    assert admission.detail == "payload.<extra>: extra_forbidden"
    unknown = rules.admit(
        _record(IDENTITY_EVENTS_V1, canonical_bytes(_identity_event(kind="NOT_A_RELEASED_TYPE")))
    )
    assert unknown.detail is not None and "enum" in unknown.detail
    assert "NOT_A_RELEASED_TYPE" not in unknown.detail
    nested = rules.admit(_record(TX_SCORED_V1, b'{"envelope": 1, "payload": {"ssn_1": 2}}'))
    assert nested.detail is not None and "ssn" not in nested.detail


def test_an_integer_outside_int64_is_unrepresentable_and_its_value_is_never_echoed() -> None:
    """Critic finding B3: the UDF would otherwise fail the whole batch converting it to a long."""
    event = _scored(_pipeline().score(_request(), now=T))
    event["payload"]["store_position"] = 2**63
    admission = rules.admit(_record(TX_SCORED_V1, canonical_bytes(event)))
    assert (admission.outcome, admission.reason) == (
        rules.Outcome.QUARANTINED,
        rules.QuarantineReason.UNREPRESENTABLE,
    )
    assert admission.detail is not None and "payload.store_position" in admission.detail
    assert str(2**63) not in admission.detail


def test_future_skew_is_judged_by_the_gateway_s_receipt_time_or_by_arrival() -> None:
    limit = T + timing.FUTURE_SKEW_LIMIT + timing.OTHER_PRODUCER_CLOCK_MARGIN
    on_limit = rules.admit(
        _record(IDENTITY_EVENTS_V1, canonical_bytes(_identity_event(occurred=limit)), logged_at=T)
    )
    beyond = rules.admit(
        _record(
            IDENTITY_EVENTS_V1, canonical_bytes(_identity_event(occurred=limit + MS)), logged_at=T
        )
    )
    assert on_limit.outcome is rules.Outcome.ADMITTED
    assert beyond.reason is rules.QuarantineReason.FUTURE_SKEW and beyond.identity is not None
    ingested_ms = int(T.timestamp() * 1000)
    skewed = _authorization(decided_ms=ingested_ms + 86_400_000 + 1, ingested_ms=ingested_ms)
    gateway = rules.admit(
        _record(TX_AUTHORIZATION_V1, canonical_bytes(skewed), logged_at=T + dt.timedelta(days=2))
    )
    assert gateway.reason is rules.QuarantineReason.FUTURE_SKEW, "the gateway's own clock decides"


def test_late_is_arrival_delay_beyond_600_seconds_and_null_for_a_backfill() -> None:
    value = canonical_bytes(_identity_event())
    boundary = T + dt.timedelta(seconds=600)
    on_time = rules.admit(_record(IDENTITY_EVENTS_V1, value, logged_at=boundary))
    late = rules.admit(_record(IDENTITY_EVENTS_V1, value, logged_at=boundary + MS))
    backfill = rules.admit(
        _record(
            IDENTITY_EVENTS_V1,
            value,
            logged_at=T + dt.timedelta(days=9),
            headers=((timing.REPLAY_MODE_HEADER, timing.BACKFILL_MODE),),
        )
    )
    assert (on_time.is_late, late.is_late, backfill.is_late) == (False, True, None)
    assert late.arrival_delay_ms == 600_001 and backfill.backfill is True
    sub_ms = rules.admit(
        _record(IDENTITY_EVENTS_V1, value, logged_at=boundary + dt.timedelta(microseconds=500))
    )
    assert (sub_ms.arrival_delay_ms, sub_ms.is_late) == (600_000, False), "never disagree (C2)"


def test_the_digest_ignores_only_what_a_producer_retry_may_change() -> None:
    first = _authorization()
    retry = _authorization(producer="trace-gateway@0.2.0")
    assert first["envelope"]["event_id"] != retry["envelope"]["event_id"]
    retry["envelope"]["trace_id"] = "2" * 32
    digests = [
        rules.admit(_record(TX_AUTHORIZATION_V1, canonical_bytes(event))).digest
        for event in (first, retry)
    ]
    changed = rules.admit(
        _record(TX_AUTHORIZATION_V1, canonical_bytes(_authorization(outcome="APPROVED")))
    )
    renamed = rules.admit(
        _record(TX_AUTHORIZATION_V1, canonical_bytes(_authorization(producer="trace-other@0.1.0")))
    )
    assert digests[0] is not None and digests[0] == digests[1], "a redeploy is not a conflict"
    assert changed.digest != digests[0] and changed.identity == "tx_000000000042"
    assert renamed.digest != digests[0], "the producer's name is still content"


# ------------------------------------------------------- tx.scored.v1 retries ---


def test_a_gateway_retry_of_one_transaction_has_the_same_digest() -> None:
    pipeline = _pipeline()
    recorded = pipeline.score(_request(), now=T)
    redelivered = pipeline.score(_request(), now=T + dt.timedelta(seconds=5))
    assert (recorded.observe_outcome.value, redelivered.observe_outcome.value) == (
        "RECORDED",
        "REDELIVERY",
    )
    first = rules.admit(_record(TX_SCORED_V1, canonical_bytes(_scored(recorded))))
    retry = rules.admit(
        _record(TX_SCORED_V1, canonical_bytes(_scored(redelivered, producer="trace-gateway@0.2.0")))
    )
    assert first.outcome is retry.outcome is rules.Outcome.ADMITTED
    assert first.digest == retry.digest
    assert (first.recorded, retry.recorded) == (True, False)


@pytest.mark.parametrize("changed", [{"amount": 5_001}, {"account": "acct_000778"}])
def test_a_different_amount_or_account_under_the_same_id_is_an_identity_conflict(
    changed: dict[str, Any],
) -> None:
    pipeline = _pipeline()
    original = rules.admit(
        _record(TX_SCORED_V1, canonical_bytes(_scored(pipeline.score(_request(), now=T))))
    )
    other_record = _record(
        TX_SCORED_V1,
        canonical_bytes(_scored(pipeline.score(_request(**changed), now=T))),
        coordinates=rules.Coordinates("topic-id-1", 1, 3),
    )
    other = rules.admit(other_record)
    assert original.digest != other.digest and original.identity == other.identity
    first = rules.candidate(original, _record(TX_SCORED_V1, b"{}"))
    result = rules.classify(
        [rules.candidate(other, other_record)],
        {first.identity: rules.Canonical(first.coordinates, first.digest, first.key)},
        supersedable=True,
    )
    assert [c.disposition for c in result] == [rules.Disposition.CONFLICT]


def test_recorded_wins_in_a_batch_whatever_arrives_first() -> None:
    pipeline = _pipeline()
    recorded = _scored(pipeline.score(_request(), now=T))
    redelivered = _scored(pipeline.score(_request(), now=T + dt.timedelta(seconds=5)))
    early = _record(
        TX_SCORED_V1,
        canonical_bytes(redelivered),
        logged_at=T + dt.timedelta(seconds=1),
        coordinates=rules.Coordinates("t", 0, 0),
    )
    late = _record(
        TX_SCORED_V1,
        canonical_bytes(recorded),
        logged_at=T + dt.timedelta(seconds=9),
        coordinates=rules.Coordinates("t", 1, 0),
    )
    candidates = [rules.candidate(rules.admit(r), r) for r in (early, late)]
    result = {
        c.candidate.coordinates: c.disposition
        for c in rules.classify(candidates, {}, supersedable=True)
    }
    assert result == {
        late.coordinates: rules.Disposition.ADMIT,
        early.coordinates: rules.Disposition.DUPLICATE,
    }


# ------------------------------------------------------------ the total order ---


def _candidate(
    identity: str,
    digest: str,
    partition: int,
    offset: int,
    seconds: float,
    *,
    recorded: bool = False,
    epoch: int | None = None,
    position: int | None = None,
) -> rules.Candidate:
    return rules.Candidate(
        identity,
        digest,
        rules.Coordinates("t", partition, offset),
        T + dt.timedelta(seconds=seconds),
        recorded,
        epoch,
        position,
    )


def test_the_order_key_puts_recorded_first_and_null_epochs_and_positions_last() -> None:
    recorded_no_epoch = _candidate("a", "d", 0, 9, 9.0, recorded=True)
    redelivery_with_epoch = _candidate("a", "d", 0, 1, 1.0, epoch=10, position=1)
    epoch_no_position = _candidate("a", "d", 0, 2, 1.0, epoch=10)
    epoch_later = _candidate("a", "d", 0, 3, 0.0, epoch=11, position=0)
    nothing_early = _candidate("a", "d", 0, 4, 0.5)
    nothing_late = _candidate("a", "d", 1, 0, 0.5)
    ordered = sorted(
        [
            nothing_late,
            epoch_later,
            nothing_early,
            epoch_no_position,
            redelivery_with_epoch,
            recorded_no_epoch,
        ],
        key=lambda c: c.key,
    )
    assert ordered == [
        recorded_no_epoch,
        redelivery_with_epoch,
        epoch_no_position,
        epoch_later,
        nothing_early,
        nothing_late,
    ]
    assert rules.KEY_NULL == 2**63 - 1


def test_within_a_batch_the_first_in_order_is_admitted_and_the_rest_duplicate_or_conflict() -> None:
    later_same = _candidate("a", "d1", 0, 5, 2.0)
    first = _candidate("a", "d1", 1, 9, 1.0)
    tie_lower_partition = _candidate("b", "x", 0, 3, 1.0)
    tie_higher_partition = _candidate("b", "y", 1, 1, 1.0)
    batch = [later_same, tie_higher_partition, first, tie_lower_partition]
    result = {c.candidate: c for c in rules.classify(batch, {}, supersedable=False)}
    assert result[first].disposition is rules.Disposition.ADMIT
    assert result[later_same].disposition is rules.Disposition.DUPLICATE
    assert result[later_same].canonical.coordinates == first.coordinates
    assert result[tie_lower_partition].disposition is rules.Disposition.ADMIT
    assert result[tie_higher_partition].disposition is rules.Disposition.CONFLICT


def test_a_replay_is_recognised_only_against_a_committed_row() -> None:
    """Critic finding C3: a coordinate repeated within one batch is a duplicate, as Spark says."""
    once = _candidate("a", "d1", 0, 1, 0.5)
    again = _candidate("a", "d1", 0, 1, 0.5)
    in_batch = [c.disposition for c in rules.classify([once, again], {}, supersedable=False)]
    assert in_batch == [rules.Disposition.ADMIT, rules.Disposition.DUPLICATE]
    committed = {"a": rules.Canonical(once.coordinates, "d1", once.key)}
    replayed = [c.disposition for c in rules.classify([once, again], committed, supersedable=False)]
    assert replayed == [rules.Disposition.REPLAYED, rules.Disposition.REPLAYED]
    duplicate = _candidate("a", "d1", 2, 4, 0.1)
    conflict = _candidate("a", "d2", 3, 8, 0.2)
    result = {
        c.candidate: c.disposition
        for c in rules.classify([conflict, duplicate, once], committed, supersedable=False)
    }
    assert result == {
        once: rules.Disposition.REPLAYED,
        duplicate: rules.Disposition.DUPLICATE,
        conflict: rules.Disposition.CONFLICT,
    }


def _canonical_after(
    batches: list[list[rules.Candidate]], *, supersedable: bool
) -> tuple[rules.Coordinates, list[rules.Coordinates]]:
    committed: dict[str, rules.Canonical] = {}
    superseded: list[rules.Coordinates] = []
    for batch in batches:
        for classified in rules.classify(batch, committed, supersedable=supersedable):
            if classified.disposition in (rules.Disposition.ADMIT, rules.Disposition.SUPERSEDE):
                committed[classified.candidate.identity] = classified.canonical
            if classified.replaces is not None:
                superseded.append(classified.replaces.coordinates)
    (only,) = committed.values()
    return only.coordinates, superseded


@pytest.mark.parametrize(
    ("first", "second"),
    [
        (
            _candidate("tx", "d", 0, 0, 1.0, epoch=100, position=5),
            _candidate("tx", "d", 0, 1, 2.0, recorded=True, epoch=100, position=5),
        ),
        (
            _candidate("tx", "d", 0, 0, 1.0, recorded=True, epoch=200, position=1),
            _candidate("tx", "d", 0, 1, 2.0, recorded=True, epoch=100, position=9),
        ),
    ],
    ids=["redelivery-then-recorded", "later-epoch-then-earlier-epoch"],
)
def test_tx_scored_converges_to_the_same_canonical_row_however_the_batches_fall(
    first: rules.Candidate, second: rules.Candidate
) -> None:
    split, superseded = _canonical_after([[first], [second]], supersedable=True)
    single, none = _canonical_after([[first, second]], supersedable=True)
    assert split == single == second.coordinates
    assert superseded == [first.coordinates] and none == []
    kept, never = _canonical_after([[first], [second]], supersedable=False)
    assert kept == first.coordinates and never == [], "other topics are insert-only"


def test_column_values_are_typed_from_the_validated_event() -> None:
    event = rules.admit(_record(TX_AUTHORIZATION_V1, canonical_bytes(_authorization()))).event
    assert event is not None
    columns = {c.name: c for c in rules.event_columns(TX_AUTHORIZATION_V1)}
    occurred = rules.column_value(columns["transaction_occurred_at"], event)
    assert isinstance(occurred, dt.datetime) and occurred.tzinfo is not None
    assert rules.column_value(columns["authorization_outcome"], event) == "DECLINED"
