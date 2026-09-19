"""The fault overlay is deterministic, exact, blind to ground truth, and keeps its schedule.

Silver and parity tests will read their expectations from the overlay's provenance
and publish manifest, so a wrong count, a fault of the wrong shape, or a record
that arrives in the wrong arrival class would make those tests pass or fail for
reasons unrelated to the code they test. Each class is therefore checked for the
property that defines it; determinism has a control showing the seed matters; and
the schedule is exercised on a base spanning two hours with a simulated clock --
including the unpaced replay that made most fault-free records late, which must
now be refused.

`tests/integration/test_kafka_platform.py` publishes an overlay through a real
broker and checks the classes arrive in exactly these counts.
"""

from __future__ import annotations

import ast
import datetime as dt
import hashlib
import json
import random
from collections import Counter
from pathlib import Path
from typing import Any

import pytest
import yaml
from eval.replay import faults
from eval.replay.faults import (
    ADDITIVE_CLASSES,
    ARRIVAL_MARGIN_S,
    DEDUP_IDENTITY,
    MAX_SCHEDULE_SLIP_S,
    ON_TIME_CEILING_S,
    REPLAY_MODE_HEADER,
    UNKNOWN_ENUM_VALUE,
    Arrival,
    FaultClass,
    FaultPlan,
    OverlayRecord,
    ReplayScheduleError,
    Timing,
    build_overlay,
    publish_overlay,
)
from pydantic import ValidationError

from trace_core.contracts.api.transaction import MAX_CLOCK_SKEW_FUTURE_S
from trace_core.contracts.envelope import build_event
from trace_core.contracts.events import (
    device_events_v1,
    identity_events_v1,
    investigation_requested_v1,
    tx_raw_v1,
)
from trace_core.contracts.topics import partition_key
from trace_core.domain.errors import SchemaValidationError
from trace_core.domain.identifiers import uuid7
from trace_core.domain.time import event_time, processing_time, to_millis

pytestmark = pytest.mark.unit

# docs/PHASE3_PLAN.md §4.3, timing semantics v1, written out by hand. The overlay
# mirrors these; the tests below use these literals, never the module's copies.
LATE_THRESHOLD_S = 600
FUTURE_BOUND_S = 86_400
CLOCK_MARGIN_S = 300

ROOT = Path(__file__).resolve().parents[2]
T0 = dt.datetime(2026, 9, 1, 12, 0, tzinfo=dt.UTC)
ANCHOR = dt.datetime(2026, 9, 13, 12, 0, tzinfo=dt.UTC)
MODELS: dict[str, Any] = {
    "tx.raw.v1": tx_raw_v1.TxRawV1,
    "identity.events.v1": identity_events_v1.IdentityEventV1,
    "device.events.v1": device_events_v1.DeviceEventV1,
    "investigation.requested.v1": investigation_requested_v1.InvestigationRequestedV1,
}


def _event(
    event_type: str, at: dt.datetime, payload: dict[str, Any], rng: random.Random, lag_ms: int
) -> Any:
    return build_event(
        event_type=event_type,
        occurred_at=event_time(at),
        payload=payload,
        producer="trace-test@1.0.0",
        trace_id="0" * 32,
        correlation_id="corr_overlay_unit",
        ingested_at=processing_time(at + dt.timedelta(milliseconds=lag_ms)),
        event_id=uuid7(millis=to_millis(at), rng=rng),
    )


def build_base(
    n_tx: int = 120,
    accounts: int = 12,
    topics: set[str] | None = None,
    spacing_ms: int = 200,
    lag_ms: int = 40,
) -> list[Any]:
    """Valid released events for every topic, with repeated keys so reordering is possible."""
    rng = random.Random(20260913)
    wanted = topics or set(MODELS)
    base: list[Any] = []
    for i in range(n_tx):
        at = T0 + dt.timedelta(milliseconds=spacing_ms * i)
        a = i % accounts
        if "tx.raw.v1" in wanted:
            payload = {
                "transaction_id": f"tx_{i:012d}",
                "account_id": f"acct_{a:09d}",
                "card_id": f"card_{a:09d}",
                "device_id": f"dev_{a:09d}",
                "merchant_id": f"mrch_{i % 7:06d}",
                "ip_id": f"ip_{a:07d}",
                "amount_minor": 1_000 + i,
                "currency": "GBP",
                "channel": "CARD_NOT_PRESENT",
                "entry_mode": "ECOMMERCE",
                "merchant_mcc": "5411",
                "merchant_country": "GB",
                "latitude": 51.5,
                "longitude": -0.12,
                "authorization_outcome": "APPROVED",
            }
            base.append(("tx.raw.v1", _event("tx.raw", at, payload, rng, lag_ms)))
        if "identity.events.v1" in wanted and i % 3 == 0:
            payload = {
                "account_id": f"acct_{a:09d}",
                "identity_event_type": "LOGIN_SUCCEEDED",
                "ip_id": f"ip_{a:07d}",
            }
            base.append(("identity.events.v1", _event("identity.event", at, payload, rng, lag_ms)))
        if "device.events.v1" in wanted and i % 4 == 0:
            payload = {
                "device_id": f"dev_{a % 5:09d}",
                "account_id": f"acct_{a:09d}",
                "device_event_type": "ATTRIBUTE_CHANGED",
                "platform": "ios",
                "ip_id": f"ip_{a:07d}",
            }
            base.append(("device.events.v1", _event("device.event", at, payload, rng, lag_ms)))
        if "investigation.requested.v1" in wanted and i % 5 == 0:
            payload = {
                "case_id": f"case_{a:032x}",
                "transaction_id": f"tx_{i:012d}",
                "account_id": f"acct_{a:09d}",
                "risk_band": "HIGH",
                "score": 0.8,
                "rule_pack_id": "core",
                "rule_pack_digest": "sha256:" + "a" * 64,
                "threshold_config_digest": "sha256:" + "b" * 64,
                "feature_set_version": "1.0.0",
                "feature_source": "ONLINE_ONLY",
                "degraded": False,
                "fired_rule_ids": ["R001_velocity"],
            }
            base.append(
                (
                    "investigation.requested.v1",
                    _event("investigation.requested", at, payload, rng, lag_ms),
                )
            )
    return base


COUNTS = {
    FaultClass.REORDERED: 3,
    FaultClass.EXACT_DUPLICATE: 3,
    FaultClass.RETRY_NEW_EVENT_ID: 3,
    FaultClass.CONFLICTING_DUPLICATE: 4,
    FaultClass.LATE_EXACT_DUPLICATE: 2,
    FaultClass.LATE_RETRY_NEW_EVENT_ID: 2,
    FaultClass.LATE: 3,
    FaultClass.FUTURE_WITHIN_24H: 2,
    FaultClass.FUTURE_BEYOND_24H: 2,
    FaultClass.MALFORMED_JSON: 2,
    FaultClass.WRONG_SCHEMA_VERSION: 2,
    FaultClass.UNKNOWN_ENUM: 2,
}


def _overlay(seed: int = 11, base: list[Any] | None = None, **plan: Any) -> Any:
    return build_overlay(
        build_base() if base is None else base,
        FaultPlan(seed=seed, counts=plan.pop("counts", COUNTS), **plan),
        base_dataset_ref="fixture:unit-overlay",
    )


def _parse(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def _rendered(record: OverlayRecord, anchor: dt.datetime = ANCHOR) -> dict[str, Any]:
    decoded: dict[str, Any] = json.loads(record.render(anchor))
    return decoded


def _identity(topic: str, event: dict[str, Any]) -> Any:
    section, name = DEDUP_IDENTITY[topic]
    return event[section][name]


def _of(overlay: Any, fault: FaultClass) -> list[OverlayRecord]:
    return [r for r in overlay.records if r.fault is fault]


def _original(overlay: Any, base_index: int) -> OverlayRecord:
    return next(
        r for r in overlay.records if r.base_index == base_index and r.fault not in ADDITIVE_CLASSES
    )


class FakeClock:
    """A clock that only moves when the publisher sleeps, or when told to jump."""

    def __init__(
        self, start: dt.datetime, *, jump_after_calls: int = -1, jump_s: float = 0
    ) -> None:
        self.now = start
        self.calls = 0
        self.jump_after_calls = jump_after_calls
        self.jump_s = jump_s
        self.slept_s = 0.0

    def __call__(self) -> dt.datetime:
        self.calls += 1
        if self.calls == self.jump_after_calls:
            self.now += dt.timedelta(seconds=self.jump_s)
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept_s += seconds
        self.now += dt.timedelta(seconds=seconds)


class RecordingPublisher:
    allow_invalid_events = True

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes]] = []

    def publish(
        self,
        topic: str,
        value: bytes,
        *,
        key_event: dict[str, Any] | None = None,
        headers: Any = None,
        block: bool = True,
    ) -> bool:
        self.published.append((topic, value))
        return True


# ---------------------------------------------------------------- constants --


def test_the_mirrored_timing_constants_are_the_approved_literals() -> None:
    assert faults.TIMING_SEMANTICS_VERSION == 1
    assert faults.IS_LATE_THRESHOLD_S == LATE_THRESHOLD_S == 600
    assert faults.FUTURE_SKEW_BOUND_S == MAX_CLOCK_SKEW_FUTURE_S == FUTURE_BOUND_S == 86_400
    assert faults.OTHER_PRODUCER_CLOCK_MARGIN_S == CLOCK_MARGIN_S == 300
    assert ON_TIME_CEILING_S == LATE_THRESHOLD_S - ARRIVAL_MARGIN_S - MAX_SCHEDULE_SLIP_S


# --------------------------------------------------------------- determinism --


def test_the_same_seed_gives_the_same_overlay() -> None:
    first, second = _overlay(), _overlay()
    assert [
        (r.topic, r.fault, r.base_index, r.schedule_offset_s, r.render(ANCHOR))
        for r in first.records
    ] == [
        (r.topic, r.fault, r.base_index, r.schedule_offset_s, r.render(ANCHOR))
        for r in second.records
    ]
    assert first.provenance == second.provenance


def test_a_different_seed_selects_different_events() -> None:
    """Control: determinism that ignored the seed would also pass the test above."""
    chosen = {
        seed: sorted((r.fault, r.base_index) for r in _overlay(seed).records if r.fault)
        for seed in (11, 12)
    }
    assert chosen[11] != chosen[12]


# -------------------------------------------------------------------- counts --


def test_every_class_is_injected_exactly_as_requested() -> None:
    overlay = _overlay()
    counted = Counter(r.fault.value for r in overlay.records if r.fault is not None)
    assert dict(counted) == {fault.value: count for fault, count in COUNTS.items()}
    assert overlay.provenance.injected == {f.value: COUNTS.get(f, 0) for f in FaultClass}
    additive = sum(COUNTS[f] for f in ADDITIVE_CLASSES)
    assert len(overlay.records) == len(build_base()) + additive


def test_each_base_event_carries_at_most_one_fault() -> None:
    overlay = _overlay()
    faulted = Counter(r.base_index for r in overlay.records if r.fault is not None)
    assert max(faulted.values()) == 1


def test_a_request_the_base_cannot_meet_raises_instead_of_injecting_fewer() -> None:
    with pytest.raises(ValueError, match="never injects fewer"):
        _overlay(counts={FaultClass.RETRY_NEW_EVENT_ID: 10_000})
    with pytest.raises(ValueError, match="disjoint pairs"):
        _overlay(counts={FaultClass.REORDERED: 10_000})


def test_records_are_in_schedule_order() -> None:
    offsets = [r.schedule_offset_s for r in _overlay().records]
    assert offsets == sorted(offsets)


# ------------------------------------------------------------ class semantics --


def test_exact_duplicates_are_byte_identical_and_arrive_shortly_after() -> None:
    overlay = _overlay()
    lo, hi = FaultPlan(seed=0, counts={}).duplicate_delay_s
    for duplicate in _of(overlay, FaultClass.EXACT_DUPLICATE):
        original = _original(overlay, duplicate.base_index)
        assert duplicate.render(ANCHOR) == original.render(ANCHOR)
        assert duplicate.position > original.position
        assert lo <= duplicate.schedule_offset_s - original.schedule_offset_s <= hi


def test_retries_keep_the_transaction_under_a_new_event_id() -> None:
    overlay = _overlay()
    base = build_base()
    for fault in (FaultClass.RETRY_NEW_EVENT_ID, FaultClass.LATE_RETRY_NEW_EVENT_ID):
        for retry in _of(overlay, fault):
            source = base[retry.base_index][1]
            rendered = _rendered(retry)
            assert retry.topic == "tx.raw.v1"
            assert retry.event["payload"] == source["payload"]
            assert rendered["envelope"]["event_id"] != source["envelope"]["event_id"]
            assert retry.event["envelope"]["occurred_at"] == source["envelope"]["occurred_at"]


@pytest.mark.parametrize("topic", sorted(MODELS))
def test_conflicting_duplicates_share_an_identity_but_not_content(topic: str) -> None:
    base = build_base(topics={topic})
    overlay = build_overlay(
        base,
        FaultPlan(seed=5, counts={FaultClass.CONFLICTING_DUPLICATE: 2}),
        base_dataset_ref=f"fixture:{topic}",
    )
    conflicts = _of(overlay, FaultClass.CONFLICTING_DUPLICATE)
    assert len(conflicts) == 2
    for conflict in conflicts:
        source = base[conflict.base_index][1]
        assert _identity(topic, conflict.event) == _identity(topic, source)
        assert conflict.event["payload"] != source["payload"]
        MODELS[topic].model_validate_json(conflict.render(ANCHOR))  # still a valid event


def test_a_reordered_event_arrives_just_after_the_next_event_for_its_key() -> None:
    overlay = _overlay()
    originals = [r for r in overlay.records if r.fault not in ADDITIVE_CLASSES]
    for held in _of(overlay, FaultClass.REORDERED):
        successor = min(
            (
                r
                for r in originals
                if r.topic == held.topic and r.key == held.key and r.base_index > held.base_index
            ),
            key=lambda r: r.base_index,
        )
        assert successor.position < held.position, "the later event must now arrive first"
        occurred = _parse(_rendered(held)["envelope"]["occurred_at"])
        planned_delay = (held.scheduled_at(ANCHOR) - occurred).total_seconds()
        assert 0 <= planned_delay <= ON_TIME_CEILING_S


def test_late_copies_are_held_back_with_their_content_intact() -> None:
    overlay = _overlay()
    lo, hi = FaultPlan(seed=0, counts={}).late_arrival_delay_s
    for late_copy in _of(overlay, FaultClass.LATE_EXACT_DUPLICATE):
        original = _original(overlay, late_copy.base_index)
        assert late_copy.render(ANCHOR) == original.render(ANCHOR), "same bytes, arriving late"
        assert late_copy.arrival is Arrival.LATE
    for late_copy in [
        *_of(overlay, FaultClass.LATE_EXACT_DUPLICATE),
        *_of(overlay, FaultClass.LATE_RETRY_NEW_EVENT_ID),
    ]:
        occurred = _parse(_rendered(late_copy)["envelope"]["occurred_at"])
        delay = (late_copy.scheduled_at(ANCHOR) - occurred).total_seconds()
        assert lo <= delay <= hi
        assert delay > LATE_THRESHOLD_S + ARRIVAL_MARGIN_S


def test_stamped_records_carry_their_delay_and_processing_time() -> None:
    overlay = _overlay()
    late_lo, late_hi = FaultPlan(seed=0, counts={}).late_arrival_delay_s
    for record in _of(overlay, FaultClass.LATE):
        envelope = _rendered(record)["envelope"]
        scheduled = record.scheduled_at(ANCHOR)
        assert _parse(envelope["ingested_at"]) == scheduled
        delay = (scheduled - _parse(envelope["occurred_at"])).total_seconds()
        assert late_lo <= delay <= late_hi and delay > LATE_THRESHOLD_S + ARRIVAL_MARGIN_S
    for record in _of(overlay, FaultClass.FUTURE_WITHIN_24H):
        lead = _parse(_rendered(record)["envelope"]["occurred_at"]) - record.scheduled_at(ANCHOR)
        assert ARRIVAL_MARGIN_S <= lead.total_seconds() < FUTURE_BOUND_S - ARRIVAL_MARGIN_S
    for record in _of(overlay, FaultClass.FUTURE_BEYOND_24H):
        lead = _parse(_rendered(record)["envelope"]["occurred_at"]) - record.scheduled_at(ANCHOR)
        assert lead.total_seconds() > FUTURE_BOUND_S + CLOCK_MARGIN_S + ARRIVAL_MARGIN_S


def test_processing_time_never_precedes_event_time_except_on_future_dated_records() -> None:
    overlay = _overlay()
    checked = 0
    for record in overlay.records:
        if record.malformed_rank is not None:
            continue
        envelope = _rendered(record)["envelope"]
        future = record.fault in (FaultClass.FUTURE_WITHIN_24H, FaultClass.FUTURE_BEYOND_24H)
        before = _parse(envelope["ingested_at"]) < _parse(envelope["occurred_at"])
        assert before is future, (record.fault, envelope["occurred_at"], envelope["ingested_at"])
        checked += 1
    assert checked > len(build_base())


def test_base_timed_records_move_both_clocks_by_one_shift_and_keep_identities() -> None:
    overlay = _overlay()
    base = build_base()
    shift = ANCHOR - T0
    for record in overlay.records:
        if record.fault is not None:
            continue
        source = base[record.base_index][1]["envelope"]
        rendered = _rendered(record)["envelope"]
        for field in ("occurred_at", "ingested_at"):
            assert _parse(rendered[field]) == _parse(source[field]) + shift, field
        for field in ("event_id", "idempotency_key"):
            assert rendered[field] == source[field]


def test_invalid_copies_are_invalid_where_a_consumer_can_tell_and_collide_with_nothing() -> None:
    overlay = _overlay()
    base = build_base()
    base_identities = {(topic, _identity(topic, event)) for topic, event in base}
    for record in _of(overlay, FaultClass.MALFORMED_JSON):
        with pytest.raises(json.JSONDecodeError):
            json.loads(record.render(ANCHOR))
        assert record.key == partition_key(record.topic, base[record.base_index][1])
    for record in _of(overlay, FaultClass.WRONG_SCHEMA_VERSION):
        value = record.render(ANCHOR)
        # The released model ACCEPTS it: the envelope only requires schema_version >= 1.
        MODELS[record.topic].model_validate_json(value)
        assert json.loads(value)["envelope"]["schema_version"] == 2
        assert not record.topic.endswith(".v2"), "only a version-suffix check can catch it"
        assert (record.topic, _identity(record.topic, record.event)) not in base_identities
    for record in _of(overlay, FaultClass.UNKNOWN_ENUM):
        with pytest.raises(ValidationError):
            MODELS[record.topic].model_validate_json(record.render(ANCHOR))
        assert UNKNOWN_ENUM_VALUE in record.render(ANCHOR).decode()
        assert (record.topic, _identity(record.topic, record.event)) not in base_identities


# -------------------------------------------------------------- the schedule --


def _long_base() -> list[Any]:
    """Two hours of events, one a minute: far longer than the late threshold."""
    return build_base(n_tx=120, accounts=40, spacing_ms=60_000, lag_ms=40)


LONG_COUNTS = {
    FaultClass.EXACT_DUPLICATE: 2,
    FaultClass.LATE: 2,
    FaultClass.LATE_EXACT_DUPLICATE: 1,
    FaultClass.FUTURE_BEYOND_24H: 1,
    FaultClass.MALFORMED_JSON: 1,
}


def test_a_long_paced_base_plans_every_fault_free_record_on_time() -> None:
    overlay = _overlay(base=_long_base(), counts=LONG_COUNTS)
    provenance = overlay.provenance
    assert provenance.base_event_span_s >= 7_000
    planned = provenance.planned_fault_free_delay_s
    assert planned is not None and planned["count"] > 100
    assert planned["max"] <= ON_TIME_CEILING_S
    assert planned["max"] == pytest.approx(0.04), "fault-free records keep the recorded lag"
    assert provenance.schedule_span_s >= provenance.base_event_span_s


def test_a_long_paced_replay_keeps_its_schedule_on_a_simulated_clock() -> None:
    overlay = _overlay(base=_long_base(), counts=LONG_COUNTS)
    clock = FakeClock(ANCHOR)
    publisher = RecordingPublisher()
    manifest = publish_overlay(overlay, publisher, anchor=ANCHOR, clock=clock, sleep=clock.sleep)
    assert len(publisher.published) == len(overlay.records)
    assert clock.slept_s >= overlay.provenance.base_event_span_s, "a paced replay takes its span"
    measured = manifest.fault_free_delay_at_handover_s
    assert measured is not None and measured["max"] < LATE_THRESHOLD_S - ARRIVAL_MARGIN_S
    assert manifest.max_schedule_slip_s <= MAX_SCHEDULE_SLIP_S
    assert manifest.shift_s == (ANCHOR - T0).total_seconds()


def test_the_unpaced_replay_that_made_fault_free_records_late_is_refused() -> None:
    """The defect: shift the newest event to the publish start, then publish at once."""
    overlay = _overlay(base=_long_base(), counts=LONG_COUNTS)
    start = ANCHOR + dt.timedelta(seconds=overlay.provenance.schedule_span_s)
    fault_free = [r for r in overlay.records if r.fault is None]
    late_by_old_scheme = [
        r
        for r in fault_free
        if (start - _parse(_rendered(r, ANCHOR)["envelope"]["occurred_at"])).total_seconds()
        >= LATE_THRESHOLD_S
    ]
    assert len(late_by_old_scheme) / len(fault_free) > 0.9, "the scenario reproduces the defect"
    for record in late_by_old_scheme:
        with pytest.raises(ReplayScheduleError):
            record.render(ANCHOR, published_at=start)


def test_a_replay_that_falls_behind_stops() -> None:
    overlay = _overlay(base=_long_base(), counts=LONG_COUNTS)
    clock = FakeClock(ANCHOR, jump_after_calls=200, jump_s=20 * 60)
    with pytest.raises(ReplayScheduleError):
        publish_overlay(
            overlay, RecordingPublisher(), anchor=ANCHOR, clock=clock, sleep=clock.sleep
        )


def test_the_manifest_reproduces_every_published_byte() -> None:
    overlay = _overlay()
    clock = FakeClock(ANCHOR)
    manifest = publish_overlay(
        overlay, RecordingPublisher(), anchor=ANCHOR, clock=clock, sleep=clock.sleep
    )
    document = manifest.as_dict()
    assert document["anchor"].startswith("2026-09-13T12:00:00")
    for entry, published in zip(document["records"], manifest.records, strict=True):
        again = published.record.render(_parse(document["anchor"]))
        assert "sha256:" + hashlib.sha256(again).hexdigest() == entry["value_sha256"]
        if published.record.malformed_rank is None:
            assert json.loads(again)["envelope"]["occurred_at"] == entry["occurred_at"]


def test_a_paced_base_with_too_much_ingestion_lag_is_refused() -> None:
    laggy = build_base(n_tx=20, lag_ms=400_000)
    with pytest.raises(ValueError, match="refuses rather than mislabel"):
        build_overlay(laggy, FaultPlan(seed=1, counts={}), base_dataset_ref="x")
    # A backfill has no lateness to protect.
    build_overlay(laggy, FaultPlan(seed=1, counts={}, timing=Timing.BACKFILL), base_dataset_ref="x")


# ------------------------------------------------------------------ backfill --


def test_a_backfill_refuses_late_classes_and_marks_every_record() -> None:
    for fault in (
        FaultClass.LATE,
        FaultClass.LATE_EXACT_DUPLICATE,
        FaultClass.LATE_RETRY_NEW_EVENT_ID,
    ):
        with pytest.raises(ValueError, match="is_late is null"):
            FaultPlan(seed=1, counts={fault: 1}, timing=Timing.BACKFILL)
    backfill = _overlay(
        counts={FaultClass.FUTURE_BEYOND_24H: 1, FaultClass.EXACT_DUPLICATE: 1},
        timing=Timing.BACKFILL,
    )
    assert all(r.headers == ((REPLAY_MODE_HEADER, b"backfill"),) for r in backfill.records)
    assert all(r.headers == () for r in _overlay().records), "a paced replay carries no marker"
    base = build_base()
    for record in backfill.records:
        if record.fault is None:
            assert json.loads(record.render(ANCHOR)) == base[record.base_index][1], "no shift"
    future = _of(backfill, FaultClass.FUTURE_BEYOND_24H)[0]
    with pytest.raises(ValueError, match="published_at"):
        future.render(ANCHOR)
    clock = FakeClock(ANCHOR)
    manifest = publish_overlay(
        backfill, RecordingPublisher(), anchor=ANCHOR, clock=clock, sleep=clock.sleep
    )
    assert clock.slept_s == 0 and manifest.shift_s is None


def test_bands_that_touch_a_threshold_are_refused() -> None:
    with pytest.raises(ValueError, match="late arrival"):
        FaultPlan(seed=1, counts={}, late_arrival_delay_s=(LATE_THRESHOLD_S + 1, 2_000))
    with pytest.raises(ValueError, match="future-beyond"):
        FaultPlan(seed=1, counts={}, future_beyond_24h_lead_s=(FUTURE_BOUND_S + 1, 100_000))
    with pytest.raises(ValueError, match="duplicate_delay_s"):
        FaultPlan(seed=1, counts={}, duplicate_delay_s=(1, ON_TIME_CEILING_S))
    with pytest.raises(ValueError, match="reorder_max_gap_s"):
        FaultPlan(seed=1, counts={}, reorder_max_gap_s=ON_TIME_CEILING_S)


def test_an_overlay_with_invalid_records_needs_the_invalid_opt_in() -> None:
    class StrictPublisher(RecordingPublisher):
        allow_invalid_events = False

    overlay = _overlay(counts={FaultClass.MALFORMED_JSON: 1})
    with pytest.raises(ValueError, match="allow_invalid_events"):
        publish_overlay(overlay, StrictPublisher(), anchor=ANCHOR)


# ------------------------------------------------------------- ground truth --


def test_a_base_record_carrying_anything_but_a_released_event_is_refused() -> None:
    topic, event = build_base()[0]
    labelled = {**event, "label": {"is_fraud": True}}
    with pytest.raises(SchemaValidationError, match="released events only"):
        build_overlay([(topic, labelled)], FaultPlan(seed=1, counts={}), base_dataset_ref="x")
    smuggled = json.loads(json.dumps(event))
    smuggled["payload"]["fraud_pattern"] = "account_takeover"
    with pytest.raises(SchemaValidationError, match="not a valid released event"):
        build_overlay([(topic, smuggled)], FaultPlan(seed=1, counts={}), base_dataset_ref="x")


def test_the_overlay_imports_nothing_that_holds_labels() -> None:
    tree = ast.parse((ROOT / "eval" / "replay" / "faults.py").read_text())
    imported = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    imported += [
        a.name for node in ast.walk(tree) if isinstance(node, ast.Import) for a in node.names
    ]
    assert imported, "the import scan found nothing; it would pass vacuously"
    for module in imported:
        assert not any(
            token in module for token in ("groundtruth", "labels", "scenario", "generator")
        ), f"eval/replay/faults.py imports {module}"


def test_provenance_is_serialisable_and_binds_the_base() -> None:
    overlay = _overlay()
    document = overlay.provenance.as_dict()
    text = json.dumps(document, sort_keys=True)
    assert document["base_dataset_ref"] == "fixture:unit-overlay"
    assert document["seed"] == 11
    assert document["requested"] == document["injected"]
    for forbidden in ("is_fraud", "fraud_pattern", "causal_evidence"):
        assert forbidden not in text

    altered = build_base()
    altered[0][1]["payload"]["amount_minor"] += 1
    other = build_overlay(altered, FaultPlan(seed=11, counts=COUNTS), base_dataset_ref="x")
    assert other.provenance.base_digest != overlay.provenance.base_digest


def test_the_dedup_identities_match_the_topic_declaration() -> None:
    declared = yaml.safe_load((ROOT / "deploy" / "kafka" / "topics.yaml").read_text())["topics"]
    mirrored = {topic: ".".join(parts) for topic, parts in DEDUP_IDENTITY.items()}
    assert mirrored == {name: body["dedup_identity"] for name, body in declared.items()}
