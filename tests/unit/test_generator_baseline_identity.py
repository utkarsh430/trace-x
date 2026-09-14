"""eval-v2 stages 1, 1b and 1c: the legitimate baseline in the generator.

What this file proves, in order of how much damage a failure would do:

1. **eval-v1 is untouched.** With `baseline_identity` absent the output is
   byte-identical to the generator at commit 42acb0e -- the literal digests below
   were captured from that commit before any change -- and eval-v1's recorded
   config digest still resolves.
2. **The planted episodes are unchanged under the gate.** The same transaction
   count and labels, and every planted event keeps its account, its channel, its
   planned second and every other override; only its millisecond (T2), its
   location and its entry mode (M1-M3) are redrawn -- and the location only around
   its planted anchor, with impossible travel still impossible.
3. **The new rows are coherent:** schema-valid, event-time ordered, unique
   envelopes, a legitimate FIRST_SEEN truly first, every legitimate row on a device
   the account knows at that moment, every retry identical to its declined attempt.
4. **No new structural proxy:** one payload shape per event type, a coherent
   platform, no whole-second planted events, no copied planted coordinates, planted
   entry modes from the legitimate distribution, one decision-latency distribution.
5. **The declared rates are honoured** within 4-sigma bounds.

Every generation is small and in memory. Nothing here touches PostgreSQL, and
nothing here is eval-v2 acceptance evidence.
"""

from __future__ import annotations

import collections
import datetime as dt
import hashlib
import json
import math
import re
import statistics
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest
from data.generator import planted
from data.generator.baseline import (
    DAYS_PER_YEAR,
    STREAM_ABANDONED,
    STREAM_CHANGES,
    STREAM_DEVICE_ATTRS,
    STREAM_DEVICES,
    STREAM_LOGINS,
    BaselineEvent,
    BaselinePlan,
    DeviceReferences,
    plan_baseline,
    poisson,
    standalone_change_rates,
)
from data.generator.config import CORRECTIONS, BaselineIdentityConfig, GeneratorConfig
from data.generator.digest import canonical_bytes
from data.generator.emit import ValidationPolicy, encode_and_validate
from data.generator.engine import (
    COVERAGE_FLOOR_PREFIX,
    GeneratedRow,
    coverage_mix,
    generate_dataset,
    plan_fraud,
    plan_order_key,
    scenario_device_references,
    tie_order_key,
)
from data.generator.lpc5 import rules as lpc5_rules
from data.generator.lpc5.frame import build_frame
from data.generator.lpc5.frame import knowledge as lpc5_knowledge
from data.generator.population import Universe, build_universe
from data.generator.rng import derive
from data.generator.scenarios import (
    SCENARIOS_BY_PATTERN,
    ImpossibleTravel,
    PlannedEvent,
    ScenarioInstance,
)

from trace_core.domain.enums import FraudPattern
from trace_core.domain.geo import GeoPoint, implied_speed_kmh
from trace_core.domain.time import to_millis

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
TX = "tx.raw.v1"
IDENTITY = "identity.events.v1"
DEVICE = "device.events.v1"


def _millis(row: GeneratedRow) -> int:
    stamp = row.event["envelope"]["occurred_at"]
    return to_millis(dt.datetime.fromisoformat(stamp.replace("Z", "+00:00")))


def _is_baseline(row: GeneratedRow) -> bool:
    return row.topic != TX and row.scenario_instance is None


def _is_scenario_side(row: GeneratedRow) -> bool:
    return row.topic != TX and row.scenario_instance is not None


def _is_legit_tx(row: GeneratedRow) -> bool:
    return row.topic == TX and row.label is not None and not row.label.is_fraud


def _is_fraud_tx(row: GeneratedRow) -> bool:
    return row.topic == TX and row.label is not None and row.label.is_fraud


def _event_type(row: GeneratedRow) -> str:
    payload = row.event["payload"]
    return str(payload.get("identity_event_type") or payload.get("device_event_type"))


def _whole_second(row: GeneratedRow) -> bool:
    return _millis(row) % 1000 == 0


def _assert_within(observed: float, expected: float, sd: float, what: str) -> None:
    tolerance = 4.0 * sd + 1.0
    assert abs(observed - expected) <= tolerance, (
        f"{what}: observed {observed}, expected {expected:.1f} +/- {tolerance:.1f} (4 sd)"
    )


# =========================================================== 1. eval-v1 =====

PRE_CHANGE_SMALL = {
    "all_rows": "sha256:7e1477b0141056ffd59d5cbf8fc873e52ece686f42427308e9845c4214212b01",
    "tx_rows": "sha256:4ba077b991b7a443b22a095d629090a9358e26414612081deff4ff542eff6700",
    "labels": "sha256:96015d83419d4673dc9e8e06135a0ed6d2ba648d6a2080ab1e4938d35d69e086",
    "counts": {"tx.raw.v1": 20000, "identity.events.v1": 30, "device.events.v1": 2},
}
"""Captured from commit 42acb0e, before this change, with `_digests` below."""

PRE_CHANGE_EVAL_V1_PREFIX = {
    "all_rows": "sha256:2aee472c76012aff29105c2be79b1b33a93adf0f9a71ab413037a9fd2fea5b72",
    "tx_rows": "sha256:942db37c29cd6123a28424f5ddf9bd9f5f3ff97c445ebf648a0aaeaf6c34675d",
    "labels": "sha256:b3d064200ee948211890ff2f28c2a2338e6c0d5b029eceb86504da53f4a75ade",
    "counts": {"tx.raw.v1": 2000, "identity.events.v1": 31, "device.events.v1": 1},
}
"""eval-v1's recorded config at 2,000 rows, captured from commit 42acb0e."""


def _digests(rows: Iterable[GeneratedRow]) -> dict[str, Any]:
    """Every row of every topic, the transaction rows, and every label and episode."""
    everything = hashlib.sha256()
    tx_only = hashlib.sha256()
    labels = hashlib.sha256()
    counts: dict[str, int] = {}
    for row in rows:
        counts[row.topic] = counts.get(row.topic, 0) + 1
        encoded = canonical_bytes(row.event)
        everything.update(row.topic.encode() + b"\x1f" + encoded + b"\n")
        if row.topic == TX:
            tx_only.update(encoded + b"\n")
        label = row.label
        labels.update(
            json.dumps(
                {
                    "topic": row.topic,
                    "tx": label.transaction_id if label else None,
                    "fraud": label.is_fraud if label else None,
                    "pattern": label.fraud_pattern.value if label and label.fraud_pattern else None,
                    "instance": row.scenario_instance.instance_id
                    if row.scenario_instance
                    else None,
                    "keys": sorted(k.value for k in label.causal_evidence_keys) if label else None,
                },
                sort_keys=True,
            ).encode()
            + b"\n"
        )
    return {
        "all_rows": "sha256:" + everything.hexdigest(),
        "tx_rows": "sha256:" + tx_only.hexdigest(),
        "labels": "sha256:" + labels.hexdigest(),
        "counts": counts,
    }


def _eval_v1_manifest() -> dict[str, Any]:
    manifest: dict[str, Any] = json.loads(
        (ROOT / "eval" / "track_a" / "eval-v1.manifest.json").read_text()
    )
    return manifest


def test_gate_off_output_is_byte_identical_to_the_pre_change_generator() -> None:
    """Every row of every topic, every label: not merely the transaction digest."""
    config = GeneratorConfig(
        row_count=20_000,
        account_count=800,
        merchant_count=60,
        device_count=960,
        ip_count=400,
        fraud_rate=0.01,
        seed=7,
    )
    assert _digests(generate_dataset(config)) == PRE_CHANGE_SMALL


def test_gate_off_eval_v1_config_prefix_is_byte_identical_to_the_pre_change_generator() -> None:
    config = GeneratorConfig.from_mapping({**_eval_v1_manifest()["config"], "row_count": 2000})
    assert config.baseline_identity is None
    assert _digests(generate_dataset(config)) == PRE_CHANGE_EVAL_V1_PREFIX


def test_an_absent_block_is_omitted_from_the_canonical_config() -> None:
    assert "baseline_identity" not in json.loads(GeneratorConfig().canonical_json())


def test_eval_v1_recorded_config_digest_still_resolves() -> None:
    manifest = _eval_v1_manifest()
    assert "baseline_identity" not in manifest["config"]
    config = GeneratorConfig.from_mapping(manifest["config"])
    assert config.digest() == manifest["fraud_scenario_config_digest"]


def test_a_present_block_changes_the_digest_and_round_trips() -> None:
    enabled = GeneratorConfig(baseline_identity=BaselineIdentityConfig())
    assert enabled.digest() != GeneratorConfig().digest()
    restored = GeneratorConfig.from_mapping(json.loads(enabled.canonical_json()))
    assert restored == enabled
    assert restored.digest() == enabled.digest()


@pytest.mark.parametrize("field", sorted(BaselineIdentityConfig.model_fields))
def test_every_baseline_setting_changes_the_config_digest(field: str) -> None:
    """A knob that does not reach the digest is a knob that lies."""
    default = BaselineIdentityConfig()
    value = getattr(default, field)
    changed: Any
    if field == "disabled_corrections":
        changed = ("T1",)
    elif isinstance(value, tuple):
        changed = tuple(reversed(value))
    elif isinstance(value, int):
        changed = value // 2
    else:
        changed = round(float(value) * 0.5, 6)
    altered = default.model_copy(update={field: changed})
    assert (
        GeneratorConfig(baseline_identity=altered).digest()
        != GeneratorConfig(baseline_identity=default).digest()
    )


@pytest.mark.parametrize(
    "update",
    [
        {"login_away_ip_share": 1.5},
        {"new_device_rate_per_account_year": -1.0},
        {"decline_share_per_transaction": 1.2},
        {"typo_burst_size_weights": (0.0, 0.0, 0.0, 0.0, 0.0)},
        {"mfa_reset_on_new_device_share": 0.6, "mfa_enrolled_on_new_device_share": 0.6},
        {"coverage_floor_instances": 0},
        {"disabled_corrections": ("X9",)},
        {"disabled_corrections": ("T2", "T1")},
        {"disabled_corrections": ("T1", "T1")},
    ],
    ids=[
        "share-above-one",
        "negative-rate",
        "decline-share-above-one",
        "zero-weights",
        "exclusive-shares-exceed-one",
        "floor-below-one",
        "unknown-correction",
        "corrections-out-of-order",
        "duplicate-correction",
    ],
)
def test_invalid_baseline_settings_are_refused(update: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        BaselineIdentityConfig.model_validate({**BaselineIdentityConfig().model_dump(), **update})


# ====================================================== 2-4. gate on/off =====


def _coherence_config(**overrides: Any) -> GeneratorConfig:
    base: dict[str, Any] = {
        "row_count": 30_000,
        "account_count": 1_200,
        "merchant_count": 90,
        "device_count": 1_440,
        "ip_count": 600,
        "fraud_rate": 0.005,
        "seed": 42,
    }
    base.update(overrides)
    return GeneratorConfig(**base)


@pytest.fixture(scope="module")
def gate_off() -> list[GeneratedRow]:
    return list(generate_dataset(_coherence_config()))


_EVAL_V1_INSTANCES = BaselineIdentityConfig(coverage_floor_instances=1)
"""The gate with eval-v1's instance plan: G2's floor of one adds nothing, so gate-on and gate-off
plan the same episodes and the tests below compare like with like. G2 has its own tests."""


@pytest.fixture(scope="module")
def gate_on() -> list[GeneratedRow]:
    return list(generate_dataset(_coherence_config(baseline_identity=_EVAL_V1_INSTANCES)))


@pytest.fixture(scope="module")
def plan() -> tuple[Universe, BaselinePlan]:
    """The same plan the engine builds, recomputed through the public API."""
    config = _coherence_config(baseline_identity=_EVAL_V1_INSTANCES)
    universe = build_universe(config)
    instances, _ = plan_fraud(config, universe)
    references = scenario_device_references(config, universe, instances)
    return universe, plan_baseline(config, universe, references)


@pytest.fixture(scope="module")
def universe(plan: tuple[Universe, BaselinePlan]) -> Universe:
    return plan[0]


def _copied(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        k: v for k, v in payload.items() if k not in ("transaction_id", "authorization_outcome")
    }


def _retry_pairs(rows: list[GeneratedRow]) -> list[tuple[GeneratedRow, GeneratedRow]]:
    """(declined attempt, approved retry) pairs, found from the rows alone."""
    by_account: dict[str, list[GeneratedRow]] = collections.defaultdict(list)
    for row in rows:
        if _is_legit_tx(row):
            by_account[row.event["payload"]["account_id"]].append(row)
    pairs: list[tuple[GeneratedRow, GeneratedRow]] = []
    for account_rows in by_account.values():
        account_rows.sort(key=_millis)
        for index, row in enumerate(account_rows):
            if row.event["payload"]["authorization_outcome"] != "DECLINED":
                continue
            copied = _copied(row.event["payload"])
            for later in account_rows[index + 1 :]:
                if _millis(later) - _millis(row) > 600_000:
                    break
                payload = later.event["payload"]
                if payload["authorization_outcome"] == "APPROVED" and _copied(payload) == copied:
                    pairs.append((row, later))
                    break
    return pairs


@pytest.fixture(scope="module")
def pairs(gate_on: list[GeneratedRow]) -> list[tuple[GeneratedRow, GeneratedRow]]:
    return _retry_pairs(gate_on)


def test_transaction_count_and_labels_are_preserved_under_the_gate(
    gate_off: list[GeneratedRow], gate_on: list[GeneratedRow]
) -> None:
    def labels(rows: list[GeneratedRow]) -> collections.Counter[tuple[object, ...]]:
        return collections.Counter(
            (r.label.is_fraud, r.label.fraud_pattern, r.label.scenario_instance_id)
            for r in rows
            if r.topic == TX and r.label is not None
        )

    assert sum(1 for r in gate_on if r.topic == TX) == _coherence_config().row_count
    assert labels(gate_on) == labels(gate_off)


_REDRAWN_UNDER_THE_GATE = frozenset({"latitude", "longitude", "entry_mode"})
"""M1-M3: the planted fields the gate redraws with the legitimate model."""


def _planted_matches(
    planned: PlannedEvent,
    row: GeneratedRow,
    universe: Universe,
    redrawn: frozenset[str] = frozenset(),
) -> bool:
    payload = row.event["payload"]
    if planned.topic != row.topic:
        return False
    if universe.profiles[planned.account_index].account_id != payload["account_id"]:
        return False
    if planned.occurred_ms // 1000 != _millis(row) // 1000:
        return False
    if row.topic == IDENTITY and payload["identity_event_type"] != (
        planned.identity_event_type or "UNKNOWN"
    ):
        return False
    if row.topic == DEVICE and payload["device_event_type"] != (
        planned.device_event_type or "UNKNOWN"
    ):
        return False
    return all(
        payload.get(key) == value for key, value in planned.overrides.items() if key not in redrawn
    )


def _planted_instances_following_their_plan(
    rows: list[GeneratedRow], universe: Universe, redrawn: frozenset[str] = frozenset()
) -> int:
    emitted: dict[str, list[GeneratedRow]] = collections.defaultdict(list)
    instances = {}
    for row in rows:
        if row.scenario_instance is not None:
            emitted[row.scenario_instance.instance_id].append(row)
            instances[row.scenario_instance.instance_id] = row.scenario_instance
    for instance_id, instance in instances.items():
        unmatched = list(instance.events)
        for row in emitted[instance_id]:
            match = next(
                (p for p in unmatched if _planted_matches(p, row, universe, redrawn)), None
            )
            assert match is not None, f"{instance_id}: an emitted {row.topic} matches no plan"
            unmatched.remove(match)
        assert not unmatched, f"{instance_id}: {len(unmatched)} planned events were not emitted"
    return len(instances)


def test_planted_events_keep_account_overrides_and_second_with_the_gate_on_or_off(
    gate_off: list[GeneratedRow], gate_on: list[GeneratedRow], universe: Universe
) -> None:
    """Scenario definitions are unchanged. Under the gate the millisecond (T2) and the
    planted location and entry mode (M1-M3) are redrawn; every other planted field --
    channel included -- is kept exactly."""
    off = _planted_instances_following_their_plan(gate_off, universe)
    assert off > 0
    assert (
        _planted_instances_following_their_plan(gate_on, universe, _REDRAWN_UNDER_THE_GATE) == off
    )


def _plan_of(row: GeneratedRow, universe: Universe) -> PlannedEvent:
    """The planned event an emitted planted row came from, ignoring redrawn fields."""
    assert row.scenario_instance is not None
    candidates = [
        planned
        for planned in row.scenario_instance.events
        if _planted_matches(planned, row, universe, _REDRAWN_UNDER_THE_GATE)
    ]
    assert candidates, f"no planned event for {row.event['payload'].get('transaction_id')}"
    return candidates[0]


def test_planted_locations_are_drawn_around_their_anchor_never_copied(
    gate_off: list[GeneratedRow], gate_on: list[GeneratedRow], universe: Universe
) -> None:
    """M1 and M2. eval-v1 copies the planted point onto every planted transaction --
    repeated through a takeover, the home point itself on a travel leg. The gate
    draws around that anchor with the legitimate noise model."""
    from trace_core.domain.geo import haversine_km

    homes = {
        profile.account_id: (profile.account.home.latitude, profile.account.home.longitude)
        for profile in universe.profiles
    }

    def located(rows: list[GeneratedRow]) -> list[tuple[GeneratedRow, PlannedEvent]]:
        found = []
        for row in rows:
            if _is_fraud_tx(row):
                planned = _plan_of(row, universe)
                if "latitude" in planned.overrides:
                    found.append((row, planned))
        return found

    off = located(gate_off)
    assert off
    assert all(
        (r.event["payload"]["latitude"], r.event["payload"]["longitude"])
        == (p.overrides["latitude"], p.overrides["longitude"])
        for r, p in off
    )

    on = located(gate_on)
    assert len(on) == len(off)
    jitter = _coherence_config().geo_jitter_km
    for row, planned in on:
        payload = row.event["payload"]
        point = (payload["latitude"], payload["longitude"])
        anchor = (planned.overrides["latitude"], planned.overrides["longitude"])
        assert point != anchor
        assert point != homes[payload["account_id"]]
        assert haversine_km(GeoPoint(*anchor), GeoPoint(*point)) < 20 * jitter
        if row.label and row.label.fraud_pattern is FraudPattern.ACCOUNT_TAKEOVER:
            home = GeoPoint(*homes[payload["account_id"]])
            assert haversine_km(home, GeoPoint(*point)) > 100, "a takeover must stay away from home"
    places = collections.Counter(
        (
            r.event["payload"]["account_id"],
            r.event["payload"]["latitude"],
            r.event["payload"]["longitude"],
        )
        for r in gate_on
        if _is_fraud_tx(r)
    )
    assert max(places.values()) == 1, "a planted transaction repeats an exact coordinate"


def test_planted_entry_modes_are_drawn_from_the_channels_legitimate_modes(
    gate_off: list[GeneratedRow], gate_on: list[GeneratedRow], universe: Universe
) -> None:
    """M3 and M6. eval-v1 forces ECOMMERCE (card-not-present) and CHIP (card-present) on
    planted transactions. Under the gate only impossible travel keeps a planted channel (M6),
    and its entry mode is drawn from that channel's legitimate modes (M3). Every other planted
    transaction draws channel and entry mode the legitimate way."""
    from data.generator.engine import _CHANNEL_ENTRY

    def planted_modes(rows: list[GeneratedRow]) -> list[tuple[GeneratedRow, PlannedEvent]]:
        found = []
        for row in rows:
            if _is_fraud_tx(row):
                planned = _plan_of(row, universe)
                if "entry_mode" in planned.overrides:
                    found.append((row, planned))
        return found

    off = planted_modes(gate_off)
    assert off
    assert {r.event["payload"]["entry_mode"] for r, _ in off} <= {"ECOMMERCE", "CHIP"}

    on = planted_modes(gate_on)
    assert on
    for row, planned in on:
        payload = row.event["payload"]
        assert _instance(row).pattern is FraudPattern.IMPOSSIBLE_TRAVEL
        assert payload["channel"] == planned.overrides["channel"] == "CARD_PRESENT"
        assert payload["entry_mode"] in _CHANNEL_ENTRY[payload["channel"]]
    cnp = [
        r
        for r in gate_on
        if _is_fraud_tx(r) and r.event["payload"]["channel"] == "CARD_NOT_PRESENT"
    ]
    assert len(cnp) >= 30
    modes = collections.Counter(r.event["payload"]["entry_mode"] for r in cnp)
    assert set(modes) == {"ECOMMERCE", "TOKEN", "MANUAL"}
    assert modes["ECOMMERCE"] / len(cnp) < 0.6


def test_the_gate_actually_adds_legitimate_activity(gate_on: list[GeneratedRow]) -> None:
    kinds = collections.Counter((r.topic, _event_type(r)) for r in gate_on if _is_baseline(r))
    scenario_side = sum(1 for r in gate_on if _is_scenario_side(r))
    assert sum(kinds.values()) > 20 * scenario_side
    for expected in (
        (IDENTITY, "LOGIN_SUCCEEDED"),
        (IDENTITY, "LOGIN_FAILED"),
        (IDENTITY, "PASSWORD_CHANGE"),
        (IDENTITY, "EMAIL_CHANGE"),
        (IDENTITY, "PHONE_CHANGE"),
        (IDENTITY, "ADDRESS_CHANGE"),
        (IDENTITY, "MFA_RESET"),
        (IDENTITY, "MFA_ENROLLED"),
        (DEVICE, "FIRST_SEEN"),
        (DEVICE, "ATTRIBUTE_CHANGED"),
        (DEVICE, "FINGERPRINT_CHANGED"),
    ):
        assert kinds[expected] > 0, f"no legitimate {expected} events at all"


def test_every_event_satisfies_its_released_schema(gate_on: list[GeneratedRow]) -> None:
    """docs/EVENT_CONTRACTS.md §6.1: an invalid message is never published."""
    for position, row in enumerate(gate_on):
        encode_and_validate(row.topic, row.event, ValidationPolicy.ALL, position)


def test_the_dataset_is_event_time_ordered(gate_on: list[GeneratedRow]) -> None:
    """Compared as milliseconds, not strings: a whole-second ISO string sorts
    after a fractional one within the same second."""
    times = [_millis(r) for r in gate_on]
    assert times == sorted(times)


def test_processing_time_is_never_before_event_time(gate_on: list[GeneratedRow]) -> None:
    for row in gate_on:
        envelope = row.event["envelope"]
        occurred = dt.datetime.fromisoformat(envelope["occurred_at"].replace("Z", "+00:00"))
        ingested = dt.datetime.fromisoformat(envelope["ingested_at"].replace("Z", "+00:00"))
        assert ingested >= occurred


@pytest.mark.parametrize("field", ["event_id", "idempotency_key", "trace_id", "correlation_id"])
def test_envelopes_are_unique_across_every_event(gate_on: list[GeneratedRow], field: str) -> None:
    """N10 included: until outcome events exist, every business flow is one event."""
    values = collections.Counter(r.event["envelope"][field] for r in gate_on)
    duplicated = {value: count for value, count in values.items() if count > 1}
    assert not duplicated, f"{len(duplicated)} {field} values are shared"


_MS_Z = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


def test_n11_every_timestamp_carries_exactly_three_fractional_digits(
    gate_off: list[GeneratedRow], gate_on: list[GeneratedRow]
) -> None:
    for row in gate_on:
        envelope = row.event["envelope"]
        assert _MS_Z.fullmatch(envelope["occurred_at"]), envelope["occurred_at"]
        assert _MS_Z.fullmatch(envelope["ingested_at"]), envelope["ingested_at"]
    assert not all(_MS_Z.fullmatch(r.event["envelope"]["occurred_at"]) for r in gate_off)


def test_n11_ties_are_ordered_by_identity_not_by_whether_an_event_is_planted(
    universe: Universe,
) -> None:
    """At one millisecond, a planted event comes first for some seeds and later for others."""
    config = _coherence_config(baseline_identity=_EVAL_V1_INSTANCES)
    instance = plan_fraud(config, universe)[0][0]
    planted = (1_000, 10**9, 1, (instance, instance.events[0], 0))
    legitimate = (1_000, 0, 3, None)
    first = [plan_order_key(seed, planted) < plan_order_key(seed, legitimate) for seed in range(64)]
    assert any(first) and not all(first)
    assert plan_order_key(7, planted)[1] == tie_order_key(7, f"{instance.instance_id}:0")
    assert plan_order_key(7, legitimate)[1] == tie_order_key(7, "0")


def test_gate_off_still_carries_eval_v1_shared_side_event_envelopes(
    gate_off: list[GeneratedRow],
) -> None:
    """eval-v1 is frozen, defects included. This pins that the defect the gate
    fixes is real, so the uniqueness test above is not trivially true."""
    keys = collections.Counter(
        r.event["envelope"]["idempotency_key"] for r in gate_off if _is_scenario_side(r)
    )
    assert any(count > 1 for count in keys.values())


def test_legitimate_and_scenario_events_share_one_payload_shape_per_type(
    gate_on: list[GeneratedRow],
) -> None:
    """A different key set per source would betray which events are fraud."""
    shapes: dict[tuple[str, str], dict[bool, set[tuple[str, ...]]]] = collections.defaultdict(
        lambda: collections.defaultdict(set)
    )
    for row in gate_on:
        if row.topic == TX:
            continue
        shapes[(row.topic, _event_type(row))][_is_baseline(row)].add(
            tuple(sorted(row.event["payload"]))
        )
    shared = [kind for kind, by_source in shapes.items() if len(by_source) == 2]
    for kind in shared:
        assert len(shapes[kind][True]) == 1, (kind, shapes[kind][True])
        assert shapes[kind][True] == shapes[kind][False], kind
    assert {
        (IDENTITY, "LOGIN_FAILED"),
        (IDENTITY, "LOGIN_SUCCEEDED"),
        (DEVICE, "FIRST_SEEN"),
    } <= set(shared)
    assert any(kind[1] in {"PASSWORD_CHANGE", "EMAIL_CHANGE", "MFA_RESET"} for kind in shared)


def test_device_events_report_the_universe_devices_platform(
    gate_on: list[GeneratedRow], universe: Universe
) -> None:
    platforms = {device.device_id: device.platform for device in universe.devices}
    device_rows = [r for r in gate_on if r.topic == DEVICE]
    assert any(_is_scenario_side(r) for r in device_rows)
    for row in device_rows:
        payload = row.event["payload"]
        assert payload["platform"] == platforms[payload["device_id"]]


def test_planted_events_lose_the_whole_second_shape(
    gate_off: list[GeneratedRow], gate_on: list[GeneratedRow]
) -> None:
    """T2. eval-v1: every takeover change, FIRST_SEEN and single-transaction
    scenario lands on a whole second. eval-v2: a uniform millisecond, like
    legitimate transactions."""
    takeover_kinds = {"FIRST_SEEN", "PASSWORD_CHANGE", "EMAIL_CHANGE", "MFA_RESET"}
    off_side = [r for r in gate_off if _is_scenario_side(r) and _event_type(r) in takeover_kinds]
    assert off_side and all(map(_whole_second, off_side))
    singles = {FraudPattern.ANOMALOUS_HIGH_VALUE, FraudPattern.UNUSUAL_LOCATION_DEVICE}
    off_tx = [
        r for r in gate_off if _is_fraud_tx(r) and r.label and r.label.fraud_pattern in singles
    ]
    assert off_tx and all(map(_whole_second, off_tx))

    planted = [r for r in gate_on if r.scenario_instance is not None]
    milliseconds = [_millis(r) % 1000 for r in planted]
    assert sum(1 for ms in milliseconds if ms == 0) <= max(2, 0.02 * len(milliseconds))
    assert 350 <= sum(milliseconds) / len(milliseconds) <= 650
    legitimate = [r for r in gate_on if _is_legit_tx(r)]
    assert sum(map(_whole_second, legitimate)) / len(legitimate) < 0.005
    baseline = [r for r in gate_on if _is_baseline(r)]
    assert sum(map(_whole_second, baseline)) / len(baseline) < 0.01


def test_impossible_travel_stays_infeasible_on_emitted_rows(gate_on: list[GeneratedRow]) -> None:
    """A sub-second timing change must never make an implied speed feasible."""
    legs: dict[str, list[GeneratedRow]] = collections.defaultdict(list)
    for row in gate_on:
        if (
            _is_fraud_tx(row)
            and row.label
            and row.label.fraud_pattern is FraudPattern.IMPOSSIBLE_TRAVEL
        ):
            legs[str(row.label.scenario_instance_id)].append(row)
    assert legs
    threshold = ImpossibleTravel().min_speed_kmh
    for rows in legs.values():
        first, second = sorted(rows, key=_millis)
        a, b = first.event["payload"], second.event["payload"]
        speed = implied_speed_kmh(
            GeoPoint(a["latitude"], a["longitude"]),
            GeoPoint(b["latitude"], b["longitude"]),
            (_millis(second) - _millis(first)) / 1000,
        )
        assert speed > threshold


def _legitimate_first_seen(rows: list[GeneratedRow]) -> dict[tuple[str, str], int]:
    first_seen: dict[tuple[str, str], int] = {}
    for row in rows:
        if _is_baseline(row) and row.topic == DEVICE and _event_type(row) == "FIRST_SEEN":
            pair = (row.event["payload"]["account_id"], row.event["payload"]["device_id"])
            assert pair not in first_seen, f"two legitimate FIRST_SEEN for {pair}"
            first_seen[pair] = _millis(row)
    return first_seen


def test_a_legitimate_first_seen_precedes_every_reference_to_that_device(
    gate_on: list[GeneratedRow],
) -> None:
    first_seen = _legitimate_first_seen(gate_on)
    assert first_seen
    later_on_a_transaction: set[tuple[str, str]] = set()
    for row in gate_on:
        payload = row.event["payload"]
        device = payload.get("device_id")
        if device is None:
            continue
        pair = (payload["account_id"], device)
        if pair not in first_seen:
            continue
        assert _millis(row) >= first_seen[pair], (
            f"{row.topic} references {pair} before its legitimate FIRST_SEEN"
        )
        if row.topic == TX:
            assert _millis(row) > first_seen[pair]
            later_on_a_transaction.add(pair)
    assert later_on_a_transaction
    assert set(first_seen) - later_on_a_transaction


def test_legitimate_rows_use_only_devices_known_at_their_time(
    gate_on: list[GeneratedRow], plan: tuple[Universe, BaselinePlan]
) -> None:
    universe, baseline = plan
    first_seen = _legitimate_first_seen(gate_on)
    index_of = {profile.account_id: index for index, profile in enumerate(universe.profiles)}
    planned_enrolments = {
        (universe.profiles[index].account_id, device): tau
        for index, devices in baseline.payment_devices.items()
        for tau, device in devices.enrolled
    }
    assert first_seen == planned_enrolments, "the engine emitted a different enrolment plan"

    on_secondary = on_enrolled = 0
    for row in gate_on:
        if not (_is_baseline(row) or _is_legit_tx(row)):
            continue
        payload = row.event["payload"]
        account, device = payload["account_id"], payload["device_id"]
        index = index_of[account]
        if device in set(universe.profiles[index].home_devices):
            continue
        devices = baseline.payment_devices.get(index)
        if devices is not None and device == devices.secondary:
            on_secondary += row.topic == TX
            continue
        tau = first_seen.get((account, device))
        assert tau is not None, f"{row.topic} on {account}/{device}: unknown to the account"
        if row.topic == TX:
            assert tau < _millis(row)
            on_enrolled += 1
        else:
            assert tau <= _millis(row)
    assert on_secondary > 0 and on_enrolled > 0


def test_scenario_novel_devices_stay_novel_with_the_gate_on(
    gate_on: list[GeneratedRow], universe: Universe
) -> None:
    """DEVICE_NOVELTY is a causal key for takeover and unusual-location-device.
    No legitimate row may make the account already know that device."""
    index_of = {profile.account_id: profile for profile in universe.profiles}
    legit_devices: dict[str, set[str]] = collections.defaultdict(set)
    for row in gate_on:
        if _is_baseline(row) or _is_legit_tx(row):
            legit_devices[row.event["payload"]["account_id"]].add(row.event["payload"]["device_id"])
    checked = 0
    novel = {FraudPattern.ACCOUNT_TAKEOVER, FraudPattern.UNUSUAL_LOCATION_DEVICE}
    for row in gate_on:
        if not (_is_fraud_tx(row) and row.label and row.label.fraud_pattern in novel):
            continue
        account, device = row.event["payload"]["account_id"], row.event["payload"]["device_id"]
        assert device not in set(index_of[account].home_devices)
        assert device not in legit_devices[account]
        checked += 1
    assert checked > 0


def test_card_testing_declines_are_unchanged_and_no_other_planted_row_declines(
    gate_off: list[GeneratedRow], gate_on: list[GeneratedRow]
) -> None:
    def outcomes(rows: list[GeneratedRow]) -> collections.Counter[tuple[object, ...]]:
        return collections.Counter(
            (r.label.scenario_instance_id, r.event["payload"]["authorization_outcome"])
            for r in rows
            if _is_fraud_tx(r) and r.label
        )

    assert outcomes(gate_on) == outcomes(gate_off)
    declined = {
        r.label.fraud_pattern
        for r in gate_on
        if _is_fraud_tx(r) and r.label and r.event["payload"]["authorization_outcome"] == "DECLINED"
    }
    assert declined == {FraudPattern.CARD_TESTING}


def test_legitimate_declines_exist_and_retries_copy_their_attempt(
    gate_on: list[GeneratedRow], pairs: list[tuple[GeneratedRow, GeneratedRow]]
) -> None:
    assert any(
        _is_legit_tx(r) and r.event["payload"]["authorization_outcome"] == "DECLINED"
        for r in gate_on
    )
    assert pairs
    for attempt, retry in pairs:
        gap = _millis(retry) - _millis(attempt)
        assert 20_000 <= gap < 600_000, gap
        assert (
            attempt.event["payload"]["transaction_id"] != retry.event["payload"]["transaction_id"]
        )


def test_decision_time_exists_only_under_the_gate(
    gate_off: list[GeneratedRow], gate_on: list[GeneratedRow]
) -> None:
    assert all(r.authorization_decided_ms is None for r in gate_off)
    for row in gate_on:
        if row.topic == TX:
            assert row.authorization_decided_ms is not None
            assert row.authorization_decided_ms >= _millis(row) + 40
        else:
            assert row.authorization_decided_ms is None


def test_decision_latency_does_not_depend_on_outcome_or_label(gate_on: list[GeneratedRow]) -> None:
    """A latency that differed by outcome or label would become a proxy the moment
    authorization outcomes were emitted as their own dated events."""
    groups: dict[str, list[int]] = collections.defaultdict(list)
    for row in gate_on:
        if row.topic != TX or row.authorization_decided_ms is None:
            continue
        latency = row.authorization_decided_ms - _millis(row)
        if _is_fraud_tx(row):
            groups["fraud"].append(latency)
        else:
            groups[f"legit-{row.event['payload']['authorization_outcome']}"].append(latency)
    reference = groups["legit-APPROVED"]
    sd = statistics.pstdev(reference)
    for name, values in groups.items():
        assert len(values) >= 30, (name, len(values))
        tolerance = 5 * sd * (1 / math.sqrt(len(values)) + 1 / math.sqrt(len(reference)))
        assert abs(statistics.fmean(values) - statistics.fmean(reference)) <= tolerance, name


def test_gate_on_generation_is_deterministic() -> None:
    config = _coherence_config(
        row_count=6_000, account_count=240, baseline_identity=BaselineIdentityConfig()
    )
    assert _digests(generate_dataset(config)) == _digests(generate_dataset(config))


def test_retuning_identity_activity_never_moves_a_transaction() -> None:
    """Login and change rates feed no transaction decision, so they must not move
    a single transaction row."""
    base = _coherence_config(row_count=6_000, account_count=240)
    default = base.model_copy(update={"baseline_identity": BaselineIdentityConfig()})
    busier = base.model_copy(
        update={
            "baseline_identity": BaselineIdentityConfig(
                login_rate_per_account_day=1.5, password_change_rate_per_account_year=4.0
            )
        }
    )
    first, second = _digests(generate_dataset(default)), _digests(generate_dataset(busier))
    assert first["tx_rows"] == second["tx_rows"]
    assert first["all_rows"] != second["all_rows"]


# ------------------------------------------------ T1 / T3 rates, emitted ----


def _singles(
    gate_on: list[GeneratedRow], pairs: list[tuple[GeneratedRow, GeneratedRow]]
) -> list[GeneratedRow]:
    """Legitimate transactions that made their own device decision (not retries)."""
    retries = {id(retry) for _, retry in pairs}
    return [r for r in gate_on if _is_legit_tx(r) and id(r) not in retries]


def test_new_device_payment_share_is_honoured(
    gate_on: list[GeneratedRow],
    pairs: list[tuple[GeneratedRow, GeneratedRow]],
    plan: tuple[Universe, BaselinePlan],
) -> None:
    universe, baseline = plan
    index_of = {profile.account_id: index for index, profile in enumerate(universe.profiles)}
    eligible = chosen = 0
    for row in _singles(gate_on, pairs):
        payload = row.event["payload"]
        devices = baseline.payment_devices.get(index_of[payload["account_id"]])
        latest = devices.latest_enrolled_before(_millis(row)) if devices else None
        if latest is None:
            continue
        eligible += 1
        chosen += payload["device_id"] == latest
    share = BaselineIdentityConfig().new_device_payment_share
    assert eligible > 200
    _assert_within(
        chosen, eligible * share, math.sqrt(eligible * share * (1 - share)), "new-device share"
    )


def test_secondary_device_transaction_share_is_honoured(
    gate_on: list[GeneratedRow],
    pairs: list[tuple[GeneratedRow, GeneratedRow]],
    plan: tuple[Universe, BaselinePlan],
) -> None:
    universe, baseline = plan
    index_of = {profile.account_id: index for index, profile in enumerate(universe.profiles)}
    eligible = chosen = 0
    for row in _singles(gate_on, pairs):
        payload = row.event["payload"]
        devices = baseline.payment_devices.get(index_of[payload["account_id"]])
        if devices is None or devices.secondary is None:
            continue
        if devices.latest_enrolled_before(_millis(row)) is not None:
            continue
        eligible += 1
        chosen += payload["device_id"] == devices.secondary
    share = BaselineIdentityConfig().secondary_device_transaction_share
    assert eligible > 1_000
    _assert_within(
        chosen, eligible * share, math.sqrt(eligible * share * (1 - share)), "secondary share"
    )


def test_decline_rates_are_honoured_with_per_account_structure(
    gate_on: list[GeneratedRow], pairs: list[tuple[GeneratedRow, GeneratedRow]]
) -> None:
    settings = BaselineIdentityConfig()
    spread = math.exp(settings.decline_propensity_sigma**2) - 1
    legitimate = [r for r in gate_on if _is_legit_tx(r)]
    rows_per_account = collections.Counter(r.event["payload"]["account_id"] for r in legitimate)
    pairs_per_account = collections.Counter(a.event["payload"]["account_id"] for a, _ in pairs)

    def expectation(p: float, counts: list[int]) -> tuple[float, float]:
        mean = sum(n * p for n in counts)
        variance = sum(n * p + (n * p) ** 2 * spread for n in counts)
        return mean, math.sqrt(variance)

    decisions = [rows_per_account[a] - pairs_per_account[a] for a in rows_per_account]
    mean, sd = expectation(settings.decline_retry_share_per_transaction, decisions)
    _assert_within(len(pairs), mean, sd, "retry pairs")

    attempts = {id(attempt) for attempt, _ in pairs}
    standalone = sum(
        1
        for r in legitimate
        if r.event["payload"]["authorization_outcome"] == "DECLINED" and id(r) not in attempts
    )
    singles = [rows_per_account[a] - 2 * pairs_per_account[a] for a in rows_per_account]
    mean, sd = expectation(settings.decline_share_per_transaction, singles)
    _assert_within(standalone, mean, sd, "standalone declines")

    declines = collections.Counter(
        r.event["payload"]["account_id"]
        for r in legitimate
        if r.event["payload"]["authorization_outcome"] == "DECLINED"
    )
    counts = [declines.get(account, 0) for account in rows_per_account]
    average = statistics.fmean(counts)
    assert statistics.pvariance(counts) / average > 1.5, "declines are not concentrated"


# ============================================================= 5. rates =====

RATE_ACCOUNTS = 6_000


def _rates_config(**settings: Any) -> GeneratorConfig:
    return GeneratorConfig(
        row_count=1_000,
        account_count=RATE_ACCOUNTS,
        merchant_count=100,
        device_count=7_200,
        ip_count=3_000,
        fraud_rate=0.0,
        seed=11,
        baseline_identity=BaselineIdentityConfig(**settings),
    )


@pytest.fixture(scope="module")
def rates_universe() -> Universe:
    return build_universe(_rates_config())


@pytest.fixture(scope="module")
def rates_plan(rates_universe: Universe) -> BaselinePlan:
    return plan_baseline(_rates_config(), rates_universe, DeviceReferences())


@pytest.fixture(scope="module")
def planned(rates_plan: BaselinePlan) -> list[BaselineEvent]:
    return rates_plan.events


def _years() -> float:
    return _rates_config().window_seconds / 86_400 / DAYS_PER_YEAR


def _days() -> float:
    return _rates_config().window_seconds / 86_400


def _count(events: list[BaselineEvent], stream: str, event_type: str) -> int:
    return sum(1 for e in events if e.stream == stream and e.event_type == event_type)


@pytest.mark.parametrize(
    ("event_type", "rate"), standalone_change_rates(BaselineIdentityConfig()), ids=lambda v: str(v)
)
def test_standalone_change_rates_are_honoured(
    planned: list[BaselineEvent], event_type: str, rate: float
) -> None:
    expected = RATE_ACCOUNTS * rate * _years()
    _assert_within(
        _count(planned, STREAM_CHANGES, event_type), expected, math.sqrt(expected), event_type
    )


def test_new_device_rate_is_honoured(planned: list[BaselineEvent]) -> None:
    settings = BaselineIdentityConfig()
    expected = RATE_ACCOUNTS * settings.new_device_rate_per_account_year * _years()
    observed = _count(planned, STREAM_DEVICES, "FIRST_SEEN")
    _assert_within(observed, expected, math.sqrt(expected), "FIRST_SEEN")


@pytest.mark.parametrize(
    ("event_type", "share_field"),
    [
        ("MFA_RESET", "mfa_reset_on_new_device_share"),
        ("MFA_ENROLLED", "mfa_enrolled_on_new_device_share"),
    ],
)
def test_mfa_changes_coupled_to_new_devices_honour_their_share(
    planned: list[BaselineEvent], event_type: str, share_field: str
) -> None:
    devices = _count(planned, STREAM_DEVICES, "FIRST_SEEN")
    share = float(getattr(BaselineIdentityConfig(), share_field))
    _assert_within(
        _count(planned, STREAM_DEVICES, event_type),
        devices * share,
        math.sqrt(devices * share * (1 - share)),
        event_type,
    )


def test_secondary_device_account_share_is_honoured(
    rates_plan: BaselinePlan, rates_universe: Universe
) -> None:
    secondaries = {
        index: devices.secondary
        for index, devices in rates_plan.payment_devices.items()
        if devices.secondary is not None
    }
    share = BaselineIdentityConfig().secondary_device_account_share
    _assert_within(
        len(secondaries),
        RATE_ACCOUNTS * share,
        math.sqrt(RATE_ACCOUNTS * share * (1 - share)),
        "secondary accounts",
    )
    for index, device in secondaries.items():
        assert device not in set(rates_universe.profiles[index].home_devices)


def test_new_devices_are_always_new_to_the_account(
    rates_plan: BaselinePlan, rates_universe: Universe
) -> None:
    for index, devices in rates_plan.payment_devices.items():
        known = set(rates_universe.profiles[index].home_devices) | {devices.secondary}
        enrolled = [device for _, device in devices.enrolled]
        assert len(set(enrolled)) == len(enrolled)
        assert not known & set(enrolled)


def test_login_rate_is_honoured_including_per_account_dispersion(
    planned: list[BaselineEvent],
) -> None:
    settings = BaselineIdentityConfig()
    mean = settings.login_rate_per_account_day * _days()
    variance_per_account = mean + mean**2 * (math.exp(settings.login_rate_dispersion_sigma**2) - 1)
    logins = [e for e in planned if e.stream == STREAM_LOGINS and e.event_type == "LOGIN_SUCCEEDED"]
    _assert_within(
        len(logins), RATE_ACCOUNTS * mean, math.sqrt(RATE_ACCOUNTS * variance_per_account), "logins"
    )
    per_account = collections.Counter(e.account_index for e in logins)
    counts = [per_account.get(i, 0) for i in range(RATE_ACCOUNTS)]
    average = sum(counts) / len(counts)
    dispersion = sum((c - average) ** 2 for c in counts) / len(counts) / average
    assert dispersion > 3.0, "per-account engagement is not heterogeneous"


def test_typo_bursts_and_resets_honour_their_shares(planned: list[BaselineEvent]) -> None:
    settings = BaselineIdentityConfig()
    logins = _count(planned, STREAM_LOGINS, "LOGIN_SUCCEEDED")
    weights = settings.typo_burst_size_weights
    total = sum(weights)
    mean_size = sum((i + 1) * w for i, w in enumerate(weights)) / total
    mean_size_sq = sum((i + 1) ** 2 * w for i, w in enumerate(weights)) / total
    p = settings.typo_burst_share_per_login
    expected_failures = logins * p * mean_size
    sd_failures = math.sqrt(logins * (p * mean_size_sq - (p * mean_size) ** 2))
    _assert_within(
        _count(planned, STREAM_LOGINS, "LOGIN_FAILED"), expected_failures, sd_failures, "typos"
    )
    long_share = sum(w for i, w in enumerate(weights) if i + 1 >= 3) / total
    reset_p = p * long_share * settings.reset_after_burst_share
    _assert_within(
        _count(planned, STREAM_LOGINS, "PASSWORD_CHANGE"),
        logins * reset_p,
        math.sqrt(logins * reset_p * (1 - reset_p)),
        "resets after bursts",
    )


def test_login_episodes_are_ordered_and_bounded(planned: list[BaselineEvent]) -> None:
    """Failures, then an optional reset, then the success -- same device, within
    the hour, and never more than five failures."""
    streams: dict[int, list[BaselineEvent]] = collections.defaultdict(list)
    for event in planned:
        if event.stream == STREAM_LOGINS:
            streams[event.account_index].append(event)
    episodes = 0
    for events in streams.values():
        episode: list[BaselineEvent] = []
        for event in sorted(events, key=lambda e: e.ordinal):
            episode.append(event)
            if event.event_type != "LOGIN_SUCCEEDED":
                continue
            times = [e.occurred_ms for e in episode]
            assert times == sorted(times) and len(set(times)) == len(times), episode
            assert times[-1] - times[0] < 3_600_000
            assert len({e.device_id for e in episode}) == 1
            failures = [e for e in episode if e.event_type == "LOGIN_FAILED"]
            assert len(failures) <= 5
            resets = [e for e in episode if e.event_type == "PASSWORD_CHANGE"]
            if resets:
                assert episode[-2].event_type == "PASSWORD_CHANGE"
                assert resets[0].ip_id is None
            episodes += 1
            episode = []
        assert not episode, "a login stream ended without its success"
    assert episodes > 0


def test_abandoned_bursts_honour_their_rate(planned: list[BaselineEvent]) -> None:
    settings = BaselineIdentityConfig()
    weights = settings.typo_burst_size_weights
    total = sum(weights)
    mean_size = sum((i + 1) * w for i, w in enumerate(weights)) / total
    mean_size_sq = sum((i + 1) ** 2 * w for i, w in enumerate(weights)) / total
    mu = settings.abandoned_burst_rate_per_account_day * _days()
    spread = math.exp(settings.login_rate_dispersion_sigma**2) - 1
    variance = mu * mean_size_sq + mu**2 * spread * mean_size**2
    _assert_within(
        _count(planned, STREAM_ABANDONED, "LOGIN_FAILED"),
        RATE_ACCOUNTS * mu * mean_size,
        math.sqrt(RATE_ACCOUNTS * variance),
        "abandoned failures",
    )


@pytest.mark.parametrize(
    ("event_type", "rate_field"),
    [
        ("ATTRIBUTE_CHANGED", "attribute_change_rate_per_device_year"),
        ("FINGERPRINT_CHANGED", "fingerprint_change_rate_per_device_year"),
    ],
)
def test_device_attribute_rates_are_honoured_on_pre_window_devices(
    planned: list[BaselineEvent],
    rates_plan: BaselinePlan,
    rates_universe: Universe,
    event_type: str,
    rate_field: str,
) -> None:
    pre_window: set[tuple[int, str]] = set()
    for index, profile in enumerate(rates_universe.profiles):
        pre_window.update((index, device) for device in set(profile.home_devices))
        devices = rates_plan.payment_devices.get(index)
        if devices is not None and devices.secondary is not None:
            pre_window.add((index, devices.secondary))
    rate = float(getattr(BaselineIdentityConfig(), rate_field))
    expected = len(pre_window) * rate * _years()
    observed = sum(
        1
        for e in planned
        if e.stream == STREAM_DEVICE_ATTRS
        and e.event_type == event_type
        and (e.account_index, e.device_id) in pre_window
    )
    _assert_within(observed, expected, math.sqrt(expected), event_type)


def test_away_ip_share_is_honoured(planned: list[BaselineEvent], rates_universe: Universe) -> None:
    logins = [e for e in planned if e.stream == STREAM_LOGINS and e.event_type == "LOGIN_SUCCEEDED"]
    away = sum(
        1 for e in logins if e.ip_id not in set(rates_universe.profiles[e.account_index].home_ips)
    )
    share = BaselineIdentityConfig().login_away_ip_share
    _assert_within(
        away, len(logins) * share, math.sqrt(len(logins) * share * (1 - share)), "away IPs"
    )


def test_zero_rates_plan_no_events(rates_universe: Universe) -> None:
    zeros = {
        name: 0.0
        for name in BaselineIdentityConfig.model_fields
        if name.endswith(("_per_account_day", "_per_account_year", "_per_device_year"))
    }
    assert zeros
    plan_ = plan_baseline(_rates_config(**zeros), rates_universe, DeviceReferences())
    assert plan_.events == []


def test_absent_block_plans_nothing(rates_universe: Universe) -> None:
    config = _rates_config().model_copy(update={"baseline_identity": None})
    plan_ = plan_baseline(config, rates_universe, DeviceReferences())
    assert plan_.events == [] and not plan_.payment_devices


@pytest.mark.parametrize("mean", [0.0, 0.3, 4.0, 60.0])
def test_the_poisson_sampler_has_poisson_moments(mean: float) -> None:
    rng = derive(5, "poisson-test", str(mean))
    draws = [poisson(rng, mean) for _ in range(20_000)]
    average = sum(draws) / len(draws)
    variance = sum((d - average) ** 2 for d in draws) / len(draws)
    assert abs(average - mean) <= 4 * math.sqrt(max(mean, 1e-9) / len(draws)) + 1e-9
    if mean > 0:
        assert 0.9 * mean <= variance <= 1.1 * mean


# ============================================== G2 and the ablations =====


def _floor_config(**overrides: Any) -> GeneratorConfig:
    base: dict[str, Any] = {
        "row_count": 20_000,
        "account_count": 800,
        "merchant_count": 60,
        "device_count": 960,
        "ip_count": 400,
        "fraud_rate": 0.005,
        "seed": 11,
    }
    base.update(overrides)
    return GeneratorConfig(**base)


def test_g2_tops_every_pattern_up_to_twenty_and_discloses_what_it_added() -> None:
    """`LPC-5` §16.3: G2 adding instances to patterns below 20, in a configuration built for it."""
    gate = _floor_config(baseline_identity=BaselineIdentityConfig())
    universe = build_universe(gate)
    instances, total = plan_fraud(gate, universe)
    mix = coverage_mix(instances)
    assert set(mix) == {pattern.value for pattern in FraudPattern}
    assert all(planned + added >= 20 for planned, added in mix.values()), mix
    assert all(added == 0 for planned, added in mix.values() if planned >= 20), mix
    assert sum(added for _, added in mix.values()) > 0, "the configuration must need the floor"
    assert all(planned + added == 20 for planned, added in mix.values() if added), mix
    assert total == sum(instance.transaction_count for instance in instances)

    off_instances, off_total = plan_fraud(_floor_config(), universe)
    mixed = [i for i in instances if not i.instance_id.startswith(COVERAGE_FLOOR_PREFIX)]
    assert [i.instance_id for i in mixed] == [i.instance_id for i in off_instances]
    assert total > off_total
    assert plan_fraud(
        _floor_config(fraud_rate=0.0, baseline_identity=BaselineIdentityConfig()), universe
    ) == ([], 0)


def test_every_declared_ablation_names_a_generator_correction() -> None:
    from data.generator.lpc5 import declaration

    assert set(CORRECTIONS) == set(declaration.ABLATIONS)


def _ablation_config(*disabled: str) -> GeneratorConfig:
    return GeneratorConfig(
        row_count=4_000,
        account_count=300,
        merchant_count=60,
        device_count=360,
        ip_count=150,
        fraud_rate=0.01,
        seed=5,
        baseline_identity=BaselineIdentityConfig(
            coverage_floor_instances=1, disabled_corrections=disabled
        ),
    )


def _ablated(*disabled: str) -> list[GeneratedRow]:
    return list(generate_dataset(_ablation_config(*disabled)))


def _planned_event(row: GeneratedRow) -> PlannedEvent:
    assert row.scenario_instance is not None and row.planned_ordinal is not None
    return row.scenario_instance.events[row.planned_ordinal]


def test_ablating_t1_leaves_every_legitimate_payment_on_a_home_device() -> None:
    rows = _ablated("T1")
    universe = build_universe(
        GeneratorConfig(
            row_count=4_000,
            account_count=300,
            merchant_count=60,
            device_count=360,
            ip_count=150,
            fraud_rate=0.01,
            seed=5,
        )
    )
    home = {p.account_id: set(p.home_devices) for p in universe.profiles}
    legitimate = [r for r in rows if _is_legit_tx(r)]
    assert legitimate
    assert all(
        r.event["payload"]["device_id"] in home[r.event["payload"]["account_id"]]
        for r in legitimate
    )


def test_ablating_t2_keeps_every_planted_event_on_its_planned_millisecond() -> None:
    planted = [r for r in _ablated("T2") if r.scenario_instance is not None]
    assert planted
    assert all(_millis(r) == _planned_event(r).occurred_ms for r in planted)


def test_ablating_t3_leaves_no_legitimate_decline() -> None:
    legitimate = [r for r in _ablated("T3") if _is_legit_tx(r)]
    assert legitimate
    assert all(r.event["payload"]["authorization_outcome"] == "APPROVED" for r in legitimate)


@pytest.mark.parametrize(
    ("correction", "pattern", "fields"),
    [
        ("M1", FraudPattern.ACCOUNT_TAKEOVER, ("latitude", "longitude")),
        ("M2", FraudPattern.IMPOSSIBLE_TRAVEL, ("latitude", "longitude")),
        ("M3", FraudPattern.ACCOUNT_TAKEOVER, ("entry_mode",)),
    ],
)
def test_ablating_m1_to_m3_leaves_their_fields_as_planned(
    correction: str, pattern: FraudPattern, fields: tuple[str, ...]
) -> None:
    rows = [
        r
        for r in _ablated(correction)
        if _is_fraud_tx(r) and r.label is not None and r.label.fraud_pattern is pattern
    ]
    assert rows
    kept = redrawn = 0
    for row in rows:
        planned = _planned_event(row).overrides
        same = all(row.event["payload"][f] == planned[f] for f in fields if f in planned)
        kept += same
        redrawn += not same
    if correction == "M2":
        # Only the leg planted on the home point is M2's; the away leg is still redrawn (M1).
        assert kept and redrawn
    else:
        assert kept == len(rows)


def test_ablating_n10_restores_the_borrowed_correlation_ids() -> None:
    rows = _ablated("N10")
    ids = collections.Counter(r.event["envelope"]["correlation_id"] for r in rows)
    assert all(re.fullmatch(r"corr_\d{12}", value) for value in ids)
    assert any(count > 1 for count in ids.values())


def test_ablating_n11_restores_value_dependent_timestamp_strings() -> None:
    rows = _ablated("N11")
    assert not all(_MS_Z.fullmatch(r.event["envelope"]["occurred_at"]) for r in rows)


# ============================================== 6b: the planted episodes =====


def _planted_tx(rows: list[GeneratedRow], *patterns: FraudPattern) -> list[GeneratedRow]:
    return [
        r
        for r in rows
        if _is_fraud_tx(r)
        and r.label is not None
        and (not patterns or r.label.fraud_pattern in patterns)
    ]


def _by_instance(rows: list[GeneratedRow]) -> dict[str, list[GeneratedRow]]:
    grouped: dict[str, list[GeneratedRow]] = collections.defaultdict(list)
    for row in rows:
        assert row.scenario_instance is not None
        grouped[row.scenario_instance.instance_id].append(row)
    return grouped


def _ordinal(row: GeneratedRow) -> int:
    assert row.planned_ordinal is not None
    return row.planned_ordinal


def _instance(row: GeneratedRow) -> ScenarioInstance:
    assert row.scenario_instance is not None
    return row.scenario_instance


def test_g1_every_planted_amount_is_the_one_lpc5_recomputes(
    gate_on: list[GeneratedRow], universe: Universe
) -> None:
    """Against the criterion's own implementation of G1, not the generator's."""
    seed = _coherence_config().seed
    profiles = {p.account_id: p for p in universe.profiles}
    grouped = _by_instance(_planted_tx(gate_on))
    assert len({_instance(rows[0]).pattern for rows in grouped.values()}) == len(FraudPattern)
    for rows in grouped.values():
        instance = _instance(rows[0])
        payoff = max(map(_ordinal, rows)) if instance.pattern is FraudPattern.CARD_TESTING else None
        for row in rows:
            payload = row.event["payload"]
            expected = lpc5_rules.g1_amount(
                seed,
                instance.instance_id,
                _ordinal(row),
                instance.pattern,
                profiles[payload["account_id"]],
                payoff=_ordinal(row) == payoff,
            )
            assert payload["amount_minor"] == expected, (instance.instance_id, _ordinal(row))


def test_g4_card_testing_and_stuffing_pay_from_devices_the_account_knows(
    gate_on: list[GeneratedRow], plan: tuple[Universe, BaselinePlan]
) -> None:
    universe, baseline = plan
    index_of = {str(p.account_id): i for i, p in enumerate(universe.profiles)}

    def known(account: str, device: str, at_ms: int) -> bool:
        index = index_of[account]
        if device in universe.profiles[index].home_devices:
            return True
        devices = baseline.payment_devices.get(index)
        return devices is not None and (
            device == devices.secondary
            or any(tau < at_ms and enrolled == device for tau, enrolled in devices.enrolled)
        )

    testing = _by_instance(_planted_tx(gate_on, FraudPattern.CARD_TESTING))
    stuffing = _planted_tx(gate_on, FraudPattern.CREDENTIAL_STUFFING)
    assert testing and stuffing
    for rows in testing.values():
        assert len({r.event["payload"]["device_id"] for r in rows}) == 1
    for row in [*(r for rows in testing.values() for r in rows), *stuffing]:
        payload = row.event["payload"]
        assert known(payload["account_id"], payload["device_id"], _millis(row)), payload
    logins = [
        r
        for r in gate_on
        if _is_scenario_side(r) and _instance(r).pattern is FraudPattern.CREDENTIAL_STUFFING
    ]
    for rows in _by_instance(logins).values():
        assert len({r.event["payload"]["device_id"] for r in rows}) == 1


def test_g6_collusion_payments_are_ordinary_for_their_payers(
    gate_on: list[GeneratedRow], universe: Universe
) -> None:
    profiles = {p.account_id: p for p in universe.profiles}
    grouped = _by_instance(_planted_tx(gate_on, FraudPattern.MERCHANT_COLLUSION))
    assert grouped
    for rows in grouped.values():
        amounts = [r.event["payload"]["amount_minor"] for r in rows]
        assert len({r.event["payload"]["merchant_id"] for r in rows}) == 1
        assert (max(amounts) - min(amounts)) / max(1.0, statistics.mean(amounts)) < 0.2
        for row in rows:
            payload = row.event["payload"]
            profile = profiles[payload["account_id"]]
            spread = abs(math.log(payload["amount_minor"]) - profile.amount_mu)
            assert spread <= profile.amount_sigma + 0.011, payload


def test_g7_gated_keys_are_only_what_the_mechanism_creates(gate_on: list[GeneratedRow]) -> None:
    seen: set[FraudPattern] = set()
    for row in _planted_tx(gate_on):
        assert row.label is not None and row.label.fraud_pattern is not None
        pattern = row.label.fraud_pattern
        expected = planted.EVAL_V2_CAUSAL_KEYS.get(
            pattern, SCENARIOS_BY_PATTERN[pattern].causal_evidence_keys()
        )
        assert row.label.causal_evidence_keys == expected, pattern
        seen.add(pattern)
    assert set(planted.EVAL_V2_CAUSAL_KEYS) <= seen
    stuffing_side = {
        _event_type(r)
        for r in gate_on
        if _is_scenario_side(r) and _instance(r).pattern is FraudPattern.CREDENTIAL_STUFFING
    }
    assert stuffing_side <= {"LOGIN_FAILED", "LOGIN_SUCCEEDED"}


def test_m5_planted_merchants_follow_their_documented_choice(
    gate_on: list[GeneratedRow], universe: Universe
) -> None:
    profiles = {p.account_id: p for p in universe.profiles}
    mcc = {str(m.merchant_id): m.mcc for m in universe.merchants}
    checked: collections.Counter[FraudPattern] = collections.Counter()
    for row in _planted_tx(
        gate_on, FraudPattern.ACCOUNT_TAKEOVER, FraudPattern.ANOMALOUS_HIGH_VALUE
    ):
        payload = row.event["payload"]
        profile = profiles[payload["account_id"]]
        assert payload["merchant_id"] not in profile.habitual_merchants
        pattern = _instance(row).pattern
        if pattern is FraudPattern.ANOMALOUS_HIGH_VALUE:
            assert payload["merchant_mcc"] not in {mcc[m] for m in profile.habitual_merchants}
        checked[pattern] += 1
    assert len(checked) == 2
    for rows in _by_instance(_planted_tx(gate_on, FraudPattern.CARD_TESTING)).values():
        payoff = max(map(_ordinal, rows))
        probes = [r.event["payload"]["merchant_id"] for r in rows if _ordinal(r) != payoff]
        assert len(set(probes)) == len(probes)


def test_m6_only_impossible_travel_keeps_a_planted_channel(gate_on: list[GeneratedRow]) -> None:
    rows = [r for r in gate_on if r.topic == TX and r.scenario_instance is not None]
    assert rows
    for row in rows:
        overrides = _planned_event(row).overrides
        if _instance(row).pattern is FraudPattern.IMPOSSIBLE_TRAVEL:
            assert overrides["channel"] == "CARD_PRESENT" == row.event["payload"]["channel"]
        else:
            assert "channel" not in overrides and "entry_mode" not in overrides


def test_lpc5_s7a_scenario_invariants_hold_on_the_gated_generator(
    gate_on: list[GeneratedRow], universe: Universe
) -> None:
    frame = build_frame(gate_on, seed=universe.config.seed)
    result = lpc5_rules.s7_instances(frame, lpc5_knowledge(universe))
    assert result.judged > 0
    assert not result.findings, [f"{f.check}: {f.detail}" for f in result.findings]


def _eval_v1_plan(config: GeneratorConfig) -> dict[str, ScenarioInstance]:
    off = config.model_copy(update={"baseline_identity": None})
    return {i.instance_id: i for i in plan_fraud(off, build_universe(off))[0]}


@pytest.mark.parametrize(
    ("correction", "fields"),
    [
        ("N9", ("amount_minor",)),
        ("M5", ("merchant_id",)),
        ("M6", ("channel",)),
    ],
)
def test_ablating_n9_m5_or_m6_keeps_eval_v1_planned_fields(
    correction: str, fields: tuple[str, ...]
) -> None:
    config = _ablation_config(correction)
    universe = build_universe(config)
    eval_v1 = _eval_v1_plan(config)
    compared = 0
    for row in generate_dataset(config, universe):
        if not _is_fraud_tx(row):
            continue
        planned_event = eval_v1[_instance(row).instance_id].events[_ordinal(row)]
        payload = row.event["payload"]
        if correction == "N9":
            assert (
                payload["account_id"] == universe.profiles[planned_event.account_index].account_id
            )
        for field in fields:
            if field in planned_event.overrides:
                assert payload[field] == planned_event.overrides[field], (correction, field)
                compared += 1
    assert compared > 0


def test_takeover_changes_follow_the_legitimate_change_type_mix() -> None:
    """`LPC-5` §18 item 2: eval-v1 drew three of the five Q4e types; the gate draws all five,
    weighted as legitimate identity changes are."""
    config = _coherence_config(baseline_identity=_EVAL_V1_INSTANCES)
    universe = build_universe(config)
    scenario = SCENARIOS_BY_PATTERN[FraudPattern.ACCOUNT_TAKEOVER]
    changes: collections.Counter[str] = collections.Counter()
    for n in range(400):
        instance = scenario.inject(
            derive(config.seed, "change-type-test", str(n)), universe, f"fi_{n:08d}", 0
        )
        revised = planted.revise(config, universe, instance)
        changes.update(
            e.identity_event_type
            for e in revised.events
            if e.topic == IDENTITY and e.identity_event_type is not None
        )
    assert set(changes) == set(planted.Q4E_TYPES)
    assert config.baseline_identity is not None
    weights = dict(
        zip(planted.Q4E_TYPES, planted.change_type_weights(config.baseline_identity), strict=True)
    )
    total = sum(changes.values())
    for change, weight in weights.items():
        expected = total * weight / sum(weights.values())
        _assert_within(changes[change], expected, math.sqrt(expected), change)
