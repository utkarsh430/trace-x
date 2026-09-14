"""The generation engine: legitimate traffic with fraud episodes woven in.

**Why timestamps are planned first and rows built second.** Events are emitted in
event-time order, which is what a real stream approximates. Planning all
timestamps, sorting them, and only then building rows keeps peak memory at one
array rather than N dictionaries -- on a 1M-row run that is the difference
between megabytes and gigabytes.

**Why transaction ids are assigned by final position.** An id derived from the
scenario (`tx_fraud_0001`) would leak ground truth straight into the application
path, and ADR-0004 exists to make that structurally impossible. After the merge,
a fraudulent transaction is indistinguishable from a legitimate one except in the
ways its scenario intends.

**Why dicts rather than Pydantic models.** Validation is a produce-time
requirement (`docs/EVENT_CONTRACTS.md` §6.1), but validating inside the loop puts
the cost on every caller. The engine yields plain dicts; `validate_events`
applies the released schema as a pass-through, and the CLI records which policy
a run used rather than leaving it implicit.
"""

from __future__ import annotations

import datetime as dt
import random
from collections import Counter, defaultdict
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any, Final

from data.generator.baseline import (
    BaselineEvent,
    DeviceReferences,
    PaymentDevices,
    plan_baseline,
)
from data.generator.behavior import (
    pick_merchant_index,
    sample_amount_minor,
    sample_location,
    sample_occurred_at,
)
from data.generator.config import PRODUCER, BaselineIdentityConfig, GeneratorConfig
from data.generator.labels import TransactionLabel
from data.generator.outcomes import iso_millis
from data.generator.population import Universe, build_universe
from data.generator.rng import derive, substream_seed
from data.generator.scenarios import ALL_SCENARIOS, PlannedEvent, ScenarioInstance, default_mix
from trace_core.domain.enums import FraudPattern
from trace_core.domain.identifiers import uuid7
from trace_core.domain.time import to_millis

SCHEMA_VERSION: Final = 1

# Card-present entry modes are plausible only for a physical channel. An
# ECOMMERCE entry mode on a CARD_PRESENT transaction would be a contradiction a
# rule could exploit as a fraud signal -- an artefact of the generator rather
# than of fraud.
_CHANNEL_ENTRY: Final[dict[str, tuple[str, ...]]] = {
    "CARD_PRESENT": ("CHIP", "CONTACTLESS", "MAGSTRIPE"),
    "CARD_NOT_PRESENT": ("ECOMMERCE", "TOKEN", "MANUAL"),
    "ATM": ("CHIP", "MAGSTRIPE"),
    "RECURRING": ("TOKEN",),
}
_CHANNELS: Final[tuple[tuple[str, float], ...]] = (
    ("CARD_PRESENT", 0.46),
    ("CARD_NOT_PRESENT", 0.44),
    ("ATM", 0.06),
    ("RECURRING", 0.04),
)
_USER_AGENTS: Final[tuple[str, ...]] = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)",
    "Mozilla/5.0 (Linux; Android 14)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "TraceXPay/2.1 (POS terminal)",
)


@dataclass(frozen=True, slots=True)
class GeneratedRow:
    """One emitted event and, for transactions, its ground-truth label."""

    topic: str
    event: dict[str, Any]
    label: TransactionLabel | None = None
    scenario_instance: ScenarioInstance | None = None
    """Present on injected rows. Never serialised into the event -- it exists so
    the ground-truth writer can record which episode a row belonged to."""
    authorization_decided_ms: int | None = None
    """eval-v2 only: when this transaction's authorization outcome was decided.

    Kept in the plan and never serialised into the event, so a later decision to
    emit authorization outcomes as their own dated events can take it from here.
    One latency distribution applies to every transaction -- approved or declined,
    legitimate or planted -- because a latency that differed by outcome or label
    would become a proxy the moment it was emitted. None when the gate is off."""
    planned_ordinal: int | None = None
    """Planted rows only: the index of this row's planned event within its scenario instance.

    Never serialised. LPC-5's generator-rule checks recompute a planted amount from the instance and
    this index (eval/track_a/criteria/lpc-5.md §11, G1)."""


def _iso(millis: int) -> str:
    return dt.datetime.fromtimestamp(millis / 1000, tz=dt.UTC).isoformat().replace("+00:00", "Z")


def _pick_channel(value: float) -> str:
    running = 0.0
    for channel, weight in _CHANNELS:
        running += weight
        if value <= running:
            return channel
    return _CHANNELS[-1][0]


def _corrected(config: GeneratorConfig, correction: str) -> bool:
    """Whether an eval-v2 correction applies: the gate is on and the correction is not ablated."""
    settings = config.baseline_identity
    return settings is not None and settings.applies(correction)


def _envelope(
    event_type: str,
    occurred_ms: int,
    ingested_ms: int,
    position: int,
    rng: Any,
    config: GeneratorConfig | None = None,
) -> dict[str, Any]:
    """The envelope. eval-v1's draws, in eval-v1's order, whatever the gate.

    Under the gate:
    - **N10.** `correlation_id` names the event's own business flow, drawn after every eval-v1
      draw. eval-v1 used the next transaction's position, which side events borrowed, so unrelated
      events shared one (LP-21). A transaction's outcome event takes its transaction's.
    - **N11.** Both timestamps always carry exactly three fractional digits. eval-v1 dropped the
      fraction on a whole second, so a string's length depended on its value (LP-07).
    """
    event_id = str(uuid7(millis=occurred_ms, rng=rng))
    trace_id = f"{rng.getrandbits(128):032x}"
    idempotency_key = "sha256:" + f"{rng.getrandbits(256):064x}"
    if config is not None and _corrected(config, "N10"):
        correlation_id = f"corr_{rng.getrandbits(128):032x}"
    else:
        correlation_id = f"corr_{position:012d}"
    render = iso_millis if config is not None and _corrected(config, "N11") else _iso
    return {
        "event_id": event_id,
        "event_type": event_type,
        "schema_version": SCHEMA_VERSION,
        "occurred_at": render(occurred_ms),
        "ingested_at": render(ingested_ms),
        "producer": PRODUCER,
        "trace_id": trace_id,
        "correlation_id": correlation_id,
        "idempotency_key": idempotency_key,
    }


def _build_transaction(
    config: GeneratorConfig,
    universe: Universe,
    account_index: int,
    occurred_ms: int,
    position: int,
    lag: Any,
    overrides: dict[str, Any],
) -> dict[str, Any]:
    """One `tx.raw.v1` event.

    The account's own behaviour fills every field; a scenario's `overrides`
    replace only what it means to change. That is what makes an injected
    transaction a *departure* from a baseline rather than a different kind of
    object.
    """
    profile = universe.profiles[account_index]
    rng = derive(config.seed, "tx", str(position))

    merchant_index = pick_merchant_index(
        rng, profile, universe.merchant_cum_weights, config.habitual_merchant_ratio
    )
    if merchant_index is None:
        habitual = profile.habitual_merchants[rng.randrange(len(profile.habitual_merchants))]
        merchant = universe.merchants[int(habitual.split("_")[1])]
    else:
        merchant = universe.merchants[merchant_index]

    channel = _pick_channel(rng.random())
    location = sample_location(rng, profile.account.home, config.geo_jitter_km)
    device = profile.home_devices[rng.randrange(len(profile.home_devices))]
    ip = profile.home_ips[rng.randrange(len(profile.home_ips))]
    card = profile.cards[rng.randrange(len(profile.cards))]

    # Processing time is at or after event time; the gap is the lag the warm path
    # measures. Without it Bronze would carry an ingested_at identical to
    # occurred_at, and the distinction ADR-0026 rests on would be untestable.
    ingested_ms = occurred_ms + int(abs(lag.gauss(70, 40))) + 5

    payload: dict[str, Any] = {
        "transaction_id": f"tx_{position:012d}",
        "account_id": profile.account_id,
        "card_id": card.card_id,
        "device_id": device,
        "merchant_id": merchant.merchant_id,
        "ip_id": ip,
        "amount_minor": sample_amount_minor(rng, profile),
        "currency": config.currency,
        "channel": channel,
        "entry_mode": _CHANNEL_ENTRY[channel][rng.randrange(len(_CHANNEL_ENTRY[channel]))],
        "merchant_mcc": merchant.mcc,
        "merchant_country": merchant.country,
        "merchant_name": merchant.name,
        "latitude": location.latitude,
        "longitude": location.longitude,
        "user_agent": _USER_AGENTS[rng.randrange(len(_USER_AGENTS))],
        "memo": "",
        "authorization_outcome": "APPROVED",
    }
    payload.update(overrides)
    # An override may change the channel, so entry mode is re-checked against it
    # rather than left contradictory.
    if payload["entry_mode"] not in _CHANNEL_ENTRY[payload["channel"]]:
        payload["entry_mode"] = _CHANNEL_ENTRY[payload["channel"]][0]

    return {
        "envelope": _envelope("tx.raw", occurred_ms, ingested_ms, position, rng, config),
        "payload": payload,
    }


def _build_side_event(
    config: GeneratorConfig,
    universe: Universe,
    planned: Any,
    occurred_ms: int,
    position: int,
    lag: Any,
) -> dict[str, Any]:
    """An `identity.events.v1` or `device.events.v1` event.

    These exist because two scenarios are defined by them: account takeover
    begins with a credential change, and credential stuffing is a burst of failed
    logins. Encoding those as transactions would misrepresent both, and would make
    IDENTITY_CHANGE an uncausal key.
    """
    profile = universe.profiles[planned.account_index]
    rng = derive(config.seed, "side", f"{position}")
    ingested_ms = occurred_ms + int(abs(lag.gauss(70, 40))) + 5
    overrides = planned.overrides

    if planned.topic == "identity.events.v1":
        payload: dict[str, Any] = {
            "account_id": profile.account_id,
            "identity_event_type": planned.identity_event_type or "UNKNOWN",
            "user_agent": _USER_AGENTS[rng.randrange(len(_USER_AGENTS))],
        }
        for key in ("device_id", "ip_id"):
            if key in overrides:
                payload[key] = overrides[key]
        event_type = "identity.events"
    else:
        payload = {
            "device_id": overrides.get(
                "device_id", profile.home_devices[rng.randrange(len(profile.home_devices))]
            ),
            "account_id": profile.account_id,
            "device_event_type": planned.device_event_type or "UNKNOWN",
            "platform": ("ios", "android", "web", "desktop")[rng.randrange(4)],
        }
        if "ip_id" in overrides:
            payload["ip_id"] = overrides["ip_id"]
        event_type = "device.events"

    return {
        "envelope": _envelope(event_type, occurred_ms, ingested_ms, position, rng, config),
        "payload": payload,
    }


# ------------------------------------------------------------- eval-v2 -----
#
# Everything in this section runs only when `config.baseline_identity` is set.
# With it absent, `generate_dataset` takes exactly the path it took before this
# section existed -- pinned by eval-v1's frozen digest and by literal digests of
# the pre-change generator in tests/unit/test_generator_baseline_identity.py.

_RETRY_GAP_MS: Final = (20_000, 600_000)
"""From a legitimate declined attempt to the customer's retry (chosen)."""
_MAX_DECLINE_PROBABILITY: Final = 0.5
"""Cap on one legitimate transaction's decline probability after the account's
multiplier, so a lognormal tail cannot produce accounts that almost always decline."""
_DECISION_LATENCY_FLOOR_MS: Final = 40
_DECISION_LATENCY_MEAN_MS: Final = 300.0
_DECISION_LATENCY_SD_MS: Final = 150.0

_SINGLE: Final = 0
_ATTEMPT: Final = 1
_RETRY: Final = 2
_RETRY_FRESH_FIELDS: Final = frozenset({"transaction_id", "authorization_outcome"})
"""The only payload fields a retry does not copy from its declined attempt."""


@dataclass(frozen=True, slots=True)
class _LegitimateTransaction:
    """A legitimate transaction's eval-v2 decisions, fixed before emission."""

    account_index: int
    device_id: str | None
    """A non-home device chosen by T1, or None for the eval-v1 home-device choice."""
    declined: bool
    role: int
    pair: int
    """Identifies a retry pair (the retry's draw index); the draw's own index otherwise."""


def _device_platform(universe: Universe, device: str) -> str:
    """The universe's platform for a device, so every event about it agrees.

    eval-v1 drew a platform at random per side event, so one device could report
    two platforms -- and only scenario devices ever did.
    """
    return universe.devices[int(device.split("_")[1])].platform


def _scenario_side_stream(
    config: GeneratorConfig, instance: ScenarioInstance, ordinal: int
) -> random.Random:
    """One substream per scenario side event, keyed by the event itself.

    eval-v1 keyed side events by the NEXT TRANSACTION's position, which
    consecutive side events share, so they shared `trace_id` and
    `idempotency_key` too. Were only scenario events to keep doing that once
    legitimate events exist, a shared envelope would mark fraud.
    """
    return derive(config.seed, "scenario-side-event", f"{instance.instance_id}:{ordinal}")


def _scenario_side_device(
    universe: Universe, planned: PlannedEvent, rng: random.Random
) -> str | None:
    """The device a scenario side event names. Drawn first from its stream."""
    override = planned.overrides.get("device_id")
    if override is not None or planned.topic != "device.events.v1":
        return None if override is None else str(override)
    profile = universe.profiles[planned.account_index]
    return profile.home_devices[rng.randrange(len(profile.home_devices))]


def _build_scenario_side_event_v2(
    config: GeneratorConfig,
    universe: Universe,
    instance: ScenarioInstance,
    planned: PlannedEvent,
    ordinal: int,
    occurred_ms: int,
    position: int,
    lag: Any,
) -> dict[str, Any]:
    """A scenario's identity or device event under eval-v2 semantics.

    Same payload shape as `_build_side_event`; a unique envelope substream and a
    coherent platform. Its lag still comes from the shared stream, in emission
    order, as in eval-v1.
    """
    profile = universe.profiles[planned.account_index]
    rng = _scenario_side_stream(config, instance, ordinal)
    device = _scenario_side_device(universe, planned, rng)
    ingested_ms = occurred_ms + int(abs(lag.gauss(70, 40))) + 5
    overrides = planned.overrides

    if planned.topic == "identity.events.v1":
        payload: dict[str, Any] = {
            "account_id": profile.account_id,
            "identity_event_type": planned.identity_event_type or "UNKNOWN",
            "user_agent": _USER_AGENTS[rng.randrange(len(_USER_AGENTS))],
        }
        if device is not None:
            payload["device_id"] = device
        if "ip_id" in overrides:
            payload["ip_id"] = overrides["ip_id"]
        event_type = "identity.events"
    else:
        if device is None:  # pragma: no cover - _scenario_side_device always names one
            raise ValueError(f"{instance.instance_id}:{ordinal}: device event without a device")
        payload = {
            "device_id": device,
            "account_id": profile.account_id,
            "device_event_type": planned.device_event_type or "UNKNOWN",
            "platform": _device_platform(universe, device),
        }
        if "ip_id" in overrides:
            payload["ip_id"] = overrides["ip_id"]
        event_type = "device.events"

    return {
        "envelope": _envelope(event_type, occurred_ms, ingested_ms, position, rng, config),
        "payload": payload,
    }


def _build_baseline_event(
    config: GeneratorConfig, universe: Universe, planned: BaselineEvent, position: int
) -> dict[str, Any]:
    """A legitimate identity or device event (`data.generator.baseline`).

    Shaped exactly like a scenario event of the same type -- key set, user-agent
    distribution, correlation scheme, lag distribution -- because any difference
    in shape would betray which kind of event it is.
    """
    profile = universe.profiles[planned.account_index]
    rng = derive(config.seed, "baseline-event", planned.key)
    ingested_ms = planned.occurred_ms + int(abs(rng.gauss(70, 40))) + 5

    if planned.topic == "identity.events.v1":
        payload: dict[str, Any] = {
            "account_id": profile.account_id,
            "identity_event_type": planned.event_type,
            "user_agent": _USER_AGENTS[rng.randrange(len(_USER_AGENTS))],
            "device_id": planned.device_id,
        }
        if planned.ip_id is not None:
            payload["ip_id"] = planned.ip_id
        event_type = "identity.events"
    else:
        payload = {
            "device_id": planned.device_id,
            "account_id": profile.account_id,
            "device_event_type": planned.event_type,
            "platform": _device_platform(universe, planned.device_id),
        }
        event_type = "device.events"

    return {
        "envelope": _envelope(event_type, planned.occurred_ms, ingested_ms, position, rng, config),
        "payload": payload,
    }


def scenario_device_references(
    config: GeneratorConfig, universe: Universe, instances: Sequence[ScenarioInstance]
) -> DeviceReferences:
    """Every device a scenario names, per account.

    Legitimate activity never adopts one of these for that account, as a new
    device or a secondary one: an account that already knew a scenario's device
    would make that scenario's DEVICE_NOVELTY key false.
    """
    devices: dict[int, set[str]] = defaultdict(set)
    for instance in instances:
        for ordinal, planned in enumerate(instance.events):
            if planned.topic == "tx.raw.v1":
                override = planned.overrides.get("device_id")
                if override is not None:
                    devices[planned.account_index].add(str(override))
                continue
            side_device = _scenario_side_device(
                universe, planned, _scenario_side_stream(config, instance, ordinal)
            )
            if side_device is not None:
                devices[planned.account_index].add(side_device)
    return DeviceReferences(scenario_devices=dict(devices))


def _legitimate_device(
    settings: BaselineIdentityConfig,
    devices: PaymentDevices | None,
    at_ms: int,
    rng: random.Random,
) -> str | None:
    """T1: a non-home device for a legitimate transaction at `at_ms`, or None.

    A device enrolled strictly before `at_ms` first, then the secondary device;
    None keeps the eval-v1 home-device choice. Never a device the account does not
    yet know.
    """
    if devices is None:
        return None
    latest = devices.latest_enrolled_before(at_ms)
    if latest is not None and rng.random() < settings.new_device_payment_share:
        return latest
    if devices.secondary is not None and rng.random() < settings.secondary_device_transaction_share:
        return devices.secondary
    return None


def _plan_eval_v2(
    config: GeneratorConfig,
    universe: Universe,
    merged: list[tuple[int, int, int, Any]],
    legit_count: int,
    instances: Sequence[ScenarioInstance],
) -> list[tuple[int, int, int, Any]]:
    """The gate-on plan: eval-v1's merged plan transformed before emission.

    Scenario references -> identity and device plan -> legitimate transaction
    decisions (T1, T3) -> scenario sub-second timing (T2) -> planted location and
    entry-mode draws (M1-M3) -> one sort. The number of transactions, every label,
    every planted channel and every other planted override are unchanged; which
    transaction sits at which position may change.
    """
    settings = config.baseline_identity
    if settings is None:  # pragma: no cover - guarded by generate_dataset
        raise ValueError("_plan_eval_v2 requires baseline_identity")
    seed = config.seed
    start_ms = to_millis(config.start_at)

    # Legitimate draws, recovered in draw order: a draw's tiebreak is its index.
    draw_ms = [0] * legit_count
    draw_account = [0] * legit_count
    planted: list[tuple[int, int, int, Any]] = []
    for occurred_ms, tiebreak, kind, payload in merged:
        if kind == 0:
            draw_ms[tiebreak] = occurred_ms
            draw_account[tiebreak] = payload
        else:
            planted.append((occurred_ms, tiebreak, kind, payload))

    references = scenario_device_references(config, universe, instances)
    baseline = plan_baseline(config, universe, references)
    payment = baseline.payment_devices

    plan: list[tuple[int, int, int, Any]] = []

    # T1 and T3. One substream per account: its first draw is the account's
    # decline-propensity multiplier, then its decisions in draw order.
    sigma = settings.decline_propensity_sigma
    # An ablated T1 or T3 still makes its draws, so the other decisions stay where they were.
    t1 = settings.applies("T1")
    t3 = settings.applies("T3")
    streams: dict[int, tuple[random.Random, float]] = {}
    consumed = False
    for index in range(legit_count):
        if consumed:
            consumed = False
            continue
        account = draw_account[index]
        at_ms = draw_ms[index]
        if account not in streams:
            stream = derive(seed, "baseline-transactions", str(account))
            multiplier = stream.lognormvariate(-(sigma**2) / 2.0, sigma) if sigma > 0 else 1.0
            streams[account] = (stream, multiplier)
        rng, multiplier = streams[account]
        devices = payment.get(account)

        retry_p = (
            min(_MAX_DECLINE_PROBABILITY, settings.decline_retry_share_per_transaction * multiplier)
            if t3
            else 0.0
        )
        if index + 1 < legit_count and rng.random() < retry_p:
            attempt_ms = at_ms - rng.randrange(*_RETRY_GAP_MS)
            if attempt_ms >= start_ms:
                device = _legitimate_device(settings, devices, attempt_ms, rng) if t1 else None
                # The attempt takes the consumed draw's tiebreak; the retry keeps
                # this draw's time and tiebreak. The count is exactly unchanged.
                plan.append(
                    (
                        attempt_ms,
                        index + 1,
                        3,
                        _LegitimateTransaction(account, device, True, _ATTEMPT, index),
                    )
                )
                plan.append(
                    (at_ms, index, 3, _LegitimateTransaction(account, device, False, _RETRY, index))
                )
                consumed = True
                continue

        decline_p = (
            min(_MAX_DECLINE_PROBABILITY, settings.decline_share_per_transaction * multiplier)
            if t3
            else 0.0
        )
        declined = rng.random() < decline_p
        device = _legitimate_device(settings, devices, at_ms, rng)
        if not t1:
            device = None
        plan.append(
            (at_ms, index, 3, _LegitimateTransaction(account, device, declined, _SINGLE, index))
        )

    # T2. Every planted event keeps its planned second and gets a uniform
    # millisecond, as legitimate transactions do.
    retimed: list[tuple[int, int, int, Any]] = []
    for occurred_ms, tiebreak, kind, payload in planted:
        if not settings.applies("T2"):
            retimed.append((occurred_ms, tiebreak, kind, payload))
            continue
        instance, _planned, ordinal = payload
        subsecond = derive(seed, "scenario-subsecond", f"{instance.instance_id}:{ordinal}")
        retimed.append(
            (occurred_ms - occurred_ms % 1000 + subsecond.randrange(1000), tiebreak, kind, payload)
        )
    # M1-M3. Planted locations and entry modes are drawn the way legitimate ones
    # are, on the final times; every other planted field is kept.
    plan.extend(_redraw_planted_transactions(config, universe, retimed))

    # Legitimate identity and device events. Tiebreaks start above every other.
    base = legit_count + len(instances) * 1000
    for offset, planned_event in enumerate(baseline.events):
        plan.append((planned_event.occurred_ms, base + offset, 2, planned_event))

    if settings.applies("N11"):
        plan.sort(key=lambda item: plan_order_key(seed, item))
    else:
        plan.sort(key=lambda item: (item[0], item[1]))
    return plan


def tie_order_key(seed: int, identity: str) -> int:
    """N11: the key that orders events sharing a millisecond.

    eval-v1 ordered ties by planning structure, which put every planted event after every
    legitimate one at a tie (LP-24). A stable hash of the event's own identity decides instead, so
    which of two tied events comes first does not depend on which is planted."""
    return substream_seed(seed, "tie-order", identity)


def plan_order_key(seed: int, item: tuple[int, int, int, Any]) -> tuple[int, int]:
    """The gate-on emission order: event time, then `tie_order_key` of the event's identity.

    Identities: a legitimate transaction's draw index, a planted event's `instance:ordinal`, a
    legitimate identity or device event's plan key. The three shapes cannot collide."""
    occurred_ms, tiebreak, kind, payload = item
    if kind == 1:
        instance, _planned, ordinal = payload
        identity = f"{instance.instance_id}:{ordinal}"
    elif kind == 2:
        identity = payload.key
    else:
        identity = str(tiebreak)
    return occurred_ms, tie_order_key(seed, identity)


_LOCATION_ATTEMPTS: Final = 32
"""Location redraws allowed before a planted episode fails loudly rather than
emit a label its rows no longer support."""


def _redraw_planted_transactions(
    config: GeneratorConfig, universe: Universe, planted: list[tuple[int, int, int, Any]]
) -> list[tuple[int, int, int, Any]]:
    """M1-M3: planted transactions with their location and entry mode redrawn.

    eval-v1 copied a scenario's planted point onto the transactions it placed --
    one exact coordinate repeated through a takeover, the account's home point
    itself on an impossible-travel leg -- and forced `ECOMMERCE` or `CHIP` as the
    entry mode. No legitimate transaction looks like that, so each marked fraud.
    Here the planted point is the anchor of the legitimate noise model, and the
    entry mode is drawn from the planted channel's legitimate modes. Channels,
    amounts, merchants, devices, IPs and every other planted field are untouched,
    and side events pass through unchanged.
    """
    by_instance: dict[str, list[tuple[int, int, int, Any]]] = defaultdict(list)
    for entry in planted:
        by_instance[entry[3][0].instance_id].append(entry)

    redrawn: list[tuple[int, int, int, Any]] = []
    for instance_id, entries in by_instance.items():
        candidate: list[tuple[int, int, int, Any]] = []
        for attempt in range(_LOCATION_ATTEMPTS):
            candidate = [
                _with_redrawn_fields(config, universe, entry, attempt) for entry in entries
            ]
            if _travel_stays_impossible(candidate):
                break
        else:
            raise ValueError(
                f"{instance_id}: no location draw in {_LOCATION_ATTEMPTS} attempts kept the "
                f"planted travel impossible; emitting it would be a wrong label"
            )
        redrawn.extend(candidate)
    return redrawn


def _with_redrawn_fields(
    config: GeneratorConfig, universe: Universe, entry: tuple[int, int, int, Any], attempt: int
) -> tuple[int, int, int, Any]:
    """One planted transaction with M1-M3 applied; any other entry unchanged.

    A point planted on the account's exact home point is M2's; any other planted point is M1's.
    An ablated correction leaves its field as planned."""
    from dataclasses import replace

    from trace_core.domain.geo import GeoPoint

    occurred_ms, tiebreak, kind, (instance, planned, ordinal) = entry
    if planned.topic != "tx.raw.v1":
        return entry
    overrides = dict(planned.overrides)
    key = f"{instance.instance_id}:{ordinal}"
    if "latitude" in overrides or "longitude" in overrides:
        anchor = GeoPoint(
            latitude=float(overrides["latitude"]), longitude=float(overrides["longitude"])
        )
        home = universe.profiles[planned.account_index].account.home
        if _corrected(config, "M2" if anchor == home else "M1"):
            location = derive(config.seed, "scenario-location", f"{key}:{attempt}")
            point = sample_location(location, anchor, config.geo_jitter_km)
            overrides["latitude"] = point.latitude
            overrides["longitude"] = point.longitude
    if "entry_mode" in overrides and _corrected(config, "M3"):
        modes = _CHANNEL_ENTRY.get(str(overrides.get("channel")))
        if modes is None:
            raise ValueError(f"{key}: a planted entry mode can only be redrawn with its channel")
        chooser = derive(config.seed, "scenario-entry-mode", key)
        overrides["entry_mode"] = modes[chooser.randrange(len(modes))]
    return occurred_ms, tiebreak, kind, (instance, replace(planned, overrides=overrides), ordinal)


def _travel_stays_impossible(entries: list[tuple[int, int, int, Any]]) -> bool:
    """IMPOSSIBLE_TRAVEL's label is true only while its emitted legs stay infeasible."""
    import itertools

    from data.generator.scenarios import SCENARIOS_BY_PATTERN, ImpossibleTravel
    from trace_core.domain.geo import GeoPoint, implied_speed_kmh

    if not entries or entries[0][3][0].pattern is not FraudPattern.IMPOSSIBLE_TRAVEL:
        return True
    scenario = SCENARIOS_BY_PATTERN[FraudPattern.IMPOSSIBLE_TRAVEL]
    threshold = (
        scenario.min_speed_kmh
        if isinstance(scenario, ImpossibleTravel)
        else ImpossibleTravel().min_speed_kmh
    )
    legs = sorted((e for e in entries if e[3][1].topic == "tx.raw.v1"), key=lambda e: (e[0], e[1]))
    for first, second in itertools.pairwise(legs):
        a = first[3][1].overrides
        b = second[3][1].overrides
        seconds = (second[0] - first[0]) / 1000
        if seconds <= 0:
            return False
        speed = implied_speed_kmh(
            GeoPoint(latitude=a["latitude"], longitude=a["longitude"]),
            GeoPoint(latitude=b["latitude"], longitude=b["longitude"]),
            seconds,
        )
        if speed <= threshold:
            return False
    return True


def generate_events(
    config: GeneratorConfig, universe: Universe | None = None
) -> Iterator[dict[str, Any]]:
    """Legitimate `tx.raw.v1` events only, in event-time order.

    Retained as its own entry point because the baseline is worth being able to
    generate and reason about without fraud in it -- both for tests and for the
    rules-only arm.
    """
    universe = universe or build_universe(config)
    clock = derive(config.seed, "clock")
    account_pick = derive(config.seed, "account-pick")
    lag = derive(config.seed, "ingest-lag")
    n_accounts = len(universe.profiles)

    times = sorted(
        to_millis(sample_occurred_at(clock, config.start_at, config.end_at))
        for _ in range(config.row_count)
    )
    for position, occurred_ms in enumerate(times):
        yield _build_transaction(
            config, universe, account_pick.randrange(n_accounts), occurred_ms, position, lag, {}
        )


def _pick_scenario(value: float, mix: Sequence[tuple[Any, float]]) -> Any:
    running = 0.0
    total = sum(weight for _, weight in mix)
    target = value * total
    for scenario, weight in mix:
        running += weight
        if target <= running:
            return scenario
    return mix[-1][0]


COVERAGE_FLOOR_PREFIX: Final = "fc_"
"""Instance ids G2 adds start with this; the mix's own start `fs_` or `fi_`."""


def coverage_mix(instances: Sequence[ScenarioInstance]) -> dict[str, tuple[int, int]]:
    """G2's disclosure (`LPC-5` §14.4): per pattern, (planned by the mix, added by G2).

    The mix count includes eval-v1's one instance per pattern (ADR-0030), planted before the
    weighted draw. Every pattern is listed, absent ones with zeros."""
    planned: Counter[str] = Counter()
    added: Counter[str] = Counter()
    for instance in instances:
        counts = added if instance.instance_id.startswith(COVERAGE_FLOOR_PREFIX) else planned
        counts[instance.pattern.value] += 1
    return {
        pattern.value: (planned[pattern.value], added[pattern.value]) for pattern in FraudPattern
    }


def _cover_floor(
    config: GeneratorConfig, universe: Universe, instances: list[ScenarioInstance], floor: int
) -> int:
    """G2: top every pattern up to `floor` instances. Returns the transactions added.

    Each extra instance is planned exactly as a mix instance is, from its own substream
    `derive(seed, "coverage-floor", f"{pattern}:{n}")`, so the top-up never moves the mix."""
    counts = Counter(instance.pattern for instance in instances)
    added_tx = 0
    for scenario in ALL_SCENARIOS:
        pattern = scenario.pattern
        attempt = 0
        while counts[pattern] < floor:
            if attempt >= 4 * floor + 64:
                raise ValueError(
                    f"G2: {pattern.value} planned no transaction in {attempt} attempts"
                )
            instance_id = f"{COVERAGE_FLOOR_PREFIX}{pattern.value.lower()}_{attempt:04d}"
            rng = derive(config.seed, "coverage-floor", f"{pattern.value}:{attempt}")
            span = max(1, config.window_seconds - 3 * 86_400)
            start_ms = to_millis(config.start_at) + rng.randrange(span) * 1000
            instance = scenario.inject(rng, universe, instance_id, start_ms)
            attempt += 1
            if instance.transaction_count:
                instances.append(instance)
                counts[pattern] += 1
                added_tx += instance.transaction_count
    return added_tx


def plan_fraud(config: GeneratorConfig, universe: Universe) -> tuple[list[ScenarioInstance], int]:
    """Choose and plan fraud instances until the target base rate is reached.

    Planned before any row is emitted, because a scenario is a multi-event episode
    with correlated timing: an instance must know all of its own timestamps before
    the merged stream can be ordered.
    """
    if config.fraud_rate <= 0.0:
        return [], 0

    target = int(config.row_count * config.fraud_rate)
    mix = default_mix(config)
    chooser = derive(config.seed, "scenario-choice")
    instances: list[ScenarioInstance] = []
    total_tx = 0
    attempt = 0

    # Every pattern appears at least once, before the weighted mix runs.
    #
    # Without this, sampling alone leaves rare patterns absent from small
    # datasets: the multi-transaction episodes (a ring contributes tens of rows)
    # exhaust the budget first, and MERCHANT_COLLUSION or IMPOSSIBLE_TRAVEL may
    # never be drawn at all. A Track A dataset missing a pattern silently drops
    # that pattern from every per-pattern metric in Phase 9, and the absence
    # would look like a modelling result rather than a sampling artefact.
    #
    # On a dataset too small to hold ten episodes this overshoots `fraud_rate`.
    # That is the right trade -- coverage matters more than hitting a rate on a
    # toy dataset -- and the realised rate is measured and reported rather than
    # assumed, so the overshoot is visible.
    for scenario in ALL_SCENARIOS:
        instance_id = f"fs_{scenario.pattern.value.lower()}"
        rng = derive(config.seed, "scenario", instance_id)
        span = max(1, config.window_seconds - 3 * 86_400)
        start_ms = to_millis(config.start_at) + rng.randrange(span) * 1000
        instance = scenario.inject(rng, universe, instance_id, start_ms)
        if instance.transaction_count:
            instances.append(instance)
            total_tx += instance.transaction_count
    # Bounded: every scenario contributes at least one transaction, so this
    # terminates. The cap is defensive, not load-bearing.
    while total_tx < target and attempt < target * 4 + 64:
        scenario = _pick_scenario(chooser.random(), mix)
        instance_id = f"fi_{attempt:08d}"
        rng = derive(config.seed, "scenario", instance_id)
        # Leave room at the end of the window so a multi-day episode is not
        # truncated by the boundary into a shape it does not have.
        span = max(1, config.window_seconds - 3 * 86_400)
        start_ms = to_millis(config.start_at) + rng.randrange(span) * 1000
        instance = scenario.inject(rng, universe, instance_id, start_ms)
        if instance.transaction_count:
            instances.append(instance)
            total_tx += instance.transaction_count
        attempt += 1
    if config.baseline_identity is not None:
        total_tx += _cover_floor(
            config, universe, instances, config.baseline_identity.coverage_floor_instances
        )
    return instances, total_tx


def generate_dataset(
    config: GeneratorConfig, universe: Universe | None = None
) -> Iterator[GeneratedRow]:
    """The full dataset: legitimate traffic with fraud episodes woven in.

    With `config.baseline_identity` set (eval-v2), the legitimate baseline is
    woven in as well -- identity and device activity, non-home payment devices,
    declines -- and planted events take eval-v2 timing, side-event semantics, and
    locations and entry modes drawn the legitimate way. The transaction count,
    every label, every planted channel and every other planted override are
    unchanged.
    """
    universe = universe or build_universe(config)
    instances, fraud_tx = plan_fraud(config, universe)
    legit_count = max(0, config.row_count - fraud_tx)

    clock = derive(config.seed, "clock")
    account_pick = derive(config.seed, "account-pick")
    lag = derive(config.seed, "ingest-lag")
    n_accounts = len(universe.profiles)

    # (occurred_ms, tiebreak, kind, payload). The tiebreak keeps the merge total
    # and deterministic when two events land on the same millisecond.
    merged: list[tuple[int, int, int, Any]] = []
    for index in range(legit_count):
        occurred_ms = to_millis(sample_occurred_at(clock, config.start_at, config.end_at))
        merged.append((occurred_ms, index, 0, account_pick.randrange(n_accounts)))
    for offset, instance in enumerate(instances):
        for ordinal, planned in enumerate(instance.events):
            merged.append(
                (
                    planned.occurred_ms,
                    legit_count + offset * 1000 + ordinal,
                    1,
                    (instance, planned, ordinal),
                )
            )
    merged.sort(key=lambda item: (item[0], item[1]))

    eval_v2 = config.baseline_identity is not None
    decisions: random.Random | None = None
    if eval_v2:
        merged = _plan_eval_v2(config, universe, merged, legit_count, instances)
        decisions = derive(config.seed, "authorization-decision")
    pending_retries: dict[int, dict[str, Any]] = {}

    def decided_at(occurred_ms: int) -> int | None:
        if decisions is None:
            return None
        latency = int(abs(decisions.gauss(_DECISION_LATENCY_MEAN_MS, _DECISION_LATENCY_SD_MS)))
        return occurred_ms + _DECISION_LATENCY_FLOOR_MS + latency

    position = 0
    for occurred_ms, _tiebreak, kind, payload in merged:
        if kind == 0:
            event = _build_transaction(config, universe, payload, occurred_ms, position, lag, {})
            yield GeneratedRow(
                topic="tx.raw.v1",
                event=event,
                label=TransactionLabel(
                    transaction_id=event["payload"]["transaction_id"], is_fraud=False
                ),
            )
            position += 1
            continue

        if kind == 2:
            yield GeneratedRow(
                topic=payload.topic,
                event=_build_baseline_event(config, universe, payload, position),
                label=None,
            )
            continue

        if kind == 3:
            legitimate: _LegitimateTransaction = payload
            if legitimate.role == _RETRY:
                if legitimate.pair not in pending_retries:  # pragma: no cover - attempt is earlier
                    raise ValueError(f"retry {legitimate.pair} emitted before its attempt")
                overrides = pending_retries.pop(legitimate.pair)
            else:
                overrides = {}
                if legitimate.device_id is not None:
                    overrides["device_id"] = legitimate.device_id
                if legitimate.declined:
                    overrides["authorization_outcome"] = "DECLINED"
            event = _build_transaction(
                config, universe, legitimate.account_index, occurred_ms, position, lag, overrides
            )
            if legitimate.role == _ATTEMPT:
                pending_retries[legitimate.pair] = {
                    key: value
                    for key, value in event["payload"].items()
                    if key not in _RETRY_FRESH_FIELDS
                }
            yield GeneratedRow(
                topic="tx.raw.v1",
                event=event,
                label=TransactionLabel(
                    transaction_id=event["payload"]["transaction_id"], is_fraud=False
                ),
                authorization_decided_ms=decided_at(occurred_ms),
            )
            position += 1
            continue

        instance, planned, ordinal = payload
        if planned.topic == "tx.raw.v1":
            event = _build_transaction(
                config,
                universe,
                planned.account_index,
                occurred_ms,
                position,
                lag,
                planned.overrides,
            )
            yield GeneratedRow(
                topic="tx.raw.v1",
                event=event,
                label=TransactionLabel(
                    transaction_id=event["payload"]["transaction_id"],
                    is_fraud=True,
                    fraud_pattern=instance.pattern,
                    scenario_instance_id=instance.instance_id,
                    causal_evidence_keys=instance.causal_evidence_keys,
                ),
                scenario_instance=instance,
                authorization_decided_ms=decided_at(occurred_ms),
                planned_ordinal=ordinal,
            )
            position += 1
        else:
            side_event = (
                _build_scenario_side_event_v2(
                    config, universe, instance, planned, ordinal, occurred_ms, position, lag
                )
                if eval_v2
                else _build_side_event(config, universe, planned, occurred_ms, position, lag)
            )
            yield GeneratedRow(
                topic=planned.topic,
                event=side_event,
                label=None,
                scenario_instance=instance,
                planned_ordinal=ordinal,
            )


def validate_events(events: Iterator[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    """Validate each event against the released contract as it passes through.

    `docs/EVENT_CONTRACTS.md` §6.1: an invalid message is never published, so
    producers fail fast rather than poisoning consumers. A pass-through so the
    caller chooses whether to pay for it, and the choice is recorded in the run
    record rather than left implicit.
    """
    from data.generator.digest import canonical_bytes
    from trace_core.contracts.events.tx_raw_v1 import TxRawV1

    for event in events:
        TxRawV1.model_validate_json(canonical_bytes(event))
        yield event
