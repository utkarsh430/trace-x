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
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any, Final

from data.generator.behavior import (
    pick_merchant_index,
    sample_amount_minor,
    sample_location,
    sample_occurred_at,
)
from data.generator.config import PRODUCER, GeneratorConfig
from data.generator.labels import TransactionLabel
from data.generator.population import Universe, build_universe
from data.generator.rng import derive
from data.generator.scenarios import ALL_SCENARIOS, ScenarioInstance, default_mix
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


def _iso(millis: int) -> str:
    return dt.datetime.fromtimestamp(millis / 1000, tz=dt.UTC).isoformat().replace("+00:00", "Z")


def _pick_channel(value: float) -> str:
    running = 0.0
    for channel, weight in _CHANNELS:
        running += weight
        if value <= running:
            return channel
    return _CHANNELS[-1][0]


def _envelope(
    event_type: str, occurred_ms: int, ingested_ms: int, position: int, rng: Any
) -> dict[str, Any]:
    return {
        "event_id": str(uuid7(millis=occurred_ms, rng=rng)),
        "event_type": event_type,
        "schema_version": SCHEMA_VERSION,
        "occurred_at": _iso(occurred_ms),
        "ingested_at": _iso(ingested_ms),
        "producer": PRODUCER,
        "trace_id": f"{rng.getrandbits(128):032x}",
        "correlation_id": f"corr_{position:012d}",
        "idempotency_key": "sha256:" + f"{rng.getrandbits(256):064x}",
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
        "envelope": _envelope("tx.raw", occurred_ms, ingested_ms, position, rng),
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
        "envelope": _envelope(event_type, occurred_ms, ingested_ms, position, rng),
        "payload": payload,
    }


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
    return instances, total_tx


def generate_dataset(
    config: GeneratorConfig, universe: Universe | None = None
) -> Iterator[GeneratedRow]:
    """The full dataset: legitimate traffic with fraud episodes woven in."""
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
                (planned.occurred_ms, legit_count + offset * 1000 + ordinal, 1, (instance, planned))
            )
    merged.sort(key=lambda item: (item[0], item[1]))

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

        instance, planned = payload
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
            )
            position += 1
        else:
            yield GeneratedRow(
                topic=planned.topic,
                event=_build_side_event(config, universe, planned, occurred_ms, position, lag),
                label=None,
                scenario_instance=instance,
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
