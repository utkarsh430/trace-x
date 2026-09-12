"""The generation engine: legitimate traffic as `tx.raw.v1` event dicts.

Fraud scenarios are layered on top of this in step 7; this module produces the
baseline they depart from.

**Why timestamps are sampled and then sorted.** Events are emitted in event-time
order, which is what a real stream approximates and what makes the output usable
without a shuffle. Sampling all N timestamps first, sorting them, and then
filling in the rest of each row keeps peak memory at one array of integers
rather than a million dictionaries -- on a 1 M-row run that is the difference
between about 8 MB and well over a gigabyte.

**Why dicts rather than Pydantic models.** Validation is a produce-time
requirement (docs/EVENT_CONTRACTS.md §6.1), but validating inside the hot loop
would put a per-row cost on every row whether or not the caller wants it. The
engine yields plain dicts; `validate_events` applies the released schema, and
the CLI decides the policy. The measured cost of each choice is recorded rather
than assumed.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from typing import Any, Final

from data.generator.behavior import (
    pick_merchant_index,
    sample_amount_minor,
    sample_location,
    sample_occurred_at,
)
from data.generator.config import PRODUCER, GeneratorConfig
from data.generator.population import Universe, build_universe
from data.generator.rng import derive
from trace_core.domain.identifiers import uuid7
from trace_core.domain.time import to_millis

EVENT_TYPE: Final = "tx.raw"
SCHEMA_VERSION: Final = 1

# Card-present entry modes are plausible only for a physical channel; using an
# ECOMMERCE entry mode on a CARD_PRESENT transaction would be a contradiction a
# rule could trivially exploit as a fraud signal, which would be an artefact of
# the generator rather than of fraud.
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


def _pick_channel(value: float) -> str:
    running = 0.0
    for channel, weight in _CHANNELS:
        running += weight
        if value <= running:
            return channel
    return _CHANNELS[-1][0]


def generate_events(
    config: GeneratorConfig, universe: Universe | None = None
) -> Iterator[dict[str, Any]]:
    """Yield `config.row_count` legitimate `tx.raw.v1` events in event-time order.

    Deterministic: the same config yields byte-identical rows in the same order.
    """
    universe = universe or build_universe(config)
    seed = config.seed

    # 1. Timestamps first, so the output is event-time ordered without holding
    #    the rows themselves in memory.
    clock = derive(seed, "clock")
    times = sorted(
        to_millis(sample_occurred_at(clock, config.start_at, config.end_at))
        for _ in range(config.row_count)
    )

    account_pick = derive(seed, "account-pick")
    n_accounts = len(universe.profiles)
    lag = derive(seed, "ingest-lag")

    for index, occurred_ms in enumerate(times):
        profile = universe.profiles[account_pick.randrange(n_accounts)]
        # One substream per emitted row, keyed by its ordinal: a row's content
        # depends on its position and the seed, never on what was drawn for the
        # rows before it. That is what lets step 7 inject scenarios without
        # shifting the legitimate rows around them.
        rng = derive(seed, "tx", str(index))

        merchant_index = pick_merchant_index(
            rng, profile, universe.merchant_cum_weights, config.habitual_merchant_ratio
        )
        if merchant_index is None:
            merchant_id_value = profile.habitual_merchants[
                rng.randrange(len(profile.habitual_merchants))
            ]
            merchant = universe.merchants[int(merchant_id_value.split("_")[1])]
        else:
            merchant = universe.merchants[merchant_index]

        channel = _pick_channel(rng.random())
        entry_modes = _CHANNEL_ENTRY[channel]
        location = sample_location(rng, profile.account.home, config.geo_jitter_km)
        device = profile.home_devices[rng.randrange(len(profile.home_devices))]
        ip = profile.home_ips[rng.randrange(len(profile.home_ips))]
        card = profile.cards[rng.randrange(len(profile.cards))]

        occurred = dt.datetime.fromtimestamp(occurred_ms / 1000, tz=dt.UTC)
        # Processing time is always at or after event time; the gap is the lag
        # the warm path measures. Sampling it here means Bronze has a realistic
        # ingested_at rather than a copy of occurred_at.
        ingested_ms = occurred_ms + int(abs(lag.gauss(70, 40))) + 5

        yield {
            "envelope": {
                "event_id": str(uuid7(millis=occurred_ms, rng=rng)),
                "event_type": EVENT_TYPE,
                "schema_version": SCHEMA_VERSION,
                "occurred_at": occurred.isoformat().replace("+00:00", "Z"),
                "ingested_at": dt.datetime.fromtimestamp(ingested_ms / 1000, tz=dt.UTC)
                .isoformat()
                .replace("+00:00", "Z"),
                "producer": PRODUCER,
                "trace_id": f"{rng.getrandbits(128):032x}",
                "correlation_id": f"corr_{index:012d}",
                "idempotency_key": "sha256:" + f"{rng.getrandbits(256):064x}",
            },
            "payload": {
                "transaction_id": f"tx_{index:012d}",
                "account_id": profile.account_id,
                "card_id": card.card_id,
                "device_id": device,
                "merchant_id": merchant.merchant_id,
                "ip_id": ip,
                "amount_minor": sample_amount_minor(rng, profile),
                "currency": config.currency,
                "channel": channel,
                "entry_mode": entry_modes[rng.randrange(len(entry_modes))],
                "merchant_mcc": merchant.mcc,
                "merchant_country": merchant.country,
                "merchant_name": merchant.name,
                "latitude": location.latitude,
                "longitude": location.longitude,
                "user_agent": _USER_AGENTS[rng.randrange(len(_USER_AGENTS))],
                "memo": "",
                "authorization_outcome": "APPROVED",
            },
        }


def validate_events(events: Iterator[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    """Validate each event against the released contract as it passes through.

    docs/EVENT_CONTRACTS.md §6.1: an invalid message is never published, so
    producers fail fast rather than poisoning consumers. Applied as a pass-through
    so the caller chooses whether to pay for it, and the choice is recorded in
    the run record rather than left implicit.
    """
    from data.generator.digest import canonical_bytes
    from trace_core.contracts.events.tx_raw_v1 import TxRawV1

    for event in events:
        TxRawV1.model_validate_json(canonical_bytes(event))
        yield event
