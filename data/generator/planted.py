"""eval-v2 planted episodes: the generator decisions of `LPC-5` revision 2 §11 (ADR-0050).

Applied under the gate only, to the episodes `engine.plan_fraud` planned, before any row is built.
eval-v1 is untouched. A revision changes nuisance parameters only, never a scenario's mechanism,
required relationships or documented signature:

- **G1 (N9) amounts.** Ordinary amounts come from the account's own sampler, restricted to the
  documented region where one is documented; card-testing probes and anomalous high values keep
  their documented absolute and far-beyond mechanisms.
- **G4 devices.** Card-testing, credential-stuffing and velocity-attack transactions lose their
  planted device; the engine gives them the account's own payment device once the legitimate device
  plan exists. The stuffing logins keep their one shared device (the documented `DEVICE_SHARING`).
  A velocity attack's catalogue documents a burst, not device multiplicity (`LPC-5` revision 3).
- **G6 merchant collusion.** One price per instance; payers for whom that price is ordinary.
- **G7 causal keys.** Only what the revised mechanism creates.
- **M5 merchants.** Popularity-weighted, as legitimate spend outside the habitual set is. A takeover
  rejects the account's habitual merchants; an anomalous high value rejects its habitual
  categories, because its signature is "a merchant category it never uses".
- **M6 channels.** Channel and entry-mode overrides removed where no documentation names a channel.
- **Takeover change type.** Drawn from the legitimate identity-change mix instead of three of the
  five Q4e types (`LPC-5` §18, item 2).

Every draw comes from a substream keyed by the instance and its planned event, so a revision never
depends on generation order. An exhausted rejection loop raises: there is no fallback value.
"""

from __future__ import annotations

import bisect
import math
import random
from dataclasses import replace
from typing import Any, Final

from data.generator.baseline import standalone_change_rates
from data.generator.behavior import sample_amount_minor
from data.generator.config import BaselineIdentityConfig, GeneratorConfig
from data.generator.population import AccountProfile, Universe
from data.generator.rng import derive
from data.generator.scenarios import PlannedEvent, ScenarioInstance
from trace_core.domain.enums import EvidenceKind, FraudPattern

FP = FraudPattern
TX_TOPIC: Final = "tx.raw.v1"
IDENTITY_TOPIC: Final = "identity.events.v1"

AMOUNT_NAMESPACE: Final = "scenario-amount"
MERCHANT_NAMESPACE: Final = "scenario-merchant"
PAYER_NAMESPACE: Final = "scenario-payer"
CHANGE_TYPE_NAMESPACE: Final = "scenario-change-type"
ACCOUNT_NAMESPACE: Final = "scenario-account"
MAX_DRAWS: Final = 10_000

_Region = tuple[float | None, bool, float | None]
"""(lower multiple of typical or None, lower bound exclusive, exclusive upper multiple or None)."""

_ORDINARY: Final = frozenset(
    {FP.IMPOSSIBLE_TRAVEL, FP.DEVICE_FARM, FP.FRAUD_RING, FP.CREDENTIAL_STUFFING}
)
_REGIONS: Final[dict[FraudPattern, _Region]] = {
    FP.VELOCITY_ATTACK: (None, False, 3.0),
    FP.UNUSUAL_LOCATION_DEVICE: (0.5, True, 2.0),
    FP.ACCOUNT_TAKEOVER: (3.0, False, None),
}
_PAYOFF_REGION: Final[_Region] = (3.0, False, None)
PROBE_RANGE: Final = (1, 250)
HIGH_VALUE_MULTIPLE: Final = (20.0, 60.0)
PRICE_LOG_RANGE: Final = (8.3, 8.9)
PRICE_MULTIPLIER: Final = (0.99, 1.01)

EVAL_V2_CAUSAL_KEYS: Final[dict[FraudPattern, frozenset[EvidenceKind]]] = {
    FP.CREDENTIAL_STUFFING: frozenset(
        {
            EvidenceKind.AUTHENTICATION_ANOMALY,
            EvidenceKind.IP_REPUTATION,
            EvidenceKind.DEVICE_SHARING,
        }
    ),
    FP.CARD_TESTING: frozenset(
        {EvidenceKind.VELOCITY, EvidenceKind.AMOUNT_ANOMALY, EvidenceKind.MCC_ANOMALY}
    ),
}
"""G7: scenarios whose eval-v2 keys differ from eval-v1's. Every other keeps its own."""

LEGITIMATE_DEVICE_PATTERNS: Final = frozenset(
    {FP.CARD_TESTING, FP.CREDENTIAL_STUFFING, FP.VELOCITY_ATTACK}
)
"""G4: transactions the engine gives the account's own payment device."""

_CHANNEL_FREE: Final = frozenset(
    {
        FP.ACCOUNT_TAKEOVER,
        FP.CARD_TESTING,
        FP.DEVICE_FARM,
        FP.MERCHANT_COLLUSION,
        FP.CREDENTIAL_STUFFING,
    }
)
"""M6: no documentation names a channel. `IMPOSSIBLE_TRAVEL` keeps its documented card-present."""

_MERCHANT_FIELDS: Final = ("merchant_id", "merchant_mcc", "merchant_country", "merchant_name")

Q4E_TYPES: Final = (
    "PASSWORD_CHANGE",
    "EMAIL_CHANGE",
    "PHONE_CHANGE",
    "ADDRESS_CHANGE",
    "MFA_RESET",
)


def change_type_weights(settings: BaselineIdentityConfig) -> tuple[float, ...]:
    """The legitimate Q4e mix, in `Q4E_TYPES` order: each type's expected rate per account-year.

    MFA resets coupled to a new device count with the standalone ones, as legitimate rows do."""
    rates = dict(standalone_change_rates(settings))
    rates["MFA_RESET"] += (
        settings.new_device_rate_per_account_year * settings.mfa_reset_on_new_device_share
    )
    return tuple(rates[change] for change in Q4E_TYPES)


def _weighted_change(rng: random.Random, weights: tuple[float, ...]) -> str:
    target = rng.random() * sum(weights)
    running = 0.0
    for change, weight in zip(Q4E_TYPES, weights, strict=True):
        running += weight
        if target < running:
            return change
    return Q4E_TYPES[max(i for i, w in enumerate(weights) if w > 0)]


def causal_keys(
    pattern: FraudPattern, eval_v1_keys: frozenset[EvidenceKind]
) -> frozenset[EvidenceKind]:
    """G7: a scenario's causal keys under the gate."""
    return EVAL_V2_CAUSAL_KEYS.get(pattern, eval_v1_keys)


# ----------------------------------------------------------------------------- G1 and G6 ---
def _in_region(amount: int, typical: float, region: _Region) -> bool:
    lower, exclusive, upper = region
    if lower is not None:
        bound = lower * typical
        if amount < bound or (exclusive and amount <= bound):
            return False
    return upper is None or amount < upper * typical


def collusion_price(seed: int, instance_id: str) -> int:
    """G6: one price per merchant-collusion instance."""
    rng = derive(seed, AMOUNT_NAMESPACE, f"{instance_id}:price")
    return int(math.exp(rng.uniform(*PRICE_LOG_RANGE)))


def g1_amount(
    seed: int,
    instance_id: str,
    ordinal: int,
    pattern: FraudPattern,
    profile: AccountProfile,
    *,
    payoff: bool = False,
) -> int:
    """G1: a planted amount. `ordinal` is the planned event's index in its instance."""
    rng = derive(seed, AMOUNT_NAMESPACE, f"{instance_id}:{ordinal}")
    typical = math.exp(profile.amount_mu)
    if payoff or pattern in _REGIONS:
        region = _PAYOFF_REGION if payoff else _REGIONS[pattern]
        for _ in range(MAX_DRAWS):
            amount = sample_amount_minor(rng, profile)
            if _in_region(amount, typical, region):
                return amount
        raise ValueError(
            f"G1: {instance_id}:{ordinal} ({pattern.value}) drew no amount in its region in "
            f"{MAX_DRAWS} draws; there is no fallback value"
        )
    if pattern in _ORDINARY:
        return sample_amount_minor(rng, profile)
    if pattern is FP.CARD_TESTING:
        return rng.randrange(*PROBE_RANGE)
    if pattern is FP.ANOMALOUS_HIGH_VALUE:
        return int(typical * rng.uniform(*HIGH_VALUE_MULTIPLE)) + 1
    if pattern is FP.MERCHANT_COLLUSION:
        return int(collusion_price(seed, instance_id) * rng.uniform(*PRICE_MULTIPLIER))
    raise ValueError(f"G1 has no amount mechanism for {pattern.value}")  # pragma: no cover


def _collusion_payer(universe: Universe, rng: random.Random, price: int, where: str) -> int:
    """G6: an account drawn uniformly, rejected while `price` is not ordinary for it."""
    log_price = math.log(price)
    for _ in range(MAX_DRAWS):
        index = rng.randrange(len(universe.profiles))
        profile = universe.profiles[index]
        if abs(log_price - profile.amount_mu) <= profile.amount_sigma:
            return index
    raise ValueError(f"G6: {where} found no payer for price {price} in {MAX_DRAWS} draws")


# ----------------------------------------------------------------------------------- M5 ----
def _popular_merchant(rng: random.Random, universe: Universe) -> int:
    """A merchant index by popularity, as legitimate spend outside the habitual set draws."""
    target = rng.random() * universe.merchant_cum_weights[-1]
    return min(
        bisect.bisect_left(universe.merchant_cum_weights, target), len(universe.merchants) - 1
    )


def _merchant_index(merchant_id: str) -> int:
    return int(merchant_id.split("_")[1])


def _new_merchant(
    rng: random.Random,
    universe: Universe,
    account_index: int,
    *,
    new_category: bool,
    taken: set[int],
    where: str,
) -> int:
    profile = universe.profiles[account_index]
    habitual = {_merchant_index(m) for m in profile.habitual_merchants}
    habitual_mccs = {universe.merchants[m].mcc for m in habitual}
    for _ in range(MAX_DRAWS):
        index = _popular_merchant(rng, universe)
        if index in habitual or index in taken:
            continue
        if new_category and universe.merchants[index].mcc in habitual_mccs:
            continue
        return index
    raise ValueError(f"M5: {where} found no eligible merchant in {MAX_DRAWS} draws")


def _any_merchant(rng: random.Random, universe: Universe, taken: set[int], where: str) -> int:
    for _ in range(MAX_DRAWS):
        index = _popular_merchant(rng, universe)
        if index not in taken:
            return index
    raise ValueError(f"M5: {where} found no unused merchant in {MAX_DRAWS} draws")


def _merchant_payload(universe: Universe, index: int) -> dict[str, Any]:
    merchant = universe.merchants[index]
    return {
        "merchant_id": merchant.merchant_id,
        "merchant_mcc": merchant.mcc,
        "merchant_country": merchant.country,
        "merchant_name": merchant.name,
    }


# ------------------------------------------------------------------------ device farm ----
def _distinct_farm_accounts(
    seed: int,
    universe: Universe,
    iid: str,
    events: list[PlannedEvent],
    tx_ordinals: list[int],
) -> None:
    """Every device-farm session gets its own account, in place.

    eval-v1 drew a farm's accounts with replacement, so two sessions could share one and an
    instance could fall below the six accounts its signature needs. A session is a run of at most
    two consecutive transactions by one account, as eval-v1 plans them. A repeated account is
    replaced from `derive(seed, "scenario-account", f"{instance_id}:{ordinal}")`."""
    used: set[int] = set()
    session_account = -1
    session_length = 0
    previous: int | None = None
    for ordinal in tx_ordinals:
        event = events[ordinal]
        if event.account_index == previous and session_length < 2:
            session_length += 1
        else:
            session_length = 1
            account = event.account_index
            if account in used:
                rng = derive(seed, ACCOUNT_NAMESPACE, f"{iid}:{ordinal}")
                for _ in range(MAX_DRAWS):
                    account = rng.randrange(len(universe.profiles))
                    if account not in used:
                        break
                else:
                    raise ValueError(f"{iid}: no unused account for a device-farm session")
            used.add(account)
            session_account = account
        previous = event.account_index
        events[ordinal] = replace(event, account_index=session_account)


# ------------------------------------------------------------------------------ revision ---
def revise(
    config: GeneratorConfig, universe: Universe, instance: ScenarioInstance
) -> ScenarioInstance:
    """One planned episode with the gate's scenario decisions applied (module docstring)."""
    settings = config.baseline_identity
    if settings is None:  # pragma: no cover - guarded by the caller
        raise ValueError("revise requires the eval-v2 gate")
    seed = config.seed
    pattern = instance.pattern
    iid = instance.instance_id
    events = list(instance.events)
    participants = {key: list(values) for key, values in instance.participants.items()}
    tx_ordinals = [ordinal for ordinal, event in enumerate(events) if event.topic == TX_TOPIC]
    payoff = tx_ordinals[-1] if pattern is FP.CARD_TESTING and tx_ordinals else None
    amounts = settings.applies("N9")
    merchants = settings.applies("M5")

    if amounts and pattern is FP.MERCHANT_COLLUSION:
        price = collusion_price(seed, iid)
        for ordinal in tx_ordinals:
            payer = _collusion_payer(
                universe,
                derive(seed, PAYER_NAMESPACE, f"{iid}:{ordinal}"),
                price,
                f"{iid}:{ordinal}",
            )
            events[ordinal] = replace(events[ordinal], account_index=payer)
        participants["accounts"] = sorted(
            {universe.profiles[events[o].account_index].account_id for o in tx_ordinals}
        )

    if pattern is FP.DEVICE_FARM:
        _distinct_farm_accounts(seed, universe, iid, events, tx_ordinals)
        participants["accounts"] = sorted(
            {universe.profiles[events[o].account_index].account_id for o in tx_ordinals}
        )

    ring_merchants: dict[str, int] = {}
    if merchants and pattern is FP.FRAUD_RING:
        planned_ids = list(
            dict.fromkeys(
                str(events[o].overrides["merchant_id"])
                for o in tx_ordinals
                if "merchant_id" in events[o].overrides
            )
        )
        rng = derive(seed, MERCHANT_NAMESPACE, f"{iid}:set")
        taken: set[int] = set()
        for planned_id in planned_ids:
            index = _any_merchant(rng, universe, taken, f"{iid}:set")
            taken.add(index)
            ring_merchants[planned_id] = index
        participants["merchants"] = sorted(
            {universe.merchants[index].merchant_id for index in ring_merchants.values()}
        )
    probe_rng = derive(seed, MERCHANT_NAMESPACE, f"{iid}:probes")
    probe_taken: set[int] = set()

    for ordinal in tx_ordinals:
        event = events[ordinal]
        overrides = dict(event.overrides)
        profile = universe.profiles[event.account_index]
        if amounts:
            overrides["amount_minor"] = g1_amount(
                seed, iid, ordinal, pattern, profile, payoff=ordinal == payoff
            )
        if pattern in LEGITIMATE_DEVICE_PATTERNS:
            overrides.pop("device_id", None)
        if settings.applies("M6") and pattern in _CHANNEL_FREE:
            overrides.pop("channel", None)
            overrides.pop("entry_mode", None)
        if merchants:
            where = f"{iid}:{ordinal}"
            if pattern in (FP.ACCOUNT_TAKEOVER, FP.ANOMALOUS_HIGH_VALUE):
                index = _new_merchant(
                    derive(seed, MERCHANT_NAMESPACE, where),
                    universe,
                    event.account_index,
                    new_category=pattern is FP.ANOMALOUS_HIGH_VALUE,
                    taken=set(),
                    where=where,
                )
                overrides.update(_merchant_payload(universe, index))
            elif pattern is FP.CARD_TESTING and ordinal != payoff:
                index = _any_merchant(probe_rng, universe, probe_taken, f"{iid}:probes")
                probe_taken.add(index)
                overrides.update(_merchant_payload(universe, index))
            elif pattern in (FP.CARD_TESTING, FP.CREDENTIAL_STUFFING):
                for field in _MERCHANT_FIELDS:
                    overrides.pop(field, None)
            elif pattern is FP.FRAUD_RING and "merchant_id" in overrides:
                overrides.update(
                    _merchant_payload(universe, ring_merchants[str(overrides["merchant_id"])])
                )
        events[ordinal] = replace(event, overrides=overrides)

    weights = change_type_weights(settings)
    if pattern is FP.ACCOUNT_TAKEOVER and sum(weights) > 0:
        for ordinal, event in enumerate(events):
            if event.topic == IDENTITY_TOPIC and event.identity_event_type in Q4E_TYPES:
                rng = derive(seed, CHANGE_TYPE_NAMESPACE, f"{iid}:{ordinal}")
                events[ordinal] = replace(event, identity_event_type=_weighted_change(rng, weights))

    if pattern is FP.CARD_TESTING:
        participants.pop("devices", None)
    return replace(
        instance,
        causal_evidence_keys=causal_keys(pattern, instance.causal_evidence_keys),
        events=tuple(events),
        participants=participants,
    )
