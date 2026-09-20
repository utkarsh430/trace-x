"""The ten fraud scenarios (ADR-0030, docs/FRAUD_SCENARIOS.md).

Each scenario declares three things, and all three are load-bearing:

* a **signature** -- the mechanically assertable shape it produces, so a test can
  verify the injection actually did what it claims;
* **`causal_evidence_keys`** -- the specific evidence kinds that genuinely explain
  *this* instance. This is what makes evidence precision and recall set
  operations rather than an LLM judgement (`docs/EVALUATION.md` §5), and it is
  the reason Track A can measure agent reasoning at all;
* an **injection** that plans events against the existing population, so fraud
  is a departure from an account's own baseline rather than a separate universe.

**Causal keys are a claim about causation, not a wish list.** Listing every
plausible signal would inflate evidence recall for free and make the metric
meaningless. A key belongs here only if the injection actually creates that
signal -- which is why every scenario's signature test checks the shape, not just
the label.

Nothing in this module writes ground truth. It returns labels alongside events,
and only `data.generator.groundtruth` persists them -- to a schema the
application role cannot read at all (ADR-0004).
"""

from __future__ import annotations

import math
import random
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Final, Protocol

from data.generator.config import GeneratorConfig
from data.generator.population import CITIES, Universe
from trace_core.domain.enums import EvidenceKind, FraudPattern
from trace_core.domain.geo import GeoPoint, haversine_km

MINUTE_MS: Final = 60_000
HOUR_MS: Final = 3_600_000
DAY_MS: Final = 86_400_000


@dataclass(frozen=True, slots=True)
class PlannedEvent:
    """One event a scenario wants emitted.

    `overrides` are payload fields that differ from the account's normal
    behaviour. Everything not overridden is filled in by the engine from the
    account's own profile, so an injected transaction is indistinguishable from a
    legitimate one except in the ways the scenario intends.
    """

    occurred_ms: int
    account_index: int
    overrides: dict[str, Any] = field(default_factory=dict)
    topic: str = "tx.raw.v1"
    identity_event_type: str | None = None
    device_event_type: str | None = None


@dataclass(frozen=True, slots=True)
class ScenarioInstance:
    """One injected fraud episode."""

    instance_id: str
    pattern: FraudPattern
    causal_evidence_keys: frozenset[EvidenceKind]
    events: tuple[PlannedEvent, ...]
    participants: dict[str, list[str]]

    @property
    def transaction_count(self) -> int:
        return sum(1 for e in self.events if e.topic == "tx.raw.v1")


class Scenario(Protocol):
    """What every fraud scenario implements.

    `pattern` is a read-only property rather than an attribute: the scenarios are
    frozen dataclasses, and a Protocol declaring a mutable attribute is invariant,
    so a frozen implementation would not satisfy it.
    """

    @property
    def pattern(self) -> FraudPattern: ...

    def causal_evidence_keys(self) -> frozenset[EvidenceKind]: ...

    def signature(self) -> str:
        """One-line description of the shape, kept in step with FRAUD_SCENARIOS.md."""
        ...

    def inject(
        self, rng: random.Random, universe: Universe, instance_id: str, start_ms: int
    ) -> ScenarioInstance: ...


# --------------------------------------------------------------- helpers ----


def _distant_city(rng: random.Random, home: GeoPoint) -> GeoPoint:
    """A population centre far from `home`.

    A *city*, not a random point: fraud that lands in the North Sea is separable
    by a check nobody would deploy, which would flatter the detector.
    """
    far = [c for c in CITIES if abs(c[1] - home.latitude) + abs(c[2] - home.longitude) > 5.0] or [
        c for c in CITIES if abs(c[1] - home.latitude) + abs(c[2] - home.longitude) > 3.0
    ]
    _country, lat, lon, _weight = (far or list(CITIES))[rng.randrange(len(far or CITIES))]
    return GeoPoint(
        latitude=round(lat + rng.gauss(0, 0.05), 6),
        longitude=round(lon + rng.gauss(0, 0.05), 6),
    )


distant_city = _distant_city
"""Public for eval-v2's legitimate trips, which go where takeovers do (N2)."""


def _typical_amount(universe: Universe, account_index: int) -> int:
    profile = universe.profiles[account_index]
    return max(1, int(math.exp(profile.amount_mu)))


def _unhabitual_merchant(rng: random.Random, universe: Universe, account_index: int) -> int:
    """A merchant index the account does not normally use."""
    habitual = {int(m.split("_")[1]) for m in universe.profiles[account_index].habitual_merchants}
    for _ in range(16):
        candidate = rng.randrange(len(universe.merchants))
        if candidate not in habitual:
            return candidate
    return rng.randrange(len(universe.merchants))


def _novel_device(rng: random.Random, universe: Universe, account_index: int) -> str:
    """A device this account has never used."""
    home = set(universe.profiles[account_index].home_devices)
    for _ in range(16):
        candidate = universe.devices[rng.randrange(len(universe.devices))].device_id
        if candidate not in home:
            return candidate
    return universe.devices[rng.randrange(len(universe.devices))].device_id


def _merchant_payload(universe: Universe, merchant_index: int) -> dict[str, Any]:
    merchant = universe.merchants[merchant_index]
    return {
        "merchant_id": merchant.merchant_id,
        "merchant_mcc": merchant.mcc,
        "merchant_country": merchant.country,
        "merchant_name": merchant.name,
    }


def _geo(point: GeoPoint) -> dict[str, Any]:
    return {"latitude": point.latitude, "longitude": point.longitude}


# ------------------------------------------------------------- scenarios ----


@dataclass(frozen=True, slots=True)
class AccountTakeover:
    """Credentials changed, then a novel device spends fast and far from home."""

    pattern: FraudPattern = FraudPattern.ACCOUNT_TAKEOVER

    def causal_evidence_keys(self) -> frozenset[EvidenceKind]:
        return frozenset(
            {
                EvidenceKind.IDENTITY_CHANGE,
                EvidenceKind.DEVICE_NOVELTY,
                EvidenceKind.SPEND_PROFILE,
                EvidenceKind.AMOUNT_ANOMALY,
            }
        )

    def signature(self) -> str:
        return (
            "an identity change, then within hours a device the account has never used, "
            "spending well above profile at unhabitual merchants away from home"
        )

    def inject(
        self, rng: random.Random, universe: Universe, instance_id: str, start_ms: int
    ) -> ScenarioInstance:
        account_index = rng.randrange(len(universe.profiles))
        profile = universe.profiles[account_index]
        device = _novel_device(rng, universe, account_index)
        away = _distant_city(rng, profile.account.home)
        typical = _typical_amount(universe, account_index)

        events: list[PlannedEvent] = [
            # The credential change that starts it. A real takeover shows this
            # first; without it IDENTITY_CHANGE would not be a causal key.
            PlannedEvent(
                occurred_ms=start_ms,
                account_index=account_index,
                topic="identity.events.v1",
                identity_event_type=rng.choice(["PASSWORD_CHANGE", "EMAIL_CHANGE", "MFA_RESET"]),
                overrides={"device_id": device},
            ),
            PlannedEvent(
                occurred_ms=start_ms + 2 * MINUTE_MS,
                account_index=account_index,
                topic="device.events.v1",
                device_event_type="FIRST_SEEN",
                overrides={"device_id": device},
            ),
        ]
        offset = rng.randrange(20 * MINUTE_MS, 4 * HOUR_MS)
        for n in range(rng.randrange(3, 7)):
            events.append(
                PlannedEvent(
                    occurred_ms=start_ms
                    + offset
                    + n * rng.randrange(3 * MINUTE_MS, 25 * MINUTE_MS),
                    account_index=account_index,
                    overrides={
                        "device_id": device,
                        "amount_minor": int(typical * rng.uniform(3.0, 9.0)) + 1,
                        "channel": "CARD_NOT_PRESENT",
                        "entry_mode": "ECOMMERCE",
                        **_merchant_payload(
                            universe, _unhabitual_merchant(rng, universe, account_index)
                        ),
                        **_geo(away),
                    },
                )
            )
        return ScenarioInstance(
            instance_id=instance_id,
            pattern=self.pattern,
            causal_evidence_keys=self.causal_evidence_keys(),
            events=tuple(events),
            participants={"accounts": [profile.account_id], "devices": [device]},
        )


@dataclass(frozen=True, slots=True)
class CardTesting:
    """A burst of tiny authorisations across many merchants, probing a stolen card."""

    pattern: FraudPattern = FraudPattern.CARD_TESTING

    def causal_evidence_keys(self) -> frozenset[EvidenceKind]:
        return frozenset(
            {
                EvidenceKind.VELOCITY,
                EvidenceKind.AMOUNT_ANOMALY,
                EvidenceKind.MCC_ANOMALY,
                EvidenceKind.DEVICE_SHARING,
            }
        )

    def signature(self) -> str:
        return (
            "many sub-threshold authorisations across many distinct merchants and MCCs "
            "inside a few minutes from one device, often followed by one larger charge"
        )

    def inject(
        self, rng: random.Random, universe: Universe, instance_id: str, start_ms: int
    ) -> ScenarioInstance:
        account_index = rng.randrange(len(universe.profiles))
        profile = universe.profiles[account_index]
        device = _novel_device(rng, universe, account_index)
        probes = rng.randrange(9, 21)
        events: list[PlannedEvent] = []
        for n in range(probes):
            events.append(
                PlannedEvent(
                    occurred_ms=start_ms + n * rng.randrange(8_000, 45_000),
                    account_index=account_index,
                    overrides={
                        "device_id": device,
                        # Deliberately tiny: the point of card testing is to stay
                        # under the amount at which anyone looks.
                        "amount_minor": rng.randrange(1, 250),
                        "channel": "CARD_NOT_PRESENT",
                        "entry_mode": "ECOMMERCE",
                        "authorization_outcome": "DECLINED" if rng.random() < 0.45 else "APPROVED",
                        **_merchant_payload(universe, rng.randrange(len(universe.merchants))),
                    },
                )
            )
        # The payoff charge, which is what makes the probing worth detecting.
        events.append(
            PlannedEvent(
                occurred_ms=start_ms + probes * 45_000 + rng.randrange(MINUTE_MS, 30 * MINUTE_MS),
                account_index=account_index,
                overrides={
                    "device_id": device,
                    "amount_minor": int(
                        _typical_amount(universe, account_index) * rng.uniform(4, 12)
                    )
                    + 1,
                    "channel": "CARD_NOT_PRESENT",
                    "entry_mode": "ECOMMERCE",
                    **_merchant_payload(
                        universe, _unhabitual_merchant(rng, universe, account_index)
                    ),
                },
            )
        )
        return ScenarioInstance(
            instance_id=instance_id,
            pattern=self.pattern,
            causal_evidence_keys=self.causal_evidence_keys(),
            events=tuple(events),
            participants={"accounts": [profile.account_id], "devices": [device]},
        )


@dataclass(frozen=True, slots=True)
class ImpossibleTravel:
    """Two transactions whose separation implies an infeasible speed."""

    pattern: FraudPattern = FraudPattern.IMPOSSIBLE_TRAVEL
    min_speed_kmh: float = 900.0

    def causal_evidence_keys(self) -> frozenset[EvidenceKind]:
        return frozenset({EvidenceKind.GEO_DISPERSION, EvidenceKind.VELOCITY})

    def signature(self) -> str:
        return (
            "two card-present transactions whose great-circle distance over elapsed time "
            "implies a speed no commercial travel achieves"
        )

    def inject(
        self, rng: random.Random, universe: Universe, instance_id: str, start_ms: int
    ) -> ScenarioInstance:
        account_index = rng.randrange(len(universe.profiles))
        profile = universe.profiles[account_index]
        home = profile.account.home
        away = _distant_city(rng, home)

        # The gap is derived FROM the distance, not drawn independently.
        #
        # Drawing a gap at random produced instances implying ~500 km/h -- fast,
        # but an ordinary commercial flight, so not impossible at all. A label of
        # IMPOSSIBLE_TRAVEL on a possible journey is a wrong ground-truth label,
        # and every metric computed against it would be wrong in a way nothing
        # downstream could detect. The invariant now holds by construction and a
        # test verifies it over many samples.
        distance_km = haversine_km(home, away)
        max_gap_ms = int(distance_km / self.min_speed_kmh * HOUR_MS)
        lower = max(MINUTE_MS, int(max_gap_ms * 0.15))
        upper = max(lower + MINUTE_MS, int(max_gap_ms * 0.75))
        gap_ms = rng.randrange(lower, upper)
        return ScenarioInstance(
            instance_id=instance_id,
            pattern=self.pattern,
            causal_evidence_keys=self.causal_evidence_keys(),
            events=(
                PlannedEvent(
                    occurred_ms=start_ms,
                    account_index=account_index,
                    overrides={"channel": "CARD_PRESENT", "entry_mode": "CHIP", **_geo(home)},
                ),
                PlannedEvent(
                    occurred_ms=start_ms + gap_ms,
                    account_index=account_index,
                    # Card-present at both ends is what makes it impossible: a
                    # card-not-present leg would have an innocent explanation.
                    overrides={"channel": "CARD_PRESENT", "entry_mode": "CHIP", **_geo(away)},
                ),
            ),
            participants={"accounts": [profile.account_id]},
        )


@dataclass(frozen=True, slots=True)
class VelocityAttack:
    """Transaction count far above the account's own baseline, in minutes."""

    pattern: FraudPattern = FraudPattern.VELOCITY_ATTACK

    def causal_evidence_keys(self) -> frozenset[EvidenceKind]:
        return frozenset({EvidenceKind.VELOCITY, EvidenceKind.SPEND_PROFILE})

    def signature(self) -> str:
        return "a burst of transactions on one account far above its own 1m/5m/1h baseline"

    def inject(
        self, rng: random.Random, universe: Universe, instance_id: str, start_ms: int
    ) -> ScenarioInstance:
        account_index = rng.randrange(len(universe.profiles))
        profile = universe.profiles[account_index]
        typical = _typical_amount(universe, account_index)
        count = rng.randrange(16, 41)
        events = tuple(
            PlannedEvent(
                occurred_ms=start_ms + n * rng.randrange(10_000, 55_000),
                account_index=account_index,
                # Amounts stay ordinary on purpose: velocity must be detectable
                # on its own, not as a side effect of an amount anomaly.
                overrides={"amount_minor": int(typical * rng.uniform(0.6, 1.8)) + 1},
            )
            for n in range(count)
        )
        return ScenarioInstance(
            instance_id=instance_id,
            pattern=self.pattern,
            causal_evidence_keys=self.causal_evidence_keys(),
            events=events,
            participants={"accounts": [profile.account_id]},
        )


@dataclass(frozen=True, slots=True)
class DeviceFarm:
    """One device driving many unrelated accounts."""

    pattern: FraudPattern = FraudPattern.DEVICE_FARM
    min_accounts: int = 6

    def causal_evidence_keys(self) -> frozenset[EvidenceKind]:
        return frozenset(
            {EvidenceKind.DEVICE_SHARING, EvidenceKind.DEVICE_NOVELTY, EvidenceKind.GRAPH_CLUSTER}
        )

    def signature(self) -> str:
        return (
            "one device fingerprint used by many accounts with no shared geography, "
            "each account transacting only once or twice"
        )

    def inject(
        self, rng: random.Random, universe: Universe, instance_id: str, start_ms: int
    ) -> ScenarioInstance:
        device = universe.devices[rng.randrange(len(universe.devices))].device_id
        account_count = rng.randrange(self.min_accounts, 16)
        indices = [rng.randrange(len(universe.profiles)) for _ in range(account_count)]
        events: list[PlannedEvent] = []
        for slot, account_index in enumerate(indices):
            for n in range(rng.randrange(1, 3)):
                events.append(
                    PlannedEvent(
                        occurred_ms=start_ms
                        + slot * rng.randrange(3 * MINUTE_MS, 40 * MINUTE_MS)
                        + n * MINUTE_MS,
                        account_index=account_index,
                        overrides={
                            "device_id": device,
                            "channel": "CARD_NOT_PRESENT",
                            "entry_mode": "ECOMMERCE",
                        },
                    )
                )
        return ScenarioInstance(
            instance_id=instance_id,
            pattern=self.pattern,
            causal_evidence_keys=self.causal_evidence_keys(),
            events=tuple(events),
            participants={
                "accounts": sorted({universe.profiles[i].account_id for i in indices}),
                "devices": [device],
            },
        )


@dataclass(frozen=True, slots=True)
class FraudRing:
    """A connected component of accounts sharing devices and IPs."""

    pattern: FraudPattern = FraudPattern.FRAUD_RING

    def causal_evidence_keys(self) -> frozenset[EvidenceKind]:
        return frozenset(
            {
                EvidenceKind.GRAPH_CLUSTER,
                EvidenceKind.RING_SCORE,
                EvidenceKind.LINK_PATH,
                EvidenceKind.DEVICE_SHARING,
            }
        )

    def signature(self) -> str:
        return (
            "several accounts sharing a small pool of devices and IPs, with internal link "
            "density above baseline, converging on a shared merchant set over days"
        )

    def inject(
        self, rng: random.Random, universe: Universe, instance_id: str, start_ms: int
    ) -> ScenarioInstance:
        members = [rng.randrange(len(universe.profiles)) for _ in range(rng.randrange(4, 9))]
        shared_devices = [
            universe.devices[rng.randrange(len(universe.devices))].device_id
            for _ in range(rng.randrange(2, 4))
        ]
        shared_ips = [
            universe.ips[rng.randrange(len(universe.ips))].ip_id for _ in range(rng.randrange(1, 3))
        ]
        shared_merchants = [
            rng.randrange(len(universe.merchants)) for _ in range(rng.randrange(2, 4))
        ]
        events: list[PlannedEvent] = []
        for member_slot, account_index in enumerate(members):
            for n in range(rng.randrange(2, 6)):
                events.append(
                    PlannedEvent(
                        occurred_ms=start_ms
                        + member_slot * rng.randrange(HOUR_MS, 6 * HOUR_MS)
                        + n * rng.randrange(20 * MINUTE_MS, 3 * HOUR_MS),
                        account_index=account_index,
                        overrides={
                            "device_id": shared_devices[rng.randrange(len(shared_devices))],
                            "ip_id": shared_ips[rng.randrange(len(shared_ips))],
                            **_merchant_payload(
                                universe, shared_merchants[rng.randrange(len(shared_merchants))]
                            ),
                        },
                    )
                )
        return ScenarioInstance(
            instance_id=instance_id,
            pattern=self.pattern,
            causal_evidence_keys=self.causal_evidence_keys(),
            events=tuple(events),
            participants={
                "accounts": sorted({universe.profiles[i].account_id for i in members}),
                "devices": sorted(set(shared_devices)),
                "ips": sorted(set(shared_ips)),
                "merchants": sorted({universe.merchants[m].merchant_id for m in shared_merchants}),
            },
        )


@dataclass(frozen=True, slots=True)
class MerchantCollusion:
    """A merchant whose own volume is anomalous against its MCC baseline."""

    pattern: FraudPattern = FraudPattern.MERCHANT_COLLUSION

    def causal_evidence_keys(self) -> frozenset[EvidenceKind]:
        return frozenset(
            {EvidenceKind.MERCHANT_RISK, EvidenceKind.MERCHANT_PATTERN, EvidenceKind.MCC_ANOMALY}
        )

    def signature(self) -> str:
        return (
            "one merchant taking an implausible share of high, unusually uniform amounts "
            "from many unrelated accounts over days"
        )

    def inject(
        self, rng: random.Random, universe: Universe, instance_id: str, start_ms: int
    ) -> ScenarioInstance:
        merchant_index = rng.randrange(len(universe.merchants))
        payload = _merchant_payload(universe, merchant_index)
        # Uniform-ish high amounts are the tell: real spend at one merchant is
        # dispersed, laundering through one is not.
        band = rng.randrange(15_000, 60_000)
        indices = [rng.randrange(len(universe.profiles)) for _ in range(rng.randrange(18, 45))]
        events = tuple(
            PlannedEvent(
                occurred_ms=start_ms + slot * rng.randrange(30 * MINUTE_MS, 5 * HOUR_MS),
                account_index=account_index,
                overrides={
                    "amount_minor": band + rng.randrange(-400, 400),
                    "channel": "CARD_NOT_PRESENT",
                    "entry_mode": "ECOMMERCE",
                    **payload,
                },
            )
            for slot, account_index in enumerate(indices)
        )
        return ScenarioInstance(
            instance_id=instance_id,
            pattern=self.pattern,
            causal_evidence_keys=self.causal_evidence_keys(),
            events=events,
            participants={
                "merchants": [payload["merchant_id"]],
                "accounts": sorted({universe.profiles[i].account_id for i in indices}),
            },
        )


@dataclass(frozen=True, slots=True)
class CredentialStuffing:
    """Many accounts probed from a small IP pool; a few succeed and spend."""

    pattern: FraudPattern = FraudPattern.CREDENTIAL_STUFFING

    def causal_evidence_keys(self) -> frozenset[EvidenceKind]:
        return frozenset(
            {EvidenceKind.IP_REPUTATION, EvidenceKind.DEVICE_SHARING, EvidenceKind.IDENTITY_CHANGE}
        )

    def signature(self) -> str:
        return (
            "a burst of failed logins across many unrelated accounts from a small "
            "datacenter IP pool, a minority succeeding and transacting immediately"
        )

    def inject(
        self, rng: random.Random, universe: Universe, instance_id: str, start_ms: int
    ) -> ScenarioInstance:
        datacenter = [ip.ip_id for ip in universe.ips if ip.is_datacenter] or [
            universe.ips[0].ip_id
        ]
        pool = [datacenter[rng.randrange(len(datacenter))] for _ in range(rng.randrange(1, 4))]
        device = universe.devices[rng.randrange(len(universe.devices))].device_id
        targets = [rng.randrange(len(universe.profiles)) for _ in range(rng.randrange(16, 41))]
        events: list[PlannedEvent] = []
        succeeded: list[int] = []
        for slot, account_index in enumerate(targets):
            ip = pool[rng.randrange(len(pool))]
            success = rng.random() < 0.12
            events.append(
                PlannedEvent(
                    occurred_ms=start_ms + slot * rng.randrange(2_000, 25_000),
                    account_index=account_index,
                    topic="identity.events.v1",
                    identity_event_type="LOGIN_SUCCEEDED" if success else "LOGIN_FAILED",
                    overrides={"ip_id": ip, "device_id": device},
                )
            )
            if success:
                succeeded.append(account_index)
                events.append(
                    PlannedEvent(
                        occurred_ms=start_ms
                        + slot * 25_000
                        + rng.randrange(MINUTE_MS, 12 * MINUTE_MS),
                        account_index=account_index,
                        overrides={
                            "ip_id": ip,
                            "device_id": device,
                            "channel": "CARD_NOT_PRESENT",
                            "entry_mode": "ECOMMERCE",
                            **_merchant_payload(
                                universe, _unhabitual_merchant(rng, universe, account_index)
                            ),
                        },
                    )
                )
        if not succeeded:
            # A stuffing run with no successful takeover produces no fraudulent
            # transaction at all, which would be an instance with no positives.
            account_index = targets[0]
            events.append(
                PlannedEvent(
                    occurred_ms=start_ms + 30 * MINUTE_MS,
                    account_index=account_index,
                    overrides={
                        "ip_id": pool[0],
                        "device_id": device,
                        "channel": "CARD_NOT_PRESENT",
                        "entry_mode": "ECOMMERCE",
                    },
                )
            )
        return ScenarioInstance(
            instance_id=instance_id,
            pattern=self.pattern,
            causal_evidence_keys=self.causal_evidence_keys(),
            events=tuple(events),
            participants={
                "ips": sorted(set(pool)),
                "devices": [device],
                "accounts": sorted({universe.profiles[i].account_id for i in targets}),
            },
        )


@dataclass(frozen=True, slots=True)
class AnomalousHighValue:
    """A single charge far outside the account's own amount distribution."""

    pattern: FraudPattern = FraudPattern.ANOMALOUS_HIGH_VALUE
    min_multiple: float = 20.0

    def causal_evidence_keys(self) -> frozenset[EvidenceKind]:
        return frozenset(
            {EvidenceKind.AMOUNT_ANOMALY, EvidenceKind.SPEND_PROFILE, EvidenceKind.MCC_ANOMALY}
        )

    def signature(self) -> str:
        return (
            "one transaction far beyond the account's own amount distribution, "
            "at a merchant category it never uses"
        )

    def inject(
        self, rng: random.Random, universe: Universe, instance_id: str, start_ms: int
    ) -> ScenarioInstance:
        account_index = rng.randrange(len(universe.profiles))
        profile = universe.profiles[account_index]
        typical = _typical_amount(universe, account_index)
        return ScenarioInstance(
            instance_id=instance_id,
            pattern=self.pattern,
            causal_evidence_keys=self.causal_evidence_keys(),
            events=(
                PlannedEvent(
                    occurred_ms=start_ms,
                    account_index=account_index,
                    overrides={
                        "amount_minor": int(typical * rng.uniform(self.min_multiple, 60.0)) + 1,
                        **_merchant_payload(
                            universe, _unhabitual_merchant(rng, universe, account_index)
                        ),
                    },
                ),
            ),
            participants={"accounts": [profile.account_id]},
        )


@dataclass(frozen=True, slots=True)
class UnusualLocationDevice:
    """First-seen device AND first-seen place, at an ordinary amount.

    **Deliberately the weakest and most ambiguous scenario.** It overlaps heavily
    with a customer travelling with a new phone, and that is the point: it is the
    case the Phase 7 Skeptic should challenge and the Phase 9 Arm F ablation is
    measured on. Strengthening it -- adding an amount anomaly, say -- would make
    the ablation easy and destroy what it measures. Recorded in ADR-0030 so a
    later session does not "fix" it.
    """

    pattern: FraudPattern = FraudPattern.UNUSUAL_LOCATION_DEVICE

    def causal_evidence_keys(self) -> frozenset[EvidenceKind]:
        return frozenset({EvidenceKind.DEVICE_NOVELTY, EvidenceKind.GEO_DISPERSION})

    def signature(self) -> str:
        return (
            "a single transaction on a never-seen device in a never-seen place, with an "
            "amount squarely inside the account's normal range"
        )

    def inject(
        self, rng: random.Random, universe: Universe, instance_id: str, start_ms: int
    ) -> ScenarioInstance:
        account_index = rng.randrange(len(universe.profiles))
        profile = universe.profiles[account_index]
        device = _novel_device(rng, universe, account_index)
        away = _distant_city(rng, profile.account.home)
        typical = _typical_amount(universe, account_index)
        return ScenarioInstance(
            instance_id=instance_id,
            pattern=self.pattern,
            causal_evidence_keys=self.causal_evidence_keys(),
            events=(
                PlannedEvent(
                    occurred_ms=start_ms,
                    account_index=account_index,
                    overrides={
                        "device_id": device,
                        # Ordinary amount, on purpose.
                        "amount_minor": int(typical * rng.uniform(0.7, 1.5)) + 1,
                        **_geo(away),
                    },
                ),
            ),
            participants={"accounts": [profile.account_id], "devices": [device]},
        )


ALL_SCENARIOS: Final[tuple[Scenario, ...]] = (
    AccountTakeover(),
    CardTesting(),
    ImpossibleTravel(),
    VelocityAttack(),
    DeviceFarm(),
    FraudRing(),
    MerchantCollusion(),
    CredentialStuffing(),
    AnomalousHighValue(),
    UnusualLocationDevice(),
)

SCENARIOS_BY_PATTERN: Final[dict[FraudPattern, Scenario]] = {s.pattern: s for s in ALL_SCENARIOS}


def default_mix(config: GeneratorConfig) -> Sequence[tuple[Scenario, float]]:
    """Relative frequency of each scenario.

    Not uniform: single-transaction scenarios are common in reality and
    multi-account ones are rare, and a uniform mix would give the rare,
    many-transaction patterns an implausible share of all fraudulent rows.
    """
    del config
    return (
        (SCENARIOS_BY_PATTERN[FraudPattern.ANOMALOUS_HIGH_VALUE], 0.18),
        (SCENARIOS_BY_PATTERN[FraudPattern.UNUSUAL_LOCATION_DEVICE], 0.16),
        (SCENARIOS_BY_PATTERN[FraudPattern.ACCOUNT_TAKEOVER], 0.14),
        (SCENARIOS_BY_PATTERN[FraudPattern.CARD_TESTING], 0.12),
        (SCENARIOS_BY_PATTERN[FraudPattern.IMPOSSIBLE_TRAVEL], 0.11),
        (SCENARIOS_BY_PATTERN[FraudPattern.VELOCITY_ATTACK], 0.09),
        (SCENARIOS_BY_PATTERN[FraudPattern.DEVICE_FARM], 0.07),
        (SCENARIOS_BY_PATTERN[FraudPattern.CREDENTIAL_STUFFING], 0.05),
        (SCENARIOS_BY_PATTERN[FraudPattern.FRAUD_RING], 0.05),
        (SCENARIOS_BY_PATTERN[FraudPattern.MERCHANT_COLLUSION], 0.03),
    )
