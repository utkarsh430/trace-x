"""The entity universe one generation run draws from.

Built once per run from named substreams, so the population is a pure function
of `(seed, config)` and never depends on generation order.

Two distributions matter enough to be deliberate rather than uniform:

* **Merchant popularity is a power law.** Real card volume concentrates on a
  small number of merchants. Uniform choice would give every merchant a similar
  transaction count, which makes merchant-risk aggregates and MCC baselines
  meaningless -- and the merchant-collusion scenario needs an outlier to be
  visible *against* a baseline.
* **Geography is clustered.** Accounts sit in a handful of population centres
  rather than scattered over a rectangle, so distance between two accounts is
  usually small and occasionally large. Uniform geography would make every
  impossible-travel injection trivially separable.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Final

from data.generator.config import GeneratorConfig
from data.generator.rng import derive
from trace_core.domain.entities import Account, Card, Device, IpAddress, Merchant
from trace_core.domain.geo import GeoPoint
from trace_core.domain.identifiers import (
    AccountId,
    account_id,
    card_id,
    device_id,
    ip_id,
    merchant_id,
)
from trace_core.domain.time import event_time

# Population centres with rough weights. Deliberately a short, explicit list: a
# synthetic city map nobody can inspect is worse than one that is obviously a
# simplification.
CITIES: Final[tuple[tuple[str, float, float, float], ...]] = (
    ("GB", 51.5074, -0.1278, 0.34),  # London
    ("GB", 53.4808, -2.2426, 0.12),  # Manchester
    ("GB", 55.9533, -3.1883, 0.08),  # Edinburgh
    ("GB", 52.4862, -1.8904, 0.10),  # Birmingham
    ("IE", 53.3498, -6.2603, 0.06),  # Dublin
    ("FR", 48.8566, 2.3522, 0.09),  # Paris
    ("DE", 52.5200, 13.4050, 0.08),  # Berlin
    ("ES", 40.4168, -3.7038, 0.07),  # Madrid
    ("NL", 52.3676, 4.9041, 0.06),  # Amsterdam
)

# ISO 18245 merchant category codes, weighted towards everyday spend.
MCC_WEIGHTS: Final[tuple[tuple[str, float], ...]] = (
    ("5411", 0.20),  # grocery stores
    ("5812", 0.13),  # eating places
    ("5541", 0.09),  # service stations
    ("5999", 0.09),  # miscellaneous retail
    ("5691", 0.07),  # clothing
    ("4121", 0.06),  # taxicabs
    ("5732", 0.05),  # electronics
    ("7011", 0.05),  # lodging
    ("4511", 0.04),  # airlines
    ("5912", 0.05),  # drug stores
    ("7995", 0.03),  # betting -- a high-risk MCC, present so risk varies by MCC
    ("6011", 0.04),  # ATM / financial institution
    ("5967", 0.02),  # direct marketing -- high chargeback
    ("7372", 0.04),  # software
    ("5942", 0.04),  # book stores
)

KM_PER_DEGREE: Final = 111.0


@dataclass(frozen=True, slots=True)
class AccountProfile:
    """An account's habitual behaviour. This is what fraud departs from."""

    account: Account
    cards: tuple[Card, ...]
    home_devices: tuple[str, ...]
    habitual_merchants: tuple[str, ...]
    home_ips: tuple[str, ...]
    amount_mu: float
    """Mean of log(amount in minor units). Lognormal spend is the standard shape:
    most transactions small, a long tail of large ones."""
    amount_sigma: float

    @property
    def account_id(self) -> AccountId:
        return self.account.account_id


@dataclass(frozen=True, slots=True)
class Universe:
    """Everything a run draws from, built once and never mutated."""

    config: GeneratorConfig
    profiles: tuple[AccountProfile, ...]
    merchants: tuple[Merchant, ...]
    devices: tuple[Device, ...]
    ips: tuple[IpAddress, ...]
    merchant_cum_weights: tuple[float, ...]
    """Cumulative Zipf weights, precomputed so merchant choice is one
    `random.choices` call rather than a per-row computation."""

    def profile_of(self, index: int) -> AccountProfile:
        return self.profiles[index]


def _weighted_pick[T](rng_value: float, items: tuple[tuple[T, float], ...]) -> T:
    """Pick by weight from a small explicit table, given a uniform draw."""
    total = sum(weight for _, weight in items)
    target = rng_value * total
    running = 0.0
    for item, weight in items:
        running += weight
        if target <= running:
            return item
    return items[-1][0]


def build_universe(config: GeneratorConfig) -> Universe:
    """Construct the population. Pure function of `(seed, config)`."""
    seed = config.seed
    opened_floor = config.start_at - dt.timedelta(days=5 * 365)

    # --- merchants ---------------------------------------------------------
    merchants: list[Merchant] = []
    for index in range(config.merchant_count):
        rng = derive(seed, "merchant", str(index))
        country, lat, lon, _ = _weighted_pick(
            rng.random(), tuple((city, city[3]) for city in CITIES)
        )
        merchants.append(
            Merchant(
                merchant_id=merchant_id(index),
                mcc=_weighted_pick(rng.random(), MCC_WEIGHTS),
                country=country,
                location=GeoPoint(
                    latitude=round(lat + rng.gauss(0, 0.05), 6),
                    longitude=round(lon + rng.gauss(0, 0.05), 6),
                ),
                name=f"Merchant {index:05d}",
            )
        )

    # Zipf popularity: weight ~ 1 / (rank ** exponent).
    exponent = config.merchant_zipf_exponent
    cumulative: list[float] = []
    running = 0.0
    for rank in range(1, config.merchant_count + 1):
        running += 1.0 / (rank**exponent)
        cumulative.append(running)

    # --- devices and ips ---------------------------------------------------
    devices = tuple(
        Device(
            device_id=device_id(index),
            first_seen_at=event_time(opened_floor),
            platform=("ios", "android", "web", "desktop")[index % 4],
            label=f"device-{index:06d}",
        )
        for index in range(config.device_count)
    )
    ips = tuple(
        IpAddress(
            ip_id=ip_id(index),
            asn=64500 + (index % 512),
            country=CITIES[index % len(CITIES)][0],
            # A small minority are datacenter ranges: credential stuffing needs a
            # plausible origin that is not a home broadband line.
            is_datacenter=(index % 37 == 0),
            is_proxy=(index % 53 == 0),
        )
        for index in range(config.ip_count)
    )

    # --- accounts ----------------------------------------------------------
    profiles: list[AccountProfile] = []
    for index in range(config.account_count):
        rng = derive(seed, "account", str(index))
        country, lat, lon, _ = _weighted_pick(
            rng.random(), tuple((city, city[3]) for city in CITIES)
        )
        home = GeoPoint(
            latitude=round(lat + rng.gauss(0, 0.08), 6),
            longitude=round(lon + rng.gauss(0, 0.08), 6),
        )
        tenure_days = int(rng.triangular(1, 5 * 365, 900))
        account = Account(
            account_id=account_id(index),
            opened_at=event_time(config.start_at - dt.timedelta(days=tenure_days)),
            home=home,
            country=country,
            currency=config.currency,
        )
        cards = tuple(
            Card(
                card_id=card_id(index * config.cards_per_account + n),
                account_id=account.account_id,
                issued_at=account.opened_at,
                last_four=f"{rng.randrange(0, 10000):04d}",
            )
            for n in range(config.cards_per_account)
        )
        device_rng = derive(seed, "account-devices", str(index))
        home_devices = tuple(
            device_id(device_rng.randrange(config.device_count))
            for _ in range(device_rng.choice((1, 1, 1, 2, 2, 3)))
        )
        ip_rng = derive(seed, "account-ips", str(index))
        home_ips = tuple(
            ip_id(ip_rng.randrange(config.ip_count)) for _ in range(ip_rng.choice((1, 1, 2)))
        )
        merchant_rng = derive(seed, "account-merchants", str(index))
        habitual = tuple(
            merchant_id(merchant_rng.randrange(config.merchant_count))
            for _ in range(merchant_rng.randrange(4, 13))
        )
        profiles.append(
            AccountProfile(
                account=account,
                cards=cards,
                home_devices=home_devices,
                habitual_merchants=habitual,
                home_ips=home_ips,
                # ~ exp(7.4) = 1600 minor units = GBP 16, with a long tail.
                amount_mu=rng.uniform(6.9, 8.1),
                amount_sigma=rng.uniform(0.7, 1.25),
            )
        )

    return Universe(
        config=config,
        profiles=tuple(profiles),
        merchants=tuple(merchants),
        devices=devices,
        ips=ips,
        merchant_cum_weights=tuple(cumulative),
    )
