"""Population entities: the nouns the generator instantiates and the graph links.

Frozen dataclasses, not Pydantic models: these are internal domain objects, not
wire contracts. Pydantic is reserved for things that cross a boundary
(CLAUDE.md §6), and paying validation cost per object at 50k rows/second for a
value nobody outside the process sees would be the wrong trade.

Only entities Phase 1 actually populates live here. `Case` and `Investigation`
are deliberately absent: they are persisted records whose shape is decided when
they are first written (Phases 2 and 5). The state machines in
`domain.state_machines` operate on states, not on entity structs, so nothing
here is needed to define them.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from trace_core.domain.enums import TrustTier
from trace_core.domain.geo import GeoPoint
from trace_core.domain.identifiers import (
    AccountId,
    CardId,
    DeviceId,
    IpId,
    MerchantId,
)
from trace_core.domain.time import EventTime


@dataclass(frozen=True, slots=True)
class Account:
    """A customer account. Synthetic throughout — no real PII exists (SECURITY.md §10)."""

    account_id: AccountId
    opened_at: EventTime
    home: GeoPoint
    country: str
    currency: str

    @property
    def is_synthetic(self) -> bool:
        """Always true. Present so a future real-data path cannot pretend otherwise."""
        return True


@dataclass(frozen=True, slots=True)
class Card:
    """A payment instrument. Only a token and last four are ever retained."""

    card_id: CardId
    account_id: AccountId
    issued_at: EventTime
    last_four: str

    def __post_init__(self) -> None:
        # SECURITY.md §10: "Card numbers are never stored beyond a token and
        # last-four." Enforced here so no code path can attach a PAN.
        if len(self.last_four) != 4 or not self.last_four.isdigit():
            raise ValueError(
                f"last_four must be exactly four digits, got {self.last_four!r}; "
                f"a full PAN must never reach a domain object"
            )


@dataclass(frozen=True, slots=True)
class Device:
    """A device fingerprint. Shared devices are the device-farm and ring signal."""

    device_id: DeviceId
    first_seen_at: EventTime
    platform: str
    label: str = ""
    label_trust: TrustTier = TrustTier.UNTRUSTED
    """`label` is client-supplied and therefore attacker-controlled (SECURITY.md §5.1)."""


@dataclass(frozen=True, slots=True)
class Merchant:
    """A merchant with an MCC. `name` is attacker-controlled at registration."""

    merchant_id: MerchantId
    mcc: str
    country: str
    location: GeoPoint
    name: str = ""
    name_trust: TrustTier = TrustTier.UNTRUSTED
    """Merchant names are the primary prompt-injection surface (SECURITY.md §T1)."""

    def __post_init__(self) -> None:
        if len(self.mcc) != 4 or not self.mcc.isdigit():
            raise ValueError(f"mcc must be a four-digit ISO 18245 code, got {self.mcc!r}")


@dataclass(frozen=True, slots=True)
class IpAddress:
    """An IP as an opaque domain entity.

    The literal address is not carried: `trace_core.observability.redaction`
    redacts IPs from logs, and holding one here invites it into a log line. The
    generator keeps the mapping it needs privately.
    """

    ip_id: IpId
    asn: int
    country: str
    is_datacenter: bool = False
    is_proxy: bool = False


@dataclass(frozen=True, slots=True)
class Population:
    """The full entity universe one generation run draws from."""

    accounts: tuple[Account, ...] = field(default_factory=tuple)
    cards: tuple[Card, ...] = field(default_factory=tuple)
    devices: tuple[Device, ...] = field(default_factory=tuple)
    merchants: tuple[Merchant, ...] = field(default_factory=tuple)
    ips: tuple[IpAddress, ...] = field(default_factory=tuple)
