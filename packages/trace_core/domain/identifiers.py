"""Entity identifiers and UUIDv7 generation.

Ids are `NewType`s over `str`: they cost nothing at runtime, and mypy refuses to
pass a `MerchantId` where an `AccountId` belongs. In a system whose graph tier
links accounts, devices, cards, IPs and merchants, that mix-up is easy to make
and produces plausible-looking nonsense rather than a crash.

`uuid7` accepts an explicit clock and RNG so the generator can produce a
byte-identical dataset from a seed (ADR-0027). Reproducibility is a Phase 1 exit
condition; an `event_id` sourced from a non-reproducible entropy pool would make
the dataset digest move on every run.
"""

from __future__ import annotations

import random
import secrets
from typing import Final, NewType
from uuid import UUID

AccountId = NewType("AccountId", str)
CardId = NewType("CardId", str)
DeviceId = NewType("DeviceId", str)
MerchantId = NewType("MerchantId", str)
IpId = NewType("IpId", str)
TransactionId = NewType("TransactionId", str)
InvestigationId = NewType("InvestigationId", str)
CaseId = NewType("CaseId", str)
EvidenceId = NewType("EvidenceId", str)

# Synthetic identifier formats.
#
# `ACCOUNT_FORMAT` is not cosmetic: `trace_core.observability.redaction.ACCOUNT`
# matches `acct[_-]?\d{6,}`, so account ids minted here are recognised and
# redacted by the logging pipeline. A format change that stopped matching would
# leak identifiers into logs, so a test pins the two together.
ACCOUNT_FORMAT: Final = "acct_{:06d}"
CARD_FORMAT: Final = "card_{:06d}"
DEVICE_FORMAT: Final = "dev_{:06d}"
MERCHANT_FORMAT: Final = "mrch_{:05d}"
IP_FORMAT: Final = "ip_{:05d}"

_UUID7_VERSION: Final = 0x7
_UUID7_VARIANT: Final = 0b10
_RAND_BITS: Final = 74  # 12 bits rand_a + 62 bits rand_b
_MAX_MILLIS: Final = (1 << 48) - 1


def account_id(index: int) -> AccountId:
    return AccountId(ACCOUNT_FORMAT.format(index))


def card_id(index: int) -> CardId:
    return CardId(CARD_FORMAT.format(index))


def device_id(index: int) -> DeviceId:
    return DeviceId(DEVICE_FORMAT.format(index))


def merchant_id(index: int) -> MerchantId:
    return MerchantId(MERCHANT_FORMAT.format(index))


def ip_id(index: int) -> IpId:
    return IpId(IP_FORMAT.format(index))


def uuid7(*, millis: int, rng: random.Random | None = None) -> UUID:
    """A UUIDv7: 48-bit big-endian Unix-millisecond prefix, then randomness.

    Time-ordered, which is why `docs/EVENT_CONTRACTS.md` §2 chose it as
    `event_id`: it is the deduplication key in both Redis and Spark, and a
    time-sortable key keeps index locality sane at Delta scale.

    Layout (RFC 9562 §5.7):
        bits 127..80  unix_ts_ms (48)
        bits  79..76  version = 7
        bits  75..64  rand_a (12)
        bits  63..62  variant = 0b10
        bits  61..0   rand_b (62)

    `millis` is required rather than defaulted to the wall clock: every caller
    in this system already has an authoritative timestamp, and a hidden
    `time.time()` would make generated datasets irreproducible.
    """
    if not 0 <= millis <= _MAX_MILLIS:
        raise ValueError(f"millis must fit in 48 bits, got {millis}")
    # `secrets` when unseeded; a caller-supplied Random when the run must
    # reproduce. Non-cryptographic randomness is the *requirement* there, not a
    # weakness: a seeded stream is what makes the dataset digest stable.
    entropy = secrets.randbits(_RAND_BITS) if rng is None else rng.getrandbits(_RAND_BITS)
    rand_a = (entropy >> 62) & 0xFFF
    rand_b = entropy & ((1 << 62) - 1)
    value = (
        (millis << 80) | (_UUID7_VERSION << 76) | (rand_a << 64) | (_UUID7_VARIANT << 62) | rand_b
    )
    return UUID(int=value)


def uuid7_millis(value: UUID) -> int:
    """Recover the millisecond timestamp a UUIDv7 was minted with."""
    if value.version != 7:
        raise ValueError(f"not a UUIDv7: version={value.version}")
    return value.int >> 80
