"""Population entities, and the two invariants that are security controls.

`Card.last_four` and `Merchant.mcc` are validated in `__post_init__` rather
than trusted, because SECURITY.md §10 says a full PAN must never be stored and a
malformed MCC would silently break the MCC-baseline features the merchant
scenarios depend on.
"""

from __future__ import annotations

import datetime as dt

import pytest

from trace_core.domain.entities import Account, Card, Device, IpAddress, Merchant, Population
from trace_core.domain.enums import TrustTier
from trace_core.domain.geo import GeoPoint
from trace_core.domain.identifiers import account_id, card_id, device_id, ip_id, merchant_id
from trace_core.domain.time import UTC, event_time

pytestmark = pytest.mark.unit

WHEN = event_time(dt.datetime(2026, 1, 1, tzinfo=UTC))
LONDON = GeoPoint(51.5074, -0.1278)


def test_account_is_frozen() -> None:
    acct = Account(account_id(1), WHEN, LONDON, "GB", "GBP")
    with pytest.raises(AttributeError):
        acct.country = "US"  # type: ignore[misc]


@pytest.mark.parametrize(
    "bad", ["4111111111111111", "12345", "123", "abcd", "", "12a4"], ids=lambda s: s or "empty"
)
def test_card_refuses_anything_that_is_not_exactly_four_digits(bad: str) -> None:
    """A full PAN must never reach a domain object (SECURITY.md §10)."""
    with pytest.raises(ValueError, match="last_four"):
        Card(card_id(1), account_id(1), WHEN, bad)


def test_card_accepts_a_real_last_four() -> None:
    assert Card(card_id(1), account_id(1), WHEN, "4242").last_four == "4242"


@pytest.mark.parametrize("bad", ["59", "59999", "abcd", ""])
def test_merchant_requires_a_four_digit_mcc(bad: str) -> None:
    with pytest.raises(ValueError, match="mcc"):
        Merchant(merchant_id(1), bad, "GB", LONDON)


def test_attacker_controlled_strings_default_to_untrusted() -> None:
    """SECURITY.md §5.1: merchant names and device labels are the injection surface.

    Defaulting to UNTRUSTED means forgetting to set the tier fails safe. The
    opposite default would mean one missed call site silently promotes
    attacker-controlled text to trusted.
    """
    assert Merchant(merchant_id(1), "5812", "GB", LONDON).name_trust is TrustTier.UNTRUSTED
    assert Device(device_id(1), WHEN, "ios").label_trust is TrustTier.UNTRUSTED


def test_ip_entity_carries_no_literal_address() -> None:
    """Holding the address here would invite it into a log line, which redaction
    would then have to catch. Not carrying it is the stronger control."""
    ip = IpAddress(ip_id(1), asn=64500, country="GB")
    assert not any(
        isinstance(getattr(ip, f), str) and "." in getattr(ip, f) for f in ("ip_id", "country")
    )


def test_empty_population_is_valid() -> None:
    assert Population().accounts == ()
