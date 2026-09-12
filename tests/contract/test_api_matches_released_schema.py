"""The API must not accept what the released event contract would reject.

The gateway is a producer for `tx.raw.v1`, whose schema is **released and
immutable** (docs/EVENT_CONTRACTS.md §4). If `POST /v1/transactions` accepted a
value the schema forbids, the gateway would take a transaction it cannot
publish and find out at produce time, with the caller long gone and the failure
attributed to the wrong component.

So the request model's constraints are diffed against the released schema field
by field. Two copies of a constraint drift; a test that compares them does not.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from trace_core.contracts.api.transaction import TransactionRequest

pytestmark = pytest.mark.contract

ROOT = Path(__file__).resolve().parents[2]
TX_SCHEMA = ROOT / "docs" / "contracts" / "events" / "tx.raw.v1.json"


@pytest.fixture(scope="module")
def released_payload_properties() -> dict[str, dict[str, Any]]:
    schema = json.loads(TX_SCHEMA.read_text())
    props: dict[str, dict[str, Any]] = schema["properties"]["payload"]["properties"]
    return props


@pytest.fixture(scope="module")
def request_properties() -> dict[str, dict[str, Any]]:
    props: dict[str, dict[str, Any]] = TransactionRequest.model_json_schema()["properties"]
    return props


def _unwrap(spec: dict[str, Any]) -> dict[str, Any]:
    """Optional fields render as `anyOf: [<type>, {"type": "null"}]`."""
    if "anyOf" in spec:
        for option in spec["anyOf"]:
            if option.get("type") != "null":
                return dict(option)
    return dict(spec)


def test_every_request_field_exists_in_the_released_payload(
    released_payload_properties: dict[str, dict[str, Any]],
    request_properties: dict[str, dict[str, Any]],
) -> None:
    """A field the caller can send but the event cannot carry has nowhere to go."""
    # occurred_at is an ENVELOPE field, not a payload field; it is the one
    # deliberate exception and `to_payload()` removes it.
    extra = set(request_properties) - set(released_payload_properties) - {"occurred_at"}
    assert not extra, (
        f"{sorted(extra)} are accepted by the API but are not fields of the released "
        f"tx.raw.v1 payload. The schema is immutable, so the API is what must change."
    )


def test_string_patterns_match_the_released_schema(
    released_payload_properties: dict[str, dict[str, Any]],
    request_properties: dict[str, dict[str, Any]],
) -> None:
    checked = 0
    for name, released in released_payload_properties.items():
        if "pattern" not in released or name not in request_properties:
            continue
        mine = _unwrap(request_properties[name])
        assert mine.get("pattern") == released["pattern"], (
            f"{name}: API pattern {mine.get('pattern')!r} differs from the released "
            f"schema's {released['pattern']!r}"
        )
        checked += 1
    assert checked >= 5, f"only {checked} patterns compared; the diff has gone vacuous"


def test_enum_members_match_the_released_schema(
    released_payload_properties: dict[str, dict[str, Any]],
) -> None:
    """A value the API accepts but the schema does not is unpublishable."""
    api_schema = TransactionRequest.model_json_schema()
    defs = api_schema.get("$defs", {})
    by_field = {
        "channel": "TransactionChannel",
        "entry_mode": "EntryMode",
        "authorization_outcome": "AuthorizationOutcome",
    }
    for field, def_name in by_field.items():
        released = set(released_payload_properties[field]["enum"])
        mine = set(defs[def_name]["enum"])
        assert mine == released, (
            f"{field}: API accepts {sorted(mine - released)} that the released schema "
            f"rejects, and omits {sorted(released - mine)}"
        )


def test_max_lengths_match_the_released_schema(
    released_payload_properties: dict[str, dict[str, Any]],
    request_properties: dict[str, dict[str, Any]],
) -> None:
    """The untrusted fields especially: their caps are a security control."""
    for name in ("merchant_name", "user_agent", "memo"):
        released = released_payload_properties[name]["maxLength"]
        mine = _unwrap(request_properties[name])["maxLength"]
        assert mine == released, f"{name}: API maxLength {mine} != released {released}"


def _valid() -> dict[str, Any]:
    return {
        "transaction_id": "tx_0000000001",
        "account_id": "acct_000001",
        "amount_minor": 1999,
        "currency": "GBP",
        "occurred_at": "2026-09-12T10:00:00Z",
        "merchant_id": "mrch_00001",
        "merchant_mcc": "5411",
        "merchant_country": "GB",
    }


def test_a_valid_request_produces_a_publishable_payload() -> None:
    request = TransactionRequest.model_validate(_valid())
    payload = request.to_payload()
    assert "occurred_at" not in payload, "occurred_at belongs to the envelope, not the payload"
    assert "ingested_at" not in payload, "processing time is stamped by the gateway"
    assert payload["account_id"] == "acct_000001"


def test_identifiers_that_the_released_schema_rejects_are_rejected_here() -> None:
    """The `acc_` prefix API_CONTRACTS used to document would fail at produce time."""
    for bad in ("acc_000001", "account_1", "acct_1", ""):
        with pytest.raises(ValidationError):
            TransactionRequest.model_validate({**_valid(), "account_id": bad})
