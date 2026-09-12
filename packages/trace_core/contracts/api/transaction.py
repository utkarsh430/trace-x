"""`POST /v1/transactions` request contract.

The patterns here are **not** chosen freely. `tx.raw.v1` is a released,
immutable schema (docs/EVENT_CONTRACTS.md §4) and the gateway is a producer for
it, so anything this endpoint accepts must already satisfy that contract --
otherwise the gateway would accept a transaction it cannot publish, and discover
it at produce time with the caller long gone.
`tests/contract/test_api_matches_released_schema.py` diffs the two.

**`ingested_at` is deliberately absent.** It is processing time: when *we*
observed the event. A client that could set it would be setting our lag metric,
and event-time/processing-time confusion is the single most damaging shortcut
available in this system (ADR-0026). The gateway stamps it. Because the model
forbids extra fields, a client that sends one is told so rather than having it
silently ignored.
"""

from __future__ import annotations

from typing import Annotated, Final

from pydantic import Field

from trace_core.contracts.api.base import JSON_SPELLED, ApiModel, JsonDatetime
from trace_core.domain.enums import (
    AuthorizationOutcome,
    EntryMode,
    TransactionChannel,
)

# Mirrors the released tx.raw.v1 patterns. Diffed against the schema by a
# contract test, so the two cannot drift.
ACCOUNT_ID_PATTERN: Final = r"^acct_\d{6,}$"
CARD_ID_PATTERN: Final = r"^card_\d{6,}$"
DEVICE_ID_PATTERN: Final = r"^dev_\d{6,}$"
MERCHANT_ID_PATTERN: Final = r"^mrch_\d{5,}$"
IP_ID_PATTERN: Final = r"^ip_\d{5,}$"

# Enums stay CLOSED -- an unknown value is a 422, never a default
# (docs/API_CONTRACTS.md §6.6). Only the spelling is relaxed, so `"ATM"` parses
# to the member while `"Z"` is still rejected.
JsonChannel = Annotated[TransactionChannel, JSON_SPELLED]
JsonEntryMode = Annotated[EntryMode, JSON_SPELLED]
JsonAuthorizationOutcome = Annotated[AuthorizationOutcome, JSON_SPELLED]

AMOUNT_MINOR_MIN: Final = -100_000_000_000
AMOUNT_MINOR_MAX: Final = 100_000_000_000

MAX_CLOCK_SKEW_FUTURE_S: Final = 24 * 3600
"""`occurred_at` more than 24 h ahead is rejected as clock skew
(docs/EVENT_CONTRACTS.md §6.3). Checked in the handler, not here: it needs the
current time, and a model that reads the wall clock is not reproducible."""

MAX_BACKDATE_S: Final = 90 * 24 * 3600
"""More than 90 days old is **accepted and flagged**, never rejected -- a replay
of historical data is a legitimate operation, and a silently dropped old event
is indistinguishable from a bug."""


class TransactionRequest(ApiModel):
    """One transaction presented for synchronous scoring."""

    transaction_id: Annotated[str, Field(min_length=1, max_length=64)]
    account_id: Annotated[str, Field(pattern=ACCOUNT_ID_PATTERN)]
    amount_minor: int
    """Integer minor units, signed -- a refund is negative. Never a float
    (CLAUDE.md §6); `ApiModel` is strict, so `19.99` and `"1999"` are both
    rejected rather than coerced."""
    currency: Annotated[str, Field(pattern=r"^[A-Z]{3}$")]
    occurred_at: JsonDatetime
    """EVENT time, supplied by the caller. Processing time is stamped by the
    gateway and is not part of this contract."""

    card_id: Annotated[str, Field(pattern=CARD_ID_PATTERN)] | None = None
    device_id: Annotated[str, Field(pattern=DEVICE_ID_PATTERN)] | None = None
    merchant_id: Annotated[str, Field(pattern=MERCHANT_ID_PATTERN)] | None = None
    ip_id: Annotated[str, Field(pattern=IP_ID_PATTERN)] | None = None
    channel: JsonChannel | None = None
    entry_mode: JsonEntryMode | None = None
    merchant_mcc: Annotated[str, Field(pattern=r"^\d{4}$")] | None = None
    merchant_country: Annotated[str, Field(pattern=r"^[A-Z]{2}$")] | None = None
    latitude: Annotated[float, Field(ge=-90, le=90), JSON_SPELLED] | None = None
    longitude: Annotated[float, Field(ge=-180, le=180), JSON_SPELLED] | None = None
    authorization_outcome: JsonAuthorizationOutcome | None = None

    # --- attacker-controlled (docs/SECURITY.md §5.1) ------------------------
    # Length-capped here, stamped `trust_tier=UNTRUSTED` at ingestion, and never
    # un-stamped. They reach a model only inside an `<untrusted_data>` fence
    # (Phase 7); nothing on the hot path interprets them.
    merchant_name: Annotated[str, Field(max_length=128)] | None = None
    user_agent: Annotated[str, Field(max_length=256)] | None = None
    memo: Annotated[str, Field(max_length=256)] | None = None

    def to_payload(self) -> dict[str, object]:
        """The `tx.raw.v1` payload for this request.

        `None` fields are omitted rather than emitted as JSON null: the released
        schema marks most of them required, and a field the caller did not supply
        is absent, not null.
        """
        data = self.model_dump(mode="json", exclude_none=True)
        # occurred_at is an ENVELOPE field on tx.raw.v1, not a payload field.
        data.pop("occurred_at", None)
        return data
