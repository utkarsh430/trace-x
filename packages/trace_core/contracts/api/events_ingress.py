"""`POST /v1/events/identity` and `/v1/events/device` request contracts.

These exist because two fraud scenarios are *defined* by non-transaction events
(docs/FRAUD_SCENARIOS.md §2): an account takeover begins with a credential
change, and credential stuffing **is** a burst of failed logins. Without them,
`IDENTITY_CHANGE` would not be a causal evidence key and two of the ten
scenarios would have no hot-path signal at all.

Both are `202 Accepted`: they update online state for later transactions rather
than producing a decision now, so there is nothing synchronous to return. As on
the transaction surface, patterns mirror the released `identity.events.v1` and
`device.events.v1` schemas, and `ingested_at` is stamped server-side.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import Field

from trace_core.contracts.api.base import JSON_SPELLED, ApiModel, JsonDatetime
from trace_core.contracts.api.transaction import (
    ACCOUNT_ID_PATTERN,
    DEVICE_ID_PATTERN,
    IP_ID_PATTERN,
)
from trace_core.domain.enums import DeviceEventType, IdentityEventType

JsonIdentityEventType = Annotated[IdentityEventType, JSON_SPELLED]
JsonDeviceEventType = Annotated[DeviceEventType, JSON_SPELLED]


class IdentityEventRequest(ApiModel):
    """A change to the account holder's identity, credentials or login state."""

    account_id: Annotated[str, Field(pattern=ACCOUNT_ID_PATTERN)]
    identity_event_type: JsonIdentityEventType
    occurred_at: JsonDatetime
    device_id: Annotated[str, Field(pattern=DEVICE_ID_PATTERN)] | None = None
    ip_id: Annotated[str, Field(pattern=IP_ID_PATTERN)] | None = None
    user_agent: Annotated[str, Field(max_length=256)] | None = None
    """Attacker-controlled (docs/SECURITY.md §5.1)."""

    def to_payload(self) -> dict[str, object]:
        data = self.model_dump(mode="json", exclude_none=True)
        data.pop("occurred_at", None)
        return data


class DeviceEventRequest(ApiModel):
    """A change in the device fingerprint an account presents."""

    device_id: Annotated[str, Field(pattern=DEVICE_ID_PATTERN)]
    account_id: Annotated[str, Field(pattern=ACCOUNT_ID_PATTERN)]
    device_event_type: JsonDeviceEventType
    occurred_at: JsonDatetime
    platform: Annotated[str, Field(max_length=32)] | None = None
    ip_id: Annotated[str, Field(pattern=IP_ID_PATTERN)] | None = None

    def to_payload(self) -> dict[str, object]:
        data = self.model_dump(mode="json", exclude_none=True)
        data.pop("occurred_at", None)
        return data


class AcceptedResponse(ApiModel):
    """What a 202 returns: enough to correlate, nothing more."""

    accepted: bool
    event_id: Annotated[str, Field(min_length=1, max_length=64)]
    request_id: Annotated[str, Field(min_length=1, max_length=64)]
