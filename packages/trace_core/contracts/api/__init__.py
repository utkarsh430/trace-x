"""HTTP wire contracts.

`docs/API_CONTRACTS.md` §1 fixes the direction of truth for APIs: Pydantic
models -> OpenAPI -> TypeScript client. These models are therefore the source,
and `docs/contracts/openapi.yaml` is generated from them and committed. That is
the opposite of events, where JSON Schema is the source (ADR-0028) because a
schema is a cross-language contract and deriving it from one runtime's type
system would privilege that runtime.
"""

from trace_core.contracts.api.base import JSON_SPELLED, ApiModel, JsonDatetime
from trace_core.contracts.api.decision import RiskDecision, RuleReason
from trace_core.contracts.api.events_ingress import (
    AcceptedResponse,
    DeviceEventRequest,
    IdentityEventRequest,
)
from trace_core.contracts.api.problem import ErrorType, Problem
from trace_core.contracts.api.transaction import TransactionRequest

__all__ = [
    "JSON_SPELLED",
    "AcceptedResponse",
    "ApiModel",
    "DeviceEventRequest",
    "ErrorType",
    "IdentityEventRequest",
    "JsonDatetime",
    "Problem",
    "RiskDecision",
    "RuleReason",
    "TransactionRequest",
]
