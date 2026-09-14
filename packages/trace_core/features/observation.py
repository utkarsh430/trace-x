"""What the online store records: observations, their identity, and the receipt a write returns.

**Every observation has an identity, and the identity is the dedup key everywhere** (ADR-0046
§1). A transaction's is its `transaction_id`; an identity or device event's is the `event_id`
the gateway mints at ingress. Phase 2 let the reference store fall back to a tuple of field
values when no id was supplied, so the reference -- the oracle -- could not recognise a retry,
and two implementations could disagree about whether a redelivery counted. There is no fallback
now: an observation without an identity cannot be constructed.

**The first delivery of an identity is the observation.** A later delivery under the same
identity contributes nothing, whatever it carries.

**Order is part of the meaning.** `EvaluationMode.AS_SERVED` follows the order the online store
recorded observations in -- the position an `ObserveReceipt` reports. `EVENT_TIME_COMPLETE`
follows `Event.order_key`, `(occurred_ms, identity)`.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final

from trace_core.contracts.canonical import CanonicalField, CanonicalTransaction
from trace_core.domain.enums import AuthorizationOutcome, TransactionChannel
from trace_core.domain.time import EventTime, to_millis
from trace_core.features.semantics import Dimension, Entity, Stream

if TYPE_CHECKING:  # pragma: no cover - typing only
    from trace_core.features.context import FeatureContext

POST_DECISION_FIELDS: Final = frozenset({CanonicalField.AUTHORIZATION_OUTCOME})
"""Fields a scored transaction carries that are not known when TRACE-X scores it.

TRACE-X's decision gates authorization (docs/ARCHITECTURE.md §1: the payment system sends the
transaction synchronously and receives the `RiskDecision`; ADR-0002: "an authorization decision
must return in tens of milliseconds"), and `AuthorizationOutcome` is "what the upstream
authorization system did with the transaction" -- including `REVERSED`, which can only happen
later. So a scored transaction's own outcome is post-decision information, and it **never
contributes to that transaction's own features**: `declined_ratio_1h` counts the current
observation's outcome as not yet known. Since ADR-0049 no transaction's outcome is taken from its
scoring request at all: outcomes arrive as their own dated events on `Stream.AUTHORIZATION_OUTCOME`,
and a store does not record the field from a transaction.
"""


class IdentityNamespace(StrEnum):
    """Which released stream an observation's id is unique within (plan §3 Q2).

    An id is an identity only inside its namespace. Without one, a failed-login event whose
    id happened to equal a transaction id would be recorded first and swallow the
    transaction: it would vanish from every window as a "redelivery".
    """

    IDENTITY_EVENT = "identity_event"
    TRANSACTION = "transaction"
    TRANSACTION_AUTHORIZATION = "transaction_authorization"
    """An authorization outcome is identified by its transaction id, in its own namespace, so it
    never collides with the transaction; the name sorts after `transaction`, so at one
    millisecond an outcome orders after a transaction (ADR-0049 §5)."""


NAMESPACES: Final[dict[Stream, IdentityNamespace]] = {
    Stream.TRANSACTION: IdentityNamespace.TRANSACTION,
    Stream.IDENTITY_FAILED_LOGIN: IdentityNamespace.IDENTITY_EVENT,
    Stream.IDENTITY_CHANGE: IdentityNamespace.IDENTITY_EVENT,
    Stream.AUTHORIZATION_OUTCOME: IdentityNamespace.TRANSACTION_AUTHORIZATION,
}
"""Declared per stream, never defaulted: a namespace also decides the order at one millisecond,
so a new stream must say which it belongs to."""


def namespace_of(stream: Stream) -> IdentityNamespace:
    try:
        return NAMESPACES[stream]
    except KeyError:
        raise ValueError(f"no identity namespace is declared for stream {stream}") from None


@dataclass(frozen=True, slots=True, kw_only=True)
class Event:
    """One observation, flat, with everything any feature might read.

    Deliberately not `CanonicalTransaction`: identity events are observations too, and forcing
    them into a transaction shape would misrepresent them -- the same reason the generator emits
    `identity.events.v1` rather than encoding a login as a payment (docs/FRAUD_SCENARIOS.md §2).
    """

    stream: Stream
    occurred_at: EventTime
    account_id: str
    event_id: str
    """The observation's identity (ADR-0046 §1). Required and non-empty."""
    currency: str = ""
    amount_minor: int = 0
    card_id: str | None = None
    device_id: str | None = None
    merchant_id: str | None = None
    ip_id: str | None = None
    merchant_mcc: str | None = None
    merchant_country: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    channel: TransactionChannel | None = None
    authorization_outcome: AuthorizationOutcome | None = None

    def __post_init__(self) -> None:
        if not self.event_id:
            raise ValueError(
                "an observation needs an identity: a transaction's transaction_id or an "
                "ingress-minted event_id. Without one a redelivery is indistinguishable from "
                "a new observation, and every count it touches silently double-counts."
            )

    @property
    def namespace(self) -> IdentityNamespace:
        return namespace_of(self.stream)

    @property
    def identity(self) -> str:
        """`<namespace>:<event_id>`: unique across every stream the store records."""
        return f"{self.namespace.value}:{self.event_id}"

    @property
    def dedup_key(self) -> str:
        """The identity, under the name the Phase 2 Redis store reads it by."""
        return self.identity

    @property
    def occurred_ms(self) -> int:
        """Event time floored to epoch milliseconds, the precision every comparison uses."""
        return to_millis(self.occurred_at)

    @property
    def order_key(self) -> tuple[int, str, str]:
        """`(occurred_ms, namespace, event_id)`: the declared total order (ADR-0046 §1).

        At one millisecond an identity event sorts before a transaction (the namespace names
        compare that way), and within a namespace ids compare as strings."""
        return (self.occurred_ms, self.namespace.value, self.event_id)

    @property
    def located(self) -> bool:
        return self.latitude is not None and self.longitude is not None

    def recorded_form(self) -> tuple[object, ...]:
        """What a store records of this delivery: enough to tell a plain redelivery from one
        carrying a different observation.

        Event time at the declared millisecond, and no post-decision field: a retry that only
        reports the authorization outcome learned since carries the same observation (ADR-0046 §7).
        """
        # On the outcome stream the outcome IS the observation (ADR-0049 §6), so a redelivery with
        # another outcome is a conflict there, and only there.
        skipped = (
            set()
            if self.stream is Stream.AUTHORIZATION_OUTCOME
            else {field.value for field in POST_DECISION_FIELDS}
        )
        return tuple(
            self.occurred_ms if f.name == "occurred_at" else getattr(self, f.name)
            for f in dataclasses.fields(self)
            if f.name not in skipped
        )

    def entity_id(self, entity: Entity) -> str | None:
        return {
            Entity.ACCOUNT: self.account_id,
            Entity.CARD: self.card_id,
            Entity.DEVICE: self.device_id,
            Entity.MERCHANT: self.merchant_id,
            Entity.IP: self.ip_id,
        }[entity]

    def dimension_value(self, dimension: Dimension) -> str | None:
        return {
            Dimension.MERCHANT: self.merchant_id,
            Dimension.MCC: self.merchant_mcc,
            Dimension.DEVICE: self.device_id,
            Dimension.COUNTRY: self.merchant_country,
            Dimension.ACCOUNT: self.account_id,
        }[dimension]

    @property
    def card_present(self) -> bool:
        return self.channel is TransactionChannel.CARD_PRESENT


class Verification(StrEnum):
    """Whether an authorization outcome may reach a feature (ADR-0049 §2)."""

    VERIFIED = "VERIFIED"
    """Its transaction, with the same account, is known."""
    PENDING = "PENDING"
    """Its transaction is not known yet. It reaches no feature until it is."""
    REJECTED = "REJECTED"
    """Its transaction is known with another account. It never reaches a feature."""


@dataclass(frozen=True, slots=True)
class ObserveReceipt:
    """What a store reports after it was asked to record an observation."""

    position: int
    """The store's observation counter after this call: how many observations it has recorded.

    It moves by exactly one for each newly recorded observation and never for a redelivery, so
    the state a read saw is exactly "every observation at a position up to this one". This is
    the as-served order an offline replay follows (plan §4.3). A Redis store pairs it with its
    epoch, because a store that restarts restarts its counter."""
    recorded: bool
    """False when the identity had already been recorded: a redelivery contributes nothing."""
    conflicting: bool = False
    """True when the identity had been recorded with a different observation -- another account,
    amount, time or place. The first delivery stays the observation (ADR-0046 §1), so a context a
    score returns is the first delivery's and must not be read as this payload's."""
    verification: Verification | None = None
    """For an authorization outcome: whether it may reach a feature yet, as the store stands after
    this call (ADR-0049 §6). None for every other stream."""


@dataclass(frozen=True, slots=True)
class ServedRead:
    """One atomic score-time read: the transaction recorded, then everything it may read.

    Recording first and reading in the same atomic operation is what makes the scored
    transaction part of its own INCLUDED windows (ADR-0046 §2) without a race: no other
    observation can land between the write and the read."""

    receipt: ObserveReceipt
    context: FeatureContext


def transaction_observation(transaction: CanonicalTransaction) -> Event:
    """The observation a scored transaction becomes. One mapping, shared by every implementation.

    Its own `authorization_outcome` is not recorded (ADR-0049 §3): it is post-decision, and every
    outcome reaches the store as its own dated event (`authorization_observation`)."""
    return Event(
        stream=Stream.TRANSACTION,
        occurred_at=EventTime(transaction.occurred_at),
        account_id=transaction.account_id,
        event_id=transaction.transaction_id,
        currency=transaction.currency,
        amount_minor=transaction.amount_minor,
        card_id=transaction.card_id,
        device_id=transaction.device_id,
        merchant_id=transaction.merchant_id,
        ip_id=transaction.ip_id,
        merchant_mcc=transaction.merchant_mcc,
        merchant_country=transaction.merchant_country,
        latitude=transaction.latitude,
        longitude=transaction.longitude,
        channel=transaction.channel,
        authorization_outcome=None,
    )


_OBSERVED_OUTCOMES: Final = frozenset(
    {AuthorizationOutcome.APPROVED, AuthorizationOutcome.DECLINED}
)


def authorization_observation(
    *,
    transaction_id: str,
    account_id: str,
    authorization_outcome: AuthorizationOutcome,
    decided_at: EventTime,
) -> Event:
    """The observation an authorization outcome becomes: identified by its transaction, dated when
    it was decided (ADR-0049 §5). Only approvals and declines are observations."""
    if authorization_outcome not in _OBSERVED_OUTCOMES:
        raise ValueError(
            f"{authorization_outcome} is not an observed authorization outcome (ADR-0049 §2)"
        )
    return Event(
        stream=Stream.AUTHORIZATION_OUTCOME,
        occurred_at=decided_at,
        account_id=account_id,
        event_id=transaction_id,
        authorization_outcome=authorization_outcome,
    )


__all__ = [
    "POST_DECISION_FIELDS",
    "Event",
    "IdentityNamespace",
    "ObserveReceipt",
    "ServedRead",
    "Verification",
    "authorization_observation",
    "namespace_of",
    "transaction_observation",
]
