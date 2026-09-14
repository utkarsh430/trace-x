"""`LPC-5` §4.6: whether each released feature can be computed for a TX row.

`avail:<feature>` is the state -- AVAILABLE, INSUFFICIENT_HISTORY or UNAVAILABLE -- of the feature
as `trace_core.features.reference` computes it in EVENT_TIME_COMPLETE mode with
`complete_since = start_at`, over the four streams mapped as the gateway maps them (ADR-0046 §1 and
§4; ADR-0049 §5).

**The mapping is the ingress's.**
- A TX row becomes the canonical transaction the generator adapter yields, every field covered,
  and then `transaction_observation`, the one mapping every store uses.
- An identity event feeds the stream `identity_stream` declares for its type, or none. Device
  events feed none.
- An outcome row becomes `authorization_observation`, dated at its millisecond as the ingress
  dates it.
- The first delivery of each identity is the observation.

**Each row is given only what its context can read.** `reference.event_time_complete_context` scans
the whole history for every read: quadratic, and out of reach at acceptance scale. Each row gets
instead:
- every observation of its account up to the end of its sketch bucket, because windows, the profile
  and the previous observation all end at the row;
- the observations of its card, device, IP and merchant from the start of the sketch bucket holding
  the widest window's start to the end of the row's, because the approximate distinct counts read
  whole buckets at both edges (ADR-0046 §2);
- the transactions its account's outcomes name, wherever they are, because verification reads them.
`reference.build_context` then builds the context from those, unchanged, and ignores whatever it
does not read. A test compares every feature against the whole-history context.
"""

from __future__ import annotations

import datetime as dt
from bisect import bisect_left
from collections.abc import Mapping, Sequence
from typing import Final

from data.adapters.generator_adapter import FULL_COVERAGE, SOURCE_DATASET
from data.generator.lpc5 import declaration as d
from data.generator.lpc5.attributes import AvailabilityProvider
from data.generator.lpc5.frame import Frame, OutRow, SideRow, TxRow
from trace_core.contracts.canonical import CanonicalTransaction
from trace_core.domain.enums import AuthorizationOutcome, EntryMode, TransactionChannel
from trace_core.domain.time import event_time, from_millis
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.observation import (
    Event,
    authorization_observation,
    transaction_observation,
)
from trace_core.features.reference import build_context
from trace_core.features.semantics import (
    APPROXIMATE_BUCKET_MS,
    WINDOWS,
    Entity,
    EvaluationMode,
    Stream,
    identity_stream,
)

_WIDEST_MS: Final = max(window.seconds for window in WINDOWS) * 1000


def _at(text: str) -> dt.datetime:
    return dt.datetime.fromisoformat(text.replace("Z", "+00:00"))


def canonical(tx: TxRow) -> CanonicalTransaction:
    """A TX row as the generator adapter maps its event: every field covered."""
    if tx.currency is None:
        raise ValueError(
            f"transaction {tx.transaction_id} has no currency; tx.raw.v1 requires one, so the row "
            f"cannot be the transaction the gateway would receive"
        )
    occurred = _at(tx.envelope.occurred_at)
    return CanonicalTransaction(
        source_dataset=SOURCE_DATASET,
        source_row_id=tx.envelope.event_id or tx.transaction_id,
        field_coverage=FULL_COVERAGE,
        transaction_id=tx.transaction_id,
        account_id=tx.account,
        amount_minor=tx.amount,
        currency=tx.currency,
        occurred_at=occurred,
        ingested_at=_at(tx.envelope.ingested_at) if tx.envelope.ingested_at else occurred,
        card_id=tx.card,
        device_id=tx.device,
        merchant_id=tx.merchant,
        ip_id=tx.ip,
        channel=TransactionChannel(tx.channel) if tx.channel else None,
        entry_mode=EntryMode(tx.entry_mode) if tx.entry_mode else None,
        merchant_mcc=tx.mcc,
        merchant_country=tx.country,
        latitude=tx.latitude,
        longitude=tx.longitude,
        user_agent=tx.user_agent,
        memo=tx.memo,
        authorization_outcome=AuthorizationOutcome(tx.outcome_field) if tx.outcome_field else None,
    )


def identity_observation(side: SideRow) -> Event | None:
    """The observation an identity event becomes at the ingress, or None when it feeds no stream."""
    stream = identity_stream(side.event_type)
    if stream is None:
        return None
    if not side.envelope.event_id:
        raise ValueError(
            f"the identity event at order {side.order} has no event id, so it has no identity and "
            f"a redelivery could not be told from a new observation (ADR-0046 §1)"
        )
    return Event(
        stream=stream,
        occurred_at=event_time(_at(side.envelope.occurred_at)),
        account_id=side.account,
        device_id=side.device,
        ip_id=side.ip,
        event_id=side.envelope.event_id,
    )


def outcome_observation(out: OutRow) -> Event:
    """The observation an outcome row becomes at the ingress, dated at its millisecond."""
    return authorization_observation(
        transaction_id=out.transaction_id,
        account_id=out.account,
        authorization_outcome=AuthorizationOutcome(out.authorization_outcome),
        decided_at=event_time(from_millis(out.t)),
    )


def observations(
    frame: Frame,
) -> tuple[list[CanonicalTransaction], list[Event], dict[str, int]]:
    """The TX rows as canonical transactions; every observation once, first delivery, in frame
    order; and each identity's position in that list."""
    subjects = [canonical(tx) for tx in frame.tx]
    ordered: list[tuple[int, Event]] = [
        (tx.order, transaction_observation(subject))
        for tx, subject in zip(frame.tx, subjects, strict=True)
    ]
    ordered += [
        (row.order, event)
        for row in frame.ident
        if (event := identity_observation(row)) is not None
    ]
    ordered += [(row.order, outcome_observation(row)) for row in frame.out]
    ordered.sort(key=lambda item: item[0])
    events: list[Event] = []
    position: dict[str, int] = {}
    for _, event in ordered:
        if event.identity not in position:
            position[event.identity] = len(events)
            events.append(event)
    return subjects, events, position


class Index:
    """Each entity's observations by event time, and each transaction by id."""

    def __init__(self, events: Sequence[Event]) -> None:
        self.events = events
        self.transactions: dict[str, int] = {}
        timed: dict[tuple[Entity, str], list[tuple[int, int]]] = {}
        for i, event in enumerate(events):
            if event.stream is Stream.TRANSACTION:
                self.transactions.setdefault(event.event_id, i)
            for entity in Entity:
                value = event.entity_id(entity)
                if value is not None:
                    timed.setdefault((entity, value), []).append((event.occurred_ms, i))
        self.by_entity: dict[tuple[Entity, str], tuple[list[int], list[int]]] = {}
        for key, items in timed.items():
            items.sort()
            self.by_entity[key] = ([t for t, _ in items], [i for _, i in items])

    def visible(self, current_index: int) -> list[Event]:
        """What the context of the observation at `current_index` can read, in frame order."""
        current = self.events[current_index]
        upper = (current.occurred_ms // APPROXIMATE_BUCKET_MS + 1) * APPROXIMATE_BUCKET_MS
        lower = (current.occurred_ms - _WIDEST_MS) // APPROXIMATE_BUCKET_MS * APPROXIMATE_BUCKET_MS
        chosen: set[int] = {current_index}
        for entity in Entity:
            value = current.entity_id(entity)
            if value is None:
                continue
            times, indices = self.by_entity.get((entity, value), ([], []))
            start = 0 if entity is Entity.ACCOUNT else bisect_left(times, lower)
            chosen.update(indices[start : bisect_left(times, upper)])
        for i in list(chosen):
            event = self.events[i]
            if event.stream is Stream.AUTHORIZATION_OUTCOME:
                named = self.transactions.get(event.event_id)
                if named is not None:
                    chosen.add(named)
        return [self.events[i] for i in sorted(chosen)]


def reference_availability(start_ms: int) -> AvailabilityProvider:
    """The §4.6 provider for a generation that starts at `start_ms`."""
    complete_since = event_time(from_millis(start_ms))

    def provide(frame: Frame) -> Mapping[str, Sequence[str]]:
        subjects, events, position = observations(frame)
        index = Index(events)
        values: dict[str, list[str]] = {feature: [] for feature in d.RELEASED_FEATURES}
        for subject in subjects:
            current_index = position[transaction_observation(subject).identity]
            current = events[current_index]
            context = build_context(
                index.visible(current_index),
                as_of_ms=current.occurred_ms,
                currency=current.currency,
                ids={entity: current.entity_id(entity) for entity in Entity},
                current=current,
                mode=EvaluationMode.EVENT_TIME_COMPLETE,
                complete_since=complete_since,
            )
            evaluated = ONLINE_FEATURES.evaluate_all(subject, context)
            for feature in d.RELEASED_FEATURES:
                values[feature].append(evaluated[feature].state.value)
        return values

    return provide


__all__ = [
    "Index",
    "canonical",
    "identity_observation",
    "observations",
    "outcome_observation",
    "reference_availability",
]
