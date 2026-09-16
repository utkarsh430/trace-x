"""The reference's two evaluation modes over a stream too long to rescan (ADR-0056 §2).

`ReferenceFeatureStore.score` and `event_time_complete_context` scan every observation for every
read, so a 100 k-event stream would cost ~10^10 steps. These classes call the same
`reference.build_context` on a **subset** of the observations that is provably equivalent:

- `build_context` reads observations only through an entity id the subject carries. Windows and
  previous observations filter by `entity_id(entity) == id`, and the profile and its §8 cap filter
  by account. Everything outside the union of those entity sets is invisible to it.
- The one cross-entity read is an outcome's verification against its transaction. A transaction
  with the outcome's id and the same account is in the account's set, and a verified outcome counts
  only then. A transaction with another account's id reads REJECTED in full and PENDING in the
  subset, and neither counts.
- An entity other than the account is read only inside its widest window, previous-observation
  lookback and approximate bucket. So its observations older than `NON_ACCOUNT_HORIZON_MS` before
  `as_of` are left out; the account's profile reads its whole lifetime, so the account set is never
  trimmed.

`tests/unit/test_parity_replay.py` checks the equivalence against the unmodified reference on
generated streams with redeliveries, conflicts, cross-account and pending outcomes, identity events,
out-of-order arrival and accounts deep enough to be capped.

`AsServedReplay` also keeps the few facts about what a real store has done that ADR-0046 §8's
declared exceptions depend on: how far each account's transactions have been folded, and the
store-wide newest write.
"""

from __future__ import annotations

import bisect
from collections.abc import Iterable, Mapping
from typing import Final

from trace_core.domain.time import EventTime
from trace_core.features.context import FeatureContext
from trace_core.features.observation import Event, ObserveReceipt
from trace_core.features.reference import ReferenceFeatureStore, build_context
from trace_core.features.semantics import (
    APPROXIMATE_BUCKET_MS,
    WINDOWS,
    Entity,
    EvaluationMode,
    Stream,
)
from trace_core.features.state_plan import PLAN

NON_ACCOUNT_HORIZON_MS: Final = (
    max(
        max(window.seconds for window in WINDOWS),
        *(lookback.seconds for lookback in PLAN.previous.values()),
    )
    * 1000
    + APPROXIMATE_BUCKET_MS
)
ACCOUNT_RAW_MS: Final = PLAN.account_raw_ms((Stream.TRANSACTION,))
"""How far behind a write the online store keeps an account's transactions raw (ADR-0046 §5, §8)."""

OrderKey = tuple[int, str, str]


def entity_ids(event: Event) -> dict[Entity, str | None]:
    return {entity: event.entity_id(entity) for entity in Entity}


class EntityIndex:
    """Observations per entity, each list in the declared total order."""

    def __init__(self) -> None:
        self._keys: dict[tuple[Entity, str], list[OrderKey]] = {}
        self._events: dict[tuple[Entity, str], list[Event]] = {}

    def add(self, event: Event) -> None:
        key = event.order_key
        for entity, entity_id in entity_ids(event).items():
            if entity_id is None:
                continue
            keys = self._keys.setdefault((entity, entity_id), [])
            at = bisect.bisect_right(keys, key)
            keys.insert(at, key)
            self._events.setdefault((entity, entity_id), []).insert(at, event)

    def visible_for(self, subject: Event) -> list[Event]:
        chosen: dict[str, Event] = {}
        for entity, entity_id in entity_ids(subject).items():
            if entity_id is None or (entity, entity_id) not in self._keys:
                continue
            keys = self._keys[(entity, entity_id)]
            start = (
                0
                if entity is Entity.ACCOUNT
                else bisect.bisect_left(keys, (subject.occurred_ms - NON_ACCOUNT_HORIZON_MS,))
            )
            for event in self._events[(entity, entity_id)][start:]:
                chosen.setdefault(event.identity, event)
        return list(chosen.values())

    def count_at(self, entity: Entity, entity_id: str, stream: Stream, occurred_ms: int) -> int:
        """Observations of `entity` on `stream` at exactly `occurred_ms`."""
        keys = self._keys.get((entity, entity_id), [])
        low = bisect.bisect_left(keys, (occurred_ms,))
        high = bisect.bisect_left(keys, (occurred_ms + 1,))
        events = self._events.get((entity, entity_id), [])
        return sum(1 for event in events[low:high] if event.stream is stream)


class AsServedReplay:
    """AS_SERVED, incrementally: observations applied in the store's order, each score read at once.

    Receipts come from an unmodified `ReferenceFeatureStore`, so dedup, conflicts and the position
    counter are the reference's own."""

    def __init__(self) -> None:
        self._store = ReferenceFeatureStore()
        self._index = EntityIndex()
        self._first: dict[str, Event] = {}
        self._newest_transaction_ref: dict[str, int] = {}
        self._oldest_transaction_ms: dict[str, int] = {}
        self._high_ref_ms: int | None = None

    @property
    def recorded(self) -> Mapping[str, Event]:
        """Every recorded identity and its first delivery."""
        return self._first

    @property
    def high_watermark_ms(self) -> int | None:
        """The newest `min(occurred, written)` of any recorded observation: the store's `hw`."""
        return self._high_ref_ms

    def fold_frontier_ms(self, account_id: str) -> int | None:
        """Account transactions dated before this have been folded out of raw history, or None."""
        newest = self._newest_transaction_ref.get(account_id)
        return None if newest is None else newest - ACCOUNT_RAW_MS

    def oldest_transaction_ms(self, account_id: str) -> int | None:
        return self._oldest_transaction_ms.get(account_id)

    def transactions_at(self, account_id: str, occurred_ms: int) -> int:
        return self._index.count_at(Entity.ACCOUNT, account_id, Stream.TRANSACTION, occurred_ms)

    def observe(self, event: Event, *, written_ms: int | None = None) -> ObserveReceipt:
        """Record like the store. `written_ms`: the store's clock at the write, when known; the
        store keys retention on `min(occurred, written)`, never on a clock ahead of event time."""
        receipt = self._store.observe(event)
        if receipt.recorded:
            self._first[event.identity] = event
            self._index.add(event)
            ref = event.occurred_ms if written_ms is None else min(event.occurred_ms, written_ms)
            self._high_ref_ms = ref if self._high_ref_ms is None else max(self._high_ref_ms, ref)
            if event.stream is Stream.TRANSACTION:
                account = event.account_id
                self._newest_transaction_ref[account] = max(
                    ref, self._newest_transaction_ref.get(account, ref)
                )
                self._oldest_transaction_ms[account] = min(
                    event.occurred_ms, self._oldest_transaction_ms.get(account, event.occurred_ms)
                )
        return receipt

    def score(
        self, event: Event, *, complete_since: EventTime | None, written_ms: int | None = None
    ) -> tuple[ObserveReceipt, FeatureContext]:
        """Record, then read with the first delivery as the current observation (ADR-0046 §1)."""
        receipt = self.observe(event, written_ms=written_ms)
        current = self._first[event.identity]
        context = build_context(
            self._index.visible_for(current),
            as_of_ms=current.occurred_ms,
            currency=current.currency,
            ids=entity_ids(current),
            current=current,
            mode=EvaluationMode.AS_SERVED,
            complete_since=complete_since,
        )
        return receipt, context


class CompleteHistory:
    """EVENT_TIME_COMPLETE over a complete history: the first delivery of each identity, in the
    order given."""

    def __init__(self, events: Iterable[Event]) -> None:
        self._first: dict[str, Event] = {}
        self._index = EntityIndex()
        for event in events:
            if event.identity not in self._first:
                self._first[event.identity] = event
                self._index.add(event)

    @property
    def observations(self) -> Mapping[str, Event]:
        return self._first

    def context(self, identity: str, *, complete_since: EventTime | None) -> FeatureContext:
        current = self._first.get(identity)
        if current is None:
            raise KeyError(f"{identity} is not in the complete history")
        return build_context(
            self._index.visible_for(current),
            as_of_ms=current.occurred_ms,
            currency=current.currency,
            ids=entity_ids(current),
            current=current,
            mode=EvaluationMode.EVENT_TIME_COMPLETE,
            complete_since=complete_since,
        )
