"""A naive, obviously-correct `FeatureContext` builder.

ROADMAP Phase 2 requires "feature correctness against a naive reference
implementation". This is it: it holds every observation in a list and answers
each question by scanning, with no windows to trim, no HyperLogLog, no running
estimators, and no Redis.

**It is not a test double.** It is a genuine second implementation of the same
declared semantics, and it is the reason the Redis store has something to be
wrong *against*. A conformance suite (`tests/conformance/feature_semantics_suite.py`)
runs both and requires them to agree; Phase 3 adds Spark as a third
implementation of the same suite, which is how feature parity gets verified
without anyone redefining what a feature means.

It is deliberately O(n) per query and unsuitable for production. Reading it
should make the window boundary, the currency partition and the dedup rule
obvious by inspection, because that is what "reference" has to mean for a
disagreement to be attributable.
"""

from __future__ import annotations

import statistics
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Final

from trace_core.domain.enums import AuthorizationOutcome, FeatureSource, TransactionChannel
from trace_core.domain.time import EventTime
from trace_core.features.context import (
    MIN_OBSERVATIONS_FOR_ROBUST_Z,
    FeatureContext,
    Observation,
    Profile,
    WindowState,
)
from trace_core.features.semantics import WINDOWS, Dimension, Entity, Stream

HABITUAL_MIN_VISITS: Final = 3
"""Visits before a merchant or category counts as habitual for an account.

A single visit is not a habit, and treating it as one would make
`merchant_is_habitual` true for exactly the merchants a takeover just used.
"""


@dataclass(frozen=True, slots=True)
class Event:
    """One observation, flat, with everything any feature might read.

    Deliberately not `CanonicalTransaction`: identity events are observations
    too, and forcing them into a transaction shape would misrepresent them --
    the same reason the generator emits `identity.events.v1` rather than
    encoding a login as a payment (docs/FRAUD_SCENARIOS.md §2).
    """

    stream: Stream
    occurred_at: EventTime
    account_id: str
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


@dataclass
class ReferenceFeatureStore:
    """Accumulates events and answers feature questions by scanning them."""

    events: list[Event] = field(default_factory=list)

    def observe(self, event: Event) -> None:
        self.events.append(event)

    def observe_all(self, events: Iterable[Event]) -> None:
        for event in events:
            self.observe(event)

    # -- windows ------------------------------------------------------------

    def _in_window(
        self, entity: Entity, entity_id: str, stream: Stream, as_of: EventTime, seconds: int
    ) -> list[Event]:
        """Half-open `(as_of - seconds, as_of]`, matching `Window`'s contract.

        Written as an explicit scan so the boundary is readable: an observation
        exactly `seconds` old is OUTSIDE, and one at `as_of` is inside. Both
        implementations must agree here, and a one-observation disagreement at
        the edge is exactly the kind of drift nobody manages to attribute.
        """
        lower = as_of.timestamp() - seconds
        return [
            e
            for e in self.events
            if e.stream is stream
            and e.entity_id(entity) == entity_id
            and lower < e.occurred_at.timestamp() <= as_of.timestamp()
        ]

    def _window_state(self, events: Sequence[Event], currency: str) -> WindowState:
        # Amount aggregates are partitioned by currency; counts are not. A
        # transaction in another currency still happened, and hiding it from the
        # velocity count would understate exactly the burst we are looking for.
        same_currency = [e for e in events if e.currency == currency]
        outcomes = [e for e in events if e.authorization_outcome is not None]
        distinct: dict[Dimension, int] = {}
        for dimension in Dimension:
            values = {v for e in events if (v := e.dimension_value(dimension)) is not None}
            if values:
                distinct[dimension] = len(values)
        return WindowState(
            count=len(events),
            amount_sum_minor=sum(e.amount_minor for e in same_currency),
            amount_sum_squares=sum(e.amount_minor * e.amount_minor for e in same_currency),
            declined_count=sum(
                1 for e in outcomes if e.authorization_outcome is AuthorizationOutcome.DECLINED
            ),
            outcome_known_count=len(outcomes),
            distinct=distinct,
        )

    # -- profiles -----------------------------------------------------------

    def _profile(self, account_id: str, as_of: EventTime, currency: str) -> Profile | None:
        """Everything known about an account STRICTLY BEFORE `as_of`.

        Strictly before, because a transaction may not contribute to the profile
        it is scored against -- that is leakage, and it would make every
        transaction look normal relative to itself.
        """
        history = [
            e
            for e in self.events
            if e.stream is Stream.TRANSACTION
            and e.account_id == account_id
            and e.occurred_at.timestamp() < as_of.timestamp()
        ]
        if not history:
            return None
        amounts = [abs(e.amount_minor) for e in history if e.currency == currency]
        median = mad = None
        if len(amounts) >= MIN_OBSERVATIONS_FOR_ROBUST_Z:
            median = float(statistics.median(amounts))
            mad = float(statistics.median([abs(a - median) for a in amounts]))

        def _habitual(values: list[str | None]) -> frozenset[str]:
            counts: dict[str, int] = {}
            for value in values:
                if value is not None:
                    counts[value] = counts.get(value, 0) + 1
            return frozenset(k for k, n in counts.items() if n >= HABITUAL_MIN_VISITS)

        located = [e for e in history if e.latitude is not None and e.longitude is not None]
        return Profile(
            first_seen_at=min(e.occurred_at for e in history),
            observation_count=len(amounts),
            amount_median_minor=median,
            amount_mad_minor=mad,
            habitual_merchants=_habitual([e.merchant_id for e in history]),
            habitual_mccs=_habitual([e.merchant_mcc for e in history]),
            known_devices=frozenset(e.device_id for e in history if e.device_id is not None),
            home_latitude=(
                statistics.median([e.latitude for e in located if e.latitude is not None])
                if located
                else None
            ),
            home_longitude=(
                statistics.median([e.longitude for e in located if e.longitude is not None])
                if located
                else None
            ),
        )

    # -- previous observation ----------------------------------------------

    def _previous(
        self, entity: Entity, entity_id: str, stream: Stream, as_of: EventTime
    ) -> Observation | None:
        earlier = [
            e
            for e in self.events
            if e.stream is stream
            and e.entity_id(entity) == entity_id
            and e.occurred_at.timestamp() < as_of.timestamp()
        ]
        if not earlier:
            return None
        latest = max(earlier, key=lambda e: e.occurred_at.timestamp())
        return Observation(
            occurred_at=latest.occurred_at,
            latitude=latest.latitude,
            longitude=latest.longitude,
            card_present=latest.card_present,
        )

    # -- the snapshot -------------------------------------------------------

    def snapshot(
        self,
        *,
        as_of: EventTime,
        account_id: str,
        currency: str,
        card_id: str | None = None,
        device_id: str | None = None,
        merchant_id: str | None = None,
        ip_id: str | None = None,
    ) -> FeatureContext:
        """Read everything the feature set needs for one transaction.

        Mirrors what the Redis store reads in a single pipeline, so the two are
        asked the same questions in the same order.
        """
        ids: dict[Entity, str | None] = {
            Entity.ACCOUNT: account_id,
            Entity.CARD: card_id,
            Entity.DEVICE: device_id,
            Entity.MERCHANT: merchant_id,
            Entity.IP: ip_id,
        }
        windows: dict[tuple[Entity, str, Stream, str], WindowState] = {}
        for entity, entity_id in ids.items():
            if entity_id is None:
                continue
            for stream in Stream:
                for window in WINDOWS:
                    events = self._in_window(entity, entity_id, stream, as_of, window.seconds)
                    if events:
                        windows[(entity, entity_id, stream, window.label)] = self._window_state(
                            events, currency
                        )

        profiles: dict[tuple[Entity, str], Profile] = {}
        if (profile := self._profile(account_id, as_of, currency)) is not None:
            profiles[(Entity.ACCOUNT, account_id)] = profile

        previous: dict[tuple[Entity, str, Stream], Observation] = {}
        for stream in Stream:
            if (
                observation := self._previous(Entity.ACCOUNT, account_id, stream, as_of)
            ) is not None:
                previous[(Entity.ACCOUNT, account_id, stream)] = observation

        return FeatureContext(
            as_of=as_of,
            source=FeatureSource.ONLINE_ONLY,
            windows=windows,
            profiles=profiles,
            previous=previous,
        )
