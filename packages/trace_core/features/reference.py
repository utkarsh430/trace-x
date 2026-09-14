"""A naive, obviously-correct implementation of the declared feature semantics.

ROADMAP Phase 2 requires "feature correctness against a naive reference implementation". This
is it: it holds every recorded observation in a list, in the order it recorded them, and answers
each question by scanning, with no windows to trim, no HyperLogLog, no running estimators and no
Redis.

**It is not a test double.** It is a genuine second implementation of the declarations in
`trace_core.features` (ADR-0032, ADR-0046), and it is the reason the Redis store has something to
be wrong *against*. It is not the oracle either: the literal fixtures in
`tests/conformance/feature_semantics_suite.py` are, and this implementation must agree with them
like every other.

**Two evaluation modes, one scan** (`EvaluationMode`, ADR-0046 §1):

* `ReferenceFeatureStore.score` is AS_SERVED: the scored transaction is recorded, then the read
  sees exactly the observations recorded up to and including it.
* `event_time_complete_context` is EVENT_TIME_COMPLETE: every observation, whatever order it
  arrived in, with ties at the scored transaction's millisecond broken by identity.

Both reduce to `build_context`, so the modes differ in exactly two places -- which observations
are visible, and where an INCLUDED window's upper edge falls -- and nowhere else.

It is deliberately O(n) per query and unsuitable for production. Reading it should make the
window boundary, the currency scope, the dedup rule and the tie-break obvious by inspection,
because that is what "reference" has to mean for a disagreement to be attributable.
"""

from __future__ import annotations

import datetime as dt
import functools
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Final

from trace_core.contracts.canonical import CanonicalField
from trace_core.domain.enums import AuthorizationOutcome, FeatureSource
from trace_core.domain.time import EventTime, from_millis, to_millis
from trace_core.features.context import FeatureContext, Observation, Profile, WindowState
from trace_core.features.observation import POST_DECISION_FIELDS, Event, ObserveReceipt, ServedRead
from trace_core.features.profile_math import geodesic_medoid, robust_centre
from trace_core.features.semantics import (
    ALIGNED_MINUTE_MS,
    AMOUNT_SAMPLE_SIZE,
    APPROXIMATE_BUCKET_MS,
    HABITUAL_MIN_VISITS,
    HOME_SAMPLE_SIZE,
    PROFILE_LIFETIME_GAP_S,
    WINDOWS,
    CardinalityStorage,
    CurrentObservation,
    Dimension,
    Entity,
    EvaluationMode,
    Stream,
    Window,
    WindowedAggregate,
)

_LIFETIME_GAP_MS: Final = PROFILE_LIFETIME_GAP_S * 1000
_OUTCOME_IS_POST_DECISION: Final = CanonicalField.AUTHORIZATION_OUTCOME in POST_DECISION_FIELDS


@dataclass(frozen=True, slots=True)
class ReadScope:
    """What one read is about: the instant, the scope, and the current observation."""

    as_of_ms: int
    currency: str
    current: Event | None
    """The scored transaction's recorded observation -- its first delivery -- or None for a
    read-only snapshot, which scores nothing."""
    mode: EvaluationMode

    @property
    def identity(self) -> str | None:
        return None if self.current is None else self.current.identity

    @property
    def current_key(self) -> tuple[int, str, str] | None:
        return None if self.current is None else self.current.order_key


@functools.cache
def _declared_current(entity: Entity, stream: Stream, window: Window) -> CurrentObservation:
    """What the released features declare for this window. A window no feature reads is
    computed as INCLUDED; nothing reads it, and the registry refuses a conflicting pair."""
    from trace_core.features.definitions import ONLINE_FEATURES

    for spec in ONLINE_FEATURES:
        shape = spec.semantics
        if isinstance(shape, WindowedAggregate) and (shape.entity, shape.stream, shape.window) == (
            entity,
            stream,
            window,
        ):
            return shape.current_observation
    return CurrentObservation.INCLUDED


def _in_window(
    event: Event, *, lower_ms: int, read: ReadScope, declared: CurrentObservation
) -> bool:
    """Half-open `(as_of - W, as_of]`, with the upper edge the declaration and the mode define."""
    if event.occurred_ms <= lower_ms:
        return False
    if declared is CurrentObservation.EXCLUDED:
        return event.occurred_ms < read.as_of_ms
    key = read.current_key
    if read.mode is EvaluationMode.AS_SERVED or key is None:
        # Everything visible was recorded no later than the read, so any observation at the
        # scored millisecond that the read can see arrived before it.
        return event.occurred_ms <= read.as_of_ms
    return event.order_key <= key


def _aligned(observations: Sequence[Event], *, read: ReadScope, window: Window) -> list[Event]:
    """The merchant CV's declared minute-aligned, same-currency window (ADR-0046 §2)."""
    lower_minute = (read.as_of_ms - window.seconds * 1000) // ALIGNED_MINUTE_MS
    upper_minute = read.as_of_ms // ALIGNED_MINUTE_MS
    return [
        e
        for e in observations
        if e.currency == read.currency
        and e.identity != read.identity
        and lower_minute < e.occurred_ms // ALIGNED_MINUTE_MS < upper_minute
    ]


def window_state(
    observations: Sequence[Event],
    *,
    read: ReadScope,
    entity: Entity,
    stream: Stream,
    window: Window,
) -> WindowState | None:
    from trace_core.features.state_plan import PLAN

    declared = _declared_current(entity, stream, window)
    lower_ms = read.as_of_ms - window.seconds * 1000
    members = [
        e for e in observations if _in_window(e, lower_ms=lower_ms, read=read, declared=declared)
    ]
    same_currency = [e for e in members if e.currency == read.currency]
    # The scored transaction's own outcome is decided after TRACE-X answers; it never
    # enters its own features (observation.POST_DECISION_FIELDS).
    outcomes = [
        e
        for e in members
        if e.authorization_outcome is not None
        and not (_OUTCOME_IS_POST_DECISION and e.identity == read.identity)
    ]
    contributed = bool(members)

    distinct: dict[Dimension, int] = {}
    for dimension in Dimension:
        if PLAN.storage(entity, dimension) is CardinalityStorage.APPROXIMATE:
            if declared is CurrentObservation.EXCLUDED:
                raise NotImplementedError(
                    "no estimand is declared for an approximate distinct count that excludes "
                    "the scored transaction"
                )
            # The declared estimand: five-minute buckets inclusive at both edges, the one
            # exception to strict event time (ADR-0046 §2).
            first = lower_ms // APPROXIMATE_BUCKET_MS
            last = read.as_of_ms // APPROXIMATE_BUCKET_MS
            pool = [
                e for e in observations if first <= e.occurred_ms // APPROXIMATE_BUCKET_MS <= last
            ]
        else:
            pool = members
        values = {v for e in pool if (v := e.dimension_value(dimension)) is not None}
        if values:
            distinct[dimension] = len(values)
            contributed = True

    aligned = _aligned(observations, read=read, window=window)
    current = read.current
    if (
        declared is CurrentObservation.INCLUDED
        and current is not None
        and current.currency == read.currency
        and any(e.identity == current.identity for e in observations)
    ):
        aligned.append(current)
    contributed = contributed or bool(aligned)

    if not contributed:
        return None
    return WindowState(
        count=len(members),
        amount_sum_minor=sum(e.amount_minor for e in same_currency),
        amount_sum_squares=sum(e.amount_minor * e.amount_minor for e in same_currency),
        declined_count=sum(
            1 for e in outcomes if e.authorization_outcome is AuthorizationOutcome.DECLINED
        ),
        outcome_known_count=len(outcomes),
        distinct=distinct,
        aligned_count=len(aligned),
        aligned_amount_sum_minor=sum(e.amount_minor for e in aligned),
        aligned_amount_sum_squares=sum(e.amount_minor * e.amount_minor for e in aligned),
    )


def lifetime_profile(transactions: Sequence[Event], read: ReadScope) -> Profile | None:
    """The account's lifetime strictly before `as_of`, reduced (ADR-0046 §3).

    Strictly before, in both modes: a transaction entering its own baseline would make every
    transaction look normal relative to itself, and excluding the whole millisecond keeps the
    baseline independent of arrival order at a tie.
    """
    history = sorted(
        (e for e in transactions if e.occurred_ms < read.as_of_ms and e.identity != read.identity),
        key=lambda e: e.order_key,
    )
    if not history or read.as_of_ms - history[-1].occurred_ms >= _LIFETIME_GAP_MS:
        return None
    start = 0
    for index in range(len(history) - 1, 0, -1):
        if history[index].occurred_ms - history[index - 1].occurred_ms >= _LIFETIME_GAP_MS:
            start = index
            break
    lifetime = history[start:]

    amounts = [abs(e.amount_minor) for e in lifetime if e.currency == read.currency]
    sample = amounts[-AMOUNT_SAMPLE_SIZE:]
    centre = robust_centre(sample)

    located = [
        (e.occurred_ms, e.identity, e.latitude, e.longitude)
        for e in lifetime
        if e.latitude is not None and e.longitude is not None
    ][-HOME_SAMPLE_SIZE:]
    home = geodesic_medoid(located)

    def _habitual(values: Iterable[str | None]) -> frozenset[str]:
        visits: dict[str, int] = {}
        for value in values:
            if value is not None:
                visits[value] = visits.get(value, 0) + 1
        return frozenset(k for k, n in visits.items() if n >= HABITUAL_MIN_VISITS)

    return Profile(
        first_seen_at=EventTime(from_millis(lifetime[0].occurred_ms)),
        observation_count=len(sample),
        amount_median_minor=None if centre is None else centre[0],
        amount_mad_minor=None if centre is None else centre[1],
        habitual_merchants=_habitual(e.merchant_id for e in lifetime),
        habitual_mccs=_habitual(e.merchant_mcc for e in lifetime),
        known_devices=frozenset(e.device_id for e in lifetime if e.device_id is not None),
        home_latitude=None if home is None else home[0],
        home_longitude=None if home is None else home[1],
    )


def previous_observation(
    observations: Sequence[Event], read: ReadScope, lookback: Window
) -> Observation | None:
    """The latest `(occurred_ms, identity)` strictly before `as_of`, inside the lookback."""
    lower_ms = read.as_of_ms - lookback.seconds * 1000
    earlier = [
        e
        for e in observations
        if lower_ms < e.occurred_ms < read.as_of_ms and e.identity != read.identity
    ]
    if not earlier:
        return None
    latest = max(earlier, key=lambda e: e.order_key)
    return Observation(
        occurred_at=EventTime(from_millis(latest.occurred_ms)),
        latitude=latest.latitude,
        longitude=latest.longitude,
        card_present=latest.card_present,
    )


def build_context(
    visible: Sequence[Event],
    *,
    as_of_ms: int,
    currency: str,
    ids: dict[Entity, str | None],
    current: Event | None,
    mode: EvaluationMode,
    complete_since: EventTime | None,
) -> FeatureContext:
    """Every value the feature set reads, from the observations this read may see.

    `visible` holds each identity at most once (its first delivery). For AS_SERVED it is the
    store's log up to the read's position; for EVENT_TIME_COMPLETE it is everything.
    """
    from trace_core.features.state_plan import PLAN

    read = ReadScope(as_of_ms=as_of_ms, currency=currency, current=current, mode=mode)
    windows: dict[tuple[Entity, str, Stream, str], WindowState] = {}
    for entity, entity_id in ids.items():
        if entity_id is None:
            continue
        mine = [e for e in visible if e.entity_id(entity) == entity_id]
        for stream in Stream:
            on_stream = [e for e in mine if e.stream is stream]
            for window in WINDOWS:
                state = window_state(
                    on_stream, read=read, entity=entity, stream=stream, window=window
                )
                if state is not None:
                    windows[(entity, entity_id, stream, window.label)] = state

    profiles: dict[tuple[Entity, str], Profile] = {}
    account_id = ids.get(Entity.ACCOUNT)
    if account_id is not None:
        transactions = [
            e for e in visible if e.stream is Stream.TRANSACTION and e.account_id == account_id
        ]
        if (profile := lifetime_profile(transactions, read)) is not None:
            profiles[(Entity.ACCOUNT, account_id)] = profile

    previous: dict[tuple[Entity, str, Stream], Observation] = {}
    for (entity, stream), lookback in PLAN.previous.items():
        entity_id = ids.get(entity)
        if entity_id is None:
            continue
        on_stream = [e for e in visible if e.stream is stream and e.entity_id(entity) == entity_id]
        if (observation := previous_observation(on_stream, read, lookback)) is not None:
            previous[(entity, entity_id, stream)] = observation

    return FeatureContext(
        as_of=EventTime(from_millis(as_of_ms)),
        source=FeatureSource.ONLINE_ONLY,
        windows=windows,
        profiles=profiles,
        previous=previous,
        complete_since=complete_since,
        distinct_dimensions={e: PLAN.distinct_dimensions(e) for e in Entity},
    )


def _ids(event: Event) -> dict[Entity, str | None]:
    return {entity: event.entity_id(entity) for entity in Entity}


@dataclass
class ReferenceFeatureStore:
    """Records observations in order and answers feature questions by scanning them."""

    complete_since: EventTime | None = None
    """When this store began recording, on the event-time axis.

    None -- the default -- means the store does not claim completeness for any period, and
    every absent window reads as `INSUFFICIENT_HISTORY`. Set it, and the store vouches for every
    window that began after it: an absent window is then a measured zero (ADR-0044)."""
    _log: list[Event] = field(default_factory=list, init=False)
    _recorded: dict[str, Event] = field(default_factory=dict, init=False)

    @property
    def events(self) -> tuple[Event, ...]:
        """Recorded observations in recording order, one per identity."""
        return tuple(self._log)

    def establish_epoch(self, *, at: EventTime | None = None) -> EventTime:
        """When recording began, if not already said: an existing claim is never moved earlier."""
        if self.complete_since is None:
            self.complete_since = (
                at if at is not None else EventTime(from_millis(to_millis(dt.datetime.now(dt.UTC))))
            )
        return self.complete_since

    def withdraw_completeness(self, *, resume_at: EventTime) -> None:
        """Vouch for no window that began before `resume_at`, keeping any later claim
        (ADR-0046 §5): a withdrawal never moves completeness backwards."""
        if self.complete_since is None or self.complete_since < resume_at:
            self.complete_since = resume_at

    def observe(self, event: Event) -> ObserveReceipt:
        """Record an observation unless its identity already was (ADR-0046 §1)."""
        if (first := self._recorded.get(event.identity)) is not None:
            return ObserveReceipt(
                position=len(self._log),
                recorded=False,
                conflicting=first.recorded_form() != event.recorded_form(),
            )
        self._log.append(event)
        self._recorded[event.identity] = event
        return ObserveReceipt(position=len(self._log), recorded=True)

    def observe_all(self, events: Iterable[Event]) -> None:
        for event in events:
            self.observe(event)

    def score(self, event: Event) -> ServedRead:
        """Record the scored transaction, then read with it as the current observation.

        A redelivery is read at its FIRST delivery -- that is the observation (ADR-0046 §1) --
        against the store as it is now.
        """
        receipt = self.observe(event)
        current = self._recorded[event.identity]
        context = build_context(
            self._log[: receipt.position],
            as_of_ms=current.occurred_ms,
            currency=current.currency,
            ids=_ids(current),
            current=current,
            mode=EvaluationMode.AS_SERVED,
            complete_since=self.complete_since,
        )
        return ServedRead(receipt=receipt, context=context)

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
        """A read-only read with no current observation.

        What a scoring read degrades to when the store refused to record the transaction: its
        windows then describe the store as it is, without the transaction in them, and the
        decision says so (ADR-0046 §5).
        """
        return build_context(
            self._log,
            as_of_ms=to_millis(as_of),
            currency=currency,
            ids={
                Entity.ACCOUNT: account_id,
                Entity.CARD: card_id,
                Entity.DEVICE: device_id,
                Entity.MERCHANT: merchant_id,
                Entity.IP: ip_id,
            },
            current=None,
            mode=EvaluationMode.AS_SERVED,
            complete_since=self.complete_since,
        )


def event_time_complete_context(
    events: Iterable[Event], subject: Event, *, complete_since: EventTime | None = None
) -> FeatureContext:
    """The EVENT_TIME_COMPLETE context for `subject`, from every observation in any order.

    Deliveries are deduplicated to the first per identity, in the order given -- the order
    they arrived in, which is what "first delivery" means offline as well.
    """
    first: dict[str, Event] = {}
    for event in events:
        first.setdefault(event.identity, event)
    current = first.get(subject.identity)
    anchor = subject if current is None else current
    return build_context(
        list(first.values()),
        as_of_ms=anchor.occurred_ms,
        currency=anchor.currency,
        ids=_ids(anchor),
        current=current,
        mode=EvaluationMode.EVENT_TIME_COMPLETE,
        complete_since=complete_since,
    )


__all__ = [
    "HABITUAL_MIN_VISITS",
    "Event",
    "ReadScope",
    "ReferenceFeatureStore",
    "build_context",
    "event_time_complete_context",
    "lifetime_profile",
    "previous_observation",
    "window_state",
]
