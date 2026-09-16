"""Implementation parity and arrival skew over a run's collected inputs (ADR-0056 §2-§4).

Pure: no Spark, no HTTP. The inputs are the driver's posts, the scored deliveries from Bronze, the
complete history from Silver at Gold's pins, and Gold's per-transaction context rows. Every step
that could make a comparison attributable to the wrong input fails closed:
- `link` pairs each scored request with its scored delivery, and requires the gateway's sequence
  numbers to follow the driver's order;
- `compare_as_served` requires the reference's receipt to match the store's at every scored
  delivery;
- `verify_history` requires Silver's observations to be exactly the ones the store recorded, with
  the same content, so arrival skew never measures a difference of input.

**ADR-0046 §8's declared situations**, where the online store may serve an absence the reference
does not (`comparator.excused`), are recognised per read from what the store has done:
- `late_read_after_fold`: an earlier write to the account, dated later, folded its raw transactions
  past `as_of` minus the raw history, and something was folded. Applies to the features that read
  the account's transactions;
- `scored_ahead_of_wall_clock`: the scored transaction is dated after the gateway received it;
- `cap_at_scored_millisecond`: `SCORE_READ_CAP` or more account transactions share the scored
  millisecond. Applies to the features that read the account's transactions;
- `snapshot_with_unfolded_history`: a read-only snapshot. Snapshots are never compared, because the
  store did not record the transaction (`observe_outcome_refused` and `_unreachable` skips).
Anything else that differs is a divergence, recorded with how far the read was behind the store's
newest write.
"""

from __future__ import annotations

from collections import Counter, defaultdict, deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final, Protocol

from eval.parity.comparator import Observed, Pairing, ParityTally
from eval.parity.driver import IDENTITY_EVENTS, TRANSACTIONS, Applied, Posted
from eval.parity.replay import ACCOUNT_RAW_MS, AsServedReplay, CompleteHistory
from eval.parity.served import ServedScore, parse_time
from eval.parity.skew import SkewTally, classify, windowed_counters

from trace_core.domain.enums import AuthorizationOutcome
from trace_core.domain.time import EventTime, event_time, from_millis, to_millis
from trace_core.features.context import Completeness, FeatureContext
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.observation import (
    Event,
    authorization_observation,
    transaction_observation,
)
from trace_core.features.semantics import (
    SCORE_READ_CAP,
    Entity,
    PairwiseWithPrevious,
    ProfileAttribute,
    Stream,
    WindowedAggregate,
    identity_stream,
)
from trace_core.features.spec import FeatureRegistry, FeatureSpec
from trace_core.observation.scored_event import lookback_completeness

RECORDED: Final = "RECORDED"
EXPECTED_RECEIPT: Final[Mapping[str, tuple[bool, bool]]] = {
    RECORDED: (True, False),
    "REDELIVERY": (False, False),
    "CONFLICT": (False, True),
}
"""Observe outcomes for which the store ran its record-and-read, and the receipt each implies."""

LATE_READ_AFTER_FOLD: Final = "late_read_after_fold"
SCORED_AHEAD_OF_WALL_CLOCK: Final = "scored_ahead_of_wall_clock"
CAP_AT_SCORED_MILLISECOND: Final = "cap_at_scored_millisecond"
SNAPSHOT_WITH_UNFOLDED_HISTORY: Final = "snapshot_with_unfolded_history"


class LinkError(RuntimeError):
    """Scored requests and scored deliveries do not pair up."""


class OrderEvidenceError(RuntimeError):
    """The driver's order and the store's own evidence disagree: nothing can be compared."""


class HistoryMismatchError(RuntimeError):
    """The complete history is not the history the store recorded."""


class ContextSource(Protocol):
    def context(self, *, complete_since: EventTime | None) -> FeatureContext: ...


@dataclass(frozen=True)
class Delivery:
    posted: Posted
    served: ServedScore | None = None
    event: Event | None = None


def identity_observation(posted: Posted) -> Event:
    """The observation the gateway's identity ingress records (`services.gateway.app._ingest`)."""
    request, body = posted.request, posted.body
    stream = identity_stream(str(request["identity_event_type"]))
    if stream is None or body is None:
        raise LinkError(f"post {posted.order} changes no online state and has no observation")
    return Event(
        stream=stream,
        occurred_at=event_time(parse_time(str(request["occurred_at"]))),
        account_id=str(request["account_id"]),
        device_id=request.get("device_id"),
        ip_id=request.get("ip_id"),
        event_id=str(body["event_id"]),
    )


def outcome_observation(posted: Posted) -> Event:
    """The observation the gateway's outcome ingress applies (`_apply_authorization`)."""
    request = posted.request
    decided_ms = to_millis(event_time(parse_time(str(request["decided_at"]))))
    return authorization_observation(
        transaction_id=str(request["transaction_id"]),
        account_id=str(request["account_id"]),
        authorization_outcome=AuthorizationOutcome(str(request["authorization_outcome"])),
        decided_at=event_time(from_millis(decided_ms)),
    )


def link(posts: Sequence[Posted], served: Sequence[ServedScore]) -> list[Delivery]:
    """Every post that reached the store, in the driver's order, with its scored delivery."""
    sessions = {s.session_id for s in served}
    if len(sessions) > 1:
        raise LinkError(
            f"{len(sessions)} writer sessions published scored events; the store's order across "
            f"sessions is not recorded, so a run must keep one"
        )
    if any(s.seq is None for s in served):
        raise LinkError("a scored delivery carries no sequence number header")
    queues: dict[str, deque[ServedScore]] = defaultdict(deque)
    for score in sorted(served, key=lambda s: s.seq or 0):
        queues[score.transaction_id].append(score)
    deliveries: list[Delivery] = []
    last_seq = 0
    for posted in posts:
        if posted.topic == TRANSACTIONS and posted.applied is Applied.SCORED:
            transaction_id = str(posted.request["transaction_id"])
            if not queues[transaction_id]:
                raise LinkError(
                    f"{transaction_id} was scored (post {posted.order}) but no tx.scored.v1 "
                    f"delivery is left for it"
                )
            score = queues[transaction_id].popleft()
            assert score.seq is not None
            if score.seq <= last_seq:
                raise OrderEvidenceError(
                    f"{transaction_id} (post {posted.order}) carries sequence {score.seq}, not "
                    f"after {last_seq}: the gateway did not see the driver's order"
                )
            last_seq = score.seq
            deliveries.append(Delivery(posted, served=score))
        elif posted.applied is Applied.OBSERVED:
            observe = (
                identity_observation if posted.topic == IDENTITY_EVENTS else outcome_observation
            )
            deliveries.append(Delivery(posted, event=observe(posted)))
    leftover = sum(len(q) for q in queues.values())
    if leftover:
        raise LinkError(f"{leftover} tx.scored.v1 deliveries have no scored request")
    return deliveries


def reads_account_transactions(spec: FeatureSpec) -> bool:
    shape = spec.semantics
    if isinstance(shape, ProfileAttribute):
        return shape.entity is Entity.ACCOUNT
    if isinstance(shape, (WindowedAggregate, PairwiseWithPrevious)):
        return shape.entity is Entity.ACCOUNT and shape.stream is Stream.TRANSACTION
    return False


@dataclass(frozen=True)
class ReadSituations:
    everywhere: frozenset[str]
    account_transactions: frozenset[str]
    late_by_ms: int | None
    """The store's newest write minus this read's `as_of`, when positive: how late the read was."""

    def for_feature(self, spec: FeatureSpec) -> frozenset[str]:
        if reads_account_transactions(spec):
            return self.everywhere | self.account_transactions
        return self.everywhere


def situations(score: ServedScore, current: Event, replay: AsServedReplay) -> ReadSituations:
    """The declared situations of one recorded as-served read (module docstring)."""
    everywhere: set[str] = set()
    account: set[str] = set()
    if current.occurred_ms > to_millis(score.canonical.ingested_at):
        everywhere.add(SCORED_AHEAD_OF_WALL_CLOCK)
    if replay.transactions_at(current.account_id, current.occurred_ms) >= SCORE_READ_CAP:
        account.add(CAP_AT_SCORED_MILLISECOND)
    frontier = replay.fold_frontier_ms(current.account_id)
    oldest = replay.oldest_transaction_ms(current.account_id)
    if (
        frontier is not None
        and oldest is not None
        and frontier > current.occurred_ms - ACCOUNT_RAW_MS
        and oldest < frontier
    ):
        account.add(LATE_READ_AFTER_FOLD)
    high = replay.high_watermark_ms
    late = None if high is None or high <= current.occurred_ms else high - current.occurred_ms
    return ReadSituations(frozenset(everywhere), frozenset(account), late)


@dataclass(frozen=True)
class AsServedResult:
    replay: AsServedReplay
    epochs: frozenset[int | None]
    unrecorded: frozenset[str]
    """Transaction identities scored while the store failed to record them."""
    capped: frozenset[tuple[str, str]]
    """`(transaction_id, feature_id)` capped by the reference's read of a RECORDED delivery."""


def compare_as_served(
    deliveries: Sequence[Delivery],
    tally: ParityTally,
    registry: FeatureRegistry = ONLINE_FEATURES,
) -> AsServedResult:
    replay = AsServedReplay()
    epochs: set[int | None] = set()
    unrecorded: set[str] = set()
    capped: set[tuple[str, str]] = set()
    for delivery in deliveries:
        if delivery.event is not None:
            replay.observe(delivery.event)
            continue
        score = delivery.served
        assert score is not None
        epochs.add(score.store_epoch_ms)
        event = transaction_observation(score.canonical)
        expected = EXPECTED_RECEIPT.get(score.observe_outcome)
        if expected is None:
            unrecorded.add(event.identity)
            tally.skip(f"observe_outcome_{score.observe_outcome.lower()}")
            continue
        epoch = score.store_epoch_ms
        since = None if epoch is None else EventTime(from_millis(epoch))
        receipt, context = replay.score(
            event, complete_since=since, written_ms=to_millis(score.canonical.ingested_at)
        )
        got = (receipt.recorded, receipt.conflicting)
        if got != expected or receipt.position != score.store_position:
            raise OrderEvidenceError(
                f"{score.transaction_id}: the store reported {score.observe_outcome} at position "
                f"{score.store_position}; the reference, in the driver's order, reports "
                f"recorded={receipt.recorded} conflicting={receipt.conflicting} at position "
                f"{receipt.position}"
            )
        if score.observe_outcome == "CONFLICT":
            tally.skip("observe_outcome_conflict")
            continue
        read = situations(score, replay.recorded[event.identity], replay)
        detail = {"observe_outcome": score.observe_outcome, "late_by_ms": read.late_by_ms}
        for spec in registry:
            reference = spec.evaluate(score.canonical, context)
            # Mirrors `observation.scored_event.served_features`: an absence the bounded read or
            # an unprovable lifetime start caused was never vouched for, whatever the store's age
            # (ADR-0046 §8, §3).
            completeness = (
                Completeness.INCOMPLETE.value
                if reference.depth_capped or reference.lifetime_unobserved
                else lookback_completeness(spec.feature_id, context)
            )
            if reference.depth_capped and score.observe_outcome == RECORDED:
                capped.add((score.transaction_id, spec.feature_id))
            tally.add(
                score.transaction_id,
                spec,
                score.features.get(spec.feature_id),
                Observed.of(reference, completeness),
                declared=read.for_feature(spec),
                detail=detail,
            )
    return AsServedResult(replay, frozenset(epochs), frozenset(unrecorded), frozenset(capped))


def verify_history(result: AsServedResult, history: CompleteHistory) -> dict[str, int]:
    store = result.replay.recorded
    lake = history.observations
    store_only = sorted(set(store) - set(lake))
    lake_only = sorted(set(lake) - set(store) - result.unrecorded)
    differing = sorted(
        identity
        for identity in set(store) & set(lake)
        if store[identity].recorded_form() != lake[identity].recorded_form()
    )
    if store_only or lake_only or differing:
        raise HistoryMismatchError(
            f"the complete history is not what the store recorded: {len(store_only)} recorded but "
            f"absent from Silver {store_only[:3]}, {len(lake_only)} in Silver but never recorded "
            f"{lake_only[:3]}, {len(differing)} with different content {differing[:3]}"
        )
    return {
        "recorded_by_store": len(store),
        "in_silver": len(lake),
        "in_silver_unrecorded_by_store": len(set(lake) & result.unrecorded - set(store)),
    }


def subjects_of(deliveries: Sequence[Delivery]) -> list[ServedScore]:
    """One scored delivery per transaction: its first RECORDED one, else its first."""
    first: dict[str, ServedScore] = {}
    recorded: dict[str, ServedScore] = {}
    for delivery in deliveries:
        score = delivery.served
        if score is None:
            continue
        first.setdefault(score.transaction_id, score)
        if score.observe_outcome == RECORDED:
            recorded.setdefault(score.transaction_id, score)
    return [recorded.get(tx, score) for tx, score in first.items()]


def compare_complete(
    history: CompleteHistory,
    subjects: Sequence[ServedScore],
    gold: Mapping[str, ContextSource],
    *,
    complete_since: EventTime | None,
    tally: ParityTally,
    skew: SkewTally,
    skew_subjects: frozenset[str],
    capped: frozenset[tuple[str, str]] = frozenset(),
    registry: FeatureRegistry = ONLINE_FEATURES,
) -> None:
    counters = frozenset(windowed_counters(registry))
    for score in subjects:
        in_skew = score.transaction_id in skew_subjects
        rows = gold.get(score.transaction_id)
        if rows is None:
            tally.skip("no_gold_context")
            if in_skew:
                skew.exclude("no_gold_context")
            continue
        if in_skew and score.observe_outcome != RECORDED:
            skew.exclude(f"observe_outcome_{score.observe_outcome.lower()}")
            in_skew = False
        gold_context = rows.context(complete_since=complete_since)
        identity = transaction_observation(score.canonical).identity
        reference_context = history.context(identity, complete_since=complete_since)
        for spec in registry:
            gold_value = Observed.of(spec.evaluate(score.canonical, gold_context))
            reference = Observed.of(spec.evaluate(score.canonical, reference_context))
            tally.add(score.transaction_id, spec, gold_value, reference)
            if not in_skew or spec.feature_id not in counters:
                continue
            served = score.features.get(spec.feature_id)
            if served is None:
                skew.exclude("feature_not_served")
                continue
            was_capped = (score.transaction_id, spec.feature_id) in capped
            skew.add(spec.feature_id, classify(served, gold_value, capped=was_capped))


@dataclass
class Evaluation:
    as_served: ParityTally
    complete: ParityTally
    skew: SkewTally
    epoch_ms: int | None
    history: dict[str, int]
    deliveries: dict[str, int]

    def as_dict(
        self, *, approximate_features: Sequence[str], gated: bool, measured: bool
    ) -> dict[str, Any]:
        return {
            "store_epoch_ms": self.epoch_ms,
            "history": dict(self.history),
            "deliveries": dict(self.deliveries),
            "as_served": self.as_served.as_dict(approximate_features),
            "event_time_complete": self.complete.as_dict(),
            "arrival_skew": self.skew.as_dict(gated=gated, measured=measured),
        }


def evaluate(
    posts: Sequence[Posted],
    served: Sequence[ServedScore],
    history_events: Sequence[Event],
    gold: Mapping[str, ContextSource],
    registry: FeatureRegistry = ONLINE_FEATURES,
) -> Evaluation:
    deliveries = link(posts, served)
    as_served = ParityTally(Pairing.AS_SERVED)
    result = compare_as_served(deliveries, as_served, registry)
    epochs = set(result.epochs)
    if len(epochs) > 1 or None in epochs:
        raise HistoryMismatchError(
            f"the store's epoch was {sorted(e for e in epochs if e is not None)} "
            f"{'and absent ' if None in epochs else ''}during the run: completeness moved, so "
            f"neither side's completeness claim is attributable"
        )
    epoch_ms = next(iter(epochs)) if epochs else None
    history = CompleteHistory(history_events)
    if len(history.observations) != len(history_events):
        raise HistoryMismatchError("Silver holds an observation identity twice")
    history_counts = verify_history(result, history)
    subjects = subjects_of(deliveries)
    subject_ids = {s.transaction_id for s in subjects}
    if set(gold) != subject_ids:
        raise HistoryMismatchError(
            f"Gold has contexts for {len(set(gold) - subject_ids)} transactions never scored and "
            f"none for {len(subject_ids - set(gold))} scored ones"
        )
    complete = ParityTally(Pairing.EVENT_TIME_COMPLETE)
    skew = SkewTally()
    slice_transactions = frozenset(
        str(p.request["transaction_id"])
        for p in posts
        if p.phase == "slice" and p.topic == TRANSACTIONS and p.applied is Applied.SCORED
    )
    since = None if epoch_ms is None else EventTime(from_millis(epoch_ms))
    compare_complete(
        history,
        subjects,
        gold,
        complete_since=since,
        tally=complete,
        skew=skew,
        skew_subjects=slice_transactions,
        capped=result.capped,
        registry=registry,
    )
    skew.exclude("warmup_transactions_compressed", len(subject_ids - slice_transactions))
    counted = Counter(f"{p.phase}:{p.topic}:{p.applied.value}" for p in posts)
    return Evaluation(
        as_served, complete, skew, epoch_ms, history_counts, dict(sorted(counted.items()))
    )
