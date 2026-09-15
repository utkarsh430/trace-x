"""One transaction through the hot path, and what happens when a piece is missing.

The sequence is the product, so it is written in one place rather than spread
across a route handler:

    map to canonical -> read features (one pipeline, 20 ms cap)
    -> evaluate rules -> score and band -> triage if HIGH/CRITICAL -> respond

**Degradation is the interesting half.** `docs/ARCHITECTURE.md` §18 says Redis
loss degrades the hot path to rules-only and must never produce a 5xx. That is
implemented here rather than at the edge, because the honest degraded answer is
not "an error" but "a decision with fewer inputs, and a response that says so":
features become absent, rules over them abstain, the score falls, and
`degraded=true` travels in both the body and a header. A silent degradation would
be a gateway confidently approving traffic it could not assess.

**Postgres loss is different, and deliberately so.** Redis loss costs scoring
*quality*; Postgres loss means a CRITICAL transaction's investigation cannot be
durably recorded, which is data loss rather than degraded quality. Approving and
losing the case would be worse than refusing, so triage failure surfaces as 503
(ADR-0035) and readiness fails so a load balancer drains the instance. The
asymmetry is the point: CLAUDE.md §3.7 asks for fail-open on scoring, and this is
where the boundary of "scoring" actually lies.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

from trace_core.contracts.api.decision import RiskDecision
from trace_core.contracts.api.transaction import (
    MAX_BACKDATE_S,
    MAX_CLOCK_SKEW_FUTURE_S,
    TransactionRequest,
)
from trace_core.contracts.canonical import CanonicalField, CanonicalTransaction
from trace_core.domain.enums import FeatureSource
from trace_core.domain.errors import FeatureWriteFailedError
from trace_core.domain.time import event_time, utc_now
from trace_core.features import FeatureContext, FeatureState, FeatureValue
from trace_core.features.completeness import CompletenessGuard, HoleReason
from trace_core.features.context import Completeness
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.observation import transaction_observation
from trace_core.features.state_plan import PLAN
from trace_core.observability.logging import get_logger
from trace_core.observation.session import WriterSessionError
from trace_core.repositories.circuit_breaker import CircuitBreaker
from trace_core.rules.engine import Evaluation, evaluate_pack
from trace_core.rules.pack import CompiledPack
from trace_core.scoring.banding import ThresholdConfig
from trace_core.scoring.decision import build_decision

GENERATOR_COVERAGE: Final = frozenset(CanonicalField) - {CanonicalField.MEMO}
"""What a transaction submitted over HTTP supplies.

Declared per request from the fields actually present, not assumed: a caller that
omits geography must produce UNAVAILABLE geo features rather than
INSUFFICIENT_HISTORY ones, because the two mean different things and only one of
them resolves with time (ADR-0022, ADR-0032).
"""

log = get_logger(__name__)

REASON_REDIS: Final = "redis_unavailable"
REASON_RATE_LIMIT: Final = "rate_limit_unavailable"
REASON_CLOCK_SKEW: Final = "occurred_at_backdated"
REASON_WRITE_FAILED: Final = "feature_write_failed"
"""The store is up and full. Under `noeviction` this is the only way memory
pressure can present (ADR-0044), and it is counted rather than hidden. The
breaker is NOT tripped: reads still work, and blinding scoring to punish a
refused write would be the wrong direction."""
REASON_OBSERVATION_CONFLICT: Final = "observation_conflict"
"""The transaction id was already recorded with a different observation (ADR-0046 §1).

The replay cache is disposable, so a reused id can reach scoring. The store serves the first
delivery's context; evaluated for this payload it would read another account's -- or another
instant's -- windows as this one's, measured zeros included. The decision is made rules-only on an
empty context instead."""
REASON_HISTORY_INCOMPLETE: Final = "history_incomplete"
"""At least one feature was INSUFFICIENT_HISTORY because the store has not been
recording for as long as that feature looks back -- not because the entity is
new. A fresh or restarted store carries this on every decision until it has
warmed for the widest declared lookback; the decision says so rather than
presenting a warm-looking answer from a cold store (ADR-0044)."""


class ObserveOutcome(StrEnum):
    """What happened to the scored transaction's write to the online store (ADR-0046 §5).

    Carried on the outcome -- and, from Step 4, on the published scored event -- so an
    as-served replay knows whether the values served contained the transaction itself.
    """

    RECORDED = "RECORDED"
    REDELIVERY = "REDELIVERY"
    """Its identity was already recorded; the first delivery is the observation."""
    CONFLICT = "CONFLICT"
    """Its identity was already recorded with a different observation. The first delivery stays the
    observation, and this decision was made rules-only (`observation_conflict`)."""
    REFUSED = "REFUSED"
    """The store answered and refused the write. The decision was made on a read-only
    snapshot that does not contain the transaction, and completeness is withdrawn."""
    UNREACHABLE = "UNREACHABLE"
    """The store did not answer. Rules-only, and completeness is withdrawn."""
    SKIPPED = "SKIPPED"
    """Not attempted: no store is configured, or the breaker already knew it was down."""


class TriageUnavailableError(RuntimeError):
    """The system of record could not accept a case.

    Distinct from a scoring failure: the decision was reached, but the
    investigation it demands cannot be durably recorded. Surfaces as 503.
    """


@dataclass(frozen=True, slots=True)
class ScoringOutcome:
    """The decision, plus what the pipeline noticed while producing it."""

    decision: RiskDecision
    evaluation: Evaluation
    features: dict[str, FeatureValue]
    canonical: CanonicalTransaction
    feature_read_seconds: float
    degraded_reasons: tuple[str, ...] = ()
    triaged: bool = False
    observe_outcome: ObserveOutcome = ObserveOutcome.SKIPPED
    observe_position: int | None = None
    """The store's observation counter after this write, when it recorded or recognised it."""
    store_epoch_ms: int | None = None
    """The store's recording epoch the position was counted in (ADR-0051 §3); None when unknown."""
    context: FeatureContext | None = None
    """The context the features were evaluated against, completeness withdrawals applied."""


@dataclass
class ScoringPipeline:
    """Assembles the hot path from its parts. Holds no request state."""

    pack: CompiledPack
    thresholds: ThresholdConfig
    feature_store: Any | None = None
    """`RedisOnlineFeatureStore`, or None when the store is not configured.

    Typed loosely on purpose: the pipeline depends on the SHAPE -- an atomic `score`
    returning a `ServedRead`, and a read-only `snapshot` -- never on Redis. That is what
    lets the reference implementation stand in during tests without the pipeline knowing.
    """
    producer_version: str = "0.1.0"
    breaker: CircuitBreaker | None = None
    """Opened after repeated store failures so an outage costs one probe per
    cooldown rather than one timeout per call per request. Measured: without it,
    a paused Redis made a single scored request take 21.8 s (ADR-0035)."""
    completeness: CompletenessGuard | None = None
    """Withdraws the store's completeness after any unrecorded observation (ADR-0046 §5)."""
    writer: Any | None = None
    """The online store's writer fence (`WriterSupervisor`), or None when writes are not fenced.

    Its `ready` is read again immediately before the store write, after everything that can wait
    on PostgreSQL, so the write lands within the lease plus the takeover margin (ADR-0051 §2). A
    lost fence raises `WriterSessionError` and writes nothing."""

    # -- canonical mapping ---------------------------------------------------

    def to_canonical(self, request: TransactionRequest) -> CanonicalTransaction:
        """Map the request onto the canonical shape, declaring real coverage.

        `ingested_at` is stamped HERE, from our own clock. It is processing time,
        and a caller able to set it would be setting our lag metric (ADR-0026).
        """
        supplied = {
            field
            for field in CanonicalField
            if field not in {CanonicalField.INGESTED_AT}
            and getattr(request, field.value, None) is not None
        }
        coverage = frozenset(supplied | {CanonicalField.INGESTED_AT})
        values = request.model_dump(exclude_none=True)
        values.pop("occurred_at", None)
        return CanonicalTransaction(
            source_dataset="gateway",
            source_row_id=request.transaction_id,
            field_coverage=coverage,
            occurred_at=request.occurred_at,
            ingested_at=utc_now(),
            **values,
        )

    # -- clock-skew policy ---------------------------------------------------

    @staticmethod
    def check_clock(occurred_at: dt.datetime, *, now: dt.datetime) -> str | None:
        """`docs/EVENT_CONTRACTS.md` §6.3, both halves.

        More than 24 h ahead is rejected as clock skew; more than 90 d behind is
        ACCEPTED and flagged, because replaying historical data is a legitimate
        operation and a silently dropped old event is indistinguishable from a
        bug. Returns a flag reason, or raises for the rejectable case.
        """
        skew = (occurred_at - now).total_seconds()
        if skew > MAX_CLOCK_SKEW_FUTURE_S:
            raise ValueError(
                f"occurred_at is {skew / 3600:.1f} h in the future, beyond the "
                f"{MAX_CLOCK_SKEW_FUTURE_S / 3600:.0f} h clock-skew tolerance"
            )
        if -skew > MAX_BACKDATE_S:
            return REASON_CLOCK_SKEW
        return None

    # -- feature read --------------------------------------------------------

    def record_and_read(
        self, canonical: CanonicalTransaction
    ) -> tuple[FeatureContext, float, str | None, ObserveOutcome, int | None, int | None]:
        """Record the scored transaction and read its context, in one atomic store call.

        The transaction is inside its own transactional windows (ADR-0046 §2), and the
        record precedes the read in the same operation, so no other observation can land
        between them. Every way the write can fail to happen withdraws completeness before
        any later read may claim it (ADR-0046 §5):

        * refused (the store is full): a read-only snapshot, which does not contain the
          transaction, is served and the decision says `feature_write_failed`;
        * unreachable, or skipped by an open breaker: rules-only, `redis_unavailable`.

        An empty context is not an error: every feature then reports INSUFFICIENT_HISTORY,
        every rule over one abstains, and the decision says so -- the rules-only degraded
        mode §18 requires, with no second scoring implementation to keep correct.
        """
        as_of = event_time(canonical.occurred_at)
        empty = FeatureContext(as_of=as_of)
        if self.feature_store is None:
            return empty, 0.0, REASON_REDIS, ObserveOutcome.SKIPPED, None, None
        guard = self.completeness
        if self.breaker is not None and not self.breaker.allows():
            # Skipped, not attempted: the circuit already established that the store is
            # unavailable. The transaction still went unrecorded, which is a hole.
            if guard is not None:
                guard.observation_unrecorded(HoleReason.BREAKER_OPEN)
            return empty, 0.0, REASON_REDIS, ObserveOutcome.SKIPPED, None, None
        began = time.perf_counter()
        if guard is not None:
            guard.reconcile()
        if self.writer is not None and not self.writer.ready:
            # The reconcile above may have waited on PostgreSQL past the lease: nothing is written.
            raise WriterSessionError("the writer fence was lost before the store write")
        reason: str | None = None
        position: int | None = None
        epoch: int | None = None
        try:
            served = self.feature_store.score(transaction_observation(canonical))
        except FeatureWriteFailedError:
            # Reachable and full. The store answered; it refused. Not an outage, so the
            # breaker stays closed -- opening it would stop reads that still work in order
            # to react to a write that did not.
            if self.breaker is not None:
                self.breaker.record_success()
            if guard is not None:
                guard.observation_unrecorded(HoleReason.REFUSED)
            outcome = ObserveOutcome.REFUSED
            reason = REASON_WRITE_FAILED
            try:
                context = self.feature_store.snapshot(
                    as_of=as_of,
                    account_id=canonical.account_id,
                    currency=canonical.currency,
                    card_id=canonical.card_id,
                    device_id=canonical.device_id,
                    merchant_id=canonical.merchant_id,
                    ip_id=canonical.ip_id,
                )
            except Exception:
                context = empty
        except Exception as exc:
            # Never re-raised: §18 says Redis loss degrades to rules-only and never 5xx. The
            # type is logged and the message is not: a Redis error can quote a key, and a key
            # names an account.
            log.warning("feature_store_score_failed", error=type(exc).__name__)
            if self.breaker is not None:
                self.breaker.record_failure()
            if guard is not None:
                guard.observation_unrecorded(HoleReason.UNREACHABLE)
            elapsed = time.perf_counter() - began
            return empty, elapsed, REASON_REDIS, ObserveOutcome.UNREACHABLE, None, None
        else:
            if self.breaker is not None:
                self.breaker.record_success()
            position = served.receipt.position
            epoch = served.store_epoch_ms
            if served.receipt.conflicting:
                # The first delivery's context, which this payload must not be evaluated against:
                # another account's windows would read as this one's, zeros included.
                context = empty
                reason = REASON_OBSERVATION_CONFLICT
                outcome = ObserveOutcome.CONFLICT
            else:
                context = served.context
                outcome = (
                    ObserveOutcome.RECORDED
                    if served.receipt.recorded
                    else ObserveOutcome.REDELIVERY
                )
        if guard is not None and guard.pending:
            # The store may still hold an epoch from before the hole; nothing it claims about
            # completeness may be believed until the hole has been withdrawn.
            context = dataclasses.replace(context, complete_since=None)
        return context, time.perf_counter() - began, reason, outcome, position, epoch

    # -- the sequence --------------------------------------------------------

    def score(
        self,
        request: TransactionRequest,
        *,
        now: dt.datetime | None = None,
        extra_degraded: tuple[str, ...] = (),
    ) -> ScoringOutcome:
        """Score one transaction. Never raises for a missing dependency."""
        began = time.perf_counter()
        moment = now or dt.datetime.now(dt.UTC)
        reasons: list[str] = list(extra_degraded)

        if (flag := self.check_clock(request.occurred_at, now=moment)) is not None:
            reasons.append(flag)

        canonical = self.to_canonical(request)
        context, read_seconds, read_reason, observe_outcome, position, epoch = self.record_and_read(
            canonical
        )
        if read_reason is not None:
            reasons.append(read_reason)

        features = ONLINE_FEATURES.evaluate_all(canonical, context)
        if history_incomplete(features, context):
            reasons.append(REASON_HISTORY_INCOMPLETE)
        evaluation = evaluate_pack(self.pack, canonical, features)
        decision = build_decision(
            transaction_id=request.transaction_id,
            evaluation=evaluation,
            features=features,
            config=self.thresholds,
            scored_at=moment,
            latency_ms=(time.perf_counter() - began) * 1000.0,
            degraded=bool(reasons),
            degraded_reasons=tuple(dict.fromkeys(reasons)),
            feature_source=context.source or FeatureSource.ONLINE_ONLY,
        )
        return ScoringOutcome(
            decision=decision,
            evaluation=evaluation,
            features=features,
            canonical=canonical,
            feature_read_seconds=read_seconds,
            degraded_reasons=tuple(dict.fromkeys(reasons)),
            observe_outcome=observe_outcome,
            observe_position=position,
            store_epoch_ms=epoch,
            context=context,
        )

    def opens_investigation(self, decision: RiskDecision) -> bool:
        return self.thresholds.opens_investigation(decision.risk_band)


def history_incomplete(features: dict[str, FeatureValue], context: FeatureContext) -> bool:
    """Was any absence caused by the store's youth rather than the entity's?

    A feature is INSUFFICIENT_HISTORY for one of two reasons: the entity has
    too little history (a new account, four observations where eight are
    needed), or the store has not been recording for as long as the feature
    looks back. Only the second is a property of the deployment rather than of
    the transaction, and only the second is worth flagging on the decision --
    the first is the ordinary condition every genuinely new entity is in.
    """
    for feature_id, value in features.items():
        if value.state is not FeatureState.INSUFFICIENT_HISTORY:
            continue
        lookback = PLAN.lookback_s.get(feature_id, 0)
        if lookback and context.completeness(lookback) is not Completeness.COMPLETE:
            return True
    return False


def absent_feature_reasons(features: dict[str, FeatureValue]) -> dict[str, str]:
    """`feature_id -> reason`, for the absence counter.

    UNAVAILABLE and INSUFFICIENT_HISTORY are counted separately: one says the
    source will never supply the input and the other says not yet, and an
    operator seeing them merged would not know which one to act on.
    """
    return {
        feature_id: value.state.value
        for feature_id, value in features.items()
        if value.state is not FeatureState.AVAILABLE
    }
