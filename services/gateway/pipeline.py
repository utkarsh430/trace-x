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

import datetime as dt
import time
from dataclasses import dataclass, field
from typing import Any, Final

from trace_core.contracts.api.decision import RiskDecision
from trace_core.contracts.api.transaction import (
    MAX_BACKDATE_S,
    MAX_CLOCK_SKEW_FUTURE_S,
    TransactionRequest,
)
from trace_core.contracts.canonical import CanonicalField, CanonicalTransaction
from trace_core.domain.enums import FeatureSource
from trace_core.domain.time import EventTime, event_time, utc_now
from trace_core.features import FeatureContext, FeatureState, FeatureValue
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.reference import Event
from trace_core.features.semantics import Stream
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

REASON_REDIS: Final = "redis_unavailable"
REASON_RATE_LIMIT: Final = "rate_limit_unavailable"
REASON_CLOCK_SKEW: Final = "occurred_at_backdated"


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


@dataclass
class ScoringPipeline:
    """Assembles the hot path from its parts. Holds no request state."""

    pack: CompiledPack
    thresholds: ThresholdConfig
    feature_store: Any | None = None
    """`RedisOnlineFeatureStore`, or None when the store is not configured.

    Typed loosely on purpose: the pipeline depends on the SHAPE (a `snapshot`
    call returning a `FeatureContext`), never on Redis. That is what lets the
    reference implementation stand in during tests without the pipeline knowing.
    """
    producer_version: str = "0.1.0"
    _observed: list[Event] = field(default_factory=list, repr=False)

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

    def read_features(
        self, canonical: CanonicalTransaction
    ) -> tuple[FeatureContext, float, str | None]:
        """One snapshot, or an empty one plus a reason.

        An empty context is not an error: every feature then reports
        INSUFFICIENT_HISTORY, every rule over one abstains, and the decision says
        so. That is the rules-only degraded mode §18 requires, expressed as
        missing inputs rather than as a special code path -- so there is no second
        scoring implementation to keep correct.
        """
        as_of = event_time(canonical.occurred_at)
        if self.feature_store is None:
            return FeatureContext(as_of=as_of), 0.0, REASON_REDIS
        began = time.perf_counter()
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
            # Never re-raised: §18 says Redis loss degrades to rules-only and
            # never 5xx. The reason is returned so the caller counts it.
            return FeatureContext(as_of=as_of), time.perf_counter() - began, REASON_REDIS
        return context, time.perf_counter() - began, None

    def observe(self, canonical: CanonicalTransaction) -> str | None:
        """Record the transaction in the online store, for later transactions.

        Best effort, and after the decision: a write failure must not cost the
        caller its answer. Returns a degradation reason if it failed.
        """
        if self.feature_store is None:
            return REASON_REDIS
        try:
            self.feature_store.observe(_as_event(canonical))
        except Exception:
            return REASON_REDIS
        return None

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
        context, read_seconds, read_reason = self.read_features(canonical)
        if read_reason is not None:
            reasons.append(read_reason)

        features = ONLINE_FEATURES.evaluate_all(canonical, context)
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
        )

    def opens_investigation(self, decision: RiskDecision) -> bool:
        return self.thresholds.opens_investigation(decision.risk_band)


def _as_event(canonical: CanonicalTransaction) -> Event:
    """The online store's observation shape for a scored transaction."""
    return Event(
        stream=Stream.TRANSACTION,
        occurred_at=EventTime(canonical.occurred_at),
        account_id=canonical.account_id,
        currency=canonical.currency,
        amount_minor=canonical.amount_minor,
        card_id=canonical.card_id,
        device_id=canonical.device_id,
        merchant_id=canonical.merchant_id,
        ip_id=canonical.ip_id,
        merchant_mcc=canonical.merchant_mcc,
        merchant_country=canonical.merchant_country,
        latitude=canonical.latitude,
        longitude=canonical.longitude,
        channel=canonical.channel,
        authorization_outcome=canonical.authorization_outcome,
        event_id=canonical.transaction_id,
    )


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
