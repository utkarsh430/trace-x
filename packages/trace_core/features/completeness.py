"""An observation the online store did not record withdraws its completeness — durably.

**The defect this closes** (`docs/PHASE3_PLAN.md` §3, found while reviewing the plan). ADR-0044 lets
the store vouch for every window that began after its epoch, and a store that *refuses* a write
deletes that epoch in the same breath. A store that cannot be *reached* cannot: the observation is
lost, the epoch it would have to withdraw is on the far side of the outage, and when the store comes
back it still claims completeness over the hole. A gateway restart before the store returns forgets
that anything was lost at all.

**The rule** (ADR-0046 §5). Every unrecorded observation — refused, unreachable, or never attempted
because the circuit breaker had already learned the store was down — opens a *hole*:

1. The hole is recorded durably, **once per episode** rather than once per transaction (no Postgres
   write per scored transaction, plan §4.1), and the process believes no completeness claim while it
   is pending.
2. Before the store is used again, its epoch is **moved forward** to the moment of withdrawal plus
   the accepted future clock skew. Deleting it would not be enough: a lost observation may be dated
   up to `MAX_CLOCK_SKEW_FUTURE_S` ahead of its arrival, and a window beginning after "now" could
   still have contained it. Only then is the hole cleared.
3. A process that starts while a hole is open inherits it, and a ledger that is configured but
   cannot be read is treated as holding one: it cannot say there is none.

Every failure along the way leaves the hole pending, which errs towards claiming less.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from enum import StrEnum
from typing import Final, Protocol

from trace_core.contracts.api.transaction import MAX_CLOCK_SKEW_FUTURE_S
from trace_core.domain.time import EventTime
from trace_core.observability.logging import get_logger

log = get_logger(__name__)

RESUME_MARGIN: Final = dt.timedelta(seconds=MAX_CLOCK_SKEW_FUTURE_S)
"""How far past the withdrawal completeness resumes: the latest event time an observation lost
in the hole could carry, relative to when it arrived (plan §4.1 point 6)."""


class HoleReason(StrEnum):
    """Why an observation went unrecorded."""

    UNREACHABLE = "UNREACHABLE"
    """The store did not answer: a connection error or a timeout."""
    BREAKER_OPEN = "BREAKER_OPEN"
    """The write was not attempted, because the breaker had already learned the store was down."""
    REFUSED = "REFUSED"
    """The store answered and refused the write (under `noeviction`, it is full)."""


class CompletenessControl(Protocol):
    """What a store must offer for its completeness claim to be withdrawn."""

    def withdraw_completeness(self, *, resume_at: EventTime) -> None:
        """Vouch for no window that began before `resume_at`, whatever the store believed."""


class HoleLedger(Protocol):
    """Durable record of holes that have not yet been withdrawn from the store."""

    def record_hole(self, *, reason: HoleReason, instance_id: str) -> None: ...

    def open_holes(self) -> int: ...

    def latest_open_hole(self) -> int | None: ...

    def clear_holes(self, *, through_hole_id: int) -> int: ...


class CompletenessGuard:
    """Tracks whether the online store's completeness claim may be believed.

    Single-writer by construction (Phase 3 has one gateway writer, plan §4.1 point 2), and called
    from the serialised request path, so it holds no lock.
    """

    def __init__(
        self,
        store: CompletenessControl,
        ledger: HoleLedger | None,
        *,
        instance_id: str,
        clock: Callable[[], dt.datetime] = lambda: dt.datetime.now(dt.UTC),
    ) -> None:
        self._store = store
        self._ledger = ledger
        self._instance_id = instance_id
        self._clock = clock
        self._pending = False
        self._recorded = False
        """Whether the pending episode is already in the ledger."""
        self._unverified = False
        """Pending only because the ledger could not be read at start-up: no hole is known."""
        self._lost_here = False
        """Whether this process itself lost an observation during the pending episode."""

    @property
    def pending(self) -> bool:
        """True while no completeness claim from the store may be believed."""
        return self._pending

    def resume(self) -> None:
        """At start-up: inherit a hole a previous process recorded and never withdrew."""
        if self._ledger is None:
            return
        try:
            open_holes = self._ledger.open_holes()
        except Exception as exc:
            log.warning("feature_store_hole_ledger_unreadable", error=type(exc).__name__)
            self._pending = True
            self._unverified = True
            return
        if open_holes:
            log.warning("feature_store_hole_inherited", open_holes=open_holes)
            self._pending = True
            self._recorded = True

    def observation_unrecorded(self, reason: HoleReason) -> None:
        """An observation was lost. Called on the request path, before the response is sent."""
        if not self._pending:
            log.warning("feature_store_hole_opened", reason=reason.value)
        self._pending = True
        self._lost_here = True
        if self._recorded or self._ledger is None:
            return
        try:
            self._ledger.record_hole(reason=reason, instance_id=self._instance_id)
        except Exception as exc:
            # Still pending in this process; recording is retried on the next lost observation
            # and the hole is withdrawn before any claim is believed. A crash before either
            # forgets it: plan §4.1's writer fencing (Step 4), under which a process that cannot
            # reach PostgreSQL stops writing and serving, is what closes that window.
            log.error(
                "feature_store_hole_not_recorded", reason=reason.value, error=type(exc).__name__
            )
            return
        self._recorded = True

    def reconcile(self) -> bool:
        """Withdraw a pending hole from the store. True once nothing is pending."""
        if not self._pending:
            return True
        if self._unverified and not self._lost_here and self._ledger is not None:
            # Pending only because the ledger was unreadable at start-up. Read now, it answers
            # the question start-up could not: an empty ledger here vouches exactly as an empty
            # ledger at start-up would have, so there is nothing to withdraw. Withdrawing anyway
            # would cost every slow-database restart a day of `history_incomplete`.
            try:
                open_holes = self._ledger.open_holes()
            except Exception as exc:
                log.warning("feature_store_hole_ledger_unreadable", error=type(exc).__name__)
                return False
            self._unverified = False
            if not open_holes:
                self._pending = False
                log.info("feature_store_hole_ledger_verified")
                return True
            log.warning("feature_store_hole_inherited", open_holes=open_holes)
            self._recorded = True
        through: int | None = None
        if self._ledger is not None:
            try:
                # Captured BEFORE the withdrawal: only holes that already existed are covered by it.
                through = self._ledger.latest_open_hole()
            except Exception as exc:
                log.warning("feature_store_hole_ledger_unreadable", error=type(exc).__name__)
                return False
        resume_at = EventTime(self._clock() + RESUME_MARGIN)
        try:
            self._store.withdraw_completeness(resume_at=resume_at)
        except Exception as exc:
            log.info("feature_store_hole_withdrawal_deferred", error=type(exc).__name__)
            return False
        if self._ledger is not None:
            try:
                if through is not None:
                    self._ledger.clear_holes(through_hole_id=through)
                if self._ledger.open_holes():
                    # Another instance recorded a hole while this one withdrew: not covered.
                    return False
            except Exception as exc:
                # The store no longer claims completeness, but the ledger still says a hole is
                # open. Staying pending withdraws again next time: over-cautious, never wrong.
                log.warning("feature_store_hole_not_cleared", error=type(exc).__name__)
                return False
        self._pending = False
        self._recorded = False
        self._unverified = False
        self._lost_here = False
        log.info("feature_store_hole_withdrawn", resume_at=resume_at.isoformat())
        return True


__all__ = [
    "RESUME_MARGIN",
    "CompletenessControl",
    "CompletenessGuard",
    "HoleLedger",
    "HoleReason",
]
