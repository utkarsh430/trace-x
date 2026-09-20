"""The fenced writer session of the gateway's observation log (ADR-0051; plan §4.1 points 2-4).

A process becomes the online-state writer only by holding a session-scoped advisory lock and
committing its session row. Until then it is not ready and does not score. The lock is held by a
database connection, so a process that dies loses the lock with its socket.

- `next_seq` hands out contiguous sequence numbers from 1, in memory, once per observation.
- `heartbeat` runs every few seconds, never per transaction, and confirms two things: the lock is
  still this backend's, and the row is still open. If either fails, or the database cannot be
  reached, the session is lost and the process stops writing and producing at once.
- `close` records `last_seq` only when the caller's delivery report is confirmed: nothing
  outstanding, no failed delivery, nothing shed. Otherwise the session is left unclosed, which the
  coverage rule reads as a gap bounded by its last heartbeat.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Final, Protocol

from trace_core.domain.errors import TraceXError
from trace_core.observability.logging import get_logger

log = get_logger(__name__)

WRITER_LOCK_KEY: Final = 0x7472_6163_6578_0004
"""The advisory lock a feature store's single online-state writer holds (a signed bigint).

Tests pass their own key to `WriterSession.lock_key`, so a running gateway and a test never
contend for the fence."""


class WriterSessionError(TraceXError):
    """A writer-session operation was attempted outside the state that permits it."""


class SessionState(StrEnum):
    NOT_STARTED = "not_started"
    ACTIVE = "active"
    LOST = "lost"
    CLOSED = "closed"


class SessionLedger(Protocol):
    def open(self, *, session_id: str, producer: str, instance_id: str) -> Any: ...

    def heartbeat(self, session_id: str) -> bool: ...

    def close(self, session_id: str, *, last_seq: int) -> bool: ...

    def others_live(self, session_id: str, *, within_s: float) -> bool: ...


class WriterLock(Protocol):
    def try_acquire(self, key: int) -> bool: ...

    def still_held(self, key: int) -> bool: ...


def _new_session_id() -> str:
    return uuid.uuid4().hex


@dataclass
class WriterSession:
    """One process's claim to be the writer, from start to close or loss."""

    ledger: SessionLedger
    lock: WriterLock
    producer: str
    instance_id: str
    new_session_id: Callable[[], str] = _new_session_id
    lock_key: int = WRITER_LOCK_KEY
    state: SessionState = SessionState.NOT_STARTED
    session_id: str | None = None
    _last_seq: int = field(default=0, repr=False)
    _gate: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    @property
    def ready(self) -> bool:
        """Whether this process may write online state and produce observations."""
        return self.state is SessionState.ACTIVE

    @property
    def last_seq(self) -> int:
        """The highest sequence number assigned; 0 before the first."""
        return self._last_seq

    def start(self) -> bool:
        """Take the writer's lock and open a session row; False, and not ready, if it is held."""
        with self._gate:
            if self.state is not SessionState.NOT_STARTED:
                raise WriterSessionError(f"a writer session starts once; this one is {self.state}")
            if not self.lock.try_acquire(self.lock_key):
                log.warning("writer_session_lock_held_elsewhere", producer=self.producer)
                return False
            session_id = self.new_session_id()
            self.ledger.open(
                session_id=session_id, producer=self.producer, instance_id=self.instance_id
            )
            self.session_id = session_id
            self.state = SessionState.ACTIVE
            return True

    def next_seq(self) -> int:
        """The next contiguous sequence number. Refused unless the session is active."""
        with self._gate:
            if self.state is not SessionState.ACTIVE:
                raise WriterSessionError(
                    f"no sequence number outside an active session (session is {self.state})"
                )
            self._last_seq += 1
            return self._last_seq

    def heartbeat(self) -> bool:
        """Keep the session, or lose it at once if its lock or its row is gone or unreachable."""
        if self.state is not SessionState.ACTIVE or self.session_id is None:
            return False
        reason: str | None = None
        try:
            if not self.lock.still_held(self.lock_key):
                reason = "lock_lost"
            elif not self.ledger.heartbeat(self.session_id):
                reason = "row_closed_or_missing"
        except Exception as exc:  # the fence cannot be confirmed, so it is not held
            reason = f"unreachable: {type(exc).__name__}"
        if reason is None:
            return True
        with self._gate:
            self.state = SessionState.LOST
        log.warning("writer_session_lost", session_id=self.session_id, reason=reason)
        return False

    def close(self, *, confirmed: bool) -> bool:
        """Record `last_seq` and close, only on a confirmed flush. Otherwise leave it unclosed."""
        with self._gate:
            if self.state is not SessionState.ACTIVE or self.session_id is None:
                return False
            if not confirmed:
                self.state = SessionState.LOST
                log.warning(
                    "writer_session_left_unclosed",
                    session_id=self.session_id,
                    last_seq=self._last_seq,
                )
                return False
            closed = self.ledger.close(self.session_id, last_seq=self._last_seq)
            self.state = SessionState.CLOSED if closed else SessionState.LOST
            return closed
