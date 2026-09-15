"""Keeping a gateway the fenced writer, or keeping it not ready (ADR-0051 §2; plan §4.1 point 2).

One supervision step, `tick`, runs every `interval_s` on its own thread, never on the event loop, so
a slow or partitioned PostgreSQL cannot stall scoring. It holds a `WriterSession` on a dedicated
connection:
- **No active session** (never acquired, lost, or held elsewhere): reconnect and start a NEW
  session. A lost session is never resumed; its gap stays bounded by its last heartbeat.
- **Active session:** heartbeat it. A heartbeat that cannot confirm the lock and the open row loses
  the session at once.

Two clocks guard what a heartbeat alone cannot:
- **The lease.** `ready` holds only while the fence was confirmed within `lease_s`. A heartbeat
  stuck on a silent network never returns to report a loss, so the request path does not wait for
  it: an unconfirmed fence expires by itself.
- **The takeover grace.** PostgreSQL can release a lock (a restart, a terminated backend) before
  its holder learns of it. So a process that acquires the lock while another session is unclosed
  and was heartbeated within `takeover_grace_s` writes nothing until `takeover_grace_s` has passed,
  by which time that holder's lease has expired. A predecessor that closed cleanly is not waited
  out.

`ready` is true only while a session is active, confirmed within its lease, AND its acquisition
hook has completed. The gateway runs the start-up steps that write online state (inheriting and
withdrawing holes, dating the store's epoch) in that hook, so only the writer performs them, and
every request that would write online state is refused until they are done.

Both guards compare durations: this process's monotonic clock for its own lease and grace, the
database clock for a predecessor's heartbeat age. What neither bounds is a process suspended
between its readiness check and its write (a paused VM). The store checks no fencing token, so
that residual is not prevented here; ADR-0051 records it as a risk.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any, Final

from trace_core.observability.logging import get_logger
from trace_core.observation.session import (
    WRITER_LOCK_KEY,
    SessionState,
    WriterSession,
    WriterSessionError,
)
from trace_core.repositories.postgres_sessions import PostgresSessionLedger, PostgresWriterLock

log = get_logger(__name__)

HEARTBEAT_INTERVAL_S: Final = 2.0
"""How often the fence is confirmed: every few seconds, never per transaction (plan §4.1)."""
LEASE_S: Final = 3 * HEARTBEAT_INTERVAL_S
"""How long one confirmation lets this process write: two heartbeats may be missed, not three."""
TAKEOVER_MARGIN_S: Final = 2.0
"""Added to the lease before a takeover writes, so a predecessor's request that passed its
readiness check just before its lease expired has finished its bounded write."""
TAKEOVER_GRACE_S: Final = LEASE_S + TAKEOVER_MARGIN_S


class WriterSupervisor:
    """Acquire, prepare and keep the writer session; report whether this process may write."""

    def __init__(
        self,
        *,
        connect: Callable[[], Any],
        producer: str,
        instance_id: str,
        on_acquired: Callable[[], None] | None = None,
        interval_s: float = HEARTBEAT_INTERVAL_S,
        lease_s: float = LEASE_S,
        takeover_grace_s: float = TAKEOVER_GRACE_S,
        lock_key: int = WRITER_LOCK_KEY,
        clock: Callable[[], float] = time.monotonic,
        session_factory: Callable[[Any], WriterSession] | None = None,
    ) -> None:
        if not 0 < interval_s < lease_s <= takeover_grace_s:
            raise WriterSessionError(
                "a lease must outlast a heartbeat interval, and a takeover must outlast a lease: "
                f"interval {interval_s} s, lease {lease_s} s, takeover grace {takeover_grace_s} s"
            )
        self._connect = connect
        self._producer = producer
        self._instance_id = instance_id
        self._on_acquired = on_acquired
        self._interval_s = interval_s
        self._lease_s = lease_s
        self._grace_s = takeover_grace_s
        self._lock_key = lock_key
        self._clock = clock
        self._session_factory = session_factory or self._postgres_session
        self._gate = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._conn: Any = None
        self._session: WriterSession | None = None
        self._writable: WriterSession | None = None
        """The session, once prepared. Published last and withdrawn first, so the request path
        never sees a session that has not finished its acquisition hook."""
        self._confirmed_at: float | None = None
        self._writes_from = 0.0
        self._status = "not started"

    def _postgres_session(self, conn: Any) -> WriterSession:
        return WriterSession(
            ledger=PostgresSessionLedger(conn),
            lock=PostgresWriterLock(conn),
            producer=self._producer,
            instance_id=self._instance_id,
            lock_key=self._lock_key,
        )

    @property
    def ready(self) -> bool:
        """Whether this process is the prepared writer, within its lease. Read per request."""
        session, confirmed = self._writable, self._confirmed_at
        return (
            session is not None
            and not self._stop.is_set()
            and session.state is SessionState.ACTIVE
            and confirmed is not None
            and self._clock() - confirmed < self._lease_s
        )

    @property
    def status(self) -> str:
        """What readiness reports: why this process may not write, or the session it writes in."""
        session, confirmed = self._writable, self._confirmed_at
        if session is not None and session.state is SessionState.ACTIVE and confirmed is not None:
            age = self._clock() - confirmed
            if age >= self._lease_s:
                return f"lease expired: the fence was last confirmed {age:.1f} s ago"
        return self._status

    @property
    def session(self) -> WriterSession | None:
        return self._session

    def next_seq(self) -> tuple[str, int]:
        """The writable session's id and its next contiguous sequence number (ADR-0051 §3).

        Refused unless this process is the prepared writer within its lease, so a request that
        passed its readiness check and then lost the fence assigns nothing and writes nothing.
        """
        session = self._writable
        if session is None or not self.ready or session.session_id is None:
            raise WriterSessionError("not the writer: no sequence number is assigned")
        return session.session_id, session.next_seq()

    def tick(self) -> None:
        """One supervision step."""
        with self._gate:
            if self._stop.is_set():
                return
            session = self._session
            if session is not None and session.state is SessionState.ACTIVE:
                began = self._clock()
                if session.heartbeat():
                    self._confirmed_at = began
                    if self._writable is None:
                        self._prepare_when_safe(session)
                    else:
                        self._status = f"active {session.session_id}"
                    return
                log.warning("writer_supervisor_session_lost", session_id=session.session_id)
            self._acquire()

    def _acquire(self) -> None:
        self._writable = None
        self._session = None
        self._confirmed_at = None
        self._close_connection()
        try:
            conn = self._connect()
        except Exception as exc:  # PostgreSQL unreachable: not ready, retried on the next tick
            self._status = f"unreachable: {type(exc).__name__}"
            return
        session = self._session_factory(conn)
        began = self._clock()
        try:
            acquired = session.start()
        except Exception as exc:  # the lock or the row could not be taken: not the writer
            self._status = f"unreachable: {type(exc).__name__}"
            self._close(conn)
            return
        if not acquired:
            self._status = "lock held elsewhere"
            self._close(conn)
            return
        self._conn, self._session, self._confirmed_at = conn, session, began
        waits = self._predecessor_may_be_writing(session)
        self._writes_from = self._clock() + (self._grace_s if waits else 0.0)
        log.info(
            "writer_supervisor_session_acquired",
            session_id=session.session_id,
            waits_out_predecessor=waits,
        )
        self._prepare_when_safe(session)

    def _predecessor_may_be_writing(self, session: WriterSession) -> bool:
        """Whether another session may still be writing; True whenever that cannot be ruled out."""
        if session.session_id is None:
            return True
        try:
            return session.ledger.others_live(session.session_id, within_s=self._grace_s)
        except Exception as exc:  # unknown is not absent
            log.warning("writer_supervisor_predecessor_unknown", error=type(exc).__name__)
            return True

    def _prepare_when_safe(self, session: WriterSession) -> None:
        remaining = self._writes_from - self._clock()
        if remaining > 0:
            self._status = (
                f"taking over: waiting {remaining:.1f} s for a predecessor's lease to expire"
            )
            return
        try:
            if self._on_acquired is not None:
                self._on_acquired()
        except Exception as exc:  # holds the fence but may not write yet: retried next tick
            self._status = f"preparing failed: {type(exc).__name__}"
            log.warning("writer_supervisor_prepare_failed", error=type(exc).__name__)
            return
        self._writable = session
        self._status = f"active {session.session_id}"

    def start(self) -> None:
        """Run the first step now, then one every interval on a daemon thread."""
        self.tick()
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name="writer-supervisor", daemon=True)
            self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(self._interval_s):
            try:
                self.tick()
            except Exception as exc:  # the thread outlives a failed step; the lease covers it
                log.error("writer_supervisor_step_failed", error=type(exc).__name__)

    def stop(self, *, confirmed: bool) -> bool:
        """Stop supervising; close the session only if `confirmed`; release the connection.

        Never waits forever on a step stuck on the network: the session is then left unclosed, a
        gap bounded by its last heartbeat.
        """
        self._stop.set()
        self._writable = None
        bound = self._interval_s + self._lease_s
        if self._thread is not None:
            self._thread.join(timeout=bound)
        if not self._gate.acquire(timeout=bound):
            log.warning("writer_supervisor_stop_timed_out", session_id=self._session_id())
            return False
        try:
            closed = False
            session = self._session
            if session is not None and session.state is SessionState.ACTIVE:
                try:
                    closed = session.close(confirmed=confirmed)
                except Exception as exc:  # the session stays unclosed: a bounded gap
                    log.warning("writer_supervisor_close_failed", error=type(exc).__name__)
            self._status = "stopped"
            self._close_connection()
            return closed
        finally:
            self._gate.release()

    def _session_id(self) -> str | None:
        session = self._session
        return None if session is None else session.session_id

    def _close_connection(self) -> None:
        if self._conn is not None:
            self._close(self._conn)
            self._conn = None

    @staticmethod
    def _close(conn: Any) -> None:
        try:
            conn.close()
        except Exception as exc:  # closing a dead connection is not an event worth failing on
            log.info("writer_supervisor_connection_close_failed", error=type(exc).__name__)


__all__ = [
    "HEARTBEAT_INTERVAL_S",
    "LEASE_S",
    "TAKEOVER_GRACE_S",
    "TAKEOVER_MARGIN_S",
    "WriterSupervisor",
]
