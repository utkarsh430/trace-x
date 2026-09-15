"""The gateway's durable observation log: sequence, write online, produce (ADR-0051 §3-4).

Every write that changes online state is covered (plan §4.1 point 1), in a fixed order:
1. `sequence` assigns the next contiguous number of the writable session, in memory. It is refused
   unless this process is the writer within its lease.
2. The caller writes the online store.
3. `publish` produces the event, even when the online write failed, with `block=False`. Session
   headers travel beside the payload, never inside it.

A number that is not handed to the producer, for whatever reason, makes its session unclosable.
The coverage rule then reads that session as a gap bounded by its last heartbeat. The reasons are
a shed record, a refused one, an unverified topic, no configured broker, or a request that failed
after sequencing. `close` allows the session to close only when every number it assigned was
handed over and the producer's flush confirmed every delivery (plan §4.1 point 4).

**No publish waits on the broker.** Topics are verified at start and retried on this log's own
thread, never on the event loop, and a topic not yet verified is not published to. Scoring never
depends on Kafka: a broker outage costs history coverage, which the ledger records, never a
decision.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

from trace_core.contracts.canonical_json import canonical_bytes
from trace_core.contracts.publish import EventPublisher
from trace_core.observability.logging import get_logger
from trace_core.observation.supervisor import WriterSupervisor

log = get_logger(__name__)

SESSION_HEADER: Final = "tracex-session-id"
SEQ_HEADER: Final = "tracex-seq"
DELIVERY_TIMEOUT_MS: Final = 30_000
"""How long a produced observation may wait for the broker before its delivery fails. It bounds an
unclosed session's gap (§5), so it is far below the generator's two minutes."""
TOPIC_RETRY_S: Final = 5.0
TOPIC_CHECK_TIMEOUT_S: Final = 2.0
CLOSE_FLUSH_S: Final = 5.0
"""How long shutdown waits for outstanding deliveries. Longer would outlast a container's stop
grace; an unconfirmed flush leaves the session unclosed, which is the correct record."""


class LogOutcome(StrEnum):
    HANDED_OVER = "handed_over"
    SHED = "shed"
    REFUSED = "refused"
    NOT_CONFIGURED = "not_configured"
    TOPIC_UNVERIFIED = "topic_unverified"
    UNPUBLISHED = "unpublished"


@dataclass(frozen=True, slots=True)
class Sequenced:
    """One assigned sequence number: the session it belongs to and its place in it."""

    session_id: str
    seq: int

    @property
    def headers(self) -> tuple[tuple[str, bytes], ...]:
        return ((SESSION_HEADER, self.session_id.encode()), (SEQ_HEADER, str(self.seq).encode()))


class ObservationLog:
    """Sequence and publish every online-state observation; decide whether a session may close."""

    def __init__(
        self,
        *,
        writer: WriterSupervisor,
        publisher: EventPublisher | None,
        topics: Sequence[str],
        record: Callable[[str, LogOutcome], None] | None = None,
        retry_s: float = TOPIC_RETRY_S,
        check_timeout_s: float = TOPIC_CHECK_TIMEOUT_S,
    ) -> None:
        self._writer = writer
        self._publisher = publisher
        self._topics = tuple(topics)
        self._record = record
        self._retry_s = retry_s
        self._check_timeout_s = check_timeout_s
        self._gate = threading.Lock()
        self._verified: frozenset[str] = frozenset()
        self._handed_over: dict[str, int] = {}
        self._unconfirmable: set[str] = set()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._status = (
            "not configured: nothing is published, so no writer session closes"
            if publisher is None
            else "verifying topics"
        )

    @property
    def status(self) -> str:
        """What readiness reports. Reported, never gated on: scoring does not depend on Kafka."""
        return self._status

    def start(self) -> None:
        """Verify the topics now if the broker answers; otherwise keep trying on a daemon thread."""
        if self._publisher is None or self._verify():
            return
        if self._thread is None:
            self._thread = threading.Thread(
                target=self._retry, name="observation-log-topics", daemon=True
            )
            self._thread.start()

    def _retry(self) -> None:
        while not self._stop.wait(self._retry_s):
            if self._verify():
                return

    def _verify(self) -> bool:
        publisher = self._publisher
        if publisher is None:
            return False
        try:
            unconfirmed = publisher.check_topics(self._topics, timeout_s=self._check_timeout_s)
        except Exception as exc:  # the broker could not be asked; retried
            self._status = f"unavailable: {type(exc).__name__}"
            return False
        if unconfirmed:
            self._status = f"unavailable: unverified topics {sorted(unconfirmed)}"
            return False
        self._verified = frozenset(self._topics)
        self._status = "ok"
        log.info("observation_log_topics_verified", topics=list(self._topics))
        return True

    def sequence(self) -> Sequenced:
        """Assign the next number; `WriterSessionError` when this process is not the writer."""
        session_id, seq = self._writer.next_seq()
        return Sequenced(session_id=session_id, seq=seq)

    def publish(self, sequenced: Sequenced, topic: str, event: Mapping[str, Any]) -> LogOutcome:
        """Hand the event to the producer without blocking. Anything else makes the session
        unclosable; nothing here raises, because scoring fails open (CLAUDE.md §3.7)."""
        publisher = self._publisher
        if publisher is None:
            return self._lost(sequenced, topic, LogOutcome.NOT_CONFIGURED)
        if topic not in self._verified:
            return self._lost(sequenced, topic, LogOutcome.TOPIC_UNVERIFIED)
        try:
            accepted = publisher.publish(
                topic, canonical_bytes(event), headers=sequenced.headers, block=False
            )
        except Exception as exc:  # invalid, unkeyed or a fatal producer: never a 5xx
            log.warning("observation_publish_refused", topic=topic, error=type(exc).__name__)
            return self._lost(sequenced, topic, LogOutcome.REFUSED)
        if not accepted:
            return self._lost(sequenced, topic, LogOutcome.SHED)
        with self._gate:
            self._handed_over[sequenced.session_id] = (
                self._handed_over.get(sequenced.session_id, 0) + 1
            )
        self._count(topic, LogOutcome.HANDED_OVER)
        return LogOutcome.HANDED_OVER

    def unpublished(self, sequenced: Sequenced, topic: str) -> None:
        """A number was assigned and nothing will be produced for it."""
        self._lost(sequenced, topic, LogOutcome.UNPUBLISHED)

    def _lost(self, sequenced: Sequenced, topic: str, outcome: LogOutcome) -> LogOutcome:
        with self._gate:
            self._unconfirmable.add(sequenced.session_id)
        self._count(topic, outcome)
        return outcome

    def _count(self, topic: str, outcome: LogOutcome) -> None:
        if self._record is not None:
            self._record(topic, outcome)

    def close(self, *, timeout_s: float = CLOSE_FLUSH_S) -> bool:
        """Flush, and say whether the writable session may close (plan §4.1 point 4).

        True only when the producer confirmed every delivery, and every number the session
        assigned was handed over.
        """
        self._stop.set()
        publisher = self._publisher
        if publisher is None:
            return False
        try:
            publisher.close(timeout_s)
            flushed = True
        except Exception as exc:  # EventPublishError with the per-topic accounting, logged there
            log.warning("observation_log_flush_unconfirmed", error=type(exc).__name__)
            flushed = False
        session = self._writer.session
        if session is None or session.session_id is None:
            return False
        with self._gate:
            complete = (
                session.session_id not in self._unconfirmable
                and self._handed_over.get(session.session_id, 0) == session.last_seq
            )
        return flushed and complete


__all__ = [
    "CLOSE_FLUSH_S",
    "DELIVERY_TIMEOUT_MS",
    "SEQ_HEADER",
    "SESSION_HEADER",
    "LogOutcome",
    "ObservationLog",
    "Sequenced",
]
