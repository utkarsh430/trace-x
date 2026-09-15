"""The outbox relay: publish what PostgreSQL committed, then mark it (ADR-0051 §7; ADR-0007).

One pass works in one transaction:
1. **Claim** the oldest unpublished rows, a small batch, with `FOR UPDATE SKIP LOCKED`. Two relays
   never hold the same row, and neither waits for the other.
2. **Publish** each row's event through the one producer factory, without blocking.
3. **Flush**, then mark published exactly the rows the broker confirmed. That is every handed-over
   row, or none: delivery reports are counted per topic, so the batch is confirmed as a whole.
4. **Record** an attempt and the error on every row that was not published. Delivery is at least
   once, and consumers deduplicate on each topic's declared identity (PHASE3_PLAN §3 Q2).

**A row that can never be published does not block the queue.** A row whose event fails its
contract, or whose stored partition key differs from the key its event names, is refused. It is
marked `refused: ...` and never claimed again: its content is immutable (migration 0007), so a
retry could only fail the same way. Every such row is counted, and it stays in the table for an
operator. A broker failure is not a refusal, and it is retried.
"""

from __future__ import annotations

import threading
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

from trace_core.contracts.canonical_json import canonical_bytes
from trace_core.contracts.publish import EventPublisher
from trace_core.contracts.topics import partition_key
from trace_core.domain.errors import (
    ContractError,
    SchemaValidationError,
    UnreleasedTopicError,
)
from trace_core.observability.logging import get_logger

log = get_logger(__name__)

BATCH_SIZE: Final = 100
IDLE_INTERVAL_S: Final = 1.0
MESSAGE_TIMEOUT_MS: Final = 5_000
"""How long a relayed event may wait for the broker. The flush waits longer, so every handed-over
row reaches a verdict inside its pass."""
FLUSH_TIMEOUT_S: Final = 10.0
ERROR_MAX_CHARS: Final = 512
REFUSED_PREFIX: Final = "refused: "

CLAIM_SQL: Final = """
    SELECT outbox_id, topic, partition_key, payload
    FROM app.outbox
    WHERE published_at IS NULL
      AND (last_error IS NULL OR last_error NOT LIKE 'refused: %%')
    ORDER BY created_at, outbox_id
    LIMIT %s
    FOR UPDATE SKIP LOCKED
"""


class RowOutcome(StrEnum):
    PUBLISHED = "published"
    FAILED = "failed"
    REFUSED = "refused"


@dataclass(frozen=True, slots=True)
class RelayPass:
    """What one pass did."""

    claimed: int
    published: int
    failed: int
    refused: int


class OutboxRelay:
    """Drain `app.outbox` to Kafka, at least once, marking only confirmed deliveries."""

    def __init__(
        self,
        *,
        pool: Any,
        publisher: EventPublisher,
        batch_size: int = BATCH_SIZE,
        flush_timeout_s: float = FLUSH_TIMEOUT_S,
        idle_interval_s: float = IDLE_INTERVAL_S,
        record: Callable[[str, RowOutcome, int], None] | None = None,
    ) -> None:
        self._pool = pool
        self._publisher = publisher
        self._batch_size = batch_size
        self._flush_timeout_s = flush_timeout_s
        self._idle_interval_s = idle_interval_s
        self._record = record
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def run_once(self) -> RelayPass:
        """Claim, publish, flush and mark one batch, in one transaction."""
        with self._pool.connection() as conn, conn.transaction():
            rows = conn.execute(CLAIM_SQL, (self._batch_size,)).fetchall()
            if not rows:
                return RelayPass(claimed=0, published=0, failed=0, refused=0)
            refused: dict[int, str] = {}
            handed: list[tuple[int, str]] = []
            failure: str | None = None
            before = self._publisher.report()
            for outbox_id, topic, stored_key, payload in rows:
                verdict = self._hand_over(int(outbox_id), str(topic), str(stored_key), payload)
                if verdict is None:
                    handed.append((int(outbox_id), str(topic)))
                elif verdict.startswith(REFUSED_PREFIX):
                    refused[int(outbox_id)] = verdict
                else:
                    failure = verdict
                    break
            confirmed = self._confirm(before, handed) if handed and failure is None else False
            published_ids = [outbox_id for outbox_id, _ in handed] if confirmed else []
            failed_ids = [
                int(row[0])
                for row in rows
                if int(row[0]) not in refused and int(row[0]) not in published_ids
            ]
            if published_ids:
                conn.execute(
                    "UPDATE app.outbox SET published_at = now() WHERE outbox_id = ANY(%s)",
                    (published_ids,),
                )
            for outbox_id, error in refused.items():
                self._mark_failed(conn, [outbox_id], error)
            if failed_ids:
                reason = failure or "delivery unconfirmed at flush"
                self._mark_failed(conn, failed_ids, reason)
        self._count(rows, published_ids, failed_ids, refused)
        return RelayPass(
            claimed=len(rows),
            published=len(published_ids),
            failed=len(failed_ids),
            refused=len(refused),
        )

    def _hand_over(self, outbox_id: int, topic: str, stored_key: str, payload: Any) -> str | None:
        """None when handed to the producer; otherwise why not, prefixed when permanent."""
        if not isinstance(payload, dict):
            return f"{REFUSED_PREFIX}outbox row {outbox_id} holds no event object"
        try:
            key = partition_key(topic, payload)
        except (ContractError, UnreleasedTopicError) as exc:  # unreleased or unkeyed: permanent
            return f"{REFUSED_PREFIX}{type(exc).__name__}: {exc}"
        if key != stored_key:
            return (
                f"{REFUSED_PREFIX}outbox row {outbox_id} stored key {stored_key!r} but its event "
                f"names {key!r}"
            )
        try:
            accepted = self._publisher.publish(topic, canonical_bytes(payload), block=False)
        except (ContractError, SchemaValidationError, UnreleasedTopicError) as exc:
            # The row's own content: it would fail the same way on every retry.
            return f"{REFUSED_PREFIX}{type(exc).__name__}: {exc}"
        except Exception as exc:  # a missing topic, a closed or fatal producer: retried
            return f"not handed over: {type(exc).__name__}: {exc}"
        return None if accepted else "shed: the producer queue is full"

    def _confirm(self, before: Any, handed: list[tuple[int, str]]) -> bool:
        after = self._publisher.flush(self._flush_timeout_s)
        if after.outstanding or after.fatal:
            return False
        expected = Counter(topic for _, topic in handed)
        for topic, count in expected.items():
            delivered = after.delivered.get(topic, 0) - before.delivered.get(topic, 0)
            failed = after.failed.get(topic, 0) - before.failed.get(topic, 0)
            if failed or delivered != count:
                return False
        return True

    @staticmethod
    def _mark_failed(conn: Any, outbox_ids: list[int], error: str) -> None:
        conn.execute(
            "UPDATE app.outbox SET attempts = attempts + 1, last_error = %s "
            "WHERE outbox_id = ANY(%s)",
            (error[:ERROR_MAX_CHARS], outbox_ids),
        )

    def _count(
        self,
        rows: list[Any],
        published: list[int],
        failed: list[int],
        refused: dict[int, str],
    ) -> None:
        if self._record is None:
            return
        topics = {int(row[0]): str(row[1]) for row in rows}
        tallies: Counter[tuple[str, RowOutcome]] = Counter()
        for outbox_id in published:
            tallies[(topics[outbox_id], RowOutcome.PUBLISHED)] += 1
        for outbox_id in failed:
            tallies[(topics[outbox_id], RowOutcome.FAILED)] += 1
        for outbox_id in refused:
            tallies[(topics[outbox_id], RowOutcome.REFUSED)] += 1
        for (topic, outcome), count in tallies.items():
            self._record(topic, outcome, count)

    def start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name="outbox-relay", daemon=True)
            self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                result = self.run_once()
            except Exception as exc:  # PostgreSQL unreachable: retried after the idle interval
                log.warning("outbox_relay_pass_failed", error=type(exc).__name__)
                result = RelayPass(claimed=0, published=0, failed=0, refused=0)
            busy = result.claimed == self._batch_size and result.failed == 0
            if not busy and self._stop.wait(self._idle_interval_s):
                return

    def stop(self, timeout_s: float = FLUSH_TIMEOUT_S + 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout_s)


__all__ = [
    "BATCH_SIZE",
    "CLAIM_SQL",
    "MESSAGE_TIMEOUT_MS",
    "REFUSED_PREFIX",
    "OutboxRelay",
    "RelayPass",
    "RowOutcome",
]
