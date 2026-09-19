"""Chaos: Bronze, Silver and Gold resume from kills at injected points, losing nothing and counting
nothing twice (`P3.checkpoint-resume`; docs/PHASE3_PLAN.md §7, docs/ROADMAP.md Phase 3).

Acceptance command: `pytest -m chaos tests/chaos/test_spark_resume.py`. Everything is real -- a
throwaway broker, the pinned JVM toolchain, local Delta, and the jobs as an operator runs them
(`python -m services.stream.bronze run`, `... silver run`, `... gold build`) in child processes.
Nothing is mocked. tests/chaos/conftest.py fails a session that collects no `test_inject_*` case or
skips all of them.

The traffic is `tx.authorization.v1`: one outcome per transaction (the dedup identity), keyed by
account, and one of the three Silver tables Gold reads, so one workload exercises every tier.

**Injection points**, one test each:
1. process kill: the Bronze job, then the Silver job, SIGKILLed with their JVMs at seeded moments
   over many batches, including inside a batch, and restarted (`test_inject_process_kill_*`);
2. a crash after a batch's sink commit and before Spark's commit entry, produced by process death at
   exactly that point, and the same window with the batch's offsets entry lost as well, which
   `decide_start` must refuse rather than re-plan (ADR-0048 (b));
3. Silver dying between its per-target commits, at every point its commit order allows;
4. a broker restart, graceful and then by SIGKILL, while Bronze ingests;
5. replay from arbitrary offsets into a fresh lake;
6. the ROADMAP manual validation: the Spark JVM itself killed mid-stream under Silver and under
   Bronze, then resume, ending with Gold's build and its consistency check.

**Oracles are Kafka and the records the test published, never the tables under test.** Bronze must
equal what a plain consumer reads, byte for byte, headers and coordinates included. Silver must
equal what the published records say at the coordinates Kafka gave them: the test knows each
record's kind (an original, a producer retry, an identity conflict, a future-skewed outcome, a
poison value), so every canonical row, duplicate, quarantine reason and late event is derived
without reading Silver. Gold's observation count is the canonical outcomes the records imply.

**Invariants after every kill and every resume** (`_invariants`): Bronze conservation, Silver
conservation, no identity canonical twice, and no Bronze coordinate twice in `silver.duplicates` or
`silver.quarantine` within one checkpoint version -- the last three counted here directly as well,
so a defect in a conservation check cannot hide one in the pipeline.

**Evidence.** Every schedule is drawn from `TRACE_CHAOS_SEED` and recorded before it runs. Each
injection and what followed it -- the exit status, the checkpoint and Delta state it left, the
window it landed in, and the invariants -- is printed as an `INJECTION` JSON line, and appended to
`spark-resume-injections.jsonl` in `TRACE_CHAOS_RECORD_DIR` when that is set.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pytest
from tests.integration.test_bronze_kafka import (  # noqa: F401 -- `spark` is a fixture
    CONFLUENT_LOG_APPEND_TIME,
    SHA,
    _as_bronze_holds,
    _broker_view,
    _conservation,
    _fresh,
    _ingest,
    _landed,
    _produce_raw,
    _publish,
    spark,
)
from tests.integration.test_kafka_platform import (  # noqa: F401 -- `broker` is a fixture
    DECLARATION,
    Broker,
    _admin,
    _consume,
    _docker,
    _watermarks,
    broker,
)

from trace_core.contracts import authorization
from trace_core.contracts.canonical_json import canonical_bytes
from trace_core.contracts.publish import java_partition
from trace_core.contracts.topics import IDENTITY_EVENTS_V1, TX_AUTHORIZATION_V1, TX_SCORED_V1
from trace_core.domain.identifiers import uuid7
from trace_core.stream import checkpoints
from trace_core.stream import silver_rules as rules
from trace_core.stream.bronze import BRONZE_COLUMNS, BRONZE_TOPICS, Trigger, bronze_declaration
from trace_core.stream.bronze_conservation import OffsetRange
from trace_core.stream.checkpoints import KafkaSourceStart, SparkProgress, StartAction
from trace_core.stream.lake import LakeConfig
from trace_core.stream.silver import canonical_schema, create_silver_tables, start_silver_query
from trace_core.stream.silver_conservation import check_silver_conservation
from trace_core.stream.tables import CommitProvenance, create_table

pytestmark = [pytest.mark.chaos, pytest.mark.integration, pytest.mark.stream]

ROOT = Path(__file__).resolve().parents[2]
TOPIC = TX_AUTHORIZATION_V1
BRONZE = BRONZE_TOPICS[TOPIC]
SILVER = rules.silver_topic(TOPIC)
SEED = int(os.environ.get("TRACE_CHAOS_SEED", "20260915"))
RECORD_DIR_ENV = "TRACE_CHAOS_RECORD_DIR"

PRODUCER = "trace-test@1.0.0"
"""Not the gateway, so future skew is judged against LogAppendTime (timing semantics v1)."""
POISON = b"not json"
LATE_BY = dt.timedelta(minutes=20)
FUTURE_BY = dt.timedelta(days=2)
LATE_AFTER_MS = 600_000
"""docs/PHASE3_PLAN.md §4.3, written out by hand. The workload keeps every original far from it:
on time within seconds of its outcome, or late by twenty minutes."""

EXIT_OK = 0
EXIT_REFUSED = 2
EXIT_QUERY_FAILED = 3
"""The services' documented exit codes (services/stream/bronze.py), written out by hand."""
SIGKILLED = -signal.SIGKILL

START_TIMEOUT_S = 300.0
"""A job's JVM start and first batches, with slack for a loaded laptop."""
DRAIN_TIMEOUT_S = 900.0
JVM_DEATH_TIMEOUT_S = 180.0

KINDS = ("on_time", "late", "retry", "conflict", "future", "poison")
WEIGHTS = (40, 14, 18, 8, 8, 12)
BRONZE_CONTENT = tuple(
    column
    for column in BRONZE_COLUMNS
    if column not in {"bronze_ingested_at", "bronze_batch_id", "bronze_checkpoint_id"}
)
"""What the broker delivered; the rest of a Bronze row records which run wrote it."""
SILVER_LINEAGE = frozenset(
    {
        "bronze_batch_id",
        "bronze_checkpoint_id",
        "silver_batch_id",
        "silver_checkpoint_id",
        "silver_admitted_at",
    }
)
SILVER_COMMIT_ORDER = ("quarantine", "duplicates", "canonical", "late_events")
"""The order `trace_core.stream.silver._write_batch` commits its targets in."""
BRONZE_KILLS = 6
SILVER_KILLS = 4


# ------------------------------------------------------------------- evidence ---


def _record(test: str, entry: dict[str, Any]) -> None:
    """Print one injection record, and append it where a run collects its evidence."""
    line = json.dumps({"test": test, "seed": SEED, **entry}, sort_keys=True, default=str)
    print(f"INJECTION {line}", flush=True)
    directory = os.environ.get(RECORD_DIR_ENV, "").strip()
    if directory:
        path = Path(directory) / "spark-resume-injections.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


# ------------------------------------------------------------------- workload ---


@dataclass(frozen=True)
class Sent:
    """One record the test published, as the test knows it without reading Kafka or the lake."""

    kind: str
    value: bytes
    transaction_id: str | None = None
    content: tuple[str, str] | None = None
    """(transaction, outcome): what a retry repeats and a conflict changes; None for a poison."""
    event_id: str | None = None
    occurred_ms: int | None = None
    late: bool = False


@dataclass(frozen=True)
class _Outcome:
    transaction_id: str
    account: str
    decided_ms: int
    occurred_ms: int
    outcome: str
    late: bool


class Workload:
    """Seeded `tx.authorization.v1` traffic with every kind Silver must tell apart, over every
    partition. An identity's deliveries all carry its account key, so they share one partition and
    their order is their offset order."""

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        partitions = DECLARATION.topics[TOPIC].partitions
        found: dict[int, list[str]] = {number: [] for number in range(partitions)}
        number = 0
        while any(len(accounts) < 3 for accounts in found.values()):
            account = f"acct_{number:09d}"
            bucket = found[java_partition(account.encode(), partitions)]
            if len(bucket) < 3:
                bucket.append(account)
            number += 1
        self.accounts = [account for key in sorted(found) for account in found[key]]
        self.originals: list[_Outcome] = []
        self.sent: list[Sent] = []
        self._next = 0
        self._lock = threading.Lock()

    def _event(self, outcome: _Outcome, now_ms: int) -> dict[str, Any]:
        return authorization.build_event(
            transaction_id=outcome.transaction_id,
            account_id=outcome.account,
            authorization_outcome=outcome.outcome,
            decided_ms=outcome.decided_ms,
            transaction_occurred_ms=outcome.occurred_ms,
            transaction_occurred_at=authorization.iso_millis(outcome.occurred_ms),
            producer=PRODUCER,
            trace_id=f"{self.rng.getrandbits(128):032x}",
            correlation_id=outcome.transaction_id,
            # A retry is redelivered later, and carries a new envelope: none of this is content.
            ingested_ms=now_ms,
            event_id=uuid7(millis=now_ms, rng=self.rng),
        )

    def publish(self, target: Broker, count: int, *, every_kind: bool = False) -> list[Sent]:
        """Publish one seeded chunk. `every_kind` guarantees each kind appears, so a Silver batch
        over the chunk writes every one of its four targets."""
        with self._lock:
            now_ms = int(dt.datetime.now(dt.UTC).timestamp() * 1000)
            kinds = list(KINDS) if every_kind else []
            kinds += self.rng.choices(KINDS, weights=WEIGHTS, k=max(0, count - len(kinds)))
            events: list[tuple[str, dict[str, Any], tuple[tuple[str, bytes], ...]]] = []
            poison: list[tuple[bytes, bytes, tuple[tuple[str, bytes | None], ...]]] = []
            chunk: list[Sent] = []
            for drawn in kinds:
                kind = drawn
                if kind in {"retry", "conflict"} and not self.originals:
                    kind = "on_time"
                if kind == "poison":
                    account = self.rng.choice(self.accounts)
                    poison.append((account.encode(), POISON, ()))
                    chunk.append(Sent("poison", POISON))
                    continue
                if kind in {"retry", "conflict"}:
                    original = self.rng.choice(self.originals)
                    outcome = (
                        original
                        if kind == "retry"
                        else _Outcome(
                            original.transaction_id,
                            original.account,
                            original.decided_ms,
                            original.occurred_ms,
                            "DECLINED" if original.outcome == "APPROVED" else "APPROVED",
                            original.late,
                        )
                    )
                else:
                    shift = {"late": -LATE_BY, "future": FUTURE_BY}.get(kind, dt.timedelta(0))
                    decided = now_ms + int(shift.total_seconds() * 1000)
                    outcome = _Outcome(
                        transaction_id=f"tx_chaos_{self._next:010d}",
                        account=self.rng.choice(self.accounts),
                        decided_ms=decided,
                        occurred_ms=decided - 1_000,
                        outcome=self.rng.choice(("APPROVED", "DECLINED")),
                        late=kind == "late",
                    )
                    self._next += 1
                    if kind != "future":
                        self.originals.append(outcome)
                event = self._event(outcome, now_ms)
                events.append((TOPIC, event, ()))
                chunk.append(
                    Sent(
                        kind=kind,
                        value=canonical_bytes(event),
                        transaction_id=outcome.transaction_id,
                        content=(outcome.transaction_id, outcome.outcome),
                        event_id=str(event["envelope"]["event_id"]),
                        occurred_ms=outcome.decided_ms,
                        late=outcome.late,
                    )
                )
            if events:
                _publish(target, events)
            if poison:
                _produce_raw(target, TOPIC, poison)
            self.sent.extend(chunk)
            return chunk


class Feeder:
    """Publishes a seeded chunk every few seconds, so a running job always has new work to do."""

    def __init__(self, workload: Workload, target: Broker, *, every_s: float, count: int) -> None:
        self.workload = workload
        self.target = target
        self.every_s = every_s
        self.count = count
        self.chunks = 0
        self.errors: list[str] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="spark-resume-feeder", daemon=True)

    def _run(self) -> None:
        while not self._stop.wait(self.every_s):
            try:
                self.workload.publish(self.target, self.count)
            except Exception as exc:  # reported by stop(); a silent feeder death hides the cause
                self.errors.append(repr(exc))
                return
            self.chunks += 1

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=120)
        assert not self._thread.is_alive(), "the feeder thread did not stop"
        assert not self.errors, self.errors


# -------------------------------------------------------------------- oracles ---


@dataclass(frozen=True)
class Delivery:
    partition: int
    offset: int
    logged_ms: int
    sent: Sent

    @property
    def coordinates(self) -> tuple[int, int]:
        return (self.partition, self.offset)


def _deliveries(
    target: Broker, workload: Workload, start: dict[int, int] | None = None
) -> list[Delivery]:
    """Every record the topic holds from `start`, matched to the record this test published."""
    published: dict[bytes, Sent] = {}
    for sent in workload.sent:
        if sent.kind != "poison":
            assert sent.value not in published, "the workload never publishes one record twice"
            published[sent.value] = sent
    found: list[Delivery] = []
    seen: set[bytes] = set()
    for record in _consume(target, TOPIC, start or {}):
        assert record.timestamp_type == CONFLUENT_LOG_APPEND_TIME, record
        if record.value == POISON:
            sent = Sent("poison", POISON)
        else:
            matched = published.get(record.value)
            assert matched is not None, (
                f"{record.partition}/{record.offset} holds a record this test never published"
            )
            assert record.value not in seen, (
                f"{record.partition}/{record.offset}: a published record is in Kafka twice"
            )
            seen.add(record.value)
            sent = matched
        found.append(Delivery(record.partition, record.offset, record.timestamp_ms, sent))
    if not start:
        poison = sum(1 for sent in workload.sent if sent.kind == "poison")
        assert seen == set(published), "every acknowledged record is in Kafka"
        assert len(found) == len(published) + poison, "and Kafka holds nothing else"
    return found


@dataclass(frozen=True)
class SilverView:
    canonical: dict[str, tuple[int, int]]
    is_late: dict[str, bool]
    event_ids: dict[str, str]
    duplicates: dict[tuple[int, int], str]
    quarantine: dict[tuple[int, int], str]
    late_events: dict[str, tuple[int, int]]


def _expected_silver(deliveries: Sequence[Delivery]) -> SilverView:
    """What Silver must hold, derived from the published records and Kafka's coordinates alone."""
    quarantine: dict[tuple[int, int], str] = {}
    by_identity: dict[str, list[Delivery]] = {}
    for delivery in deliveries:
        if delivery.sent.kind == "poison":
            quarantine[delivery.coordinates] = "invalid_event"
        elif delivery.sent.kind == "future":
            quarantine[delivery.coordinates] = "future_skew"
        else:
            assert delivery.sent.transaction_id is not None
            by_identity.setdefault(delivery.sent.transaction_id, []).append(delivery)
    canonical: dict[str, tuple[int, int]] = {}
    is_late: dict[str, bool] = {}
    event_ids: dict[str, str] = {}
    duplicates: dict[tuple[int, int], str] = {}
    for identity, items in by_identity.items():
        assert len({item.partition for item in items}) == 1, (
            f"{identity}: the account key keeps an identity's deliveries on one partition"
        )
        # ADR-0053 §2.4: every topic but tx.scored.v1 orders by (LogAppendTime, partition, offset).
        first, *rest = sorted(items, key=lambda item: (item.logged_ms, item.partition, item.offset))
        assert first.sent.event_id is not None and first.sent.occurred_ms is not None
        canonical[identity] = first.coordinates
        event_ids[identity] = first.sent.event_id
        late = first.logged_ms - first.sent.occurred_ms > LATE_AFTER_MS
        if first.sent.kind in {"on_time", "late"}:
            assert late is first.sent.late, f"{identity}: the workload's margins decide lateness"
        is_late[identity] = late
        for delivery in rest:
            if delivery.sent.content == first.sent.content:
                duplicates[delivery.coordinates] = "duplicate"
            else:
                quarantine[delivery.coordinates] = "identity_conflict"
    late_events = {identity: where for identity, where in canonical.items() if is_late[identity]}
    return SilverView(canonical, is_late, event_ids, duplicates, quarantine, late_events)


def _actual_silver(session: Any, lake: LakeConfig) -> SilverView:
    from pyspark.sql import functions as F  # noqa: N812

    app_id = _identity(lake, SILVER.query).app_id

    def read(ref: Any) -> Any:
        return session.read.format("delta").load(str(ref.local_path(lake)))

    canonical: dict[str, tuple[int, int]] = {}
    is_late: dict[str, bool] = {}
    event_ids: dict[str, str] = {}
    for row in read(SILVER.table).collect():
        identity = str(row["silver_identity"])
        assert identity not in canonical, f"{identity} is canonical twice"
        canonical[identity] = (int(row["kafka_partition"]), int(row["kafka_offset"]))
        is_late[identity] = bool(row["is_late"])
        event_ids[identity] = str(row["event_id"])

    def per_version(ref: Any, column: str) -> dict[tuple[int, int], str]:
        found: dict[tuple[int, int], str] = {}
        rows = read(ref).filter(
            (F.col("silver_topic") == TOPIC) & (F.col("silver_checkpoint_id") == app_id)
        )
        for row in rows.select("kafka_partition", "kafka_offset", column).collect():
            where = (int(row["kafka_partition"]), int(row["kafka_offset"]))
            assert where not in found, f"{ref} counts {where} twice within one checkpoint version"
            found[where] = str(row[column])
        return found

    late_events: dict[str, tuple[int, int]] = {}
    for row in read(rules.LATE_EVENTS).filter(F.col("silver_topic") == TOPIC).collect():
        identity = str(row["silver_identity"])
        assert identity not in late_events, f"{identity} is in late_events twice"
        late_events[identity] = (int(row["kafka_partition"]), int(row["kafka_offset"]))
    return SilverView(
        canonical,
        is_late,
        event_ids,
        per_version(rules.DUPLICATES, "disposition"),
        per_version(rules.QUARANTINE, "reason"),
        late_events,
    )


def _difference(actual: dict[Any, Any], expected: dict[Any, Any]) -> dict[str, Any]:
    return {
        "missing": sorted(map(str, set(expected) - set(actual)))[:8],
        "unexpected": sorted(map(str, set(actual) - set(expected)))[:8],
        "different": sorted(
            f"{key}: {actual[key]} != {expected[key]}"
            for key in set(actual) & set(expected)
            if actual[key] != expected[key]
        )[:8],
    }


def _assert_silver_is_expected(
    session: Any, lake: LakeConfig, deliveries: Sequence[Delivery]
) -> dict[str, int]:
    expected = _expected_silver(deliveries)
    actual = _actual_silver(session, lake)
    for name in ("canonical", "is_late", "event_ids", "duplicates", "quarantine", "late_events"):
        mine: dict[Any, Any] = getattr(actual, name)
        theirs: dict[Any, Any] = getattr(expected, name)
        assert mine == theirs, (name, _difference(mine, theirs))
    return {
        "canonical": len(expected.canonical),
        "duplicates": len(expected.duplicates),
        "quarantine": len(expected.quarantine),
        "late_events": len(expected.late_events),
    }


def _assert_bronze_is_kafka(
    session: Any, lake: LakeConfig, target: Broker, workload: Workload
) -> int:
    landed = _landed(session, lake, TOPIC)
    assert len(landed) == len({(row.partition, row.offset) for row in landed}), (
        "Bronze holds an offset more than once"
    )
    assert sorted(_as_bronze_holds(row) for row in landed) == _broker_view(target, TOPIC), (
        "Bronze is not, byte for byte, what the broker holds"
    )
    assert len(_deliveries(target, workload)) == len(landed)
    return len(landed)


def _content(
    session: Any, path: Path, columns: Sequence[str], key: Sequence[str]
) -> dict[tuple[Any, ...], tuple[Any, ...]]:
    """Rows by key, with binary and nested values made comparable between two lakes."""

    def plain(value: Any) -> Any:
        if isinstance(value, bytes | bytearray):
            return bytes(value)
        if isinstance(value, list):
            return tuple(plain(item) for item in value)
        if hasattr(value, "asDict"):
            return tuple(sorted((name, plain(item)) for name, item in value.asDict().items()))
        return value

    found: dict[tuple[Any, ...], tuple[Any, ...]] = {}
    for row in session.read.format("delta").load(str(path)).select(*columns).collect():
        where = tuple(row[name] for name in key)
        assert where not in found, f"{path.name} holds {where} twice"
        found[where] = tuple(plain(row[name]) for name in columns)
    return found


# -------------------------------------------------- checkpoint and Delta state ---

_COMMIT_FILE = re.compile(r"[0-9]{20}\.json")


def _identity(lake: LakeConfig, query: str) -> checkpoints.CheckpointIdentity:
    state = checkpoints.read_state(lake, query)
    assert state.identity is not None, f"{query} has no checkpoint identity"
    return state.identity


def _directory(lake: LakeConfig, query: str) -> Path:
    identity = _identity(lake, query)
    return checkpoints.query_directory(lake, query) / f"v{identity.version}"


def _progress(lake: LakeConfig, query: str) -> SparkProgress:
    state = checkpoints.read_state(lake, query)
    if state.identity is None:
        return SparkProgress()
    return SparkProgress.read(
        checkpoints.query_directory(lake, query) / f"v{state.identity.version}"
    )


def _txn_commits(table: Path, app_id: str) -> list[int]:
    """Every batch `app_id` committed to `table`, ONE ENTRY PER COMMIT, in log order.

    Read from Delta's commit files rather than through its log replay, which keeps only the highest
    version per app id: a batch committed twice must be countable."""
    log = table / "_delta_log"
    versions: list[int] = []
    if not log.is_dir():
        return versions
    for path in sorted(entry for entry in log.iterdir() if _COMMIT_FILE.fullmatch(entry.name)):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            action = json.loads(line).get("txn")
            if isinstance(action, dict) and action.get("appId") == app_id:
                versions.append(int(action["version"]))
    return versions


def _log_version(table: Path) -> int:
    log = table / "_delta_log"
    return max(
        (int(entry.stem) for entry in log.iterdir() if _COMMIT_FILE.fullmatch(entry.name)),
        default=-1,
    )


def _silver_tables(lake: LakeConfig) -> dict[str, Path]:
    return {
        "quarantine": rules.QUARANTINE.local_path(lake),
        "duplicates": rules.DUPLICATES.local_path(lake),
        "canonical": SILVER.table.local_path(lake),
        "late_events": rules.LATE_EVENTS.local_path(lake),
    }


def _window(progress: SparkProgress, commits: dict[str, list[int]]) -> str:
    """Where a kill landed, read from the state it left behind rather than from its timing."""
    if not progress.planned:
        return "before_any_batch_was_planned"
    batch = max(progress.planned)
    if progress.last_committed is not None and progress.last_committed >= batch:
        return f"between_batches(after batch {batch})"
    landed = [name for name in SILVER_COMMIT_ORDER if batch in commits.get(name, [])]
    landed += [
        name
        for name in sorted(commits)
        if name not in SILVER_COMMIT_ORDER and batch in commits[name]
    ]
    if not landed:
        return f"inside_batch_{batch}_before_any_sink_commit"
    return f"inside_batch_{batch}_after_sink_commits[{','.join(landed)}]_before_spark_commit"


def _move_aside(directory: Path, batch: int, aside: Path) -> list[tuple[Path, Path]]:
    """Take a batch's offsets entry out of the checkpoint, as a lost file would be, keeping it."""
    moved: list[tuple[Path, Path]] = []
    for name in (str(batch), f".{batch}.crc"):
        source = directory / name
        if source.exists():
            destination = aside / name
            shutil.move(str(source), str(destination))
            moved.append((source, destination))
    assert moved and moved[0][0].name == str(batch), f"{directory} has no entry for batch {batch}"
    return moved


def _restore(moved: Sequence[tuple[Path, Path]]) -> None:
    for source, destination in moved:
        shutil.move(str(destination), str(source))


# ----------------------------------------------------------------------- jobs ---


@dataclass
class Job:
    name: str
    process: subprocess.Popen[bytes]
    log_path: Path

    def log(self) -> str:
        return self.log_path.read_text(encoding="utf-8", errors="replace")

    def tail(self, lines: int = 60) -> str:
        return "\n".join(self.log().splitlines()[-lines:])

    @property
    def running(self) -> bool:
        return self.process.poll() is None


def _group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _await_group_gone(pgid: int, timeout_s: float = 90.0) -> None:
    deadline = time.monotonic() + timeout_s
    while _group_alive(pgid):
        if time.monotonic() > deadline:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(pgid, signal.SIGKILL)
            pytest.fail(f"process group {pgid} outlived its driver by {timeout_s:.0f} s")
        time.sleep(0.1)


class Jobs:
    """Every service process a test starts, each in its own session so it can be killed whole."""

    def __init__(self, env: dict[str, str], directory: Path) -> None:
        self.env = env
        self.directory = directory
        self.launched: list[Job] = []

    def launch(
        self,
        lake: LakeConfig,
        service: str,
        args: Sequence[str],
        *,
        harness: Sequence[str] = (),
    ) -> Job:
        name = f"{len(self.launched):02d}-{service}{'-harness' if harness else ''}"
        module = (
            ["-m", "tests.chaos.spark_resume_harness", "--job", service, *harness, "--"]
            if harness
            else ["-m", f"services.stream.{service}"]
        )
        log_path = self.directory / f"{name}.log"
        with log_path.open("wb") as handle:
            process = subprocess.Popen(  # noqa: S603 -- literal arguments, this interpreter
                [sys.executable, *module, *args],
                cwd=ROOT,
                env={**self.env, "TRACE_DELTA_ROOT": str(lake.root)},
                stdin=subprocess.DEVNULL,
                stdout=handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        job = Job(name, process, log_path)
        self.launched.append(job)
        return job

    def wait(self, job: Job, timeout_s: float) -> int:
        try:
            code = job.process.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            self.kill(job)
            pytest.fail(f"{job.name} did not exit within {timeout_s:.0f} s:\n{job.tail()}")
        _await_group_gone(job.process.pid)
        return code

    def kill(self, job: Job) -> int:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(job.process.pid, signal.SIGKILL)
        code = job.process.wait(timeout=90)
        _await_group_gone(job.process.pid)
        return code

    def stop(self, job: Job, timeout_s: float = 300.0) -> int:
        """SIGTERM to the driver alone: the service stops its queries and exits."""
        job.process.send_signal(signal.SIGTERM)
        return self.wait(job, timeout_s)

    def close(self) -> None:
        for job in self.launched:
            if job.running or _group_alive(job.process.pid):
                print(f"cleanup: killing {job.name}")
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(job.process.pid, signal.SIGKILL)
                with contextlib.suppress(subprocess.TimeoutExpired):
                    job.process.wait(timeout=60)


@pytest.fixture(scope="module")
def child_env() -> dict[str, str]:
    """The environment a service child runs in, proven to import this checkout's code."""
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"TRACE_DELTA_ROOT", "TRACE_KAFKA_BOOTSTRAP"}
    }
    env["PYTHONPATH"] = os.pathsep.join((str(ROOT / "packages"), str(ROOT)))
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            "import trace_core, services.stream.bronze as bronze, "
            "tests.chaos.spark_resume_harness as harness; "
            "print(trace_core.__file__); print(bronze.__file__); print(harness.__file__)",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert probe.returncode == 0, probe.stderr
    for line in probe.stdout.split():
        assert Path(line).resolve().is_relative_to(ROOT), (
            f"a service child would run code from outside this checkout: {line}"
        )
    return env


@pytest.fixture
def jobs(child_env: dict[str, str], tmp_path: Path) -> Iterator[Jobs]:
    directory = tmp_path / "jobs"
    directory.mkdir()
    running = Jobs(child_env, directory)
    try:
        yield running
    finally:
        running.close()


def _await(
    predicate: Callable[[], bool],
    what: str,
    *,
    timeout_s: float,
    job: Job | None = None,
    poll_s: float = 0.05,
) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if job is not None and not job.running:
            pytest.fail(
                f"{job.name} exited {job.process.returncode} while waiting for {what}:"
                f"\n{job.tail()}"
            )
        if time.monotonic() > deadline:
            tail = "" if job is None else f":\n{job.tail()}"
            pytest.fail(f"timed out after {timeout_s:.0f} s waiting for {what}{tail}")
        time.sleep(poll_s)


def _harness_kill(job: Job) -> dict[str, Any]:
    found = [
        json.loads(line[line.index("{") :])["harness_kill"]
        for line in job.log().splitlines()
        if '"harness_kill"' in line
    ]
    assert len(found) == 1, f"{job.name} did not die at its kill point:\n{job.tail()}"
    return dict(found[0])


# ------------------------------------------------------------- kill schedules ---


@dataclass(frozen=True)
class KillPlan:
    after_commits: int
    """Commit entries the restarted job must record before the kill is armed."""
    inside_batch: bool
    """Wait for a planned, uncommitted batch -- its offsets entry without its commit entry."""
    delay_ms: int


def _schedule(name: str, count: int, *, max_commits: int, max_delay_ms: int) -> list[KillPlan]:
    rng = random.Random(f"{SEED}:{name}:schedule")
    return [
        KillPlan(
            after_commits=rng.randint(1, max_commits),
            inside_batch=rng.random() < 0.6,
            delay_ms=rng.randint(0, max_delay_ms),
        )
        for _ in range(count)
    ]


def _kill_as_planned(
    jobs: Jobs, job: Job, lake: LakeConfig, query: str, plan: KillPlan, committed: frozenset[int]
) -> int:
    _await(
        lambda: len(_progress(lake, query).committed - committed) >= plan.after_commits,
        f"{plan.after_commits} new commit entries from {job.name}",
        timeout_s=START_TIMEOUT_S,
        job=job,
        poll_s=0.1,
    )
    if plan.inside_batch:

        def planned_not_committed() -> bool:
            progress = _progress(lake, query)
            last = progress.last_committed
            return bool(progress.planned) and (last is None or max(progress.planned) > last)

        _await(
            planned_not_committed,
            f"{job.name} to be inside a batch",
            timeout_s=START_TIMEOUT_S,
            job=job,
            poll_s=0.01,
        )
    time.sleep(plan.delay_ms / 1000)
    return jobs.kill(job)


# ----------------------------------------------------------------- invariants ---


def _invariants(
    session: Any, lake: LakeConfig, target: Broker, *, label: str, silver: bool
) -> dict[str, Any]:
    """Every invariant the capability rests on, after a kill or a resume."""
    from pyspark.sql import functions as F  # noqa: N812

    evidence: dict[str, Any] = {}

    def read(ref: Any) -> Any:
        return session.read.format("delta").load(str(ref.local_path(lake)))

    if checkpoints.read_state(lake, BRONZE.query).identity is not None:
        report = _conservation(session, lake, target, TOPIC)
        assert report.conserved, (label, report.summary())
        bronze = read(BRONZE.table)
        repeated = (
            bronze.groupBy("kafka_topic_id", "kafka_partition", "kafka_offset")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        assert repeated == 0, f"{label}: Bronze holds {repeated} coordinates more than once"
        evidence["bronze"] = {
            "conserved": True,
            "rows": bronze.count(),
            "through_batch": report.consumed.through_batch,
            "checkpoint_versions": list(checkpoints.read_state(lake, BRONZE.query).versions),
        }
    if silver and checkpoints.read_state(lake, SILVER.query).identity is not None:
        report_silver = check_silver_conservation(session, lake, TOPIC)
        assert report_silver.conserved, (label, report_silver.summary())
        twice = (
            read(SILVER.table).groupBy("silver_identity").count().filter(F.col("count") > 1).count()
        )
        assert twice == 0, f"{label}: {SILVER.table} holds {twice} identities twice"
        late_twice = (
            read(rules.LATE_EVENTS)
            .filter(F.col("silver_topic") == TOPIC)
            .groupBy("silver_identity")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        assert late_twice == 0, f"{label}: late_events holds {late_twice} identities twice"
        for ref in (rules.DUPLICATES, rules.QUARANTINE):
            counted = (
                read(ref)
                .filter(F.col("silver_topic") == TOPIC)
                .groupBy(
                    "silver_checkpoint_id", "kafka_topic_id", "kafka_partition", "kafka_offset"
                )
                .count()
                .filter(F.col("count") > 1)
                .count()
            )
            assert counted == 0, (
                f"{label}: {ref} counts {counted} Bronze coordinates twice within one "
                f"checkpoint version"
            )
        summary = report_silver.summary()
        evidence["silver"] = {
            key: summary[key]
            for key in (
                "conserved",
                "through_version",
                "bronze_rows",
                "canonical",
                "duplicates",
                "quarantined",
            )
        }
    return evidence


# ---------------------------------------------------------------------- Gold ---


def _service_json(job: Job, key: str) -> dict[str, Any]:
    """The last JSON object a service printed that carries `key`."""
    for line in reversed(job.log().splitlines()):
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        try:
            parsed = json.loads(stripped)
        except ValueError:
            continue
        if isinstance(parsed, dict) and key in parsed:
            return parsed
    pytest.fail(f"{job.name} printed no JSON object with {key!r}:\n{job.tail()}")


def _gold_final_state(
    session: Any, lake: LakeConfig, jobs: Jobs, *, expected_observations: int
) -> dict[str, Any]:
    """Gold's build over the Silver this run produced, and its consistency check (ADR-0055)."""
    for topic in (TX_SCORED_V1, IDENTITY_EVENTS_V1):
        create_silver_tables(session, lake, topic, git_sha=SHA, dirty_worktree=False)
    build = jobs.launch(lake, "gold", ["build"])
    assert jobs.wait(build, DRAIN_TIMEOUT_S) == EXIT_OK, build.tail()
    check = jobs.launch(lake, "gold", ["check"])
    assert jobs.wait(check, DRAIN_TIMEOUT_S) == EXIT_OK, check.tail()
    record = _service_json(build, "targets")
    report = _service_json(check, "consistent")
    assert report["consistent"], report
    observations = int(record["targets"]["gold.observations"]["rows"])
    assert observations == expected_observations, (
        f"Gold holds {observations} observations; the published records imply "
        f"{expected_observations} canonical outcomes"
    )
    return {"build": record, "check": report}


# ---------------------------------------------------------- service arguments ---

STREAMING = ("--interval-s", "0.5", "--max-offsets-per-trigger", "6")
DRAIN = ("--available-now",)


def _bronze_args(target: Broker, *mode: str) -> list[str]:
    return ["run", "--bootstrap", target.bootstrap, "--topic", TOPIC, *mode]


def _silver_args(*mode: str) -> list[str]:
    return ["run", "--topic", TOPIC, *mode]


def _silver_here(session: Any, lake: LakeConfig) -> None:
    """One Silver drain in this process, for the lakes a test builds rather than kills."""
    handle = start_silver_query(
        session,
        lake,
        TOPIC,
        git_sha=SHA,
        dirty_worktree=False,
        now=dt.datetime.now(dt.UTC),
        trigger=Trigger(available_now=True),
    )
    handle.query.awaitTermination()
    assert handle.query.exception() is None, handle.query.exception()


# --------------------------------------------------------------------- tests ---


def test_inject_process_kill_of_the_bronze_job_at_seeded_moments(
    broker: Broker,  # noqa: F811
    spark: Any,  # noqa: F811
    jobs: Jobs,
    tmp_path: Path,
) -> None:
    """Injection 1, Bronze: the job and its JVM SIGKILLed at seeded moments over many batches."""
    name = "bronze-process-kill"
    _fresh(broker, TOPIC)
    lake = LakeConfig.at(tmp_path / "lake")
    workload = Workload(random.Random(f"{SEED}:{name}:workload"))
    workload.publish(broker, 150, every_kind=True)
    schedule = _schedule(name, BRONZE_KILLS, max_commits=4, max_delay_ms=1500)
    _record(name, {"schedule": [asdict(plan) for plan in schedule]})
    table = BRONZE.table.local_path(lake)
    windows: list[str] = []

    for index, plan in enumerate(schedule):
        committed = _progress(lake, BRONZE.query).committed
        job = jobs.launch(lake, "bronze", _bronze_args(broker, *STREAMING))
        code = _kill_as_planned(jobs, job, lake, BRONZE.query, plan, committed)
        assert code == SIGKILLED, f"{job.name} stopped on its own:\n{job.tail()}"
        progress = _progress(lake, BRONZE.query)
        commits = {"bronze": _txn_commits(table, _identity(lake, BRONZE.query).app_id)}
        window = _window(progress, commits)
        windows.append(window)
        evidence = _invariants(spark, lake, broker, label=f"{name} kill {index}", silver=False)
        _record(
            name,
            {
                "injection": index,
                "plan": asdict(plan),
                "exit": code,
                "window": window,
                "last_planned": max(progress.planned, default=None),
                "last_committed": progress.last_committed,
                "last_sink_commit": max(commits["bronze"], default=None),
                "after_kill": evidence,
            },
        )
        workload.publish(broker, 30)  # arrives while the job is down

    final = jobs.launch(lake, "bronze", _bronze_args(broker, *DRAIN))
    assert jobs.wait(final, DRAIN_TIMEOUT_S) == EXIT_OK, final.tail()
    rows = _assert_bronze_is_kafka(spark, lake, broker, workload)
    evidence = _invariants(spark, lake, broker, label=f"{name} final", silver=False)
    assert checkpoints.read_state(lake, BRONZE.query).versions == (1,), "a resume needed no reset"
    versions = _txn_commits(table, _identity(lake, BRONZE.query).app_id)
    assert len(versions) == len(set(versions)), "a batch was committed to Bronze twice"
    _record(name, {"final": {"rows": rows, "windows": windows, **evidence}})
    assert any(window.startswith("inside_batch") for window in windows), (
        f"no kill landed inside a batch, so the suite proved nothing about that window: {windows}"
    )


def test_inject_process_kill_of_the_silver_job_at_seeded_moments(
    broker: Broker,  # noqa: F811
    spark: Any,  # noqa: F811
    jobs: Jobs,
    tmp_path: Path,
) -> None:
    """Injection 1, Silver: killed at seeded moments while Bronze keeps ingesting new records."""
    name = "silver-process-kill"
    _fresh(broker, TOPIC)
    lake = LakeConfig.at(tmp_path / "lake")
    workload = Workload(random.Random(f"{SEED}:{name}:workload"))
    workload.publish(broker, 60, every_kind=True)
    schedule = _schedule(name, SILVER_KILLS, max_commits=2, max_delay_ms=4000)
    _record(name, {"schedule": [asdict(plan) for plan in schedule]})
    tables = _silver_tables(lake)
    windows: list[str] = []

    bronze = jobs.launch(
        lake, "bronze", _bronze_args(broker, "--interval-s", "1", "--max-offsets-per-trigger", "8")
    )
    _await(
        lambda: bool(_progress(lake, BRONZE.query).committed),
        "Bronze's first batch",
        timeout_s=START_TIMEOUT_S,
        job=bronze,
        poll_s=0.2,
    )
    feeder = Feeder(workload, broker, every_s=2.0, count=8)
    feeder.start()
    try:
        for index, plan in enumerate(schedule):
            committed = _progress(lake, SILVER.query).committed
            job = jobs.launch(lake, "silver", _silver_args("--interval-s", "1"))
            code = _kill_as_planned(jobs, job, lake, SILVER.query, plan, committed)
            assert code == SIGKILLED, f"{job.name} stopped on its own:\n{job.tail()}"
            progress = _progress(lake, SILVER.query)
            app_id = _identity(lake, SILVER.query).app_id
            commits = {target: _txn_commits(path, app_id) for target, path in tables.items()}
            window = _window(progress, commits)
            windows.append(window)
            assert bronze.running, f"Bronze stopped during the Silver kills:\n{bronze.tail()}"
            evidence = _invariants(spark, lake, broker, label=f"{name} kill {index}", silver=True)
            _record(
                name,
                {
                    "injection": index,
                    "plan": asdict(plan),
                    "exit": code,
                    "window": window,
                    "last_planned": max(progress.planned, default=None),
                    "last_committed": progress.last_committed,
                    "after_kill": evidence,
                },
            )
    finally:
        feeder.stop()

    assert jobs.stop(bronze) == EXIT_OK, bronze.tail()
    drain_bronze = jobs.launch(lake, "bronze", _bronze_args(broker, *DRAIN))
    assert jobs.wait(drain_bronze, DRAIN_TIMEOUT_S) == EXIT_OK, drain_bronze.tail()
    drain_silver = jobs.launch(lake, "silver", _silver_args(*DRAIN))
    assert jobs.wait(drain_silver, DRAIN_TIMEOUT_S) == EXIT_OK, drain_silver.tail()

    rows = _assert_bronze_is_kafka(spark, lake, broker, workload)
    counts = _assert_silver_is_expected(spark, lake, _deliveries(broker, workload))
    evidence = _invariants(spark, lake, broker, label=f"{name} final", silver=True)
    for target, path in tables.items():
        versions = _txn_commits(path, _identity(lake, SILVER.query).app_id)
        assert len(versions) == len(set(versions)), f"{target} committed a batch twice"
    assert checkpoints.read_state(lake, SILVER.query).versions == (1,), "a resume needed no reset"
    _record(name, {"final": {"bronze_rows": rows, "windows": windows, **counts, **evidence}})
    assert any(window.startswith("inside_batch") for window in windows), (
        f"no kill landed inside a batch: {windows}"
    )


def test_inject_crash_after_the_sink_commit_before_spark_records_the_batch(
    broker: Broker,  # noqa: F811
    spark: Any,  # noqa: F811
    jobs: Jobs,
    tmp_path: Path,
) -> None:
    """Injection 2: the window in which Spark re-runs a batch Delta has already committed.

    (a) the job dies in that window and the re-run is idempotent; (b) the batch's offsets entry is
    lost as well, and `decide_start` refuses rather than re-planning the batch over a wider range,
    which Delta would skip whole (ADR-0048 (b))."""
    name = "bronze-commit-window"
    _fresh(broker, TOPIC)
    lake = LakeConfig.at(tmp_path / "lake")
    aside = tmp_path / "aside"
    aside.mkdir()
    workload = Workload(random.Random(f"{SEED}:{name}:workload"))
    workload.publish(broker, 48, every_kind=True)
    args = _bronze_args(broker, *DRAIN, "--max-offsets-per-trigger", "6")
    table = BRONZE.table.local_path(lake)

    job = jobs.launch(
        lake, "bronze", args, harness=("--kill-at", "after_sink", "--from-batch", "2")
    )
    assert jobs.wait(job, START_TIMEOUT_S) == SIGKILLED, job.tail()
    batch = int(_harness_kill(job)["batch_id"])
    directory = _directory(lake, BRONZE.query)
    app_id = _identity(lake, BRONZE.query).app_id
    progress = SparkProgress.read(directory)
    assert batch in progress.planned and batch not in progress.committed
    assert progress.last_committed == batch - 1
    assert _txn_commits(table, app_id)[-1] == batch, "the sink's commit landed before the kill"
    offsets_entry = (directory / "offsets" / str(batch)).read_bytes()
    before = {
        (row.partition, row.offset): (row.batch_id, row.value)
        for row in _landed(spark, lake, TOPIC)
    }
    crashed = _invariants(spark, lake, broker, label=f"{name} (a) crashed", silver=False)
    _record(
        name,
        {
            "injection": "a: process death after the sink commit",
            "batch": batch,
            "window": _window(progress, {"bronze": _txn_commits(table, app_id)}),
            "after_kill": crashed,
        },
    )

    workload.publish(broker, 30)  # new records, so a re-planned batch would be a wider one
    resumed = jobs.launch(lake, "bronze", args)
    assert jobs.wait(resumed, DRAIN_TIMEOUT_S) == EXIT_OK, resumed.tail()
    assert "checkpoint_resumed" in resumed.log()
    assert (directory / "offsets" / str(batch)).read_bytes() == offsets_entry, (
        "Spark re-ran the batch from the offsets it had recorded"
    )
    progress = SparkProgress.read(directory)
    assert batch in progress.committed
    versions = _txn_commits(table, app_id)
    assert versions.count(batch) == 1, "the re-run batch was committed a second time"
    assert len(versions) == len(set(versions))
    after = {
        (row.partition, row.offset): (row.batch_id, row.value)
        for row in _landed(spark, lake, TOPIC)
    }
    assert {key: value for key, value in after.items() if key in before} == before, (
        "the replayed batch's rows were rewritten"
    )
    rows = _assert_bronze_is_kafka(spark, lake, broker, workload)
    _record(
        name,
        {
            "injection": "a",
            "outcome": "resumed: the batch re-ran from its recorded offsets and Delta skipped it",
            "rows": rows,
            "after_resume": _invariants(spark, lake, broker, label=f"{name} (a)", silver=False),
        },
    )

    workload.publish(broker, 30)
    first = (progress.last_committed or 0) + 1
    job = jobs.launch(
        lake, "bronze", args, harness=("--kill-at", "after_sink", "--from-batch", str(first))
    )
    assert jobs.wait(job, START_TIMEOUT_S) == SIGKILLED, job.tail()
    lost = int(_harness_kill(job)["batch_id"])
    assert _txn_commits(table, app_id)[-1] == lost
    moved = _move_aside(directory / "offsets", lost, aside)
    progress = SparkProgress.read(directory)
    assert lost not in progress.planned and progress.last_committed == lost - 1
    workload.publish(broker, 30)
    table_version = _log_version(table)
    refused = jobs.launch(lake, "bronze", args)
    assert jobs.wait(refused, START_TIMEOUT_S) == EXIT_REFUSED, refused.tail()
    log = refused.log()
    assert "refusing to start" in log and "silently skip" in log, refused.tail()
    assert re.search(
        rf"recorded batch {lost} for \S+ but checkpoint v1 has committed up to batch {lost - 1}",
        log,
    ) or re.search(
        rf"recorded batch {lost} for [^,]+, but checkpoint v1 has committed up to batch {lost - 1}",
        log,
    ), refused.tail()
    assert _log_version(table) == table_version, "the refused start wrote something"
    _record(
        name,
        {
            "injection": "b: the same window with the batch's offsets entry lost",
            "batch": lost,
            "outcome": f"refused to start, exit {EXIT_REFUSED}, nothing written",
        },
    )

    _restore(moved)
    recovered = jobs.launch(lake, "bronze", args)
    assert jobs.wait(recovered, DRAIN_TIMEOUT_S) == EXIT_OK, recovered.tail()
    rows = _assert_bronze_is_kafka(spark, lake, broker, workload)
    versions = _txn_commits(table, app_id)
    assert len(versions) == len(set(versions))
    _record(
        name,
        {
            "injection": "b",
            "outcome": "the restored checkpoint resumed and lost nothing",
            "rows": rows,
            "after_resume": _invariants(spark, lake, broker, label=f"{name} (b)", silver=False),
        },
    )


def test_inject_crash_between_silver_s_per_target_commits(
    broker: Broker,  # noqa: F811
    spark: Any,  # noqa: F811
    jobs: Jobs,
    tmp_path: Path,
) -> None:
    """Injection 3: Silver dies at each point its commit order allows, and every target converges.

    The sink commits quarantine, duplicates, the canonical MERGE, then late_events, so those are the
    states a crash can leave. Each chunk carries every kind, so each batch writes all four."""
    name = "silver-commit-points"
    _fresh(broker, TOPIC)
    lake = LakeConfig.at(tmp_path / "lake")
    aside = tmp_path / "aside"
    aside.mkdir()
    workload = Workload(random.Random(f"{SEED}:{name}:workload"))
    tables = _silver_tables(lake)
    points = (
        "after_quarantine",
        "after_duplicates",
        "after_canonical",
        "after_late_events",
        "after_sink",
    )

    for point in points:
        workload.publish(broker, 18, every_kind=True)
        _ingest(spark, lake, broker, TOPIC)
        planned = _progress(lake, SILVER.query).planned
        batch = max(planned) + 1 if planned else 0
        job = jobs.launch(
            lake,
            "silver",
            _silver_args(*DRAIN),
            harness=("--kill-at", point, "--from-batch", str(batch)),
        )
        assert jobs.wait(job, START_TIMEOUT_S) == SIGKILLED, job.tail()
        killed = _harness_kill(job)
        assert (killed["point"], killed["batch_id"]) == (point, batch), killed
        app_id = _identity(lake, SILVER.query).app_id
        progress = _progress(lake, SILVER.query)
        assert batch in progress.planned and batch not in progress.committed
        commits = {target: _txn_commits(path, app_id) for target, path in tables.items()}
        landed = tuple(target for target in SILVER_COMMIT_ORDER if batch in commits[target])
        reached = point.removeprefix("after_")
        expected = (
            SILVER_COMMIT_ORDER
            if reached == "sink"
            else SILVER_COMMIT_ORDER[: SILVER_COMMIT_ORDER.index(reached) + 1]
        )
        assert landed == expected, f"{point}: {landed} committed, expected {expected}"
        crashed = _invariants(spark, lake, broker, label=f"{name} {point} crashed", silver=True)

        resumed = jobs.launch(lake, "silver", _silver_args(*DRAIN))
        assert jobs.wait(resumed, DRAIN_TIMEOUT_S) == EXIT_OK, resumed.tail()
        assert batch in _progress(lake, SILVER.query).committed
        commits = {target: _txn_commits(path, app_id) for target, path in tables.items()}
        assert {
            target: versions.count(batch) for target, versions in commits.items()
        } == dict.fromkeys(SILVER_COMMIT_ORDER, 1), (
            "every target must hold exactly one commit for the batch after the resume"
        )
        counts = _assert_silver_is_expected(spark, lake, _deliveries(broker, workload))
        _record(
            name,
            {
                "injection": point,
                "batch": batch,
                "landed_at_kill": list(landed),
                "after_kill": crashed,
                "after_resume": {
                    **counts,
                    **_invariants(spark, lake, broker, label=f"{name} {point}", silver=True),
                },
            },
        )

    workload.publish(broker, 18, every_kind=True)
    _ingest(spark, lake, broker, TOPIC)
    planned = _progress(lake, SILVER.query).planned
    batch = max(planned) + 1
    job = jobs.launch(
        lake,
        "silver",
        _silver_args(*DRAIN),
        harness=("--kill-at", "after_sink", "--from-batch", str(batch)),
    )
    assert jobs.wait(job, START_TIMEOUT_S) == SIGKILLED, job.tail()
    moved = _move_aside(_directory(lake, SILVER.query) / "offsets", batch, aside)
    workload.publish(broker, 18, every_kind=True)
    _ingest(spark, lake, broker, TOPIC)
    versions_before = {target: _log_version(path) for target, path in tables.items()}
    refused = jobs.launch(lake, "silver", _silver_args(*DRAIN))
    assert jobs.wait(refused, START_TIMEOUT_S) == EXIT_REFUSED, refused.tail()
    assert "refusing to start" in refused.log() and "silently skip" in refused.log()
    assert {target: _log_version(path) for target, path in tables.items()} == versions_before, (
        "the refused start wrote something"
    )
    _record(
        name,
        {
            "injection": "after_sink with the batch's offsets entry lost",
            "batch": batch,
            "outcome": f"refused to start, exit {EXIT_REFUSED}, no target written",
        },
    )
    _restore(moved)
    recovered = jobs.launch(lake, "silver", _silver_args(*DRAIN))
    assert jobs.wait(recovered, DRAIN_TIMEOUT_S) == EXIT_OK, recovered.tail()
    counts = _assert_silver_is_expected(spark, lake, _deliveries(broker, workload))
    _record(
        name,
        {
            "injection": "restored offsets entry",
            "outcome": "resumed and converged",
            **counts,
            **_invariants(spark, lake, broker, label=f"{name} restored", silver=True),
        },
    )


def test_inject_replay_from_arbitrary_offsets_into_a_fresh_lake(
    broker: Broker,  # noqa: F811
    spark: Any,  # noqa: F811
    jobs: Jobs,
    tmp_path: Path,
) -> None:
    """Injection 5: lake A reads the topic whole; lake B replays a chosen range per partition.

    B's Bronze rows equal A's byte for byte where the ranges overlap, and Silver over B holds the
    same canonical rows as A for every identity whose deliveries all lie in the range."""
    name = "replay-from-offsets"
    rng = random.Random(f"{SEED}:{name}:offsets")
    _fresh(broker, TOPIC)
    workload = Workload(random.Random(f"{SEED}:{name}:workload"))
    workload.publish(broker, 150, every_kind=True)

    lake_a = LakeConfig.at(tmp_path / "lake-a")
    _ingest(spark, lake_a, broker, TOPIC)
    _silver_here(spark, lake_a)
    ends = _watermarks(broker, TOPIC)
    report_a = _conservation(spark, lake_a, broker, TOPIC)
    assert report_a.conserved, report_a.summary()
    assert {p: r.end for p, r in report_a.consumed.ranges.items()} == ends
    _assert_silver_is_expected(spark, lake_a, _deliveries(broker, workload))
    assert all(end >= 3 for end in ends.values()), f"every partition needs records: {ends}"
    starts = {partition: rng.randint(1, end - 1) for partition, end in sorted(ends.items())}

    workload.publish(broker, 60)  # past A's end: B's range is bounded by A's, not by the topic's
    later = _watermarks(broker, TOPIC)

    lake_b = LakeConfig.at(tmp_path / "lake-b")
    create_table(
        spark, bronze_declaration(TOPIC), lake_b, CommitProvenance(SHA, False, BRONZE.query)
    )
    explicit = json.dumps({TOPIC: {str(p): o for p, o in starts.items()}})
    opened = checkpoints.open_checkpoint(
        spark,
        lake_b,
        BRONZE.query,
        targets=[BRONZE.table],
        sources=[KafkaSourceStart(TOPIC, explicit)],
        git_sha=SHA,
        dirty_worktree=False,
        now=dt.datetime.now(dt.UTC),
    )
    assert opened.action is StartAction.CREATE
    job = jobs.launch(lake_b, "bronze", _bronze_args(broker, *DRAIN))
    assert jobs.wait(job, DRAIN_TIMEOUT_S) == EXIT_OK, job.tail()

    landed_b = _landed(spark, lake_b, TOPIC)
    assert sorted(_as_bronze_holds(row) for row in landed_b) == _broker_view(
        broker, TOPIC, starts
    ), "B is not what the broker holds from the chosen offsets"
    assert {p: min(r.offset for r in landed_b if r.partition == p) for p in starts} == starts
    report_b = _conservation(spark, lake_b, broker, TOPIC)
    assert report_b.conserved, report_b.summary()
    assert dict(report_b.consumed.ranges) == {p: OffsetRange(starts[p], later[p]) for p in starts}
    rows_a = _content(
        spark, BRONZE.table.local_path(lake_a), BRONZE_CONTENT, ("kafka_partition", "kafka_offset")
    )
    rows_b = _content(
        spark, BRONZE.table.local_path(lake_b), BRONZE_CONTENT, ("kafka_partition", "kafka_offset")
    )

    def shared(key: tuple[Any, ...]) -> bool:
        return starts[int(key[0])] <= int(key[1]) < ends[int(key[0])]

    overlap_a = {key: value for key, value in rows_a.items() if shared(key)}
    assert overlap_a == {key: value for key, value in rows_b.items() if shared(key)}
    assert len(overlap_a) == sum(ends[p] - starts[p] for p in starts)

    _silver_here(spark, lake_b)
    report_silver = check_silver_conservation(spark, lake_b, TOPIC)
    assert report_silver.conserved, report_silver.summary()
    everything = _deliveries(broker, workload)
    in_range = [d for d in everything if d.offset >= starts[d.partition]]
    _assert_silver_is_expected(spark, lake_b, in_range)

    identities: dict[str, list[Delivery]] = {}
    for delivery in everything:
        if delivery.sent.kind not in {"poison", "future"}:
            assert delivery.sent.transaction_id is not None
            identities.setdefault(delivery.sent.transaction_id, []).append(delivery)
    inside = {
        identity
        for identity, items in identities.items()
        if all(starts[d.partition] <= d.offset < ends[d.partition] for d in items)
    }
    straddling = {
        identity
        for identity, items in identities.items()
        if any(d.offset < starts[d.partition] for d in items)
        and any(starts[d.partition] <= d.offset < ends[d.partition] for d in items)
    }
    assert len(inside) >= 5 and straddling, (len(inside), len(straddling))
    columns = tuple(f for f in canonical_schema(TOPIC).fieldNames() if f not in SILVER_LINEAGE)
    canonical_a = _content(spark, SILVER.table.local_path(lake_a), columns, ("silver_identity",))
    canonical_b = _content(spark, SILVER.table.local_path(lake_b), columns, ("silver_identity",))
    keys = {(identity,) for identity in inside}
    assert keys <= set(canonical_a) and keys <= set(canonical_b)
    assert {k: v for k, v in canonical_a.items() if k in keys} == {
        k: v for k, v in canonical_b.items() if k in keys
    }, "an identity wholly inside the replayed range has a different canonical row in B"
    _record(
        name,
        {
            "injection": "replay from arbitrary offsets into a fresh lake",
            "starts": starts,
            "a_ends": ends,
            "b_ends": later,
            "identities_in_range": len(inside),
            "identities_straddling_the_start": len(straddling),
            "bronze_rows_compared": len(overlap_a),
            "outcome": "B equals Kafka and A byte for byte in range; canonical rows equal",
            "started_after_trim": report_b.started_after_trim,
        },
    )


def test_inject_kill_of_the_spark_jvm_mid_stream_then_resume(
    broker: Broker,  # noqa: F811
    spark: Any,  # noqa: F811
    jobs: Jobs,
    tmp_path: Path,
) -> None:
    """Injection 6, ROADMAP's manual validation: the local-mode Spark JVM killed mid-stream under
    Silver and under Bronze, then resume, ending with Gold's build and its consistency check."""
    name = "spark-jvm-kill"
    rng = random.Random(f"{SEED}:{name}:schedule")
    delays = {"silver": rng.randint(0, 3000), "bronze": rng.randint(0, 1500)}
    _record(name, {"schedule": {"delay_ms": delays, "order": ["silver", "bronze"]}})
    _fresh(broker, TOPIC)
    lake = LakeConfig.at(tmp_path / "lake")
    workload = Workload(random.Random(f"{SEED}:{name}:workload"))
    workload.publish(broker, 60, every_kind=True)

    bronze = jobs.launch(
        lake, "bronze", _bronze_args(broker, "--interval-s", "1", "--max-offsets-per-trigger", "8")
    )
    _await(
        lambda: bool(_progress(lake, BRONZE.query).committed),
        "Bronze's first batch",
        timeout_s=START_TIMEOUT_S,
        job=bronze,
        poll_s=0.2,
    )
    feeder = Feeder(workload, broker, every_s=2.0, count=8)
    feeder.start()
    try:
        silver = jobs.launch(lake, "silver", _silver_args("--interval-s", "1"))
        _await_inside_batch(lake, SILVER.query, silver, after=frozenset())
        time.sleep(delays["silver"] / 1000)
        outcome = _kill_the_jvm(jobs, silver)
        evidence = _invariants(spark, lake, broker, label=f"{name} silver JVM", silver=True)
        _record(
            name, {"injection": "Spark JVM of the Silver job", **outcome, "after_kill": evidence}
        )

        silver = jobs.launch(lake, "silver", _silver_args("--interval-s", "1"))
        committed = _progress(lake, BRONZE.query).committed
        _await_inside_batch(lake, BRONZE.query, bronze, after=committed)
        time.sleep(delays["bronze"] / 1000)
        outcome = _kill_the_jvm(jobs, bronze)
        evidence = _invariants(spark, lake, broker, label=f"{name} bronze JVM", silver=True)
        _record(
            name, {"injection": "Spark JVM of the Bronze job", **outcome, "after_kill": evidence}
        )

        bronze = jobs.launch(
            lake,
            "bronze",
            _bronze_args(broker, "--interval-s", "1", "--max-offsets-per-trigger", "8"),
        )
        committed = _progress(lake, SILVER.query).committed
        _await(
            lambda: len(_progress(lake, SILVER.query).committed - committed) >= 1,
            "Silver to commit a batch after both JVMs were killed",
            timeout_s=START_TIMEOUT_S,
            job=silver,
            poll_s=0.2,
        )
    finally:
        feeder.stop()
    assert jobs.stop(silver) == EXIT_OK, silver.tail()
    assert jobs.stop(bronze) == EXIT_OK, bronze.tail()

    drain_bronze = jobs.launch(lake, "bronze", _bronze_args(broker, *DRAIN))
    assert jobs.wait(drain_bronze, DRAIN_TIMEOUT_S) == EXIT_OK, drain_bronze.tail()
    drain_silver = jobs.launch(lake, "silver", _silver_args(*DRAIN))
    assert jobs.wait(drain_silver, DRAIN_TIMEOUT_S) == EXIT_OK, drain_silver.tail()
    rows = _assert_bronze_is_kafka(spark, lake, broker, workload)
    counts = _assert_silver_is_expected(spark, lake, _deliveries(broker, workload))
    evidence = _invariants(spark, lake, broker, label=f"{name} final", silver=True)
    gold = _gold_final_state(spark, lake, jobs, expected_observations=counts["canonical"])
    _record(
        name,
        {
            "final": {
                "bronze_rows": rows,
                **counts,
                **evidence,
                "gold_build": gold["build"],
                "gold_check": gold["check"],
            }
        },
    )


def _await_inside_batch(lake: LakeConfig, query: str, job: Job, *, after: frozenset[int]) -> None:
    _await(
        lambda: len(_progress(lake, query).committed - after) >= 1,
        f"a committed batch from {job.name}",
        timeout_s=START_TIMEOUT_S,
        job=job,
        poll_s=0.1,
    )

    def planned_not_committed() -> bool:
        progress = _progress(lake, query)
        last = progress.last_committed
        return bool(progress.planned) and (last is None or max(progress.planned) > last)

    _await(
        planned_not_committed,
        f"{job.name} to be inside a batch",
        timeout_s=START_TIMEOUT_S,
        job=job,
        poll_s=0.01,
    )


def _spark_jvm(job: Job) -> int:
    listing = subprocess.run(
        ["ps", "-A", "-ww", "-o", "pid=,ppid=,command="],
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    ).stdout
    children: dict[int, list[tuple[int, str]]] = {}
    for line in listing.splitlines():
        fields = line.split(None, 2)
        if len(fields) == 3 and fields[0].isdigit() and fields[1].isdigit():
            children.setdefault(int(fields[1]), []).append((int(fields[0]), fields[2]))
    found: list[int] = []
    pending = [job.process.pid]
    while pending:
        for pid, command in children.get(pending.pop(), []):
            pending.append(pid)
            if (
                "org.apache.spark.deploy.SparkSubmit" in command
                and "org.apache.spark.launcher.Main" not in command
            ):
                found.append(pid)
    assert len(found) == 1, f"{job.name}: expected one Spark JVM under the driver, found {found}"
    return found[0]


def _kill_the_jvm(jobs: Jobs, job: Job) -> dict[str, Any]:
    """SIGKILL the job's Spark JVM alone, and watch what its Python driver does."""
    pid = _spark_jvm(job)
    began = time.monotonic()
    os.kill(pid, signal.SIGKILL)
    try:
        code = job.process.wait(timeout=JVM_DEATH_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        jobs.kill(job)
        pytest.fail(
            f"{job.name}'s driver outlived its Spark JVM by {JVM_DEATH_TIMEOUT_S:.0f} s without "
            f"exiting: nothing would restart it:\n{job.tail()}"
        )
    _await_group_gone(job.process.pid)
    assert code != EXIT_OK, (
        f"{job.name} exited 0 after its Spark JVM was killed, reporting a clean stop:\n{job.tail()}"
    )
    return {
        "jvm_pid": pid,
        "driver_exit": code,
        "driver_exit_after_s": round(time.monotonic() - began, 1),
    }


def test_inject_broker_restart_during_bronze_ingestion(
    broker: Broker,  # noqa: F811
    spark: Any,  # noqa: F811
    jobs: Jobs,
    tmp_path: Path,
) -> None:
    """Injection 4: the broker restarted under an ingesting Bronze job, gracefully and by SIGKILL.

    Either the job rides through the outage or it stops loudly with its documented exit code; both
    are correct, and which happened is recorded. Nothing may be lost or counted twice either way.
    This test runs last: it disturbs the broker every other test shares."""
    name = "broker-restart"
    _fresh(broker, TOPIC)
    lake = LakeConfig.at(tmp_path / "lake")
    workload = Workload(random.Random(f"{SEED}:{name}:workload"))
    workload.publish(broker, 240, every_kind=True)
    args = _bronze_args(broker, *STREAMING)
    job = jobs.launch(lake, "bronze", args)

    try:
        for mode in ("graceful_restart", "sigkill_then_start"):
            committed = _progress(lake, BRONZE.query).committed

            def before_outage(seen: frozenset[int] = committed) -> bool:
                return len(_progress(lake, BRONZE.query).committed - seen) >= 3

            _await(
                before_outage,
                "three batches before the outage",
                timeout_s=START_TIMEOUT_S,
                job=job,
                poll_s=0.1,
            )
            behind = _offsets_behind(lake, broker)
            assert behind > 0, "the outage must land while Bronze still has records to read"
            began = time.monotonic()
            if mode == "graceful_restart":
                results = [_docker("restart", "-t", "10", broker.name, timeout=300)]
            else:
                results = [
                    _docker("kill", broker.name, timeout=120),
                    _docker("start", broker.name, timeout=300),
                ]
            for result in results:
                assert result.returncode == 0, result.stderr
            _await_broker(broker)
            outage_s = round(time.monotonic() - began, 1)
            workload.publish(broker, 60)
            recorded = _progress(lake, BRONZE.query).committed

            def ingesting_again(seen: frozenset[int] = recorded, running: Job = job) -> bool:
                return (
                    not running.running or len(_progress(lake, BRONZE.query).committed - seen) >= 3
                )

            _await(
                ingesting_again,
                "Bronze to ingest again after the outage, or to stop",
                timeout_s=START_TIMEOUT_S,
                poll_s=0.2,
            )
            if job.running:
                outcome: dict[str, Any] = {"job": "rode through the outage"}
            else:
                code = jobs.wait(job, 60)
                assert code == EXIT_QUERY_FAILED, (
                    f"a job the outage stopped must exit {EXIT_QUERY_FAILED}, not {code}:"
                    f"\n{job.tail()}"
                )
                assert "bronze_query_failed" in job.log(), job.tail()
                outcome = {"job": "stopped loudly", "exit": code}
                job = jobs.launch(lake, "bronze", args)
                restarted = _progress(lake, BRONZE.query).committed

                def restarted_committed(seen: frozenset[int] = restarted) -> bool:
                    return len(_progress(lake, BRONZE.query).committed - seen) >= 1

                _await(
                    restarted_committed,
                    "the restarted Bronze job to commit a batch",
                    timeout_s=START_TIMEOUT_S,
                    job=job,
                    poll_s=0.2,
                )
            evidence = _invariants(spark, lake, broker, label=f"{name} {mode}", silver=False)
            _record(
                name,
                {
                    "injection": mode,
                    "offsets_behind_at_outage": behind,
                    "outage_s": outage_s,
                    **outcome,
                    "after": evidence,
                },
            )
    finally:
        if _docker("inspect", "-f", "{{.State.Running}}", broker.name).stdout.strip() != "true":
            _docker("start", broker.name, timeout=300)
            _await_broker(broker)

    assert jobs.stop(job) == EXIT_OK, job.tail()
    final = jobs.launch(lake, "bronze", _bronze_args(broker, *DRAIN))
    assert jobs.wait(final, DRAIN_TIMEOUT_S) == EXIT_OK, final.tail()
    rows = _assert_bronze_is_kafka(spark, lake, broker, workload)
    evidence = _invariants(spark, lake, broker, label=f"{name} final", silver=False)
    _record(name, {"final": {"rows": rows, **evidence}})


def _offsets_behind(lake: LakeConfig, target: Broker) -> int:
    """Records published that Bronze's last committed batch had not read."""
    progress = _progress(lake, BRONZE.query)
    if progress.last_committed is None:
        return sum(_watermarks(target, TOPIC).values())
    read = checkpoints.read_batch_offsets(
        _directory(lake, BRONZE.query), progress.last_committed, TOPIC
    )
    return sum(_watermarks(target, TOPIC).values()) - sum(read.values())


def _await_broker(target: Broker) -> None:
    from confluent_kafka import KafkaException

    def serving() -> bool:
        try:
            described = _admin(target).list_topics(topic=TOPIC, timeout=5).topics.get(TOPIC)
        except KafkaException:
            return False
        return (
            described is not None
            and described.error is None
            and bool(described.partitions)
            and all(p.leader >= 0 for p in described.partitions.values())
        )

    _await(serving, "the broker to serve the topic again", timeout_s=300, poll_s=1.0)
