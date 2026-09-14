"""Checkpoint conventions: a query's progress, the idempotency identity paired with it, and the
only sanctioned way to write its targets and read its Delta sources.

Layout: `<lake root>/_checkpoints/<query>/v<N>/`, outside every table directory. Each `v<N>`
holds Spark's checkpoint files plus `trace-checkpoint.json`, this module's identity record
(observed on Spark 4.0.1: Spark leaves the extra file alone across a run and a resume).

**The traps this module closes** (each observed on Delta 4.0.1, see
`tests/stream/test_delta_capabilities.py`):

* A `foreachBatch` sink idempotent through `txnAppId`/`txnVersion=batch_id` silently loses
  batches when a NEW checkpoint reuses an OLD app id: batch ids restart at 0 and Delta skips
  every one at or below the highest it recorded. So the app id is minted once, stored in
  the checkpoint, read back on restart, and never accepted from a caller.
* A second idempotent commit to the same table in the same batch is skipped silently. So
  `OpenedCheckpoint.append` and `.merge` perform exactly one commit per target per batch and
  refuse a second.
* MERGE reads its transaction identity from session configuration, which Delta leaves set
  after the commit (while `write.txnVersion.autoReset.enabled` is off, the default). So
  `.merge` sets and always unsets it around one `execute()`.
* A running query reading a Delta source with `failOnDataLoss=false` completes without error
  when the log it needs was cleaned, and never delivers the rows. So Delta sources are read
  only through `OpenedCheckpoint.delta_source`, which forces `failOnDataLoss=true`.

**Starting is refused** (`CheckpointRefusedError`) whenever the checkpoint on disk cannot be
the one that produced the targets' state: no checkpoint while a target holds the query's
commits; commits from a newer or recreated checkpoint version; a target that recorded a batch
this checkpoint never planned; a target dropped and recreated, replaced by an older copy, or
RESTOREd since the checkpoint was opened (Delta's transaction identifiers survive a RESTORE,
so only the retained history shows it); a changed set of targets or sources; or a source whose
start position or table changed. Every table a query writes and every source it reads must be
named; an empty list is refused.

**Reset is explicit and versioned.** `reset_checkpoint` publishes `v<N+1>` with a new app id,
the start position of every source, and a record of what it supersedes and why. Nothing is
deleted. A Kafka source may not start at `latest` (`docs/PHASE3_PLAN.md` §4.1 point 8).

`decide_start` is a pure function of recorded facts, unit-tested without a JVM; the functions
that take a `SparkSession` read those facts from live tables.
"""

from __future__ import annotations

import _thread
import errno
import json
import os
import re
import threading
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast

from trace_core.domain.errors import CheckpointRefusedError, NaiveDatetimeError
from trace_core.observability import get_logger
from trace_core.stream.lake import AppId, LakeConfig, require_identifier
from trace_core.stream.tables import (
    LOSS_TOLERANT_SESSION_CONF,
    CommitProvenance,
    DeltaSourceOffset,
    TableRef,
    require_git_sha,
    require_no_loss_tolerance,
    require_source_retained,
    retained_log,
    snapshot_facts,
    stamped_commits,
)

if TYPE_CHECKING:
    from delta.tables import DeltaMergeBuilder, DeltaTable
    from pyspark.sql import DataFrame, SparkSession

IDENTITY_FILENAME: Final = "trace-checkpoint.json"
IDENTITY_FORMAT: Final = 1
TXN_APP_ID_CONF: Final = "spark.databricks.delta.write.txnAppId"
TXN_VERSION_CONF: Final = "spark.databricks.delta.write.txnVersion"
DEFAULT_HISTORY_LIMIT: Final = 1000

_VERSION_DIR: Final = re.compile(r"v([1-9][0-9]*)")
_BATCH_FILE: Final = re.compile(r"0|[1-9][0-9]*")
_KAFKA_TOPIC: Final = re.compile(r"[a-z0-9][a-z0-9._-]{0,248}")

_log = get_logger(__name__)


def _utc(moment: datetime) -> datetime:
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise NaiveDatetimeError(
            f"checkpoint timestamps must be timezone-aware UTC, got {moment!r}"
        )
    return moment.astimezone(UTC)


# --------------------------------------------------------------------- sources ---


@dataclass(frozen=True, slots=True)
class DeltaSourceStart:
    """A Delta source and where a new checkpoint starts reading it.

    `starting_version=None` reads the table's snapshot, then every later commit."""

    table: TableRef
    starting_version: int | None = None

    def __post_init__(self) -> None:
        if self.starting_version is not None and self.starting_version < 0:
            raise CheckpointRefusedError(f"starting_version {self.starting_version} is negative")

    @property
    def key(self) -> str:
        return f"delta:{self.table}"

    @property
    def kind(self) -> str:
        return "delta"

    @property
    def start(self) -> str:
        return "snapshot" if self.starting_version is None else f"version:{self.starting_version}"


@dataclass(frozen=True, slots=True)
class KafkaSourceStart:
    """A Kafka topic and where a new checkpoint starts reading it: `earliest`, or explicit
    offsets as the JSON Spark's `startingOffsets` accepts. Never `latest`, and never
    unspecified, which for a streaming read means `latest`."""

    topic: str
    starting_offsets: str

    def __post_init__(self) -> None:
        if not _KAFKA_TOPIC.fullmatch(self.topic):
            raise CheckpointRefusedError(f"{self.topic!r} is not a valid topic name")
        text = self.starting_offsets.strip()
        if text.lower() == "latest":
            raise CheckpointRefusedError(
                f"{self.topic}: a checkpoint may not start at latest -- it silently skips "
                f"everything published before it, and docs/PHASE3_PLAN.md §4.1 point 8 forbids "
                f"resetting offsets to latest"
            )
        if text != "earliest":
            try:
                offsets = json.loads(text)
            except ValueError as exc:
                raise CheckpointRefusedError(
                    f"{self.topic}: starting_offsets must be 'earliest' or explicit offsets JSON"
                ) from exc
            if not isinstance(offsets, dict) or set(offsets) != {self.topic}:
                raise CheckpointRefusedError(
                    f"{self.topic}: explicit starting offsets must name exactly this topic"
                )

    @property
    def key(self) -> str:
        return f"kafka:{self.topic}"

    @property
    def kind(self) -> str:
        return "kafka"

    @property
    def start(self) -> str:
        text = self.starting_offsets.strip()
        return text if text == "earliest" else json.dumps(json.loads(text), sort_keys=True)


type SourceStart = DeltaSourceStart | KafkaSourceStart


@dataclass(frozen=True, slots=True)
class SourceRecord:
    """What a checkpoint version recorded about one source when it was opened."""

    kind: str
    start: str
    table_id: str | None


@dataclass(frozen=True, slots=True)
class SourceEvidence:
    """A source as requested now, with its current Delta table id (None for Kafka)."""

    start: SourceStart
    table_id: str | None

    def record(self) -> SourceRecord:
        return SourceRecord(self.start.kind, self.start.start, self.table_id)


@dataclass(frozen=True, slots=True)
class TargetRecord:
    """What a checkpoint version recorded about one target when it was opened."""

    table_id: str
    version: int


@dataclass(frozen=True, slots=True)
class TargetEvidence:
    """What a target table records now about its identity and its writers."""

    table: str
    table_id: str
    version: int
    transactions: Mapping[str, int] = field(default_factory=dict)
    """Delta's transaction identifiers: app id -> highest recorded version (batch id)."""
    provenance: tuple[CommitProvenance, ...] = ()
    """TRACE-X provenance from the table's retained, most recent history."""
    restores: tuple[int, ...] = ()
    """Versions of RESTORE commits in that same retained history."""

    def record(self) -> TargetRecord:
        return TargetRecord(self.table_id, self.version)


# -------------------------------------------------------------------- identity ---


@dataclass(frozen=True, slots=True)
class Supersession:
    """What a reset replaced, and why."""

    version: int
    app_id: str
    reason: str
    reset_at: datetime


@dataclass(frozen=True, slots=True)
class CheckpointIdentity:
    """The record stored in `v<N>/trace-checkpoint.json`."""

    query: str
    version: int
    app_id: str
    created_at: datetime
    targets: Mapping[str, TargetRecord]
    sources: Mapping[str, SourceRecord]
    supersedes: Supersession | None = None

    def __post_init__(self) -> None:
        require_identifier("query name", self.query)
        parsed = AppId.parse(self.app_id)
        if parsed is None or parsed.query != self.query or parsed.version != self.version:
            raise CheckpointRefusedError(
                f"app id {self.app_id!r} is not an app id minted for query {self.query!r} "
                f"checkpoint v{self.version}"
            )
        _utc(self.created_at)
        if not self.targets or not self.sources:
            raise CheckpointRefusedError(
                f"checkpoint v{self.version} of {self.query!r} records no targets or no sources"
            )
        if self.version == 1 and self.supersedes is not None:
            raise CheckpointRefusedError("checkpoint v1 cannot supersede anything")
        if self.version > 1 and (
            self.supersedes is None or self.supersedes.version != self.version - 1
        ):
            raise CheckpointRefusedError(
                f"checkpoint v{self.version} of {self.query!r} does not record superseding "
                f"v{self.version - 1}; later versions are created only by reset_checkpoint"
            )
        if self.supersedes is not None:
            _utc(self.supersedes.reset_at)
            if not self.supersedes.reason.strip():
                raise CheckpointRefusedError(
                    f"checkpoint v{self.version} records a reset without a reason"
                )

    def to_json(self) -> str:
        supersedes = (
            None
            if self.supersedes is None
            else {
                "version": self.supersedes.version,
                "app_id": self.supersedes.app_id,
                "reason": self.supersedes.reason,
                "reset_at": _utc(self.supersedes.reset_at).isoformat(),
            }
        )
        return json.dumps(
            {
                "format": IDENTITY_FORMAT,
                "query": self.query,
                "version": self.version,
                "app_id": self.app_id,
                "created_at": _utc(self.created_at).isoformat(),
                "targets": {
                    name: {"table_id": r.table_id, "version": r.version}
                    for name, r in self.targets.items()
                },
                "sources": {
                    key: {"kind": r.kind, "start": r.start, "table_id": r.table_id}
                    for key, r in self.sources.items()
                },
                "supersedes": supersedes,
            },
            indent=2,
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, text: str, *, source: Path) -> CheckpointIdentity:
        keys = {"format", "query", "version", "app_id", "created_at", "targets", "sources"}
        try:
            data = json.loads(text)
            if data["format"] != IDENTITY_FORMAT:
                raise ValueError(f"unknown format {data['format']!r}")
            if set(data) != keys | {"supersedes"}:
                raise ValueError(f"unexpected keys {sorted(data)}")
            raw = data["supersedes"]
            supersedes = (
                None
                if raw is None
                else Supersession(
                    version=int(raw["version"]),
                    app_id=str(raw["app_id"]),
                    reason=str(raw["reason"]),
                    reset_at=datetime.fromisoformat(raw["reset_at"]),
                )
            )
            return cls(
                query=str(data["query"]),
                version=int(data["version"]),
                app_id=str(data["app_id"]),
                created_at=datetime.fromisoformat(data["created_at"]),
                targets={
                    str(name): TargetRecord(str(r["table_id"]), int(r["version"]))
                    for name, r in data["targets"].items()
                },
                sources={
                    str(key): SourceRecord(
                        str(r["kind"]),
                        str(r["start"]),
                        None if r["table_id"] is None else str(r["table_id"]),
                    )
                    for key, r in data["sources"].items()
                },
                supersedes=supersedes,
            )
        except (ValueError, KeyError, TypeError, AttributeError, NaiveDatetimeError) as exc:
            raise CheckpointRefusedError(f"unreadable checkpoint identity {source}: {exc}") from exc


# ------------------------------------------------------------- recorded facts ---


@dataclass(frozen=True, slots=True)
class SparkProgress:
    """What Spark's own files in a checkpoint directory record."""

    planned: frozenset[int] = frozenset()
    """Batch ids with an `offsets/<n>` file."""
    committed: frozenset[int] = frozenset()
    """Batch ids with a `commits/<n>` file."""
    query_id: str | None = None
    """The persistent query id in `metadata` -- the app id Delta's native streaming sink uses."""

    @property
    def last_committed(self) -> int | None:
        return max(self.committed) if self.committed else None

    @classmethod
    def read(cls, directory: Path) -> SparkProgress:
        def batches(name: str) -> frozenset[int]:
            sub = directory / name
            if not sub.is_dir():
                return frozenset()
            return frozenset(int(p.name) for p in sub.iterdir() if _BATCH_FILE.fullmatch(p.name))

        query_id: str | None = None
        metadata = directory / "metadata"
        if metadata.is_file():
            try:
                query_id = str(json.loads(metadata.read_text().splitlines()[0])["id"])
            except (ValueError, KeyError, IndexError, TypeError) as exc:
                raise CheckpointRefusedError(
                    f"unreadable Spark checkpoint metadata {metadata}: {exc}"
                ) from exc
        return cls(planned=batches("offsets"), committed=batches("commits"), query_id=query_id)


@dataclass(frozen=True, slots=True)
class CheckpointState:
    """What a query's checkpoint directory holds."""

    query: str
    versions: tuple[int, ...] = ()
    identity: CheckpointIdentity | None = None
    """The identity of the highest version, or None when that version has none."""
    progress: SparkProgress = field(default_factory=SparkProgress)
    """Spark's progress in the highest version."""
    unexpected: tuple[str, ...] = ()


class StartAction(StrEnum):
    CREATE = "create"
    RESUME = "resume"


@dataclass(frozen=True, slots=True)
class _Write:
    table: str
    app_id: str
    checkpoint_version: int
    batch_id: int | None
    via: str

    def __str__(self) -> str:
        batch = "" if self.batch_id is None else f", batch {self.batch_id}"
        return f"{self.table} ({self.via} {self.app_id}{batch})"


def _writes_by(query: str, targets: Sequence[TargetEvidence]) -> tuple[list[_Write], list[str]]:
    writes: list[_Write] = []
    problems: list[str] = []
    for target in targets:
        for app_id, batch in sorted(target.transactions.items()):
            parsed = AppId.parse(app_id)
            if parsed is not None and parsed.query == query:
                writes.append(_Write(target.table, app_id, parsed.version, batch, "transaction"))
        for stamp in target.provenance:
            if stamp.query != query or stamp.checkpoint_id is None:
                continue  # a commit made outside a checkpoint (table creation, a batch job)
            parsed = AppId.parse(stamp.checkpoint_id)
            if parsed is None:  # CommitProvenance validates this; kept as a second line
                problems.append(f"{target.table} has an unattributable stamp {stamp}")
                continue
            writes.append(
                _Write(
                    target.table,
                    stamp.checkpoint_id,
                    parsed.version,
                    stamp.batch_id,
                    "commit userMetadata",
                )
            )
    return writes, problems


def _replayable(batch: int, progress: SparkProgress) -> bool:
    """Whether a batch a target recorded is one this checkpoint will not re-plan differently.

    At or below the last committed batch: done. Exactly one past it, with its offsets
    recorded: a crash after the sink committed; Spark replays it from the same offsets and
    Delta skips it (observed). Anything else: new batches would reuse ids the table has seen.
    """
    last = progress.last_committed
    if last is not None and batch <= last:
        return True
    return batch == (-1 if last is None else last) + 1 and batch in progress.planned


_REMEDY: Final = (
    "Restore the checkpoint directory that produced these commits, or -- if reprocessing is the "
    "intent and every sink of this query is idempotent by content -- run reset_checkpoint with a "
    "recorded reason. Never delete or hand-edit a checkpoint."
)


def _target_changes(identity: CheckpointIdentity, targets: Sequence[TargetEvidence]) -> list[str]:
    version = identity.version
    current = {t.table: t for t in targets}
    recorded = identity.targets
    problems: list[str] = []
    if set(current) != set(recorded):
        problems.append(
            f"v{version} was opened for targets {sorted(recorded)}, but the query now names "
            f"{sorted(current)}; changing what a query writes needs reset_checkpoint"
        )
    for name in sorted(set(current) & set(recorded)):
        now, then = current[name], recorded[name]
        if now.table_id != then.table_id:
            problems.append(
                f"{name} is table id {now.table_id}, not {then.table_id} as when v{version} was "
                f"opened: it was dropped and recreated, and rows this checkpoint delivered are gone"
            )
        elif now.version < then.version:
            problems.append(
                f"{name} is at version {now.version}, older than version {then.version} when "
                f"v{version} was opened: it was replaced by an older copy"
            )
        restored = [r for r in now.restores if r > then.version]
        if restored:
            problems.append(
                f"{name} was RESTOREd at version(s) {restored} after v{version} was opened: rows "
                f"this checkpoint delivered may be gone, and Delta's transaction identifiers "
                f"survive a RESTORE, so only the history shows it"
            )
    return problems


def _source_changes(identity: CheckpointIdentity, sources: Sequence[SourceEvidence]) -> list[str]:
    version = identity.version
    current = {s.start.key: s.record() for s in sources}
    recorded = identity.sources
    problems: list[str] = []
    if set(current) != set(recorded):
        problems.append(
            f"v{version} was opened for sources {sorted(recorded)}, but the query now names "
            f"{sorted(current)}; changing what a query reads needs reset_checkpoint"
        )
    for key in sorted(set(current) & set(recorded)):
        now, then = current[key], recorded[key]
        if (now.kind, now.start) != (then.kind, then.start):
            problems.append(
                f"{key} is requested to start at {now.start!r}, but v{version} was opened to start "
                f"at {then.start!r}; a different start position is a reset, not a restart"
            )
        if now.table_id != then.table_id:
            problems.append(
                f"{key} is table id {now.table_id}, not {then.table_id} as when v{version} was "
                f"opened: the source was replaced"
            )
    return problems


def decide_start(
    state: CheckpointState,
    targets: Sequence[TargetEvidence],
    sources: Sequence[SourceEvidence],
) -> StartAction:
    """CREATE a first checkpoint, RESUME the current one, or raise `CheckpointRefusedError`."""
    query = state.query
    if not targets:
        raise CheckpointRefusedError(
            f"refusing to start {query!r}: no target tables were named; every table the query "
            f"writes must be named, or the evidence that makes a restart safe is never read"
        )
    if not sources:
        raise CheckpointRefusedError(
            f"refusing to start {query!r}: no sources were named; a checkpoint records where each "
            f"source starts, which is what makes a reset explicit"
        )
    problems: list[str] = []
    for kind, names in (
        ("targets", [t.table for t in targets]),
        ("sources", [s.start.key for s in sources]),
    ):
        if len(set(names)) != len(names):
            problems.append(f"{kind} named more than once: {sorted(names)}")
    writes, attribution = _writes_by(query, targets)
    problems.extend(attribution)
    if state.unexpected:
        problems.append(
            f"unexpected entries {list(state.unexpected)} in the checkpoint directory of "
            f"{query!r}; only v<N> directories belong there"
        )

    if not state.versions:
        if writes:
            problems.append(
                f"no checkpoint exists for {query!r}, but its commits are already in: "
                + "; ".join(str(w) for w in writes)
            )
        if problems:
            raise CheckpointRefusedError(
                f"refusing to start {query!r}: " + " ".join(problems) + " " + _REMEDY
            )
        return StartAction.CREATE

    current = max(state.versions)
    identity = state.identity
    if identity is None:
        raise CheckpointRefusedError(
            f"refusing to start {query!r}: checkpoint v{current} has no {IDENTITY_FILENAME}, so it "
            f"was not created by these conventions and its transaction app id is unknown. "
            + _REMEDY
        )
    if identity.query != query or identity.version != current:
        problems.append(
            f"v{current} holds the identity of {identity.query!r} v{identity.version}; it was "
            f"copied from elsewhere"
        )
    problems.extend(_target_changes(identity, targets))
    problems.extend(_source_changes(identity, sources))

    this_checkpoint = {identity.app_id} | (
        {state.progress.query_id} if state.progress.query_id else set()
    )
    for write in writes:
        if write.checkpoint_version > identity.version:
            problems.append(
                f"{write} comes from checkpoint v{write.checkpoint_version}, newer than "
                f"v{identity.version} on disk: the checkpoint directory is stale or was restored "
                f"from a copy"
            )
        elif write.checkpoint_version == identity.version and write.app_id != identity.app_id:
            problems.append(
                f"{write} comes from another v{identity.version} than the one on disk "
                f"({identity.app_id}): the version directory was recreated"
            )
        elif (
            identity.supersedes is not None
            and write.checkpoint_version == identity.supersedes.version
            and write.app_id != identity.supersedes.app_id
        ):
            problems.append(
                f"{write} comes from a v{write.checkpoint_version} the reset to "
                f"v{identity.version} "
                f"did not supersede ({identity.supersedes.app_id})"
            )

    for target in targets:
        recorded: list[tuple[str, int]] = [
            (app_id, batch)
            for app_id, batch in target.transactions.items()
            if app_id in this_checkpoint
        ]
        recorded += [
            (stamp.checkpoint_id, stamp.batch_id)
            for stamp in target.provenance
            if stamp.checkpoint_id in this_checkpoint and stamp.batch_id is not None
        ]
        for app_id, batch in sorted(recorded):
            if not _replayable(batch, state.progress):
                problems.append(
                    f"{target.table} recorded batch {batch} for {app_id}, but checkpoint "
                    f"v{identity.version} has committed up to batch "
                    f"{state.progress.last_committed} and planned "
                    f"{sorted(state.progress.planned)[-3:]}: Delta would silently skip every new "
                    f"batch numbered up to {batch}"
                )

    if problems:
        raise CheckpointRefusedError(
            f"refusing to start {query!r}: " + " ".join(problems) + " " + _REMEDY
        )
    return StartAction.RESUME


# ------------------------------------------------------------------ filesystem ---


def query_directory(lake: LakeConfig, query: str) -> Path:
    return lake.checkpoints_root / require_identifier("query name", query)


def read_state(lake: LakeConfig, query: str) -> CheckpointState:
    directory = query_directory(lake, query)
    if not directory.exists():
        return CheckpointState(query=query)
    if not directory.is_dir():
        raise CheckpointRefusedError(f"{directory} is not a directory")
    versions: list[int] = []
    unexpected: list[str] = []
    for entry in sorted(directory.iterdir()):
        if entry.name.startswith("."):
            continue  # a publication in progress, or one a crash abandoned (see _publish)
        match = _VERSION_DIR.fullmatch(entry.name)
        if match is not None and entry.is_dir():
            versions.append(int(match.group(1)))
        else:
            unexpected.append(entry.name)
    if not versions:
        return CheckpointState(query=query, unexpected=tuple(unexpected))
    current = directory / f"v{max(versions)}"
    identity_path = current / IDENTITY_FILENAME
    identity = (
        CheckpointIdentity.from_json(identity_path.read_text(), source=identity_path)
        if identity_path.is_file()
        else None
    )
    return CheckpointState(
        query=query,
        versions=tuple(sorted(versions)),
        identity=identity,
        progress=SparkProgress.read(current),
        unexpected=tuple(unexpected),
    )


def _publish(directory: Path, identity: CheckpointIdentity) -> bool:
    """Create `v<N>` already holding its identity, atomically; False if `v<N>` exists.

    The identity is written into a hidden staging directory which is then renamed into
    place, so a crash never leaves a `v<N>` without its identity, and two processes creating
    the same version cannot both succeed."""
    directory.mkdir(parents=True, exist_ok=True)
    staging = directory / f".v{identity.version}.staging-{uuid.uuid4().hex}"
    staging.mkdir()
    staged_identity = staging / IDENTITY_FILENAME
    with staged_identity.open("x", encoding="utf-8") as handle:
        handle.write(identity.to_json())
        handle.flush()
        os.fsync(handle.fileno())
    target = directory / f"v{identity.version}"
    try:
        if target.exists():
            raise FileExistsError(errno.EEXIST, "exists", str(target))
        staging.rename(target)
    except OSError as exc:
        staged_identity.unlink(missing_ok=True)
        staging.rmdir()
        if exc.errno in (errno.EEXIST, errno.ENOTEMPTY):
            return False
        raise
    return True


def committed_source_offsets(directory: Path) -> tuple[str | None, ...]:
    """Every source's offset a restart of this checkpoint resumes from, in the log's order.

    That is the offset of the last committed batch (a planned but uncommitted batch re-runs
    from there), or, before any commit, the end offset of the first planned batch. Empty when
    nothing was ever planned. Spark's offset log: `v1`, a metadata line, one line per source,
    `-` for none."""
    progress = SparkProgress.read(directory)
    if progress.last_committed is not None:
        batch = progress.last_committed
    elif progress.planned:
        batch = min(progress.planned)
    else:
        return ()
    path = directory / "offsets" / str(batch)
    lines = path.read_text().splitlines()
    if not lines or lines[0] != "v1" or len(lines) < 3:
        raise CheckpointRefusedError(f"unrecognised Spark offset log {path}: {lines[:1]}")
    return tuple(None if line.strip() in ("", "-") else line.strip() for line in lines[2:])


# -------------------------------------------------------------------- writing ---


def _refuse_session_txn(conf: Any) -> None:
    present = [
        key for key in (TXN_APP_ID_CONF, TXN_VERSION_CONF) if conf.get(key, None) is not None
    ]
    if present:
        raise CheckpointRefusedError(
            f"{present} already set on this session: a leaked transaction identity silently skips "
            f"later writes, so it is refused rather than overwritten"
        )


@contextmanager
def _session_txn(conf: Any, app_id: str, batch_id: int) -> Iterator[None]:
    """Both transaction keys for the block, set inside the `try` so a failure setting the second
    cannot leak the first. Session configuration is shared by every thread using the session."""
    _refuse_session_txn(conf)
    try:
        conf.set(TXN_APP_ID_CONF, app_id)
        conf.set(TXN_VERSION_CONF, str(batch_id))
        yield
    finally:
        conf.unset(TXN_APP_ID_CONF)
        conf.unset(TXN_VERSION_CONF)


@dataclass(frozen=True, slots=True)
class OpenedCheckpoint:
    """A checkpoint a query may run from, and the only way it writes its targets."""

    identity: CheckpointIdentity
    directory: Path
    action: StartAction
    lake: LakeConfig
    git_sha: str
    dirty_worktree: bool
    _claims: set[tuple[int, str]] = field(default_factory=set, compare=False, repr=False)
    _lock: _thread.LockType = field(default_factory=threading.Lock, compare=False, repr=False)

    def provenance(self, batch_id: int | None) -> CommitProvenance:
        return CommitProvenance(
            git_sha=self.git_sha,
            dirty_worktree=self.dirty_worktree,
            query=self.identity.query,
            checkpoint_id=self.identity.app_id,
            batch_id=batch_id,
        )

    def _claim(self, batch_id: int, target: TableRef) -> Path:
        """Reserve the one idempotent commit `target` may receive in `batch_id`.

        Kept even if the commit then fails: whether Delta committed before the error is
        unknown, and a retry inside the same batch would be skipped if it had."""
        name = str(target)
        if batch_id < 0:
            raise CheckpointRefusedError(f"batch id {batch_id} is negative")
        if name not in self.identity.targets:
            raise CheckpointRefusedError(
                f"{name} is not a target of checkpoint v{self.identity.version} of "
                f"{self.identity.query!r} ({sorted(self.identity.targets)})"
            )
        with self._lock:
            if (batch_id, name) in self._claims:
                raise CheckpointRefusedError(
                    f"a second idempotent commit to {name} in batch {batch_id}: Delta would "
                    f"skip it "
                    f"silently (observed), so combine the writes into one commit"
                )
            self._claims.add((batch_id, name))
        return target.local_path(self.lake)

    def append(self, frame: DataFrame, *, batch_id: int, target: TableRef) -> None:
        """Append `frame` to `target` as batch `batch_id`'s one idempotent, stamped commit."""
        path = self._claim(batch_id, target)
        session: Any = frame.sparkSession
        _refuse_session_txn(session.conf)
        options = {
            "txnAppId": self.identity.app_id,
            "txnVersion": str(batch_id),
            **self.provenance(batch_id).writer_options(),
        }
        frame.write.format("delta").mode("append").options(**options).save(str(path))

    def merge(
        self,
        session: SparkSession,
        *,
        batch_id: int,
        target: TableRef,
        build: Callable[[DeltaTable], DeltaMergeBuilder],
    ) -> None:
        """Run one MERGE into `target` as batch `batch_id`'s one idempotent, stamped commit.

        `build` receives `target` itself and returns the unexecuted merge; `execute()` is
        called exactly once, here, with the transaction identity set only around it. Use the
        session a `foreachBatch` function receives."""
        from delta.tables import DeltaTable

        path = self._claim(batch_id, target)
        builder = build(DeltaTable.forPath(session, str(path)))
        with (
            _session_txn(session.conf, self.identity.app_id, batch_id),
            stamped_commits(session, self.provenance(batch_id)),
        ):
            builder.execute()

    def kafka_options(self, topic: str) -> dict[str, str]:
        """The start position this checkpoint recorded for `topic`, and `failOnDataLoss=true`."""
        record = self.identity.sources.get(f"kafka:{topic}")
        if record is None or record.kind != "kafka":
            raise CheckpointRefusedError(f"kafka:{topic} is not a source of this checkpoint")
        return {"startingOffsets": record.start, "failOnDataLoss": "true"}

    def delta_source(
        self,
        spark: SparkSession,
        source: TableRef,
        options: Mapping[str, str] | None = None,
    ) -> DataFrame:
        """A streaming read of `source` that cannot silently skip data, or a refusal.

        Always `failOnDataLoss=true`; the start position is the one this checkpoint recorded;
        loss-tolerant settings on the reader or the session are refused; and a restart whose
        checkpoint needs log versions the source no longer retains is refused before start.
        Session settings changed after this call are not seen."""
        record = self.identity.sources.get(f"delta:{source}")
        if record is None or record.kind != "delta" or record.table_id is None:
            raise CheckpointRefusedError(f"delta:{source} is not a source of this checkpoint")
        extra = dict(options or {})
        owned = sorted(
            key
            for key in extra
            if key.lower() in {"startingversion", "startingtimestamp", "failondataloss"}
        )
        if owned:
            raise CheckpointRefusedError(
                f"{owned} may not be passed: the start position comes from the checkpoint and "
                f"failOnDataLoss is always true"
            )
        conf: Any = spark.conf
        require_no_loss_tolerance(
            session_conf={key: str(conf.get(key, "false")) for key in LOSS_TOLERANT_SESSION_CONF},
            source_options=extra,
        )
        path = source.local_path(self.lake)
        facts = snapshot_facts(spark, path)
        if facts is None or facts.table_id != record.table_id:
            raise CheckpointRefusedError(
                f"source {source} at {path} is "
                + ("absent" if facts is None else f"table id {facts.table_id}")
                + f", not table id {record.table_id} as when v{self.identity.version} was opened"
            )
        recorded_delta_ids = {
            r.table_id for r in self.identity.sources.values() if r.kind == "delta"
        }
        for text in committed_source_offsets(self.directory):
            if text is None or not DeltaSourceOffset.is_delta_offset(text):
                continue
            offset = DeltaSourceOffset.parse(text)
            if offset.reservoir_id not in recorded_delta_ids:
                raise CheckpointRefusedError(
                    f"the checkpoint tracks a Delta source with table id {offset.reservoir_id}, "
                    f"which is none of the sources v{self.identity.version} was opened for"
                )
            if offset.reservoir_id == record.table_id:
                require_source_retained(
                    source=str(source),
                    offset=offset,
                    table_id=facts.table_id,
                    retained=retained_log(path),
                )
        reader: Any = spark.readStream.format("delta").option("failOnDataLoss", "true")
        if record.start.startswith("version:"):
            reader = reader.option("startingVersion", record.start.split(":", 1)[1])
        for key, value in sorted(extra.items()):
            reader = reader.option(key, value)
        return cast("DataFrame", reader.load(str(path)))


# -------------------------------------------------------------- opening, reset ---


def read_target_evidence(
    spark: SparkSession,
    lake: LakeConfig,
    target: TableRef,
    *,
    history_limit: int = DEFAULT_HISTORY_LIMIT,
) -> TargetEvidence:
    """A target's identity, version, transaction identifiers, provenance and RESTOREs.

    Transactions come from the snapshot and survive log cleanup. Provenance and RESTOREs come
    from the most recent `history_limit` retained commits, so they are bounded."""
    from delta.tables import DeltaTable

    path = target.local_path(lake)
    facts = snapshot_facts(spark, path)
    if facts is None:
        raise CheckpointRefusedError(
            f"target {target} does not exist at {path}; create it from its declaration "
            f"(tables.create_table) before opening a checkpoint -- a table a query creates "
            f"implicitly loses every NOT NULL (observed)"
        )
    rows = (
        DeltaTable.forPath(spark, str(path))
        .history(history_limit)
        .select("version", "operation", "userMetadata")
        .collect()
    )
    stamps = tuple(
        stamp
        for row in rows
        if (stamp := CommitProvenance.from_user_metadata(row["userMetadata"])) is not None
    )
    restores = tuple(sorted(int(row["version"]) for row in rows if row["operation"] == "RESTORE"))
    return TargetEvidence(
        table=str(target),
        table_id=facts.table_id,
        version=facts.version,
        transactions=facts.transactions,
        provenance=stamps,
        restores=restores,
    )


def read_source_evidence(
    spark: SparkSession, lake: LakeConfig, start: SourceStart
) -> SourceEvidence:
    if isinstance(start, KafkaSourceStart):
        return SourceEvidence(start, None)
    path = start.table.local_path(lake)
    facts = snapshot_facts(spark, path)
    if facts is None:
        raise CheckpointRefusedError(f"source {start.table} does not exist at {path}")
    return SourceEvidence(start, facts.table_id)


def _refuse_empty(query: str, targets: Sequence[object], sources: Sequence[object]) -> None:
    if not targets or not sources:
        raise CheckpointRefusedError(
            f"{query!r}: every target and every source must be named (targets={len(targets)}, "
            f"sources={len(sources)}); an unnamed one is never checked"
        )


def _open(
    lake: LakeConfig,
    query: str,
    targets: Sequence[TargetEvidence],
    sources: Sequence[SourceEvidence],
    *,
    git_sha: str,
    dirty_worktree: bool,
    now: datetime,
) -> OpenedCheckpoint:
    require_git_sha(git_sha)
    directory = query_directory(lake, query)
    for _attempt in range(2):
        state = read_state(lake, query)
        action = decide_start(state, targets, sources)
        if action is StartAction.RESUME:
            assert state.identity is not None  # decide_start refuses a version without one
            _log.info(
                "checkpoint_resumed",
                query=query,
                checkpoint_version=state.identity.version,
                app_id=state.identity.app_id,
                last_committed_batch=state.progress.last_committed,
            )
            return OpenedCheckpoint(
                state.identity,
                directory / f"v{state.identity.version}",
                action,
                lake,
                git_sha,
                dirty_worktree,
            )
        identity = CheckpointIdentity(
            query=query,
            version=1,
            app_id=str(AppId.new(query, 1)),
            created_at=_utc(now),
            targets={t.table: t.record() for t in targets},
            sources={s.start.key: s.record() for s in sources},
        )
        if _publish(directory, identity):
            _log.info(
                "checkpoint_created", query=query, checkpoint_version=1, app_id=identity.app_id
            )
            return OpenedCheckpoint(
                identity, directory / "v1", StartAction.CREATE, lake, git_sha, dirty_worktree
            )
    raise CheckpointRefusedError(
        f"{query!r}: another process kept creating checkpoint v1 concurrently"
    )


def open_checkpoint(
    spark: SparkSession,
    lake: LakeConfig,
    query: str,
    *,
    targets: Sequence[TableRef],
    sources: Sequence[SourceStart],
    git_sha: str,
    dirty_worktree: bool,
    now: datetime,
    history_limit: int = DEFAULT_HISTORY_LIMIT,
) -> OpenedCheckpoint:
    """The checkpoint a query must start from, or `CheckpointRefusedError`.

    Reads the evidence of every named target and source itself, so none can be skipped."""
    _refuse_empty(query, targets, sources)
    return _open(
        lake,
        query,
        [read_target_evidence(spark, lake, ref, history_limit=history_limit) for ref in targets],
        [read_source_evidence(spark, lake, start) for start in sources],
        git_sha=git_sha,
        dirty_worktree=dirty_worktree,
        now=now,
    )


def _reset(
    lake: LakeConfig,
    query: str,
    targets: Sequence[TargetEvidence],
    sources: Sequence[SourceEvidence],
    *,
    reason: str,
    now: datetime,
) -> CheckpointIdentity:
    _refuse_empty(query, targets, sources)
    if not reason.strip():
        raise CheckpointRefusedError(f"resetting {query!r} needs a recorded reason")
    state = read_state(lake, query)
    writes, problems = _writes_by(query, targets)
    if state.unexpected:
        problems.append(f"unexpected entries {list(state.unexpected)} in the checkpoint directory")
    if state.versions and state.identity is None:
        problems.append(f"v{max(state.versions)} has no {IDENTITY_FILENAME}")
    if problems:
        raise CheckpointRefusedError(f"refusing to reset {query!r}: " + " ".join(problems))
    candidates = {(write.checkpoint_version, write.app_id) for write in writes}
    if state.identity is not None:
        candidates.add((state.identity.version, state.identity.app_id))
    if not candidates:
        raise CheckpointRefusedError(
            f"nothing to reset for {query!r}: no checkpoint and no commits; open_checkpoint "
            f"creates v1"
        )
    previous_version = max(version for version, _ in candidates)
    previous_ids = sorted(app_id for version, app_id in candidates if version == previous_version)
    if len(previous_ids) > 1:
        raise CheckpointRefusedError(
            f"refusing to reset {query!r}: v{previous_version} has conflicting app ids "
            f"{previous_ids}; decide which checkpoint is authoritative first"
        )
    moment = _utc(now)
    version = previous_version + 1
    identity = CheckpointIdentity(
        query=query,
        version=version,
        app_id=str(AppId.new(query, version)),
        created_at=moment,
        targets={t.table: t.record() for t in targets},
        sources={s.start.key: s.record() for s in sources},
        supersedes=Supersession(previous_version, previous_ids[0], reason.strip(), moment),
    )
    if not _publish(query_directory(lake, query), identity):
        raise CheckpointRefusedError(f"checkpoint v{version} of {query!r} was created concurrently")
    _log.warning(
        "checkpoint_reset",
        query=query,
        from_version=previous_version,
        to_version=version,
        superseded_app_id=previous_ids[0],
        app_id=identity.app_id,
        sources={key: r.start for key, r in identity.sources.items()},
        reason=reason.strip(),
    )
    return identity


def reset_checkpoint(
    spark: SparkSession,
    lake: LakeConfig,
    query: str,
    *,
    targets: Sequence[TableRef],
    sources: Sequence[SourceStart],
    reason: str,
    now: datetime,
    history_limit: int = DEFAULT_HISTORY_LIMIT,
) -> CheckpointIdentity:
    """Publish checkpoint `v<N+1>`: a new app id, source start positions, what it supersedes.

    `N` is the highest version found on disk OR in the targets' records, so a reset after a
    checkpoint directory was lost still moves past every app id the tables have seen. Nothing is
    deleted. A reset reprocesses each source from its recorded start: safe only when every sink
    is idempotent by content (Silver's insert-only MERGE, §4.2) or its targets are rebuilt."""
    _refuse_empty(query, targets, sources)
    return _reset(
        lake,
        query,
        [read_target_evidence(spark, lake, ref, history_limit=history_limit) for ref in targets],
        [read_source_evidence(spark, lake, start) for start in sources],
        reason=reason,
        now=now,
    )


__all__ = [
    "DEFAULT_HISTORY_LIMIT",
    "IDENTITY_FILENAME",
    "TXN_APP_ID_CONF",
    "TXN_VERSION_CONF",
    "CheckpointIdentity",
    "CheckpointState",
    "DeltaSourceStart",
    "KafkaSourceStart",
    "OpenedCheckpoint",
    "SourceEvidence",
    "SourceRecord",
    "SourceStart",
    "SparkProgress",
    "StartAction",
    "Supersession",
    "TargetEvidence",
    "TargetRecord",
    "committed_source_offsets",
    "decide_start",
    "open_checkpoint",
    "query_directory",
    "read_source_evidence",
    "read_state",
    "read_target_evidence",
    "reset_checkpoint",
]
