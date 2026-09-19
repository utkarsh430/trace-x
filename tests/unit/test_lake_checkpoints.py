"""Checkpoint conventions: identity, sources, the start decision, reset, writes -- no JVM.

`decide_start` is exercised as a matrix of recorded facts; each refused state is one the
stream tests reproduce for real (`tests/stream/test_delta_capabilities.py`), and each is paired
with the nearest state that must NOT be refused, so a guard that refuses everything fails here
as surely as one that refuses nothing.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import uuid
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from trace_core.domain.errors import CheckpointRefusedError, NaiveDatetimeError
from trace_core.stream import checkpoints
from trace_core.stream.checkpoints import (
    CheckpointIdentity,
    CheckpointState,
    DeltaSourceStart,
    KafkaSourceStart,
    OpenedCheckpoint,
    SourceEvidence,
    SourceRecord,
    SparkProgress,
    StartAction,
    Supersession,
    TargetEvidence,
    TargetRecord,
    decide_start,
    read_state,
)
from trace_core.stream.lake import AppId, LakeConfig, Tier
from trace_core.stream.tables import CommitProvenance, TableRef

pytestmark = pytest.mark.unit

NOW = datetime(2026, 9, 13, 8, 0, tzinfo=UTC)
SHA = hashlib.sha1(b"trace-x checkpoint convention tests", usedforsecurity=False).hexdigest()
QUERY = "silver_transform"
SPARK_QUERY_ID = "d2fcc376-0492-4a92-9b3c-6843f861ba11"
TARGET = TableRef(Tier.SILVER, "tx")
TARGET_ID = "0b6f43a0-5a77-4f7e-9d3b-7cb0c8e5c001"
SOURCE_REF = TableRef(Tier.BRONZE, "tx")
SOURCE_ID = "9f1c2a4e-3d5b-4c6a-8e7f-1a2b3c4d5e6f"
SOURCE = SourceEvidence(DeltaSourceStart(SOURCE_REF), SOURCE_ID)
RECORDED_TARGETS = {str(TARGET): TargetRecord(TARGET_ID, 5)}
RECORDED_SOURCES = {SOURCE.start.key: SOURCE.record()}


def identity(version: int = 1, *, supersedes: Supersession | None = None) -> CheckpointIdentity:
    if version > 1 and supersedes is None:
        supersedes = Supersession(version - 1, str(AppId.new(QUERY, version - 1)), "test", NOW)
    return CheckpointIdentity(
        QUERY,
        version,
        str(AppId.new(QUERY, version)),
        NOW,
        RECORDED_TARGETS,
        RECORDED_SOURCES,
        supersedes,
    )


def progress(
    committed: Sequence[int] = (), planned: Sequence[int] = (), query_id: str | None = None
) -> SparkProgress:
    return SparkProgress(frozenset(planned), frozenset(committed), query_id)


def state(
    ident: CheckpointIdentity | None,
    prog: SparkProgress | None = None,
    *,
    versions: tuple[int, ...] | None = None,
) -> CheckpointState:
    found = versions if versions is not None else ((ident.version,) if ident else ())
    return CheckpointState(QUERY, found, ident, prog or SparkProgress())


def target(*pairs: tuple[str, int], **changes: Any) -> TargetEvidence:
    evidence = TargetEvidence(str(TARGET), TARGET_ID, 5, transactions=dict(pairs))
    return replace(evidence, **changes)


def decide(current: CheckpointState, *targets: TargetEvidence) -> StartAction:
    return decide_start(current, list(targets) or [target()], [SOURCE])


# --------------------------------------------------------------------- sources ---


def test_a_kafka_source_may_never_start_at_latest_or_at_an_unspecified_position() -> None:
    for refused in ("latest", " LATEST ", "", "tomorrow", '{"other.topic": {"0": 3}}', "[1]"):
        with pytest.raises(CheckpointRefusedError):
            KafkaSourceStart("tx.raw.v1", refused)
    assert KafkaSourceStart("tx.raw.v1", "earliest").start == "earliest"
    explicit = KafkaSourceStart("tx.raw.v1", '{"tx.raw.v1": {"1": 7, "0": 3}}')
    assert explicit.start == '{"tx.raw.v1": {"0": 3, "1": 7}}'  # canonical, so comparable
    assert explicit.key == "kafka:tx.raw.v1"
    with pytest.raises(CheckpointRefusedError, match="topic"):
        KafkaSourceStart("Bad Topic", "earliest")


def test_a_delta_source_starts_at_its_snapshot_or_an_explicit_version() -> None:
    assert DeltaSourceStart(SOURCE_REF).start == "snapshot"
    assert DeltaSourceStart(SOURCE_REF, 12).start == "version:12"
    assert DeltaSourceStart(SOURCE_REF).key == "delta:bronze.tx"
    with pytest.raises(CheckpointRefusedError, match="negative"):
        DeltaSourceStart(SOURCE_REF, -1)


# -------------------------------------------------------------------- identity ---


def test_the_identity_round_trips_and_its_invariants_hold(tmp_path: Path) -> None:
    ident = identity(2)
    assert CheckpointIdentity.from_json(ident.to_json(), source=tmp_path) == ident
    with pytest.raises(NaiveDatetimeError):
        replace(ident, created_at=datetime(2026, 1, 1))
    with pytest.raises(CheckpointRefusedError, match="only by reset_checkpoint"):
        replace(ident, supersedes=None)
    with pytest.raises(CheckpointRefusedError, match="not an app id minted"):
        replace(ident, app_id=str(AppId.new("other_query", 2)))
    with pytest.raises(CheckpointRefusedError, match="not an app id minted"):
        replace(ident, app_id=f"trace-x:{QUERY}:v2:{'0' * 32}")
    with pytest.raises(CheckpointRefusedError, match="no targets or no sources"):
        replace(ident, targets={})
    with pytest.raises(CheckpointRefusedError, match="cannot supersede"):
        replace(identity(1), supersedes=Supersession(0, "x", "r", NOW))
    data = json.loads(ident.to_json())
    data["surprise"] = 1
    for text in (json.dumps(data), "{}", "not json", ident.to_json().replace('"targets"', '"t"')):
        with pytest.raises(CheckpointRefusedError, match="unreadable checkpoint identity"):
            CheckpointIdentity.from_json(text, source=tmp_path)


def test_no_public_function_or_method_accepts_an_app_id_from_its_caller() -> None:
    """The structural half of the guarantee: an app id can only come from a checkpoint."""
    members = [getattr(checkpoints, name) for name in checkpoints.__all__]
    members += [
        member
        for _, member in inspect.getmembers(OpenedCheckpoint, inspect.isfunction)
        if not member.__name__.startswith("_")
    ]
    for member in members:
        if inspect.isfunction(member):
            assert "app_id" not in inspect.signature(member).parameters, member.__name__


# ---------------------------------------------------------------- decide_start ---


def test_every_target_and_source_must_be_named() -> None:
    with pytest.raises(CheckpointRefusedError, match="no target tables"):
        decide_start(state(None), [], [SOURCE])
    with pytest.raises(CheckpointRefusedError, match="no sources"):
        decide_start(state(None), [target()], [])
    with pytest.raises(CheckpointRefusedError, match="more than once"):
        decide_start(state(None), [target(), target()], [SOURCE])


def test_a_first_start_with_no_evidence_creates() -> None:
    assert decide(state(None)) is StartAction.CREATE
    assert decide(state(None), target((str(AppId.new("other_query", 1)), 9))) is StartAction.CREATE


def test_a_stamp_made_outside_any_checkpoint_is_not_a_checkpoint_write() -> None:
    """create_table stamps the table's creation with the query's name and no checkpoint."""
    setup = CommitProvenance(SHA, False, QUERY)
    assert decide(state(None), target(provenance=(setup,))) is StartAction.CREATE


def test_a_missing_checkpoint_with_the_querys_commits_in_a_target_is_refused() -> None:
    old = identity()
    with pytest.raises(CheckpointRefusedError) as raised:
        decide(state(None), target((old.app_id, 3)))
    text = str(raised.value)
    assert "no checkpoint exists" in text and old.app_id in text and str(TARGET) in text
    assert "Never delete" in text


def test_a_missing_checkpoint_is_also_caught_through_commit_provenance() -> None:
    old = identity()
    stamp = CommitProvenance(SHA, False, QUERY, old.app_id, 4)
    stamped = target((SPARK_QUERY_ID, 4), provenance=(stamp,))
    with pytest.raises(CheckpointRefusedError, match="commit userMetadata"):
        decide(state(None), stamped)
    assert decide(state(None), replace(stamped, provenance=())) is StartAction.CREATE  # the limit


@pytest.mark.parametrize(
    ("committed", "planned", "recorded", "resumes"),
    [
        (range(4), range(4), 3, True),  # clean stop
        (range(4), range(5), 4, True),  # crash after the sink committed batch 4, before Spark did
        (range(4), range(4), 4, False),  # ... and the offsets for batch 4 were lost too
        (range(4), range(6), 5, False),  # recorded two batches past the last commit
        ([], [], 3, False),  # Spark's files wiped, identity kept
        ([], [0], 0, True),  # crash inside the very first batch
        ([], [], None, True),  # a checkpoint that never ran
    ],
)
def test_a_target_ahead_of_the_checkpoint_is_refused_and_a_replayable_one_resumes(
    committed: Sequence[int], planned: Sequence[int], recorded: int | None, resumes: bool
) -> None:
    ident = identity()
    evidence = target() if recorded is None else target((ident.app_id, recorded))
    current = state(ident, progress(committed, planned))
    if resumes:
        assert decide(current, evidence) is StartAction.RESUME
    else:
        with pytest.raises(CheckpointRefusedError, match="silently skip"):
            decide(current, evidence)


def test_the_native_sinks_query_id_is_held_to_the_same_rule() -> None:
    ident = identity()
    evidence = target((SPARK_QUERY_ID, 7))
    with pytest.raises(CheckpointRefusedError, match="silently skip"):
        decide(state(ident, progress(range(4), range(4), SPARK_QUERY_ID)), evidence)
    resumed = decide(state(ident, progress(range(8), range(8), SPARK_QUERY_ID)), evidence)
    assert resumed is StartAction.RESUME


def test_a_provenance_batch_ahead_of_the_checkpoint_is_refused() -> None:
    ident = identity()
    stamp = CommitProvenance(SHA, False, QUERY, ident.app_id, 9)
    with pytest.raises(CheckpointRefusedError, match="batch 9"):
        decide(state(ident, progress(range(3), range(3))), target(provenance=(stamp,)))
    resumed = decide(state(ident, progress(range(10), range(10))), target(provenance=(stamp,)))
    assert resumed is StartAction.RESUME


def test_a_stale_or_recreated_checkpoint_directory_is_refused() -> None:
    on_disk = identity(1)
    with pytest.raises(CheckpointRefusedError, match="stale or was restored"):
        decide(state(on_disk), target((identity(2).app_id, 0)))
    with pytest.raises(CheckpointRefusedError, match="recreated"):
        decide(state(on_disk), target((identity(1).app_id, 0)))


def test_commits_from_the_superseded_version_do_not_block_the_reset_checkpoint() -> None:
    previous = identity(1)
    current = identity(2, supersedes=Supersession(1, previous.app_id, "rebuild", NOW))
    resumed = decide(state(current, versions=(1, 2)), target((previous.app_id, 40)))
    assert resumed is StartAction.RESUME
    with pytest.raises(CheckpointRefusedError, match="did not supersede"):
        decide(state(current, versions=(1, 2)), target((identity(1).app_id, 40)))


def test_a_version_without_an_identity_or_with_foreign_entries_is_refused() -> None:
    with pytest.raises(CheckpointRefusedError, match="not created by these conventions"):
        decide(state(None, versions=(1,)))
    with pytest.raises(CheckpointRefusedError, match="unexpected entries"):
        decide(replace(state(identity()), unexpected=("latest",)))


@pytest.mark.parametrize(
    ("evidence", "message"),
    [
        (target(table_id=str(uuid.uuid4())), "dropped and recreated"),
        (target(version=4), "older copy"),
        (target(version=9, restores=(7,)), "RESTOREd"),
        (
            replace(target(), table=str(TableRef(Tier.SILVER, "other"))),
            "changing what a query writes",
        ),
    ],
)
def test_a_target_that_lost_rows_the_checkpoint_delivered_is_refused(
    evidence: TargetEvidence, message: str
) -> None:
    ident = identity()  # opened when the target was table TARGET_ID at version 5
    with pytest.raises(CheckpointRefusedError, match=message):
        decide(state(ident, progress(range(3), range(3))), evidence)


def test_a_restore_before_the_checkpoint_was_opened_is_not_its_concern() -> None:
    ident = identity()
    assert decide(state(ident), target(version=9, restores=(2, 5))) is StartAction.RESUME


@pytest.mark.parametrize(
    ("sources", "message"),
    [
        ([SourceEvidence(DeltaSourceStart(SOURCE_REF, 3), SOURCE_ID)], "a reset, not a restart"),
        ([SourceEvidence(DeltaSourceStart(SOURCE_REF), str(uuid.uuid4()))], "source was replaced"),
        (
            [SOURCE, SourceEvidence(KafkaSourceStart("tx.raw.v1", "earliest"), None)],
            "changing what a query reads",
        ),
    ],
)
def test_a_changed_source_is_refused(sources: list[SourceEvidence], message: str) -> None:
    with pytest.raises(CheckpointRefusedError, match=message):
        decide_start(state(identity()), [target()], sources)


# ------------------------------------------------------------------ filesystem ---

OFFSET_METADATA = '{"batchWatermarkMs":0,"batchTimestampMs":1789286837719,"conf":{}}'


def spark_checkpoint(
    root: Path, offsets: dict[int, str], commits: list[int], query_id: str | None
) -> Path:
    (root / "offsets").mkdir(parents=True)
    (root / "commits").mkdir()
    for batch, source_line in offsets.items():
        (root / "offsets" / str(batch)).write_text(f"v1\n{OFFSET_METADATA}\n{source_line}")
        (root / "offsets" / f".{batch}.crc").write_text("")
    for batch in commits:
        (root / "commits" / str(batch)).write_text('v1\n{"nextBatchWatermarkMs":0}')
    if query_id is not None:
        (root / "metadata").write_text(json.dumps({"id": query_id}))
    return root


def open_(lake: LakeConfig, *targets: TargetEvidence) -> OpenedCheckpoint:
    return checkpoints._open(
        lake,
        QUERY,
        list(targets) or [target()],
        [SOURCE],
        git_sha=SHA,
        dirty_worktree=False,
        now=NOW,
    )


def test_the_app_id_is_created_once_and_read_back_on_restart(tmp_path: Path) -> None:
    lake = LakeConfig.at(tmp_path)
    first = open_(lake)
    assert first.action is StartAction.CREATE
    assert first.directory == lake.checkpoints_root / QUERY / "v1"
    assert first.identity.targets == RECORDED_TARGETS
    assert first.identity.sources == RECORDED_SOURCES
    spark_checkpoint(first.directory, {0: "-"}, [0], SPARK_QUERY_ID)  # Spark ran batch 0
    second = open_(lake, target((first.identity.app_id, 0)))
    assert second.action is StartAction.RESUME
    assert second.identity == first.identity


def test_opening_needs_a_full_commit_id(tmp_path: Path) -> None:
    with pytest.raises(Exception, match="40-hex"):
        checkpoints._open(
            LakeConfig.at(tmp_path),
            QUERY,
            [target()],
            [SOURCE],
            git_sha="a" * 7,
            dirty_worktree=False,
            now=NOW,
        )


def test_a_refused_start_leaves_no_trace_on_disk(tmp_path: Path) -> None:
    lake = LakeConfig.at(tmp_path)
    with pytest.raises(CheckpointRefusedError):
        open_(lake, target((str(AppId.new(QUERY, 1)), 2)))
    assert not lake.checkpoints_root.exists()


def reset(lake: LakeConfig, *targets: TargetEvidence, reason: str = "why") -> CheckpointIdentity:
    return checkpoints._reset(
        lake, QUERY, list(targets) or [target()], [SOURCE], reason=reason, now=NOW
    )


def test_reset_publishes_the_next_version_records_the_present_and_deletes_nothing(
    tmp_path: Path,
) -> None:
    lake = LakeConfig.at(tmp_path)
    v1 = open_(lake)
    written = target((v1.identity.app_id, 12), table_id=str(uuid.uuid4()), version=20)
    v2 = reset(lake, written, reason="target rebuilt after the parser fix")
    assert v2.version == 2 and v2.app_id != v1.identity.app_id
    assert v2.supersedes == Supersession(
        1, v1.identity.app_id, "target rebuilt after the parser fix", NOW
    )
    assert v2.targets == {str(TARGET): TargetRecord(written.table_id, 20)}  # the rebuilt table
    kept = v1.directory / checkpoints.IDENTITY_FILENAME
    assert CheckpointIdentity.from_json(kept.read_text(), source=kept) == v1.identity
    reopened = open_(lake, written)
    assert (reopened.action, reopened.identity) == (StartAction.RESUME, v2)


def test_reset_after_the_directory_was_lost_moves_past_every_version_the_tables_saw(
    tmp_path: Path,
) -> None:
    lake = LakeConfig.at(tmp_path)
    v1, v2 = identity(1), identity(2)
    evidence = target((v1.app_id, 30), (v2.app_id, 4))
    v3 = reset(lake, evidence, reason="checkpoint volume lost")
    assert v3.version == 3 and v3.supersedes is not None
    assert v3.supersedes.app_id == v2.app_id
    assert open_(lake, evidence).identity == v3


def test_reset_refusals(tmp_path: Path) -> None:
    lake = LakeConfig.at(tmp_path)
    with pytest.raises(CheckpointRefusedError, match="reason"):
        reset(lake, reason="  ")
    with pytest.raises(CheckpointRefusedError, match="nothing to reset"):
        reset(lake)
    with pytest.raises(CheckpointRefusedError, match="conflicting app ids"):
        reset(lake, target((identity(1).app_id, 1), (identity(1).app_id, 2)))
    with pytest.raises(CheckpointRefusedError, match="must be named"):
        checkpoints._reset(lake, QUERY, [], [SOURCE], reason="why", now=NOW)
    assert not lake.checkpoints_root.exists()


BRONZE_QUERY = "bronze_ingest_tx_raw_v1"


def bronze_v1(root: Path) -> tuple[LakeConfig, OpenedCheckpoint]:
    """A Bronze checkpoint v1 that began at 0 on two partitions, committed batches 0 and 1 (ending
    at 20 and 7) and planned batch 2 (ending at 26), which Spark never recorded as done."""
    lake = LakeConfig.at(root)
    earliest = SourceEvidence(KafkaSourceStart("tx.raw.v1", "earliest"), None)
    v1 = checkpoints._open(
        lake, BRONZE_QUERY, [target()], [earliest], git_sha=SHA, dirty_worktree=False, now=NOW
    )
    offsets = {
        0: '{"tx.raw.v1":{"0":12,"1":7}}',
        1: '{"tx.raw.v1":{"0":20,"1":7}}',
        2: '{"tx.raw.v1":{"0":26,"1":7}}',
    }
    spark_checkpoint(v1.directory, offsets, [0, 1], None)
    (v1.directory / "sources" / "0").mkdir(parents=True)
    (v1.directory / "sources" / "0" / "0").write_bytes(b'\x00v1\n{"tx.raw.v1":{"0":0,"1":0}}')
    return lake, v1


def reset_kafka(lake: LakeConfig, start: str, *targets: TargetEvidence) -> CheckpointIdentity:
    source = SourceEvidence(KafkaSourceStart("tx.raw.v1", start), None)
    return checkpoints._reset(
        lake,
        BRONZE_QUERY,
        list(targets) or [target()],
        [source],
        reason="Kafka data loss stopped the query",
        now=NOW,
    )


def test_a_reset_may_not_start_a_kafka_source_above_where_the_superseded_version_stopped(
    tmp_path: Path,
) -> None:
    """Critic findings A1 and C3, at the reset. Offsets between where v1 stopped and an explicit
    start above it would be read by no checkpoint version, silently. Continuing, re-reading (the
    duplicates conservation reports) and `earliest` (judged by conservation once it has run, since
    only the broker knows where earliest is) are not refused here."""
    lake, _ = bronze_v1(tmp_path / "skip")
    with pytest.raises(
        CheckpointRefusedError, match=r"tx\.raw\.v1\[0\] would start at 25, above 20"
    ):
        reset_kafka(lake, '{"tx.raw.v1":{"0":25,"1":7}}')
    with pytest.raises(CheckpointRefusedError, match=r"tx\.raw\.v1\[1\] would start at 8, above 7"):
        reset_kafka(lake, '{"tx.raw.v1":{"0":-2,"1":8}}')
    assert [p.name for p in (lake.checkpoints_root / BRONZE_QUERY).iterdir()] == ["v1"]

    for name, start in (
        ("continue", '{"tx.raw.v1":{"0":20,"1":7}}'),
        ("re-read", '{"tx.raw.v1":{"0":-2,"1":3}}'),
        ("earliest", "earliest"),
    ):
        control, _ = bronze_v1(tmp_path / name)
        assert reset_kafka(control, start).version == 2, name

    # The table recorded batch 2 before Spark did: v1 consumed partition 0 through 26.
    lake, v1 = bronze_v1(tmp_path / "written")
    written = target((v1.identity.app_id, 2))
    with pytest.raises(CheckpointRefusedError, match=r"would start at 27, above 26"):
        reset_kafka(lake, '{"tx.raw.v1":{"0":27,"1":7}}', written)
    assert reset_kafka(lake, '{"tx.raw.v1":{"0":26,"1":7}}', written).version == 2


def test_publication_is_exclusive_and_leaves_no_staging_behind(tmp_path: Path) -> None:
    lake = LakeConfig.at(tmp_path)
    directory = checkpoints.query_directory(lake, QUERY)
    assert checkpoints._publish(directory, identity(1)) is True
    assert checkpoints._publish(directory, identity(1)) is False
    assert sorted(p.name for p in directory.iterdir()) == ["v1"]
    (directory / ".v2.staging-abandoned").mkdir()  # what a crash mid-publication leaves
    assert read_state(lake, QUERY).unexpected == ()


def test_an_unreadable_identity_on_disk_is_refused(tmp_path: Path) -> None:
    lake = LakeConfig.at(tmp_path)
    opened = open_(lake)
    (opened.directory / checkpoints.IDENTITY_FILENAME).write_text("{")
    with pytest.raises(CheckpointRefusedError, match="unreadable"):
        open_(lake)


# ----------------------------------------------------------------- Spark files ---


def test_spark_progress_is_read_from_its_own_files(tmp_path: Path) -> None:
    root = spark_checkpoint(tmp_path, {0: "-", 1: "-", 2: "-"}, [0, 1], SPARK_QUERY_ID)
    read = SparkProgress.read(root)
    assert read == SparkProgress(frozenset({0, 1, 2}), frozenset({0, 1}), SPARK_QUERY_ID)
    assert read.last_committed == 1
    assert SparkProgress.read(tmp_path / "absent") == SparkProgress()


def test_the_offsets_a_restart_resumes_from(tmp_path: Path) -> None:
    lines = {b: json.dumps({"reservoirVersion": b}) + "\n-" for b in range(4)}
    committed = spark_checkpoint(tmp_path / "a", lines, [0, 1, 2], None)
    assert checkpoints.committed_source_offsets(committed) == ('{"reservoirVersion": 2}', None)
    uncommitted = spark_checkpoint(tmp_path / "b", {0: lines[0]}, [], None)
    assert checkpoints.committed_source_offsets(uncommitted)[0] == '{"reservoirVersion": 0}'
    assert checkpoints.committed_source_offsets(tmp_path / "never") == ()
    (committed / "offsets" / "2").write_text("v9\n{}\n-")
    with pytest.raises(CheckpointRefusedError, match="unrecognised"):
        checkpoints.committed_source_offsets(committed)


# ---------------------------------------------------------------------- writes ---


def test_one_idempotent_commit_per_target_per_batch(tmp_path: Path) -> None:
    opened = open_(LakeConfig.at(tmp_path))
    path = opened._claim(0, TARGET)
    assert path == TARGET.local_path(opened.lake)
    with pytest.raises(CheckpointRefusedError, match="second idempotent commit"):
        opened._claim(0, TARGET)
    opened._claim(1, TARGET)  # the next batch may write it again
    with pytest.raises(CheckpointRefusedError, match="not a target"):
        opened._claim(2, TableRef(Tier.GOLD, "elsewhere"))
    with pytest.raises(CheckpointRefusedError, match="negative"):
        opened._claim(-1, TARGET)
    stamp = opened.provenance(3)
    assert (stamp.query, stamp.checkpoint_id, stamp.batch_id) == (QUERY, opened.identity.app_id, 3)


def test_the_recorded_kafka_start_is_what_the_reader_gets(tmp_path: Path) -> None:
    lake = LakeConfig.at(tmp_path)
    kafka = SourceEvidence(KafkaSourceStart("tx.raw.v1", "earliest"), None)
    opened = checkpoints._open(
        lake, "bronze_ingest", [target()], [kafka], git_sha=SHA, dirty_worktree=False, now=NOW
    )
    assert opened.identity.sources == {"kafka:tx.raw.v1": SourceRecord("kafka", "earliest", None)}
    assert opened.kafka_options("tx.raw.v1") == {
        "startingOffsets": "earliest",
        "failOnDataLoss": "true",
    }
    with pytest.raises(CheckpointRefusedError, match="not a source"):
        opened.kafka_options("other.topic")


class FakeConf:
    def __init__(self, fail_on: str | None = None, **values: str) -> None:
        self.values: dict[str, str] = dict(values)
        self.fail_on = fail_on

    def get(self, key: str, default: str | None = None) -> str | None:
        return self.values.get(key, default)

    def set(self, key: str, value: str) -> None:
        if key == self.fail_on:
            raise RuntimeError(f"cannot set {key}")
        self.values[key] = value

    def unset(self, key: str) -> None:
        self.values.pop(key, None)


def test_the_session_transaction_is_scoped_even_when_setting_it_fails() -> None:
    app_id = str(AppId.new(QUERY, 1))
    conf = FakeConf()
    with (
        pytest.raises(RuntimeError, match="the MERGE failed"),
        checkpoints._session_txn(conf, app_id, 4),
    ):
        assert conf.values == {
            checkpoints.TXN_APP_ID_CONF: app_id,
            checkpoints.TXN_VERSION_CONF: "4",
        }
        raise RuntimeError("the MERGE failed")
    assert conf.values == {}
    half = FakeConf(fail_on=checkpoints.TXN_VERSION_CONF)
    with pytest.raises(RuntimeError, match="cannot set"), checkpoints._session_txn(half, app_id, 4):
        pass
    assert half.values == {}  # the app id set before the failure did not leak
    leaked = FakeConf(**{checkpoints.TXN_VERSION_CONF: "7"})
    with (
        pytest.raises(CheckpointRefusedError, match="leaked"),
        checkpoints._session_txn(leaked, app_id, 4),
    ):
        pass
    assert leaked.values == {checkpoints.TXN_VERSION_CONF: "7"}
