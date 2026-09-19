"""Lake root, names, app ids, declarations, drift, provenance and source safety -- no JVM.

DESCRIBE DETAIL, offset and log-listing fixtures are literal copies of what the pinned Delta
4.0.1 reported during the capability spike; `tests/stream/test_delta_capabilities.py`
re-derives each from a live table, so a fixture that stops matching reality fails there.
Every guard is shown to FAIL on a single named mutation, not merely to pass on a good input.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

from trace_core.domain.errors import (
    LakeConfigError,
    LakeContractError,
    LakeNameError,
    ProvenanceError,
    StreamingSourceRetentionError,
    TableDeclarationError,
    TableDriftError,
)
from trace_core.stream import lake, tables
from trace_core.stream.lake import AppId, LakeConfig, Tier
from trace_core.stream.tables import (
    CheckConstraint,
    CommitProvenance,
    DeltaSourceOffset,
    Environment,
    LiveTable,
    RetainedLog,
    TableDeclaration,
    TableLayout,
    TableRef,
)

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
# A real-shaped commit id with digits and letters, computed so no literal looks like a secret.
SHA = hashlib.sha1(b"trace-x lake convention tests", usedforsecurity=False).hexdigest()


# ------------------------------------------------------------------ lake root ---


def test_the_default_root_is_the_documented_one_anchored_to_the_checkout_not_the_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    documented = re.search(r"^TRACE_DELTA_ROOT=(.+)$", (ROOT / ".env.example").read_text(), re.M)
    assert documented is not None and documented.group(1).strip() == lake.DEFAULT_LAKE_ROOT
    assert "data/lake/" in (ROOT / ".gitignore").read_text().splitlines()
    monkeypatch.chdir(tmp_path)  # e.g. a job started from services/stream/
    config = LakeConfig.from_env({})
    assert config.root == (ROOT / "data" / "lake").resolve()
    assert not config.root.is_relative_to(tmp_path.resolve())
    assert lake.source_checkout_root() == ROOT.resolve()


def test_a_relative_root_with_no_checkout_to_anchor_it_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(lake, "source_checkout_root", lambda: None)
    with pytest.raises(LakeConfigError, match="no source checkout"):
        LakeConfig.from_env({})
    with pytest.raises(LakeConfigError, match="no source checkout"):
        LakeConfig.from_env({lake.LAKE_ROOT_ENV: "lake"})


def test_an_explicit_root_is_used_and_resolved_once(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "alias").symlink_to(real)
    assert LakeConfig.from_env({lake.LAKE_ROOT_ENV: str(tmp_path / "alias")}) == LakeConfig.at(real)
    relative = LakeConfig.from_env({lake.LAKE_ROOT_ENV: "lake"}, base=tmp_path)
    assert relative.root == (tmp_path / "lake").resolve()


class Recorder:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def info(self, event: str, **fields: Any) -> None:
        self.events.append((event, fields))


def test_the_resolved_root_is_logged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = Recorder()
    monkeypatch.setattr(lake, "_log", recorder)
    config = LakeConfig.from_env({lake.LAKE_ROOT_ENV: str(tmp_path)})
    assert recorder.events == [
        (
            "lake_root_resolved",
            {"lake_root": str(config.root), "setting": str(tmp_path), "origin": lake.LAKE_ROOT_ENV},
        )
    ]


@pytest.mark.parametrize("value", ["", "   "])
def test_a_blank_root_is_refused_rather_than_defaulted(value: str) -> None:
    with pytest.raises(LakeConfigError, match="blank"):
        LakeConfig.from_env({lake.LAKE_ROOT_ENV: value})


@pytest.mark.parametrize(
    "value", ["s3://bucket/lake", "dbfs:/lake", "file:///tmp/lake", "abfss://c@a/l"]
)
def test_a_uri_root_is_refused_until_object_stores_are_verified(value: str) -> None:
    with pytest.raises(LakeConfigError, match="URI"):
        LakeConfig.from_env({lake.LAKE_ROOT_ENV: value})


def test_a_root_that_cannot_be_quoted_or_is_relative_is_refused(tmp_path: Path) -> None:
    with pytest.raises(LakeConfigError, match="backquote"):
        LakeConfig.at(tmp_path / "a`b")
    with pytest.raises(LakeConfigError, match="not absolute"):
        LakeConfig(root=Path("relative/lake"))


def test_checkpoints_live_beside_the_tiers_never_inside_a_table(tmp_path: Path) -> None:
    config = LakeConfig.at(tmp_path)
    assert config.checkpoints_root.parent == config.root
    assert config.checkpoints_root.name.startswith("_")  # hidden from Spark's file listing
    for tier in Tier:
        assert not config.checkpoints_root.is_relative_to(config.tier_root(tier))
        assert not config.checkpoints_root.is_relative_to(TableRef(tier, "t").local_path(config))


@pytest.mark.parametrize("name", ["transactions", "late_events", "a", "a1_b2"])
def test_valid_names(name: str) -> None:
    assert lake.require_identifier("table name", name) == name


@pytest.mark.parametrize(
    "name", ["", "Silver", "late-events", "_hidden", "1abc", "a.b", "a b", "a" * 65, "é"]
)
def test_names_that_would_resolve_differently_somewhere_are_refused(name: str) -> None:
    with pytest.raises(LakeNameError):
        lake.require_identifier("table name", name)


# -------------------------------------------------------------------- app ids ---


def test_an_app_id_names_its_query_and_version_and_carries_a_random_nonce() -> None:
    first, second = AppId.new("silver_transform", 3), AppId.new("silver_transform", 3)
    assert first != second
    assert AppId.parse(str(first)) == first
    assert str(first).startswith("trace-x:silver_transform:v3:")
    assert uuid.UUID(hex=first.nonce).version == 4


@pytest.mark.parametrize(
    "text",
    [
        "d2fcc376-0492-4a92-9b3c-6843f861ba11",  # Spark's query id: the native sink's app id
        "silver-writer",
        f"trace-x:q:v1:{'0' * 32}",  # an all-zero nonce was typed, not minted
        f"trace-x:q:v1:{uuid.uuid1().hex}",  # a time-based UUID is not a random one
        f"trace-x:q:v0:{uuid.uuid4().hex}",
        f"trace-x:Q:v1:{uuid.uuid4().hex}",
        f"trace-x:q:v1:{uuid.uuid4().hex.upper()}",
    ],
)
def test_app_ids_this_project_did_not_mint_are_not_attributed(text: str) -> None:
    assert AppId.parse(text) is None


@pytest.mark.parametrize("nonce", ["0" * 32, uuid.uuid1().hex, "not-hex"])
def test_an_app_id_cannot_be_built_with_a_nonce_that_is_not_random(nonce: str) -> None:
    with pytest.raises(LakeNameError, match="nonce"):
        AppId("q", 1, nonce)


# --------------------------------------------------------------------- naming ---


def test_one_logical_table_has_one_local_path_and_one_unity_catalog_name(tmp_path: Path) -> None:
    config = LakeConfig.at(tmp_path)
    ref = TableRef(Tier.SILVER, "transactions")
    assert ref.local_path(config) == config.root / "silver" / "transactions"
    assert ref.uc_name() == "tracex.silver.transactions"
    assert ref.identifier(Environment.DATABRICKS) == ref.uc_name()
    assert ref.identifier(Environment.LOCAL, config) == f"delta.`{config.root}/silver/transactions`"
    assert str(ref) == "silver.transactions"
    with pytest.raises(LakeContractError, match="lake root"):
        ref.identifier(Environment.LOCAL)
    with pytest.raises(LakeNameError):
        TableRef(Tier.GOLD, "Features")


# ---------------------------------------------------------------- declarations ---

SCHEMA = StructType(
    [
        StructField("id", LongType(), nullable=False),
        StructField("amount_minor", LongType(), nullable=True),
    ]
)
NULLABLE_SCHEMA = StructType([StructField("id", LongType(), nullable=True)])
REF = TableRef(Tier.SILVER, "spike")


def declare(**overrides: Any) -> TableDeclaration:
    values: dict[str, Any] = {"ref": REF, "schema": SCHEMA}
    values.update(overrides)
    return TableDeclaration(**values)


def test_clustering_and_partitioning_together_are_refused_up_front() -> None:
    with pytest.raises(TableDeclarationError, match="both partitioned and clustered"):
        TableLayout(partition_columns=("id",), clustering_columns=("amount_minor",))


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"properties": {"delta.feature.deletionVectors": "supported"}}, "adds a protocol feature"),
        ({"properties": {"delta.feature.appendOnly": "supported"}}, "adds a protocol feature"),
        ({"properties": {"delta.minWriterVersion": "7"}}, "sets the protocol"),
        ({"properties": {"delta.minReaderVersion": "3"}}, "sets the protocol"),
        ({"properties": {"delta.enableChangeDataFeed": "true"}}, "not on the allow-list"),
        ({"properties": {"delta.enableRowTracking": "true"}}, "not on the allow-list"),
        ({"properties": {"delta.enableExpiredLogCleanup": "false"}}, "not on the allow-list"),
        ({"properties": {"delta.enableDeletionVectors": "true"}}, "deletionVectors"),
        ({"properties": {"delta.columnMapping.mode": "name"}}, "columnMapping"),
        ({"properties": {"delta.enableTypeWidening": "TRUE"}}, "typeWidening"),
        ({"properties": {"delta.constraints.x": "id > 0"}}, "check_constraints"),
        ({"properties": {"delta.setTransactionRetentionDuration": "interval 1 days"}}, "expires"),
        ({"properties": {"owner": "me"}}, "not on the allow-list"),
        ({"layout": TableLayout(clustering_columns=("missing",))}, "not in the schema"),
        ({"opt_in_features": frozenset({"changeDataFeed"})}, "can be opted into"),
        (
            {"check_constraints": (CheckConstraint("c", "id > 0"), CheckConstraint("c", "id < 9"))},
            "duplicate constraint",
        ),
    ],
)
def test_declarations_that_break_the_lake_contract_are_refused(
    overrides: dict[str, Any], message: str
) -> None:
    with pytest.raises(TableDeclarationError, match=message):
        declare(**overrides)


def test_every_allow_listed_property_is_accepted_without_an_opt_in() -> None:
    samples = {
        "delta.appendOnly": "true",
        "delta.checkpointInterval": "10",
        "delta.logRetentionDuration": "interval 30 days",
        "delta.deletedFileRetentionDuration": "interval 7 days",
        "delta.dataSkippingNumIndexedCols": "8",
        "delta.dataSkippingStatsColumns": "id",
    }
    assert set(samples) == set(tables.ALLOWED_PROPERTIES)
    declaration = declare(properties=samples)
    assert declaration.allowed_features() == tables.BASELINE_FEATURES


def test_a_forbidden_feature_is_accepted_only_as_an_explicit_opt_in() -> None:
    declaration = declare(
        properties={"delta.enableDeletionVectors": "true"},
        opt_in_features=frozenset({"deletionVectors"}),
    )
    assert "deletionVectors" in declaration.required_features()
    inert = declare(properties={"delta.columnMapping.mode": "none"})
    assert "columnMapping" not in inert.allowed_features()


def test_required_and_allowed_features_are_derived_from_the_declaration() -> None:
    assert declare(schema=NULLABLE_SCHEMA).required_features() == frozenset()
    assert declare().required_features() == {"invariants"}  # NOT NULL
    nested = StructType(
        [StructField("s", StructType([StructField("x", LongType(), nullable=False)]), True)]
    )
    assert declare(schema=nested).required_features() == {"invariants"}
    with_check = declare(check_constraints=(CheckConstraint("amount_nonneg", "amount_minor >= 0"),))
    assert with_check.required_features() == {"invariants", "checkConstraints"}
    assert with_check.creation_properties() == {
        **tables.DECLARED_RETENTION,
        "delta.constraints.amount_nonneg": "amount_minor >= 0",
    }
    clustered_on_databricks_only = declare(
        layout=TableLayout(partition_columns=("id",)),
        layout_overrides={Environment.DATABRICKS: TableLayout(clustering_columns=("id",))},
    )
    assert "clustering" not in clustered_on_databricks_only.allowed_features(Environment.LOCAL)
    databricks_required = clustered_on_databricks_only.required_features(Environment.DATABRICKS)
    assert databricks_required >= tables.CLUSTERING_FEATURES
    assert tables.FORBIDDEN_BY_DEFAULT.isdisjoint(declare().allowed_features())


@pytest.mark.parametrize(
    ("features", "observed"),
    [
        # (reader, writer) DESCRIBE DETAIL reported during the spike for tables with these features.
        (frozenset(), (1, 1)),  # delta.minWriterVersion=7, every column nullable
        (frozenset({"appendOnly", "changeDataFeed", "invariants"}), (1, 7)),  # not (1, 4)
        (frozenset({"appendOnly", "invariants"}), (1, 2)),
        (frozenset({"appendOnly", "checkConstraints", "invariants"}), (1, 3)),
        (frozenset({"appendOnly", "clustering", "domainMetadata", "invariants"}), (1, 7)),
        (frozenset({"appendOnly", "deletionVectors", "invariants"}), (3, 7)),
        (frozenset({"appendOnly", "columnMapping", "invariants"}), (2, 7)),
        (frozenset({"appendOnly", "invariants", "typeWidening"}), (3, 7)),
    ],
)
def test_the_protocol_ceiling_covers_every_observed_protocol(
    features: frozenset[str], observed: tuple[int, int]
) -> None:
    reader, writer = tables.protocol_ceiling(features)
    assert observed[0] <= reader and observed[1] <= writer
    if features and features <= tables.BASELINE_FEATURES | {
        "checkConstraints",
        "clustering",
        "domainMetadata",
    }:
        assert (reader, writer) == observed  # exact for the features declarations derive


def test_a_table_above_the_ceiling_its_features_need_is_protocol_drift() -> None:
    """Observed: delta.minWriterVersion=7 with a NOT NULL column yields (1, 7) and only
    `invariants` -- a protocol no declared feature needs, so drift must report it."""
    assert tables.protocol_ceiling(frozenset({"invariants"})) == (1, 2)
    live = replace(
        OBSERVED_CHECKED,
        min_writer_version=7,
        table_features=frozenset({"invariants"}),
        properties={},
        schema_json=schema_with([ID]),
    )
    not_null = declare(schema=StructType([StructField("id", LongType(), nullable=False)]))
    assert [d.aspect for d in tables.check_drift(not_null, live)] == ["protocol"]


# ---------------------------------------------------------------------- drift ---

CHECKED = declare(check_constraints=(CheckConstraint("amount_nonneg", "amount_minor >= 0"),))
OBSERVED_CHECKED = LiveTable(
    # DESCRIBE DETAIL of `(id BIGINT NOT NULL, amount_minor BIGINT)` + CHECK, as observed.
    table_id="5f371952-be0c-4df4-8ed8-9c0a25091aaa",
    format="delta",
    min_reader_version=1,
    min_writer_version=3,
    table_features=frozenset({"appendOnly", "checkConstraints", "invariants"}),
    properties={"delta.constraints.amount_nonneg": "amount_minor >= 0"},
    partition_columns=(),
    clustering_columns=(),
    schema_json=json.loads(
        '{"fields":[{"metadata":{},"name":"id","nullable":false,"type":"long"},'
        '{"metadata":{},"name":"amount_minor","nullable":true,"type":"long"}],"type":"struct"}'
    ),
)


def schema_with(fields: list[dict[str, Any]]) -> dict[str, Any]:
    return {"type": "struct", "fields": fields}


ID = {"metadata": {}, "name": "id", "nullable": False, "type": "long"}
AMOUNT = {"metadata": {}, "name": "amount_minor", "nullable": True, "type": "long"}


def test_a_table_matching_its_declaration_has_no_drift() -> None:
    assert tables.check_drift(CHECKED, OBSERVED_CHECKED) == ()
    tables.require_no_drift(CHECKED, OBSERVED_CHECKED)


@pytest.mark.parametrize(
    ("mutation", "aspect"),
    [
        ({"format": "parquet"}, "format"),
        ({"table_features": OBSERVED_CHECKED.table_features | {"deletionVectors"}}, "feature"),
        ({"table_features": frozenset({"appendOnly", "invariants"})}, "feature"),  # CHECK lost
        ({"min_writer_version": 7}, "protocol"),
        ({"min_reader_version": 3}, "protocol"),
        (
            # Observed: this property lowers the VACUUM safety floor below 168 hours.
            {
                "properties": {
                    **OBSERVED_CHECKED.properties,
                    "delta.deletedFileRetentionDuration": "interval 1 hours",
                }
            },
            "property",
        ),
        ({"properties": {"delta.constraints.amount_nonneg": "amount_minor > 0"}}, "constraint"),
        ({"partition_columns": ("id",)}, "partitioning"),
        ({"clustering_columns": ("id",)}, "clustering"),
        ({"schema_json": schema_with([{**ID, "type": "integer"}, AMOUNT])}, "schema"),
        ({"schema_json": schema_with([{**ID, "nullable": True}, AMOUNT])}, "schema"),
        ({"schema_json": schema_with([ID])}, "schema"),
        ({"schema_json": schema_with([ID, AMOUNT, {**AMOUNT, "name": "extra"}])}, "schema"),
        ({"schema_json": schema_with([AMOUNT, ID])}, "schema"),
    ],
)
def test_each_kind_of_drift_is_detected_on_its_own(mutation: dict[str, Any], aspect: str) -> None:
    drifts = tables.check_drift(CHECKED, replace(OBSERVED_CHECKED, **mutation))
    assert [d.aspect for d in drifts] == [aspect], drifts


def test_a_not_null_column_on_a_table_without_invariants_is_drift() -> None:
    """Observed: delta.minWriterVersion=7 on an all-nullable table yields (1, 1), no features."""
    live = replace(
        OBSERVED_CHECKED,
        min_writer_version=1,
        table_features=frozenset(),
        properties={},
        schema_json=schema_with([ID]),
    )
    not_null = declare(schema=StructType([StructField("id", LongType(), nullable=False)]))
    assert [str(d) for d in tables.check_drift(not_null, live)] == [
        "feature: 'invariants' is required by the declaration but absent"
    ]


def test_what_drift_deliberately_ignores() -> None:
    reformatted = replace(
        OBSERVED_CHECKED,
        properties={"delta.constraints.amount_nonneg": "  amount_minor   >=  0 "},
        schema_json=schema_with([{**ID, "metadata": {"comment": "a comment"}}, AMOUNT]),
    )
    assert tables.check_drift(CHECKED, reformatted) == ()


def test_nested_nullability_is_compared_not_just_the_type_name() -> None:
    nested = declare(
        schema=StructType(
            [StructField("tags", ArrayType(StringType(), containsNull=False), nullable=False)]
        )
    )
    live = replace(
        OBSERVED_CHECKED,
        min_writer_version=2,
        table_features=tables.BASELINE_FEATURES,
        properties={},
        schema_json=nested.schema.jsonValue(),
    )
    assert tables.check_drift(nested, live) == ()
    loosened = json.loads(json.dumps(nested.schema.jsonValue()))
    loosened["fields"][0]["type"]["containsNull"] = True
    drifts = tables.check_drift(nested, replace(live, schema_json=loosened))
    assert [d.aspect for d in drifts] == ["schema"]


def test_every_difference_is_reported_at_once() -> None:
    live = replace(
        OBSERVED_CHECKED,
        table_features=OBSERVED_CHECKED.table_features | {"columnMapping"},
        min_reader_version=2,
        min_writer_version=7,
        properties={"delta.columnMapping.mode": "name"},
    )
    with pytest.raises(TableDriftError) as raised:
        tables.require_no_drift(CHECKED, live)
    text = str(raised.value)
    for aspect in ("feature", "protocol", "property", "constraint"):
        assert f"- {aspect}:" in text


# ----------------------------------------------------------------- provenance ---


def checkpoint_id(query: str = "silver_transform") -> str:
    return str(AppId.new(query, 1))


def test_the_test_commit_id_exercises_digits_as_well_as_letters() -> None:
    """A regex that accepted only [a-f] would still pass on 'a' * 40; this id has both."""
    assert re.search(r"[0-9]", SHA) and re.search(r"[a-f]", SHA)
    assert tables.require_git_sha(SHA) == SHA


def test_provenance_round_trips_through_user_metadata() -> None:
    stamp = CommitProvenance(SHA, False, "silver_transform", checkpoint_id(), 7)
    text = stamp.to_user_metadata()
    assert CommitProvenance.from_user_metadata(text) == stamp
    assert json.loads(text)[tables.PROVENANCE_MARKER] == tables.PROVENANCE_FORMAT
    assert text == replace(stamp).to_user_metadata()  # canonical: same stamp, same bytes
    assert stamp.writer_options() == {"userMetadata": text}
    setup = CommitProvenance(SHA, True, "table_setup")  # a commit made outside any checkpoint
    assert CommitProvenance.from_user_metadata(setup.to_user_metadata()) == setup


@pytest.mark.parametrize(
    ("query", "checkpoint", "batch", "message"),
    [
        ("silver_transform", checkpoint_id("bronze_ingest"), 1, "not an app id of query"),
        ("silver_transform", "trace-x:silver_transform:v1:" + "0" * 32, 1, "not an app id"),
        ("silver_transform", None, 1, "needs a checkpoint_id"),
        ("silver_transform", checkpoint_id(), -1, "must not be negative"),
    ],
)
def test_provenance_that_would_misattribute_a_commit_is_refused(
    query: str, checkpoint: str | None, batch: int, message: str
) -> None:
    with pytest.raises(ProvenanceError, match=message):
        CommitProvenance(SHA, False, query, checkpoint, batch)


@pytest.mark.parametrize("text", [None, "", "not json", '{"q":"x"}', "[1, 2]"])
def test_foreign_or_absent_user_metadata_is_not_ours(text: str | None) -> None:
    assert CommitProvenance.from_user_metadata(text) is None


def _stamp_json(**changes: Any) -> str:
    data: dict[str, Any] = {
        tables.PROVENANCE_MARKER: 1,
        "git_sha": SHA,
        "dirty_worktree": False,
        "query": "q",
        "checkpoint_id": checkpoint_id("q"),
        "batch_id": 1,
    }
    data.update(changes)
    return json.dumps({k: v for k, v in data.items() if v is not ...})


@pytest.mark.parametrize(
    "text",
    [
        _stamp_json(**{tables.PROVENANCE_MARKER: 2}),
        _stamp_json(batch_id=...),
        _stamp_json(extra="x"),
        _stamp_json(batch_id="3"),
        _stamp_json(batch_id=True),
        _stamp_json(git_sha="0123abc"),
        _stamp_json(query="Not-A-Query"),
        _stamp_json(checkpoint_id=7),
    ],
)
def test_malformed_provenance_carrying_our_marker_is_an_error(text: str) -> None:
    with pytest.raises(ProvenanceError):
        CommitProvenance.from_user_metadata(text)


@pytest.mark.parametrize("sha", ["0123abc", SHA.upper(), SHA + "0", SHA[:-1] + "g"])
def test_provenance_needs_a_full_lowercase_commit_id(sha: str) -> None:
    with pytest.raises(ProvenanceError):
        CommitProvenance(sha, False, "q")


class FakeConf:
    """Stands in for `SparkSession.conf` (`get(key, default)`, `set`, `unset`)."""

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


class FakeSession:
    def __init__(self, conf: FakeConf) -> None:
        self.conf = conf


def test_commit_stamping_is_scoped_to_the_block_and_refuses_an_existing_stamp() -> None:
    stamp = CommitProvenance(SHA, True, "q")
    conf = FakeConf()
    with pytest.raises(RuntimeError), tables.stamped_commits(cast(Any, FakeSession(conf)), stamp):
        assert conf.values[tables.USER_METADATA_CONF] == stamp.to_user_metadata()
        raise RuntimeError("the MERGE failed")
    assert tables.USER_METADATA_CONF not in conf.values
    leaked = FakeConf(**{tables.USER_METADATA_CONF: "someone else"})
    with (
        pytest.raises(ProvenanceError, match="already set"),
        tables.stamped_commits(cast(Any, FakeSession(leaked)), stamp),
    ):
        pass
    assert leaked.values[tables.USER_METADATA_CONF] == "someone else"


# -------------------------------------------------------------- source safety ---

OBSERVED_OFFSET = (
    '{"sourceVersion":1,"reservoirId":"ef5b9a4f-257c-466f-959b-a3778a12497f",'
    '"reservoirVersion":3,"index":-1,"isStartingVersion":false}'
)


def delta_log(root: Path, names: list[str]) -> Path:
    log = root / "_delta_log"
    log.mkdir(parents=True)
    for name in names:
        (log / name).write_text("")
    return root


def test_the_retained_log_is_read_from_the_log_listing(tmp_path: Path) -> None:
    # The listing observed after Delta's own metadata cleanup deleted versions 0-8.
    observed = [
        "00000000000000000009.checkpoint.parquet", "00000000000000000009.crc",
        "00000000000000000009.json", "00000000000000000010.crc", "00000000000000000010.json",
        "00000000000000000011.crc", "00000000000000000011.json",
        "00000000000000000012.checkpoint.parquet", "00000000000000000012.crc",
        "00000000000000000012.json", "_last_checkpoint",
    ]  # fmt: skip
    assert tables.retained_log(delta_log(tmp_path / "t", observed)) == RetainedLog(9, 12)
    assert tables.retained_log(tmp_path / "missing") is None
    assert tables.retained_log(delta_log(tmp_path / "empty", ["_last_checkpoint"])) is None


def test_a_gap_in_the_log_moves_the_earliest_usable_version_past_it(tmp_path: Path) -> None:
    names = [f"{v:020d}.json" for v in (0, 1, 2, 3, 5, 6, 7)]
    assert tables.retained_log(delta_log(tmp_path / "t", names)) == RetainedLog(5, 7)


def test_the_observed_offset_format_parses_and_anything_else_is_refused() -> None:
    offset = DeltaSourceOffset.parse(OBSERVED_OFFSET)
    assert offset == DeltaSourceOffset("ef5b9a4f-257c-466f-959b-a3778a12497f", 3, -1, False)
    assert DeltaSourceOffset.is_delta_offset(OBSERVED_OFFSET)
    assert not DeltaSourceOffset.is_delta_offset('{"tx.raw.v1":{"0":12}}')  # a Kafka offset
    assert not DeltaSourceOffset.is_delta_offset("-")
    for bad in (OBSERVED_OFFSET.replace('"sourceVersion":1', '"sourceVersion":9'), "{}", "nope"):
        with pytest.raises(StreamingSourceRetentionError):
            DeltaSourceOffset.parse(bad)


def test_a_checkpoint_behind_the_retained_log_is_refused_with_a_safe_remedy() -> None:
    offset = DeltaSourceOffset.parse(OBSERVED_OFFSET)
    with pytest.raises(StreamingSourceRetentionError) as raised:
        tables.require_source_retained(
            source="bronze.tx",
            offset=offset,
            table_id=offset.reservoir_id,
            retained=RetainedLog(9, 12),
        )
    text = str(raised.value)
    assert "needs version 3" in text and "from version 9" in text
    assert "Do NOT delete the checkpoint" in text and "reset_checkpoint" in text


@pytest.mark.parametrize(
    ("version", "refused"),
    [(8, True), (9, False), (12, False), (13, False), (14, True)],
)
def test_the_retention_boundary_is_exact(version: int, refused: bool) -> None:
    offset = DeltaSourceOffset("t", version, -1, False)

    def check() -> None:
        tables.require_source_retained(
            source="s", offset=offset, table_id="t", retained=RetainedLog(9, 12)
        )

    if refused:
        with pytest.raises(StreamingSourceRetentionError):
            check()
    else:
        check()


def test_a_replaced_or_empty_source_is_refused() -> None:
    offset = DeltaSourceOffset("old-id", 10, -1, False)
    with pytest.raises(StreamingSourceRetentionError, match="replaced"):
        tables.require_source_retained(
            source="s", offset=offset, table_id="new-id", retained=RetainedLog(9, 12)
        )
    with pytest.raises(StreamingSourceRetentionError, match="no Delta commits"):
        tables.require_source_retained(
            source="s", offset=replace(offset, reservoir_id="t"), table_id="t", retained=None
        )


REFUSED_SETTINGS: list[tuple[dict[str, str], dict[str, str]]] = [
    ({"spark.sql.files.ignoreMissingFiles": "true"}, {}),
    ({"spark.sql.files.ignoreCorruptFiles": "TRUE"}, {}),
    ({}, {"failOnDataLoss": "False"}),
    ({}, {"FailOnDataLoss": "false"}),
    ({}, {"ignoreMissingFiles": "true"}),
    ({}, {"ignoreCorruptFiles": "true"}),
    ({}, {"skipChangeCommits": "true"}),
    ({}, {"ignoreChanges": "true"}),
    ({}, {"ignoreDeletes": "true"}),
]


def test_every_refused_setting_is_exercised() -> None:
    session_keys = {key for conf, _ in REFUSED_SETTINGS for key in conf}
    option_keys = {key.lower() for _, options in REFUSED_SETTINGS for key in options}
    assert session_keys == set(tables.LOSS_TOLERANT_SESSION_CONF)
    assert option_keys == {key.lower() for key in tables.LOSS_TOLERANT_SOURCE_OPTIONS}


@pytest.mark.parametrize(("session_conf", "options"), REFUSED_SETTINGS)
def test_settings_whose_purpose_is_to_skip_data_are_refused(
    session_conf: dict[str, str], options: dict[str, str]
) -> None:
    with pytest.raises(StreamingSourceRetentionError, match="tolerate data loss"):
        tables.require_no_loss_tolerance(session_conf=session_conf, source_options=options)


@pytest.mark.parametrize(
    ("session_conf", "options"),
    [
        ({"spark.sql.files.ignoreMissingFiles": "false"}, {}),
        ({}, {"failOnDataLoss": "true"}),
        ({}, {"skipChangeCommits": "false"}),
        ({}, {"maxFilesPerTrigger": "1"}),
    ],
)
def test_the_same_settings_in_their_safe_positions_are_accepted(
    session_conf: dict[str, str], options: dict[str, str]
) -> None:
    tables.require_no_loss_tolerance(session_conf=session_conf, source_options=options)
