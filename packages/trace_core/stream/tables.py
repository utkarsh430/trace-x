"""Delta lake table conventions: names, declarations, drift, provenance, sources, measurement.

No concrete Bronze, Silver or Gold table is declared here; each arrives with the step that
builds it. What lives here is what every one of them must satisfy:

1. **`TableRef`** -- one logical name, two physical addresses: a local path and a Unity
   Catalog name. Only DDL (layout, ADR-0015) may differ between them.
2. **`TableDeclaration`** -- schema, allow-listed properties, CHECK constraints, layout, and
   the protocol features the table is allowed and required to carry (Q6: the minimum; no
   column mapping, deletion vectors or type widening without an explicit opt-in).
3. **`create_table` and `check_drift`** -- a table is created from its declaration in ONE
   commit, and a live table is compared with its declaration before any job writes to it.
4. **`CommitProvenance`** -- the `userMetadata` on every commit: code, query, checkpoint, batch.
5. **Source safety** -- settings whose purpose is to tolerate loss are refused, and a
   restart whose checkpoint needs log versions the source no longer retains is refused.
6. **`measure_scans`** -- what a query's file scans selected, and what their tasks read.

Every Delta behaviour relied on here was observed on the pinned Delta 4.0.1 and is asserted
by `tests/stream/test_delta_capabilities.py`:

* A table at protocol (1, 2) reports `appendOnly` and `invariants` among its
  `tableFeatures`, whether or not either is enabled: they are the legacy writer-version-2
  features. Tables at other protocols report other sets -- with `delta.minWriterVersion=7`
  a table with a NOT NULL column was observed at (1, 7) with only `invariants`, and one
  without at (1, 1) with none, through SQL and the builder alike -- which is one reason
  declarations refuse protocol properties outright.
* A CHECK constraint raises the writer version to 3 (`checkConstraints`); `CLUSTER BY`
  raises it to 7 (`clustering`, `domainMetadata`). NOT NULL and partitioning do not.
* Deletion vectors and type widening produce (3, 7); column mapping (2, 7); change data
  feed (1, 7); row tracking (1, 7) with `domainMetadata` and two generated properties.
* Clustering and partitioning cannot be combined (SQL, builder, ALTER).
* A table created by a DataFrame write, `writeTo().create()` or a streaming sink's first
  batch stores every column as nullable. Only DDL or the builder keeps NOT NULL.

pyspark is imported only inside the functions that talk to a live session.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast

from trace_core.domain.errors import (
    LakeContractError,
    ProvenanceError,
    ScanMeasurementError,
    StreamingSourceRetentionError,
    TableDeclarationError,
    TableDriftError,
)
from trace_core.stream.lake import UC_CATALOG, AppId, LakeConfig, Tier, require_identifier

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


class Environment(StrEnum):
    """Where a table physically lives. Layout is DDL, and may differ between them (ADR-0015)."""

    LOCAL = "local"
    DATABRICKS = "databricks"


# --------------------------------------------------------------------- names ---


@dataclass(frozen=True, slots=True, order=True)
class TableRef:
    """A logical table: `<tier>.<name>`, addressable locally by path and on Databricks by name."""

    tier: Tier
    name: str

    def __post_init__(self) -> None:
        require_identifier("table name", self.name)

    def __str__(self) -> str:
        return f"{self.tier.value}.{self.name}"

    def local_path(self, lake: LakeConfig) -> Path:
        return lake.tier_root(self.tier) / self.name

    def path_identifier(self, lake: LakeConfig) -> str:
        """The SQL identifier of the local table (`LakeConfig` refuses backquotes in paths)."""
        return f"delta.`{self.local_path(lake)}`"

    def uc_name(self, catalog: str = UC_CATALOG) -> str:
        return f"{require_identifier('catalog', catalog)}.{self.tier.value}.{self.name}"

    def identifier(self, environment: Environment, lake: LakeConfig | None = None) -> str:
        """The one SQL identifier job code uses, whichever environment it runs in."""
        if environment is Environment.DATABRICKS:
            return self.uc_name()
        if lake is None:
            raise LakeContractError(f"{self}: a local identifier needs the lake root")
        return self.path_identifier(lake)


# ------------------------------------------------------------------ protocol ---

BASELINE_FEATURES: Final = frozenset({"appendOnly", "invariants"})
"""Allowed on every table: the legacy writer-version-2 features a (1, 2) table reports."""

CHECK_CONSTRAINT_FEATURES: Final = frozenset({"checkConstraints"})
CLUSTERING_FEATURES: Final = frozenset({"clustering", "domainMetadata"})

FORBIDDEN_BY_DEFAULT: Final = frozenset({"columnMapping", "deletionVectors", "typeWidening"})
"""Q6: never present unless a declaration opts in (and an ADR says why)."""

READER_FEATURES: Final = frozenset(
    {"columnMapping", "deletionVectors", "typeWidening", "typeWidening-preview", "v2Checkpoint"}
)
"""Features that also constrain readers."""


def protocol_ceiling(features: frozenset[str]) -> tuple[int, int]:
    """The highest (reader, writer) protocol a table whose features are `features` needs.

    Written from what Delta 4.0.1 was observed to choose, not from the protocol's legacy
    version table: (1, 2) for the baseline pair or no feature at all, (1, 3) once CHECK
    constraints are added, and writer version 7 for anything else -- change data feed was
    observed at (1, 7), not the legacy (1, 4), and column mapping at (2, 7), not (2, 5). Any
    reader feature bounds the reader version at 3."""
    if features & READER_FEATURES:
        return 3, 7
    if features <= BASELINE_FEATURES:
        return 1, 2
    if features <= BASELINE_FEATURES | CHECK_CONSTRAINT_FEATURES:
        return 1, 3
    return 1, 7


# ---------------------------------------------------------------- properties ---

ALLOWED_PROPERTIES: Final[Mapping[str, str]] = {
    "delta.appendOnly": "refuses DELETE and UPDATE; still admits insert-only MERGE (observed)",
    "delta.checkpointInterval": "how many commits between log checkpoints",
    "delta.logRetentionDuration": "how long commits stay replayable; bounds consumer lag",
    "delta.deletedFileRetentionDuration": "the table's own VACUUM safety floor (observed)",
    "delta.dataSkippingNumIndexedCols": "how many leading columns carry file statistics",
    "delta.dataSkippingStatsColumns": "which columns carry file statistics",
}
"""Properties a declaration may set without an opt-in. Together they were observed to leave a
table at (1, 2) with only the baseline features
(`test_k_allowed_properties_add_no_protocol_feature`)."""

FEATURE_ENABLING_PROPERTIES: Final[Mapping[str, tuple[str, frozenset[str]]]] = {
    "delta.enableDeletionVectors": ("deletionVectors", frozenset({"false"})),
    "delta.columnMapping.mode": ("columnMapping", frozenset({"none"})),
    "delta.enableTypeWidening": ("typeWidening", frozenset({"false"})),
}
"""Property -> (the feature it adds, the values that do NOT add it). Each pairing was observed;
each is accepted only with the matching opt-in."""

REFUSED_PROPERTY_PATTERNS: Final[tuple[tuple[re.Pattern[str], str], ...]] = (
    (
        re.compile(r"delta\.feature\..+"),
        "it adds a protocol feature directly (observed: delta.feature.deletionVectors=supported "
        "produced a (3, 7) deletion-vector table and left no property to show it)",
    ),
    (
        re.compile(r"delta\.min(Reader|Writer)Version"),
        "it sets the protocol directly (observed: delta.minWriterVersion=7 produced (1, 7) with a "
        "NOT NULL column and (1, 1) without one)",
    ),
    (
        re.compile(r"delta\.constraints\..+"),
        "constraints are declared as check_constraints; create_table sets them at creation",
    ),
    (
        re.compile(r"delta\.setTransactionRetentionDuration"),
        "it expires the transaction identifiers (txnAppId -> txnVersion) that make a replayed "
        "micro-batch a no-op; once expired, a replay writes its batch a second time",
    ),
)

CONSTRAINT_PROPERTY_PREFIX: Final = "delta.constraints."
SESSION_PROPERTY_DEFAULTS_PREFIX: Final = "spark.databricks.delta.properties.defaults."
"""Session settings Delta copies into every new table (observed: `...enableDeletionVectors=true`
made the table builder create a deletion-vector table)."""


def _property_problem(key: str, value: str, opt_in: frozenset[str]) -> str | None:
    for pattern, why in REFUSED_PROPERTY_PATTERNS:
        if pattern.fullmatch(key):
            return f"property {key!r} is refused: {why}"
    if key in ALLOWED_PROPERTIES:
        return None
    if key in FEATURE_ENABLING_PROPERTIES:
        feature, inert = FEATURE_ENABLING_PROPERTIES[key]
        if value.strip().lower() in inert or feature in opt_in:
            return None
        return (
            f"property {key}={value!r} adds the {feature!r} protocol feature, which the "
            f"declaration has not opted into (Q6: no {feature} by default)"
        )
    return (
        f"property {key!r} is not on the allow-list {sorted(ALLOWED_PROPERTIES)}; properties such "
        f"as delta.enableChangeDataFeed and delta.enableRowTracking were observed to add protocol "
        f"features, so anything unlisted is refused until it is observed not to"
    )


# --------------------------------------------------------------- declarations ---


@dataclass(frozen=True, slots=True)
class CheckConstraint:
    """A named CHECK constraint. NOT NULL is not one: it is the schema's `nullable=False`."""

    name: str
    expression: str

    def __post_init__(self) -> None:
        require_identifier("constraint name", self.name)
        if not self.expression.strip():
            raise TableDeclarationError(f"CHECK constraint {self.name!r} has an empty expression")


@dataclass(frozen=True, slots=True)
class TableLayout:
    """Partitioning or liquid clustering -- never both (Delta 4.0.1 refuses the combination)."""

    partition_columns: tuple[str, ...] = ()
    clustering_columns: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.partition_columns and self.clustering_columns:
            raise TableDeclarationError(
                "a table cannot be both partitioned and clustered: Delta 4.0.1 refuses the "
                "combination in SQL, in the table builder and in ALTER TABLE ... CLUSTER BY"
            )
        for kind, columns in (
            ("partition", self.partition_columns),
            ("clustering", self.clustering_columns),
        ):
            if len(set(columns)) != len(columns):
                raise TableDeclarationError(f"duplicate {kind} columns: {list(columns)}")


def _normalise_expression(expression: str) -> str:
    return " ".join(expression.split())


def _has_non_nullable_field(value: Any) -> bool:
    if isinstance(value, dict):
        if value.get("nullable") is False and "name" in value:
            return True
        return any(_has_non_nullable_field(item) for item in value.values())
    if isinstance(value, list):
        return any(_has_non_nullable_field(item) for item in value)
    return False


@dataclass(frozen=True, slots=True)
class TableDeclaration:
    """What a table is allowed, and required, to be.

    `schema` is a `pyspark.sql.types.StructType` -- building one needs no JVM -- so declared
    types keep full fidelity (nested nullability included) and creation uses them directly.
    """

    ref: TableRef
    schema: StructType
    layout: TableLayout = field(default_factory=TableLayout)
    layout_overrides: Mapping[Environment, TableLayout] = field(default_factory=dict)
    properties: Mapping[str, str] = field(default_factory=dict)
    check_constraints: tuple[CheckConstraint, ...] = ()
    opt_in_features: frozenset[str] = frozenset()
    """Features Q6 forbids by default, accepted only here. Each needs a recorded reason (an ADR)."""

    def __post_init__(self) -> None:
        problems: list[str] = []
        names = [str(name) for name in self.schema.fieldNames()]
        if len(set(names)) != len(names):
            problems.append(f"duplicate column names: {names}")
        for environment in Environment:
            layout = self.layout_for(environment)
            for column in (*layout.partition_columns, *layout.clustering_columns):
                if column not in names:
                    problems.append(
                        f"{environment.value} layout column {column!r} is not in the schema"
                    )
        constraint_names = [c.name for c in self.check_constraints]
        if len(set(constraint_names)) != len(constraint_names):
            problems.append(f"duplicate constraint names: {constraint_names}")
        unknown_opt_in = self.opt_in_features - FORBIDDEN_BY_DEFAULT
        if unknown_opt_in:
            problems.append(
                f"opt_in_features {sorted(unknown_opt_in)} are not features a declared property "
                f"can add; only {sorted(FORBIDDEN_BY_DEFAULT)} can be opted into"
            )
        for key, value in sorted(self.properties.items()):
            problem = _property_problem(key, value, self.opt_in_features)
            if problem is not None:
                problems.append(problem)
        if problems:
            raise TableDeclarationError(f"{self.ref}: " + "; ".join(problems))

    def layout_for(self, environment: Environment) -> TableLayout:
        return self.layout_overrides.get(environment, self.layout)

    def required_features(self, environment: Environment = Environment.LOCAL) -> frozenset[str]:
        """Features the live table must carry for the declaration to be enforced at all."""
        required: set[str] = set()
        if _has_non_nullable_field(self.schema.jsonValue()):
            required.add("invariants")  # NOT NULL is a column invariant (writer version 2)
        if self.check_constraints:
            required |= CHECK_CONSTRAINT_FEATURES
        if self.layout_for(environment).clustering_columns:
            required |= CLUSTERING_FEATURES
        for key, value in self.properties.items():
            if key in FEATURE_ENABLING_PROPERTIES:
                feature, inert = FEATURE_ENABLING_PROPERTIES[key]
                if value.strip().lower() not in inert:
                    required.add(feature)
        return frozenset(required)

    def allowed_features(self, environment: Environment = Environment.LOCAL) -> frozenset[str]:
        return BASELINE_FEATURES | self.required_features(environment) | self.opt_in_features

    def creation_properties(self) -> dict[str, str]:
        """Declared properties plus one `delta.constraints.<name>` per CHECK constraint.

        Observed: the builder accepts constraints at creation, in the same single commit,
        and refuses a violating write afterwards."""
        return {
            **self.properties,
            **{CONSTRAINT_PROPERTY_PREFIX + c.name: c.expression for c in self.check_constraints},
        }


# ---------------------------------------------------------------------- drift ---


@dataclass(frozen=True, slots=True)
class LiveTable:
    """What a live table reports: DESCRIBE DETAIL plus its schema as Spark's JSON."""

    table_id: str
    format: str
    min_reader_version: int
    min_writer_version: int
    table_features: frozenset[str]
    properties: Mapping[str, str]
    partition_columns: tuple[str, ...]
    clustering_columns: tuple[str, ...]
    schema_json: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class Drift:
    aspect: str
    detail: str

    def __str__(self) -> str:
        return f"{self.aspect}: {self.detail}"


def _strip_field_metadata(value: Any) -> Any:
    """Spark's schema JSON without per-field `metadata` (comments, Delta-internal column ids).

    Metadata-carried features -- column mapping ids, identity and generated columns -- are
    protocol features, and `check_drift` catches those through `tableFeatures` instead. A
    changed column comment is not reported."""
    if isinstance(value, dict):
        return {
            key: _strip_field_metadata(item)
            for key, item in value.items()
            if not (key == "metadata" and "name" in value and "nullable" in value)
        }
    if isinstance(value, list):
        return [_strip_field_metadata(item) for item in value]
    return value


def _schema_drift(declared: Mapping[str, Any], live: Mapping[str, Any]) -> list[Drift]:
    declared_fields = [_strip_field_metadata(f) for f in declared.get("fields", [])]
    live_fields = [_strip_field_metadata(f) for f in live.get("fields", [])]
    declared_by_name = {str(f["name"]): f for f in declared_fields}
    live_by_name = {str(f["name"]): f for f in live_fields}
    drifts: list[Drift] = []
    for name in declared_by_name.keys() - live_by_name.keys():
        drifts.append(Drift("schema", f"column {name!r} is declared but absent"))
    for name in live_by_name.keys() - declared_by_name.keys():
        drifts.append(Drift("schema", f"column {name!r} is present but undeclared"))
    for name in declared_by_name.keys() & live_by_name.keys():
        want, got = declared_by_name[name], live_by_name[name]
        if want.get("type") != got.get("type"):
            drifts.append(
                Drift(
                    "schema",
                    f"column {name!r} type {json.dumps(got['type'])}, declared "
                    f"{json.dumps(want['type'])}",
                )
            )
        if want.get("nullable") != got.get("nullable"):
            drifts.append(
                Drift(
                    "schema",
                    f"column {name!r} nullable={got['nullable']}, declared "
                    f"nullable={want['nullable']}",
                )
            )
    if not drifts:
        declared_order = [str(f["name"]) for f in declared_fields]
        live_order = [str(f["name"]) for f in live_fields]
        if declared_order != live_order:
            drifts.append(Drift("schema", f"column order {live_order}, declared {declared_order}"))
    return sorted(drifts, key=str)


def check_drift(
    declaration: TableDeclaration,
    live: LiveTable,
    environment: Environment = Environment.LOCAL,
) -> tuple[Drift, ...]:
    """Every difference between a live table and its declaration; empty means none."""
    drifts: list[Drift] = []
    if live.format != "delta":
        drifts.append(Drift("format", f"{live.format!r}, declared 'delta'"))

    allowed = declaration.allowed_features(environment)
    for feature in sorted(live.table_features - allowed):
        drifts.append(
            Drift("feature", f"{feature!r} is present but not allowed ({sorted(allowed)})")
        )
    for feature in sorted(declaration.required_features(environment) - live.table_features):
        drifts.append(Drift("feature", f"{feature!r} is required by the declaration but absent"))
    reader, writer = protocol_ceiling(allowed)
    if live.min_reader_version > reader or live.min_writer_version > writer:
        drifts.append(
            Drift(
                "protocol",
                f"({live.min_reader_version}, {live.min_writer_version}) exceeds the "
                f"({reader}, {writer}) the allowed features need",
            )
        )

    live_constraints = {
        key[len(CONSTRAINT_PROPERTY_PREFIX) :]: _normalise_expression(value)
        for key, value in live.properties.items()
        if key.startswith(CONSTRAINT_PROPERTY_PREFIX)
    }
    declared_constraints = {
        c.name: _normalise_expression(c.expression) for c in declaration.check_constraints
    }
    for name in sorted(declared_constraints.keys() | live_constraints.keys()):
        want, got = declared_constraints.get(name), live_constraints.get(name)
        if want != got:
            drifts.append(Drift("constraint", f"{name!r} is {got!r}, declared {want!r}"))

    live_properties = {
        key: value
        for key, value in live.properties.items()
        if not key.startswith(CONSTRAINT_PROPERTY_PREFIX)
    }
    for key in sorted(declaration.properties.keys() | live_properties.keys()):
        want, got = declaration.properties.get(key), live_properties.get(key)
        if want != got:
            drifts.append(Drift("property", f"{key!r} is {got!r}, declared {want!r}"))

    layout = declaration.layout_for(environment)
    if live.partition_columns != layout.partition_columns:
        drifts.append(
            Drift(
                "partitioning",
                f"{list(live.partition_columns)}, declared {list(layout.partition_columns)}",
            )
        )
    if live.clustering_columns != layout.clustering_columns:
        drifts.append(
            Drift(
                "clustering",
                f"{list(live.clustering_columns)}, declared {list(layout.clustering_columns)}",
            )
        )

    drifts.extend(_schema_drift(declaration.schema.jsonValue(), live.schema_json))
    return tuple(drifts)


def require_no_drift(
    declaration: TableDeclaration,
    live: LiveTable,
    environment: Environment = Environment.LOCAL,
) -> None:
    drifts = check_drift(declaration, live, environment)
    if drifts:
        raise TableDriftError(
            f"{declaration.ref} does not match its declaration; refusing to use it:\n"
            + "\n".join(f"  - {drift}" for drift in drifts)
        )


def describe_live_table(spark: SparkSession, identifier: str) -> LiveTable:
    """DESCRIBE DETAIL and the schema of `identifier` (a `TableRef.identifier(...)` value)."""
    [row] = spark.sql(f"DESCRIBE DETAIL {identifier}").collect()
    detail = row.asDict()
    schema = spark.table(identifier).schema.jsonValue()
    return LiveTable(
        table_id=str(detail["id"]),
        format=str(detail["format"]),
        min_reader_version=int(detail["minReaderVersion"]),
        min_writer_version=int(detail["minWriterVersion"]),
        table_features=frozenset(str(f) for f in (detail.get("tableFeatures") or ())),
        properties={str(k): str(v) for k, v in (detail.get("properties") or {}).items()},
        partition_columns=tuple(str(c) for c in (detail.get("partitionColumns") or ())),
        clustering_columns=tuple(str(c) for c in (detail.get("clusteringColumns") or ())),
        schema_json=cast(Mapping[str, Any], schema),
    )


@dataclass(frozen=True, slots=True)
class SnapshotFacts:
    """A Delta table's identity, version and transaction identifiers, from one snapshot."""

    table_id: str
    version: int
    transactions: Mapping[str, int]


def snapshot_facts(spark: SparkSession, path: Path) -> SnapshotFacts | None:
    """Read through Delta's own log replay; None when `path` is not a Delta table.

    `DeltaLog` is an internal API, pinned to Delta 4.0.1 and exercised by the stream tests.
    Transaction identifiers come from the snapshot, so they survive log cleanup."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, str(path)):
        return None
    session: Any = spark
    jvm: Any = session.sparkContext._jvm
    delta_log = jvm.org.apache.spark.sql.delta.DeltaLog.forTable(session._jsparkSession, str(path))
    snapshot = delta_log.update(False, jvm.scala.Option.empty(), jvm.scala.Option.empty())
    transactions: dict[str, int] = {}
    iterator = snapshot.setTransactions().iterator()
    while iterator.hasNext():
        txn = iterator.next()
        transactions[str(txn.appId())] = int(txn.version())
    return SnapshotFacts(
        table_id=str(snapshot.metadata().id()),
        version=int(snapshot.version()),
        transactions=transactions,
    )


# ---------------------------------------------------------------- provenance ---

PROVENANCE_MARKER: Final = "trace_x_provenance"
PROVENANCE_FORMAT: Final = 1
USER_METADATA_CONF: Final = "spark.databricks.delta.commitInfo.userMetadata"
_GIT_SHA: Final = re.compile(r"[0-9a-f]{40}")
_PROVENANCE_KEYS: Final = frozenset(
    {PROVENANCE_MARKER, "git_sha", "dirty_worktree", "query", "checkpoint_id", "batch_id"}
)


def require_git_sha(git_sha: str) -> str:
    if not _GIT_SHA.fullmatch(git_sha):
        raise ProvenanceError(
            f"git_sha {git_sha!r} is not a full lowercase 40-hex commit id; an abbreviated id "
            f"stops being unique as history grows"
        )
    return git_sha


@dataclass(frozen=True, slots=True)
class CommitProvenance:
    """Who wrote a commit: the code, the query, the checkpoint and the micro-batch.

    `checkpoint_id` is the checkpoint's transaction app id, and must name `query`. It is
    None for a commit made outside a streaming checkpoint (table creation, a batch job),
    and such commits are never attributed to a checkpoint.
    """

    git_sha: str
    dirty_worktree: bool
    query: str
    checkpoint_id: str | None = None
    batch_id: int | None = None

    def __post_init__(self) -> None:
        require_git_sha(self.git_sha)
        require_identifier("query name", self.query)
        if self.checkpoint_id is not None:
            parsed = AppId.parse(self.checkpoint_id)
            if parsed is None or parsed.query != self.query:
                raise ProvenanceError(
                    f"checkpoint_id {self.checkpoint_id!r} is not an app id of query {self.query!r}"
                )
        if self.batch_id is not None and (self.batch_id < 0 or self.checkpoint_id is None):
            raise ProvenanceError(
                f"batch_id {self.batch_id} needs a checkpoint_id and must not be negative"
            )

    def to_user_metadata(self) -> str:
        return json.dumps(
            {
                PROVENANCE_MARKER: PROVENANCE_FORMAT,
                "git_sha": self.git_sha,
                "dirty_worktree": self.dirty_worktree,
                "query": self.query,
                "checkpoint_id": self.checkpoint_id,
                "batch_id": self.batch_id,
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def from_user_metadata(cls, text: str | None) -> CommitProvenance | None:
        """Ours, or None when the metadata is absent or belongs to someone else."""
        if not text:
            return None
        try:
            data = json.loads(text)
        except ValueError:
            return None
        if not isinstance(data, dict) or PROVENANCE_MARKER not in data:
            return None
        if data[PROVENANCE_MARKER] != PROVENANCE_FORMAT or set(data) != _PROVENANCE_KEYS:
            raise ProvenanceError(f"unreadable TRACE-X commit provenance: {text!r}")
        batch_id, checkpoint_id = data["batch_id"], data["checkpoint_id"]
        if not (
            isinstance(data["git_sha"], str)
            and isinstance(data["dirty_worktree"], bool)
            and isinstance(data["query"], str)
            and (checkpoint_id is None or isinstance(checkpoint_id, str))
            and (batch_id is None or (isinstance(batch_id, int) and not isinstance(batch_id, bool)))
        ):
            raise ProvenanceError(f"TRACE-X commit provenance has mistyped fields: {text!r}")
        try:
            return cls(
                git_sha=data["git_sha"],
                dirty_worktree=data["dirty_worktree"],
                query=data["query"],
                checkpoint_id=checkpoint_id,
                batch_id=batch_id,
            )
        except LakeContractError as exc:
            raise ProvenanceError(f"invalid TRACE-X commit provenance {text!r}: {exc}") from exc

    def writer_options(self) -> dict[str, str]:
        """For DataFrame writes, including Delta's native streaming sink (observed to honour it)."""
        return {"userMetadata": self.to_user_metadata()}


@contextmanager
def stamped_commits(session: SparkSession, provenance: CommitProvenance) -> Iterator[None]:
    """Stamp every commit made through `session` inside the block: MERGE and DDL take no
    writer options, so the stamp travels in session configuration.

    A value already present is refused, never overwritten: it means other code is stamping
    commits on this session. The key is always unset on exit. Session configuration is
    shared by every thread using the session, so use the session a `foreachBatch` function
    receives (observed on Spark 4.0.1 not to be the session that started the query) and
    never a session other threads write through at the same time.
    """
    conf: Any = session.conf
    if conf.get(USER_METADATA_CONF, None) is not None:
        raise ProvenanceError(
            f"{USER_METADATA_CONF} is already set on this session; nested or leaked commit "
            f"stamping would attribute commits to the wrong writer"
        )
    try:
        conf.set(USER_METADATA_CONF, provenance.to_user_metadata())
        yield
    finally:
        conf.unset(USER_METADATA_CONF)


# ------------------------------------------------------------------- creation ---


def create_table(
    spark: SparkSession,
    declaration: TableDeclaration,
    lake: LakeConfig,
    provenance: CommitProvenance,
) -> LiveTable:
    """Create the local table from its declaration in one commit if absent, then prove it matches.

    CHECK constraints are creation properties, so the table never exists without them.
    Creation is refused while the session carries `spark.databricks.delta.properties.defaults.*`
    settings, which Delta would copy into the table. A table that exists and drifted is
    refused, not repaired: rows written meanwhile may violate the declaration."""
    from delta.tables import DeltaTable

    path = declaration.ref.local_path(lake)
    identifier = declaration.ref.path_identifier(lake)
    layout = declaration.layout_for(Environment.LOCAL)
    if not DeltaTable.isDeltaTable(spark, str(path)):
        injected = sorted(
            key for key in spark.conf.getAll if key.startswith(SESSION_PROPERTY_DEFAULTS_PREFIX)
        )
        if injected:
            raise TableDeclarationError(
                f"{declaration.ref}: refusing to create a table while the session sets {injected}; "
                f"Delta copies them into every new table"
            )
        with stamped_commits(spark, provenance):
            builder: Any = (
                DeltaTable.create(spark).location(str(path)).addColumns(declaration.schema)
            )
            if layout.partition_columns:
                builder = builder.partitionedBy(*layout.partition_columns)
            if layout.clustering_columns:
                builder = builder.clusterBy(*layout.clustering_columns)
            for key, value in sorted(declaration.creation_properties().items()):
                builder = builder.property(key, value)
            builder.execute()
    live = describe_live_table(spark, identifier)
    require_no_drift(declaration, live, Environment.LOCAL)
    return live


# -------------------------------------------------------------- source safety ---

LOSS_TOLERANT_SESSION_CONF: Final[Mapping[str, tuple[str, str]]] = {
    "spark.sql.files.ignoreMissingFiles": (
        "true",
        "observed on Spark 4.0.1: a Delta streaming source whose files had been vacuumed "
        "completed, advanced its checkpoint past them and never delivered their rows",
    ),
    "spark.sql.files.ignoreCorruptFiles": (
        "true",
        "not exercised here; refused because its purpose is to skip unreadable data",
    ),
}
"""Session settings that turn missing or unreadable data into silently absent rows."""

LOSS_TOLERANT_SOURCE_OPTIONS: Final[Mapping[str, tuple[str, str]]] = {
    "failOnDataLoss": (
        "false",
        "observed on Delta 4.0.1: a running query whose source log was cleaned past its next "
        "version completed without error and never delivered the rows",
    ),
    "ignoreMissingFiles": ("true", "its purpose is to skip missing data"),
    "ignoreCorruptFiles": ("true", "its purpose is to skip unreadable data"),
    "skipChangeCommits": (
        "true",
        "it skips commits that rewrite existing files; this project's Delta streaming sources are "
        "append-only, so it is never needed, and on any other source it hides the changes",
    ),
    "ignoreChanges": ("true", "as skipChangeCommits: it hides rewrites of existing files"),
    "ignoreDeletes": ("true", "as skipChangeCommits: it hides deletions"),
}
"""Reader options with the same purpose (option names are case-insensitive)."""


def require_no_loss_tolerance(
    *, session_conf: Mapping[str, str], source_options: Mapping[str, str]
) -> None:
    """Refuse any setting whose job is to let a streaming source skip data it cannot read."""
    problems: list[str] = []
    for key, (refused, why) in LOSS_TOLERANT_SESSION_CONF.items():
        if session_conf.get(key, "").strip().lower() == refused:
            problems.append(f"session {key}={refused}: {why}")
    options = {key.lower(): value for key, value in source_options.items()}
    for key, (refused, why) in LOSS_TOLERANT_SOURCE_OPTIONS.items():
        if options.get(key.lower(), "").strip().lower() == refused:
            problems.append(f"source option {key}={refused}: {why}")
    if problems:
        raise StreamingSourceRetentionError(
            "refusing a streaming source configured to tolerate data loss: " + "; ".join(problems)
        )


_COMMIT_FILE: Final = re.compile(r"(\d{20})\.json")


@dataclass(frozen=True, slots=True)
class RetainedLog:
    """The versions a Delta table's log can still replay commit by commit.

    `earliest` is the first version of the trailing contiguous run of JSON commit files: a
    streaming source reads every commit from its offset onward, so a gap below the latest
    version is as fatal as a truncated prefix.
    """

    earliest: int
    latest: int


def retained_log(table_path: Path) -> RetainedLog | None:
    """Read from the local `_delta_log` listing; None when the directory holds no commits."""
    log = table_path / "_delta_log"
    if not log.is_dir():
        return None
    versions = sorted(
        int(match.group(1))
        for entry in log.iterdir()
        if (match := _COMMIT_FILE.fullmatch(entry.name)) is not None
    )
    if not versions:
        return None
    earliest = versions[-1]
    for version in reversed(versions[:-1]):
        if version != earliest - 1:
            break
        earliest = version
    return RetainedLog(earliest=earliest, latest=versions[-1])


@dataclass(frozen=True, slots=True)
class DeltaSourceOffset:
    """A Delta streaming source's offset as a Spark checkpoint records it (observed on 4.0.1).

    Whatever the other fields say, the next batch needs the source's commit (or, for the
    initial snapshot, its state) at `reservoir_version`."""

    reservoir_id: str
    reservoir_version: int
    index: int
    is_starting_version: bool

    @staticmethod
    def is_delta_offset(text: str) -> bool:
        try:
            data = json.loads(text)
        except ValueError:
            return False
        return isinstance(data, dict) and "reservoirId" in data

    @classmethod
    def parse(cls, text: str) -> DeltaSourceOffset:
        try:
            data = json.loads(text)
            if data["sourceVersion"] != 1:
                raise ValueError(f"unknown sourceVersion {data['sourceVersion']}")
            return cls(
                reservoir_id=str(data["reservoirId"]),
                reservoir_version=int(data["reservoirVersion"]),
                index=int(data["index"]),
                is_starting_version=bool(data["isStartingVersion"]),
            )
        except (ValueError, KeyError, TypeError) as exc:
            raise StreamingSourceRetentionError(
                f"cannot read a Delta source offset from the checkpoint ({exc}): {text!r}"
            ) from exc


def require_source_retained(
    *,
    source: str,
    offset: DeltaSourceOffset,
    table_id: str,
    retained: RetainedLog | None,
) -> None:
    """Refuse to RESTART a query whose checkpoint the source table can no longer serve.

    This runs before start and cannot protect a query that is already running: a running
    query that falls behind the log's retention is protected only by `failOnDataLoss=true`
    (observed: with `false` it completes and silently loses the rows), which the checkpoint
    convention's Delta reader always sets."""
    problems: list[str] = []
    if offset.reservoir_id != table_id:
        problems.append(
            f"the checkpoint tracks table id {offset.reservoir_id}, but {source} is now table id "
            f"{table_id}: the source was replaced"
        )
    if retained is None:
        problems.append(f"{source} has no Delta commits to read")
    elif offset.reservoir_version > retained.latest + 1:
        problems.append(
            f"the checkpoint is at version {offset.reservoir_version}, ahead of the source's "
            f"latest version {retained.latest}: the source was restored to an older state"
        )
    elif offset.reservoir_version < retained.earliest:
        problems.append(
            f"the checkpoint needs version {offset.reservoir_version}, but the source log is "
            f"retained only from version {retained.earliest} (latest {retained.latest})"
        )
    if problems:
        raise StreamingSourceRetentionError(
            "refusing to start a streaming query from Delta source "
            + source
            + ": "
            + "; ".join(problems)
            + ". Do NOT delete the checkpoint (Delta's own error suggests it): a fresh checkpoint "
            "re-reads the source from its starting position, which duplicates rows in an append "
            "sink and, with a reused transaction app id, silently skips them. Reset explicitly "
            "with trace_core.stream.checkpoints.reset_checkpoint and a recorded reason, and "
            "raise the source's delta.logRetentionDuration above the longest expected consumer "
            "outage."
        )


# ---------------------------------------------------------------- measurement ---


@dataclass(frozen=True, slots=True)
class ScanMeasurement:
    """What one executed query's file scans selected, and what their tasks read."""

    rows: int
    """Rows the query produced."""
    scans: int
    """File scans in the executed plan; 0 when Delta answered from its log (observed for count)."""
    selected_files: int
    """Files the scans selected after partition pruning and data skipping -- not files opened."""
    selected_bytes: int
    """The on-disk size of the selected files. Blind to column pruning and row-group skipping:
    a scan that reads one column of one row group still selects the whole file (observed)."""
    scan_output_rows: int
    """Rows the scans produced, after row-group skipping and before filters above them."""
    input_bytes: int
    """Bytes the scans' tasks read from the filesystem (Spark task metrics), footers included --
    so it can exceed selected_bytes on small files. Delta log reads during planning, which run
    as separate stages, are not included."""
    input_records: int
    """Records those tasks read (Spark task metrics)."""


_SCAN_NODE: Final = "FileSourceScanExec"
_SCAN_METRICS: Final = ("numFiles", "filesSize", "numOutputRows")
_JOB_GROUP_PROPERTIES: Final = (
    "spark.jobGroup.id",
    "spark.job.description",
    "spark.job.interruptOnCancel",
)
UNMEASURABLE_ROOTS: Final = frozenset(
    {"CollectLimitExec", "CollectTailExec", "TakeOrderedAndProjectExec"}
)
"""Plan roots whose `collect()` takes rows incrementally; measuring them through `toRdd()` would
measure a different execution than the one a caller runs."""


def _scala_items(collection: Any) -> Iterator[Any]:
    iterator = collection.iterator()
    while iterator.hasNext():
        yield iterator.next()


def _kind(node: Any) -> str:
    return str(node.getClass().getSimpleName())


def _call_with_defaults(target: Any, name: str, *args: Any) -> Any:
    """Call a Scala method whose trailing parameters have defaults, which py4j cannot omit."""
    methods = [m for m in target.getClass().getMethods() if str(m.getName()) == name]
    if len(methods) != 1:
        raise ScanMeasurementError(
            f"expected exactly one {name} on {target.getClass().getName()}, found {len(methods)}"
        )
    count = int(methods[0].getParameterCount())
    defaults = [getattr(target, f"{name}$default${i}")() for i in range(len(args) + 1, count + 1)]
    return getattr(target, name)(*args, *defaults)


def _plan_nodes(jvm: Any, root: Any) -> list[Any]:
    """Every node reachable from `root` once, by JVM reference identity.

    Follows adaptive execution into its current plan and query stages into their plans, so a
    reused exchange's subtree is visited once. Identity, not `identityHashCode`, which can
    collide, and not `equals`, which is structural and would merge two distinct scans."""
    seen = jvm.java.util.IdentityHashMap()
    nodes: list[Any] = []
    pending: list[Any] = [root]
    while pending:
        node = pending.pop()
        if seen.containsKey(node):
            continue
        seen.put(node, True)
        nodes.append(node)
        kind = _kind(node)
        if kind == "AdaptiveSparkPlanExec":
            pending.append(node.executedPlan())
        elif kind.endswith("QueryStageExec"):
            pending.append(node.plan())
        pending.extend(_scala_items(node.children()))
        pending.extend(_scala_items(node.subqueries()))
    return nodes


def measure_scans(dataframe: DataFrame) -> ScanMeasurement:
    """Execute `dataframe` once and measure its file scans; `ScanMeasurementError` if incomplete.

    * **Selection** comes from each `FileSourceScanExec`'s SQL metrics (`numFiles`,
      `filesSize`, `numOutputRows`) in the plan that ran.
    * **Reads** come from Spark's task metrics for the stages that executed those scans'
      input RDDs, found through a job group unique to this call. The status store is fed by
      an asynchronous listener, so the listener bus is drained first.
    * The query's own `QueryExecution` is executed through `toRdd().count()`: rows are
      counted on the JVM, none shipped to Python. That is not the path `collect()` takes
      for a top-level LIMIT, so such roots are refused rather than measured differently.
    * Adaptive execution can drop a finished stage from its final plan (observed: a join
      with an empty side). A scan present in the initial plan and absent from the final one
      is refused, never counted as zero. Exchange reuse legitimately halves the scans and
      is recognised by the scans' semantic hashes.
    """
    frame: Any = dataframe
    context: Any = frame.sparkSession.sparkContext
    jvm: Any = context._jvm
    jsc: Any = context._jsc
    execution = frame._jdf.queryExecution()
    root = execution.executedPlan()
    if _kind(root) in UNMEASURABLE_ROOTS:
        raise ScanMeasurementError(
            f"the plan's root is {_kind(root)}, whose collect() takes rows incrementally; "
            f"measure the query without its top-level limit"
        )

    group = f"trace-x-scan-measurement-{uuid.uuid4().hex}"
    saved = {key: jsc.getLocalProperty(key) for key in _JOB_GROUP_PROPERTIES}
    jsc.setJobGroup(group, "trace-x scan measurement", False)
    try:
        rows = int(execution.toRdd().count())
    finally:
        for key, value in saved.items():
            jsc.setLocalProperty(key, value)
    jsc.sc().listenerBus().waitUntilEmpty()

    nodes = _plan_nodes(jvm, root)
    for node in nodes:
        if _kind(node) != "AdaptiveSparkPlanExec":
            continue
        initial = {
            int(n.semanticHash())
            for n in _plan_nodes(jvm, node.initialPlan())
            if _kind(n) == _SCAN_NODE
        }
        final = {
            int(n.semanticHash())
            for n in _plan_nodes(jvm, node.executedPlan())
            if _kind(n) == _SCAN_NODE
        }
        if initial - final:
            raise ScanMeasurementError(
                f"adaptive execution's final plan lacks {len(initial - final)} of the file scans "
                f"in its initial plan (it replaces finished stages, for example around an empty "
                f"join side); their reads cannot be attributed, so nothing is measured"
            )

    scans = [node for node in nodes if _kind(node) == _SCAN_NODE]
    selected_files = selected_bytes = scan_output_rows = 0
    rdd_selected: dict[int, int] = {}
    for scan in scans:
        metrics = {str(item._1()): int(item._2().value()) for item in _scala_items(scan.metrics())}
        missing = [name for name in _SCAN_METRICS if name not in metrics]
        if missing:
            raise ScanMeasurementError(
                f"{_SCAN_NODE} no longer reports {missing} (it reports {sorted(metrics)}); scan "
                f"measurements would silently read as zero"
            )
        selected_files += metrics["numFiles"]
        selected_bytes += metrics["filesSize"]
        scan_output_rows += metrics["numOutputRows"]
        rdd_selected[int(scan.inputRDD().id())] = metrics["numFiles"]

    store = jsc.sc().statusStore()
    stage_ids: set[int] = set()
    for job in _scala_items(_call_with_defaults(store, "jobsList", None)):
        job_group = job.jobGroup()
        if job_group.isDefined() and str(job_group.get()) == group:
            stage_ids.update(int(stage_id) for stage_id in _scala_items(job.stageIds()))
    input_bytes = input_records = 0
    completed: set[int] = set()
    for stage_id in sorted(stage_ids):
        for attempt in _scala_items(_call_with_defaults(store, "stageData", stage_id)):
            scanned = {int(rdd) for rdd in _scala_items(attempt.rddIds())} & rdd_selected.keys()
            if not scanned:
                continue
            input_bytes += int(attempt.inputBytes())
            input_records += int(attempt.inputRecords())
            if str(attempt.status()) == "COMPLETE":
                completed |= scanned
    unread = sorted(
        rdd for rdd, files in rdd_selected.items() if files > 0 and rdd not in completed
    )
    if unread:
        raise ScanMeasurementError(
            f"{len(unread)} file scan(s) selected files, but no completed stage in Spark's status "
            f"store executed them; their reads cannot be measured"
        )
    return ScanMeasurement(
        rows=rows,
        scans=len(scans),
        selected_files=selected_files,
        selected_bytes=selected_bytes,
        scan_output_rows=scan_output_rows,
        input_bytes=input_bytes,
        input_records=input_records,
    )


__all__ = [
    "ALLOWED_PROPERTIES",
    "BASELINE_FEATURES",
    "CHECK_CONSTRAINT_FEATURES",
    "CLUSTERING_FEATURES",
    "CONSTRAINT_PROPERTY_PREFIX",
    "FEATURE_ENABLING_PROPERTIES",
    "FORBIDDEN_BY_DEFAULT",
    "LOSS_TOLERANT_SESSION_CONF",
    "LOSS_TOLERANT_SOURCE_OPTIONS",
    "PROVENANCE_FORMAT",
    "PROVENANCE_MARKER",
    "READER_FEATURES",
    "REFUSED_PROPERTY_PATTERNS",
    "SESSION_PROPERTY_DEFAULTS_PREFIX",
    "UNMEASURABLE_ROOTS",
    "USER_METADATA_CONF",
    "CheckConstraint",
    "CommitProvenance",
    "DeltaSourceOffset",
    "Drift",
    "Environment",
    "LiveTable",
    "RetainedLog",
    "ScanMeasurement",
    "SnapshotFacts",
    "TableDeclaration",
    "TableLayout",
    "TableRef",
    "check_drift",
    "create_table",
    "describe_live_table",
    "measure_scans",
    "protocol_ceiling",
    "require_git_sha",
    "require_no_drift",
    "require_no_loss_tolerance",
    "require_source_retained",
    "retained_log",
    "snapshot_facts",
    "stamped_commits",
]
