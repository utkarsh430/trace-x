"""Gold's plan, without Spark: tables, the declared windows it compiles, contexts, build progress.

ADR-0055. Gold is batch. A build reads Silver's canonical observation tables, each at one pinned
Delta version, and writes two kinds of table:

- **Primitive-shaped state** (`trace_core.features.state_plan.PLAN`), so reconstruction (Step 9) can
  rebuild the online primitives from it: every observation once under its identity
  (`gold.observations`, which serves PLAN's velocity, exact distinct, previous and profile state),
  per-currency minute buckets (`gold.minute_buckets`, PLAN's buckets) and five-minute distinct
  buckets (`gold.distinct_buckets`, PLAN's approximate distinct counts).
- **Per-transaction point-in-time context** in the `EVENT_TIME_COMPLETE` mode (ADR-0046 §1):
  `gold.tx_windows`, `gold.tx_profiles` and `gold.tx_previous` hold what `FeatureContext` holds, so
  the shared feature definitions evaluate a Gold context exactly as they evaluate the reference's or
  the Redis store's. A context never carries a completeness claim: `feature_context` takes one.

Everything here is pure, so it is held to its declarations by unit tests without a JVM.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Final

from trace_core.contracts.events.identity_events_v1 import IdentityEventType
from trace_core.contracts.topics import IDENTITY_EVENTS_V1, TX_AUTHORIZATION_V1, TX_SCORED_V1
from trace_core.domain.enums import AuthorizationOutcome, FeatureSource
from trace_core.domain.errors import LakeContractError
from trace_core.domain.time import EventTime, from_millis
from trace_core.features.context import FeatureContext, Observation, Profile, WindowState
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.observation import NAMESPACES
from trace_core.features.semantics import (
    Aggregation,
    CardinalityStorage,
    CurrentObservation,
    Dimension,
    Entity,
    Stream,
    Window,
    WindowedAggregate,
    identity_stream,
)
from trace_core.features.spec import FeatureRegistry
from trace_core.features.state_plan import PLAN, StatePlan
from trace_core.stream.checkpoints import SparkProgress
from trace_core.stream.lake import Tier
from trace_core.stream.silver_rules import silver_topic
from trace_core.stream.tables import TableRef

GOLD_JOB: Final = "gold_build"
"""The checkpoint query name a Gold build writes through (ADR-0048 §6)."""


class GoldPlanError(LakeContractError):
    """A released declaration Gold does not compile. Refused before any job runs."""


class GoldRefusedError(LakeContractError):
    """A build that cannot be run safely: a missing or replaced source, or unreadable progress."""


class GoldUniquenessError(LakeContractError):
    """A Gold frame holds a key twice: the build stops before writing it."""


# --------------------------------------------------------------------- tables ---

OBSERVATIONS: Final = TableRef(Tier.GOLD, "observations")
MINUTE_BUCKETS: Final = TableRef(Tier.GOLD, "minute_buckets")
DISTINCT_BUCKETS: Final = TableRef(Tier.GOLD, "distinct_buckets")
TX_WINDOWS: Final = TableRef(Tier.GOLD, "tx_windows")
TX_PROFILES: Final = TableRef(Tier.GOLD, "tx_profiles")
TX_PREVIOUS: Final = TableRef(Tier.GOLD, "tx_previous")
BUILDS: Final = TableRef(Tier.GOLD, "builds")

REPLACED_TABLES: Final = (
    OBSERVATIONS,
    MINUTE_BUCKETS,
    DISTINCT_BUCKETS,
    TX_WINDOWS,
    TX_PROFILES,
    TX_PREVIOUS,
)
"""Replaced whole by every build, by one MERGE each on its key (ADR-0055 §4)."""
GOLD_TARGETS: Final = (*REPLACED_TABLES, BUILDS)

TABLE_KEYS: Final[Mapping[TableRef, tuple[str, ...]]] = MappingProxyType(
    {
        OBSERVATIONS: ("observation_identity",),
        MINUTE_BUCKETS: ("entity", "entity_id", "currency", "minute"),
        DISTINCT_BUCKETS: ("entity", "entity_id", "dimension", "bucket", "value"),
        TX_WINDOWS: ("transaction_id", "entity", "stream", "window_label"),
        TX_PROFILES: ("transaction_id",),
        TX_PREVIOUS: ("transaction_id", "entity", "stream"),
    }
)

TRANSACTIONS: Final = silver_topic(TX_SCORED_V1).table
IDENTITY_EVENTS: Final = silver_topic(IDENTITY_EVENTS_V1).table
AUTHORIZATIONS: Final = silver_topic(TX_AUTHORIZATION_V1).table
GOLD_SOURCES: Final = (TRANSACTIONS, IDENTITY_EVENTS, AUTHORIZATIONS)
"""Silver's canonical observation tables, and nothing else: never `silver.duplicates`,
`silver.quarantine` or `silver.late_events`, and no column but the event's own (never `is_late`)."""

ENTITY_COLUMNS: Final[Mapping[Entity, str]] = MappingProxyType(
    {
        Entity.ACCOUNT: "account_id",
        Entity.CARD: "card_id",
        Entity.DEVICE: "device_id",
        Entity.MERCHANT: "merchant_id",
        Entity.IP: "ip_id",
    }
)
"""The `gold.observations` column an entity is keyed by: `Event.entity_id`'s field, by name."""

DIMENSION_COLUMNS: Final[Mapping[Dimension, str]] = MappingProxyType(
    {
        Dimension.MERCHANT: "merchant_id",
        Dimension.MCC: "merchant_mcc",
        Dimension.DEVICE: "device_id",
        Dimension.COUNTRY: "merchant_country",
        Dimension.ACCOUNT: "account_id",
    }
)
"""The column a distinct count counts: `Event.dimension_value`'s field, by name."""

IDENTITY_TYPE_STREAMS: Final[Mapping[str, Stream]] = MappingProxyType(
    {
        member.value: stream
        for member in IdentityEventType
        if (stream := identity_stream(member.value)) is not None
    }
)
"""Every released identity event type that feeds a stream, by the one declared mapping. The rest
change no online state (ADR-0046 §4) and are not observations."""

OBSERVED_OUTCOMES: Final = (
    AuthorizationOutcome.APPROVED.value,
    AuthorizationOutcome.DECLINED.value,
)

NAMESPACE_VALUES: Final[Mapping[Stream, str]] = MappingProxyType(
    {stream: namespace.value for stream, namespace in NAMESPACES.items()}
)


# ------------------------------------------------------------------ windows ---

SCALAR_FIELDS: Final = (
    "count",
    "amount_sum_minor",
    "aligned_count",
    "aligned_amount_sum_minor",
    "aligned_amount_sum_squares",
    "declined_count",
    "outcome_known_count",
)
"""The `WindowState` scalars Gold can compute. `amount_sum_squares` is not one: nothing reads it."""

ALIGNED_FIELDS: Final = frozenset(
    {"aligned_count", "aligned_amount_sum_minor", "aligned_amount_sum_squares"}
)

READS: Final[Mapping[Aggregation, frozenset[str]]] = MappingProxyType(
    {
        Aggregation.COUNT: frozenset({"count"}),
        Aggregation.AMOUNT_SUM: frozenset({"amount_sum_minor"}),
        Aggregation.DISTINCT_COUNT: frozenset(),
        Aggregation.AMOUNT_CV: ALIGNED_FIELDS,
        Aggregation.DECLINED_RATIO: frozenset({"count", "declined_count", "outcome_known_count"}),
    }
)
"""The `WindowState` fields each aggregation's feature reads (`definitions.py`). A distinct count
reads its dimension's entry instead. Gold computes exactly these for each declared window, and NULL
in a column means "not computed for this window"."""


def distinct_column(dimension: Dimension) -> str:
    return f"distinct_{dimension.value.lower()}"


@dataclass(frozen=True, slots=True)
class WindowSpec:
    """One declared `(entity, stream, window)`, and what Gold computes for it."""

    entity: Entity
    stream: Stream
    window: Window
    current_observation: CurrentObservation
    fields: frozenset[str]
    exact_dimensions: frozenset[Dimension] = frozenset()
    approximate_dimensions: frozenset[Dimension] = frozenset()

    @property
    def label(self) -> str:
        return self.window.label

    @property
    def window_ms(self) -> int:
        return self.window.seconds * 1000

    @property
    def present_by_construction(self) -> bool:
        """The scored transaction is a member of its own INCLUDED transaction window, so the
        window is never absent for it (`reference.window_state` returns a state)."""
        return (
            self.current_observation is CurrentObservation.INCLUDED
            and self.stream is Stream.TRANSACTION
        )

    @property
    def raw_fields(self) -> frozenset[str]:
        return self.fields - ALIGNED_FIELDS

    @property
    def needs_raw(self) -> bool:
        return bool(self.raw_fields or self.exact_dimensions)

    @property
    def needs_aligned(self) -> bool:
        return bool(self.fields & ALIGNED_FIELDS)


def _shapes(
    registry: FeatureRegistry,
) -> dict[tuple[Entity, Stream, Window], list[WindowedAggregate]]:
    shapes: dict[tuple[Entity, Stream, Window], list[WindowedAggregate]] = {}
    for spec in registry:
        semantics = spec.semantics
        if isinstance(semantics, WindowedAggregate):
            key = (semantics.entity, semantics.stream, semantics.window)
            shapes.setdefault(key, []).append(semantics)
    return shapes


def unsupported_declarations(
    registry: FeatureRegistry = ONLINE_FEATURES, plan: StatePlan = PLAN
) -> list[str]:
    """Every declared window shape Gold does not compile. Must be empty for the released set (a unit
    test asserts it): a shape Gold skipped would read as absent in every Gold context."""
    problems: list[str] = []
    for (entity, stream, window), shapes in sorted(
        _shapes(registry).items(), key=lambda item: (item[0][0], item[0][1], item[0][2].seconds)
    ):
        where = f"({entity}, {stream}, {window.label})"
        currents = {shape.current_observation for shape in shapes}
        if len(currents) != 1:
            problems.append(f"{where}: declares more than one CurrentObservation")
            continue
        current = next(iter(currents))
        if stream is Stream.AUTHORIZATION_OUTCOME:
            if entity is not Entity.ACCOUNT or any(
                shape.aggregation is not Aggregation.DECLINED_RATIO for shape in shapes
            ):
                problems.append(f"{where}: outcomes carry only an account and answer only a ratio")
            continue
        if current is not CurrentObservation.INCLUDED:
            problems.append(
                f"{where}: {current} is compiled only for authorization outcomes; no released "
                f"transaction or identity window declares it"
            )
        for shape in shapes:
            if stream is not Stream.TRANSACTION and shape.aggregation in (
                Aggregation.AMOUNT_SUM,
                Aggregation.AMOUNT_CV,
            ):
                problems.append(
                    f"{where}: {shape.aggregation} needs amounts, which only transactions carry"
                )
            if shape.aggregation is Aggregation.AMOUNT_CV and entity not in plan.buckets:
                problems.append(f"{where}: AMOUNT_CV needs PLAN minute buckets for {entity}")
            if shape.storage is CardinalityStorage.APPROXIMATE and (
                stream is not Stream.TRANSACTION
                or shape.dimension is None
                or (entity, shape.dimension) not in plan.approx_distinct
            ):
                problems.append(
                    f"{where}: an approximate distinct count is compiled only from "
                    f"PLAN's transaction distinct buckets"
                )
    return problems


def compile_windows(
    registry: FeatureRegistry = ONLINE_FEATURES, plan: StatePlan = PLAN
) -> tuple[WindowSpec, ...]:
    """The declared windows, or `GoldPlanError` naming every shape Gold does not compile."""
    problems = unsupported_declarations(registry, plan)
    if problems:
        raise GoldPlanError("Gold does not compile: " + "; ".join(problems))
    specs: list[WindowSpec] = []
    for (entity, stream, window), shapes in _shapes(registry).items():
        current = shapes[0].current_observation
        fields: set[str] = set()
        exact: set[Dimension] = set()
        approximate: set[Dimension] = set()
        for shape in shapes:
            fields |= READS[shape.aggregation]
            if shape.dimension is not None:
                target = approximate if shape.storage is CardinalityStorage.APPROXIMATE else exact
                target.add(shape.dimension)
        spec = WindowSpec(
            entity,
            stream,
            window,
            current,
            frozenset(fields),
            frozenset(exact),
            frozenset(approximate),
        )
        if not spec.present_by_construction:
            # Presence is then "at least one member", which needs the member count.
            spec = WindowSpec(
                entity,
                stream,
                window,
                current,
                frozenset(fields | {"count"}),
                frozenset(exact),
                frozenset(approximate),
            )
        specs.append(spec)
    return tuple(sorted(specs, key=lambda s: (s.entity.value, s.stream.value, s.window.seconds)))


# ----------------------------------------------------------------- contexts ---


@dataclass(frozen=True, slots=True)
class WindowRow:
    """One `gold.tx_windows` row. None: not computed for this window (no declared feature reads
    it)."""

    entity: Entity
    entity_id: str
    stream: Stream
    window_label: str
    count: int | None = None
    amount_sum_minor: int | None = None
    aligned_count: int | None = None
    aligned_amount_sum_minor: int | None = None
    aligned_amount_sum_squares: int | None = None
    declined_count: int | None = None
    outcome_known_count: int | None = None
    distinct: Mapping[Dimension, int] = field(default_factory=dict)
    """Computed distinct counts, zero included; `state()` drops zeros, as the reference does."""

    def state(self) -> WindowState:
        """The `WindowState` the features read. Fields no declared feature reads are left at their
        defaults, so this state is valid for the released feature set only (ADR-0055 §3)."""
        return WindowState(
            count=self.count or 0,
            amount_sum_minor=self.amount_sum_minor or 0,
            declined_count=self.declined_count or 0,
            outcome_known_count=self.outcome_known_count or 0,
            distinct={d: n for d, n in self.distinct.items() if n > 0},
            aligned_count=self.aligned_count or 0,
            aligned_amount_sum_minor=self.aligned_amount_sum_minor or 0,
            aligned_amount_sum_squares=self.aligned_amount_sum_squares or 0,
        )


@dataclass(frozen=True, slots=True)
class ProfileRow:
    """One `gold.tx_profiles` row: the account's lifetime strictly before `as_of`, reduced."""

    account_id: str
    first_seen_ms: int
    observation_count: int
    amount_median_minor: float | None
    amount_mad_minor: float | None
    habitual_merchants: frozenset[str]
    habitual_mccs: frozenset[str]
    known_devices: frozenset[str]
    home_latitude: float | None
    home_longitude: float | None

    def profile(self) -> Profile:
        return Profile(
            first_seen_at=EventTime(from_millis(self.first_seen_ms)),
            observation_count=self.observation_count,
            amount_median_minor=self.amount_median_minor,
            amount_mad_minor=self.amount_mad_minor,
            habitual_merchants=self.habitual_merchants,
            habitual_mccs=self.habitual_mccs,
            known_devices=self.known_devices,
            home_latitude=self.home_latitude,
            home_longitude=self.home_longitude,
        )


@dataclass(frozen=True, slots=True)
class PreviousRow:
    """One `gold.tx_previous` row: the latest observation on a stream strictly before `as_of`."""

    entity: Entity
    entity_id: str
    stream: Stream
    occurred_ms: int
    latitude: float | None
    longitude: float | None
    card_present: bool

    def observation(self) -> Observation:
        return Observation(
            occurred_at=EventTime(from_millis(self.occurred_ms)),
            latitude=self.latitude,
            longitude=self.longitude,
            card_present=self.card_present,
        )


@dataclass(frozen=True, slots=True)
class ContextRows:
    """Everything Gold holds for one transaction's point-in-time context."""

    transaction_id: str
    as_of_ms: int
    windows: tuple[WindowRow, ...] = ()
    profile: ProfileRow | None = None
    previous: tuple[PreviousRow, ...] = ()

    def context(self, *, complete_since: EventTime | None) -> FeatureContext:
        return feature_context(
            as_of_ms=self.as_of_ms,
            windows=self.windows,
            profile=self.profile,
            previous=self.previous,
            complete_since=complete_since,
        )


def feature_context(
    *,
    as_of_ms: int,
    windows: Iterable[WindowRow],
    profile: ProfileRow | None,
    previous: Iterable[PreviousRow],
    complete_since: EventTime | None,
) -> FeatureContext:
    """A `FeatureContext` from Gold rows. Gold vouches for no period: `complete_since` is the
    caller's evidence-backed claim, or None. The source is `ONLINE_ONLY`, as the reference's
    event-time-complete context says: `RECONCILED` needs hydration-verified history (Q4g)."""
    return FeatureContext(
        as_of=EventTime(from_millis(as_of_ms)),
        source=FeatureSource.ONLINE_ONLY,
        windows={(w.entity, w.entity_id, w.stream, w.window_label): w.state() for w in windows},
        profiles={}
        if profile is None
        else {(Entity.ACCOUNT, profile.account_id): profile.profile()},
        previous={(p.entity, p.entity_id, p.stream): p.observation() for p in previous},
        complete_since=complete_since,
        distinct_dimensions={e: PLAN.distinct_dimensions(e) for e in Entity},
    )


# ----------------------------------------------------------------- progress ---

PLAN_MARKER: Final = "trace_x_gold_plan"
PLAN_FORMAT: Final = 1
OFFSETS_DIR: Final = "offsets"
COMMITS_DIR: Final = "commits"


@dataclass(frozen=True, slots=True)
class SourcePin:
    """One Silver table as a build reads it: exactly this version of exactly this table."""

    table: str
    table_id: str
    version: int
    committed_at_ms: int

    def __post_init__(self) -> None:
        if self.version < 0:
            raise GoldRefusedError(f"{self.table}: version {self.version} is negative")


@dataclass(frozen=True, slots=True)
class BuildPlan:
    """What build `build_id` reads, fixed before it writes anything."""

    build_id: int
    planned_at_ms: int
    sources: tuple[SourcePin, ...]

    def __post_init__(self) -> None:
        if self.build_id < 0:
            raise GoldRefusedError(f"build id {self.build_id} is negative")
        names = [pin.table for pin in self.sources]
        if names != [str(ref) for ref in GOLD_SOURCES]:
            raise GoldRefusedError(
                f"a build plan pins {names}, not {[str(r) for r in GOLD_SOURCES]}"
            )

    def pin(self, ref: TableRef) -> SourcePin:
        return next(p for p in self.sources if p.table == str(ref))

    @property
    def newest_commit_ms(self) -> int:
        return max(pin.committed_at_ms for pin in self.sources)


def plan_text(plan: BuildPlan) -> str:
    """The plan as a batch file in the checkpoint's `offsets/` directory: `v1`, a metadata line,
    then one line per source, so `checkpoints.decide_start` judges Gold's progress by Spark's
    rule."""
    header = {
        PLAN_MARKER: PLAN_FORMAT,
        "build_id": plan.build_id,
        "planned_at_ms": plan.planned_at_ms,
    }
    lines = ["v1", json.dumps(header, sort_keys=True, separators=(",", ":"))]
    lines += [
        json.dumps(
            {
                "table": p.table,
                "table_id": p.table_id,
                "version": p.version,
                "committed_at_ms": p.committed_at_ms,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        for p in plan.sources
    ]
    return "\n".join(lines) + "\n"


def parse_plan(text: str, *, source: Path) -> BuildPlan:
    try:
        lines = text.splitlines()
        if not lines or lines[0] != "v1":
            raise ValueError("not a v1 batch file")
        header = json.loads(lines[1])
        if header.get(PLAN_MARKER) != PLAN_FORMAT:
            raise ValueError(f"not a Gold plan: {header!r}")
        pins = []
        for line in lines[2:]:
            data = json.loads(line)
            pins.append(
                SourcePin(
                    table=str(data["table"]),
                    table_id=str(data["table_id"]),
                    version=int(data["version"]),
                    committed_at_ms=int(data["committed_at_ms"]),
                )
            )
        return BuildPlan(int(header["build_id"]), int(header["planned_at_ms"]), tuple(pins))
    except (ValueError, KeyError, IndexError, TypeError) as exc:
        raise GoldRefusedError(f"unreadable Gold build plan {source}: {exc}") from exc


def _publish_exclusive(path: Path, text: str) -> None:
    """Write `path` whole or not at all, and never over an existing file (two builds race here)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staging = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    with staging.open("x", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.link(staging, path)
    except FileExistsError as exc:
        raise GoldRefusedError(
            f"{path} already exists: another build planned or committed it concurrently"
        ) from exc
    finally:
        staging.unlink()


def write_plan(directory: Path, plan: BuildPlan) -> None:
    _publish_exclusive(directory / OFFSETS_DIR / str(plan.build_id), plan_text(plan))


def read_plan(directory: Path, build_id: int) -> BuildPlan:
    path = directory / OFFSETS_DIR / str(build_id)
    plan = parse_plan(path.read_text(encoding="utf-8"), source=path)
    if plan.build_id != build_id:
        raise GoldRefusedError(f"{path} holds the plan of build {plan.build_id}")
    return plan


def write_commit(directory: Path, build_id: int) -> None:
    body = json.dumps({"trace_x_gold_build": build_id}, sort_keys=True, separators=(",", ":"))
    _publish_exclusive(directory / COMMITS_DIR / str(build_id), f"v1\n{body}\n")


def next_build(progress: SparkProgress) -> tuple[int, bool]:
    """The build to run, and whether it is a replay of a planned, uncommitted build.

    Builds are numbered 0, 1, 2 ... with no gaps; every committed build was planned; at most the one
    after the last commit is planned and uncommitted. Anything else is refused: it is not a history
    this job writes."""
    last = progress.last_committed
    expected = set(range(0 if last is None else last + 1))
    if progress.committed != expected or not progress.committed <= progress.planned:
        raise GoldRefusedError(
            f"Gold progress is not contiguous: committed {sorted(progress.committed)}, planned "
            f"{sorted(progress.planned)}"
        )
    following = 0 if last is None else last + 1
    stray = sorted(b for b in progress.planned if b > following)
    if stray:
        raise GoldRefusedError(f"Gold builds {stray} are planned beyond build {following}")
    return following, following in progress.planned


def lag_ms(finished_ms: int, plan: BuildPlan) -> int:
    """Build lag behind the Silver commit it covers (PHASE3_PLAN §4.3): its finish minus the newest
    commit among the pinned Silver versions."""
    return finished_ms - plan.newest_commit_ms


__all__ = [
    "ALIGNED_FIELDS",
    "AUTHORIZATIONS",
    "BUILDS",
    "COMMITS_DIR",
    "DIMENSION_COLUMNS",
    "DISTINCT_BUCKETS",
    "ENTITY_COLUMNS",
    "GOLD_JOB",
    "GOLD_SOURCES",
    "GOLD_TARGETS",
    "IDENTITY_EVENTS",
    "IDENTITY_TYPE_STREAMS",
    "MINUTE_BUCKETS",
    "NAMESPACE_VALUES",
    "OBSERVATIONS",
    "OBSERVED_OUTCOMES",
    "OFFSETS_DIR",
    "READS",
    "REPLACED_TABLES",
    "SCALAR_FIELDS",
    "TABLE_KEYS",
    "TRANSACTIONS",
    "TX_PREVIOUS",
    "TX_PROFILES",
    "TX_WINDOWS",
    "BuildPlan",
    "ContextRows",
    "GoldPlanError",
    "GoldRefusedError",
    "GoldUniquenessError",
    "PreviousRow",
    "ProfileRow",
    "SourcePin",
    "WindowRow",
    "WindowSpec",
    "compile_windows",
    "distinct_column",
    "feature_context",
    "lag_ms",
    "next_build",
    "parse_plan",
    "plan_text",
    "read_plan",
    "unsupported_declarations",
    "write_commit",
    "write_plan",
]
