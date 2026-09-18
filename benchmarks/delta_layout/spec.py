"""What the layout benchmark compares, how it measures, and how it ranks -- declared before a run.

Pure: nothing here starts a JVM, so every rule below is unit-tested without Spark
(`tests/unit/test_delta_layout_benchmark.py`). `run.py` executes it.

**The layouts** (ADR-0015's options, plus two controls, each a separate Delta table built from the
identical staged rows by the same MERGE Gold's writer issues, `gold.replace_table`):

- `control`: no layout, no OPTIMIZE -- what `gold.observations` is today (ADR-0055 §2).
- `control_compacted`: no layout, plain `OPTIMIZE` (bin-packing). It separates the effect of
  compaction from the effect of a layout: the Z-order and liquid variants are also compacted, so
  without this control a gain from fewer, larger files would be credited to clustering.
- `partitioned_date`: `PARTITIONED BY (event_date)`, `event_date` a generated
  `CAST(occurred_at AS DATE)` column (ADR-0015's partition option).
- `partitioned_date_zorder`: the same, then `OPTIMIZE ... ZORDER BY (account_id)` (ADR-0015).
- `partitioned_stream`: `PARTITIONED BY (stream)`: `stream` is the one predicate every code-backed
  shape carries (`gold.read_context_rows`), and has four values.
- `liquid`: `CLUSTER BY (account_id, occurred_at)`, then `OPTIMIZE` (ADR-0015's liquid option;
  `occurred_at` rather than a derived `event_date`, because liquid clustering orders by value
  ranges and needs no day granularity, and a generated column would add a protocol feature the
  option does not need).

**Why `event_date` is the partition column.** Of the real predicates, `event_id` and `account_id`
have hundreds of thousands to millions of values, far too many for directories; `stream` has four,
with one transaction row per outcome row, so pruning on it can at best halve a scan and is
measured separately (`partitioned_stream`); `occurred_at` bounds the time-range shape and every
time-sliced extract, and a day has a bounded, even size. It is generated from `occurred_at` so
readers keep filtering on `occurred_at` and Delta derives the partition filter
(generated-column partition pruning); a query written against `event_date` would be a different
workload per variant.

**Warm and cold.** Declared up front: the page cache is not flushed (that needs root), so every
measurement is warm-cache. Each (variant, shape, repetition) is executed once, unmeasured, by the
correctness pass before any timing; the measured pass then interleaves the variants, rotating
their order each repetition so drift over the run falls on every variant alike. Selected files and
bytes are cache-independent; bytes read are too; wall time is not, and understates cold I/O.
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, Literal

from benchmarks.delta_layout.generator import (
    COLUMNS,
    DAY_MS,
    GeneratorSpec,
    Vocabulary,
    account_id,
    draw,
    transaction_account,
    transaction_id,
)

MIN_PUBLISHABLE_ROWS: Final = 10_000_000
"""ROADMAP Phase 3 and ADR-0015: the layout decision is taken at ten million rows or more."""
DEFAULT_REPS: Final = 5
"""The repetitions `make bench-layout` declares by default (`run.py --reps`)."""
MIN_PUBLISHABLE_REPS: Final = DEFAULT_REPS
"""A run with fewer repetitions than the declared default is diagnostic, never evidence."""

EXIT_PUBLISHED: Final = 0
"""A valid, publishable run: record, results and report written into the repository."""
EXIT_HARNESS_ERROR: Final = 2
"""The harness could not measure (a refusal, an integrity failure): the run is invalid."""
EXIT_NOT_PUBLISHABLE: Final = 3
"""A valid run that is not publishable: everything written under its own lake directory only."""

SUBJECT: Final = "delta-layout"
CONTROL: Final = "control"
TABLE_KEY: Final = ("observation_identity",)
"""`gold_plan.TABLE_KEYS[OBSERVATIONS]`, the MERGE key (asserted equal by the unit tests)."""

Optimize = Literal["none", "compact", "zorder", "cluster"]


class BenchmarkRefusedError(RuntimeError):
    """A run whose result would be wrong or unattributable. Never reported as a number."""


class LayoutChangedResultsError(BenchmarkRefusedError):
    """Two layouts returned different results for one query: a layout must never change results."""


class MissingMetricError(BenchmarkRefusedError):
    """A scan metric was absent or implausible; it is never read as zero."""


# ---------------------------------------------------------------- vocabulary ---


def vocabulary() -> Vocabulary:
    """The released names, read from the declarations Gold itself uses, never restated."""
    from benchmarks.delta_layout.generator import CHANNEL_WEIGHTS

    from trace_core.domain.enums import AuthorizationOutcome, TransactionChannel
    from trace_core.features.observation import Verification
    from trace_core.features.semantics import Stream
    from trace_core.stream.gold_plan import (
        AUTHORIZATIONS,
        IDENTITY_EVENTS,
        NAMESPACE_VALUES,
        OBSERVED_OUTCOMES,
        TRANSACTIONS,
    )

    vocab = Vocabulary(
        stream_transaction=Stream.TRANSACTION.value,
        stream_outcome=Stream.AUTHORIZATION_OUTCOME.value,
        stream_failed_login=Stream.IDENTITY_FAILED_LOGIN.value,
        stream_identity_change=Stream.IDENTITY_CHANGE.value,
        namespace_transaction=NAMESPACE_VALUES[Stream.TRANSACTION],
        namespace_outcome=NAMESPACE_VALUES[Stream.AUTHORIZATION_OUTCOME],
        namespace_identity=NAMESPACE_VALUES[Stream.IDENTITY_FAILED_LOGIN],
        source_transactions=str(TRANSACTIONS),
        source_outcomes=str(AUTHORIZATIONS),
        source_identity=str(IDENTITY_EVENTS),
        outcome_approved=AuthorizationOutcome.APPROVED.value,
        outcome_declined=AuthorizationOutcome.DECLINED.value,
        verification_verified=Verification.VERIFIED.value,
        channels=tuple(c.value for c in TransactionChannel),
    )
    problems = []
    if NAMESPACE_VALUES[Stream.IDENTITY_CHANGE] != vocab.namespace_identity:
        problems.append("the two identity streams no longer share a namespace")
    if {vocab.outcome_approved, vocab.outcome_declined} != set(OBSERVED_OUTCOMES):
        problems.append(f"Gold observes {OBSERVED_OUTCOMES}, not APPROVED and DECLINED")
    if unknown := [c for c, _ in CHANNEL_WEIGHTS if c not in vocab.channels]:
        problems.append(f"channels {unknown} are not TransactionChannel values")
    if problems:
        raise BenchmarkRefusedError("the generator no longer matches Gold: " + "; ".join(problems))
    return vocab


# ------------------------------------------------------------------ variants ---


@dataclass(frozen=True, slots=True)
class GeneratedColumn:
    name: str
    sql_type: str
    expression: str


EVENT_DATE: Final = GeneratedColumn("event_date", "DATE", "CAST(occurred_at AS DATE)")


@dataclass(frozen=True, slots=True)
class Variant:
    name: str
    description: str
    partition_by: tuple[str, ...] = ()
    cluster_by: tuple[str, ...] = ()
    generated: tuple[GeneratedColumn, ...] = ()
    optimize: Optimize = "none"
    zorder_by: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.partition_by and self.cluster_by:
            raise ValueError(
                f"{self.name}: Delta refuses partitioning with clustering (ADR-0048 h)"
            )
        if (self.optimize == "zorder") != bool(self.zorder_by):
            raise ValueError(f"{self.name}: ZORDER BY columns go with optimize='zorder' only")
        if (self.optimize == "cluster") != bool(self.cluster_by):
            raise ValueError(f"{self.name}: a clustered table is laid out by OPTIMIZE (ADR-0048 h)")
        if set(self.zorder_by) & set(self.partition_by):
            raise ValueError(
                f"{self.name}: Z-ordering by a partition column is refused (ADR-0048 i)"
            )
        known = set(COLUMNS) | {g.name for g in self.generated}
        unknown = [
            c for c in (*self.partition_by, *self.cluster_by, *self.zorder_by) if c not in known
        ]
        if unknown:
            raise ValueError(f"{self.name}: unknown layout columns {unknown}")

    def summary(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "partition_by": list(self.partition_by),
            "cluster_by": list(self.cluster_by),
            "generated": [f"{g.name} {g.sql_type} AS ({g.expression})" for g in self.generated],
            "optimize": self.optimize,
            "zorder_by": list(self.zorder_by),
        }


VARIANTS: Final = (
    Variant(CONTROL, "no layout, no OPTIMIZE: gold.observations as declared today"),
    Variant(
        "control_compacted", "no layout, plain OPTIMIZE: isolates compaction", optimize="compact"
    ),
    Variant(
        "partitioned_date",
        "PARTITIONED BY (event_date), generated from occurred_at",
        partition_by=("event_date",),
        generated=(EVENT_DATE,),
    ),
    Variant(
        "partitioned_date_zorder",
        "PARTITIONED BY (event_date), then OPTIMIZE ZORDER BY (account_id)",
        partition_by=("event_date",),
        generated=(EVENT_DATE,),
        optimize="zorder",
        zorder_by=("account_id",),
    ),
    Variant("partitioned_stream", "PARTITIONED BY (stream)", partition_by=("stream",)),
    Variant(
        "liquid",
        "CLUSTER BY (account_id, occurred_at), then OPTIMIZE",
        cluster_by=("account_id", "occurred_at"),
        optimize="cluster",
    ),
)


def variant_named(name: str) -> Variant:
    for variant in VARIANTS:
        if variant.name == name:
            return variant
    raise KeyError(f"no layout variant {name!r}; known: {[v.name for v in VARIANTS]}")


def quote(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def create_table_sql(variant: Variant, path: str, fields: Sequence[tuple[str, str, bool]]) -> str:
    """`CREATE TABLE delta.<path>` with `fields` as `(name, sql_type, nullable)`: one commit, the
    declared nullability kept (a table created by a write loses it, ADR-0048 k)."""
    columns = [f"{quote(n)} {t}{'' if nullable else ' NOT NULL'}" for n, t, nullable in fields]
    columns += [
        f"{quote(g.name)} {g.sql_type} GENERATED ALWAYS AS ({g.expression})"
        for g in variant.generated
    ]
    layout = ""
    if variant.partition_by:
        layout = f" PARTITIONED BY ({', '.join(quote(c) for c in variant.partition_by)})"
    elif variant.cluster_by:
        layout = f" CLUSTER BY ({', '.join(quote(c) for c in variant.cluster_by)})"
    return f"CREATE TABLE delta.{quote(path)} ({', '.join(columns)}) USING DELTA{layout}"


def optimize_sql(variant: Variant, path: str) -> str | None:
    if variant.optimize == "none":
        return None
    statement = f"OPTIMIZE delta.{quote(path)}"
    if variant.optimize == "zorder":
        statement += f" ZORDER BY ({', '.join(quote(c) for c in variant.zorder_by)})"
    return statement


# -------------------------------------------------------------------- shapes ---


@dataclass(frozen=True, slots=True)
class Shape:
    name: str
    provenance: Literal["code", "adr-0015"]
    """`code`: a query a Gold reader in this repository issues. `adr-0015`: a query ADR-0015 names
    that no reader in the code issues yet; ranked alike, reported separately."""
    source: str
    description: str
    projection: tuple[str, ...]


PROJECTION_CONTEXT: Final = ("event_id", "occurred_ms")

SHAPES: Final = (
    Shape(
        "context_lookup",
        "code",
        "packages/trace_core/stream/gold.py read_context_rows (lines 561-569), transaction_ids "
        "given: gold.observations filtered to stream = TRANSACTION and event_id IN (ids), "
        "projected to (event_id, occurred_ms)",
        "one transaction's point-in-time subject row, by transaction id",
        PROJECTION_CONTEXT,
    ),
    Shape(
        "context_extract",
        "code",
        "packages/trace_core/stream/gold.py read_context_rows (lines 561-569), transaction_ids "
        "None, as eval/parity/lake.py gold_context_rows (lines 144-147) calls it: every "
        "transaction's subject row, projected to (event_id, occurred_ms)",
        "the full context extract parity and training read",
        PROJECTION_CONTEXT,
    ),
    Shape(
        "account_history",
        "adr-0015",
        "ADR-0015 Decision: 'point lookup by account_id'; no reader in the code yet (the "
        "investigation tools of Phase 5 are its expected caller)",
        "every observation of one account, all columns",
        COLUMNS,
    ),
    Shape(
        "time_range",
        "adr-0015",
        "ADR-0015 Decision: 'time-range scan'; no Gold reader in the code yet (hydration reads "
        "the same projection time-ordered, but from Silver: hydration.py observation_frame)",
        "every observation in one 24-hour event-time window, all columns",
        COLUMNS,
    ),
)
TIME_RANGE_MS: Final = DAY_MS


@dataclass(frozen=True, slots=True)
class QueryParams:
    """One execution's parameters, identical for every variant."""

    shape: str
    rep: int
    event_ids: tuple[str, ...] = ()
    account_id: str = ""
    lo_ms: int = 0
    hi_ms: int = 0

    def summary(self) -> dict[str, Any]:
        return {
            "shape": self.shape,
            "rep": self.rep,
            "event_ids": list(self.event_ids),
            "account_id": self.account_id,
            "lo_ms": self.lo_ms,
            "hi_ms": self.hi_ms,
        }


_P_LOOKUP, _P_ACCOUNT, _P_WINDOW = 101, 102, 103


def parameters(spec: GeneratorSpec, reps: int) -> list[QueryParams]:
    """Each shape's parameters for repetitions `0..reps-1`, from the seed alone.

    The account is the account of a uniformly drawn transaction, so accounts are drawn by
    activity -- the account behind a transaction under investigation -- not uniformly."""
    if reps < 1:
        raise ValueError(f"reps must be positive, got {reps}")
    out: list[QueryParams] = []
    window_room = max(1, spec.span_ms - TIME_RANGE_MS)
    for rep in range(reps):
        t = draw(spec.seed, _P_LOOKUP, rep) % spec.transactions
        out.append(QueryParams("context_lookup", rep, event_ids=(transaction_id(t),)))
        out.append(QueryParams("context_extract", rep))
        owner = draw(spec.seed, _P_ACCOUNT, rep) % spec.transactions
        out.append(
            QueryParams(
                "account_history", rep, account_id=account_id(transaction_account(spec, owner))
            )
        )
        lo = spec.start_ms + draw(spec.seed, _P_WINDOW, rep) % window_room
        out.append(QueryParams("time_range", rep, lo_ms=lo, hi_ms=lo + TIME_RANGE_MS))
    return out


def expected_rows(params: QueryParams, spec: GeneratorSpec) -> int | None:
    """What the generator guarantees, where it is known without a scan: every transaction id is
    one TRANSACTION row, and there are `spec.transactions` of them."""
    if params.shape == "context_lookup":
        return len(params.event_ids)
    if params.shape == "context_extract":
        return spec.transactions
    return None


# ----------------------------------------------------------------- metrics ---

MEASUREMENT_FIELDS: Final = (
    "rows",
    "scans",
    "selected_files",
    "selected_bytes",
    "scan_output_rows",
    "input_bytes",
    "input_records",
)
"""`trace_core.stream.tables.ScanMeasurement`'s fields (asserted equal by the unit tests)."""


@dataclass(frozen=True, slots=True)
class ScanNumbers:
    rows: int
    scans: int
    selected_files: int
    selected_bytes: int
    scan_output_rows: int
    input_bytes: int
    input_records: int


def extract_scan_metrics(raw: Mapping[str, object]) -> ScanNumbers:
    """A measurement from `measure_scans` (as a dict), or `MissingMetricError`.

    `measure_scans` already refuses a scan node missing `numFiles`/`filesSize`/`numOutputRows`
    (ADR-0048 §8). This is the second gate, on what reaches the record: a field that is absent,
    not an integer, negative, or zero where the query returned rows is refused -- a query that
    returned rows selected at least one file, of non-zero size, and read bytes."""
    missing = [name for name in MEASUREMENT_FIELDS if name not in raw]
    if missing:
        raise MissingMetricError(f"the scan measurement lacks {missing}; refusing to read zero")
    values: dict[str, int] = {}
    for name in MEASUREMENT_FIELDS:
        value = raw[name]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise MissingMetricError(f"scan metric {name}={value!r} is not a count")
        values[name] = value
    if values["rows"] > 0:
        zero = [
            name
            for name in (
                "scans",
                "selected_files",
                "selected_bytes",
                "input_bytes",
                "input_records",
            )
            if values[name] == 0
        ]
        if zero:
            raise MissingMetricError(
                f"the query returned {values['rows']} rows but reports zero {zero}: the plan that "
                f"ran was not measured (answered from metadata, or a metric went missing)"
            )
    return ScanNumbers(**values)


@dataclass(frozen=True, slots=True)
class QueryRun:
    variant: str
    shape: str
    rep: int
    wall_s: float
    numbers: ScanNumbers

    def summary(self) -> dict[str, Any]:
        return {
            "variant": self.variant,
            "shape": self.shape,
            "rep": self.rep,
            "wall_s": self.wall_s,
            **{name: getattr(self.numbers, name) for name in MEASUREMENT_FIELDS},
        }


@dataclass(frozen=True, slots=True)
class ResultCheck:
    """The correctness pass: a query's row count and order-independent content digest."""

    variant: str
    shape: str
    rep: int
    rows: int
    digest: str


# -------------------------------------------------------------- validation ---


def require_identical_results(
    checks: Sequence[ResultCheck],
    variants: Sequence[str],
    params: Sequence[QueryParams],
    spec: GeneratorSpec,
) -> None:
    """Every variant returned the same rows, with the same content, for every execution; and
    where the generator knows the answer, that answer. Otherwise `LayoutChangedResultsError`."""
    by_key: dict[tuple[str, int], dict[str, ResultCheck]] = {}
    for check in checks:
        slot = by_key.setdefault((check.shape, check.rep), {})
        if check.variant in slot:
            raise BenchmarkRefusedError(f"{check} was recorded twice")
        slot[check.variant] = check
    problems: list[str] = []
    for p in params:
        slot = by_key.get((p.shape, p.rep), {})
        absent = [v for v in variants if v not in slot]
        if absent:
            problems.append(f"{p.shape} rep {p.rep}: no result for {absent}")
            continue
        outcomes = {(slot[v].rows, slot[v].digest) for v in variants}
        if len(outcomes) != 1:
            detail = {v: (slot[v].rows, slot[v].digest) for v in variants}
            problems.append(f"{p.shape} rep {p.rep}: the layouts disagree {detail}")
        expected = expected_rows(p, spec)
        if expected is not None and any(slot[v].rows != expected for v in variants):
            problems.append(
                f"{p.shape} rep {p.rep}: returned {sorted({slot[v].rows for v in variants})} "
                f"rows, the generator guarantees {expected}"
            )
        if not any(slot[v].rows > 0 for v in variants):
            problems.append(f"{p.shape} rep {p.rep}: returned no rows, so measured nothing")
    if problems:
        raise LayoutChangedResultsError("results differ or are wrong: " + "; ".join(problems))


def require_complete_runs(
    runs: Sequence[QueryRun],
    checks: Sequence[ResultCheck],
    variants: Sequence[str],
    params: Sequence[QueryParams],
) -> None:
    """Exactly one measured run per (variant, shape, rep), with the correctness pass's rows."""
    rows = {(c.variant, c.shape, c.rep): c.rows for c in checks}
    seen: dict[tuple[str, str, int], QueryRun] = {}
    for run in runs:
        key = (run.variant, run.shape, run.rep)
        if key in seen:
            raise BenchmarkRefusedError(f"{key} was measured twice")
        seen[key] = run
        if key in rows and rows[key] != run.numbers.rows:
            raise LayoutChangedResultsError(
                f"{key}: measured {run.numbers.rows} rows, the correctness pass counted {rows[key]}"
            )
    wanted = {(v, p.shape, p.rep) for v in variants for p in params}
    if missing := sorted(wanted - seen.keys()):
        raise BenchmarkRefusedError(f"unmeasured executions: {missing[:5]} ({len(missing)})")
    if extra := sorted(seen.keys() - wanted):
        raise BenchmarkRefusedError(f"measured executions nobody planned: {extra[:5]}")


# ----------------------------------------------------------------- ranking ---

RANKING_RULE: Final = (
    "For each shape and variant, take the median over repetitions of bytes read (Spark task "
    "input bytes of the scans), of files selected and of wall time. Divide each by the control's "
    "median for that shape. A variant's score is the geometric mean of its bytes-read ratios over "
    "every shape, each shape weighted equally; lower is better. Ties (scores equal to three "
    "decimal places) are broken by the geometric mean of the files-selected ratios, then by the "
    "geometric mean of the wall-time ratios. Build and OPTIMIZE cost and protocol features are "
    "reported beside the ranking, not folded into it. A zero anywhere is refused, never ranked."
)
"""Declared before measurement. Bytes read is primary because it is what a layout exists to
reduce, it is cache-independent, and it carries to object storage; wall time on a warm local disk
does not. Files selected is second because per-file cost dominates on object storage."""


@dataclass(frozen=True, slots=True)
class ShapeSummary:
    variant: str
    shape: str
    reps: int
    rows_median: float
    input_bytes: float
    input_records: float
    selected_files: float
    selected_bytes: float
    wall_median_s: float
    wall_min_s: float
    wall_max_s: float


def summarize(runs: Iterable[QueryRun]) -> dict[tuple[str, str], ShapeSummary]:
    grouped: dict[tuple[str, str], list[QueryRun]] = {}
    for run in runs:
        grouped.setdefault((run.variant, run.shape), []).append(run)
    out: dict[tuple[str, str], ShapeSummary] = {}
    for (variant, shape), group in grouped.items():
        walls = [r.wall_s for r in group]
        out[(variant, shape)] = ShapeSummary(
            variant=variant,
            shape=shape,
            reps=len(group),
            rows_median=statistics.median(r.numbers.rows for r in group),
            input_bytes=statistics.median(r.numbers.input_bytes for r in group),
            input_records=statistics.median(r.numbers.input_records for r in group),
            selected_files=statistics.median(r.numbers.selected_files for r in group),
            selected_bytes=statistics.median(r.numbers.selected_bytes for r in group),
            wall_median_s=statistics.median(walls),
            wall_min_s=min(walls),
            wall_max_s=max(walls),
        )
    return out


@dataclass(frozen=True, slots=True)
class Ranked:
    rank: int
    variant: str
    bytes_score: float
    files_score: float
    wall_score: float
    ratios: dict[str, dict[str, float]] = field(default_factory=dict)
    """Per shape: this variant's bytes, files and wall ratios to the control."""


def _geomean(values: Sequence[float]) -> float:
    return math.exp(sum(math.log(v) for v in values) / len(values))


def rank(
    summaries: Mapping[tuple[str, str], ShapeSummary],
    variants: Sequence[str],
    shapes: Sequence[str],
    control: str = CONTROL,
) -> list[Ranked]:
    """`RANKING_RULE`, applied. Refuses a missing cell or a zero rather than ranking around it."""
    if control not in variants:
        raise BenchmarkRefusedError(f"the control {control!r} was not measured")
    metrics = {"bytes": "input_bytes", "files": "selected_files", "wall": "wall_median_s"}
    scored: list[tuple[tuple[float, float, float], str, dict[str, dict[str, float]]]] = []
    for variant in variants:
        ratios: dict[str, dict[str, float]] = {}
        for shape in shapes:
            cell, base = summaries.get((variant, shape)), summaries.get((control, shape))
            if cell is None or base is None:
                raise BenchmarkRefusedError(f"no summary for {variant} or {control} on {shape}")
            ratios[shape] = {}
            for label, attribute in metrics.items():
                value, reference = getattr(cell, attribute), getattr(base, attribute)
                if value <= 0 or reference <= 0:
                    raise BenchmarkRefusedError(
                        f"{attribute} is {value} for {variant} and {reference} for {control} on "
                        f"{shape}: a zero cannot be ranked"
                    )
                ratios[shape][label] = value / reference
        scores = tuple(_geomean([ratios[s][label] for s in shapes]) for label in metrics)
        scored.append(((scores[0], scores[1], scores[2]), variant, ratios))
    scored.sort(key=lambda item: (round(item[0][0], 3), round(item[0][1], 3), item[0][2]))
    return [
        Ranked(i + 1, variant, scores[0], scores[1], scores[2], ratios)
        for i, (scores, variant, ratios) in enumerate(scored)
    ]


def shape_leaders(
    summaries: Mapping[tuple[str, str], ShapeSummary], variants: Sequence[str], shape: str
) -> list[str]:
    """Variants ordered for one shape: bytes read, then files selected, then wall time."""
    cells = [summaries[(v, shape)] for v in variants]
    cells.sort(key=lambda c: (c.input_bytes, c.selected_files, c.wall_median_s))
    return [c.variant for c in cells]


# ---------------------------------------------------------------- manifest ---

TOOLCHAIN_KEYS: Final = ("spark", "delta", "hadoop", "java", "scala", "python")

MANIFEST_REQUIRED: Final = (
    # scripts/check_claims.py BENCHMARK_REQUIRED (asserted a subset by the unit tests)
    "run_id",
    "record_type",
    "track",
    "git_commit_sha",
    "dirty_worktree",
    "env_lock_digest",
    "python_version",
    "started_at",
    "finished_at",
    "subject",
    "tool",
    "tool_version",
    "measured",
    # this benchmark's provenance (CLAUDE.md §13; ADR-0048 correction 5)
    "mode",
    "publishable",
    "seed",
    "row_count",
    "generator_version",
    "generator_spec",
    "generator_digest",
    "dataset_digest",
    "toolchain",
    "session",
    "spark_conf",
    "machine",
    "variants",
    "query_shapes",
    "warm_cold_policy",
    "ranking_rule",
)


def missing_manifest_fields(record: Mapping[str, Any]) -> list[str]:
    """Required fields absent or empty, including every toolchain version."""
    missing = [
        name
        for name in MANIFEST_REQUIRED
        if name not in record or record[name] is None or record[name] == "" or record[name] == {}
    ]
    toolchain = record.get("toolchain")
    if isinstance(toolchain, Mapping):
        missing += [f"toolchain.{k}" for k in TOOLCHAIN_KEYS if not toolchain.get(k)]
    measured = record.get("measured")
    if isinstance(measured, Mapping):
        missing += [
            f"measured.{k}"
            for k in (
                "staging",
                "builds",
                "summaries",
                "ranking",
                "shape_leaders",
                "results_file",
                "results_digest",
            )
            if not measured.get(k)
        ]
    return missing


def unpublishable_reasons(
    *,
    rows: int,
    reps: int,
    dirty_at_start: bool,
    dirty_at_end: bool,
    same_commit: bool,
    integrity_passed: bool,
) -> list[str]:
    """Every reason a run may not be published; empty exactly when it may be.

    The worktree is checked at the start and again at the end, before anything is written into
    the repository: a tree that was dirty, became dirty, or moved to another commit during the
    run measured code that no commit names.
    """
    reasons = []
    if rows < MIN_PUBLISHABLE_ROWS:
        reasons.append(f"{rows} rows < {MIN_PUBLISHABLE_ROWS}")
    if reps < MIN_PUBLISHABLE_REPS:
        reasons.append(f"{reps} repetitions < {MIN_PUBLISHABLE_REPS}")
    if dirty_at_start:
        reasons.append("the worktree was dirty at the start")
    if dirty_at_end:
        reasons.append("the worktree was dirty at the end")
    if not same_commit:
        reasons.append("HEAD moved to another commit during the run")
    if not integrity_passed:
        reasons.append("an integrity check failed")
    return reasons


def publishable(
    *,
    rows: int,
    reps: int,
    dirty_at_start: bool,
    dirty_at_end: bool,
    same_commit: bool,
    integrity_passed: bool,
) -> bool:
    return not unpublishable_reasons(
        rows=rows,
        reps=reps,
        dirty_at_start=dirty_at_start,
        dirty_at_end=dirty_at_end,
        same_commit=same_commit,
        integrity_passed=integrity_passed,
    )


@dataclass(frozen=True)
class OutputPaths:
    manifest: Path
    results: Path
    report: Path
    in_repository: bool


def output_paths(
    *,
    publishable: bool,
    run_id: str,
    run_dir: Path,
    manifest_dir: Path,
    results_dir: Path,
    report_path: Path,
) -> OutputPaths:
    """Only a publishable run writes into the repository; any other run writes all three under
    its own (gitignored) lake directory, so it can never overwrite the publishable evidence."""
    if publishable:
        return OutputPaths(
            manifest=manifest_dir / f"{run_id}.json",
            results=results_dir / f"{run_id}.json",
            report=report_path,
            in_repository=True,
        )
    return OutputPaths(
        manifest=run_dir / "manifest.json",
        results=run_dir / "results.json",
        report=run_dir / "REPORT.md",
        in_repository=False,
    )


def exit_code(*, publishable: bool) -> int:
    return EXIT_PUBLISHED if publishable else EXIT_NOT_PUBLISHABLE


__all__ = [
    "CONTROL",
    "DEFAULT_REPS",
    "EXIT_HARNESS_ERROR",
    "EXIT_NOT_PUBLISHABLE",
    "EXIT_PUBLISHED",
    "MANIFEST_REQUIRED",
    "MEASUREMENT_FIELDS",
    "MIN_PUBLISHABLE_REPS",
    "MIN_PUBLISHABLE_ROWS",
    "RANKING_RULE",
    "SHAPES",
    "SUBJECT",
    "TABLE_KEY",
    "VARIANTS",
    "BenchmarkRefusedError",
    "LayoutChangedResultsError",
    "MissingMetricError",
    "OutputPaths",
    "QueryParams",
    "QueryRun",
    "Ranked",
    "ResultCheck",
    "ScanNumbers",
    "Shape",
    "ShapeSummary",
    "Variant",
    "create_table_sql",
    "exit_code",
    "expected_rows",
    "extract_scan_metrics",
    "missing_manifest_fields",
    "optimize_sql",
    "output_paths",
    "parameters",
    "publishable",
    "rank",
    "require_complete_runs",
    "require_identical_results",
    "shape_leaders",
    "summarize",
    "unpublishable_reasons",
    "variant_named",
]
