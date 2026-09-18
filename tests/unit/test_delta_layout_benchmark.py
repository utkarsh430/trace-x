"""The Delta layout benchmark's pure parts, without Spark (Phase 3 Step 14, ADR-0015).

The benchmark itself runs on the JVM (`make bench-layout`); these pin what must hold for its numbers
to mean anything: the generator is deterministic and emits exactly `gold.observations`' schema, the
ranking follows the rule declared before measurement, a missing metric is refused rather than read
as zero, layouts that return different results stop the run, and the run record is complete.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import importlib.util
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pytest
from benchmarks.delta_layout import generator as gen
from benchmarks.delta_layout import report, spec
from benchmarks.delta_layout.generator import GeneratorSpec
from benchmarks.delta_layout.spec import (
    BenchmarkRefusedError,
    LayoutChangedResultsError,
    MissingMetricError,
    QueryRun,
    ResultCheck,
    ScanNumbers,
    ShapeSummary,
)

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
VOCAB = spec.vocabulary()
SMALL = GeneratorSpec.for_rows(20_001, seed=7)


def _check_claims() -> Any:
    loaded = importlib.util.spec_from_file_location(
        "check_claims_for_layout", ROOT / "scripts" / "check_claims.py"
    )
    assert loaded and loaded.loader
    module = importlib.util.module_from_spec(loaded)
    sys.modules["check_claims_for_layout"] = module
    loaded.loader.exec_module(module)
    return module


# ---------------------------------------------------------------- generator ---


def test_the_generator_emits_the_released_gold_observations_columns() -> None:
    from trace_core.stream.gold_features import observations_schema

    assert tuple(f.name for f in observations_schema().fields) == gen.COLUMNS


def test_every_generated_row_passes_sparks_own_schema_verifier() -> None:
    """The verifier `createDataFrame(verifySchema=True)` applies: types and NOT NULL alike."""
    from pyspark.sql.types import _make_type_verifier

    from trace_core.stream.gold_features import observations_schema

    verify = _make_type_verifier(observations_schema())
    rows = list(gen.rows_between(SMALL, VOCAB, 0, SMALL.rows))
    for row in rows:
        verify(row)
    streams = Counter(r[0] for r in rows)
    assert streams[VOCAB.stream_transaction] == SMALL.transactions
    assert streams[VOCAB.stream_outcome] == SMALL.transactions
    assert streams[VOCAB.stream_failed_login] + streams[VOCAB.stream_identity_change] == (
        SMALL.identity_rows
    )
    assert sum(streams.values()) == SMALL.rows


def test_rows_are_a_function_of_seed_and_index_whatever_the_split() -> None:
    whole = list(gen.rows_between(SMALL, VOCAB, 0, SMALL.rows))
    assert whole == [gen.row(SMALL, VOCAB, i) for i in range(SMALL.rows)]
    for parts in (1, 3, 7, 64):
        pieced = [
            r
            for bounds in gen.split(0, SMALL.rows, parts)
            for r in gen.rows_for_slice(SMALL, VOCAB, bounds)
        ]
        assert pieced == whole, f"splitting into {parts} changed the rows"
    # a slice starting on an outcome row (odd index) still yields exactly its rows
    assert list(gen.rows_between(SMALL, VOCAB, 5, 9)) == whole[5:9]
    assert (
        list(gen.rows_between(GeneratorSpec.for_rows(20_001, seed=7), VOCAB, 0, 50)) == whole[:50]
    )
    assert (
        list(gen.rows_between(GeneratorSpec.for_rows(20_001, seed=8), VOCAB, 0, 50)) != whole[:50]
    )


def test_keys_are_unique_and_transactions_are_time_ordered() -> None:
    rows = list(gen.rows_between(SMALL, VOCAB, 0, SMALL.rows))
    key = gen.COLUMNS.index("observation_identity")
    assert len({r[key] for r in rows}) == len(rows), "the MERGE key must be unique"
    ms = gen.COLUMNS.index("occurred_ms")
    at = gen.COLUMNS.index("occurred_at")

    def millis(r: gen.Row) -> int:
        value = r[ms]
        assert isinstance(value, int)
        return value

    tx = [r for r in rows if r[0] == VOCAB.stream_transaction]
    assert [millis(r) for r in tx] == sorted(millis(r) for r in tx)
    assert len({millis(r) for r in tx}) == len(tx), "transaction times are strictly increasing"
    for r in rows:
        occurred = r[at]
        assert isinstance(occurred, dt.datetime) and occurred.tzinfo is not None
        assert occurred == gen.timestamp(millis(r))
        assert SMALL.start_ms <= millis(r) < SMALL.start_ms + SMALL.span_ms + 3_000
    for t in range(0, SMALL.transactions, 97):
        transaction, outcome = rows[2 * t], rows[2 * t + 1]
        assert transaction[2] == outcome[2] == gen.transaction_id(t)
        assert millis(outcome) > millis(transaction), "an outcome follows its transaction"
        assert transaction[6] == outcome[6], "an outcome carries its transaction's account"


def test_activity_is_skewed_as_documented() -> None:
    big = GeneratorSpec.for_rows(400_000, seed=3)
    accounts = Counter(gen.transaction_account(big, t) for t in range(big.transactions))
    top = sum(n for a, n in accounts.items() if a < big.accounts // 100)
    assert 0.08 < top / big.transactions < 0.12, "the busiest 1% of accounts carry about 10%"
    assert gen.skewed_index(10, 0.0, 2.0) == 0 and gen.skewed_index(10, 0.999999, 2.0) == 9


def test_the_spec_digest_names_the_dataset() -> None:
    assert SMALL.digest(VOCAB) == GeneratorSpec.for_rows(20_001, seed=7).digest(VOCAB)
    assert SMALL.digest(VOCAB) != GeneratorSpec.for_rows(20_001, seed=8).digest(VOCAB)
    assert SMALL.digest(VOCAB) != GeneratorSpec.for_rows(20_002, seed=7).digest(VOCAB)
    with pytest.raises(ValueError, match="identity_share"):
        dataclasses.replace(SMALL, identity_share=1.5).validate()


def test_bounded_batches_cover_every_row_once() -> None:
    assert gen.batches(10, 4) == [(0, 4), (4, 8), (8, 10)]
    assert gen.split(0, 10, 3) == [(0, 4), (4, 7), (7, 10)]
    assert gen.split(3, 5, 8) == [(3, 4), (4, 5)]
    with pytest.raises(ValueError):
        gen.batches(10, 0)


# -------------------------------------------------------- declarations match ---


def test_the_benchmark_matches_the_real_declarations() -> None:
    from trace_core.stream.gold_plan import OBSERVATIONS, TABLE_KEYS
    from trace_core.stream.tables import ScanMeasurement

    assert tuple(TABLE_KEYS[OBSERVATIONS]) == spec.TABLE_KEY
    assert tuple(f.name for f in dataclasses.fields(ScanMeasurement)) == spec.MEASUREMENT_FIELDS
    assert spec.CONTROL in [v.name for v in spec.VARIANTS]
    assert len(spec.VARIANTS) >= 4 and len(spec.SHAPES) >= 3
    assert sum(s.provenance == "code" for s in spec.SHAPES) >= 2


def test_layout_ddl_is_what_each_variant_declares() -> None:
    fields = [("stream", "string", False), ("latitude", "double", True)]
    liquid = spec.variant_named("liquid")
    assert spec.create_table_sql(liquid, "/l/t", fields) == (
        "CREATE TABLE delta.`/l/t` (`stream` string NOT NULL, `latitude` double) USING DELTA "
        "CLUSTER BY (`account_id`, `occurred_at`)"
    )
    dated = spec.variant_named("partitioned_date_zorder")
    assert spec.create_table_sql(dated, "/l/t", fields).endswith(
        "`event_date` DATE GENERATED ALWAYS AS (CAST(occurred_at AS DATE))) USING DELTA "
        "PARTITIONED BY (`event_date`)"
    )
    assert spec.optimize_sql(dated, "/l/t") == "OPTIMIZE delta.`/l/t` ZORDER BY (`account_id`)"
    assert spec.optimize_sql(spec.variant_named("control"), "/l/t") is None
    assert spec.optimize_sql(spec.variant_named("control_compacted"), "/l/t") == (
        "OPTIMIZE delta.`/l/t`"
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"partition_by": ("stream",), "cluster_by": ("account_id",), "optimize": "cluster"},
        {"partition_by": ("stream",), "optimize": "zorder", "zorder_by": ("stream",)},
        {"cluster_by": ("account_id",)},
        {"partition_by": ("no_such_column",)},
    ],
)
def test_a_layout_delta_refuses_is_refused_at_declaration(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        spec.Variant("bad", "bad", **kwargs)


def test_query_parameters_are_deterministic_and_point_at_real_data() -> None:
    params = spec.parameters(SMALL, 4)
    assert params == spec.parameters(SMALL, 4)
    assert {p.shape for p in params} == {s.name for s in spec.SHAPES}
    rows = list(gen.rows_between(SMALL, VOCAB, 0, SMALL.rows))
    tx_ids = {r[2] for r in rows if r[0] == VOCAB.stream_transaction}
    accounts = {r[6] for r in rows}
    for p in params:
        if p.shape == "context_lookup":
            assert set(p.event_ids) <= tx_ids
            assert spec.expected_rows(p, SMALL) == len(p.event_ids)
        elif p.shape == "context_extract":
            assert spec.expected_rows(p, SMALL) == SMALL.transactions
        elif p.shape == "account_history":
            assert p.account_id in accounts
        else:
            assert SMALL.start_ms <= p.lo_ms < p.hi_ms <= SMALL.start_ms + SMALL.span_ms
            assert p.hi_ms - p.lo_ms == spec.TIME_RANGE_MS


# ------------------------------------------------------------------ metrics ---

_GOOD = {
    "rows": 1,
    "scans": 1,
    "selected_files": 3,
    "selected_bytes": 1_000,
    "scan_output_rows": 10,
    "input_bytes": 400,
    "input_records": 10,
}


def test_scan_metrics_are_extracted_from_a_recorded_measurement() -> None:
    assert spec.extract_scan_metrics(_GOOD) == ScanNumbers(**_GOOD)
    empty = dict.fromkeys(_GOOD, 0)
    assert spec.extract_scan_metrics(empty).rows == 0, "a query with no rows may read nothing"


@pytest.mark.parametrize("name", list(_GOOD))
def test_a_missing_metric_is_refused_not_read_as_zero(name: str) -> None:
    raw = {k: v for k, v in _GOOD.items() if k != name}
    with pytest.raises(MissingMetricError, match="lacks"):
        spec.extract_scan_metrics(raw)


@pytest.mark.parametrize("value", [None, -1, True, "3", 2.0])
def test_a_metric_that_is_not_a_count_is_refused(value: object) -> None:
    with pytest.raises(MissingMetricError):
        spec.extract_scan_metrics({**_GOOD, "selected_bytes": value})


@pytest.mark.parametrize("name", ["scans", "selected_files", "selected_bytes", "input_bytes"])
def test_rows_without_a_measured_scan_are_refused(name: str) -> None:
    with pytest.raises(MissingMetricError, match="zero"):
        spec.extract_scan_metrics({**_GOOD, name: 0})


# --------------------------------------------------------------- validation ---

_PARAMS = spec.parameters(SMALL, 2)
_VARIANTS = ["control", "liquid"]


def _checks(**override: tuple[int, str]) -> list[ResultCheck]:
    out = []
    for p in _PARAMS:
        expected = spec.expected_rows(p, SMALL)
        rows = expected if expected is not None else 5
        for v in _VARIANTS:
            n, digest = override.get(f"{v}:{p.shape}:{p.rep}", (rows, f"{rows}:h"))
            out.append(ResultCheck(v, p.shape, p.rep, n, digest))
    return out


def test_identical_results_across_layouts_pass() -> None:
    spec.require_identical_results(_checks(), _VARIANTS, _PARAMS, SMALL)


def test_results_with_differing_row_counts_across_variants_are_rejected() -> None:
    with pytest.raises(LayoutChangedResultsError, match="disagree"):
        spec.require_identical_results(
            _checks(**{"liquid:account_history:1": (4, "4:h")}), _VARIANTS, _PARAMS, SMALL
        )


def test_results_with_differing_content_are_rejected() -> None:
    with pytest.raises(LayoutChangedResultsError, match="disagree"):
        spec.require_identical_results(
            _checks(**{"liquid:time_range:0": (5, "5:other")}), _VARIANTS, _PARAMS, SMALL
        )


def test_results_the_generator_contradicts_are_rejected_even_when_layouts_agree() -> None:
    n = SMALL.transactions - 1
    both = {f"{v}:context_extract:0": (n, f"{n}:h") for v in _VARIANTS}
    with pytest.raises(LayoutChangedResultsError, match="guarantees"):
        spec.require_identical_results(_checks(**both), _VARIANTS, _PARAMS, SMALL)


def test_a_missing_or_empty_result_is_rejected() -> None:
    partial = [c for c in _checks() if not (c.variant == "liquid" and c.shape == "time_range")]
    with pytest.raises(LayoutChangedResultsError, match="no result"):
        spec.require_identical_results(partial, _VARIANTS, _PARAMS, SMALL)
    empty = {f"{v}:account_history:0": (0, "0:None") for v in _VARIANTS}
    with pytest.raises(LayoutChangedResultsError, match="no rows"):
        spec.require_identical_results(_checks(**empty), _VARIANTS, _PARAMS, SMALL)


def _runs(checks: list[ResultCheck]) -> list[QueryRun]:
    return [
        QueryRun(c.variant, c.shape, c.rep, 0.1, ScanNumbers(**{**_GOOD, "rows": c.rows}))
        for c in checks
    ]


def test_the_measured_pass_must_be_complete_and_agree_with_the_correctness_pass() -> None:
    checks = _checks()
    runs = _runs(checks)
    spec.require_complete_runs(runs, checks, _VARIANTS, _PARAMS)
    with pytest.raises(BenchmarkRefusedError, match="unmeasured"):
        spec.require_complete_runs(runs[1:], checks, _VARIANTS, _PARAMS)
    with pytest.raises(BenchmarkRefusedError, match="twice"):
        spec.require_complete_runs([*runs, runs[0]], checks, _VARIANTS, _PARAMS)
    wrong = [dataclasses.replace(runs[0], numbers=ScanNumbers(**{**_GOOD, "rows": 999})), *runs[1:]]
    with pytest.raises(LayoutChangedResultsError, match="correctness pass"):
        spec.require_complete_runs(wrong, checks, _VARIANTS, _PARAMS)


# ------------------------------------------------------------------ ranking ---


def _cell(variant: str, shape: str, bytes_read: float, files: float, wall: float) -> ShapeSummary:
    return ShapeSummary(variant, shape, 3, 1, bytes_read, 1, files, 1, wall, wall, wall)


def _summaries(table: dict[str, list[tuple[float, float, float]]]) -> dict[Any, ShapeSummary]:
    shapes = ["a", "b"]
    return {
        (v, s): _cell(v, s, *cells[i]) for v, cells in table.items() for i, s in enumerate(shapes)
    }


def test_the_ranking_follows_the_declared_rule() -> None:
    summaries = _summaries(
        {
            "control": [(100, 10, 1.0), (100, 10, 1.0)],
            "fast_wall": [(100, 10, 0.1), (100, 10, 0.1)],  # only wall time better
            "fewer_bytes": [(50, 10, 2.0), (50, 10, 2.0)],  # bytes halved, slower
            "mixed": [(25, 10, 1.0), (400, 10, 1.0)],  # geometric mean 1.0: equal to control
        }
    )
    ranked = spec.rank(summaries, ["control", "fast_wall", "fewer_bytes", "mixed"], ["a", "b"])
    assert ranked[0].variant == "fewer_bytes", "bytes read is primary"
    assert ranked[0].bytes_score == pytest.approx(0.5)
    tied = [r.variant for r in ranked[1:]]
    assert tied == ["fast_wall", "control", "mixed"] or tied == ["fast_wall", "mixed", "control"]
    assert next(r for r in ranked if r.variant == "control").ratios["a"] == {
        "bytes": 1.0,
        "files": 1.0,
        "wall": 1.0,
    }
    reordered = spec.rank(summaries, ["mixed", "fewer_bytes", "fast_wall", "control"], ["a", "b"])
    assert [r.variant for r in reordered][:2] == ["fewer_bytes", "fast_wall"]


def test_files_break_a_bytes_tie_before_wall_time() -> None:
    summaries = _summaries(
        {
            "control": [(100, 10, 1.0), (100, 10, 1.0)],
            "fewer_files": [(100, 2, 5.0), (100, 2, 5.0)],
            "faster": [(100, 10, 0.2), (100, 10, 0.2)],
        }
    )
    ranked = spec.rank(summaries, ["control", "fewer_files", "faster"], ["a", "b"])
    assert [r.variant for r in ranked] == ["fewer_files", "faster", "control"]


def test_a_zero_or_missing_cell_is_refused_not_ranked() -> None:
    zero = _summaries({"control": [(100, 1, 1.0), (100, 1, 1.0)], "x": [(0, 1, 1.0), (1, 1, 1.0)]})
    with pytest.raises(BenchmarkRefusedError, match="zero"):
        spec.rank(zero, ["control", "x"], ["a", "b"])
    gap = _summaries({"control": [(100, 1, 1.0), (100, 1, 1.0)], "x": [(1, 1, 1.0), (1, 1, 1.0)]})
    del gap[("x", "b")]
    with pytest.raises(BenchmarkRefusedError, match="no summary"):
        spec.rank(gap, ["control", "x"], ["a", "b"])
    with pytest.raises(BenchmarkRefusedError, match="control"):
        spec.rank(gap, ["x"], ["a"])


def test_summaries_take_medians_over_repetitions() -> None:
    runs = [
        QueryRun("control", "a", rep, wall, ScanNumbers(**{**_GOOD, "input_bytes": b}))
        for rep, (wall, b) in enumerate([(0.3, 10), (0.1, 1_000), (0.2, 30)])
    ]
    cell = spec.summarize(runs)[("control", "a")]
    assert (cell.input_bytes, cell.wall_median_s, cell.wall_min_s, cell.wall_max_s) == (
        30,
        0.2,
        0.1,
        0.3,
    )


# ---------------------------------------------------------------- manifest ---


def _record() -> dict[str, Any]:
    summaries = _summaries(
        {"control": [(100, 10, 1.0), (100, 10, 1.0)], "liquid": [(10, 1, 0.5), (50, 5, 0.9)]}
    )
    ranking = spec.rank(summaries, ["control", "liquid"], ["a", "b"])
    shapes: list[dict[str, Any]] = [
        {"name": n, "provenance": "code", "source": "src", "description": "d", "projection": []}
        for n in ("a", "b")
    ]
    build = {
        "variant": "control",
        "num_files": 4,
        "size_bytes": 4 * 1024 * 1024,
        "file_bytes_min": 1024 * 1024,
        "file_bytes_median": 1024 * 1024,
        "file_bytes_max": 1024 * 1024,
        "partitions": 1,
        "min_reader_version": 1,
        "min_writer_version": 2,
        "table_features": ["appendOnly", "invariants"],
        "write_s": 1.0,
        "optimize_s": None,
    }
    return {
        "run_id": "bench-20260918-000000-delta-layout-0123abcd",
        "record_type": "BENCHMARK",
        "track": "SYNTHETIC",
        "subject": spec.SUBJECT,
        "tool": "spark+delta",
        "tool_version": "spark 4.0.1 / delta 4.0.1",
        "git_commit_sha": "0123abcd" * 5,
        "dirty_worktree": False,
        "env_lock_digest": "sha256:x",
        "python_version": "3.12.9",
        "started_at": "2026-09-18T00:00:00+00:00",
        "finished_at": "2026-09-18T01:00:00+00:00",
        "mode": "full",
        "publishable": True,
        "seed": 42,
        "row_count": 10_000_000,
        "generator_version": gen.GENERATOR_VERSION,
        "generator_spec": dataclasses.asdict(SMALL),
        "generator_digest": SMALL.digest(VOCAB),
        "dataset_digest": "10000000:123",
        "toolchain": dict.fromkeys(spec.TOOLCHAIN_KEYS, "x"),
        "session": {
            "master": "local[4]",
            "driver_memory": "4g",
            "shuffle_partitions": 4,
            "reps": 3,
        },
        "spark_conf": {"spark.sql.adaptive.enabled": "true"},
        "machine": {"cpu_count": 12},
        "variants": [
            {"name": "control", "description": "c"},
            {"name": "liquid", "description": "l"},
        ],
        "query_shapes": shapes,
        "warm_cold_policy": "warm",
        "ranking_rule": spec.RANKING_RULE,
        "measured": {
            "staging": {"rows": 1},
            "builds": [build, {**build, "variant": "liquid", "optimize_s": 2.0}],
            "summaries": [dataclasses.asdict(s) for s in summaries.values()],
            "ranking": [dataclasses.asdict(r) for r in ranking],
            "shape_leaders": {
                s: spec.shape_leaders(summaries, ["control", "liquid"], s) for s in "ab"
            },
            "results_file": "benchmarks/delta_layout/results/x.json",
            "results_digest": "sha256:y",
        },
    }


def test_a_complete_record_satisfies_this_benchmark_and_the_claims_linter() -> None:
    record = _record()
    assert spec.missing_manifest_fields(record) == []
    linter = _check_claims()
    assert set(linter.BENCHMARK_REQUIRED) <= set(spec.MANIFEST_REQUIRED)
    assert linter.incomplete_fields(record) == []


@pytest.mark.parametrize("name", spec.MANIFEST_REQUIRED)
def test_every_required_field_is_enforced(name: str) -> None:
    record = _record()
    del record[name]
    assert name in spec.missing_manifest_fields(record)


def test_every_toolchain_version_and_measured_section_is_enforced() -> None:
    record = _record()
    record["toolchain"] = {**record["toolchain"], "delta": ""}
    record["measured"] = {**record["measured"], "ranking": []}
    assert {"toolchain.delta", "measured.ranking"} <= set(spec.missing_manifest_fields(record))


def test_only_a_clean_full_run_is_publishable() -> None:
    assert spec.publishable(rows=10_000_000, reps=3, dirty_worktree=False)
    assert not spec.publishable(rows=200_000, reps=5, dirty_worktree=False)
    assert not spec.publishable(rows=10_000_000, reps=2, dirty_worktree=False)
    assert not spec.publishable(rows=10_000_000, reps=5, dirty_worktree=True)


def test_the_report_puts_every_number_under_a_run_id_and_names_no_winner_itself() -> None:
    record = _record()
    text = report.render(record)
    headings = [line for line in text.splitlines() if line.startswith("## ")]
    assert headings and all(f"run_id: {record['run_id']}" in h for h in headings)
    assert report.CAVEAT in text
    ranking = text.split("## Ranking")[1]
    assert "| 1 | `liquid` |" in ranking and "| 2 | `control` |" in ranking, "printed as computed"
