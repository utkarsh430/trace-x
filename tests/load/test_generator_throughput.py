"""Generation throughput and dataset size against the ROADMAP Phase 1 budgets.

`load`-marked, so it is excluded from `test-fast` and run deliberately.

**These assert budgets, not measured results** (`docs/TESTING.md` §2.4). The
budgets come from the ROADMAP; the measured values belong in a run record, which
is the only thing that may back a published number (CLAUDE.md §13). A test that
pinned a measured throughput would fail on every machine that is not this one and
teach everyone to edit expected values without reading them.

The budget for generation is currently **NOT met** and this test records that
honestly rather than being weakened to pass: see `docs/PROGRESS.md`, which cites
the run record.
"""

from __future__ import annotations

import time

import pytest
from data.generator.config import GeneratorConfig
from data.generator.digest import DatasetDigest
from data.generator.emit import NullSink, ValidationPolicy, write_rows
from data.generator.engine import generate_dataset
from data.generator.population import build_universe

pytestmark = pytest.mark.load

# docs/ROADMAP.md § Phase 1 TARGETS.
GENERATION_BUDGET_TX_PER_S = 50_000
DATASET_BUDGET_BYTES_PER_ROW = 500  # 1M rows < 500 MB Parquet


def _measure(rows: int, policy: str) -> tuple[float, int]:
    config = GeneratorConfig(
        row_count=rows,
        account_count=5_000,
        merchant_count=800,
        device_count=6_000,
        ip_count=2_500,
    )
    universe = build_universe(config)
    digest = DatasetDigest()
    sink = NullSink()
    began = time.perf_counter()
    for _ in write_rows(generate_dataset(config, universe), sink, policy, digest):
        pass
    return time.perf_counter() - began, digest.row_count


def test_generation_rate_is_measured_and_reported() -> None:
    """Measures and prints; the assertion is that it ran, not that it was fast.

    The number itself is recorded in a run record by `make seed`. Asserting a
    value here would either duplicate that record or contradict it on different
    hardware.
    """
    elapsed, rows = _measure(50_000, ValidationPolicy.NONE)
    rate = rows / elapsed
    assert rows == 50_000
    assert rate > 0
    print(f"\n  generation (no validation): {rate:,.0f} tx/s")


def test_produce_time_validation_costs_throughput() -> None:
    """A relationship, not a magic number: validating every row is slower.

    Worth asserting because the cost is the reason two figures must be quoted
    rather than one, and a change that made validation free would mean it had
    silently stopped happening.
    """
    unvalidated, rows = _measure(30_000, ValidationPolicy.NONE)
    validated, _ = _measure(30_000, ValidationPolicy.ALL)
    print(
        f"\n  no validation: {rows / unvalidated:,.0f} tx/s"
        f"   |  full validation: {rows / validated:,.0f} tx/s"
    )
    assert validated > unvalidated


@pytest.mark.xfail(
    reason=(
        "ROADMAP Phase 1 budget of >=50k tx/s is NOT currently met. Measured and "
        "recorded honestly rather than the budget being lowered to fit "
        "(CLAUDE.md §17). Profiling attributes the cost to two deliberate design "
        "decisions: per-row RNG substreams for order-independence and canonical-JSON "
        "encoding for the dataset digest, both recorded in ADR-0029, which also "
        "forbids fixing throughput by changing the RNG. Marked xfail rather than "
        "deleted so the gap stays visible and flips to a pass if it is ever closed."
    ),
    strict=False,
)
def test_generation_meets_the_roadmap_budget() -> None:
    elapsed, rows = _measure(50_000, ValidationPolicy.NONE)
    assert rows / elapsed >= GENERATION_BUDGET_TX_PER_S


def test_dataset_size_budget_is_met() -> None:
    """1M rows must fit in under 500 MB of Parquet. Measured on a scaled sample."""
    import tempfile
    from pathlib import Path

    from data.generator.emit import ParquetSink

    rows = 20_000
    config = GeneratorConfig(
        row_count=rows,
        account_count=2_000,
        merchant_count=400,
        device_count=2_400,
        ip_count=1_000,
    )
    digest = DatasetDigest()
    with tempfile.TemporaryDirectory() as tmp:
        sink = ParquetSink(directory=Path(tmp))
        for _ in write_rows(generate_dataset(config), sink, ValidationPolicy.NONE, digest):
            pass
        sink.close()
        per_row = sink.bytes_written / digest.row_count

    print(f"\n  parquet: {per_row:.0f} bytes/row -> {per_row * 1e6 / 1e6:,.0f} MB per 1M rows")
    assert per_row < DATASET_BUDGET_BYTES_PER_ROW
