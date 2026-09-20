"""`make seed` — generate a dataset, write ground truth, record the run.

Output goes through `rich`, not `print`: ruff's T20 forbids `print` in library
code, and a CLI that writes structured progress is easier to read in CI logs.

The command does four things in one pass over the data, because a 1M-row dataset
should be generated once rather than once per consumer:

1. generate events,
2. encode / validate / digest / write them,
3. collect labels,
4. write ground truth and emit a run record.

Ground truth is written **last and separately**, as `trace_generator`, into a
schema no other credential can read (ADR-0004, ADR-0031). `--no-groundtruth`
skips that step for a purely local dataset, and says so in the run record rather
than leaving it implicit.
"""

from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.table import Table

from data.generator.config import GENERATOR_VERSION, GeneratorConfig
from data.generator.digest import DatasetDigest
from data.generator.emit import (
    JsonlSink,
    KafkaSink,
    NullSink,
    ParquetSink,
    Sink,
    ValidationPolicy,
    write_rows,
)
from data.generator.engine import generate_dataset
from data.generator.labels import TransactionLabel
from data.generator.population import build_universe
from data.generator.record import GeneratorRunRecord, new_run_id
from data.generator.scenarios import ScenarioInstance
from trace_core.domain.errors import GroundTruthAccessError, MissingDependencyError

app = typer.Typer(add_completion=False, help="TRACE-X Track A transaction generator.")
console = Console()

DEFAULT_OUT = Path("data/generated")


@app.command()
def seed(
    rows: Annotated[int, typer.Option(help="Number of transactions to generate.")] = 10_000,
    seed_value: Annotated[int, typer.Option("--seed", help="Master seed.")] = 42,
    dataset_version: Annotated[
        str, typer.Option(help="Dataset identity, e.g. eval-v1.")
    ] = "dev-v1",
    out: Annotated[Path, typer.Option(help="Output directory.")] = DEFAULT_OUT,
    sink: Annotated[str, typer.Option(help="jsonl | parquet | kafka | none")] = "jsonl",
    validate: Annotated[str, typer.Option(help="all | sample | none")] = ValidationPolicy.ALL,
    fraud_rate: Annotated[float, typer.Option(help="Target fraudulent share.")] = 0.005,
    accounts: Annotated[int, typer.Option(help="Account population size.")] = 2_000,
    merchants: Annotated[int, typer.Option(help="Merchant population size.")] = 400,
    compress: Annotated[bool, typer.Option(help="gzip the JSONL output.")] = False,
    groundtruth: Annotated[bool, typer.Option(help="Write labels to PostgreSQL.")] = True,
    replace: Annotated[
        bool,
        typer.Option(
            help=(
                "Overwrite an existing dataset_version. NEVER use on a frozen dataset: "
                "eval-v1 is referenced by digest and must not be regenerated in place "
                "(docs/EVALUATION.md section 2). Recorded in the run record."
            )
        ),
    ] = False,
    bootstrap: Annotated[str, typer.Option(help="Kafka bootstrap servers.")] = "localhost:9092",
    record_dir: Annotated[Path | None, typer.Option(help="Where to write the run record.")] = None,
    manifest: Annotated[
        Path | None,
        typer.Option(
            help=(
                "Generate exactly a frozen manifest's configuration (eval-v2's gate included). "
                "The sizing and seed options are ignored; the configuration digest must match, "
                "and every recorded stream digest must be reproduced before ground truth is "
                "written."
            )
        ),
    ] = None,
) -> None:
    """Generate a dataset and record the run."""
    if validate not in ValidationPolicy.CHOICES:
        raise typer.BadParameter(f"--validate must be one of {ValidationPolicy.CHOICES}")

    frozen: dict[str, Any] | None = None
    if manifest is None:
        config = GeneratorConfig(
            seed=seed_value,
            row_count=rows,
            fraud_rate=fraud_rate,
            account_count=accounts,
            merchant_count=merchants,
            device_count=max(accounts, accounts * 6 // 5),
            ip_count=max(1, accounts // 2),
        )
    else:
        frozen = json.loads(manifest.read_text(encoding="utf-8"))
        config = GeneratorConfig.from_mapping(frozen["config"])
        recorded = frozen.get("fraud_scenario_config_digest")
        if recorded != config.digest():
            raise typer.BadParameter(
                f"{manifest}: configuration digest {config.digest()} does not match the recorded "
                f"{recorded}; this manifest does not describe its own configuration"
            )
        version = frozen.get("dataset_version")
        if dataset_version not in ("dev-v1", version):
            raise typer.BadParameter(
                f"--dataset-version {dataset_version!r} contradicts {manifest}, which describes "
                f"{version!r}"
            )
        dataset_version = str(version)
        seed_value = config.seed
        rows = config.row_count
        fraud_rate = config.fraud_rate

    started = dt.datetime.now(dt.UTC)
    run_id = new_run_id(dataset_version, started)
    console.print(f"[bold]TRACE-X generator[/bold]  run_id=[cyan]{run_id}[/cyan]")
    console.print(
        f"  rows={rows:,}  seed={seed_value}  fraud_rate={fraud_rate}  validate={validate}"
    )

    universe = build_universe(config)
    active_sink = _build_sink(sink, out / dataset_version, compress, bootstrap)

    digest = DatasetDigest()
    streams: dict[str, DatasetDigest] = {}
    labels = []
    instances = {}
    began = time.perf_counter()
    try:
        for row in write_rows(
            generate_dataset(config, universe),
            active_sink,
            validate,
            digest,
            topic_digests=streams,
        ):
            if row.label is not None:
                labels.append(row.label)
            if row.scenario_instance is not None:
                instances[row.scenario_instance.instance_id] = row.scenario_instance
    finally:
        active_sink.close()
    elapsed = time.perf_counter() - began

    fraud_count = sum(1 for label in labels if label.is_fraud)
    measured = {
        "generation_seconds": round(elapsed, 4),
        "generation_rate_tx_per_s": round(digest.row_count / elapsed, 1) if elapsed else None,
        "transactions": digest.row_count,
        "fraudulent_transactions": fraud_count,
        "realised_fraud_rate": round(fraud_count / max(1, digest.row_count), 6),
        "output_bytes": getattr(active_sink, "bytes_written", None),
        "sink": sink,
        # Recorded, not implicit: a dataset written over an earlier one of the
        # same version is a different artefact from one written once.
        "replaced_existing": replace,
    }

    record = GeneratorRunRecord(
        run_id=run_id,
        generator_version=GENERATOR_VERSION,
        seed=seed_value,
        fraud_scenario_config_digest=config.digest(),
        dataset_version=dataset_version,
        dataset_digest=digest.hexdigest(),
        row_count=digest.row_count,
        started_at=started.isoformat().replace("+00:00", "Z"),
        finished_at=dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
        validation_policy=validate,
        measured=measured,
    )

    if frozen is not None:
        moved = _manifest_mismatches(frozen, digest, streams)
        if moved:
            console.print("[red]the generation does not reproduce its manifest[/red]")
            for item in moved:
                console.print(f"  {item}")
            raise typer.Exit(code=2)
        console.print(f"  reproduces [cyan]{manifest}[/cyan]: every recorded digest matches")

    if groundtruth:
        _write_groundtruth(record, labels, list(instances.values()), replace=replace)
    else:
        console.print("  [yellow]ground truth NOT written[/yellow] (--no-groundtruth)")

    path = record.write(record_dir)
    _report(record, measured, path)


def _manifest_mismatches(
    frozen: dict[str, Any], digest: DatasetDigest, streams: dict[str, DatasetDigest]
) -> list[str]:
    """Every digest or row count the manifest records that this generation did not reproduce."""
    problems: list[str] = []
    for key, value in (("dataset_digest", digest.hexdigest()), ("row_count", digest.row_count)):
        if key in frozen and frozen[key] != value:
            problems.append(f"{key}: manifest {frozen[key]!r}, generated {value!r}")
    for topic, recorded in sorted((frozen.get("streams") or {}).items()):
        stream = streams.get(topic)
        generated = (
            None if stream is None else {"digest": stream.hexdigest(), "rows": stream.row_count}
        )
        if generated != recorded:
            problems.append(f"{topic}: manifest {recorded!r}, generated {generated!r}")
    return problems


def _build_sink(kind: str, directory: Path, compress: bool, bootstrap: str) -> Sink:
    if kind == "jsonl":
        return JsonlSink(directory=directory, compress=compress)
    if kind == "parquet":
        return ParquetSink(directory=directory)
    if kind == "kafka":
        # Fails loudly without the `stream` extra. Never silently a file.
        return KafkaSink(bootstrap_servers=bootstrap)
    if kind == "none":
        return NullSink()
    raise typer.BadParameter(f"unknown sink {kind!r}; expected jsonl, parquet, kafka or none")


def _write_groundtruth(
    record: GeneratorRunRecord,
    labels: list[TransactionLabel],
    instances: list[ScenarioInstance],
    *,
    replace: bool = False,
) -> None:
    from data.generator.groundtruth import DatasetRecord, write_dataset

    try:
        write_dataset(
            DatasetRecord(
                dataset_version=record.dataset_version,
                seed=record.seed,
                generator_version=record.generator_version,
                fraud_scenario_config_digest=record.fraud_scenario_config_digest,
                dataset_digest=record.dataset_digest,
                row_count=record.row_count,
            ),
            labels,
            instances,
            replace=replace,
        )
    except (GroundTruthAccessError, MissingDependencyError) as exc:
        # Loud and fatal. A dataset whose ground truth was not written is
        # unevaluable, and discovering that during a Phase 9 run -- after the
        # events have already been consumed -- is far worse than failing here.
        console.print(f"[red]ground truth was NOT written:[/red] {exc}")
        raise typer.Exit(code=2) from exc
    console.print(f"  ground truth written for [cyan]{record.dataset_version}[/cyan]")


def _report(record: GeneratorRunRecord, measured: dict[str, Any], path: Path) -> None:
    table = Table(show_header=False, box=None, pad_edge=False)
    table.add_row("transactions", f"{measured['transactions']:,}")
    table.add_row("fraudulent", f"{measured['fraudulent_transactions']:,}")
    table.add_row("realised fraud rate", f"{measured['realised_fraud_rate']:.4%}")
    table.add_row("generation rate", f"{measured['generation_rate_tx_per_s']:,.0f} tx/s")
    if measured["output_bytes"]:
        table.add_row("output size", f"{measured['output_bytes'] / 1e6:,.1f} MB")
    table.add_row("dataset digest", record.dataset_digest[:26] + "...")
    console.print(table)
    console.print(f"  run record: [cyan]{path}[/cyan]")

    problems = record.validate()
    if problems:
        console.print(f"  [red]record incomplete:[/red] {problems}")
        raise typer.Exit(code=2)
    if record.dirty_worktree:
        console.print(
            "  [yellow]dirty worktree: this run is NOT publishable[/yellow] "
            "(docs/EVALUATION.md §8 rule 4)"
        )


if __name__ == "__main__":
    app()
