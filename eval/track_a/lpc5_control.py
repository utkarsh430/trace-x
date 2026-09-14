"""Run `LPC-5` over one generation; say whether a control failed for its named reasons.

Diagnostic, not acceptance evidence (eval/track_a/criteria/lpc-5.md §1.3), except the acceptance run
on a frozen candidate below. This runner generates in memory and prints the report, either at a
chosen scale or from a frozen manifest.

With `--manifest`, it checks the transaction `dataset_digest`, row count and scenario-config digest
against the manifest, as §1.2 requires of a control. A mismatch makes the run invalid (exit 2).

    python -m eval.track_a.lpc5_control --manifest eval/track_a/eval-v1.manifest.json  # §14.1
    python -m eval.track_a.lpc5_control --control eval-v1  # fast-lane scale smoke
    python -m eval.track_a.lpc5_control --control eval-v2  # the gated generator, as it is
    python -m eval.track_a.lpc5_control --control zero-rate  # §14.2
    python -m eval.track_a.lpc5_control --control ablation --correction N12  # §14.3, one
    python -m eval.track_a.lpc5_control --control ablation --candidate candidate.manifest.json

§14.2 and §14.3 are taken from a candidate: the gated generator as it is, or the configuration
of `--candidate`, whose scenario-config digest is checked. Each control changes only what the
criterion names (`controls.zero_rate_control`, `controls.ablation_control`), and a control whose
run is invalid meets nothing. They are acceptance evidence only on the frozen candidate at
acceptance scale (§16.4).

With `--control eval-v2 --candidate`, it is the acceptance run of §1.2 on the frozen candidate
(`eval/track_a/freeze_candidate.py`). It refuses a dirty tree, a changed configuration and a
criterion other than the one frozen with the candidate. It digests every stream while the rows are
read, and a stream whose digest or row count differs from the manifest stops the run before any
check, as invalid (exit 2). Otherwise it exits 0 on PASS and 1 on FAIL.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import resource
import sys
import time
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Any

from data.generator.config import CORRECTIONS, BaselineIdentityConfig, GeneratorConfig
from data.generator.digest import DatasetDigest, canonical_bytes
from data.generator.engine import GeneratedRow, coverage_mix, generate_dataset, plan_fraud
from data.generator.lpc5 import controls, run
from data.generator.lpc5 import declaration as d
from data.generator.lpc5.availability import reference_availability
from data.generator.lpc5.frame import knowledge
from data.generator.population import build_universe
from data.generator.record import git_commit_sha, is_dirty
from eval.track_a import freeze_candidate

from trace_core.domain.errors import DeterminismError


def _config(args: argparse.Namespace) -> GeneratorConfig:
    return GeneratorConfig(
        seed=args.seed,
        row_count=args.rows,
        account_count=args.accounts,
        merchant_count=args.merchants,
        device_count=args.devices,
        ip_count=args.ips,
        fraud_rate=args.fraud_rate,
        start_at=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
        end_at=dt.datetime(2026, 3, 1, tzinfo=dt.UTC),
        baseline_identity=BaselineIdentityConfig() if args.control != "eval-v1" else None,
    )


def _tee_transactions(
    rows: Iterable[GeneratedRow], digest: DatasetDigest
) -> Iterator[GeneratedRow]:
    """The transaction-only digest eval-v1 was frozen with (`emit.write_rows`)."""
    topic = d.TOPICS[d.Population.TX]
    for row in rows:
        if row.topic == topic:
            digest.update_bytes(canonical_bytes(row.event))
        yield row


def _manifest_mismatches(
    manifest: dict[str, object], config: GeneratorConfig, digest: DatasetDigest
) -> list[str]:
    observed = {
        "dataset_digest": digest.hexdigest(),
        "row_count": digest.row_count,
        "fraud_scenario_config_digest": config.digest(),
    }
    return [
        f"{key}: manifest {manifest.get(key)!r}, regenerated {value!r}"
        for key, value in observed.items()
        if manifest.get(key) != value
    ]


def stream_mismatches(
    recorded: Mapping[str, Any], digests: Mapping[str, DatasetDigest]
) -> list[str]:
    """Every stream whose digest or row count differs from the candidate manifest's (§1.2)."""
    observed = {
        topic: {"digest": digest.hexdigest(), "rows": digest.row_count}
        for topic, digest in digests.items()
    }
    return [
        f"{topic}: manifest {recorded.get(topic)!r}, regenerated {observed.get(topic)!r}"
        for topic in sorted({*recorded, *observed})
        if recorded.get(topic) != observed.get(topic)
    ]


def _verify_streams(
    rows: Iterable[GeneratedRow], recorded: Mapping[str, Any]
) -> Iterator[GeneratedRow]:
    """Digest every stream as the frame reads it, over the bytes `emit.write_rows` digests.

    Raises at the end of the stream, before any check runs, when a digest or row count moved."""
    digests: dict[str, DatasetDigest] = {}
    for row in rows:
        digest = digests.get(row.topic)
        if digest is None:
            digest = digests[row.topic] = DatasetDigest()
        digest.update_bytes(canonical_bytes(row.event))
        yield row
    moved = stream_mismatches(recorded, digests)
    if moved:
        raise DeterminismError("; ".join(moved))


def evaluate_config(
    config: GeneratorConfig,
    digest: DatasetDigest | None = None,
    *,
    recorded_streams: Mapping[str, Any] | None = None,
) -> run.Lpc5Report:
    """`LPC-5` over one in-memory generation of `config`, with availability and its mix.

    `recorded_streams`, a candidate manifest's `streams`, makes a moved stream raise
    `DeterminismError` before any check runs (§1.2)."""
    universe = build_universe(config)
    know = knowledge(universe)
    rows: Iterable[GeneratedRow] = generate_dataset(config, universe)
    if digest is not None:
        rows = _tee_transactions(rows, digest)
    if recorded_streams is not None:
        rows = _verify_streams(rows, recorded_streams)
    return run.evaluate(
        rows,
        know,
        availability=reference_availability(know.start_ms),
        mix=coverage_mix(plan_fraud(config, universe)[0]),
    )


def control_configs(
    control: str, candidate: GeneratorConfig, correction: str
) -> list[tuple[str, str | None, GeneratorConfig]]:
    """What a §14.2 or §14.3 run evaluates: (label, the correction disabled, configuration)."""
    if control == "zero-rate":
        return [("§14.2 zero-rate control", None, controls.zero_rate_control(candidate))]
    chosen = CORRECTIONS if correction == "all" else (correction,)
    return [(f"§14.3 ablation {c}", c, controls.ablation_control(candidate, c)) for c in chosen]


def control_unmet(correction: str | None, report: run.Lpc5Report) -> list[str]:
    """Why a control did not fail as required; empty when it did. An invalid run meets nothing."""
    if report.invalid:
        return [f"the run is invalid: {reason}" for reason in report.invalid]
    if correction is None:
        return controls.zero_rate_unmet(report)
    return controls.ablation_unmet(correction, report)


def acceptance_refusals(manifest: Mapping[str, Any], config: GeneratorConfig) -> list[str]:
    """Why an acceptance run may not start: provenance or criterion out of step (§0, §1.2)."""
    refusals: list[str] = []
    if config.baseline_identity is None:
        refusals.append("the candidate has the eval-v2 gate off")
    recorded = manifest.get("fraud_scenario_config_digest")
    if recorded != config.digest():
        refusals.append(f"configuration digest: manifest {recorded!r}, now {config.digest()!r}")
    current = freeze_candidate.criterion()
    if manifest.get("criterion") != current:
        refusals.append(f"criterion: frozen with {manifest.get('criterion')!r}, now {current!r}")
    if current["sha256"] != d.CRITERION_SHA256:
        refusals.append("the criterion document does not match the declaration's digest")
    if not manifest.get("streams"):
        refusals.append("the manifest records no stream digests")
    if is_dirty():
        refusals.append("the worktree is dirty, so the run could not be cited")
    return refusals


def _run_acceptance(path: Path, limit: int) -> int:
    out = sys.stdout
    manifest = json.loads(path.read_text(encoding="utf-8"))
    config = GeneratorConfig.from_mapping(manifest["config"])
    refusals = acceptance_refusals(manifest, config)
    if refusals:
        out.write("LPC-5 acceptance run: refused (invalid)\n")
        for item in refusals:
            out.write(f"  {item}\n")
        return 2
    out.write(
        f"LPC-5 acceptance run (§1.2): candidate={path} run_id={manifest.get('run_id')} "
        f"rows={config.row_count} commit={git_commit_sha()} criterion revision {d.REVISION}\n"
    )
    started = time.monotonic()
    try:
        report = evaluate_config(config, recorded_streams=manifest["streams"])
    except DeterminismError as exc:
        out.write(f"candidate stream digests: MISMATCH (invalid), before any check: {exc}\n")
        return 2
    elapsed = time.monotonic() - started
    peak_mib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)
    out.write(f"candidate stream digests: verified ({len(manifest['streams'])} streams)\n")
    out.write(report.format(limit=limit) + "\n")
    out.write(f"instances per scenario: {dict(sorted(report.instance_counts.items()))}\n")
    disclosure = (manifest.get("coverage_floor") or {}).get("disclosure")
    out.write(f"§14.4 disclosure: {disclosure or 'G2 added no instances'}\n")
    out.write(f"wall {elapsed:.0f} s, peak rss {peak_mib:.0f} MiB (macOS reports bytes)\n")
    return 0 if report.passed else 1


def _run_controls(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    out = sys.stdout
    if args.candidate is not None:
        manifest = json.loads(args.candidate.read_text(encoding="utf-8"))
        candidate = GeneratorConfig.from_mapping(manifest["config"])
        recorded = manifest.get("fraud_scenario_config_digest")
        if recorded is not None and recorded != candidate.digest():
            out.write(
                f"candidate digest: MISMATCH (invalid): manifest {recorded!r}, configuration "
                f"{candidate.digest()!r}\n"
            )
            return 2
        source = f"candidate={args.candidate}"
    else:
        candidate = _config(args)
        source = f"the gated generator as it is, seed={candidate.seed}"
    try:
        runs = control_configs(args.control, candidate, args.correction)
    except ValueError as exc:
        parser.error(str(exc))
    out.write(
        f"diagnostic unless on the frozen candidate at acceptance scale (§16.4): {source} "
        f"rows={candidate.row_count}\n"
    )
    unmet_controls = 0
    for label, correction, config in runs:
        started = time.monotonic()
        report = evaluate_config(config)
        unmet = control_unmet(correction, report)
        unmet_controls += bool(unmet)
        verdict = "MET" if not unmet else "NOT MET"
        out.write(f"{label}: {verdict} ({time.monotonic() - started:.0f} s)\n")
        for item in unmet:
            out.write(f"  unmet: {item}\n")
        if len(runs) == 1:
            out.write(report.format(limit=args.limit) + "\n")
    return 1 if unmet_controls else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, help="regenerate gate-off from this manifest")
    parser.add_argument(
        "--control", choices=("eval-v1", "eval-v2", "zero-rate", "ablation"), default="eval-v1"
    )
    parser.add_argument(
        "--correction",
        choices=(*CORRECTIONS, "all"),
        default="all",
        help="the correction a §14.3 ablation disables",
    )
    parser.add_argument(
        "--candidate", type=Path, help="take §14.2 and §14.3 from this candidate manifest"
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--rows", type=int, default=120_000)
    parser.add_argument("--accounts", type=int, default=4_800)
    parser.add_argument("--merchants", type=int, default=360)
    parser.add_argument("--devices", type=int, default=5_760)
    parser.add_argument("--ips", type=int, default=2_400)
    parser.add_argument("--fraud-rate", type=float, default=0.005)
    parser.add_argument("--limit", type=int, default=8, help="findings shown per check")
    args = parser.parse_args(argv)
    if args.candidate is not None and args.control in ("eval-v1", "eval-v2"):
        if args.control == "eval-v1" or args.manifest is not None:
            parser.error("--candidate takes --control eval-v2 (acceptance), zero-rate or ablation")
        return _run_acceptance(args.candidate, args.limit)
    if args.control in ("zero-rate", "ablation"):
        if args.manifest is not None:
            parser.error("--manifest regenerates eval-v1; take §14.2 and §14.3 from --candidate")
        return _run_controls(args, parser)

    manifest: dict[str, object] | None = None
    if args.manifest is not None:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        config = GeneratorConfig.from_mapping(manifest["config"])  # type: ignore[arg-type]
        if config.baseline_identity is not None:
            parser.error("--manifest regenerates a gate-off dataset; this one has the gate on")
        control = "eval-v1"
    else:
        config = _config(args)
        control = args.control

    started = time.monotonic()
    digest = DatasetDigest()
    report = evaluate_config(config, digest)
    elapsed = time.monotonic() - started
    peak_mib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)

    out = sys.stdout
    source = f"manifest={args.manifest}" if manifest is not None else f"seed={config.seed}"
    out.write(
        f"diagnostic (not acceptance evidence): control={control} rows={config.row_count} "
        f"{source}\n"
    )
    mismatches = _manifest_mismatches(manifest, config, digest) if manifest is not None else []
    if manifest is not None:
        out.write(f"manifest digests: {'verified' if not mismatches else 'MISMATCH (invalid)'}\n")
        for item in mismatches:
            out.write(f"  {item}\n")
    out.write(report.format(limit=args.limit) + "\n")
    out.write(f"instances per scenario: {dict(sorted(report.instance_counts.items()))}\n")
    block = config.baseline_identity
    if block is not None and not block.disabled_corrections:
        # An ablation whose check already fails without it cannot show its correction mattered.
        firing = [c for c in CORRECTIONS if controls.ABLATION_CHECKS[c](report)]
        out.write(f"§14.3 checks already failing with no correction disabled: {firing or 'none'}\n")
    if control == "eval-v1":
        unmet = controls.negative_control_unmet(report)
        out.write(f"§14.1 negative control: {'MET' if not unmet else 'NOT MET'}\n")
        for item in unmet:
            out.write(f"  unmet: {item}\n")
    out.write(f"wall {elapsed:.0f} s, peak rss {peak_mib:.0f} MiB (macOS reports bytes)\n")
    return 2 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
