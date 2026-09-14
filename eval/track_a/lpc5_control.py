"""Diagnostic: run `LPC-5` over one generation; say whether a control failed for its named reasons.

Not acceptance evidence (eval/track_a/criteria/lpc-5.md §1.3). This runner generates in memory and
prints the report, either at a chosen scale or from a frozen manifest.

With `--manifest`, it checks the transaction `dataset_digest`, row count and scenario-config digest
against the manifest, as §1.2 requires of a control. A mismatch makes the run invalid (exit 2).

    python -m eval.track_a.lpc5_control --manifest eval/track_a/eval-v1.manifest.json  # §14.1
    python -m eval.track_a.lpc5_control --control eval-v1  # fast-lane scale smoke
    python -m eval.track_a.lpc5_control --control eval-v2  # the gated generator, as it is
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import resource
import sys
import time
from collections.abc import Iterable, Iterator
from pathlib import Path

from data.generator.config import BaselineIdentityConfig, GeneratorConfig
from data.generator.digest import DatasetDigest, canonical_bytes
from data.generator.engine import GeneratedRow, coverage_mix, generate_dataset, plan_fraud
from data.generator.lpc5 import controls, run
from data.generator.lpc5 import declaration as d
from data.generator.lpc5.availability import reference_availability
from data.generator.lpc5.frame import knowledge
from data.generator.population import build_universe


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
        baseline_identity=BaselineIdentityConfig() if args.control == "eval-v2" else None,
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, help="regenerate gate-off from this manifest")
    parser.add_argument("--control", choices=("eval-v1", "eval-v2"), default="eval-v1")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--rows", type=int, default=120_000)
    parser.add_argument("--accounts", type=int, default=4_800)
    parser.add_argument("--merchants", type=int, default=360)
    parser.add_argument("--devices", type=int, default=5_760)
    parser.add_argument("--ips", type=int, default=2_400)
    parser.add_argument("--fraud-rate", type=float, default=0.005)
    parser.add_argument("--limit", type=int, default=8, help="findings shown per check")
    args = parser.parse_args(argv)

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
    universe = build_universe(config)
    know = knowledge(universe)
    digest = DatasetDigest()
    report = run.evaluate(
        _tee_transactions(generate_dataset(config, universe), digest),
        know,
        availability=reference_availability(know.start_ms),
        mix=coverage_mix(plan_fraud(config, universe)[0]),
    )
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
    if control == "eval-v1":
        unmet = controls.negative_control_unmet(report)
        out.write(f"§14.1 negative control: {'MET' if not unmet else 'NOT MET'}\n")
        for item in unmet:
            out.write(f"  unmet: {item}\n")
    out.write(f"wall {elapsed:.0f} s, peak rss {peak_mib:.0f} MiB (macOS reports bytes)\n")
    return 2 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
