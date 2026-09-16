"""A parity report rendered from PARITY records only, citing each record's run_id (ADR-0056 §6).

`python -m eval.parity.report [--manifests eval/manifest] [--out PATH]`. Nothing here computes or
estimates: every figure is read from a record, under a heading that names the record's run_id, so
`make check-claims` can resolve it once the PARITY record type is declared there.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

from eval.parity.record import RECORD_TYPE, missing_fields

CAVEAT: Final = (
    "> *Synthetic benchmark results measure relative arm performance under known causal "
    "ground truth.\n"
    "> They do **not** estimate real-world fraud detection performance. See Track B "
    "(run_id …) for\n"
    "> external generalization.*"
)


def load_records(directory: Path) -> list[dict[str, Any]]:
    records = []
    for path in sorted(directory.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("record_type") == RECORD_TYPE:
            records.append(data)
    return records


def _fraction(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):.6f}"


def _section(record: Mapping[str, Any]) -> list[str]:
    run_id = record["run_id"]
    results = record["results"]
    partition = record["partition"]
    publishable = bool(record.get("publishable"))
    lines = [
        f"## {partition['name']} ({record['mode']}) — run_id: {run_id}",
        "",
        f"- Verdict: **{record['verdict']}**; "
        + ("publishable." if publishable else "**NOT publishable**."),
        f"- Commit `{record['git_commit_sha'][:12]}`, dirty worktree: {record['dirty_worktree']}; "
        f"partition digest `{partition.get('digest')}`; lateness model "
        f"`{record['lateness_model_digest']}`.",
    ]
    if problems := missing_fields(record):
        lines.append(f"- Record incomplete: {problems}")
    if "error" in results:
        return [*lines, f"- The run stopped before comparing: {results['error']}", ""]
    served, complete, skew = (
        results["as_served"],
        results["event_time_complete"],
        results["arrival_skew"],
    )
    lines += [
        f"- As-served implementation parity: {served['compared']} comparisons, "
        f"{served['divergent']} divergent.",
        f"- Event-time-complete implementation parity: {complete['compared']} comparisons, "
        f"{complete['divergent']} divergent.",
        "- Declared exceptions (ADR-0046 §8), counted apart: "
        f"{served.get('declared_exceptions', 0)} "
        f"{served.get('declared_exceptions_by_situation', {})}.",
        "- Excluded, as-served absences the read did not vouch for (ADR-0046 §5): "
        f"{served.get('not_vouched_excluded', 0)} "
        f"({_fraction(served.get('not_vouched_fraction'))} of every comparison offered); "
        f"per feature and window: {served.get('not_vouched_by_feature_window', {})}.",
        f"- Arrival skew on windowed counters: {skew['numerator']} of {skew['denominator']} "
        f"(fraction {_fraction(skew['fraction'])}), {skew['capped_excluded']} capped windows "
        f"excluded; verdict {skew['verdict']}.",
        "",
        "| Approximate feature | Stratum | Comparisons | RMS relative error | Within bound |",
        "|---|---|---|---|---|",
    ]
    for feature_id, detail in sorted(served["approximate"].items()):
        for name, stratum in detail["strata"].items():
            lines.append(
                f"| {feature_id} | {name} | {stratum['count']} | "
                f"{_fraction(stratum.get('rms_relative_error'))} | "
                f"{stratum.get('within_bound', 'n/a')} |"
            )
    violations = record["guard"]["violations"]
    lines += ["", f"- Guard violations: {len(violations)}"]
    lines += [f"  - {v['kind']}: {v['detail']}" for v in violations]
    return [*lines, ""]


def render(records: Sequence[Mapping[str, Any]]) -> str:
    lines = ["# Feature parity (P3.feature-parity)", "", CAVEAT, ""]
    if not records:
        lines.append("No PARITY record exists.")
    for record in sorted(records, key=lambda r: str(r["run_id"])):
        lines += _section(record)
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m eval.parity.report")
    parser.add_argument("--manifests", type=Path, default=Path("eval/manifest"))
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    text = render(load_records(args.manifests))
    if args.out is None:
        sys.stdout.write(text)
    else:
        args.out.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
