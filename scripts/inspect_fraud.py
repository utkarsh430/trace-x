#!/usr/bin/env python3
"""Sample injected fraud episodes for human inspection.

ROADMAP Phase 1 MANUAL VALIDATION: "inspect ... a sample of each fraud pattern
for plausibility."

**This script connects as `trace_eval`, and it has to.** Ground truth is
unreachable from the application role (ADR-0004) and unreadable even by the
generator that wrote it (ADR-0031), so joining transactions to their labels
requires the one role permitted to read labels. That the inspection tool needs a
different credential from everything else is the isolation working, visibly.

Judgement this supports: do the injected episodes look like the fraud typologies
they claim to be, and do the recorded `causal_evidence_keys` actually explain
them? Every Phase 9 evidence metric inherits that judgement.
"""

from __future__ import annotations

import argparse
import collections
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "packages"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-version", default="eval-v1")
    parser.add_argument("--per-pattern", type=int, default=2)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    import psycopg

    password = os.getenv("TRACE_EVAL_DB_PASSWORD")
    if not password:
        print(
            "TRACE_EVAL_DB_PASSWORD is not set. Ground truth is readable only by "
            "trace_eval (ADR-0004); no other credential can produce this report.",
            file=sys.stderr,
        )
        return 2

    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT", "5442")),
        dbname=os.getenv("POSTGRES_DB", "tracex"),
        user="trace_eval",
        password=password,
    )
    lines: list[str] = [
        f"# Injected fraud — `{args.dataset_version}`",
        "",
        "> Produced by `scripts/inspect_fraud.py`, which connects as **`trace_eval`** —",
        "> the only role that may read ground truth (ADR-0004). Not even the generator",
        "> that wrote these labels can read them back (ADR-0031).",
        "",
    ]
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT dataset_id, row_count, dataset_digest FROM groundtruth.datasets "
            "WHERE dataset_version = %s",
            (args.dataset_version,),
        )
        found = cur.fetchone()
        if not found:
            print(f"no dataset {args.dataset_version!r} in groundtruth", file=sys.stderr)
            return 1
        dataset_id, row_count, digest = found
        lines += [f"- rows: **{row_count:,}**", f"- digest: `{digest}`", ""]

        cur.execute(
            "SELECT fraud_pattern, count(*) FROM groundtruth.transaction_labels "
            "WHERE dataset_id = %s AND is_fraud GROUP BY 1 ORDER BY 2 DESC",
            (dataset_id,),
        )
        counts = cur.fetchall()
        total = sum(c for _, c in counts)
        lines += ["## Pattern mix", "", "```"]
        for pattern, count in counts:
            lines.append(f"  {pattern:<26} {count:>6,}  {count / total:6.2%}")
        lines += [f"  {'TOTAL':<26} {total:>6,}", "```", ""]

        cur.execute(
            "SELECT fraud_pattern, count(DISTINCT scenario_instance_id) "
            "FROM groundtruth.transaction_labels WHERE dataset_id = %s AND is_fraud "
            "GROUP BY 1 ORDER BY 1",
            (dataset_id,),
        )
        lines += ["## Episodes per pattern", "", "```"]
        for pattern, episodes in cur.fetchall():
            lines.append(f"  {pattern:<26} {episodes:>5,} episodes")
        lines += ["```", ""]

        lines += ["## Causal evidence keys, as recorded", ""]
        cur.execute(
            "SELECT l.fraud_pattern, e.evidence_kind, count(*) "
            "FROM groundtruth.transaction_labels l "
            "JOIN groundtruth.causal_evidence e "
            "  ON e.dataset_id = l.dataset_id AND e.transaction_id = l.transaction_id "
            "WHERE l.dataset_id = %s AND l.is_fraud GROUP BY 1, 2 ORDER BY 1, 2",
            (dataset_id,),
        )
        per_pattern: dict[str, list[str]] = collections.defaultdict(list)
        for pattern, kind, _count in cur.fetchall():
            per_pattern[pattern].append(kind)
        lines += ["```"]
        for pattern in sorted(per_pattern):
            lines.append(f"  {pattern:<26} {', '.join(sorted(per_pattern[pattern]))}")
        lines += ["```", ""]

        lines += ["## Sample episodes", ""]
        for pattern, _count in counts:
            cur.execute(
                "SELECT instance_id, participants, transaction_count "
                "FROM groundtruth.scenario_instances "
                "WHERE dataset_id = %s AND fraud_pattern = %s LIMIT %s",
                (dataset_id, pattern, args.per_pattern),
            )
            lines += [f"### {pattern}", "", "```"]
            for instance_id, participants, tx_count in cur.fetchall():
                summary = ", ".join(
                    f"{key}={len(value)}" for key, value in sorted(participants.items())
                )
                lines.append(f"  {instance_id:<28} {tx_count:>4} tx   {summary}")
            lines += ["```", ""]

    report = "\n".join(lines)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(report + "\n")
        print(f"wrote {args.out}")
    else:
        print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
