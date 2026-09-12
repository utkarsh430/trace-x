#!/usr/bin/env python3
"""Replay a generated stream through the gateway and check where fraud lands.

ROADMAP Phase 2, MANUAL VALIDATION: *"Replay a generated stream through the
gateway; confirm known-fraud transactions land in HIGH/CRITICAL bands."*

**This lives in `eval/` because of who it connects as.** It reads
`groundtruth.transaction_labels`, and the only role permitted to do that is
`trace_eval` (ADR-0004). The application, the gateway and every agent connect as
`trace_app`, which has no grant on that schema at all -- so this check is
structurally impossible to perform from inside the thing being checked, which is
the entire point. `tests/unit/test_groundtruth_not_referenced.py` bounds the
application-side code that may even mention the schema; this file is outside that
boundary by design.

**Nothing here influences a decision.** The gateway is driven over HTTP exactly
as a payment processor would drive it, and labels are joined to the responses
*afterwards*. The transactions carry no hint of their label -- Phase 1 asserts
that ids and payloads never reveal one -- so the gateway cannot have known.

**What this is not.** It is a distribution check, not a quality metric. Phase 2
has no model, the thresholds are declared rather than fitted, and precision and
recall on synthetic data would be a number with no meaning attached (CLAUDE.md
§13 rule 3: quality numbers come only from the EVAL tier). What it answers is
narrower and worth answering: *does the hot path put known fraud in the bands
that open an investigation, more often than it does for legitimate traffic?* A
"no" here means the rules are blind, and that is a Phase 2 defect rather than a
Phase 4 modelling question.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import uuid
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

ROOT: Final = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "packages"))
sys.path.insert(0, str(ROOT))

REPORT: Final = ROOT / "benchmarks" / "gateway" / "triage-bands.md"
TRIAGE_BANDS: Final = frozenset({"HIGH", "CRITICAL"})


@dataclass
class Replayed:
    """One transaction's decision, before any label is attached."""

    transaction_id: str
    risk_band: str
    score: float
    decision: str
    fired_rules: tuple[str, ...]
    degraded: bool


@dataclass
class Outcome:
    """Band distribution, split by label. Labels are joined LAST."""

    by_label: dict[bool, Counter[str]] = field(default_factory=dict)
    rules_by_label: dict[bool, Counter[str]] = field(default_factory=dict)
    unlabelled: int = 0

    def triage_rate(self, *, fraudulent: bool) -> float | None:
        bands = self.by_label.get(fraudulent)
        if not bands or sum(bands.values()) == 0:
            return None
        return sum(bands[b] for b in TRIAGE_BANDS if b in bands) / sum(bands.values())


def _eval_dsn() -> str:
    """The evaluation role's DSN. `trace_eval` is the only role that may read
    ground truth, and only the harness connects as it (ADR-0004)."""
    return "postgresql://{u}:{p}@{h}:{port}/{db}".format(
        u=os.environ.get("TRACE_EVAL_DB_USER", "trace_eval"),
        p=os.environ.get("TRACE_EVAL_DB_PASSWORD", ""),
        h=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5442"),
        db=os.environ.get("POSTGRES_DB", "tracex"),
    )


def _service_token() -> str:
    explicit = os.environ.get("TRACE_LOAD_TOKEN")
    if explicit:
        return explicit
    for key, value in os.environ.items():
        if key.startswith("TRACE_SERVICE_TOKEN_") and value:
            return f"{key[len('TRACE_SERVICE_TOKEN_') :].lower()}.{value}"
    raise SystemExit(
        "no service token available. Export the TRACE_SERVICE_TOKEN_<ID> variable the "
        "gateway was started with; every request is authenticated (SECURITY §3)."
    )


def replay(events: list[dict[str, Any]], *, base_url: str) -> list[Replayed]:
    """Drive the gateway over HTTP, one transaction at a time.

    Sequential rather than concurrent: the features are event-time windowed, and
    firing a burst concurrently would have the gateway score transactions in an
    order the generated stream never had -- which would change the answer for
    reasons that have nothing to do with the rules.
    """
    import httpx

    token = _service_token()
    replayed: list[Replayed] = []
    with httpx.Client(base_url=base_url, timeout=10.0) as client:
        for event in events:
            payload = _to_request(event)
            response = client.post(
                "/v1/transactions",
                json=payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "X-Idempotency-Key": f"replay-{uuid.uuid4().hex}",
                },
            )
            if response.status_code != 200:
                raise SystemExit(
                    f"gateway returned {response.status_code} for "
                    f"{payload['transaction_id']}: {response.text[:300]}"
                )
            body = response.json()
            replayed.append(
                Replayed(
                    transaction_id=body["transaction_id"],
                    risk_band=body["risk_band"],
                    score=body["score"],
                    decision=body["decision"],
                    fired_rules=tuple(r["rule_id"] for r in body["reasons"]),
                    degraded=body["degraded"],
                )
            )
    return replayed


def _to_request(event: dict[str, Any]) -> dict[str, Any]:
    """A `tx.raw.v1` event as the gateway's request contract.

    `ingested_at` is dropped: it is processing time, stamped by the gateway, and
    the request contract forbids it (ADR-0026).
    """
    payload = dict(event["payload"])
    payload.pop("memo", None)
    request = {k: v for k, v in payload.items() if v not in (None, "")}
    request["occurred_at"] = event["envelope"]["occurred_at"]
    return request


def join_labels(replayed: list[Replayed], *, dataset_version: str) -> Outcome:
    """Attach ground truth, AS `trace_eval`, after every decision was made."""
    import psycopg

    outcome = Outcome()
    ids = [r.transaction_id for r in replayed]
    with psycopg.connect(_eval_dsn()) as conn:
        rows = conn.execute(
            """
            SELECT l.transaction_id, l.is_fraud
            FROM groundtruth.transaction_labels l
            JOIN groundtruth.datasets d ON d.dataset_id = l.dataset_id
            WHERE d.dataset_version = %s AND l.transaction_id = ANY(%s)
            """,
            (dataset_version, ids),
        ).fetchall()
    labels = {str(tid): bool(is_fraud) for tid, is_fraud in rows}

    for item in replayed:
        label = labels.get(item.transaction_id)
        if label is None:
            outcome.unlabelled += 1
            continue
        outcome.by_label.setdefault(label, Counter())[item.risk_band] += 1
        rules = outcome.rules_by_label.setdefault(label, Counter())
        for rule in item.fired_rules:
            rules[rule] += 1
    return outcome


def render(outcome: Outcome, *, dataset_version: str, replayed: int) -> str:
    fraud_rate = outcome.triage_rate(fraudulent=True)
    legit_rate = outcome.triage_rate(fraudulent=False)
    lines = [
        "# Gateway triage bands on a replayed Track A stream",
        "",
        "> ROADMAP Phase 2 MANUAL VALIDATION. Produced by `eval/replay/gateway_replay.py`,",
        "> which connects as **`trace_eval`** -- the only role permitted to read ground truth",
        "> (ADR-0004). The gateway scored every transaction over HTTP before any label was",
        "> joined, and the transactions carry no hint of their label.",
        "",
        "**This is a distribution check, not a quality metric.** Phase 2 has no model and its",
        "thresholds are declared rather than fitted, so precision and recall here would be",
        "numbers with no meaning attached (CLAUDE.md §13). The question it answers is whether",
        "the hot path puts known fraud into the bands that open an investigation more often",
        "than it does legitimate traffic. A 'no' would mean the rules are blind.",
        "",
        f"Dataset `{dataset_version}`, {replayed:,} transactions replayed.",
        "",
        "| label | LOW | MEDIUM | HIGH | CRITICAL | triaged |",
        "|---|---|---|---|---|---|",
    ]
    for label, name in ((True, "fraudulent"), (False, "legitimate")):
        bands = outcome.by_label.get(label, Counter())
        total = sum(bands.values())
        rate = outcome.triage_rate(fraudulent=label)
        lines.append(
            f"| {name} ({total:,}) | {bands.get('LOW', 0):,} | {bands.get('MEDIUM', 0):,} | "
            f"{bands.get('HIGH', 0):,} | {bands.get('CRITICAL', 0):,} | "
            f"{'n/a' if rate is None else f'{rate:.1%}'} |"
        )
    lines += [
        "",
        "## Rules that fired, by label",
        "",
        "| rule | on fraud | on legitimate |",
        "|---|---|---|",
    ]
    fraud_rules = outcome.rules_by_label.get(True, Counter())
    legit_rules = outcome.rules_by_label.get(False, Counter())
    for rule in sorted(set(fraud_rules) | set(legit_rules)):
        lines.append(f"| `{rule}` | {fraud_rules.get(rule, 0):,} | {legit_rules.get(rule, 0):,} |")
    if not fraud_rules and not legit_rules:
        lines.append("| _no rule fired on any replayed transaction_ | 0 | 0 |")

    lines += ["", "## Reading this honestly", ""]
    if fraud_rate is None:
        lines.append(
            "- **No fraudulent transaction was replayed**, so this run says nothing "
            "about fraud. Replay a slice that contains some."
        )
    elif legit_rate is None:
        lines.append(
            "- No legitimate transaction was replayed; the comparison is missing its control."
        )
    elif fraud_rate > legit_rate:
        lines.append(
            f"- Known fraud is triaged at **{fraud_rate:.1%}** against **{legit_rate:.1%}** for "
            f"legitimate traffic. The hot path separates them, which is what this check asks."
        )
    else:
        lines.append(
            f"- **Known fraud is triaged at {fraud_rate:.1%}, no more than the {legit_rate:.1%} "
            f"for legitimate traffic.** The rules are not separating the two. Recorded as found "
            f"rather than re-framed (CLAUDE.md §17)."
        )
    lines.append(
        "- A replayed slice is not a fraud rate: the slice is chosen to contain fraud, so the "
        "proportions here are a property of the slice and not of the dataset."
    )
    lines.append(
        "- Every transaction was scored against a cold store that warmed as the replay "
        "proceeded, so early transactions had less history than later ones -- the same "
        "condition a real deployment starts in."
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, required=True, help="tx.raw.v1 JSONL from make seed")
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--base-url", default="http://localhost:8010")
    parser.add_argument("--limit", type=int, default=2000)
    args = parser.parse_args()

    events: list[dict[str, Any]] = []
    with args.events.open() as handle:
        for line in handle:
            if len(events) >= args.limit:
                break
            events.append(json.loads(line))
    if not events:
        raise SystemExit(f"{args.events} contained no events; there is nothing to replay")

    print(f"replaying {len(events):,} transactions through {args.base_url} ...")
    replayed = replay(events, base_url=args.base_url)
    degraded = sum(1 for r in replayed if r.degraded)
    if degraded:
        print(
            f"WARNING: {degraded:,} of {len(replayed):,} decisions were DEGRADED. The band "
            f"distribution below describes a gateway that could not read its features.",
            file=sys.stderr,
        )

    outcome = join_labels(replayed, dataset_version=args.dataset_version)
    if outcome.unlabelled:
        print(
            f"WARNING: {outcome.unlabelled:,} replayed transactions had no label in dataset "
            f"{args.dataset_version!r} and were excluded.",
            file=sys.stderr,
        )
    report = render(outcome, dataset_version=args.dataset_version, replayed=len(replayed))
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(report)
    print(report)
    print(f"written: {REPORT.relative_to(ROOT)}   ({dt.datetime.now(dt.UTC):%Y-%m-%dT%H:%M:%SZ})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
