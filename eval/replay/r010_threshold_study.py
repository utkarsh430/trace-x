#!/usr/bin/env python3
"""The R010 threshold study (decision U10) on a replayed `eval-v2` prefix. Not a quality metric.

`R010_device_shared_across_accounts` fires when `device_distinct_accounts_24h >= 5` and floors the
band at HIGH. U10 kept 5 on `eval-v1`, whose device proxies would fit the ruleset to a flawed
dataset, and asked for this comparison on `eval-v2`.

**What it compares.** For each candidate threshold, the transactions of the replayed prefix whose
device reaches it, and the HIGH-or-CRITICAL decisions that would result, split into fraudulent and
legitimate after counting. It also counts the decisions R010 alone is responsible for: those whose
band without R010 would be below HIGH.

**How, without replaying per threshold.**
- Each transaction's device count over the preceding 24 hours, its own account included (ADR-0046
  §2), is recomputed from the dataset in the order the replay posts it.
- Each decision's band is recomputed from the rules it fired, with the rule pack's weights and
  floors and the threshold configuration (noisy-OR, floors raise only), with R010 added or removed.
- Two self-checks are reported, not assumed: recomputed bands against the bands the gateway
  returned, and offline R010 at the declared threshold against the R010 the gateway fired. The
  served count is an approximate distinct count (ADR-0034), so the second may disagree at bucket
  edges.

Labels are read as `trace_eval` (ADR-0004), only after every count is taken.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import hashlib
import json
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

import yaml
from eval.replay import gateway_replay
from eval.replay.attribute_rule_changes import (
    EPOCH,
    RULE_PACK,
    WINDOW_MS,
    _labels,
    conditions_root,
)

from trace_core.scoring.banding import DEFAULT_CONFIG, load_thresholds

R010: Final = "R010_device_shared_across_accounts"
BANDS: Final = ("LOW", "MEDIUM", "HIGH", "CRITICAL")
HIGH_RISK: Final = frozenset({"HIGH", "CRITICAL"})
LPC5_STATUS: Final = (
    "`eval-v2` did not pass `LPC-5` (revision 4 strict acceptance: FAIL, with zero Category A "
    "findings; `eval/track_a/audits/stage-2-evidence/lpc5-eval-v2-acceptance-rev4.txt`). Its "
    "scenario mix is a coverage floor, not natural prevalence. Counts here compare operating "
    "points on synthetic data; they are not rates and not a quality metric."
)


@dataclass(frozen=True, slots=True)
class Rule:
    weight: float
    band_floor: str | None


def pack_rules(pack: Any) -> dict[str, Rule]:
    """Every rule's weight and band floor, by id."""
    return {
        str(rule["id"]): Rule(float(rule["weight"]), rule.get("band_floor"))
        for rule in conditions_root(pack)
    }


def r010_threshold(pack: Any) -> int:
    """The value R010 compares `device_distinct_accounts_24h` with, from its single `>=`."""
    (rule,) = [rule for rule in conditions_root(pack) if rule.get("id") == R010]
    condition = rule["when"]
    if condition.get("feature") != "device_distinct_accounts_24h" or condition.get("op") != ">=":
        raise SystemExit(f"{R010} no longer compares device_distinct_accounts_24h with one `>=`")
    return int(condition["value"])


def band(fired: Iterable[str], rules: Mapping[str, Rule], band_for: Callable[[float], str]) -> str:
    """The band of a decision that fired `fired`: the noisy-OR score's band, raised by any floor."""
    survival = 1.0
    floor = 0
    for rule_id in fired:
        rule = rules[rule_id]
        survival *= 1.0 - rule.weight
        if rule.band_floor is not None:
            floor = max(floor, BANDS.index(rule.band_floor))
    score = max(0.0, min(1.0, 1.0 - survival))
    return BANDS[max(BANDS.index(band_for(score)), floor)]


def device_accounts(events: Sequence[tuple[dt.datetime, str, dict[str, Any]]]) -> dict[str, int]:
    """Per transaction with a device, the device's distinct accounts over `(t - 24 h, t]`, its own
    account included, in the order the replay posts the transactions."""
    seen: dict[str, collections.deque[tuple[int, str]]] = collections.defaultdict(collections.deque)
    counts: dict[str, int] = {}
    for occurred, topic, record in events:
        if topic != "tx.raw.v1":
            continue
        payload = record["payload"]
        device = payload.get("device_id")
        if not device:
            continue
        at_ms = (occurred - EPOCH) // dt.timedelta(milliseconds=1)
        window = seen[device]
        while window and window[0][0] <= at_ms - WINDOW_MS:
            window.popleft()
        account = str(payload["account_id"])
        counts[str(payload["transaction_id"])] = len(
            {a for m, a in window if m <= at_ms} | {account}
        )
        window.append((at_ms, account))
    return counts


@dataclass
class OperatingPoint:
    threshold: int
    meets: collections.Counter[str] = field(default_factory=collections.Counter)
    high_risk: collections.Counter[str] = field(default_factory=collections.Counter)
    raised_by_r010: collections.Counter[str] = field(default_factory=collections.Counter)


def operating_points(
    thresholds: Sequence[int],
    counts: Mapping[str, int],
    decisions: Mapping[str, Sequence[str]],
    labels: Mapping[str, bool],
    rules: Mapping[str, Rule],
    band_for: Callable[[float], str],
) -> list[OperatingPoint]:
    """Each threshold's device matches, high-risk decisions and R010-only raises, by label."""
    points = [OperatingPoint(threshold) for threshold in thresholds]
    for transaction_id, fired in decisions.items():
        label = (
            "unlabelled"
            if transaction_id not in labels
            else ("fraudulent" if labels[transaction_id] else "legitimate")
        )
        others = [rule_id for rule_id in fired if rule_id != R010]
        without = band(others, rules, band_for)
        count = counts.get(transaction_id)
        for point in points:
            meets = count is not None and count >= point.threshold
            with_rule = band([*others, R010], rules, band_for) if meets else without
            point.meets[label] += meets
            point.high_risk[label] += with_rule in HIGH_RISK
            point.raised_by_r010[label] += meets and without not in HIGH_RISK
    return points


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def render(
    points: Sequence[OperatingPoint],
    *,
    dataset_version: str,
    transactions: int,
    band_mismatches: int,
    agreement: Mapping[tuple[bool, bool], int],
    declared: int,
    decisions_path: Path,
) -> str:
    lines = [
        f"# R010 threshold study on `{dataset_version}` (decision U10)",
        "",
        f"> {LPC5_STATUS}",
        "",
        "Produced by `eval/replay/r010_threshold_study.py` from a replayed prefix's decisions.",
        "",
        "| input | value |",
        "|---|---|",
        f"| transactions decided | {transactions:,} |",
        f"| decisions file | `{decisions_path.name}` `{_sha256(decisions_path)}` |",
        f"| rule pack | `{RULE_PACK.name}` `{_sha256(RULE_PACK)}` |",
        f"| thresholds | `{DEFAULT_CONFIG.name}` `{_sha256(DEFAULT_CONFIG)}` |",
        f"| declared R010 threshold | {declared} |",
        "",
        "## Self-checks",
        "",
        f"- Recomputed bands differing from the gateway's: **{band_mismatches:,}**.",
        f"- Offline R010 at {declared} against the R010 the gateway fired "
        "(offline, served: count):",
    ]
    for (offline, served), count in sorted(agreement.items()):
        lines.append(f"  - offline {offline}, served {served}: {count:,}")
    lines += [
        "",
        "## Operating points",
        "",
        "| threshold | device matches (fraud / legit) | HIGH or CRITICAL (fraud / legit) | "
        "raised by R010 alone (fraud / legit) |",
        "|---|---|---|---|",
    ]
    for point in points:
        marker = " (declared)" if point.threshold == declared else ""
        lines.append(
            f"| {point.threshold}{marker} | {point.meets['fraudulent']:,} / "
            f"{point.meets['legitimate']:,} | {point.high_risk['fraudulent']:,} / "
            f"{point.high_risk['legitimate']:,} | {point.raised_by_r010['fraudulent']:,} / "
            f"{point.raised_by_r010['legitimate']:,} |"
        )
    lines += [
        "",
        "## Decision rule",
        "",
        "R010's threshold changes only on clear evidence of a better operating point: more fraud "
        "and fewer legitimate high-risk decisions together, or a large legitimate reduction for a "
        "negligible fraud loss, with both self-checks clean. Otherwise it stays at 5 (U10). Any "
        "change is a separate, versioned ruleset decision.",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-dir", type=Path, required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--limit", type=int, required=True, help="Transactions, as the replay.")
    parser.add_argument("--decisions", type=Path, required=True, help="The replay's --decisions.")
    parser.add_argument("--thresholds", type=int, nargs="+", default=list(range(2, 11)))
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)

    events, _ = gateway_replay.load_streams(args.dataset_dir, limit=args.limit)
    counts = device_accounts(events)
    served: dict[str, list[str]] = {}
    recorded_band: dict[str, str] = {}
    with args.decisions.open(encoding="utf-8") as handle:
        for line in handle:
            decision = json.loads(line)
            served[decision["transaction_id"]] = list(decision["fired_rules"])
            recorded_band[decision["transaction_id"]] = decision["risk_band"]
    pack = yaml.safe_load(RULE_PACK.read_text(encoding="utf-8"))
    rules = pack_rules(pack)
    declared = r010_threshold(pack)
    config = load_thresholds()

    def band_for(score: float) -> str:
        return str(config.band_for(score).value)

    band_mismatches = sum(
        band(fired, rules, band_for) != recorded_band[tid] for tid, fired in served.items()
    )
    agreement: collections.Counter[tuple[bool, bool]] = collections.Counter(
        (counts.get(tid, 0) >= declared, R010 in fired) for tid, fired in served.items()
    )
    labels = _labels(gateway_replay, args.dataset_version, list(served))
    points = operating_points(args.thresholds, counts, served, labels, rules, band_for)
    report = render(
        points,
        dataset_version=args.dataset_version,
        transactions=len(served),
        band_mismatches=band_mismatches,
        agreement=agreement,
        declared=declared,
        decisions_path=args.decisions,
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(report, encoding="utf-8")
    sys.stdout.write(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
