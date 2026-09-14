#!/usr/bin/env python3
"""Attribute a feature-set change's rule effects on the replayed eval-v1 prefix. A diagnostic.

Written for Phase 3 Step 1. On a store vouching for the whole replay, feature set 2.0.0 fired
`R010_device_shared_across_accounts` on more legitimate transactions than the Phase 2 code
(feature set 1.0.0) did. The question is whether ADR-0046 §2's self-inclusion -- the scored
transaction's own account now counts among its device's accounts -- is the whole cause.

No gateway and no store. It walks the transaction prefix the manual replay posts, in the order it
posts them, and counts each device's distinct accounts over the preceding 24 hours with and without
the scored transaction's own account. It reimplements that one aggregate rather than calling the
online store, so agreeing with both gateways' rule counts is a cross-check, not an assumption.

It also tallies the identity event types in the prefix (ADR-0046 §4 feeds `LOGIN_SUCCEEDED` to no
stream, where Phase 2 counted it as an identity change), and counts located transactions whose
account had one or two earlier located observations: a home before, none under Q4c's minimum.

Labels are read as `trace_eval` (ADR-0004), after counting, and only for the transactions a
hypothesis singles out. Nothing it prints is a quality number: a replayed prefix is not a fraud
rate (docs/EVALUATION.md).
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Final

import yaml

ROOT: Final = Path(__file__).resolve().parents[2]
RULE_PACK: Final = ROOT / "packages" / "trace_core" / "rules" / "packs" / "core.v1.yaml"
EPOCH: Final = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
WINDOW_MS: Final = 86_400_000
HOME_MIN_OBSERVATIONS: Final = 3
"""Q4c's minimum of located observations for a home (ADR-0046 §3)."""


def _replay_module() -> ModuleType:
    """The manual replay itself, so the prefix and the label query are the ones it uses."""
    spec = importlib.util.spec_from_file_location(
        "gateway_replay", ROOT / "eval" / "replay" / "gateway_replay.py"
    )
    if spec is None or spec.loader is None:
        raise SystemExit("eval/replay/gateway_replay.py could not be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules["gateway_replay"] = module
    spec.loader.exec_module(module)
    return module


def _declared_threshold(rule_id: str, feature_id: str) -> int:
    """The value the released rule pack compares `feature_id` against in `rule_id`."""

    def conditions(node: Any) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = []
        if isinstance(node, dict):
            if node.get("feature") == feature_id:
                found.append(node)
            for value in node.values():
                found.extend(conditions(value))
        elif isinstance(node, list):
            for value in node:
                found.extend(conditions(value))
        return found

    pack = yaml.safe_load(RULE_PACK.read_text())
    rules = [r for r in conditions_root(pack) if r.get("id") == rule_id]
    matches = [c for rule in rules for c in conditions(rule)]
    if len(matches) != 1 or matches[0].get("op") != ">=":
        raise SystemExit(f"{rule_id} no longer compares {feature_id} with a single `>=`")
    return int(matches[0]["value"])


def conditions_root(pack: Any) -> list[dict[str, Any]]:
    """Every rule mapping in the pack, wherever the pack nests its list."""
    if isinstance(pack, dict):
        if "id" in pack:
            return [pack]
        return [rule for value in pack.values() for rule in conditions_root(value)]
    if isinstance(pack, list):
        return [rule for value in pack for rule in conditions_root(value)]
    return []


def _labels(replay: ModuleType, dataset_version: str, ids: list[str]) -> dict[str, bool]:
    import psycopg

    with psycopg.connect(replay._eval_dsn()) as conn:
        rows = conn.execute(
            """
            SELECT l.transaction_id, l.is_fraud
            FROM groundtruth.transaction_labels l
            JOIN groundtruth.datasets d ON d.dataset_id = l.dataset_id
            WHERE d.dataset_version = %s AND l.transaction_id = ANY(%s)
            """,
            (dataset_version, ids),
        ).fetchall()
    return {str(transaction_id): bool(is_fraud) for transaction_id, is_fraud in rows}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-dir", type=Path, default=ROOT / "data" / "generated" / "eval-v1")
    parser.add_argument("--dataset-version", default="eval-v1")
    parser.add_argument("--limit", type=int, default=60_000, help="Transactions, as the replay.")
    args = parser.parse_args(argv)

    replay = _replay_module()
    threshold = _declared_threshold(
        "R010_device_shared_across_accounts", "device_distinct_accounts_24h"
    )
    device_events: dict[str, list[tuple[int, str]]] = collections.defaultdict(list)
    located_before: dict[str, int] = collections.defaultdict(int)
    met_either_way: list[str] = []
    met_only_with_self: list[str] = []
    home_withdrawn: list[str] = []
    identity_types: collections.Counter[str] = collections.Counter()
    transactions = 0
    events, _ = replay.load_streams(args.dataset_dir, limit=args.limit)
    for occurred, topic, record in events:
        payload = record["payload"]
        if topic == "identity.events.v1":
            identity_types[str(payload.get("identity_event_type"))] += 1
            continue
        if topic != "tx.raw.v1":
            continue
        transactions += 1
        # Event time is the envelope's, already parsed by the loader, as the replay posts it.
        at_ms = (occurred - EPOCH) // dt.timedelta(milliseconds=1)
        account, device = payload["account_id"], payload.get("device_id")
        if device:
            others = {a for m, a in device_events[device] if at_ms - WINDOW_MS < m <= at_ms}
            if len(others) >= threshold:
                met_either_way.append(payload["transaction_id"])
            elif len(others | {account}) >= threshold:
                met_only_with_self.append(payload["transaction_id"])
            device_events[device].append((at_ms, account))
        if payload.get("latitude") is not None and payload.get("longitude") is not None:
            if 1 <= located_before[account] < HOME_MIN_OBSERVATIONS:
                home_withdrawn.append(payload["transaction_id"])
            located_before[account] += 1

    ids = list(dict.fromkeys(met_either_way + met_only_with_self + home_withdrawn))
    labels = _labels(replay, args.dataset_version, ids)

    def by_label(transaction_ids: list[str]) -> dict[str, int]:
        return dict(
            collections.Counter(
                "unlabelled" if t not in labels else ("fraud" if labels[t] else "legitimate")
                for t in transaction_ids
            )
        )

    print(f"dataset {args.dataset_version}, {transactions:,} transactions in the replayed prefix")
    print(f"R010: device_distinct_accounts_24h >= {threshold} (read from {RULE_PACK.name})")
    print(f"  met with or without its own account:   {by_label(met_either_way)}")
    print(f"  met only when its own account counts:  {by_label(met_only_with_self)}")
    print(f"identity event types in the prefix: {dict(identity_types)}")
    print(
        f"located transactions whose account had 1-{HOME_MIN_OBSERVATIONS - 1} earlier located "
        f"observations (a home before, none under Q4c): {by_label(home_withdrawn)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
