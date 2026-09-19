#!/usr/bin/env python3
"""Replay the frozen Track A stream through the gateway and check where fraud lands.

ROADMAP Phase 2, MANUAL VALIDATION: *"Replay a generated stream through the
gateway; confirm known-fraud transactions land in HIGH/CRITICAL bands."*

**All four released streams, interleaved by event time.** An earlier version of
this harness replayed `tx.raw.v1` alone. That is not a smaller version of the
check, it is a different one: `failed_logins_1h` and `hours_since_identity_change`
are computed from `identity.events.v1`, so replaying transactions only holds
those features permanently absent, and under Kleene semantics (ADR-0033) every
rule over them abstains rather than fires. R008, R012 and R013 were structurally
unreachable, and two of the ten fraud scenarios are *defined* by non-transaction
events (docs/FRAUD_SCENARIOS.md). A validation that cannot fail for those
scenarios was not validating them.

Feature set 3.0.0 adds the fourth: `declined_ratio_1h` reads authorization outcomes as their own
dated events (ADR-0049), so a replay without them holds it absent and R002 abstains. A dataset
without the stream -- eval-v1 -- has its outcomes derived by DM-1 with the source manifest's seed
(`--source-manifest`), through the generator's own builder; the frozen dataset is untouched.

Ordering is therefore load-bearing. The streams are merged on
`occurred_at` and replayed in that order, because a credential-stuffing burst
that arrives after the transaction it explains explains nothing.

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
*afterwards*. The frozen parquet carries `envelope` and `payload` and nothing
else -- asserted at load, not assumed -- so there is no label to leak even by
accident.

**The time shift, and why it is not cheating.** `eval-v1` is frozen in the past.
An `occurred_at` older than `MAX_BACKDATE_S` (90 days) is *accepted and flagged*
rather than rejected -- replaying history is a legitimate operation and a
silently dropped old event is indistinguishable from a bug -- but the flag sets
`degraded` on every decision in the run. That is the problem: a uniform
degradation flag on all of them carries no information and, worse, hides a
genuine one among it. So the replay projects every event forward by ONE
deterministic constant, chosen so the last event lands just before now. Every
relative gap and the ordering across all the streams are preserved exactly,
which is all the event-time windows depend on. The frozen artifact is never
modified; the shift exists only in this projection and is recorded in the run's
provenance.

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
import hashlib
import json
import os
import sys
import uuid
from collections import Counter
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Final

from trace_core.contracts.topics import TX_AUTHORIZATION_V1
from trace_core.domain.errors import NonConformantFeatureSetError
from trace_core.domain.time import to_millis
from trace_core.features.spec import FEATURE_SET_VERSION, require_served_conformance

ROOT: Final = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "packages"))
sys.path.insert(0, str(ROOT))

REPORT: Final = ROOT / "benchmarks" / "gateway" / "triage-bands.md"
TRIAGE_BANDS: Final = frozenset({"HIGH", "CRITICAL"})

STREAMS: Final[dict[str, str]] = {
    "tx.raw.v1": "/v1/transactions",
    "identity.events.v1": "/v1/events/identity",
    "device.events.v1": "/v1/events/device",
    TX_AUTHORIZATION_V1: "/v1/events/authorization",
}
"""Every released ingress contract, and where it is posted.

Keyed by topic so adding a released stream here is the only change needed; a
stream that exists but is not replayed is a rule that cannot fire.
"""

REPLAY_ORDER: Final[dict[str, int]] = {
    "device.events.v1": 0,
    "identity.events.v1": 1,
    "tx.raw.v1": 2,
    TX_AUTHORIZATION_V1: 3,
}
"""The order at one instant: an outcome after transactions (ADR-0049 §7). The other streams keep the
order they were always replayed in."""


@dataclass(frozen=True)
class OutcomeSource:
    """Where the replayed authorization outcomes came from, for the report (ADR-0049 §7)."""

    derived: bool
    """True when the dataset has no `tx.authorization.v1` stream and outcomes were derived from its
    transactions' outcome column."""
    count: int
    """Outcomes replayed."""
    seed: int | None = None
    """The source manifest's seed DM-1 was keyed by, when derived."""
    delay_model: str | None = None
    omitted: dict[str, int] = field(default_factory=dict)
    """Loaded transactions whose outcome column derives nothing, by value."""


@dataclass
class Replayed:
    """One transaction's decision, before any label is attached."""

    transaction_id: str
    risk_band: str
    score: float
    decision: str
    fired_rules: tuple[str, ...]
    degraded: bool
    degraded_reasons: tuple[str, ...]
    feature_source: str


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


def _parse(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def _iso(value: dt.datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def load_streams(
    dataset_dir: Path, *, limit: int, seed: int | None = None
) -> tuple[list[tuple[dt.datetime, str, dict[str, Any]]], OutcomeSource]:
    """Load every released stream and merge them on event time.

    `limit` bounds the TRANSACTIONS; identity and device events are taken for
    the whole time span those transactions cover, because they exist to give the
    transactions context and truncating them independently would remove exactly
    the context being tested.

    Authorization outcomes come from the dataset's `tx.authorization.v1` stream when it has one.
    Otherwise each loaded transaction whose outcome column is APPROVED or DECLINED derives one,
    dated by DM-1 with `seed`, the source manifest's, through the generator's own builder: a derived
    outcome is exactly the event the generator would have emitted (ADR-0049 §7). Any other value
    derives nothing and is counted. The frozen dataset is never modified.
    """
    import pyarrow.parquet as pq
    from data.generator import outcomes as dm1

    def records(topic: str) -> Iterator[dict[str, Any]]:
        path = dataset_dir / f"{topic}.parquet"
        handle = pq.ParquetFile(path)
        columns = handle.schema_arrow.names
        if set(columns) != {"envelope", "payload"}:
            raise SystemExit(
                f"{path} carries columns {columns}. The replay input must be the released "
                f"envelope and payload only -- anything else risks handing the runtime a "
                f"field ground truth lives in (CLAUDE.md §11)."
            )
        for batch in handle.iter_batches(batch_size=10_000):
            yield from batch.to_pylist()

    merged: list[tuple[dt.datetime, str, dict[str, Any]]] = []
    horizon: dt.datetime | None = None

    # Transactions first: they fix the horizon. Every other stream is then kept only up to it while
    # it is read. Rows beyond the horizon were always dropped (below); keeping them until then made
    # a short prefix of a dataset with a full identity or outcome stream -- eval-v2 -- hold every
    # row of those files in memory at once.
    for topic in ("tx.raw.v1", "identity.events.v1", "device.events.v1"):
        path = dataset_dir / f"{topic}.parquet"
        if not path.exists():
            raise SystemExit(
                f"{path} is missing. Run `make seed` to generate the frozen dataset; this "
                f"harness replays the released streams and will not silently skip one."
            )
        taken = 0
        for record in records(topic):
            occurred = _parse(record["envelope"]["occurred_at"])
            if topic == "tx.raw.v1":
                if taken >= limit:
                    break
                taken += 1
                horizon = occurred if horizon is None else max(horizon, occurred)
            elif horizon is None or occurred > horizon:
                continue
            merged.append((occurred, topic, record))

    if horizon is None:
        raise SystemExit("no transactions were loaded; there is nothing to replay")

    omitted: Counter[str] = Counter()
    derived = not (dataset_dir / f"{TX_AUTHORIZATION_V1}.parquet").exists()
    if not derived:
        for record in records(TX_AUTHORIZATION_V1):
            occurred = _parse(record["envelope"]["occurred_at"])
            if occurred <= horizon:
                merged.append((occurred, TX_AUTHORIZATION_V1, record))
    elif seed is None:
        raise SystemExit(
            f"{dataset_dir} has no {TX_AUTHORIZATION_V1} stream, and deriving its outcomes needs "
            f"the source manifest's seed (ADR-0049 §7)."
        )
    else:
        for occurred, topic, record in list(merged):
            if topic != "tx.raw.v1":
                continue
            envelope, payload = record["envelope"], record["payload"]
            value = payload.get("authorization_outcome")
            if value not in dm1.OUTCOMES:
                omitted[str(value)] += 1
                continue
            event = dm1.outcome_event(
                seed,
                transaction_id=str(payload["transaction_id"]),
                account_id=str(payload["account_id"]),
                authorization_outcome=str(value),
                transaction_occurred_at=str(envelope["occurred_at"]),
                transaction_occurred_ms=to_millis(occurred),
                producer=str(envelope.get("producer") or ""),
                trace_id=str(envelope.get("trace_id") or ""),
                correlation_id=str(envelope.get("correlation_id") or ""),
            )
            merged.append((_parse(event["envelope"]["occurred_at"]), TX_AUTHORIZATION_V1, event))

    # Context events beyond the last replayed transaction describe a future the
    # gateway never sees, so they are dropped rather than replayed into it.
    merged = [row for row in merged if row[0] <= horizon]
    merged.sort(key=lambda row: (row[0], REPLAY_ORDER[row[1]]))
    source = OutcomeSource(
        derived=derived,
        count=sum(1 for row in merged if row[1] == TX_AUTHORIZATION_V1),
        seed=seed if derived else None,
        delay_model=(
            f"DM-1: {dm1.DM1_FLOOR_MS} ms + |N({dm1.DM1_MEAN_MS:g} ms, {dm1.DM1_SD_MS:g} ms)|, "
            f"keyed by the seed and the transaction id"
            if derived
            else None
        ),
        omitted=dict(sorted(omitted.items())),
    )
    return merged, source


def shift_to_now(
    events: list[tuple[dt.datetime, str, dict[str, Any]]], *, margin_s: int = 120
) -> tuple[list[tuple[dt.datetime, str, dict[str, Any]]], dt.timedelta]:
    """Project every event forward by ONE constant. Ordering and gaps preserved.

    One constant across all four streams, so cross-stream ordering -- the whole
    reason for merging them -- survives the projection. Returned alongside the
    events so the caller can record it: an undeclared shift would make the run
    unreproducible, which is the same defect as an undeclared seed.
    """
    latest = max(row[0] for row in events)
    delta = (dt.datetime.now(dt.UTC) - dt.timedelta(seconds=margin_s)) - latest
    shifted: list[tuple[dt.datetime, str, dict[str, Any]]] = []
    for occurred, topic, record in events:
        record = json.loads(json.dumps(record))  # never mutate the loaded frame
        record["envelope"]["occurred_at"] = _iso(occurred + delta)
        if record["payload"].get("occurred_at"):
            record["payload"]["occurred_at"] = _iso(
                _parse(record["payload"]["occurred_at"]) + delta
            )
        if record["payload"].get("transaction_occurred_at"):
            record["payload"]["transaction_occurred_at"] = _iso(
                _parse(record["payload"]["transaction_occurred_at"]) + delta
            )
        shifted.append((occurred + delta, topic, record))
    return shifted, delta


def _to_request(topic: str, event: dict[str, Any]) -> dict[str, Any]:
    """A released event as the gateway's request contract.

    `ingested_at` is dropped: it is processing time, stamped by the gateway, and
    the request contract forbids it (ADR-0026). A transaction is sent without its
    `authorization_outcome`: outcomes arrive as their own events (ADR-0049 §7).
    """
    payload = dict(event["payload"])
    if topic == TX_AUTHORIZATION_V1:
        return {
            "transaction_id": payload["transaction_id"],
            "account_id": payload["account_id"],
            "authorization_outcome": payload["authorization_outcome"],
            "decided_at": event["envelope"]["occurred_at"],
            "transaction_occurred_at": payload["transaction_occurred_at"],
        }
    payload.pop("memo", None)
    if topic == "tx.raw.v1":
        payload.pop("authorization_outcome", None)
    request = {k: v for k, v in payload.items() if v not in (None, "")}
    request["occurred_at"] = event["envelope"]["occurred_at"]
    return request


def replay(
    events: list[tuple[dt.datetime, str, dict[str, Any]]], *, base_url: str
) -> tuple[list[Replayed], Counter[str]]:
    """Drive the gateway over HTTP, one event at a time, in event-time order.

    Sequential rather than concurrent: the features are event-time windowed, and
    firing a burst concurrently would have the gateway score transactions in an
    order the generated stream never had -- which would change the answer for
    reasons that have nothing to do with the rules.
    """
    import httpx

    token = _service_token()
    replayed: list[Replayed] = []
    posted: Counter[str] = Counter()
    with httpx.Client(base_url=base_url, timeout=30.0) as client:
        for _occurred, topic, event in events:
            payload = _to_request(topic, event)
            response = client.post(
                STREAMS[topic],
                json=payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "X-Idempotency-Key": f"replay-{uuid.uuid4().hex}",
                },
            )
            if response.status_code not in (200, 202):
                raise SystemExit(
                    f"gateway returned {response.status_code} for a {topic} event: "
                    f"{response.text[:300]}"
                )
            posted[topic] += 1
            if topic != "tx.raw.v1":
                continue
            body = response.json()
            replayed.append(
                Replayed(
                    transaction_id=body["transaction_id"],
                    risk_band=body["risk_band"],
                    score=body["score"],
                    decision=body["decision"],
                    fired_rules=tuple(r["rule_id"] for r in body["reasons"]),
                    degraded=body["degraded"],
                    degraded_reasons=tuple(body.get("degraded_reasons") or ()),
                    feature_source=str(body.get("feature_source")),
                )
            )
    return replayed, posted


EPOCH_KEY: Final = "f:epoch"
"""The online feature store's completeness epoch (`RedisOnlineFeatureStore.epoch_key`)."""


def vouch(store: Any, *, epoch_ms: int, open_holes: int) -> None:
    """Make an empty feature store vouch for a whole replay, from `epoch_ms`.

    The gateway dates the epoch when it starts, or at its first write if absent, so a replay of
    history onto a fresh store reads every earlier window as incomplete and every profile rule
    abstains. An empty store that records every replayed observation from the first is complete
    from the dataset's own start, so the epoch is set there, before anything is posted.

    Refused unless the claim is true: the store holds nothing but a start-up epoch, and no hole is
    open (the gateway would move the epoch past it)."""
    if open_holes:
        raise SystemExit(
            f"refused to vouch: {open_holes} open feature-store hole(s), which the gateway "
            f"would move the epoch past"
        )
    others = [key for key in store.scan_iter(count=100) if _text_key(key) != EPOCH_KEY]
    if others:
        raise SystemExit(
            f"refused to vouch: the feature store is not empty ({len(others)} key(s) besides the "
            f"epoch, e.g. {_text_key(others[0])!r}); flush it before replaying"
        )
    store.set(EPOCH_KEY, epoch_ms)


def _text_key(key: Any) -> str:
    return key.decode() if isinstance(key, bytes) else str(key)


RESET_STATE_SQL: Final = (
    "TRUNCATE app.case_transitions, app.investigation_queue, app.cases, app.outbox, "
    "app.authorization_outcomes"
)
"""What a local operator runs, as the database owner, before replaying a different dataset."""


def existing_state_problem(*, outcomes: int, cases: int, reuse: bool) -> str | None:
    """Why a replay may not start on the system of record it found, or None.

    Frozen datasets reuse positional transaction ids, so a second dataset replayed onto a system
    of record holding the first one's outcomes meets a 409 conflict mid-run, and its triage reuses
    the first dataset's cases. Refused unless the operator asserts the rows came from this same
    dataset (`--reuse-existing-state`), where every repeat is an identical, idempotent delivery."""
    if reuse or (outcomes == 0 and cases == 0):
        return None
    return (
        f"the system of record already holds {outcomes:,} authorization outcome(s) and {cases:,} "
        f"case(s) for this prefix's transaction ids. If they came from another dataset, reset it "
        f"locally as the database owner ({RESET_STATE_SQL}); if they came from this one, pass "
        f"--reuse-existing-state."
    )


def withhold_outcomes(
    events: list[tuple[dt.datetime, str, dict[str, Any]]],
) -> list[tuple[dt.datetime, str, dict[str, Any]]]:
    """The same events without `tx.authorization.v1`: for a controlled comparison against a
    gateway that predates ADR-0049 and has no outcome route. Transactions, identity and device
    events are unchanged, in the same order."""
    return [event for event in events if event[1] != TX_AUTHORIZATION_V1]


def write_decisions(path: Path, replayed: list[Replayed]) -> None:
    """One JSON line per replayed decision, in replay order, before any label is joined.

    Kept so a later analysis (the R010 threshold study) can recompute a band from the rules that
    fired without replaying again; it holds no label."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as out:
        for decision in replayed:
            out.write(json.dumps(asdict(decision), sort_keys=True) + "\n")


FEATURE_BEARING_DEGRADATIONS: Final = frozenset({"redis_unavailable"})
"""Degraded reasons that actually mean the features could not be read.

`occurred_at_backdated` is NOT one of them, and conflating the two was a real
defect in this harness: a replay of a frozen dataset is backdated by
construction, so every decision was reported as "a gateway that could not read
its features" while the feature store had answered normally. The operator-facing
consequence of that message is someone investigating a Redis outage that never
happened.
"""


def _degradation_summary(replayed: list[Replayed]) -> tuple[int, Counter[str]]:
    reasons: Counter[str] = Counter()
    for item in replayed:
        for reason in item.degraded_reasons:
            reasons[reason] += 1
    blind = sum(
        1 for r in replayed if FEATURE_BEARING_DEGRADATIONS.intersection(r.degraded_reasons)
    )
    return blind, reasons


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


def render(
    outcome: Outcome,
    *,
    dataset_version: str,
    replayed: int,
    posted: Counter[str],
    shift: dt.timedelta,
    reasons: Counter[str],
    blind: int,
    outcome_source: OutcomeSource,
) -> str:
    fraud_rate = outcome.triage_rate(fraudulent=True)
    legit_rate = outcome.triage_rate(fraudulent=False)
    lines = [
        "# Gateway triage bands on a replayed Track A stream",
        "",
        "> ROADMAP Phase 2 MANUAL VALIDATION. Produced by `eval/replay/gateway_replay.py`,",
        "> which connects as **`trace_eval`** -- the only role permitted to read ground truth",
        "> (ADR-0004). The gateway scored every transaction over HTTP before any label was",
        "> joined, and the replayed events carry the released envelope and payload only.",
        "",
        "**This is a distribution check, not a quality metric.** Phase 2 has no model and its",
        "thresholds are declared rather than fitted, so precision and recall here would be",
        "numbers with no meaning attached (CLAUDE.md §13). The question it answers is whether",
        "the hot path puts known fraud into the bands that open an investigation more often",
        "than it does legitimate traffic. A 'no' would mean the rules are blind.",
        "",
        f"Dataset `{dataset_version}`, {replayed:,} transactions scored.",
        "",
        "## What was replayed",
        "",
        "| stream | events |",
        "|---|---|",
    ]
    for topic in STREAMS:
        lines.append(f"| `{topic}` | {posted.get(topic, 0):,} |")
    lines += [
        "",
        "All four released ingress streams, merged on `occurred_at` and replayed in that",
        "order, an outcome after transactions at one instant. Transactions alone would hold",
        "`failed_logins_1h`, `hours_since_identity_change` and `declined_ratio_1h`",
        "permanently absent, and every rule over them would abstain rather than fire.",
        "",
        "**Event-time projection:** every event shifted forward by one constant of",
        f"`{int(shift.total_seconds()):,}s` ({shift.days} days), applied identically to all",
        "four streams. Relative gaps and cross-stream ordering are preserved exactly; the",
        "frozen dataset is unmodified. Without it every decision in the run carries the",
        "`occurred_at_backdated` flag -- correct, since the events really are old, but a",
        "uniform flag on all of them carries no information and hides a genuine degradation",
        "among it.",
        "",
        "## Authorization outcomes",
        "",
        f"Feature set `{FEATURE_SET_VERSION}` reads verified authorization outcomes, never a",
        "transaction's own request field (ADR-0049 §5).",
        "",
    ]
    if outcome_source.derived:
        lines += [
            f"The dataset has no `{TX_AUTHORIZATION_V1}` stream, so {outcome_source.count:,}",
            "outcomes were derived from its transactions' outcome column by the generator's own",
            f"builder ({outcome_source.delay_model}; seed `{outcome_source.seed}`, the source",
            "manifest's). Loaded transactions whose column derives nothing:",
            "",
            "| value | transactions |",
            "|---|---|",
        ]
        lines += [f"| `{value}` | {n:,} |" for value, n in outcome_source.omitted.items()]
        if not outcome_source.omitted:
            lines.append("| _none_ | 0 |")
    else:
        lines.append(
            f"Replayed from the dataset's `{TX_AUTHORIZATION_V1}` stream: "
            f"{outcome_source.count:,} outcomes."
        )
    lines += [
        "",
        "## Triage bands",
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

    lines += ["", "## Degradation", "", "| reason | decisions |", "|---|---|"]
    for reason, count in sorted(reasons.items(), key=lambda kv: -kv[1]):
        blind_marker = (
            " **(features unavailable)**" if reason in FEATURE_BEARING_DEGRADATIONS else ""
        )
        lines.append(f"| `{reason}`{blind_marker} | {count:,} |")
    if not reasons:
        lines.append("| _none_ | 0 |")
    lines += [
        "",
        f"**{blind:,}** decisions were made without the feature store. Only reasons marked",
        "above mean that; a decision can be flagged degraded for reasons that leave feature",
        "retrieval entirely intact, and reporting those as a blind gateway sends an operator",
        "to look for an outage that did not happen.",
        "",
        "## Reading this honestly",
        "",
    ]
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
        "- A replayed prefix is not a fraud rate: which scenarios fall inside it is a property "
        "of where the prefix ends, not of the dataset."
    )
    lines.append(
        "- Every transaction was scored against a cold store that warmed as the replay "
        "proceeded, so early transactions had less history than later ones -- the same "
        "condition a real deployment starts in."
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset-dir",
        type=Path,
        default=ROOT / "data" / "generated" / "eval-v1",
        help="Directory holding the released stream parquets.",
    )
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--base-url", default="http://localhost:8010")
    parser.add_argument("--limit", type=int, default=2000, help="Transactions to score.")
    parser.add_argument(
        "--source-manifest",
        type=Path,
        default=None,
        help="The dataset's generation run manifest. Its seed dates derived outcomes by DM-1 when "
        "the dataset has no tx.authorization.v1 stream (ADR-0049 §7).",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=REPORT,
        help="Where the markdown report is written (eval-v1's manual replay by default).",
    )
    parser.add_argument(
        "--decisions",
        type=Path,
        default=None,
        help="Also write every decision, unlabelled, as JSON lines.",
    )
    parser.add_argument(
        "--withhold-outcomes",
        action="store_true",
        help="Do not post authorization outcomes: only to compare with a gateway that predates "
        "ADR-0049. Outcomes are still loaded or derived, so a dataset without the stream still "
        "needs --source-manifest, and then withheld: the events and the time shift match the "
        "run that posts them. The report says so.",
    )
    parser.add_argument(
        "--reuse-existing-state",
        action="store_true",
        help="Replay onto a system of record that already holds this dataset's outcomes or cases.",
    )
    parser.add_argument(
        "--vouch-from-manifest",
        type=Path,
        default=None,
        help="The frozen dataset manifest: make the empty feature store vouch for the replay from "
        "its window start, shifted as the events are.",
    )
    args = parser.parse_args()

    # A run record names the feature set its values were computed with. While the online
    # path does not serve that version, no record may be produced (ADR-0046,
    # spec.SERVED_FEATURES_CONFORM).
    try:
        require_served_conformance("A gateway replay report")
    except NonConformantFeatureSetError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    seed: int | None = None
    if args.source_manifest is not None:
        manifest = json.loads(args.source_manifest.read_text())
        if manifest.get("dataset_version") != args.dataset_version:
            print(
                f"refused: {args.source_manifest} describes dataset "
                f"{manifest.get('dataset_version')!r}, not {args.dataset_version!r}; its seed "
                f"would date another dataset's outcomes.",
                file=sys.stderr,
            )
            return 2
        seed = int(manifest["seed"])
    elif not (args.dataset_dir / f"{TX_AUTHORIZATION_V1}.parquet").exists():
        print(
            f"refused: feature set {FEATURE_SET_VERSION} reads authorization outcomes. The dataset "
            f"has no {TX_AUTHORIZATION_V1} stream, and deriving them needs the source manifest's "
            f"seed: pass --source-manifest (ADR-0049 §7).",
            file=sys.stderr,
        )
        return 2

    events, outcome_source = load_streams(args.dataset_dir, limit=args.limit, seed=seed)
    prefix_ids = [
        str(record["payload"]["transaction_id"])
        for _o, topic, record in events
        if topic == "tx.raw.v1"
    ]
    import psycopg

    with psycopg.connect(_eval_dsn()) as conn:
        found_outcomes = conn.execute(
            "SELECT count(*) FROM app.authorization_outcomes WHERE transaction_id = ANY(%s)",
            (prefix_ids,),
        ).fetchone()
        found_cases = conn.execute(
            "SELECT count(*) FROM app.cases WHERE trigger_transaction_id = ANY(%s)", (prefix_ids,)
        ).fetchone()
    problem = existing_state_problem(
        outcomes=int(found_outcomes[0]) if found_outcomes else 0,
        cases=int(found_cases[0]) if found_cases else 0,
        reuse=args.reuse_existing_state,
    )
    if problem is not None:
        print(f"refused: {problem}", file=sys.stderr)
        return 2
    if args.withhold_outcomes:
        events = withhold_outcomes(events)
        print("authorization outcomes WITHHELD: a comparison against a pre-ADR-0049 gateway")
    shifted, delta = shift_to_now(events)
    fingerprint = hashlib.sha256(
        f"{args.dataset_version}:{args.limit}:{int(delta.total_seconds())}:{seed}".encode()
    ).hexdigest()[:16]

    counts = Counter(topic for _o, topic, _r in shifted)
    print(
        f"replaying {len(shifted):,} events through {args.base_url} "
        f"({', '.join(f'{t}={counts[t]:,}' for t in STREAMS)}) ..."
    )
    print(f"event-time shift: +{int(delta.total_seconds()):,}s   projection id: {fingerprint}")

    store_note = "not vouched: a fresh store dates its epoch when the gateway starts"
    if args.vouch_from_manifest is not None:
        import psycopg
        import redis

        frozen = json.loads(args.vouch_from_manifest.read_text(encoding="utf-8"))
        if frozen.get("dataset_version") != args.dataset_version:
            print(
                f"refused: {args.vouch_from_manifest} describes {frozen.get('dataset_version')!r}",
                file=sys.stderr,
            )
            return 2
        window_start = _parse(frozen["config"]["start_at"]) + delta
        epoch_ms = int(window_start.timestamp() * 1000)
        with psycopg.connect(_eval_dsn()) as conn:
            row = conn.execute(
                "SELECT count(*) FROM app.feature_store_holes WHERE cleared_at IS NULL"
            ).fetchone()
        store = redis.Redis(
            host=os.environ.get("REDIS_HOST", "localhost"),
            port=int(os.environ.get("REDIS_PORT", "6389")),
            db=int(os.environ.get("REDIS_DB", "0")),
        )
        vouch(store, epoch_ms=epoch_ms, open_holes=int(row[0]) if row else 0)
        store_note = f"vouched from the shifted window start, {_iso(window_start)}"
    print(f"feature store: {store_note}")
    replayed, posted = replay(shifted, base_url=args.base_url)
    if args.decisions is not None:
        write_decisions(args.decisions, replayed)
    blind, reasons = _degradation_summary(replayed)
    if blind:
        print(
            f"WARNING: {blind:,} of {len(replayed):,} decisions were made WITHOUT the feature "
            f"store. The band distribution below partly describes a gateway that could not "
            f"read its features.",
            file=sys.stderr,
        )

    outcome = join_labels(replayed, dataset_version=args.dataset_version)
    if outcome.unlabelled:
        print(
            f"WARNING: {outcome.unlabelled:,} replayed transactions had no label in dataset "
            f"{args.dataset_version!r} and were excluded.",
            file=sys.stderr,
        )
    report = render(
        outcome,
        dataset_version=args.dataset_version,
        replayed=len(replayed),
        posted=posted,
        shift=delta,
        reasons=reasons,
        blind=blind,
        outcome_source=outcome_source,
    )
    report += f"\n**Feature store:** {store_note}.\n"
    if args.withhold_outcomes:
        report += (
            "\n**Authorization outcomes were withheld** for a comparison against a gateway that "
            "predates ADR-0049; this is not the feature set 3.0.0 replay.\n"
        )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(report)
    print(report)
    print(f"written: {args.report}   ({dt.datetime.now(dt.UTC):%Y-%m-%dT%H:%M:%SZ})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
