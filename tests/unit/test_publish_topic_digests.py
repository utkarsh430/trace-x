"""Per-topic content digests from the generator's single encoding pass.

The dataset digest covers transactions only, and that is frozen into `eval-v1`.
A later dataset that changes only its identity or device streams -- `eval-v2`
adds legitimate identity and device activity -- would therefore carry the same
transaction digest as `eval-v1` and be indistinguishable from it by that digest.
`write_rows(..., topic_digests=...)` records one digest per topic over the same
bytes, in the same pass.

The requirement that shapes these tests is the other half: asking for per-topic
digests must not change a single byte of the default digest or of the output.
The fast tests show that on a small stream, with a control proving the
per-topic digest actually moves when a non-transaction stream changes; the
`slow` test regenerates all of `eval-v1` through `write_rows` and compares with
its frozen digest.
"""

from __future__ import annotations

import datetime as dt
import json
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from data.generator.digest import DatasetDigest, digest_of
from data.generator.emit import JsonlSink, NullSink, ValidationPolicy, write_rows

from trace_core.contracts.envelope import build_event
from trace_core.domain.identifiers import uuid7
from trace_core.domain.time import event_time, processing_time, to_millis

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
FREEZE = ROOT / "eval" / "track_a" / "eval-v1.manifest.json"
T0 = dt.datetime(2026, 9, 2, 8, 0, tzinfo=dt.UTC)


@dataclass
class Row:
    topic: str
    event: dict[str, Any]


def _event(event_type: str, at: dt.datetime, payload: dict[str, Any], rng: random.Random) -> Any:
    return build_event(
        event_type=event_type,
        occurred_at=event_time(at),
        payload=payload,
        producer="trace-test@1.0.0",
        trace_id="0" * 32,
        correlation_id="corr_topic_digests",
        ingested_at=processing_time(at + dt.timedelta(milliseconds=25)),
        event_id=uuid7(millis=to_millis(at), rng=rng),
    )


def _rows() -> list[Row]:
    rng = random.Random(7)
    rows: list[Row] = []
    for i in range(60):
        at = T0 + dt.timedelta(seconds=i)
        account = f"acct_{i % 9:09d}"
        rows.append(
            Row(
                "tx.raw.v1",
                _event(
                    "tx.raw",
                    at,
                    {
                        "transaction_id": f"tx_{i:012d}",
                        "account_id": account,
                        "card_id": f"card_{i % 9:09d}",
                        "device_id": f"dev_{i % 9:09d}",
                        "merchant_id": f"mrch_{i % 4:06d}",
                        "ip_id": f"ip_{i % 9:07d}",
                        "amount_minor": 700 + i,
                        "currency": "GBP",
                        "channel": "CARD_PRESENT",
                        "entry_mode": "CONTACTLESS",
                        "merchant_mcc": "5411",
                        "merchant_country": "GB",
                        "latitude": 51.5,
                        "longitude": -0.12,
                        "authorization_outcome": "APPROVED",
                    },
                    rng,
                ),
            )
        )
        if i % 4 == 0:
            rows.append(
                Row(
                    "identity.events.v1",
                    _event(
                        "identity.event",
                        at,
                        {"account_id": account, "identity_event_type": "LOGIN_SUCCEEDED"},
                        rng,
                    ),
                )
            )
        if i % 7 == 0:
            rows.append(
                Row(
                    "device.events.v1",
                    _event(
                        "device.event",
                        at,
                        {
                            "device_id": f"dev_{i % 9:09d}",
                            "account_id": account,
                            "device_event_type": "FIRST_SEEN",
                            "platform": "ios",
                        },
                        rng,
                    ),
                )
            )
    return rows


def _run(rows: list[Row], directory: Path, *, per_topic: bool) -> tuple[str, Any, bytes]:
    digest = DatasetDigest()
    topic_digests: dict[str, DatasetDigest] | None = {} if per_topic else None
    with JsonlSink(directory=directory) as sink:
        for _ in write_rows(rows, sink, ValidationPolicy.ALL, digest, topic_digests=topic_digests):
            pass
    written = b"".join(path.read_bytes() for path in sorted(directory.glob("*.jsonl")))
    return digest.hexdigest(), topic_digests, written


def test_asking_for_topic_digests_changes_nothing_else(tmp_path: Path) -> None:
    rows = _rows()
    plain_digest, _, plain_bytes = _run(rows, tmp_path / "plain", per_topic=False)
    tee_digest, topic_digests, tee_bytes = _run(rows, tmp_path / "tee", per_topic=True)
    assert tee_digest == plain_digest
    assert tee_bytes == plain_bytes
    assert topic_digests


def test_the_default_digest_is_still_transactions_only(tmp_path: Path) -> None:
    rows = _rows()
    default_digest, _, _ = _run(rows, tmp_path, per_topic=False)
    transactions, _ = digest_of(row.event for row in rows if row.topic == "tx.raw.v1")
    everything, _ = digest_of(row.event for row in rows)
    assert default_digest == transactions
    assert default_digest != everything, "control: the stream holds more than transactions"


def test_each_topic_is_digested_over_its_own_rows_in_emission_order(tmp_path: Path) -> None:
    rows = _rows()
    default_digest, topic_digests, _ = _run(rows, tmp_path, per_topic=True)
    assert set(topic_digests) == {row.topic for row in rows}
    for topic, recorded in topic_digests.items():
        expected, count = digest_of(row.event for row in rows if row.topic == topic)
        assert recorded.hexdigest() == expected, topic
        assert recorded.row_count == count, topic
    assert topic_digests["tx.raw.v1"].hexdigest() == default_digest


def test_a_changed_identity_stream_moves_its_digest_and_not_the_transaction_digest(
    tmp_path: Path,
) -> None:
    """The eval-v2 case: same transactions, different identity activity."""
    rows = _rows()
    base_default, base_topics, _ = _run(rows, tmp_path / "base", per_topic=True)
    changed = [Row(row.topic, json.loads(json.dumps(row.event))) for row in rows]
    identity = next(row for row in changed if row.topic == "identity.events.v1")
    identity.event["payload"]["identity_event_type"] = "PASSWORD_CHANGE"
    changed_default, changed_topics, _ = _run(changed, tmp_path / "changed", per_topic=True)

    assert changed_default == base_default, "the transaction digest cannot see the change"
    assert changed_topics["identity.events.v1"].hexdigest() != (
        base_topics["identity.events.v1"].hexdigest()
    )
    for topic in ("tx.raw.v1", "device.events.v1"):
        assert changed_topics[topic].hexdigest() == base_topics[topic].hexdigest(), topic


@pytest.mark.slow
def test_eval_v1_reproduces_its_frozen_digest_through_write_rows_with_topic_digests() -> None:
    """All one million transactions, through the changed function, both ways at once."""
    from data.generator.config import GeneratorConfig
    from data.generator.engine import generate_dataset

    manifest = json.loads(FREEZE.read_text())
    config = GeneratorConfig.from_mapping(manifest["config"])
    digest = DatasetDigest()
    topic_digests: dict[str, DatasetDigest] = {}
    for _ in write_rows(
        generate_dataset(config),
        NullSink(),
        ValidationPolicy.NONE,
        digest,
        topic_digests=topic_digests,
    ):
        pass
    assert digest.hexdigest() == manifest["dataset_digest"]
    assert digest.row_count == manifest["row_count"]
    assert topic_digests["tx.raw.v1"].hexdigest() == manifest["dataset_digest"]
