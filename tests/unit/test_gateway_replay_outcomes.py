"""F6: a replay preserves DM-1 and order (ADR-0049 §7, §8).

`eval/replay/gateway_replay.py` replays four released streams. A dataset without a
`tx.authorization.v1` stream -- eval-v1 -- has its outcomes derived by DM-1 with the source
manifest's seed, through the generator's own builder. These tests read real generated streams
written to Parquet, as the replay reads the frozen dataset:
- every outcome follows its own transaction, at exactly DM-1 for the seed and the transaction id;
- a rerun is identical, and DM-1's inputs are the seed and the id only;
- a value that observes nothing derives nothing and is counted;
- a dataset that carries the stream is replayed from it, never derived;
- every request the replay sends satisfies the gateway's request contract, and a transaction is
  sent without its outcome.
"""

from __future__ import annotations

import datetime as dt
import json
import shutil
from collections import Counter
from pathlib import Path
from typing import Any

import pytest
from data.generator import outcomes as dm1
from data.generator.config import BaselineIdentityConfig, GeneratorConfig
from data.generator.digest import DatasetDigest
from data.generator.emit import ParquetSink, ValidationPolicy, write_rows
from data.generator.engine import generate_dataset
from eval.replay.gateway_replay import (
    REPLAY_ORDER,
    STREAMS,
    Outcome,
    OutcomeSource,
    _to_request,
    load_streams,
    render,
    shift_to_now,
)

from trace_core.contracts.api.events_ingress import (
    AuthorizationOutcomeRequest,
    DeviceEventRequest,
    IdentityEventRequest,
)
from trace_core.contracts.api.transaction import TransactionRequest
from trace_core.contracts.topics import TX_AUTHORIZATION_V1
from trace_core.domain.time import to_millis
from trace_core.features.spec import FEATURE_SET_VERSION

pytestmark = pytest.mark.unit

SEED = 42
LIMIT = 1_500
TX = "tx.raw.v1"


def _at(text: str) -> dt.datetime:
    return dt.datetime.fromisoformat(text.replace("Z", "+00:00"))


def _config(**overrides: Any) -> GeneratorConfig:
    base: dict[str, Any] = {
        "row_count": 3_000,
        "account_count": 300,
        "merchant_count": 40,
        "device_count": 360,
        "ip_count": 150,
        "fraud_rate": 0.01,
        "seed": SEED,
    }
    base.update(overrides)
    return GeneratorConfig(**base)


def _write(directory: Path, config: GeneratorConfig) -> Path:
    pytest.importorskip(
        "pyarrow", reason="the replay reads Parquet; the `stream` extra provides it"
    )
    sink = ParquetSink(directory=directory)
    for _ in write_rows(generate_dataset(config), sink, ValidationPolicy.NONE, DatasetDigest()):
        pass
    sink.close()
    return directory


@pytest.fixture(scope="module")
def without_stream(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generated as eval-v1 is: no outcome stream, the outcome in the transaction's column."""
    return _write(tmp_path_factory.mktemp("without-outcome-stream"), _config())


@pytest.fixture(scope="module")
def with_stream(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generated under the eval-v2 gate: N12 emits every outcome as its own event."""
    config = _config(baseline_identity=BaselineIdentityConfig(coverage_floor_instances=1))
    return _write(tmp_path_factory.mktemp("with-outcome-stream"), config)


def _decided(events: list[tuple[dt.datetime, str, dict[str, Any]]]) -> dict[str, str]:
    return {
        record["payload"]["transaction_id"]: record["envelope"]["occurred_at"]
        for _, topic, record in events
        if topic == TX_AUTHORIZATION_V1
    }


def _assert_each_outcome_follows_its_transaction(
    events: list[tuple[dt.datetime, str, dict[str, Any]]],
) -> None:
    keys = [(at, REPLAY_ORDER[topic]) for at, topic, _ in events]
    assert keys == sorted(keys), "the merged streams are not in replay order"
    position: dict[str, int] = {}
    for index, (_, topic, record) in enumerate(events):
        transaction_id = record["payload"].get("transaction_id")
        if topic == TX:
            position[transaction_id] = index
        elif topic == TX_AUTHORIZATION_V1:
            assert transaction_id in position, (
                f"outcome for {transaction_id} before its transaction"
            )


def test_a_derived_outcome_follows_its_transaction_at_exactly_dm1(without_stream: Path) -> None:
    events, source = load_streams(without_stream, limit=LIMIT, seed=SEED)
    assert source.derived and source.seed == SEED
    assert source.count == len(_decided(events)) > 0
    _assert_each_outcome_follows_its_transaction(events)
    occurred = {
        record["payload"]["transaction_id"]: to_millis(at)
        for at, topic, record in events
        if topic == TX
    }
    for transaction_id, decided_at in _decided(events).items():
        expected = dm1.decided_ms(SEED, transaction_id, occurred[transaction_id])
        assert to_millis(_at(decided_at)) == expected


def test_a_rerun_is_identical_and_dm1_reads_only_the_seed_and_the_id(without_stream: Path) -> None:
    first, _ = load_streams(without_stream, limit=LIMIT, seed=SEED)
    again, _ = load_streams(without_stream, limit=LIMIT, seed=SEED)
    assert first == again
    decided = _decided(first)
    fewer = _decided(load_streams(without_stream, limit=LIMIT // 3, seed=SEED)[0])
    assert fewer and all(decided[transaction_id] == at for transaction_id, at in fewer.items())
    reseeded = _decided(load_streams(without_stream, limit=LIMIT, seed=SEED + 1)[0])
    assert reseeded.keys() == decided.keys()
    assert any(reseeded[transaction_id] != at for transaction_id, at in decided.items())


def test_a_value_that_observes_nothing_derives_nothing_and_is_counted(
    without_stream: Path, tmp_path: Path
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    copy = shutil.copytree(without_stream, tmp_path / "copy")
    path = copy / f"{TX}.parquet"
    schema = pq.read_schema(path)
    rows = pq.read_table(path).to_pylist()
    rows[0]["payload"]["authorization_outcome"] = "REVERSED"
    rows[1]["payload"]["authorization_outcome"] = "UNKNOWN"
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)

    events, source = load_streams(copy, limit=LIMIT, seed=SEED)
    values = Counter(
        str(record["payload"]["authorization_outcome"])
        for _, topic, record in events
        if topic == TX
    )
    assert source.omitted == {v: n for v, n in sorted(values.items()) if v not in dm1.OUTCOMES}
    assert source.omitted["REVERSED"] >= 1 and source.omitted["UNKNOWN"] >= 1
    derived = _decided(events)
    assert rows[0]["payload"]["transaction_id"] not in derived
    assert rows[1]["payload"]["transaction_id"] not in derived


def test_a_dataset_with_an_outcome_stream_is_replayed_from_it(with_stream: Path) -> None:
    events, source = load_streams(with_stream, limit=LIMIT)
    assert not source.derived and source.seed is None and source.delay_model is None
    assert source.count == len(_decided(events)) > 0
    _assert_each_outcome_follows_its_transaction(events)
    ignored_seed, _ = load_streams(with_stream, limit=LIMIT, seed=SEED + 7)
    assert ignored_seed == events, "a delivered stream must never be re-derived"


def test_every_replayed_request_satisfies_the_gateway_contract(without_stream: Path) -> None:
    events, _ = load_streams(without_stream, limit=300, seed=SEED)
    shifted, _delta = shift_to_now(events)
    models: dict[str, Any] = {
        TX: TransactionRequest,
        "identity.events.v1": IdentityEventRequest,
        "device.events.v1": DeviceEventRequest,
        TX_AUTHORIZATION_V1: AuthorizationOutcomeRequest,
    }
    assert set(models) == set(STREAMS)
    replayed: Counter[str] = Counter()
    for (_, topic, record), (_, _, original) in zip(shifted, events, strict=True):
        request = _to_request(topic, record)
        models[topic].model_validate_json(json.dumps(request))
        replayed[topic] += 1
        if topic == TX:
            assert "authorization_outcome" not in request
        elif topic == TX_AUTHORIZATION_V1:
            gap = _at(request["decided_at"]) - _at(request["transaction_occurred_at"])
            original_gap = _at(original["envelope"]["occurred_at"]) - _at(
                original["payload"]["transaction_occurred_at"]
            )
            assert gap == original_gap >= dt.timedelta(milliseconds=dm1.DM1_FLOOR_MS - 1)
    assert replayed[TX] and replayed[TX_AUTHORIZATION_V1]


def test_the_report_says_where_the_outcomes_came_from() -> None:
    source = OutcomeSource(
        derived=True, count=3, seed=SEED, delay_model="DM-1", omitted={"REVERSED": 2}
    )
    report = render(
        Outcome(),
        dataset_version="eval-v1",
        replayed=0,
        posted=Counter(),
        shift=dt.timedelta(0),
        reasons=Counter(),
        blind=0,
        outcome_source=source,
    )
    assert FEATURE_SET_VERSION in report
    assert f"seed `{SEED}`" in report and "DM-1" in report
    assert "| `REVERSED` | 2 |" in report
    for topic in STREAMS:
        assert f"| `{topic}` |" in report
