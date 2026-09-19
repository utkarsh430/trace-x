"""Stage 2 step 11: the memory-bounded LPC-5 paths yield exactly what the materialised ones did.

S0's rows are merged lazily by emission order instead of copied and sorted, so the merge must see
every TX, ID and DEV row once, in order, and must refuse input that is not in emission order rather
than reorder it silently."""

from __future__ import annotations

import datetime as dt

import pytest
from data.generator.config import BaselineIdentityConfig, GeneratorConfig
from data.generator.engine import generate_dataset
from data.generator.lpc5 import declaration as d
from data.generator.lpc5 import run
from data.generator.lpc5.frame import Frame, build_frame
from data.generator.population import build_universe

pytestmark = pytest.mark.unit

START = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)


def _frame() -> Frame:
    config = GeneratorConfig(
        seed=4242,
        row_count=4_000,
        account_count=240,
        merchant_count=60,
        device_count=290,
        ip_count=120,
        fraud_rate=0.02,
        start_at=START,
        end_at=START + dt.timedelta(days=59),
        baseline_identity=BaselineIdentityConfig(),
    )
    return build_frame(generate_dataset(config, build_universe(config)), seed=config.seed)


def test_s0_rows_are_every_transaction_and_side_row_in_emission_order() -> None:
    frame = _frame()
    assert frame.tx and frame.ident and frame.dev
    tx_topic = d.TOPICS[d.Population.TX]
    expected = sorted(
        [
            *((tx.order, tx_topic, tx.transaction_id) for tx in frame.tx),
            *((row.order, d.TOPICS[d.Population.ID], None) for row in frame.ident),
            *((row.order, d.TOPICS[d.Population.DEV], None) for row in frame.dev),
        ],
        key=lambda item: item[0],
    )
    rows = list(run._s0_rows(frame))
    assert [row.topic for row in rows] == [topic for _, topic, _ in expected]
    assert [row.event["payload"]["transaction_id"] for row in rows if row.topic == tx_topic] == [
        transaction_id for _, topic, transaction_id in expected if topic == tx_topic
    ]
    assert all((row.label is not None) == (row.topic == tx_topic) for row in rows)


def test_s0_rows_refuse_rows_out_of_emission_order() -> None:
    frame = _frame()
    frame.tx[0], frame.tx[1] = frame.tx[1], frame.tx[0]
    with pytest.raises(ValueError, match="emission order"):
        list(run._s0_rows(frame))
