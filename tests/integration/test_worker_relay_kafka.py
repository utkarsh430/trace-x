"""trace-worker's relay as a real process: PostgreSQL to a real broker (ADR-0051 §7, option B).

`python -m services.worker` runs as it does in its container, against a throwaway broker and the
local migrated PostgreSQL. The test inserts an authorization outcome, waits until the worker marks
it published, and then stops the worker with SIGTERM, as a container stop does. The relay's own
guarantees are proved in tests/integration/test_outbox_relay_kafka.py; this proves the process
around them: it starts from its environment, relays, and stops cleanly.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest
from tests.integration.test_kafka_platform import (  # noqa: F401
    Broker,
    _consume,
    _watermarks,
    broker,
)
from tests.integration.test_outbox_relay import _event, _insert, _rows, pool  # noqa: F401
from tests.integration.test_outbox_relay_kafka import outbox_topic  # noqa: F401

from trace_core.contracts.topics import TX_AUTHORIZATION_V1

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]


def _worker_env(bootstrap: str) -> dict[str, str]:
    return {
        **os.environ,
        "PYTHONPATH": str(ROOT),
        "TRACE_WORKER_KAFKA_BOOTSTRAP": bootstrap,
        "TRACE_WORKER_CLIENT_ID": "it-trace-worker",
        "TRACE_LOG_FORMAT": "json",
    }


def test_the_worker_relays_a_committed_outcome_and_stops_cleanly_on_sigterm(
    outbox_topic: Broker,  # noqa: F811
    pool: Any,  # noqa: F811
) -> None:
    start = _watermarks(outbox_topic, TX_AUTHORIZATION_V1)
    outbox_id = _insert(pool, _event(7_000, account="acct_700000"))
    process = subprocess.Popen(
        [sys.executable, "-m", "services.worker"],
        cwd=ROOT,
        env=_worker_env(outbox_topic.bootstrap),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        deadline = time.monotonic() + 45.0
        while time.monotonic() < deadline and _rows(pool)[outbox_id][0] is None:
            assert process.poll() is None, "the worker exited before relaying anything"
            time.sleep(0.2)
        assert _rows(pool)[outbox_id][0] is not None, "the worker never marked the row published"
    finally:
        process.send_signal(signal.SIGTERM)
        output, _ = process.communicate(timeout=30)
    assert process.returncode == 0, output[-2000:]
    assert "worker_outbox_relay_started" in output and "worker_outbox_relay_stopping" in output
    delivered = [
        json.loads(record.value)["payload"]["transaction_id"]
        for record in _consume(outbox_topic, TX_AUTHORIZATION_V1, start)
    ]
    assert f"tx_{7_000:012d}" in delivered


def test_the_worker_refuses_to_start_without_a_broker() -> None:
    env = {**_worker_env(""), "TRACE_APP_DB_PASSWORD": os.environ.get("TRACE_APP_DB_PASSWORD", "x")}
    done = subprocess.run(
        [sys.executable, "-m", "services.worker"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert done.returncode == 2, done.stdout[-1000:]
    assert "worker_refused_to_start" in done.stdout
