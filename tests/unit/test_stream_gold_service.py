"""The Gold job's entrypoint, without a JVM: what each outcome exits with (ADR-0055 §7).

The build and the check themselves run on real Spark in tests/stream/test_gold_tables.py; these hold
the thin entrypoint to the exit codes the Bronze and Silver jobs use.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from services.stream import gold as service
from services.stream.bronze import EXIT_FAILED_CHECK, EXIT_OK, EXIT_QUERY_FAILED, EXIT_REFUSED

from trace_core.domain.errors import LakeConfigError
from trace_core.stream import gold, session
from trace_core.stream.gold_plan import GoldRefusedError
from trace_core.stream.lake import LakeConfig

pytestmark = pytest.mark.unit


class _Spark:
    def __init__(self) -> None:
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True


@pytest.fixture
def spark(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> _Spark:
    fake = _Spark()
    monkeypatch.setenv("TRACE_DELTA_ROOT", str(tmp_path / "lake"))
    monkeypatch.setenv("TRACE_GIT_SHA", "a" * 40)
    monkeypatch.setenv("TRACE_DIRTY_WORKTREE", "false")
    monkeypatch.setattr(session, "build_session", lambda *_a, **_k: fake)
    return fake


def test_a_lake_refused_before_a_session_starts_exits_2(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_a: Any, **_k: Any) -> LakeConfig:
        raise LakeConfigError("no lake")

    monkeypatch.setattr(LakeConfig, "from_env", refuse)
    assert service.main(["build"]) == EXIT_REFUSED


def test_a_refused_build_exits_2_and_stops_the_session(
    monkeypatch: pytest.MonkeyPatch, spark: _Spark
) -> None:
    def refuse(*_a: Any, **_k: Any) -> Any:
        raise GoldRefusedError("run Silver first")

    monkeypatch.setattr(gold, "build_gold", refuse)
    assert service.main(["build"]) == EXIT_REFUSED
    assert spark.stopped


def test_a_build_that_fails_on_spark_exits_3(
    monkeypatch: pytest.MonkeyPatch, spark: _Spark
) -> None:
    from pyspark.errors import PySparkException

    def fail(*_a: Any, **_k: Any) -> Any:
        raise PySparkException("an executor was lost")

    monkeypatch.setattr(gold, "build_gold", fail)
    assert service.main(["build"]) == EXIT_QUERY_FAILED
    assert spark.stopped


def test_check_exits_0_only_when_consistent(
    monkeypatch: pytest.MonkeyPatch, spark: _Spark, capsys: pytest.CaptureFixture[str]
) -> None:
    reports = iter(
        [
            gold.GoldCheckReport(4, (), {"TRANSACTION": 2}),
            gold.GoldCheckReport(4, ("gold.tx_profiles is not the table build 4 wrote",), {}),
            gold.GoldCheckReport(None, ("no committed Gold build",), {}),
        ]
    )
    monkeypatch.setattr(gold, "check_gold", lambda *_a, **_k: next(reports))
    assert service.main(["check"]) == EXIT_OK
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["consistent"] is True
    assert service.main(["check"]) == EXIT_FAILED_CHECK
    assert service.main(["check"]) == EXIT_FAILED_CHECK
