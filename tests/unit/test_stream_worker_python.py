"""Spark's Python workers are pinned to the driver's interpreter (Step 6 finding), without a JVM.

The real-JVM proof that a worker imports the driver's own `trace_core` is
tests/stream/test_session_worker_python.py.
"""

from __future__ import annotations

import pytest

from trace_core.domain.errors import ToolchainMismatchError
from trace_core.stream.session import pin_worker_python

pytestmark = pytest.mark.unit

DRIVER = "/work/trace-x/.claude/worktrees/w/.venv/bin/python"


def test_an_unset_worker_python_is_pinned_to_the_driver_s_interpreter() -> None:
    env: dict[str, str] = {}
    pin_worker_python(env, executable=DRIVER)
    assert env["PYSPARK_PYTHON"] == DRIVER


def test_another_name_in_the_driver_s_own_environment_is_accepted_as_it_is() -> None:
    env = {"PYSPARK_PYTHON": "/work/trace-x/.claude/worktrees/w/.venv/bin/python3"}
    pin_worker_python(env, executable=DRIVER)
    assert env["PYSPARK_PYTHON"].endswith("python3")


@pytest.mark.parametrize("name", ["PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON"])
def test_an_interpreter_from_another_environment_is_refused(name: str) -> None:
    env = {name: "/work/trace-x/.venv/bin/python3"}
    with pytest.raises(ToolchainMismatchError, match=name):
        pin_worker_python(env, executable=DRIVER)


def test_a_bare_name_is_judged_by_where_path_finds_it() -> None:
    env = {"PYSPARK_PYTHON": "python3", "PATH": "/nonexistent/bin"}
    with pytest.raises(ToolchainMismatchError, match="PYSPARK_PYTHON"):
        pin_worker_python(env, executable=DRIVER)
