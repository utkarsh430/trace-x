"""A stream driver leaves its process whatever threads remain (`session.run_driver`).

A Spark JVM killed mid-`foreachBatch` leaves py4j's non-daemon callback threads behind, and a
driver ending in `sys.exit(main())` then never exits: the interpreter waits on them. These run real
child processes holding exactly such a thread.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest

pytestmark = pytest.mark.unit

LINGERING_THREAD = """
import threading
threading.Thread(target=threading.Event().wait, daemon=False).start()
"""


def _child(ending: str) -> subprocess.Popen[str]:
    script = LINGERING_THREAD + textwrap.dedent(ending)
    return subprocess.Popen(  # noqa: S603 - a fixed script run by this interpreter
        [sys.executable, "-c", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )


def test_the_old_ending_hangs_on_a_lingering_thread() -> None:
    """The premise: without run_driver, a raising main leaves a process that never exits."""
    child = _child(
        """
        import sys
        def main():
            raise RuntimeError("the Spark JVM is gone")
        sys.exit(main())
        """
    )
    with pytest.raises(subprocess.TimeoutExpired):
        child.wait(timeout=5)
    child.kill()
    child.wait()


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("raise RuntimeError('the Spark JVM is gone')", 1),
        ("return 3", 3),
        ("return 0", 0),
        ("raise SystemExit(2)", 2),
    ],
    ids=["raised", "failed-check", "clean", "usage-error"],
)
def test_run_driver_exits_with_mains_code_despite_a_lingering_thread(
    body: str, expected: int
) -> None:
    child = _child(
        f"""
        from trace_core.stream.session import run_driver
        def main():
            {body}
        run_driver(main)
        """
    )
    try:
        code = child.wait(timeout=30)
    except subprocess.TimeoutExpired:
        child.kill()
        pytest.fail("run_driver did not leave the process: a lingering thread kept it alive")
    assert code == expected
    if expected == 1:
        assert "RuntimeError: the Spark JVM is gone" in (
            child.stderr.read() if child.stderr else ""
        )
