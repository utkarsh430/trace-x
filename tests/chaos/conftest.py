"""The collection guard of `tests/chaos/test_spark_resume.py` (`P3.checkpoint-resume`).

docs/PHASE3_PLAN.md §4.4: every marker- or selector-based acceptance command must fail when it
collects zero tests or when every selected test is skipped. Each injection case in that module is a
function named `test_inject_*`. A missing broker or JVM skips each case loudly, which is right for a
developer's laptop and wrong as acceptance evidence. So the session FAILS when:

- it names that module on the command line, or selects `chaos` tests with the module in scope, and
  collects no injection case from it; or
- it selected injection cases and none of them reached its call phase (every one skipped).

These hooks look only at that module's items, and fire only when that module was asked for or its
cases were selected; every other chaos test and every other session is left as it was.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

MODULE = Path(__file__).resolve().parent / "test_spark_resume.py"
PREFIX = "test_inject_"
_selected: set[str] = set()
_executed: set[str] = set()


def _is_injection(item: pytest.Item) -> bool:
    return Path(str(item.path)).resolve() == MODULE and item.name.startswith(PREFIX)


def pytest_collection_finish(session: pytest.Session) -> None:
    _selected.clear()
    _selected.update(item.nodeid for item in session.items if _is_injection(item))


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if report.when == "call" and not report.skipped and report.nodeid in _selected:
        _executed.add(report.nodeid)


def _asked_for_the_module(config: pytest.Config) -> bool:
    base = Path(config.invocation_params.dir)
    selects_chaos = re.search(r"(?<!not )\bchaos\b", str(config.getoption("markexpr") or ""))
    for arg in config.args:
        path = (base / arg.split("::", 1)[0]).resolve()
        if path == MODULE or (selects_chaos and path in MODULE.parents):
            return True
    return False


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    del exitstatus
    if session.config.option.collectonly:
        return
    if not _selected and _asked_for_the_module(session.config):
        problem = (
            "collected no injection case (`test_inject_*`) from tests/chaos/test_spark_resume.py"
        )
    elif _selected and not _executed:
        problem = f"every one of the {len(_selected)} selected injection cases skipped"
    else:
        return
    message = (
        f"\nFAILED (collection guard, P3.checkpoint-resume): {problem}. Skips are not evidence; "
        f"see docs/PHASE3_PLAN.md §4.4.\n"
    )
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_line(message, red=True, bold=True)
    else:
        sys.stderr.write(message)
    session.exitstatus = pytest.ExitCode.TESTS_FAILED
