"""In CI, a session that selects real services fails when any of its tests skipped."""

from __future__ import annotations

import pytest
from tests.conftest import ci_skip_failure

pytestmark = pytest.mark.unit

SKIPPED = ["tests/integration/test_triage_store.py::test_claims_are_exclusive"]


def test_a_skip_fails_a_ci_session_that_selects_services() -> None:
    for expression in (
        "integration",
        "integration or chaos",
        "chaos",
        "stream and not integration",
    ):
        message = ci_skip_failure(expression, SKIPPED, in_ci=True)
        assert message is not None and SKIPPED[0] in message, expression


def test_a_local_session_a_session_without_service_markers_or_without_skips_may_pass() -> None:
    assert ci_skip_failure("integration", SKIPPED, in_ci=False) is None
    assert (
        ci_skip_failure("not integration and not chaos and not stream", SKIPPED, in_ci=True) is None
    )
    assert ci_skip_failure("", SKIPPED, in_ci=True) is None
    assert ci_skip_failure("integration", [], in_ci=True) is None


def test_a_long_list_of_skips_is_truncated_but_counted() -> None:
    skipped = [f"tests/integration/test_x.py::test_{i}" for i in range(25)]
    message = ci_skip_failure("integration", skipped, in_ci=True)
    assert message is not None
    assert "25 test(s) skipped" in message and "... and 5 more" in message
    assert "test_24" not in message
