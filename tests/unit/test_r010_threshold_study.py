"""The R010 threshold study's arithmetic, on hand-built inputs (decision U10)."""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest
from eval.replay.attribute_rule_changes import EPOCH
from eval.replay.r010_threshold_study import (
    R010,
    Rule,
    band,
    device_accounts,
    operating_points,
)

pytestmark = pytest.mark.unit

RULES = {
    R010: Rule(0.85, "HIGH"),
    "R_WEAK": Rule(0.2, None),
    "R_CRIT": Rule(0.1, "CRITICAL"),
}


def _band_for(score: float) -> str:
    if score >= 0.95:
        return "CRITICAL"
    if score >= 0.8:
        return "HIGH"
    if score >= 0.4:
        return "MEDIUM"
    return "LOW"


def test_a_band_is_the_noisy_or_score_band_raised_by_the_highest_floor() -> None:
    assert band([], RULES, _band_for) == "LOW"
    assert band(["R_WEAK"], RULES, _band_for) == "LOW"
    assert band([R010], RULES, _band_for) == "HIGH"
    assert band([R010, "R_WEAK"], RULES, _band_for) == "HIGH"  # 1 - 0.15 * 0.8 = 0.88
    assert band(["R_WEAK", "R_CRIT"], RULES, _band_for) == "CRITICAL"


def _tx(minute: int, transaction_id: str, account: str, device: str | None) -> tuple[Any, ...]:
    occurred = EPOCH + dt.timedelta(days=20_000, minutes=minute)
    payload = {"transaction_id": transaction_id, "account_id": account, "device_id": device}
    return (occurred, "tx.raw.v1", {"payload": payload})


def test_device_accounts_count_the_24_hour_window_with_the_own_account_included() -> None:
    events = [
        _tx(0, "t1", "a1", "d1"),
        _tx(10, "t2", "a2", "d1"),
        _tx(20, "t3", "a1", "d1"),
        _tx(24 * 60 + 5, "t4", "a3", "d1"),  # t1 has left the window; t2 and t3 have not
        _tx(30, "t5", "a9", None),
    ]
    assert device_accounts(events) == {"t1": 1, "t2": 2, "t3": 2, "t4": 3}


def test_operating_points_separate_matches_high_risk_and_raises_by_label() -> None:
    counts = {"f1": 6, "l1": 4, "l2": 2, "f2": 3}
    decisions = {"f1": [R010], "l1": ["R_WEAK"], "l2": ["R_CRIT"], "f2": []}
    labels = {"f1": True, "f2": True, "l1": False, "l2": False}
    four, six = operating_points([4, 6], counts, decisions, labels, RULES, _band_for)
    assert (four.meets["fraudulent"], four.meets["legitimate"]) == (1, 1)
    assert (four.high_risk["fraudulent"], four.high_risk["legitimate"]) == (1, 2)
    assert (four.raised_by_r010["fraudulent"], four.raised_by_r010["legitimate"]) == (1, 1)
    assert (six.meets["fraudulent"], six.meets["legitimate"]) == (1, 0)
    assert (six.high_risk["fraudulent"], six.high_risk["legitimate"]) == (1, 1)
    assert six.raised_by_r010["legitimate"] == 0


def test_the_declared_threshold_is_read_from_the_released_pack() -> None:
    import yaml
    from eval.replay.attribute_rule_changes import RULE_PACK
    from eval.replay.r010_threshold_study import r010_threshold

    assert r010_threshold(yaml.safe_load(RULE_PACK.read_text(encoding="utf-8"))) == 5
