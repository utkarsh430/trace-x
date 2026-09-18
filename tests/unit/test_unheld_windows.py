"""A window the store no longer holds is unvouched for its own features only (ADR-0046 §5).

Each online structure keeps its widest window plus the late-arrival margin behind the newest
observation. A read further behind -- a late arrival -- reaches behind some structures and not
others: the card set keeps five minutes plus an hour, the account's raw transactions twenty-five
hours. The store leaves an unheld window out and stops vouching for it. Feature set 5.0.0 folded
that per-window fact into one context-wide `complete_since`, so on a read more than an hour late
every feature with a lookback of five minutes or more read INCOMPLETE -- a held, genuinely empty
`failed_logins_1h` included, which was then served as INSUFFICIENT_HISTORY rather than 0.

The store's own `_assemble` is exercised against a canned reply of its read script, so what is
tested is the Python that turns the script's reply into a context; the script itself is covered
against a real Redis by `tests/integration/test_redis_feature_store.py`.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, Final

import pytest
from services.gateway.pipeline import history_incomplete
from tests.conformance.feature_semantics_suite import ACCOUNT, CARD, T0, transaction

from trace_core.domain.time import EventTime, from_millis, to_millis
from trace_core.features import FeatureContext, FeatureState
from trace_core.features.context import Completeness, WindowState
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.semantics import Entity, Stream
from trace_core.features.state_plan import PLAN
from trace_core.observation.scored_event import lookback_completeness, served_features
from trace_core.repositories.redis_features import LAYOUT, READ_SCRIPT, RedisOnlineFeatureStore

MINUTE_MS: Final = 60_000
AS_OF_MS: Final = to_millis(T0)
EPOCH_MS: Final = AS_OF_MS - 400 * 24 * 60 * MINUTE_MS
"""Watched long before every lookback, so only held-ness can withdraw a window's vouching."""

HELD_ACCOUNT_FEATURES: Final = (
    "account_tx_count_1m",
    "account_tx_count_5m",
    "account_tx_count_1h",
    "account_tx_count_24h",
    "account_amount_sum_1h",
    "failed_logins_1h",
)


class _CannedRedis:
    """Registers the store's scripts; the read script answers with one canned reply."""

    def __init__(self, reply: Sequence[Any]) -> None:
        self._reply = reply

    def register_script(self, script: str) -> Callable[..., Any]:
        def run(*_: Any, **__: Any) -> Any:
            if script != READ_SCRIPT:
                raise AssertionError("only the read script is expected in this test")
            return self._reply

        return run


def _reply(*, behind_ms: int, card_members: int = 3, legacy: int = 0) -> list[Any]:
    """The read script's reply for a snapshot of `ACCOUNT` and `CARD` at `AS_OF_MS`, with the
    store's newest write `behind_ms` later. The account and its identity streams hold nothing in
    their windows and have folded or dropped nothing; the card set held `card_members`."""
    identity: list[tuple[list[str], list[str]]] = [
        ([str(0) for _ in LAYOUT.account_windows[stream]], []) for stream in LAYOUT.identity_streams
    ]
    return [
        str(EPOCH_MS),  # epoch
        [],  # newest transactions
        legacy,  # identity events in the account set streams shared before ADR-0046 §8
        [],  # profile scalars
        [],  # profile counters
        [],  # prefix amounts
        [],  # prefix located
        [str(card_members) for _ in LAYOUT.card_windows],
        [],  # device
        [],  # ip
        [],  # merchant
        [],  # cv
        "0",  # position
        str(AS_OF_MS + behind_ms),  # high watermark
        [["0", "0"] for _ in LAYOUT.outcome_windows],
        ["0" for _ in LAYOUT.account_windows[Stream.TRANSACTION]],
        ["0", "", ""],  # raw depth
        [],  # previous transaction
        identity,
    ]


def _read(*, behind_ms: int, legacy: int = 0) -> FeatureContext:
    reply = _reply(behind_ms=behind_ms, legacy=legacy)
    store = RedisOnlineFeatureStore(_CannedRedis(reply))  # type: ignore[arg-type]
    return store.snapshot(
        as_of=EventTime(from_millis(AS_OF_MS)),
        account_id=ACCOUNT,
        currency="GBP",
        card_id=CARD,
    )


SUBJECT: Final = transaction(device_id=None, merchant_id=None, ip_id=None)

LATE_MS: Final = 90 * MINUTE_MS
"""Past the card set's retention (5 min + 60 min margin) and the outcome set's (1 h + 60 min), well
inside the account's raw 25 hours: the adversarial partition's late band, 3,915-7,187 s."""


def test_a_late_read_keeps_the_account_windows_it_still_holds() -> None:
    """Fails on 5.0.0: the unheld card window moved `complete_since` to `as_of - 5m + 1`, and every
    account window of five minutes or more read absent and INCOMPLETE."""
    context = _read(behind_ms=LATE_MS)
    assert context.complete_since == EventTime(from_millis(EPOCH_MS)), "the epoch was moved"
    values = ONLINE_FEATURES.evaluate_all(SUBJECT, context)
    for feature_id in HELD_ACCOUNT_FEATURES:
        value = values[feature_id]
        assert value.state is FeatureState.AVAILABLE and value.or_none() == 0.0, (
            f"{feature_id}: served {value.state} {value.or_none()} for a held, empty window"
        )
        assert lookback_completeness(feature_id, context) == Completeness.COMPLETE.value, feature_id


def test_a_held_empty_failed_login_window_is_a_real_zero() -> None:
    context = _read(behind_ms=LATE_MS)
    logins = ONLINE_FEATURES.get("failed_logins_1h").evaluate(SUBJECT, context)
    assert logins.state is FeatureState.AVAILABLE and logins.or_none() == 0.0
    entries = {e["feature_id"]: e for e in served_features({"failed_logins_1h": logins}, context)}
    assert entries["failed_logins_1h"]["value"] == 0.0
    assert entries["failed_logins_1h"]["lookback_completeness"] == "COMPLETE"


def test_an_unheld_window_is_absent_and_incomplete_for_its_own_features_only() -> None:
    context = _read(behind_ms=LATE_MS)
    assert context.unheld_windows == {
        (Entity.CARD, Stream.TRANSACTION, "5m"),
        (Entity.ACCOUNT, Stream.AUTHORIZATION_OUTCOME, "1h"),
    }
    values = ONLINE_FEATURES.evaluate_all(SUBJECT, context)
    for feature_id in ("card_tx_count_5m", "declined_ratio_1h"):
        assert values[feature_id].state is FeatureState.INSUFFICIENT_HISTORY, feature_id
        assert lookback_completeness(feature_id, context) == "INCOMPLETE", feature_id
    # The card's members from then are trimmed; the count left behind would be a believable lower
    # bound, so it is not served, whatever the reply held.
    assert context.window(Entity.CARD, CARD, Stream.TRANSACTION, "5m") is None
    # The absence is the deployment's, not the entity's, so the decision says so.
    assert history_incomplete(values, context)
    # A lookback of the same length over a held window is untouched.
    assert lookback_completeness("account_tx_count_5m", context) == "COMPLETE"
    assert lookback_completeness("account_tx_count_1h", context) == "COMPLETE"


def test_a_read_with_nothing_unheld_is_unchanged() -> None:
    """The representative partition's ordinary read: nothing unheld, every held window served and
    vouched for exactly as 5.0.0 served it. Uses only what 5.0.0 had, so it runs, and passes,
    against the code before the fix too."""
    context = _read(behind_ms=MINUTE_MS)
    assert context.complete_since == EventTime(from_millis(EPOCH_MS))
    values = ONLINE_FEATURES.evaluate_all(SUBJECT, context)
    assert values["card_tx_count_5m"].or_none() == 3.0
    for feature_id in (*HELD_ACCOUNT_FEATURES, "card_tx_count_5m"):
        assert lookback_completeness(feature_id, context) == "COMPLETE", feature_id
    assert not history_incomplete(values, context)


def test_a_timely_read_holds_every_window() -> None:
    context = _read(behind_ms=MINUTE_MS)
    assert not context.unheld_windows and not context.unheld_previous


def test_a_legacy_identity_set_withdraws_the_identity_features_alone() -> None:
    """While an account's pre-§8 identity set exists, neither identity stream is held. Fails on
    5.0.0, which withdrew every account feature with a lookback of an hour or more with them."""
    context = _read(behind_ms=MINUTE_MS, legacy=1)
    assert (Entity.ACCOUNT, Stream.IDENTITY_FAILED_LOGIN, "1h") in context.unheld_windows
    assert context.unheld_previous == {(Entity.ACCOUNT, Stream.IDENTITY_CHANGE)}
    values = ONLINE_FEATURES.evaluate_all(SUBJECT, context)
    for feature_id in ("failed_logins_1h", "hours_since_identity_change"):
        assert values[feature_id].state is FeatureState.INSUFFICIENT_HISTORY, feature_id
        assert lookback_completeness(feature_id, context) == "INCOMPLETE", feature_id
    for feature_id in ("account_tx_count_1h", "account_tx_count_24h", "card_tx_count_5m"):
        assert values[feature_id].state is FeatureState.AVAILABLE, feature_id
        assert lookback_completeness(feature_id, context) == "COMPLETE", feature_id


@pytest.mark.parametrize("complete_since_ms", [EPOCH_MS, None])
def test_the_context_decides_completeness_per_window(complete_since_ms: int | None) -> None:
    """The pure rule, with no store: an unheld window never vouches, a held one keeps the store's
    completeness, and a store with no epoch stays UNKNOWN either way."""
    since = None if complete_since_ms is None else EventTime(from_millis(complete_since_ms))
    card = (Entity.CARD, Stream.TRANSACTION, "5m")
    context = FeatureContext(
        as_of=EventTime(from_millis(AS_OF_MS)),
        complete_since=since,
        windows={(Entity.CARD, CARD, Stream.TRANSACTION, "5m"): WindowState(count=4)},
        unheld_windows=frozenset({card}),
        unheld_previous=frozenset({(Entity.ACCOUNT, Stream.IDENTITY_CHANGE)}),
    )
    held = (Entity.ACCOUNT, Stream.TRANSACTION, "5m")
    if since is None:
        assert context.completeness(300, window=card) is Completeness.UNKNOWN
        assert context.completeness(300, window=held) is Completeness.UNKNOWN
        return
    assert context.window(Entity.CARD, CARD, Stream.TRANSACTION, "5m") is None
    assert context.completeness(300, window=card) is Completeness.INCOMPLETE
    assert context.completeness(300, window=held) is Completeness.COMPLETE
    assert context.completeness(300) is Completeness.COMPLETE
    assert context.window(Entity.ACCOUNT, ACCOUNT, Stream.TRANSACTION, "5m") == WindowState()
    assert context.previous_observation(Entity.ACCOUNT, ACCOUNT, Stream.IDENTITY_CHANGE) is None
    assert PLAN.completeness("hours_since_identity_change", context) is Completeness.INCOMPLETE
    assert PLAN.completeness("seconds_since_last_transaction", context) is Completeness.COMPLETE
    # Profiles read no window: the store's age alone decides them.
    assert PLAN.completeness("account_tenure_days", context) is Completeness.COMPLETE


def test_every_windowed_and_pairwise_feature_names_what_it_reads() -> None:
    """A feature missing from the plan's reads would silently keep the store's completeness."""
    for spec in ONLINE_FEATURES:
        if PLAN.lookback_s.get(spec.feature_id) and spec.feature_id not in (
            PLAN.window_reads.keys() | PLAN.previous_reads.keys()
        ):
            assert type(spec.semantics).__name__ == "ProfileAttribute", spec.feature_id
