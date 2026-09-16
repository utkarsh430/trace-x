"""What a rebuilt store may claim, from evidence alone (ADR-0057 §4).

The Spark and Redis halves are integration-tested; these are the pure decisions: how a gap in
write time maps onto event time, which findings refuse a claim outright, and which only push it
later.
"""

from __future__ import annotations

import datetime as dt
from types import MappingProxyType

import pytest

from trace_core.domain.enums import TransactionChannel
from trace_core.domain.time import to_millis
from trace_core.features.observation import IdentityNamespace, authorization_observation
from trace_core.features.semantics import Stream
from trace_core.observation.coverage import Coverage, Gap, SessionCoverage, SessionRow
from trace_core.stream.hydration import (
    BACKDATE,
    FUTURE_SKEW,
    ClaimInputs,
    HydrationRefusedError,
    LostRecord,
    after_ms,
    compute_claim,
    cursor_text,
    digest_of,
    event_from_row,
    event_time_image,
    merge_ordered,
    parse_cursor,
    replay_span_bound_ms,
)

pytestmark = pytest.mark.unit

T0 = dt.datetime(2026, 9, 15, 12, 0, tzinfo=dt.UTC)
MARGIN_S = 1.0
BEGIN = to_millis(T0 + dt.timedelta(days=3))
"""Hydration's own withdrawal: far enough ahead that these claims are all claimable."""


def _session_row(started: dt.datetime = T0, closed: dt.datetime | None = None) -> SessionRow:
    return SessionRow(
        session_id="s1",
        started_at=started,
        heartbeat_at=started,
        closed_at=closed,
        last_seq=None if closed is None else 3,
    )


def _coverage(
    *gaps: Gap, anomalies: tuple[str, ...] = (), session_anomalies: tuple[str, ...] = ()
) -> Coverage:
    session = SessionCoverage(
        session_id="s1",
        closed=True,
        certified_through=3,
        gaps=tuple(gaps),
        anomalies=session_anomalies,
    )
    return Coverage(
        sessions=MappingProxyType({"s1": session}),
        unknown=(),
        through=T0 + dt.timedelta(minutes=5),
        anomalies=anomalies,
    )


def _inputs(coverage: Coverage, **overrides: object) -> ClaimInputs:
    base: dict[str, object] = {
        "coverage": coverage,
        "sessions": (_session_row(closed=T0 + dt.timedelta(seconds=5)),),
        "ledger_read_at": T0 + dt.timedelta(minutes=5),
        "clock_margin_s": MARGIN_S,
        "begin_epoch_ms": BEGIN,
    }
    base.update(overrides)
    return ClaimInputs(**base)  # type: ignore[arg-type]


def test_a_gap_maps_onto_event_time_by_the_backdate_and_future_skew_bounds() -> None:
    gap = Gap("s1", T0, T0 + dt.timedelta(seconds=10), (2,))
    lower, upper = event_time_image(gap)
    assert lower == T0 - BACKDATE
    assert upper == T0 + dt.timedelta(seconds=10) + FUTURE_SKEW


def test_an_open_gap_has_no_upper_edge_so_nothing_bounds_it() -> None:
    lower, upper = event_time_image(Gap("s1", T0, None))
    assert (lower, upper) == (T0 - BACKDATE, None)


def test_a_clean_coverage_claims_from_the_ledgers_first_session_plus_the_future_skew() -> None:
    claim = compute_claim(_inputs(_coverage()))
    assert claim.claimable
    assert claim.since_ms == after_ms(T0 + FUTURE_SKEW + dt.timedelta(seconds=MARGIN_S))
    assert claim.components["ledger_origin"] == claim.since_ms


def test_a_bounded_gap_pushes_the_claim_past_its_end_plus_the_future_skew() -> None:
    end = T0 + dt.timedelta(minutes=1)
    claim = compute_claim(_inputs(_coverage(Gap("s1", T0, end, (2,)))))
    assert claim.claimable
    assert claim.since_ms == after_ms(end + FUTURE_SKEW)
    assert claim.since_ms > claim.components["ledger_origin"]


def test_an_open_gap_refuses_the_claim() -> None:
    claim = compute_claim(_inputs(_coverage(Gap("s1", T0, None))))
    assert not claim.claimable
    assert any("open gap" in reason for reason in claim.reasons)


def test_an_anomaly_refuses_the_claim_wherever_it_was_found() -> None:
    for coverage in (
        _coverage(anomalies=("a stranger session",)),
        _coverage(session_anomalies=("a write after the close",)),
    ):
        claim = compute_claim(_inputs(coverage))
        assert not claim.claimable
        assert any("anomaly" in reason for reason in claim.reasons)


def test_an_open_hole_refuses_the_claim() -> None:
    claim = compute_claim(_inputs(_coverage(), open_holes=1))
    assert not claim.claimable
    assert any("hole" in reason for reason in claim.reasons)


def test_a_lost_fence_a_foreign_session_and_a_new_outcome_each_refuse_the_claim() -> None:
    for overrides in (
        {"fence_held": False},
        {"foreign_sessions": ("s9",)},
        {"outcomes_changed": True},
    ):
        claim = compute_claim(_inputs(_coverage(), **overrides))
        assert not claim.claimable, overrides
        assert claim.reasons


def test_a_quarantined_record_pushes_the_claim_past_its_arrival() -> None:
    arrival = T0 + dt.timedelta(minutes=2)
    claim = compute_claim(
        _inputs(
            _coverage(),
            quarantined=(LostRecord("tx.scored.v1", arrival, "invalid_event"),),
        )
    )
    assert claim.claimable
    expected = after_ms(arrival + dt.timedelta(seconds=MARGIN_S) + FUTURE_SKEW)
    assert claim.since_ms == expected == claim.components["quarantined"]


def test_an_identity_conflict_pushes_the_claim_past_its_own_event_time() -> None:
    occurred = to_millis(T0 + FUTURE_SKEW + dt.timedelta(hours=2))
    claim = compute_claim(_inputs(_coverage(), conflicts=(occurred,)))
    assert claim.claimable
    assert claim.since_ms == occurred + 1


def test_evidence_that_reaches_the_runs_own_withdrawal_claims_nothing() -> None:
    late = Gap("s1", T0, T0 + dt.timedelta(days=4), (2,))
    claim = compute_claim(_inputs(_coverage(late)))
    assert not claim.claimable
    assert any("nothing is claimable" in reason for reason in claim.reasons)
    assert claim.components["coverage_gaps"] >= BEGIN


def test_a_claim_without_any_session_falls_back_to_the_ledger_read() -> None:
    claim = compute_claim(_inputs(_coverage(), sessions=()))
    read_at = T0 + dt.timedelta(minutes=5)
    assert claim.since_ms == after_ms(read_at + FUTURE_SKEW + dt.timedelta(seconds=MARGIN_S))


def test_the_batch_span_bound_is_inside_both_raw_horizons() -> None:
    from trace_core.features.semantics import LATE_ARRIVAL_MARGIN_S
    from trace_core.repositories.redis_features import LAYOUT

    bound = replay_span_bound_ms()
    assert 0 < bound <= min(LAYOUT.raw_tx_ms, LAYOUT.raw_ie_ms) - LATE_ARRIVAL_MARGIN_S * 1_000


def test_the_cursor_round_trips_and_an_unreadable_one_is_refused() -> None:
    key = (1_726_400_000_000, IdentityNamespace.TRANSACTION.value, "tx_1")
    assert parse_cursor(cursor_text(key)) == key
    assert parse_cursor("") is None
    with pytest.raises(HydrationRefusedError):
        parse_cursor("{not json")


def test_the_digest_chains_so_a_different_prefix_cannot_collide() -> None:
    one = digest_of(digest_of("", "transaction:a"), "transaction:b")
    assert one != digest_of(digest_of("", "transaction:b"), "transaction:a")
    assert one == digest_of(digest_of("", "transaction:a"), "transaction:b")


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "stream": Stream.TRANSACTION.value,
        "identity_namespace": IdentityNamespace.TRANSACTION.value,
        "event_id": "tx_000000000001",
        "occurred_ms": to_millis(T0),
        "account_id": "acct_000001",
        "currency": "GBP",
        "amount_minor": 4_200,
        "card_id": "card_1",
        "device_id": "dev_1",
        "merchant_id": "mer_1",
        "ip_id": "ip_1",
        "merchant_mcc": "5411",
        "merchant_country": "GB",
        "latitude": 51.5,
        "longitude": -0.12,
        "channel": TransactionChannel.CARD_PRESENT.value,
    }
    row.update(overrides)
    return row


def test_a_silver_row_becomes_the_observation_the_store_recorded() -> None:
    event = event_from_row(_row())
    assert event.identity == "transaction:tx_000000000001"
    assert (event.occurred_ms, event.amount_minor, event.currency) == (to_millis(T0), 4_200, "GBP")
    assert event.channel is TransactionChannel.CARD_PRESENT
    assert (event.latitude, event.longitude) == (51.5, -0.12)
    assert event.authorization_outcome is None, "a transaction never carries its own outcome"


def test_an_identity_row_is_keyed_by_the_id_the_store_recorded_it_under() -> None:
    event = event_from_row(
        _row(
            stream=Stream.IDENTITY_FAILED_LOGIN.value,
            identity_namespace=IdentityNamespace.IDENTITY_EVENT.value,
            event_id="idev_" + "a" * 32,
            currency="",
            amount_minor=0,
            card_id=None,
            merchant_id=None,
            merchant_mcc=None,
            merchant_country=None,
            latitude=None,
            longitude=None,
            channel=None,
        )
    )
    assert event.identity == "identity_event:idev_" + "a" * 32
    assert event.stream is Stream.IDENTITY_FAILED_LOGIN
    assert (event.card_id, event.channel, event.latitude) == (None, None, None)


def test_outcomes_merge_into_the_declared_total_order() -> None:
    transaction = event_from_row(_row())
    identity = event_from_row(
        _row(
            stream=Stream.IDENTITY_CHANGE.value,
            identity_namespace=IdentityNamespace.IDENTITY_EVENT.value,
            event_id="idev_1",
        )
    )
    from trace_core.domain.enums import AuthorizationOutcome
    from trace_core.domain.time import EventTime

    outcome = authorization_observation(
        transaction_id="tx_000000000001",
        account_id="acct_000001",
        authorization_outcome=AuthorizationOutcome.APPROVED,
        decided_at=EventTime(T0),
    )
    merged = list(merge_ordered([identity, transaction], [outcome]))
    assert [event.identity for event in merged] == [
        "identity_event:idev_1",
        "transaction:tx_000000000001",
        "transaction_authorization:tx_000000000001",
    ], "at one millisecond: identity events, then transactions, then outcomes"
