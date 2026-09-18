"""Silver's bounded MERGEs without a JVM (ADR-0053 Amendment 1).

The bounds are exact only because every record of one digest has one `occurred_at`: held here for
every released topic. Then the predicate text Delta prunes files with, and the guards that refuse a
batch whose premise failed.
"""

from __future__ import annotations

import datetime as dt
import re

import pytest

from trace_core.stream import silver
from trace_core.stream import silver_rules as rules
from trace_core.stream.silver import (
    STATS_SLACK_US,
    MergeBounds,
    OccurredRange,
    SilverPruningError,
    canonical_merge_condition,
    late_events_merge_condition,
    merge_bounds,
    occurred_at_predicate,
)

pytestmark = pytest.mark.unit

EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
T = dt.datetime(2026, 9, 15, 9, 0, 0, 123_456, tzinfo=dt.UTC)


def _us(moment: dt.datetime) -> int:
    return (moment - EPOCH) // dt.timedelta(microseconds=1)


def _event(
    occurred_at: str, *, ingested_at: str = "2026-09-15T09:00:01.000000Z"
) -> dict[str, dict]:
    return {
        "envelope": {
            "event_id": "0192f0a0-0000-7000-8000-000000000001",
            "event_type": "any",
            "schema_version": "1",
            "occurred_at": occurred_at,
            "ingested_at": ingested_at,
            "producer": "trace-generator@1.0.0",
            "trace_id": "1" * 32,
            "correlation_id": "corr",
            "idempotency_key": "key",
        },
        "payload": {"transaction_id": "tx_000000000001"},
    }


# ------------------------------------------------------------------- premise ---


@pytest.mark.parametrize("topic", sorted(rules.SILVER_TOPICS))
def test_every_topic_s_digest_covers_occurred_at(topic: str) -> None:
    """One digest, one `occurred_at`: the premise every bound rests on."""
    first = rules.content_digest(topic, _event("2026-09-15T09:00:00.000000Z"))
    moved = rules.content_digest(topic, _event("2026-09-15T09:00:00.000001Z"))
    retried = rules.content_digest(
        topic, _event("2026-09-15T09:00:00.000000Z", ingested_at="2026-09-16T00:00:00.000000Z")
    )
    assert first != moved, "a microsecond of occurred_at is content"
    assert first == retried, "control: a retry's ingested_at is not"


def test_occurred_at_is_a_statistics_column_of_every_bounded_table() -> None:
    """Delta collects file statistics on the first 32 columns unless a table says otherwise; the
    bounds prune only if `occurred_at` is one of them."""
    for topic in rules.SILVER_TOPICS:
        declaration = silver.silver_declaration(topic)
        names = [field.name for field in declaration.schema.fields]
        assert names.index(rules.DIGESTED_EVENT_TIME) < 32, topic
        assert not any(key.startswith("delta.dataSkipping") for key in declaration.properties)
    late = [field.name for field in silver.late_events_schema().fields]
    assert late.index(rules.DIGESTED_EVENT_TIME) < 32


def test_the_digest_may_not_exclude_occurred_at() -> None:
    assert rules.DIGESTED_EVENT_TIME == "occurred_at"
    excluded = rules.RETRY_VARIABLE_ENVELOPE_FIELDS | rules.SCORED_RETRY_VARIABLE_ENVELOPE_FIELDS
    assert rules.DIGESTED_EVENT_TIME not in excluded


# -------------------------------------------------------------------- bounds ---


def test_bounds_from_a_batch_that_supersedes_nothing() -> None:
    bounds = merge_bounds(
        supersede_low_us=None,
        supersede_high_us=None,
        committed_low_us=10,
        committed_high_us=20,
        supersede_mismatched=0,
    )
    assert bounds == MergeBounds(None, OccurredRange(10, 20))


def test_bounds_from_a_batch_that_supersedes() -> None:
    bounds = merge_bounds(
        supersede_low_us=12,
        supersede_high_us=12,
        committed_low_us=10,
        committed_high_us=20,
        supersede_mismatched=0,
    )
    assert bounds == MergeBounds(OccurredRange(12, 12), OccurredRange(10, 20))


def test_an_empty_batch_has_no_bounds() -> None:
    assert merge_bounds(
        supersede_low_us=None,
        supersede_high_us=None,
        committed_low_us=None,
        committed_high_us=None,
        supersede_mismatched=0,
    ) == MergeBounds(None, None)


def test_a_supersede_whose_occurred_at_differs_from_the_row_it_replaces_is_refused() -> None:
    with pytest.raises(SilverPruningError, match="1 superseding row"):
        merge_bounds(
            supersede_low_us=12,
            supersede_high_us=12,
            committed_low_us=10,
            committed_high_us=20,
            supersede_mismatched=1,
        )


@pytest.mark.parametrize(
    ("supersede", "committed"),
    [
        ((12, None), (10, 20)),
        ((None, None), (10, None)),
        ((9, 12), (10, 20)),
        ((12, 21), (10, 20)),
        ((12, 12), (None, None)),
    ],
    ids=["half-open-supersede", "half-open-committed", "below", "above", "no-committed"],
)
def test_inconsistent_aggregates_are_refused(
    supersede: tuple[int | None, int | None], committed: tuple[int | None, int | None]
) -> None:
    with pytest.raises(SilverPruningError):
        merge_bounds(
            supersede_low_us=supersede[0],
            supersede_high_us=supersede[1],
            committed_low_us=committed[0],
            committed_high_us=committed[1],
            supersede_mismatched=0,
        )


def test_an_empty_range_is_refused() -> None:
    with pytest.raises(SilverPruningError, match="empty"):
        OccurredRange(2, 1)


# ----------------------------------------------------------------- predicate ---

_LITERAL = re.compile(r"TIMESTAMP '([^']+)'")


def _literals(predicate: str) -> list[dt.datetime]:
    return [dt.datetime.fromisoformat(text) for text in _LITERAL.findall(predicate)]


def test_no_range_reads_no_file() -> None:
    assert occurred_at_predicate("t.occurred_at", None) == "FALSE"


def test_the_predicate_is_the_range_widened_by_the_statistics_slack_in_utc() -> None:
    bounds = OccurredRange(_us(T), _us(T + dt.timedelta(minutes=5)))
    predicate = occurred_at_predicate("t.occurred_at", bounds)
    assert predicate.startswith("(t.occurred_at >= TIMESTAMP '")
    assert " AND t.occurred_at <= TIMESTAMP '" in predicate
    low, high = _literals(predicate)
    slack = dt.timedelta(microseconds=STATS_SLACK_US)
    assert low == T - slack and high == T + dt.timedelta(minutes=5) + slack
    assert low.utcoffset() == high.utcoffset() == dt.timedelta(0)
    assert STATS_SLACK_US == 1_000, "Delta's timestamp statistics are millisecond-precise"


def test_the_predicate_keeps_microseconds() -> None:
    single = OccurredRange(_us(T), _us(T))
    low, high = _literals(occurred_at_predicate("occurred_at", single))
    assert (low.microsecond, high.microsecond) == (122_456, 124_456)


def test_the_predicate_clamps_to_the_timestamp_range() -> None:
    earliest = dt.datetime(1, 1, 1, tzinfo=dt.UTC)
    latest = dt.datetime(9999, 12, 31, 23, 59, 59, 999_999, tzinfo=dt.UTC)
    low, high = _literals(occurred_at_predicate("c", OccurredRange(_us(earliest), _us(latest))))
    assert (low, high) == (earliest, latest)
    assert "TIMESTAMP '0001-01-01 00:00:00.000000+00:00'" in occurred_at_predicate(
        "c", OccurredRange(_us(earliest), _us(earliest))
    )


def test_the_canonical_condition_is_the_identity_and_the_supersede_range() -> None:
    assert canonical_merge_condition(None) == "t.silver_identity = s.silver_identity AND FALSE"
    bounds = OccurredRange(_us(T), _us(T))
    condition = canonical_merge_condition(bounds)
    assert condition == (
        "t.silver_identity = s.silver_identity AND "
        + occurred_at_predicate("t.occurred_at", bounds)
    )


def test_the_late_events_condition_keeps_the_partition_literal_first() -> None:
    """ADR-0053 §1: the literal topic predicate keeps topic-disjoint MERGEs from conflicting."""
    bounds = OccurredRange(_us(T), _us(T))
    condition = late_events_merge_condition("tx.raw.v1", bounds)
    assert condition.startswith("t.silver_topic = 'tx.raw.v1' AND (t.occurred_at >= TIMESTAMP")
    assert condition.endswith(
        "AND t.silver_topic = s.silver_topic AND t.silver_identity = s.silver_identity"
    )
