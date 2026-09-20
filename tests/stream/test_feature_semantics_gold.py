"""Gold against the shared feature-semantics suite, over a complete history, on a real JVM.

The third implementation of `EventTimeCompleteConformanceSuite`, subclassed unmodified
(ADR-0046, ADR-0055): the reference runs it in
`tests/conformance/test_feature_semantics_reference.py`, the Redis store runs the as-served half.
The literal fixtures are the oracle; agreement with the reference is necessary, never sufficient.

**How a fixture reaches Spark.** `context_for(log, subject_index, subject, complete_since)`
returns the context a Gold build computed for the subject: the log's first deliveries are written
as Silver canonical rows (`gold_silver_rows`), `build_gold` runs on real Spark and Delta, and the
subject's
point-in-time rows are read back from the Gold tables and turned into a `FeatureContext` with the
fixture's completeness claim. No Python reference code runs to produce a context.

**One build for every fixture.** A Gold build is minutes of Spark work, and the suite asks for many
contexts. So the logs are collected first, by running each fixture of the unmodified suite against a
recorder that only notes what it is asked (its answers are empty contexts and are never judged); all
logs are then written, id-prefixed so they cannot meet, into one lake and built once. A log the
recorder did not see is built on its own when asked, never answered any other way.
`test_batched_contexts_equal_single_case_builds` rebuilds several logs alone, unprefixed, and
requires identical rows, so the prefixing cannot be what makes a fixture pass.

**Collection guard.** `test_every_event_time_complete_fixture_ran_on_gold` fails unless every
collected fixture of the class read at least one Gold context, and the stream marker's session guard
fails a run in which no stream test executed (tests/conftest.py).
"""

from __future__ import annotations

import datetime as dt
import os
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import pytest
from tests.conformance.feature_semantics_suite import EventTimeCompleteConformanceSuite
from tests.stream.gold_silver_rows import comparable, rows_for_log, unprefixed, write_silver

from trace_core.contracts.canonical import CanonicalTransaction
from trace_core.domain.time import EventTime
from trace_core.features import FeatureContext
from trace_core.features.observation import Event
from trace_core.stream.gold import build_gold, read_context_rows
from trace_core.stream.gold_plan import ContextRows
from trace_core.stream.lake import LakeConfig

pytestmark = [pytest.mark.stream, pytest.mark.parity]

SHA: Final = "5" * 40
CaseKey = tuple[tuple[Event, ...], int]


@dataclass(frozen=True, slots=True)
class Case:
    log: tuple[Event, ...]
    subject_index: int

    @property
    def key(self) -> CaseKey:
        return (self.log, self.subject_index)

    @property
    def subject(self) -> Event:
        return self.log[self.subject_index]


class _Recorder(EventTimeCompleteConformanceSuite):
    """Notes every `(log, subject_index)` a fixture asks about; judges nothing."""

    def __init__(self) -> None:
        self.cases: dict[CaseKey, Case] = {}

    def _note(self, log: Sequence[Event], subject_index: int) -> None:
        case = Case(tuple(log), subject_index)
        self.cases.setdefault(case.key, case)

    def check_log(
        self,
        log: Sequence[Event],
        subject_index: int,
        subject: CanonicalTransaction,
        expectations: Any,
        *,
        complete_since: EventTime | None = None,
    ) -> None:
        self._note(log, subject_index)

    def context_for(
        self,
        log: Sequence[Event],
        subject_index: int,
        subject: CanonicalTransaction,
        *,
        complete_since: EventTime | None,
    ) -> FeatureContext:
        self._note(log, subject_index)
        return FeatureContext(as_of=EventTime(subject.occurred_at))


def fixture_names() -> list[str]:
    return sorted(n for n in dir(EventTimeCompleteConformanceSuite) if n.startswith("test_"))


def record_cases(names: Sequence[str]) -> list[Case]:
    recorder = _Recorder()
    for name in names:
        try:
            getattr(recorder, name)()
        except AssertionError:
            # A fixture that asserts on the recorder's empty context directly has already asked.
            continue
    return list(recorder.cases.values())


class GoldContexts:
    """Gold-built context rows for fixture logs, and which fixtures read them."""

    def __init__(self, spark: Any, root: Path) -> None:
        self.spark = spark
        self.root = root
        self.rows: dict[CaseKey, ContextRows] = {}
        self.served: set[str] = set()
        self.builds = 0
        self.single_case_builds = 0

    def build(self, cases: Sequence[Case], *, prefixed: bool) -> dict[CaseKey, ContextRows]:
        lake = LakeConfig.at(self.root / f"lake-{self.builds}")
        self.builds += 1
        rows: dict[str, list[dict[str, Any]]] = {}
        prefixes: dict[CaseKey, str] = {}
        for index, case in enumerate(cases):
            prefix = f"c{index:04d}|" if prefixed else ""
            prefixes[case.key] = prefix
            for topic, topic_rows in rows_for_log(case.log, prefix=prefix).items():
                rows.setdefault(topic, []).extend(topic_rows)
        write_silver(self.spark, lake, rows)
        result = build_gold(
            self.spark, lake, git_sha=SHA, dirty_worktree=False, now=dt.datetime.now(dt.UTC)
        )
        built = read_context_rows(self.spark, lake, result.record)
        found: dict[CaseKey, ContextRows] = {}
        for case in cases:
            prefix = prefixes[case.key]
            transaction = prefix + first_delivery(case).event_id
            assert transaction in built, f"Gold built no context for {transaction}"
            found[case.key] = unprefixed(built[transaction], prefix)
        return found

    def context(
        self, log: Sequence[Event], subject_index: int, *, complete_since: EventTime | None
    ) -> FeatureContext:
        current = os.environ.get("PYTEST_CURRENT_TEST", "")
        self.served.add(current.split("::")[-1].split(" ")[0])
        case = Case(tuple(log), subject_index)
        if case.key not in self.rows:
            self.single_case_builds += 1
            self.rows.update(self.build([case], prefixed=False))
        return self.rows[case.key].context(complete_since=complete_since)


def first_delivery(case: Case) -> Event:
    """The subject's first delivery: what a complete history holds for its identity."""
    return next(e for e in case.log if e.identity == case.subject.identity)


@pytest.fixture(scope="module")
def spark() -> Iterator[Any]:
    from trace_core.stream.session import build_session

    session = build_session("trace-x-gold-conformance", driver_memory="2g")
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture(scope="module")
def gold(spark: Any, tmp_path_factory: pytest.TempPathFactory) -> GoldContexts:
    contexts = GoldContexts(spark, tmp_path_factory.mktemp("gold-conformance"))
    cases = record_cases(fixture_names())
    assert cases, "the recorder collected no fixture log"
    contexts.rows.update(contexts.build(cases, prefixed=True))
    return contexts


class TestGoldEventTimeComplete(EventTimeCompleteConformanceSuite):
    """Gold must give the literal answers over a complete history."""

    _contexts: GoldContexts

    @pytest.fixture(autouse=True)
    def _gold(self, gold: GoldContexts) -> None:
        self._contexts = gold

    def context_for(
        self,
        log: Sequence[Event],
        subject_index: int,
        subject: CanonicalTransaction,
        *,
        complete_since: EventTime | None,
    ) -> FeatureContext:
        return self._contexts.context(log, subject_index, complete_since=complete_since)


CROSS_CHECKED: Final = (
    "test_same_millisecond_ties_break_by_identity_whatever_the_arrival_order",
    "test_home_sums_distances_in_whole_metres_rounded_half_up",
    "test_an_outcome_whose_transaction_arrived_later_counts_in_a_complete_history",
    "test_baselines_read_only_the_lifetime",
)


def test_batched_contexts_equal_single_case_builds(gold: GoldContexts) -> None:
    """Logs rebuilt alone, without prefixes, give the rows the shared prefixed build gave."""
    checked = 0
    for name in CROSS_CHECKED:
        case = record_cases([name])[0]
        alone = gold.build([case], prefixed=False)[case.key]
        assert comparable(alone) == comparable(gold.rows[case.key]), name
        checked += 1
    assert checked == len(CROSS_CHECKED)


def test_every_event_time_complete_fixture_ran_on_gold(
    gold: GoldContexts, request: pytest.FixtureRequest
) -> None:
    collected = {
        item.name
        for item in request.session.items
        if getattr(item, "cls", None) is TestGoldEventTimeComplete
    }
    assert collected, "no event-time-complete fixture was collected for Gold"
    assert collected <= set(fixture_names())
    unread = sorted(collected - gold.served)
    assert not unread, f"fixtures that never read a Gold context: {unread}"
    assert gold.builds >= 1
