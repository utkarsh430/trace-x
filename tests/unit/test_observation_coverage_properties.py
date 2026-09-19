"""The coverage rule against generated histories that keep its premises (ADR-0051 §5; plan §4.1).

Hand-built cases pin each clause. These generate histories nobody wrote by hand, and hold the rule
to what it exists for:
- a lost write is never outside a gap;
- `vouches` never spans a loss;
- a history that keeps every premise is never reported as an anomaly.

Time is integer milliseconds on PostgreSQL's clock, the reference. A history is one to three
sessions, one after another, written by one or two processes, each a serial writer as the gateway
is.
- A process's host clock (its stamps and store writes) is offset from PostgreSQL by at most the
  clock margin. So is the broker's clock (arrivals, and the log's high-water mark).
- A session opens, which is a confirmation, then heartbeats. A heartbeat may commit late, so it can
  be in flight across the ledger read. It may also commit without the writer learning of it, or
  never commit; after either of those, the writer sends no more heartbeats.
- The writer is ready from a confirmation's commit until `lease` after that heartbeat's start.
  Each observation is handled in a window that starts while the writer is ready. The window holds
  the stamp, a final readiness check, and the store write, which comes at most `takeover` after that
  check. Writes run on until the lease ends, whether or not heartbeats still commit. Some numbers
  are assigned and never written.
- A record arrives after its window, or never; some arrive twice. A session whose writer never lost
  it closes only when every number it assigned was written and delivered.
- A successor opens after a clean close, or `lease + takeover` after its predecessor's last
  heartbeat that PostgreSQL committed. The predecessor then commits nothing more.
- The ledger is read at any moment, often near a boundary (an open, a heartbeat's commit, a
  lease's end, a write), and sees only what had committed by then. So a session may open after it,
  and be absent. The log is read through any high-water mark.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from trace_core.observation.coverage import Coverage, Observed, SessionRow, assess

pytestmark = [pytest.mark.property, pytest.mark.unit]

T0 = dt.datetime(2026, 9, 15, 12, 0, tzinfo=dt.UTC)
MS = dt.timedelta(milliseconds=1)


def _t(ms: int) -> dt.datetime:
    return T0 + ms * MS


@dataclass(frozen=True)
class Write:
    seq: int
    stamp: int
    """When it was stamped, on PostgreSQL's clock; the record carries it on the host's."""
    written: int | None
    """When the store was written, on PostgreSQL's clock; None: assigned, never written."""
    arrivals: tuple[int, ...]
    """On the broker's clock: none when never delivered, two when delivered twice."""


@dataclass(frozen=True)
class Heartbeat:
    start: int
    commit: int | None


@dataclass(frozen=True)
class Session:
    session_id: str
    host: int
    """The writer's host clock minus PostgreSQL's."""
    opened: int
    open_commit: int
    heartbeats: tuple[Heartbeat, ...]
    writes: tuple[Write, ...]
    closed: int | None


@dataclass(frozen=True)
class History:
    sessions: tuple[Session, ...]
    lease: int
    takeover: int
    margin: int
    ledger_read: int
    log_through: int
    probe: tuple[int, int]


def _after(high: int) -> st.SearchStrategy[int]:
    """A duration biased to the boundaries a bound can be off by one margin at: zero, a few
    milliseconds, or anywhere up to `high`."""
    return st.one_of(st.just(0), st.integers(0, min(20, high)), st.integers(0, high))


def _offset(margin: int) -> st.SearchStrategy[int]:
    return st.one_of(st.just(margin), st.just(-margin), st.integers(-margin, margin))


def _ready(confirmations: list[tuple[int, int]], lease: int) -> list[tuple[int, int]]:
    """When the writer may write: from a confirmation's commit to its heartbeat's start + lease."""
    spans = sorted(
        (commit, start + lease) for start, commit in confirmations if commit <= start + lease
    )
    merged: list[tuple[int, int]] = []
    for low, high in spans:
        if merged and low <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], high))
        else:
            merged.append((low, high))
    return merged


def _session(
    draw: st.DrawFn,
    session_id: str,
    *,
    host: int,
    broker: int,
    not_before: int,
    lease: int,
    takeover: int,
) -> tuple[Session, int]:
    """One session, and the earliest time its successor may open."""
    opened = not_before + draw(_after(3_000))
    open_commit = opened + draw(_after(200))
    confirmations = [(opened, open_commit)]
    beats: list[Heartbeat] = []
    lost_by_writer = False
    cursor = open_commit
    for _ in range(draw(st.integers(0, 8))):
        start = cursor + draw(st.integers(50, lease + 500))
        outcome = draw(st.sampled_from(("confirmed", "confirmed", "late", "unconfirmed", "failed")))
        if outcome == "failed":
            beats.append(Heartbeat(start, None))
            lost_by_writer = True
            break
        commit = start + draw(st.integers(0, lease + 1_000 if outcome == "late" else 300))
        beats.append(Heartbeat(start, commit))
        if outcome == "unconfirmed":
            lost_by_writer = True
            break
        confirmations.append((start, commit))
        cursor = commit
    ready = _ready(confirmations, lease)

    writes: list[Write] = []
    cursor = open_commit
    for seq in range(1, draw(st.integers(0, 12)) + 1):
        candidate = cursor + draw(_after(1_500))
        span = next(((low, high) for low, high in ready if high >= candidate), None)
        if span is None:
            break
        low, high = span
        begin = max(candidate, low)
        if draw(st.booleans()):  # handled at the very end of the lease
            begin = max(begin, high - draw(st.integers(0, 50)))
        check = begin + draw(st.one_of(st.just(high - begin), _after(min(100, high - begin))))
        written = check + draw(st.one_of(st.just(takeover), _after(takeover)))
        stamp = draw(st.one_of(st.just(begin), st.just(written), st.integers(begin, written)))
        cursor = written + draw(_after(100))
        if draw(st.integers(0, 9)) == 0:  # refused at the final check: nothing written or produced
            writes.append(Write(seq, stamp, None, ()))
            continue
        fate = draw(st.sampled_from(("delivered", "delivered", "lost", "twice")))
        arrivals: tuple[int, ...] = ()
        if fate != "lost":
            delay = draw(st.one_of(st.integers(0, 50), st.integers(0, 40_000)))
            first = cursor + delay + broker
            arrivals = (first,)
            if fate == "twice":
                arrivals = (first, first + draw(st.integers(0, 5_000)))
        writes.append(Write(seq, stamp, written, arrivals))

    closed: int | None = None
    complete = all(write.written is not None and write.arrivals for write in writes)
    if not lost_by_writer and complete and draw(st.booleans()):
        acknowledged = max([cursor, *(write.arrivals[0] - broker for write in writes)])
        closed = acknowledged + draw(_after(500))
        beats = [
            b for b in beats if b.commit is not None and b.start < closed and b.commit <= closed
        ]
    committed = [
        (opened, open_commit),
        *((b.start, b.commit) for b in beats if b.commit is not None),
    ]
    successor = (
        closed
        if closed is not None
        else max(
            max(start for start, _ in committed) + lease + takeover,
            max(commit for _, commit in committed),
            cursor,
        )
    )
    session = Session(session_id, host, opened, open_commit, tuple(beats), tuple(writes), closed)
    return session, successor


@st.composite
def histories(draw: st.DrawFn) -> History:
    margin = draw(st.one_of(st.just(0), st.integers(0, 1_000), st.integers(300, 1_000)))
    lease = draw(st.integers(300, 6_000))
    takeover = draw(st.one_of(st.just(0), st.integers(0, 2_000)))
    hosts = [draw(_offset(margin)) for _ in range(draw(st.integers(1, 2)))]
    broker = draw(_offset(margin))
    sessions: list[Session] = []
    not_before = 0
    for index in range(draw(st.integers(1, 3))):
        host = hosts[draw(st.integers(0, len(hosts) - 1))]
        session, not_before = _session(
            draw,
            f"s{index}",
            host=host,
            broker=broker,
            not_before=not_before,
            lease=lease,
            takeover=takeover,
        )
        sessions.append(session)

    marks: list[int] = []
    writes: list[int] = []
    for session in sessions:
        confirmed = [(session.opened, session.open_commit)]
        confirmed += [(b.start, b.commit) for b in session.heartbeats if b.commit is not None]
        for start, commit in confirmed:
            marks += [commit, start + lease + takeover, start + lease + takeover + 2 * margin]
        writes += [w.written + session.host for w in session.writes if w.written is not None]
        marks += [session.opened, *([] if session.closed is None else [session.closed])]
    marks += writes
    horizon = max(marks)
    near = 2 * margin + 50
    ledger_read = draw(
        st.one_of(
            st.integers(-1_000, horizon + 1_000),
            st.sampled_from(marks).flatmap(lambda mark: st.integers(mark - near, mark + near)),
        )
    )
    arrivals = [a for session in sessions for w in session.writes for a in w.arrivals] or [0]
    log_through = draw(
        st.one_of(
            st.integers(-1_000, horizon + 45_000),
            st.just(horizon + 100_000),
            st.sampled_from(arrivals).flatmap(lambda a: st.integers(a - 50, a + 50)),
        )
    )
    low = draw(
        st.one_of(
            st.integers(-1_000, horizon),
            st.sampled_from(writes or [0]).flatmap(lambda w: st.integers(w - near - 100, w)),
        )
    )
    return History(
        sessions=tuple(sessions),
        lease=lease,
        takeover=takeover,
        margin=margin,
        ledger_read=ledger_read,
        log_through=log_through,
        probe=(low, low + draw(st.integers(0, 3_000))),
    )


def _assess(history: History) -> tuple[Coverage, set[tuple[str, int]]]:
    """The ledger as a read at `ledger_read` sees it, and the log read through its mark."""
    read = history.ledger_read
    rows: list[SessionRow] = []
    observed: list[Observed] = []
    for session in history.sessions:
        if session.open_commit <= read:
            beats = [session.opened]
            beats += [
                b.start for b in session.heartbeats if b.commit is not None and b.commit <= read
            ]
            closed = (
                session.closed if session.closed is not None and session.closed <= read else None
            )
            rows.append(
                SessionRow(
                    session_id=session.session_id,
                    started_at=_t(session.opened),
                    heartbeat_at=_t(max(beats)),
                    closed_at=None if closed is None else _t(closed),
                    last_seq=None if closed is None else len(session.writes),
                )
            )
        for write in session.writes:
            observed += [
                Observed(session.session_id, write.seq, _t(arrival), _t(write.stamp + session.host))
                for arrival in write.arrivals
                if arrival <= history.log_through
            ]
    coverage = assess(
        rows,
        observed,
        ledger_read_at=_t(read),
        log_read_through=_t(history.log_through),
        lease_s=history.lease / 1_000,
        takeover_margin_s=history.takeover / 1_000,
        clock_margin_s=history.margin / 1_000,
    )
    return coverage, {(item.session_id or "", item.seq or 0) for item in observed}


def _lost(history: History, seen: set[tuple[str, int]]) -> list[tuple[str, int, dt.datetime]]:
    """Every store write the log does not hold, at its time on its writer's host clock."""
    return [
        (session.session_id, write.seq, _t(write.written + session.host))
        for session in history.sessions
        for write in session.writes
        if write.written is not None and (session.session_id, write.seq) not in seen
    ]


SETTINGS = settings(
    max_examples=300,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)


@SETTINGS
@given(histories())
def test_every_lost_write_before_through_lies_inside_a_gap_of_its_session(history: History) -> None:
    coverage, seen = _assess(history)
    for session_id, seq, write in _lost(history, seen):
        if write >= coverage.through:
            continue
        gaps = [gap for gap in coverage.gaps if gap.session_id == session_id]
        assert any(gap.contains(write) for gap in gaps), (session_id, seq, write, coverage)


@SETTINGS
@given(histories())
def test_a_session_the_ledger_does_not_hold_is_an_open_gap_over_every_write_it_lost(
    history: History,
) -> None:
    """The rule's clause for a session in the log but not in the ledger: its gap starts at the
    ledger read, so it holds every write the session made, not only those before `through`."""
    coverage, seen = _assess(history)
    logged = {session_id for session_id, _ in seen}
    for session_id, seq, write in _lost(history, seen):
        if session_id in coverage.sessions or session_id not in logged:
            continue
        gaps = [gap for gap in coverage.unknown if gap.session_id == session_id]
        assert any(gap.contains(write) for gap in gaps), (session_id, seq, write, coverage)


@SETTINGS
@given(histories())
def test_every_gap_is_well_formed_and_the_certified_prefix_is_present(history: History) -> None:
    coverage, seen = _assess(history)
    for gap in coverage.gaps:
        assert gap.end is None or gap.start <= gap.end, gap
    for session_id, verdict in coverage.sessions.items():
        certified = {(session_id, seq) for seq in range(1, verdict.certified_through + 1)}
        assert certified <= seen, (session_id, verdict)


@SETTINGS
@given(histories())
def test_the_log_never_vouches_for_a_span_that_contains_a_lost_write(history: History) -> None:
    coverage, seen = _assess(history)
    start, end = _t(history.probe[0]), _t(history.probe[1])
    if not coverage.vouches(start, end):
        return
    for session_id, seq, write in _lost(history, seen):
        assert not start <= write <= end, (session_id, seq, write, coverage)


@SETTINGS
@given(histories())
def test_a_history_that_keeps_every_premise_is_never_an_anomaly(history: History) -> None:
    """Critic finding 3: host stamps and PostgreSQL times within the margin are consistent."""
    coverage, _ = _assess(history)
    assert not coverage.anomalies, coverage
    for verdict in coverage.sessions.values():
        assert not verdict.anomalies, verdict
