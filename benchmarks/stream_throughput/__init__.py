"""The Phase 3 stream throughput and outage benchmark (`make load-stream`, `P3.stream-throughput`).

Modules, split so everything that decides a verdict is pure and unit-tested without a JVM or a
broker:

    spec        the frozen targets and the run configuration, declared before any measurement
    lag         `freshness_lag` from per-partition Silver commits, windows, recovery detection
    clock       the broker-to-host clock offset, bounded from delivery reports
    verdict     pass / fail / invalid, and the authenticity guard
    delta_log   reads Silver's Delta commits (log + data files, pyarrow; no JVM)
    events      the declared event mix, built with the project's own event builders
    producer    paced producer worker processes on the project's `EventPublisher`
    consumer    the consumer process: the real Bronze and Silver queries in one JVM
    record      the run manifest and the report
    run         the orchestration (`python -m benchmarks.stream_throughput`)
"""
