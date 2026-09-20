"""A Bronze or Silver job in a child process, killed for real at a named commit point (chaos only).

`tests/chaos/test_spark_resume.py` runs this as

    python -m tests.chaos.spark_resume_harness --job silver --kill-at after_canonical \
        --from-batch 3 -- run --topic tx.raw.v1 --available-now

Everything after `--` goes to the service entrypoint itself (`services.stream.bronze.main` or
`services.stream.silver.main`), so the job runs with production's session factory, checkpoint,
sink and supervision. The harness adds one thing: a kill point. It wraps calls the job already
makes -- `OpenedCheckpoint.append` and `.merge`, and the `foreachBatch` function the job builds.
Each wrapped call first runs to completion, unchanged, against real Delta. Then, at the named point
of the first batch numbered `--from-batch` or later, the harness prints one JSON line saying so and
sends SIGKILL to its whole process group: this Python driver, the Spark JVM it launched, and any
Python workers. Nothing after the named point runs. No production code carries a hook for this.

Kill points:
- bronze `after_append`: the batch's Bronze append committed; the sink's after-write topic check and
  Spark's commit entry did not run.
- bronze `after_sink`: the `foreachBatch` function returned; Spark has not recorded the batch.
- silver `after_quarantine`, `after_duplicates`, `after_canonical`, `after_late_events`: the named
  target's commit for the batch landed, and nothing after it ran. The sink commits in that order
  (`trace_core.stream.silver._write_batch`), each only when it has rows to write.
- silver `after_sink`: every commit and the uniqueness assertions finished; Spark has not recorded
  the batch.

The test must start this process in its own session (`start_new_session=True`), so that the group
it kills is the job's and never the test's.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import signal
import sys
from collections.abc import Callable
from typing import Any

KILL_POINTS = {
    "bronze": ("after_append", "after_sink"),
    "silver": (
        "after_quarantine",
        "after_duplicates",
        "after_canonical",
        "after_late_events",
        "after_sink",
    ),
}
MARKER = "harness_kill"


def _parse(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    if "--" not in argv:
        raise SystemExit("usage: spark_resume_harness --job J --kill-at P --from-batch N -- ARGS")
    split = argv.index("--")
    parser = argparse.ArgumentParser(prog="python -m tests.chaos.spark_resume_harness")
    parser.add_argument("--job", choices=sorted(KILL_POINTS), required=True)
    parser.add_argument("--kill-at", required=True)
    parser.add_argument("--from-batch", type=int, required=True)
    args = parser.parse_args(argv[:split])
    if args.kill_at not in KILL_POINTS[args.job]:
        parser.error(f"--kill-at {args.kill_at!r} is not a {args.job} kill point")
    if os.getpgrp() != os.getpid():
        parser.error("start the harness in its own session: it kills its whole process group")
    return args, argv[split + 1 :]


def _arm(job: str, kill_at: str, from_batch: int) -> None:
    from trace_core.stream import checkpoints

    def fire(point: str, batch_id: int, target: str) -> None:
        if point != kill_at or batch_id < from_batch:
            return
        line = {MARKER: {"point": point, "batch_id": batch_id, "target": target}}
        sys.stdout.write(json.dumps(line, sort_keys=True) + "\n")
        sys.stdout.flush()
        os.killpg(os.getpgrp(), signal.SIGKILL)

    opened: Any = checkpoints.OpenedCheckpoint
    original_append = opened.append
    original_merge = opened.merge

    if job == "bronze":
        from trace_core.stream import bronze

        def bronze_append(self: Any, frame: Any, *, batch_id: int, target: Any) -> None:
            original_append(self, frame, batch_id=batch_id, target=target)
            fire("after_append", batch_id, str(target))

        original_bronze_sink = bronze.bronze_sink

        def bronze_sink(spec: Any, checkpoint: Any, **kwargs: Any) -> Callable[[Any, int], None]:
            sink = original_bronze_sink(spec, checkpoint, **kwargs)

            def wrapped(batch: Any, batch_id: int) -> None:
                sink(batch, batch_id)
                fire("after_sink", batch_id, str(spec.table))

            return wrapped

        module: Any = bronze
        opened.append = bronze_append
        module.bronze_sink = bronze_sink
        return

    from trace_core.stream import silver
    from trace_core.stream.silver_rules import DUPLICATES, LATE_EVENTS, QUARANTINE

    appended = {str(QUARANTINE): "after_quarantine", str(DUPLICATES): "after_duplicates"}

    def silver_append(self: Any, frame: Any, *, batch_id: int, target: Any) -> None:
        original_append(self, frame, batch_id=batch_id, target=target)
        fire(appended.get(str(target), "after_other_append"), batch_id, str(target))

    def silver_merge(self: Any, session: Any, *, batch_id: int, target: Any, build: Any) -> None:
        original_merge(self, session, batch_id=batch_id, target=target, build=build)
        point = "after_late_events" if str(target) == str(LATE_EVENTS) else "after_canonical"
        fire(point, batch_id, str(target))

    original_silver_sink = silver.silver_sink

    def silver_sink(spec: Any, checkpoint: Any, *, git_sha: str) -> Callable[[Any, int], None]:
        sink = original_silver_sink(spec, checkpoint, git_sha=git_sha)

        def wrapped(batch: Any, batch_id: int) -> None:
            sink(batch, batch_id)
            fire("after_sink", batch_id, str(spec.table))

        return wrapped

    module = silver
    opened.append = silver_append
    opened.merge = silver_merge
    module.silver_sink = silver_sink


def main(argv: list[str]) -> int:
    args, service_argv = _parse(argv)
    _arm(args.job, args.kill_at, args.from_batch)
    service: Any = importlib.import_module(f"services.stream.{args.job}")
    return int(service.main(service_argv))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
