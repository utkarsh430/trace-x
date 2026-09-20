"""`python -m benchmarks.stream_throughput` (what `make load-stream` runs).

Guarded: the producer workers are spawned processes, and a spawned child re-imports the parent's
main module under another name. Unguarded, every worker would start a benchmark of its own.
"""

from benchmarks.stream_throughput.run import main

if __name__ == "__main__":
    raise SystemExit(main())
