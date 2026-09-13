#!/usr/bin/env python3
"""Print a JDK home matching the Java pin, or nothing.

The Makefile's single resolution point for `JAVA_HOME`, the same way it resolves one
Python interpreter (`VPY`): every Spark-facing target runs on the pinned JDK when one
is installed, instead of on whichever Java a shell profile happens to select. A shell
that does not source the profile -- tooling, CI steps, another terminal -- would
otherwise fall back to the system default, which on this project's reference machine
is Java 25 and cannot run Spark 4.0.1.

Prints nothing (exit 0) when no pinned JDK is found, so `make` keeps the caller's
`JAVA_HOME` and `make doctor` then reports the real problem loudly instead of this
resolver hiding it.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "packages"))

from trace_core.stream.toolchain import discover_java_home


def main() -> int:
    home = discover_java_home()
    if home is not None:
        print(home)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
