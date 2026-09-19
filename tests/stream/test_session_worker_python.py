"""A Spark Python worker runs the driver's own interpreter, and so imports the driver's own code.

Before `pin_worker_python`, PySpark started workers with the first `python3` on PATH. On a machine
with several checkouts that ran another checkout's `trace_core`, which Silver's admission UDF hit in
Step 6. The pure rule is unit-tested in tests/unit/test_stream_worker_python.py.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest
from tests.stream.test_bronze_tables import spark  # noqa: F401 -- a fixture

pytestmark = pytest.mark.stream


def test_a_python_worker_imports_the_same_trace_core_as_the_driver(spark: Any) -> None:  # noqa: F811
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import StringType

    import trace_core

    def where(_: int) -> str:
        import sys as worker_sys

        import trace_core as worker_trace_core

        return f"{worker_sys.executable}|{worker_trace_core.__file__}"

    probe = F.udf(where, StringType())
    (row,) = spark.range(1).select(probe("id").alias("where")).collect()
    executable, module = str(row["where"]).split("|")
    assert module == trace_core.__file__, "the worker imported another checkout's trace_core"
    assert Path(executable).absolute().parent == Path(sys.executable).absolute().parent
