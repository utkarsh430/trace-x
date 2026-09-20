"""A real Spark session on the verified toolchain: the JVM, Delta and the Kafka connector.

Marker `stream`: skipped loudly when the toolchain does not match its pins, and a
`-m stream` session in which none of these executed FAILS (tests/conftest.py), so
this file cannot turn a CI job green by skipping.
"""

from __future__ import annotations

import datetime as dt
import os
import re
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from trace_core.stream import toolchain

pytestmark = pytest.mark.stream


@pytest.fixture(scope="module")
def spark() -> Iterator[Any]:
    from trace_core.stream.session import build_session

    session = build_session("trace-x-toolchain-test")
    try:
        yield session
    finally:
        session.stop()


def test_the_running_jvm_is_the_pinned_toolchain(spark: Any) -> None:
    """Asked of the live JVM, not of files: this is what will execute queries."""
    from trace_core.stream.session import running_versions

    versions = running_versions(spark)
    assert versions["spark"] == toolchain.SPARK_VERSION
    assert versions["java"] == toolchain.JAVA_MAJOR
    assert versions["scala"].startswith(f"{toolchain.SCALA_LINE}.")
    assert versions["hadoop"].startswith(f"{toolchain.HADOOP_LINE}.")


def test_no_dependency_is_resolved_from_maven_at_run_time(spark: Any) -> None:
    conf = spark.sparkContext.getConf()
    assert not conf.contains("spark.jars.packages")
    jars = [Path(entry) for entry in conf.get("spark.jars").split(",")]
    assert sorted(jar.name for jar in jars) == sorted(j.filename for j in toolchain.load_lock())
    assert {jar.parent for jar in jars} == {toolchain.jars_dir()}


def test_spark_manages_nothing_outside_the_lake_root(spark: Any) -> None:
    """A managed table created without a path lands under the lake root's `_warehouse`, never in a
    `spark-warehouse` beside whichever directory the job started in (ADR-0048)."""
    from trace_core.stream.lake import WAREHOUSE_DIRNAME, LakeConfig

    value = str(spark.conf.get("spark.sql.warehouse.dir"))
    path = "/" + value.split(":", 1)[1].lstrip("/") if value.startswith("file:") else value
    assert Path(path) == LakeConfig.from_env().root / WAREHOUSE_DIRNAME


def test_a_delta_table_round_trips_with_millisecond_utc_event_time(
    spark: Any, tmp_path: Path
) -> None:
    """The window arithmetic of all three implementations meets at epoch milliseconds."""
    occurred = dt.datetime(2026, 3, 1, 0, 0, 0, 123_000, tzinfo=dt.UTC)
    frame = spark.createDataFrame([(1, occurred)], "id INT, occurred_at TIMESTAMP")
    path = tmp_path / "delta"
    frame.write.format("delta").save(str(path))

    assert (path / "_delta_log").is_dir()
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    [row] = (
        spark.read.format("delta")
        .load(str(path))
        .selectExpr("id", "unix_millis(occurred_at) AS occurred_ms")
        .collect()
    )
    assert row["id"] == 1
    assert row["occurred_ms"] == int(occurred.timestamp() * 1000)


def test_the_kafka_connector_resolves_from_the_verified_jars(spark: Any) -> None:
    """Defining a Kafka source loads the provider class without contacting a broker."""
    frame = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "127.0.0.1:9")
        .option("subscribe", "toolchain-probe")
        .load()
    )
    assert {"key", "value", "topic", "partition", "offset", "timestamp"} <= set(frame.columns)


def test_an_invalid_cast_raises_instead_of_inventing_a_null(spark: Any) -> None:
    """ANSI mode: a silent null would be a missing value nobody declared."""
    with pytest.raises(Exception, match="CAST_INVALID_INPUT"):
        spark.sql("SELECT CAST('not-a-number' AS INT)").collect()


def _an_installed_jdk_that_is_not_the_pin() -> Path | None:
    candidates = [
        Path(value)
        for key, value in os.environ.items()
        if re.fullmatch(r"JAVA_HOME_\d+_(X64|ARM64|AARCH64)", key)
        and not key.startswith(f"JAVA_HOME_{toolchain.JAVA_MAJOR}_")
    ]
    if sys.platform == "darwin" and Path("/usr/libexec/java_home").exists():
        listing = subprocess.run(
            ["/usr/libexec/java_home", "-V"], capture_output=True, text=True, check=False
        )
        candidates += [
            Path(m) for m in re.findall(r"(/\S+/Contents/Home)", listing.stderr + listing.stdout)
        ]
    for home in candidates:
        exe = home / "bin" / "java"
        if exe.is_file() and toolchain.java_major(exe) not in (None, toolchain.JAVA_MAJOR):
            return home
    return None


def test_a_real_jdk_that_is_not_the_pin_is_refused_before_a_jvm_starts() -> None:
    """The fake-JDK unit tests prove the check; this proves it against a real JDK, in a
    fresh interpreter, and that the refusal happens before pyspark.sql is imported."""
    home = _an_installed_jdk_that_is_not_the_pin()
    if home is None:
        pytest.skip(
            "SKIPPED (NOT PASSED): no installed JDK other than the pin to refuse; "
            "tests/unit/test_stream_toolchain.py still covers the check with fake JDKs"
        )
    code = (
        "import sys\n"
        "from trace_core.domain.errors import ToolchainMismatchError\n"
        "from trace_core.stream.session import build_session\n"
        "try:\n"
        "    build_session('wrong-java')\n"
        "except ToolchainMismatchError as exc:\n"
        "    print('REFUSED', 'pyspark.sql' in sys.modules)\n"
        "    print(exc)\n"
        "    sys.exit(3)\n"
        "print('BUILT')\n"
    )
    result = subprocess.run(  # noqa: S603 -- the running interpreter, a fixed script
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env={**os.environ, "JAVA_HOME": str(home)},
        timeout=120,
        check=False,
    )
    assert result.returncode == 3, result.stdout + result.stderr
    assert result.stdout.splitlines()[0] == "REFUSED False"
    assert f"Java {toolchain.java_major(home / 'bin' / 'java')}" in result.stdout


def test_delta_and_kafka_classes_load_from_the_verified_jars(spark: Any) -> None:
    """Present on the classpath is not enough; they must come from the pinned bytes."""
    from trace_core.stream.session import VERIFIED_CLASSES, class_locations

    verified = {path.resolve() for path in toolchain.locked_jar_paths()}
    locations = class_locations(spark)
    assert set(locations) == set(VERIFIED_CLASSES)
    for class_name, location in locations.items():
        assert Path(location).resolve() in verified, f"{class_name} came from {location}"


def test_a_running_jvm_that_contradicts_the_pins_is_stopped_and_refused() -> None:
    """The post-launch failure path, triggered for real: a fresh interpreter whose
    Hadoop pin the running JVM cannot satisfy. The preflight passes (it reads the
    jar names captured at import), the JVM starts, reports Hadoop 3.4.x, and the
    factory must stop that session and refuse it."""
    code = (
        "import sys\n"
        "from trace_core.domain.errors import ToolchainMismatchError\n"
        "from trace_core.stream import toolchain\n"
        "from trace_core.stream.session import build_session\n"
        "toolchain.HADOOP_LINE = '3.3'\n"
        "try:\n"
        "    build_session('contradicted')\n"
        "except ToolchainMismatchError as exc:\n"
        "    from pyspark.sql import SparkSession\n"
        "    print('REFUSED', SparkSession.getActiveSession() is None)\n"
        "    print(exc)\n"
        "    sys.exit(3)\n"
        "print('BUILT')\n"
    )
    result = subprocess.run(  # noqa: S603 -- the running interpreter, a fixed script
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=dict(os.environ),
        timeout=300,
        check=False,
    )
    assert result.returncode == 3, result.stdout + result.stderr
    assert result.stdout.splitlines()[0] == "REFUSED True"
    assert "the running JVM contradicts the verified toolchain" in result.stdout
    assert "Hadoop" in result.stdout
