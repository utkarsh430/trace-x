"""The only way a Spark session is built (the Phase 3 toolchain contract, ADR-0045).

`build_session` checks the pinned toolchain BEFORE any JVM starts
(`toolchain.inspect_toolchain`), refuses the environment variables that would put jars
or configuration on the classpath from outside that toolchain, puts only verified jars
on the classpath, sets the configuration every warm-path job depends on, and then
interrogates the running JVM. The post-launch check is not redundant: the preflight
reads files and package metadata, while the running JVM is what will execute queries
-- a `SPARK_HOME` pointing at another distribution, or a class loaded from somewhere
other than the verified jar, is invisible to the first and caught by the second.

Set here, once, rather than per job:

* `spark.sql.session.timeZone=UTC` and a UTC JVM default. `occurred_at` is compared
  across three implementations at millisecond window boundaries, and a session in
  local time shifts every window by the host's offset.
* `spark.sql.ansi.enabled=true`. An invalid cast raises instead of becoming a null,
  and a silent null is a missing value nobody declared.
* The RocksDB state store with changelog checkpointing, so streaming state lives
  off the JVM heap on a laptop-sized driver.
* Delta's SQL extension and catalog.

pyspark is imported inside `build_session`, after every check that can run without a
JVM, so importing this module never starts one and a wrong toolchain is reported
before one is launched.
"""

from __future__ import annotations

import os
import shutil
import sys
from collections.abc import Mapping, MutableMapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from trace_core.domain.errors import ToolchainMismatchError
from trace_core.stream import toolchain
from trace_core.stream.lake import WAREHOUSE_DIRNAME, LakeConfig

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

BASE_CONF: Final[dict[str, str]] = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.session.timeZone": "UTC",
    "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
    "spark.executor.extraJavaOptions": "-Duser.timezone=UTC",
    "spark.sql.ansi.enabled": "true",
    "spark.sql.streaming.stateStore.providerClass": (
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
    ),
    "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled": "true",
}
"""The configuration that is part of the contract. `extra_conf` may not override it."""

RESERVED_KEYS: Final = frozenset(
    {
        *BASE_CONF,
        "spark.jars",
        "spark.jars.packages",
        "spark.jars.repositories",
        "spark.jars.excludes",
        "spark.jars.ivy",
        "spark.jars.ivySettings",
        "spark.driver.extraClassPath",
        "spark.executor.extraClassPath",
        "spark.sql.warehouse.dir",
    }
)
"""Every setting that can add jars or resolve them from a repository, the contract, and where
Spark puts what it manages without a path (the lake root's `_warehouse`, ADR-0048)."""

LOCAL_DRIVER_CONF: Final[dict[str, str]] = {
    "spark.driver.bindAddress": "127.0.0.1",
    "spark.driver.host": "127.0.0.1",
}
"""Applied to `local[...]` masters only.

A local-mode driver otherwise binds its RPC endpoint to whatever the host name
resolves to. When that is an address the process cannot bind -- a VPN interface, a
stale DHCP lease, a sandboxed shell -- the JVM gives up with
`BindException: Can't assign requested address` after sixteen retries, which is how
the first session on this project's reference machine failed. A single-JVM local
session talks only to itself, so loopback is the one address it needs, and pinning
it removes a dependency on how the host happens to be named."""

VERIFIED_CLASSES: Final[dict[str, str]] = {
    "io.delta.sql.DeltaSparkSessionExtension": "delta-spark",
    "org.apache.spark.sql.kafka010.KafkaSourceProvider": "the Kafka connector",
}
"""Classes that must load from a verified jar in the running JVM, not merely exist."""


def refuse_classpath_bypasses(environ: Mapping[str, str] | None = None) -> None:
    """Refuse environment that would add jars or configuration outside the contract.

    `RESERVED_KEYS` only governs what a caller passes to `build_session`. These do the
    same job from outside it, invisibly: `PYSPARK_SUBMIT_ARGS=--packages ...` resolves
    from Maven, a `spark-defaults.conf` under `SPARK_CONF_DIR` can add classpath
    entries, and `SPARK_HOME` can point the launcher at another Spark distribution.
    """
    env = os.environ if environ is None else environ
    problems: list[str] = []
    submit_args = env.get("PYSPARK_SUBMIT_ARGS", "").strip()
    if submit_args not in ("", "pyspark-shell"):
        problems.append(
            f"PYSPARK_SUBMIT_ARGS={submit_args!r} can add jars or settings outside the contract"
        )
    if env.get("SPARK_CONF_DIR"):
        problems.append(
            f"SPARK_CONF_DIR={env['SPARK_CONF_DIR']!r} can supply a spark-defaults.conf that adds "
            f"jars or overrides the contract"
        )
    spark_home = env.get("SPARK_HOME")
    if spark_home:
        pinned = toolchain.pyspark_jars_dir()
        if pinned is None or Path(spark_home).resolve() != pinned.parent.resolve():
            problems.append(
                f"SPARK_HOME={spark_home!r} is not the pinned pyspark installation "
                f"({pinned.parent if pinned else 'pyspark is not installed'})"
            )
    if problems:
        raise ToolchainMismatchError(
            "refusing to start Spark with classpath or configuration from outside the verified "
            "toolchain: " + "; ".join(problems) + ". Unset them; the session factory sets "
            "everything a warm-path job needs."
        )


def pin_worker_python(
    environ: MutableMapping[str, str] | None = None, *, executable: str | None = None
) -> None:
    """Make Spark's Python workers run the driver's own interpreter, or refuse.

    PySpark starts its Python workers with `PYSPARK_PYTHON`, or with the first `python3` on PATH
    when that is unset, never with the driver's interpreter (`pyspark/core/context.py`). With
    several virtual environments on one machine, the workers then import another checkout's
    installed `trace_core`: a UDF runs different admission, digest or timing rules than the driver,
    and a module added since fails outright. Observed in Step 6: Silver's admission UDF raised
    `ModuleNotFoundError: No module named 'trace_core.stream.silver_rules'` because the workers ran
    the main checkout's environment. So an unset `PYSPARK_PYTHON` is set to the driver's
    interpreter, and a value naming another environment is refused, as is a `PYSPARK_DRIVER_PYTHON`
    naming another environment.

    Environments are compared by the interpreter's directory, without resolving symlinks: every
    virtual environment's `python` links to the same base interpreter, so resolving would make two
    different environments look identical.
    """
    env = os.environ if environ is None else environ
    driver = Path(executable or sys.executable).absolute()
    problems: list[str] = []
    for name in ("PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON"):
        value = env.get(name, "").strip()
        if not value:
            continue
        located = value if os.sep in value else shutil.which(value, path=env.get("PATH"))
        candidate = Path(located or value).absolute()
        if candidate.parent != driver.parent:
            problems.append(
                f"{name}={value!r} is not in the driver's environment ({driver.parent})"
            )
    if problems:
        raise ToolchainMismatchError(
            "refusing to start Spark with Python workers from another environment: "
            + "; ".join(problems)
            + ". Unset them; the session factory pins the workers to the driver's interpreter, so "
            "they run the same code."
        )
    if not env.get("PYSPARK_PYTHON", "").strip():
        env["PYSPARK_PYTHON"] = str(driver)


def require_toolchain(environ: Mapping[str, str] | None = None) -> None:
    """Raise `ToolchainMismatchError` listing every failed check, or return."""
    failures = [f for f in toolchain.inspect_toolchain(environ) if not f.ok]
    if failures:
        lines = "\n".join(f"  - {f.component}: {f.detail}\n      fix: {f.remedy}" for f in failures)
        raise ToolchainMismatchError(
            "the Phase 3 toolchain does not match its pins; no Spark session was started:\n" + lines
        )


def build_session(
    app_name: str,
    *,
    master: str = "local[2]",
    shuffle_partitions: int = 2,
    driver_memory: str = "1g",
    ui: bool = False,
    extra_conf: Mapping[str, str] | None = None,
) -> SparkSession:
    """A Spark session on the verified toolchain, or `ToolchainMismatchError`.

    Refuses to adopt a session that is already active in the process: Spark's
    `getOrCreate` would silently ignore every static setting passed here (the
    jars, the extensions, the driver memory) and hand back a session configured
    by whoever built it first.
    """
    if extra_conf is not None and (clashes := sorted(RESERVED_KEYS & set(extra_conf))):
        raise ValueError(
            f"extra_conf may not set {clashes}: they are part of the toolchain contract"
        )
    refuse_classpath_bypasses()
    pin_worker_python()
    require_toolchain()

    from pyspark.sql import SparkSession

    if SparkSession.getActiveSession() is not None:
        raise ToolchainMismatchError(
            "a Spark session is already active in this process; stop it before building "
            "another, or its static configuration would silently win"
        )

    conf: dict[str, str] = {
        **(LOCAL_DRIVER_CONF if master.startswith("local") else {}),
        **BASE_CONF,
        "spark.jars": ",".join(str(path) for path in toolchain.locked_jar_paths()),
        "spark.sql.shuffle.partitions": str(shuffle_partitions),
        "spark.driver.memory": driver_memory,
        "spark.ui.enabled": "true" if ui else "false",
        # Resolved quietly: the job that uses the lake logs its root; a session factory that logged
        # it on every build polluted the output of every process, a refused one included.
        "spark.sql.warehouse.dir": str(LakeConfig.from_env(log=False).root / WAREHOUSE_DIRNAME),
        **(extra_conf or {}),
    }
    builder: Any = SparkSession.builder.appName(app_name).master(master)
    for key, value in conf.items():
        builder = builder.config(key, value)
    session: SparkSession = builder.getOrCreate()
    try:
        verify_running_jvm(session)
    except ToolchainMismatchError:
        session.stop()
        raise
    return session


def running_versions(session: SparkSession) -> dict[str, str]:
    """What the live JVM reports, read through the py4j gateway.

    `java_vendor` is recorded for run manifests rather than enforced: the pin asserts
    the Java major version, and any OpenJDK 17 build runs Spark 4.0.1 identically;
    Temurin is the pinned and tested distribution (ADR-0045)."""
    jvm: Any = session.sparkContext._jvm
    system: Any = jvm.java.lang.System
    return {
        "spark": str(session.version),
        "java": str(system.getProperty("java.specification.version")),
        "java_vendor": str(system.getProperty("java.vendor")),
        "scala": str(jvm.scala.util.Properties.versionNumberString()),
        "hadoop": str(jvm.org.apache.hadoop.util.VersionInfo.getVersion()),
    }


def class_locations(session: SparkSession) -> dict[str, str]:
    """The file each `VERIFIED_CLASSES` entry was actually loaded from, or an error."""
    jvm: Any = session.sparkContext._jvm
    locations: dict[str, str] = {}
    for class_name in VERIFIED_CLASSES:
        try:
            klass: Any = jvm.org.apache.spark.util.Utils.classForName(class_name, False, False)
            locations[class_name] = str(
                klass.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()
            )
        except Exception as exc:  # py4j surfaces JVM failures as its own error types
            locations[class_name] = f"unresolvable: {type(exc).__name__}: {exc}"
    return locations


def verify_running_jvm(session: SparkSession) -> None:
    reported = running_versions(session)
    problems: list[str] = []
    if reported["spark"] != toolchain.SPARK_VERSION:
        problems.append(f"Spark {reported['spark']}, pin {toolchain.SPARK_VERSION}")
    if reported["java"] != toolchain.JAVA_MAJOR:
        problems.append(f"Java {reported['java']}, pin {toolchain.JAVA_MAJOR}")
    if not reported["scala"].startswith(f"{toolchain.SCALA_LINE}."):
        problems.append(f"Scala {reported['scala']}, pin {toolchain.SCALA_LINE}.x")
    if not reported["hadoop"].startswith(f"{toolchain.HADOOP_LINE}."):
        problems.append(f"Hadoop {reported['hadoop']}, pin {toolchain.HADOOP_LINE}.x")
    if session.sparkContext.getConf().contains("spark.jars.packages"):
        problems.append("spark.jars.packages is set on the running session")
    verified = {path.resolve() for path in toolchain.locked_jar_paths()}
    for class_name, location in class_locations(session).items():
        if location.startswith("unresolvable:") or Path(location).resolve() not in verified:
            problems.append(f"{class_name} was loaded from {location}, not from a verified jar")
    if problems:
        raise ToolchainMismatchError(
            "the running JVM contradicts the verified toolchain: " + "; ".join(problems)
        )


__all__ = [
    "BASE_CONF",
    "LOCAL_DRIVER_CONF",
    "RESERVED_KEYS",
    "VERIFIED_CLASSES",
    "build_session",
    "class_locations",
    "refuse_classpath_bypasses",
    "require_toolchain",
    "running_versions",
    "verify_running_jvm",
]
