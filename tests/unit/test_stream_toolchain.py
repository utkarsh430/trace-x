"""The Phase 3 toolchain is checked before a JVM starts, and every failure says how to fix it.

Unit tests: no JVM, and pyspark is never imported. The fake JDKs here are real
executables that print exactly what a JDK prints to `java -version`, so the code
under test runs its real subprocess path -- it is the JDK that is substituted, not
the check. The real-JVM half of this contract is `tests/stream/test_session.py`.
"""

from __future__ import annotations

import ast
import hashlib
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

from trace_core.domain.errors import ToolchainMismatchError
from trace_core.stream import toolchain
from trace_core.stream.session import RESERVED_KEYS, build_session, require_toolchain

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]


def fake_jdk(root: Path, banner: str) -> Path:
    """A directory that behaves like a JDK home whose `java -version` prints `banner`."""
    home = root / "jdk"
    (home / "bin").mkdir(parents=True)
    java = home / "bin" / "java"
    java.write_text(f"#!/bin/sh\necho '{banner}' 1>&2\nexit 0\n")
    java.chmod(0o755)
    return home


# --- pins ---------------------------------------------------------------------


def test_the_mirrored_pins_equal_pyproject() -> None:
    """The toolchain module mirrors `[tool.trace_x.pins]` because a deployed artefact
    does not ship pyproject.toml. Mirroring is only safe if the two cannot drift."""
    pins = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["trace_x"]["pins"]
    mirrored = {
        "java": toolchain.JAVA_MAJOR,
        "spark": toolchain.SPARK_VERSION,
        "delta": toolchain.DELTA_VERSION,
        "hadoop": toolchain.HADOOP_LINE,
        "scala": toolchain.SCALA_LINE,
    }
    assert {key: pins[key] for key in mirrored} == mirrored


# --- java -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("banner", "major"),
    [
        ('openjdk version "17.0.18" 2026-01-20 LTS', "17"),
        ('openjdk version "25.0.2" 2026-01-20 LTS', "25"),
        ('openjdk version "21" 2023-09-19', "21"),
        ('java version "1.8.0_392"', "8"),
        ("no version banner at all", None),
    ],
)
def test_the_java_major_is_read_from_every_banner_shape(banner: str, major: str | None) -> None:
    assert toolchain.parse_java_major(banner) == major


def test_java_25_is_refused_with_the_command_that_fixes_it(tmp_path: Path) -> None:
    home = fake_jdk(tmp_path, 'openjdk version "25.0.2" 2026-01-20 LTS')
    finding = toolchain.check_java({"JAVA_HOME": str(home), "PATH": ""})
    assert not finding.ok
    assert "Java 25" in finding.detail
    assert f"Temurin {toolchain.JAVA_MAJOR}" in finding.detail
    assert "JAVA_HOME" in finding.remedy


def test_the_pinned_java_passes(tmp_path: Path) -> None:
    home = fake_jdk(tmp_path, f'openjdk version "{toolchain.JAVA_MAJOR}.0.18" 2026-01-20 LTS')
    assert toolchain.check_java({"JAVA_HOME": str(home), "PATH": ""}).ok


def test_without_java_home_the_java_on_path_is_the_one_checked(tmp_path: Path) -> None:
    """Spark's launcher falls back to PATH when JAVA_HOME is unset; the check must too."""
    home = fake_jdk(tmp_path, 'openjdk version "25.0.2"')
    finding = toolchain.check_java({"PATH": str(home / "bin")})
    assert not finding.ok
    assert str(home / "bin" / "java") in finding.detail


def test_no_java_at_all_is_a_failure_not_a_pass() -> None:
    finding = toolchain.check_java({"PATH": ""})
    assert not finding.ok
    assert finding.remedy


def test_the_resolver_prefers_the_pinned_jdk_over_a_wrong_java_home(tmp_path: Path) -> None:
    wrong = fake_jdk(tmp_path / "wrong", 'openjdk version "25.0.2"')
    right = fake_jdk(tmp_path / "right", f'openjdk version "{toolchain.JAVA_MAJOR}.0.18"')
    env = {"JAVA_HOME": str(wrong), f"JAVA_HOME_{toolchain.JAVA_MAJOR}_X64": str(right)}
    assert toolchain.discover_java_home(env) == right


def test_the_resolver_believes_a_jdk_only_after_running_it(tmp_path: Path) -> None:
    """A directory called temurin-17 that is really Java 25 must not be selected."""
    liar = fake_jdk(tmp_path / "temurin-17", 'openjdk version "25.0.2"')
    env = {"JAVA_HOME": str(liar), f"JAVA_HOME_{toolchain.JAVA_MAJOR}_X64": str(liar)}
    assert toolchain.discover_java_home(env) != liar


# --- bundled hadoop / scala -------------------------------------------------------


def _jars(tmp_path: Path, *names: str) -> Path:
    directory = tmp_path / "jars"
    directory.mkdir()
    for name in names:
        (directory / name).write_bytes(b"jar")
    return directory


def test_the_pinned_hadoop_and_scala_lines_pass(tmp_path: Path) -> None:
    jars = _jars(tmp_path, "hadoop-client-api-3.4.1.jar", "scala-library-2.13.16.jar")
    assert toolchain.check_bundled("hadoop", jars).ok
    assert toolchain.check_bundled("scala", jars).ok


def test_a_hadoop_33_jar_is_refused_as_the_nosuchmethoderror_it_would_become(
    tmp_path: Path,
) -> None:
    finding = toolchain.check_bundled("hadoop", _jars(tmp_path, "hadoop-client-api-3.3.6.jar"))
    assert not finding.ok
    assert "NoSuchMethodError" in finding.remedy


def test_scala_212_is_refused(tmp_path: Path) -> None:
    assert not toolchain.check_bundled("scala", _jars(tmp_path, "scala-library-2.12.18.jar")).ok


def test_two_hadoop_jars_are_ambiguous_and_refused(tmp_path: Path) -> None:
    jars = _jars(tmp_path, "hadoop-client-api-3.4.1.jar", "hadoop-client-api-3.3.6.jar")
    assert not toolchain.check_bundled("hadoop", jars).ok


def test_an_absent_pyspark_is_reported_rather_than_assumed() -> None:
    finding = toolchain.check_bundled("scala", None)
    assert not finding.ok
    assert "make setup" in finding.remedy


# --- locked jars -------------------------------------------------------------------


def test_a_verified_jar_passes_and_a_single_flipped_bit_is_refused(tmp_path: Path) -> None:
    body = b"PK\x03\x04 stand-in jar bytes"
    jar = toolchain.LockedJar("org.example:thing:1.0", hashlib.sha256(body).hexdigest(), len(body))
    (tmp_path / jar.filename).write_bytes(body)
    assert all(f.ok for f in toolchain.check_locked_jars([jar], tmp_path))

    tampered = bytearray(body)
    tampered[-1] ^= 0x01
    (tmp_path / jar.filename).write_bytes(bytes(tampered))
    [finding] = toolchain.check_locked_jars([jar], tmp_path)
    assert not finding.ok
    assert "does not match jars.lock" in finding.detail
    assert "Never edit jars.lock" in finding.remedy


def test_a_missing_jar_names_the_command_that_fetches_it(tmp_path: Path) -> None:
    jar = toolchain.LockedJar("org.example:thing:1.0", "0" * 64, 1)
    [finding] = toolchain.check_locked_jars([jar], tmp_path)
    assert not finding.ok
    assert "make stream-jars" in finding.remedy


def test_the_lock_pins_every_jar_by_content_for_the_declared_versions() -> None:
    jars = toolchain.load_lock()
    coordinates = [jar.coordinate for jar in jars]
    assert len(coordinates) == len(set(coordinates))
    for jar in jars:
        assert re.fullmatch(r"[0-9a-f]{64}", jar.sha256), jar.coordinate
        assert jar.size > 0
        if re.search(r"_2\.\d+$", jar.artifact):
            assert jar.artifact.endswith(f"_{toolchain.SCALA_LINE}"), jar.coordinate
    scala = toolchain.SCALA_LINE
    assert f"io.delta:delta-spark_{scala}:{toolchain.DELTA_VERSION}" in coordinates
    assert f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{toolchain.SPARK_VERSION}" in coordinates


def test_no_locked_jar_duplicates_one_pyspark_already_bundles() -> None:
    """Two copies of a class on one classpath: which loads first is not something a
    test should depend on, and not something a lock should allow."""
    bundled_dir = toolchain.pyspark_jars_dir()
    if bundled_dir is None or not bundled_dir.is_dir():
        pytest.skip(
            "SKIPPED (NOT PASSED): pyspark is not installed, so its bundled jars cannot be "
            "compared with jars.lock. Run `make setup`."
        )
    bundled = {
        match.group(1)
        for path in bundled_dir.glob("*.jar")
        if (match := re.match(r"^(.*?)-\d", path.name)) is not None
    }
    duplicated = sorted(jar.coordinate for jar in toolchain.load_lock() if jar.artifact in bundled)
    assert not duplicated, f"pyspark already bundles {duplicated}"


# --- the session factory, without a JVM ------------------------------------------


def test_the_toolchain_module_is_stdlib_only() -> None:
    """`make doctor` imports it before dependencies are guaranteed to exist."""
    tree = ast.parse((ROOT / "packages" / "trace_core" / "stream" / "toolchain.py").read_text())
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            imported.add(node.module.split(".")[0])
    assert imported <= set(sys.stdlib_module_names) | {"__future__"}, imported


def test_importing_the_session_module_does_not_import_pyspark() -> None:
    code = "import sys, trace_core.stream.session; print('pyspark' in sys.modules)"
    result = subprocess.run(  # noqa: S603 -- the running interpreter, a fixed script
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ, "PYTHONPATH": str(ROOT / "packages")},
    )
    assert result.stdout.strip() == "False"


def test_a_wrong_toolchain_is_refused_with_every_failure_listed(tmp_path: Path) -> None:
    """One run shows the whole problem, not only its first symptom."""
    home = fake_jdk(tmp_path, 'openjdk version "25.0.2"')
    empty = tmp_path / "no-jars"
    with pytest.raises(ToolchainMismatchError) as caught:
        require_toolchain({"JAVA_HOME": str(home), toolchain.JARS_DIR_ENV: str(empty)})
    message = str(caught.value)
    assert "no Spark session was started" in message
    assert "java: Java 25" in message
    assert "make stream-jars" in message


def test_the_contract_configuration_cannot_be_overridden() -> None:
    assert {"spark.jars", "spark.jars.packages", "spark.sql.session.timeZone"} <= RESERVED_KEYS
    with pytest.raises(ValueError, match=r"spark\.jars\.packages"):
        build_session(
            "override", extra_conf={"spark.jars.packages": "io.delta:delta-spark_2.13:4.0.1"}
        )


# --- installed distributions: drift, absence -------------------------------------


def test_an_installed_distribution_at_another_version_is_refused() -> None:
    """pip is always installed, so this exercises the drift comparison on a real
    distribution's real metadata rather than on a fabricated one."""
    finding = toolchain.check_distribution("pip", "0.0.0-not-the-installed-version")
    assert not finding.ok
    assert "the pin is 0.0.0-not-the-installed-version" in finding.detail
    assert "requirements.lock" in finding.remedy


def test_an_uninstalled_distribution_is_refused() -> None:
    finding = toolchain.check_distribution("trace-x-no-such-distribution", "1.0")
    assert not finding.ok
    assert "is not installed" in finding.detail


# --- environment that would bypass the classpath contract ------------------------


@pytest.mark.parametrize(
    ("environ", "named"),
    [
        (
            {"PYSPARK_SUBMIT_ARGS": "--packages io.delta:delta-spark_2.13:4.0.1 pyspark-shell"},
            "PYSPARK_SUBMIT_ARGS",
        ),
        ({"PYSPARK_SUBMIT_ARGS": "--jars /tmp/anything.jar pyspark-shell"}, "PYSPARK_SUBMIT_ARGS"),
        ({"SPARK_CONF_DIR": "/etc/spark-with-its-own-spark-defaults"}, "SPARK_CONF_DIR"),
        ({"SPARK_HOME": "/opt/another-spark-distribution"}, "SPARK_HOME"),
    ],
)
def test_classpath_bypasses_are_refused_before_a_jvm_starts(
    environ: dict[str, str], named: str
) -> None:
    from trace_core.stream.session import refuse_classpath_bypasses

    with pytest.raises(ToolchainMismatchError, match=named):
        refuse_classpath_bypasses(environ)


def test_a_plain_environment_is_not_a_bypass() -> None:
    from trace_core.stream.session import refuse_classpath_bypasses

    refuse_classpath_bypasses({})
    refuse_classpath_bypasses({"PYSPARK_SUBMIT_ARGS": "pyspark-shell"})


def test_every_setting_that_can_add_jars_is_reserved() -> None:
    assert {
        "spark.jars",
        "spark.jars.packages",
        "spark.jars.repositories",
        "spark.jars.ivy",
        "spark.jars.ivySettings",
        "spark.driver.extraClassPath",
        "spark.executor.extraClassPath",
    } <= RESERVED_KEYS


# --- the collection guard itself ----------------------------------------------------


def test_an_all_skipped_stream_session_fails_the_run(tmp_path: Path) -> None:
    """The negative control for tests/conftest.py's guard: under a wrong JDK every
    stream test is skipped, and a `-m stream` run must then exit non-zero, loudly."""
    home = fake_jdk(tmp_path, 'openjdk version "25.0.2" 2026-01-20 LTS')
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-m",
            "stream",
            "tests/stream",
            "-o",
            "addopts=",
            "-q",
            "-p",
            "no:cacheprovider",
        ],
        cwd=ROOT,
        env={**os.environ, "JAVA_HOME": str(home)},
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert "collection guard" in output
    assert "skipped" in output


def test_the_jar_lock_is_exactly_what_the_lock_command_pins() -> None:
    """jars.lock claims to be written only by `scripts/stream_jars.py lock`. Without this,
    a jar added by hand -- or an entry with no upstream anchor -- would pass every other
    check, because a SHA-256 of the right shape proves nothing about where it came from."""
    import importlib.util
    import json

    spec = importlib.util.spec_from_file_location(
        "stream_jars", ROOT / "scripts" / "stream_jars.py"
    )
    assert spec is not None and spec.loader is not None
    stream_jars = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(stream_jars)

    entries = json.loads(toolchain.LOCK_PATH.read_text())["jars"]
    assert {entry["coordinate"] for entry in entries} == set(stream_jars.COORDINATES)
    for entry in entries:
        assert re.fullmatch(r"[0-9a-f]{40}", entry["sha1"]), entry["coordinate"]
        assert "sha1" in entry["cross_checked"], entry["coordinate"]
        assert set(entry["cross_checked"]) <= {"sha1", "sha256", "sha512"}
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", entry["locked_at"]), entry["coordinate"]


def test_spark_manages_nothing_outside_the_lake_root() -> None:
    """A managed table created without a path lands under the lake root's `_warehouse`, never in a
    `spark-warehouse` beside whichever directory a job started in, and no caller can redirect it."""
    assert "spark.sql.warehouse.dir" in RESERVED_KEYS


def test_the_local_delta_tuning_is_local_only_and_stays_overridable() -> None:
    """`LOCAL_DELTA_CONF` is a local-mode performance setting, not part of the toolchain
    contract: it must stay out of `BASE_CONF` and out of `RESERVED_KEYS`, so a cluster deployment
    can raise `snapshotPartitions` through `extra_conf` without touching the contract."""
    from trace_core.stream.session import BASE_CONF, LOCAL_DELTA_CONF, RESERVED_KEYS

    assert LOCAL_DELTA_CONF == {"spark.databricks.delta.snapshotPartitions": "4"}
    assert not set(LOCAL_DELTA_CONF) & set(BASE_CONF)
    assert not set(LOCAL_DELTA_CONF) & RESERVED_KEYS
