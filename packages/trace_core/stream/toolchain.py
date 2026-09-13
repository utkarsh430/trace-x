"""The Phase 3 JVM toolchain, checked before any Spark session exists.

**Why this is not left to Spark.** Spark 4.0.1 runs on Java 17 or 21 only, and
Delta 4.0.1 needs Hadoop 3.4.x under Scala 2.13 (ADR-0018). Every mismatch among
them surfaces late and opaquely -- `UnsupportedClassVersionError` from the launcher,
`NoSuchMethodError` from inside a query -- usually far from its cause. So the pins
are asserted here, first, with a message naming the component and the fix.

**Stdlib only, on purpose.** `make doctor` imports this module before dependencies
are guaranteed to be installed, and a check that needs the thing it checks for cannot
report that thing missing. pyspark is located through `importlib`, never imported.

**Jars are verified, never resolved.** Delta and the Kafka connector are JVM
artefacts. Letting Spark fetch them with `spark.jars.packages` would resolve them
from Maven at session start, over the network, with no content check -- exactly the
kind of unpinned, unverified dependency `requirements.lock` forbids on the Python
side. They are pinned in `jars.lock` beside this module by coordinate, size and
SHA-256, fetched by `scripts/stream_jars.py`, and checked byte for byte before a
session may put them on its classpath.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import json
import os
import platform
import re
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

JAVA_MAJOR: Final = "17"
SPARK_VERSION: Final = "4.0.1"
DELTA_VERSION: Final = "4.0.1"
HADOOP_LINE: Final = "3.4"
SCALA_LINE: Final = "2.13"
"""Mirrored from `[tool.trace_x.pins]` rather than read from `pyproject.toml` at run
time, because a deployed artefact does not ship the project file. A unit test diffs
the two, so neither can move alone."""

LOCK_PATH: Final = Path(__file__).with_name("jars.lock")
JARS_DIR_ENV: Final = "TRACE_SPARK_JARS_DIR"
MAVEN_CENTRAL: Final = "https://repo1.maven.org/maven2"

_JAVA_VERSION: Final = re.compile(r'version "(\d+)(?:\.(\d+))?')

_BUNDLED: Final[dict[str, tuple[str, str]]] = {
    "hadoop": ("hadoop-client-api", HADOOP_LINE),
    "scala": ("scala-library", SCALA_LINE),
}
"""Components pyspark ships inside its own `jars/` directory, asserted from the jar
file names rather than trusted from the pyspark version alone."""


@dataclass(frozen=True, slots=True)
class LockedJar:
    """One JVM artefact, pinned by content."""

    coordinate: str
    """`group:artifact:version`."""
    sha256: str
    size: int

    @property
    def group(self) -> str:
        return self.coordinate.split(":")[0]

    @property
    def artifact(self) -> str:
        return self.coordinate.split(":")[1]

    @property
    def version(self) -> str:
        return self.coordinate.split(":")[2]

    @property
    def filename(self) -> str:
        return f"{self.artifact}-{self.version}.jar"

    @property
    def url(self) -> str:
        group_path = self.group.replace(".", "/")
        return f"{MAVEN_CENTRAL}/{group_path}/{self.artifact}/{self.version}/{self.filename}"


@dataclass(frozen=True, slots=True)
class Finding:
    """The outcome of one toolchain check. `remedy` is empty exactly when `ok`."""

    component: str
    ok: bool
    detail: str
    remedy: str = ""


def load_lock(path: Path = LOCK_PATH) -> tuple[LockedJar, ...]:
    data = json.loads(path.read_text())
    return tuple(
        LockedJar(coordinate=str(j["coordinate"]), sha256=str(j["sha256"]), size=int(j["size"]))
        for j in data["jars"]
    )


def jars_dir(environ: Mapping[str, str] | None = None) -> Path:
    """Where verified jars live: `$TRACE_SPARK_JARS_DIR`, else a per-user cache.

    Outside the repository so every git worktree shares one verified download, and
    so a dataset-shaped directory of binaries never sits next to committed code.
    """
    env = os.environ if environ is None else environ
    configured = env.get(JARS_DIR_ENV)
    if configured:
        return Path(configured)
    cache_home = env.get("XDG_CACHE_HOME")
    base = Path(cache_home) if cache_home else Path.home() / ".cache"
    return base / "trace-x" / "spark-jars"


# ---------------------------------------------------------------------- java ---


def parse_java_major(version_output: str) -> str | None:
    """`openjdk version "17.0.18"` -> "17"; the legacy `"1.8.0_392"` form -> "8"."""
    match = _JAVA_VERSION.search(version_output)
    if match is None:
        return None
    first, second = match.group(1), match.group(2)
    if first == "1" and second is not None:
        return second
    return first


def java_executable(environ: Mapping[str, str] | None = None) -> Path | None:
    """The `java` Spark's launcher will actually run.

    `$JAVA_HOME/bin/java` when `JAVA_HOME` is set, otherwise the first `java` on
    `PATH` -- the same rule as pyspark's `bin/spark-class`, so this check and the
    real launch cannot disagree about which JVM is meant.
    """
    env = os.environ if environ is None else environ
    home = env.get("JAVA_HOME")
    if home:
        return Path(home) / "bin" / "java"
    found = shutil.which("java", path=env.get("PATH"))
    return Path(found) if found else None


def java_major(executable: Path) -> str | None:
    try:
        result = subprocess.run(  # noqa: S603 -- a fixed argv against a resolved JDK path
            [str(executable), "-version"], capture_output=True, text=True, timeout=30, check=False
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return parse_java_major(result.stderr + result.stdout)


def _java_remedy(found: str | None) -> str:
    why = f"Spark {SPARK_VERSION} runs on Java 17 or 21 only"
    if found is not None:
        why += f", and Java {found} fails with an opaque launcher error"
    if platform.system() == "Darwin":
        how = f"export JAVA_HOME=$(/usr/libexec/java_home -v {JAVA_MAJOR})"
    else:
        how = f"point JAVA_HOME at a Temurin {JAVA_MAJOR} JDK"
    return f"{why}. Fix: {how}. `make` targets select Temurin {JAVA_MAJOR} when one is installed."


def check_java(environ: Mapping[str, str] | None = None) -> Finding:
    exe = java_executable(environ)
    if exe is None:
        return Finding(
            "java", False, "no java: JAVA_HOME is unset and none is on PATH", _java_remedy(None)
        )
    major = java_major(exe)
    if major is None:
        return Finding("java", False, f"cannot determine the version of {exe}", _java_remedy(None))
    if major != JAVA_MAJOR:
        return Finding(
            "java",
            False,
            f"Java {major} at {exe}; the pin is Temurin {JAVA_MAJOR}",
            _java_remedy(major),
        )
    return Finding("java", True, f"Java {major} at {exe}")


def discover_java_home(environ: Mapping[str, str] | None = None) -> Path | None:
    """A JDK home whose major version is the pin, or None.

    Checked in order: `JAVA_HOME` itself; the `JAVA_HOME_<major>_<arch>` variables
    `actions/setup-java` exports; `/usr/libexec/java_home -v <major>` on macOS; the
    conventional `/usr/lib/jvm` layout on Linux. Every candidate is confirmed by
    running it -- a directory name is not a version.
    """
    env = os.environ if environ is None else environ
    candidates: list[Path] = []
    if env.get("JAVA_HOME"):
        candidates.append(Path(env["JAVA_HOME"]))
    for arch in ("X64", "ARM64", "AARCH64"):
        value = env.get(f"JAVA_HOME_{JAVA_MAJOR}_{arch}")
        if value:
            candidates.append(Path(value))
    if platform.system() == "Darwin" and Path("/usr/libexec/java_home").exists():
        try:
            probe = subprocess.run(  # noqa: S603 -- fixed macOS utility, fixed arguments
                ["/usr/libexec/java_home", "-v", JAVA_MAJOR],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            if probe.returncode == 0 and probe.stdout.strip():
                candidates.append(Path(probe.stdout.strip()))
        except (OSError, subprocess.SubprocessError):
            pass
    candidates.extend(sorted(Path("/usr/lib/jvm").glob(f"*{JAVA_MAJOR}*")))
    for home in candidates:
        exe = home / "bin" / "java"
        if exe.is_file() and java_major(exe) == JAVA_MAJOR:
            return home
    return None


# ----------------------------------------------------------- python packages ---


def check_distribution(distribution: str, want: str) -> Finding:
    try:
        got = importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return Finding(
            distribution,
            False,
            f"{distribution} is not installed (pin {want})",
            "Install from the hashed lock: `make setup`.",
        )
    if got != want:
        return Finding(
            distribution,
            False,
            f"{distribution} {got}; the pin is {want}",
            "Reinstall from requirements.lock (`make setup`). A drifted version invalidates "
            "benchmark comparability (ADR-0017, ADR-0018).",
        )
    return Finding(distribution, True, f"{distribution} {got}")


def pyspark_jars_dir() -> Path | None:
    """pyspark's bundled `jars/` directory, located without importing pyspark."""
    spec = importlib.util.find_spec("pyspark")
    if spec is None or not spec.submodule_search_locations:
        return None
    return Path(next(iter(spec.submodule_search_locations))) / "jars"


def check_bundled(component: str, jars: Path | None) -> Finding:
    artifact, line = _BUNDLED[component]
    if jars is None or not jars.is_dir():
        return Finding(
            component,
            False,
            f"pyspark is not installed, so its bundled {artifact} cannot be inspected",
            "Install from the hashed lock: `make setup`.",
        )
    prefix = f"{artifact}-"
    versions = sorted(p.name[len(prefix) : -len(".jar")] for p in jars.glob(f"{prefix}*.jar"))
    if len(versions) != 1:
        return Finding(
            component,
            False,
            f"expected exactly one {artifact} jar in {jars}, found {versions or 'none'}",
            f"Reinstall pyspark {SPARK_VERSION} from requirements.lock.",
        )
    version = versions[0]
    if version != line and not version.startswith(f"{line}."):
        return Finding(
            component,
            False,
            f"{artifact} {version}; the pin is {line}.x",
            f"A {component} mismatch surfaces later as NoSuchMethodError. Reinstall pyspark "
            f"{SPARK_VERSION} from requirements.lock.",
        )
    return Finding(component, True, f"{artifact} {version}")


# ---------------------------------------------------------------------- jars ---


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def check_locked_jars(
    lock: Sequence[LockedJar] | None = None, directory: Path | None = None
) -> list[Finding]:
    jars = load_lock() if lock is None else lock
    where = jars_dir() if directory is None else directory
    findings: list[Finding] = []
    for jar in jars:
        path = where / jar.filename
        component = f"jar:{jar.artifact}"
        if not path.is_file():
            findings.append(
                Finding(
                    component,
                    False,
                    f"{jar.coordinate} is missing from {where}",
                    "Run `make stream-jars`.",
                )
            )
            continue
        size = path.stat().st_size
        digest = sha256_of(path)
        if size != jar.size or digest != jar.sha256:
            findings.append(
                Finding(
                    component,
                    False,
                    f"{path.name} does not match jars.lock (sha256 {digest[:16]}..., {size} bytes)",
                    "Delete the file and run `make stream-jars`. Never edit jars.lock to match a "
                    "downloaded file -- that turns a detected substitution into an accepted one.",
                )
            )
            continue
        findings.append(Finding(component, True, f"{jar.coordinate} verified"))
    return findings


def inspect_toolchain(environ: Mapping[str, str] | None = None) -> list[Finding]:
    """Every Phase 3 toolchain check, in the order a reader would fix them."""
    jars = pyspark_jars_dir()
    return [
        check_java(environ),
        check_distribution("pyspark", SPARK_VERSION),
        check_distribution("delta-spark", DELTA_VERSION),
        check_bundled("hadoop", jars),
        check_bundled("scala", jars),
        *check_locked_jars(directory=jars_dir(environ)),
    ]


def locked_jar_paths(environ: Mapping[str, str] | None = None) -> tuple[Path, ...]:
    """The classpath entries a session uses. Callers must have verified them first."""
    where = jars_dir(environ)
    return tuple(where / jar.filename for jar in load_lock())


__all__ = [
    "DELTA_VERSION",
    "HADOOP_LINE",
    "JARS_DIR_ENV",
    "JAVA_MAJOR",
    "LOCK_PATH",
    "SCALA_LINE",
    "SPARK_VERSION",
    "Finding",
    "LockedJar",
    "check_bundled",
    "check_distribution",
    "check_java",
    "check_locked_jars",
    "discover_java_home",
    "inspect_toolchain",
    "jars_dir",
    "java_executable",
    "java_major",
    "load_lock",
    "locked_jar_paths",
    "parse_java_major",
    "pyspark_jars_dir",
    "sha256_of",
]
