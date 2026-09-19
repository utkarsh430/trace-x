#!/usr/bin/env python3
"""Verified JVM dependencies for Spark: pinned coordinates, SHA-256, no runtime Maven.

    python scripts/stream_jars.py fetch    # download missing jars, verify every byte
    python scripts/stream_jars.py verify   # verify what is present; non-zero on any gap
    python scripts/stream_jars.py lock     # MAINTAINER: re-pin COORDINATES into jars.lock

`fetch` is what `make setup` and CI run. It never trusts a download: each file is
written to a temporary name, checked against the size and SHA-256 recorded in
`packages/trace_core/stream/jars.lock`, and only then moved into place.

`lock` is the only command that writes the lock. It downloads each coordinate and
cross-checks every digest Maven Central publishes beside it -- SHA-1 always, SHA-256
and SHA-512 where they exist (they do for kafka-clients, not for the Delta jars) --
then records the SHA-256 it computed, which digests it cross-checked, and the date.
Where only SHA-1 is published, the first lock is trust-on-first-use anchored to that
SHA-1 as served on the lock date; every later fetch is held to the recorded SHA-256.
Verifying the publishers' PGP signatures would anchor trust to the release managers
instead of to Maven Central, and is deferred (ADR-0045).

Why these six and no more: pyspark 4.0.1 already bundles Hadoop 3.4.1, Scala 2.13,
antlr4-runtime, jsr305, scala-parallel-collections and the compression and logging
libraries kafka-clients needs, at versions at least as new as kafka-clients declares.
Adding them again would put two copies of one class on the classpath, and which one
loads first is not something a test should have to depend on.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "packages"))

from trace_core.stream import toolchain  # noqa: E402 -- stdlib-only, importable before setup

COORDINATES: dict[str, str] = {
    "io.delta:delta-spark_2.13:4.0.1": "Delta Lake for Spark 4.0 and Scala 2.13 (the pin)",
    "io.delta:delta-storage:4.0.1": "delta-spark's LogStore; not bundled by Spark",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1": (
        "the Structured Streaming Kafka source and sink"
    ),
    "org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.1": (
        "a compile dependency of the Kafka connector"
    ),
    "org.apache.kafka:kafka-clients:3.9.1": (
        "the client Spark 4.0.1 builds against (its pom's kafka.version)"
    ),
    "org.apache.commons:commons-pool2:2.12.0": (
        "the connector's consumer pool; not bundled by Spark 4.0.1"
    ),
}

TIMEOUT_S = 60


def _download(url: str) -> bytes:
    if not url.startswith(toolchain.MAVEN_CENTRAL + "/"):
        raise SystemExit(f"refusing to download from outside Maven Central: {url}")
    with urllib.request.urlopen(url, timeout=TIMEOUT_S) as response:  # noqa: S310  # nosec B310 -- fixed https Maven Central prefix checked above
        return bytes(response.read())


def _published_digest(url: str) -> str | None:
    """The hex digest Maven Central publishes at `url`, or None if it publishes none."""
    try:
        return _download(url).decode().split()[0].strip().lower()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def cmd_lock() -> int:
    locked_at = dt.datetime.now(dt.UTC).date().isoformat()
    jars = []
    for coordinate, reason in COORDINATES.items():
        pinned = toolchain.LockedJar(coordinate=coordinate, sha256="", size=0)
        body = _download(pinned.url)
        computed = {
            "sha1": hashlib.sha1(body, usedforsecurity=False).hexdigest(),
            "sha256": hashlib.sha256(body).hexdigest(),
            "sha512": hashlib.sha512(body).hexdigest(),
        }
        cross_checked = []
        for algorithm, digest in computed.items():
            published = _published_digest(f"{pinned.url}.{algorithm}")
            if published is None:
                if algorithm == "sha1":
                    print(f"FAIL {coordinate}: Maven Central publishes no SHA-1 to anchor the pin")
                    return 1
                continue
            if published != digest:
                print(f"FAIL {coordinate}: {algorithm} {digest} != Maven Central's {published}")
                return 1
            cross_checked.append(algorithm)
        jars.append(
            {
                "coordinate": coordinate,
                "sha256": computed["sha256"],
                "size": len(body),
                "sha1": computed["sha1"],
                "cross_checked": cross_checked,
                "locked_at": locked_at,
                "reason": reason,
            }
        )
        print(f"  pinned {coordinate}  cross-checked {'+'.join(cross_checked)}  {len(body)} bytes")
    document = {
        "$comment": (
            "JVM artefacts Spark sessions put on their classpath, pinned by size and "
            "SHA-256. Written only by `scripts/stream_jars.py lock`, which cross-checks "
            "every digest Maven Central publishes (SHA-1 always; SHA-256 and SHA-512 where "
            "published) before recording the SHA-256 it computed. `cross_checked` records "
            "which were available. Sessions never resolve jars from Maven at run time "
            "(spark.jars.packages is refused)."
        ),
        "jars": jars,
    }
    toolchain.LOCK_PATH.write_text(json.dumps(document, indent=2) + "\n")
    print(f"wrote {toolchain.LOCK_PATH.relative_to(ROOT)}")
    return 0


def cmd_fetch() -> int:
    where = toolchain.jars_dir()
    where.mkdir(parents=True, exist_ok=True)
    failures = 0
    for jar in toolchain.load_lock():
        target = where / jar.filename
        if target.is_file() and toolchain.sha256_of(target) == jar.sha256:
            print(f"  ok      {jar.coordinate}")
            continue
        body = _download(jar.url)
        digest = hashlib.sha256(body).hexdigest()
        if digest != jar.sha256 or len(body) != jar.size:
            print(f"  REFUSED {jar.coordinate}: downloaded sha256 {digest} does not match the lock")
            failures += 1
            continue
        with tempfile.NamedTemporaryFile(dir=where, delete=False, suffix=".part") as handle:
            handle.write(body)
            partial = Path(handle.name)
        # NamedTemporaryFile creates 0600. A jar is not a secret, and a directory
        # mounted into a container runs as another user, which could not read it.
        partial.chmod(0o644)
        partial.replace(target)
        print(f"  fetched {jar.coordinate}")
    return 1 if failures else cmd_verify()


def cmd_verify() -> int:
    findings = toolchain.check_locked_jars()
    for finding in findings:
        print(f"  {'ok  ' if finding.ok else 'FAIL'} {finding.detail}")
        if not finding.ok:
            print(f"       {finding.remedy}")
    bad = [f for f in findings if not f.ok]
    print(
        f"{len(findings) - len(bad)}/{len(findings)} locked jars verified in {toolchain.jars_dir()}"
    )
    return 1 if bad else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("command", choices=("fetch", "verify", "lock"))
    args = parser.parse_args()
    return {"fetch": cmd_fetch, "verify": cmd_verify, "lock": cmd_lock}[args.command]()


if __name__ == "__main__":
    raise SystemExit(main())
