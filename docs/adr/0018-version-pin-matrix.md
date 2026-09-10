# ADR-0018: Version pin matrix for the JVM data stack

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 0

## Context
The reference machine has **Java 25 as the system default** with Temurin 17 also installed. Spark 4.0
supports **Java 17 and 21 only**. Running Spark on Java 25 produces an `UnsupportedClassVersionError`
that tells a developer nothing useful about the cause.

Similarly, Delta 4.0.1 with Spark 4.0.1 requires Hadoop 3.4.x; a mismatch surfaces as an opaque
`NoSuchMethodError` deep in a stack trace.

Separately, benchmark comparability depends on runtime stability. A metric that moved because Spark was
upgraded, without that being recorded, is a misleading result (ADR-0017).

## Decision
Pin, declare in `pyproject.toml` under `[tool.trace_x.pins]`, and **assert at runtime**:

| Component | Pin |
|---|---|
| Python | 3.12 |
| Java | **Temurin 17** |
| Spark | 4.0.1 |
| Delta | 4.0.1 |
| Hadoop | 3.4.x |
| Scala | 2.13 |

`make doctor` checks every pin **before any Spark job starts** and fails with an actionable message —
including the exact `export JAVA_HOME=$(/usr/libexec/java_home -v 17)` command on macOS.

Every pin is recorded in every run manifest. **Changing any pin invalidates prior benchmark
comparability and requires a manifest-diff note in the next evaluation report.**

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Document the requirement in a README | Readers skip READMEs, and the failure mode is an opaque JVM error hours later. The check must be executable |
| Pin only Spark and Java | Misses the Delta/Hadoop coupling, which produces the least debuggable failure of the three |
| Use whatever Java is default | Guarantees failure on this machine, and non-determinism on others |
| Java 21 instead of 17 | Also supported by Spark 4.0, but 17 is installed here and is the more widely used LTS in Databricks runtimes |
| Containerize Spark to avoid host Java entirely | Done for the `streaming` profile, but local PySpark development and tests still run against host Java |

## Consequences
**Positive.** The most confusing class of failure in the stack is caught by a preflight check with a
copy-pasteable fix. Benchmark comparability is protected by construction.
**Negative.** Pins age; upgrades become deliberate work rather than drift. Contributors must install a
specific JDK.
**Risks.** Pins becoming stale enough to block a needed dependency upgrade. Signal: a security advisory
against a pinned version. Mitigation: an upgrade is a normal change with a manifest-diff note.

## Status
Accepted
