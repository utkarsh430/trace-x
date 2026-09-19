# ADR-0045: The Phase 3 toolchain contract — hashed installs, verified JVM jars, and Java 17 enforced by the repository

- **Status:** Accepted
- **Date:** 2026-09-13
- **Phase:** 3
- **Supersedes / Superseded by:** Amends how ADR-0018 is *enforced*; the pins themselves (Python 3.12, Temurin 17, Spark 4.0.1, Delta 4.0.1, Hadoop 3.4.x, Scala 2.13) are unchanged. Implements Step 0 of `docs/PHASE3_PLAN.md`.

## Context

ADR-0018 says the pin matrix is "asserted at runtime" and that `make doctor` "checks every pin before
any Spark job starts". Phase 3 planning found that none of that was yet true, and that the gaps were of
the kind that report success:

- **The Java check could not fail.** It was warning-level in every phase, so `make doctor` exited
  green while a non-interactive shell — tooling, CI steps, any terminal that does not source the
  developer's profile — ran the system Java 25, on which Spark 4.0.1 does not start.
- **Hadoop and Scala were "declared", never asserted.** Nothing compared them with the jars pyspark
  actually ships, and a mismatch there surfaces much later as a `NoSuchMethodError`.
- **`requirements.lock` was installed by nothing.** `make setup` and every CI job resolved
  `pyproject.toml` ranges, so the hashed lock was documentation — and a comment in `lint.yml` said
  otherwise.
- **The `stream` extra was outside the lock, outside `make setup` and outside CI**, so Spark code could
  not be type-checked everywhere mypy runs, and a Spark test would have run only on machines where
  someone had installed pyspark by hand.
- **Delta and the Kafka connector are JVM artefacts.** The convenient path, `spark.jars.packages`
  (which `configure_spark_with_delta_pip` uses), resolves them from Maven at session start — over the
  network, with no content check, every time.
- **pyspark 4.0.1 is published only as a source archive.** Installing it builds a wheel, and under
  pip's default build isolation the build backend is fetched into a throwaway environment without hash
  checking, even in `--require-hashes` mode.
- **No CI workflow installed Java.**
- **The first real session on the reference machine failed to bind its driver:** the host name
  resolved to an address the process could not bind, and a local-mode driver binds to the host name
  unless told otherwise.

## Decision

**1. Every install is hash-checked.** `scripts/install_locked.sh` is how `make setup` and every CI job
install: `requirements-build.lock` (the build backends, hash-checked), then `requirements.lock` with
`--require-hashes --no-deps --no-build-isolation`, then the project itself editable with `--no-deps`,
then `pip check`. The gateway image installs `requirements-gateway.lock` the same way — the runtime
subset of `requirements.lock` (core dependencies plus the `db`, `api` and `obs` extras, closed for every
platform the image is built on), carrying the same versions and hashes, derived by
`scripts/runtime_lock.py` rather than resolved a second time, and checked for staleness by a unit test.
`pip-audit` in CI runs from the lock rather than being installed unpinned. `greenlet` is declared
directly (see Risks), and a test evaluates every locked dependency's markers for Linux x86_64, Linux
aarch64 and macOS arm64 so a platform-conditional omission fails the fast suite. The `stream` extra is
in the lock, and therefore everywhere. `pip-tools` joins the `dev` extra so a clean install can
regenerate the locks with `make lock`, which compiles both lockfiles and re-derives the gateway's.

The one component installed before any hash check is **pip itself**, as the Python distribution ships
it — the pip `python -m venv` bundles, the pip `actions/setup-python` provides, the pip inside the
digest-pinned base image. That bootstrap pip then installs the pinned pip recorded in the lock.

**2. JVM jars are pinned by content and never resolved at run time.** Six coordinates — Delta
(`delta-spark`, `delta-storage`), the Structured Streaming Kafka connector and its token provider,
`kafka-clients` 3.9.1 (the version Spark 4.0.1 builds against) and `commons-pool2` — are pinned by
size and SHA-256 in `packages/trace_core/stream/jars.lock`, shipped as package data. Only
`scripts/stream_jars.py lock` writes that file. It cross-checks every digest Maven Central publishes
beside each artefact — SHA-1 for all six, and SHA-256 and SHA-512 where they are published (for
`kafka-clients`; not for the Delta jars) — then records the SHA-256 it computed, which digests it
cross-checked, the SHA-1 anchor and the lock date. `make stream-jars` (run by `make setup`) fetches into a
per-user cache (`$TRACE_SPARK_JARS_DIR`, default `~/.cache/trace-x/spark-jars`) — outside the
repository, so every git worktree shares one verified download — and moves a file into place only
after its hash matches. Sessions put those local paths in `spark.jars`. Jars pyspark already bundles are not locked again, and a test
asserts that, because two copies of one class on a classpath load in an order no test should depend on.

**3. Java 17 is resolved and enforced by the repository.** The checks live once, in the stdlib-only
`trace_core.stream.toolchain`, imported by `make doctor` (before dependencies are guaranteed), by the
JDK resolver, by the jar CLI and by the session factory. The Makefile resolves `JAVA_HOME` through
`scripts/java_home.py`, which believes a JDK only after running it, and exports it to every target.
From Phase 3, `make doctor` makes Java, the installed pyspark and delta-spark versions, the Hadoop and
Scala versions read from pyspark's bundled jar names, and the verified jars **required** checks.

**4. `trace_core.stream.session.build_session` is the only way a Spark session is built.** Before any
JVM starts it checks the whole toolchain, reporting every failure with its fix in one error, and it
refuses the ways jars or configuration could reach the classpath from outside the contract: any
`extra_conf` key that adds jars or resolves them (`spark.jars*`, the driver and executor
`extraClassPath`) or that belongs to the contract, and the environment that does the same invisibly
(`PYSPARK_SUBMIT_ARGS` beyond `pyspark-shell`, `SPARK_CONF_DIR`, a `SPARK_HOME` that is not the pinned
pyspark). It sets the contract configuration (UTC session and JVM time zone, ANSI mode, the RocksDB
state store with changelog checkpointing, Delta's extension and catalog), binds `local[...]` drivers to
loopback, and refuses to adopt an already-active session, whose static settings would silently win.
After launch it asks the running JVM for its Spark, Java, Scala and Hadoop versions, confirms that
`spark.jars.packages` is unset, and confirms that Delta's session extension and the Kafka source
provider were loaded **from the verified jar files**, stopping the session and refusing it otherwise.
The Java *major* version is enforced; the vendor is recorded for run manifests rather than enforced,
because any OpenJDK 17 build runs Spark 4.0.1 identically and Temurin is the pinned, tested distribution.

**5. Spark is wired into CI, non-vacuously — pending its first recorded green run on GitHub, which this
ADR does not claim.** Tests needing the JVM carry the `stream` marker and are
excluded from the fast selector, which is asserted to be one expression in the Makefile,
`scripts/verify.sh` and `test-fast.yml`. `test-stream.yml` installs Temurin 17 with `actions/setup-java`,
restores the jar cache keyed by the lock's digest (and re-verifies every restored byte), and runs
`pytest -m stream`. A `-m stream` session in which no stream test actually executed **fails**: skips
are right for a developer without a JDK and wrong as evidence.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| `spark.jars.packages` / `configure_spark_with_delta_pip` | Resolves jars from Maven at every session start, over the network, unverified — the exact supply-chain gap the Python lock closes |
| Commit the jars to the repository | Binaries in git, a large permanent history cost, and a second copy per worktree; a content-pinned cache gives the same guarantee without either |
| Keep a separate lock or extra for Spark and install it only where Spark runs | mypy's scope imports pyspark, and a Spark test installed only on some machines is a Spark test that silently does not run elsewhere |
| Run Spark tests only inside a container image | A slower inner loop, and the host toolchain would still need checking for doctor and local jobs; a container remains the plan for the long-running stream service, not for tests |
| Have the session factory pick a JDK 17 by itself when `JAVA_HOME` is wrong | Hides a misconfiguration inside library code; the Makefile's resolution is one visible line, and the factory refuses rather than guesses |
| Accept Java 21 as well, since Spark 4.0.1 supports it | The pin is a single version so benchmark manifests stay comparable (ADR-0018); supporting two would need its own decision |
| Keep build isolation for the pyspark source build | pip does not hash-check an isolated build environment's backend, which would leave one unverified dependency in a hashed install |
| Verify the publishers' PGP signatures (`.asc`) for the jars | A genuine improvement, not rejected on merit: it would anchor trust to the release managers' keys instead of to what Maven Central served on the lock date, and signatures exist even for the Delta jars that publish no SHA-256. **Deferred**, because it needs `gpg` and a maintained keyring of Apache and Delta release keys that the project does not yet carry |
| Install the full lock into the gateway image | Puts pyspark and the development tooling into the hot-path image; the derived runtime lock gives the same hashes and versions for only what the gateway imports |

## Consequences

**Positive.** Every environment installs hash-verified Python dependencies at identical versions, and the
same verified jars. (The inputs are identical; the pyspark wheel is built locally from its verified
source archive, so that one built artefact is not byte-identical across machines.)
A wrong Java, a drifted pyspark or Delta, an unexpected Hadoop or Scala, or a substituted jar is
refused before a JVM starts, with the command that fixes it. `make doctor` can now fail on the
toolchain, so "green" means something. CI executes Spark rather than skipping it.

**Negative.** Every developer machine and every CI job now installs pyspark, whose source archive is
large and has to be built into a wheel on first install. The first `make setup` needs network access
to Maven Central as well as PyPI. There are two lockfiles to regenerate instead of one. The jar cache
lives outside the repository, so clearing a home directory means refetching. For five of the six jars Maven
Central publishes only a SHA-1, so their first pin is trust-on-first-use anchored to that SHA-1 as
served on the lock date (recorded in the lock); `kafka-clients` is additionally cross-checked against
its published SHA-256 and SHA-512. Every later fetch is held to the recorded SHA-256. The Makefile's JDK resolution runs a small script on
every `make` invocation. Pinning one JDK refuses Java 21 even though Spark supports it.

**Risks.** The lock is compiled on macOS, and pip-compile resolves only for the machine it runs on. That
risk materialised while this decision was implemented: SQLAlchemy requires `greenlet` on x86_64 and
aarch64 Linux but not on arm64 macOS, so the first lock omitted it and a hashed install in a Linux
container failed `pip check` — the failure every CI job would have hit. `greenlet` is now declared
directly, and `test_the_lock_is_complete_on_every_platform_it_is_installed_on` evaluates every locked
package's own dependency markers for Linux x86_64, Linux aarch64 and macOS arm64 offline, so a
platform-conditional dependency the lock is missing fails the fast suite rather than CI. A new platform
may still fail to build the pyspark wheel; that fails loudly at install time, which is the direction this
ADR chooses.

## Status

Accepted
