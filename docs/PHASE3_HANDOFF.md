# Phase 3 — Operational Handoff

> **What this is.** The continuation document for the next Claude Code session: the state of Phase 3,
> the evidence behind it, and the ordered next actions. It is **not** an architecture document —
> decisions live in the ADRs and `docs/PHASE3_PLAN.md`, and step-by-step history lives in
> `docs/PROGRESS.md` (*WORK IN PROGRESS*). Where those are canonical, this file points to them.
> Read order for a cold session: `CLAUDE.md` → **this file** → `docs/PROGRESS.md` → `docs/ROADMAP.md`
> § Phase 3 → `docs/PHASE3_PLAN.md`.

| | |
|---|---|
| Branch | `phase/03-stream-medallion` |
| Phase | 3 — Streaming & Medallion (mandatory), in progress since 2026-09-13 |
| Handoff written | 2026-09-18 |
| Handoff reason | The interim machine (h3noyce) has no usable container runtime, so nothing Docker-dependent could run and no commit could be gated on a green `make verify` (§4) |
| Previous environments | the original macOS laptop (memory-starved; see PROGRESS's 2026-09-16 history), then **h3noyce**, a shared Ubuntu 22.04 lab host with no root |
| Next environment | a **Mac Pro with Docker available** |
| Last green commit | `aca29b0` "Phase 3: handoff for the move to a new machine" (gated on `make verify` 11/11 on the old laptop) |
| Last commit with acceptance evidence | `4fc23de` (`tests/acceptance/status.json` `last_verified_commit`) |
| Working tree at handoff | Clean. Step 11 (`684eef9`) plus the h3noyce edits is preserved, unvalidated, on branch **`phase3-step11-integration-prep`** (§3); nothing remains only on h3noyce |

> **Progress since this handoff (2026-09-18, on the Mac).** §7 A is done: a clean TraceX Docker
> environment was rebuilt from the Makefile, `make doctor` PASS, `make verify` 11/0/0 on `c4912d6`.
> §7 B step 6 is done: the Docker-backed runs found one Step 11 defect (the retention floor key refused
> the `+`/`/` that confluent-kafka puts in about half of all topic ids), fixed, and Step 11 is landed on
> `phase/03-stream-medallion` with a hydration-versus-floor test. Details, evidence and new debt:
> `docs/PROGRESS.md`, *Phase 3 — Mac validation and Step 11 landed*. **Not true as written above:** the
> "Mac Pro" is the same Mac where a session on 2026-09-16/17 made four commits that were never pushed
> (`075f713`, `b5c830a`, `a1403ca`, `471d8ba`); they are preserved on the local-only branch
> `backup/mac-phase3-471d8ba` and were **not** used as evidence. Since then: §7 step 7 done,
> `P3.checkpoint-resume` and `P3.medallion` PASS, D20 fixed. The live next list is in PROGRESS.

---

## 0. Standing user instructions for the rest of Phase 3

The operating model (ownership, escalation, failure classes, reports, heavy runs) is `CLAUDE.md` §18.
In addition, the user set a **delivery priority** for Phase 3:
- The architecture is settled: ADRs 0044–0057 are the baseline. Do not re-derive or re-open accepted
  or user-decided choices, and write a new ADR only if something cannot be built without one.
- No capabilities, tests or analysis beyond the Phase 3 exit criteria; no speculative investigation.
  Diagnostic autonomy (`CLAUDE.md` §8 of the accuracy section) applies to failures actually hit.
  Off-critical-path ideas go to `docs/PROGRESS.md` as debt.
- Cut scope and speculation, never rigour: correctness, evidence integrity, security boundaries and
  the verify gate stay as strict as `CLAUDE.md` requires.
- Report to the user when Phase 3 closes, before starting Phase 4.

## 1. Phase 3 in one paragraph

Phase 3 builds the warm path: Kafka (KRaft) topics, Bronze/Silver/Gold on Delta via Spark with
checkpointing, a durable gateway observation log whose gaps are detectable, precise feature
semantics, Redis reconstruction from the lake, a feature-parity harness, and two benchmarks
(stream throughput/outage recovery; Delta layout, which writes ADR-0015). The gate is
`docs/ROADMAP.md` § Phase 3; the approved plan, decisions (Q1–Q9, B1–B10), safeguards (§4) and step
order are `docs/PHASE3_PLAN.md`. **Exit rule (user decision 2026-09-15):** every REQUIRED Phase 3
capability PASS with executable evidence; `P3.eval-v2` is tracked, **non-gating**, and its FAIL is
never converted to PASS/SKIP.

## 2. Status by step and capability

Capability status is copied from `tests/acceptance/status.json` at `aca29b0`; that file is
authoritative. "Evidence" names where the proof is recorded.

| Step | What | State | Capability → status |
|---|---|---|---|
| 0 | Toolchain contract (ADR-0045, Accepted) | done | `P3.pin-failfast` **PASS** (2026-09-13) |
| 1 | Feature semantics + online-store correctness (ADR-0046) | done | `P3.semantics-hardening` **PASS** (2026-09-14) |
| 2 | Kafka platform, declared topics (ADR-0047) | done | part of `P3.kafka-ingest` |
| 3 | Delta spike, lake conventions (ADR-0048) | done | — |
| E | `eval-v2` dataset (ADR-0050) | closed for Phase 3 | `P3.eval-v2` **FAIL, non-gating — leave as is** |
| 4 | Gateway observation log `tx.scored.v1` (ADR-0051) | done | `P3.observation-log` **PASS** (2026-09-15) |
| 5 | Bronze (ADR-0052) | done | `P3.kafka-ingest` **PASS** (2026-09-15) |
| 6 | Silver (ADR-0053) | done | `P3.event-time` **PASS** (2026-09-15) |
| 7 | Gold batch build (ADR-0055) | done | feeds `P3.medallion` |
| 8 | Parity framework (ADR-0056) | built, **no measured run** | `P3.feature-parity` NOT_STARTED |
| 9 | Redis reconstruction (ADR-0057) | done | `P3.redis-hydration` **PASS** (2026-09-16, `4fc23de`) |
| 10 | Checkpoint/resume under fault injection | built; 7/7 passed only in five `-k` splits | `P3.checkpoint-resume` NOT_STARTED (needs one-session run) |
| 11 | Lake maintenance + resource guards (ADR-0052 Amendment 1) | implemented at `684eef9`, **not integrated** (§3) | `P3.resource-bounds` NOT_STARTED |
| 12 | Memory model + bounded score-time reads (ADR-0054, ADR-0046 §8) | in progress: one item left (§7 step 11) | — |
| 13 | Stream throughput + outage benchmark | **not started — no harness exists** | `P3.stream-throughput` NOT_STARTED |
| 14 | Delta layout benchmark + ADR-0015 | **not started — `benchmarks/delta_layout/` does not exist** | `P3.layout-benchmark` NOT_STARTED |
| 15 | Databricks strategy (documented, not deployed) | done (`6166c1b`) | — |
| — | Medallion end-to-end | tables built by Steps 5–7 | `P3.medallion` NOT_STARTED — its command names a test that does not exist |

Exit-checklist items outside the capability table (`docs/PHASE3_PLAN.md` §8):
- Silver tokenization claim removed — **done** (`docs/DATA_ENGINEERING.md`, the `trust_tier`
  paragraph: "No PII tokenization is performed or claimed").
- Local Kafka byte caps / lake retention documented as local-development overrides — **done**
  (`docs/DATA_ENGINEERING.md`, Q8 paragraph).
- Phase 2 load gate re-met after hot-path changes — **open** (Step 12's last item).
- Redis, Kafka and Spark tests running in CI with nothing skipped — workflows configured
  (`.github/workflows/test-integration.yml`, `test-stream.yml`), **never observed running on GitHub**.
- Final adversarial review — one was run on 2026-09-16 over Steps 6–10 (PROGRESS entry); the
  phase-exit review is still required after Steps 11–14.
- ADRs 0046–0057 are all **Proposed**; ADR-0015 becomes Accepted only on Phase 3 exit citing committed
  benchmark output. Acceptance is a user decision.

## 3. Step 11 work at handoff: branch `phase3-step11-integration-prep`

**Source:** `origin/phase3-step11-maintenance` = `684eef9`, a labelled UNVERIFIED WIP snapshot based
on `5cba28a` (an ancestor of `aca29b0`). Its commit message lists what was proven and what was not;
in brief, the agent reported (on the old laptop, not re-verified): 44 unit/integration tests for
`-k resource_bounds`, 14 `resource_bounds` stream tests, 9 Bronze+Silver Kafka regression tests, the
full unit suite with 1 loud skip, ruff and mypy clean; not closed were the full stream regression
(debt item 4, now run on h3noyce, §5) and Amendment 1's *Risks* items 1–3. Carry this list into the
Step 11 commit message, since the branch is deleted afterwards.
On h3noyce it was applied with `git merge --squash origin/phase3-step11-maintenance` (clean: 19 files,
+5067/−69) and not committed to this branch, because `make verify` cannot be green there (§4).
It is preserved as one **unvalidated WIP commit** on `origin/phase3-step11-integration-prep`,
branched from the documentation commit that carries this file, so its diff against
`phase/03-stream-medallion` is exactly the Step 11 content plus the changes below.

Four changes were made on top of `684eef9` and are in that WIP commit:
1. `packages/trace_core/stream/maintenance.py` (the retention `DELETE` in `_delete`): appended
   `# nosec B608` beside the existing `# noqa: S608`. **Why:** `make verify`'s bandit gate failed on
   the staged tree (B608, f-string SQL) — `684eef9` never ran bandit. Classified **D** after checking
   that every interpolated value is validated (`AppId.parse` for checkpoint ids;
   `retention_floor_key`'s `[A-Za-z0-9+/_-]{1,64}` for topic ids (widened on the Mac, see PROGRESS); ints for partition, offset, batch id;
   identifier from the table registry). Bandit and `ruff format --check . && ruff check .` then clean.
2. `docs/adr/0053-silver-canonical-events.md` §7: the "Starting from version 0" limit rewritten as
   amended by ADR-0052 Amendment 1 point 6 (after retention a new Silver checkpoint starts at
   `tables.retention_start`; version 0 is then refused; a Silver checkpoint must exist before any floor
   advances; `ignoreDeletes` only on Silver's Bronze reader).
3. `docs/adr/README.md`: the 0052 row names Amendment 1 (status "Proposed (Amendment 1 Proposed)");
   the 0053 row notes §7 amended by 0052 Amendment 1.

4. `spikes/step11-maintenance/run_spike.sh`: `684eef9` hard-coded the original laptop's absolute
   paths (a home directory, worktree paths, a session scratchpad, the macOS-only `java_home` call).
   Made portable: the repository is found from the script's location, the interpreter defaults to
   the repository venv (`VPY` overrides), the JDK comes from `scripts/java_home.py` (the Makefile's
   resolver), `.env` from the repository root, and the lock and lake root live under
   `TRACE_SCRATCH_DIR` (default `${TMPDIR:-/tmp}/trace-x-scratch`). The spike's own semantics and
   arguments are unchanged. The runner was not executed after the change.

**Already found:** the "known class-C failure" in `684eef9`'s message
(`test_bronze_tables.py::test_coverage_rows_read_in_spark_equal_the_rows_that_were_written`) is
**already fixed inside `684eef9`** — the eight expected `BronzeRecord`s carry `TOPIC_ID`. The message
was stale. The test passed on h3noyce.

## 4. Why h3noyce could not finish (the Docker blocker)

h3noyce: Ubuntu 22.04, 16 CPU, 125 GB RAM, NFS home. Checked on 2026-09-17:
- no `sudo` (password required, not granted);
- `docker`, `podman`, `apptainer`/`singularity` not installed;
- rootless Docker/Podman prerequisites absent: no `/etc/subuid`/`/etc/subgid` entries for the user,
  no `newuidmap`/`newgidmap` (`uidmap`), no `slirp4netns`/`fuse-overlayfs`/`rootlesskit`;
- a single-uid user namespace cannot run the images (postgres drops to uid 999 via `gosu`;
  `apache/kafka` runs as uid 1000).

Consequences: `make doctor` FAILs its required `docker` check, so `make verify` is 10/11 and no commit
could be gated green; and every test that drives the `docker` CLI could not run — the Kafka fixture
(`tests/integration/test_kafka_platform.py::broker` starts throwaway `apache/kafka` containers from
the compose spec), the chaos suites (`docker kill/start/pause`), ground-truth isolation, parity and
hydration. **On the Mac Pro, validate the existing Phase 3 implementation through the project's real
Docker/Compose workflow before any new architectural work.**

## 5. Evidence gathered on h3noyce (2026-09-17/18)

Run with user-space Temurin 17.0.20.1 and CPython 3.12.14; hashed lock installed by `make setup`;
6/6 Spark jars verified. Logs were kept outside the repository and are not committed; these numbers
are session evidence, not acceptance evidence, and nothing was recorded in `status.json`.

| Command | Tree | Result |
|---|---|---|
| `make doctor` | `aca29b0` | every pin PASS (python 3.12, java 17, pyspark 4.0.1, delta-spark 4.0.1, hadoop-client-api 3.4.1, scala-library 2.13.16, jars); **FAIL `docker` not installed** |
| `make verify` | `aca29b0` | 10 passed, **1 failed (doctor)**, 0 skipped |
| `make verify` | staged Step 11, before edit 1 | 9 passed, 2 failed (doctor; **bandit** B608) |
| bandit + `ruff format --check . && ruff check .` | staged Step 11 + edit 1 | clean |
| `pytest -m stream <file>`, one JVM per file | staged Step 11 | Bronze 5, Delta capabilities 42, semantics-Gold 82, **Gold 7 (372 s, no GC trouble)**, resource_bounds 14, session 8 + 1 loud skip, worker 1, Silver 12 — **171 passed, 0 failed**; 2,104 s total |
| `pytest -m stream tests/stream/test_session.py` with a second real JDK exposed as `JAVA_HOME_11_X64` | staged Step 11 | **9 passed, 0 skipped** (the skip above needs a non-pin JDK to refuse) |

This closes `684eef9`'s debt item 4 (full stream regression, split by file) for the tree's code, but
the Kafka-backed Bronze/Silver/resource-bounds **integration** tests and the hydration re-run were not
run (no Docker). `make verify` after the Step 11 edits was **not** re-run to completion with doctor
green; it must be on the Mac.

## 6. Validation matrix

| Category | Items |
|---|---|
| Implemented AND tested with acceptance evidence | Steps 0, 1, 4, 5, 6, 9 (`P3.pin-failfast`, `P3.semantics-hardening`, `P3.observation-log`, `P3.kafka-ingest`, `P3.event-time`, `P3.redis-hydration`) |
| Implemented, tested, evidence not yet acceptable | Step 10 (7/7 only in splits); Step 8 (framework tested — 144 parity unit/mutation tests, stream and end-to-end runs in the lead worktree — but no measured run on a clean commit) |
| Implemented; unit + stream tested on h3noyce; **integration blocked by Docker** | Step 11 (§3, §5) |
| Implemented, **never run on GitHub** | CI provisioning for service-backed tests (`test-integration.yml`, `test-stream.yml`, the CI skip guard) — proven by a CI-equivalent local run only |
| In progress | Step 12: the Phase 2 load gate re-run with the score-time cap on a quiet machine |
| Not implemented | Step 13 harness and `make load-stream` (named by `status.json`, **absent from the Makefile**); Step 14 `benchmarks/delta_layout/` and `make bench-layout` (still a phase-guard stub); ADR-0015's decision |

## 7. Ordered continuation checklist (Mac Pro)

Use the repository's commands; `docs/LOCAL_DEVELOPMENT.md` is the bootstrap authority.

**A. Environment**
1. Install Python 3.12, **Temurin 17** (not 21/25; `/usr/libexec/java_home -v 17` must resolve),
   Docker Desktop, git. Give Docker Desktop at least the RAM `docs/LOCAL_DEVELOPMENT.md` §1 names
   (≥ 8 GB for the full profile). **Spark runs in a host JVM, not in Docker** — leave host memory for
   it. The stream session's driver heap is fixed at `1g` (`packages/trace_core/stream/session.py`),
   so more machine RAM does not make a monolithic `-m stream tests/stream` run safe; split by file.
2. Clone, `git checkout phase/03-stream-medallion`, confirm the tip and a clean tree.
3. Create `.env` from `.env.example` (gitignored; never commit). Generate every `*_PASSWORD` and
   `TRACE_SERVICE_TOKEN_LOCAL` randomly with the generator in
   `.github/workflows/test-integration.yml` (`secrets.token_hex`). Service-token secrets must be
   ≥ 32 characters or the gateway refuses to start.
4. `make setup` → `make doctor` (must be all PASS on the pin matrix and docker) → `make up`
   (core profile: postgres, redis, redis-cache, gateway; also applies migrations via `make migrate`).
   Kafka is **not** needed as a compose service for the tests: the Kafka-backed tests start their own
   throwaway brokers. `make up-streaming` + `make kafka-topics` are for manual runs and benchmarks.
   Before running `pytest` or `alembic` directly, load the environment: `set -a; . ./.env; set +a`.
   (Fixed 2026-09-18, D20: on fresh volumes the gateway used to stay unready until restarted; it now
   recovers by itself once `make migrate` creates `trace_app`.)
5. `make verify` — must be **11 passed, 0 failed, 0 skipped** before any other work. If red, classify
   (A–F, see `CLAUDE.md` §18) and fix the root cause first.

**B. Integrate Step 11** (recommended *before* checkpoint-resume: Step 11 changes
`stream/checkpoints.py` and `stream/silver.py`, which `tests/chaos/test_spark_resume.py` exercises, so a
resume PASS recorded before integration would describe superseded code)
6. Land the Step 11 work as **one** green commit on `phase/03-stream-medallion`:
   `git merge --squash origin/phase3-step11-integration-prep` (stages it, commits nothing; never a
   merge or cherry-pick of the WIP commit).
   Run `pytest -m 'unit or integration' -k resource_bounds` (the `P3.resource-bounds` command; needs
   Docker for its Kafka test), the Bronze and Silver Kafka integration tests
   (`tests/integration/test_bronze_kafka.py`, `test_silver_kafka.py`), and the hydration suite
   `pytest -m 'stream and integration' tests/integration/test_redis_hydration.py` — Amendment 1 §2 says
   a retired Bronze region is one gap and hydration refuses to claim before the floor, which closes
   Step 9's recorded debt; show the two agree. Optionally re-run the per-file stream split (§5).
   `make verify` green → commit, noting provenance (`684eef9` and its proven/unproven list). Record
   `P3.resource-bounds` only on executed evidence.
7. Only after that commit is on origin and verified: delete `origin/phase3-step11-integration-prep`,
   `origin/phase3-step11-maintenance`, and `origin/worktree-agent-aae9faa608636c9c8` (confirmed
   identical to `aca29b0` on 2026-09-17). Until then they hold the only copies of that work.

**C. Remaining acceptance runs** (heavy; one at a time on a quiet machine)
8. `P3.checkpoint-resume`: `pytest -m chaos tests/chaos/test_spark_resume.py` as **one** session, zero
   skips. Record only if it passes that way.
9. `P3.feature-parity`: `make parity`. ADR-0056 §5's declarations and
   `MAX_NOT_VOUCHED_FRACTION_PER_FEATURE` (0.5) are frozen — do not tune after seeing results. Record
   the run_ids. (`status.json` records `pytest -m parity`; `make parity` runs the same marker as two
   sessions — non-service parity tests, then `integration and stream and parity` with `.env` loaded.
   Record the command actually executed.)
10. `P3.medallion`: repoint its command (it names `tests/integration/test_medallion.py`, which does not
    exist) at the suites that actually produce and check every table — Bronze/Silver Kafka integration
    and the Gold-producing runs (`tests/integration/test_parity_run.py`, `tests/stream/test_gold_tables.py`)
    — then execute exactly that command.
11. Step 12's last item: the Phase 2 load gate (`make load-gateway`) re-run on a quiet machine with the
    score-time cap in place (PROGRESS, Step 12 "Still to do"; the 2026-09-15 attempt was invalid,
    classified E, nothing published).

**D. Not-started work**
12. Step 13 → `P3.stream-throughput`: build the stream load harness and the `make load-stream` target
    `status.json` already names. Targets (ROADMAP Phase 3): sustain ≥ 5k events/s; lag recovers
    < 10 s after a 2-minute outage offered at 5,000 events/s. The lag definition, trigger and clock
    rules are frozen in `docs/PHASE3_PLAN.md` §4.3 ("Consumer lag", "Gold freshness target") — do not
    redefine them. A missed target is recorded as a failure, never re-framed.
13. Step 14 → `P3.layout-benchmark`: create `benchmarks/delta_layout/` running the real Gold query mix
    at ≥ 10 M rows across partitioning / partition+Z-order / liquid `CLUSTER BY` plus a no-layout
    control, files- and bytes-scanned recorded; replace the `bench-layout` phase guard; write ADR-0015
    from the committed numbers ("the winner is whatever the numbers say").
14. Push and confirm the first GitHub runs of `test-integration` and `test-stream` (pushing is
    outward-facing: confirm with the user).

**E. Exit**
15. Every REQUIRED capability PASS in `status.json` with evidence; `P3.eval-v2` untouched.
16. The phase-exit adversarial review (`CLAUDE.md` §10 "Completion standard").
17. Update `docs/PROGRESS.md`, and report to the user **before** starting Phase 4. ADR acceptance
    (0046–0057, 0015) is the user's decision.

## 8. Unresolved issues and things to verify rather than assume

- **Heavy-suite lock is a session convention, not a repository tool.** Serialize Spark/Kafka/chaos/
  benchmark runs yourself (a `mkdir` lock with an owner file naming the pid; release only if it names
  your pid; an `EXIT` trap in a `zsh -c` wrapper does not fire on SIGTERM, so the pid check is what
  makes release safe). Background wrappers must propagate the real exit code (`rc=$?; …; exit $rc`).
- **`test_session.py`'s real-JDK refusal test skips** unless a second, non-pin JDK is discoverable
  (`JAVA_HOME_<major>_X64`/`ARM64` variables, or `/usr/libexec/java_home -V` on macOS). Install one
  (e.g. Temurin 21) if zero skips are required for that file.
- **Monolithic stream run:** untested since the 2026-09-16 GC death spiral; the 1 GB driver heap is a
  code default, so assume it still fails and split by file.
- **Step 12 load-gate hypothesis** (the 2026-09-15 dip was machine contention) is unconfirmed.
- **Debt carried in PROGRESS / ADRs** (unchanged here): `684eef9`'s ADR-0052 Amendment 1 *Risks* items
  1–3; D14 (CI service provisioning, configured but unobserved); D17 (unbounded `amount_minor`); D18
  (local `tx.scored.v1` per-partition cap); D19; `eval/replay/faults.py` cannot build a conflicting
  duplicate for `tx.authorization.v1`; Step 10's `-m "chaos and not stream"` false-failure form.
- **Open user decisions:** acceptance of ADRs 0046–0057 (all Proposed); ADR-0056 becomes Accepted only
  after the measured runs.
- **CI:** no GitHub Actions run of the Phase 3 workflows has been observed.

## 9. When this document is stale

Replace or delete this file once Steps 11–14 are committed and the capabilities above are recorded;
`docs/PROGRESS.md` remains the live state file.
