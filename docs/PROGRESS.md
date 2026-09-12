# TRACE-X — Progress Tracker

> **This is the live state file.** A future session must be able to recover the entire current state
> from this document plus git history. It records reality **including failures** — an over-optimistic
> PROGRESS.md is worse than none.
>
> Rules and invariants live in `CLAUDE.md`; they never appear here. Status never appears there.

---

## CURRENT PHASE

**Phase 0 — Foundation & Constitution** (mandatory)

Gate: `docs/ROADMAP.md` § Phase 0.

## CURRENT STATUS

**Phase 0 is COMPLETE. All 10 capabilities PASS. All five exit criteria are met and verified.**

`make verify` is 9/9 green locally, **and all four GitHub Actions workflows pass on the current
commit**. 257 tests pass (234 fast + 23 integration against real PostgreSQL).

Phase 1 has not begun and must not begin without explicit user approval.

### Recovery after an unexpected machine shutdown (2026-09-12)

The shutdown caused **no repository damage**. Verified, not assumed:

| Check | Result |
|---|---|
| `git fsck` | clean |
| Working tree vs `HEAD` | byte-identical; nothing partially written |
| Tracked files modified after the last commit | none |
| Zero-byte tracked files | only `eval/manifest/.gitkeep` (intentionally empty) |
| `.env` | complete, all four role passwords present |
| venv | intact; all 9 key modules import |
| Stray containers / volumes | none |

**The recovery pass found two real defects that predated the shutdown**, both now fixed with
regression tests — see COMPLETED WORK.

### Environment changes since the last session
- **Disk: 3.5 GB → ~28 GB free.** The `doctor` disk check that was failing now passes. The threshold
  was never lowered to force it green.
- **Git remote added and pushed** (`origin/main` = local `HEAD`). CI has therefore actually executed.
- Docker daemon was down after the shutdown; restarted for verification.

**No TRACE-X product functionality exists yet.** No transaction processing, ML, streaming, agents or
frontend code has been written, by instruction.

## COLD-START CHECKLIST (read this first in a new session)

```bash
make setup      # venv + dev/db/obs extras
make verify     # expect: 9 passed, 0 failed  -> VERIFY OK
make ci-status  # expect: all 4 workflows pass
```

If both pass, Phase 0 is intact and **Phase 1 may begin**. If either fails, fix that before any new
work — the failure is the current task.

| Question a cold session will ask | Answer |
|---|---|
| What phase are we in? | Phase 0 **complete**. Phase 1 not started. |
| What do I do next? | `docs/ROADMAP.md` § Phase 1 → **FIRST TASKS**, an ordered list of 8. |
| What is already built? | Control plane only: docs, ADRs, tooling, migrations, observability scaffold. **No product code.** |
| What must I never do? | `CLAUDE.md` §17, and §11 (ground-truth isolation) above all. |
| Do I need Docker / Java / an API key? | Not for Phase 1. Docker only to re-run the Phase 0 integration suite. |
| Where do I record results? | This file and `tests/acceptance/status.json` (which refuses `PASS` without evidence). |

**Known environment gaps — not blocking Phase 1:**
`JAVA_HOME` unset and system Java is 25 (Spark 4.0 needs Temurin 17) — blocks **Phase 3**.
Ollama not installed — blocks **Phase 6**. No AWS credentials — blocks **Phase 12**.
`gh` unauthenticated, so CI logs cannot be downloaded; failures are diagnosed by local reproduction.

---

## LAST VERIFIED COMMIT

`aaf6f28` — **Phase 0 handoff commit. Phase 0 is complete and verified at this tree.**

The stamp below is written by the commit that immediately follows and changes only this
file and `tests/acceptance/status.json` — no code path differs. Re-confirm in ~10 s with
`make verify`.

All results below were produced after the recovery fixes.

---

## COMPLETED WORK

### Defects found and fixed during shutdown recovery (2026-09-12)

**1. `make doctor` passed while the Docker daemon was down.** With the daemon unreachable,
`docker info --format` still renders the template against a zero-valued struct and prints `0|0|` on
stdout, sending the real error to stderr. `check_docker` tested only the output shape, so it reported a
healthy daemon and exited 0 — while `make up` would then fail confusingly, which is precisely what
doctor exists to prevent. Two compounding causes: `_run()` merged stdout with stderr and discarded the
return code, making a failed command indistinguishable from a successful one.

*Fixed:* `_run()` now returns `(returncode, stdout, stderr)` separately; `check_docker` treats a
non-zero code or an empty `ServerVersion` as a **required failure** with an actionable remedy, and
reports a socket permission error specifically. 13 new tests in `tests/unit/test_doctor.py` — the
doctor previously had none, which is why this survived.

**2. CI `lint` was failing on `mypy` while `make verify` passed locally.** mypy's scope was extended to
cover `migrations/` (which imports `alembic` and `sqlalchemy`), but neither `make setup` nor
`lint.yml` was updated to install the `db` extra; the lazily-imported OTLP exporter needed `obs`
likewise. Locally everything was already installed, so the drift was invisible.

**This was worse than a CI-only problem: `make setup` installed only `.[dev]`, so a fresh clone running
`make setup && make verify` would have failed mypy too — a direct violation of the Phase 0 exit
criteria.** Reproduced in a throwaway venv built exactly as CI builds one (6 errors, 3 files) rather
than diagnosed by inspection.

*Fixed:* `make setup` and `lint.yml` both install `.[dev,db,obs]`, matching the other workflows.
`tests/unit/test_toolchain_consistency.py` asserts that every extra mypy's declared scope needs is
installed by **both** `make setup` and every workflow that runs mypy, so this drift cannot recur
silently.

**3. CI `lint` failed again after fix 2 — on a step the first failure had masked.** With mypy fixed,
the job reached the *Secret scan* step, which had been skipped. Two defects there:

- `make secrets` and the CI step implemented the same control **two different ways**, and CI installed
  the tool with a bare `pip install detect-secrets` — **unpinned**, while `requirements.lock` pins
  1.5.0. A security gate whose behaviour depends on which machine runs it is not a gate.
- **Secret scanning was in CI but not in `make verify`**, so a local run could pass while CI went red.
  That is the structural reason this class of failure kept surprising us.

*Fixed:* one shared `scripts/secret_scan.py` called identically by `make secrets`, `make verify` and
CI, using the locked version and printing `file:line:type` so a failure explains itself without the CI
log. `.env` is excluded (it holds local credentials by design) and replaced by a stronger control that
asserts `.env` is gitignored and untracked. `requirements.lock` is excluded with a comment (1800+
sha256 wheel hashes, high entropy by design, public). 9 tests in `tests/unit/test_secret_scan.py`,
including a planted-credential test proving the scanner actually fails — one of which caught that the
test file itself must not embed a literal key.

`make verify` is now **9 gates**, up from 8.

### Control-plane documents (12)
`CLAUDE.md` (constitution) · `docs/ARCHITECTURE.md` · `docs/ROADMAP.md` · `docs/PROGRESS.md` ·
`docs/TESTING.md` · `docs/EVALUATION.md` · `docs/SECURITY.md` · `docs/API_CONTRACTS.md` ·
`docs/EVENT_CONTRACTS.md` · `docs/DATA_ENGINEERING.md` · `docs/LOCAL_DEVELOPMENT.md` ·
`docs/OPERATIONS.md`

### Architecture Decision Records (26 + template + index)
All major approved decisions recorded in `Context · Decision · Alternatives Considered · Consequences ·
Status` format. 25 Accepted; **ADR-0015 deliberately remains `Proposed`** until the Phase 3 Delta layout
benchmark produces committed evidence.

### Executable tooling
| Command | What it does | Verified |
|---|---|---|
| `make doctor` | Asserts the version pin matrix, disk, Docker RAM, ports, LLM tier | ✅ exit 0; correctly flags Java 25 vs required Temurin 17 |
| `make verify` | **Canonical health check** — doctor + acceptance + claims + lint + types + tests + bandit | ✅ see LAST VERIFICATION RESULTS |
| `make check-claims` | Benchmark-integrity linter enforcing all five publication rules | ✅ exit 0; caught a real ambiguous line in `ARCHITECTURE.md` |
| `make acceptance` | Machine-readable capability status | ✅ 58 capabilities tracked |
| `make setup` / `up` / `down` / `lint` / `typecheck` / `test-fast` / `test` | Standard workflow | ✅ |
| `seed`, `demo`, `eval`, `eval-external`, `e2e`, `bench-layout`, `fetch-external`, `pull-model` | Phase-gated | ✅ exit non-zero with an explicit phase message |

### Bootstrap environment
Python 3.12 venv, `pyproject.toml` with per-phase dependency extras (keeps bootstrap small on
constrained disk), ruff / mypy / pytest / bandit / detect-secrets configuration, the version pin matrix
declared once under `[tool.trace_x.pins]`, `.env.example` with working local defaults for every setting,
profiled `docker compose` (`core` / `streaming` / `graph` / `llm`) with per-service memory limits and
non-colliding ports (Postgres **5442**, Redis **6389**).

### Database layer — Alembic owns schemas, roles and grants
`migrations/versions/0001_schemas_roles_grants.py` is the **single source of truth**. The compose init
script that previously duplicated the grant logic was removed: the grants *are* the ground-truth
isolation control, and a control with two definitions can drift. A production deployment (RDS) has no
init script either, so local and cloud follow one path. Role passwords are bound as query parameters
and quoted server-side by `format(%L)` — no credential in any committed file.

**Executed against a live PostgreSQL 16 container:**

| Check | Result |
|---|---|
| `trace_app` / `trace_stream` / `trace_auditor` reading `groundtruth` | ❌ denied — `permission denied for schema groundtruth` |
| `trace_app` creating objects in `groundtruth` | ❌ denied |
| Ground-truth values leaking through error output | ❌ none |
| `trace_eval` reading `groundtruth` | ✅ permitted (the harness must measure) |
| `trace_app` reading `app` schema | ✅ permitted (isolation must not break the app) |
| `trace_app` INSERT into `audit` | ✅ permitted |
| `trace_app` SELECT / UPDATE / DELETE on `audit` | ❌ denied — append-only at the database level |
| `trace_app` `INSERT ... RETURNING` on `audit` | ❌ denied (RETURNING needs SELECT) — write-only is genuinely write-only |
| `trace_auditor` reading what the app appended | ✅ permitted |
| `alembic downgrade base` | ✅ removes all 5 schemas and all 4 roles (needs `DROP OWNED BY`) |
| `alembic upgrade head` after downgrade | ✅ restores everything |

This is the highest-severity control in the project: leakage would silently invalidate every metric.

### Observability scaffold
`configure_telemetry()` installs tracer and meter providers; `configure_logging()` installs a structlog
chain whose **last processor before the renderer** is PII redaction, so nothing can introduce PII after
it runs. Every log line inside a span carries `trace_id` and `span_id`. `carrier_inject`/`carrier_extract`
carry W3C trace context across the hops nothing instruments for us — Kafka headers, MCP requests and
Spark job parameters. A missing collector never breaks the application.

### Dependency lock
`requirements.lock` — 92 packages, fully hashed, generated with `--allow-unsafe` so a hashed install
actually works. Its SHA-256 is the `env_lock_digest` field required by every evaluation run manifest
(ADR-0017).

### Test suite (249 tests, all passing)
| File | Tests | Covers |
|---|---|---|
| `tests/unit/test_version_pins.py` | 7 | Pin matrix; Java 17/21-only assertion; drift rejection |
| `tests/unit/test_doctor.py` | 13 | **NEW** — daemon-down must FAIL not WARN; `_run` keeps code/streams separate; disk floors; Java 25 flagged |
| `tests/unit/test_toolchain_consistency.py` | 6 | **NEW** — `make setup` and every mypy workflow install the extras mypy's scope needs |
| `tests/unit/test_secret_scan.py` | 9 | **NEW** — one shared definition, pinned tool version, planted credential is detected, `.env` untracked |
| `tests/unit/test_pii_redaction.py` | 13 | Email, PAN, IPv4/6, IBAN, account; nested structures; Luhn false-positive guard |
| `tests/unit/test_claim_linter.py` | 12 | All five publication rules, each proven by a fabricated violation |
| `tests/unit/test_control_plane.py` | 146 | Doc existence and substance; ADR format, alternatives, negative consequences; dangling-reference check |
| `tests/acceptance/test_acceptance_status.py` | 10 | PASS-requires-evidence rule; tooling refuses dishonest PASS |
| `tests/integration/test_groundtruth_isolation.py` | 15 | Release-blocking isolation, against real PostgreSQL |

**188 fast + 15 integration = 203 passed.**

Two real defects were found and fixed by these tests during this session, which is the point of writing
them first: the claim linter was over-applying tier gating to operational metrics (an obstructive rule
gets bypassed) and was not recognising `e.g.` as example prose. Both are fixed and covered by
regression tests.

---

## WORK IN PROGRESS

Nothing in flight. This is a clean stopping point.

---

## CURRENTLY FAILING TESTS

**None.** Full results under LAST VERIFICATION RESULTS.

---

## KNOWN TECHNICAL DEBT

| # | Item | Impact | When |
|---|---|---|---|
| ~~D1~~ | ~~No Alembic migration layer~~ | **RESOLVED** — Alembic owns schemas, roles and grants; round-trip verified against real PostgreSQL | done |
| ~~D2~~ | ~~CI execution~~ | **RESOLVED** — remote added, pushed, and all four workflows pass (`make ci-status`). Two red runs were root-caused and fixed, not waived | done |
| ~~D3~~ | ~~No OpenTelemetry wiring~~ | **RESOLVED** — tracer/meter providers, W3C context propagation for non-HTTP hops, `trace_id` in every log line | done |
| D4 | `compose.yml` declares `streaming`/`graph`/`llm` services that nothing consumes yet | Profile shape is reviewable but unexercised | Phases 3, 5, 6 |
| D5 | `postgres:16` used instead of `postgres:16-alpine` | ~250 MB more disk; chosen because `postgres:16` was already local and disk is the binding constraint | Revisit if disk is freed |
| D10 | No declarative SQLAlchemy models yet, so Alembic autogenerate is unused | Migrations are hand-written. Correct for the security-critical grants; will matter once tables arrive | Phase 1 |
| D11 | CI job **logs** need repo-admin rights to download, so a CI-only failure cannot be read directly | Both CI failures this session had to be diagnosed by reproducing them locally. That is a healthier default, but it is slow. `gh auth login` would remove the friction | Optional |
| ~~D6~~ | ~~No dependency lockfile~~ | **RESOLVED** — `requirements.lock`, 92 packages, hashed, installable; `env_lock_digest` is computable | done |
| ~~D7~~ | ~~`make up-streaming` / `up-full` documented but undefined~~ | **RESOLVED** — both targets exist | done |
| D8 | macOS Docker keychain credential helper hangs, blocking all registry pulls | Blocks `make up` on a cold image cache; workaround documented in `LOCAL_DEVELOPMENT.md` | Environment issue, not code |
| D9 | Role passwords in `.env.example` are the literal `change_me_locally` | Fine locally; a real deployment must supply real values. Compose fails fast (`:?`) if any is unset, and the init script refuses to create passwordless roles | Before any shared deployment |

---

## OPEN RISKS

| # | Risk | Status |
|---|---|---|
| ~~R1~~ | ~~Disk below the core-profile floor~~ | **RESOLVED** — ~28 GB free. `make doctor` passes; enough for Phase 3's ~15 GB. The threshold was never lowered to force it green |
| R2 | **Java 25 is the system default**; Spark 4.0 requires Temurin 17. `JAVA_HOME` is unset | Mitigated — `make doctor` detects it and prints the exact `JAVA_HOME` fix. **Blocks Phase 3 entry**, not current work |
| R3 | Docker RAM ceiling 7.7 GB; the full profile budget is tight | Open — mitigated by profiles and per-service memory limits |
| R4 | No AWS credentials on this machine | Open — blocks Phase 12 only |
| R8 | **Docker daemon does not survive a reboot unless Docker Desktop is set to start at login** | Open — `make doctor` now fails loudly with the fix, instead of passing and letting `make up` break confusingly |
| R5 | IEEE-CIS (Track B) requires Kaggle credentials and a ~1.5 GB download | Open — blocks Phase 4B entry; `make fetch-external` fails with instructions |
| R6 | Ollama not installed; `SMOKE` tier unavailable | Open — blocks Phase 6 keyless demo. `make doctor` warns |
| R7 | Two transports (MCP + in-process) mean two places authorization could be wrong | Mitigated by design (single middleware chain, no unwrapped entrypoint); unproven until Phase 5 |

---

## UNRESOLVED DECISIONS

| # | Decision | Resolves at |
|---|---|---|
| U1 | **Delta table layout** — partitioning vs Z-order vs liquid clustering, locally and on Databricks | Phase 3, by benchmark. ADR-0015 stays `Proposed` until then |
| U2 | Whether a 3B-class local model can complete investigations within budget | Phase 6. Fallback to a 7B-class model is documented in ADR-0016 |
| U3 | Whether Neo4j outperforms `PostgresGraphStore` at this scale | Phase 9 Arm G ablation. Either answer is publishable |
| U4 | Whether the Skeptic agent pays for its cost | Phase 9 Arm F ablation. A negative result will be published |
| U5 | Whether LightGBM or XGBoost wins on measured PR-AUC | Phase 4. ADR-0011 chose LightGBM on operational grounds and says to revisit |
| U6 | Whether Phase 13 (Go gateway) is worth doing | Phase 13 entry, against recorded Phase 2 p99 |

---

## PHASE 0 EXIT CRITERIA — all met

| # | Criterion | Status | Evidence |
|---|---|---|---|
| 1 | All control-plane documents exist and are non-placeholder | ✅ | `pytest tests/unit/test_control_plane.py` — 146 passed (12 docs, 26 ADRs, no placeholders, no dangling ADR refs) |
| 2 | `make verify` green | ✅ | 9/9 gates locally; 4/4 workflows green in CI (`make ci-status`) |
| 3 | Ground-truth isolation test passing | ✅ | `pytest -m integration` — 23 passed against real PostgreSQL 16 with real Alembic |
| 4 | Claim linter active | ✅ | `make check-claims` in `make verify` and in CI; 12 self-tests prove it rejects fabricated numbers |
| 5 | ADRs merged | ✅ | 26 ADRs; ADR-0015 deliberately still `Proposed` pending its Phase 3 benchmark |

Phase 0 targets also met: `make up` 6.4 s (target < 90 s), core RSS ~42 MiB (target < 2.7 GB),
`test-fast` < 1 s (target < 5 min), and `make doctor` flags Java 25 loudly.

## NEXT EXECUTABLE TASKS

**Phase 0 is closed.** The next action is a decision, not a task:

1. **Await explicit user approval to begin Phase 1** — domain model, both state machines, event JSON
   Schemas, `CanonicalTransaction` + `SourceAdapter` + `field_coverage`, the transaction generator with
   all 10 fraud scenarios, and `causal_evidence_keys` written to `groundtruth` only.

### Recommended before Phase 3 (not blocking Phase 1)
- Set `JAVA_HOME` to Temurin 17 permanently. `JAVA_HOME` is currently unset and the system default is
  Java 25, which Spark 4.0 does not support. `make doctor` warns.
- Install Ollama and run `make pull-model` for the keyless `SMOKE` tier (needed from Phase 6).
- Optionally `gh auth login`, so CI job logs can be read directly instead of reproduced locally (D11).

### Optional, before Phase 3
- Set `JAVA_HOME` to Temurin 17 permanently (`make doctor` warns; Spark 4.0 will fail on Java 25).
- Install Ollama and `make pull-model` for the keyless `SMOKE` tier (needed from Phase 6).

---

## LAST VERIFICATION RESULTS

Recorded from actual runs on 2026-09-12, after the shutdown-recovery fixes.

```
TRACE-X verify
  PASS  doctor              disk ~28 GB; docker v29.2.1 reachable, 7.7 GB RAM, 10 CPU
                            (2 expected warnings: Java 25 vs Temurin 17, pyspark/delta absent)
  PASS  acceptance-status   58 capabilities, internally consistent
  PASS  check-claims        40 documents scanned, 0 manifests, no unbacked numeric claim
  PASS  ruff-format         clean
  PASS  ruff-lint           clean
  PASS  mypy                clean, strict on trace_core (27 files, incl. migrations/)
  PASS  test-fast           234 passed
  PASS  bandit              clean at MEDIUM+
  PASS  secret-scan         detect-secrets 1.5.0, no findings
======================================================================
  phase 0   9 passed   0 failed   0 skipped     VERIFY OK
```

Full suite: `pytest -m "not cloud"` — **257 passed** (234 fast + 23 integration).

GitHub Actions (`make ci-status`) — **all 4 workflows pass**:
```
  claims  success | lint  success | test-fast  success | test-integration  success
```

CI history this session, which is the point of recording it:
```
  fa048a6  lint FAILURE   mypy: missing db/obs extras
  bf38f45  lint FAILURE   secret scan: unpinned tool + duplicated definition (masked by the above)
  03278d5  ALL GREEN
```

Fresh-clone validation (throwaway clone, no API keys, all key env vars unset):
```
  make setup && make doctor && make verify   ->  all pass
```
This is the check the `make setup` defect would have broken, so it is now run explicitly rather than
assumed.

Live stack (`make up`, then torn down):
```
  startup 6.4 s (target < 90 s) | postgres + redis healthy on 5442 / 6389
  migrations auto-applied, alembic at 0001 (head) | core RSS ~42 MiB (target < 2.7 GB)
  trace_app -> groundtruth: ERROR permission denied | trace_eval -> groundtruth: permitted
```

**Acceptance status: 10 PASS · 0 IN_PROGRESS · 48 NOT_STARTED · 0 FAIL · 0 BLOCKED** across 58 tracked
capabilities. `tests/acceptance/status.json` is the authoritative machine-readable record.
