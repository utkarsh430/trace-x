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

**Phase 0 is 9/10 capabilities PASS.** The one outstanding item (CI execution) is blocked on a git
remote, not on code.

The engineering control plane — constitution, specifications, ADRs, acceptance tracking, developer
command interface, bootstrap environment — is in place and **proven by executed commands**.
230 tests pass (207 fast + 23 integration against real PostgreSQL).

**`make verify` currently reports 7 PASS / 1 FAIL.** The failure is the `doctor` disk check: the machine
has **3.5 GB free on a 228 GB disk that is 99% full**, below the 6 GB floor the core profile needs. This
is an environment condition, not a code defect — every other gate is green, and the threshold has
deliberately **not** been lowered to make the check pass (CLAUDE.md §17 forbids silently reducing a
requirement). This project's own footprint is 291 MB.

**No TRACE-X product functionality exists yet.** No transaction processing, ML, streaming, agents or
frontend code has been written, by instruction.

## LAST VERIFIED COMMIT

`51c2af8` — *Complete Phase 0 implementation: migrations, observability, dependency lock*

All results below were produced against this commit.

---

## COMPLETED WORK

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

### Test suite (203 tests, all passing)
| File | Tests | Covers |
|---|---|---|
| `tests/unit/test_version_pins.py` | 7 | Pin matrix; Java 17/21-only assertion; drift rejection |
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
| D2 | CI workflows written (`lint`, `test-fast`, `claims`, `test-integration`) but never executed on a real push | YAML validates; behaviour on a runner is unverified. **Cannot be resolved locally — needs a git remote** | Blocked on a remote |
| ~~D3~~ | ~~No OpenTelemetry wiring~~ | **RESOLVED** — tracer/meter providers, W3C context propagation for non-HTTP hops, `trace_id` in every log line | done |
| D4 | `compose.yml` declares `streaming`/`graph`/`llm` services that nothing consumes yet | Profile shape is reviewable but unexercised | Phases 3, 5, 6 |
| D5 | `postgres:16` used instead of `postgres:16-alpine` | ~250 MB more disk; chosen because `postgres:16` was already local and disk is the binding constraint | Revisit if disk is freed |
| D10 | No declarative SQLAlchemy models yet, so Alembic autogenerate is unused | Migrations are hand-written. Correct for the security-critical grants; will matter once tables arrive | Phase 1 |
| ~~D6~~ | ~~No dependency lockfile~~ | **RESOLVED** — `requirements.lock`, 92 packages, hashed, installable; `env_lock_digest` is computable | done |
| ~~D7~~ | ~~`make up-streaming` / `up-full` documented but undefined~~ | **RESOLVED** — both targets exist | done |
| D8 | macOS Docker keychain credential helper hangs, blocking all registry pulls | Blocks `make up` on a cold image cache; workaround documented in `LOCAL_DEVELOPMENT.md` | Environment issue, not code |
| D9 | Role passwords in `.env.example` are the literal `change_me_locally` | Fine locally; a real deployment must supply real values. Compose fails fast (`:?`) if any is unset, and the init script refuses to create passwordless roles | Before any shared deployment |

---

## OPEN RISKS

| # | Risk | Status |
|---|---|---|
| R1 | **Disk: 3.5 GB free on a 228 GB disk at 99% capacity.** Below the 6 GB core-profile floor; Phase 3 needs ≥ 15 GB | **Open — `make doctor` now FAILS, not warns.** This project's footprint is 291 MB, so the space must come from elsewhere. ~1.9 GB of unused Docker images and ~1 GB of dangling Docker volumes belong to other projects and were deliberately left untouched |
| R2 | **Java 25 is the system default**; Spark 4.0 requires Temurin 17 | Mitigated — `make doctor` detects it and prints the exact `JAVA_HOME` fix. Not yet blocking |
| R3 | Docker RAM ceiling 7.7 GB; the full profile budget is tight | Open — mitigated by profiles and per-service memory limits |
| R4 | No AWS credentials on this machine | Open — blocks Phase 12 only. `P12.cloud-validation` is tracked as blocked |
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

## NEXT EXECUTABLE TASKS

Phase 0 implementation is complete. Two items remain before the gate can be closed, and **neither is
a coding task** — both need something only the user can provide.

1. **Free ~3 GB of disk** so `make doctor` passes and `make verify` returns fully green. This
   project occupies 291 MB; the space must come from elsewhere on a disk that is 99% full.
   Candidates deliberately left untouched because they belong to other projects:
   `docker image prune -a` (~1.9 GB) and `docker volume prune` (~1 GB). **Ask before running either.**
   Phase 3 will need ~15 GB.
2. **Add a git remote and push**, so the four CI workflows actually execute. Their YAML validates and
   the gates run locally, but "CI passes" is an unverified claim until a runner has run them (D2).

Then:

3. **Close Phase 0** against the gate in `docs/ROADMAP.md` and request approval to begin Phase 1.

**Do not begin Phase 1** (domain model, generator, source adapters) until Phase 0 exit conditions are
met and the user approves.

### Optional, before Phase 3
- Set `JAVA_HOME` to Temurin 17 permanently (`make doctor` warns; Spark 4.0 will fail on Java 25).
- Install Ollama and `make pull-model` for the keyless `SMOKE` tier (needed from Phase 6).

---

## LAST VERIFICATION RESULTS

Recorded from an actual `make verify` run on 2026-09-10, after the migration, observability and
lockfile work.

```
TRACE-X verify
  FAIL  doctor              disk 3.5 GB free, need >= 6.0 GB for the core profile
                            (other doctor checks pass; 4 expected warnings:
                             Java 25 vs Temurin 17, pyspark/delta not installed, ollama absent)
  PASS  acceptance-status   58 capabilities, internally consistent
  PASS  check-claims        41 documents scanned, 0 manifests, no unbacked numeric claim
  PASS  ruff-format         clean
  PASS  ruff-lint           clean
  PASS  mypy                clean, strict on trace_core (22 files, now incl. migrations/)
  PASS  test-fast           207 passed
  PASS  bandit              clean at MEDIUM+ (packages, scripts, eval, migrations)
======================================================================
  phase 0   7 passed   1 failed   0 skipped
```

**The single failure is a host resource condition.** It is reported rather than suppressed: lowering
the disk floor to turn the gate green would be exactly the shortcut CLAUDE.md §17 prohibits.
Freeing ~3 GB restores a fully green `make verify`; Phase 3 will need ~15 GB.

Integration suite (real PostgreSQL 16 container, migrations applied by real Alembic):
```
  pytest -m integration    23 passed
```

Full suite:
```
  pytest -m "not cloud"    230 passed
```

Phase-gated commands correctly refuse to run:
```
  make seed | demo | eval | eval-external | bench-layout   ->  exit non-zero
  "PHASE NOT IMPLEMENTED ... requires Phase N ... repo is at Phase 0"
```

**Acceptance status: 9 PASS · 1 IN_PROGRESS · 48 NOT_STARTED · 0 FAIL · 0 BLOCKED** across 58 tracked
capabilities. `tests/acceptance/status.json` is the authoritative machine-readable record.
