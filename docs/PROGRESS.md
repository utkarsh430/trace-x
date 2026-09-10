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

**Control plane established and verified. Phase 0 is 8/10 capabilities PASS.**

The engineering control plane — constitution, specifications, ADRs, acceptance tracking, developer
command interface, bootstrap environment — is in place and **proven by executed commands**.
`make verify` passes all 8 gates. 203 tests pass (188 fast + 15 integration against real PostgreSQL).

Remaining Phase 0 work: the Alembic migration layer (D1) and CI execution on a real push (D2).

**No TRACE-X product functionality exists yet.** No transaction processing, ML, streaming, agents or
frontend code has been written, by instruction.

## LAST VERIFIED COMMIT

`(pending — the foundation commit for this work)`

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

### Ground-truth isolation — verified against real PostgreSQL
`deploy/postgres/init/01-roles-and-schemas.sql` creates 5 schemas and 4 roles. **Executed against a live
PostgreSQL 16 container and confirmed:**

| Check | Result |
|---|---|
| `trace_app` reading `groundtruth` | ❌ denied — `permission denied for schema groundtruth` |
| `trace_stream` reading `groundtruth` | ❌ denied |
| `trace_auditor` reading `groundtruth` | ❌ denied |
| `trace_app` creating objects in `groundtruth` | ❌ denied |
| `trace_eval` reading `groundtruth` | ✅ permitted (the harness must measure) |
| `trace_app` reading `app` schema | ✅ permitted (isolation must not break the app) |

This is the highest-severity control in the project: leakage would silently invalidate every metric.

### Local stack — started and verified
`make up` brings up the `core` profile. Confirmed running:

| Service | Port | Health | Verified |
|---|---|---|---|
| postgres 16 | 5442 | healthy | Init script created 5 schemas + 4 roles from **env-injected** passwords; `trace_eval` authenticates; `trace_app` still denied on `groundtruth` |
| redis 7-alpine | 6389 | healthy | `redis-cli ping` → `PONG` |

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
| D1 | No Alembic migration layer yet — only the raw role/schema bootstrap SQL | Application tables cannot be created; migration up/down round-trip is untested | Phase 0, remaining |
| D2 | CI workflows written (`lint`, `test-fast`, `claims`) but never executed on a real push | YAML is valid; behaviour on a runner is unverified | Phase 0, remaining — needs a remote |
| D3 | No OpenTelemetry wiring — only PII-redacting logging exists | Tracing scaffold incomplete | Phase 0, remaining |
| D4 | `compose.yml` declares `streaming`/`graph`/`llm` services that nothing consumes yet | Profile shape is reviewable but unexercised | Phases 3, 5, 6 |
| D5 | `postgres:16` used instead of `postgres:16-alpine` | ~250 MB more disk; chosen because `postgres:16` was already local and disk is the binding constraint | Revisit if disk is freed |
| D6 | No dependency lockfile — `pyproject.toml` uses ranges | `env_lock_digest` in the run manifest has nothing to hash | Before Phase 9 |
| D7 | `make up-streaming` / `up-full` referenced in docs but not yet defined as targets | Documented commands that do not exist | Phase 3 |
| D8 | macOS Docker keychain credential helper hangs, blocking all registry pulls | Blocks `make up` on a cold image cache; workaround documented in `LOCAL_DEVELOPMENT.md` | Environment issue, not code |
| D9 | Role passwords in `.env.example` are the literal `change_me_locally` | Fine locally; a real deployment must supply real values. Compose fails fast (`:?`) if any is unset, and the init script refuses to create passwordless roles | Before any shared deployment |

---

## OPEN RISKS

| # | Risk | Status |
|---|---|---|
| R1 | **Disk: 7.0 GB free.** Phase 3 needs ≥ 15 GB for Kafka + Spark + Neo4j + MLflow images | **Open — blocks Phase 3 entry.** `make doctor` warns |
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

In order. Each is a Phase 0 exit condition.

1. **Alembic migration layer** — initialise, wire to the five-schema model, add an up/down round-trip
   test. Resolves D1.
2. **CI workflows** — `.github/workflows/lint.yml`, `test-fast.yml`, `claims.yml`. Resolves D2.
3. **OpenTelemetry scaffold** — tracer/meter providers, OTLP exporter config, `trace_id` in the log
   context. Resolves D3.
4. **Dependency lock** — generate and commit a lockfile so `env_lock_digest` is computable. Resolves D6.
5. **Re-run `make verify`**, update this file and `tests/acceptance/status.json` with real results.
6. **Close Phase 0** against the gate in `docs/ROADMAP.md`, then request approval to begin Phase 1.

**Do not begin Phase 1** (domain model, generator, source adapters) until Phase 0 exit conditions are
met and the user approves.

---

## LAST VERIFICATION RESULTS

Recorded from an actual `make verify` run on 2026-09-10.

```
TRACE-X verify — 2026-09-10T15:56:22Z
  PASS  doctor                 exit 0 (5 warnings, all expected at Phase 0: Java 25 vs Temurin 17,
                                       disk 7.0 GB, pyspark/delta not installed, ollama absent)
  PASS  acceptance-status      58 capabilities, internally consistent
  PASS  check-claims           38 documents scanned, 0 manifests, no unbacked numeric claim
  PASS  ruff-format            clean (58 files)
  PASS  ruff-lint              clean
  PASS  mypy                   clean, strict on trace_core (16 files)
  PASS  test-fast              188 passed
  PASS  bandit                 clean at MEDIUM+ (3 LOW are verified false positives, documented)
======================================================================
  phase 0   8 passed   0 failed   0 skipped
  VERIFY OK
```

Integration suite (requires Docker, run separately):
```
  pytest -m integration    15 passed    (against a live PostgreSQL 16 container)
```

Phase-gated commands correctly refuse to run:
```
  make seed | demo | eval | eval-external | bench-layout   ->  exit non-zero
  "PHASE NOT IMPLEMENTED ... requires Phase N ... repo is at Phase 0"
```

Local stack:
```
  make up   ->  tracex-postgres-1 healthy (5442), tracex-redis-1 healthy (6389)
                5 schemas + 4 roles bootstrapped automatically; redis PONG
```

**Acceptance status: 8 PASS · 1 IN_PROGRESS · 49 NOT_STARTED · 0 FAIL · 0 BLOCKED** across 58 tracked
capabilities. `tests/acceptance/status.json` is the authoritative machine-readable record.
