# TRACE-X — Local Development

> **The complete core product runs locally with no cloud account and no API key.**
> Paid AWS/Databricks resources are required only for Phase 12 cloud validation.

---

## 1. Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | **3.12** | 3.13 is not supported by the pinned dependency set |
| Docker Desktop | any recent | **≥ 6 GB** allocated RAM; ≥ 8 GB for the full profile |
| Disk | **≥ 6 GB** free for `core`; **≥ 15 GB** for the full profile | Images dominate |
| Java | **Temurin 17** | Phase 3+. Spark 4.0 supports Java 17/21 **only** |
| Ollama | any recent | Phase 6+, for the keyless `SMOKE` tier |
| Node | 20+ | Phase 10+, dashboard only |

`make doctor` checks all of these and tells you exactly what to fix.

### The Java trap

If `java -version` reports anything other than 17 or 21, Spark **will** fail with an opaque
`UnsupportedClassVersionError`. On macOS:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

Put it in your shell profile. `make doctor` warns until it is correct.

---

## 2. First run

```bash
git clone <repo> && cd trace-x
make setup      # venv + dev dependencies, creates .env from the template
make doctor     # preflight — fix anything it reports before continuing
make up         # core profile: postgres + redis, then applies migrations
make verify     # ★ canonical health check
```

`make verify` is the single command that answers "is this repository healthy?". Run it before claiming
anything works, and after every change.

---

## 3. Compose profiles

`core` is a complete working product on its own. Everything else is opt-in, because the full stack does
not fit in 8 GB alongside a laptop's other work.

| Profile | Services | ~RAM | ~Disk | Behaviour when **off** |
|---|---|---|---|---|
| `core` (default) | postgres, redis, gateway, api, worker, ui, 3 MCP servers over stdio | 2.7 GB | ~4 GB | — |
| `streaming` | kafka (KRaft), spark master + worker | 3.5 GB | ~5 GB | Online features only; responses carry `X-Feature-Source: ONLINE_ONLY` |
| `graph` | neo4j | 1.5 GB | ~1 GB | `PostgresGraphStore` fallback; graph evidence confidence reduced and recorded |
| `ml` | mlflow | 0.5 GB | ~1 GB | Serving unaffected — artifacts are digest-pinned; training unavailable |
| `obs` | otel-collector, prometheus, grafana, jaeger | 1.5 GB | ~2 GB | `/metrics` still exposed; no dashboards |
| `llm` | ollama + a 3B-class quantized model | 1.5 GB | ~2.5 GB | Falls back to `DEV`/`CI` if configured; otherwise `make demo` says so plainly |
| `mcp-http` | the 3 MCP servers as HTTP services | 0.5 GB | ~0.3 GB | stdio transport is used instead (the default) |

```bash
make up                  # core
make up-streaming        # core + streaming
make up-full             # everything — needs ~15 GB free and 8 GB Docker RAM
```

**Every degraded mode is visible, never silent.** A response made without reconciled features says so
in a header; graph evidence gathered from the fallback adapter carries lower confidence and records
which adapter produced it.

### MCP transport

MCP servers run over **stdio by default**, spawned by the worker. That is a real protocol boundary at
**zero container cost**, which is what makes local-first MCP viable on a small VM. Set
`TRACE_MCP_TRANSPORT=http` and enable the `mcp-http` profile to exercise streamable HTTP with OAuth 2.1
— the same transport Phase 12's AgentCore Gateway federates.

---

## 4. Ports

Chosen to avoid collisions with other local projects. Override any of them in `.env`.

| Service | Port | Service | Port |
|---|---|---|---|
| gateway | 8010 | mlflow | 5010 |
| api | 8011 | prometheus | 9090 |
| dashboard | 3010 | grafana | 3011 |
| postgres | **5442** | jaeger UI | 16686 |
| redis | **6389** | otlp grpc | 4317 |
| kafka | 9092 | ollama | 11434 |
| neo4j | 7474 / 7687 | | |

Postgres and Redis are deliberately **not** on 5432/6379 — those are commonly occupied.
`make doctor` reports any conflict before Docker fails confusingly.

---

## 5. LLM tiers — no API key required

| Tier | Provider | When | Publishable quality numbers? |
|---|---|---|---|
| `SMOKE` | local Ollama | **Default.** Bare clone, demos, development | **No** |
| `DEV` | OpenAI-compatible | You have a key and want better output | No |
| `EVAL` | AWS Bedrock | Phase 12 and formal benchmarks | **Yes — only tier that may** |
| `CI` | cassette replay | Deterministic, zero-cost CI | No |

```bash
make pull-model    # one-time, ~2 GB
make demo          # full multi-agent investigation, no API key needed
```

The local model does **not** match Bedrock quality, and nothing in this repository claims otherwise.
Where it is too weak to complete an investigation within budget, you will see
`INSUFFICIENT_EVIDENCE` — that is **designed behaviour**, not a bug. The demo degrades honestly rather
than fabricating a decision.

---

## 6. Everyday commands

```bash
make doctor        # preflight
make up / down / ps / logs
make migrate       # apply migrations (schemas, roles, grants — ADR-0004)
make migrate-down  # roll back
make lock          # regenerate the hashed dependency lockfile
make lint          # ruff format check + lint
make typecheck     # mypy (strict on trace_core)
make test-fast     # unit + property + contract + conformance (< 5 min, no services)
make test          # everything except cloud
make verify        # ★ canonical health check
make check-claims  # benchmark-integrity linter
make acceptance    # capability status report
make clean
```

Phase-gated commands (`seed`, `demo`, `eval`, `eval-external`, `e2e`, `bench-layout`, `fetch-external`,
`pull-model`) **exit non-zero with a clear message** until their phase lands. They never pretend to
succeed.

---

## 7. Working on this repository

1. Read `CLAUDE.md`, then `docs/PROGRESS.md`, then your phase in `docs/ROADMAP.md`.
2. Work only within the current phase's scope.
3. Run `make verify` before claiming anything works.
4. Update `docs/PROGRESS.md` and `tests/acceptance/status.json` with **actual** results.
5. Write an ADR if you made an architectural decision.

A capability is `PASS` only with executable evidence. `scripts/acceptance.py` refuses to record `PASS`
without a `--result`.

---

## 8. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `UnsupportedClassVersionError` from Spark | Java 25 (or 21+ mismatch) | `export JAVA_HOME=$(/usr/libexec/java_home -v 17)` |
| `NoSuchMethodError` in Delta | Hadoop/Spark/Delta version drift | `make doctor` — pins must be 4.0.1 / 4.0.1 / 3.4.x |
| `port is already allocated` | Another project holds the port | Change it in `.env`; `make doctor` lists conflicts |
| Containers OOM-killed | Docker RAM too low for the profile | Raise Docker RAM, or use fewer profiles |
| `no space left on device` | Images exceeded free disk | `docker system prune -a`; `core` needs ~4 GB |
| `make demo` reports no LLM | Ollama not installed or model not pulled | `make pull-model`, or set a `DEV` key in `.env` |
| Investigation ends `INSUFFICIENT_EVIDENCE` on `SMOKE` | Small model hit its budget | Expected. Use `DEV`/`EVAL` for quality |
| `permission denied for schema groundtruth` in app code | **Working as designed** | Ground truth is unreachable by the app. Only the eval harness may read it |
| `permission denied for table events` on an audit `SELECT` | **Working as designed** | The audit log is append-only: `trace_app` may `INSERT` only. Note `INSERT ... RETURNING` also fails, because it needs `SELECT` |
| Migration fails: `TRACE_APP_DB_PASSWORD is not set` | `.env` not exported | `make migrate` sources `.env`; if running alembic directly, export the four `TRACE_*_DB_PASSWORD` variables |
| `docker pull` hangs; `error getting credentials` | macOS keychain credential helper is blocked (common when the keychain is locked or the login session is non-interactive) | Unlock the login keychain, or bypass the helper for one command: `export DOCKER_CONFIG=$(mktemp -d); echo '{}' > $DOCKER_CONFIG/config.json; ln -s ~/.docker/cli-plugins $DOCKER_CONFIG/cli-plugins`. The symlink is required — without it the `docker compose` plugin is not discovered |
| Integration tests skipped | Docker not running | Start Docker; the skip message names the reason |
| `external` tests skipped | IEEE-CIS not downloaded | `make fetch-external` (needs Kaggle credentials) |
