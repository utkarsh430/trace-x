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

Spark 4.0.1 runs on Java 17 or 21 only, and this project pins **Temurin 17** (ADR-0018). On anything
else Spark fails with an opaque `UnsupportedClassVersionError`.

**`make` selects it for you** once Temurin 17 is installed: every target resolves `JAVA_HOME` through
`scripts/java_home.py`, which confirms a JDK's version by running it rather than trusting its directory
name. Outside `make` — a bare `pytest`, an IDE test runner — select it yourself. On macOS:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

From Phase 3, `make doctor` **fails** on any other Java, and `trace_core.stream.session.build_session`
refuses to start a JVM on one, naming the fix (ADR-0045).

---

## 2. First run

```bash
git clone <repo> && cd trace-x
make setup      # venv, every dependency from the hashed locks, verified Spark jars, .env from the template
make doctor     # preflight — fix anything it reports before continuing
make up         # core profile: postgres, redis, gateway, then applies migrations
make verify     # ★ canonical health check
```

The first `make up` **builds** the gateway image from this repository, which takes a few minutes on a
cold Docker cache and is then reused. It also needs one service token in `.env` — see
[Running the gateway](#running-the-gateway) — because the gateway refuses to start without one.

`make verify` is the single command that answers "is this repository healthy?". Run it before claiming
anything works, and after every change.

**Dependencies are installed from hashed lockfiles, never from `pyproject.toml` ranges.**
`scripts/install_locked.sh` is the only install path, and every CI job runs the same script. The first
`make setup` builds pyspark from its source archive and downloads the Spark JVM jars pinned in
`packages/trace_core/stream/jars.lock` into `~/.cache/trace-x/spark-jars` (override with
`TRACE_SPARK_JARS_DIR`), verifying each by SHA-256; later runs reuse both. After changing a dependency,
run `make lock` and review the lock diff.

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
make kafka-topics        # then create the declared topics: the broker never creates one
make up-full             # everything — needs ~15 GB free and 8 GB Docker RAM
```

The table is the design. **What `core` actually starts today is postgres, redis and the gateway**;
`trace-api` and the dashboard arrive with their phases; `trace-worker` runs the outbox relay today, in the `streaming` profile, and its investigations arrive with their phase; and the MCP servers are
spawned over stdio by the worker rather than run as containers. `docs/PROGRESS.md` is the live state.

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
| `UnsupportedClassVersionError` from Spark | Java 25 (or 21+ mismatch) | Install Temurin 17; `make` selects it, or `export JAVA_HOME=$(/usr/libexec/java_home -v 17)` |
| `ToolchainMismatchError` from `build_session` | The JVM, pyspark, Delta, Hadoop, Scala or a jar does not match its pin | The message lists every failed check with its fix; `make doctor` shows the same |
| `does not match jars.lock` | A cached jar differs from its pinned SHA-256 | Delete it and run `make stream-jars`. Never edit `jars.lock` to match a download |
| `THESE PACKAGES DO NOT MATCH THE HASHES` during install | The lock and the package index disagree | Do not bypass `--require-hashes`; regenerate deliberately with `make lock` and review the diff |
| `BindException: Can't assign requested address` from Spark | The host name resolves to an address this machine cannot bind | Build sessions only through `build_session`, which binds local drivers to loopback |
| `collection guard` failure after `pytest -m stream` | Every stream test was skipped (no Temurin 17, pyspark or verified jars) | `make setup`, select Temurin 17, re-run; a skip is not evidence |
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
| Any `docker compose` command stops with `required variable TRACE_SERVICE_TOKEN_LOCAL is missing a value` | No service token in `.env` | Add `TRACE_SERVICE_TOKEN_LOCAL=<32+ chars>`. The gateway refuses to start without one, so compose refuses first |
| Gateway container restarts in a loop, logs `TokenConfigurationError` | A token shorter than 32 characters | `openssl rand -hex 32`. A short secret is guessable and a gateway is a public surface |
| `make up` hangs on `Container tracex-gateway-1 Waiting` | The image is building on a cold cache, or the app is waiting out its Postgres pool timeout | `docker compose -f deploy/compose.yml logs -f gateway`. Start-up waits the pool's full timeout before serving degraded, which is why the healthcheck allows for it |
| Gateway is healthy but `/readyz` returns 503 | Migrations have not run, so the `trace_app` role does not exist | `make migrate`. Health is liveness; readiness needs the database |
| `/readyz` 503 with `writer_session: lock held elsewhere` | Another gateway process (a second `uvicorn`, a stale container) holds the online store's writer fence | Stop the other process cleanly. One gateway writes online state at a time (ADR-0051) |
| `/readyz` shows `observation_log: not configured` | `TRACE_GATEWAY_KAFKA_BOOTSTRAP` is empty, the default, because Kafka is the `streaming` profile | Scoring works either way. To publish the observation log, start `--profile streaming`, apply the topics, and set `TRACE_GATEWAY_KAFKA_BOOTSTRAP=kafka:19092` |
| Cases and authorization outcomes wait in `app.outbox` | The relay runs in the `worker` service (ADR-0051 §7), which needs the `streaming` profile | Start it with `--profile core --profile streaming up -d worker`. `/readyz` `outbox_relay: disabled` on the gateway is expected: its in-gateway relay exists only to reproduce the A/B |
| Gateway logs `connection refused` for port 5442 or 6389 | The container inherited the **host** ports from `.env` | Inside the compose network Postgres is `postgres:5432` and Redis is `redis:6379`; `deploy/compose.yml` sets those explicitly |
| Code changes have no effect on the running gateway | The image is built, not mounted | `docker compose -f deploy/compose.yml --env-file .env --profile core up -d --build gateway` |
| Integration tests skipped | Docker not running | Start Docker; the skip message names the reason |
| `external` tests skipped | IEEE-CIS not downloaded | `make fetch-external` (needs Kaggle credentials) |

---

## Running the gateway

`trace-gateway` is the synchronous scoring surface. It runs in the `core` profile as a container built
from this repository (`deploy/gateway.Dockerfile`), published on **8010**.

### It will not start without a service token

Callers are payment systems, not people, so the gateway authenticates **service identity**
(`docs/SECURITY.md` §3, Plane B) and **refuses to start when no token is configured** — a gateway that
authenticated a built-in credential would be open by default and fail silently. `.env` must therefore
carry at least one:

```bash
TRACE_SERVICE_TOKEN_LOCAL=<at least 32 characters>   # openssl rand -hex 32
```

A caller presents it as `Bearer <token_id>.<secret>`, where the id is the part of the variable name
after the prefix, lowercased — `TRACE_SERVICE_TOKEN_LOCAL` is presented as `local.<secret>`. Only the
id is ever logged or used as a rate-limit key; the secret stays out of Redis, logs and metrics.

If the variable is missing, **every** `docker compose` command in this repository stops with
`required variable TRACE_SERVICE_TOKEN_LOCAL is missing a value` — including `make down`. That is
deliberate: the alternative is a container that starts, crash-loops and has to be diagnosed from a
stack trace.

### Probing it

```bash
curl -s localhost:8010/healthz    # liveness — touches no dependency
curl -s localhost:8010/readyz     # readiness — 503 while Postgres is unreachable
curl -s localhost:8010/metrics    # Prometheus exposition, with the `obs` profile down
```

`/readyz` returns 503 until `make migrate` has run, because it connects as `trace_app` and that role is
created by the migration. The container's own healthcheck probes `/healthz` for exactly this reason:
`make up` waits for health *before* it migrates, so a readiness probe there would deadlock a fresh
clone — and a process degrading correctly through a database blip is not a process to restart.

### Scoring a transaction

```bash
curl -s -X POST localhost:8010/v1/transactions \
  -H "Authorization: Bearer local.$TRACE_SERVICE_TOKEN_LOCAL" \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: $(uuidgen)" \
  -d '{"transaction_id":"txn_local_001","account_id":"acct_100001","amount_minor":4599,
       "currency":"USD","occurred_at":"'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'",
       "merchant_id":"mrch_10001","channel":"CARD_PRESENT"}'
```

`X-Idempotency-Key` is required on every mutating POST: without one the request is rejected rather than
given an invented key, which would make a retry a new request. Repeat the call with the same key and
the same body to get the stored response back and no second case.

Two response headers say what the decision was made with — `X-Trace-Degraded` and `X-Feature-Source`.
Stop Redis (`docker compose -f deploy/compose.yml --env-file .env stop redis`) and score again: the
request still succeeds, the rules over online features abstain, and both headers change. That is the
designed degraded mode, not a fault.

### After changing the code

The image is **built, not mounted**, so a change to `services/` or `packages/` does not reach a running
container until it is rebuilt:

```bash
docker compose -f deploy/compose.yml --env-file .env --profile core up -d --build gateway
```

Its memory limit is set so the whole `core` profile stays inside the budget `docs/ARCHITECTURE.md` §14
gives it; `tests/unit/test_compose_profiles.py` fails if a change to any limit breaks that sum.

---

## Generating a dataset

`make seed` generates a Track A dataset, writes its ground truth, and records the run.

```bash
# A small local dataset. Needs PostgreSQL up (`make up`) for the ground-truth step.
make seed ARGS="--rows 50000 --dataset-version dev-v1"

# No database: events only. Says so in the run record rather than leaving it implicit.
make seed ARGS="--rows 10000 --no-groundtruth --out /tmp/tx"

# Publish to Kafka instead of files (needs the `stream` extra, a broker and `make kafka-topics`).
# The run fails, and writes no run record, unless every event is confirmed delivered.
make seed ARGS="--sink kafka --bootstrap localhost:9092"

# A frozen dataset, exactly as its manifest describes it (eval-v2's gate included). Refused unless
# the configuration digest matches, and exits before writing ground truth unless every recorded
# stream digest is reproduced.
make seed ARGS="--manifest eval/track_a/eval-v2.candidate.manifest.json --sink parquet --out data/generated"
```

Useful options:

| Option | Meaning |
|---|---|
| `--rows` | Number of transactions. Identity and device events are additional. |
| `--seed` | Master seed. Same seed, same dataset, same digest (ADR-0029). |
| `--dataset-version` | Dataset identity. Re-using one is refused by the database. |
| `--fraud-rate` | Target fraudulent share. Has no effect below the coverage floor — see `docs/FRAUD_SCENARIOS.md` §2. |
| `--validate` | `all` (default), `sample`, or `none`. Produce-time schema validation (`docs/EVENT_CONTRACTS.md` §6.1). Whichever is used is recorded in the run record. |
| `--no-groundtruth` | Skip the PostgreSQL write. |
| `--sink` | `jsonl`, `parquet`, `kafka`, or `none`. `none` measures generation without I/O. |
| `--manifest` | Generate a frozen manifest's configuration instead of the sizing options, and verify its digests. |

**Every run writes a record** to `eval/manifest/`. That record is what
`make check-claims` resolves when a number appears in the documentation — a measurement with no
record cannot be cited (CLAUDE.md §13). A run from a dirty worktree is recorded as such and is **not
publishable**, and the CLI says so when it happens.

Ground truth is written as `trace_generator`, a role that may **insert** labels and cannot **read**
them (ADR-0031). If that step fails, the command exits non-zero rather than leaving a dataset nobody
can evaluate.

## Replaying a frozen dataset through the gateway

`eval/replay/gateway_replay.py` posts a dataset's prefix to the running gateway in event-time order,
then joins labels as `trace_eval`. `eval/replay/r010_threshold_study.py` reads its saved decisions.

```bash
set -a; . ./.env; set +a

# 1. The gateway must run the code you mean to validate.
docker compose -f deploy/compose.yml --env-file .env --profile core up -d --build gateway

# 2. Switching datasets? Reset the replay tables first, as the database owner. Frozen datasets reuse
#    positional transaction ids, so a second dataset's outcomes conflict with the first one's (409)
#    and its triage reuses the first one's cases. The harness refuses rather than finding out mid-run;
#    `--reuse-existing-state` is for replaying the same dataset again.
docker exec -e PGPASSWORD="$POSTGRES_SUPERUSER_PASSWORD" tracex-postgres-1 psql -U "$POSTGRES_SUPERUSER" \
  -d tracex -c "TRUNCATE app.case_transitions, app.investigation_queue, app.cases, app.outbox, app.authorization_outcomes"

# 3. An empty feature store, vouched for the whole replay from the dataset's shifted window start.
docker exec tracex-redis-1 redis-cli -n 0 FLUSHDB
.venv/bin/python eval/replay/gateway_replay.py --dataset-dir data/generated/eval-v2 \
  --dataset-version eval-v2 --limit 60000 --vouch-from-manifest eval/track_a/eval-v2.manifest.json \
  --decisions /tmp/decisions.jsonl --report benchmarks/gateway/triage-bands-eval-v2.md

# 4. R010's operating points from those decisions.
.venv/bin/python -m eval.replay.r010_threshold_study --dataset-dir data/generated/eval-v2 \
  --dataset-version eval-v2 --limit 60000 --decisions /tmp/decisions.jsonl \
  --report benchmarks/gateway/r010-threshold-study-eval-v2.md
```

- **eval-v1 has no outcome stream.** Pass its generation run record as `--source-manifest`
  (`eval/manifest/gen-20260912-eval-v1-ccfd38d9.json`) so outcomes are derived with its seed.
- **`history_incomplete` on every decision is expected** for a prefix of a few days. The store is
  vouched from the window start, and a 30-day feature cannot be complete inside a few days of data, so
  profile rules abstain. Windowed rules are unaffected.
- **Comparing with an older gateway** that predates authorization outcomes: run it on another port and
  add `--withhold-outcomes --base-url http://localhost:8011`. Outcomes are still derived and then
  withheld, so both runs see the same events and time shift.

## Regenerating event models

Event models are generated from the committed JSON Schemas, never hand-edited (ADR-0028):

```bash
make codegen        # regenerate after changing docs/contracts/events/*.json
```

`make verify` fails if the committed models differ from a fresh render, so a hand edit is reverted
rather than merged.
