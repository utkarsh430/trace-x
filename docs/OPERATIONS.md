# TRACE-X — Operations Runbook

> Authoritative for **operating** TRACE-X: what breaks, how it is detected, what the operator does.
> Every failure mode in `docs/ARCHITECTURE.md` §18 appears here with a response, and each has a chaos
> test asserting the documented behaviour.

---

## 1. Operating principles

1. **Fail-safe direction is asymmetric and deliberate.** *Scoring* fails **open** — approve and flag —
   because declining all traffic is worse than missing fraud for a few minutes. Every fail-open
   decision is tagged and counted. *Actions* fail **closed** — never execute under uncertainty.
2. **Degradation is always visible.** A degraded decision carries `X-Trace-Degraded: true` and
   `X-Feature-Source`. Silent degradation is treated as an incident in its own right.
3. **Budget exhaustion is a valid outcome, not an error.** `INSUFFICIENT_EVIDENCE` → human queue is the
   system working as designed.
4. **Never restart to clear a symptom before capturing state.** Checkpoints, queue rows and the audit
   chain are the diagnostic record.

---

## 2. Health surfaces

| Surface | Where | Meaning |
|---|---|---|
| `/healthz` | gateway, api | Process alive |
| `/readyz` | gateway, api | Dependencies reachable; **returns 503 while degraded past threshold** |
| container healthcheck | gateway | Probes `/healthz` only. **Liveness and readiness are wired to different consumers on purpose**: the orchestrator restarts on liveness, the load balancer drains on readiness. A restart triggered by a database blip takes out a process that was degrading correctly |
| `/metrics` | all services | Prometheus scrape |
| `make verify` | CLI | Canonical repository health |
| Grafana → Real-Time Ops | `obs` profile | Latency, throughput, bands, degraded rate |
| Grafana → Security | `obs` profile | Injection attempts, authz denials, **audit-chain status** |

---

## 3. Alerts and first response

| Alert | Condition | Severity | First action |
|---|---|---|---|
| `AuditChainBroken` | Verifier detects a hash mismatch | **P1 — security** | Freeze action execution. Preserve the DB. Treat as an incident, not a data-quality bug |
| `UnapprovedHighRiskExecution` | Any HIGH action executed without approval | **P1 — security** | Trip the global circuit breaker. Audit every action in the window |
| `GatewayP99Breach` | p99 > 100 ms for 5 min | P2 | Check Redis latency, then model-scoring time. Consider shedding to rules-only |
| `DegradedModeSustained` | `degraded_mode_total` rising > 10 min | P2 | Identify the failed dependency; degraded scoring is materially less accurate |
| `ConsumerLagGrowing` | Kafka lag rising 15 min | P2 | Check Spark batch duration and executor health |
| `FeatureParityDrift` | drift > 1% on a windowed counter | P2 | Online and offline features disagree — **suspect both** until reconciled |
| `ModelDigestMismatch` | Boot-time digest check failed | P2 | Service refuses to start **by design**. Restore the pinned artifact |
| `InjectionAttemptSpike` | `injection_attempts_total` step change | P2 | Review flagged cases; confirm no decision moved |
| `BudgetExhaustionHigh` | > 10% of investigations exhaust budget | P3 | Check LLM latency and tool timeouts before raising budgets |
| `DLQGrowth` | Any DLQ receiving messages | P3 | Inspect the envelope and consumer version; fix forward |
| `ApprovalSLABreach` | Approvals aging past 30 min | P3 | Staffing issue; expired approvals auto-escalate |

---

## 4. Failure playbooks

### Gateway refuses to start
**Detect:** the container never reports healthy; logs end in `TokenConfigurationError`, a rule-pack
load error, or `Application startup failed`. Under compose a missing `TRACE_SERVICE_TOKEN_<ID>` stops
the `up` itself, before any container is created.
**Behaviour:** **by design.** Service tokens and the rule pack are fatal when absent, for the same
reason a corrupt model artifact is (`docs/ARCHITECTURE.md` §18): a gateway serving traffic it cannot
authenticate, or scoring with rules it could not load, is worse than a gateway that is down, because it
looks like it is working.
**Action:** read the last log line — it names the missing configuration and what it must contain.
Restore it from the secret store; **never** start the gateway with a hand-typed placeholder token to
"get traffic flowing", because every request it then authenticates is unattributable. Restarting will
not fix it: the process is refusing, not crashing.

### Gateway is live but never becomes ready
**Detect:** `/healthz` 200, `/readyz` 503 with `checks.postgres` naming the error. The load balancer
drains the instance; the container healthcheck still reports healthy, and that is correct — the process
is alive.
**Behaviour:** triage cannot durably record a case, so a CRITICAL transaction is refused with 503
rather than approved and lost (ADR-0035).
**Action:** distinguish the two causes. *Postgres down* → the Postgres playbook below. *Postgres up but
the role or grants are missing* (the usual cause on a fresh environment) → run the migrations; the
schema, roles and grants are created there and nowhere else (ADR-0004). Do not grant the gateway a
broader role to clear the error: `trace_app` has no access to `groundtruth` and that is the isolation
control (CLAUDE.md §11).

### Gateway killed on its memory limit
**Detect:** exit code 137, no stack trace, restart loop under `restart: unless-stopped`.
**Behaviour:** the container is bounded so it dies alone rather than starving Postgres and Redis
beside it — on a laptop an unbounded container takes the whole VM with it.
**Action:** capture the limit and the working set before changing anything, then look for the cause
rather than raising the ceiling: a connection pool sized past the database's own limit, or an
unbounded in-process cache, both present exactly this way. If the limit genuinely needs to rise,
`docs/ARCHITECTURE.md` §14 budgets the whole `core` profile, so raising one service means lowering
another — `tests/unit/test_compose_profiles.py` fails when the sum stops fitting.

### Redis unavailable
**Detect:** health probe, 20 ms timeouts, `degraded_mode_total{reason="redis"}` climbing.
**Behaviour:** hot path falls back to rules-only; responses carry `X-Trace-Degraded: true`.
**Action:** confirm the fallback is active (not 5xx). Restore Redis. **Do not** backfill counters by
hand — let the Spark reconciliation job rebuild them, then confirm `feature_parity_drift` settles.
**Do not** treat the degraded window's decisions as normal-quality; they are flagged for review.

### Postgres unavailable
**Detect:** connection errors; gateway 503; workers stop consuming.
**Behaviour:** no work is lost — the queue lives in Postgres.
**Action:** restore Postgres. Workers resume from queue leases; in-flight investigations resume from
their LangGraph checkpoints. Verify no duplicate action execution via idempotency keys.

### Kafka unavailable
**Detect:** producer timeouts.
**Behaviour:** gateway buffers to a bounded local WAL, then sheds. **Scoring continues.**
**Action:** restore Kafka; drain the WAL. Confirm dedup absorbed replays. Check `late_events` growth —
buffered events may now be late and must land there, not vanish.

### Spark job crash
**Detect:** supervisor restart, consumer lag.
**Behaviour:** resumes from checkpoint; online store serves stale-but-flagged features.
**Action:** never delete a checkpoint to "fix" a stuck job — that silently reprocesses or skips data.
Diagnose first. If a checkpoint must be reset, record it as a data-lineage event.

### Neo4j unavailable
**Detect:** health probe.
**Behaviour:** `GraphStore` falls back to `PostgresGraphStore`; graph evidence carries reduced
confidence, and the adapter used is recorded on each evidence record.
**Action:** restore Neo4j; re-run graph sync. Investigations decided during the window remain valid but
their graph evidence is weaker — this is visible in the ledger, not hidden.

### MCP server unreachable
**Detect:** spawn/connect timeout; `tool_calls_total{transport,status="error"}`.
**Behaviour:** the owning agent applies its `on_failure` policy (usually DEGRADE) — recorded as reduced
evidence, never a silent gap.
**Action:** for stdio, check worker process spawn and the injected capability token. For HTTP, check
the `mcp-http` profile and OAuth 2.1 credentials. Confirm the parity suite still passes after recovery.

### LLM provider failing (timeout / 429)
**Detect:** `llm_*` error rate, investigation latency.
**Behaviour:** 2 jittered retries, then the agent's `on_failure` policy; the investigation continues
with reduced evidence, or terminates as `INSUFFICIENT_EVIDENCE`.
**Action:** check the tier. Consider dropping to `CI` replay for non-production work.
**Never** raise budgets to mask provider latency — that converts a provider incident into a cost
incident.

### Action execution failed verification
**Detect:** verification read-back mismatch.
**Behaviour:** compensating action runs; case → `ESCALATED`; page.
**Action:** confirm the compensation actually landed — read the ledger, do not trust the log line.
Verify the audit chain covers both the failed action and the compensation.

### Audit chain broken — **P1**
**Detect:** verifier job.
**Action:** **Stop action execution immediately.** Snapshot the database. Identify the first broken
`seq`. Do not repair the chain — a repaired chain has no evidentiary value. Escalate as a security
incident and preserve everything for forensics.

---

## 5. Routine operations

| Task | Cadence | Command |
|---|---|---|
| Repository health | every change | `make verify` |
| Audit-chain verification | hourly | verifier job (alerts on break) |
| Feature-parity check | per Spark batch | `feature_parity_drift` metric |
| Model drift (PSI) | daily | Spark batch job |
| Benchmark regression | nightly | `make eval` |
| Dependency audit | weekly | `make audit` |
| DLQ review | weekly | inspect and fix forward |

### Model promotion
1. Train and register; capture a complete run manifest.
2. Compare against the incumbent on the frozen dataset — **PR-AUC on the temporal test split**.
3. `POST /v1/models/{version}/promote` (`fraud_manager` only).
4. The gateway verifies the artifact digest at boot and **refuses to start on mismatch**.
5. Watch the score distribution and PSI for 24 h; roll back by promoting the prior version.

### Deploying a schema change
Follow the breaking-change procedure in `docs/EVENT_CONTRACTS.md` §4 — new topic version, dual-write,
consumer migration, verify zero lag and zero DLQ for a full retention period, then retire. Never mutate
a released schema.

---

## 6. Escalation

| Severity | Examples | Response |
|---|---|---|
| **P1 — security** | Audit chain broken; unapproved HIGH execution; suspected ground-truth leakage | Immediate. Freeze actions, preserve state, escalate |
| **P2 — degraded** | Sustained degraded mode; p99 breach; parity drift; model digest mismatch | Same day. Decisions in the window are flagged for review |
| **P3 — quality** | DLQ growth; high budget exhaustion; approval SLA breach | Next working day |

**Any suspicion that `trace_app` gained access to `groundtruth` is P1.** It silently invalidates every
metric in the project. Confirm with the isolation test, and treat prior benchmark results as suspect
until re-run.
