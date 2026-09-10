# TRACE-X — Architecture Specification

> Durable architecture specification. Authoritative for **design**; `docs/PROGRESS.md` is
> authoritative for **what exists**. If code and this document disagree, one of them is a bug —
> resolve it, do not let the drift stand.
>
> Decisions are recorded in `docs/adr/`. This document explains the system; ADRs explain *why* each
> choice was made and what was rejected.

---

## 1. System context

TRACE-X sits between a transaction authorization flow and a fraud operations team.

```mermaid
graph LR
  subgraph External
    PSP[Payment / auth system]
    IDP[Identity + KYC provider]
    DEV[Device fingerprint vendor]
    MRC[Merchant reputation feed]
    CORE[Core banking ledger]
    ANALYST[Fraud analyst]
  end

  subgraph TRACE-X
    GW[trace-gateway<br/>synchronous scoring]
    API[trace-api<br/>control plane]
    WK[trace-worker<br/>investigations]
    ST[trace-stream<br/>Spark medallion]
    UI[trace-ui<br/>dashboard]
  end

  PSP -->|transaction, sync| GW
  GW -->|RiskDecision less than 100ms| PSP
  IDP -.evidence.-> WK
  DEV -.evidence.-> WK
  MRC -.evidence.-> WK
  GW --> ST
  GW -->|HIGH / CRITICAL| WK
  WK -->|ValidatedAction| CORE
  WK --> API
  ANALYST --> UI --> API
```

**Inside the boundary:** ingestion, feature computation, risk scoring, investigation orchestration,
evidence retrieval, decisioning, action proposal and policy gating, human approval, audit, evaluation.

**Outside, behind ports (never mocked in E2E):** the authorization system, the core banking ledger
(`ActionExecutor` port with a real `LocalLedgerAdapter` — a genuine Postgres-backed ledger service, so
the E2E path truly executes, verifies, and can fail), identity/device/merchant intelligence
(`EvidenceSource` ports with deterministic local implementations), and case-management/SIEM export.

**Non-goals:** settlement, chargeback lifecycle, multi-tenancy, real PII.

### Inbound data surfaces

Two, and both must work:

1. **Live event ingress** — the generator or any producer speaking `tx.raw.v1`.
2. **External dataset ingress** — a `SourceAdapter` port mapping a foreign schema onto
   `CanonicalTransaction`. Implementations: `GeneratorAdapter` (Track A) and `IeeeCisAdapter`
   (Track B). This is what makes "ingestion adaptability" a tested property rather than a claim.

---

## 2. Component boundaries

The core is **one Python package, `trace_core`**, deployed under four entrypoints plus three MCP
servers. Every process boundary has a stated technical reason; there are no others.

| Process | Reason it is separate | Why it is not merged |
|---|---|---|
| `trace-gateway` | Hot path, ~100 ms p99 budget, stateless, scales on TPS | Must never share a thread pool, connection pool or GC pause with 60-second LLM investigations |
| `trace-api` | Control plane; scales on analyst concurrency | Different SLO (seconds), different authz surface (human RBAC vs service tokens), different availability class |
| `trace-worker` | Investigations: 30–120 s, LLM-bound, checkpointed, retried; scales on queue depth | A long-running non-idempotent workload inside an HTTP server loses work on restart and truncates on request timeout |
| `trace-stream` | JVM runtime, checkpointed, event-time stateful | Cannot run inside a Python web process |
| `trace-ui` | Different runtime | — |
| 3 MCP servers | A genuine protocol + serialization boundary for tool domains that must be independently addressable, independently authorized, and federatable by AgentCore later | Merging collapses the boundary the design asserts, and turns the Phase 12 Gateway step into a rewrite |

`trace-gateway` and `trace-api` are **the same codebase, two ASGI apps**, collapsible into one process
by `TRACE_SINGLE_PROCESS=1` for laptop development and split in `compose.full` / Kubernetes. The split
is a deployment choice, not a rewrite — that is the modular monolith paying off.

**Worker communication** is a Postgres-backed durable queue (`SELECT … FOR UPDATE SKIP LOCKED`), not
HTTP: investigations must survive worker crashes, and the queue row and the case row must move in one
transaction (ADR-0007).

```
packages/trace_core/
  domain/        entities, value objects, state machines, enums, errors
  contracts/     pydantic v2 models = the versioned wire contracts
  features/      online + offline feature definitions (single source of truth)
  rules/         deterministic rule engine
  scoring/       model loading, calibration, ensemble
  evidence/      evidence ledger, provenance, trust tiers
  tools/         registry, middleware chain, in-process + MCP client adapters
  agents/        agent specs, prompts, runtime adapters
  orchestration/ LangGraph graphs, evidence-gap router, budget manager
  policy/        deterministic policy engine (non-LLM)
  actions/       proposal to validation to execution to verification
  audit/         hash-chained audit log
  repositories/  Postgres / Redis / Neo4j / Delta adapters (ports + impls)
  observability/ OTel setup, metrics, PII-safe logging
  llm/           LLMProvider port + tier-tagged adapters
```

---

## 3. Data flow — the dual-path decision

The single most important architectural call: **Spark is not on the hot path.** Micro-batch latency
cannot serve a 100 ms authorization decision. Conflating the two produces either a slow product or
wrong features.

```mermaid
flowchart TD
  TX[transaction] --> GW[trace-gateway]
  GW --> RF[Redis online features]
  RF --> RU[rule engine]
  RU --> ML[LightGBM + IsolationForest]
  ML --> ENS[ensemble and band]
  ENS -->|RiskDecision, p99 under 100ms| CALLER[caller]

  ENS -->|tx.raw / tx.scored| K[(Kafka)]
  K --> BR[Bronze Delta<br/>raw + envelope]
  BR --> SI[Silver Delta<br/>dedup, watermark, typed]
  SI --> GO[Gold Delta<br/>event-time aggregates]
  GO -->|reconcile| RF
  GO --> GS[graph sync]
  GO --> TR[training sets]

  ENS -->|HIGH or CRITICAL| Q[(investigation queue)]
  Q --> WK[trace-worker LangGraph]
  WK --> DEC[decision + evidence ledger]
  DEC --> POL[policy engine]
  POL --> APR[human approval]
  APR --> EX[idempotent executor]
  EX --> AUD[(hash-chained audit)]
```

**Why Spark still earns its place:** it owns *correctness*. Redis counters are fast and approximate and
drift under out-of-order, late, and duplicate events. Spark recomputes the same features with
watermarking and exact dedup, writes authoritative Gold values, and **reconciles Redis**. Divergence
between the two is a monitored metric (`feature_parity_drift`), not a hidden bug.

**Degraded mode:** with the `streaming` profile down, the hot path is fully functional on Redis alone.
A `FeatureSource` marker records `ONLINE_ONLY` vs `RECONCILED`, so degradation is visible, never silent.

---

## 4. Event flow

Backbone: Kafka (KRaft, single broker locally). Justified by replay-from-offset for evaluation reruns,
partition-key ordering guarantees, and being the canonical Spark Structured Streaming source (ADR-0006
records Redpanda as a documented alternative if the RAM ceiling binds).

| Topic | Key | Partitions (local/cloud) | Retention | Flow |
|---|---|---|---|---|
| `tx.raw.v1` | `account_id` | 6 / 24 | 7 d | gateway → stream |
| `identity.events.v1` | `account_id` | 3 / 12 | 30 d | gateway → stream |
| `device.events.v1` | `device_id` | 3 / 12 | 30 d | gateway → stream |
| `tx.scored.v1` | `account_id` | 6 / 24 | 7 d | gateway → stream, triage |
| `investigation.requested.v1` | `investigation_id` | 3 / 6 | 30 d | triage → worker |
| `investigation.events.v1` | `investigation_id` | 3 / 6 | 90 d | worker → api, audit, eval |
| `action.proposed.v1` | `investigation_id` | 3 / 6 | 90 d | worker → policy |
| `action.executed.v1` | `action_id` | 3 / 6 | ∞ compacted | executor → audit |
| `audit.v1` | `entity_id` | 3 / 6 | ∞ | all → audit sink |
| `*.dlq` | original key | 1 / 3 | 30 d | poison messages, with full context |

Envelope, keying and evolution rules: `docs/EVENT_CONTRACTS.md`.

---

## 5. Storage architecture

| Store | Holds | Why this store |
|---|---|---|
| PostgreSQL 16 | Cases, investigations, evidence ledger, hypotheses, actions, approvals, audit chain, work queue, users/roles, model pointers, eval runs | Transactional integrity between case state and queue state; RLS; system of record |
| Redis 7 | Online features (sorted sets for sliding windows, HLL for distinct cardinality, hashes for profiles), rate limits, idempotency keys, agent capability tokens | Sub-millisecond hot-path reads; every structure O(log n) or better |
| Delta Lake 4.0.1 | Bronze/Silver/Gold medallion, training sets, feature snapshots, eval artifacts | ACID on object storage, time travel (reproducible training sets), schema enforcement. Layout per ADR-0015 — **not** assumed |
| Neo4j 5 | Entity graph and community structure | Variable-length path queries; behind `GraphStore` port |
| MLflow | Experiments, registry, params/metrics | Reproducibility. **Training-time only** — serving loads a digest-pinned artifact |

### Schema separation is a security control, not organization

| Schema | Owner | `trace_app` | `trace_eval` |
|---|---|---|---|
| `app` | `tracex_owner` | SELECT/INSERT/UPDATE | SELECT |
| `audit` | `tracex_owner` | INSERT only | SELECT |
| **`groundtruth`** | `tracex_owner` | **no grant at all** | SELECT |
| `eval` | `tracex_owner` | none | SELECT/INSERT |
| `external` | `tracex_owner` | none | SELECT/INSERT |

The application and every agent connect as `trace_app`. Ground truth is not hidden by convention; it is
**unreachable**. A test asserts `permission denied for schema groundtruth`, and that test is a release
blocker. Schemas, roles and grants are created **only** by Alembic
(`migrations/versions/0001_schemas_roles_grants.py`) — one definition, so the control cannot drift. `eval` holds two disjoint result families that are never unioned: `eval.synthetic_runs`
(Track A) and `eval.external_runs` (Track B).

---

## 6. Streaming architecture

Event-time semantics, explicitly implemented:

- `withWatermark("occurred_at", "10 minutes")` on every stateful stage.
- Deduplication via `dropDuplicatesWithinWatermark(["event_id"])`.
- Late data beyond the watermark is routed to a `late_events` Delta table and counted — **never
  silently dropped**.
- All aggregation is event-time windowed. Processing time is never used for business logic.
- Checkpoints per query under `_checkpoints/{query}/`; `availableNow` trigger for batch replay.
- Stateful operations use `flatMapGroupsWithState` with explicit TTL.
- **Data layout is deliberately undecided in code** — it is table DDL configured per ADR-0015, chosen
  by benchmark, and permitted to differ between local OSS Delta and Databricks.
- Local caps sized for an 8 GB Docker VM: `spark.sql.shuffle.partitions=8`, driver 2g, executor 2g.
- **Track B reuse:** IEEE-CIS loads through the *same* medallion code via `IeeeCisAdapter` in
  `availableNow` batch mode. No parallel pipeline exists.

Full detail: `docs/DATA_ENGINEERING.md`.

---

## 7. ML architecture

Three signals with deliberately different failure modes:

1. **Rules** — deterministic, YAML-declared, versioned, hot-reloadable, individually testable. Zero
   latency, full explainability. Always runs.
2. **LightGBM** (ADR-0011) — supervised, trained on Gold with time-based splits.
3. **Isolation Forest** (ADR-0012) — unsupervised, plus a per-account **robust z-score control**. An
   anomaly detector that cannot beat a z-score is not earning its place; that comparison is in the
   eval harness.

Non-negotiables: temporal splits only (random splits leak); point-in-time joins so features are
computed *as of* transaction time; isotonic calibration with a reported Brier score and reliability
curve; a digest-pinned serving artifact that refuses to boot if corrupt; PSI drift monitoring.

PR-AUC is the primary selection metric because the positive class is rare (roughly one in two
hundred transactions). ROC-AUC is reported but never selected on — it is uninformative at that
level of imbalance.

**Generalization is a separate claim** established only by Track B — a model trained and tested on
synthetic data has demonstrated that the *pipeline* works, not that the *model* works.

---

## 8. Agent architecture

### The routing principle

Agents do not call each other, and no LLM picks the next agent. The graph is driven by an
**evidence-gap ledger**:

```
open_gaps = ⋃ h.required_evidence for h in hypotheses if h.status == OPEN
          ∪ ⋃ c.required_refutation_evidence for c in open_challenges

eligible(a) ⟺ a.produces_evidence ∩ open_gaps ≠ ∅
            ∧ a.required_evidence ⊆ collected_kinds
            ∧ a.invocations < a.max_invocations
            ∧ budget_remaining(steps, tokens, usd, wall_clock)

next = argmax_{a ∈ eligible} priority(a, open_gaps)   # deterministic tie-break by agent_id
```

The **LLM** proposes and revises *hypotheses* and interprets evidence. The **router** performs the
mechanical map from gaps → agents. The path genuinely varies run to run, while the reachable state
space stays finite, auditable and replayable.

```mermaid
stateDiagram-v2
  [*] --> INIT
  INIT --> PLAN
  PLAN --> ROUTE
  ROUTE --> INVOKE_AGENT
  INVOKE_AGENT --> VALIDATE_OUTPUT
  VALIDATE_OUTPUT --> RETRY_OR_ABSTAIN : schema invalid
  RETRY_OR_ABSTAIN --> ROUTE
  VALIDATE_OUTPUT --> INTEGRATE_EVIDENCE
  INTEGRATE_EVIDENCE --> UPDATE_HYPOTHESES
  UPDATE_HYPOTHESES --> SKEPTIC_GATE
  SKEPTIC_GATE --> ROUTE : challenge creates new gaps
  SKEPTIC_GATE --> DECIDE : gaps empty or budget exhausted
  DECIDE --> PROPOSE_ACTIONS
  PROPOSE_ACTIONS --> [*]
  DECIDE --> HUMAN_QUEUE : INSUFFICIENT_EVIDENCE
  HUMAN_QUEUE --> [*]
```

**Termination is guaranteed** by four independent bounds — max steps (30), wall-clock (120 s), cost
(default $0.25), and monotonic per-agent invocation caps. A budget-exhausted investigation is a valid
recorded outcome, never a hang.

### Agent roster

| Agent | Produces evidence | Tools | Budget | On failure |
|---|---|---|---|---|
| Orchestrator | — (routes only) | **none — cannot retrieve** | 30 steps, 120 s | FAIL |
| Behavioral | `SPEND_PROFILE`, `VELOCITY`, `AMOUNT_ANOMALY`, `ACCOUNT_TENURE` | in-process feature reads | 6 calls, 20 s | DEGRADE |
| Device & Identity | `DEVICE_NOVELTY`, `DEVICE_SHARING`, `IP_REPUTATION`, `IDENTITY_CHANGE` | `identity-mcp` | 6, 20 s | DEGRADE |
| Merchant Intelligence | `MERCHANT_RISK`, `MCC_ANOMALY`, `MERCHANT_PATTERN` | `fraud-intelligence-mcp` | 5, 20 s | DEGRADE |
| Graph Investigation | `GRAPH_CLUSTER`, `RING_SCORE`, `LINK_PATH` | `graph-mcp` | 5, 30 s | DEGRADE |
| Historical Case | `HISTORICAL_MATCH`, `PRIOR_OUTCOME` | `fraud-intelligence-mcp` | 4, 20 s | DEGRADE |
| Skeptic / Adversarial | `CHALLENGE`, `ALTERNATIVE_EXPLANATION` | ledger read + `get_base_rate` | 2 rounds, 30 s | ABSTAIN (dissent recorded) |
| Decision | `DECISION` | **ledger read only — no retrieval** | 1 invocation, 30 s | FAIL → human queue |
| Remediation | `PROPOSED_ACTION` | `list_available_actions` — **cannot execute** | 1, 15 s | ABSTAIN |

The Orchestrator has no tools so it cannot contaminate evidence. The Decision agent has no retrieval
tools so decisions are reproducible from the stored ledger. The Remediation agent proposes only.

### Dynamic investigation — worked example

```
Device agent finds a device shared by 6 accounts
  → Evidence(DEVICE_SHARING) + Hypothesis(FRAUD_RING, required=[GRAPH_CLUSTER, HISTORICAL_MATCH])
  → gap GRAPH_CLUSTER → router selects Graph agent
  → Evidence(GRAPH_CLUSTER: 4 accounts, density 0.7)
  → gap HISTORICAL_MATCH → router selects Historical Case agent
  → Skeptic: "shared device may be a family plan — check tenure and geo dispersion"
     → Challenge(required_refutation=[ACCOUNT_TENURE, GEO_DISPERSION]) → NEW gaps → reroute
  → Behavioral agent supplies them → challenge resolved or upheld
  → Decision agent runs only when open_gaps = ∅ or the budget is exhausted
```

### LLM provider tiers (ADR-0016)

| Tier | Adapter | Purpose | May publish quality numbers? |
|---|---|---|---|
| `SMOKE` | `OllamaProvider` | Keyless local demo and development | **No** |
| `DEV` | `OpenAICompatibleProvider` | Development against a hosted model | No |
| `EVAL` | `BedrockProvider` | **The only publishable tier** | **Yes** |
| `CI` | `CassetteProvider` | Deterministic replay of recorded real traffic | No |

The local model does **not** match Bedrock quality; that is stated wherever it could mislead. Where it
is too weak to complete an investigation, the resulting `INSUFFICIENT_EVIDENCE` is designed behaviour —
the demo degrades honestly rather than faking a decision.

---

## 9. MCP and tool architecture

A tool is **not** a Python function handed to an LLM. Every tool is a declared `ToolSpec`:
`tool_id`, `version`, strict input/output schemas, `required_permissions`, `side_effects`
(WRITE tools are unreachable from any agent), `timeout_ms`, `max_rows`, `rate_limit`, `cost_class`,
`pii_policy`, and the `trust_tier` of returned data.

**Invariants.** Every tool returns an `EvidenceEnvelope{data, provenance, trust_tier, query, params,
row_count, truncated}` — never a bare string. **No LLM-authored queries exist**: there is no free-text
SQL or Cypher tool. Tool results reach the prompt only inside a `<untrusted_data>` fence.

### Real MCP boundaries, local, from Phase 5

| Server | Transport | Exposes | Consumers |
|---|---|---|---|
| `fraud-intelligence-mcp` | stdio (default) / streamable HTTP | merchant profile, merchant tx stats, MCC baseline, base rate, similar cases, case outcome | Merchant Intelligence, Historical Case, Skeptic |
| `identity-mcp` | stdio / HTTP | device profile, device accounts, IP intel, identity events | Device & Identity |
| `graph-mcp` | stdio / HTTP | the six allow-listed graph queries | Graph Investigation |

`InProcessAdapter` is **deliberately retained** for latency-sensitive feature reads and Decision-agent
ledger reads, where a protocol hop is not technically justified. Two transports existing side by side
is the point: it proves the abstraction and gives the parity suite something to compare.

**The critical invariant — behaviour is transport-independent.**

```mermaid
flowchart LR
  A[agent] --> T{transport}
  T -->|MCP stdio / HTTP| S[MCP server shell]
  T -->|in-process| D[direct call]
  S --> M
  D --> M
  subgraph M[single middleware chain — trace_core/tools/middleware.py]
    direction TB
    M1[authn: capability token] --> M2[authz: tool in allowed_tools<br/>resource in investigation scope]
    M2 --> M3[rate limit] --> M4[input schema validate]
    M4 --> M5[timeout guard] --> M6[execute]
    M6 --> M7[output schema validate] --> M8[EvidenceEnvelope + trust_tier]
    M8 --> M9[row cap / truncate] --> M10[audit emit]
  end
  M --> R[EvidenceEnvelope]
```

The MCP server is a **thin transport shell** over this chain. It cannot bypass it, because the
functions it registers are the middleware-wrapped ones — **there is no unwrapped entrypoint to
expose**, and a test asserts it.

- Capability tokens: stdio receives the token by per-session env injection at spawn; HTTP uses
  OAuth 2.1 client credentials. Same validation code path. TTL = investigation deadline.
- MCP responses are re-validated on receipt: a compromised MCP server cannot inject unvalidated
  evidence.
- `ToolTransportParitySuite` asserts byte-identical envelopes and identical authz-denial, rate-limit,
  timeout, truncation and audit behaviour across transports. This is a Phase 5 exit gate.
- Phase 12 AgentCore Gateway **federates these existing servers**, adding IAM/SigV4 and VPC reach. It
  introduces nothing new — which is why it is a configuration step, not a rewrite.

---

## 10. Graph architecture

`GraphStore` port with **two real adapters** from Phase 5 — `Neo4jGraphStore` (default) and
`PostgresGraphStore` (recursive CTEs, correct to 3 hops, used when the `graph` profile is off and in
CI). Both pass the same `GraphStoreConformanceSuite`. `NeptuneGraphStore` is a third adapter in
Phase 12 inheriting the identical suite. **An abstraction with one implementation is not an
abstraction** (ADR-0009).

Model: `(:Account)-[:USED_DEVICE]->(:Device)`, `(:Account)-[:USED_IP]->(:IP)`,
`(:Account)-[:HOLDS]->(:Card)`, `(:Account)-[:PAID {amount,ts}]->(:Merchant)`,
`(:Account)-[:LINKED_TO {reason,confidence}]->(:Account)`.

Allow-listed, parameterized queries — never LLM-generated Cypher — each with a row cap and timeout:
`shared_device_accounts(k)`, `shared_ip_cluster(window)`, `k_hop_neighborhood(id, k≤3)`,
`ring_score(account)`, `merchant_collusion_candidates(merchant)`, `shortest_link_path(a,b,max=4)`.

---

## 11. Authorization boundaries

Three independent planes (detail in `docs/SECURITY.md`):

- **A. Human RBAC** — `analyst`, `senior_analyst`, `fraud_manager`, `auditor`, `admin`. Enforced by
  FastAPI dependencies **and** Postgres RLS, so an API bug is not automatically a data breach.
- **B. Service identity** — each process has a distinct DB role and distinct Kafka ACLs. The gateway
  cannot read the evidence ledger; the worker cannot write `tx.raw.v1`; nothing but `trace_eval` reads
  `groundtruth`.
- **C. Agent capability tokens** — minted per investigation per agent, TTL = investigation deadline,
  scoped to exactly that agent's `allowed_tools` and that investigation's entity set. An agent
  physically cannot read another investigation's data.

Action authorization is a **separate** plane — §12.

---

## 12. Action execution architecture

```mermaid
flowchart TD
  A[agent] --> P[ProposedAction<br/>typed, evidence-cited]
  P --> PE[PolicyEngine — deterministic, ZERO LLM]
  PE --> V1[schema + referential validation]
  V1 --> V2[evidence sufficiency per action type]
  V2 --> V3[authority: does confidence justify blast radius]
  V3 --> V4[blast-radius calculation]
  V4 --> V5[circuit breaker]
  V5 --> RC{risk classification}
  RC -->|LOW| EX[idempotent execution]
  RC -->|MEDIUM / HIGH| HA[human approval queue]
  RC -->|PROHIBITED| REJ[reject + audit]
  HA -->|approved| EX
  HA -->|rejected / expired| REJ
  EX --> VR[verification read-back]
  VR -->|ok| AUD[(immutable hash-chained audit)]
  VR -->|mismatch| COMP[compensating action] --> ESC[ESCALATED] --> AUD
```

Raw LLM output cannot reach the executor: the executor accepts only a `ValidatedAction` carrying a
policy-engine signature, and `ValidatedAction` is **constructible only inside the policy engine** —
enforced by types, not discipline. Every action type declares a compensating action. Idempotency key
is `hash(investigation, action_type, target, params)`, applied through a transactional outbox for
at-most-once effect.

**Audit** is append-only and hash-chained: each row carries `prev_hash` and `chain_hash`, a trigger
forbids UPDATE/DELETE, and a verifier job detects tampering. A broken chain is a security incident.

---

## 13. Observability

**Tracing.** One `trace_id` flows HTTP ingress → Kafka header → Spark → worker → each agent node →
each tool call (including across MCP) → action execution. An investigation is one distributed trace.

**Metrics.** Hot path: `tx_score_latency_seconds`, `tx_scored_total{band}`, `degraded_mode_total{reason}`.
Streaming: consumer lag, batch duration, `late_events_total`, `dedup_dropped_total`,
`feature_parity_drift{feature}`. Agents: `investigation_duration_seconds`,
`agent_invocations_total{agent,outcome}`, `tool_calls_total{tool,transport,status}`,
`tool_authorization_denied_total`, `llm_tokens_total{agent,dir}`, `investigation_cost_usd`,
`budget_exhausted_total`, `schema_validation_failures_total`, `agent_disagreement_total`,
`injection_attempts_total`. Actions: proposed/approved/rejected/executed/failed, approval latency,
override rate. ML: score distribution, PSI drift, `model_version` gauge.

**Logging.** Structured JSON with `trace_id` on every line, and **PII-aware by construction**: a
`PIIRedactingProcessor` sits in the pipeline and a unit test asserts that account numbers, PANs, emails
and IPs never appear in output.

**Dashboards, provisioned as code:** Real-Time Ops, Investigation Quality, Agent Economics, Model
Health, Security.

---

## 14. Local environment

Profiled compose; `core` is a complete working product on its own.

| Profile | Services | ~RAM | ~Disk | Degraded behaviour when off |
|---|---|---|---|---|
| `core` | postgres, redis, gateway, api, worker, ui, 3 MCP servers via stdio | 2.7 GB | ~4 GB | — |
| `streaming` | kafka (KRaft), spark master + worker | 3.5 GB | ~5 GB | Online features only; `FeatureSource=ONLINE_ONLY` |
| `graph` | neo4j | 1.5 GB | ~1 GB | `PostgresGraphStore` fallback; graph evidence confidence reduced |
| `ml` | mlflow | 0.5 GB | ~1 GB | Serving unaffected (pinned artifacts); training unavailable |
| `obs` | otel-collector, prometheus, grafana, jaeger | 1.5 GB | ~2 GB | Metrics endpoint still exposed; no dashboards |
| `llm` | ollama + a 3B-class quantized model | 1.5 GB | ~2.5 GB | Falls back to `DEV`/`CI` if available; otherwise `make demo` says so plainly |
| `mcp-http` | the 3 MCP servers as HTTP services | 0.5 GB | ~0.3 GB | stdio transport used instead (default) |

MCP servers run over **stdio by default**, spawned by the worker — a real protocol boundary at zero
container cost, which is what makes local-first MCP viable on an 8 GB VM.

Full workflow: `docs/LOCAL_DEVELOPMENT.md`.

---

## 15. AWS environment (Phase 12)

```mermaid
graph TD
  P[producers] --> NLB --> EKS[EKS: trace-gateway HPA on RPS]
  EKS --> EC[(ElastiCache Redis)]
  EKS --> RDS[(RDS Postgres Multi-AZ)]
  EKS --> MSK[(MSK Kafka)]
  MSK --> DBX[Databricks Structured Streaming]
  DBX --> S3[(S3 Delta medallion)]
  MSK --> WK[EKS: trace-worker]
  WK --> BR[Bedrock Converse + Guardrail]
  WK --> GWY[AgentCore Gateway<br/>federates existing MCP servers]
  WK --> NEP[(Neptune)]
  EKS --> OBS[CloudWatch / AMP / AMG / X-Ray]
  SM[Secrets Manager + KMS + IAM IRSA] --> EKS
```

**Bedrock AgentCore is adopted selectively** (ADR-0014): **Gateway** — which federates the three MCP
servers that already exist and already pass the parity suite — and **Observability**. **Rejected:**
Runtime (would break local-first; LangGraph runs identically everywhere), Memory (case memory must be
SQL-queryable by the eval harness), Identity (our capability-token model is finer-grained and already
audited).

Everything is `count`/`for_each`-gated so the validation window brings up a minimal footprint, proves
it, and destroys it. Budget alarm, mandatory cost tags, and a scheduled teardown Lambda as a safety net.

## 16. Databricks environment (Phase 12)

Asset Bundles (`databricks.yml`) for jobs and clusters as code. **The Spark code is identical to
local** — notebooks are thin entrypoints calling `trace_core.stream.*`. That is the guard against
"works on Databricks, unrunnable locally."

Jobs: `bronze_ingest` (continuous), `silver_transform`, `gold_features`, `graph_sync`,
`training_pipeline`, `drift_monitor`, `external_validation` (Track B).

Unity Catalog `tracex.{bronze,silver,gold,ml,external}`, with `groundtruth` in a **separately-granted**
catalog mirroring the local Postgres isolation. **Table layout is re-decided for Databricks**
(ADR-0015) — liquid clustering, with `CLUSTER BY AUTO` + predictive optimization where the runtime
supports it — rather than inheriting the local choice.

---

## 17. Trust boundaries

```mermaid
flowchart TD
  subgraph TB1[1 — untrusted event ingress]
    E[merchant_name, user_agent, memo, device_label]
  end
  subgraph TB2[2 — untrusted evidence]
    R[anything retrieved that originated in an event]
  end
  subgraph TB3[3 — LLM boundary]
    L[model output is a PROPOSAL, never an instruction]
  end
  subgraph TB4[4 — action egress]
    A[only the policy engine authorizes a side effect]
  end
  E -->|tagged trust_tier=UNTRUSTED at ingestion| R
  R -->|fenced in untrusted_data JSON, never a system message| L
  L -->|ProposedAction, typed and evidence-cited| A
```

Prompt-injection defences, all testable: trust tiers that never degrade; `<untrusted_data>` fencing;
an instruction-pattern detector whose hits are stripped, counted and attached to the case; structured
outputs so the model cannot emit an out-of-list tool call or an out-of-enum action; and a **red-team
corpus** asserting the decision distribution is statistically unchanged versus a clean control set with
zero unauthorized tool calls.

---

## 18. Failure model

| Failure | Detection | Behaviour |
|---|---|---|
| Redis down | health probe / 20 ms timeout | Hot path → rules-only, `degraded=true` on the response, alert. Never fail-open silently |
| Postgres down | connection error | Gateway 503; worker stops consuming — no work lost, the queue is in PG |
| Kafka down | producer timeout | Gateway buffers to a bounded local WAL then sheds; scoring continues |
| Model artifact missing/corrupt | digest check at boot | **Refuse to start.** A gateway serving an unknown model is worse than a down gateway |
| Spark job crash | checkpoint + supervisor | Resume from checkpoint; online store serves stale-but-flagged features |
| Neo4j down | health probe | Fall back to `PostgresGraphStore`; graph evidence confidence reduced and recorded |
| MCP server unreachable | spawn/connect timeout | Owning agent applies `on_failure` (usually DEGRADE); recorded as reduced evidence, never a silent gap |
| LLM timeout / 429 | per-call timeout + jittered retry | 2 retries → agent `on_failure` policy |
| LLM invalid schema | pydantic validation | One reprompt with the validation error → then ABSTAIN. Never parse loosely |
| Agent exceeds budget | budget manager | Agent halted mid-flight, partial evidence retained, investigation continues |
| Investigation exceeds budget | deadline | Forced `INSUFFICIENT_EVIDENCE` → human queue |
| Worker crash mid-investigation | queue lease expiry | Another worker resumes from the LangGraph checkpoint, not from scratch |
| Action execution fails | verification read-back | Compensate → `ESCALATED` → page |
| Duplicate event | `event_id` dedup (Redis + Spark) | Idempotent; counted |
| Poison message | 3 failures | → DLQ with full context; never blocks the partition |
| Audit chain broken | verifier job | **Page immediately.** Treated as a security incident |

**Fail-safe direction is explicit and asymmetric.** Scoring fails **open** (approve + flag) because
declining all traffic is worse than missing fraud for a few minutes — and every fail-open decision is
tagged and counted. Actions fail **closed** — never execute under uncertainty.

Every row above has a chaos test (`docs/TESTING.md`). Operator response: `docs/OPERATIONS.md`.
