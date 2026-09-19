# CLAUDE.md — TRACE-X Project Constitution

> **Stable rules. Read this first, every session.**
> This file contains rules and invariants only. It must **never** contain progress or status —
> that lives in `docs/PROGRESS.md`. Changing a rule here requires explicit user approval and,
> for anything architectural, an ADR.

---

## 1. Project purpose

TRACE-X is a real-time fraud detection and autonomous investigation platform. It ingests transaction
and identity events, scores them synchronously, opens investigations on suspicious ones, and runs a
bounded multi-agent investigation that gathers evidence, challenges its own hypotheses, and produces
an evidence-backed decision a human can audit or override.

It demonstrates **two** competencies that must both be genuine: production data engineering
(Kafka / Spark Structured Streaming / Delta / Databricks) and production agentic AI (LangGraph,
MCP tool boundaries, bounded autonomy, action safety). Neither is decoration for the other.

**This is not** a tutorial, a CRUD app, a chatbot, or a thin LLM wrapper.

---

## 2. Session protocol (mandatory)

Every session:

1. **Read** `CLAUDE.md` → `docs/PROGRESS.md` → the current phase in `docs/ROADMAP.md`.
2. **Work** only within the current phase's scope. Do not skip ahead.
3. **Run** `make verify` before claiming anything works.
4. **Update** `docs/PROGRESS.md` and `tests/acceptance/status.json` with actual results.
5. **Write an ADR** if an architectural decision was made or changed.

A cold session must be able to recover complete state from `docs/PROGRESS.md` + git history.
If it cannot, `PROGRESS.md` is defective and fixing it is the first task.

---

## 3. Architectural principles

1. **Every process boundary needs a stated technical reason.** Absent one, code belongs in the
   `trace_core` modular monolith. Microservices are not a default.
2. **Latency regimes do not mix.** The hot path (synchronous scoring) has a ~100 ms budget. The warm
   path (Spark, event-time correct) takes seconds. The cold path (investigations) takes 30–120 s.
   Never put a slow dependency on a fast path.
3. **Correctness beats throughput; observability beats cleverness; reproducibility beats both.**
4. **An abstraction with one implementation is not an abstraction.** Every port ships with at least
   two adapters and one shared conformance suite before its portability is claimed.
5. **Determinism where it is achievable.** Seeded generators, `temperature=0`, pinned digests,
   recorded cassettes.
6. **Bounded autonomy.** Agents propose; deterministic code decides and executes.
7. **Fail-safe direction is explicit and asymmetric.** Scoring fails *open* (approve + flag, counted).
   Actions fail *closed* (never execute under uncertainty).

---

## 4. Repository structure

```
CLAUDE.md                  this file — the constitution
README.md                  entry point
Makefile                   developer command interface
docs/                      the control plane (see §14)
packages/trace_core/       the modular monolith: domain, contracts, features, rules, scoring,
                           evidence, tools, agents, orchestration, policy, actions, audit,
                           repositories, observability, llm
services/{gateway,api,worker,stream}/   thin entrypoints over trace_core
mcp_servers/{fraud_intelligence,identity,graph}/   real MCP servers (ADR-0013)
apps/dashboard/            Next.js + TypeScript
data/{generator,adapters,external}/      Track A generator, SourceAdapters, Track B data (gitignored)
ml/{pipelines,models}/     training and artifacts
eval/{harness,manifest,track_a,track_b}/ benchmark harness and run manifests
benchmarks/delta_layout/   ADR-0015 evidence
tests/{unit,integration,contract,conformance,e2e,load,chaos,adversarial,transport_parity,external,acceptance}/
infra/{terraform,k8s,databricks}/
deploy/                    docker compose profiles
scripts/                   doctor, verify, acceptance, claim linter, phase guard
```

---

## 5. Approved technology choices

Locked. Deviating requires an ADR.

| Layer | Choice | Notes |
|---|---|---|
| Streaming | Kafka (KRaft), Spark Structured Streaming | ADR-0002, ADR-0006 |
| Lake | Delta Lake, medallion Bronze→Silver→Gold | ADR-0005 |
| Online store | Redis (sorted sets, HLL, hashes) | ADR-0003 |
| System of record | PostgreSQL | ADR-0001, ADR-0004 |
| Work queue | PostgreSQL `SELECT … FOR UPDATE SKIP LOCKED` | ADR-0007 |
| ML | LightGBM + Isolation Forest + robust-z control, MLflow | ADR-0011, ADR-0012 |
| Graph | Neo4j + `GraphStore` port (Postgres, Neptune adapters) | ADR-0009 |
| Agents | LangGraph | ADR-0010 |
| LLM | `LLMProvider` port, four tiers | ADR-0016 |
| Tools | `ToolSpec` + MCP and in-process adapters | ADR-0013 |
| Cloud | AWS + Databricks, Terraform | ADR-0014, ADR-0025 |
| Backend | Python 3.12 / FastAPI. **Go only in optional Phase 13.** | ADR-0008 |
| Frontend | Next.js + TypeScript | |

### Version pin matrix (ADR-0018) — asserted by `make doctor`

| Component | Pin | Why it matters |
|---|---|---|
| Python | 3.12 | |
| Java | **Temurin 17** | Spark 4.0 supports Java 17/21 **only**. Java 25 fails with an opaque error. |
| Spark | 4.0.1 | |
| Delta | 4.0.1 | Requires Hadoop 3.4.x with Spark 4.0.1. |
| Hadoop | 3.4.x | Mismatch surfaces as `NoSuchMethodError`. |
| Scala | 2.13 | |

Changing any pin invalidates prior benchmark comparability and **requires** a manifest-diff note in
the next evaluation report.

---

## 6. Coding conventions

- **Typed everywhere.** `mypy` is strict on `trace_core`: no untyped defs, no implicit `Any` generics.
- **Pydantic v2** for all wire contracts and tool I/O. Strict models; `extra="forbid"`.
- Line length 100. `ruff format` is authoritative — never hand-format.
- Prefer pure functions in `domain/`; push I/O to `repositories/`.
- **Ports and adapters.** Domain code depends on protocols, never on a driver.
- Errors are typed exceptions from `trace_core.domain.errors`. Never `except Exception: pass`.
- No `print` in library code — use the structured logger. (`ruff` rule `T20` enforces this.)
- Time is always timezone-aware UTC. Event time and processing time are distinct fields, never
  interchanged.
- Money is integer minor units (`amount_minor`). **Never a float.**
- Naming: `snake_case` Python, `camelCase` TypeScript, `SCREAMING_SNAKE` env vars,
  `kebab-case` file paths and topic names.
- **Data-platform identifiers are `snake_case`.** The kebab-case path convention applies to
  human-authored repository filesystem paths. Persisted or externally addressable data-platform
  identifiers follow `snake_case` when they must stay compatible and identical across Spark, Delta
  Lake, Unity Catalog, Databricks, checkpoints and transaction identifiers: schema and table
  identifiers, Structured Streaming query names, checkpoint logical identifiers, Delta transaction
  app ids, Databricks job and task identifiers derived from pipeline names, and the metric and
  manifest identifiers that represent the same logical name (`silver.late_events`, `bronze_ingest`,
  `gold_tx_features`). Physical lake directories derived from table identifiers keep the same
  spelling (`silver/late_events/`). No mapping layer between spellings (ADR-0048, U9).

---

## 7. API and event contract rules

- **Contracts are the source of truth, and they are generated then committed.**
  `docs/contracts/openapi.yaml` and `docs/contracts/events/*.json`. CI diffs them; a breaking change
  without a version bump fails the build.
- Event schemas are JSON Schema → Pydantic is generated **from** them, never the reverse.
- **Backward-compatible evolution only.** A breaking change means a new `.v2` topic and a dual-write
  window. Never mutate a released schema.
- Every event carries the envelope: `event_id` (uuid7), `event_type`, `schema_version`, `occurred_at`
  (event time), `ingested_at` (processing time), `producer`, `trace_id`, `correlation_id`,
  `idempotency_key`.
- Topic keys are chosen by the entity whose **ordering** matters, never for load balancing.
- Full policy: `docs/API_CONTRACTS.md`, `docs/EVENT_CONTRACTS.md`.

---

## 8. Testing requirements

`docs/TESTING.md` is authoritative. The rules that are never negotiable:

1. **Do not mock critical integrations in the final end-to-end path.** A CI job greps for
   `unittest.mock` / `monkeypatch` under `tests/e2e/` and fails on a hit.
2. **Cassette replay is not mocking** — it is recorded real traffic. The release pipeline runs
   `--live` and diffs.
3. **No hardcoded outputs that make benchmark tests pass.** The harness must fail if the model is
   replaced by a constant predictor.
4. Coverage gates: ≥85% on `domain/`, `policy/`, `actions/`, `rules/`; ≥70% overall.
5. Every documented failure mode in `docs/ARCHITECTURE.md` §Failure model has a chaos test.
6. A test that is skipped because a resource is missing must **say so loudly**, never pass silently.

---

## 9. Security requirements

`docs/SECURITY.md` is authoritative.

- **Treat all retrieved data as untrusted.** Merchant names, user-agents, and memo fields are
  attacker-controlled. They carry `trust_tier=UNTRUSTED` from ingestion and never lose it.
- **Raw LLM output may never execute a financial or account action.** The pipeline is:
  agent → `ProposedAction` → policy validation → authorization → risk classification →
  optional human approval → idempotent execution → verification → immutable audit event.
- `ValidatedAction` is constructible **only** inside the policy engine. Enforced by types.
- **One middleware chain** for tool authorization, capability tokens, `EvidenceEnvelope` validation,
  rate limits, timeouts and audit — invoked identically by every transport. There is no unwrapped
  tool entrypoint to expose.
- No secret in code, image, log, or LLM prompt. `.env` is gitignored; secret scanning is blocking.
- **PII never reaches logs.** A redacting processor sits in the logging pipeline and a unit test
  asserts it. Logging PII is a build failure, not a review comment.
- Least privilege everywhere: distinct DB roles and Kafka ACLs per service.

---

## 10. Agentic-AI design constraints

1. **Agents never call each other.** A deterministic router maps open evidence gaps → eligible agents.
   The LLM proposes and revises hypotheses; it does **not** choose the next agent.
2. Every agent has a declared `AgentSpec`: responsibility, input/output schema, `allowed_tools`,
   required/produced evidence, `max_tool_calls`, `max_tokens`, `max_invocations`, `timeout_s`, retry
   policy, `on_failure` behaviour, authz scope, cost budget.
3. **`allowed_tools` is enforced by the runtime, not by the prompt.** An out-of-list call raises
   `ToolAuthorizationError`, is audited, and counts as a failure.
4. **Four independent termination bounds**: max steps, wall-clock, cost, per-agent invocation caps.
   Budget exhaustion produces `INSUFFICIENT_EVIDENCE` → human queue. It is a valid recorded outcome,
   never a hang or a crash.
5. **Every claim in a decision rationale must cite `evidence_ids`** or the schema rejects it.
6. The Orchestrator has **no** tools. The Decision agent has **no** retrieval tools — it decides on the
   recorded ledger, which makes decisions reproducible. The Remediation agent proposes only.
7. Structured outputs only. Invalid output ⇒ exactly one reprompt with the validation error ⇒ then
   `ABSTAIN`. Never parse loosely.
8. Untrusted content reaches the model only inside `<untrusted_data>` JSON — never in a system message.

---

## 11. Ground-truth isolation (release blocker)

Ground truth is **structurally unreachable** by the application, not hidden by convention.

- Labels, `fraud_pattern`, and `causal_evidence_keys` live in the Postgres schema `groundtruth`.
- The application role `trace_app` has **no grant on that schema at all**.
- Only `trace_eval` may read it. The evaluation harness connects as `trace_eval`.
- A test asserts `trace_app` receives `permission denied for schema groundtruth`. **That test failing
  blocks release.** Leakage silently invalidates every metric in the project.
- Agents are never given ground truth in any form, including indirectly through a feature.

---

## 12. Local-first requirement

- **`make up` must work on a laptop with no cloud account and no API key.**
- Compose is profiled: `core` (always) + `streaming` / `graph` / `ml` / `obs` / `llm` / `mcp-http`
  opt-in. Every profile has a documented degraded mode, so `core` alone is a working product.
- MCP servers default to **stdio** — a real protocol boundary at zero container cost.
- The default LLM tier is `SMOKE` (local Ollama). A new contributor clones and runs `make demo`
  without purchasing anything.
- Paid AWS/Databricks resources are required only for cloud validation (Phase 12), never for
  ordinary development.

---

## 13. Reproducibility and benchmark integrity

**No benchmark number may be invented. Ever.**

1. Every evaluation run emits a complete `RunManifest` (dataset/generator/scenario digests, Spark /
   Delta / Hadoop / Java / Python versions, env-lock and image digests, model + calibration digests,
   provider / tier / model-id / inference config, agent-graph / spec / prompt / tool-contract / MCP /
   policy versions, git SHA, dirty-worktree flag). **A run that cannot produce one is not a valid run.**
2. **No number appears in `README.md` or `docs/**` unless it cites a `run_id` that resolves.**
   Enforced by `make check-claims`.
3. Quality numbers may only come from the `EVAL` tier. `SMOKE` / `DEV` / `CI` may publish latency,
   cost and operational metrics only.
4. A run recorded with `dirty_worktree: true` is not publishable.
5. **Track A (synthetic causal benchmark) and Track B (external real-world data) are never conflated.**
   Track A measures agent reasoning quality under known causal ground truth. Track B measures ML
   generalization on IEEE-CIS. A Track-B `run_id` may never be cited beside an agent-quality metric.
6. **No document may imply that synthetic benchmark accuracy represents real-world fraud performance.**
   Every report header carries the standing caveat verbatim (see `docs/EVALUATION.md`).
7. A large synthetic-to-real transfer gap is an **expected, publishable, acceptable** result.
   Concealing or re-framing it is a violation of this constitution.

---

## 14. Documentation rules

| Document | Role | Update cadence |
|---|---|---|
| `CLAUDE.md` | Constitution — rules and invariants | Rarely; requires user approval |
| `docs/ARCHITECTURE.md` | Architecture specification | When the design changes |
| `docs/ROADMAP.md` | Phase gates | When a gate changes |
| `docs/PROGRESS.md` | **Live state** | **Every session** |
| `docs/TESTING.md` | Test strategy + Definition of Done | When a test layer changes |
| `docs/EVALUATION.md` | Metrics, arms, manifests, integrity rules | When evaluation changes |
| `docs/SECURITY.md` | Threat model and controls | When a boundary changes |
| `docs/API_CONTRACTS.md` | API versioning policy | When the policy changes |
| `docs/EVENT_CONTRACTS.md` | Event versioning policy | When the policy changes |
| `docs/DATA_ENGINEERING.md` | Medallion, event-time, pins, layout | When the pipeline changes |
| `docs/FRAUD_SCENARIOS.md` | The ten Track A fraud scenarios and their causal evidence keys | When a scenario changes |
| `docs/LOCAL_DEVELOPMENT.md` | How to run it | When the workflow changes |
| `docs/OPERATIONS.md` | Runbook | When a failure mode changes |
| `docs/adr/NNNN-*.md` | Decision records | One per decision |
| `tests/acceptance/status.json` | Machine-readable capability status | Every session |

**Rules.** Code and `ARCHITECTURE.md` disagreeing is a bug in one of them. ADRs are **immutable once
accepted** — supersede with a new ADR, never edit. `PROGRESS.md` records reality including failures;
an over-optimistic `PROGRESS.md` is worse than none.

---

## 15. Commands

```bash
make doctor         # preflight: pins, disk, RAM, ports, LLM tier
make setup          # venv + dev dependencies
make up / down / ps / logs
make lint typecheck secrets audit
make test-fast      # unit + property + contract + conformance
make test           # everything except cloud
make verify         # ★ CANONICAL health check — run before any completion claim
make check-claims   # benchmark-integrity linter
make acceptance     # capability status report
make eval / eval-external / demo / seed / e2e / bench-layout   # phase-gated
```

Commands whose phase has not landed **exit non-zero with a clear message**. They never pretend to
succeed.

---

## 16. Definition of Done

**CODE WRITTEN ≠ FEATURE COMPLETE.**

A capability is done only when all seven hold:

1. **Implementation** exists and is typed.
2. **Tests** exist at the layers `docs/TESTING.md` requires — including the failure path.
3. **Actually executed** — the command was run and its real output recorded.
4. **Failure-path validated** — the documented degraded/failure behaviour was triggered and observed.
5. **Observable** — emits the metrics, traces and structured logs the design requires.
6. **Documented** — the relevant doc updated; an ADR written if a decision was made.
7. **Phase exit criteria** in `docs/ROADMAP.md` are met.

Only then may `tests/acceptance/status.json` record `PASS`, and it must carry the evidence.

---

## 17. Prohibited shortcuts

Never:

- Mark a capability `PASS` without executable evidence.
- Mock a critical integration in the end-to-end path to make a test pass.
- Hardcode a value that makes a benchmark or acceptance test pass.
- Invent, estimate, round up, or carry forward an unverified benchmark number.
- Publish a number without a resolvable `run_id`, or a quality number from a non-`EVAL` tier.
- Imply synthetic results predict real-world performance.
- Conceal or re-frame an unfavourable result — including a failed ablation or a large transfer gap.
- Expose ground truth to an agent, or grant `trace_app` access to `groundtruth`.
- Let raw LLM output reach an executor, or construct `ValidatedAction` outside the policy engine.
- Add a tool entrypoint that bypasses the middleware chain.
- Concatenate untrusted retrieved text into a prompt outside `<untrusted_data>`.
- Add a network boundary without a stated technical reason, or a dependency without an ADR.
- Silently reduce a requirement, quietly narrow scope, or skip a phase gate.
- Log PII or commit a secret or a dataset.
- Write documentation describing something that does not exist as though it does.
- Claim completion while any part of the requested scope is unfinished or unverified.

---

## 18. Working protocol (user-issued operating model)

**Ownership.** The lead session owns implementation strategy and sequencing (data structures,
algorithms, SQL, Spark, Kafka, Redis, tests, benchmarks, refactors), decides them, and says what it
decided. It escalates **only** external/event/schema contract changes, product behaviour or feature
semantics, security/auth boundaries, data-loss guarantees, persistence topology, destructive
migrations, changes to accepted evaluation criteria, large scope increases, or paid cloud cost — as
*Problem / Evidence / Recommendation / Why / Alternatives / Cost-risk / What continues meanwhile*,
always with a recommendation.

**Failure classes.** Classify every failure before fixing it: **A** product/correctness/security
defect, **B** implementation bug, **C** stale test, **D** harness/tooling, **E** machine/environment,
**F** expected consequence of an approved change. Apply the smallest fix for the class. Never redesign
production code to satisfy a broken test; never weaken a valid test to hide a defect. Reproduce a
Category A defect before fixing it.

**Step reports.** End each step with *IMPLEMENTED / EVIDENCE / DECISIONS / DEBT / NEXT / ESCALATION*.

**Heavy runs.** Spark, Kafka, chaos suites and benchmarks never run concurrently. Serialise them on a
lock whose owner file names a pid, and release it only when it names your own. A background wrapper
propagates the real exit code (`rc=$?; …; exit $rc`), never a trailing command's. Lint with the gate's
exact invocation, `ruff format --check . && ruff check .`. Gate every commit on `make verify`'s exit
code.

**Sub-agents.** Few, well-scoped, each in its own git worktree off the phase branch with
non-overlapping file ownership. Agents do not commit and do not edit `docs/PROGRESS.md`,
`tests/acceptance/status.json` or the ADR index; the lead integrates and commits. Never force-push.

## Accuracy-First Engineering & Diagnostic Autonomy

TRACE-X should optimize first for correctness, evidence quality, reliability,
security, and reproducibility.

Performance, simplicity, cost, and speed of implementation matter, but must not
silently reduce correctness or evaluation integrity.

### 1. Think before changing

For non-trivial failures, unexpected measurements, or ambiguous behaviour:

1. inspect the actual implementation and evidence
2. identify plausible competing hypotheses
3. challenge the first diagnosis
4. look for confounding variables
5. design the smallest controlled experiment that distinguishes the hypotheses
6. run that experiment when it is safe, reversible, and within the current phase
7. fix the root cause rather than the visible symptom
8. verify the fix with executable evidence

Do not immediately ask the user what to do when the answer can first be learned
through inspection, testing, profiling, benchmarking, or controlled experiments.

Prefer evidence over intuition.

---

### 2. Use deep reasoning on difficult problems

When a problem is technically difficult, subtle, safety-critical, performance-
critical, or capable of silently producing plausible but incorrect results,
spend additional reasoning effort before acting.

Examples include:

- concurrency and race conditions
- distributed state
- idempotency
- transaction boundaries
- cache consistency
- event ordering
- schema evolution
- authorization
- ground-truth isolation
- feature correctness
- model/evaluation leakage
- agent/tool permissions
- benchmark interpretation
- performance bottlenecks
- data distribution changes
- cross-system identifiers
- failure recovery

Do not choose the first plausible solution merely because it works.

Compare alternatives and choose the strongest evidence-backed design.

---

### 3. Proactively improve accuracy

Do not restrict yourself to making tests pass.

While implementing the authorized phase, actively look for ways to improve
correctness and accuracy without unnecessary scope expansion.

Pay particular attention to:

- silent failure modes
- plausible-but-wrong outputs
- incorrect joins or identifiers
- stale or inconsistent state
- race conditions
- data leakage
- ground-truth leakage
- temporal leakage
- incorrect feature semantics
- online/offline feature drift
- unrealistic workload assumptions
- distribution shift
- false-positive / false-negative tradeoffs
- calibration
- evidence quality
- unsupported claims
- missing negative tests
- weak invariants
- benchmark confounding
- retry/idempotency defects
- security boundaries
- degraded-mode correctness

If a low-risk local improvement materially increases correctness, testability,
or robustness and remains within the current phase, implement it.

If the improvement changes architecture or an accepted external contract,
surface it for review instead.

---

### 4. Accuracy Review

Before considering an important component complete, ask:

- Can this produce a believable but incorrect result?
- Are identifiers and references guaranteed to point to real entities?
- Are retries and concurrent requests safe?
- Are transaction boundaries correct?
- Can state become stale or partially committed?
- Can evaluation data influence runtime behaviour?
- Can hidden labels or ground truth leak?
- Are absent/missing values handled explicitly?
- Are failure modes safe and observable?
- Are tests proving behaviour rather than merely exercising code?
- Are negative and adversarial cases covered?
- Is the benchmark workload representative of what the target claims?
- Are comparisons controlled?
- Are numerical claims reproducible and backed by recorded evidence?
- Could the test itself be giving a misleading pass?

If any answer is uncertain, investigate before declaring completion.

---

### 5. Controlled experimentation

When comparing implementations, configurations, optimizations, models, agents,
or architectures, control relevant variables.

Do not infer causality from two runs that differ in multiple important ways.

Where practical:

- preserve identical datasets
- preserve identical service state
- preserve identical workload
- preserve identical hardware/resource limits
- use reproducible seeds/configuration
- record environment and commit SHA
- compare before/after using the same measurement methodology

If a comparison is confounded, say so and design a better experiment.

---

### 6. Do not game targets

Never make a target pass by:

- weakening the target after seeing the result
- silently changing the workload
- removing difficult examples
- hiding failed runs
- cherry-picking favourable measurements
- disabling correctness/security behaviour
- changing semantics to improve benchmark numbers
- presenting diagnostic measurements as acceptance evidence

A missed target is useful evidence.

Investigate it, improve the implementation where justified, and record the
truth if it remains missed.

---

### 7. Architectural evolution

Existing architecture and ADRs are the current baseline, not unquestionable
dogma.

Implementation evidence may justify improvements.

Existing architecture wins by default unless a proposed change has a materially
better evidence-backed tradeoff.

When evidence suggests an architectural improvement, evaluate:

1. current design
2. proposed design
3. evidence motivating the change
4. expected correctness/accuracy benefit
5. performance benefit
6. complexity added
7. migration cost
8. new risks
9. effect on later phases
10. recommendation

Do not silently change accepted architecture.

Use a new ADR or approved amendment where required.

---

### 8. Diagnostic autonomy

Before escalating a problem to the user, ask:

- Can I answer this by reading the code?
- Can I answer this with a test?
- Can I profile it?
- Can I run a controlled A/B experiment?
- Can I inspect runtime state?
- Can I reproduce the failure?
- Have I challenged my first explanation?
- Are there confounding variables?
- Is this actually an implementation problem, workload problem, environment
  problem, architecture problem, or acceptance-specification problem?

If safe reversible investigation can answer the question, investigate first.

Do not ask the user to choose between alternatives that evidence can eliminate.

---

### 9. When to escalate

Escalate only when the remaining decision involves one or more of:

- irreversible architectural change
- immutable external/event/API contract
- security or safety policy
- acceptance criteria changing
- destructive operation
- significant paid cloud resources
- irreversible data migration
- multiple materially different choices remain after reasonable investigation
- product/business preference
- an issue genuinely outside available evidence or capability

When escalating, provide:

1. problem
2. evidence gathered
3. hypotheses tested
4. experiments performed
5. root cause, if known
6. viable options
7. tradeoffs
8. your recommendation

Do not merely ask "what should I do?"

---

### 10. Completion standard

A feature is complete only when appropriate evidence demonstrates:

- correct implementation
- correctness tests
- negative/failure tests
- concurrency behaviour where relevant
- integration behaviour
- observability
- security boundaries
- reproducibility
- documentation
- acceptance evidence

Passing tests are necessary but not sufficient if there is evidence that the
tests do not represent the real behaviour.

At phase completion, perform one final adversarial review:

"What is most likely to be wrong even though the current suite is green?"

Investigate credible high-impact answers before declaring the phase complete.