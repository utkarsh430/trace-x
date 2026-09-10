# TRACE-X

**Real-time fraud detection and autonomous multi-agent investigation platform.**

TRACE-X ingests transaction and identity events, scores them synchronously, opens investigations on
suspicious ones, and runs a bounded multi-agent investigation that gathers evidence, challenges its own
hypotheses, and produces an evidence-backed decision a human can audit or override.

It is built to demonstrate **two** competencies that must both be genuine: production data engineering
(Kafka, Spark Structured Streaming, Delta Lake, Databricks) and production agentic AI (LangGraph, MCP
tool boundaries, bounded autonomy, action safety). Neither is decoration for the other.

> **Status: Phase 0 — Foundation.** The engineering control plane is in place and verified.
> Product functionality has not been implemented yet. See [`docs/PROGRESS.md`](docs/PROGRESS.md) for
> exact current state and [`docs/ROADMAP.md`](docs/ROADMAP.md) for the phase gates.

---

## Quick start

No cloud account and no API key required.

```bash
make setup     # Python 3.12 venv + dev dependencies, creates .env
make doctor    # preflight: version pins, disk, Docker RAM, ports, LLM tier
make up        # core profile: postgres + redis
make verify    # ★ canonical health check
```

`make verify` is the single command that answers *"is this repository healthy?"*.

---

## The four hard problems

This is not a wrapper around an LLM. The architecture exists to solve four specific problems:

**1. Two incompatible latency regimes.** An authorization decision must return in tens of milliseconds.
Event-time-correct aggregation with watermarks, late data and deduplication cannot. TRACE-X splits them:
a **hot path** (Redis + rules + ML, synchronous), a **warm path** (Spark, which owns correctness and
reconciles the online store), and a **cold path** (agents, asynchronous). Divergence between the online
and offline feature paths is a monitored metric, not a hidden bug. → [ADR-0002](docs/adr/0002-dual-path-spark-not-on-hot-path.md)

**2. Non-predetermined investigation paths that are still bounded.** Agents never call each other, and
no LLM picks the next agent. A deterministic router maps **open evidence gaps** to eligible agents; the
LLM proposes and revises hypotheses. The path genuinely varies with the evidence while the reachable
state space stays finite, auditable and replayable. Four independent budget bounds guarantee
termination. → [ADR-0019](docs/adr/0019-evidence-gap-routing.md)

**3. Untrusted data reaching a system that can propose financial actions.** Merchant names, user-agents
and memo fields are attacker-controlled. They are tagged `UNTRUSTED` at ingestion, never lose the tag,
and reach a model only inside a fenced JSON block. Raw LLM output cannot reach the executor: it accepts
only a `ValidatedAction`, which is **constructible only inside the policy engine**. → [ADR-0023](docs/adr/0023-action-safety-human-approval.md)

**4. Benchmarks that mean something.** Two rigorously separated tracks: a **controlled synthetic causal
benchmark** where the generator records which facts actually explain each injected fraud (making
evidence precision/recall computable without an LLM judge), and **external validation on IEEE-CIS** for
generalization. Seven arms including two ablations. No number may be published unless it cites a
resolvable run manifest — enforced by `make check-claims`. → [ADR-0021](docs/adr/0021-two-track-validation.md)

---

## Non-negotiables

These are enforced by code and tests, not by convention:

- **Ground truth is unreachable by the application.** Separate schema, separate role, no grant. A
  release-blocking test asserts `permission denied for schema groundtruth`. Leakage would silently
  invalidate every metric in the project.
- **No benchmark number may be invented — or be unreproducible.** Every run emits a 25-field manifest;
  every published number must cite one; only the `EVAL` tier may publish quality numbers.
- **Unfavourable results are published as found.** A failed ablation, an anomaly detector beaten by a
  z-score, or a large synthetic-to-real transfer gap are reported, not re-framed.
- **Code written ≠ feature complete.** A capability reaches `PASS` only with executable evidence, and
  the tooling refuses to record `PASS` without it.
- **The end-to-end path is never mocked.** A CI job greps for it and fails.

---

## Documentation

| Document | Purpose |
|---|---|
| [`CLAUDE.md`](CLAUDE.md) | **Project constitution** — rules, invariants, prohibited shortcuts |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Architecture specification with diagrams |
| [`docs/ROADMAP.md`](docs/ROADMAP.md) | Phase gates: entry, implementation, tests, targets, exit |
| [`docs/PROGRESS.md`](docs/PROGRESS.md) | **Live state** — what works, what fails, what is next |
| [`docs/TESTING.md`](docs/TESTING.md) | Test strategy and the Definition of Done |
| [`docs/EVALUATION.md`](docs/EVALUATION.md) | Metrics, arms, manifests, benchmark integrity |
| [`docs/SECURITY.md`](docs/SECURITY.md) | Threat model, trust boundaries, authorization planes |
| [`docs/DATA_ENGINEERING.md`](docs/DATA_ENGINEERING.md) | Medallion, event-time semantics, version pins |
| [`docs/API_CONTRACTS.md`](docs/API_CONTRACTS.md) · [`docs/EVENT_CONTRACTS.md`](docs/EVENT_CONTRACTS.md) | Versioning and compatibility policy |
| [`docs/LOCAL_DEVELOPMENT.md`](docs/LOCAL_DEVELOPMENT.md) | Running it locally |
| [`docs/OPERATIONS.md`](docs/OPERATIONS.md) | Runbook: failure playbooks and escalation |
| [`docs/adr/`](docs/adr/README.md) | 26 architecture decision records |

---

## Stack

**Data:** Kafka (KRaft) · Spark Structured Streaming 4.0.1 · Delta Lake 4.0.1 · Databricks
**Backend:** Python 3.12 · FastAPI · PostgreSQL 16 · Redis 7
**ML:** LightGBM · Isolation Forest · MLflow
**Agents:** LangGraph · MCP · AWS Bedrock (+ selective AgentCore)
**Graph:** Neo4j, behind a `GraphStore` port with a Postgres adapter and a Neptune adapter
**Infra:** Docker · Kubernetes · Terraform · GitHub Actions
**Observability:** OpenTelemetry · Prometheus · Grafana

Every choice above is recorded in an ADR with the alternatives that were rejected and why.

---

## Contributing

Read [`CLAUDE.md`](CLAUDE.md) first — it is the constitution, and it is binding. Then
[`docs/PROGRESS.md`](docs/PROGRESS.md) for current state, then your phase in
[`docs/ROADMAP.md`](docs/ROADMAP.md).

Run `make verify` before claiming anything works.
