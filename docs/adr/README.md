# Architecture Decision Records

One decision per file. **ADRs are immutable once accepted** — to change a decision, write a new ADR that
supersedes the old one and add a "Superseded by" pointer to the original. Never edit an accepted ADR.

Format: `Context · Decision · Alternatives Considered · Consequences · Status` — see
[`0000-template.md`](0000-template.md).

Every alternative listed must be one a competent engineer would genuinely propose. Straw men are worse
than no alternatives at all. Every ADR must state **negative** consequences; one without them has not
been thought through.

| ADR | Decision | Phase | Status |
|---|---|---|---|
| [0001](0001-modular-monolith-process-boundaries.md) | Modular monolith with four justified process boundaries | 0 | Accepted |
| [0002](0002-dual-path-spark-not-on-hot-path.md) | Dual-path architecture — Spark is not on the hot path | 0 | Accepted |
| [0003](0003-redis-online-feature-store.md) | Redis as the online feature store | 0 | Accepted — distinct-cardinality row amended by [0034](0034-hybrid-distinct-cardinality-storage.md) |
| [0004](0004-postgres-system-of-record-ground-truth-isolation.md) | PostgreSQL system of record; ground-truth isolation by schema and role | 0 | Accepted |
| [0005](0005-delta-lake-medallion.md) | Delta Lake medallion architecture | 0 | Accepted |
| [0006](0006-kafka-kraft-event-backbone.md) | Kafka (KRaft) as the event backbone | 0 | Accepted |
| [0007](0007-postgres-durable-work-queue.md) | PostgreSQL durable work queue | 0 | Accepted |
| [0008](0008-python-only-core-go-deferred.md) | Python-only core; Go deferred to an optional phase | 0 | Accepted |
| [0009](0009-neo4j-graphstore-abstraction.md) | Neo4j with a GraphStore abstraction proven by two adapters | 5 | Accepted |
| [0010](0010-langgraph-orchestration.md) | LangGraph as the agent orchestration engine | 6 | Accepted |
| [0011](0011-lightgbm-over-xgboost.md) | LightGBM over XGBoost | 4 | Accepted |
| [0012](0012-anomaly-detection-isolation-forest-with-control.md) | Isolation Forest with a robust z-score control | 4 | Accepted |
| [0013](0013-mcp-real-local-transport.md) | MCP as a real local transport boundary, from Phase 5 | 5 | Accepted |
| [0014](0014-selective-agentcore-adoption.md) | Selective adoption of AWS Bedrock AgentCore | 12 | Accepted |
| [0015](0015-delta-data-layout-local-vs-databricks.md) | Delta data layout — local and Databricks decided separately, by benchmark | 3 | **Proposed** |
| [0016](0016-llm-provider-tiers.md) | LLMProvider tiers, with a local model for keyless bootstrap | 6 | Accepted |
| [0017](0017-run-manifest-reproducibility.md) | Complete RunManifest — an unmanifested run is not a valid run | 9 | Accepted |
| [0018](0018-version-pin-matrix.md) | Version pin matrix for the JVM data stack | 0 | Accepted |
| [0019](0019-evidence-gap-routing.md) | Evidence-gap routing — no LLM selects the next agent | 7 | Accepted |
| [0020](0020-hash-chained-audit-log.md) | Hash-chained immutable audit log | 5 | Accepted |
| [0021](0021-two-track-validation.md) | Two-track validation — synthetic causal and external real-world | 1 / 4B | Accepted |
| [0022](0022-source-adapter-field-coverage.md) | SourceAdapter with declared field_coverage — no silent imputation | 1 | Accepted |
| [0023](0023-action-safety-human-approval.md) | Action safety pipeline with human approval | 8 | Accepted |
| [0024](0024-local-first-design.md) | Local-first design with profiled compose | 0 | Accepted |
| [0025](0025-cloud-validation-strategy.md) | Cloud validation — complete IaC, one funded window, then destroy | 12 | Accepted |
| [0026](0026-event-envelope-and-schema-evolution.md) | Event envelope, keying, backward-only schema evolution | 0 | Accepted |
| [0027](0027-case-and-investigation-state-machines.md) | Case, investigation and agent state machines as validated transition tables | 1 | Accepted |
| [0028](0028-event-model-codegen.md) | Event models generated from JSON Schema, with a drift gate and a release ledger | 1 | Accepted |
| [0029](0029-deterministic-generation.md) | Deterministic generation — stdlib RNG, named substreams, row-JSON digests | 1 | Accepted |
| [0030](0030-fraud-scenario-taxonomy.md) | Fraud scenario taxonomy and the causal evidence key contract | 1 | Accepted |
| [0031](0031-trace-generator-write-only-role.md) | `trace_generator` — a write-only ground-truth role | 1 | Accepted |
| [0032](0032-declarative-feature-semantics.md) | Features declare their semantics, not just their implementation | 2 | Accepted |
| [0033](0033-declarative-rule-packs.md) | Rule packs are data in a closed grammar, digest-pinned and fail-safe on reload | 2 | Accepted |
| [0034](0034-hybrid-distinct-cardinality-storage.md) | Hybrid distinct-cardinality storage — exact where bounded, estimated where not | 2 | Accepted (amends 0003) |

## Notes on status

**ADR-0015 is deliberately `Proposed`, not `Accepted`.** It may only be accepted once the Phase 3
benchmark under `benchmarks/delta_layout/` produces committed output that its conclusion cites. A CI
check enforces this. Writing the conventional answer and back-filling justification is precisely the
failure that ADR exists to prevent.
