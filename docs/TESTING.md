# TRACE-X — Testing Specification

> Authoritative for test strategy and for the project's **Definition of Done**.

---

## 1. Definition of Done

> ### CODE WRITTEN ≠ FEATURE COMPLETE.

A capability is complete only when **all seven** hold:

| # | Requirement | What proves it |
|---|---|---|
| 1 | **Implementation** | Typed code exists; `mypy` strict passes on `trace_core` |
| 2 | **Tests** | Every layer §3 requires for that capability, **including the failure path** |
| 3 | **Actually executed** | The acceptance command was run and its real output recorded in `tests/acceptance/status.json` |
| 4 | **Failure-path validated** | The documented degraded/failure behaviour was *triggered and observed*, not merely coded |
| 5 | **Observability** | Emits the metrics, traces and structured logs the design requires |
| 6 | **Documentation** | Relevant doc updated; ADR written if a decision was made |
| 7 | **Phase exit criteria** | Every EXIT CONDITION in `docs/ROADMAP.md` for that phase is met |

Only then may `tests/acceptance/status.json` record `PASS`, and it must carry the evidence.
`scripts/acceptance.py` **refuses** to mark `PASS` without a `--result`.

**Requirement 4 is the one most often skipped.** A retry policy that has never been made to retry, a
circuit breaker that has never tripped, and a degraded mode that has never degraded are all
unimplemented regardless of how much code exists.

---

## 2. Anti-cheating rules (enforced, not advisory)

1. **No mocking critical integrations in the end-to-end path.** A CI job greps `tests/e2e/` for
   `unittest.mock`, `MagicMock`, `monkeypatch` and fails on a hit.
2. **Cassette replay is not mocking.** It is recorded real traffic, byte-replayed. The release pipeline
   runs `--live` against the real provider and diffs. A cassette is only valid if it was recorded from
   a real run whose manifest is stored.
3. **No hardcoded outputs that make benchmark tests pass.** `test_harness_integrity` replaces the model
   with a constant predictor; the harness **must** fail. If it passes, the harness is measuring nothing.
4. **No test asserts a specific benchmark number.** Tests assert *relationships* (calibrated beats
   uncalibrated; ML beats rules-only) and *properties* (termination, idempotency), never magic values.
5. **A skipped test must say so loudly.** Resource-gated tests (`external`, `cloud`, `integration`)
   print an explicit reason. A silent skip is a false pass.
6. **Coverage gates:** ≥85% on `domain/`, `policy/`, `actions/`, `rules/`; ≥70% overall.

---

## 3. Test layers

| Layer | Marker | Tooling | Scope | Runs |
|---|---|---|---|---|
| **Unit** | `unit` | pytest | Rules, features, state machines, policy engine, parsers, redaction | every commit |
| **Property** | `property` | hypothesis | State-machine legality, idempotency, feature monotonicity, **agent termination** | every commit |
| **Contract (API)** | `contract` | schemathesis vs committed OpenAPI | Request/response conformance, breaking-change diff | every commit |
| **Contract (events)** | `contract` | JSON-Schema round-trip + compat check | Producer/consumer compatibility, backward-compat gate | every commit |
| **Conformance** | `conformance` | shared suites | `GraphStore` ×3, `LLMProvider` ×4, `ActionExecutor` ×2, `SourceAdapter` ×2 *(one adapter from Phase 1, the second at Phase 4B — both run the same suite file, unmodified)* | every commit |
| **Integration** | `integration` | testcontainers (PG, Redis, Kafka, Neo4j) | Repositories, streaming, tools — **real services, no mocks** | PR |
| **Streaming recovery** | `integration`, `chaos` | testcontainers + kill | Checkpoint resume, no loss, no double-count | PR |
| **Feature parity** | `parity` | pytest + Spark | Online (Redis) vs offline (Spark) on the same stream, tolerance-bounded | PR |
| **Transport parity** | `transport_parity` | MCP client + in-process | Identical envelope, authz denial, rate limit, timeout, truncation, audit | PR |
| **Agent (replay)** | `unit` | cassette adapter | Full LangGraph runs, deterministic, zero API cost | PR |
| **Agent (live)** | `slow` | real provider | 20-investigation smoke against the real LLM | nightly + release |
| **Adversarial** | `adversarial` | red-team corpus | Prompt injection, authz bypass, tool escape | PR |
| **Security** | `adversarial` | bandit, RLS probes, token scoping | Least privilege, isolation, secret scanning | PR |
| **End-to-end** | `e2e` | docker compose full | generator → Kafka → Spark → score → investigate → decide → approve → execute → verify → audit | nightly + release |
| **Load** | `load` | k6 / locust | Gateway p99 under sustained TPS | pre-release |
| **Chaos** | `chaos` | toxiproxy + container kill | Every row of the failure model | pre-release |
| **External data** | `external` | pytest + Spark | IEEE-CIS through unmodified medallion; no silent zero-imputation | PR (sampled) / nightly (full) |
| **Cloud smoke** | `cloud` | live AWS/Databricks | One transaction → audit, in the cloud | Phase 12 window only |

Two contract gates were added in Phase 1 and run inside `make verify`:

- **codegen drift** — `scripts/generate_event_models.py --check` re-renders the event models into a
  temporary directory and compares. Hermetic, so it gives the same answer on a dirty tree, in CI and
  in a test; one definition with three callers.
- **topic release gate** — no code may reference a topic whose schema has not been released, in either
  direction. A released schema file is immutable, so freezing a contract before a producer exists
  guarantees a `.v2` later.

Phase 2 added three more:

- **openapi drift** — `scripts/generate_openapi.py --check` regenerates the committed spec from the
  Pydantic models and compares. In `make verify` rather than CI alone, for the reason the Phase 0
  secret scan was moved there: a gate that only runs in CI lets a local run pass while CI goes red.
- **breaking-change gate** — `scripts/openapi_diff.py`, the pinned `oasdiff` image (ADR-0036,
  ADR-0037). It has a **self-test with both halves**: a breaking fixture pair that must be rejected
  and a compatible pair that must be accepted. A checker that rejects everything is bypassed within a
  week, and then nothing is checked at all.
- **online-store correctness** — `tests/integration/test_online_store_capacity.py` starts a real
  Redis with a 2 MB limit and `noeviction`, fills it, and asserts the store refuses rather than evicts
  (`evicted_keys` stays 0), the refusal is typed, and the breaker stays closed;
  `tests/chaos/test_redis_down.py` pauses the **cache** instance and asserts no decision changes, and
  `FLUSHALL`s the feature store and asserts `history_incomplete`; the conformance suite carries
  `complete_since` explicitly, so "an unseen device is not known" is asserted on a store that has
  watched for its horizon and "cannot tell" on one that has not (ADR-0044).
- **feature-semantics conformance** — `tests/conformance/feature_semantics_suite.py`, run unmodified
  against the naive reference implementation and against Redis, and against Spark from Phase 3. It is
  how online/offline parity is proven without anyone redefining a feature (ADR-0032).

```bash
make test-fast   # unit + property + contract + conformance + transport_parity  (< 5 min, no services)
make test        # everything except cloud
make verify      # ★ canonical: doctor + acceptance + claims + codegen + openapi + lint + types +
                 #   test-fast + bandit + secret-scan
```

**Chaos tests produce the failure, they do not simulate it.** `pytest -m chaos
tests/chaos/test_redis_down.py` pauses the real Redis container under a running gateway. `docker
pause` rather than `stop`, because pausing reproduces a network partition — connections hang and time
out — while stopping gives an immediate refusal, and the timeout path is the one with a latency budget
attached. That distinction is not academic: the first run of that suite found a degraded request
taking **21.8 s** against a configured 20 ms timeout (ADR-0035).

---

## 4. What each layer may and may not mock

| Layer | May mock | May **never** mock |
|---|---|---|
| Unit | Everything outside the unit | — |
| Property | Adapters behind ports | The invariant under test |
| Contract | Handlers | The committed schema |
| Conformance | Nothing — the point is the real adapter | The adapter under test |
| Integration | External vendor HTTP | Postgres, Redis, Kafka, Neo4j — **use real containers** |
| Agent (replay) | Nothing — cassettes are recorded real traffic | Tool execution, middleware, the router |
| Adversarial | Nothing | Authorization, validation, the middleware chain |
| **E2E** | **Nothing at all** | Every service is real; the CI grep enforces it |

---

## 5. Critical test cases by risk

These exist because a specific failure would be severe and silent.

| Risk | Test | Consequence if absent |
|---|---|---|
| Ground-truth leakage | `trace_app` gets `permission denied for schema groundtruth` | **Every metric in the project is invalid** |
| Authz enforced on one transport only | `ToolTransportParitySuite` asserts denial over MCP **and** in-process | An MCP path bypasses authorization |
| Unwrapped tool entrypoint | No MCP server registers a function bypassing the middleware | Rate limits, audit and validation silently skipped |
| LLM reaching the executor | `ValidatedAction` unconstructable outside the policy engine | Model output executes a financial action |
| Silent zero-imputation | `UNAVAILABLE` features propagate as null | Cross-dataset transfer results become meaningless |
| Predetermined agent path | Two fraud patterns visit provably different agent sequences | The "dynamic investigation" claim is false |
| Non-termination | Property test: every generated state converges within budget | An investigation hangs a worker forever |
| Prompt injection | Red-team corpus: decision distribution unchanged vs clean control | Attacker-controlled merchant name steers a decision |
| Training leakage | Shuffled labels ⇒ PR-AUC ≈ base rate | The model looks excellent and is worthless |
| Random train/test split | No training row postdates any test row | Temporal leakage inflates every metric |
| Audit tampering | Verifier detects a mutated row | The audit trail is not evidence |
| PII in logs | Redaction test over account numbers, PANs, emails, IPs | A privacy incident |
| Benchmark fabrication | `make check-claims` resolves every published number | An invented number reaches a résumé |
| Constant predictor passes | Harness fails on a constant predictor | The benchmark measures nothing |

---

## 6. Test data

- **Track A:** the seeded generator. Same seed ⇒ identical digest. The frozen `eval-v1` dataset is
  referenced by digest and never regenerated in place.
- **Track B:** IEEE-CIS, downloaded by `make fetch-external` and SHA-256 verified. **Never committed.**
  Tests marked `external` skip loudly with instructions when it is absent.
- **Fixtures:** small, hand-checked, committed. Any fixture asserting a computed value carries a
  comment showing the derivation.
- **Red-team corpus:** committed under `tests/adversarial/corpus/`, with a clean control set of equal
  size so the comparison is statistical rather than anecdotal.

---

## 7. CI topology

| Workflow | Trigger | Gates |
|---|---|---|
| `lint` | every push | ruff format + lint, mypy strict, bandit, secret scan, pip-audit |
| `test-fast` | every push | unit, property, contract, conformance, transport parity |
| `claims` | every push | `make check-claims` |
| `test-integration` | PR | testcontainers, feature parity, agent replay, adversarial |
| `contracts` | PR | OpenAPI + event-schema breaking-change diff vs `main` |
| `e2e` | nightly + release | full compose, real services, **no-mock grep assertion** |
| `eval` | nightly + manual | Track A arms A–G, Track B E1–E5, manifest + tier gates, regression gate |
| `iac` | PR touching `infra/` | validate, tflint, checkov, conftest, plan — **never apply** |
| `release` | tag | signed images (cosign), SBOM (syft), live-LLM smoke, publish |

Branch protection: no direct push to `main`; all gates green; an ADR is required for any change
touching `docs/contracts/` or `docs/adr/`.
