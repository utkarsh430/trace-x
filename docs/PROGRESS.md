# TRACE-X — Progress Tracker

> **This is the live state file.** A future session must be able to recover the entire current state
> from this document plus git history. It records reality **including failures** — an over-optimistic
> PROGRESS.md is worse than none.
>
> Rules and invariants live in `CLAUDE.md`; they never appear here. Status never appears there.

---

## CURRENT PHASE

**Phase 2 — Hot Path: Gateway, Rules, Online Features** (mandatory)

Gate: `docs/ROADMAP.md` § Phase 2.

## CURRENT STATUS

**All five Phase 2 capabilities PASS. All three exit conditions are met with recorded evidence.**
Phase 0 and Phase 1 remain complete; nothing in either was weakened.

`make verify` is **11/11 green**. **1,360 tests pass** across unit, contract, conformance,
integration and chaos, the integration and chaos layers against real PostgreSQL and Redis
containers.

**CI on the Phase 2 PR went red on two defects; both are fixed at the commit that follows this file,
and neither touched runtime code or Phase 2 semantics.** (1) `contracts.yml` runs `make codegen-openapi`,
and the Makefile ran `.venv/bin/python` unconditionally — present on every laptop, absent on every GitHub
runner, where setup-python installs into the interpreter on PATH. The Makefile now resolves ONE
interpreter (`make toolchain` prints it: the venv when `make setup` created it, PATH otherwise),
`scripts/verify.sh` consumes that same resolution instead of keeping its own copy, and
`tests/unit/test_toolchain_consistency.py` dry-runs every target CI calls with the venv pointed at a
directory that does not exist. (2) `test_request_latency_strictly_exceeds_scoring_latency` compared one
response's `latency_ms` against a histogram `_sum` that is cumulative over the whole pytest process —
`/metrics` serves the process-global registry, so a fresh `TestClient` isolates nothing. The seven
requests an earlier test file makes summed to a fraction of the tolerance locally and to several times
it on the runner. The test now measures before/after deltas over exactly one transaction, with a
deliberate delay on each side of the scoring boundary, and its agreement tolerance is one microsecond
because the two figures are the same number; the instrumentation itself was correct. GitHub showed
three failed checks for these two causes because `test-fast.yml` runs on both `push` and
`pull_request`, so one push produces two runs of it.

**The canonical load gate passes on the corrected topology**, `run_id: load-20260913-gateway-c86c6cdd`,
on a clean tree: 499.98 TPS sustained of 500 offered, **0 dropped iterations**, **p50 1.73 ms** against
a 20 ms budget, **p99 25.30 ms** against a 100 ms budget, **0 5xx / 0 4xx / 0 429**, 0 unparseable,
over 600 s — and the two correctness verdicts ADR-0044 added: **0 feature-state evictions** and
**0 unexpected degradations** (every decision carried `history_incomplete`, which a cold store reports
by design; nothing else). Feature store at end of run **548.1 MiB** against the model's projected
548.0 MiB and the 704 MiB limit. Report: `benchmarks/gateway/REPORT.md`. The earlier pass on the
single-instance topology (`run_id: load-20260912-gateway-550789bb`) stands as the measurement that
exposed the eviction defect.

**No target was moved to reach that.** The ROADMAP numbers are the ones it was measured against. What
changed was the workload, a bug in the load generator, Redis persistence, and the measurement
technique — in that order of impact. See ADR-0040 and ADR-0042.

**Phase 3 has not begun and must not begin without explicit user approval.**

### The correctness blocker, resolved (ADR-0044)

The exposure recorded here previously — a passing acceptance run had evicted 346,067 feature keys
while looking perfectly healthy — is closed, and it was closed as a correctness fix rather than a
memory tune:

* **Feature state cannot silently evict.** Two Redis instances: `redis` (feature state, `noeviction`,
  refuses when full and says `feature_write_failed`) and `redis-cache` (replay cache + rate-limit
  windows, `allkeys-lru`, nothing in it decides). Proven on a real 2 MB `noeviction` container and
  by pausing the cache under a live gateway with no change to any decision.
* **The store holds what the released features declare, and nothing else.** `state_plan.py` derives
  writes and per-primitive retention from the declarations; the ten-minute working set fell from a
  measured 1.32 GB to a projected 548 MiB (`run_id: bench-20260913-memory-model-5c770259`).
* **A missing key means zero only when the store would know.** A completeness epoch separates a new
  account (measured zero) from an unwarmed or wiped store (`INSUFFICIENT_HISTORY` and
  `history_incomplete` on the decision). A `FLUSHALL` can no longer produce a clean answer.

### What is NOT resolved

**The steady-state memory requirement does not fit a laptop, and no feature was changed to make it.**
The model projects **25.82 GiB** for 500 TPS at the declared retention; the local feature store is
704 MiB and holds **~13 minutes** of 500 TPS before it refuses writes — loudly. ADR-0044 §5 names the
five structures responsible and the feature-semantics decision each would take. That decision is not
made.

**The `core` budget is tight.** 2,144 MiB of `docs/ARCHITECTURE.md` §14's 2.7 GB is allocated,
leaving 621 MiB for `api`, `worker` and `ui`, none of which exist yet.

**A restart costs a warm-up that is now visible.** Windowed features need 24 h, profile features
30 d; the new-device and tenure rules cannot fire meanwhile. Phase 3's reconciliation is the only thing
that shortens it. Before ADR-0044 the same restart produced a day of confident wrong scores and thirty
days of false new-device alarms, none flagged.

**A host stall is amplified ~15× by the single event loop, and the gate has little slack to absorb
one.** An acceptance run at the noeviction topology met every correctness verdict (0 feature-state
evictions, 0 unexpected degradations) and every target but the tail: 1,589 dropped iterations and a
tail an order of magnitude over budget, so the harness refused it and its latencies are not quoted
here (no `run_id`, CLAUDE.md §13). Its scoring core stayed sub-millisecond and its whole-request mean
was about a millisecond; the feature store's own slowlog held one entry — an `HINCRBY` taking 33.6 ms at the exact second the stall began,
which for a trivial command in single-threaded Redis means the VM itself was descheduled. Everything
froze together, so nothing timed out; k6's in-flight count went from 2 to 790 in twelve seconds, and
the backlog took ~18 s to drain because the loop's capacity (~1/1.7 ms ≈ 590 rps) sits only ~15%
above the 500 TPS offered. `recovery ≈ backlog ÷ (capacity − offered)`. Two defects found on the way
are fixed (histogram buckets that could not show a tail; a cache container limit tighter than its
allocator's RSS) and the run was repeated once — once, with the external cause named, not until it
passed.

**Gate reproducibility is not established.** Recorded previously and still true: one run at the same
configuration dropped 4 iterations of 300,001 and was refused; the next dropped none.

## COLD-START CHECKLIST (read this first in a new session)

```bash
make setup      # venv + dev/db/obs/gen extras
make up         # postgres, redis (features), redis-cache, gateway
make verify     # expect: 11 passed, 0 failed  -> VERIFY OK
```

Docker is needed for the integration and chaos suites, for `make seed`, and for the load harness
(pinned k6 image, ADR-0036). Everything else is pure Python.

| Question a cold session will ask | Answer |
|---|---|
| What phase are we in? | Phase 2 **complete on its stated exit conditions**; the correctness blocker is resolved (ADR-0044), with the steady-state capacity limitation recorded. Phase 3 not started. |
| What do I do next? | Await approval for Phase 3; the steady-state memory decision (ADR-0044 §5) is the one open product-level question. |
| What exists now? | Everything from Phases 0–1, plus: `trace-gateway` with auth, rate limiting, idempotency, RFC 9457 problems and triage; two Redis instances — a `noeviction` feature store with a completeness epoch and a declaration-driven write plan, and a disposable cache — holding 26 declared features with hybrid distinct-count storage; 18 declarative rules with Kleene semantics and fail-safe hot reload; noisy-OR scoring and banding; the transactional outbox; two workload profiles and a three-stream replay harness. |
| Which benchmark is the gate? | `benchmarks/gateway/REPORT.md` (representative profile). `benchmarks/gateway/ADVERSARIAL.md` is **not** a gate — it characterises saturation at 97% triage. |
| What must I never do? | `CLAUDE.md` §17, and §11 (ground-truth isolation) above all. |
| Where do I record results? | This file and `tests/acceptance/status.json` (which refuses `PASS` without evidence). |

**Known environment gaps:**
`JAVA_HOME` unset and system Java is 25 (Spark 4.0 needs Temurin 17) — blocks **Phase 3**.
Ollama not installed — blocks **Phase 6**. No AWS credentials — blocks **Phase 12**.

---

## PHASE 2 — EXIT CONDITIONS

| Condition | Evidence |
|---|---|
| Load report committed with real measured numbers | `benchmarks/gateway/REPORT.md`, `run_id: load-20260913-gateway-c86c6cdd`. All eleven conditions pass, including ADR-0044's two correctness verdicts; `make check-claims` resolves every published figure |
| Degraded mode proven | `pytest -m chaos` — 6 passed. A real Redis container is **paused** under a live gateway; the hot path degrades to rules-only, answers 200 with `degraded=true`, never 5xx, and recovers without a restart. Its first run found a real 21.8 s request (ADR-0035) |
| OpenAPI under the CI breaking-change gate | `scripts/openapi_diff.py` with the digest-pinned `oasdiff` image (ADR-0036, ADR-0037), two-sided self-test: a breaking fixture pair must be rejected and a compatible pair accepted |

**MANUAL VALIDATION** — `benchmarks/gateway/triage-bands.md`. 60,000 frozen `eval-v1` transactions
replayed through the gateway over HTTP with the interleaved identity and device events, labels joined
afterwards as `trace_eval`: known fraud triaged at **37.3%** against **0.0%** for legitimate traffic.

---

## LAST VERIFIED COMMIT

Phase 2's acceptance evidence was recorded at `c86c6cd` (`run_id: load-20260913-gateway-c86c6cdd`).
The CI fix described under CURRENT STATUS is the commit that immediately follows this file; it changes
the Makefile's interpreter resolution, `scripts/verify.sh`, and two unit-test files — no runtime code.
Re-confirm in ~70 s with `make verify`.

---

## PHASE 1 EXIT CRITERIA — all met

| # | Criterion | Status | Evidence |
|---|---|---|---|
| 1 | 10 scenarios implemented and documented | ✅ | `pytest tests/unit/test_scenarios.py` — 63 passed. Each signature asserted against the events actually produced. `docs/FRAUD_SCENARIOS.md` + ADR-0030; a test fails if the catalogue and the code disagree on causal keys |
| 2 | Ground truth written only to `groundtruth` | ✅ | `pytest tests/integration/test_ground_truth_write_path.py` — 23 passed against real PostgreSQL. `trace_generator` can write and **cannot read back**; `trace_app`/`trace_stream`/`trace_auditor` denied across 9 role×table combinations *with tables present* |
| 3 | Frozen `eval-v1` committed by digest | ✅ | `eval/track_a/eval-v1.manifest.json`. `pytest -m slow tests/unit/test_eval_v1_freeze.py` regenerates all 1,000,000 rows and the digest matches |
| 4 | `SourceAdapter` port merged with ADR-0021 and ADR-0022 | ✅ | `pytest -m conformance tests/conformance/test_source_adapter.py` — 31 passed. Shared suite Phase 4B's second adapter must pass unmodified |

## PHASE 2 TARGETS — all met

| Target | Budget | Result |
|---|---|---|
| Sustained throughput | 500 TPS | ✅ **met** — 499.98 TPS, 0 dropped (`run_id: load-20260913-gateway-c86c6cdd`) |
| p50 latency | < 20 ms | ✅ **met** — 1.73 ms |
| p99 latency | < 100 ms | ✅ **met** — 25.30 ms |
| Zero 5xx over a 10-minute run | 0 | ✅ **met** — 0 over 600 s |
| Feature-state evictions (ADR-0044) | 0 | ✅ **met** — 0; store at 548.1 MiB of 704 MiB |
| Unexpected degradations (ADR-0044) | 0 | ✅ **met** — 0; 299,999 `history_incomplete` (cold store, by design) |

Measured on the **representative** profile, whose entity model is derived from the frozen `eval-v1`
manifest and whose population is derived from the offered rate so per-account velocity stays realistic
(ADR-0040). The saturation profile is a separate benchmark and misses both latency targets by design
at 97% triage — `benchmarks/gateway/ADVERSARIAL.md`, and it is not a gate.

---

## PHASE 1 TARGETS — one met, one met, one **NOT met**

| Target | Budget | Result |
|---|---|---|
| 1 M-row dataset in Parquet | under 500 MB | ✅ **met** (run_id: gen-20260912-eval-v1-ccfd38d9) |
| Digest-reproducible | same seed ⇒ same digest | ✅ **met** — verified by an independent re-run and by full-size regeneration |
| Single-process generation | at least 50 k tx/s | ❌ **NOT met** (run_id: gen-20260912-bench-generation-e4c63451) |

### The throughput miss

**The budget was not lowered to fit** (CLAUDE.md §17). The measured rate is recorded in the run
record cited above and in `benchmarks/generator/REPORT.md`.

Profiling attributes the cost to two deliberate decisions, both in ADR-0029: **per-row RNG
substreams** (which is what makes generation order not part of the contract, so fraud injection does
not reshuffle legitimate rows) and **canonical-JSON encoding for the dataset digest** (a digest over
output bytes would change with a compression setting). ADR-0029 explicitly forbids recovering
throughput by changing the RNG, because that would silently move every recorded dataset digest.

A `load`-marked test asserts the budget and is marked `xfail`, so the gap stays visible in the test
report and flips to a pass if it is ever closed. The recorded next step is the batched-substream
scheme already named in ADR-0029's alternatives.

**One genuine defect was found while measuring**, which is why measuring before publishing mattered:
the pipeline encoded every row twice — once to validate and write, once again inside the digest —
despite the sink module's docstring claiming a single serialisation. Fixed in `d5b5e20`.

---

## COMPLETED WORK

Eleven commits, each leaving `make verify` green.

### Step 0 — toolchain prerequisites, deliberately first
Three defects would each have turned CI red the moment Phase 1 code landed:
1. `test-fast.yml` asserted **`make seed` exits non-zero**. `make seed` is a Phase 1 deliverable, so
   implementing it would have failed CI *on success*. Repointed at `make demo` (Phase 7), and guarded
   by a new test that parses the phase-gated targets out of the Makefile and fails if the probe ever
   names a command whose phase has landed.
2. mypy's scope excluded `data/`, where CLAUDE.md §4 puts the generator — Phase 1's largest surface
   would have been untyped by default.
3. New `gen` extra plus codegen tooling, added to `make setup`, all mypy workflows and `make lock` in
   one commit, and registered in `test_toolchain_consistency`.

### Step 1 — domain layer
Enums split into **wire** (carry `UNKNOWN`) and **closed** (must not — an `UNKNOWN` on `ActionType`
would give raw model output somewhere to land). `EventTime`/`ProcessingTime` are distinct NewTypes.
Money is integer minor units. Two real defects found by the tests:
- `Money.scaled` rounded negative ties the wrong way (`divmod` floors; the tie-break assumed
  truncation), so −2.5 rounded to −4. Cross-checked exhaustively against `round(Fraction(...))`.
- `to_millis` lost a millisecond to float imprecision, found by a hypothesis property test. Those
  milliseconds are the UUIDv7 prefix and feed watermark arithmetic.

### Step 2 — three state machines (ADR-0027)
**The case lifecycle did not exist**: ROADMAP cited `ARCHITECTURE.md` §13, which is Observability. Now
authored in **ARCHITECTURE.md §19** with every edge derived from an existing authority. **The §8
investigation diagram had a modelling gap at `ROUTE`** — its only exit was `AGENT_SELECTED`, but
ADR-0019's `eligible(a)` predicate is routinely empty. Two edges added and *declared*; a test diffs
the table against the mermaid block and fails on undeclared divergence.

### Step 3 — event contracts (ADR-0028)
Three ingress topics released plus the shared envelope; six stay PLANNED because a released schema is
immutable. `docs/contracts/RELEASED.json` is the ledger (schema, sha256, partition key, and the stated
*reason* for that key). `make codegen --check` is hermetic and called identically by verify, CI and a
test.

### Step 4 — `CanonicalTransaction`, `SourceAdapter`, `GeneratorAdapter` (ADR-0022)
Coverage is enforced per row in one direction only: an undeclared field must be absent, while a
declared field *may* be null on a given row — the distinction between "this source never provides it"
and "this row happens to lack it". A deliberately lying adapter proves the conformance check can fail.
Also corrected a **wrong claim in the base model's docstring**: pydantic strictness applies to both
validation modes, so a dict parsed from JSON must be validated *as* JSON.

### Step 5 — the UNAVAILABLE mechanism
`FeatureValue.value` **raises** rather than returning a default, and the type exposes no `__float__`
or arithmetic, so it cannot silently become 0.0. The registry refuses a feature declaring no
`required_fields`.

### Steps 6–7 — the generator and ten scenarios (ADR-0029, ADR-0030)
Deterministic substreams, a baseline with real diurnal/Zipf/lognormal shape, and ten scenarios whose
signatures are asserted against produced events. Two defects caught by those tests:
- **`IMPOSSIBLE_TRAVEL` did not enforce its own threshold.** An independently-drawn time gap produced
  instances implying ordinary airline speed — a *wrong ground-truth label*, which nothing downstream
  could detect. The gap is now derived from the distance.
- **`fraud_rate` silently had no effect on small datasets**, because ten mandatory coverage instances
  form a floor. Documented on the field and pinned by a test asserting the two *are* equal below it.

### Step 8 — ground-truth write path (ADR-0031)
A fifth role, `trace_generator`: INSERT on the label tables, SELECT on the dataset registry only. **It
cannot read the labels it writes**, so no credential used in ordinary development can read ground
truth. Isolation re-verified *with tables present*, which the Phase 0 test could not do. Adding the
role broke three existing integration tests — all correct detections, all updated rather than loosened.

### Step 9 — CLI, `make seed`, and provenance
`GeneratorRunRecord` fills the gap before ADR-0017's full manifest. The claim linter gained three
checks because **it failed to catch a real number I published in ADR-0029 during step 6** — its
throughput pattern required the words "throughput" or "sustained" nearby, so a rate slipped through.
Now any per-second rate and dataset size is caught, a GENERATOR record cannot back a quality claim,
and an incomplete record resolves nothing. Seven tests prove each rejection fires.

### Steps 10–11 — freeze, measure, document
Parquet sink (the size target was previously unmeasurable), `eval-v1` frozen and reproduced at full
size, distributions and fraud episodes inspected, docs and acceptance status updated.

---

## MANUAL VALIDATION PERFORMED

ROADMAP Phase 1 requires generating 1 M transactions and inspecting distributions and a sample of each
fraud pattern. Done, with the artefacts committed:

- `benchmarks/generator/eval-v1-distributions.md` — amounts (lognormal with a long right tail), hour
  of day (a real overnight trough, peaks at midday and early evening), day of week (Friday highest,
  Sunday lowest), channel mix, merchant country, MCC mix (everyday spend dominant, high-risk
  categories present but uncommon), and merchant concentration (strongly skewed, not uniform).
- `benchmarks/generator/eval-v1-fraud.md` — all ten patterns present with episode counts and the
  recorded causal keys per pattern. **Produced by connecting as `trace_eval`**, the only role that may
  read ground truth: the isolation control visible in ordinary tooling.

Judgement: the shapes are plausible for card spending, and each pattern's episodes match its documented
signature. Note that the mix weights **episodes**, so patterns contributing many transactions each
(velocity, rings) are a larger share of fraudulent *rows* than of *episodes* — expected, and visible in
both reports.

---

## WORK IN PROGRESS

Nothing in flight. This is a clean stopping point.

## CURRENTLY FAILING TESTS

**None.** One test is deliberately `xfail`: the generation-throughput budget, discussed above.

---

## KNOWN TECHNICAL DEBT

| # | Item | Impact | When |
|---|---|---|---|
| D4 | `compose.yml` declares `streaming`/`graph`/`llm` services nothing consumes yet | Profile shape reviewable but unexercised | Phases 3, 5, 6 |
| D5 | `postgres:16` rather than `postgres:16-alpine` | ~250 MB more disk | Revisit if disk binds |
| D8 | macOS Docker keychain helper hangs, blocking cold registry pulls | Workaround in `LOCAL_DEVELOPMENT.md` | Environment, not code |
| D9 | Role passwords in `.env.example` are `change_me_locally` | Fine locally; a shared deployment must supply real values | Before any shared deployment |
| D11 | CI job logs need repo-admin rights to download | CI-only failures are diagnosed by local reproduction | Optional (`gh auth login`) |
| **D12** | **Generation throughput below the ROADMAP budget** | Slower dataset builds; no correctness impact. ADR-0029 names the batched-substream scheme as the first thing to try | Revisit if dataset size grows |
| **D13** | `identity.events.v1` / `device.events.v1` are produced but nothing consumes them yet | The contracts are exercised by the generator only | Phase 3 |
| ~~D1, D2, D3, D6, D7~~ | ~~Migrations, CI, OTel, lockfile, compose targets~~ | **RESOLVED in Phase 0** | done |
| ~~D10~~ | ~~No declarative SQLAlchemy models, so autogenerate is unused~~ | Still true and still correct: migrations 0001 and 0002 are hand-written because they are security-critical grants. Re-evaluate when ordinary application tables arrive | Phase 2 |

---

## OPEN RISKS

| # | Risk | Status |
|---|---|---|
| R2 | Java 25 is the system default; Spark 4.0 needs Temurin 17 | Mitigated — `make doctor` detects it. **Blocks Phase 3 entry** |
| R3 | Docker RAM ceiling is tight for the full profile | Open — mitigated by profiles and per-service limits |
| R4 | No AWS credentials | Open — blocks Phase 12 only |
| R5 | IEEE-CIS needs Kaggle credentials and a ~1.5 GB download | Open — blocks Phase 4B entry |
| R6 | Ollama not installed | Open — blocks Phase 6 keyless demo |
| R7 | Two transports mean two places authorization could be wrong | Mitigated by design; unproven until Phase 5 |
| R8 | Docker does not survive reboot unless set to start at login | Open — `make doctor` fails loudly |
| **R9** | **The ten fraud typologies are an engineer's model, not a fraud analyst's ground truth** | Inherent to synthetic data, and precisely why Track B exists. No document may imply synthetic accuracy predicts real-world performance (CLAUDE.md §13) |

---

## UNRESOLVED DECISIONS

| # | Decision | Resolves at |
|---|---|---|
| U1 | Delta table layout | Phase 3, by benchmark. ADR-0015 stays `Proposed` |
| U2 | Whether a 3B-class local model can complete investigations within budget | Phase 6 |
| U3 | Whether Neo4j outperforms `PostgresGraphStore` at this scale | Phase 9 Arm G |
| U4 | Whether the Skeptic agent pays for its cost | Phase 9 Arm F — `UNUSUAL_LOCATION_DEVICE` was built deliberately ambiguous to give this ablation something real to measure |
| U5 | LightGBM vs XGBoost on measured PR-AUC | Phase 4 |
| U6 | Whether Phase 13 (Go gateway) is worth doing | Phase 13 entry |

---

## NEXT EXECUTABLE TASKS

**Phase 1 is closed.** The next action is a decision, not a task:

1. **Await explicit user approval to begin Phase 2** — `trace-gateway`, the Redis online feature store,
   ~20 features, a declarative rule engine, `POST /v1/transactions`, idempotency, degraded mode, and
   the committed OpenAPI.

Phase 2 needs no new environment prerequisites beyond Docker, which is already in use.

---

## LAST VERIFICATION RESULTS

```
TRACE-X verify — 2026-09-13T03:45:35Z
  PASS  doctor
  PASS  acceptance-status
  PASS  check-claims
  PASS  codegen-drift
  PASS  openapi-drift
  PASS  ruff-format
  PASS  ruff-lint
  PASS  mypy
  PASS  test-fast           1260 passed, 120 deselected
  PASS  bandit
  PASS  secret-scan
======================================================================
  phase 2   11 passed   0 failed   0 skipped     VERIFY OK
```

Run on the CI-fix tree with `.venv` present; the same targets were also replayed with the venv pointed
at a nonexistent directory and the interpreter taken from PATH, which is the runner's situation
(`make codegen-openapi`, the spec drift check, `make contracts-self-test`, the event-schema contract
tests). The integration and chaos layers were last run in the session that closed the load gate
(1,360 tests across all layers, see CURRENT STATUS) and were not re-run for a change that touches no
runtime code.

**Acceptance status: 19 PASS · 0 IN_PROGRESS · 39 NOT_STARTED · 0 FAIL · 0 BLOCKED** across 58 tracked
capabilities. `tests/acceptance/status.json` is the authoritative machine-readable record.
