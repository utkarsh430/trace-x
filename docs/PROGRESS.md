# TRACE-X — Progress Tracker

> **This is the live state file.** A future session must be able to recover the entire current state
> from this document plus git history. It records reality **including failures** — an over-optimistic
> PROGRESS.md is worse than none.
>
> Rules and invariants live in `CLAUDE.md`; they never appear here. Status never appears there.

---

## CURRENT PHASE

**Phase 3 — Streaming & Medallion** (mandatory) — **in progress**, approved 2026-09-13.

Gate: `docs/ROADMAP.md` § Phase 3. Approved scope, decisions, safeguards and step order:
`docs/PHASE3_PLAN.md`. Phase 2 is complete on its exit conditions (below), with the caveats Phase 3
planning surfaced recorded under *What Phase 3 planning found in Phase 2's artefacts*.

## CURRENT STATUS

### Phase 3

**Planning is complete and approved, and Step 0 (toolchain and dependency contract, ADR-0045) is
complete locally** — `make verify`, the real-JVM stream tests on macOS, a hashed install in a Linux
container, and a Linux build of the gateway image from its hashed runtime lock. Its first GitHub CI run
(including the new `test-stream` job) is still pending. Wave B (Steps 1, 2, 3 and E) is under way; see
*WORK IN PROGRESS*. One
Phase 3 capability is PASS: `P3.pin-failfast`. The plan was built by six specialist reviews whose load-bearing claims were
checked against the code, by experiment in a throwaway container, or against cited upstream
documentation — claims resting only on documentation are re-verified by the step that depends on
them. The architecture that
came out of it (reconstruction by rebuilding primitives, a durable gateway observation log, a complete
exactly-deduplicated Silver, batch Gold, declared feature semantics, a split parity criterion, and
`eval-v2`) is summarised in `docs/PHASE3_PLAN.md` §2.

**The entry condition "`make doctor` green on the full pin matrix (Java 17)" was met only nominally at
approval:** the Java check is warning-level, Hadoop and Scala are declared but never asserted, and a
non-interactive shell runs Java 25. Step 0 remediated it rather than waiving it: `make doctor` now fails
on a wrong Java and on drifted pyspark, Delta, Hadoop, Scala or jars, and a test runs it under a real
installed Temurin 25 to prove it.

### What Phase 3 planning found in Phase 2's artefacts

Recorded because an over-optimistic tracker is worse than none. None of these changes a Phase 2
exit condition; each is owned by a Phase 3 step.

* **The online store disagrees with the naive reference on inputs the conformance suite never
  supplies** — boundary minutes of bucket-derived sums, duplicate delivery, out-of-order arrival of
  the previous observation and of first-seen, the home location, per-currency profile keys,
  event-time filtering of profile reads, future-dated trims and backdated distinct counts; and the
  coefficient of variation divides same-currency sums by an all-currency count in *both*
  implementations. Each produces a wrong number rather than an absence. Owned by Step 1, which re-runs
  the Phase 2 load gate afterwards.
* **An unreachable feature store does not withdraw completeness.** When Redis is unreachable (as
  opposed to full) an observation goes unrecorded but the completeness epoch stands, so after the
  outage the store claims completeness over a hole. Owned by Step 1.
* **Phase 2's manual validation carries a label-proxy caveat.** The generator emits identity and
  device events only inside fraud scenarios, so any identity signal in `eval-v1` marks fraud. The
  recorded fraud-vs-legitimate banding result stands as measured, but it cannot be read as evidence
  that identity-based rules generalise. Owned by Step E (`eval-v2`).
* **The steady-state memory projection in ADR-0044 §5 is not a reliable architecture input.** Its
  velocity fits extrapolate listpack-encoded sizes into skiplist territory, buckets were counted per
  observation rather than per active minute, arrivals are uniform, and the population is held fixed
  across a 30-day horizon; the ADR also quotes a merchant-bucket figure that differs from the report
  its own `run_id` produced. The ten-minute acceptance figure is unaffected (it was measured). Owned
  by Step 12.
* **Several gates were weaker than they looked:** the Redis conformance suite and other
  Redis-dependent integration tests skip in CI (no Redis is provisioned) and flush the live feature
  store when run locally; the profile-leakage conformance test cannot fail (identical amounts saturate
  the z-score); `requirements.lock` is installed by nothing; the ADR-0015 gate unlocks on any non-empty
  file; and the generator's Kafka sink drops unsent messages at close. Owned by Steps 0, 1, 2, 8 and 14.
* **The served feature values excluded the scored transaction, contrary to ADR-0032.** The gateway
  read features before recording the transaction. The user decided (2026-09-13) to honour ADR-0032,
  with self-inclusion declared per feature and the thresholds left unchanged; ADR-0046 §2. Owned by
  Step 1, which re-runs the load gate, the manual replay and rule validation on the new values.
* **`authorization_outcome` is post-decision information carried by a pre-decision contract**
  (ADR-0046 §7). Step 1 keeps a transaction's own outcome out of its own features. U7, decided on
  2026-09-14, makes authorization decisions their own dated events (ADR-0049, Proposed). It is not
  implemented yet: until Stage 2 step 7, earlier outcomes still come from scoring requests.
* **Phase 2's manual replay predates ADR-0044.** `benchmarks/gateway/triage-bands.md` was generated
  (`42f907c`) hours before completeness gating existed (`5c77025`), so its profile rules fired on a
  store that could not tell "not known" from "cannot tell". Replayed again by its own commit on a
  store vouching for the whole replay, it reproduces exactly: it stands as a complete-store baseline,
  and a cold-store run is not comparable with it. Found and controlled for in Step 1.
* **`eval-v1` has label proxies in its transaction rows**, found by the Step E generator work and by
  ADR-0046 §7: no legitimate transaction uses a non-home device (fraudulent ones often do), planted
  high-value and unusual-location transactions carry whole-second timestamps, and `DECLINED` occurs
  only in fraud. Rules reading device novelty or the declined ratio therefore look better on Track A
  than the signal justifies. The approved plan's B10 ("`eval-v2` without label proxies") requires
  `eval-v2` to remove them under its own gate; `eval-v1` stays immutable. Shared envelope ids on
  `eval-v1`'s scenario side events also mean Silver must never deduplicate on `idempotency_key` (Step 6).

### Phase 2 (complete)

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

**Phase 3 was approved and begun on 2026-09-13** (see *Phase 3* above).

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
| What phase are we in? | **Phase 3 in progress** (approved 2026-09-13). Phase 2 complete on its exit conditions, with the caveats recorded above. |
| What do I do next? | The next unfinished step in `docs/PHASE3_PLAN.md` §5, in wave order. Read §3 (decisions) and §4 (safeguards) first — they are binding. |
| What exists now? | Everything from Phases 0–1, plus: `trace-gateway` with auth, rate limiting, idempotency, RFC 9457 problems and triage; two Redis instances — a `noeviction` feature store with a completeness epoch and a declaration-driven write plan, and a disposable cache — holding 26 declared features with hybrid distinct-count storage; 18 declarative rules with Kleene semantics and fail-safe hot reload; noisy-OR scoring and banding; the transactional outbox; two workload profiles and a three-stream replay harness. |
| Which benchmark is the gate? | `benchmarks/gateway/REPORT.md` (representative profile). `benchmarks/gateway/ADVERSARIAL.md` is **not** a gate — it characterises saturation at 97% triage. |
| What must I never do? | `CLAUDE.md` §17, and §11 (ground-truth isolation) above all. |
| Where do I record results? | This file and `tests/acceptance/status.json` (which refuses `PASS` without evidence). |

**Known environment gaps:**
Temurin 17 is installed and an interactive shell's profile selects it, but a non-interactive shell
(tooling, `make` from elsewhere) still gets system Java 25 — Step 0 enforces Java 17 in the repository
itself rather than relying on a shell profile.
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

* **Step 1 — feature semantics and online-store correctness (lead): complete.** `P3.semantics-hardening`
  is PASS. U10 was decided on 2026-09-14 (R010 unchanged). U7 was decided the same day (ADR-0049) and
  is not implemented yet.
  * **Step 1a is committed** (`1ecd6a5`). ADR-0046 (Proposed) declares the semantics. The reference
    implements both evaluation modes and passes the hand-derived literal fixtures; 58 mutants and the
    mode-agreement properties keep the fixtures honest. The durable hole ledger (migration 0004) and
    the run guard landed with it.
  * **Step 1b is committed** (`129a590`), with a start-up fix on top.
    * **The Redis store conforms.** One Lua script records the scored transaction and reads its
      context atomically, and Redis refuses it whole when full. All 66 literal fixtures pass as
      served, so the 34 strict expected failures are gone. Account history is kept raw for 25 hours
      and reduced by the reference's own code, then folded in event-time order into a bounded
      profile prefix.
    * **Identity is store-wide.** A redelivery naming another account is still a redelivery. One
      carrying a different observation is reported as conflicting, and the gateway decides it
      rules-only with `observation_conflict` instead of evaluating it against another payload's
      context.
    * **A read behind the store's retention is absent, not a lower bound**, and its context stops
      vouching for the windows the store no longer holds.
    * **Merchant sums are exact at the released amount bound**: kept as base-1e9 limbs, because the
      square of the largest amount overflows `HINCRBY` and a failed script is not rolled back.
    * **The completeness guard is wired** into scoring and identity/device ingress. A refused,
      unreachable or breaker-skipped write records one hole per episode, and a restarted gateway
      inherits it and moves the epoch past it (`tests/chaos/test_feature_store_holes.py`).
    * **Identity events accept `X-Idempotency-Key`**, so their retries are recognised.
    * **New tests:** seeded months-long histories against the reference, with reach assertions
      (folding, lifetime gaps, redeliveries, reads behind retention); eight concurrent writers
      replayed in receipt order; all-or-nothing refusal on a real `noeviction` Redis.
    * **A start-up defect, found by the rebuilt gateway and fixed.** Start-up read the hole ledger
      before opening the database pool, so every restart counted the unreadable ledger as a hole:
      readiness said `withdrawn`, and the first request would have moved the epoch a day ahead with
      nothing lost. The pool now opens first, and a ledger unreadable at start-up that later reads
      empty withdraws nothing unless this process lost an observation meanwhile. The restart chaos
      test had handed the app an already-open pool and could not see it; it now builds the pool
      closed, as `build_state` does, and a new clean-restart test fails without the fix.
    * `SERVED_FEATURES_CONFORM` is true: all four of ADR-0046 §5's conditions hold.
  * **The Phase 2 gates are re-met on feature set 2.0.0, with every threshold unchanged.**
    * **Load gate:** `run_id: load-20260914-gateway-b43a75ce` passes every exit condition on the
      representative profile, from a cold store, on a clean tree (`benchmarks/gateway/REPORT.md`).
      An earlier run on `3020401` was refused by the harness and wrote no record: 26 iterations were
      dropped, and one decision was degraded when a feature-store call failed while Redis ran an
      unusually slow script, most likely on the gateway's 20 ms timeout. The gateway handled that as
      designed -- a hole recorded and completeness withdrawn -- but did not log the failure's type;
      it now does (`b43a75c`). The rerun from an identical cold start passed. One run each cannot
      show whether Step 1b makes such stalls likelier.
    * **Manual replay:** `benchmarks/gateway/triage-bands.md`, the 60,000-transaction `eval-v1`
      prefix on a cold store. The profile rules cannot fire there: on a store younger than their
      30-day horizon, known-device, habitual and tenure features are absent by design (ADR-0044).
    * **Rule re-validation, controlled.** Phase 2's replay report predates ADR-0044 (below), so a
      cold run is not comparable with it. The Phase 2 acceptance commit `c86c6cd` (feature set 1.0.0)
      and this code were replayed back to back on a store vouching for the whole replay, through the
      same client; the Phase 2 code reproduced its committed report exactly. Against it, 2.0.0:
      * fires the velocity rules R001, R003, R004 and R005 more often on fraud and still never on
        legitimate traffic -- self-inclusion, as ADR-0046 §2 declares;
      * **fires R010 on 9 more legitimate transactions, and 3 more fraud**, taking legitimate
        HIGH-or-CRITICAL decisions from 19 to 28 of 59,678. `eval/replay/attribute_rule_changes.py`
        attributes all of it to self-inclusion: exactly those transactions reach R010's five distinct
        accounts on their device only when their own account counts, and the ones that reach it
        either way match the 1.0.0 run's R010 counts. This is expected semantic drift, not a
        regression: R010 stays at 5 by decision U10, and its threshold is studied on `eval-v2`;
      * no longer fires R017 (4 fraud and 2 legitimate before): Q4c's home needs three located
        observations, and most accounts in the prefix have fewer;
      * fires R008 on 4 fraud transactions rather than 6, consistent with ADR-0046 §4 no longer
        counting a successful login as an identity change (the prefix holds two).
  * **Known limits and risks, recorded rather than resolved:**
    * A score decodes its account's last 25 hours in the gateway, so scoring cost grows with the
      account's velocity. A diagnostic measurement without a `run_id` showed it material for
      accounts with thousands of transactions a day, which an adversary controls. Step 12 measures
      it with a `run_id`; pre-aggregating account windows inside the script is the fallback design.
    * The memory model (`run_id: bench-20260913-memory-model-5c770259`) describes the Phase 2
      layout, not this one. Memory is re-measured in Step 12.
    * The script spans several entities' keys, so the store runs on one Redis instance, not a
      Cluster (Phase 12).
    * Key expiry is wall-clock garbage collection, sized for event time advancing at least as fast
      as the clock. That holds for the live gateway, the load harness and the time-shifted replay.
    * A running gateway learns of another instance's hole only when that instance withdraws it. One
      gateway runs locally; several instances need plan §4.1's writer fencing (Step 4).
* **Execution is single-agent from 2026-09-13**, at the user's instruction, to conserve usage.
  Implementation, integration and critic-style self-review run in sequence, and the lead integrates
  the worktrees below itself.
* **Step 2 — Kafka platform: integrated** (ADR-0047, Proposed). Built in the Kafka agent's worktree
  with every first-pass critic finding fixed; the lead merged it, checked the paced-replay fix the
  critic had blocked on, and added what integration needed. A single-agent self-review took the
  place of the critic's second pass, which did not run.
  * **Topics:** `deploy/kafka/topics.yaml` declares every released topic, and `scripts/kafka_topics.py`
    (`make kafka-topics`, `kafka-topics-verify`, `kafka-topics-budget`) is the only thing that creates
    them. It verifies drift, and derives the local disk bound, reservations included.
  * **Producer:** `trace_core.contracts.publish` is the one factory. It is idempotent and
    Java-partitioned (murmur2, pinned to Kafka's vectors), validates every event against its schema
    and keys it from the release ledger, and raises `EventPublishError` unless every accepted record
    was delivered. The generator's Kafka sink is built on it.
  * **Fault overlay:** `eval/replay/faults.py` injects seeded faults on a publish schedule that keeps
    fault-free records on time, and refuses the unpaced replay that made them late.
  * **Broker:** the compose broker is pinned by digest, keeps its log on its volume, has an internal
    listener, and runs a healthcheck that can fail.
  * **CI guard:** `tests/conftest.py` fails Docker-backed tests at setup when Docker is missing in CI.
  * **Docs:** EVENT_CONTRACTS (declared topics, per-topic dedup identities, no DLQ topics, the factory
    owning `trace_id`), ARCHITECTURE, LOCAL_DEVELOPMENT, OPERATIONS and TESTING.
  * **Evidence:** `make verify` with the new unit tests, and
    `tests/integration/test_kafka_platform.py` passed 11 against its own broker.
  * **Open, from the Kafka work:**
    * how Silver treats unknown enum values (Step 5);
    * where the mirrored §4.3 timing constants live (Step 6);
    * the `tx.scored.v1` byte reservation, budgeted but undeclared until that topic is released;
    * every broker started from this image sharing its default cluster id.
* **Step 3 — Delta spike and lake conventions: integrated** (ADR-0048, Proposed). U9 was decided on
  2026-09-14: data-platform identifiers are snake_case, CLAUDE.md §6 is amended narrowly, and
  `tests/unit/test_lake_identifier_consistency.py` pins that derived identifiers cannot diverge. Built in the Delta agent's worktree with every first-pass critic finding
  fixed; a single-agent self-review took the place of the critic's second pass.
  * **Observed on the pinned Delta 4.0.1**, each asserted by `tests/stream/test_delta_capabilities.py`:
    * how idempotent commits behave;
    * the silent-loss states: a reused app id, a second write in one batch, a log cleaned under a
      running query, and vacuumed files skipped under `ignoreMissingFiles`;
    * which protocol features each table property adds;
    * per-table VACUUM floors;
    * liquid clustering applied only by `OPTIMIZE`;
    * what a scan selected against what it read.
  * **Conventions built on it:**
    * one lake root (`TRACE_DELTA_ROOT`, anchored to the checkout);
    * `TableRef`, and declarations that cannot quietly add protocol features;
    * tables created from their declaration in one commit, and refused, never repaired, on drift;
    * provenance on every commit;
    * checkpoints that own their app id, read their own evidence and refuse the silent-loss states;
    * scan measurement that refuses rather than under-reports.
  * **Lead's additions:**
    * the merged `LakeContractError` family;
    * Spark's warehouse directory under the lake root, reserved against override;
    * the `delta` toolchain scope entry;
    * DATA_ENGINEERING, TESTING and OPERATIONS.
  * **Corrections ADR-0048 proposes to the approved plan, pending approval:**
    * deduplicate within each micro-batch before the insert-only MERGE, because Delta inserts every
      in-batch duplicate;
    * weigh the protocol cost of constraints and clustering in Q6;
    * declare VACUUM retention per table (Step 11);
    * guard data loss twice: a reader that forces `failOnDataLoss=true`, and the pre-start guard;
    * in the layout benchmark, report selected bytes and bytes read separately (Step 14);
    * create every table from its declaration before its first query (Steps 5–7).
  * **Evidence:** `make verify` with the lake unit tests. The full `-m stream` suite on Temurin 17
    passed 50 of 51: the one failure was the lead's own regression. The warehouse change made every
    Spark session log the lake root to stdout, ahead of the refusal line a toolchain test reads. The
    session factory now resolves the root quietly, and `tests/stream/test_session.py`, rerun after the
    fix, 9 passed, 1 warning. `tests/stream/test_delta_capabilities.py` passed in full.
* **Step E — `eval-v2`.** Stages 1, 1b and 1c are complete. Their code is integrated onto this branch.
  * **Integration.** The worktree branch `worktree-agent-aae9faa608636c9c8` was fast-forwarded to the
    phase branch, and the Step E work was committed on top. The phase branch fast-forwards to it.
  * **Drafts and evidence.** The eval-v2 draft ADR is now `eval/track_a/drafts/eval-v2-adr-draft.md`,
    and the Stage 1d probes and outputs are archived in `eval/track_a/audits/stage-1d-evidence/`,
    outside lint and type scope.
  * **`LPC-5` implemented (Stage 2 step 2)** in `data/generator/lpc5/`:
    - `declaration` pins revision 2 and checks every citation against the catalogue;
    - `frame` and `attributes` compute §4;
    - `judge` holds the statistical checks, and `rules` the exact and per-instance checks;
    - `controls` evaluates §14, and `run.evaluate` produces the report;
    - `data/generator/outcomes.py` is the one ADR-0049 outcome-row builder (DM-1), shared with
      the generator.

    Self-tests on hand-built rows cover every check at its stated boundaries, plus a smoke run on a
    small eval-v1-configured generation.

    **Until ADR-0049 is implemented (step 7), every run is invalid or fails for two known
    reasons:**
    - no availability provider exists for feature set 3.0.0 (§4.6);
    - `tx.authorization.v1` has no released schema, which S2c rule 1 flags.

    **No code names the outcome topic yet.** The topic release gate forbids naming an unreleased
    topic, so the OUT population is recorded as unreleased: outcome rows are derived only, and
    step 7 sets the stream when it releases the schema.

    **Verification slip, fixed.** Commit `78ad5ea` was made while `make verify` failed. Its output
    was piped through `grep`, which hid the exit status. Two gates had failed:
    - the topic release gate: outcome-topic literals had landed ahead of ADR-0049's release;
    - the secret scan: the pinned criterion digest.

    The next commit fixes both, and verification now gates on `make verify`'s own exit code.

    **Evaluation also changed:** LPC-2's decline signals can now read outcome rows, as `LPC-5`
    §5.2 declares, and planted rows carry their planned-event ordinal. This is metadata only and
    never serialised.
  * **LPC-4 module retired.** The partial, untested `label_proxy_audit.py` is gone: `LPC-5` §5.4
    carries R7–R9, so Stage 2 step 4 folds into step 2.
  * **Main checkout.** It still holds uncommitted copies of the same Step E files, from an
    integration attempt made there before the session moved into the worktree. Discard them
    before fast-forwarding `phase/03-stream-medallion`.

  What stages 1–1c delivered:
  * legitimate identity, device and decline activity;
  * planted-row markers removed;
  * label-proxy criteria LPC-1 to LPC-3 declared before any gated run. The `eval-v1` negative
    control fails them for the proxy reason, and gate-off output is byte-identical.

  **Stage 1d, the systematic label-proxy audit, is complete as analysis**
  (`eval/track_a/audits/label-proxy-audit-stage-1d.md`); no generator behaviour changed. It measured
  eval-v1 and probed the current gated generator structurally, never by model performance. The earlier
  fixes hold, but proxies survive: legitimate transactions never use a non-home IP or travel far from
  home, the declared time-of-day, merchant-popularity and channel corrections are not implemented, no
  episode starts in the last three days of the window, and two scenario timing constants are fixed.
  It proposes `LPC-5` -- no observable may be exclusive to planted rows, even a documented one, plus
  representation, calendar-coverage and fixed-offset rules -- and a Stage 2 order.

  **Reviewed by the user on 2026-09-14. No eval-v2 generation has started.**
  * **Audit and corrections.** The audit is accepted. M4–M6 and N1–N11 are approved conceptually,
    every one behind the gate; `eval-v1` stays byte-identical.
  * **Q1 extension.** A scenario's identity is its causal mechanism, its required causal relationships,
    its documented behavioural signature and its scenario-specific acceptance tests.
    - Nuisance parameters are not part of it: exact timestamps and offsets, amounts, merchant choice,
      window position, non-required entry modes, formatting and precision, and tie order.
    - N6–N9 are therefore approved as nuisance randomisation, with seed and manifest reproducibility
      kept.
    - To be recorded as a decision that updates ADR-0030 (Stage 2 step 5).
  * **U7 decided.** See UNRESOLVED DECISIONS; designed in ADR-0049 (Proposed), not implemented.
  * **`LPC-5` amended and frozen** as `eval/track_a/criteria/lpc-5.md`. Revision 1 was committed
    first; **revision 2**, the current freeze, has sha256 `49f401d1a469bd9874157a72915e33f88f9dfdaa88ecfaea2c5e0e35a8dc2b06`.
    - **Revision 2 applies the user's blocking corrections** of the same day:
      - a thirds rule for per-scenario calendar coverage, with the pooled 20-slice rule unchanged;
      - disclosure whenever the G2 coverage floor changes the natural scenario mix;
      - the outcome field renamed `authorization_outcome`.
    - **Generator decisions frozen with checks:**
      - G3: only `IMPOSSIBLE_TRAVEL` implies an impossible speed.
      - G4: no guaranteed device novelty for card testing or credential stuffing.
      - G5: randomised spacing for takeover, fraud ring and device farm.
      - G6: merchant-collusion amounts are account-conditioned; the signal is relational.
      - G7: credential stuffing gains `AUTHENTICATION_ANOMALY` and loses `IDENTITY_CHANGE`; card testing
        loses `DEVICE_SHARING`, because its mechanism no longer creates sharing.
    - **Fully mechanical.** An INTRINSIC_BEHAVIOURAL_SIGNAL allowlist of cited entries (55 in revision 2), each with a
      value set, exemption, composition rule, minimum effect and consequences; the S0–S8 checks; a
      declared amount mechanism (G1) and coverage floor (G2); and eval-v1, zero-rate and per-correction
      ablation controls.
    - **Revisions.** A threshold changes only by a numbered revision, with a fresh candidate generated
      after it.
    - **Predicted conflicts.** §18 lists 14, read from the code rather than from any run, for the
      diagnostic probe to confirm or refute. Those needing user decisions were resolved by revision 2.

## CURRENTLY FAILING TESTS

**None unexpected.** Deliberate expected failures:

* The generation-throughput budget, discussed above.

---

## KNOWN TECHNICAL DEBT

| # | Item | Impact | When |
|---|---|---|---|
| D4 | `compose.yml` declares `streaming`/`graph`/`llm` services nothing consumes yet | Profile shape reviewable but unexercised | Phase 3 in progress (Steps 2–3); Phases 5, 6 |
| D5 | `postgres:16` rather than `postgres:16-alpine` | ~250 MB more disk | Revisit if disk binds |
| D8 | macOS Docker keychain helper hangs, blocking cold registry pulls | Workaround in `LOCAL_DEVELOPMENT.md` | Environment, not code |
| D9 | Role passwords in `.env.example` are `change_me_locally` | Fine locally; a shared deployment must supply real values | Before any shared deployment |
| D11 | CI job logs need repo-admin rights to download | CI-only failures are diagnosed by local reproduction | Optional (`gh auth login`) |
| **D12** | **Generation throughput below the ROADMAP budget** | Slower dataset builds; no correctness impact. ADR-0029 names the batched-substream scheme as the first thing to try | Revisit if dataset size grows |
| **D13** | `identity.events.v1` / `device.events.v1` are produced but nothing consumes them yet | The contracts are exercised by the generator only | Phase 3 Steps 4–6 |
| ~~D1, D2, D3, D6, D7~~ | ~~Migrations, CI, OTel, lockfile, compose targets~~ | **RESOLVED in Phase 0** | done |
| ~~D10~~ | ~~No declarative SQLAlchemy models, so autogenerate is unused~~ | Still true and still correct: migrations 0001 and 0002 are hand-written because they are security-critical grants. Re-evaluate when ordinary application tables arrive | Phase 2 |
| **D14** | CI provisions no Redis or PostgreSQL, so the Redis conformance suite, the Redis store tests, the hole-ledger tests and the other service-backed integration tests skip there, loudly. Their evidence is local runs | CI cannot catch a regression in the online store or the ledger | Before Phase 3 exit: provision the services in `test-integration.yml`, or start throwaway containers in those fixtures as the capacity test does |

---

## OPEN RISKS

| # | Risk | Status |
|---|---|---|
| R2 | Java 25 is the system default; Spark 4.0 needs Temurin 17 | Resolved locally by Phase 3 Step 0 (ADR-0045): the repository selects and enforces Temurin 17. Confirmation on GitHub's runners is pending the first `test-stream` run |
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
| U1 | Delta table layout | Phase 3 Step 14, by benchmark, for local OSS Delta; Databricks layout in Phase 12. ADR-0015 stays `Proposed` until then |
| U2 | Whether a 3B-class local model can complete investigations within budget | Phase 6 |
| U3 | Whether Neo4j outperforms `PostgresGraphStore` at this scale | Phase 9 Arm G |
| U4 | Whether the Skeptic agent pays for its cost | Phase 9 Arm F — `UNUSUAL_LOCATION_DEVICE` was built deliberately ambiguous to give this ablation something real to measure |
| U5 | LightGBM vs XGBoost on measured PR-AUC | Phase 4 |
| U6 | Whether Phase 13 (Go gateway) is worth doing | Phase 13 entry |
| **U7** | **Decided 2026-09-14: authorization decisions are their own dated events.** A transaction's own outcome is post-decision and never enters its own features. Outcomes arrive as `tx.authorization.v1` events (field `authorization_outcome`). PostgreSQL records them first and decides duplicates and conflicts; the Redis store holds derived state only. An outcome is pending until its transaction is known with the same account, and pending outcomes never count. `declined_ratio_1h` reads only verified outcomes decided before `as_of` and known before the read, never the scored transaction's own (`CurrentObservation.PRIOR_KNOWN`, feature set 3.0.0). Historical sources replay the transaction first and the decision after it, through the declared delay model DM-1. Designed in ADR-0049 (Proposed). **Not implemented:** until Stage 2 step 7, earlier outcomes still come from scoring requests | Decided; implementation in Stage 2 step 7 |
| **U9** | **Decided 2026-09-14: snake_case** for every data-platform identifier -- schema and table identifiers and the lake directories derived from them, streaming query and checkpoint names, Delta transaction app ids, Databricks job and task identifiers, and metric and manifest identifiers for the same logical name. CLAUDE.md §6 amended narrowly; no mapping layer; a consistency test pins it (ADR-0048) | Decided |
| **U10** | **Decided 2026-09-14: R010 stays at `device_distinct_accounts_24h >= 5`.** The `eval-v1` replay delta -- 9 more legitimate and 3 more fraud high-risk decisions, all attributed to self-inclusion -- is expected semantic drift, not a regression. Not retuned on `eval-v1`, whose device-related label proxies would fit the ruleset to a flawed dataset. A threshold study follows on `eval-v2` (NEXT EXECUTABLE TASKS); any change is a separate, versioned ruleset decision | Decided |

---

## NEXT EXECUTABLE TASKS

Phase 3, in the wave order of `docs/PHASE3_PLAN.md` §5:

1. **User decisions still open:** acceptance of ADR-0046 to ADR-0049, with ADR-0048's proposed plan
   corrections.
2. **Step E Stage 2, in the approved order.**
   1. Done: amend and freeze `LPC-5` (revision 2).
   2. Done: integrate the Step E worktree; implement `LPC-5` with its §16 self-tests.
   3. Done (diagnostic, not acceptance evidence): eval-v1, regenerated from its manifest with its
      transaction digest verified, fails every §14.1 expectation on its named cell
      (`eval/track_a/lpc5_control.py --manifest`; report in
      `eval/track_a/audits/stage-2-evidence/lpc5-eval-v1-negative-control.txt`).
      - **Scale.** A fast-lane-scale run first left the S3 slice-20 and S5b expectations unmet: with
        so few instances their intervals are too wide to fail. §16.4 puts the controls at
        acceptance scale, and there both fail as intended.
      - **Predicted for eval-v2** from the same report, to confirm or refute in step 9:
        - **AHV-4 (`mcc_habitual`).** eval-v1 picks an unhabitual *merchant*, but the documented
          signature is "a merchant category it never uses". Step 6 draws the merchant from
          categories the account never uses. This implements the documented mechanism; no
          threshold changes.
        - **MC-5 (`mcc_habitual`, from the `MCC_ANOMALY` key).** Payers drawn without regard to
          category use a habitual category about as often as not, and frozen G6 draws payers by
          amount only. MC-5's E_min looks unreachable. If step 9 confirms it, the choice between a
          criterion revision and a generator rule goes to the user.
        - **DF-5 (`device_age` in `first-use`, `<1h`).** G5 spreads a device farm over up to a day,
          so an account's second transaction usually comes more than an hour after its first.
          DF-5's E_min looks unreachable under G5; same route.
        - **Cost.** The pure-Python evaluation is slow and memory-heavy at acceptance scale, and
          eval-v2 adds the identity, device and outcome streams. Watched in step 9; optimised only
          if it blocks a run.
   4. Folded into step 2: `LPC-5` §5.4 carries R7–R9, and the partial LPC-4 module is retired.
   5. Done: record the Q1 extension as ADR-0050 (Proposed). It also records G1–G7 and the eval-v2
      causal-key sets. The card-testing `DEVICE_SHARING` removal is flagged for the user's
      confirmation.
   6. In progress: the eval-v2 corrections behind the gate, committed in sub-units.
      - **6a done.**
        - G2 coverage floor, with its disclosure (`engine.coverage_mix`).
        - N10: `correlation_id` per business flow.
        - N11: millisecond rendering, and a tie order that does not depend on the label.
        - A switch per correction for the ablation controls
          (`BaselineIdentityConfig.disabled_corrections`), wired for T1–T3, M1–M3, N10 and N11.
        - Test fixtures that compare gate-on with gate-off set the floor to one, so both plan
          eval-v1's episodes. G2 has its own tests.
      - **6b done** (`data/generator/planted.py`, applied to every planned episode under the gate).
        - G1 amounts (N9), checked against `LPC-5`'s own recompute.
        - G4: card testing and credential stuffing pay from the account's own devices, resolved
          after the legitimate device plan. The stuffing logins keep their one shared device.
        - G6: one collusion price per instance, and payers for whom that price is ordinary.
        - G7 keys, with the new `EvidenceKind.AUTHENTICATION_ANOMALY`. The catalogue lists
          eval-v2 keys beside eval-v1's, and the ARCHITECTURE roster names the new kind.
        - M5 merchants, including `ANOMALOUS_HIGH_VALUE`'s documented new category (§18 item 4).
        - M6 channels.
        - Takeover change types drawn from the legitimate mix (§18 item 2).
        - Ablation switches wired for N9, M5 and M6.
        - `LPC-5` S7a holds on a small gated generation.
      - **6c next:** placement and timing: N6, M4, G5 (N7, N8), G3.
      - **6d:** legitimate look-alikes N1–N5.
   7. U7/N12 (ADR-0049).
   8. Ablation controls.
   9. Diagnostic eval-v2 probe.
   10. Final candidate.
   11. Frozen `LPC-5` acceptance.
   12. Freeze the manifest and digests.
   13. Re-run replay validation, rules validation, the R010 study and the Phase 2 manual comparison.

   No `LPC-5` threshold is tuned after the final candidate is seen.
3. **R010 threshold study (U10)**, once `eval-v2` is frozen and its label-proxy checks pass. Compare
   candidate thresholds on fraud recall, false-positive rate, legitimate and fraud high-risk counts,
   the business and risk trade-off, and confidence intervals or sample counts, with attribution
   confirming R010 is responsible. Any change is a separate, versioned ruleset decision.
4. **Confirm the first `test-stream` CI run on GitHub** executed Spark: it has only been reproduced
   locally so far (macOS session, and a Linux container for the hashed install).

---

## LAST VERIFICATION RESULTS

```
TRACE-X verify — 2026-09-14T03:47:56Z
  PASS  doctor
  PASS  acceptance-status
  PASS  check-claims
  PASS  codegen-drift
  PASS  openapi-drift
  PASS  ruff-format
  PASS  ruff-lint
  PASS  mypy
  PASS  test-fast
  PASS  bandit
  PASS  secret-scan
======================================================================
  phase 3   11 passed   0 failed   0 skipped     VERIFY OK
```

Run on the Step 1b tree before this entry was written. Outside `make verify`, on the same code with
the local stack up and `.env` loaded: `pytest -m integration tests/integration` passed 155 with none
skipped, and `tests/chaos/test_redis_down.py` with `tests/chaos/test_feature_store_holes.py` passed 9.
After the start-up fix, `make verify` at 2026-09-14T03:54:36Z again reported VERIFY OK, 11 passed; the guard's
unit tests and both restart chaos tests passed on that code.
At the Step 1 close-out, `make verify` at 2026-09-14T04:41:22Z again reported VERIFY OK, 11 passed.
After freezing `LPC-5` and writing ADR-0049 (documents only; no code changed), `make verify` at
2026-09-14T06:42:36Z reported VERIFY OK, 11 passed.

**Acceptance status: 21 PASS · 1 IN_PROGRESS · 44 NOT_STARTED · 0 FAIL · 0 BLOCKED** across 66 tracked
capabilities. `tests/acceptance/status.json` is the authoritative machine-readable record.
