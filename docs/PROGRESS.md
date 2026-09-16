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

### Phase 3 — HANDOFF (2026-09-15 ~11:00 UTC; session limit imminent). Read this first.

**Committed through `6166c1b`:**
- Steps 4, 5, 6 (Silver) and 7 (Gold);
- Step 12's depth cap (ADR-0046 §8), its score-latency and memory re-runs;
- CI provisioning;
- the exit rule (P3.eval-v2 non-gating);
- the Databricks strategy docs.

**Three agents in flight, each in its own plain worktree under `.claude/worktrees/`.** Briefs are in the
session scratchpad (`/private/tmp/claude-501/-Users-singh-Downloads-trace-x/b342966b-066f-48a1-b65e-5319fdbc026e/scratchpad/`),
and the decisions are summarised here in case that is gone. None of their work is integrated yet.
- **Step 8, parity** (`phase3-step8-parity`, base 633f1a0, told to fast-forward to 6166c1b).
  - *Deliverables:* comparator, arrival skew, collection guard, mutation self-tests, PARITY record, and
    a Proposed ADR (0056) with the lateness model and partition frozen.
  - *Open:* its in-process run's relay published 0 of 78 outcomes, no cause classified yet. Relay silent
    paths (`not handed over` / `delivery unconfirmed at flush`) are marked only in `app.outbox.last_error`.
    Probe with a grouped `SELECT` on `app.outbox`.
  - *Lead decision to apply BEFORE comparisons:* Gold's identity-event source is gateway-produced
    events only (producer `trace-gateway`, keyed by `correlation_id`); direct generator identity events
    are excluded (amend ADR-0055 at integration).
  - *Declared as-served exceptions:* ADR-0046 §8's four cases, plus Step 9's D6.
- **Step 9, Redis reconstruction** (`phase3-step9-hydration`, base f8c0573, told to fast-forward to
  6166c1b). Its Proposed hydration ADR (number 0057) is drafted in that worktree, and is
  not in this repository yet. Phase A was approved and Phase B is in progress.
  - *Approved decisions:*
    - D1: the input is Silver, not Gold (Gold's primitive tables are unused by hydration; debt).
    - D2: the quiescence argument under the writer fence.
    - D3: outcomes come from `app.authorization_outcomes`.
    - D4: additive marker hash and compare-and-set claim script in `redis_features.py`.
    - D5: it clears holes open at begin; it claims T below its own withdrawal marker B only.
    - D6: a fifth declared difference.
  - *Conditions:* property tests for D2, and a test that a lost write absent from history keeps T after
    it. ADR-0046 §5's "never move a later epoch earlier" applies to foreign epochs; align the wording at
    integration.
- **Step 11, lake maintenance** (`phase3-step11-maintenance`, fast-forwarded to 633f1a0). Phase B is in
  progress: the user chose Option A.
  - *The design:*
    - whole-file retention deletes, and `ignoreDeletes` on Bronze sources only;
    - the floor lives in Bronze table properties;
    - commit order: floor, then appendOnly off, then delete, then appendOnly on;
    - an append-only audit table reconciled on re-run;
    - required consumer positions: Bronze checkpoint, clean conservation, every Silver checkpoint's last
      whole version;
    - Silver resets start at the lowest version with a live file;
    - 30 days of log and 7 days of deleted files;
    - OPTIMIZE refused on Bronze.
  - *Tests required:* a floor commit racing an append; `bronze_coverage.py` treating retired offsets as
    neither present nor gaps.
  - *Also:* its draft ADR-0052 amendment is in its worktree.

**Queued (briefs in the scratchpad):**
- **Step 10**, recovery chaos (worktree `phase3-step10-resume` at 86fabdb, with partial `tests/chaos` files).
- **Step 13**, throughput: measured runs are the lead's, on a quiet machine.
- **Step 14**, layout benchmark.
Keep at most three agents active: four exhausted the session limit.

**Lead work, done and committed:** the Category A tenure and negative-membership fix (see the entry
below, feature set 5.0.0, ADR-0046 §3). Tell Steps 8, 9 and 11 to rebase onto it.

**Other lead items:**
- *The quiet Phase 2 load-gate re-run* (`scratchpad/quiet_load_gate.sh`: needs a clean tree, zero
  test or Spark processes, load under 2.0). The previous attempt is recorded below as invalid.
- *Debt:* relays run in-process without a recorder log nothing for failed passes.
- *Bookkeeping:* P3.medallion's command, after Step 8; ADR numbering (0056 parity, 0057 hydration).
- *Local stack:* the gateway was rebuilt from f8c0573 with the observation log on (`kafka:19092`),
  relay off, rate limit 60000/min. The worker relay has run since 10:11Z.


**Planning is complete and approved, and Step 0 (toolchain and dependency contract, ADR-0045) is
complete locally** — `make verify`, the real-JVM stream tests on macOS, a hashed install in a Linux
container, and a Linux build of the gateway image from its hashed runtime lock. Its first GitHub CI run
(including the new `test-stream` job) is still pending. Wave B (Steps 1, 2, 3 and E) is under way; see
*WORK IN PROGRESS*. Three Phase 3 capabilities are PASS: `P3.pin-failfast`, `P3.semantics-hardening` and `P3.observation-log`. The plan was built by six specialist reviews whose load-bearing claims were
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
  2026-09-14, makes authorization decisions their own dated events (ADR-0049, Proposed). Stage 2
  step 7 is implementing it. Feature set 3.0.0, in which a scoring request's outcome reaches no
  feature, is served by the reference implementation, the Redis store and the gateway (units 2a
  and 2b). Replays deliver or derive outcomes since unit 3a.
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
    * Memory was re-measured for this layout in Step 12
      (`run_id: bench-20260915-054159-memory-model-5ee55136`, ADR-0054).
    * The script spans several entities' keys, so the store runs on one Redis instance, not a
      Cluster (Phase 12).
    * Key expiry is wall-clock garbage collection, sized for event time advancing at least as fast
      as the clock. That holds for the live gateway, the load harness and the time-shifted replay.
    * A running gateway learns of another instance's hole only when that instance withdraws it. One
      gateway runs locally, and since Step 4 slice 2 the writer fence keeps every other instance
      not ready, so only one gateway writes online state (ADR-0051 §2).
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
* **Step 4 — durable observation log (lead): complete** (ADR-0051, Proposed; `P3.observation-log`
  IN_PROGRESS, reverted from PASS after a critic review). Started 2026-09-14, once the eval-v2 sub-track closed.
  * **Slice 1, fenced producer sessions: implemented.**
    * **Migration 0006, `app.producer_sessions`.** A trigger stamps `started_at`, `heartbeat_at`
      and `closed_at` from PostgreSQL `now()`, whatever a client sends. It refuses any change to
      a session's identity, recording `last_seq` without closing, and any change after close.
    * **Grants.** `trace_app` has column-scoped INSERT and UPDATE only, and no DELETE or TRUNCATE.
    * **`PostgresSessionLedger` and `PostgresWriterLock`** run on one dedicated autocommit
      connection. The advisory lock ends with that connection.
    * **`trace_core.observation.session.WriterSession`:**
      * start is fenced; without the lock the process is not ready and opens nothing;
      * sequence numbers are contiguous, including under concurrency;
      * any heartbeat that cannot confirm both the lock and the open row loses the session at once;
      * close happens only on a confirmed flush, and an unconfirmed one leaves the session unclosed.
    * **Evidence.**
      * `tests/unit/test_observation_writer_session.py` covers every transition with fakes.
      * `tests/integration/test_producer_sessions.py`, against the local migrated PostgreSQL, covers:
        * the lifecycle on the database clock;
        * that a client cannot supply a time;
        * that `trace_app` cannot rewrite, reopen or delete a session;
        * that the lock fences a second writer until its connection ends;
        * that a second `WriterSession` is not ready while the first holds the fence.
      * Like the other service-backed integration tests, this suite skips loudly in CI (D14).
  * **Slice 2, the gateway behind the fence: implemented.**
    * **`trace_core.observation.supervisor.WriterSupervisor`** runs one step every 2 s on its own
      thread, never on the event loop. It acquires a session on a dedicated connection and
      heartbeats it. After a loss it re-acquires with a NEW session; a lost session is never closed.
    * **Two guards the plan did not spell out, added after reviewing the failure paths:**
      * *Lease (6 s).* A heartbeat stuck on a silent network never returns to report a loss. So
        readiness requires a confirmation within the lease, and the request path fences itself.
      * *Takeover grace (8 s).* PostgreSQL can release a lock (a restart, a terminated backend)
        before its holder knows. A successor that finds an unclosed, recently heartbeated predecessor
        writes nothing until the grace has passed. A cleanly closed predecessor is not waited out.
      * *Residual, recorded in ADR-0051:* a process suspended between its readiness check and its
        write is not prevented, because the store checks no fencing token.
    * **The dedicated connection** (`writer_connection`) has 2 s connect and statement timeouts, TCP
      keepalives, and `tcp_user_timeout` where the platform supports it.
    * **Gateway:**
      * `/readyz` returns 503, and `checks.writer_session` names the reason;
      * scoring, identity events a released feature reads, and authorization outcomes get 503 with
        `Retry-After`, before the rate limiter and before anything is recorded;
      * device events, and identity events that feed no stream, are accepted;
      * the start-up writes to online state (hole inheritance, the epoch) run only inside the fence;
      * `writer_refused_total{surface}` counts the refusals;
      * shutdown closed the session as if everything had been logged: a defect, fixed in slice 3.
    * **The lock key is injectable**, so tests never contend with a running gateway. The integration
      fixture no longer deletes a gateway's own session row.
    * **Evidence (2026-09-14):**
      * Six suites passed, 93 tests, none skipped, run against the local migrated PostgreSQL:
        * `tests/unit/test_observation_supervisor.py`;
        * `tests/unit/test_gateway_writer_fence.py`;
        * `tests/unit/test_observation_writer_session.py`;
        * `tests/unit/test_gateway_observability.py`;
        * `tests/contract/test_gateway_http.py`;
        * `tests/integration/test_producer_sessions.py`.
      * `pytest -m chaos tests/chaos/test_writer_fence.py`: 1 passed. It terminates the writer's
        backend with `pg_terminate_backend`, and asserts three things:
        * the two writers are never ready at once;
        * the first writer stops at its lease;
        * the successor starts only after its grace.
      * The two integration tests that assert the absence of a live predecessor skip loudly while a
        fenced gateway holds the production lock.
      * OpenAPI regenerated: the identity route documents 503, and the 503 description names the
        fence.
      * `make verify` runs before the commit that records this entry, gated on its exit code.
  * **Slice 3, `tx.scored.v1` and the observation log: implemented.**
    * **A slice-2 defect, found while designing slice 3 and fixed here.** The slice-2 gateway closed its
      session at shutdown with `last_seq` 0, as if every observation had been logged, although it
      published nothing. Under the coverage rule that close would have certified the session's whole
      lifetime. No fenced gateway image had run, so only test rows carried it. A session now closes
      only on a confirmed flush of every number it assigned; without a broker it is left unclosed.
    * **The contract, released with its producer** (ADR-0051 §6; PHASE3_PLAN §3 Q1):
      * the transaction as served, flat in the payload so `payload.transaction_id` is the dedup
        identity, with `field_coverage`;
      * a decision summary, including the fired rules and the rule pack, threshold and feature-set
        versions;
      * served features as an extensible collection: state, value when available, approximation,
        source, `missing_fields`, and `lookback_completeness`, which tells a genuinely new entity from a
        store that could not vouch;
      * `observe_outcome`, `store_position` and `store_epoch`. The epoch is new on `ServedRead`, from
        both stores, because a position names a served state only with the epoch it was counted in;
      * `amount_minor` bounded at a signed 64-bit integer rather than `tx.raw.v1`'s bound, because the
        API accepts any integer (D17).
      * The RELEASED entry, the codegen model, the `topics.py` key and the `topics.yaml` declaration
        replace the planned entry and the reservation. The gateway is recorded as an
        `identity.events.v1` producer.
    * **Record size, measured before declaring.** The builder's records from the real pipeline exceed
      the reservation's placeholder. The declaration assumes a larger mean and caps local retention per
      partition so the local disk budget still fits (D18). The measured figures are recorded beside the
      declaration, and a test rebuilds the records and requires them to fit.
    * **`trace_core.observation.log.ObservationLog`:**
      * sequence, refused unless this process is the writer within its lease; write online; then
        produce with `block=False` and the `tracex-session-id` and `tracex-seq` headers;
      * scored transactions and identity events that feed a stream are covered. Device events,
        non-feeding identity events, replays and rate-limited requests consume no number;
      * a transaction refused for clock skew is refused before its number is assigned, and one that
        triage refuses with 503 is still published;
      * topics are verified at start and retried on the log's own thread, so no publish waits on
        broker metadata. A broker outage costs coverage, never a decision;
      * `observation_log_total{topic, outcome}` counts handed-over and lost observations, and
        `/readyz` reports `checks.observation_log` without gating on it.
    * **Deployment.** The gateway image lock gains `confluent-kafka`, and only it, from the `stream`
      extra. Compose and `.env.example` gain `TRACE_GATEWAY_KAFKA_BOOTSTRAP`, empty by default because
      Kafka is the `streaming` profile. The image is not rebuilt yet; the A/B does that.
    * **Evidence (2026-09-14):**
      * 606 targeted tests passed, none skipped, across 22 suites: unit, contract, conformance, and the
        PostgreSQL and Redis integration suites. They include `tests/unit/test_observation_log_sequencing.py`,
        `tests/unit/test_gateway_observation_log.py` and `tests/unit/test_scored_event.py`.
      * `tests/integration/test_observation_log_kafka.py`, against a throwaway real broker: 2 passed.
        Every number from 1 to `last_seq` arrived across both topics in one session, keyed and stamped
        with LogAppendTime, and a paused broker refused the close.
      * The ARCHITECTURE §18 Kafka row no longer claims a local WAL the gateway never had.
      * `make verify` runs before the commit that records this entry, gated on its exit code.
  * **Slice 4, the outbox relay: implemented, off by default until the A/B.**
    * **`trace_core.observation.outbox_relay.OutboxRelay`.** One pass is one transaction: claim the
      oldest unpublished batch with `FOR UPDATE SKIP LOCKED`, publish through the relay's own producer,
      flush, and mark published only the rows the broker confirmed. Delivery reports are counted per
      topic, so a batch is confirmed as a whole; anything unconfirmed records an attempt and is retried.
    * **A row that can never be published cannot block the queue.** An invalid or unkeyed event, or a
      stored key its event contradicts, is marked `refused: ...` and never claimed again. A missing
      topic, a shed record or an unconfirmed flush is a delivery failure, never a refusal.
    * **Migration 0007.** A trigger inserts every outbox row unpublished whatever the writer sends,
      makes what a row relays immutable, stamps `published_at` once from the database clock, and
      forbids forgetting an attempt. `trace_app`'s UPDATE narrows to the relay's three columns, and it
      has no DELETE or TRUNCATE. Applied locally.
    * **Gateway, option A.** `TRACE_GATEWAY_OUTBOX_RELAY` starts the relay on a gateway thread when a
      broker is configured. It is off by default until the controlled hot-path A/B decides where the
      relay runs (ADR-0051 §7). `/readyz` reports `checks.outbox_relay`, and
      `outbox_relay_rows_total{topic, outcome}` counts published, failed and refused rows.
    * **A stale pin, found and fixed.** `tests/integration/test_groundtruth_isolation.py` pins the
      migration head on purpose, and still named 0004 through migrations 0005 and 0006: the suite is
      integration-marked, and `make verify` does not select it. It now names 0007. The isolation
      denials themselves passed throughout.
    * **Evidence (2026-09-14):**
      * 276 targeted tests passed, none skipped, across 16 suites. They include
        `tests/integration/test_outbox_relay.py` and `tests/integration/test_outbox_guard.py` against
        PostgreSQL, and `tests/integration/test_groundtruth_isolation.py`, which migrates a fresh
        database to 0007 and round-trips the downgrade.
      * `tests/integration/test_outbox_relay_kafka.py`, against a throwaway real broker: 2 passed.
        The committed event arrived keyed and stamped with LogAppendTime before its row was marked, and
        a paused broker left the batch unmarked until a later pass delivered it.
      * `make verify` runs before the commit that records this entry, gated on its exit code.
  * **Slice 5, the coverage rule and the chaos evidence: implemented; `P3.observation-log` PASS, later
    reverted (see the critic review below).**
    * **`trace_core.observation.coverage.assess`** implements ADR-0051 §5 once, for Bronze (Step 5)
      and for the chaos tests:
      * a closed session is covered only when every number to `max(seen, last_seq)` is present;
      * an unclosed session certifies its contiguous prefix, and is otherwise a gap bounded by its
        last heartbeat plus the heartbeat interval, the delivery timeout and the clock margin;
      * an observation with an unknown session, or no usable headers, is a gap at its own time.
    * **`tests/chaos/test_observation_log.py`** produces every loss in plan §4.1's table. A child
      process (`tests/chaos/observation_log_harness.py`) composes the production writer supervisor on
      PostgreSQL, the scoring pipeline over the real Redis store and the observation log against a
      real broker, and kills itself with SIGKILL between the calls the gateway makes: before and after
      the session opens, after sequencing, after the online write, after produce, after the broker's
      acknowledgement and before close. A last case sheds for real against a paused broker. The test
      then reads the log, the ledger and the store's observation counter, and applies the rule.
    * **What held:**
      * a killed session is never closed, and its gap ends exactly at its heartbeat bound;
      * the certified prefix is always in the log, and after an acknowledgement exactly the delivered
        numbers are certified;
      * every observation the store recorded is in the log or inside a reported gap;
      * a shed observation is a missing number (`missing == (3,)`), and that session never closes;
      * a clean run closes with `last_seq` equal to the observations, with no gap.
    * **A collection clash, found by `make verify` and fixed:** the chaos suite and the slice-3 unit
      suite shared the basename `test_observation_log`, which pytest cannot import twice without
      packages. The unit suite is now `tests/unit/test_observation_log_sequencing.py`.
    * **A harness bug, found and fixed:** its cleanup unpaused the broker a second time, and `docker
      unpause` fails on a running container. The shedding behaviour itself held on that run.
    * **Scope of the evidence.** The kills hit the harness's composition of the production pieces,
      not the uvicorn gateway process. The gateway's own route order is covered by unit tests with a
      fake producer. Killing the rebuilt gateway under load belongs with the A/B, which runs that image.
    * **Evidence (2026-09-15):**
      * `pytest -m chaos tests/chaos/test_observation_log.py`: 9 passed, against the local PostgreSQL
        and Redis and a throwaway real broker.
      * 83 targeted tests passed, none skipped, across 9 suites, including
        `tests/unit/test_observation_coverage.py` (14), `tests/integration/test_observation_log_kafka.py`
        and `tests/integration/test_outbox_relay.py`.
      * `make verify` runs before the commit that records this entry, gated on its exit code.
  * **Critic review of slices 1-5 (2026-09-15): `P3.observation-log` reverted to IN_PROGRESS.**
    An independent read-only critic agent reviewed the five commits. The lead confirmed its main
    findings by reproduction before acting on them.
    * **Category A, both in the coverage rule, both confirmed.**
      * *A1:* `assess` has no notion of how far the log was read or when the ledger was read. A live
        session read before a later loss produced a gap that ends before it starts, and a session
        opened after the ledger snapshot whose records were all lost produced no gap at all.
      * *A2:* a gap started at the previous record's arrival time. A record can wait in the producer
        buffer, so that time can be later than the lost write: a write shed at 2 s sat outside the
        reported 34-37 s gap.
    * **Category B.**
      * *B1:* the chaos checks could not fail. Every unclosed session always has a gap, so "no
        invisible loss" held trivially, and nothing checked that a loss's time lies inside a gap.
      * *B2, confirmed:* between sequencing and the online write the gateway waits on PostgreSQL
        (`reconcile`) with no bound. `POSTGRES_TIMEOUT_S` is applied nowhere and the pool has no
        timeout, so a store write could land past the writer's lease.
      * *B3, confirmed:* a retryable failure mid-batch makes the relay re-publish the rows already
        handed over, on every pass.
      * *B4:* authorization outcomes change online state but sit outside session coverage. ADR-0051
        does this by design, while plan §4.1 point 1 calls any unsequenced online write a defect.
        This needs a user decision.
    * **Category C:** the unclosed-session bound should use the writer's lease and margin, not the
      heartbeat interval; a sequence above `last_seq` in a closed session is accepted silently; relay
      refusals depend on the relay's own contract version; migration 0007's docstring overclaims.
    * **What the critic checked and found sound:** every early return (replay, 429, 409, clock skew,
      writer refusal) comes before sequencing and any store write; close accounting only ever errs
      towards no claim; the heartbeat's lock check and the grace arithmetic; the contract, including
      no evaluation-only data in events.
    * **Disposition: A1, A2, B1, B2, B3, C1, C2 and C4 fixed; C3 open (2026-09-15).** The fix agent
      stopped part-way on an API error. The lead integrated its partial diff by hand, reviewed it,
      and finished the work.
      * **A1 and A2, the rule** (ADR-0051 §5 states the revision and its premises):
        * `assess` now takes the ledger read's database time and the log's read-through time;
        * gaps are bounded by the writer's own stamps (the envelope `ingested_at`), never by
          arrival times;
        * nothing written at or after `Coverage.through` is vouched for. `through` is the earlier of
          the two reads, less the clock margin;
        * a gap may be open;
        * stamps that no serial writer could produce are anomalies.
      * **C1 and C2.** An unclosed session's tail is bounded by the writer's lease and takeover
        margin, and only when the ledger was read after both could have run out. A number above
        `last_seq` is an anomaly.
      * **B1, the chaos checks.**
        * The harness records the time of each online write. The test now asserts that every lost
          write before `through` lies inside a gap of its own session, and that the log never vouches
          for it. It no longer passes just because some gap exists.
        * A new case reads the ledger mid-run, before a loss, and checks both directions: the later
          loss is not vouched for, and earlier logged writes are.
        * `tests/unit/test_observation_coverage_properties.py` uses hypothesis to generate
          serial-writer histories with losses, delays, heartbeats and snapshot times. It holds the
          rule to the same invariants.
      * **B2.**
        * The gateway pool (`_postgres_pool`) bounds acquiring a connection and each statement by
          `POSTGRES_TIMEOUT_S`.
        * The writer is re-checked immediately before every store write: scoring, identity events and
          authorization outcomes.
        * A lost fence writes nothing and answers 503. A sequenced number stays unpublished. An
          authorization outcome stays recorded durably, and completeness is withdrawn.
      * **B3.** Rows handed over before a retryable failure are still confirmed and marked. Rows after
        the failed one are not attempted: they count as `deferred` in the pass result and carry no
        attempt.
      * **C4.** Migration 0007's docstring no longer overclaims.
      * **C3, open.** Relay refusals still depend on the relay's own contract version.
    * **Two test expectations were wrong under the revised rule. The tests were corrected, not the
      rule.**
      * An unclosed session that lost a number has a run gap and a separate tail. The harness rotates
        accounts over partitions, so a kill can drop an earlier record while a later one on another
        partition arrives. The old "exactly one gap" held only because the old rule merged the two.
        The mid-flight `before_close` case failed in both full runs until this was corrected, and
        passed when run alone.
      * The mid-run ledger case at first paused before the produce step, so the record it relied on
        was still unsent. The pause now follows the hand-over.
    * **Two gaps found during integration, and fixed.**
      * The Kafka relay suite caught a regression from the B3 change. A paused-broker pass reported
        `failed=1` for three claimed rows, because the rows after the failed one were counted
        nowhere. `RelayPass.deferred` now accounts for them, and every claimed row has exactly one
        outcome.
      * B2 had no test of its own. `tests/unit/test_gateway_writer_recheck.py` makes the completeness
        reconcile outlast the lease, and asserts that nothing reaches the store on any of the three
        routes. Those tests were mutation-checked: with `ready` forced true, all three fail.
        `tests/integration/test_gateway_postgres_pool.py` proves that PostgreSQL cancels a stuck
        statement after 2 s and that an exhausted pool gives up after 2 s.
    * **Evidence (2026-09-15):**
      * `pytest -m chaos tests/chaos/test_observation_log.py`: 10 passed, in two consecutive runs;
      * `pytest -m chaos tests/chaos/test_outbox_watermark.py`: 1 passed;
      * 185 unit tests passed across the gateway, observation, coverage, property, history and
        writer re-check suites;
      * against PostgreSQL, 21 passed:
        * `tests/integration/test_outbox_relay.py`, `test_outbox_relay_watermark.py` and
          `test_outbox_delivery_watermark.py`: 19;
        * `test_gateway_postgres_pool.py`: 2;
      * against a throwaway real broker, `test_outbox_relay_kafka.py` and
        `test_observation_log_kafka.py`: 4 passed;
      * `tests/integration/test_gateway_concurrency.py` skipped loudly: it needs the deployed gateway,
        which the A/B rebuilds and runs.
  * **The A/B's first attempt was voided by a harness defect (2026-09-15).** Run ids were date plus
    commit, so the second arm (`log-on`) took the first arm's id and its record silently replaced run
    1's (`log-off`). The driver stopped at run 2 rather than continue. Run 1's raw k6 summary and run
    2's record are kept outside the repository as diagnostic history, never published. Run ids are now
    unique per run, and a record is never overwritten. Because arm order is part of the method, and
    the critic fixes change the hot path, the A/B reruns in full after those fixes land.
  * **The authorization-outcome delivery watermark (user decision, Option 1, 2026-09-15): implemented.**
    Authorization outcomes change online state outside session coverage (critic finding B4). The user
    chose an outbox delivery watermark over a second gateway sequencing path.
    * **Implemented:**
      * migration 0008, `app.outbox_delivery_watermark`: per topic, moves only forward, never in the
        future; `trace_app` may create and advance it, never move it back or delete it;
      * `trace_core.observation.outbox_watermark.advance`, which runs inside a transaction and never
        passes an unpublished, refused or still-uncommitted authorization row. It reads marks, never
        record counts, so duplicates cannot move it;
      * `trace_core.observation.history_completeness.assess_history`: COMPLETE only when both hold:
        * observation coverage vouches for the horizon: it ends before `Coverage.through`, no gap
          overlaps it, and there is no anomaly;
        * the watermark clears the horizon's end by the clock margin;
      * the relay advances the watermark in the transaction that marks rows, on every pass, an empty
        one included.
    * **Evidence (2026-09-15):**
      * `tests/unit/test_history_completeness.py` and
        `tests/integration/test_outbox_delivery_watermark.py`, against PostgreSQL, passed. They include
        a row written by a still-open transaction, which a naive bound would have passed; the
        watermark holds at it.
      * `tests/integration/test_outbox_relay_watermark.py`, against PostgreSQL:
        * the watermark advances only after a confirmed pass;
        * a partial batch stops at the first unconfirmed row;
        * extra Kafka records never move it;
        * a restarted relay continues from it;
        * a refused row keeps later horizons incomplete;
        * a row confirmed before a mid-batch failure is never delivered again, and the row after the
          failure is deferred, with no attempt.
      * `tests/chaos/test_outbox_watermark.py`, against a throwaway broker and PostgreSQL, 1 passed:
        * a pass has its backend terminated after the broker confirmed its three rows, so they stay
          unmarked;
        * the watermark stays at or before the oldest of them;
        * history over them is INCOMPLETE, although Kafka holds the records;
        * a fresh relay re-delivers them as duplicates, each transaction id at least twice;
        * history becomes COMPLETE only after the marks, and after a later pass carries the watermark
          past the horizon and the clock margin.
      * The first chaos draft expected COMPLETE straight after the marking pass. That cannot happen:
        no pass moves the watermark past its own start, and COMPLETE needs it past the horizon by the
        margin. The test was corrected, not the rule.
  * **`P3.observation-log` stays IN_PROGRESS.** The evidence above was committed in `1d216c5` with
    `make verify` green. The critic's re-review then found a Category A defect (below). That defect
    is now fixed and tested, and the capability returns to PASS at Step 4's exit, after the A/B.
  * **The A/B harness now pins its gateway image.** The driver used to start the gateway without
    building it, and no record named the image, so an image built from an older tree could have run
    unnoticed.
    * `run` builds the image once from the clean committed tree and records its id beside the run
      records.
    * Before every run, it checks that the tree has not moved and that the gateway runs that image.
    * `report` refuses runs from any other commit, and names the image.
  * **Critic re-review of `1d216c5` (2026-09-15): one Category A defect, fixed before the A/B.**
    * **A1, fixed.**
      * *The defect.* The check before a store write asked whether the process was the writer, not
        whether the session that numbered the observation still was. A request waiting in the
        completeness reconcile could lose its session while the same process re-acquired a new one.
        Its old-session write then passed. The critic proved at the rule level that such a write is
        vouched for.
      * *The fix.* `WriterSupervisor.ready_as(session_id)` now requires the session that numbered
        the observation, and both scoring and identity events pass it.
      * *Tests.* One supervisor test, plus one route test per path in which the reconcile loses the
        fence and re-acquires. All three fail when the session id is ignored.
    * **Already fixed when reviewed:**
      * the relay's fallback `RelayPass` (in `1d216c5`);
      * tests for the B2 refusal paths.
    * **B, fixed:**
      * Readiness reported `outbox_relay: running` even after the relay thread had died. It now
        reports whether the thread is alive. The tests fail against a relay whose failed pass
        escapes the loop, and against readiness that ignores liveness.
      * A host stamp within the clock margin of a PostgreSQL time counted as an anomaly, which kept
        history incomplete. A run is now an anomaly only when its bounds cross by more than twice
        the margin, and `vouches` is false while any anomaly stands.
    * **B, recorded as residuals or caller obligations:**
      * the completeness withdrawal happens before the writer check; it fails safe;
      * a Redis command can run after the client gave up on it; that causes a loss only together
        with a lost record;
      * the watermark proves delivery to Kafka, not presence in Bronze, so a caller certifying
        history rebuilt from Bronze must also require that.
    * **C, fixed:**
      * the property-test generator, widened by an agent (below);
      * the watermark chaos test now seeds the watermark, runs a concurrent relay pass during the
        stalled flush, and asserts exact values;
      * the wording of ADR-0052's headerless-record rule;
      * the checkpoint reset advice: start exactly where the old version stopped;
      * the stamp order stated in the coverage docstring;
      * `coverage` prints `open_gaps`, and ADR-0052 documents that it exits 1 while a writer is live.
    * **Evidence (2026-09-15):**
      * 269 unit tests across the gateway, observation, relay, coverage and Bronze suites, and 43
        checkpoint tests;
      * `pytest -m chaos` on `test_observation_log.py` and `test_outbox_watermark.py`: 11 passed;
      * 25 integration tests passed: the observation-log and relay suites against a real broker,
        and the pool, relay and watermark suites against PostgreSQL;
      * four mutation checks of the new tests, each of which failed them;
      * **the widened property tests**, from an agent, in `tests/unit/test_observation_coverage_properties.py`:
        * the generated histories now include:
          * host and broker clocks offset from PostgreSQL, within the margin;
          * heartbeats that commit late, across the ledger read, or never, while writes continue
            until the lease ends;
          * one to three sessions from one or two processes, with takeovers;
          * sessions opened after the ledger read;
          * duplicate and missing deliveries;
        * a new invariant: a history that keeps every premise is never an anomaly;
        * 5 tests, all passing on seeds 1-3, about 7 s each.
      * **Mutation checks of the rule.** Each of 10 mutants ran against a copy of `coverage.py`,
        leaving the worktree file untouched. Property-test columns count catches over 3 hypothesis
        seeds:

        | mutant | old property tests | widened property tests | hand-written tests |
        |---|---|---|---|
        | takeover margin dropped from the fence | 0/3 | 2/3 | caught |
        | margin dropped from the lease check and bound | 0/3 | 0/3 | caught |
        | `through` without the margin | 0/3 | 0/3 | caught |
        | run gaps not widened | 0/3 | 3/3 | caught |
        | unclosed tail not widened | 0/3 | 1/3 | caught |
        | stranger gap starts at its first record | 0/3 | 3/3 | caught |
        | stranger anomaly without the margin | 0/3 | 1/3 | caught, by a new boundary test |
        | strangers ignored | 0/3 | 3/3 | caught |
        | fence shrunk to 0.3 x lease | 2/3 | 3/3 | caught |
        | finding-3 fix reverted | 0/3 | 3/3 | caught |

        * The committed widened file catches 22 of the 30 mutant-seed runs. The first widened
          version caught 16, and the original caught 2.
        * Biasing generated times towards the bounds made two mutants caught on every seed: run gaps
          not widened, and the finding-3 fix reverted.
        * The cost fell on two margin mutants, the lease bound and `through`. Both now survive every
          seed.
        * While biasing, the agent found and fixed a bug in its own generator. One draw let a write
          land 1 ms past the lease plus the takeover margin, which the premises forbid.
        * So a passing property run is not evidence that the margins are right. The hand-written
          tests catch all ten mutants on every run, and they remain the deterministic guard.
  * **Slice 6, the controlled hot-path A/B: done. Option A is rejected, and the relay runs in
    `trace-worker`.**
    * **How it was decided.** `scripts/observation_log_ab.py report` applied the pre-registered rule
      as written. It used six runs from one pinned image on commit `8c9a85a`. The report is
      `benchmarks/gateway/observation-log-ab.md`, and every number in it cites its `run_id`.
    * **Scoring-core p99:** `log-relay` against `log-on` was within noise.
    * **Achieved rate:** outside noise, so the rule rejects option A. On that metric `log-relay` was
      the faster arm, and the noise band was tiny because the offered rate is fixed. The rule compares
      absolute differences, so it stands as written.
    * **The publisher's cost** (`log-on` against `log-off`) was within noise on both metrics.
    * **Attempts:**
      * `ab-records` was voided by the run-id collision (above);
      * `ab-records-2` stopped at run 1 (`log-off`), which failed the harness's integrity conditions;
      * `ab-records-3` recorded runs 1-4. Run 5 (`log-on`) failed integrity twice and passed on its
        third try. Run 6 passed.
    * **What the failed runs showed.** Each wrote no record, and its raw k6 summary is kept outside the
      repository.
      * Each failure was a short burst of gateway-side latency, with no errors and no lost data. The
        bursts were not tied to one arm.
      * PostgreSQL checkpoints did not coincide with them.
      * The host was in interactive use during the runs.
    * **How the failed slot was repeated.** The driver restarts only whole experiments, so the slot
      was repeated with the driver's own functions, from a script outside the repository. It used the
      same state reset, image, commit and order.
  * **Option B, implemented:** `services/worker/relay.py`.
    * It runs `OutboxRelay` in its own process, with its own bounded pool and producer.
    * It refuses to start without a broker or credentials.
    * It exits non-zero when the relay thread dies, and logs row counts.
    * The compose `worker` service runs it in the `streaming` profile.
    * The gateway's `TRACE_GATEWAY_OUTBOX_RELAY` stays, off, only to reproduce the A/B.
    * **Evidence:**
      * `tests/unit/test_worker_relay.py`: 5 passed.
      * `tests/integration/test_worker_relay_kafka.py`: 2 passed. The real process relays a committed
        outcome to a real broker, marks it, and stops with exit 0 on SIGTERM. Without a broker it
        exits 2.
      * The compose profile tests: 25 passed.
  * **Step 4: complete. `P3.observation-log` is PASS** on its acceptance command,
    `pytest -m chaos tests/chaos/test_observation_log.py`, which passed 10 twice on the fixed tree.
    The critic re-review's Category A defect is fixed, and the A/B is decided. Committed as `65857d4`.
  * **One unexplained verify failure (2026-09-15).**
    * A `make verify` run failed in `test-fast`. The gate keeps only its last five lines, which held
      a warning, so the failing test was not captured.
    * A full `test-fast` run then passed, and so did ten loops of this session's timing-sensitive
      suites.
    * The next `make verify` passed, and `65857d4` was committed on it.
    * The failure is recorded, not explained.
* **Step 5 — Bronze ingest (Spark agent, integrated by the lead): implemented, before exit evidence**
  (ADR-0052, Proposed; `P3.kafka-ingest` IN_PROGRESS).
  * **What exists:**
    * `trace_core.stream.bronze`: one append-only Delta table and one Structured Streaming query per
      released topic.
      * Rows hold the key, value and headers exactly as delivered, plus `kafka_topic_id` from the
        checkpoint's topic-id sidecar.
      * `failOnDataLoss` is on, and queries start at `earliest`; `-1` (latest) is refused.
      * The topic id is checked before and after every append.
    * `trace_core.stream.bronze_conservation.check_conservation` compares every checkpoint version's
      consumed ranges with the rows, per topic id and partition. It reports missing, duplicate,
      out-of-range, and skipped-between-versions offsets.
    * `trace_core.stream.bronze_coverage` is the one place Bronze meets the coverage rule. It
      supplies each record's writer stamp and session headers, and a strict high-water mark, and
      reads the ledger after the mark.
    * `services/stream/bronze.py` provides `run`, `conservation` and `coverage` commands.
  * **Critic review (2026-09-15).** The lead reproduced both Category A findings before fixing
    them.
    * **A1:** conservation judged only the current checkpoint version. Offsets trimmed before Bronze
      read them, followed by a reset at earliest, left a hole that read as conserved. Now:
      * every version is judged;
      * the skip is counted and is permanent;
      * a reset with an explicit start above the superseded end is refused.
    * **A2:** the coverage high-water mark left out partitions that were never read. It is now
      withheld when a covered checkpoint reports a problem or records no partition.
    * **B1-B5:**
      * the mark is strict, because broker timestamps have millisecond precision;
      * commits are read before the snapshot, and planned batches after it;
      * the topic id is checked per batch;
      * duplicates are counted per topic id;
      * envelope fields are extracted in Spark, so no value is parsed on the driver. A stream test
        proves that a value nested 100,000 deep and one that is not UTF-8 both read as null.
    * **Open:**
      * B6: a declared retention floor, needed before Step 11's bounded retention;
      * CI PostgreSQL for the live coverage tests, which skip loudly in `test-stream` today.
  * **Moved to the revised coverage rule (Step 4):**
    * `written_at` comes from the envelope's `ingested_at`;
    * the lease and takeover margin default to the gateway `WriterSupervisor`'s;
    * without a mark, nothing is vouched for;
    * `coverage` exits 0 only when there is no gap and a mark exists.
  * **Evidence (2026-09-15):**
    * 157 Bronze unit tests passed (coverage adapter, conservation, registry, checkpoints, service);
    * `pytest -m stream tests/stream/test_bronze_tables.py`: 5 passed, on real Delta 4.0.1 and
      Temurin 17;
    * `tests/integration/test_bronze_kafka.py`, against a throwaway real broker: 8 passed. It covers:
      * byte-for-byte delivery, including a null-valued header;
      * a restart with no loss and no duplicates;
      * poison values;
      * trimmed offsets: a refused skip, and a reset at earliest reported as skipped `[20, 25)`;
      * a recreated topic, refused;
      * a topic recreated under a running query: the query stops, and nothing of the new topic lands;
      * the two coverage cases: the exact gaps, and vouching before the mark once every partition has
        arrived.
* **Step 5 — Bronze: complete. `P3.kafka-ingest` is PASS (2026-09-15)** on the existing suites. No
  Bronze functionality was added to close it.
  * **Evidence:**
    * `pytest -m integration tests/integration/test_kafka_platform.py tests/integration/test_bronze_kafka.py`:
      19 passed, none skipped, against a real broker and PostgreSQL;
    * `pytest -m stream tests/stream/test_bronze_tables.py`: 5 passed;
    * the Bronze unit suites: 157 passed.
  * **Each invariant, and the test that proves it:**
    * produced records equal Bronze records, byte for byte:
      `test_every_released_topic_lands_in_bronze_byte_for_byte_and_is_conserved`, and the stream test
      `test_a_micro_batch_lands_byte_for_byte_through_the_checkpoint_and_is_never_parsed`;
    * an unparseable value is stored raw and blocks nothing:
      `test_a_poison_value_between_valid_records_is_stored_raw_and_blocks_nothing`;
    * a restart neither loses nor duplicates, and a batch Spark replays from its offsets is skipped by
      Delta: `test_a_restart_from_checkpoint_neither_loses_nor_duplicates`;
    * expired offsets and a changed topic identity fail loudly:
      `test_trimmed_unread_offsets_stop_bronze_loudly_and_trimmed_read_ones_do_not`,
      `test_a_recreated_topic_is_refused_where_spark_alone_silently_skips_its_first_records` and
      `test_a_topic_recreated_under_a_running_query_stops_it_and_lands_nothing_of_the_new_topic`;
    * conservation: the unit suite, and the stream test
      `test_conservation_statistics_computed_in_spark_equal_the_reference`;
    * coverage: the two Bronze coverage integration tests, and the stream coverage-rows test;
    * declared topics and a correct producer: `tests/integration/test_kafka_platform.py`.
  * **The acceptance command** named `tests/integration/test_kafka_ingest.py`, which was never
    written. It now names the suites that hold the evidence.
  * **Not Step 5's, and recorded where they belong:**
    * replay from an arbitrary offset into a fresh lake is `P3.checkpoint-resume` (Step 10);
    * CI PostgreSQL for the live coverage tests is a Phase 3 exit item (Kafka and Spark tests in CI,
      nothing skipped);
    * B6, the retention floor, is decided before Step 11.
* **Step 7 — Gold (batch): complete** (ADR-0055, Proposed; three-way conformance, 2026-09-15).
  * **What was built:**
    * `trace_core.stream.gold_plan`: the pure plan, and rows back into a `FeatureContext`.
    * `gold_features`: Spark SQL with no Python UDFs, never calling the reference.
    * `gold`: declarations, the build, `check_gold`.
    * `services/stream/gold.py`: `build` and `check`.
  * **Sources.** Each build reads `silver.tx_scored_v1`, `silver.identity_events_v1` and
    `silver.tx_authorization_v1` at pinned Delta versions. Every transaction the gateway accepted
    counts, including those the store failed to record. `silver.tx_raw_v1` is excluded, since a
    replayed transaction would count twice.
  * **Tables.**
    * *Primitive state:* `gold.observations`, `gold.minute_buckets` and `gold.distinct_buckets`.
    * *Point-in-time contexts:* `gold.tx_windows`, `gold.tx_profiles` and `gold.tx_previous`.
    * *Build records:* `gold.builds`, append-only, each with its lag behind the Silver commits it
      pins.
  * **Contexts, not values.** Gold stores contexts, and the shared feature definitions evaluate
    them. It claims no completeness.
  * **Replay safety.** A build records its plan in the `gold_build` checkpoint before touching any
    table, so `decide_start` judges it as it judges a streaming batch, and each table is one
    idempotent MERGE.
  * **The oracle held.** Gold passes `EventTimeCompleteConformanceSuite` unmodified, on a real JVM.
    * *Batching.* One build serves every fixture. Logs are id-prefixed per fixture, and four logs
      rebuilt alone must give identical rows.
    * *Guard.* It fails unless every fixture read a Gold context.
    * *Mutation check.* The agent planted five one-line defects in the SQL, and each was caught by a
      feature expectation.
  * **Evidence in the lead worktree.** Gold stream tests and the worker-interpreter test passed 86 on
    Temurin 17.0.18 and Delta 4.0.1, none skipped. Unit tests: `test_gold_plan` passed 17 and
    `test_stream_gold_service` 4; the control plane passed with the ADR-0055 index row.
  * **Found while building, fixed:** a BIGINT array index where Spark needs INT, an element-nullability
    cast ANSI mode refuses, and an empty `NOT ()` in the MERGE of an all-key table.
  * **Not tested yet, recorded:**
    - two concurrent builds;
    - the CLI against a real session (only fakes);
    - Kafka→Bronze→Silver→Gold end to end (fixtures write Silver rows directly);
    - freshness, which belongs to Step 13.
    - Every build is a full rebuild (ADR-0055 Negative).
  * **For later steps:**
    * *Step 9.* A gateway identity event's Silver identity (its envelope `event_id`) differs from the
      online store's observation id (`idev_…`, carried in `correlation_id`). Both derive from the
      same token, so values agree, but hydration must key on `correlation_id`.
    * *A risk.* The JVM's trigonometry is not bit-equal to Python's. Distances agree within the float
      tolerance, but an exact half-metre tie could choose a different home point.
    * *Naming debt.* Gold contexts report `ONLINE_ONLY`, as the reference's event-time-complete
      context does, though they are offline.
  * **Documentation.** DATA_ENGINEERING's Gold tier row and section now describe what was built; they
    had said "authoritative feature values, upsert".
  * **Next for this line:** Step 8 (parity), whose oracle Gold now is.
* **Step 11 — lake maintenance and resource guards: in progress.**
  * **Phase A design spike, done** (agent, diagnostic runs on Delta 4.0.1, no run_id).
    * *What the spike found.*
      - A floor that cuts data files makes DELETE rewrite them, and Silver's Delta reader then fails
        at that version, loudly.
      - `skipChangeCommits` passes a rewrite, but a fresh reader that started after it silently
        missed rows that were not retired. Rejected.
      - A delete whose predicate covers whole files commits removes only. With `ignoreDeletes`,
        running and restarted readers delivered every appended row exactly once, and any file a
        reader still needed after VACUUM failed loudly.
      - The floor, kept in Bronze table properties, is readable from the same snapshot as the rows,
        and it survives log cleanup.
    * *The recommendation relaxed a loss refusal, so it went to the user.*
  * **Decided by the user (2026-09-15): Option A.**
    * Retention deletes remove only whole files entirely below the per-(topic id, partition) floor.
    * Silver's reader of Bronze sets `ignoreDeletes`, on Bronze sources only. Every other
      loss-tolerant option and setting stays refused.
    * *Not chosen:* Silver reading Bronze's change data feed (protocol 1,7, and it reopens Step 6),
      and no local deletion (contradicts Q8).
    * *The lead's additions for Phase B:*
      - an append-only audit table, written after each floor commit and reconciled on re-run
        (non-authoritative, never assumed atomic with Bronze);
      - a crash test at every commit boundary, and a floor commit racing a Bronze append;
      - once a floor exists, Silver resets start at the lowest Bronze version with a live file (an
        ADR-0053 §7 change);
      - retention of 30 days of log and 7 days of deleted files, declared per table.
  * **Phase B:** being implemented by the agent in worktree `phase3-step11-maintenance`.
  * **B6, decided by the user (2026-09-15):** an audited local Bronze retention floor, recorded in
    ADR-0052's open questions and implemented in Step 11.
    * The retirement state is authoritative in the Bronze table or its own commit log, never in a
      separate audit table assumed to commit atomically with the delete.
    * The floor is per (topic id, partition), local-only; production Bronze stays append-only.
    * Offsets below the floor are RETIRED, not LOST.
    * The floor never passes a consumer or checkpoint position, and never retires data replay or
      recovery still needs.
    * Crash-safe at any point, idempotent, and audited on every advance.
    * No generalised retention framework.
  * **Already in place for `P3.resource-bounds`:**
    * loss-tolerant reader options and session settings are refused;
    * a restart the source log can no longer serve is refused;
    * table properties are allow-listed.
    * VACUUM's floor is each table's declared `delta.deletedFileRetentionDuration` (ADR-0048 (f)).
* **Step 12 — memory model correction (lead): in progress** (ADR-0054, Proposed).
  * **Memory, measured through the store itself**
    (`run_id: bench-20260915-054159-memory-model-5ee55136`, from clean commit `5ee5513`):
    * **Why a rewrite.** The Phase 2 model restated Phase 2 key shapes and fitted two-point lines.
      `benchmarks/features/memory_model.py` now drives `RedisOnlineFeatureStore`'s own scripts on a
      throwaway Redis of the compose image, and sums exact `MEMORY USAGE` by key family into curves.
    * **Ten-minute acceptance run:** 477.7 MiB projected. 188.8 MiB of that is an assumed
      authorization outcome per transaction, labelled as an upper bound.
    * **Steady state at 500 TPS:** 36.53 GiB, dominated by one string key per observation:
      45,000,000 keys at 416 bytes.
    * **The encoding jump is measured:** a sorted set costs 7,232 bytes at 128 members and 18,040 at
      129.
    * **Limit.** The model's rule implies 640 MiB; the configured 704 MiB stays until the Phase 3
      re-run of Phase 2's load gate measures this layout's real end-of-run memory (ADR-0054 §3).
    * **Evidence:** `tests/unit/test_memory_model.py` 7 passed; every measured family non-zero; the
      record is clean.
  * **Score latency by account depth, measured (2026-09-15)**, on a quiet machine and a clean commit
    (`run_id: bench-20260915-071647-score-latency-86fabdbf`, `benchmarks/features/SCORE_LATENCY.md`).
    * *What it measures.* One account, one throwaway Redis, 200 scores per depth. Depth is the
      transactions the account holds in its 25-hour raw window.
    * *The result.* Cost grows about linearly with depth, and the client's decoding and reductions
      dominate the Redis script's own time. At depth 8,192 a score took p50 171.5 ms and p99
      190.3 ms, and the script alone 32.8 ms per call; at depth 2,048, p99 54.8 ms and 6.9 ms
      (`run_id: bench-20260915-071647-score-latency-86fabdbf`).
  * **A security finding from it: the depth that makes scoring blind is adversary-controlled.**
    * *Why the depth matters.* The gateway's Redis client has a 20 ms socket timeout
      (`REDIS_TIMEOUT_S`) and no retries. A score whose script outruns it goes rules-only
      (`redis_unavailable`), is recorded as a coverage hole, and counts toward the store-wide breaker,
      which opens after 3 consecutive failures for 5 s. The script crosses the timeout between depths
      2,048 and 8,192 (`run_id: bench-20260915-071647-score-latency-86fabdbf`).
    * *Nothing bounds one account's depth.* The rate limiter is keyed per API token, not per account.
    * *What follows.*
      - A deep account's own transactions lose their feature-based checks.
      - A diagnostic probe with no `run_id` also saw other accounts' calls exceed the timeout while
        one deep account scored. It used a throwaway Redis and the gateway's client settings.
        Redis runs scripts on one thread, so a long script delays every other call.
      - The breaker did not open in that probe. The probe did not feed the deep account's own
        failures to it, so a breaker trip from a burst is plausible and unmeasured.
    * **Decided by the user (2026-09-15): cap the score-time read and make depth a signal.**
      * A score reads at most a declared number of each window's most recent observations.
      * Above the cap, the context says so explicitly: a depth-capped flag, with counts as declared
        lower bounds.
      * Policy treats a capped read as a risk signal, not a blind spot.
      * *Not chosen:* pre-aggregating windows in the Lua script, a per-account admission cap, and
        leaving it as a known limit.
      * **Designed by the lead: ADR-0046 §8, "Bounded score-time reads"** (Proposed).
        * *Exact at any depth.* Counts come from range counts. Identity events move to one sorted
          set per stream, so `failed_logins_1h` stays exact and R012 is not blinded during
          credential stuffing. The outcome counts behind `declined_ratio_1h` are exact, and the
          previous transaction and latest identity change are `LIMIT 1` reads.
        * *Capped.* Content is read up to `SCORE_READ_CAP = 512` per raw set, a value fixed from
          the recorded curve before any re-measurement, and only for the six content features.
          A capped window reads absent and INCOMPLETE, within `tx.scored.v1` as released, and the
          decision carries `history_depth_capped`.
        * *Declared risk signal.* `R019_history_depth_capped` fires on `account_tx_count_24h >=
          513`. Scoring is noisy-OR over fired rules, so no other decision moves.
        * *Where it applies.* As-served only; event-time-complete reads and Gold stay exact.
      * **Implementation assigned** to an agent in worktree `phase3-step12-depth-cap` (base
        3898a19): the reference, the Redis store, the gateway, R019 and the literal fixtures.
      * **A gap in §8 found before any code (2026-09-15), and closed by the lead.**
        * *The gap.* §8 called the account profile "already bounded". It is not: the profile is
          the folded prefix plus the whole raw 25-hour lifetime.
        * *Why it mattered.* The agent's probe (600 transactions, the oldest 88 on another device
          and merchant) showed a capped read would make a known device read unknown and a habitual
          merchant non-habitual. R008, R009 and R017 would then fire falsely.
        * *Decided (§8, amended), within the user's cap-read decision.* The profile reads the same
          capped set:
          - positive memberships stay exact;
          - the z-score and home stay exact when the capped read holds 128 same-currency amounts
            or 20 located points;
          - tenure is exact when the prefix holds the lifetime's start;
          - everything else is absent with `history_depth_capped`.
        * *The declared cost.* On a deep account, R008, R009 and R015 to R018 may abstain. Before
          §8, such an account's score timed out with every feature absent.
        * *Also decided:*
          - R019 weight 0.5 with no band floor, by analogy with R006; a floor is the policy lever
            if depth alone should escalate;
          - `FEATURE_SET_VERSION` 4.0.0;
          - a legacy identity-event set reads as not held until it expires, since migrating it
            inside the write script would be the O(N) stall §8 removes.
      * **Implemented and integrated (2026-09-15).**
        * *Exact at any depth.* Transaction, card and failed-login counts are range counts.
          Identity events now sit in one sorted set per stream (`ie:<acct>:fl`, `ie:<acct>:ic`).
          The previous transaction and latest identity change are `LIMIT 1` reads.
        * *Capped.* Content reads stop at 512. The profile is capped per §8 option A: positive
          memberships exact; z-score and home exact with 128 same-currency amounts or 20 located
          points; tenure exact from the prefix; everything else absent. The reference's AS_SERVED
          mode applies the same cap; EVENT_TIME_COMPLETE and Gold stay uncapped.
        * *Signal and versions.* A capped read is INSUFFICIENT_HISTORY with INCOMPLETE and
          `history_depth_capped`, within `tx.scored.v1` as released.
          `R019_history_depth_capped` (weight 0.5, no floor) is in pack 1.1.0, and
          `FEATURE_SET_VERSION` is 4.0.0.
        * *Legacy identity sets* read as not held, and expire with a dropped-through marker.
        * *Non-vacuity.* 14 new as-served fixtures failed on the old code in both the reference
          and Redis, plus the legacy fixture in Redis, before the change.
      * **Evidence in the lead worktree:**
        - conformance and the affected unit suites passed 413, including the 58 existing mutants;
        - Redis integration passed 100 under the heavy-suite lock;
        - Gold's stream suite passed 87 on Temurin 17.0.18, including the new event-time-complete
          fixtures uncapped.
        - In the agent's worktree, unit passed 2,189.
      * **Debt:**
        - §8 mutants in `test_feature_semantics_mutations.py`, for its off-by-one boundaries and a
          positive membership through the cap; the fixtures' pre-change failures show they are
          not vacuous;
        - four reference-vs-Redis cases where Redis serves only an absence, never a different
          number (ADR-0046 §8), for parity to expect.
      * **Score latency re-measured with the cap** on clean commit 870f7e9, a quiet machine
        (`run_id: bench-20260915-094759-score-latency-870f7e94`), against the run before the cap
        (`run_id: bench-20260915-071647-score-latency-86fabdbf`).
        * *Depth 8,192:* the script's time per call fell from 32.8 ms to 2.9 ms, and a whole score's
          p99 from 190.3 ms to 16.7 ms.
        * *Depth 2,048:* 6.9 ms to 1.9 ms per call, and p99 54.8 ms to 20.4 ms.
        * *The shape.* The cost stops growing past the cap. The script is now well inside the
          gateway's 20 ms Redis timeout at every measured depth, so a deep account no longer scores
          rules-only, and no longer stalls other accounts' calls.
      * **A memory-model re-run was discarded, not published.** It ran in the same job straight
        after the latency benchmark, which had already written its report, so the worktree was
        dirty and the record said so. The record is set aside in the session scratchpad. The model
        is re-run on a clean commit next.
      * **Memory model re-measured for the per-stream identity layout** (`run_id: bench-20260915-095206-memory-model-77d9afa4`, clean
        commit): the ten-minute run 477.7 MiB, which implies a 640 MiB limit, and steady state 37,401.9 MiB.
        The configured limit stays until the load gate re-runs (ADR-0054).
      * **A Phase 2 load-gate re-run was invalid, and nothing was published (2026-09-15, 09:57–10:07 UTC).**
        * *The configuration.* The gateway was rebuilt from f8c0573 (feature set 4.0.0, cap 512)
          with the observation log on, as in the Step 4 A/B's log-on arm. The per-token rate limit
          was raised for the run. The workload was `make load-gateway` defaults: representative
          profile, 500 TPS, 600 s.
        * *The script refused to record it.* It found two integrity failures (an achieved 478.0 TPS,
          and 13,158 iterations never started) and one unexpected degradation count of 3. The raw
          k6 summary is kept in the session scratchpad.
        * *What the evidence shows.*
          - The gateway's own histograms averaged 1.32 ms per request, 0.60 ms of scoring and
            0.47 ms of store reads.
          - Its completions ran at 500/s for most of the run. From 10:00:00 to 10:01:30 they dropped
            to about 370–400/s, and the backlog of about 11,000 requests queued behind the single
            async uvicorn worker, which gave the 1.6–3 s tail.
          - The three store timeouts coincide with slow Redis scripts at 10:05:28 and 10:07:03, and
            one slow script fell inside the dip.
          - The host was loaded at the time (load average about 3–3.8, the Docker VM at about 70%
            CPU, interactive browser use, two agents active). Step 4's A/B saw slot failures under
            interactive use too.
        * *Classified E* (machine contention). This is a hypothesis the next run must confirm, not a
          result: a code-path slowdown would be steady or grow with data, not one 90 s dip with full
          throughput either side. Capacity is **not** claimed from this run.
        * *Also:* the relay worker had failed to start in that job, because it was brought up with the
          streaming profile only. It is now started with both profiles.
      * **Still to do:**
        - the Phase 2 load gate re-run on a quiet machine: no agents active, and host load checked
          before starting, measuring latency with the cap in place.
      * Step 12 stays in progress until then.
* **Step 6 — Silver: complete** (ADR-0053, Proposed; `P3.event-time` PASS, 2026-09-15).
  * **Design (ADR-0053):**
    * **Tables:** one canonical Silver table per released topic, plus shared `silver.late_events`
      and `silver.quarantine`.
    * **Validation:** with the same strict generated contract models producers use. A value outside a
      released enum is quarantined, which settles Step 2's open question.
    * **Future skew:** quarantined, as §4.3 declares.
    * **Exact dedup** by each topic's declared identity. A deterministic pick within each batch comes
      first, then an insert-only MERGE, then a post-write uniqueness assertion. A same-identity record
      with different content is quarantined as a conflict.
    * **Late events** stay in Silver and are copied to `late_events`.
    * **Conservation:** every Bronze row becomes exactly one canonical row, a counted duplicate or a
      quarantine row.
  * **Lead, first:** `trace_core.stream.timing` holds timing semantics v1 as versioned constants and
    pure functions, with unit tests pinning each value at its boundary. It settles where the §4.3
    constants live.
  * **The Spark agent's build (2026-09-15), in its own worktree:**
    * `silver_rules`, `silver` and `silver_conservation`, the `services/stream/silver.py` CLI, unit,
      stream and integration tests. Integrated into the lead worktree unchanged.
    * On the integrated tree: 32 unit tests, 4 stream tests on real Delta and the Kafka integration
      test passed, the last two after the defect below was fixed.
  * **A toolchain defect found during integration, and fixed.**
    * *What happened.* The stream and integration suites failed with `ModuleNotFoundError: No module
      named 'trace_core.stream.silver_rules'` inside Spark's Python workers, although the driver
      imported it.
    * *The cause.* PySpark starts its Python workers with `PYSPARK_PYTHON`, or with the first
      `python3` on PATH when that is unset, never with the driver's interpreter. On this machine PATH
      named the main checkout's virtual environment, so the workers ran the main checkout's
      `trace_core`. Where a module exists in both checkouts, a worker can silently run different
      admission, digest or timing rules than the driver while the tests pass.
    * *Scope.* Silver's admission UDF is the first code that ships Python functions to Spark workers.
      A search found none in Bronze, its conservation and coverage, or the stream services, so the
      Step 5 evidence ran this branch's code. CI runs pytest and the workers from the same
      setup-python interpreter, and each image has one interpreter, so neither was affected.
    * *The fix.* `trace_core.stream.session.pin_worker_python`, called by `build_session`, sets an
      unset `PYSPARK_PYTHON` to the driver's interpreter. It refuses a `PYSPARK_PYTHON` or
      `PYSPARK_DRIVER_PYTHON` naming another environment. Environments are compared by the
      interpreter's directory, never through symlinks, which all lead to the same base interpreter.
    * *Tests.* `tests/unit/test_stream_worker_python.py` covers the rule without a JVM, and
      `tests/stream/test_session_worker_python.py` proves on a real JVM that a worker imports the
      driver's own `trace_core`: 41 unit tests with the toolchain suite, and the stream test, passed.
  * **The critic's review of Silver (2026-09-15)** found one Category A defect and three Category B
    defects.
    * **A1: a gateway retry could become the canonical scored transaction.**
      * *The defect.* A retry republishes `tx.scored.v1` with `observe_outcome = REDELIVERY` and
        new scoring fields, so its content differed and it was quarantined as an identity conflict.
        When the retry arrived first, it stayed canonical and the recorded delivery was quarantined.
      * *Why it matters.* As-served replay (Steps 8, 9) follows the recorded delivery's store
        position, and a redelivery carries the counter at the retry.
      * *Decided by the lead* within ADR-0053, still Proposed, and §4.3's store-counter order:
        - a preference order: RECORDED first, then store epoch and position, then arrival;
        - a digest over the transaction's own content;
        - the replaced row recorded as `superseded`.
      * *A premise corrected.* More than one RECORDED delivery can exist (a store restart, an
        expired identity key), so the order is total and the canonical row cannot depend on
        micro-batch boundaries.
    * **B1: a checkpoint reset re-appended `late_events`,** and the uniqueness assertion then stopped
      the query. `late_events` becomes a MERGE projection of the committed canonical rows.
    * **B2: the run command could exit 0 after a query failed.** It read a query's failure before
      its liveness, so a query failing between the two reads looked like a clean stop.
      * *The same race was in the Bronze CLI* (`services/stream/bronze.py`), fixed by the lead.
      * *Spark's own order.* Spark 4.0.1's bytecode shows `runStream` records the failure before its
        finally block marks the query terminated. So `_verdict` reads liveness first, and a
        requested stop stops every query before judging them.
      * *Evidence.* 4 new unit tests, including the race itself: `tests/unit/test_stream_bronze_service.py`
        6 passed.
    * **B3: an integer beyond 64 bits was stored as null.** It is now quarantined as `unrepresentable`.
    * **Category C fixes:**
      - C1: attacker-chosen field names are kept out of quarantine `detail`;
      - C2: `is_late` is judged on the stored milliseconds (`timing.is_late_ms`, lead, with a
        boundary test: `test_stream_timing.py` and `test_silver_rules.py` 23 passed);
      - C3: Python and Spark classify a repeated coordinate the same way;
      - C4: a replay test that depends on `replayed`;
      - C5: all six topics run on Spark;
      - C6: conservation's cross-table checks;
      - C7: a negative source version is refused;
      - C8: stale docs.
    * **Recorded as limits, not fixed** (ADR-0053 §7): a partly consumed Bronze version fails
      conservation closed (B4); starting from Bronze version 0 needs its log (C9); concurrent
      writes to the shared tables are untested; `publish._models` is private (C10).
    * **The critic's verdict on the agent's deviations:**
      - accepted: `silver.duplicates`, `replayed`, `not_log_append_time`, and the digest's exclusions;
      - accepted after a change: `late_events` as an append, now a MERGE projection.
  * **The fixes, delivered and integrated (2026-09-15).**
    * The Spark agent implemented A1, B1 to B3 and C1 to C7. Each fix has its own test.
    * *In the agent's worktree:* 59 unit tests, 11 stream tests on real Delta and the Kafka
      integration test passed, with none skipped.
    * *In the lead worktree:* the files are integrated. Lint, format and types are clean, and 166 unit
      tests passed, covering Silver, timing, both stream services, the toolchain, Bronze
      conservation and checkpoints.
    * *A1 checked against the gateway:* a redelivery carries the same store position as its
      RECORDED delivery, so it always sorts after it.
    * *The lead's review found no defect* in the digest, the order key and its SQL mirror, the
      supersede MERGE, the replay-safe commit order, detail sanitising, or conservation's
      cross-table checks.
    * *Docs:* the lead amended ADR-0053 and aligned DATA_ENGINEERING §2, ARCHITECTURE §6 and
      SECURITY §9 with it; they still described watermark dedup and `.dlq` topics. It also added the
      OPERATIONS playbook "Silver stopped, or not conserved". `P3.event-time`'s command now names the
      Silver suites.
  * **Open before the commit: concurrent writes to `silver.late_events`.**
    * B1's fix made `late_events` a MERGE into one unpartitioned table, run by every topic's query
      on every batch.
    * For an unpartitioned table, Delta's conflict check has no partition predicate to separate
      the writers, and `OpenedCheckpoint.merge` does not retry. So `silver run`'s concurrent
      queries may stop on a write conflict.
    * The lead and the agent raised the risk independently. No test runs two topics at once.
    * *Reproduced on the unchanged code (2026-09-15).* 7 of 16 sink batches of two topics, released
      together by a barrier, failed with `io.delta.exceptions.ConcurrentAppendException`
      (`DELTA_CONCURRENT_APPEND`: "Files were added to the root of the table by a concurrent update").
      * Every conflicting commit was the other topic's `late_events` MERGE.
      * No `duplicates` or `quarantine` append conflicted.
      * The six-query test passed that run, because its batches happened not to overlap. The
        barrier test is the deterministic reproduction.
    * *Fixed.* `silver.late_events` is partitioned by `silver_topic` in every environment
      (`LATE_EVENTS_LAYOUT`, `packages/trace_core/stream/silver.py`). The MERGE keeps its literal
      topic predicate. ADR-0053 §1 records the partition as a correctness requirement that the
      Step 14 layout benchmark may not remove.
      * Both concurrency tests and a declaration test are in `tests/stream/test_silver_tables.py`.
      * Integrated into the lead worktree; lint and types clean, and 40 Silver unit tests passed.
      * After the fix, 7 conflicts became none: the barrier test passes, and each topic ends with
        exactly its rows in `late_events`, `duplicates` and `quarantine`.
  * **Exit evidence, run in the lead worktree under the heavy-suite lock (2026-09-15):**
    * *Unit:* the `P3.event-time` command's unit part passed 48. The full unit and contract suites
      passed.
    * *Stream:* `pytest tests/stream -m stream` passed 69 on Temurin 17.0.18 and Delta 4.0.1, none
      skipped. That covers Silver tables 12 (both concurrency tests), the worker-interpreter test 1,
      Bronze 5, session 9 and Delta capabilities 42.
    * *Integration:* passed 22 against a real broker and PostgreSQL, none skipped. That covers
      Silver 1, Bronze 8, the Kafka platform 11 and the worker relay 2.
    * *A first attempt proved nothing, and is not counted.* The lead's script ran outside `make`, so
      `java` was the machine's Java 25 and `.env` was not loaded. Every JVM test skipped with its
      reason, the stream collection guard failed as designed, and the relay test skipped for want of
      a password. The rerun exported `JAVA_HOME` for Temurin 17 and loaded `.env`. The briefs for
      Steps 7, 10, 11 and 13 now state that setup.
    * `make verify` is green before the commit.
  * **Recorded, not fixed** (ADR-0053 §7):
    - a partly consumed Bronze version fails conservation closed;
    - a Silver reset needs Bronze's log from version 0;
    - there is one writer per canonical table;
    - `publish._models` is private.
* **Phase 3 exit item: service-backed tests in CI. Configured and proven locally, not yet run on
  GitHub (2026-09-15).**
  * **The gap.** `test-integration` started no services and installed no JVM.
    * About 130 integration and chaos tests use the compose PostgreSQL and Redis. In CI they skipped
      loudly, and the job stayed green.
    * The Kafka-backed Bronze and Silver integration tests skipped for want of a JDK.
    * `test-stream` selected the same broker-backed tests with no broker.
  * **The change.**
    * *Provisioning.* `test-integration` now installs Temurin 17 and the verified jars, and writes a
      throwaway `.env` from `.env.example` with random passwords and token. It starts the compose
      core services and migrates.
    * *Two pytest sessions.* The first stops the gateway, waits out the writer takeover grace (read
      from the supervisor), then runs `integration or chaos` without the gateway concurrency test.
      The second starts the gateway, waits for `/readyz`, and runs that one test.
    * *`test-stream`* runs `stream and not integration`.
    * *The CI skip guard.* `tests/conftest.py` fails any CI session whose marker expression selects
      `integration`, `chaos` or `stream` if a test skipped (`ci_skip_failure`, 3 unit tests).
    * `docs/TESTING.md` is updated.
  * **Evidence: the CI-equivalent run locally** (`CI=true pytest -m "integration or chaos"`, compose
    services, Temurin 17).
    * *The first run:* 257 passed, 2 skipped, 2 failed, 1 error. The guard failed the session as
      designed. Classified:
      - `test_migration_is_recorded_in_the_version_table` was a **stale test (C)**. Migration 0008
        landed in Step 4, but the pin said 0007. It went unnoticed because the suite never ran in CI;
        the pin is now 0008.
      - The writer-fence and producer-session tests need the gateway stopped, and
        `test_gateway_concurrency` needs it running. This was **CI topology (D)**, hence the two
        sessions.
      - A feature-store-holes setup error was a **harness race (D)**. `flushdb` ran before Redis
        answered after the previous chaos test's pause; the local Redis was empty, so data size was
        ruled out. The fixture now waits for Redis before flushing.
      - The predecessor test counted every unclosed session in the shared database within 60 s. A
        compose gateway stopped without a confirmed flush stays unclosed by design (ADR-0051 §4), so
        this was a **test-isolation defect (C)**; its window is now 5 s.
    * *The lead's own slip, not counted.* A re-run stored the compose command in a variable, which
      zsh does not word-split, so the gateway was never stopped.
    * *After the fixes:* session one (gateway stopped) passed 34 of the affected tests, none skipped;
      session two passed the gateway concurrency test.
  * **Not yet proven.** No GitHub Actions run of the new workflows exists: the branch is not pushed,
    and pushing is outward-facing. It is raised at Phase 3 exit.

* **A Category A defect in the feature semantics, found and fixed (2026-09-15).** Found by the Step 9
  hydration review, in live stores, not only hydrated ones.
  * **The defect.** `account_tenure_days` and every negative membership (an unknown device, a
    non-habitual merchant or category) were served COMPLETE when the store's completeness began
    inside the account's lifetime. `_tenure_days` and `_membership` gated on
    `ctx.completeness(horizon)` at `as_of`, which a store that started part-way through a lifetime
    passes. It then served the time since it started as tenure, and a confident zero for "unknown
    device": R008, R009 and R017 could fire on a device the account had used for years.
  * **The rule now** (ADR-0046 §3, feature set 5.0.0). A claim about the whole lifetime needs the
    store to vouch for the entire inactivity gap **before the earliest observation it holds** for the
    account. Only then does the absence of earlier observations prove the lifetime began there.
    Otherwise the feature is absent, INCOMPLETE and `history_incomplete`, never COMPLETE. ADR-0044's
    gate is kept as well; both must hold.
  * **How it is implemented.** A `LifetimeUnobserved` sentinel beside `DepthCapped`,
    `FeatureContext.vouched_lifetime_start`, a `FeatureValue.lifetime_unobserved` flag through the
    scored event and the gateway's degraded reasons. The shared definitions mean one fix covers the
    reference, Redis and Gold.
  * **Evidence.**
    - The new conformance fixture failed on the pre-change code (reference, 2 failed), so it is not
      vacuous.
    - After the fix: conformance and the affected unit suites 332 passed; Redis as-served conformance
      99 passed; Gold event-time-complete stream conformance 88 passed, none skipped.
  * **Consequence.** Parity (Step 8) and hydration (Step 9) build on 5.0.0, and a store that began
    recording inside a lifetime now says so instead of guessing.
* **Step 8 — parity framework: built and integrated** (ADR-0056, Proposed; `P3.feature-parity` awaits
  the lead's measured run).
  * **What it compares.** Two comparisons, from one recorded stream over an eval-v2 slice with the
    ADR-0047 fault overlay, published through Kafka and scored by a gateway.
    * *Implementation parity, as served:* what the gateway served against the reference's AS_SERVED
      reading of the same observations in the store's own order (epoch and position), verified
      against the store and refused on disagreement.
    * *Implementation parity, event-time-complete:* Gold against the reference's
      EVENT_TIME_COMPLETE reading, with approximate features compared exactly, since neither side
      estimates.
    * *Arrival skew,* recorded separately, deliberately keeping retention-driven absences: excluding
      them could only understate the gate.
  * **Measurement rules the lead decided, now in the ADR.**
    * A window is compared where the as-served read vouches for it, or where both sides are absent;
      an unvouched absence against a number is excluded and counted per feature and window, and
      reported as a fraction (ADR-0046 §5).
    * §5's exclusion fires before §8's declared situations, because a capped absence always renders
      INCOMPLETE; the situation is still counted, and absent-against-absent is still compared, so a
      state mismatch cannot hide behind an exclusion.
  * **Gold's identity source corrected** (lead decision): gateway-produced identity events only,
    keyed by `correlation_id`. Gold had admitted by type alone, so it could have counted events the
    online store never saw, and counted one replayed event twice under two identities. A stream
    fixture pins that a generator event and the gateway's event for one activity yield exactly one
    observation. ADR-0055 is amended at its next revision.
  * **Frozen before measuring** (ADR-0056): the representative gated partition and the adversarial
    recorded one, each with its slice, seed, fault rates in basis points, bands, dataset digest and
    lateness-model digest, pinned by tests.
  * **Evidence in the lead worktree:** 144 parity unit and mutation tests; 269 control-plane tests;
    Gold stream 89 passed on Temurin 17.0.18, including the event-time-complete conformance suite
    unmodified; the parity end-to-end integration run passed; ruff, mypy and check-claims clean.
  * **The agent's diagnostic run is NOT publishable** (dirty worktree, synthetic partition) and no
    measured run exists yet. It found 0 divergences on both sides across about 15,600 comparisons
    each, Silver's observations exactly equal to the store's, and the guard firing only where a
    stratum was genuinely under its minimum.
  * **Lead-owned pieces added at integration:** the ADR-0056 index row, `make parity` (also phony),
    and a `PARITY` entry in `scripts/check_claims.py` requiring every field the parity record's own
    contract demands, so the linter cannot accept a record the record module would reject.
  * **Next:** the lead's measured runs on a clean commit, then `P3.feature-parity`.
  * **Debt recorded:** `eval/replay/faults.py` cannot build a conflicting duplicate for
    `tx.authorization.v1` (it reads `payload.score`), so that fault is excluded from parity overlays.
* **Step 9 — Redis reconstruction: complete** (ADR-0057, Proposed; `P3.redis-hydration` PASS,
  2026-09-16, on `4fc23de`).
  * **What it does.** Rebuilds a lost online store by replaying history through the store's own
    idempotent Lua `record` — the path the gateway uses — under the ADR-0051 writer fence, and
    claims completeness only by compare-and-set on the epoch, only with evidence. Primitives are
    rebuilt, never values, so no second implementation of the online semantics exists.
  * **Shape.** `stream/hydration.py` (pure claim rules, the evidence read, and the
    Silver-to-observation projection built on Gold's own `observations`), `services/stream/hydrate.py`
    with the Bronze/Silver/Gold exit codes, and `redis_features.py` extended *additively* — a
    hydration marker plus start, update and compare-and-set claim scripts. No existing script or
    read path changed.
  * **Evidence (agent's run, at `3973c83`).** The ten cases ran together in one undivided session —
    10 passed, 0 failed, 0 skipped, 295.23 s — against a throwaway Kafka broker, a real Redis, the
    suite's own migrated PostgreSQL database and Delta 4.0.1 on Temurin 17: equality, all six
    refusals, the lost observation, idempotence, and a child SIGKILLed between two `observe` calls
    that resumes and converges. Plus 21 unit tests including three property tests for the
    quiescence lemma, and a non-vacuity probe in which 279 of 400 generated quiescent histories
    carry lost writes and every one still yields a claim.
  * **Acceptance, on the integrated and fixed commit.** The same command on `4fc23de`, after
    the adversarial review's three Category A fixes landed: **11 passed, 0 failed, 0 skipped,
    233.55 s**. The eleventh case is the fifth declared difference (`folded_out_of_order`) the
    review added, and it executed — 18.23 s setup, 8.91 s call — rather than passing
    vacuously. The agent's earlier run at `3973c83` is kept above as what was measured then,
    and is **not** the evidence for the capability: it predates the two defects in the
    hydrator that the review found.
  * **The memory figure is a diagnostic, not a target.** The hydrated namespace holds 82 keys
    against the live store's 81, and instance `used_memory` moved by under 100 KB. This does **not**
    measure the spike ADR-0057's Negative consequences predict: that needs a history longer than the
    widest retention, and this one spans ~37 hours, not 30 days. The instance-wide peak is since
    server start, shared, and is not attributed to hydration. It belongs with the Step 12 memory
    model.
  * **Lead review.** `compute_claim` and `event_time_image` were read against ADR-0057 §4: the claim
    is the `max` of independent lower bounds, an unbounded gap produces a *refusal* rather than
    being skipped, and `FUTURE_SKEW`/`BACKDATE` derive from the ingress contract constants
    (24 h / 90 d) rather than being re-declared, so they cannot drift from what the gateway
    enforces. No Category-A defect found.
* **Step 10 — checkpoint and resume under fault injection: built and integrated**
  (`P3.checkpoint-resume` awaits one single-session run; see below).
  * **Shape.** `tests/chaos/test_spark_resume.py` (7 injection cases), `spark_resume_harness.py`,
    which drives the real `services.stream.bronze|silver` entrypoints and SIGKILLs its own process
    group at exact commit points, and a directory-scoped collection guard. **No production code
    changed and no defect found.**
  * **Evidence.** 7 passed, 0 failed, 0 skipped across five `-k` splits, 1,365.6 s of pytest. Process
    kills of both jobs landed inside a batch every time. A crash after the sink commit but before
    Spark's commit entry resumed from byte-identical recorded offsets with Delta skipping the batch;
    the same window with the offsets entry also lost refused to start (exit 2, nothing written).
    Silver's five reachable per-target commit prefixes each resumed to exactly one commit per
    target. A graceful broker restart (4.9 s) was ridden through; `docker kill` (19.0 s) stopped
    loudly with `bronze_query_failed` (exit 3) and caught up. Replay from arbitrary offsets into a
    fresh lake gave 46 byte-identical Bronze rows, with 13 identities wholly in range equal and 9
    straddling ones correctly excluded — non-vacuous in both directions. The JVM-kill case ended in
    a real Gold build: 135 `gold.observations` rows, equal to the 135 canonical outcomes the
    published records imply, and `gold check` consistent.
  * **Why the capability is not recorded yet.** All seven cases passed, but only ever in five
    separate splits; the suite has never run as one session. CLAUDE.md §16.3 wants the command
    actually executed, so the split results stand as what was measured and the capability waits.
  * **ADR-0053 §4 corrected by this work.** The ADR gave the sink's targets as canonical first; the
    code commits quarantine, duplicates, canonical, then `late_events`, and carries a comment that
    the order matters for a replay between two commits. The ADR (Proposed) now states the real
    order, so the state where the canonical MERGE landed alone is no longer implied to be reachable.
  * **Debt.** `-m "chaos and not stream"` would arm the collection guard while deselecting every
    injection case, a false failure; no repo command uses that form.
* **Phase 3 adversarial review (2026-09-16), and the seven findings it produced.** CLAUDE.md §10's
  "what is most likely to be wrong even though the current suite is green", run read-only over the
  integrated Steps 6-10. Three Category A defects, each verified in the code before being fixed, and
  each with a test that fails against the old behaviour.
  * **A1 -- parity discarded every Gold-side absence.** `ParityTally.add` applied ADR-0046 §5's
    unvouched exclusion in *both* pairings. Neither side of the event-time-complete pairing records
    lookback completeness (both are built with `Observed.of`, leaving it `None`), so the
    `!= COMPLETE` test held for every Gold absence: each was downgraded to NOT_VOUCHED and
    subtracted from `compared`. The detector for exactly the missing-row defect ADR-0055 §3 rules
    out by construction was disabled. Fixed by gating on the pairing. **Consequence:** ADR-0056's
    diagnostic figure "event-time-complete parity: 15,600 comparisons, 0 divergent" was measured
    through that mask and is **withdrawn**, not re-cited.
  * **A2 -- a hydration crash between the marker and the withdrawal wedged the namespace.**
    Recovery required deleting keys by hand, which ADR-0057's own alternatives reject. The fix went
    further than the finding: `_adopt` never withdrew, so relaxing its refusal alone would have
    adopted a store with no epoch and let `observe`'s `NX` date it from the first replayed
    observation (ADR-0057 §5.2).
  * **A3 -- identity conflicts did not survive a hydration resume**, so a resumed run claimed `T`
    earlier than the evidence allowed. Silent, and in the direction that costs data.
  * **B4** the resume's foreign-write tolerance used the resuming run's batch size, not the crashed
    run's: a crash at `--batch-size 2` resumed at the 1,000 default tolerated 1,000 foreign writes.
  * **B5 -- nothing bounded the parity exclusions.** `not_vouched_fraction` was computed, recorded,
    and read by no verdict; the volume floors bind only the approximate features. A run could
    exclude nearly every comparison of an EXACT feature, report zero divergences and pass. A
    per-feature bound (`MAX_NOT_VOUCHED_FRACTION_PER_FEATURE`, 0.5) now fails the guard in either
    pairing -- **chosen, not derived, and frozen before any measured run**, and recorded in
    ADR-0056's guard list. It is a guard constant, not part of the §5 declaration, so it changes no
    frozen digest.
  * **C6** ADR-0056 cited a PARITY `run_id` that cannot resolve by construction (a diagnostic record
    is never retained in `eval/manifest/`), and its §7 still said `check-claims` did not know record
    type PARITY after the lead added `PARITY_REQUIRED`. Both corrected; `make check-claims` passes.
  * **C7** ADR-0057's fifth declared difference (`folded_out_of_order`) was asserted by no test,
    because every acceptance history is written in increasing event-time order. An eleventh test now
    produces it and asserts the declared *direction*.
  * **Checked and clean:** the quiescence lemma's property generators, the declared total order on
    all three sides (Python, Gold, Redis, including Spark's binary collation), the outbox relay's
    delivery watermark, Silver's pure dedup/supersede layer, and ground-truth isolation.
* **Three findings from Step 8's parity work, resolved (2026-09-15).**
  * **F2, a defect, fixed: one undeliverable row starved the outbox, silently.** The relay claimed
    oldest-first and abandoned the pass at the first transient failure, so a single blocked row held
    back every later row, across topics, for as long as it kept failing. One such row starved 638
    outcome rows in the agent's run. Nothing was logged: the counts went to the relay's counters, and
    the reasons only to `app.outbox.last_error`.
    * *Correctness was never at risk.* The delivery watermark never passes an undelivered row, so
      nothing was claimed complete. It was a delivery stall, and an invisible one.
    * *Now.* A failed row blocks only its own topic and partition key, which is the only ordering
      Kafka guarantees; every other lane keeps draining. A pass that cannot publish everything it
      claimed logs `outbox_relay_rows_not_published` with its counts, blocked lanes and first error.
    * *Tests.* Three unit tests (`tests/unit/test_outbox_relay_lanes.py`): another lane drains, a
      later row on the blocked lane waits rather than overtaking, and every claimed row is exactly
      one outcome. One integration test fails a single partition key, as an unavailable partition
      leader does, and runs under the heavy-suite lock.
    * *Runbook.* OPERATIONS "Outbox not draining" now names the log line and the per-lane behaviour.
  * **F1, not a defect: late reads behind the store's held history.** ADR-0046 §5 already declares it:
    each structure keeps its widest window plus the late-arrival margin, and a read reaching behind
    what is still held serves absence and stops vouching. The agent's 70-minute threshold is the card
    window's retention. Parity's rule, recorded in its ADR: compare where the as-served read vouches,
    or where both sides are absent; count and report the rest; never model trimming in the oracle.
  * **F3, disproved: outcome ingress does bound future skew.** `POST /v1/events/authorization` runs
    the same clock check transactions use and answers 422 beyond it.
* **Phase 3 exit rule, decided by the user (2026-09-15).** "All REQUIRED Phase 3 exit capabilities
  must PASS."
  * *The tracker.* `P3.eval-v2` is a tracked, non-gating evaluation artifact, not a required exit
    capability, flagged `"gating": false` in `status.json`. Its FAIL stays as recorded: a frozen
    dataset, strict LPC-5 acceptance FAIL, zero Category A findings, limitations disclosed, never
    described as LPC-5 compliant.
  * *What never happens.* It is not converted to PASS, SKIP or any misleading status, and LPC-5
    reopens only for a genuine downstream Category A defect.
  * *Where it is written.* ROADMAP's Phase 3 exit conditions and PHASE3_PLAN §7 and §8 are amended to
    match; they had said every capability, eval-v2 included, must PASS.

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

    **Until ADR-0049 is implemented (step 7), every run is invalid or fails.** Two known reasons,
    one since resolved:
    - no availability provider exists for feature set 3.0.0 (§4.6); step 7 unit 3 adds it;
    - `tx.authorization.v1` had no released schema, which S2c rule 1 flags. Step 7 unit 1
      released it.

    **The outcome topic is named since step 7 unit 1.** Before it, the topic release gate forbade
    naming an unreleased topic, so the OUT population was recorded as unreleased. Unit 1 set the
    stream when it released the schema.

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
    first. The current freeze is **revision 4**, sha256
    `96dad012376f2b30aaca9450ed14c21a5803f9f3042e4bbb4f21ee672190ee28`; revisions 3 and 4 are recorded
    under Stage 2 step 9.
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
| D12 | `hour` and `daypart` for takeovers and impossible travel are exempt in `LPC-5` only as incidental consequences of synthetic episode spans (revision 3 §6.6) | A model trained on eval-v2 could learn time of day as a shortcut | Phase 4: feature selection may not admit them on eval-v2 evidence alone (`docs/ROADMAP.md` Phase 4) |
| **D12** | **Generation throughput below the ROADMAP budget** | Slower dataset builds; no correctness impact. ADR-0029 names the batched-substream scheme as the first thing to try | Revisit if dataset size grows |
| **D13** | `identity.events.v1` / `device.events.v1` are produced but nothing consumes them yet | The contracts are exercised by the generator only | Phase 3 Steps 4–6 |
| ~~D1, D2, D3, D6, D7~~ | ~~Migrations, CI, OTel, lockfile, compose targets~~ | **RESOLVED in Phase 0** | done |
| ~~D10~~ | ~~No declarative SQLAlchemy models, so autogenerate is unused~~ | Still true and still correct: migrations 0001 and 0002 are hand-written because they are security-critical grants. Re-evaluate when ordinary application tables arrive | Phase 2 |
| **D15** | **Deferred evaluation hardening:** `LPC-5` §14 controls at acceptance scale on the frozen eval-v2 candidate -- the zero-rate control and the 21 full-scale ablations -- were not run (user decision, 2026-09-14). Unit self-tests, smoke-scale controls and the control framework exist | An ablation that would not fail its check at full scale is unverified at full scale | Only for a final research or evaluation release; not Phase 3 |
| **D16** | **Research-grade synthetic-data work:** eval-v2's `LPC-5` Category B consequences outside the declared values, and its Category C support and power limits at acceptance scale (rare burst values, MC-1 and MC-2 near misses) | eval-v2 is disclosed as not `LPC-5` compliant; no Category A defect is known | Deferred; reopened only by a downstream Category A finding |
| **D17** | **The API accepts an unbounded `amount_minor`.** The observation log bounds it at a signed 64-bit integer, the width consumers parse, so a wider amount is scored but refused by the log | That transaction is missing from history and its writer session cannot close: detectable, never silent | The next breaking API revision; narrowing v1 is a breaking change |
| **D18** | **Local `tx.scored.v1` retention is capped per partition** so the measured record size fits the local disk cap (`deploy/kafka/topics.yaml`) | A run that writes more before Bronze reads it trims unread segments, and Bronze stops loudly (`failOnDataLoss`) | Step 5 Bronze and the streaming throughput evidence: run Bronze during a load, or size the run |
| **D19** | **A reused identity-event key with a different event time, while the replay cache is down,** gets a new envelope `event_id` on the log while the store records a conflict under one observation id (ADR-0051 risks) | History and the online store disagree about that one event | Step 6 exact dedup and conflicts |
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
| **U7** | **Decided 2026-09-14: authorization decisions are their own dated events.** A transaction's own outcome is post-decision and never enters its own features. Outcomes arrive as `tx.authorization.v1` events (field `authorization_outcome`). PostgreSQL records them first and decides duplicates and conflicts; the Redis store holds derived state only. An outcome is pending until its transaction is known with the same account, and pending outcomes never count. `declined_ratio_1h` reads only verified outcomes decided before `as_of` and known before the read, never the scored transaction's own (`CurrentObservation.PRIOR_KNOWN`, feature set 3.0.0). Historical sources replay the transaction first and the decision after it, through the declared delay model DM-1. Designed in ADR-0049 (Proposed). **Partly implemented (Stage 2 step 7):** the stream and its producers (unit 1); feature set 3.0.0 in the reference implementation (unit 2a), the Redis store and the gateway (unit 2b); outcome replay (unit 3a). The `LPC-5` availability provider follows | Decided; implementation in Stage 2 step 7 |
| **U9** | **Decided 2026-09-14: snake_case** for every data-platform identifier -- schema and table identifiers and the lake directories derived from them, streaming query and checkpoint names, Delta transaction app ids, Databricks job and task identifiers, and metric and manifest identifiers for the same logical name. CLAUDE.md §6 amended narrowly; no mapping layer; a consistency test pins it (ADR-0048) | Decided |
| **U10** | **Decided 2026-09-14: R010 stays at `device_distinct_accounts_24h >= 5`.** The `eval-v1` replay delta -- 9 more legitimate and 3 more fraud high-risk decisions, all attributed to self-inclusion -- is expected semantic drift, not a regression. Not retuned on `eval-v1`, whose device-related label proxies would fit the ruleset to a flawed dataset. The threshold study ran on `eval-v2` on 2026-09-14 and found no clear better operating point, so R010 stays at 5 (`benchmarks/gateway/r010-threshold-study-eval-v2.md`); any change is a separate, versioned ruleset decision | Decided; studied |

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
   6. Done: the eval-v2 corrections behind the gate, committed in sub-units.
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
      - **6c done:** placement and timing.
        - N6: episodes may start anywhere in the window.
        - M4: episodes are accepted by the legitimate hour and weekday shape. Ring, farm and
          collusion transactions get a legitimate time of day.
        - G5 spacing for takeovers, rings and farms (N7, N8). The documented bursts accumulate
          their gaps (§18 item 3).
        - G3 is checked exactly. Every transaction's point is planned before emission, and
          takeovers and unusual-location transactions are placed last.
        - Device farms get a distinct account per session.
        - Ablation switches wired for N6, M4, N7 and N8.
        - **Two implementation readings, recorded for review:**
          - A proposal that G3 rejects continues each event's keyed `scenario-time` stream, so it
            redraws the spacing as well as the placement. Otherwise a takeover with two
            transactions a second apart could exhaust its proposals.
          - With N8 ablated, M4 moves a device-farm session as a whole (the draft's original
            class), so eval-v1's one-minute repeat survives for the N8 control to detect. Applied
            per transaction, M4 would erase it.
      - **6d-1 done.**
        - N1: every transaction without a planted IP may come from an away IP.
        - N3: legitimate micro-sessions, a follow-up purchase within a minute.
        - A transaction minutes after the same actor's previous one, from the same anchor, is drawn
          near that point. This holds for legitimate transactions and within a planted instance,
          so bursts and micro-sessions no longer imply impossible speeds.
        - Ablation switches wired for N1 and N3.
      - **6d-2 and 6d-3 done.**
        - N2: about one legitimate trip a year of two to seven days, to the destinations takeovers
          use. A trip's destination anchors legitimate points, and the home-anchored planted
          points of an account while it lasts.
        - N4: moderately busy merchants may sell at one price, which buyers pay when it is
          ordinary for them, as G6's payers do.
        - N5: households of two or three share a pre-window device and a network.
        - Ablation switches wired for N2, N4 and N5.
        - **Known limit:** a legitimate payment just before a trip and one just after it starts can
          imply a fast leg, because no outbound journey is modelled.
      - **Step 6 status.** Every correction except N12 is implemented and switchable. N12 comes
        with step 7. Whether the corrections pass `LPC-5` is step 9's diagnostic probe.
   7. Done, except the outbox relay (Phase 3 Step 4): U7/N12 (ADR-0049), in committed units.
      - **Unit 1 done: the outcome stream is released with its two producers.**
        - Contract: `tx.authorization.v1`, with its schema, generated model, ledger entry, topic
          declaration (within the local disk budget) and EVENT_CONTRACTS and API_CONTRACTS rows.
          `trace_core.contracts.authorization` defines the event and its content-hash idempotency
          key once, for every producer.
        - Generator (N12, ablatable): every transaction says `UNKNOWN`. Each yields one outcome
          event at its DM-1 time, emitted after it and sharing its trace and correlation ids.
          `LPC-5` and LPC-1..3 read the stream when it is present.
        - Gateway: `POST /v1/events/authorization` writes `app.authorization_outcomes` and its outbox
          row in one transaction (migration 0005). It answers:
          - 202 when recorded, or for an identical redelivery;
          - 409 on a conflict;
          - 422 for an outcome decided before its transaction;
          - 503 without a system of record.
          These paths are tested against a real PostgreSQL.
        - **Defect found by a test and fixed before commit.** Migration 0001's default privileges
          give `trace_app` UPDATE on every new `app` table, so a recorded outcome could have been
          overwritten. Migration 0005 revokes UPDATE, DELETE and TRUNCATE explicitly.
        - **Implementation reading, for review.** The gateway uses the transaction id as the
          outcome's correlation id, and the request's trace, as triage does: it holds no record of
          the transaction's own ids.
      - **Unit 2a done: feature set 3.0.0, declared and served by the reference implementation.**
        - `Stream.AUTHORIZATION_OUTCOME` and `CurrentObservation.PRIOR_KNOWN`. Each requires the
          other, and the declined ratio is declared only on that stream.
        - `declined_ratio_1h` reads outcome events decided strictly inside `(as_of - 1h, as_of)`,
          never the scored transaction's own. A scoring request's outcome reaches no feature.
        - An outcome is verified against the transactions the read may see: pending while its
          transaction is unknown, rejected when the accounts differ. Only verified outcomes count,
          and the reference store's receipts report which applies.
        - Literal fixtures F1–F5, F7, F8, F10–F12, F16 and F17 in both evaluation modes, with
          mutants for the scored transaction's own outcome, another account's outcome, a pending
          outcome, and an outcome decided at `as_of`.
        - **Two harness gaps found by the mutation run, fixed before commit.**
          - The currency mutant's target line now also appeared in the outcome window, so it no
            longer named one rule.
          - F17 could not catch a zero ratio: without a completeness bound no window reached the
            definition's guard. It now also scores from a store watched over the whole window,
            whose empty window is a measured zero.
        - **The served values do not conform, and this is enforced.** `SERVED_FEATURES_CONFORM` is
          False, so gateway replay and load-test records are refused. The Redis store serves no
          outcome yet, so the seven outcome fixtures fail as served on Redis, all as insufficient
          history. `P3.semantics-hardening` is back to IN_PROGRESS until they pass.
      - **Unit 2b done: the Redis store and the gateway serve 3.0.0.**
        - Redis: each outcome is recorded once under its identity, with a verdict.
          - A verified outcome joins the account's verified set, and its declines, scored by
            outcome time.
          - A pending one is verified or rejected inside its transaction's own atomic record.
          - The read counts both sets over `(as_of - 1h, as_of)`, less the scored transaction's
            own outcome, and stops vouching for the window where it has trimmed.
        - Gateway: after the durable record, a recorded or duplicate delivery is applied online.
          - An outcome the store rejects is answered 409 `authorization-account-mismatch`.
          - A store failure is still 202, counted as degraded, and withdraws completeness.
          - `authorization_outcome_total{delivery,verification}` counts every delivery.
        - Evidence against real Redis and PostgreSQL. Neither suite runs in `make verify`.
          - The Redis store passes every literal fixture as served, including the seven outcome
            fixtures.
          - The long-history differential test now also delivers outcomes: pending ones reconciled
            by their transaction, rejected ones and redeliveries. It still agrees with the reference
            on every feature and every receipt.
          - The route fixtures F9, F10/F13 and F14 pass, as does a store failure withdrawing
            completeness.
        - `SERVED_FEATURES_CONFORM` is True again. The gateway replay still refuses, mechanically,
          until unit 3 delivers or derives outcomes: a report without them would name 3.0.0 with no
          outcome input.
        - **Declared limits, for review:**
          - An outcome is verified only against a transaction the store still holds. One arriving
            after its transaction was folded away (decided a day or more later, or delivered beyond
            the margin) stays pending as served, where a complete history verifies it.
          - A pending outcome expires two hours past the later of its outcome time and the clock. A
            transaction arriving after that leaves it pending.
          - No released producer does either: DM-1 decides within seconds.
          - Rebuilding outcome state from PostgreSQL after a store loss stays deferred (ADR-0049
            §6).
      - **Unit 3a done: replays deliver or derive outcomes.**
        - The gateway replay posts `tx.authorization.v1` as a fourth stream, after transactions at
          one instant, and sends transactions without their outcome field.
        - A dataset without the stream (eval-v1) derives its outcomes through the generator's own
          DM-1 builder, with the seed of the source manifest named by `--source-manifest`.
          - A manifest for another dataset version is refused.
          - `REVERSED` and `UNKNOWN` derive nothing and are counted in the report, beside DM-1, the
            seed and the feature set version.
        - The refusal is narrowed: a dataset with neither the stream nor a manifest is refused
          before anything is read.
        - F6 is tested on real generated streams written to Parquet:
          - every outcome follows its transaction at exactly DM-1;
          - a rerun is identical, and DM-1 reads only the seed and the id;
          - a dataset that carries the stream is replayed from it;
          - every replayed request satisfies the gateway's request contract.
        - **Not yet run against a gateway.** The replay report and the rule re-validation on
          feature set 3.0.0 belong to step 13.
      - **Unit 3b done: the `LPC-5` availability provider (§4.6).**
        - `data/generator/lpc5/availability.py` evaluates every released feature for every TX row
          with the reference implementation:
          - in EVENT_TIME_COMPLETE mode from the generation's start;
          - over the four streams, mapped as the ingress maps them.
        - `eval/track_a/lpc5_control.py` passes it, so its runs judge S6 and are no longer invalid
          for want of availability.
        - The reference's whole-history context is quadratic. Each row is given only what its context
          can read, and the reference builds the context from that unchanged:
          - its account's observations up to the row;
          - its card, device, IP and merchant within the widest window and its sketch buckets;
          - the transactions its account's outcomes name.
        - Tests, on dense generations with derived outcomes and under the eval-v2 gate:
          - every feature of sampled rows equals the whole-history context, and the bounds leave
            related observations out for most of those rows;
          - a TX row is observed exactly as the generator adapter's canonical transaction is;
          - no generated row is UNAVAILABLE.
        - **Not yet measured at acceptance scale.** In a diagnostic run, per-row cost stayed flat
          from a small generation to one ten times larger. Step 9's probe measures it on the
          acceptance manifest.
        - The eval-v1 negative control recorded in step 3 ran without availability, so S6 was not
          judged in it.
      - **Step 7 status.** U7/N12 is implemented:
        - the stream and its producers;
        - feature set 3.0.0 in every implementation;
        - replay and availability.
        - The outbox relay that publishes `tx.authorization.v1` to Kafka belongs to Phase 3 Step 4,
          with the gateway's other publishing.
   8. Done: the `LPC-5` §14.2 and §14.3 controls can be run and judged.
      - `controls.zero_rate_control` and `controls.ablation_control` take a candidate (gate on, no
        correction disabled) and change only what the criterion names.
        - Zero-rate zeroes every legitimate-baseline rate: identity and device activity, T1, T3 and
          N1–N5. It refuses a rate field it does not name.
        - An ablation disables one correction and keeps the seed and everything else, so a failed
          check is attributable to it.
      - The field list moved from the stage-1c test into the library, which that test now uses.
      - `eval/track_a/lpc5_control.py` runs them:
        - `--control zero-rate`, or `--control ablation --correction <one or all>`;
        - taken from the gated generator as it is, or from `--candidate`, whose scenario-config
          digest is checked;
        - a control whose run is invalid meets nothing, and any unmet control exits 1.
      - Tests pin that every ablation differs from its candidate only in its correction, that every
        correction has a declared ablation and an evaluator, and that the zero-rate fields are
        exactly the rates §14.2 lists.
      - A fast-lane smoke test runs the N12 ablation end to end: it fails S2c rule 3 as required,
        and the candidate does not. That is diagnostic only (§16.4).
      - **Not yet run at acceptance scale, or for the other corrections.** Controls on the final
        candidate belong to step 11. Step 9's probe shows which ablations are at risk.
   9. Done (diagnostic, not acceptance evidence): the eval-v2 probe at a quarter of acceptance scale.
      - Seed 43, not the acceptance seed, with entity counts scaled from the eval-v1 manifest. Output:
        `eval/track_a/audits/stage-2-evidence/lpc5-eval-v2-probe-quarter-scale.txt`. Findings are
        capped per check, so not every S1-B finding is printed.
      - **Verdict: FAIL** on R7, R8, S1-U, S1-B, S2b and S7b. G3, R9, S0, S2c, S3, S4, S5, S6a, S7a and
        S8 pass.
      - **Predictions confirmed.**
        - MC-5 and DF-5 fall well below E_min.
        - §18.14: takeovers still cluster within hours. S1-B fails in ATO-1's stratum on hourly and
          daily transaction counts, `gap_prev`, distinct merchants and prior decisions.
        - §18.2 shows as S1-U support failures on `EMAIL_CHANGE` and `PHONE_CHANGE`, not as an ATO-3
          composition failure: legitimate ID rows are mostly logins, so those changes fall below
          S1-U's support share.
        - Cost: the quarter-scale run fits comfortably; a full-scale run extrapolates to most of this
          machine's memory.
      - **Predictions not observed:** burst offsets (CT-5, VA-5, S4 K1), AHV-4, N9 amounts, N10
        correlation, `FRAUD_RING` chance sharing, fixed-price look-alikes.
      - **Found, not predicted.**
        - Card-testing and velocity counts fail S1-U support on values their rows do not exempt.
        - Velocity attacks use several devices within a day: G4 gives legitimate payment devices
          only to card testing and credential stuffing. Card testing and velocity attacks also reach
          several merchant countries within a day, which no row names or lists as a consequence.
        - Takeover ID rows carry no IP, as legitimate credential changes do not; the enrichment of
          "no IP" follows from the event type.
        - R8: pooled signals lose the pooled exemption, because G2's floor makes velocity attacks
          the largest share of planted rows.
        - Night-time `daypart` for takeovers and impossible travel, and M4's ablation check already
          fails with no correction disabled. Multi-event episodes are accepted by the mean of their
          events' legitimate time weights, which still admits events at night hours after an
          evening start.
        - S7b near misses on CT-3 and MC-2, limited by 20 clusters.
        - S2b on outcome ingest lag for unusual-location episodes, over 20 rows. The lag is drawn
          per transaction without regard to labels, so chance is likely; it still counts (§19).
      - **Reading to review:** the generator applies M4 per transaction to merchant collusion as
        well as to rings and farms; G5 names only rings and farms.
      - **Routing.** Most failures need a numbered criterion revision or a change to a frozen
        generator rule, which are the user's decisions: exemptions, consequences, R8's pooled
        exemption, G4, G5, MC-5 and DF-5.
      - **M4 across multi-event episodes, investigated.** A diagnostic experiment,
        `eval/track_a/audits/stage-2-evidence/m4-episode-acceptance-experiment.txt`, compares
        per-episode acceptance rules on the generator's own time weights.
        - The current mean-weight acceptance reproduces the night enrichment.
        - A product of weights depletes night hours below legitimate instead. R7 does not check
          depletion, so it would pass by distorting the distribution the other way.
        - Anchoring the first transaction's time of day to the legitimate shape does not help.
        - Matching the legitimate shape exactly would fit a start-time density to the check's own
          reference.
        - The enrichment follows from documented multi-hour timing ("then within hours", the
          distance-derived travel gap). It is routed with the others: treating `hour` and `daypart`
          as consequences of those episodes is a criterion revision.
      - **Step 10 is blocked** on those decisions.
      - **Step 9 decisions (user, 2026-09-14): `LPC-5` revision 3, then one more probe.**
        - Episode consequences (§6.6), judged for support only and never signatures:
          - ATO-E1 for the takeover's clustering: hourly and daily counts, `gap_prev`, distinct
            merchants in the hour, and the prior-decision count and share derived from them;
          - ATO-E2 and IT-E1 for `hour` and `daypart`, incidental consequences of multi-hour spans.
        - G5 is unchanged and no time-of-day calibration is added. M4's ablation is judged on the
          other scenarios.
        - Value-specific support exemptions, with the support thresholds unchanged:
          - `rare` where legitimate rows exist below the share floor;
          - `none` where the documented burst is beyond the account's baseline.
        - Applicability by event type (§4.8): an ID attribute of an optional field is not judged for
          a legitimate event type no row of which carries that field.
        - R8: a pooled cell is exempt only when every contributing scenario admits the value. The
          realised-share rule is gone.
        - G4: velocity attacks pay from the account's legitimate devices (generator changed).
        - MC-5 and DF-5 keep their allowlisted status with no minimum effect.
        - Card testing's `DEVICE_SHARING` removal is confirmed; "from one device" stays an S7a
          invariant. ADR-0050 is updated.
        - Not changed: the CT-3 and MC-2 near misses, the S2b ingest-lag finding, every threshold.
        - **Reading to review.** ATO-E1 includes `prior_decisions_1h` and `prior_declined_share_1h`
          beyond the literal decision: each later transaction of the episode sees the earlier ones'
          decisions, the same hourly count through the outcome stream, as VA-6 already lists.
        - **For §14.1.** The eval-v1 negative control recorded in step 3 ran under revision 2. Its
          `R7 hour` expectation now rests on scenarios other than takeover and impossible travel.
          The controls rerun in step 11.
      - **Probe 2 (diagnostic, after revision 3).** Quarter scale, seed 44, commit `a2792e0`.
        - Output: `eval/track_a/audits/stage-2-evidence/lpc5-eval-v2-probe-quarter-scale-rev3.txt`.
          Pooled-contributor analysis of the same data:
          `eval/track_a/audits/stage-2-evidence/probe2-pooled-contributors.txt`.
        - Revision 3 resolved what it targeted: takeover clustering and time of day, the
          `EMAIL_CHANGE`/`PHONE_CHANGE` support failures, the takeover identity-event IPs, MC-5 and
          DF-5. M4's ablation check no longer fails without the ablation. S7b is down to the CT-3
          near miss.
        - **Verdict still FAIL** on R7, R8, S1-U, S1-B, S2b and S7b.
        - **Category A in the data: none found.**
          - Velocity attacks' device, merchant, MCC and country multiplicity is the ordinary
            per-transaction selection the legitimate baseline uses, at burst volume: decision 6 is
            implemented faithfully.
          - The anomalous-high-value findings (`ip_accounts = 1`, a prior failed login, decision
            latency) are a few of its single-transaction rows; its injection sets only the amount
            and the merchant.
        - **Category A in the instrument: R8's contributor rule.**
          - Revision 3 counts any scenario whose share exceeds the legitimate upper bound as
            contributing.
          - In about half of the failing pooled cells, every contributor enriched on its own admits
            the value; the cell is judged only because of scenarios at trivial shares. That does not
            implement "materially contributing".
          - Tightening it changes §5.4 and weakens R8 relative to revision 3 as committed, so it is
            the user's decision.
          - Step 10 waits: a revision after a candidate exists invalidates the candidate.
        - **Category B (documented behaviour, not revised):**
          - velocity volume in merchant, MCC, device, country and activity attributes;
          - impossible travel's two-transaction counts and decisions;
          - the takeover's new device in the DEV stream, and `ADDRESS_CHANGE` support;
          - card testing's countries, activity and TX-side declines;
          - ring, stuffing and farm consequences inside their strata;
          - merchant-collusion amounts under G6;
          - velocity `3-4` in five minutes, which revision 3 did not exempt for velocity.
        - **Category C (sample size or chance):**
          - the CT-3 near miss;
          - `rare` values short of thirty legitimate rows at quarter scale;
          - S2b on scenarios of twenty-odd rows (unusual-location amount digits,
            anomalous-high-value decision latency);
          - the anomalous-high-value findings.
        - **Consequence for acceptance.** Category B findings still fail `LPC-5`'s mechanical checks.
          Under revision 3, an acceptance run on any candidate records FAIL on them, whatever R8's
          rule. Raised with the user before any candidate is frozen.
      - **Step 9 decisions, second round (user, 2026-09-14): `LPC-5` revision 4, the last planned
        revision.** Probe 2 found no Category A defect in the data, so the generator is unchanged.
        - **R8 (§5.4).** A scenario contributes materially only when it is itself `ENRICHED` for the
          value, by §3's unchanged test. No new threshold.
        - **Documented consequence values (§6.7).** 36 value-scoped entries for exactly probe 2's
          Category B list: 19 scenario-wide, 17 inside named rows' strata. Support exemptions sit only
          on VA-C1, VA-C2 and ATO-C3, for values legitimate traffic essentially never reaches.
        - **Rows (§6.4).** ATO-3 adds `rare` `ADDRESS_CHANGE`; VA-2 and VA-4 add `rare` `3-4`. ATO-E1's
          prior-decision count and share are confirmed as they stand.
        - **Left judged:**
          - the CT-3 and MC-2 near misses;
          - chance and sample-size findings;
          - every finding outside the Category B list (§6.7, "What is not here"): takeover
            transaction-side and unusual-location findings inside their strata, card testing inside
            CT-3, CT-6, CT-7 and CT-8, daily counts inside VA-3 and CT-3, impossible travel's gap
            inside IT-2, `leg_speed` jitter, device-farm findings outside its strata, and judged
            compositions.
        - **Tests.**
          - The five hand-built R8 cases the user required.
          - A scenario-wide and a within-stratum consequence test.
          - Declaration structure and verbatim citations.
          - Each test kills its mutant: revision 3's contributor rule, a vacuous exemption with no
            contributor, ignored consequences, `within` read scenario-wide, and dropped exemptions.
        - **Re-judging probe 2.** The report prints findings only, so the saved evidence cannot show
          what revision 4 still judges. Probe 2 is therefore regenerated once at seed 44 under revision
          4, as the decision allows.
      - **Probe 2 re-judged under revision 4 (diagnostic).** One seed-44 regeneration at commit
        `5b2bd91`, generator unchanged:
        `eval/track_a/audits/stage-2-evidence/lpc5-eval-v2-probe-quarter-scale-rev4.txt`.
        - **Verdict still FAIL,** on R7, R8, S1-U, S1-B, S2b and S7b. No value §6.7 declares is still
          found.
        - **Category A: none.** Each candidate was checked against the code:
          - unusual-location amount roundness (S2b): the same lognormal sampler as legitimate spend,
            restricted to its region, with no rounding step, so a chance tail among the S2b cells;
          - anomalous-high-value decision latency (S2b): DM-1 draws one distribution for every
            transaction;
          - velocity and card-testing `leg_speed`: planted burst locations use the legitimate session
            point, so burst volume, not representation, moves the speed;
          - takeover `shared_merchant_link`: the takeover device is drawn from the universe's devices,
            which links the account to their other users (ATO-4's consequences) at
            popularity-weighted merchants;
          - merchant collusion's `merchant_popularity` inside MC-4: the colluding merchant is drawn
            uniformly, so it usually lies outside the popular head; the documented implausible share
            needs a merchant with a small baseline.
        - **Category B outside probe 2's recorded list, left judged:**
          - takeover `shared_merchant_link`, availability and profile inside ATO-5, and its
            `device_age` composition;
          - unusual-location findings inside ULD-1, ULD-2 and ULD-3;
          - card testing inside CT-3, CT-6, CT-7 and CT-8, and its `leg_speed`;
          - velocity `leg_speed`, and daily counts inside VA-3;
          - device-farm `device_age` and device-history availability outside its strata;
          - impossible travel's gap inside IT-2;
          - the ring's `device_accounts` composition;
          - merchant collusion's popularity inside MC-4.
        - **Category C:**
          - the CT-3 near miss;
          - the anomalous-high-value findings, the takeover's device-event hour, and single merchant
            codes and countries inside IT-2 and MC-1;
          - S2b on scenarios of twenty-odd rows;
          - `rare` values short of thirty legitimate rows. `tx_count_24h` `10-19` and
            `prior_declined_share_1h` `(0,0.4)` have so few that they may stay short at acceptance
            scale.
        - **Revision 4 is closed.** By the user's decision there is no revision 5 for B or C. Step 10
          proceeds, and an acceptance FAIL on these findings is reported as it stands.
   10. **Final candidate: frozen (2026-09-14).** `eval/track_a/eval-v2.candidate.manifest.json`,
       run_id `gen-20260914-eval-v2-7dac6635`, generated at commit `55e83cf` on a clean tree.
       - It uses eval-v1's scale, window and seed 42, with the eval-v2 gate at its declared defaults
         and every correction applied.
       - It records the configuration digest, the four streams' digests and row counts, the
         transaction digest, and `LPC-5` revision 4 with its sha256.
       - G2 added three merchant-collusion instances. Every report on eval-v2 therefore states that
         its scenario mix is a coverage floor, not natural prevalence (§14.4).
       - `python -m eval.track_a.freeze_candidate --check` regenerated the candidate and reproduced
         every recorded value. Evidence: `eval/track_a/audits/stage-2-evidence/eval-v2-candidate-freeze.txt`.
   11. **Frozen `LPC-5` acceptance: FAIL (2026-09-14), with no Category A finding.**
       - **Run.** `lpc5_control --control eval-v2 --candidate eval/track_a/eval-v2.candidate.manifest.json`
         on candidate run_id `gen-20260914-eval-v2-7dac6635`, from a clean tree at `fbf2190`. The four
         stream digests and row counts were verified before any check ran. Evidence:
         `eval/track_a/audits/stage-2-evidence/lpc5-eval-v2-acceptance-rev4.txt`.
       - **Verdict.** FAIL on R7, R8, S1-U, S1-B, S2b and S7b. S0, S2c, S3, S4, S5a, S5b, G3, S6a,
         S7a and every S8 check pass.
       - **First attempt: no result.** The host stopped it for low memory, after about half an hour.
         - A diagnostic profile at a tenth and a twentieth of acceptance scale traced the peak to the
           availability step, which held a validated model per transaction through its loop, and to
           S0's sorted copy of every row.
         - `fbf2190` restructures both, keeps tie ranks in per-population lists and interns payload
           keys. Nothing it computes changes: the complete report hashes identically to `8e7759e` on
           two generations, and the quarter-scale report is byte-identical to revision 4's re-judge,
           at under half the peak footprint (`lpc5-memory-bounded-equivalence.txt`,
           `lpc5-memory-bounded-quarter-scale.txt`).
       - **Category A: none.** The candidates were checked against the generator code:
         - takeover and high-value `merchant_accounts_1h` and `shared_merchant_link`: planted
           merchants use the same popularity draw as legitimate spend outside the habitual set;
         - unusual-location and device-farm device-history availability: the feature reports
           insufficient history for a never-seen device until the account is watched for its horizon
           (ADR-0044), which a new device meets by construction;
         - unusual-location recent-prior-transaction findings inside ULD-3: one uniformly placed
           transaction in a distant city, which G3 only rejects above 900 km/h, so the recent home
           activity is the documented mechanism and not a placement offset;
         - high-value amount digits (S2b): the amount formula gives uniform last digits, and the
           observed tail is among the chance results expected across the S2b cells judged.
       - **Category B, outside the declared list, left judged:**
         - takeover and high-value merchant effects, takeover inside ATO-5, ATO-6 and ATO-8, and its
           `device_age` composition;
         - velocity `distinct_devices_24h` `2`, `leg_speed`, and daily counts inside VA-3;
         - card-testing activity `51+`, `device_age` `<1h`, `leg_speed`, and findings inside CT-3,
           CT-6, CT-7 and CT-8;
         - device-farm `device_age` and device-history findings, and profile depth inside DF-5;
         - unusual-location availability, profile, amount-decile and recent-activity findings inside
           its strata;
         - ring device, IP and merchant findings inside FR-1 to FR-3, and its `device_accounts`
           composition;
         - impossible travel's gaps inside IT-2;
         - merchant collusion's per-hour payers inside MC-1 and popularity inside MC-4;
         - pooled R8 and S1-U cells whose enriched contributors do not admit the value.
       - **Category C:**
         - support short of thirty legitimate rows for `tx_count_24h` `10-19` and
           `prior_declined_share_1h` `(0,0.4)`, which probe 2 predicted would stay short at full
           scale;
         - the MC-1 and MC-2 near misses in S7b;
         - high-value amount digits (S2b);
         - takeover device-event hour, weekday cells concentrated in a few instances, and single
           merchant codes and countries inside FR-3, MC-1, MC-4, MC-5 and the ULD rows.
       - **Consequence (user decision, 2026-09-14).** The `LPC-5` research loop is closed.
         - No revision 5, no threshold tuning after seeing the frozen candidate, and no generator
           tuning to make `LPC-5` green.
         - eval-v2 is still the frozen Phase 3 dataset, and steps 12 and 13 run anyway.
         - Recorded wherever eval-v2 is described: **`LPC-5` strict acceptance FAIL; Category A
           findings 0**, with the Category B and C reasons above. It is never described as `LPC-5`
           compliant, and no natural fraud prevalence is claimed from it.
         - Only an actual Category A correctness or leakage defect found downstream reopens `LPC-5`.
           Category B or C findings do not.
       - **Controls (§14): deferred evaluation hardening (D15).**
         - By the user's decision, the acceptance-scale zero-rate control and the 21 full-scale
           ablations were not started: they cannot turn the verdict into PASS and do not block Phase 3.
         - The in-flight eval-v1 negative control finished before the runner stopped: **§14.1 MET**,
           with eval-v1's manifest digests verified
           (`eval/track_a/audits/stage-2-evidence/lpc5-eval-v1-negative-control-rev4.txt`).
         - The first acceptance attempt's out-of-memory stop is kept as diagnostic history
           (`lpc5-acceptance-attempt1-oom.txt`), not as a result.
         - The unit and self-tests, the smoke controls and the control framework are unchanged.
         - The host slept on low battery at 21:44:37Z, 25 s into the eval-v1 control, and resumed on AC
           at 22:05:51Z. That run's wall time includes the pause; its result does not depend on it.
   12. **eval-v2 frozen (2026-09-14).** `eval/track_a/eval-v2.manifest.json`.
       - Materialisation run `gen-20260914-eval-v2-276b661d`, at `a4b3d4c` on a clean tree, reproduced
         every candidate stream digest before ground truth was written.
       - The manifest records the files' digests, the materialisation's provenance and `lpc5`: strict
         acceptance FAIL, Category A findings 0, not compliant, with the Category B and C reasons and
         the disclosures.
       - `tests/unit/test_eval_v2_freeze.py` keeps it honest; its slow lane regenerates every stream.
   13. **Replay validation, rule re-validation and the R010 study, on feature set 3.0.0 (2026-09-14).**
       - **Harness.** Changes found by replaying eval-v2:
         - prefix loading now cuts context streams at the horizon while reading;
         - a replay refuses a system of record holding another dataset's outcomes or cases;
         - new options `--vouch-from-manifest`, `--decisions`, `--withhold-outcomes` and `--report`.

         The procedure is in `docs/LOCAL_DEVELOPMENT.md`.
       - **eval-v1 manual replay.** The 60,000-transaction prefix, with outcomes derived from eval-v1's
         run record, on a store vouched from the window start: `benchmarks/gateway/triage-bands.md`.
       - **Rule re-validation, controlled.** The same prefix through the same client, on the 2.0.0
         gateway (`b43a75c`, outcomes withheld) and on 3.0.0, produced byte-identical decision files:
         no rule, score or band changed (`benchmarks/gateway/rule-revalidation-fs3.md`).
         - R002, the only rule reading the feature 3.0.0 changes, fired in neither run.
         - Profile rules abstain on a store vouched from the window start.

         Both limits are disclosed there.
       - **eval-v2 replay.** The 60,000-transaction prefix on 3.0.0, with eval-v2's LPC-5 status:
         `benchmarks/gateway/triage-bands-eval-v2.md`.
       - **R010 threshold study (U10): R010 stays at `>= 5`.**
         `benchmarks/gateway/r010-threshold-study-eval-v2.md`.
         - Both self-checks are clean: recomputed bands match the gateway's, and offline R010
           agrees with the served R010 on every decision.
         - At 6, the legitimate high-risk decisions R010 alone causes disappear, but so do about a
           fifth of its fraudulent ones.
         - At 4, the legitimate ones rise by an order of magnitude.

         Neither is clear evidence of a better operating point on a prefix of a non-compliant
         dataset, so the ruleset is unchanged.
       - **Found along the way, not Category A.** Frozen datasets reuse positional transaction ids, so
         replaying a second dataset into the same system of record meets a correct 409. The harness
         now refuses first.
       - **The eval-v2 Stage 2 sub-track is CLOSED for Phase 3** (user decision, 2026-09-14). `LPC-5`
         reopens only for a downstream Category A correctness or leakage defect; Category B or C
         findings do not. Further synthetic-data work is D16, and acceptance-scale controls are D15.
       - **Phase 2 load gate re-met on feature set 3.0.0:** `run_id: load-20260914-gateway-bb2f0fa1`
         passes every exit condition from a cold store on a clean tree, with thresholds unchanged
         (`benchmarks/gateway/REPORT.md`). With the manual replay and the controlled rule re-validation
         above, `P3.semantics-hardening` is PASS again.

   No `LPC-5` threshold is tuned after the final candidate is seen.
3. ~~**R010 threshold study (U10)**~~ **Done 2026-09-14 on eval-v2, with its LPC-5 FAIL disclosed:
   R010 stays at 5** (Stage 2 step 13). Originally: once `eval-v2` is frozen and its label-proxy checks pass. Compare
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
