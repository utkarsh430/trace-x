# Gateway load harness

Measures `trace-gateway` against the ROADMAP Phase 2 targets — p99 < 100 ms and
p50 < 20 ms at 500 TPS sustained, with zero 5xx over a 10-minute run — and writes
a run record that `make check-claims` can resolve.

Two pieces:

| file | job |
|---|---|
| `gateway.js` | the load profile: seeded, skewed, contract-shaped traffic |
| `../../../scripts/load_gateway.py` | runner, gate, run-record writer, report renderer |

Run it through the Makefile, never by hand:

```bash
make load-gateway                       # 500 TPS for 600 s against localhost:8010
make load-gateway ARGS="--duration-s 60"   # a shorter smoke of the harness itself
```

A bare `k6 run` produces numbers with no run record. Under `CLAUDE.md` §13 a
number without a resolvable `run_id` cannot be published, so it is a number
nobody can use.

---

## Why a pinned container image and not a local k6

k6 here is a **measurement instrument**, and the pin is what makes two runs
comparable. A `k6` on `$PATH` is whatever Homebrew installed last; a run on
0.49.0 and a run on 0.56 differ in how they schedule iterations and compute
percentiles, so their p99s are not the same statistic — but they *look* like the
same statistic, which is the failure that matters.

The image is pinned in `pyproject.toml` under `[tool.trace_x.tools]` by tag **and**
digest (ADR-0036), because a tag can be moved and a digest cannot:

```toml
k6 = "grafana/k6:0.49.0@sha256:8cd78f9d…"
```

`scripts/load_gateway.py` reads that pin and has **no PATH fallback**. It also
records the version the image reports at run time rather than the one parsed out
of the pin, so the recorded `tool_version` describes what actually ran.

---

## What the traffic looks like, and why

**Every request carries a unique `X-Idempotency-Key`.** The gateway requires the
header — it returns 400 without one — and it caches responses against it. A
constant or per-VU key would make every request after the first a replay-cache
read, and the run would report the latency of a Redis `GET` under the heading
"synchronous scoring". The key is `${RUN_NONCE}-${vu}-${iteration}`, and the
nonce is fresh per invocation: without it the *second* run of the script would
replay the first one's cached responses end to end and get faster the more often
it was run.

**The entity mix is varied and skewed, not uniform and not constant.** Accounts,
cards, devices, IPs and merchants are drawn from pools of tens of thousands with
80% of the traffic falling on 5% of the entities. Both extremes measure the wrong
system: one account is a single hot Redis key and a perfectly warm rule path,
while a uniform spread over a huge key space makes every feature read a miss and
every account history-less. Real card traffic is heavy-tailed, and ADR-0034's
exact/approximate split is keyed on exactly that.

**Amounts span four magnitude bands** (integer minor units, never a float) because
the rule pack's thresholds are amount-dependent: a constant amount puts every
transaction on the same side of every comparison, so the run would exercise one
branch of the engine at 500 TPS. The report prints the resulting band mix for the
same reason — a run where everything scored `LOW` did not measure the rule engine.

**Channel and entry mode are paired the way an acquirer actually sends them**
(`CARD_NOT_PRESENT` with `ECOMMERCE`/`TOKEN`, `ATM` with `CHIP`, …). An
uncorrelated pairing exercises branches no real feed produces.

**Event time trails processing time by up to 30 s** and never leads it.
`occurred_at` is event time and drives every window in the system (ADR-0026); a
feed where it always equals `now` is a perfectly ordered stream, which is not the
one production sees.

**The traffic is seeded.** `mulberry32` rather than `Math.random()`, because
`Math.random()` cannot be seeded and determinism is required where it is
achievable (`CLAUDE.md` §3.5). The same `--seed` produces the same traffic shape,
so two runs differ by the system under test rather than by the load.

Identifier spellings follow `TransactionRequest` exactly (`acct_\d{6,}`,
`mrch_\d{5,}`, `dev_\d{6,}`, `ip_\d{5,}`, `card_\d{6,}`). A malformed id is a 422
before any scoring happens, so a typo here would silently turn the benchmark into
a measurement of the validation layer. The harness fails the run if any 4xx
appears, for that reason.

---

## What the harness refuses to do

`scripts/load_gateway.py` distinguishes two kinds of failure.

**INTEGRITY** — the run did not measure what it claims to. No record, no report,
non-zero exit:

| check | why it invalidates the run |
|---|---|
| `requests_were_made` | a run with no iterations substantiates nothing |
| `target_rate_sustained` | achieved rate below 99% of target: the latency describes a smaller test than the one claimed |
| `no_dropped_iterations` | offered load that never left the generator |
| `not_rate_limited` | a 429 means the limiter shaped the result, not the scoring path |
| `no_client_errors` | a 4xx exercises the validation layer |
| `responses_were_readable` | an unparseable 200 means the band and degraded counts are incomplete |

**TARGET** — the measurement is sound and the ROADMAP target was met or missed.
Recorded and published either way (`CLAUDE.md` §17 forbids concealing an
unfavourable result), non-zero exit on a miss: `zero_5xx`, `p99_under_budget`,
`p50_under_budget`.

The zero-5xx exit condition is asserted in Python, not only as a k6 threshold. A
threshold fails the run early, which is useful; but a phase exit condition that
lived only in a threshold could be relaxed in the same commit that failed it.

Three further refusals worth knowing about:

* **A metric the summary does not carry raises.** "Zero 5xx" and "5xx were never
  counted" are different facts and only one is evidence, so every counter in
  `gateway.js` is written on every iteration — with `0` where the condition did
  not hold — and `dropped_iterations` carries a threshold purely to force k6 to
  emit it when nothing was dropped.
* **A dirty worktree writes the record but not the report.** A run from an
  uncommitted tree is not publishable (`docs/EVALUATION.md` §8 rule 4) and
  `benchmarks/**/*.md` is scanned by `make check-claims`. The measurement is kept
  as evidence; the publication is refused, loudly. Commit and re-run.
* **The rule-pack digest, threshold digest and feature-set version come from the
  running gateway**, read out of a single probe decision before the window opens,
  and are required to match the checkout. A p99 that cannot be attributed to the
  rules that produced it substantiates nothing, and a gateway serving a different
  pack from this commit would make the recorded `git_commit_sha` a fiction.

---

## Prerequisites, and one that bites

1. **Docker** running, and the pinned image pulled (`make doctor` reports it).
2. **The gateway up** on `--base-url` (default `http://localhost:8010`), started
   from *this* commit.
3. **A service token**: `TRACE_LOAD_TOKEN=<token_id>.<secret>`, or the
   `TRACE_SERVICE_TOKEN_<ID>` variable the gateway was started with. Never passed
   on a command line.
4. **The rate limit raised for the load window.** The gateway's default is
   `TRACE_RATE_LIMIT_PER_MINUTE=1000` *per service token*, which is ~16.7 TPS. At
   500 TPS a single token is exhausted about two seconds in and the rest of the
   run measures the 429 path. Set
   `TRACE_RATE_LIMIT_PER_MINUTE >= target_tps * 60` (30 000 for the Phase 2
   target) on the gateway, or spread the load across enough tokens to cover it.
   The harness fails the run on any 429 rather than quietly reporting the
   limiter's latency as the gateway's.
5. **A clean worktree**, if the numbers are to be published. Note that the
   *previous* run's record in `eval/manifest/` is itself an untracked file, so
   commit it before the next run.

`--out-dir` defaults to a temporary directory **outside** the repository, and the
harness refuses an `--out-dir` inside it: k6's summary would appear as an
untracked file and dirty the very worktree the run is recording.

---

## Output

* `eval/manifest/load-<date>-gateway-<sha8>.json` — the `LOADTEST` run record,
  carrying every field `scripts/check_claims.py` requires plus the full
  measurement at unrounded precision.
* `benchmarks/gateway/REPORT.md` — the report, every number under a heading that
  declares the `run_id`. The claim linter resolves a `run_id` section-scoped and
  clears it at the next heading, so that layout is what makes the report
  checkable rather than merely readable.

Neither is written by hand, and nothing in either is a number the harness did not
measure.
