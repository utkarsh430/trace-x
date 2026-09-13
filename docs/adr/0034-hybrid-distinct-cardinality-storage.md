# ADR-0034: Hybrid distinct-cardinality storage — exact where bounded, estimated where not

- **Status:** Accepted
- **Date:** 2026-09-12
- **Phase:** 2
- **Amends:** ADR-0003 (the distinct-cardinality row of its structure table). ADR-0003 otherwise
  stands: Redis remains the online feature store, holds derived state only, and is authoritative for
  nothing.

## Context

### What ADR-0003 decided, and what it expected

ADR-0003 chose a structure per feature shape, and for distinct counts it chose HyperLogLog:

> | Distinct devices/IPs/merchants | HyperLogLog | O(1), ~0.81% error |

Its consequences section states the expectation plainly: *"HLL is approximate (~0.81%), so features
built on it are approximate — this must be stated wherever those features are used, and the parity
tolerance must account for it."* **That 0.81% figure is HyperLogLog's standard-error constant for a
single register set.** It was the right number for the question ADR-0003 was asking.

### The question it was not asked

Every distinct count in this system is an **event-time sliding window** — "distinct merchants in the
last hour", not "distinct merchants ever". That requirement comes from ADR-0002 and
`docs/DATA_ENGINEERING.md` §3, not from ADR-0003, and the two were never reconciled.

HyperLogLog has no notion of time. A sliding window over HLL therefore requires **bucketing**: one
HLL per time bucket, unioned by `PFCOUNT` at read. That adds a second, independent error source on
top of HLL's own, and the union of *N* bucket sketches does not carry the single-sketch standard
error. The 0.81% expectation was inherited by a construction it never described.

### What was measured

Measured on Redis 7.4.11 (`redis:7-alpine`, compose `core`), five-minute buckets, identical inputs
driven into both representations at each cardinality, across 5-minute, 1-hour and 24-hour windows.
Full table: `benchmarks/features/REPORT.md`. Run record: `run_id: bench-20260912-cardinality-41059ab1`
(clean worktree, commit `41059ab1a024`), resolvable by `make check-claims`.

| cardinality | exact error | bucketed-HLL error | exact memory | HLL memory |
|---|---|---|---|---|
| 100 | 0.00% | 0.00% | 2.6 KB | 0.4 KB |
| 1,000 | 0.00% | 0.40% | 90.0 KB | 2.1 KB |
| 5,000 | 0.00% | 0.32% | 487.4 KB | 14.1 KB |
| 20,000 | 0.00% | **1.05%** | 2,009.8 KB | 14.1 KB |
| 50,000 | 0.00% | 0.97% | 4,809.7 KB | 14.1 KB |

*(memory column from the 5-minute-window rows; the full table reports all three windows)*

Three results matter.

1. **The bucketed-HLL error reaches 1.05%, above the 0.81% ADR-0003 anticipated.** Median across all
   measured points is 0.36%. This is **recorded as a miss against that expectation, not as a
   redefinition of it**: 0.81% remains the correct figure for a single HLL, and the construction this
   system needs is not a single HLL.
2. **The sorted-set representation was exact at every measured point** — 0.00% relative error across
   all six cardinalities and all three windows, not merely close.
3. **Memory diverges by up to 342×** (the worst observed ratio, at 20,000 distinct over a 5-minute
   window: 2,009.8 KB against 14.1 KB). Redis runs with `maxmemory 512mb` in the compose `core`
   profile.

A fourth result was not anticipated and changes the shape of the decision: **approximate read latency
grows with window width, exact read latency does not.** A 24-hour window unions ~288 five-minute
buckets, and its `PFCOUNT` measured 0.765 ms p99 against 0.203 ms p99 for the equivalent `ZCOUNT`.
HLL is cheaper on memory but *more* expensive to read over a wide window.

## Decision

**Distinct-count storage is declared per feature, as one of two classes, and is never chosen at
runtime.**

**The classification criterion** is whether the counted dimension's cardinality is bounded by *one
entity's own behaviour* or by *the size of the population sharing that entity*:

| Class | Criterion | Features |
|---|---|---|
| `EXACT` | cardinality bounded by one entity's own behaviour | `account_distinct_merchants_1h`, `account_distinct_mcc_5m`, `account_distinct_devices_24h`, `account_distinct_countries_24h`, `device_distinct_accounts_24h` |
| `APPROXIMATE` | cardinality bounded by the sharing population | `ip_distinct_accounts_1h`, `merchant_distinct_accounts_1h` |

An account touches tens of merchants, categories, devices and countries in any window. A device
serves a household or — at the extreme the feature exists to catch — a farm of dozens. Those are
bounded, and at bounded cardinality a sorted set costs single-digit KB. An IP can be a carrier-grade
NAT and a popular merchant can take thousands of distinct accounts an hour; those are not bounded in
normal operation, and that is where the measured memory difference becomes material.

**`EXACT` is a sorted set keyed by the counted value**, scored by event time, written with `ZADD … GT`
so a late arrival cannot move a value's timestamp backwards. Each value therefore holds its latest
observation, and `ZCOUNT` over `(t − W, t]` is exactly the distinct count.

**`APPROXIMATE` is one HyperLogLog per five-minute bucket**, unioned by `PFCOUNT` over every bucket
overlapping the window, inclusive at both edges. Inclusive because a partial edge bucket may hold
values inside the window and omitting it would under-count *deterministically* — a one-sided error,
worse than HLL's symmetric one.

**The class is static and declarative.** It lives on `WindowedAggregate.storage`, is required on
`DISTINCT_COUNT` and forbidden elsewhere, and nothing reads observed cardinality to pick a
representation. A feature that switched representation under load would change its own error
characteristics exactly when a reader most needs to know what a value means, and would make any
recorded parity tolerance unattributable to a run.

**Approximation is exposed, not implied.** `FeatureSpec.approximate` derives from the declared class —
so an exact distinct count is *not* marked approximate merely because its aggregation is
`DISTINCT_COUNT` — and `FeatureValue.approximate` carries it to every caller, log line, test and
Phase 3 parity comparison.

**No replacement accuracy bound is asserted.** The measured maximum is 1.05% over the cardinality and
window ranges tested (10 – 50,000 distinct; 5 m / 1 h / 24 h). That is a *measurement*, not a
guarantee: the untested space above 50,000 is untested, and a bound stated beyond the evidence would
be the invention this project's benchmark-integrity rules exist to prevent. Phase 3 sets per-feature
parity tolerances from parity measurements against Spark, and the tolerance for the two approximate
features will be derived from that run and recorded with its `run_id`.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Sorted sets for all seven features | Exact everywhere and simplest to reason about, but the measured 342× worst-case memory ratio is material against a 512 MB ceiling. Under `allkeys-lru` the failure mode is eviction of arbitrary keys, which silently degrades *other* features |
| Bucketed HLL for all seven, as ADR-0003 reads literally | Keeps memory bounded, but makes five features approximate that need not be, forces a tolerance onto comparisons that could be equalities, and — per the fourth measurement — is *slower* to read at the 24-hour window than the exact representation it would replace |
| Raise the declared tolerance to ~1.1% and keep HLL everywhere | Would satisfy the letter of ADR-0003 while quietly restating the number it got wrong. `docs/DATA_ENGINEERING.md` §4 prohibits widening a tolerance to make a test pass; widening one to avoid an ADR amendment is the same act with worse provenance |
| Choose the representation at runtime from observed cardinality | Gives the best memory/accuracy trade per key, and makes every recorded value's error characteristics unknowable after the fact. A parity result would not be attributable to a representation |
| Finer HLL buckets (1 minute) to reduce boundary error | Reduces one error source and multiplies read cost: a 24-hour window would union ~1,440 sketches, and the 5-minute bucketing already measured 0.765 ms p99 at 288 (`run_id: bench-20260912-cardinality-41059ab1`) |
| Exact sets with a size cap, truncating beyond it | Bounds memory but introduces a third, silent behaviour — a count that is exact below the cap and a floor above it, with no way for a caller to tell which they received |

## Consequences

**Positive.** Five of seven distinct counts are now exactly comparable to their offline
implementation, so Phase 3's parity check for them is an equality rather than a tolerance — a
strictly stronger test, and one that cannot be weakened by adjusting a number. The two approximate
features are explicitly labelled at the value level, so a caller, a dashboard or a parity report can
tell an estimate from a measurement without consulting a document. Memory is bounded exactly where it
was at risk of being unbounded. `ZADD … GT` makes the exact representation correct under out-of-order
arrival, which HLL bucketing is only approximately.

**Negative.** Two representations mean two code paths, two failure modes and two things to understand
before reading a number. The exact representation's memory is genuinely unbounded in the adversarial
case — an attacker controlling `device_id` could inflate one sorted set — and the mitigation is
Redis's own eviction rather than a cap; eviction degrades the feature to *absent*, which the rules
tier abstains on, so the fail-safe direction is right, but the capacity risk is real and accepted.
Phase 3 must implement both comparisons: an equality for five features and a measured tolerance for
two, which is more work than one uniform rule.

**Risks.** A feature is moved across the classification boundary without the criterion being
re-applied — silently converting an equality comparison into a tolerated one. Signal: the storage
class changing without a corresponding benchmark run; mitigation: `test_the_storage_class_split_follows_the_stated_criterion`
pins the two sets by name, so a move is a deliberate, reviewed edit. Second risk: real-world
cardinality for a supposedly-bounded dimension exceeds the assumption — a device fingerprint shared by
thousands of legitimate accounts would make `device_distinct_accounts_24h` expensive. Signal: Redis
memory growth concentrated in `dx:DEVICE:*` keys; the remedy is to move that one feature to
`APPROXIMATE`, which is a one-line declarative change plus a re-measured tolerance. Third: the
untested cardinality range above 50,000 behaves differently from the measured range, and a future
reader treats 1.05% as a bound rather than as the maximum observed over the range stated here.

**Effect on Phase 3 parity.** The five exact features are compared by equality; a non-zero drift is a
defect, not a tolerance question. The two approximate features carry a tolerance that Phase 3 derives
from its own parity run and records with a `run_id` — not from this ADR, whose measurements describe
the online representation in isolation rather than online-versus-offline agreement. Both classes share
one definition of the feature's *meaning* (ADR-0032), so the conformance suite that proves this is the
same file for all three implementations.

## Status

Accepted
