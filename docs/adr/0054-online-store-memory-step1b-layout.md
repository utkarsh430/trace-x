# ADR-0054: Online-store memory re-measured for the Step 1b layout

## Context

ADR-0042 recorded the online feature store's memory, and the Phase 2 memory model
(`run_id: bench-20260913-memory-model-5c770259`) sized the configured feature-store limit. Both
describe the Phase 2 key shapes.

The Step 1b store (ADR-0046 §5) writes a different layout:
- every observation is one string key, kept for its raw window;
- raw per-account sets, a folded profile prefix, per-card and per-device sets, sketches and merchant
  minute buckets sit beside it;
- authorization outcomes are held as ADR-0049 §6 describes.

A planning experiment, without a `run_id`, showed a second flaw: the Phase 2 model's two-point
straight-line fit underestimates sorted sets above 128 members. Step 12 re-measures memory through
the store itself.

## Decision

### 1. The measurement

`benchmarks/features/memory_model.py` starts a throwaway Redis of the compose feature store's own
image and drives `RedisOnlineFeatureStore`, the production Lua scripts, through controlled scenarios.
It sums Redis's exact `MEMORY USAGE` by key family into curves, and projects them over the
representative profile (ADR-0040), unchanged from Phase 2, with the store's own retentions. The run
is `run_id: bench-20260915-054159-memory-model-5ee55136`, from clean commit `5ee5513` on Redis
7.4.11. `benchmarks/features/MEMORY.md` holds every figure.

### 2. What the run shows (`run_id: bench-20260915-054159-memory-model-5ee55136`)

- **The ten-minute acceptance run projects 477.7 MiB.** Of that, 188.8 MiB comes from an ASSUMED
  authorization outcome for every transaction: outcome keys (132.8 MiB) and per-account outcome sets
  (56.0 MiB). That is an upper bound, not the profile.
- **Steady state at 500 TPS projects 36.53 GiB.** The largest line is the per-observation string keys:
  45,000,000 keys at 416 bytes, 17,852.8 MiB. Merchant minute buckets follow, at 8,943.8 MiB.
- **A sorted set's compact encoding ends at 128 members.** An account's raw set costs 7,232 bytes at
  128 members and 18,040 bytes at 129, a jump a straight line through small sizes cannot show.
- **Per scored transaction,** the ten-minute projection is 1,670 bytes of feature state, or 1,010
  bytes without the assumed outcomes.

### 3. The configured limit stays 704 MiB

The model's rule, the ten-minute projection with 1.25x headroom rounded up to 64 MiB, implies 640 MiB.
The limit is not lowered on the model alone:
- under `noeviction`, an undersized store refuses writes, and the load gate fails;
- no load-gate run has measured the Step 1b layout's real end-of-run memory yet;
- the Phase 3 exit re-runs Phase 2's load gate after the hot-path changes, and that run validates
  the limit.

704 MiB is above the model's figure and inside ARCHITECTURE §14's `core` budget. If the load gate
shows the store needs more, a new decision raises the limit.

### 4. ADR-0042's footprint figures are superseded

ADR-0042's per-request feature-state figure described the Phase 2 layout. Capacity statements cite
this ADR and its `run_id` instead. ADR-0042's other decisions stand: memory pressure is a correctness
concern, and nothing walks the keyspace while a run is in flight.

## Alternatives Considered

| Alternative | Why not |
|---|---|
| Lower the limit to the model's 640 MiB now | No load-gate run on the Step 1b layout validates it; under `noeviction` an undersized store refuses writes and fails the gate |
| Keep citing ADR-0042's figures | They describe key shapes the store no longer writes |
| Re-derive the key shapes by hand, as Phase 2 did | Hand-written shapes drift from the Lua scripts; measuring through the store cannot |
| Omit authorization outcomes from the projection | The store holds them (ADR-0049 §6); an assumed upper bound, labelled, is safer than a silent zero |

## Consequences

**Positive.**
- Every figure is measured through the production scripts, so the model cannot drift from the store.
- The encoding threshold is measured, not extrapolated across.
- The outcome assumption is visible on its own lines and can be replaced by a measured profile.

**Negative.**
- Steady state at the target rate needs about 36.53 GiB (`run_id:
  bench-20260915-054159-memory-model-5ee55136`), dominated by one key per observation. A laptop
  holds minutes of target-rate traffic, not days.
- The configured limit is unvalidated on this layout until the load gate re-runs.

**Risks.**
- The assumed outcome rate could be far from a profile that posts outcomes, in either direction for
  the per-account sets.
- Pre-aggregating raw windows, the fallback if score latency by depth demands it, would change these
  figures again.

## Status

Proposed
