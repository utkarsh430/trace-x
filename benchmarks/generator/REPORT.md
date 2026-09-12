# Phase 1 generator — measured results

> Every figure below comes from a recorded run. The `run_id` values resolve in
> `eval/manifest/`, and `make check-claims` enforces that any number quoted in
> `README.md` or `docs/**` cites one (CLAUDE.md §13).
>
> Both runs were taken from a **clean worktree**. A run recorded with
> `dirty_worktree: true` is not publishable (`docs/EVALUATION.md` §8 rule 4), and
> the first attempt at each of these was discarded for exactly that reason.

## ROADMAP Phase 1 targets

| Target | Budget | Measured | Verdict |
|---|---|---|---|
| Single-process generation | ≥ 50,000 tx/s | **34,413 tx/s** | ❌ **not met** |
| 1 M-row dataset, Parquet | < 500 MB | **125.3 MB** | ✅ met |
| Digest-reproducible | same seed ⇒ same digest | verified at full size | ✅ met |

### The throughput miss, stated plainly

The generation budget is **not met**, and the budget was not lowered to fit
(CLAUDE.md §17 forbids exactly that). Two things are worth knowing about it.

**It is not a single hot spot.** Profiling attributes the time to two deliberate
design decisions, both recorded in ADR-0029:

* **Per-row RNG substreams.** Every row derives its own `random.Random` so that
  generation order is not part of the contract — which is what lets fraud
  scenarios be injected without reshuffling the legitimate rows around them. The
  `random.Random` constructor dominates; the BLAKE2b seed derivation is nearly
  free.
* **Canonical-JSON encoding.** The dataset digest is defined over canonical row
  JSON, because a digest over output bytes would change with a compression
  setting or a pyarrow upgrade and present an encoding change as a data change.

**One genuine defect was found and fixed while measuring**, which is why the
measurement was worth taking before publishing a number: the pipeline encoded
every row twice — once to validate and write it, once more inside the digest —
despite the sink module's docstring claiming a single serialisation. Fixed in
`d5b5e20`; the figures here are from after the fix.

ADR-0029 explicitly forbids recovering throughput by changing the RNG, because
that would silently move every previously recorded dataset digest. The remaining
options are a batched substream scheme (recorded in that ADR as the first thing
to try) or a faster serialiser. Neither was done here, because correctness-first
was the right trade for Phase 1 and the gap is visible rather than papered over.

A `load`-marked test asserts the budget and is marked `xfail`, so the gap stays
in the test report and flips to a pass if it is ever closed.

## Run: generation rate

- `run_id`: `gen-20260912-bench-generation-e4c63451`
- commit: `d5b5e206d8a4`, `dirty_worktree: False`
- rows: 300,000 · sink `none` · validation `none`
- **34,413.1 tx/s**

## Run: frozen `eval-v1`

- `run_id`: `gen-20260912-eval-v1-ccfd38d9`
- commit: `b79800d2e836`, `dirty_worktree: False`
- rows: **1,000,000** · validation `all` (every row against its released schema)
- fraudulent: **5,003** → realised rate **0.5003%** (target 0.5%)
- Parquet output: **125.3 MB** across three topics
- dataset digest: `sha256:0679d08a29ee3fd2e065c431cf59cfa2da922c960f382b690e9da3ab52eaef0b`
- end-to-end wall clock including ground-truth write: ~53 s

Reproduction contract: `eval/track_a/eval-v1.manifest.json`. The dataset itself is
gitignored — datasets are never committed — so the manifest carries the seed, the
full config, the generator version and the digest the combination must produce.
`pytest -m slow tests/unit/test_eval_v1_freeze.py` regenerates all million rows
and asserts the digest matches; it does.

## Validation cost

Produce-time schema validation is on by default (`docs/EVENT_CONTRACTS.md` §6.1:
an invalid message is never published). Measured on the same rows, it costs a
material fraction of throughput — which is why two figures are quoted rather than
one, each labelled with the policy that produced it. `tests/load/` asserts the
*relationship* (validated is slower than unvalidated) rather than either number,
since a change making validation free would mean it had silently stopped
happening.

## Distribution inspection

`eval-v1-distributions.md` and `eval-v1-fraud.md` in this directory are the
ROADMAP's manual-validation artefacts, produced by `scripts/inspect_dataset.py`
and `scripts/inspect_fraud.py`. The second connects as `trace_eval` — the only
role permitted to read ground truth — which is the isolation control visible in
ordinary tooling.
