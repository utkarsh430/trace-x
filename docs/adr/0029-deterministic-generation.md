# ADR-0029: Deterministic generation — stdlib RNG, named substreams, row-JSON digests

- **Status:** Accepted
- **Date:** 2026-09-12
- **Phase:** 1

## Context
"Same seed ⇒ identical digest" is a Phase 1 exit condition, and `eval-v1` is referenced by digest
rather than by path so it can never be regenerated in place (`docs/EVALUATION.md` §2). Every future run
manifest cites that digest. A dataset digest that moves for a reason unrelated to the data is therefore
not a cosmetic problem: it invalidates every number that ever referenced it, and an unreproducible
number is indistinguishable from an invented one (ADR-0017).

Three things can make a digest move without the data meaning anything different: the random stream
changing under a dependency upgrade, the generation *order* changing when unrelated code is added, and
the output *encoding* changing when a writer library is upgraded.

## Decision

**1. `random.Random` from the standard library, not `numpy.random.Generator`.**
NumPy explicitly reserves the right to change `Generator`'s stream between versions. CPython's Mersenne
Twister is documented as stable. Stream stability outranks draw speed here.

*Consequence recorded so a later session cannot undo this by accident:* if the throughput budget is
ever missed, it is fixed by optimising object construction or serialisation — **never** by swapping the
RNG, which would silently change every previously recorded digest.

**2. Named substreams, one per `(namespace, key)`, seeded by BLAKE2b of `(seed, namespace, key)`.**
With a single shared stream every draw depends on every draw before it, so adding a fraud scenario or
reordering a loop reshuffles unrelated data. With substreams, `derive(seed, "tx", "417")` is the same
stream regardless of what else the run did — which is what lets step 7 inject ten scenarios without
perturbing the legitimate traffic around them.

BLAKE2b rather than the builtin `hash()`, which is randomised per process by `PYTHONHASHSEED` and would
make the generator irreproducible across runs *on the same machine*.

**This is a real trade, and it was measured rather than assumed.** Deriving a substream per row costs
a substantial minority of total generation time, and the profile is unambiguous about where it goes:
the `random.Random(seed)` constructor dominates, while BLAKE2b seed derivation is an order of magnitude
cheaper and effectively free. The property is kept and the cost accepted.

The measurements themselves are **not reproduced here**. `CLAUDE.md` §13 permits a number in `docs/**`
only when it cites a `run_id` that resolves to a recorded run, and an ADR is immutable once accepted —
so inlining figures would either breach that rule or force a later edit that breaches immutability.
They live in the generator run record, cited from `docs/PROGRESS.md`, where they can be superseded by a
new run without rewriting a decision record.

**3. The dataset digest is computed over canonical row JSON in emission order, not over output bytes.**
Sorted keys, no incidental whitespace. A file digest would change with the container format, the
compression level or a pyarrow upgrade — none of which change the data. A row digest changes exactly
when the data changes.

**4. Timestamps are sampled first, sorted, then filled in.** Events are emitted in event-time order.
Sampling N timestamps, sorting that integer array, and only then building rows keeps peak memory at one
array of integers rather than N dictionaries — on a 1M-row run, roughly 8 MB instead of well over a
gigabyte.

**5. Produce-time validation is a policy, and the policy is recorded.**
`docs/EVENT_CONTRACTS.md` §6.1 requires every message to validate against its schema before
publication. Measurement showed that generation alone clears the ROADMAP budget while generation *plus*
per-row validation does not, so **two figures must be quoted rather than one**, each labelled with the
mode that produced it. Validation stays **on by default** — correctness first — and the mode is recorded
in the run record rather than left implicit, so no reader has to guess which number they are looking at.

Pydantic was chosen as the validator over raw `jsonschema` on measured grounds: it was faster by an
order of magnitude on the same rows. Both figures are in the run record.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| `numpy.random.Generator`, vectorised | Materially faster, and it forfeits the one property that matters: NumPy does not guarantee stream stability across versions, so a routine dependency bump would silently change every dataset digest and quietly invalidate every metric citing one |
| A single shared `random.Random` for the whole run | Fastest option and the most fragile. Generation order becomes part of the contract, so adding a scenario in step 7 would reshuffle the legitimate rows and change the digest for no semantic reason |
| One substream per batch of rows rather than per row | Recovers most of that cost while weakening the property to "a change perturbs only its own batch". Rejected for now because the budget is met without it; recorded here as the first thing to try if that stops being true |
| Hash-derive a seed and hand-roll a cheap PRNG | Removes the `random.Random` construction cost, at the price of an unreviewed PRNG in the one component whose entire job is reproducibility. A subtle bias here would be invisible and would contaminate every scenario |
| Digest the output file's bytes | Simpler to compute and wrong: a pyarrow upgrade or a compression-level change would present as a data change, training everyone to ignore digest mismatches |
| Validate a sample of rows rather than all of them | Buys the throughput budget by weakening a stated contract rule. `docs/EVENT_CONTRACTS.md` §6.1 says an invalid message is never published; sampling makes that "usually" |
| Generate unsorted and sort at the end | Requires holding every row in memory. At 1M rows that is the difference between a laptop-viable run and an impossible one |

## Consequences
**Positive.** Reproducibility is structural rather than hoped for: same seed, same digest, verified by
test. Step 7 can add scenarios without disturbing existing data. Peak memory is independent of row
count. The digest survives encoding and library changes.

**Negative.** A substantial minority of generation time goes to substream derivation. Produce-time
validation puts the end-to-end pipeline below the ROADMAP's generation budget, so two numbers must be
quoted rather than one — more honest and less convenient. The generator cannot use NumPy's vectorised
sampling, so it stays a Python loop and will never be as fast as a vectorised implementation.

**Risks.** Someone optimising throughput later replaces the RNG or the substream scheme and moves every
digest. Mitigated by stating the prohibition in this ADR, by the frozen `eval-v1` digest test failing
loudly, and by recording the measured cost so the trade can be re-evaluated with evidence. A second
risk is CPython itself changing Mersenne Twister — it is documented as stable, and `python_version`
is a recorded manifest field, so such a change would appear as a manifest diff rather than as a mystery.

## Status
Accepted
