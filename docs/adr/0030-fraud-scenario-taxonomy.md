# ADR-0030: Fraud scenario taxonomy and the causal evidence key contract

- **Status:** Accepted
- **Date:** 2026-09-12
- **Phase:** 1

## Context
ADR-0021 justifies generating a synthetic dataset on one ground: the generator knows **which facts
actually explain each injected fraud**, and no public dataset does. That is what makes evidence
precision, evidence recall and unsupported-claim rate computable as set operations instead of an LLM
judgement (`docs/EVALUATION.md` §5), and those are the most interesting metrics in the project.

The contract is therefore not "ten kinds of fraud". It is: for each injected instance, a set of
evidence kinds that a competent investigator *would* and *could* find, because the injection actually
created them. Get that wrong and the headline agent metrics measure nothing — while continuing to
produce plausible numbers, which is worse than producing none.

Three specific ways to get it wrong:

- **Claiming evidence that was never planted.** If `ACCOUNT_TAKEOVER` lists `IDENTITY_CHANGE` but the
  injection emits no identity event, every agent is penalised on recall for failing to find something
  that does not exist.
- **Claiming everything plausible.** A scenario listing most of the vocabulary inflates recall for
  free and flattens the metric across patterns.
- **Making every scenario easy.** A benchmark of ten unambiguous cases measures nothing about
  judgement, and the Skeptic ablation in particular would have nothing to bite on.

## Decision

**Ten scenarios**, matching the ROADMAP list, each declaring three things: a mechanically assertable
**signature**, a **`causal_evidence_keys`** set, and an **injection** planned against the existing
population. `docs/FRAUD_SCENARIOS.md` is the reference; `tests/unit/test_scenarios.py` asserts every
signature against the events actually produced, not against the label.

**A causal key is a claim about causation.** Four rules, each enforced by a test:

1. A key is listed only if the injection creates that signal. Where the signal is a separate event —
   a credential change, a failed login — the generator emits it on `identity.events.v1` or
   `device.events.v1` rather than encoding it as a transaction.
2. No scenario claims more than half the evidence vocabulary.
3. No scenario claims `DECISION`, `PROPOSED_ACTION` or `CHALLENGE`. Those are agent *outputs*, not
   facts about a transaction; claiming them would score an agent for citing its own conclusion.
4. Key sets are mostly distinct across scenarios, or per-pattern evidence metrics stop being
   informative.

**Fraud is a departure from a baseline, never a separate population.** A scenario overrides only the
fields it means to change; everything else comes from the account's own profile. Transaction ids are
assigned by final emission position so an id cannot betray a label, and a test asserts fraudulent and
legitimate rows are structurally identical.

**`UNUSUAL_LOCATION_DEVICE` is deliberately weak, and must stay weak.** It is a first-seen device in a
first-seen place at an entirely ordinary amount — barely distinguishable from a customer travelling
with a new phone. That ambiguity is its purpose: it is what the Phase 7 Skeptic should challenge and
what the Phase 9 Arm F ablation is measured on. A test asserts the amount stays inside the account's
normal range, so a later session cannot "improve" it without the test objecting.

**One instance of every pattern is planted before the weighted mix runs.** Sampling alone leaves rare
patterns absent from small datasets, because multi-transaction episodes exhaust the budget first. A
Track A dataset missing a pattern silently drops it from every per-pattern metric, and the absence
would read as a modelling result rather than a sampling artefact. The consequence — that `fraud_rate`
has no effect on datasets too small to hold ten episodes — is documented on the field and pinned by a
test rather than left to be rediscovered.

**The mix is not uniform.** Single-transaction fraud is common and many-account fraud is rare;
weighting them equally would give rare patterns an implausible share of all fraudulent rows.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Label rows fraudulent without recording causal evidence | Cheapest option, and it discards the entire argument for generating data (ADR-0021). Evidence precision and recall would need an LLM judge, which `docs/EVALUATION.md` §5 refuses as a headline metric |
| Derive causal keys automatically from which features fire | Circular: it would define "correct evidence" as "whatever our detector notices", so an agent could score perfectly by replicating the detector's blind spots |
| Make every scenario clearly separable | Produces a benchmark that flatters everything. With no ambiguous case the Skeptic ablation has nothing to bite on and Arm F measures noise |
| Sample scenarios purely by weight, with no coverage floor | Leaves rare patterns missing from small datasets, and the gap is invisible: a pattern with zero instances simply does not appear in the results table |
| Uniform mix across the ten | Gives many-account episodes an implausible share of fraudulent rows, because each contributes tens of transactions |
| Encode credential changes and logins as transactions | Misrepresents both scenarios and makes `IDENTITY_CHANGE` an uncausal key, since no identity event would exist to find |
| Generate fraud as a separate population and merge | Simpler to write, and detectable by artefacts rather than by fraud — the model would learn "rows built by the other code path" |

## Consequences
**Positive.** Agent reasoning quality is measurable without an LLM judge. Every scenario's claim is
verified against the events it actually produces, so a drifting implementation fails a test rather
than silently corrupting a metric. The deliberately ambiguous scenario gives the Skeptic ablation
something real to measure. Pattern coverage is guaranteed, so no per-pattern metric silently vanishes.

**Negative.** Ten scenarios plus signature tests is a substantial body of code to maintain, and each
is a small model of a fraud typology that a practitioner could argue with. The coverage floor means
`fraud_rate` is not honoured on small datasets. Emitting identity and device events means the
generator produces three topics rather than one, so every sink and every consumer must handle more
than transactions.

**Risks.** The largest is a scenario drifting until its declared keys stop matching what it plants —
which would corrupt evidence metrics invisibly. Mitigated by asserting signatures against produced
events. A second risk is that these typologies are a practitioner's sketch rather than a fraud
analyst's ground truth; that is inherent to synthetic data and is exactly why Track B exists and why
no document may imply synthetic accuracy predicts real-world performance (CLAUDE.md §13).

## Status
Accepted
