# ADR-0033: Rule packs are data in a closed grammar, digest-pinned and fail-safe on reload

- **Status:** Accepted
- **Date:** 2026-09-12
- **Phase:** 2
- **Supersedes / Superseded by:** —

## Context

`docs/ARCHITECTURE.md` §7 specifies the rules tier as "deterministic, YAML-declared, versioned,
hot-reloadable, individually testable. Zero latency, full explainability", and ROADMAP Phase 2 asks
for at least fifteen such rules. Three properties of that specification pull against each other and
none of them is free.

**Hot reload is a code-execution surface.** A YAML file that changes scoring behaviour with no deploy
and no review gate is, if predicates are evaluated as Python, arbitrary code execution in the gateway
process reachable by anyone who can write that file. It would be the one place in this system where
the principle that input never becomes executable code did not hold — `docs/SECURITY.md` §6 states
exactly that principle for tools ("no LLM-authored queries exist; graph access is six parameterized,
allow-listed queries").

**Hot reload makes "which rules produced this decision?" unanswerable from a deploy log.** Rules can
change between two transactions a second apart. If the decision does not carry the answer, an audit
reconstructs behaviour by guessing.

**Features can be absent, and absence is not falsity.** ADR-0032 gives a feature three outcomes, two
of which carry no number. A predicate over an absent feature therefore has no truth value. Treating it
as false is the same fabrication as imputing zero, one level up: the rule stops firing silently on
every source lacking the field, and the resulting score reads as a confident "not fraud" rather than
as "not assessed".

## Decision

**A predicate is data, not code.** It is a tree of five typed node kinds — a feature comparison over
six operators, a membership test against a *named* constant set, and `all_of` / `any_of` / `not` — with
no function calls, no attribute access, no name resolution and no reach beyond what the evaluator
hands it. Packs are parsed with `yaml.safe_load`; the full loader can construct arbitrary Python
objects and would reintroduce the surface this removes. Predicate depth is capped at 6, because an
unreviewable rule is one nobody can say is correct.

**Membership tests may read only sanctioned categorical fields** (`merchant_mcc`,
`merchant_country`, `currency`, `channel`, `entry_mode`) — never `merchant_name`, `user_agent` or
`memo`. Those are attacker-controlled (`docs/SECURITY.md` §5.1), and a rule branching on a merchant
name would let a fraudster choose their own risk score by renaming their shop.

**Evaluation is Kleene three-valued logic.** An absent feature yields `UNKNOWN`, the rule abstains, and
the abstention is counted and reported on the decision. `UNKNOWN` still settles where the answer is
already determined — one false conjunct settles an `all_of`, one true disjunct settles an `any_of` —
so a missing feature costs coverage only where it genuinely mattered rather than blinding the pack.

**A pack is digest-pinned**, and the digest is computed over the parsed model's canonical JSON rather
than the file bytes: reformatting, reordering keys or editing a comment must not move it, while
changing a threshold, weight, predicate or constant set must. Every `RiskDecision` carries
`rule_pack_digest`.

**A rule declares its effect twice**: a `weight` contributing to a bounded weighted score, and an
optional `band_floor` that forces a minimum risk band regardless of that score. The floor exists
because averaging is the wrong model for some signals — two card-present transactions implying
supersonic travel is not a "somewhat risky" observation to be outvoted by twenty quiet rules.

**Reload is atomic and fail-safe.** The candidate is parsed, validated and compiled in full before
anything is swapped. Any failure leaves the running pack untouched, increments a counter, and returns
the reason. There is no partial load: a pack with one bad rule does not load the other nineteen,
because the result would match neither what was reviewed nor what was intended. A pack with **zero
enabled rules is refused** — it would score every transaction zero, which is indistinguishable from a
quiet day. The **first** load failing is fatal, for the same reason a corrupt model artifact refuses
to boot (`docs/ARCHITECTURE.md` §18): there is nothing to fall back to, and serving traffic scored by
nothing is worse than not serving it.

**Thresholds are declared configuration, not fitted parameters.** Phase 2 has no model and no access
to labels — the application role has no grant on the `groundtruth` schema at all (ADR-0004). Values
are derived from the published fraud *signatures* in `docs/FRAUD_SCENARIOS.md` §3, which are
properties of the data. Phase 4 selects operating points inside the evaluation harness, which may read
ground truth because that is its job.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Evaluate predicates as Python expressions (`eval` with a restricted namespace) | Sandbox escapes from "restricted" `eval` are a well-trodden path, and the pack is hot-reloadable by someone who may not be the reviewer. The cost of a closed grammar is one afternoon; the cost of being wrong is RCE in the gateway |
| Rules as Python functions in the codebase | Loses hot reload entirely, and makes every threshold change a deploy. Also makes "which rules were live?" answerable only from git, which does not help when a pack is rolled forward and back |
| Treat an absent feature as false | Silently stops rules firing on any source lacking the field. The score then reads as a confident "not fraud" rather than "not assessed", which is the exact class of error ADR-0022 exists to prevent |
| Treat an absent feature as true (fail-safe toward flagging) | Floods the investigation queue whenever a store is cold, and inverts the meaning of a rule under exactly the conditions where it is least reliable |
| Digest the YAML file bytes | Moves on reformatting and on comment edits. A digest that changes for non-reasons trains everyone to ignore it |
| Weighted score only, no band floors | Lets a decisive signal be averaged away by quiet rules. Impossible travel would score MEDIUM alongside twenty non-firing rules |
| Band floors only, no weights | Loses the ability to accumulate weak evidence, which is most of what the rules tier contributes before Phase 4's model exists |
| Partial load: apply the rules that parse | Produces behaviour matching neither the reviewed pack nor the intended one, and the difference is invisible at runtime |

## Consequences

**Positive.** No configuration path reaches an interpreter. A decision is reproducible from its own
contents: the pack digest, the rule ids and versions, and the feature values each predicate read.
Abstention is visible, so a decision made with a third of the pack blind is distinguishable from one
where every rule was checked — `coverage` is reported rather than inferred. An invalid pack cannot
take the gateway's rules away.

**Negative.** The grammar is genuinely limited: no arithmetic between features, no ratios computed in
the predicate, no cross-field comparisons. Anything of that shape has to become a *feature*, which is
more work than editing a rule and pushes complexity into a place with its own parity obligations.
Three-valued logic is harder to reason about than two, and every consumer of an evaluation must handle
the third case rather than pattern-matching on a boolean. A capped depth will eventually reject a rule
someone wants.

**Risks.** Rule authors route around the grammar's limits by requesting features that are really
predicates in disguise, inflating the feature set and its parity surface. Signal: features with no
plausible offline translation, or used by exactly one rule with a threshold of zero. Second risk:
`band_floor` proliferates until the weighted score is decorative — a test asserts that not every rule
carries one, and that only impossible travel forces `CRITICAL`. Third: thresholds are quietly tuned
against observed outcomes once Phase 4 data exists, turning declared configuration into unrecorded
fitted parameters. Signal: a threshold change with no corresponding evaluation run id.

## Status

Accepted
