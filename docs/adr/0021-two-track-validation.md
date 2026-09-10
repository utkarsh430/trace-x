# ADR-0021: Two-track validation — controlled synthetic causal benchmark and external real-world data

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 1 (design) / 4B (execution)

## Context
The generator produces fraud with known labels **and known `causal_evidence_keys`** — the specific facts
that actually explain each injected fraud. That is what makes evidence precision, evidence recall and
unsupported-claim rate mechanically computable rather than LLM-judged. No public dataset provides it.

But a model validated only on data whose fraud you injected yourself has proven **nothing about
generalization**. Synthetic fraud is more separable than real fraud, and reporting synthetic PR-AUC as
though it estimated real-world performance would be a materially misleading claim.

These are two different claims requiring two different datasets, and conflating them is the failure to
prevent.

## Decision
Two tracks, **structurally separated**:

| | **Track A — Controlled Synthetic Causal Benchmark** | **Track B — External Real-World Data Validation** |
|---|---|---|
| Dataset | TRACE-X generator, seeded, digest-pinned | IEEE-CIS Fraud Detection (590,540 tx × 394 features, ~3.5% fraud) |
| Ground truth | `is_fraud`, `fraud_pattern`, **`causal_evidence_keys`** | `isFraud` label only; obfuscated features |
| Validates | Rules, ensemble, **agent reasoning, evidence precision/recall, unsupported-claim rate** | **Ingestion adaptability, schema adapters, feature engineering, ML generalization, distribution shift** |
| Multi-agent investigation | Yes — the only track with a causal evidence surface | **No.** Its evidence surface is too thin. A limited-evidence smoke run is an *operational* check only |
| Results | `eval.synthetic_runs` | `eval.external_runs` |

**Enforcement, not promise:** disjoint tables never unioned, disjoint report templates, a runtime guard
that raises if an agent-quality metric is written to `eval.external_runs`, and a CI linter that fails
the build if a Track-B `run_id` is cited beside an agent-quality metric.

Every report header carries the standing caveat verbatim. **No document may imply synthetic accuracy
represents real-world fraud performance.**

Track B experiment **E2 (zero-shot transfer) exists specifically to quantify the synthetic-to-real
gap.** A large degradation is the expected, accepted, publishable result and does **not** fail Phase 4B.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Synthetic only | Cannot support any generalization claim; the headline metric would be quietly misleading |
| External only | No `causal_evidence_keys`, so evidence precision/recall and unsupported-claim rate become uncomputable without an LLM judge — losing the most interesting metrics in the project |
| Merge both into one benchmark | Conflates two incompatible claims and would produce a meaningless blended number |
| Report both without enforced separation | A documented convention that erodes. The separation must be code |
| A different external dataset (PaySim, Sparkov) | PaySim is itself synthetic — it would not test generalization at all. IEEE-CIS is real, labelled, and its obfuscation is precisely what tests schema adaptability |

## Consequences
**Positive.** Both claims are supported by appropriate evidence. The synthetic-to-real gap is
quantified rather than hidden. Ingestion adaptability becomes a tested property.
**Negative.** Two harnesses, two report templates, two result families. Phase 4B adds mandatory scope.
IEEE-CIS introduces an external dependency (Kaggle credentials, licence terms, a ~1.5 GB download).
**Risks.** IEEE-CIS coverage proving too thin for a meaningful transfer experiment. Mitigated by
reporting the intersecting-feature-subset size beside every transfer metric, so a thin overlap
visibly invalidates the metric rather than quietly weakening it.

## Status
Accepted
