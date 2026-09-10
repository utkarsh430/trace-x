# ADR-0016: LLMProvider tiers, with a local model for keyless bootstrap

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 6

## Context
Three requirements pull in different directions.

1. The brief specifies AWS Bedrock. The reference machine has **no AWS credentials**.
2. **A developer cloning this repository must not be assumed to possess an external API key.** They
   should be able to run the demonstration without purchasing anything.
3. CI must run agent tests deterministically and at zero cost, while the end-to-end path must not be
   mocked.

A single provider binding cannot satisfy all three.

## Decision
One `LLMProvider` port with four **tier-tagged** adapters. The tier governs what a run's numbers may be
used for, and this is **enforced**: the evaluation harness refuses to write a quality result from a
non-`EVAL` tier, and the claim linter rejects any published quality number whose manifest names
`SMOKE`, `DEV` or `CI`.

| Tier | Adapter | Purpose | May publish quality numbers? |
|---|---|---|---|
| `SMOKE` | `OllamaProvider` (OpenAI-compatible endpoint) | **Default when no key is present.** `make demo`, local development, zero cost | **No** |
| `DEV` | `OpenAICompatibleProvider` | Development against a hosted model with the developer's own key | No |
| `EVAL` | `BedrockProvider` (Converse API + Guardrail) | Formal benchmarks and cloud validation | **Yes — the only tier that may** |
| `CI` | `CassetteProvider` | Byte-deterministic replay of recorded real traffic | No (reports the recording's tier) |

The local model is a 3B-class quantized instruct model (~2 GB). Structured output is enforced by the
same schema validator for every tier; a weaker model simply fails validation more often, which is
**visible** in `schema_validation_failures_total` rather than hidden.

**Stated plainly wherever it could mislead: the local model does not match Bedrock quality.** Its
investigation outputs are illustrative, not evaluative. Where it is too weak to finish within budget,
the resulting `INSUFFICIENT_EVIDENCE` is designed behaviour — the demo degrades honestly rather than
faking a decision.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Bedrock only | The entire agent tier would be unrunnable until AWS credentials exist. Breaks local-first and blocks any contributor without an AWS account |
| OpenAI only | Requires every contributor to buy a key; contradicts requirement 2 |
| Local model only | No credible quality tier; benchmark results would be unrepresentative and unpublishable |
| Tiers without enforcement | A documented rule that gets broken under deadline pressure. The gate must be code |
| A 7B+ local default | Better output, but a 5+ GB download on a disk-constrained machine. Recorded as the documented fallback if 3B cannot clear the ≥80% completion bar |

## Consequences
**Positive.** A bare clone runs the demo. CI is deterministic and free. Agent code is vendor-neutral,
and the port is proven by four live adapters rather than claimed with one. Benchmark integrity is
structurally protected.
**Negative.** Four adapters to maintain and keep conformant. Local and cloud output quality differ
markedly, which must be explained repeatedly to avoid misleading a reader.
**Risks.** (1) A 3B model cannot complete investigations — signal: completion rate below 80%; fallback
is a 7B-class model with a documented larger download. (2) Tier gating is bypassed — mitigated by
requiring a visible linter edit to do so.

## Status
Accepted
