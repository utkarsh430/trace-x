"""Track A synthetic transaction generator (ADR-0021, ADR-0027).

Produces a seeded, digest-reproducible dataset of transaction and identity
events, injects the fraud scenarios catalogued in `docs/FRAUD_SCENARIOS.md`, and
records each injected fraud's `causal_evidence_keys` to the `groundtruth`
PostgreSQL schema — the surface that makes evidence precision and recall
mechanically computable without an LLM judge (`docs/EVALUATION.md` §5).
"""
