# ADR-0028: Event models generated from JSON Schema, with a drift gate and a release ledger

- **Status:** Accepted
- **Date:** 2026-09-12
- **Phase:** 1

## Context
ADR-0026 already fixed the *direction* of truth for events: JSON Schema is the contract, Pydantic is
derived from it, never the reverse. Phase 1 has to make that real, and three questions had no answer
yet.

**How is the generation actually performed, and how do we know it was?** A rule that models are
generated is worth nothing if a hand edit survives. The failure is quiet: an edited model still
imports, still validates, and now describes a contract that only Python consumers see, while Spark and
any future Go consumer read the unchanged schema.

**Which topics get schemas now?** `docs/EVENT_CONTRACTS.md` §3 lists ten topics, but Phase 1 has a real
producer for only three. A schema file is **immutable once merged** (ADR-0026), so releasing a contract
before anything has exercised it is a commitment to getting it right on the first try — and when it is
wrong, the correction costs a `.vN+1` topic and a dual-write window.

**What stops a released schema being edited in place?** "Immutable by convention" is the same class of
control as "ground truth hidden by naming convention", which ADR-0004 rejected for good reason.

## Decision
**`datamodel-code-generator` renders `docs/contracts/events/*.json` into
`packages/trace_core/contracts/events/`,** driven by `scripts/generate_event_models.py` (`make
codegen`). Generation runs into a temporary directory and is copied in only on success, so a failed run
cannot leave half-written models behind. Every generated module carries a `DO NOT EDIT BY HAND` header.

**Every model derives from `StrictEventModel`** (`extra="forbid"`, `frozen=True`, `strict=True`).
`extra="forbid"` is the load-bearing setting: silently accepting an unknown field lets a producer add
one that no consumer validates, turning a "compatible" change into silent data loss.

**The drift gate is `--check` on the same script.** It re-renders into a temporary directory and
compares file by file, so it gives the same answer on a dirty working tree, in CI, and inside a test.
Comparing against `git status` instead would make the check depend on the caller's state — and in
Phase 0 a control implemented two different ways went green locally and red in CI, which is the reason
this is one definition with three callers.

**Only topics with a real producer in the current phase are released.** Phase 1 releases
`envelope.v1` (a shared `$ref`, not a topic) plus `tx.raw.v1`, `identity.events.v1` and
`device.events.v1`. The remaining six stay `PLANNED`.

**`docs/contracts/RELEASED.json` is the release ledger**, recording for each released topic its schema
file, its **sha256**, its partition key and the stated reason for that key. Editing a released schema
changes its digest and fails the build unless the ledger is updated deliberately — which converts an
invisible edit into a reviewable diff that says "a released contract changed". A test also fails if any
code references a topic the ledger does not mark released.

`event_id` carries a **pattern rather than `"format": "uuid"`**. The two are incompatible in the
generated model, and the pattern is worth more: `format` only asserts "a UUID", while the pattern pins
the version nibble to **7**. Time-ordering is the property the deduplication key and Delta index
locality actually depend on, so a v4 slipping through would be a real defect.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Hand-write the Pydantic models and treat the schemas as documentation | Reverses ADR-0026's direction of truth in practice while appearing to honour it. The two drift, and the schema — the artefact Spark and a future Go gateway read — becomes the stale one |
| Generate the JSON Schema from Pydantic (`model_json_schema()`) | Far less code, and explicitly rejected by ADR-0026: it privileges Python and makes every non-Python consumer derive from one language's type system |
| Skip the generator; validate raw dicts with `jsonschema` at runtime | Loses static types across the whole codebase, and moves every field access to a dict lookup mypy cannot check. Validation is only half of what a contract is for |
| Release all ten topics now, so downstream phases inherit a complete surface | Freezes seven contracts nobody has exercised. Because released files are immutable, each mistake costs a `.v2` topic and a dual-write window — the expensive correction path, chosen up front for no benefit |
| Enforce immutability with a git-diff-against-`main` check only | Works in a PR and not much else: it is unavailable offline, gives a different answer on the branch that introduces a schema, and cannot run in a unit test. The digest ledger works everywhere; the git check remains useful as a CI addition, not as the primary control |
| Detect drift with `make codegen && git diff --exit-code` | Depends on the working tree being clean, so it cannot be a test and fails confusingly mid-edit. The temp-directory comparison is state-independent |

## Consequences
**Positive.** The schema is genuinely the source of truth, and a hand edit is reverted rather than
merged. `extra="forbid"` plus a pattern-pinned UUIDv7 means malformed events are rejected at the
boundary, proven by eleven negative cases. The ledger makes "which contracts are frozen" a single
machine-readable fact that both the immutability test and the topic gate read.

**Negative.** A new dependency (`datamodel-code-generator`) and a build step that must be remembered —
mitigated by the gate, which fails loudly rather than letting staleness accumulate. Generated code
cannot carry inline lint suppressions, so `E501` and `S105` are ignored for that directory and
`detect-secrets` excludes it; the exclusions are justified by the source schemas themselves being
scanned and by the drift gate proving the files are never hand-edited. Schema `description` text is
copied verbatim into docstrings, so long descriptions produce long lines. Six topics have no schema
yet, so a later phase must write one before it can produce — which is the intent, but it is friction.

**Risks.** The ledger's digests could be updated reflexively along with a schema edit, defeating the
control. Mitigated only socially — by the diff being obvious in review — which is why the ledger
records *why* each key was chosen as well as what it is. A second risk is `datamodel-code-generator`
changing its output between versions and producing a large spurious diff; the version is pinned in
`requirements.lock`, and such a change would surface as a failed drift check rather than as silent
model changes.

## Status
Accepted
