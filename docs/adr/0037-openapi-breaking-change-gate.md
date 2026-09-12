# ADR-0037: The API breaking-change gate is `oasdiff`, not a bespoke rule engine

- **Status:** Accepted
- **Date:** 2026-09-12
- **Phase:** 2
- **Supersedes / Superseded by:** —

## Context

`docs/API_CONTRACTS.md` §2 states the requirement: *"every PR diffs `openapi.yaml` against `main`. A
breaking change without a major-version bump fails the build. The diff is reviewed, not merely
detected."* §2 also gives the compatibility table the gate has to enforce — about a dozen rows
covering added fields, required-ness, enum values, type changes, range narrowing and status-code
semantics.

A dozen rows looks like an afternoon's work. That impression is the trap. The rows are stated over a
flat request and a flat response, and real specifications are neither: a narrowed enum three schemas
deep behind two `$ref`s, a `required` added inside one branch of a `oneOf`, a numeric range widened on
a response (compatible) and the same change on a request (also compatible) and the reverse of each
(breaking, in opposite directions). A rule engine written from the table would handle the table and
be silently wrong everywhere else — and "silently wrong" for a compatibility checker means green.

The consequences of being wrong are asymmetric. A gate that over-rejects gets bypassed within a week,
and then nothing is checked. A gate that under-rejects lets a breaking change reach a released
`/v1` surface, and §2's only remedy is a new major version: a parallel surface, a dual-write window,
and two APIs to support.

## Decision

**Use `oasdiff`, run as the digest-pinned container image of ADR-0036**, with `--fail-on ERR` so
breaking changes fail the build and advisories do not.

**The gate has a self-test, and the self-test is itself a test.** `scripts/openapi_diff.py --self-test`
feeds the tool two fixture pairs and requires opposite verdicts:

* a **breaking** pair — an optional request field made required, and a value removed from a response
  enum — which must be rejected;
* a **compatible** pair — an optional field added, and a new endpoint added — which must be accepted.

Both halves are load-bearing. Without the first, a checker that had stopped rejecting anything would
be indistinguishable from a codebase that had stopped making breaking changes. Without the second, a
checker that rejected everything would pass its own test while being useless.

The fixtures are small hand-written documents, deliberately **not** derived from the real spec: a
fixture that tracked the real API would change whenever the API changed, and the gate's own test
would then be asserting something different every time.

**The spec it guards is generated and committed.** `docs/contracts/openapi.yaml` is produced from the
Pydantic models (§1's direction of truth) by `make codegen-openapi`, and a drift gate in `make verify`
and in CI regenerates and fails on any diff. Committed rather than generated on demand for two
reasons: the diff needs a stored previous version, and a client generator needs a file it can fetch
without booting the service. `info.version` tracks the URL major (`1`) and never the build, because a
version that moved on every deploy would make every comparison look like a version bump and nothing
would ever be flagged.

**Error responses are documented as RFC 9457 `Problem` and nothing else.** They are declared with an
explicit `content` block and a `$ref` rather than a response `model`, because passing FastAPI a model
also registers an `application/json` variant — and the published contract would then promise a media
type the service never serves, which a generated client would be built to parse. The consequence is
that FastAPI does not register the schema itself, so the generator injects it and a test asserts that
no `$ref` in the document dangles.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| A bespoke Python checker over the §2 table | The table is not the hard part. A narrowed enum behind two `$ref`s, or `required` added inside a `oneOf`, is where a hand-rolled checker is wrong — and wrong there means green |
| Compare the two documents textually and review the diff by eye | §2 asks for the diff to be reviewed *and* detected. A human reviewing a 600-line YAML diff on a Friday is the control this gate exists to replace |
| Version the API by content hash and forbid any change to `/v1` | Correct and unusable: every additive change would need a new major version, so nobody would make additive changes and the surface would ossify |
| Run `oasdiff` from a binary on `PATH` | ADR-0036: a gate whose verdict depends on who ran it is not a gate |
| Gate on `main` pushes rather than pull requests | A push to `main` has no base to diff against, so the gate would pass vacuously — worse than not running, because it would appear in the checks list |

## Consequences

**Positive.** The compatibility question is answered by a tool that has seen more specifications than
this project will, and the answer is identical locally and in CI. The spec cannot drift from the
models, and cannot describe a response shape the service does not serve. The gate is proven to still
reject, every time it runs.

**Negative.** A dependency on an external tool's *judgement*, not merely its output: if `oasdiff`
classifies a change differently from §2, the tool wins in practice and the document is then wrong
about the project's own policy. The fixtures encode our reading of §2 and would catch a gross
divergence, but not a subtle one. Docker is required for an API gate, which is a heavy prerequisite
for a text comparison.

**Risks.** `oasdiff`'s classification drifts from §2 across a version bump and nobody notices, because
the self-test only covers four cases. Signal: a rejected change that §2 plainly permits, or the
reverse. Mitigation would be more fixture pairs, one per table row — deliberately not done now,
because four pairs that are maintained beat twelve that rot. Second risk: the gate runs only on pull
requests, so a direct push to a protected branch bypasses it; branch protection, not this ADR, is what
prevents that.

## Status

Accepted
