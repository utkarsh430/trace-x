# ADR-0008: Python-only core; Go deferred to an optional phase

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 0

## Context
The original brief specified "Go for high-throughput backend components where justified." Go is not
installed on the reference machine. On a solo project, a second language means a second toolchain,
build pipeline, lint/test setup, dependency policy and — worst — a duplicated set of domain types that
can drift from the Python originals.

"Where justified" is the operative phrase, and justification requires a measurement that does not exist
yet.

## Decision
Build everything in Python 3.12. Define the ingest/decision gateway behind a stable contract (OpenAPI
plus event schemas) with a shared conformance suite.

In **optional Phase 13**, reimplement **only** the gateway in Go and require it to (a) pass the
identical contract suite unmodified and (b) beat the recorded Python p99 by a margin worth a second
toolchain. **If it does not, the phase is abandoned and an ADR records why.** Both outcomes are
acceptable and publishable.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Go gateway from Phase 2 | Adds a toolchain and duplicated domain types on day one, before any measurement shows it is needed. Optimising before measuring |
| Drop Go entirely | Simplest, but forfeits a legitimate polyglot demonstration and the genuinely interesting cross-language contract-testing work |
| Rust for the gateway | Same duplication cost, steeper curve, and no better fit than Go for an I/O-bound HTTP service |

## Consequences
**Positive.** One toolchain during the phases that matter. The gateway contract gets designed properly
because a second implementation must satisfy it. If Go lands, it earns its place with a measured number
rather than a résumé bullet.
**Negative.** The polyglot demonstration is deferred and may never happen.
**Risks.** Python p99 misses the 100 ms budget and Go becomes mandatory rather than optional. Signal:
Phase 2 load results. That would be a legitimate reason to promote Phase 13.

## Status
Accepted
