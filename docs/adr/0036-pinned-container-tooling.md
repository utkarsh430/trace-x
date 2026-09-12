# ADR-0036: Non-Python tooling is pinned as container images, by tag and digest

- **Status:** Accepted
- **Date:** 2026-09-12
- **Phase:** 2
- **Supersedes / Superseded by:** —

## Context

Phase 2 needs two tools that are not Python packages and therefore sit outside `requirements.lock`:

* **`oasdiff`**, which decides whether an API change is breaking (`docs/API_CONTRACTS.md` §2);
* **`k6`**, which produces the latency numbers the ROADMAP's Phase 2 targets are stated in.

One is a **gate** and the other is a **measuring instrument**, and both have the same property: their
version silently changes what they say. A newer `oasdiff` that classifies one more change as breaking
turns a green build red for a change nobody made; an older one turns a red build green. A different
`k6` changes what `p(99)` means across runs, and CLAUDE.md §13 requires every published number to be
attributable to a reproducible run.

The project already solves this for Python: `requirements.lock` pins exact versions with hashes, and
ADR-0018 pins the JVM data stack because a mismatch there "surfaces as an opaque `NoSuchMethodError`".
Neither mechanism reaches a Go binary.

The obvious alternative — "install it and put it on `PATH`" — fails the specific thing this project
cares about. A developer with a locally-installed `oasdiff` gets a different verdict from CI, and the
disagreement surfaces as an argument about whether the gate is right rather than about the change.

## Decision

**Non-Python tools run as container images pinned by tag AND digest**, declared in one place:

```toml
[tool.trace_x.tools]
oasdiff = "tufin/oasdiff:v1.11.7@sha256:d6f1e645…"
k6      = "grafana/k6:0.49.0@sha256:8cd78f9d…"
```

Tag *and* digest: a tag can be moved by its publisher, a digest cannot. The tag stays because it is
what a human reads; the digest is what is actually resolved.

`make doctor` reports whether each image is present, as a **warning rather than a failure** — they are
pulled on demand by the target that needs them, and a developer running `make test-fast` should not be
told their environment is broken because they have not yet run a load test. What *would* be a real
failure is an unpinned tool, and `tests/unit/test_version_pins.py` refuses any entry without a digest.

Every run that publishes a measurement records the tool and its version in its run manifest
(`tool`, `tool_version` are required fields of a `LOADTEST` and `BENCHMARK` record — `scripts/check_claims.py`),
so a number is attributable to the instrument that produced it.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Install the binaries and document the versions | A documented version is not an enforced one. The first disagreement between a developer's build and CI's is discovered as a mystery, and the second is discovered as a habit of ignoring the gate |
| Vendor the binaries into the repository | Multi-platform binaries are tens of megabytes each, cannot be reviewed, and would need updating per architecture — and the repo already refuses to commit datasets for related reasons |
| A GitHub Action that installs the tool | Works in CI and nowhere else. The local run is the one that has to agree with CI, not the other way round |
| Reimplement the checks in Python to avoid the dependency | Considered seriously for `oasdiff` and rejected in ADR-0037: the interesting breaking changes are the ones nobody writes down, and a bespoke implementation would be wrong exactly there and confidently green |
| Pin by tag only | A tag is a mutable pointer. `latest` is the obvious case; a patched `v1.11.7` is the one that would actually catch someone out |

## Consequences

**Positive.** A gate's verdict and a benchmark's number are the same on a laptop and in CI, and both
are attributable to a recorded instrument version. Adding a tool is one line and is covered by an
existing test. No developer has to install anything for the tools to work identically.

**Negative.** Docker becomes a prerequisite for two gates that would otherwise be pure CLI, on a
project whose §12 local-first promise is that `make up` needs nothing exotic — Docker was already
required for the integration suite, so this widens an existing dependency rather than adding one, but
it does widen it. Running a tool through a container costs image pull time on a cold machine and a
volume mount for every input, which is why `openapi_diff.py` copies both specs into a temporary
directory rather than mounting the repository.

**Risks.** A pinned image is never updated because updating it is friction, and the project sits on a
tool with a known bug. Signal: a pin more than a year old with no recorded reason. Second risk: an
image is pulled once and the digest is never re-verified locally, so a tampered local cache goes
unnoticed — Docker verifies the digest on pull, not on run, and this ADR does not change that.

## Status

Accepted
