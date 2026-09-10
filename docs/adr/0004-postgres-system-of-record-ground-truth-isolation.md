# ADR-0004: PostgreSQL as system of record, with ground-truth isolation by schema and role

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 0

## Context
Two requirements meet here.

First, case state, queue state, evidence, approvals and the audit chain must move **transactionally**.
An investigation that is marked complete but whose action never enqueued is a correctness failure.

Second — and more severe — the generator injects fraud with known labels and `causal_evidence_keys`.
**If any of that reaches an agent, every metric in the project is silently invalid.** A naming
convention or a code review cannot guarantee this. The failure would be invisible and would discredit
every result.

## Decision
PostgreSQL 16 is the system of record. Five schemas with distinct grants:

| Schema | `trace_app` | `trace_eval` |
|---|---|---|
| `app` | SELECT/INSERT/UPDATE | SELECT |
| `audit` | **INSERT only** | SELECT |
| **`groundtruth`** | **NO GRANT AT ALL** | SELECT |
| `eval` | none | SELECT/INSERT |
| `external` | none | SELECT/INSERT |

The application and every agent connect as `trace_app`. Ground truth is not hidden — it is
**unreachable**. A test asserts `trace_app` receives `permission denied for schema groundtruth`, and
**that test failing blocks release.**

Row-level security additionally restricts analyst case visibility, so an API authorization bug does not
become a data breach.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Same schema, different table prefix | A convention. One careless join defeats it, silently |
| Ground truth in a separate database | Stronger, but blocks the eval harness from joining predictions to labels in one query — the harness's core operation |
| Application-layer filtering | The failure mode is a code bug, so the control must sit below the code |
| Ground truth in files, not the DB | Loses transactional consistency with the generated dataset and makes the isolation test impossible to express as a grant |

## Consequences
**Positive.** Isolation is enforced by the database and is testable as a single assertion. Transactional
integrity between case and queue state. RLS gives defence in depth.
**Negative.** Multi-role migrations are more complex; local setup must create and grant several roles.
Developers will occasionally hit `permission denied` — which is the control working.
**Risks.** A future migration accidentally granting `trace_app` access. Signal: the isolation test.
This is precisely why the test is a release blocker rather than an ordinary test.

## Status
Accepted
