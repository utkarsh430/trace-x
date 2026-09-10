# ADR-0020: Hash-chained immutable audit log

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 5

## Context
TRACE-X makes automated decisions that affect customer accounts and money. The audit trail must answer,
after the fact: what was decided, on what evidence, by which agent and model version, who approved it,
what was executed, and did it verify. In a regulated context it must also be **tamper-evident** — an
audit log that can be silently edited has no evidentiary value, and "we trust our DBAs" is not a
control.

## Decision
An append-only, hash-chained audit table:

```
AuditEvent { seq (bigserial), occurred_at, actor {type: HUMAN|AGENT|SYSTEM, id},
             action, entity, before, after,
             payload_hash = sha256(canonical_json(payload)),
             prev_hash    = chain_hash of seq-1,
             chain_hash   = sha256(prev_hash || payload_hash) }
```

- `trace_app` holds **INSERT only** on the `audit` schema.
- A `BEFORE UPDATE OR DELETE` trigger raises unconditionally — append-only at the database level, not
  by convention.
- A verifier job recomputes the chain; a break **pages immediately** and is handled as a security
  incident, not a data-quality issue.
- Every state transition, tool call (over every transport), agent invocation, approval decision and
  action execution emits an event.
- Canonical JSON serialization ensures the hash is stable across languages and library versions.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Plain append-only table | Detects nothing. A row edited by anyone with UPDATE leaves no trace |
| Application-level immutability | The threat includes application bugs, so the control must sit below the application |
| External append-only log (Kafka compacted topic only) | Used *additionally* for export, but loses transactional consistency with the case state it describes |
| Blockchain / DLT | Enormous complexity for a single-writer, single-tenant system. Hash chaining provides the tamper evidence without the distributed consensus that is not needed here |
| Cloud-managed audit service (e.g. QLDB) | Cloud-only, breaking local-first, and would fragment audit across two systems |

## Consequences
**Positive.** Tamper evidence with a trivial verification procedure. Full investigation reconstruction
from the chain. Exportable for external attestation. Verification of 100 k events targets under 5 s.
**Negative.** Every audited operation costs a hash and an insert. The chain is strictly sequential, so
audit inserts serialize — acceptable at this write volume, but a genuine scaling consideration.
Legitimate corrections must be *compensating entries*, never edits.
**Risks.** Chain-write contention under high concurrency. Signal: insert latency on the audit path.
Mitigation if needed: per-entity chains with a periodic aggregating root, which preserves the property
while removing the global serialization point.

## Status
Accepted
