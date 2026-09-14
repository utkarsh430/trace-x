# TRACE-X — Event Contracts

> Authoritative for **event versioning and compatibility policy**.
> The machine-readable schemas at `docs/contracts/events/*.json` become authoritative for *shape* once
> generated in Phase 1. This document governs how they may change.

---

## 1. Authoritative locations

| Artifact | Path | Status |
|---|---|---|
| Event JSON Schemas | `docs/contracts/events/<topic>.v<N>.json` | **Source of truth.** Released in Phase 1 for the three ingress topics |
| Release ledger | `docs/contracts/RELEASED.json` | Which topics are RELEASED vs PLANNED, each released schema's sha256, and each key's stated reason (ADR-0028) |
| Generated Pydantic models | `packages/trace_core/contracts/events/` | Generated **from** the schemas by `make codegen`; a drift gate reverts hand edits |
| Topic configuration | `deploy/kafka/topics.yaml` | Partitions, retention, cleanup policy. Phase 3 |

**Direction of truth for events is the opposite of APIs:** JSON Schema → Pydantic. A schema is a
cross-language, cross-runtime contract (Python producers, Spark consumers, future Go); deriving it from
one language's type system would privilege that language. A CI check regenerates and fails on drift.

---

## 2. Envelope — mandatory on every event

```json
{
  "event_id":        "uuid7",
  "event_type":      "tx.raw",
  "schema_version":  1,
  "occurred_at":     "2026-09-10T14:03:11.482Z",
  "ingested_at":     "2026-09-10T14:03:11.559Z",
  "producer":        "trace-gateway@1.4.2",
  "trace_id":        "4bf92f3577b34da6a3ce929d0e0e4736",
  "correlation_id":  "corr_01J...",
  "idempotency_key": "sha256:...",
  "payload":         { }
}
```

| Field | Rule |
|---|---|
| `event_id` | UUIDv7 — time-ordered, the deduplication key in both Redis and Spark |
| `event_type` | Dotted, stable. Renaming is a breaking change |
| `schema_version` | Integer, matches the file suffix |
| `occurred_at` | **Event time.** All windowing and watermarking use this |
| `ingested_at` | **Processing time.** Never used for business logic — only for lag measurement |
| `producer` | `service@semver` for provenance and blast-radius analysis |
| `trace_id` | W3C trace id, propagated as a Kafka header **and** in the body |
| `correlation_id` | Business flow id, stable across the whole investigation |
| `idempotency_key` | Deterministic hash of the semantic content |

Envelope fields are never nested inside `payload`, and `payload` never redefines an envelope field.

---

## 3. Topics

**Status is not decoration.** A schema file is immutable once merged, so a topic is **RELEASED** only
in the phase that gains a real producer for it. Releasing earlier freezes a contract nobody has
exercised, and the only correction is a `.vN+1` topic plus a dual-write window. Everything else is
**PLANNED**: documented here so the event surface is reviewable, with no schema file and no frozen
contract. `docs/contracts/RELEASED.json` is the machine-readable ledger, and a test fails the build if
any code references a topic that is not RELEASED (ADR-0028).

| Topic | Key | Partitions (local/cloud) | Retention | Cleanup | Status |
|---|---|---|---|---|---|
| `tx.raw.v1` | `account_id` | 6 / 24 | 7 d | delete | **RELEASED** (Phase 1) |
| `identity.events.v1` | `account_id` | 3 / 12 | 30 d | delete | **RELEASED** (Phase 1) |
| `device.events.v1` | `device_id` | 3 / 12 | 30 d | delete | **RELEASED** (Phase 1) |
| `tx.scored.v1` | `account_id` | 6 / 24 | 7 d | delete | PLANNED (Phase 3) |
| `investigation.requested.v1` | `case_id` | 3 / 6 | 30 d | delete | **RELEASED** (Phase 2) |
| `tx.authorization.v1` | `account_id` | 6 / 24 | 7 d | delete | **RELEASED** (Phase 3) |
| `investigation.events.v1` | `investigation_id` | 3 / 6 | 90 d | delete | PLANNED (Phase 7) |
| `action.proposed.v1` | `investigation_id` | 3 / 6 | 90 d | delete | PLANNED (Phase 8) |
| `action.executed.v1` | `action_id` | 3 / 6 | ∞ | compact | PLANNED (Phase 8) |
| `audit.v1` | `entity_id` | 3 / 6 | ∞ | compact | PLANNED (Phase 5) |

**Declared, never auto-created.** `deploy/kafka/topics.yaml` declares each RELEASED topic's key, its
deduplication identity, the contract settings (`cleanup.policy`, `message.timestamp.type=LogAppendTime`
and the retention above) and, kept apart, the local-development partitions and byte caps. `make
kafka-topics` creates what is missing and `make kafka-topics-verify` fails on any drift. Brokers run
with automatic topic creation off, so publishing to an undeclared topic is an error (ADR-0047).

**No `<topic>.dlq` topics.** A record Spark cannot parse or validate goes to a Delta quarantine table;
a DLQ topic arrives only with a non-Spark consumer (ADR-0047).

**Keying rule: the key is the entity whose *ordering* matters, never a load-balancing choice.**
Transactions key on `account_id` because per-account velocity is order-sensitive; device events key on
`device_id` because device-sharing detection is. Changing a key is a breaking change — it repartitions
the topic and silently reorders history.

**Partition-count changes** break key→partition affinity for existing data. Increasing partitions
requires a new topic version and a dual-write window, exactly like a schema break.

---

## 4. Compatibility policy

**Backward-compatible evolution only.** Consumers must be able to read every message ever written to a
topic version.

| Change | Compatible? | Requires |
|---|---|---|
| Add an optional field with a default | ✅ | — |
| Add an enum value | ⚠️ | Consumers must have an `UNKNOWN` branch; verified by test |
| Widen a numeric range | ✅ | — |
| Document a field more precisely | ✅ | — |
| Remove or rename a field | ❌ | New topic version |
| Make an optional field required | ❌ | New topic version |
| Change a type, unit, or semantic meaning | ❌ | New topic version |
| Change the topic key or partition count | ❌ | New topic version |
| Tighten validation | ❌ | New topic version |

### Breaking-change procedure

1. Create `<topic>.v<N+1>` with the new schema. **Never mutate a released schema file.**
2. Producers **dual-write** to both versions.
3. Migrate consumers; Spark checkpoints are reset only for the new query.
4. Verify zero lag and zero DLQ on the new topic for one full retention period.
5. Stop the dual write; retire the old topic after its retention expires.
6. Record the migration in an ADR.

**CI gate:** every PR runs a compatibility check of each changed schema against its `main` version.
An incompatible change without a version bump **fails the build**.

---

## 5. Delivery, ordering and duplication

- **Delivery is at-least-once.** Every consumer must be idempotent. This is a design constraint, not a
  deployment detail.
- **Ordering is guaranteed per partition key only** — per `account_id`, never globally. No consumer may
  assume cross-account ordering.
- **Deduplication** is by each topic's declared identity (`deploy/kafka/topics.yaml`, PHASE3_PLAN
  §3 Q2): `payload.transaction_id` for `tx.raw.v1` -- a producer retry carries a new `event_id` and
  must still collapse -- `envelope.event_id` for identity and device events, and
  `envelope.idempotency_key` for `investigation.requested.v1`. The online store deduplicates on the
  same identities (ADR-0046 §1), and Spark within its watermark.
- **Out-of-order events** are normal and handled by event-time windowing with a 10-minute watermark.
- **Late events** beyond the watermark are routed to a `late_events` Delta table and counted. **They
  are never silently dropped** — a late event that vanishes is indistinguishable from a bug.
- **Poison messages** never block their partition. A record Spark cannot parse or validate goes to a
  Delta quarantine table with its raw bytes, the error and the consumer version (ADR-0047).

---

## 6. Validation rules

1. Every message validates against its schema **at produce time**. An invalid message is never
   published — producers fail fast rather than poisoning consumers. The one producer factory,
   `trace_core.contracts.publish`, enforces it; its only exception is the fault overlay's deliberately
   invalid records, refused except towards a loopback broker (ADR-0047).
2. Consumers re-validate on receipt. The network is not trusted, and neither is a peer service version.
3. `occurred_at` more than 24 h in the future is rejected as clock skew; more than 90 d in the past is
   accepted but flagged.
4. Money is integer minor units plus ISO-4217 currency.
5. Attacker-controllable string fields (`merchant_name`, `user_agent`, `memo`, `device_label`) carry
   maximum lengths and are stamped `trust_tier=UNTRUSTED` in Bronze — a tag that never leaves them.
6. `trace_id` is propagated as a Kafka header **and** in the body: headers survive Spark's
   transformations, bodies survive header-stripping intermediaries. The producer factory copies the
   header from `envelope.trace_id`; no caller can set a different one. A backfill replay also marks
   every record `trace-replay-mode: backfill` (ADR-0047).
7. Schema files are immutable once merged. Correcting a released schema means a new version.

---

## 7. Contract testing

| Test | Enforces |
|---|---|
| Round-trip | Every schema serializes and deserializes without loss |
| Compatibility diff vs `main` | No breaking change without a version bump |
| Generated-model drift | Regenerating Pydantic from the schemas produces no diff |
| Unknown-enum tolerance | Consumers handle an unseen enum value without crashing |
| Envelope completeness | Every produced event carries all envelope fields |
| Dedup | Replaying an identical `event_id` produces exactly one effect |
| Late-event routing | An event beyond the watermark lands in `late_events`, never nowhere |
