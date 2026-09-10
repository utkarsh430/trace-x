# TRACE-X — API Contracts

> Authoritative for **API versioning and compatibility policy**.
> The machine-readable contract at `docs/contracts/openapi.yaml` becomes authoritative for *shape*
> once generated in Phase 2. This document governs how it may change.

---

## 1. Authoritative locations

| Artifact | Path | Status |
|---|---|---|
| OpenAPI 3.1 specification | `docs/contracts/openapi.yaml` | Generated from Pydantic, **committed**, CI-diffed. Phase 2. |
| TypeScript client | `apps/dashboard/src/api/generated/` | Generated from the committed spec. Phase 10. |
| Python models | `packages/trace_core/contracts/` | Hand-written Pydantic v2 — the source the spec is generated *from*. |

**Direction of truth for APIs:** Pydantic models → OpenAPI → TypeScript client. Never edit the
generated spec or client by hand. (Events run the opposite direction — see `EVENT_CONTRACTS.md`.)

---

## 2. Versioning strategy

- **URL-path major versioning:** `/v1/...`. A major version is a parallel surface, not a mutation.
- **Additive minor evolution** within a major version. No minor version number appears in the path;
  clients discover capability, they do not negotiate versions.
- Two major versions are supported concurrently at most, with a deprecation window announced in
  `OPERATIONS.md` and a `Deprecation` / `Sunset` response header on the older surface.

### Compatibility policy

| Change | Compatible? | Requires |
|---|---|---|
| Add an optional request field | ✅ | — |
| Add a response field | ✅ | — |
| Add a new endpoint | ✅ | — |
| Add an enum value **in a response** | ⚠️ | Clients must tolerate unknown values; documented in the spec description |
| Add an enum value **in a request** | ✅ | — |
| Make an optional request field required | ❌ | New major version |
| Remove or rename any field | ❌ | New major version |
| Change a field's type or units | ❌ | New major version |
| Narrow a value range or tighten validation | ❌ | New major version |
| Change an endpoint's status-code semantics | ❌ | New major version |
| Change default behaviour of an existing parameter | ❌ | New major version |

**CI gate:** every PR diffs `openapi.yaml` against `main`. A breaking change without a major-version
bump **fails the build**. The diff is reviewed, not merely detected.

---

## 3. Surfaces

### `trace-gateway` — service-token auth, no human users

| Endpoint | Method | Notes |
|---|---|---|
| `/v1/transactions` | POST | **Synchronous** `RiskDecision`. p99 budget 100 ms |
| `/v1/events/identity` | POST | 202 Accepted |
| `/v1/events/device` | POST | 202 Accepted |
| `/healthz` `/readyz` `/metrics` | GET | Liveness, readiness, Prometheus |

### `trace-api` — human RBAC via OIDC-style JWT

| Endpoint | Method | Min role |
|---|---|---|
| `/v1/investigations` | GET, POST | `analyst` |
| `/v1/investigations/{id}` | GET | `analyst` |
| `/v1/investigations/{id}/stream` | GET (SSE) | `analyst` |
| `/v1/investigations/{id}/reopen` | POST | `senior_analyst` |
| `/v1/investigations/{id}/override` | POST | `analyst` (low-risk) / `senior_analyst` |
| `/v1/approvals` | GET | `senior_analyst` |
| `/v1/approvals/{id}/approve` | POST | `senior_analyst` (MEDIUM) / `fraud_manager` (HIGH) |
| `/v1/approvals/{id}/reject` | POST | `senior_analyst` |
| `/v1/entities/{type}/{id}/graph` | GET | `analyst` |
| `/v1/models` | GET | `analyst` |
| `/v1/models/{version}/promote` | POST | `fraud_manager` |
| `/v1/eval/runs` | GET, POST | `analyst` / `admin` |
| `/v1/audit` | GET | `auditor` |

---

## 4. Request and response conventions

**Headers.**

| Header | Direction | Meaning |
|---|---|---|
| `X-Request-Id` | in/out | Client-supplied or generated; echoed on every response |
| `traceparent` | in/out | W3C trace context; the root of the investigation's distributed trace |
| `X-Idempotency-Key` | in | Required on every POST with a side effect |
| `X-Trace-Degraded` | out | `true` when the decision was made in a degraded mode |
| `X-Feature-Source` | out | `ONLINE_ONLY` or `RECONCILED` — never silently ambiguous |
| `Deprecation` / `Sunset` | out | Present on a deprecated major version |

**Identifiers.** `trace_id` (W3C, spans ingest → audit) · `correlation_id` (business flow across
services) · `request_id` (one HTTP call) · `idempotency_key` (dedupe key for a side effect).

**Errors** — RFC 9457 problem details, always:

```json
{ "type": "https://tracex.dev/errors/insufficient-evidence",
  "title": "Insufficient evidence",
  "status": 422,
  "detail": "Investigation exhausted its budget with 2 open evidence gaps.",
  "instance": "/v1/investigations/inv_01J...",
  "trace_id": "4bf92f...", "request_id": "req_01J..." }
```

Error `type` URIs are stable and enumerated in the spec. **A new error type is additive; changing what
an existing type means is breaking.**

**Status codes.** `200` ok · `201` created · `202` accepted (async) · `400` malformed · `401`
unauthenticated · `403` unauthorized (RBAC or capability scope) · `404` not found or not visible under
RLS · `409` idempotency conflict — same key, different payload · `422` semantically invalid ·
`429` rate limited (with `Retry-After`) · `503` degraded past the usable threshold.

---

## 5. Idempotency expectations

- **Every mutating POST requires `X-Idempotency-Key`.** A request without one is rejected `400`.
- Same key + same payload ⇒ the original response is replayed; **no new side effect**.
- Same key + different payload ⇒ `409 Conflict`. This is a client bug and is surfaced, never absorbed.
- Keys are retained 24 h in Redis; action-execution keys are retained permanently in Postgres because
  they gate real financial effects.
- `POST /v1/transactions` additionally treats `transaction_id` as a natural idempotency key.

---

## 6. Validation rules

1. **Strict Pydantic v2 models, `extra="forbid"`.** An unknown field is a `400`, never ignored — silent
   field-dropping hides client bugs until they matter.
2. Money is `amount_minor` (integer minor units) plus an ISO-4217 `currency`. **Floats are rejected at
   the schema level.**
3. Timestamps are RFC 3339 with an explicit offset. Naive datetimes are rejected. `occurred_at` (event
   time) and `ingested_at` (processing time) are distinct fields and are never interchanged.
4. Identifiers are typed and prefixed (`acc_`, `dev_`, `mer_`, `inv_`, `act_`), validated by pattern.
5. String fields sourced from clients carry a maximum length and are stamped `trust_tier=UNTRUSTED`.
6. Enums are closed on input. Unknown request enum values are `422`, never coerced to a default.
7. Pagination is cursor-based (`cursor`, `limit`, max 200). Offset pagination is not offered — it is
   incorrect under concurrent insert.
8. Rate limits are per service token and per human role, returned as `429` with `Retry-After`.

---

## 7. Contract testing

| Test | Enforces |
|---|---|
| schemathesis against `openapi.yaml` | Every documented endpoint behaves as specified |
| OpenAPI diff vs `main` | No breaking change without a major-version bump |
| Generated-client compile | The TypeScript client builds against the committed spec |
| Idempotency suite | Replay, conflict, and no-key rejection |
| Error-shape suite | Every error path returns RFC 9457 with `trace_id` |
| Authz matrix suite | Every endpoint × every role, including the negative cases |
