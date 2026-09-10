# ADR-0023: Action safety pipeline with human approval

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 8

## Context
TRACE-X can propose actions with real consequences: blocking a card, freezing an account, reversing a
transaction. The requirement is absolute — **raw LLM output must never directly execute a financial or
account action** — but stating it is not enforcing it. A `# TODO: validate before executing` comment,
or a validation function someone forgets to call, satisfies the letter and not the intent.

The control must make the unsafe path **unrepresentable**, not merely discouraged.

## Decision
A pipeline where each stage is a type boundary:

```
agent → ProposedAction (typed, schema-validated, evidence-cited)
  → PolicyEngine (deterministic Python, ZERO LLM involvement)
      · schema + referential validation
      · evidence sufficiency for this action type
      · authority check: does decision confidence justify this blast radius?
      · blast-radius calculation (accounts and value affected)
      · circuit breaker (N actions of type T per window → halt + page)
  → RiskClassification {LOW | MEDIUM | HIGH | PROHIBITED}
      LOW → auto-execute | MEDIUM/HIGH → human approval | PROHIBITED → reject + audit
  → Idempotent execution (key = hash(investigation, action_type, target, params), via outbox)
  → Verification read-back (assert the world actually changed as intended)
  → Immutable hash-chained audit event (ADR-0020)
```

**The executor accepts only a `ValidatedAction`, and `ValidatedAction` is constructible only inside the
policy engine.** Enforced by types, not discipline — an agent cannot produce one, and there is no code
path from `ProposedAction` to the executor that bypasses the policy engine.

The Remediation agent's tool surface contains `list_available_actions` only; it **cannot execute
anything**. Every action type declares a compensating action. Approvals expire after 30 minutes to
`ESCALATED` rather than lingering. **Zero unapproved HIGH-risk executions is a hard zero**, not a
percentage target.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Validate inside the executor | One forgotten call, or one new call site, and the check is skipped. The type boundary makes that impossible |
| LLM-based policy checking | The LLM is the untrusted component. Using it to validate itself is circular |
| Human approval for everything | Defeats the point of automation and creates alert fatigue, which makes approvals rubber-stamped and therefore worthless |
| No human approval at all | Unacceptable blast radius for account freezes and reversals |
| Approve at the API layer only | An internal caller bypasses it. The gate must sit at the executor |

## Consequences
**Positive.** The unsafe path is unrepresentable rather than forbidden. Every executed action is
traceable to cited evidence and an approver. Compensation and verification close the loop on partial
failure. Circuit breakers bound blast radius even if classification is wrong.
**Negative.** Latency for MEDIUM/HIGH actions is bounded by human response, not the system. More types
and more ceremony around what could be a direct call. Verification read-back requires every action type
to expose a way to confirm its own effect.
**Risks.** Risk classification being mis-tuned so that too much auto-executes. Mitigated by the circuit
breaker and by the hard-zero test across the full corpus. Approval fatigue leading to rubber-stamping —
mitigated by keeping MEDIUM/HIGH genuinely rare and by measuring override rate.

## Status
Accepted
