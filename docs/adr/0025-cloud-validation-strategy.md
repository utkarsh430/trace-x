# ADR-0025: Cloud validation — complete IaC, one funded window, then destroy

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 12

## Context
"Deployable to AWS and Databricks" can mean three very different things, with very different costs and
very different credibility:

1. Terraform exists and has never been applied — an **unverified claim**.
2. Terraform is applied once, proven, and destroyed — verified, bounded cost.
3. A permanently running cloud environment — highest fidelity, but MSK + EKS + RDS + Neptune +
   Databricks is easily $300–800/month and contradicts the local-first requirement.

The reference machine currently has **no AWS credentials at all**, so nothing can be applied today.

## Decision
Write **complete, reviewed, plan-clean** Terraform and Databricks Asset Bundles. Apply them **once**, in
a time-boxed validation window with:

- a hard budget cap and a budget alarm
- mandatory cost tags on every resource
- `count`/`for_each` gating so the window brings up a **minimal** footprint
- teardown automation, tested against a dry-run stack **before** the real apply
- a scheduled teardown Lambda as a safety net
- the actual cost captured and published
- `terraform destroy`, then **Cost Explorer verification the following day** that zero billable
  resources remain

CI runs `terraform validate`, `tflint`, `checkov` and `conftest` on every plan. **Apply is
manual-approval only** and never automated.

The window also validates that ports of our abstractions hold: Neptune passes the **same**
`GraphStoreConformanceSuite`, Bedrock passes the **same** `LLMProviderConformanceSuite`, and AgentCore
Gateway passes the **same** `ToolTransportParitySuite`. One suite, a third implementation each time.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| IaC written but never applied | "Deployable to AWS" remains an unverified claim. Plans that have never applied routinely fail on first apply |
| Sustained cloud environment | A real recurring bill for a portfolio system, and it contradicts local-first |
| LocalStack only | Useful for fast feedback, but LocalStack's MSK, Neptune and Bedrock coverage is not faithful enough to validate the parts that matter |
| Apply only the cheap services | Would skip precisely the components whose cloud behaviour is uncertain (MSK, Neptune, Bedrock, Databricks) |

## Consequences
**Positive.** "Runs on AWS/Databricks" becomes an evidenced statement with artifacts, manifests and a
real cost figure. Port abstractions are proven against a third implementation. Cost stays bounded and
known.
**Negative.** A one-shot window means limited time to debug cloud-only issues; anything not caught in
the window stays unknown. Real money, however bounded. Teardown must be trustworthy or the bill
continues.
**Risks.** (1) The window overruns budget — mitigated by cap, alarm, minimal footprint and the teardown
Lambda. (2) `terraform destroy` leaves orphaned billable resources (ENIs, snapshots, log groups) —
which is why next-day Cost Explorer verification is an explicit exit condition rather than an
assumption.

## Status
Accepted
