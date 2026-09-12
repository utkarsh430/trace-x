# TRACE-X — Fraud Scenario Catalogue

> Authoritative reference for the ten injected fraud scenarios of Track A.
> The decision behind the taxonomy is ADR-0030; the implementation is
> `data/generator/scenarios.py`; the shapes described here are asserted by
> `tests/unit/test_scenarios.py`.

---

## 1. Why this document exists

Every scenario declares **`causal_evidence_keys`** — the specific evidence kinds that genuinely
explain that instance. This is the single thing that makes Track A more than a labelled dataset: it
turns evidence precision, evidence recall and unsupported-claim rate into **set operations** rather
than an LLM judgement (`docs/EVALUATION.md` §5). No public dataset provides it, which is the entire
argument for generating one (ADR-0021).

That only works if the keys are true. **A causal key is a claim about causation, not a wish list.**
Listing every plausible signal would inflate evidence recall for free and make the metric meaningless.
A key belongs to a scenario only if the injection actually creates that signal — which is why each
scenario's signature below is mechanically asserted, not merely described.

**Ground truth lives only in the `groundtruth` PostgreSQL schema.** `trace_app` — the role the
application and every agent connect as — has no grant on it at all (ADR-0004). Nothing in this
catalogue reaches an agent, including indirectly through a feature.

---

## 2. How injection works

Fraud is a **departure from an account's own baseline**, never a separate universe. A scenario plans
events against the existing population; the engine fills every field from the account's real profile
and the scenario overrides only what it means to change. After the merge, a fraudulent transaction is
indistinguishable from a legitimate one except in the ways its scenario intends — and transaction ids
are assigned by final emission position precisely so the id cannot betray the label.

Two scenarios are defined by non-transaction events, so the generator also emits
`identity.events.v1` and `device.events.v1`: a takeover *begins* with a credential change, and
credential stuffing *is* a burst of failed logins. Encoding those as transactions would misrepresent
both and would make `IDENTITY_CHANGE` an uncausal key.

**Coverage floor.** One instance of every pattern is planted before the weighted mix runs, so no
dataset is missing a pattern. On a dataset small enough that those ten already exceed
`row_count × fraud_rate`, the realised rate is set by that floor and `fraud_rate` has no effect. The
realised rate is measured and recorded, never assumed equal to the target.

---

## 3. The ten scenarios

### 3.1 `ACCOUNT_TAKEOVER`

**Signature.** An identity change, then within hours a device the account has never used, spending
well above profile at unhabitual merchants away from home.

**Causal keys.** `IDENTITY_CHANGE`, `DEVICE_NOVELTY`, `SPEND_PROFILE`, `AMOUNT_ANOMALY`

**Notes.** The identity event is emitted *before* the transactions, and the device is verified absent
from the account's home devices — otherwise the first two keys would be uncausal.

### 3.2 `CARD_TESTING`

**Signature.** Many sub-threshold authorisations across many distinct merchants and MCCs inside a few
minutes from one device, a substantial share declined, followed by one larger charge.

**Causal keys.** `VELOCITY`, `AMOUNT_ANOMALY`, `MCC_ANOMALY`, `DEVICE_SHARING`

**Notes.** Probe amounts are deliberately tiny: the point of card testing is to stay under the amount
at which anyone looks. The payoff charge is what makes the probing worth detecting.

### 3.3 `IMPOSSIBLE_TRAVEL`

**Signature.** Two **card-present** transactions whose great-circle distance over elapsed time implies
a speed no commercial travel achieves.

**Causal keys.** `GEO_DISPERSION`, `VELOCITY`

**Notes.** The elapsed time is derived *from* the distance so the threshold holds by construction.
An earlier draft drew the gap independently and produced instances implying an ordinary airline
speed — a label of `IMPOSSIBLE_TRAVEL` on a possible journey, which is a wrong ground-truth label and
therefore worse than no label. Both legs are card-present because a card-not-present leg has an
innocent explanation.

### 3.4 `VELOCITY_ATTACK`

**Signature.** A burst of transactions on one account far above its own short-window baseline.

**Causal keys.** `VELOCITY`, `SPEND_PROFILE`

**Notes.** Amounts stay ordinary on purpose, so velocity is detectable on its own rather than as a
side effect of an amount anomaly. Otherwise this scenario and `ANOMALOUS_HIGH_VALUE` would not be
distinguishable, and per-pattern metrics would be measuring the same thing twice.

### 3.5 `DEVICE_FARM`

**Signature.** One device fingerprint used by many accounts with no shared geography, each account
transacting only once or twice.

**Causal keys.** `DEVICE_SHARING`, `DEVICE_NOVELTY`, `GRAPH_CLUSTER`

**Notes.** Exactly one device across all participants — that concentration is the signal, and it is
what the graph tier is meant to surface.

### 3.6 `FRAUD_RING`

**Signature.** Several accounts sharing a small pool of devices and IPs, with internal link density
above baseline, converging on a shared merchant set over days.

**Causal keys.** `GRAPH_CLUSTER`, `RING_SCORE`, `LINK_PATH`, `DEVICE_SHARING`

**Notes.** Members share strictly fewer devices than there are members, so the sharing is structural
rather than incidental. This is the scenario the Phase 9 Arm G (minus-graph) ablation is measured on.

### 3.7 `MERCHANT_COLLUSION`

**Signature.** One merchant taking an implausible share of high, unusually **uniform** amounts from
many unrelated accounts over days.

**Causal keys.** `MERCHANT_RISK`, `MERCHANT_PATTERN`, `MCC_ANOMALY`

**Notes.** Uniformity is the tell: genuine spend at one merchant is dispersed, laundering through one
is not. This is the only scenario whose subject is a merchant rather than an account, which is why
merchant popularity is a power law — an outlier is only visible against a baseline.

### 3.8 `CREDENTIAL_STUFFING`

**Signature.** A burst of failed logins across many unrelated accounts from a small **datacenter** IP
pool, a minority succeeding and transacting immediately.

**Causal keys.** `IP_REPUTATION`, `DEVICE_SHARING`, `IDENTITY_CHANGE`

**Notes.** Origins are drawn only from IPs flagged as datacenter ranges, so `IP_REPUTATION` is
genuinely causal rather than assumed. Failures outnumber successes, as they must.

### 3.9 `ANOMALOUS_HIGH_VALUE`

**Signature.** One transaction far beyond the account's own amount distribution, at a merchant
category it never uses.

**Causal keys.** `AMOUNT_ANOMALY`, `SPEND_PROFILE`, `MCC_ANOMALY`

**Notes.** Defined against the account's **own** lognormal distribution, not a global threshold — which
is why the baseline has a long right tail in the first place.

### 3.10 `UNUSUAL_LOCATION_DEVICE`

**Signature.** A single transaction on a never-seen device in a never-seen place, with an amount
squarely inside the account's normal range.

**Causal keys.** `DEVICE_NOVELTY`, `GEO_DISPERSION`

> **Deliberately the weakest and most ambiguous scenario, and it must stay that way.**
> It overlaps heavily with a customer travelling with a new phone. That is the point: it is the case
> the Phase 7 Skeptic should challenge, and the case the Phase 9 Arm F (minus-Skeptic) ablation is
> measured on. Strengthening it — adding an amount anomaly, say — would make the ablation easy and
> destroy what it measures. A test asserts the amount stays ordinary, and it claims only two causal
> keys.

---

## 4. Rules that hold across all ten

| Rule | Why | Enforced by |
|---|---|---|
| Every scenario produces at least one transaction | An episode with no fraudulent transaction contributes no positive label | `test_every_scenario_produces_at_least_one_transaction` |
| No scenario claims more than half the evidence vocabulary | Claiming everything inflates evidence recall for free | `test_causal_keys_are_not_a_wish_list` |
| No scenario claims `DECISION`, `PROPOSED_ACTION` or `CHALLENGE` | Those are agent *outputs*, not facts about a transaction; claiming them would score an agent for citing its own conclusion | `test_causal_keys_exclude_terminal_kinds` |
| Key sets are mostly distinct across scenarios | Identical sets would make per-pattern evidence metrics uninformative | `test_scenarios_do_not_all_claim_the_same_keys` |
| Injection is deterministic | Same seed, same episode (ADR-0029) | `test_injection_is_deterministic` |
| Ids and payloads never reveal a label | Ground truth must be structurally unreachable (ADR-0004) | `test_no_transaction_id_reveals_its_label`, `test_no_event_payload_carries_ground_truth` |
| Fraudulent and legitimate rows are structurally identical | Nothing about a row's shape may betray its label | `test_fraudulent_and_legitimate_rows_are_structurally_identical` |

---

## 5. Changing this catalogue

Scenario definitions are part of the dataset's identity. Changing one changes
`fraud_scenario_config_digest`, which changes the dataset digest, which means a **new dataset
version** — `eval-v1` is never regenerated in place (`docs/EVALUATION.md` §2). Results across dataset
versions are never compared without a manifest-diff note (ADR-0017).

Adding an eleventh scenario means a new `FraudPattern` member, a new entry here, a signature test, and
a place in the mix. Removing one is a breaking change to every recorded Track A result.
