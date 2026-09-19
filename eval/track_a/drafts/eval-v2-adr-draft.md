# DRAFT ADR (unnumbered): `eval-v2` — a Track A dataset without the eval-v1 label proxies

- **Status:** Draft for lead review (Phase 3 Step E; stage 1 reviewed, stage 1b in progress). Not
  accepted; not numbered.
- **Author:** generator agent
- **Phase:** 3

> **Declaration record.**
> - **Stage 1.** §4 (identity and device rates) and §5 (`LPC-1`) were written into this file before
>   the first generation with the new activity enabled, at any scale, and before any `eval-v2` data
>   existed.
> - **Stage 1b.** After lead review, §4b (transaction-level rates), §3.6 (scenario sub-second timing)
>   and §5b (`LPC-2`) were written **before the first generation that included T1–T3**, at any scale.
>   `LPC-1` was not edited; §9 item 5 records an interim wording defect in §5 and its restoration.
> - §9 records every change and every new declaration, with its reason and its order.

---

## 1. Context

`eval-v1` emits `identity.events.v1` and `device.events.v1` **only inside fraud scenarios**: the
account-takeover identity change and device `FIRST_SEEN`, and the credential-stuffing login burst. Any
identity or device signal therefore marks fraud (`docs/PHASE3_PLAN.md` §1, §3 Q5, B10). `eval-v1` is
frozen and stays frozen: its manifest must still regenerate bit for bit.

B10 reads "eval-v2 without label proxies". The lead's stage-1 review therefore extended the scope from
identity and device presence to three transaction-stream proxies (T1–T3, §1.1).

### 1.1 Evidence gathered before design (diagnostic: no `run_id`)

A probe of the **unmodified** generator (commit `42acb0e`, 120,000 transactions with eval-v1's
population ratios, seed 42) found these. Load-bearing ones are re-established as test assertions.

| # | Artefact in eval-v1 output | Consequence |
|---|---|---|
| A1 | Side events key their envelope substream on the next transaction's position, so consecutive side events share `trace_id` and `idempotency_key` | Dedup on `idempotency_key` drops real events; if only scenario events kept it, a shared envelope would mark fraud |
| A2 | Scenario start times are whole seconds; `_iso` then drops the fractional part | "No fractional seconds" marks scenario events |
| A3 | Device events draw `platform` at random per event | A device whose platform flips marks the scenario |
| A4 | Payload key sets differ by event kind | Legitimate events with a different key set would mark fraud |
| A5 | Side events draw `ingested_at` lag from the shared transaction lag stream | Any new event shifts later transactions' `ingested_at` |
| T1 | No legitimate transaction uses a device outside the account's home devices; most fraudulent transactions do | Transaction label proxy |
| T2 | Whole-second `occurred_at` on planted transactions (every `ANOMALOUS_HIGH_VALUE` and `UNUSUAL_LOCATION_DEVICE` transaction, some legs of others) against about one legitimate transaction in a thousand | Transaction label proxy |
| T3 | `DECLINED` occurs only inside `CARD_TESTING` (found by the lead's feature-semantics review) | Transaction label proxy |

*Numbering note.* The stage-1 draft named the whole-second proxy T1 and the non-home-device proxy T2.
From stage 1b this draft follows the lead's numbering above. §5.10, restored verbatim, refers to the
two transaction proxies together as "(T1, T2)", which is correct under either numbering.

## 2. Decision summary

1. One optional config block, `GeneratorConfig.baseline_identity: BaselineIdentityConfig | None`,
   **absent in eval-v1**. Absent ⇒ output byte-identical to commit `42acb0e`, and the block is omitted
   from the canonical config JSON, so eval-v1's `fraud_scenario_config_digest` does not move.
2. Present ⇒ (a) legitimate identity and device activity (§3.1–§3.2); (b) the eval-v2 side-event
   semantics (§3.3); (c) legitimate transactions on non-home devices and legitimate declines (§3.5);
   (d) sub-second timing for every planted scenario event (§3.6). **Transaction rows change** under the
   gate (lead decision, Q1); the number of transactions, the number of fraudulent transactions, every
   scenario instance and every planned override do not.
3. Scenario definitions (`scenarios.py`), signatures and causal evidence keys are unchanged. The one
   scenario *timing* change is recorded in §3.6.
4. The generator version stays `1.0.0` (lead decision, Q2). Lineage comes from the manifest: config
   digest including the block, seed, git SHA and per-stream digests.
5. `LPC-1` (§5) and `LPC-2` (§5b) are evaluation-side code in `data/generator/label_proxy.py`. Stage 2
   runs them on the frozen eval-v2, regenerated in memory from its manifest and digest-verified.

## 3. Design

### 3.1 Identity activity (per account)

All planning draws come from substreams keyed by account index; none touches a substream the eval-v1
transaction path uses.

| Substream namespace | Key | Draws |
|---|---|---|
| `baseline-engagement` | account | the login-rate multiplier |
| `baseline-secondary` | account | whether the account has a secondary payment device, and which |
| `baseline-devices` | account | new-device enrolments and the coupled MFA change |
| `baseline-device-attrs` | account | `ATTRIBUTE_CHANGED` / `FINGERPRINT_CHANGED` |
| `baseline-logins` | account | successful logins, typo bursts, reset-after-burst |
| `baseline-abandoned` | account | abandoned failure bursts |
| `baseline-changes` | account | standalone identity changes and `MFA_ENROLLED` |
| `baseline-event` | `account:stream:ordinal` | the event's envelope, `user_agent` and lag |
| `baseline-transactions` | account | the decline-propensity multiplier, then every T1 and T3 decision for that account's legitimate transactions, in draw order |

**Logins** ~ Poisson(rate × multiplier × window days) with the generator's diurnal shape; device
uniform over devices the account knows at that moment (§3.2); IP a home IP or, with a declared share,
any universe IP. **Typo bursts** precede a declared share of logins (sizes 1–5 by declared weights,
same device and IP); bursts of three or more end in a `PASSWORD_CHANGE` with a declared share.
**Abandoned bursts** occur at a declared rate. **Identity changes** are independent Poisson processes
at declared per-account-year rates. `population.py` draws home IPs uniformly rather than by region, so
"home region" coherence is coherence with the account's home IPs.

### 3.2 Device lifecycle and device-before-use

A device is **known** to an account at `t` if it is (a) a home device, (b) the account's secondary
payment device (§3.5), both treated as known from before the window, or (c) a device whose in-window
`FIRST_SEEN` for this account is at or before `t`.

**New-device enrolment** (stage 1b): count ~ Poisson(rate × window years), time `τ` with the diurnal
shape; the device is always one new to the account — never a home or secondary device, never one
already enrolled, and never a device any scenario names for this account (so no scenario's
`DEVICE_NOVELTY` key can become false). `FIRST_SEEN` at `τ`; an optional coupled `MFA_RESET` or
`MFA_ENROLLED`, before or after `FIRST_SEEN` by declared shares. A change recorded before `FIRST_SEEN`
uses a device already known.

**Guarantee.** For every legitimate `FIRST_SEEN` of `(account, device)` at `τ`, no event of any topic,
transactions included, references that device on that account before `τ`. Asserted by test.

*Superseded in stage 1b:* stage 1's "home device the account goes on to pay with" branch, and its
diagnostic finding that the branch was rarely eligible. T1 (§3.5) replaces it by letting legitimate
transactions use the new device directly.

**Attribute changes** occur at declared per-device-year rates on each known device while it is known.

### 3.3 eval-v2 side-event semantics (only when the block is present)

1. **Unique envelopes.** Scenario side events draw from `scenario-side-event:instance:ordinal`,
   legitimate ones from `baseline-event`.
2. **Coherent platform.** Every device event reports the universe device's platform.
3. **No whole-second side events** — now provided by §3.6 for every scenario event. *Superseded:*
   stage 1's bounded jitter, which existed only to keep transaction rows invariant.
4. **Same shapes.** Legitimate events use the same payload key set per `(topic, event type)` as
   scenario events, the same `user_agent` distribution, `correlation_id` scheme and lag distribution
   (from their own substream).

### 3.4 Transaction plan under the gate

The gate-on engine builds the same merged plan as eval-v1 (legitimate draws from `clock` and
`account-pick`, scenario events from `plan_fraud`), then transforms it before emission:
scenario references per account → identity and device plan (§3.1–§3.2) → legitimate transaction
decisions (§3.5) → scenario sub-second timing (§3.6) → one sort. Transaction ids are still assigned by
final emission position.

### 3.5 Legitimate devices and declines on transactions (T1, T3)

**T1 — devices.** For each legitimate transaction at `t` on account `a`, in this order:
1. if `a` has a legitimate `FIRST_SEEN` strictly before `t`, the transaction uses the most recently
   enrolled device with the declared `new_device_payment_share`;
2. otherwise, if `a` has a secondary payment device, it uses that device with the declared
   `secondary_device_transaction_share`;
3. otherwise the eval-v1 choice among home devices.

A secondary device is drawn per account with the declared `secondary_device_account_share`, from
universe devices outside the account's home devices and outside every device a scenario names for that
account; it is known from before the window (a household or work device), so it carries no
`FIRST_SEEN`, and logins may use it.

**T3 — declines.** Each account has a mean-one lognormal decline-propensity multiplier (declared
sigma). Legitimate draws are visited in draw order:
- with probability `min(cap, decline_retry_share × multiplier)` a draw becomes a **retry pair**: a
  `DECLINED` attempt a declared gap before the draw's time, then an `APPROVED` retry at the draw's time
  with the same card, merchant, amount, channel, entry mode, device, IP, location and user agent. The
  pair consumes the next draw in draw order, so the transaction count is exactly unchanged. A pair
  whose attempt would fall before the window start is not formed;
- otherwise the draw is `DECLINED` with probability `min(cap, decline_share × multiplier)`.

`CARD_TESTING`'s declines are untouched: legitimate decisions never apply to scenario transactions.

**Decision time, kept in the plan.** Every transaction row under the gate carries
`GeneratedRow.authorization_decided_ms` — the millisecond its authorization outcome was decided —
never serialised into the event. One declared latency distribution applies to every transaction,
approved or declined, legitimate or planted, drawn from one sequential stream in emission order, so a
later decision to emit authorization outcomes as their own dated events can do so without a proxy in
the lag. No new topic is emitted.

### 3.6 Scenario sub-second timing (T2) — the one scenario timing change

Under the gate, **every planted scenario event** — transactions and side events — is emitted at
`floor_to_second(planned_ms) + u`, with `u` uniform on 0–999 ms drawn from
`derive(seed, "scenario-subsecond", "instance_id:ordinal")`. That is how legitimate transaction
milliseconds are drawn (`sample_occurred_at` picks a uniform millisecond within a second).

- Each event stays inside its own planned second, so events planned in different seconds keep their
  order. Events planned inside the same second may swap; no signature depends on sub-second order.
- `IMPOSSIBLE_TRAVEL` gaps are at least one minute and at most three quarters of the infeasible gap,
  so a sub-second change cannot make an implied speed feasible. Asserted on emitted rows.
- Scenario definitions, signatures, overrides and causal keys are unchanged.

## 4. Identity and device rates (declared before stage 1)

Every value is **chosen**; none is a measured or cited statistic.

| Parameter | Value | Rationale (chosen) |
|---|---|---|
| `login_rate_per_account_day` | 0.4 | A few digital-banking sessions a week |
| `login_rate_dispersion_sigma` | 0.8 | Mean-one per-account lognormal multiplier: some accounts rarely log in, some daily |
| `login_away_ip_share` | 0.05 | Logins from mobile networks, travel, work, VPNs; datacenter ranges included |
| `typo_burst_share_per_login` | 0.06 | A small share of logins follow mistyped attempts |
| `typo_burst_size_weights` | 0.70, 0.20, 0.07, 0.02, 0.01 | Mostly one typo |
| `reset_after_burst_share` | 0.5 | Half of long bursts end in a reset |
| `abandoned_burst_rate_per_account_day` | 0.004 | Occasionally the user gives up or is locked out |
| `password_change_rate_per_account_year` | 0.8 | Roughly one a year |
| `email_change_rate_per_account_year` | 0.1 | Rare |
| `phone_change_rate_per_account_year` | 0.1 | Rare |
| `address_change_rate_per_account_year` | 0.12 | Moving home roughly once a decade |
| `mfa_reset_rate_per_account_year` | 0.1 | Standalone second-factor loss |
| `mfa_enrolled_rate_per_account_year` | 0.1 | Standalone enrolment |
| `new_device_rate_per_account_year` | 1.0 | Phone replacement, extra devices, reinstalls surfacing a new id. **Disclosed:** `DEVICE_FIRST_SEEN_24H` precision falls as it rises; below roughly 0.4 it would fail `LPC-1` R3 by construction |
| `mfa_reset_on_new_device_share` | 0.25 | Replacing a phone often resets the second factor |
| `mfa_enrolled_on_new_device_share` | 0.3 | Or enrols the new device |
| `change_before_first_seen_share` | 0.5 | The change may come through another channel first |
| `attribute_change_rate_per_device_year` | 2.0 | OS and app updates |
| `fingerprint_change_rate_per_device_year` | 0.3 | Occasional fingerprint resets |
| ~~`new_device_used_for_payment_share`~~ | ~~0.5~~ | **Removed in stage 1b** (§9): replaced by T1 |

## 4b. Transaction-level rates (declared before any run including T1–T3)

Every value is **chosen**, not measured.

| Parameter | Value | Rationale (chosen) |
|---|---|---|
| `new_device_payment_share` | 0.5 | After a new device appears, about half of the account's payments move to it: a replacement phone takes over, an extra device takes a share |
| `secondary_device_account_share` | 0.3 | A minority of accounts also pay from a household or work device |
| `secondary_device_transaction_share` | 0.1 | Such an account makes an occasional payment from it |
| `decline_share_per_transaction` | 0.02 | Standalone legitimate declines: insufficient funds, expired card, issuer risk rules |
| `decline_retry_share_per_transaction` | 0.01 | A declined attempt the customer retries within minutes (a mistyped security code, a temporary issuer refusal) |
| `decline_propensity_sigma` | 1.0 | Mean-one per-account lognormal multiplier on both decline shares: declines concentrate on a minority of accounts |

Module constants (chosen; covered by the git SHA): retry gap 20 s – 10 min; per-transaction decline
probability cap 0.5; authorization decision latency `40 + |N(300 ms, 150 ms)|` for every transaction.

**Disclosed interactions, stated before running.**
- `LPC-1` R4c for `CHANGE_THEN_NEW_DEVICE_24H` tightens under T1: legitimate customers who replace a
  phone, reset MFA and then pay on the new phone are legitimate instances of that pattern. Analytically
  the margin at the declared rates is thin. The rates above were chosen on plausibility; a stage-1b
  R4c failure would be reported, not tuned away.
- `TX_DEVICE_FIRST_USED_24H` precision (§5b) falls as `new_device_payment_share` and the secondary
  shares rise.

T2 has no rate: §3.6.

## 5. The label-proxy criterion `LPC-1` (declared before any eval-v2 data exists)

### 5.1 Population, labels, time

- **Population:** every `tx.raw.v1` row of the dataset. `n` transactions, `F` fraudulent, `L = n − F`
  legitimate, base rate `π = F / n`.
- **Labels:** `GeneratedRow.label.is_fraud` from the in-memory generation — evaluation-side, never
  read from PostgreSQL, never passed to any runtime path. A structural test asserts no module under
  `packages/`, `services/` or `mcp_servers/` imports the criterion module.
- **Clusters:** a fraudulent transaction's cluster is its `scenario_instance_id`; a legitimate
  transaction's cluster is its `account_id`.
- **Time:** envelope `occurred_at` in milliseconds. A window `W` for a transaction at `t` is
  **`[t − W, t)`**: an event at exactly `t` is not before the transaction (strict event time, Q4f).
  Account attribution uses the event payload's `account_id`, device events included.

### 5.2 Presence signals (gated)

| Signal | Fires when the transaction's account has, within `[t − W, t)` |
|---|---|
| `ANY_IDENTITY_24H` | any `identity.events.v1` event, `W` = 24 h |
| `LOGIN_FAILED_1H` | a `LOGIN_FAILED`, `W` = 1 h |
| `IDENTITY_CHANGE_24H` | a `PASSWORD_CHANGE`, `EMAIL_CHANGE`, `PHONE_CHANGE`, `ADDRESS_CHANGE` or `MFA_RESET` (the Q4e set), `W` = 24 h |
| `DEVICE_FIRST_SEEN_24H` | a `device.events.v1` `FIRST_SEEN`, `W` = 24 h |
| `ANY_DEVICE_EVENT_24H` | any `device.events.v1` event, `W` = 24 h |
| `ANY_IDENTITY_OR_DEVICE_24H` | any identity or device event, `W` = 24 h |

### 5.3 Pattern signals (gated: the legitimate signal must survive)

| Pattern | Refines | Fires when |
|---|---|---|
| `STUFFING_BURST_1H` | `ANY_IDENTITY_24H` | at least **5** distinct accounts have a `LOGIN_FAILED` or `LOGIN_SUCCEEDED` from the **transaction's own `ip_id`** within `[t − 1 h, t)` |
| `CHANGE_THEN_NEW_DEVICE_24H` | `IDENTITY_CHANGE_24H` | `IDENTITY_CHANGE_24H` fires **and** the transaction's `device_id` is first referenced on this account, in any stream including this transaction, at or after `t − 24 h` |

Both are computable online from observed events alone: no label, no population internals.

### 5.4 Statistics

For each signal `s`: `n_s` firing transactions, `F_s` fraudulent, `L_s` legitimate; precision
`p_s = F_s / n_s`; legitimate firing share `ℓ_s = L_s / L`; lift `p_s / π` (reported, not gated). The
interval is the one-sided 95 % Wilson score interval (`z = 1.645`) on `p_s` with **`n` replaced by
the number of distinct clusters among the firing transactions**. This deliberately discounts
correlated rows: one takeover contributes several transactions but one independent observation.

### 5.5 Pass/fail rule

`LPC-1` passes if and only if every rule holds.

| Rule | Applies to | Condition |
|---|---|---|
| **R1** non-vacuity | each presence signal | `L_s ≥ 30` |
| **R2** legitimate floor | each presence signal | `ℓ_s ≥` floor: `ANY_IDENTITY_24H` 5 %; `ANY_IDENTITY_OR_DEVICE_24H` 5 %; `IDENTITY_CHANGE_24H` 0.1 %; `DEVICE_FIRST_SEEN_24H` 0.1 %; `ANY_DEVICE_EVENT_24H` 0.1 %; `LOGIN_FAILED_1H` 0.01 % |
| **R3** bounded precision | each presence signal | upper bound of `p_s` **≤ 0.25** |
| **R4a** pattern fires on fraud | each pattern | `F_p ≥ 5` |
| **R4b** pattern is enriched | each pattern | lower bound of `p_p` **≥ 10 π** |
| **R4c** pattern beats presence | each pattern | lower bound of `p_p` **≥ 3 ×** upper bound of `p_base` |

### 5.6 Why these thresholds (all chosen)

- **R3, 0.25.** Presence may carry *some* risk information — a real identity change genuinely raises
  risk — but it must not determine the label. At most one in four transactions carrying the signal
  may be fraudulent, so a detector flagging on presence alone is wrong at least three times in four.
  The bound is on precision, not lift, deliberately: at eval-v1's base rate it is equivalent to a
  lift bound near 50. A materially tighter lift bound could only be met by inflating legitimate
  change rates beyond anything defensible, which would trade one unrealistic dataset for another.
  The upper confidence bound is used, so a signal passes only on evidence, never on luck.
- **R2 floors.** A floor on the share of *legitimate* transactions carrying each signal, fixed in
  advance and independent of the generator's rates, so that making a signal rare cannot pass. Each
  floor corresponds, under independence, to a minimum legitimate frequency any bank with digital
  channels would exceed: 5 % within 24 h is about one login per twenty account-days; 0.1 % within
  24 h is about one event per 2.7 account-years; 0.01 % within one hour is about one failed login
  per account-year. The floors are sanity floors, not targets.
- **R1, 30.** Below this the interval is dominated by small-number noise, and "the signal barely
  exists among legitimate traffic" is itself a proxy symptom.
- **R4.** Without it the criterion could be passed by drowning the scenarios, for example with
  legitimate multi-account login bursts from shared IPs that make stuffing undetectable. R4b demands
  real enrichment. R4c encodes "detectable by patterns, **not** by mere presence": the pattern must
  concentrate fraud at least three times more than the presence signal it refines, measured
  conservatively (the pattern's lower bound against the presence signal's upper bound).

### 5.7 Why it cannot be met vacuously

| Evasion | Blocked by |
|---|---|
| Emit so little legitimate activity that signals rarely fire | R1 and R2 are fixed in advance and absolute |
| Emit no activity, so precision is undefined | R1 fails (`L_s = 0`) |
| Fire on legitimate traffic that never overlaps with fraud windows | R3 is pooled over all transactions; R2 still requires legitimate volume |
| Drown the scenarios in look-alike legitimate behaviour | R4b and R4c require the scenario patterns to stay enriched |
| Pass on a lucky small sample | Cluster-adjusted confidence bounds, not point estimates |
| Pass because a correction, not the activity, changed something | A zero-rate control (block present, every rate zero) must fail |

### 5.8 Controls, asserted by the test

1. **Negative control (required).** The same generation with the block absent (eval-v1 configured)
   must **fail** `LPC-1`, and must fail for the proxy reason: the *lower* bound of precision for
   `IDENTITY_CHANGE_24H` and `DEVICE_FIRST_SEEN_24H` must exceed 0.25.
2. **Zero-rate control.** The block present with every rate zero must fail.
3. **Criterion self-tests** on hand-built rows: window boundaries (`t − W` included, `t` excluded),
   a planted perfect proxy fails R3, a never-firing signal fails R1, a known Wilson value.

### 5.9 Where it runs, and the two stages kept apart

- **Stage 1 (this draft):** unit-scale, in-memory only, to prove the criterion and its controls work.
  Configuration, declared now: seed 42, 120,000 transactions, 4,800 accounts, 360 merchants, 5,760
  devices, 2,400 IPs, fraud rate 0.005, default window, default `BaselineIdentityConfig`. Its results
  are **diagnostic and not acceptance evidence**.
- **Stage 2 (after approval):** the frozen eval-v2 is regenerated in memory from its manifest; every
  stream digest must match; `LPC-1` is computed on those exact rows; the result is recorded against
  the stage-2 run. The acceptance command belongs to the lead.

### 5.10 Known limits of `LPC-1`

- It judges **identity and device presence** at the transaction level, pooled over patterns. It says
  nothing about transaction-stream proxies (T1, T2).
- Wilson bounds assume independence across clusters; episodes overlapping on one account are rare
  but not excluded.
- Only the declared windows are gated. A shorter identity-change window moves toward a pattern ("a
  change followed by spending") and is expected to be more fraud-enriched; that is the distinction
  §5.3 preserves, not a hole.

## 5b. `LPC-2` (declared before any run including T1–T3)

**`LPC-2` passes if and only if every `LPC-1` rule passes (§5.5, unchanged) and, for each signal below,
the R1–R3 analogues hold.** Population, labels, clusters, time, windows, statistics and the interval
are those of §5.1 and §5.4. Stage 1b runs it at the §5.9 scale, with the §4 and §4b defaults; those
results are diagnostic, not acceptance evidence.

**Home devices** are the account's `home_devices` from the generator's population — evaluation-side
knowledge of the population, not a label, and never passed to a runtime path.

| Signal | Fires on a transaction at `t` when | R2 floor |
|---|---|---|
| `TX_NON_HOME_DEVICE` | its `device_id` is not one of the account's home devices | 1 % |
| `TX_DEVICE_FIRST_USED_24H` | its `device_id` is not a home device, and the account's earliest transaction on that device (this one included) is at or after `t − 24 h` | 0.1 % |
| `TX_WHOLE_SECOND` | its `occurred_at` has zero milliseconds | 0.05 % |
| `TX_DECLINED` | its `authorization_outcome` is `DECLINED` | 0.5 % |
| `TX_DECLINED_PRIOR_1H` | the account has a `DECLINED` transaction within `[t − 1 h, t)` | 0.05 % |

| Rule | Condition, for each signal above |
|---|---|
| **R1** | `L_s ≥ 30` |
| **R2** | `ℓ_s ≥` its floor |
| **R3** | upper bound of `p_s` **≤ 0.25** |

**Why these floors (chosen, fixed in advance, independent of §4b).**
- `TX_NON_HOME_DEVICE` 1 %: about one legitimate payment in a hundred from a device other than the
  account's usual ones.
- `TX_DEVICE_FIRST_USED_24H` 0.1 %: a new payment device first used about once per thousand
  legitimate payments.
- `TX_WHOLE_SECOND` 0.05 %: half of the one-in-a-thousand expected when milliseconds are uniform, so
  the signal is judged against a real legitimate population.
- `TX_DECLINED` 0.5 %: one decline per two hundred legitimate payments, well below any plausible share.
- `TX_DECLINED_PRIOR_1H` 0.05 %: one legitimate payment in two thousand follows a decline on the same
  account within the hour.

**Why first use is measured against home devices.** Measured from observed transactions alone, every
home device is "first used" in the first days of the window, so the signal would fire on legitimate
traffic in eval-v1 for a reason unrelated to device novelty — letting a proxy pass by cold start. Home
devices predate the window by construction, so they are never first used inside it. Consequence,
stated: `TX_DEVICE_FIRST_USED_24H` is a subset of `TX_NON_HOME_DEVICE`. "Used" means paid: identity and
device events do not count as use for this signal.

**Controls, asserted by test.**
1. **Negative control.** The eval-v1-configured generation must fail `LPC-2`, and **for the proxy
   reason on each of the five new signals**: R3 fails **and the point estimate of precision itself
   exceeds 0.25**, so the failure is not an artefact of interval width alone. (The stronger condition
   — the lower bound above 0.25 — is reported for every signal but not required of
   `TX_WHOLE_SECOND`, whose eval-v1 precision is a proxy at a modest margin.)
2. **Zero-rate control.** Block present, every rate zero (identity, device, T1 and T3 rates), must fail
   `LPC-2`.
3. **Self-tests** on hand-built rows for each new signal: home-device boundary, first-use window
   boundary including the transaction itself, whole-second detection, declined window `[t − 1 h, t)`.

**Known limits.** `LPC-2` judges presence, not patterns; it adds no R4 analogue for transaction
signals. Other transaction-row regularities of planted scenarios are not covered by it (§8).

## 5c. `LPC-3` and the M1–M3 corrections (declared before any code or run including M1–M3)

### 5c.1 What the gate changes (stage 1c, lead decision on markers M1–M3)

Under the gate only; gate-off output stays byte-identical to commit `42acb0e`.

- **M1, M2 — planted locations are drawn, never copied.** Every planted transaction that overrides
  `latitude` and `longitude` (`ACCOUNT_TAKEOVER`, `IMPOSSIBLE_TRAVEL`, `UNUSUAL_LOCATION_DEVICE`) is
  emitted at `sample_location(rng, anchor, geo_jitter_km)` — the noise model legitimate transactions use
  around the account's home — with the planted point (the away city, or the home point) as the anchor,
  drawn from `derive(seed, "scenario-location", "instance_id:ordinal:attempt")`. For
  `IMPOSSIBLE_TRAVEL`, the implied speed between legs is re-checked on the final emitted times and
  locations; a draw at or below the scenario's `min_speed_kmh` is redrawn with the next attempt, at most
  32 attempts, and exhaustion raises rather than emit a wrong label.
- **M3 — planted entry modes are drawn from the channel's legitimate distribution.** Every planted
  `entry_mode` override is replaced by a uniform draw from the legitimate entry modes of the planted
  channel (as `_build_transaction` draws them), from `derive(seed, "scenario-entry-mode",
  "instance_id:ordinal")`. This includes `IMPOSSIBLE_TRAVEL`'s card-present `CHIP`, for the same reason.
  Planted channels are unchanged.
- **Documented signatures checked.** No signature or note in `docs/FRAUD_SCENARIOS.md` names an entry
  mode or exact coordinates. §3.3 requires both `IMPOSSIBLE_TRAVEL` legs to be **card-present**
  (lines 73 and 81): a channel, which is not changed. Location wording is "away from home" (§3.1,
  line 54), "a never-seen place" (§3.10, line 147) and an infeasible implied speed (§3.3, line 73), all
  preserved by construction or by the re-check above. So no signature needed an exception.
- No new rates. The noise scale is the existing `geo_jitter_km`.

### 5c.2 The criterion

**`LPC-3` passes if and only if every `LPC-2` rule passes (§5b, unchanged — which includes every
`LPC-1` rule, §5.5, unchanged) and the rules below hold for each new signal.** Population, labels,
clusters, time, statistics and interval are those of §5.1 and §5.4. **Home points** are each account's
`home` from the generator's population — evaluation-side knowledge, not a label.

| Signal | Fires on a transaction at `t` when | R2 floor |
|---|---|---|
| `TX_REPEATED_EXACT_COORDINATES` (M1) | its exact `(latitude, longitude)` equals that of a transaction on the same account whose `occurred_at` is strictly earlier than `t` | 0.1 % |
| `TX_EXACT_HOME_POINT` (M2) | its exact `(latitude, longitude)` equals the account's home point | 0.01 % |
| `TX_CNP_ECOMMERCE` (M3) | its channel is `CARD_NOT_PRESENT` and its entry mode is `ECOMMERCE` | 5 % |

| Signal | Rules |
|---|---|
| M1 | **R1** `L_s ≥ 30`; **R2** `ℓ_s ≥` floor; **R3** upper bound of `p_s ≤ 0.25` |
| M2 | **R1′, R2′, R3′**: each of R1, R2, R3 is also satisfied when `F_s = 0` — equivalently, M2 passes if and only if no fraudulent transaction fires it, or R1, R2 and R3 all hold |
| M3 | **R1, R2, R3** as for M1, **and R6 enrichment parity**: among card-not-present transactions, the upper bound of the share of fraudulent ones that fire must be **≤ 2 ×** the lower bound of the share of legitimate ones that fire. Both bounds are one-sided 95 % Wilson intervals; `n` is the number of distinct clusters among that class's card-not-present transactions (scenario instance for fraudulent, account for legitimate) |

**Why (all chosen).**
- **M1 floor, 0.1 %:** about one legitimate payment in a thousand repeats a location already recorded
  on the account — a retried purchase, a repeat purchase at the same terminal. *Disclosed:* in this
  generator the only legitimate source is T3's retry pairs, which copy the attempt's location (declared
  in stage 1b, before this criterion). With a zero retry rate the signal has no legitimate firings and
  R1 fails.
- **M2 absence alternative.** After the correction, no mechanism — legitimate or planted — places a
  transaction exactly on a home point, so plain R1 would fail a correct dataset by construction. R1
  exists to stop a proxy passing because *legitimate* activity was made rare. M2's proxy was a
  planted-only artefact, so the condition that replaces R1 is the strictest possible one on the fraud
  side: not one planted transaction exactly on a home point. Adding legitimate exact-home transactions
  just to satisfy R1 was considered and rejected as gaming. The floor applies if such legitimate
  transactions ever exist. *This departs from "R1–R3 and a floor" as literally specified, and is
  flagged for the lead.*
- **M3 R6.** At a 0.5 % base rate, a skew the size of eval-v1's cannot push precision anywhere near
  0.25, so R3 alone would let eval-v1 pass this signal. R6 compares the two classes' shares within
  card-not-present transactions directly. The 2.0 bound means a planted share must never be shown, on
  its upper bound, to reach double the legitimate share. *Disclosed:* R6's form was chosen after seeing
  eval-v1's M3 counts in the stage-1b gate-off probe (ECOMMERCE on about three quarters of fraudulent
  card-not-present transactions, against about a third of legitimate ones). No gated M1–M3 data existed
  when it was chosen. *Also a departure from "R1–R3 and a floor", flagged for the lead.*
- **M3 floor, 5 %:** card-not-present is a large share of payments, and `ECOMMERCE` is one of its
  three entry modes.

**Why it cannot be met vacuously.** M1 keeps R1 and R2 in full. M2's alternative is stricter than R3
on the fraud side, never looser. M3 keeps R1–R3 and adds R6. Every `LPC-2` rule still applies.

**Controls, asserted by test.**
1. **Negative control.** The eval-v1-configured generation must fail `LPC-3`, for the proxy reason on
   each new signal:
   - M1 and M2: R3 fails **and** point precision exceeds 0.25.
   - M3: R6 fails **and** the point ratio of the fraudulent to the legitimate card-not-present firing
     share exceeds 2.
2. **Zero-rate control.** The block present with every rate zero must fail `LPC-3`.
3. **Self-tests** on hand-built rows:
   - M1: a same-millisecond repeat is not earlier; equality must be exact;
   - M2: the home point must match exactly, and the absence alternative is exercised;
   - M3: fires only on card-not-present rows, and the R6 arithmetic is checked.

**Stage.** Diagnostic at the §5.9 scale with default settings; the stage-2 acceptance run is unchanged
in form.

### 5c.3 R6 sensitivity window (added after the fact, at lead request; the bound is not moved)

Computed from the stage-1c diagnostic run (in memory, no `run_id`, §5.9 scale):

- **eval-v2 configuration:** fraudulent upper bound 0.4630, legitimate lower bound 0.3230. It passes
  R6 for any bound **≥ 0.4630 / 0.3230 ≈ 1.43**.
- **eval-v1 negative control:** fraudulent upper bound 0.8334, legitimate lower bound 0.3221. It fails
  R6 for any bound **< 0.8334 / 0.3221 ≈ 2.59**.

The declared bound, 2.0, lies inside **[1.43, 2.59)**. This window was computed *after* both results
were seen, so it shows how much room the bound had; it does not justify the bound. 2.0 is unchanged.
**Stage 2's full-size run must report this window again.**

## 5d. `LPC-4` and the M4–M6 corrections (declared before any stage-1d code or generation)

Lead decision (stage 1c review): replace marker-by-marker probing with a declared, systematic audit of
every attribute the hot path, the rules, a model or an agent could read, and remove markers M4–M6 under
the gate. Everything in this section was written before any stage-1d code existed and before any
stage-1d generation, gated or gate-off. Gate-off output stays byte-identical to commit `42acb0e`.

### 5d.1 What the gate changes

**M4 — time of day.** No signature or note names a time of day. Each scenario gets one of two timing
classes, chosen by what its documentation names about duration:

| Scenario | Timing class | Documented duration (line) |
|---|---|---|
| `ACCOUNT_TAKEOVER` | whole episode | "then within hours" (53) |
| `CARD_TESTING` | whole episode | "inside a few minutes" (63–64) |
| `IMPOSSIBLE_TRAVEL` | whole episode | elapsed time derived from distance (73, 78) |
| `VELOCITY_ATTACK` | whole episode | "A burst" (86) |
| `CREDENTIAL_STUFFING` | whole episode | "A burst … transacting immediately" (127–128) |
| `ANOMALOUS_HIGH_VALUE` | whole episode | one transaction (137) |
| `UNUSUAL_LOCATION_DEVICE` | whole episode | one transaction (147) |
| `DEVICE_FARM` | per account session | none; each account's "once or twice" (97) stays together |
| `FRAUD_RING` | per transaction | "over days" (107) — a span, not a time of day |
| `MERCHANT_COLLUSION` | per transaction | "over days" (117) |

- **Whole episode.** Every event keeps its offset from the episode's first event, so every documented
  burst, gap and order survives exactly.
  - A candidate first-event time is proposed uniformly, in whole seconds, from
    `[start_at, end_at − 3 days)`, the eval-v1 range.
  - It is accepted with probability equal to the mean, over the episode's events, of
    `HOUR_WEIGHTS[hour] × WEEKDAY_WEIGHTS[weekday] / (max HOUR_WEIGHTS × max WEEKDAY_WEIGHTS)`. These
    are the weights legitimate times follow.
  - A proposal placing any event outside `[start_at, end_at)` is rejected.
  - Proposals come from `derive(seed, "scenario-start", instance_id)`, at most 10,000; exhaustion
    raises.
  - For a one-event episode this is exactly the legitimate distribution. For a multi-event episode it
    matches on average over the episode's events.
- **Per session.** A session is one account's transactions (`DEVICE_FARM`) or one transaction
  (`FRAUD_RING`, `MERCHANT_COLLUSION`).
  - The session keeps the calendar day (UTC) of its planned first event and its internal offsets.
  - Its first event's time of day is redrawn — hour from `HOUR_WEIGHTS`, minute, second and
    millisecond uniform — from `derive(seed, "scenario-session-time", "instance_id:session")`.
  - A session whose events would fall outside `[start_at, end_at)` moves to the nearest in-window day.
  - No signature depends on order across these sessions.
- T2 and M1–M3 then apply as before; `IMPOSSIBLE_TRAVEL`'s speed re-check runs on the final times.

**M5 — merchant choice.** "Unhabitual" means not in the account's habitual set, never unpopular.
Legitimate spend outside the habitual set is drawn by the generator's popularity (Zipf) weights.

| Scenario, rows | Planted merchant (line) | Under the gate |
|---|---|---|
| `ACCOUNT_TAKEOVER` transactions | unhabitual (54) | popularity-weighted, rejecting the account's habitual merchants |
| `ANOMALOUS_HIGH_VALUE` | "a merchant category it never uses" (137–138) | same as above |
| `CARD_TESTING` probes | "many distinct merchants and MCCs" (63) | popularity-weighted, without replacement within the episode |
| `CARD_TESTING` payoff | unhabitual; not named | no merchant override — the account's legitimate merchant choice |
| `CREDENTIAL_STUFFING` transactions | unhabitual; not named | no merchant override |
| `FRAUD_RING` shared set | "a shared merchant set" (107) | the set redrawn popularity-weighted without replacement; each planned member mapped to the corresponding new member, so the sharing structure is identical |
| `MERCHANT_COLLUSION` | "One merchant" (116); visibility against the popularity baseline (122–123) | kept as planned — the signature names the merchant |

**M6 — channel.**
- **Kept:** `IMPOSSIBLE_TRAVEL`'s `CARD_PRESENT` ("Two **card-present** transactions", 73; 81). Its
  entry mode is still redrawn (M3).
- **Channel and entry-mode overrides removed** — both drawn the legitimate way:
  `ACCOUNT_TAKEOVER`, `CARD_TESTING` (probes and payoff), `DEVICE_FARM`, `MERCHANT_COLLUSION`,
  `CREDENTIAL_STUFFING`. None of their documentation names a channel.

**Anything else `LPC-4` flags** that is not allowlisted is fixed the same way — the legitimate draw
unless the documentation names it — in a later declared step recorded in §9, or reported with the line
it would contradict.

### 5d.2 The criterion

**`LPC-4` passes if and only if every `LPC-3` rule passes (§5c, unchanged), and R7, R8 and R9 below
hold.**

**Populations.** Three:
- transactions (`tx.raw.v1`);
- identity events;
- device events.

In each, **planted rows** are rows of a fraud scenario (fraudulent transactions; scenario identity and
device events), grouped by scenario. **Legitimate rows** are all others. Clusters are the scenario
instance for planted rows and the account for legitimate rows, as in §5.4. Evaluation-side population
knowledge (home devices, IPs and point; habitual merchants; typical amount; merchant, IP and device
attributes; the configured window) is used, never a label.

**Attributes.** Each value (within its stratum, for a conditional attribute) is a cell. Every released
field of the three schemas is covered, either directly or through the attribute named for it.

*Transactions:*

| Attribute | Covers | Values |
|---|---|---|
| `hour` | `occurred_at` | 0–23 UTC |
| `daypart` | `occurred_at` | 00–05, 06–11, 12–17, 18–23 |
| `weekday` | `occurred_at` | Monday–Sunday |
| `in_window` | `occurred_at` | inside / outside `[start_at, end_at)` |
| `subsecond` | `occurred_at` | whole second / fractional |
| `ingest_lag` | `ingested_at` | ms `[0,40)`, `[40,80)`, `[80,120)`, `[120,160)`, `≥160` |
| `channel` | `channel` | released values |
| `entry_mode` | `entry_mode` | released values, **given `channel`** |
| `outcome` | `authorization_outcome` | released values |
| `amount_vs_account` | `amount_minor` | amount ÷ account typical (`exp μ`): `<0.1`, `0.1–0.5`, `0.5–2`, `2–5`, `5–20`, `≥20` |
| `amount_decile` | `amount_minor` | deciles of legitimate amounts |
| `amount_last_digit` | `amount_minor` | 0–9 |
| `amount_roundness` | `amount_minor` | multiple of 1000 / of 100 / of 10 / other (most specific) |
| `currency` | `currency` | released values |
| `merchant_habitual` | `merchant_id` | habitual / unhabitual for the account |
| `merchant_popularity` | `merchant_id`, `merchant_name` | popularity rank 1, 2–3, 4–10, 11–30, 31–100, 101+, **given `merchant_habitual`** |
| `merchant_mcc` | `merchant_mcc` | MCC |
| `merchant_country` | `merchant_country` | country |
| `merchant_country_home` | `merchant_country` | the account's country / another |
| `distance_home` | `latitude`, `longitude` | km `<5`, `5–25`, `25–100`, `100–500`, `≥500` |
| `coordinate_repeat` | `latitude`, `longitude` | repeats a strictly earlier transaction's exact point on the account / not |
| `coordinate_home` | `latitude`, `longitude` | exactly the account's home point / not |
| `coordinate_decimals` | `latitude`, `longitude` | most decimals of the two: `≤2`, `3–5`, `≥6` |
| `device_home` | `device_id` | a home device / not |
| `device_age` | `device_id` | this is the account's first transaction on the device / `<1 h` / `1–24 h` / `1–7 d` / `≥7 d` since it |
| `device_accounts` | `device_id` | distinct accounts transacting on the device in the dataset: 1 / 2 / 3–5 / 6+ |
| `ip_home` | `ip_id` | a home IP / not |
| `ip_accounts` | `ip_id` | distinct accounts transacting from the IP: 1 / 2 / 3–5 / 6+ |
| `ip_datacenter` | `ip_id` | datacenter range / not |
| `user_agent` | `user_agent` | released values |
| `memo` | `memo` | empty / non-empty |
| `card` | `card_id` | the account's first card / another |
| `account_activity` | `account_id` | the account's transactions in the dataset: 1–10, 11–20, 21–30, 31–50, 51+ |
| `identifier_formats` | `transaction_id`, `account_id`, `card_id`, `device_id`, `merchant_id`, `ip_id`, `merchant_name` | all match their released or generated patterns / not |
| `prior_events_24h` | the event-type mix | the account's identity and device events in `[t − 24 h, t)`, most specific: identity change > device event > failed login > other identity event > none |
| `envelope_constants` | `event_type`, `schema_version`, `producer` | the triple |
| `envelope_unique` | `event_id`, `trace_id`, `idempotency_key` | none shared with another event / some shared |
| `event_id_time` | `event_id` | UUIDv7 millisecond equals `occurred_at` / not |
| `correlation_shared` | `correlation_id` | shared with another event / unique |

*Identity events:* `event_type` (the identity event-type mix); `payload_keys` given `event_type`; `hour`;
`daypart`; `weekday`; `in_window`; `subsecond`; `ingest_lag`; `user_agent`; `ip_datacenter` (datacenter /
not / absent); `ip_accounts_logins` (distinct accounts with a login event from that IP: absent / 1 / 2 /
3–5 / 6+); `device_home` (home / not / absent); `device_age` (first reference to the device by the
account in any stream: this event / `<1 h` / `1–24 h` / `1–7 d` / `≥7 d` / absent); `identifier_formats`;
`envelope_constants`; `envelope_unique`; `event_id_time`; `correlation_shared`.

*Device events:* `event_type` (the device event-type mix); `payload_keys` given `event_type`; `platform`;
`platform_consistent` (equals the population device's platform / not); `hour`; `daypart`; `weekday`;
`in_window`; `subsecond`; `ingest_lag`; `device_home`; `device_age`; `identifier_formats`;
`envelope_constants`; `envelope_unique`; `event_id_time`; `correlation_shared`.

**Signature allowlist.** An attribute may be enriched only within the scenarios whose documentation in
`docs/FRAUD_SCENARIOS.md` names it. Source: **S** signature line, **N** notes, **K** causal-keys line.
Entries marked *(interp.)* or sourced **N** or **K** go beyond the signature line itself and are
flagged for review. Rejecting one turns any enrichment it covers into a reported, unfixable conflict.

| Scenario | Allowlisted attributes | Source (line) |
|---|---|---|
| `ACCOUNT_TAKEOVER` | `device_home`, `device_age` (all populations) | S "a device the account has never used" (53) |
| | `amount_vs_account`, `amount_decile` | S "spending well above profile" (54) |
| | `merchant_habitual` | S "at unhabitual merchants" (54) |
| | `distance_home` | S "away from home" (54) |
| | `prior_events_24h`; identity and device `event_type` | S "An identity change, then within hours a device …" (53) |
| `CARD_TESTING` | `amount_vs_account`, `amount_decile` | S "sub-threshold authorisations … followed by one larger charge" (63–64) |
| | `merchant_mcc` | S "many distinct merchants and MCCs" (63) |
| | `merchant_habitual` *(interp.)* | S "many distinct merchants" (63): distinct merchants within minutes fall outside a small habitual set |
| | `device_home`, `device_age` *(interp.)* | S "from one device" (63–64) |
| | `outcome` | S "a substantial share declined" (64) |
| `IMPOSSIBLE_TRAVEL` | `channel` | S "Two **card-present** transactions" (73); N (81) |
| | `distance_home` | S "great-circle distance" (73) |
| `VELOCITY_ATTACK` | `account_activity` | S "A burst of transactions on one account" (86) |
| | `amount_vs_account`, `amount_decile` | N "Amounts stay ordinary on purpose" (90) |
| `DEVICE_FARM` | `device_home`, `device_age`, `device_accounts` | S "One device fingerprint used by many accounts" (96) |
| | `account_activity` | S "each account transacting only once or twice" (96–97) |
| `FRAUD_RING` | `device_home`, `device_age`, `device_accounts`, `ip_home`, `ip_accounts` | S "sharing a small pool of devices and IPs" (106) |
| | `merchant_habitual`, `merchant_mcc`, `merchant_country`, `merchant_country_home` *(interp.)* | S "converging on a shared merchant set" (107): properties of the named set |
| `MERCHANT_COLLUSION` | `merchant_habitual`, `merchant_popularity`, `merchant_mcc`, `merchant_country`, `merchant_country_home` | S "One merchant" (116); N (122–123) |
| | `amount_vs_account`, `amount_decile` | S "high, unusually **uniform** amounts" (116) |
| `CREDENTIAL_STUFFING` | identity `event_type`, `prior_events_24h` | S "A burst of failed logins … a minority succeeding and transacting immediately" (127–128) |
| | `ip_datacenter`, `ip_home`, `ip_accounts`, `ip_accounts_logins` (all populations) | S "from a small **datacenter** IP pool" (127–128) |
| | `device_home`, `device_age`, `device_accounts` (all populations) | K `DEVICE_SHARING` (130) — **not in the signature line; flagged** |
| `ANOMALOUS_HIGH_VALUE` | `amount_vs_account`, `amount_decile` | S "far beyond the account's own amount distribution" (137) |
| | `merchant_habitual`, `merchant_mcc` | S "at a merchant category it never uses" (137–138) |
| `UNUSUAL_LOCATION_DEVICE` | `device_home`, `device_age` | S "a never-seen device" (147) |
| | `distance_home` | S "a never-seen place" (147) |
| | `amount_vs_account` | S "an amount squarely inside the account's normal range" (147–148) |

A conditional attribute is judged within each stratum even when its conditioning attribute is
allowlisted. For example, `ACCOUNT_TAKEOVER` allowlists `merchant_habitual`, but `merchant_popularity`
within the unhabitual stratum is still compared with legitimate unhabitual spend.

**R7 — per-scenario enrichment.** For population `P`, attribute `a` (stratum `c`), value `v`, and each
scenario `s` that does not allowlist `a`:
- `p` = the share of `s`'s planted rows in `P` (and `c`) that have `v`; `n` = the number of distinct
  instances of `s` among those rows.
- `q` = the share of legitimate rows in `P` (and `c`) that have `v`; `m` = the number of distinct
  accounts among those rows.
- `p_lo` = one-sided 95 % Wilson lower bound of `p` with `n`; `q_hi` = `max(Wilson upper bound of q
  with m, 0.001)`.
- **The cell fails if and only if `p_lo > 2.0 × q_hi` and `p_lo − q_hi > 0.02`.**

**R8 — pooled enrichment.** The same test with all planted rows of `P` pooled, `n` counting all their
instances. Exempt: attributes allowlisted by any scenario contributing more than **10 %** of `P`'s
planted rows.

**R9 — non-vacuity.** It fails if any of these holds:
- a population has fewer than 30 legitimate rows, or fewer than 30 legitimate clusters;
- the transaction population has fewer than 30 planted clusters;
- any of the ten scenarios has no planted transaction.

**Power (reported, not gated).** For each scenario and population: its instance count, and the
smallest planted share that could fail R7 against a legitimate share of 0.05. Scenarios with fewer than
5 instances are listed as underpowered. Stage 2 must report this table.

**Why these values (chosen).**
- **Direction — evidence of enrichment, not evidence of parity.** Per scenario, parity cannot be shown
  with few instances. A scenario with 11 instances has an upper bound of at least 0.20 on any share,
  so an R6-style rule would fail every hour bin by construction. R7 fails only when the data shows
  enrichment. The price, stated: an under-sampled scenario can hide a mild enrichment, which is why
  power is reported and stage 2 re-runs the audit at full size.
- **Bound 2.0:** the bound R6 uses. A value at least twice as common among a scenario's rows as among
  legitimate ones, on conservative bounds, is a split a model or rule can exploit. Milder skews are
  within what scenario semantics and legitimate heterogeneity produce.
- **Excess 0.02 and floor 0.001:** a cell needs at least two percentage points of excess on
  conservative bounds to fail. The legitimate reference is never read below 0.1 %. So noise in rare
  values does not fail, while a planted-only value carried by a material share of a scenario's rows
  still does.
- **10 % pooled exemption:** a scenario large enough to move the pool keeps its named attributes;
  smaller ones do not.
- **Interval:** `z = 1.645` and clustering as §5.4.
- **Conditional attributes:** `entry_mode` given `channel`, `merchant_popularity` given
  `merchant_habitual`, `payload_keys` given `event_type`. Without the conditioning, a legitimate mixture
  would be compared with a planted component. Legitimate spend is a mixture of habitual merchants
  (uniform) and non-habitual merchants (popularity-weighted). Judged unconditionally, a scenario drawing
  only non-habitual merchants the legitimate way would look enriched in popular merchants. Judged
  conditionally, a scenario drawing them uniformly — marker M5 — shows as the enrichment it is.

**Known limits.**
- R7 and R8 test enrichment only. A planted depletion spread thinly over many bins can escape; binary
  and banded attributes turn most depletions into enrichments of the complement.
- Planted rows other than `CARD_TESTING` are never declined and never paid from a new or secondary
  device (T1 and T3 apply to legitimate rows). These are depletions, and are reported, not gated.
- The rule was declared after stage-1c probes had shown M4–M6 with numbers. The bound, excess and floor
  were chosen by the argument above; no stage-1d data existed.

**Controls, asserted by test.**
1. **Negative control.** The eval-v1-configured generation must fail `LPC-4`, with at least one failing
   cell on `hour` or `daypart` (M4) and at least one on `merchant_habitual` or `merchant_popularity`
   (M5). Every failing attribute is reported.
2. **Zero-rate control.** The block present with every rate zero must fail `LPC-4`.
3. **Self-tests** on hand-built rows:
   - a planted-only value fails;
   - an allowlisted attribute is exempt for its own scenario only;
   - an attribute distributed like legitimate rows passes;
   - the pooled exemption applies;
   - clustering widens the interval;
   - non-vacuity fails on an empty population.

**Stage.** Diagnostic at the §5.9 scale with default settings. Stage 2 re-runs `LPC-4` on the frozen
eval-v2 and reports the failing cells, the power table and the R6 window.

## 6. Manifest plan

### 6.1 Digests for three streams

`dataset_digest` today covers transactions only (`emit.write_rows`), and `files` carries encoding-
dependent sha256s. eval-v2's manifest records `stream_digests`: for each of `tx.raw.v1`,
`identity.events.v1` and `device.events.v1`, a sha256 over canonical row JSON in emission order, plus
the row count. `dataset_digest` becomes the sha256 of the canonical JSON of
`{"scheme": "streams-v1", "streams": [{"topic", "digest", "rows"}, ...]}` sorted by topic, with
`dataset_digest_scheme: "streams-v1"` beside it. eval-v1's manifest is untouched. The per-topic digest
inside `write_rows` is routed by the lead to the Kafka agent; stage 2 consumes it.

### 6.2 Generator version

Stays `1.0.0` (lead decision). `PRODUCER` is written into every envelope; bumping it would break
eval-v1's bit-for-bit regeneration. Lineage: config digest including the block, seed, git SHA,
per-stream digests.

### 6.3 Exact config diff from eval-v1

eval-v1's config unchanged — seed, row count, population, window, fraud rate — plus one key:
`"baseline_identity": { … every §4 and §4b default … }`. The two datasets then differ in legitimate
identity and device activity, legitimate transaction devices and declines, and scenario sub-second
timing; the planted episodes, their overrides and their labels are the same.

### 6.4 Run manifest

Stage 2 produces `eval/manifest/gen-YYYYMMDD-eval-v2-<hash>.json` (`record_type: GENERATOR`) through
`GeneratorRunRecord`, with `dataset_digest_scheme`, `stream_digests`, per-topic row counts, generation
time and peak memory (lead decision, Q5), from a clean worktree; `eval/track_a/eval-v2.manifest.json`
is written from that run.

## 7. Requests to lead-owned files

- `docs/FRAUD_SCENARIOS.md`: baseline section, §2 and §4 updates, §3.10 correction, and the §3.6
  timing change (draft in `eval/track_a/drafts/fraud-scenarios-baseline-identity.md`).
- `docs/EVALUATION.md` §2: eval-v2, `dataset_digest_scheme`, `LPC-1`/`LPC-2` as the Q5 criteria.
- `tests/acceptance/status.json`: the `P3.eval-v2` evidence command after stage 2.

## 8. Decisions and open items

- **Q1 (decided):** fix T1–T3 under the gate; transaction rows may change; scenario definitions,
  signatures and keys may not; timing change recorded (§3.6).
- **Q2 (decided):** version `1.0.0`.
- **Q3 (decided):** side-event fixes only under the gate.
- **Q4:** superseded by T1.
- **Q5 (decided):** stage 2 records generation time and peak memory.
- **Q6 (decided):** the stage-1 demonstration stays in the fast lane; cost reduced without weakening R1.
- **Decided in stage 1c (lead):** T4, T5 and T6 below were renamed M1, M2 and M3 and are removed under
  the gate (§5c). The text below is kept as found.
- **Open, found during stage 1c (lead decision; not fixed).** A second gate-off probe of the unmodified
  plan (diagnostic, no `run_id`; same scale and seed) looked for remaining planted-row regularities:
  - **M4 — time of day.** Scenario start times are uniform over the day, while legitimate traffic
    follows the diurnal shape. 30 % of planted transactions fall at 00:00–05:59 UTC, against 4.1 % of
    legitimate ones. Some episodes are entirely nocturnal, because a whole device-farm or takeover
    episode inherits one start. No signature names a time of day.
  - **M5 — merchant popularity.** Scenarios draw merchants uniformly over the merchant list
    (`_unhabitual_merchant`, card testing, rings, collusion), while legitimate non-habitual spend
    follows Zipf popularity. Top-10-ranked merchants carry 8.8 % of fraudulent transactions and 15.4 %
    of legitimate ones. The signatures say "unhabitual" (§3.1, §3.9), not "unpopular".
  - **M6 — channel.** Takeover, card-testing, device-farm, collusion and credential-stuffing
    transactions are all forced to card-not-present: 64 % of fraudulent transactions against 44 % of
    legitimate ones. Only `IMPOSSIBLE_TRAVEL` documents a channel (card-present, §3.3). The
    device-driven scenarios are online by nature, so this one is partly semantic, not purely an
    artefact.
  - **Checked and not found:** exact amounts repeated on an account (legitimate 0.58 %, planted no
    higher), round amounts and last-digit distribution (flat for both), weekday (the differences come
    from a few episodes clustering), card-present entry modes (uniform in aggregate), and amounts far
    above an account's typical value, which appear only where a signature documents them
    (`ANOMALOUS_HIGH_VALUE`, `MERCHANT_COLLUSION`).
- **Found during stage 1b (now decided as M1–M3; kept as found).** A gate-off probe of the unmodified
  plan (diagnostic, no `run_id`; 120,000 transactions, seed 42) found planted-transaction regularities
  that no `LPC-2` signal judges:
  - **T4 — repeated exact coordinates.** Every `ACCOUNT_TAKEOVER` transaction in an episode carries
    the same `_geo(away)` point, so the account shows one exact coordinate repeated (45 of 45 takeover
    transactions; no legitimate transaction ever repeats an account's exact coordinate).
  - **T5 — exact home coordinates.** `IMPOSSIBLE_TRAVEL`'s first leg is `_geo(home)`, the account's
    home point itself (3 of 6 legs; no legitimate transaction lands exactly on it, because legitimate
    locations are jittered).
  - **T6 — entry-mode skew.** Scenario card-not-present overrides always set `ECOMMERCE` (74 % of
    fraudulent card-not-present transactions against about a third of legitimate ones). Enriched, not
    exclusive.

  T4 and T5 would be fixed like T2, by engine-level jitter of planted coordinates under the gate;
  T6 by drawing the entry mode for planted card-not-present rows. Each touches how planted overrides
  are applied, so each needs the same kind of lead decision Q1 gave for T1–T3.

## 9. Change and declaration log

1. **Stage 1 declaration.** §4 and §5 (`LPC-1`), before the first gated run.
2. **Stage 1 test-code fixes** (no rate, signal or rule touched): an expected count in the window
   self-test; the number of rate fields in the zero-rate helper.
3. **Stage 1b new declaration** (lead review, not a change to `LPC-1`): §4b rates, §3.6 timing and §5b
   `LPC-2`, written before the first run including T1–T3.
4. **Stage 1b changes to §3–§4, by lead decision Q1:**
   - removed `new_device_used_for_payment_share` and its home-device branch (replaced by T1);
   - new-device enrolment always picks a device new to the account (§3.2);
   - removed stage 1's bounded side-event jitter (replaced by §3.6);
   - lifted stage 1's transaction-invariance property (transaction rows may change under the gate).
5. **Integrity note (stage 1b, recorded rather than hidden).** The first stage-1b rewrite of this file
   carried `LPC-1`'s §5 in **abbreviated wording** — same signals, windows, statistics, thresholds and
   rules, but not the stage-1 text. A session interruption followed immediately. On resumption, before
   any code for T1–T3 existed and before any generation of any kind in stage 1b, §5 was **restored
   verbatim** from the stage-1 write. At the moment §4b, §3.6 and §5b were complete in this file, **no
   gated generation including T1–T3 had run** — the code for T1–T3 had not been written. The only
   stage-1b generations before that point were gate-off (eval-v1-shaped) probes.
6. **Stage 1b runs, after item 5.** The first generations including T1–T3 ran only after §4b, §3.6
   and §5b were complete and §5 was restored. No rate, signal, window, threshold or rule in §4, §4b,
   §5 or §5b changed after those runs. Outcome (diagnostic, not acceptance evidence): the eval-v2
   configuration passed `LPC-1` and `LPC-2`; the eval-v1 negative control failed both, and failed
   `LPC-2` for the declared proxy reason on each transaction signal; the zero-rate control failed both.
   The `CHANGE_THEN_NEW_DEVICE_24H` R4c margin disclosed in §4b was thin, as predicted.
7. **Stage 1c new declaration** (lead decision on markers M1–M3; not a change to `LPC-1` or `LPC-2`
   text): §5c — the M1–M3 corrections and `LPC-3` — written into this file **before any M1–M3 code
   existed and before any generation, gated or gate-off, in stage 1c**. Two departures from "R1–R3
   and a floor", declared there with reasons and flagged for the lead: the M2 absence alternative
   (R1′–R3′), and the M3 enrichment-parity rule R6. The documented-signature check for entry modes
   and locations was done before the declaration and found no signature needing an exception.
8. **Stage 1c runs, after item 7.** The first generations including M1–M3 ran only after §5c was
   complete and the M1–M3 code was written; the stage-1c gate-off probe (§8, M4–M6) also ran after the
   declaration. No rate, signal, threshold or rule in §4, §4b, §5, §5b or §5c changed after those runs.
   Outcome (diagnostic, not acceptance evidence): the eval-v2 configuration passed `LPC-1`, `LPC-2` and
   `LPC-3`; the eval-v1 negative control failed all three, and failed `LPC-3` for the declared proxy
   reason on each marker; the zero-rate control failed all three.
9. **Stage 1d declaration** (lead decision on the stage-1c review; not a change to `LPC-1`, `LPC-2` or
   `LPC-3` text). Written before any stage-1d code and before any stage-1d generation, gated or
   gate-off:
   - §5c.3, R6's sensitivity window, computed after the fact from the stage-1c diagnostic run. The
     bound stays 2.0.
   - §5d: the M4–M6 corrections and `LPC-4`.

   Flagged for review there: the per-scenario rule tests evidence of enrichment, not of parity, because
   parity is infeasible with few instances; attribute conditioning; and allowlist entries sourced from
   notes, causal keys or interpretation. The scenario catalogue lines were read, with line numbers,
   before the allowlist was written.

## 10. Implementation

| File | Role |
|---|---|
| `data/generator/config.py` | `BaselineIdentityConfig` (§4, §4b); `GeneratorConfig.baseline_identity`; omission when absent |
| `data/generator/baseline.py` | per-account identity and device plan, secondary devices (§3.1, §3.2, §3.5) |
| `data/generator/engine.py` | gate-on plan transformation, T1/T3 decisions, T2 timing, side-event semantics, decision time |
| `data/generator/label_proxy.py` | `LPC-1` and `LPC-2`, evaluation-side, one pass for both |
| `tests/unit/test_generator_baseline_identity.py` | gate-off byte identity; plan invariants under the gate; coherence; shape parity; rates |
| `tests/unit/test_eval_v2_label_proxy.py` | pinned thresholds; self-tests; stage-1b demonstration with both controls |
| `tests/unit/test_base_rate.py` | ordering compared as parsed milliseconds |

Not in stage 1b, by design: the CLI flag, per-stream digests, the eval-v2 manifest, any eval-v2
generation.
