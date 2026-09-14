# `LPC-5` — the eval-v2 label-proxy acceptance criterion

- **Status:** FROZEN, revision 1. Declared 2026-09-14, before any eval-v2 candidate dataset exists and
  before any code implementing this revision.
- **Freeze.** The commit that adds this file. Its sha256 is recorded in `docs/PROGRESS.md`. The
  acceptance command must refuse to run when this file's digest differs from the digest named in the
  eval-v2 manifest.
- **Decisions it records.**
  - The Stage 1d audit (`eval/track_a/audits/label-proxy-audit-stage-1d.md`) and the lead's review of it
    on 2026-09-14: the INTRINSIC_BEHAVIOURAL_SIGNAL allowlist, fully mechanical rules, and U7.
  - Authorization decisions reach features as their own dated events: ADR-0049.
- **What it replaces.** It replaces, as eval-v2's acceptance criterion, LPC-1 to LPC-4 of the eval-v2
  draft ADR.
  - Their rules are restated in §5, so this file is complete on its own.
  - The only change to them is §5.2, which re-expresses the two decline signals on the decision stream
    (U7).

---

## 0. Principle, verdicts, revisions

**Principle.** Eliminate generator-construction signal; never remove genuine behavioural fraud signal.

- **Generator-construction signal** is anything a row reveals about its label other than through a
  documented behaviour:
  - representation, identifiers and ordering;
  - fixed timing, precision and provenance;
  - selection and missingness;
  - differences in whether a feature can be computed;
  - undocumented enrichment.
- **Genuine behavioural signal** is an observable admitted to the allowlist (§6). Only the documented
  part of it may differ.
- **No model or rule performance.** No rule, threshold or entry here is justified by model or rule
  performance, and none is evaluated with it.

**Verdicts.**
- **Pass.** `LPC-5` passes if and only if every check in §5 and §7–§14 passes on the acceptance run
  (§1.2).
- **Pass/fail.** Every check is an executable pass/fail computation on the rows.
- **Invalid.** A check that cannot be computed makes the run **invalid**. Examples are a missing
  input, a digest mismatch, or a failed regeneration. An invalid run is recorded as invalid, never as
  a pass or a fail.

**Revision rule.**
1. **Numbered revisions only.** Any change — to thresholds, attribute definitions, allowlist entries,
   maps, pair kinds, generator rules or controls — is made by a numbered revision appended to §20,
   stating what changed and why.
2. **A fresh candidate after any revision.** A revision made after any eval-v2 candidate exists
   invalidates that candidate for acceptance. Acceptance then needs a fresh candidate generated after
   the revision.
3. **No tuning after the final candidate is seen.** A failure on the final candidate is recorded as a
   failure.
4. **Diagnostic runs are disclosed.** A revision that follows a diagnostic run (§1.3) says so and
   states what the run showed.

## 1. Scope and inputs

1. **Dataset under test.** The rows of one generation, in emission order, of four streams. The
   populations are named by stream:
   - **TX:** `tx.raw.v1`;
   - **ID:** `identity.events.v1`;
   - **DEV:** `device.events.v1`;
   - **OUT:** `tx.authorization.v1` (ADR-0049).
2. **Acceptance run.** The frozen eval-v2 candidate, regenerated in memory from its manifest.
   - **Digest check.** Before any check runs, every stream digest and row count must equal the
     manifest's; otherwise the run is invalid.
   - **Controls.** The eval-v1 control (§14.1) is regenerated from `eval/track_a/eval-v1.manifest.json`,
     and its transaction `dataset_digest` is verified. The other controls (§14.2–14.3) use the
     candidate's configuration and seed, changed only as each control states.
3. **Diagnostic runs.** Any other run: fast-lane scale, other seeds, probes. These are reported as
   diagnostic and are never acceptance evidence.
4. **Labels.** `GeneratedRow.label` and `GeneratedRow.scenario_instance` from the in-memory generation.
   They are never read from PostgreSQL and never passed to a runtime path (§16.5).
5. **Population knowledge `U`.** This is evaluation-side knowledge, not labels. It is taken from the
   generator's `Universe` for the run's configuration:
   - **Per account:** home devices, home IPs, home point, habitual merchants, `amount_mu`,
     `amount_sigma` and country.
   - **Per merchant:** popularity rank, MCC and country.
   - **Per IP and device:** each IP's datacenter flag and each device's platform.
   - **Window:** the configured window `[start_at, end_at)`.
   - **Typical amount:** an account's typical amount is the real number `exp(amount_mu)`.
   - **Habitual MCCs:** an account's habitual MCCs are the MCCs of its habitual merchants.
   - **Distance:** great-circle distance is the generator's `haversine_km`.

## 2. Rows, groups, clusters, time

1. **Planted rows** are:
   - a TX row labelled fraudulent;
   - an ID or DEV row that has a scenario instance;
   - an OUT row whose `transaction_id` is a planted TX row's.

   Every other row is **legitimate** (`LEGIT`). A planted row's scenario is its instance's `FraudPattern`.
2. **Groups.** Each population `P` has one group per scenario `s` (its planted rows in `P`), plus
   `POOLED` (all planted rows of `P`) and `LEGIT`.
3. **Clusters.**
   - A planted row's cluster is its scenario instance id.
   - A legitimate row's cluster is its payload `account_id`.
   - An OUT row takes its transaction's cluster.
4. **Time.**
   - `t(row)` is the envelope `occurred_at` as integer epoch milliseconds, with fractional
     milliseconds floored. An OUT row's `t` is its decision time.
   - Interval notation: `[a, b)` includes `a` and excludes `b`; `(a, b]` is the reverse.
5. **Earlier.** "Strictly earlier" means a smaller `t`; rows at an equal `t` are never earlier than one
   another.
   - A TX row's **previous transaction** is the account's TX row with the greatest `t` strictly
     earlier.
   - When several rows share that `t`, the one emitted last is taken.
6. **Conditional attributes.** `b@c` means attribute `b` evaluated separately within each value of
   attribute `c`. Every count in a check on `b@c` is restricted to rows with that value of `c`.

## 3. Statistics

1. **Wilson bounds.** For `x` rows having a value, among `r` rows in `k` distinct clusters, with
   `z = 1.645`:

   ```
   if r == 0 or k == 0:  (lo, hi) = (0.0, 1.0)
   p = x / r;  n = k
   centre = (p + z²/(2n)) / (1 + z²/n)
   half   = z · sqrt(p·(1 − p)/n + z²/(4n²)) / (1 + z²/n)
   (lo, hi) = (max(0, centre − half), min(1, centre + half))
   ```

   - The point estimate uses rows and the width uses clusters. This is `wilson_interval` of the
     Stage 1 implementation.
   - Arithmetic is IEEE-754 double. Comparisons are strict as written, with no rounding.
2. **Cell quantities.** For a population `P`, an attribute `a` (within its stratum), a value set `V`
   and a group `g`:
   - `r_g` is the number of rows of `g`; `x_g` is how many of them have `a ∈ V`; `k_g` is the number of
     distinct clusters among the `r_g` rows;
   - `(lo_g, hi_g) = Wilson(x_g, r_g, k_g)`;
   - `hiF = max(hi_LEGIT, 0.001)`.
3. **Named tests.**
   - `ENRICHED(g)` holds when `lo_g > 2.0 × hiF` **and** `lo_g − hiF > 0.02`.
   - `DIFFERS(g, δ)` holds when `lo_g > hi_LEGIT + δ` **or** `hi_g < lo_LEGIT − δ`.
   - `SUPPORTED(V)` holds when the legitimate rows with `a ∈ V` meet all three conditions:
     - they number ≥ 30;
     - they come from ≥ 20 distinct accounts;
     - they are ≥ 0.001 of `r_LEGIT`.
   - `PRECISION_HI(g, V)` is the `hi` of `Wilson(x_g, x_g + x_LEGIT, c)`, where `c` is the number of
     distinct clusters among those `x_g + x_LEGIT` rows.

## 4. Attributes

### 4.1 Classes

- **B — behaviour-eligible.** What an account, card, device, IP or merchant did or is. B is the only
  class an allowlist entry may name.
- **R — representation.** How the implementation encodes, identifies, timestamps, orders, correlates or
  attributes a row; which fields the row carries; and its provenance.
  - Never allowlisted, never a consequence, never judged conditionally.
  - Checked only by §8.
- **V — availability.** Whether a released feature can be computed for a TX row (§4.6).
  - Never allowlisted.
  - A consequence only through §4.7.

**Common R attributes (every population).**

| Attribute | Definition | Values |
|---|---|---|
| `in_window` | `t ∈ [start_at, end_at)` | `inside`, `outside` |
| `subsecond` | `t mod 1000` | `whole` (0), `fractional` |
| `timestamp_format` | the envelope `occurred_at` and `ingested_at` strings | `ms-z` if both match `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`, else `other` |
| `ingest_lag` | `ingested_at − occurred_at`, ms | `<0`, `[0,40)`, `[40,80)`, `[80,120)`, `[120,160)`, `≥160` |
| `tie_rank` | among all rows of all four streams with this row's `t`, by emission order | `alone`, `first`, `later` |
| `payload_keys` (`payload_keys@event_type` in ID and DEV) | the sorted payload key set | observed sets |
| `identifier_formats` | every identifier matches its released pattern; `transaction_id` has 1–64 characters | `ok`, `not` |
| `envelope_constants` | `(event_type, schema_version, producer)` | observed triples |
| `envelope_unique` | `event_id` and `idempotency_key` are shared with no other row of any stream | `unique`, `shared` |
| `event_id_time` | the UUIDv7 timestamp of `event_id` equals `t` | `equal`, `not` |
| `correlation_shape`, `trace_shape` | the id with every maximal run of `[A-Za-z0-9]` replaced by `x` | observed shapes |
| `correlation_members`, `trace_members` | rows of all four streams carrying this id | `1`, `2`, `3+` |

OUT rows are not judged on `in_window`. A decision can fall after `end_at` for a transaction at the very
end of the window, whatever its label.

### 4.2 TX attributes

**Time and payment.**

| Attribute | Class | Definition | Values |
|---|---|---|---|
| `hour` | B | the UTC hour of `t` | `0`…`23` |
| `daypart` | B | UTC hour band | `00-05`, `06-11`, `12-17`, `18-23` |
| `weekday` | B | the UTC weekday of `t` | `Mon`…`Sun` |
| `channel` | B | payload | released values |
| `entry_mode@channel` | B | payload | released values |
| `currency` | B | payload | observed |
| `amount_vs_account` | B | `amount_minor ÷ typical` | `<0.1`, `[0.1,0.5)`, `[0.5,2)`, `[2,5)`, `[5,20)`, `≥20` |
| `amount_decile` | B | how many edges are ≤ `amount_minor`. The edges are the nearest-rank 10th…90th percentiles of `amount_minor` over LEGIT TX rows | `0`…`9` |
| `amount_z` | B | `(ln amount_minor − amount_mu) ÷ amount_sigma` | `<-2`, `[-2,-1)`, `[-1,-0.5)`, `[-0.5,0)`, `[0,0.5)`, `[0.5,1)`, `[1,2)`, `≥2`; `nonpositive` when `amount_minor ≤ 0` |
| `amount_last_digit` | R | `abs(amount_minor) mod 10` | `0`…`9` |
| `amount_roundness` | R | the most specific value that holds | `x1000`, `x100`, `x10`, `other` |
| `outcome_field` | R | payload `authorization_outcome` | released values |
| `decision` | B | the `decision` of this row's OUT row | `APPROVED`, `DECLINED`, `none` |
| `card` | B | `card_id` is the account's first card in `U` | `first`, `other` |
| `user_agent` | B | payload | released values, `absent` |
| `memo` | B | payload | `absent-or-empty`, `non-empty` |

**Merchant.**

| Attribute | Class | Definition | Values |
|---|---|---|---|
| `merchant_habitual` | B | `merchant_id` is one of the account's habitual merchants | `habitual`, `unhabitual` |
| `merchant_popularity@merchant_habitual` | B | the merchant's popularity rank | `1`, `2-3`, `4-10`, `11-30`, `31-100`, `101+` |
| `merchant_mcc` | B | payload | observed |
| `mcc_habitual` | B | `merchant_mcc` is one of the account's habitual MCCs | `habitual`, `unhabitual` |
| `merchant_country` | B | payload | observed |
| `merchant_country_home` | B | equals the account's country | `home`, `other` |
| `merchant_accounts_1h` | B | distinct accounts among TX rows at this merchant with `t' ∈ (t − 1 h, t]` | `1-4`, `5-19`, `20+` |
| `merchant_amount_cv_24h` | B | over TX rows at this merchant, in this currency, with `t' ∈ (t − 24 h, t]`: population standard deviation ÷ mean of `amount_minor` | `n<2`, `<0.05`, `[0.05,0.2)`, `≥0.2` |
| `merchant_high_amount_cv_24h` | B | `not-high` when this row's `amount_decile` is not `9`. Otherwise, over TX rows at this merchant, in this currency, with `t' ∈ (t − 24 h, t]` and `amount_decile` `9`: `n<5` when fewer than 5, else population standard deviation ÷ mean | `not-high`, `n<5`, `<0.05`, `[0.05,0.2)`, `≥0.2` |
| `shared_merchant_link` | B | at least 2 other accounts that share a `device_id` or `ip_id` with this account (in any TX rows of the dataset) also have a TX row at this merchant within 7 days of `t`, on either side | `yes`, `no` |

**Location.**

| Attribute | Class | Definition | Values |
|---|---|---|---|
| `distance_home` | B | km from the account's home point | `<5`, `[5,25)`, `[25,100)`, `[100,500)`, `≥500` |
| `location_novel` | B | the minimum km to any strictly earlier TX row of the account | `no-prior`, `<25`, `[25,100)`, `[100,500)`, `≥500` |
| `leg_speed` | B | km ÷ hours to the previous transaction, when that transaction is within 24 h. Elapsed 0 with distance > 0 is `≥1000`; elapsed 0 with distance 0 is `<100` | `none`, `<100`, `[100,500)`, `[500,1000)`, `≥1000` |
| `coordinate_decimals` | R | the most decimal places of `latitude` or `longitude` in the JSON text | `≤2`, `3-5`, `≥6` |
| `coordinate_repeat` | R | the exact `(latitude, longitude)` of a strictly earlier TX row of the account | `repeat`, `new` |
| `coordinate_home` | R | exactly the account's home point | `exact`, `not` |

**Velocity, history and decisions.**

| Attribute | Class | Definition | Values |
|---|---|---|---|
| `gap_prev` | B | `t` minus the previous transaction's `t` | `none`, `<10s`, `[10s,60s)`, `[1,10)min`, `[10,60)min`, `[1,24)h`, `≥24h` |
| `tx_count_1m`, `tx_count_5m`, `tx_count_1h`, `tx_count_24h` | B | the account's TX rows with `t' ∈ (t − W, t]`, this row included | `1`, `2`, `3-4`, `5-9`, `10-19`, `20+` |
| `card_count_5m` | B | the same count by `card_id`, with W = 5 min | as above |
| `distinct_merchants_1h` | B | distinct `merchant_id` among the account's TX rows in `(t − 1 h, t]` | `1`, `2`, `3-4`, `5-9`, `10+` |
| `distinct_mcc_5m` | B | distinct `merchant_mcc` in `(t − 5 min, t]` | `1`, `2`, `3-4`, `5+` |
| `distinct_devices_24h`, `distinct_countries_24h` | B | distinct `device_id`, and distinct `merchant_country`, in `(t − 24 h, t]` | `1`, `2`, `3+` |
| `prior_decisions_1h` | B | the account's OUT rows with `t' ∈ (t − 1 h, t)` and a `transaction_id` other than this row's | `0`, `1`, `2-4`, `5-9`, `10+` |
| `prior_declined_share_1h` | B | the `DECLINED` share among those rows | `none`, `0`, `(0,0.4)`, `≥0.4` |
| `profile_depth` | B | the account's TX rows strictly earlier | `0`, `1-2`, `3-19`, `20-127`, `128+` |
| `account_activity` | B | the account's TX rows in the dataset | `1-10`, `11-20`, `21-30`, `31-50`, `51+` |

**Devices and IPs.**

| Attribute | Class | Definition | Values |
|---|---|---|---|
| `device_home` | B | `device_id` is one of the account's home devices | `home`, `not` |
| `device_age` | B | time since the account's first TX row on this device | `first-use` (this row), `<1h`, `[1h,24h)`, `[1d,7d)`, `≥7d` |
| `device_account_tx` | B | the account's TX rows on this device in the dataset | `1`, `2`, `3-5`, `6+` |
| `device_accounts` | B | distinct accounts with a TX row on the device in the dataset | `1`, `2`, `3-5`, `6+` |
| `device_accounts_24h` | B | distinct accounts among TX rows on the device in `(t − 24 h, t]` | `1`, `2`, `3-4`, `5+` |
| `ip_home` | B | `ip_id` is one of the account's home IPs | `home`, `not` |
| `ip_accounts` | B | distinct accounts with a TX row on the IP in the dataset | `1`, `2`, `3-5`, `6+` |
| `ip_accounts_1h` | B | distinct accounts among TX rows on the IP in `(t − 1 h, t]` | `1`, `2`, `3-4`, `5-9`, `10+` |
| `ip_datacenter` | B | the IP's datacenter flag | `datacenter`, `not` |
| `ip_login_accounts_1h` | B | distinct accounts with a `LOGIN_FAILED` or `LOGIN_SUCCEEDED` ID row from this `ip_id` in `[t − 1 h, t)` | `0`, `1`, `2-4`, `5+` |
| `joint_link` | B | another account shares both a `device_id` and an `ip_id` with this account, in TX rows of the dataset | `yes`, `no` |

**Identity context.** The Q4e set is `PASSWORD_CHANGE`, `EMAIL_CHANGE`, `PHONE_CHANGE`,
`ADDRESS_CHANGE` and `MFA_RESET`.

| Attribute | Class | Definition | Values |
|---|---|---|---|
| `prior_events_24h` | B | the account's ID and DEV rows in `[t − 24 h, t)`, most specific first | `identity-change` (a Q4e type) > `device-event` > `failed-login` > `other-identity` > `none` |
| `hours_since_identity_change` | B | the latest Q4e ID row of the account in `[t − 24 h, t)` | `none`, `<1h`, `[1h,6h)`, `[6h,24h)` |
| `failed_logins_1h` | B | the account's `LOGIN_FAILED` ID rows in `[t − 1 h, t)` | `0`, `1-4`, `5-19`, `20+` |

### 4.3 ID attributes

| Attribute | Class | Definition | Values |
|---|---|---|---|
| `event_type` | B | `identity_event_type` | released values |
| `hour`, `daypart`, `weekday` | B | as for TX | as for TX |
| `user_agent` | B | payload | released values, `absent` |
| `ip_datacenter` | B | as for TX | `datacenter`, `not`, `absent` |
| `ip_login_accounts` | B | distinct accounts with a `LOGIN_FAILED` or `LOGIN_SUCCEEDED` ID row from this `ip_id` in the dataset | `absent`, `1`, `2`, `3-5`, `6+` |
| `device_home` | B | as for TX | `home`, `not`, `absent` |
| `device_age` | B | time since the account's first reference to the device in any stream, this row included | `first-reference`, `<1h`, `[1h,24h)`, `[1d,7d)`, `≥7d`, `absent` |
| `device_login_accounts` | B | distinct accounts with an ID row carrying this `device_id` in the dataset | `absent`, `1`, `2`, `3-5`, `6+` |

Plus the common R attributes, with `payload_keys@event_type`.

### 4.4 DEV attributes

| Attribute | Class | Definition | Values |
|---|---|---|---|
| `event_type` | B | `device_event_type` | released values |
| `hour`, `daypart`, `weekday` | B | as for TX | as for TX |
| `platform` | B | payload | observed |
| `device_home` | B | as for TX | `home`, `not` |
| `device_age` | B | as for ID | as for ID, without `absent` |
| `platform_consistent` | R | equals the device's platform in `U` | `equal`, `not` |

Plus the common R attributes, with `payload_keys@event_type`.

### 4.5 OUT attributes

| Attribute | Class | Definition | Values |
|---|---|---|---|
| `decision` | B | payload | released values |
| `hour`, `daypart`, `weekday` | B | as for TX | as for TX |
| `decision_latency` | R | `t(OUT) − t(its TX row)`, ms | `<0`, `[0,40)`, `[40,100)`, `[100,250)`, `[250,500)`, `[500,1000)`, `≥1000` |
| `tx_link` | R | exactly one TX row has this `transaction_id` | `one`, `not` |
| `account_match` | R | `account_id` equals its TX row's | `equal`, `not` |
| `transaction_time_match` | R | payload `transaction_occurred_at` equals its TX row's envelope `occurred_at` string | `equal`, `not` |

Plus the common R attributes.

### 4.6 Availability attributes (V, TX only)

`avail:<feature>` takes the values `AVAILABLE`, `INSUFFICIENT_HISTORY` or `UNAVAILABLE`.

**How it is computed.**
- **Implementation:** `trace_core.features.reference`.
- **Mode:** `EVENT_TIME_COMPLETE`, with `complete_since = start_at`.
- **Inputs:** the four streams, mapped as the gateway maps them (ADR-0046 §1 and §4; ADR-0049 §5).
- **Feature set:** version `3.0.0`.

**The 26 released features.**
- `account_tx_count_1m`, `account_tx_count_5m`, `account_tx_count_1h`, `account_tx_count_24h`;
- `account_amount_sum_1h`, `card_tx_count_5m`, `declined_ratio_1h`;
- `account_distinct_merchants_1h`, `account_distinct_mcc_5m`, `account_distinct_devices_24h`,
  `account_distinct_countries_24h`;
- `device_distinct_accounts_24h`, `ip_distinct_accounts_1h`, `merchant_distinct_accounts_1h`,
  `merchant_amount_cv_24h`;
- `amount_zscore_vs_account`, `account_tenure_days`, `merchant_is_habitual`,
  `mcc_is_habitual_for_account`, `device_is_known_for_account`, `distance_from_account_home_km`;
- `geo_distance_from_last_km`, `implied_speed_kmh_from_last`, `seconds_since_last_transaction`;
- `hours_since_identity_change`, `failed_logins_1h`.

A feature added later is not part of revision 1.

### 4.7 `DEPENDS`: the attribute each availability attribute follows

| Features | `DEPENDS` |
|---|---|
| `account_tx_count_1m` / `_5m` / `_1h` / `_24h` | `tx_count_1m` / `_5m` / `_1h` / `_24h` |
| `account_amount_sum_1h` | `tx_count_1h` |
| `card_tx_count_5m` | `card_count_5m` |
| `declined_ratio_1h` | `prior_decisions_1h` |
| `account_distinct_merchants_1h` | `distinct_merchants_1h` |
| `account_distinct_mcc_5m` | `distinct_mcc_5m` |
| `account_distinct_devices_24h` | `distinct_devices_24h` |
| `account_distinct_countries_24h` | `distinct_countries_24h` |
| `device_distinct_accounts_24h` | `device_accounts_24h` |
| `ip_distinct_accounts_1h` | `ip_accounts_1h` |
| `merchant_distinct_accounts_1h` | `merchant_accounts_1h` |
| `merchant_amount_cv_24h` | `merchant_amount_cv_24h` |
| `amount_zscore_vs_account`, `account_tenure_days`, `merchant_is_habitual`, `mcc_is_habitual_for_account`, `device_is_known_for_account`, `distance_from_account_home_km` | `profile_depth` |
| `geo_distance_from_last_km`, `implied_speed_kmh_from_last`, `seconds_since_last_transaction` | `gap_prev` |
| `hours_since_identity_change` | `hours_since_identity_change` |
| `failed_logins_1h` | `failed_logins_1h` |

`avail:f` is a consequence of an allowlist row `e` (§6.3) exactly when `DEPENDS(f)` is `e`'s attribute or
one of `e`'s declared consequences. Otherwise its status is ORDINARY.

## 5. S0 — the inherited rules

### 5.1 LPC-1: identity and device presence

Windows are `[t − W, t)`, and account attribution uses the payload `account_id`.

**Presence signals.** Each fires on a TX row when the account has, in its window:

| Signal | Fires on | W | Floor on `ℓ_s` |
|---|---|---|---|
| `ANY_IDENTITY_24H` | any ID row | 24 h | 0.05 |
| `LOGIN_FAILED_1H` | a `LOGIN_FAILED` | 1 h | 0.0001 |
| `IDENTITY_CHANGE_24H` | a Q4e ID row | 24 h | 0.001 |
| `DEVICE_FIRST_SEEN_24H` | a DEV `FIRST_SEEN` | 24 h | 0.001 |
| `ANY_DEVICE_EVENT_24H` | any DEV row | 24 h | 0.001 |
| `ANY_IDENTITY_OR_DEVICE_24H` | any ID or DEV row | 24 h | 0.05 |

**Patterns.**

| Pattern | Refines | Fires when |
|---|---|---|
| `STUFFING_BURST_1H` | `ANY_IDENTITY_24H` | ≥ 5 distinct accounts have a `LOGIN_FAILED` or `LOGIN_SUCCEEDED` from the TX row's own `ip_id` in `[t − 1 h, t)` |
| `CHANGE_THEN_NEW_DEVICE_24H` | `IDENTITY_CHANGE_24H` | `IDENTITY_CHANGE_24H` fires, and the TX row's `device_id` is first referenced on this account (in any stream, this row included) at or after `t − 24 h` |

**Statistics.** For a signal `s`:
- **Counts:** `n_s` firing TX rows, of which `F_s` are fraudulent and `L_s` legitimate.
- **Precision:** `p_s = F_s / n_s`, with the Wilson interval taken over the distinct clusters among the
  firing rows.
- **Legitimate share:** `ℓ_s = L_s / L`, where `L` is all legitimate TX rows.
- **Base rate:** `π = F / n` over all TX rows.

**Rules.**

| Rule | Condition |
|---|---|
| R1 | each presence signal has `L_s ≥ 30` |
| R2 | each presence signal has `ℓ_s ≥` its floor |
| R3 | each presence signal has `hi(p_s) ≤ 0.25` |
| R4a | each pattern has `F_p ≥ 5` |
| R4b | each pattern has `lo(p_p) ≥ 10π` |
| R4c | each pattern has `lo(p_p) ≥ 3 × hi(p_base)`, where `p_base` is the presence signal it refines |

### 5.2 LPC-2: transaction presence, with the decisions re-expressed for U7

**Home devices** are the account's `home_devices` in `U`. Each signal fires on a TX row at `t` when:

| Signal | Fires when | Floor on `ℓ_s` |
|---|---|---|
| `TX_NON_HOME_DEVICE` | `device_id` is not a home device | 0.01 |
| `TX_DEVICE_FIRST_USED_24H` | `device_id` is not a home device, and the account's earliest TX row on it (this one included) is at or after `t − 24 h` | 0.001 |
| `TX_WHOLE_SECOND` | `t mod 1000 = 0` | 0.0005 |
| `TX_DECLINED` | **its OUT row's `decision` is `DECLINED`** | 0.005 |
| `TX_DECLINED_PRIOR_1H` | **the account has an OUT row with `decision = DECLINED`, `t' ∈ [t − 1 h, t)`, and another `transaction_id`** | 0.0005 |

**Rules.** R1, R2 and R3 of §5.1 apply to each signal.

**The U7 change.** The two signals in bold read the decision stream. `tx.raw.v1` carries no decision
(ADR-0049 §3). The thresholds are unchanged.

### 5.3 LPC-3: coordinates and entry mode

| Signal | Fires on a TX row at `t` when | Floor on `ℓ_s` |
|---|---|---|
| `TX_REPEATED_EXACT_COORDINATES` (M1) | its exact `(latitude, longitude)` equals that of a strictly earlier TX row of the account | 0.001 |
| `TX_EXACT_HOME_POINT` (M2) | its exact `(latitude, longitude)` equals the account's home point | 0.0001 |
| `TX_CNP_ECOMMERCE` (M3) | `channel = CARD_NOT_PRESENT` and `entry_mode = ECOMMERCE` | 0.05 |

**Rules per signal.**
- **M1:** R1, R2 and R3.
- **M2 (R1′–R3′):** passes if no fraudulent TX row fires it, or if R1, R2 and R3 all hold.
- **M3:** R1, R2 and R3, plus **R6**. Among `CARD_NOT_PRESENT` TX rows, `hi` of the fraudulent firing
  share must be ≤ 2.0 × `lo` of the legitimate firing share. Clusters are instances for fraudulent rows
  and accounts for legitimate rows.

### 5.4 LPC-4: enrichment over the attribute sweep

**The allowlist here is §6's.** Statuses (ALLOWLISTED, CONSEQUENCE, ORDINARY) are defined in §6.3.

- **R7 — per-scenario enrichment.**
  - For every population `P`, attribute `b` of class B, R or V (within its stratum), value `v`, and
    scenario `s` for which `v` is ORDINARY (§6.3): the cell fails if `ENRICHED(s)`.
  - R attributes are always ORDINARY.
- **R8 — pooled enrichment.**
  - The same test with `g = POOLED`.
  - Exempt attributes: any attribute that is ALLOWLISTED or CONSEQUENCE for some scenario contributing
    more than 0.10 of `P`'s planted rows.
- **R9 — non-vacuity.** Fails if any of these holds:
  - a population has fewer than 30 legitimate rows or fewer than 30 legitimate clusters;
  - TX has fewer than 30 planted clusters.

  The per-scenario count requirement is §14.4.

## 6. The INTRINSIC_BEHAVIOURAL_SIGNAL allowlist

### 6.1 Qualification

**The five checks.** An entry qualifies only if all five hold.

1. **Documented mechanism.** It is explicitly part of the scenario's documented causal mechanism.
2. **Real-system meaning.** It would be meaningful if a real system produced it.
3. **Not an artefact.** It is not metadata, provenance, formatting, precision, ID construction,
   ordering, fixed generator timing or another implementation artefact. Its attribute must be class B.
4. **Explicit citation.** `docs/FRAUD_SCENARIOS.md` cites it explicitly.
   - The source fragments must appear verbatim in the scenario's §3 subsection. Matching removes
     Markdown emphasis markers and collapses whitespace.
   - A causal-key source cites the key token, and is admissible only through the fixed map below.
5. **Regression-tested.** A regression test shows it carries no extra information beyond the signal:
   §7 S1-B, together with §13 S7b.

Model or rule performance is never an argument for an entry.

**The causal-key map.** These are the only admissible causal-key sources.

| Key | Attributes and values it can admit |
|---|---|
| `DEVICE_SHARING` | `device_accounts` ∈ {`2`,`3-5`,`6+`}; ID `device_login_accounts` ∈ {`2`,`3-5`,`6+`}; `joint_link = yes`, only together with an `S` fragment that cites shared IPs |
| `DEVICE_NOVELTY` | `device_home = not`; `device_age` ∈ {`first-use`/`first-reference`, `<1h`, `[1h,24h)`} |
| `IP_REPUTATION` | `ip_datacenter = datacenter` |
| `MCC_ANOMALY` | `mcc_habitual = unhabitual` |
| `GEO_DISPERSION` | `location_novel` ∈ {`[100,500)`, `≥500`} |

The keys `VELOCITY`, `AMOUNT_ANOMALY`, `SPEND_PROFILE`, `IDENTITY_CHANGE`, `GRAPH_CLUSTER`,
`RING_SCORE`, `LINK_PATH`, `MERCHANT_RISK` and `MERCHANT_PATTERN` admit nothing. Each is either
ambiguous between row attributes or maps to no row attribute.

### 6.2 Never exempt

No entry, consequence or control may exempt any of the following:
- scenario or source identifiers;
- label information;
- payload-shape differences;
- formatting;
- timestamp precision;
- correlation-id or trace-id construction;
- generator-only ordering;
- scenario-specific missingness of a field;
- implementation-only fixed offsets;
- provenance fields.

They are the R attributes (§4) and the exact rules of §8 S2c, and they are checked for every group.

### 6.3 Entry semantics

**Row fields.** An entry row `e = (s, P, a, V*)` also carries:
- **Exempt values** (a subset of `V*`), of two kinds:
  - `rare`: S1-U's support check drops only its 0.001 share condition, so at least 30 legitimate rows
    from 20 accounts must still exist. Declared where legitimate customers plausibly do this less
    often than once in a thousand transactions.
  - `none`: S1-U's support check is skipped. Declared only where the documented behaviour is defined as
    beyond legitimate activity, or a legitimate count at that level is not plausible for a consumer
    account.
- **Composition,** either `judged` or `documented`.
  - `judged`: the mix of values inside `V*` must match legitimate rows (§7 S1-B(iii)).
  - `documented`: the quoted text determines the mix.
- **`E_min`:** the least lower bound the documented effect must reach (§13 S7b).
- **Consequences:** attributes of `P` judged only within `e`'s stratum (§7 S1-B), never unconditionally.

**Status, for a scenario `s` and a value `v` of an attribute `b` of `P`.**
- **ALLOWLISTED** if some row of `s` names `b` in `P` with `v ∈ V*`.
- **CONSEQUENCE(e)** if row `e` of `s` lists `b`, or through §4.7. Every value of `b` then has this
  status.
- **ORDINARY** otherwise — including the values of an allowlisted attribute that lie outside `V*`.

A declaration test rejects an attribute that a row of `s` names and that is also a consequence for `s`.

**Effect.**
- ALLOWLISTED values skip S1-U's precision check and R7. R8 exempts their attribute through its own
  rule.
- CONSEQUENCE values skip S1-U's precision check and R7. They are judged for enrichment only in S1-B.
- Every value keeps S1-U's support check, unless it is an exempt value of a row of `s`.

### 6.4 Entries

**How to read the tables.**
- Source `S` is the §3 signature line; `N` is a note in the subsection.
- `K` is a causal-key token, admissible through §6.1's map.
- **Exempt** lists the `rare` and `none` values (§6.3).
- A bracketed `[review]` marks an entry that rests on an implication of its fragment. It is listed in §18
  for confirmation.

**`ACCOUNT_TAKEOVER` (§3.1).**

| # | P | Attribute | V* | Exempt | Composition | E_min | Source | Consequences |
|---|---|---|---|---|---|---|---|---|
| ATO-1 | TX | `prior_events_24h` | `identity-change` | — | — | 0.80 | S "An identity change, then within hours" | — |
| ATO-2 | TX | `hours_since_identity_change` | `<1h`, `[1h,6h)` | rare: `<1h`, `[1h,6h)` | documented | 0.80 | S "then within hours" | — |
| ATO-3 | ID | `event_type` | the Q4e set | — | judged | 0.80 | S "An identity change" | — |
| ATO-4 | TX, ID | `device_home` | `not` | — | — | 0.80 | S "a device the account has never used" | TX: `device_accounts`, `device_accounts_24h`, `distinct_devices_24h`, `device_account_tx`; ID: `device_login_accounts` |
| ATO-5 | TX, ID | `device_age` | `first-use`/`first-reference`, `<1h`, `[1h,24h)` | — | judged | 0.80 | S "a device the account has never used" | — |
| ATO-6 | TX | `amount_vs_account` | `[2,5)`, `[5,20)`, `≥20` | rare: `≥20` | judged | 0.80 | S "spending well above profile" | `amount_decile`, `amount_z` |
| ATO-7 | TX | `merchant_habitual` | `unhabitual` | — | — | 0.80 | S "at unhabitual merchants" | `mcc_habitual`, `merchant_mcc` |
| ATO-8 | TX | `distance_home` | `[100,500)`, `≥500` | — | judged | 0.80 | S "away from home" | `location_novel`, `leg_speed`, `merchant_country_home`, `distinct_countries_24h` |

**`CARD_TESTING` (§3.2).**

| # | P | Attribute | V* | Exempt | Composition | E_min | Source | Consequences |
|---|---|---|---|---|---|---|---|---|
| CT-1 | TX | `tx_count_1m` | `2`, `3-4`, `5-9`, `10-19`, `20+` | rare: `3-4`; none: `5-9`, `10-19`, `20+` | documented | 0.30 | S "Many sub-threshold authorisations" + "inside a few minutes" | — |
| CT-2 | TX | `tx_count_5m` | `2`, `3-4`, `5-9`, `10-19`, `20+` | rare: `5-9`; none: `10-19`, `20+` | documented | 0.60 | as CT-1 | — |
| CT-3 | TX | `tx_count_1h` | `2`, `3-4`, `5-9`, `10-19`, `20+` | rare: `10-19`; none: `20+` | documented | 0.80 | as CT-1 | `tx_count_24h` |
| CT-4 | TX | `card_count_5m` | as CT-2 | as CT-2 | documented | 0.60 | as CT-1 | — |
| CT-5 | TX | `gap_prev` | `<10s`, `[10s,60s)`, `[1,10)min` | — | judged | 0.50 | S "inside a few minutes" | — |
| CT-6 | TX | `distinct_merchants_1h` | `2`, `3-4`, `5-9`, `10+` | rare: `5-9`; none: `10+` | documented | 0.60 | S "across many distinct merchants" | `merchant_habitual`, `merchant_popularity@merchant_habitual`, `merchant_accounts_1h` |
| CT-7 | TX | `distinct_mcc_5m` | `2`, `3-4`, `5+` | rare: `3-4`; none: `5+` | documented | 0.40 | S "many distinct merchants and MCCs" + "inside a few minutes" | `merchant_mcc`, `mcc_habitual` |
| CT-8 | TX | `amount_vs_account` | `<0.1`, `[0.1,0.5)` | — | documented | 0.60 | S "Many sub-threshold authorisations" | `amount_decile`, `amount_z` |
| CT-9 | OUT | `decision` | `DECLINED` | — | — | 0.20 | S "a substantial share declined" | — |
| CT-10 | TX | `prior_decisions_1h` | `1`, `2-4`, `5-9`, `10+` | rare: `5-9`; none: `10+` | documented | 0.60 | as CT-1 | — |
| CT-11 | TX | `prior_declined_share_1h` | `0`, `(0,0.4)`, `≥0.4` | — | documented | 0.60 | S "a substantial share declined" | — |

**`IMPOSSIBLE_TRAVEL` (§3.3).**

| # | P | Attribute | V* | Exempt | Composition | E_min | Source | Consequences |
|---|---|---|---|---|---|---|---|---|
| IT-1 | TX | `channel` | `CARD_PRESENT` | — | — | 0.80 | S "Two card-present transactions" | — |
| IT-2 | TX | `leg_speed` | `[500,1000)`, `≥1000` | none: `≥1000` | documented | 0.30 | S "implies a speed no commercial travel achieves" | `gap_prev` |
| IT-3 | TX | `location_novel` | `[100,500)`, `≥500` | — | judged | 0.30 | S "great-circle distance" + K "`GEO_DISPERSION`" | `distance_home`, `merchant_country_home`, `distinct_countries_24h` |

**`VELOCITY_ATTACK` (§3.4).** Every row has the source S "A burst of transactions on one account far
above its own short-window baseline".

| # | P | Attribute | V* | Exempt | Composition | E_min | Consequences |
|---|---|---|---|---|---|---|---|
| VA-1 | TX | `tx_count_1m` | `2`, `3-4`, `5-9`, `10-19`, `20+` | rare: `3-4`; none: `5-9`, `10-19`, `20+` | documented | 0.30 | — |
| VA-2 | TX | `tx_count_5m` | `2`, `3-4`, `5-9`, `10-19`, `20+` | rare: `5-9`; none: `10-19`, `20+` | documented | 0.60 | — |
| VA-3 | TX | `tx_count_1h` | `2`, `3-4`, `5-9`, `10-19`, `20+` | rare: `10-19`; none: `20+` | documented | 0.80 | `tx_count_24h` |
| VA-4 | TX | `card_count_5m` | as VA-2 | as VA-2 | documented | 0.60 | — |
| VA-5 | TX | `gap_prev` | `<10s`, `[10s,60s)`, `[1,10)min` | — | judged | 0.50 | — |
| VA-6 | TX | `prior_decisions_1h` | `1`, `2-4`, `5-9`, `10+` | rare: `5-9`; none: `10+` | documented | 0.60 | `prior_declined_share_1h` |

**`DEVICE_FARM` (§3.5).**

| # | P | Attribute | V* | Exempt | Composition | E_min | Source | Consequences |
|---|---|---|---|---|---|---|---|---|
| DF-1 | TX | `device_accounts` | `3-5`, `6+` | — | documented | 0.80 | S "One device fingerprint used by many accounts" | — |
| DF-2 | TX | `device_accounts_24h` | `3-4`, `5+` | rare: `5+` | documented | 0.50 | as DF-1 | — |
| DF-3 | TX | `device_account_tx` | `1`, `2` | — | documented | 0.80 | S "each account transacting only once or twice" | — |
| DF-4 | TX | `device_home` | `not` | — | — | 0.80 | K "`DEVICE_NOVELTY`" | `distinct_devices_24h` |
| DF-5 | TX | `device_age` | `first-use`, `<1h` | — | judged | 0.80 | K "`DEVICE_NOVELTY`" | — |

**`FRAUD_RING` (§3.6).**

| # | P | Attribute | V* | Exempt | Composition | E_min | Source | Consequences |
|---|---|---|---|---|---|---|---|---|
| FR-1 | TX | `device_accounts` | `3-5`, `6+` | — | judged | 0.60 | S "Several accounts sharing a small pool of devices and IPs" | `device_home`, `device_age`, `device_accounts_24h`, `distinct_devices_24h`, `device_account_tx` |
| FR-2 | TX | `ip_accounts` | `6+` | — | — | 0.50 | as FR-1 | `ip_home`, `ip_accounts_1h` |
| FR-3 | TX | `shared_merchant_link` | `yes` | — | — | 0.60 | S "converging on a shared merchant set" | `merchant_habitual`, `merchant_popularity@merchant_habitual`, `merchant_mcc`, `mcc_habitual`, `merchant_country`, `merchant_country_home`, `merchant_accounts_1h` |
| FR-4 | TX | `joint_link` | `yes` | — | — | 0.60 | as FR-1 | — |

**`MERCHANT_COLLUSION` (§3.7).**

| # | P | Attribute | V* | Exempt | Composition | E_min | Source | Consequences |
|---|---|---|---|---|---|---|---|---|
| MC-1 | TX | `merchant_high_amount_cv_24h` | `<0.05` | — | — | 0.30 | S "unusually uniform amounts" | `merchant_amount_cv_24h` |
| MC-2 | TX | `amount_decile` | `8`, `9` | — | judged | 0.80 | S "high, unusually uniform amounts" | — |
| MC-3 | TX | `amount_vs_account` | `[2,5)`, `[5,20)`, `≥20` | rare: `≥20` | documented | 0.80 | S "high, unusually uniform amounts from many unrelated accounts" `[review]` | `amount_z` |
| MC-4 | TX | `merchant_habitual` | `unhabitual` | — | — | 0.80 | S "One merchant" + "from many unrelated accounts" `[review]` | `merchant_popularity@merchant_habitual` |
| MC-5 | TX | `mcc_habitual` | `unhabitual` | — | — | 0.50 | K "`MCC_ANOMALY`" | `merchant_mcc` |

**`CREDENTIAL_STUFFING` (§3.8).**

| # | P | Attribute | V* | Exempt | Composition | E_min | Source | Consequences |
|---|---|---|---|---|---|---|---|---|
| CS-1 | ID | `event_type` | `LOGIN_FAILED` | — | — | 0.60 | S "A burst of failed logins" | — |
| CS-2 | ID | `ip_datacenter` | `datacenter` | — | — | 0.80 | S "from a small datacenter IP pool" | — |
| CS-3 | ID | `ip_login_accounts` | `6+` | — | — | 0.80 | S "across many unrelated accounts" | — |
| CS-4 | ID | `device_login_accounts` | `3-5`, `6+` | — | documented | 0.50 | K "`DEVICE_SHARING`" | `device_home`, `device_age` |
| CS-5 | TX | `ip_datacenter` | `datacenter` | — | — | 0.80 | K "`IP_REPUTATION`" | `ip_home`, `ip_accounts`, `ip_accounts_1h` |
| CS-6 | TX | `ip_login_accounts_1h` | `2-4`, `5+` | rare: `5+` | documented | 0.50 | S "A burst of failed logins across many unrelated accounts" | — |
| CS-7 | TX | `prior_events_24h` | `other-identity`, `failed-login` | — | judged | 0.60 | S "a minority succeeding and transacting immediately" | `failed_logins_1h` |
| CS-8 | TX | `joint_link` | `yes` | — | — | 0.50 | S "from a small datacenter IP pool" + K "`DEVICE_SHARING`" | — |

**`ANOMALOUS_HIGH_VALUE` (§3.9).**

| # | P | Attribute | V* | Exempt | Composition | E_min | Source | Consequences |
|---|---|---|---|---|---|---|---|---|
| AHV-1 | TX | `amount_vs_account` | `≥20` | rare: `≥20` | — | 0.80 | S "far beyond the account's own amount distribution" | — |
| AHV-2 | TX | `amount_z` | `≥2` | — | — | 0.80 | as AHV-1 | — |
| AHV-3 | TX | `amount_decile` | `9` | — | — | 0.80 | as AHV-1 | — |
| AHV-4 | TX | `mcc_habitual` | `unhabitual` | — | — | 0.80 | S "at a merchant category it never uses" | `merchant_habitual`, `merchant_popularity@merchant_habitual`, `merchant_mcc` |

**`UNUSUAL_LOCATION_DEVICE` (§3.10).**

| # | P | Attribute | V* | Exempt | Composition | E_min | Source | Consequences |
|---|---|---|---|---|---|---|---|---|
| ULD-1 | TX | `device_home` | `not` | — | — | 0.80 | S "on a never-seen device" | `device_account_tx`, `device_accounts`, `device_accounts_24h`, `distinct_devices_24h` |
| ULD-2 | TX | `device_age` | `first-use` | — | — | 0.80 | S "on a never-seen device" | — |
| ULD-3 | TX | `location_novel` | `[100,500)`, `≥500` | — | judged | 0.80 | S "in a never-seen place" | `distance_home`, `leg_speed`, `merchant_country_home`, `distinct_countries_24h` |

### 6.5 Not admitted

These were in LPC-4's allowlist or were considered for this one.

| Candidate | Why not |
|---|---|
| `CARD_TESTING` `merchant_mcc` (LPC-4) | The signature documents MCC *diversity*, not an MCC value mix. It is replaced by CT-7, with `merchant_mcc` as its consequence. |
| `CARD_TESTING` `merchant_habitual` (LPC-4, interp.) | Not cited. It becomes a consequence of CT-6. |
| `CARD_TESTING` `device_home`, `device_age` (LPC-4, interp.) | Not cited: the signature says "from one device", not a device new to the account. Through `DEVICE_SHARING`, `device_accounts` could not show an effect on TX rows (§13 S7b), because population home devices are shared by chance (§18, item 12). They stay ORDINARY (§18, item 9). |
| `CREDENTIAL_STUFFING` TX `device_accounts` through `DEVICE_SHARING` | The stuffing device reaches few transacting accounts, so no effect could be shown on TX rows. The ID-row form is CS-4. TX device attributes stay ORDINARY (§18, item 9). |
| `ip_login_accounts` `3-5` (`CREDENTIAL_STUFFING`); `device_accounts` `2` and `ip_accounts` `2`, `3-5` (`FRAUD_RING`) | Population home IPs and devices are shared by chance, so these values are common among legitimate rows and could not show an effect (§13 S7b; §18, item 12). |
| `IMPOSSIBLE_TRAVEL` `distance_home` (LPC-4) | The signature names the distance between the two legs. It becomes a consequence of IT-3. |
| `VELOCITY_ATTACK` and `UNUSUAL_LOCATION_DEVICE` amount attributes (LPC-4) | "Amounts stay ordinary" and "squarely inside the account's normal range" state parity, not enrichment. They are enforced by §11. |
| `VELOCITY_ATTACK` and `DEVICE_FARM` `account_activity` (LPC-4) | Dataset-wide activity is not the documented burst, nor "once or twice". They are replaced by VA-1..6 and DF-3. |
| `FRAUD_RING` merchant attributes (LPC-4, interp.) | Consequences of FR-3. |
| `MERCHANT_COLLUSION` `merchant_popularity`, `merchant_mcc`, `merchant_country`, `merchant_country_home` (LPC-4) | Not cited. Popularity is a consequence of MC-4, `merchant_mcc` of MC-5, and the country attributes are ORDINARY. |
| `ACCOUNT_TAKEOVER` DEV `event_type` | Legitimate device events are `FIRST_SEEN` as well, so no effect could be shown (§13 S7b). |
| Any entry sourced from `IDENTITY_CHANGE` for `CREDENTIAL_STUFFING` | The mechanism emits logins, which are not identity changes (§18, item 8). |

## 7. S1 — no undocumented exclusivity or enrichment

**S1-U — unconditional.**
- **Scope.** Every population `P`, attribute `b` of class B or V (within its stratum), value `v`, and
  group `g`, where `g` is either:
  - any scenario `s`; or
  - `POOLED`, for an attribute not exempt under R8's rule.
- **Trigger.** A cell is checked when `lo_g(v) > 0.02`.
- **Failure.** The checked cell fails if either condition holds:
  - (a) `not SUPPORTED({v})`. For a `rare` value of a row of `s` for `b`, the 0.001 share condition is
    dropped; for a `none` value, (a) is skipped;
  - (b) `PRECISION_HI(g, {v}) > 0.25` — applied only when `v` is ORDINARY for `s`, and always for
    `POOLED`.

**S1-B — only the documented behaviour may differ.** For every allowlist row `e = (s, P, a, V*)`:
- **Stratum.** `σ` is the rows of `P` with `a ∈ V*`, restricted to `b`'s stratum when `b` is
  conditional.
- **Reference.** `ref` is the LEGIT rows in `σ` if they number ≥ 30 and come from ≥ 20 accounts.
  Otherwise it is all LEGIT rows of `P`, within `b`'s stratum.
- **(i) Sweep.** For every attribute `b` of class B or V that no row of `s` names in `P`, and that is not
  a consequence of another row of `s`: for every value `v`, fail if `ENRICHED`, where `g` is `s`'s rows
  in `σ` and the legitimate side is `ref`.
- **(ii) Consequences are judged only here.** A consequence of `e` is exempt, for `s`, from R7 and from
  S1-U's precision check. S1-U still checks its support.
- **(iii) Composition.** When `e` is `judged` and `V*` has at least two values:
  - fail if the LEGIT rows in `σ` number < 30 or come from < 20 accounts;
  - otherwise, for each `v ∈ V*`, fail if `ENRICHED` holds for `a = v`, where `g` is `s`'s rows in `σ`
    and the legitimate side is the LEGIT rows in `σ`.

## 8. S2 — representation invariance (no allowlist)

For every population, every R attribute and every group `g` (each scenario, and `POOLED`):

- **S2a — exclusive value.** Fail if `x_g(v) ≥ 1` and `x_LEGIT(v) = 0` for any value `v`.
- **S2b — parity.** Fail if `DIFFERS(g, 0.01)` for any value `v`.
- **S2c — exact row rules.** Every row is checked; no statistics apply. The check fails on the first
  violating row.
  1. Every row validates against its released schema and carries no field the schema does not declare.
  2. No string field of any row contains, case-insensitively, any of these:
     - a `FraudPattern` name;
     - `is_fraud`;
     - `fraud_pattern`;
     - `scenario_instance`;
     - `causal_evidence`.
  3. Every `tx.raw.v1` row has `authorization_outcome = UNKNOWN` (ADR-0049 §3).
  4. There is exactly one OUT row per TX row, joined by `transaction_id`. Each pair must satisfy:
     - `account_id` is equal;
     - `transaction_occurred_at` equals the TX envelope `occurred_at` string;
     - `t(OUT) − t(TX) = 40 + int(abs(g))` ms, where `g = derive(seed, "authorization-latency",
       transaction_id).gauss(300.0, 150.0)` (**DM-1**, ADR-0049 §7);
     - the OUT row is emitted after its TX row.
  5. Correlation:
     - a TX row and its OUT row share one `correlation_id` and one `trace_id`, used by no other row;
     - every ID and DEV row has a `correlation_id` and a `trace_id` used by no other row.

     A correlation or trace id never spans a scenario episode (correction N10).
  6. `event_id` and `idempotency_key` are unique across all rows of all four streams.
  7. Every envelope timestamp matches `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$` (correction N11).
  8. Every TX, ID and DEV row has `t ∈ [start_at, end_at)`. OUT rows may fall after `end_at`.

## 9. S3 — calendar coverage

1. **Slices.** Split `[start_at, end_at)` into 20 slices of equal length; the last slice ends at
   `end_at`.
2. **Instances.** An instance's start is the least `t` of its TX rows. `N_g` is the number of
   instances of group `g`, and `k_gj` is how many of them start in slice `j`.
3. **Legitimate share.** `ℓ_j` is the share of LEGIT TX rows in slice `j`.
4. **Rule.** For each group `g` (each scenario, and `POOLED`) and each slice with `ℓ_j ≥ 0.01`, take
   `(lo, hi) = Wilson(k_gj, N_g, N_g)`. Fail if `hi < 0.25 × ℓ_j` or `lo > 4.0 × ℓ_j`.

## 10. S4 — no fixed-offset spikes

**Pair kinds.** Offsets are `t(second) − t(first)`, using only offsets ≤ 86,400,000 ms.
- **K1:** a TX row → the next TX row of the same account.
- **K2:** a Q4e ID row → the next DEV `FIRST_SEEN` of the same account.
- **K3:** any ID row → the next TX row of the same account.
- **K4:** a DEV row → the next TX row of the same account on that `device_id`.
- **K5:** a TX row → the next TX row on the same `device_id` by a different account.
- **K6:** a `LOGIN_FAILED` or `LOGIN_SUCCEEDED` row → the next such row from the same `ip_id`, for any
  account.
- **K7:** a TX row → its OUT row.
- **K8:** a TX row → the next TX row at the same merchant, for any account.

**Which pairs count.**
- **Planted pairs of `s`:** both rows belong to one instance of `s`; for K7, the TX row is planted.
- **Legitimate pairs:** both rows are legitimate.
- **Other pairs** are not used.
- **A pair's cluster** is its instance, or the account of its first row.

**Spike.** For a pair kind and group `g`:
- **Bins.** `bin = floor(offset_ms / 1000)`. `T` is all pairs of `g`; `W3(b)` is the pairs in bins
  `b−1..b+1`; `C(b)` is the pairs in bins `b−31..b−2` and `b+2..b+31`.
- **Bounds.** `(lo3, ·) = Wilson(|W3|, |T|, k)` and `(·, hiC) = Wilson(|C|, |T|, k)`, where `k` is the
  distinct clusters among `T`.
- **Definition.** `spike(g, b)` holds when all three conditions hold:
  - the pairs in `W3(b)` come from ≥ 5 distinct clusters;
  - `lo3 > 0.02`;
  - `lo3 / 3 > 4.0 × hiC / 60`.

**Rule.** Fail if, for some pair kind, scenario `s` and bin `b`, `spike(s, b)` holds and
`spike(LEGIT, b′)` holds for no `b′ ∈ {b−1, b, b+1}`.

**Documented offsets.** None, and revision 1 permits none.

## 11. S5 — amounts

**G1 — the declared amount mechanism for planted TX rows (a generator rule).**
- **Keyed draws.** Every draw uses `rng = derive(seed, "scenario-amount", f"{instance_id}:{ordinal}")`.
  `ordinal` is the planned event's index within its instance.
- **Typical amount.** `typical` is `exp(amount_mu)`.
- **ORDINARY.** `sample_amount_minor(rng, profile)` — the function legitimate transactions use.
- **ORDINARY-REGION.** Draw repeatedly from `sample_amount_minor(rng, profile)` until the region holds.
  - At most 10,000 draws.
  - Exhaustion fails the generation; there is no fallback value.

| Scenario and rows | Mechanism |
|---|---|
| `IMPOSSIBLE_TRAVEL`, `DEVICE_FARM`, `FRAUD_RING`, `CREDENTIAL_STUFFING` | ORDINARY |
| `VELOCITY_ATTACK` | ORDINARY-REGION: `amount < 3 × typical` |
| `UNUSUAL_LOCATION_DEVICE` | ORDINARY-REGION: `0.5 × typical < amount < 2 × typical` |
| `ACCOUNT_TAKEOVER` | ORDINARY-REGION: `amount ≥ 3 × typical` |
| `CARD_TESTING` payoff (the one planned event after the probes) | ORDINARY-REGION: `amount ≥ 3 × typical` |
| `CARD_TESTING` probes | `rng.randrange(1, 250)` |
| `ANOMALOUS_HIGH_VALUE` | `int(exp(amount_mu) × rng.uniform(20.0, 60.0)) + 1`, which always exceeds 20 × `typical` |
| `MERCHANT_COLLUSION` | one draw per instance, `band = rng′.randrange(15000, 60000)` with `rng′ = derive(seed, "scenario-amount", f"{instance_id}:band")`; each row `band + rng.randrange(-400, 400)` |

**S5a — mechanism check.** Recompute every planted TX row's amount by G1 and require equality.

**S5b — ordinary amounts.**
- **Scope.** Every ORDINARY or ORDINARY-REGION scenario, plus the `CARD_TESTING` payoff rows as their
  own group.
- **Reference.** The LEGIT TX rows whose amount satisfies the same region; for ORDINARY scenarios, all
  LEGIT TX rows.
- **Rule.** Fail if `DIFFERS(g, 0.02)` holds for any `amount_z` value.

## 12. S6 — feature availability

- **S6a.** No TX row has `UNAVAILABLE` for any of the 26 features.
- **S6b.** Every `avail:<feature>` attribute is judged exactly as class B attributes are, by S1-U, S1-B,
  R7 and R8. Its status comes from §4.7.

## 13. S7 — behavioural signal preserved

**S7a — per-instance invariants.** These hold on the emitted rows of every instance in the candidate.
`typical` is `exp(amount_mu)`, and the speed is km ÷ elapsed hours on the emitted coordinates and times.

1. **`ACCOUNT_TAKEOVER`.**
   - A Q4e ID row precedes every TX row of the instance.
   - Every TX `device_id` is not a home device.
   - Every TX amount is > 2.5 × `typical`.
   - Every TX row is > 100 km from home.
2. **`CARD_TESTING`.** The probes are the TX rows with amount < 250.
   - There are ≥ 9 probes.
   - They cover ≥ 5 distinct merchants.
   - The probe span is < 1,800,000 ms.
   - Some TX amount is > 1000.
   - Across all `CARD_TESTING` instances pooled, at least one probe's OUT row is `DECLINED`.
3. **`IMPOSSIBLE_TRAVEL`.**
   - Exactly 2 TX rows.
   - Both are `CARD_PRESENT`.
   - The implied speed is > 900 km/h.
4. **`VELOCITY_ATTACK`.**
   - ≥ 16 TX rows on one account.
   - The span is < 2,400,000 ms.
   - Every amount is < 3 × `typical`.
5. **`DEVICE_FARM`.**
   - Exactly one `device_id` across its TX rows.
   - ≥ 6 distinct accounts.
6. **`FRAUD_RING`.**
   - ≥ 3 accounts.
   - Fewer distinct devices than accounts.
   - ≤ 3 distinct merchants.
7. **`MERCHANT_COLLUSION`.**
   - Exactly one merchant.
   - ≥ 10 accounts.
   - `(max − min) / max(1, mean)` of the amounts is < 0.2.
8. **`CREDENTIAL_STUFFING`.**
   - ≥ 16 ID rows, covering ≥ 10 accounts.
   - ≤ 3 distinct `ip_id`.
   - More `LOGIN_FAILED` than `LOGIN_SUCCEEDED`.
   - Every ID row's IP is a datacenter IP.
9. **`ANOMALOUS_HIGH_VALUE`.**
   - Exactly 1 TX row.
   - Its amount is ≥ 20 × `typical`.
10. **`UNUSUAL_LOCATION_DEVICE`.**
    - Exactly 1 TX row.
    - `0.5 × typical < amount < 2 × typical`.
    - The device is not a home device.
11. **Every instance** has ≥ 1 TX row.

**S7b — documented effect.** For every allowlist row `e = (s, P, a, V*)`, with the value set `V*`, fail
unless both hold:
- `lo_s ≥ E_min`;
- `lo_s ≥ min(2.0 × hiF, hiF + 0.25)`.

A row that names two populations is judged in each.

## 14. S8 — controls

1. **eval-v1 negative control.**
   - **Inputs.** eval-v1 is regenerated gate-off from its manifest and digest-verified. Its OUT rows are
     derived by ADR-0049 §7 (DM-1 with eval-v1's seed), with envelopes built by the eval-v2
     decision-row builder.
   - **Requirement.** It must fail every one of the following, each on the cell named.
     1. §5.1 R3 on `IDENTITY_CHANGE_24H` and on `DEVICE_FIRST_SEEN_24H`, each with a lower bound
        > 0.25.
     2. §5.2 R3 on each of the five signals, each with point precision > 0.25.
     3. §5.3:
        - M1 R3 with point precision > 0.25;
        - M2 R3 with point precision > 0.25;
        - M3 R6 with a point ratio > 2.
     4. §5.4 R7 on at least one cell of `hour` or `daypart`, and on at least one cell of
        `merchant_habitual` or `merchant_popularity@merchant_habitual`.
     5. §7 S1-U (a) on TX `ip_home = not`, for at least one scenario.
     6. §7 S1-U (a) on TX `distance_home` ∈ {`[100,500)`, `≥500`}, for `ACCOUNT_TAKEOVER`.
     7. §8 S2a or S2b on TX `subsecond`; S2c rules 3 and 7.
     8. §9 for `POOLED` on slice 20.
     9. §10, K2, for `ACCOUNT_TAKEOVER`.
     10. §11 S5b for `VELOCITY_ATTACK` or `UNUSUAL_LOCATION_DEVICE`.
2. **Zero-rate control.** Take the candidate configuration and set every legitimate-baseline rate to
   zero: identity and device activity, T1, T3 and N1–N5. It must fail at least one §5.1 R1.
3. **Ablation controls.** Each correction is disabled alone, keeping the candidate's configuration and
   seed. Each ablation must fail the check named.

   | Correction disabled | Must fail |
   |---|---|
   | T1 legitimate non-home payment devices | §5.2 `TX_NON_HOME_DEVICE`, R1 or R2 |
   | T2 planted sub-second timing | §8 S2a or S2b, TX `subsecond` |
   | T3 legitimate declines | §5.2 `TX_DECLINED`, R1 or R2 |
   | M1 | §5.3 `TX_REPEATED_EXACT_COORDINATES`, R3 |
   | M2 | §5.3 `TX_EXACT_HOME_POINT`, R1′–R3′ |
   | M3 | §5.3 R6 |
   | M4 time of day | §5.4 R7, `hour` or `daypart` |
   | M5 merchant choice | §5.4 R7, `merchant_popularity@merchant_habitual` |
   | M6 channel | §5.4 R7, `channel`, for a scenario other than `IMPOSSIBLE_TRAVEL` |
   | N1 legitimate non-home IPs | §7 S1-U (a), TX `ip_home = not` |
   | N2 legitimate travel | §7 S1-U (a), TX `distance_home` ∈ {`[100,500)`, `≥500`}, `ACCOUNT_TAKEOVER` |
   | N3 legitimate micro-sessions | §7 S1-U (a), TX `gap_prev` ∈ {`<10s`, `[10s,60s)`}, `CARD_TESTING` or `VELOCITY_ATTACK` |
   | N4 fixed-price merchants | §7 S1-U (a), TX `merchant_high_amount_cv_24h = <0.05`, `MERCHANT_COLLUSION` |
   | N5 household sharing | §7 S1-U (a), TX `joint_link = yes`, `FRAUD_RING` |
   | N6 whole-window placement | §9, `POOLED`, slice 20 |
   | N7 takeover change → `FIRST_SEEN` offset | §10, K2, `ACCOUNT_TAKEOVER` |
   | N8 device-farm session gap | §10, K1, `DEVICE_FARM` |
   | N9 amounts (eval-v1 bands) | §11 S5a |
   | N10 correlation per business flow | §8 S2c rule 5 |
   | N11 tie order and millisecond rendering | §8 S2c rule 7, or S2b `tie_rank` |
   | N12 decisions as events (decision in `tx.raw.v1`, no OUT stream) | §8 S2c rule 3 |

   An ablation that does not fail its check is a failed control, and it fails `LPC-5`.
4. **Minimum instances.**
   - **Requirement.** Every scenario has ≥ 20 instances, each with ≥ 1 TX row.
   - **G2, the generator rule that makes this hold.** After the weighted scenario mix is planned, any
     pattern with fewer than 20 instances receives further instances until it has 20.
     - The extra instances are planned exactly as mix instances are, from
       `derive(seed, "coverage-floor", f"{pattern}:{n}")`.
     - The realised fraud rate is measured and recorded.
5. **Non-vacuity of the checks.** Fail if any of the following judged nothing:
   - S1-B, for any allowlist row;
   - S4, on K1, K2 or K7;
   - S3, for `POOLED`;
   - S5b, for any group;
   - S7b, for any row.

## 15. Reported, not gated

The acceptance record carries the following, each computed on the acceptance run:
- **Per check:** every verdict, the failing cells with their counts and bounds, and the number of cells
  judged per check.
- **Power per scenario and population:** instance counts, and the smallest planted share R7 could fail
  against a legitimate share of 0.05.
- **R6:** the sensitivity window of §5.3, recomputed.
- **Mix:** the realised fraud rate and scenario mix.
- **Draws:** G1 draw counts per scenario.

## 16. Implementation obligations (before any candidate)

1. **Declaration pin.** One test asserts every constant, attribute table, allowlist row, causal-key
   map, `DEPENDS` row, pair kind, G1 row, control and ablation row literally.
2. **Citation and class checks.**
   - Every `S` or `N` fragment appears verbatim in its scenario's subsection (per §6.1 check 4).
   - Every `K` token appears in its subsection and is used through the map.
   - Allowlist attributes are class B.
   - Consequences are class B or V.
   - No attribute is both ALLOWLISTED and a CONSEQUENCE for one scenario.
3. **Self-tests on hand-built rows.** Each check gets at least one passing case and one failing case,
   including these boundaries:
   - the Wilson edge cases;
   - the 0.02 trigger;
   - the thresholds of 30 rows, 20 accounts and 0.001;
   - the stratum fallback;
   - composition with an unsupported stratum;
   - a planted-only representation value;
   - an empty slice;
   - a point-mass offset against a uniform range;
   - an offset spike with and without a legitimate counterpart;
   - G1 recomputation;
   - an S7b row where the legitimate share exceeds 0.5;
   - G2 adding instances to a pattern below 20, in a configuration built for it.
4. **Controls.** §14 runs at the acceptance scale in Stage 2. Runs at fast-lane scale in CI are
   diagnostic smoke tests only.
5. **Isolation.** No module under `packages/`, `services/` or `mcp_servers/` imports the criterion
   module.
6. **Existing LPC-4 code.** `data/generator/label_proxy_audit.py` is reviewed and tested before any of
   it is reused.

## 17. Why these values (chosen, not measured)

- **Shared with LPC-1 to LPC-4.** The 0.02 trigger, 0.25 precision, 2.0 enrichment, 0.001 floor and 0.10
  pooled share keep one scale across the families.
- **30 rows from 20 accounts.** The smallest legitimate support whose interval is not dominated by a
  few accounts.
- **S2's 0.01.** Representation is identical by construction, so any difference shown on conservative
  bounds is a defect. The tolerance only absorbs interval arithmetic.
- **S3's 20 slices, 0.25 and 4.0.** A slice is about three days — the size of the gap LP-14 left. The
  factors fail only on evidence of absence or of heavy concentration, so they tolerate genuine
  clustering.
- **S4's 3-bin window against a 60-bin background, and the 4.0 ratio.**
  - A point mass split across adjacent seconds by sub-second jitter is still a spike.
  - A documented range such as "inside a few minutes" is not.
  - ≥ 5 clusters keeps one episode from failing the rule alone.
- **S5b's 0.02.** Amount-shape parity on conservative bounds.
- **`E_min` 0.80.** The effect holds by construction for every row of the scenario, less interval
  width at 20 instances.
- **Lower `E_min`.** Used where the documented effect covers only part of a scenario's rows: a
  second leg, a probe count below the burst, a share declined.
- **S7b's `min(2 × hiF, hiF + 0.25)`.** The documented effect must stand out even when legitimate rows
  share the value often: 46 % of legitimate transactions are card-present.
- **Minimum of 20 instances.** At 20 clusters, a planted share of 1.0 still has a lower bound near 0.88.
  Without G2, a binomial draw can leave the rarest scenario below that on some seeds, and choosing a
  seed would be selection.
- **`rare` and `none` exemptions.**
  - A `rare` value still needs 30 legitimate rows from 20 accounts, so a behaviour no customer ever
    shows cannot pass as documented.
  - Only `none` drops that, and only for values beyond consumer activity.
- **Allowlisted value sets.** Burst sets include every value above the baseline (`2`, `1`, `0`), because
  the first rows of any burst take the low values.

## 18. Predicted conflicts, for the diagnostic probe to confirm or refute

These come from reading the code, not from any run. Each is resolved, if confirmed, by an approved
route. None is resolved by editing this criterion.

1. **Remote attackers trip travel speed.** `ACCOUNT_TAKEOVER` and `UNUSUAL_LOCATION_DEVICE` transact
   far from home soon after the customer's own activity. `leg_speed`, their consequence, will show
   infeasible speeds that legitimate travel (N2) does not.
   - This is real behaviour of a remote attacker, but it is not cited.
   - **Route:** a user decision — a catalogue citation, or recording the failure. It is not nuisance
     randomisation.
2. **Takeover change types.** `ACCOUNT_TAKEOVER` draws its change from three of the five Q4e types, so
   ATO-3's composition check will fail.
   - **Route:** nuisance randomisation — draw from the legitimate change-type mix (Q1 extension).
3. **Non-cumulative burst offsets.** `CARD_TESTING` and `VELOCITY_ATTACK` place event `n` at
   `n × gap_n`. This affects CT-5/VA-5 composition and S4 K1.
   - **Route:** nuisance timing.
4. **High-value MCC.** `ANOMALOUS_HIGH_VALUE` picks an unhabitual merchant, not an unused MCC, which
   threatens AHV-4's `E_min`.
   - **Route:** draw a merchant whose MCC is outside the habitual set, as documented.
5. **Amount mechanisms change.** G1 replaces the eval-v1 amount bands of `VELOCITY_ATTACK`,
   `UNUSUAL_LOCATION_DEVICE` and `ACCOUNT_TAKEOVER` (N9, made exact here).
6. **N10 is per business flow, not per causal chain.** An episode-scoped correlation id would encode
   scenario membership, so S2c rule 5 fails it.
7. **MC-3 and MC-4** rest on an implication of their fragments `[review]`, for confirmation.
8. **Catalogue inconsistency.** `CREDENTIAL_STUFFING` declares `IDENTITY_CHANGE` as a causal key, but
   its mechanism emits only logins, which are not identity changes (Q4e).
   - No entry uses the key.
   - The catalogue inconsistency is reported for a decision.
9. **Planted devices.** `CARD_TESTING` and `CREDENTIAL_STUFFING` pay from a device the account has never
   used, and their catalogue entries do not cite that. Their TX `device_home` and `device_age` are
   ORDINARY, so R7 will flag them.
   - **Route:** a user decision — a catalogue citation, or recording the failure.
10. **G2 changes the realised scenario mix** for the rarest patterns, for confirmation.
11. **Exemptions are judgements.** Exempt values (§6.3) rest on judgements about real-world legitimate
    frequency, not on measurements.
12. **Population sharing by chance.** `population.py` draws home devices and home IPs independently
    for each account, so unrelated legitimate accounts share them. Dataset-wide sharing attributes are
    therefore common among legitimate rows, and the `FRAUD_RING` link entries (FR-1 to FR-3) may show
    weaker effects than the mechanism intends. This is derived from the code, not measured.
13. **Fixed-price look-alikes need ordinary traffic.** A colluding merchant also takes ordinary spending,
    so `merchant_amount_cv_24h`, a consequence of MC-1, is high for it. N4's fixed-price merchants must
    take ordinary traffic too, or S1-B will separate `MERCHANT_COLLUSION` through that attribute.
14. **Undocumented spacing of multi-transaction episodes.** Three scenarios place several transactions
    of one account close together, but their signatures document no burst:
    - `ACCOUNT_TAKEOVER`, 3–25 minutes apart;
    - `FRAUD_RING`, 20 minutes to 3 hours apart;
    - `DEVICE_FARM`, a repeat about a minute after the first.

    Short-window counts are ORDINARY for them, so R7 will flag them.
    - **Route:** nuisance timing (Q1 extension), or a catalogue citation.

## 19. Known limits

- **Multiplicity.** The sweep judges many cells. Conservative bounds make false failures rare, not
  impossible, and a false failure is recorded as a failure.
- **Depletion.** Depletions are checked two-sided only for R attributes and amounts. Elsewhere a thin
  depletion can pass.
- **Power.** Per-scenario power is limited at 20 instances; §15 reports it.
- **Availability.** Availability comes from the reference implementation, not from the served Redis
  store. Their agreement is ADR-0046's parity obligation.
- **Catalogue.** The allowlist is only as right as the catalogue it cites.
- **Thresholds.** All thresholds are chosen.

## 20. Revision log

1. **2026-09-14, revision 1.** Initial freeze. No eval-v2 candidate exists. The latest diagnostic
   evidence is the Stage 1d gated probe (120,000 transactions, seed 42), and no threshold here was set
   from its numbers.
