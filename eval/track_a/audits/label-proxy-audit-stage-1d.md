# Label-proxy audit — Phase 3 Step E, stage 1d

> **Analysis only.** No generator behaviour changed. **Every figure below is diagnostic: none has
> a `run_id`, none is publishable, and none is a quality metric.** A replayed or generated sample is
> not a fraud rate (docs/EVALUATION.md). Nothing here uses model performance: every test is
> structural — presence, support, coverage and distribution of an observable in planted rows
> against legitimate rows.

**The question.** Could a detector infer the label because of how the dataset was generated, rather
than because it learned fraud behaviour?

## 0. Method and definitions

**Evidence sources.**

| Source | What | Scale |
|---|---|---|
| `eval-v1`, frozen | `data/generated/eval-v1/*.parquet` joined with `groundtruth.transaction_labels` as `trace_eval`, after loading; population rebuilt from the manifest config (sanity: every legitimate row uses a home device, as the code says) | 1,000,000 transactions, 5,003 fraudulent; 865 identity and 90 device events |
| Current generator, gated | The Step E worktree's engine with `BaselineIdentityConfig()` defaults, generated in memory; labels and scenario membership from the rows; nothing written | 120,000 transactions, 605 fraudulent, seed 42, eval-v1's population ratios |
| Code | `data/generator/*.py` on `phase/03-stream-medallion` (the eval-v1 generator; line numbers below), the Step E worktree's diff and its eval-v2 ADR draft | — |

The probe scripts and their outputs are kept beside the eval-v2 draft in the Step E worktree
(now `eval/track_a/audits/stage-1d-evidence/`); Stage 2's tests replace them.

**Behavioural signal.** An observable is a legitimate fraud signal only if all three hold:
1. it is named by the scenario's documented signature, notes or causal keys
   (`docs/FRAUD_SCENARIOS.md`);
2. it is produced by the documented mechanism, not by a different representation, constant or code
   path;
3. legitimate traffic also exhibits it, at a declared, non-degenerate rate. Every observable a
   fraudster produces, some customer produces too; a value no legitimate row can take teaches "this
   never happens to customers", which is a fact about the generator.

**Generator / label-construction signal.** Anything else: a representation difference (precision,
format, key set, envelope, identifier, ordering), undocumented enrichment, an observable exclusive to
planted rows (even a documented one), a fixed constant in planted timing or amounts, a calendar or
selection artefact, or a difference in whether a feature is available at all.

**Severity.** CRITICAL — near-direct leakage (an exclusive value, or a field unknown at decision
time). HIGH — a strong scenario-membership shortcut. MEDIUM — a distributional shortcut. LOW —
plausible but weak.

**Status in the current generator.** *Fixed* (measured gone under the gate), *declared* (in the
eval-v2 ADR draft but not implemented), *survives* (measured present under the gate), *new* (not in
the ADR draft).

**Change columns.** G generator semantics · C event contracts · F FeatureSpec semantics (ADR-0046) ·
E Phase 2/3 evidence.

## 1. Inventory

| ID | Proxy | Support | Severity | Current generator |
|---|---|---|---|---|
| LP-01 | Identity and device events exist only inside scenarios | fraud only | CRITICAL | fixed (LPC-1) |
| LP-02 | `DECLINED` only in `CARD_TESTING` | fraud only | CRITICAL | fixed (LPC-2, T3) |
| LP-03 | The scored transaction's own `authorization_outcome` is post-decision | both; fraud-only value in eval-v1 | CRITICAL | survives — **needs U7** |
| LP-04 | Payment from a non-home device only in fraud | fraud only | CRITICAL | fixed (LPC-2, T1) |
| LP-05 | Payment from a non-home IP only in fraud | fraud only | CRITICAL | **survives, new** |
| LP-06 | Legitimate customers never travel: ≥ 100 km from home only in fraud | fraud only | CRITICAL | **survives, new** |
| LP-07 | Whole-second planted timestamps (and no fractional part in the string) | both, different | HIGH | fixed (LPC-2, T2) |
| LP-08 | Planted episodes ignore the diurnal and weekly shape | both, different | HIGH | **declared (M4), not implemented** |
| LP-09 | Takeover `FIRST_SEEN` exactly 2 minutes after the identity change | fraud only (a 1 s band) | HIGH | **survives, new** |
| LP-10 | Legitimate sub-minute activity is almost absent | both, degenerate legitimate support | HIGH | **survives (thin), new** |
| LP-11 | Planted entry modes forced (`ECOMMERCE`, `CHIP`) | both, different | HIGH | fixed (LPC-3, M3) |
| LP-12 | A takeover repeats one exact coordinate | fraud only | HIGH | fixed (LPC-3, M1) |
| LP-13 | Impossible-travel first leg exactly on the home point | fraud only | HIGH | fixed (LPC-3, M2) |
| LP-14 | No episode starts in the last 3 days of the window | legitimate only | MEDIUM | **survives, new** (kept by M4 as declared) |
| LP-15 | Planted merchants drawn uniformly, not by popularity | both, different | MEDIUM | **declared (M5), not implemented** |
| LP-16 | Channel forced to card-not-present in five scenarios | both, different | MEDIUM | **declared (M6), not implemented** |
| LP-17 | Device-farm repeat transaction exactly 1 minute after the first | both, spiked | MEDIUM | **survives, new** |
| LP-18 | Device events draw `platform` at random | fraud-only stream in eval-v1 | MEDIUM | fixed (A3) |
| LP-19 | No legitimate fixed-price merchant (merchant-level uniform amounts) | fraud only (code-derived) | MEDIUM | **survives, new; not measured** |
| LP-20 | No legitimate household clusters (dense shared devices and IPs) | fraud only (code-derived) | MEDIUM | **survives, new; not measured** |
| LP-21 | Side events borrow the next transaction's position for envelope and `correlation_id` | both, different in eval-v1 | LOW (eval-v2) | partly fixed (A1) |
| LP-22 | Identity payload key sets differ by event kind | fraud-only stream in eval-v1 | LOW | fixed (A4) |
| LP-23 | Planted amounts are truncated uniform bands of the account's typical amount | both, different | LOW | **survives, new** |
| LP-24 | At an exact-millisecond tie, planted rows always sort after legitimate ones | both, ordered | LOW | **survives, new** |

### LP-01 — identity and device events exist only inside scenarios
1. **Location.** `data/generator/engine.py:388-394` (side events are emitted only from
   `instance.events`); planned only by `scenarios.py:160` `AccountTakeover` (identity change, then
   `FIRST_SEEN`, `:201`) and `:581` `CredentialStuffing` (logins, `:617`).
2. **Why.** Presence of any identity or device event marks a scenario participant.
3. **Support.** Fraud only. Accounts with an identity event: 12.7 % of accounts with fraud against
   1.7 % of legitimate-only accounts — and every one of those is a failed stuffing target. Change
   types, `LOGIN_SUCCEEDED` and every device event: legitimate-only accounts 0.
4. **Affected.** `failed_logins_1h`, `hours_since_identity_change`; R008, R012, R013; any model or agent
   reading identity or device streams.
5. **Severity.** CRITICAL.
6. **Correction.** Legitimate identity and device baseline — implemented under the gate. Probe: every
   planted type has legitimate counterparts (about 126,000 legitimate identity events).
7. **Changes.** G yes · C no · F no · E the eval-v1 replay's R008 separation is inflated.
8. **Test.** LPC-1 R1–R4 with its eval-v1 negative control (exists).

### LP-02 — `DECLINED` only in `CARD_TESTING`
1. **Location.** `engine.py:168` (every legitimate row `APPROVED`); `scenarios.py:279`.
2. **Why.** `authorization_outcome = DECLINED` marks fraud.
3. **Support.** Fraud only: 6.9 % of fraud, 0 legitimate.
4. **Affected.** `declined_ratio_1h`; R002.
5. **Severity.** CRITICAL.
6. **Correction.** Legitimate declines and retry pairs (T3) — implemented. Probe: 2.95 % of legitimate
   rows declined.
7. **Changes.** G yes · C depends on U7 · F `declined_ratio_1h`'s inputs depend on U7 · E R002.
8. **Test.** LPC-2 `TX_DECLINED`, `TX_DECLINED_PRIOR_1H` (exists).

### LP-03 — the scored transaction's own outcome is post-decision
1. **Location.** `tx.raw.v1` payload field `authorization_outcome` (released contract; the gateway's
   request); generated at `engine.py:168` and `scenarios.py:279`.
2. **Why.** TRACE-X's decision gates authorization, so the outcome of the transaction being scored is
   unknown when it is scored (ADR-0046 §7). A model reading the raw payload learns from the future —
   in eval-v1 a fraud-only value, in any dataset a post-decision one.
3. **Support.** Both; fraud-only value in eval-v1.
4. **Affected.** Any model or agent reading raw payload fields; `declined_ratio_1h` for earlier rows.
5. **Severity.** CRITICAL (a leakage path independent of how declines are generated).
6. **Correction.** U7: outcomes reach features through an event dated when the outcome is known; the
   generator already keeps `authorization_decided_ms` for that (worktree §3.5).
7. **Changes.** G yes · **C yes** (new event or payload change, versioned) · **F possibly**
   (`declined_ratio_1h`) · E R002 evidence.
8. **Test.** A contract test that no feature, rule or model input reads the scored transaction's own
   outcome (the FeatureSpec side exists, ADR-0046 §7); an eval-v2 check that outcome timestamps are
   never before their decision time.

### LP-04 — payment from a non-home device only in fraud
1. **Location.** `engine.py:141`; `scenarios.py:132` `_novel_device` (takeover, card testing, unusual
   location); device farm, ring and stuffing pick arbitrary universe devices.
2. **Why.** `device_is_known = 0` marks fraud.
3. **Support.** Fraud only: 54.9 % of fraud, 0 legitimate.
4. **Affected.** `device_is_known_for_account`; R008, R009, R017; R010 indirectly.
5. **Severity.** CRITICAL.
6. **Correction.** T1 (secondary and newly enrolled devices) — implemented. Probe: 6.8 % of legitimate
   rows.
7. **Changes.** G yes · C no · F no · E R008, R009, R017 separation on eval-v1.
8. **Test.** LPC-2 `TX_NON_HOME_DEVICE`, `TX_DEVICE_FIRST_USED_24H` (exists).

### LP-05 — payment from a non-home IP only in fraud (new, survives)
1. **Location.** `engine.py:142` (legitimate transactions use home IPs only; the worktree gives
   *logins* `login_away_ip_share`, never transactions); `scenarios.py:600-629` (stuffing's datacenter
   pool), `:508` (ring's shared IPs).
2. **Why.** A non-home IP marks `CREDENTIAL_STUFFING` and `FRAUD_RING`. LPC-4 allowlists IP attributes
   for both (documented pools), which exempts them from R7, and no LPC declares a legitimate floor.
3. **Support.** Fraud only: eval-v1 17.1 % of fraud, 0 legitimate; probe 29.8 % of fraud, 0 legitimate
   (stuffing 100 %, ring 100 %).
4. **Affected.** `ip_distinct_accounts_1h`; R011, R013; IP-novelty model features; graph link paths;
   agents' IP evidence.
5. **Severity.** CRITICAL.
6. **Correction.** Legitimate transactions from non-home IPs at a declared share — mobile networks,
   travel, work and public Wi-Fi — drawn from the universe (datacenter ranges at their population
   rate), coherent with the account's away logins; plus transient IP sharing among unrelated
   legitimate accounts (public networks).
7. **Changes.** G yes · C no · F no · E none recorded.
8. **Test.** LPC-5 S1 (support) on `ip_home`; an LPC-2-style `TX_NON_HOME_IP` signal with a declared
   floor.

### LP-06 — legitimate customers never travel (new, survives)
1. **Location.** `behavior.py:108-121` — Gaussian jitter with σ = `geo_jitter_km` (12 km) around home.
   Its docstring says "the occasional legitimate outlier exists", which does not hold at that scale.
   Planted: `scenarios.py:101` `_distant_city`, used at `:224` (takeover), `:364` (impossible travel),
   `:755` (unusual location).
2. **Why.** Distance ≥ 100 km marks fraud. The allowlist permits it for takeover, impossible travel and
   unusual location, so LPC-4 R7 does not judge it; with no legitimate traveller the documented
   behaviour is a construction shortcut. It also defeats ADR-0030's stated design:
   `UNUSUAL_LOCATION_DEVICE` is meant to overlap "with a customer travelling with a new phone", and no
   such customer exists.
3. **Support.** Fraud only: eval-v1 11.6 % of fraud in the 100–500 km and ≥ 500 km bands, 0 legitimate;
   probe 8.9 % of fraud, 0 legitimate (takeover 100 %, unusual location 100 %, impossible travel 50 %).
4. **Affected.** `distance_from_account_home_km`, `geo_distance_from_last_km`,
   `implied_speed_kmh_from_last`; R007, R017; geo features in any model; agents' `GEO_DISPERSION`
   evidence; the Phase 9 Arm F ablation measured on `UNUSUAL_LOCATION_DEVICE`.
5. **Severity.** CRITICAL.
6. **Correction.** Legitimate travel episodes at declared rates: trips of days in another population
   centre with a plausible outbound gap, card-present spend there, sometimes on a newly enrolled
   device; and occasional legitimate long-range card-present outliers.
7. **Changes.** **G yes** (the legitimate baseline; fulfils ADR-0030's intent rather than changing it) ·
   C no · F no · E R007 and R017 separation on eval-v1.
8. **Test.** LPC-5 S1 on `distance_home`; a `TX_FAR_FROM_HOME` floor; a look-alike floor for
   "far from home on a new device".

### LP-07 — whole-second planted timestamps
1. **Location.** `engine.py:301`, `:315` (episode start `randrange(span) * 1000`); `:82-83` (`_iso`
   omits the fraction when it is zero, so the string is shorter).
2. **Why.** Zero milliseconds, or a 20-character `occurred_at`, marks planted rows.
3. **Support.** Both, different: eval-v1 9.7 % of fraud (anomalous high value 100 %, unusual
   location 100 %, impossible travel 50 %) against 0.095 % legitimate.
4. **Affected.** Raw-timestamp or string-format model inputs; tie ordering.
5. **Severity.** HIGH.
6. **Correction.** T2 — implemented; probe 0.17 % against 0.087 %. Hygiene: render `occurred_at`
   with milliseconds always, so format never depends on value.
7. **Changes.** G yes (scenario timing, recorded) · C no · F no · E none.
8. **Test.** LPC-2 `TX_WHOLE_SECOND` (exists); LPC-5 S2 on timestamp format.

### LP-08 — planted episodes ignore the diurnal and weekly shape (declared M4, not implemented)
1. **Location.** `engine.py:300-301`, `:314-315` (uniform start); legitimate shape
   `behavior.py:32-94`.
2. **Why.** Night-time and weekday distribution mark planted rows.
3. **Support.** Both, different: 00:00–05:59 UTC eval-v1 27.4 % of fraud against 4.1 %; probe
   29.8 % against 4.1 % (device farm 85 %). Weekday TVD 0.09 in eval-v1.
4. **Affected.** Hour and weekday model features; agents' temporal reasoning. No rule.
5. **Severity.** HIGH.
6. **Correction.** M4 as declared (ADR draft §5d.1), with LP-14's change to its proposal range.
7. **Changes.** G yes (scenario timing, decided for eval-v2) · C no · F no · E none.
8. **Test.** LPC-4 R7 on `hour`, `daypart`, `weekday` (declared, untested).

### LP-09 — takeover `FIRST_SEEN` exactly 2 minutes after the identity change (new, survives)
1. **Location.** `scenarios.py:201` (`start_ms + 2 * MINUTE_MS`).
2. **Why.** The documentation says "within hours"; the constant is not documented. A device event
   120 s after an identity change marks a planted takeover.
3. **Support.** Fraud only within a 1 s band: eval-v1 all 90 device events at exactly +120,000 ms;
   probe 10 of 10 planted within 119–121 s against 0 of 759 legitimate `FIRST_SEEN`.
4. **Affected.** Device and identity sequence models; agents' timelines; fine-grained
   change-then-new-device features. No released feature reads device events today (ADR-0046 §4).
5. **Severity.** HIGH.
6. **Correction.** Draw the offset from a declared distribution consistent with "within hours" that
   overlaps legitimate enrolment couplings.
7. **Changes.** **G yes — a scenario timing change beyond decision Q1** ("scenario definitions
   unchanged") · C no · F no · E none.
8. **Test.** LPC-5 S4 (fixed-offset spikes) on the identity → device pair.

### LP-10 — legitimate sub-minute activity is almost absent (new, survives thin)
1. **Location.** `engine.py:340-342` — legitimate times independent per draw, so an account's gaps
   average days. Documented bursts: `scenarios.py:238` (card testing, 8–45 s), `:372` (velocity,
   10–55 s).
2. **Why.** The bursts are behavioural and must stay, but customers also pay twice within a minute
   (split tender, transit taps, a basket and a tip). With almost no legitimate support, velocity
   separates by construction.
3. **Support.** Gap to the account's previous transaction under 60 s: eval-v1 35.2 % of fraud against
   0.046 %; probe 32.6 % against 0.095 % (114 legitimate rows, mostly retry pairs).
4. **Affected.** `account_tx_count_1m`, `_5m`, `card_tx_count_5m`, `account_distinct_mcc_5m`; R001,
   R003, R004, R005, R006.
5. **Severity.** HIGH.
6. **Correction.** Legitimate micro-sessions at declared rates, kept well below planted burst
   intensity.
7. **Changes.** G yes · C no · F no · **E the velocity rules' legitimate firing on eval-v2 will rise
   and need re-validation** (as U10 does for R010).
8. **Test.** LPC-5 S1 with the gap and count-window buckets as attributes; R4-style enrichment kept for
   high-intensity patterns.

### LP-11 — planted entry modes forced (fixed M3)
`scenarios.py:220`, `:278`, `:296`, `:357`, `:364`, `:445`, `:562`, `:632`, `:651`. eval-v1
`ECOMMERCE` 54.1 % of fraud against 14.7 % (TVD 0.39); probe `ECOMMERCE` among card-not-present
33.3 % against 33.4 %. HIGH in eval-v1. G yes · C no · F no · E none. Test: LPC-3 (exists).

### LP-12 — a takeover repeats one exact coordinate (fixed M1)
`scenarios.py:224`. eval-v1 takeover transactions 100 % repeat an exact point, legitimate 0; probe
fraud 0 %, legitimate 1 %. HIGH in eval-v1. Test: LPC-3 (exists).

### LP-13 — impossible-travel first leg exactly on the home point (fixed M2)
`scenarios.py:357`. eval-v1 50 % of impossible-travel legs, legitimate 0; probe 0 and 0. HIGH in
eval-v1. Test: LPC-3 (exists).

### LP-14 — no episode starts in the last 3 days of the window (new, survives)
1. **Location.** `engine.py:300`, `:314` (`window_seconds - 3 * 86_400`); M4 as declared keeps
   `[start_at, end_at − 3 days)` (ADR draft line 535).
2. **Why.** A date near the end of the window marks legitimate traffic; a time-based train/test split
   puts almost no fraud in its test tail.
3. **Support.** Legitimate only in that region: eval-v1 0.1 % of fraud against 5.6 % of legitimate
   rows; probe 0 % against 5.5 %. LPC-4 `in_window` (inside/outside) cannot see it.
4. **Affected.** Calendar features; temporal splits; per-day fraud monitoring.
5. **Severity.** MEDIUM (HIGH for time-split evaluation).
6. **Correction.** Propose starts over the whole window and reject a proposal only when an event falls
   outside it (M4 already rejects those); declare the resulting taper.
7. **Changes.** G yes (scenario placement) · C no · F no · E none.
8. **Test.** LPC-5 S3 (window coverage by decile).

### LP-15 — planted merchants drawn uniformly (declared M5, not implemented)
`scenarios.py:122-129` `_unhabitual_merchant`, `:280`, `:495`, `:549`; legitimate Zipf
`behavior.py:124-145`. Top-1 % merchants: eval-v1 7.6 % of fraud against 15.0 %; probe 6.0 % against
10.6 %. Affects merchant aggregates (R014) and merchant-risk models. MEDIUM. Correction: M5 as
declared. G yes · C no · F no · E none. Test: LPC-4 R7 `merchant_popularity` (declared, untested).

### LP-16 — channel forced to card-not-present (declared M6, not implemented)
`scenarios.py:219`, `:277`, `:295`, `:444`, `:561`, `:631`, `:650`. eval-v1 69.5 % of fraud against
44.0 %; probe 65.0 % against 44.3 %. Affects channel features and whether `implied_speed` is
computable (card-present legs only). MEDIUM (partly behavioural for device-driven scenarios).
Correction: M6 as declared. Test: LPC-4 R7 `channel` (declared, untested).

### LP-17 — device-farm repeat exactly 1 minute later (new, survives)
`scenarios.py:440` (`+ n * MINUTE_MS`); the documentation says "once or twice", not the spacing.
eval-v1 3.2 % of device-farm gaps exactly 60,000 ms, legitimate 0; probe 3.0 % within 59–61 s against
3 legitimate rows. MEDIUM. Correction: draw the session gap from a declared distribution. **G yes —
beyond Q1.** Test: LPC-5 S4.

### LP-18 — device events draw `platform` at random (fixed A3)
`engine.py:219`. eval-v1 74 % of device events contradict the device registry (all planted); probe
consistent 100 %. MEDIUM in eval-v1. Test: LPC-4 `platform_consistent` (declared) and LPC-5 S2.

### LP-19 — no legitimate fixed-price merchant (new, code-derived, not measured)
1. **Location.** `scenarios.py:530-560` (`band + randrange(-400, 400)` on 15,000–60,000);
   legitimate amounts are per-account lognormals with σ 0.7–1.25 (`population.py:230-231`).
2. **Why.** A merchant whose amounts vary by a few percent across many accounts can only be
   `MERCHANT_COLLUSION`; mixing legitimate per-account lognormals cannot approach R014's coefficient of
   variation of 0.05.
3. **Support.** Fraud only, at merchant level (derived from the code, not measured).
4. **Affected.** `merchant_amount_cv_24h`, `merchant_distinct_accounts_1h`; R014; merchant-risk models.
5. **Severity.** MEDIUM.
6. **Correction.** A declared share of legitimate fixed-price merchants (transit, parking,
   subscriptions).
7. **Changes.** G yes · C no · F no · E none.
8. **Test.** LPC-5 S1 applied to a merchant population (merchants with low CV and many accounts).

### LP-20 — no legitimate household clusters (new, code-derived, not measured)
`population.py:208-216` draws each account's home devices and IPs independently, so legitimate
sharing is coincidental and unstructured; `scenarios.py:462-528` builds dense shared device and IP
clusters. Graph cluster, ring and link-path features (Phase 5) would separate rings by existence.
MEDIUM (no consumer yet). Correction: household and workplace structure in the population. G yes.
Test: LPC-5 S1 on graph density once the graph tier exists.

### LP-21 — side events borrow the next transaction's position (partly fixed A1)
`engine.py:198` (`derive(seed, "side", position)`) and `:106` (`corr_{position}`), with `position`
not advanced for side events (`:388-394`). eval-v1: `correlation_id` shared with a side event, fraud
0.2 % against 0.07 % (and consecutive side events share `trace_id` and `idempotency_key`). Probe:
envelopes unique; `correlation_id` still shared, fraud 40 % against legitimate 52 % — no longer
predictive, but unrelated events collide, which mis-joins any consumer grouping by it. LOW as a
proxy, MEDIUM as data quality. Correction: `correlation_id` per causal chain, one scheme for planted
and legitimate events. G yes · C semantics of an existing field, no schema change · F no · E none.
Test: LPC-4 `correlation_shared` (declared) plus an envelope uniqueness test.

### LP-22 — identity payload key sets differ by event kind (fixed A4)
eval-v1: `ip_id` present only on login events. Probe: identical key sets per event type for planted
and legitimate rows. LOW. Test: LPC-4 `payload_keys` (declared), LPC-5 S2.

### LP-23 — planted amounts are truncated uniform bands (new, survives)
`scenarios.py:396` (velocity 0.6–1.8 × typical), `:754` (unusual location 0.7–1.5 ×), `:218`
(takeover 3–9 ×), `:292` (card-testing payoff 4–12 ×), `:701` (anomalous 20–60 ×); typical is the
median `exp(μ)` (`:117`). eval-v1: velocity and unusual-location amounts lie entirely within
2^-1–2^0 of typical (legitimate 53 % there); takeover only in 2^1–2^3. The *level* is documented;
the hard edges and missing tail are not. LOW. Correction: "ordinary" amounts from the account's own
lognormal; "above profile" from a declared log-space distribution. G yes (beyond Q1). Test: LPC-4
`amount_vs_account` (declared) plus an LPC-5 S5 dispersion check.

### LP-24 — planted rows sort after legitimate rows at a millisecond tie (new, survives)
`engine.py:340-348` (legitimate tiebreak = draw index; planted = `legit_count + offset × 1000 +
ordinal`), so a planted transaction gets the higher `transaction_id` at a tie — and ADR-0046's order
key breaks ties on it. eval-v1: 1 mixed-label tie group. LOW. Correction: a label-independent
tiebreak. Test: a unit test on the merge key.

## 2. Checked and clean

Measured with no fraud/legitimate difference, or with a difference a signature documents:
- identifiers always inside the population; no fraud-only or legitimate-only entity creation; a card
  always belongs to its account; merchant name, MCC and country always match the registry;
- `currency`, `memo`, `event_type`, `schema_version` and `producer` constant; `user_agent` flat;
  UUIDv7 `event_id` time always equals `occurred_at`;
- `ingested_at` lag distribution identical;
- amounts never above the legitimate ceiling; round amounts flat; amounts far above profile only where
  documented (anomalous high value, collusion);
- account tenure, home country and number of home devices unbiased among accounts with fraud;
- datacenter IPs: legitimate at the population rate; `CREDENTIAL_STUFFING` 100 %, documented;
- transactions per account slightly higher on accounts with fraud — behavioural (episodes add rows);
- scenario and instance identifiers never serialised (`engine.py:78-79`); transaction ids by final
  position (`engine.py:9-13`).

## 3. Severity ranking

1. **CRITICAL, survives:** LP-05 (non-home IP), LP-06 (no legitimate travel), LP-03 (post-decision
   outcome; U7).
2. **CRITICAL, fixed:** LP-01, LP-02, LP-04.
3. **HIGH, survives:** LP-08 (time of day; M4 not implemented), LP-09 (2-minute offset), LP-10 (thin
   sub-minute support).
4. **HIGH, fixed:** LP-07, LP-11, LP-12, LP-13.
5. **MEDIUM, survives:** LP-14 (window end), LP-15 (M5), LP-16 (M6), LP-17 (1-minute spacing), LP-19
   and LP-20 (code-derived).
6. **MEDIUM, fixed:** LP-18.
7. **LOW:** LP-21, LP-22, LP-23, LP-24.

## 4. Proposed eval-v2 corrections

Already implemented under the gate and holding: legitimate identity and device activity, T1, T2, T3,
M1, M2, M3, unique side-event envelopes, coherent platform, shared key sets.

Still to do — declared: **M4** (time of day), **M5** (merchant popularity), **M6** (channel).

Still to do — new, proposed here:

| # | Correction | Fixes | Kind |
|---|---|---|---|
| N1 | Legitimate transactions from non-home IPs, and transient legitimate IP sharing | LP-05 | legitimate baseline |
| N2 | Legitimate travel episodes and long-range card-present outliers | LP-06 | legitimate baseline |
| N3 | Legitimate micro-sessions (sub-minute repeat payments) | LP-10 | legitimate baseline |
| N4 | Legitimate fixed-price merchants | LP-19 | population |
| N5 | Household and workplace sharing structure | LP-20 | population |
| N6 | Episode starts over the whole window, rejecting only out-of-window events | LP-14 | scenario placement |
| N7 | Takeover identity → `FIRST_SEEN` offset drawn, not constant | LP-09 | scenario timing (beyond Q1) |
| N8 | Device-farm session gap drawn, not constant | LP-17 | scenario timing (beyond Q1) |
| N9 | Planted amounts drawn from the account's lognormal (ordinary) or a declared log-space distribution (above profile) | LP-23 | scenario amounts (beyond Q1) |
| N10 | `correlation_id` per causal chain, one scheme for all events | LP-21 | envelope |
| N11 | Label-independent tiebreak; `occurred_at` always rendered with milliseconds | LP-24, LP-07 | ordering and representation |
| N12 | Outcomes reach features through an event dated when known | LP-03 | **requires U7** |

Every correction applies only under the gate: `eval-v1` stays byte-identical and frozen.

## 5. Proposed pre-declared acceptance criterion: `LPC-5`

To be declared in the eval-v2 ADR **before any Stage 2 generation**, with every threshold fixed in
advance. Populations, clusters (scenario instance for planted rows, account for legitimate rows),
one-sided 95 % Wilson intervals on cluster counts and time windows are those of LPC-1 §5.4.

**`LPC-5` passes if and only if every rule below holds.**

| Rule | Condition | Catches |
|---|---|---|
| **S0** inherit | LPC-1, LPC-2, LPC-3 and LPC-4 pass as declared | LP-01, 02, 04, 07, 11-13, 15, 16, 18, 21, 22 |
| **S1** no exclusive observable | For every attribute cell (LPC-4's attribute tables, plus gap-to-previous buckets, count-window buckets of every released velocity feature, and merchant-level cells) in which some scenario's planted share has lower bound > 0.02: legitimate rows in the cell number ≥ 30 from ≥ 20 distinct accounts **and** are ≥ 0.1 % of legitimate rows; and the cell's single-attribute precision has upper bound ≤ 0.25. **Applies to allowlisted attributes too: the allowlist permits enrichment, never exclusivity.** | LP-05, 06, 10, 19 |
| **S2** representation invariance (no allowlist) | For representation attributes (timestamp precision and string format, coordinate decimals, payload key set per topic and event type, identifier formats, envelope constants and uniqueness, `event_id` time, `correlation_id` scheme, platform consistency, ingest lag): no planted-only value, and per-scenario TVD upper bound ≤ 0.02 against legitimate rows | LP-07, 18, 21, 22 |
| **S3** calendar coverage | Split the window into 10 equal slices. In every slice holding ≥ 5 % of legitimate rows, each scenario with ≥ 10 instances has a planted share lower bound ≥ 0.25 × the legitimate share | LP-14 |
| **S4** no fixed-offset spikes | For each ordered event-kind pair inside an episode (transaction → transaction on one account, identity → device, identity → transaction, device → transaction, login → transaction), quantise offsets to 1 s. No bin may hold > 2 % of a scenario's offsets across ≥ 5 instances unless legitimate offsets for the same pair place ≥ half that share within ±1 s of it, or the offset is listed as documented (the list is empty today) | LP-09, 17 |
| **S5** ordinary-amount dispersion | For scenarios documented as ordinary amounts (`VELOCITY_ATTACK`, `UNUSUAL_LOCATION_DEVICE`): the standard deviation of planted log(amount / typical) is ≥ 0.5 × the legitimate per-account standard deviation | LP-23 |
| **S6** feature availability | For every released online feature and every scenario that does not allowlist it: P(available \| planted) against P(available \| legitimate) passes R7's enrichment bound, and availability is never planted-only (S1) | availability shortcuts |
| **S7** behavioural signal preserved | For every allowlisted (scenario, attribute) pair: planted share lower bound ≥ 2 × legitimate share upper bound; every scenario's signature test still passes | corrections that flatten real fraud signal |
| **S8** controls | eval-v1 (gate off) fails S1 on `ip_home` and `distance_home`, S2 on timestamp format, S3 on the last slice, S4 on the identity → device pair; each correction, disabled alone under the gate, fails the rule it exists for; the zero-rate control fails; every scenario has ≥ 10 instances (no vacuous pass) | tests that pass for the wrong reason |

**Why these thresholds (chosen, not measured).** S1's 0.02 planted-share trigger and 0.25 precision
bound reuse LPC-1 R3 and LPC-4 R7 so the families agree; the 0.1 % legitimate floor is LPC-4's
`LEGIT_SHARE_FLOOR`. S2's 0.02 TVD allows sampling noise but no systematic representation difference.
S3's 0.25 factor tolerates genuine clustering of episodes while refusing a slice that holds none.
S4's 2 % bin allows natural concentration (for example 60 s being a round number people choose) only
when legitimate activity shows it too.

**Scope.** Acceptance runs on the frozen eval-v2 itself, regenerated from its manifest and
digest-verified. Runs at the fast-lane scale are diagnostic only.

## 6. Tests to enforce it

1. **Declaration pin** — thresholds, attribute and bucket lists, the allowlist (each citation checked
   against `docs/FRAUD_SCENARIOS.md` lines) and the documented-offset list, asserted literally; a
   change is a declaration change recorded in the ADR log.
2. **Self-tests per rule** on hand-built rows: exclusive cell with and without legitimate support,
   the 0.1 % floor boundary, a representation difference, an empty calendar slice, a fixed-offset
   spike with and without a legitimate counterpart, amount dispersion, availability.
3. **Negative control** — the gate-off (eval-v1-configured) generation fails each S8-named rule for the
   named cell, with the point estimate on the failing side (as LPC-2 requires).
4. **Ablation controls** — under the gate, each correction N1–N11 (and M4–M6) disabled alone fails its
   own rule.
5. **Zero-rate control** — block present, every legitimate rate zero, fails.
6. **Isolation** — no runtime package imports the criterion modules; labels are read only
   evaluation-side (exists for LPC-1–4; extend).
7. **Acceptance command** — a Stage 2 command that regenerates the frozen eval-v2 from its manifest,
   verifies digests and runs `LPC-5`, recording each rule's verdict in the eval-v2 ADR.
8. **Existing gap** — `data/generator/label_proxy_audit.py` (the partial LPC-4) has no tests; it needs
   review and the tests above before it is relied on.
9. **Per-proxy regression tests** — as listed in §1, item 8 of each entry.

## 7. Requires U7 before correction

- **LP-03 / N12** — how the scored transaction's outcome, and earlier transactions' outcomes, reach
  features: an authorization-result event dated when known, or a change to `tx.raw.v1`. It fixes the
  shape eval-v2 freezes, so **Stage 2's freeze waits for U7**.
- **LP-02's consumption** — T3's legitimate declines are generated already, but whether
  `declined_ratio_1h` reads them from request payloads or from dated outcome events is U7.

Nothing else in this audit depends on U7.

## 8. Changes to accepted Phase 2/3 semantics

- **Scenario definitions (ADR-0030, decision Q1).** N6–N9 change planted timing, placement and amounts
  beyond Q1's "scenario definitions unchanged". Signatures and causal keys stay; the change needs a
  lead decision like Q1's and a recorded note beside `docs/FRAUD_SCENARIOS.md`.
- **Legitimate baseline.** N1–N5 change what legitimate customers do. LP-06's correction restores
  ADR-0030's stated intent for `UNUSUAL_LOCATION_DEVICE`; the eval-v1 generator never met it.
- **Event contracts.** N12 (U7) adds an event or changes a payload and follows the versioning rules.
  N10 changes the meaning of `correlation_id` values, not the schema.
- **FeatureSpec (ADR-0046).** No feature definition changes. Only U7 may change `declined_ratio_1h`'s
  inputs.
- **Phase 2 and Step 1 evidence.** The Phase 2 manual validation
  (`benchmarks/gateway/triage-bands.md`) and Step 1's controlled rule re-validation ran on eval-v1;
  they remain true records of eval-v1, but their fraud/legitimate separation is inflated by LP-01,
  LP-02, LP-04 and by missing legitimate look-alikes (LP-05, LP-06, LP-10). Both must be re-run on
  eval-v2, and the velocity rules and R010 (U10) re-validated there. The Phase 2 load gate
  (`run_id: load-20260914-gateway-b43a75ce`) does not depend on labels and stands; eval-v2's volume of
  legitimate identity events (about one login per transaction in the probe) changes the stream mix a
  representative profile and Step 2's topic budgets assume.

## 9. Recommended Stage 2 order

1. **Review this audit**, approve or amend `LPC-5` and N1–N11, and take the lead decision extending
   Q1 to N6–N9.
2. **Declare `LPC-5`** in the eval-v2 ADR, then implement and self-test it, with the gate-off negative
   control failing for the named reasons — before any generator change.
3. **Review and test the partial LPC-4** (`label_proxy_audit.py`).
4. **M4 (with N6), M5, M6.**
5. **Legitimate look-alikes:** N1, N2, N3, then N4 and N5.
6. **Scenario constants:** N7, N8, N9.
7. **Hygiene:** N10, N11.
8. **Ablation controls** for every correction; `LPC-5` at fast-lane scale (diagnostic).
9. **After U7:** N12.
10. **Generate and freeze eval-v2**, run `LPC-5` on the frozen dataset as acceptance, record the
    manifest; then re-run the manual replay, the controlled rule re-validation and the R010 study
    (U10) on eval-v2.

## 10. Limits of this audit

- The gated probe is one seed at 120,000 transactions with 605 fraudulent rows; per-scenario cells are
  small, and its figures are directions, not estimates.
- LP-19 and LP-20 are derived from the code and not measured.
- Labels are per transaction. Account-level and side-event associations are derived through scenario
  membership.
- The partial LPC-4 implementation was read for scope, not reviewed.
- An audit can only find the shortcuts it looks for. S1, S2 and S6 are written as sweeps over
  attributes and features rather than as a list of known proxies for that reason, and S8's ablations
  show each rule can fail.
