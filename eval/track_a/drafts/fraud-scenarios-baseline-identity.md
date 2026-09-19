# DRAFT: proposed changes to `docs/FRAUD_SCENARIOS.md` for eval-v2

> Draft text for the lead, who owns `docs/`. Nothing here is applied. Each block names the section it
> would change. It describes code that exists after stage 1b (the gated baseline, T1–T3, `LPC-1` and
> `LPC-2`); it says nothing about the eval-v2 dataset itself, which does not exist yet.

---

## Replace the second paragraph of §2 ("Two scenarios are defined by non-transaction events…")

Two scenarios are defined by non-transaction events, so the generator also emits
`identity.events.v1` and `device.events.v1`: a takeover *begins* with a credential change, and
credential stuffing *is* a burst of failed logins. Encoding those as transactions would misrepresent
both and would make `IDENTITY_CHANGE` an uncausal key.

**In `eval-v1` those topics carry scenario events only**, so the mere presence of an identity or
device event marks fraud. Three transaction fields are proxies there too: only planted transactions
use a device outside the account's home devices, land on a whole second, or are declined. `eval-v1`
keeps all of this, because it is frozen. From `eval-v2` the legitimate baseline in §5 removes them.

## New §5 (renumber the current §5 "Changing this catalogue" to §6)

### 5. The legitimate baseline (eval-v2)

Fraud is a departure from a baseline, and until eval-v2 parts of the baseline were missing. With
`GeneratorConfig.baseline_identity` set:

- **Identity and device activity.** Ordinary accounts log in, sometimes mistype (occasionally in a
  burst, sometimes ending in a password reset), change credentials and contact details, enrol and reset
  MFA, get new devices, and see device attribute and fingerprint changes.
- **Payments from non-home devices.** After a legitimate `FIRST_SEEN` an account pays with its new
  device at a declared share, and some accounts occasionally pay from a household or work device.
- **Legitimate declines.** A declared share of legitimate transactions is declined, concentrated on a
  minority of accounts, and some declined attempts are retried within minutes with the same purchase.

Every rate is a declared, **chosen** configuration value; none is a statistic about real customers (the
eval-v2 ADR lists each with its rationale).

**What stays distinctive is the pattern, not the presence.**

| Scenario | Presence (no longer a proxy) | Pattern (still detectable) |
|---|---|---|
| `ACCOUNT_TAKEOVER` | an identity change, a `FIRST_SEEN`, or a non-home device | a change, then spending within hours on a device new to the account, far from home, above profile |
| `CREDENTIAL_STUFFING` | any login event in the day before a transaction | many distinct accounts failing and succeeding from one small datacenter IP pool within the hour |
| `CARD_TESTING` | a declined outcome, or a recent decline on the account | many tiny authorisations across many merchants within minutes from one device, then a larger charge |

**The baseline is shaped like the scenarios' events.** For each event type, legitimate and scenario
events carry the same payload fields, the same user-agent distribution, the same correlation and lag
scheme and a coherent device platform. Any difference in shape would betray the label, so a test
asserts each.

**Coherence.** Logins use the account's own devices and home IPs. A legitimate `FIRST_SEEN` is
genuinely first: nothing — not even a transaction — references that device on that account before it.
Legitimate activity never adopts a device a scenario names for the account, so no scenario's
`DEVICE_NOVELTY` key becomes false.

### How planted events are emitted under eval-v2

Scenario definitions, signatures, channels and causal evidence keys do not change. Three details of how
planted rows are emitted do, so that no planted row carries a mark legitimate rows never have:

- **Timing.** Every planted event — transactions and identity and device events — keeps its planned
  second and gets a uniformly drawn millisecond, the way legitimate transaction times are drawn.
  Events planned in different seconds keep their order.
- **Location.** A planted location — the away city of a takeover or an unusual-location transaction,
  either leg of an impossible journey — is an anchor, not a copy. The transaction is placed around it
  with the same noise model legitimate transactions use around home. No planted transaction repeats an
  exact coordinate or sits exactly on an account's home point.
- **Entry mode.** A planted entry mode is drawn from the legitimate entry modes of the planted channel.
  No signature names an entry mode; `IMPOSSIBLE_TRAVEL` requires card-present legs, and they stay
  card-present.

`IMPOSSIBLE_TRAVEL` stays infeasible: the implied speed is re-checked on the emitted times and
locations, and a draw that would make the journey possible is redrawn (asserted on emitted rows).

## Add a note to §3.10

> *eval-v1 caveat:* on the eval-v1 transaction stream no legitimate transaction uses a device outside
> the account's home devices, so the overlap with "a customer with a new phone" was conceptual rather
> than present in the data. From eval-v2, legitimate customers do pay from new and secondary devices.

## Add rows to §4 "Rules that hold across all ten"

| Rule | Why | Enforced by |
|---|---|---|
| With the baseline off, output is byte-identical to the pre-baseline generator | eval-v1 is frozen and referenced by digest | `test_gate_off_output_is_byte_identical_to_the_pre_change_generator` |
| With the baseline on, every planted event keeps its account, overrides and planned second | Scenario definitions do not change; only the millisecond does | `test_planted_events_keep_account_overrides_and_second_with_the_gate_on_or_off` |
| `IMPOSSIBLE_TRAVEL` stays infeasible under the timing change | A wrong label is worse than none | `test_impossible_travel_stays_infeasible_on_emitted_rows` |
| Legitimate and scenario identity or device events share one shape per type | A shape difference would betray the label | `test_legitimate_and_scenario_events_share_one_payload_shape_per_type` |
| A scenario's novel device stays novel with the baseline on | `DEVICE_NOVELTY` must stay causal | `test_scenario_novel_devices_stay_novel_with_the_gate_on` |
| `CARD_TESTING` declines are unchanged; no other planted transaction is declined | The scenario's signature is not diluted or moved | `test_card_testing_declines_are_unchanged_and_no_other_planted_row_declines` |
| Presence of identity or device events, and five transaction-row facts, are not label proxies (eval-v2 only) | Q5, B10; `LPC-1` and `LPC-2` | `tests/unit/test_eval_v2_label_proxy.py` (stage-1b demonstration); stage-2 acceptance on the frozen eval-v2 |
