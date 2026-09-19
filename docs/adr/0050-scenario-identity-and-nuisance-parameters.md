# ADR-0050: A scenario's identity is its mechanism, relationships, signature and tests — everything else is a nuisance parameter

- **Status:** Proposed (Phase 3, Step E Stage 2 step 5). The user decided the identity rule on
  2026-09-14, approving N6–N9 as nuisance randomisation, and froze the generator decisions recorded
  here the same day. No generator code implements it yet (Stage 2 step 6).
- **Date:** 2026-09-14
- **Phase:** 3
- **Supersedes / Superseded by:** — .
  - Refines ADR-0030 for eval-v2. ADR-0030's rules all stand: causal keys are claims about causation;
    no scenario claims more than half the vocabulary; fraud is a departure from a baseline;
    `UNUSUAL_LOCATION_DEVICE` stays weak; the coverage floor and the weighted mix stay.
  - Replaces the eval-v2 draft's decision Q1, "scenario definitions, signatures and keys may not
    change", with a precise rule.

## Context

**eval-v1 plants episodes with fixed constants.** The Stage 1d audit
(`eval/track_a/audits/label-proxy-audit-stage-1d.md`) found many of these constants to be label
proxies:
- a takeover's new device appears exactly two minutes after the credential change (LP-09);
- a device farm's repeat payment comes exactly one minute later (LP-17);
- no episode starts in the window's last three days (LP-14);
- planted amounts are uniform bands of the account's typical amount (LP-23).

None of these is part of what the scenario *is*: none is named in `docs/FRAUD_SCENARIOS.md`, and no
signature test asserts it.

**Q1 blocked every fix.** The eval-v2 draft's decision Q1 froze "scenario definitions", which, read
literally, forbids removing them. Freezing too little has the opposite danger: the benchmark drifts
until per-pattern results stop meaning anything.

**`LPC-5` needed the line drawn precisely** (`eval/track_a/criteria/lpc-5.md`), because it may demand
parity only where the scenario itself is not the reason for a difference.

## Decision

### 1. What a scenario is

A scenario's identity is exactly four things:
1. its **causal mechanism** — what the fraudster does;
2. its **required causal relationships** — the events and links the mechanism creates, which its
   causal keys claim (ADR-0030);
3. its **documented behavioural signature** (`docs/FRAUD_SCENARIOS.md` §3);
4. its **scenario-specific acceptance tests** — the signature tests in `tests/unit/test_scenarios.py`,
   and the per-instance invariants of `LPC-5` §13 S7a.

Changing any of these is a scenario change: a catalogue edit, a new `fraud_scenario_config_digest`
and a new dataset version (`docs/FRAUD_SCENARIOS.md` §5).

### 2. What a scenario is not

These are nuisance parameters, not part of a scenario's identity, unless the scenario's documentation
names them:
- exact timestamps and offsets;
- amounts, unless the signature documents an amount region;
- merchant choice, unless documented;
- position in the window;
- a non-required entry mode;
- formatting and precision;
- tie ordering unrelated to the mechanism.

**Under the eval-v2 gate a nuisance parameter may be randomised, subject to two conditions:**
- **Reproducibility.** The randomisation stays reproducible from the seed and the manifest: every draw
  comes from a keyed substream, so it depends on neither generation order nor labels.
- **No accidental signal.** A randomised parameter must not create a signal another scenario is defined
  by (see G3).

eval-v1 is untouched: its bytes, its labels and its recorded causal keys.

### 3. Corrections this covers

- **Retained:** M4 (time of day), M5 (merchant popularity) and M6 (channel).
- **Approved as nuisance randomisation:**
  - N6: placement anywhere in the window;
  - N7: takeover change-to-`FIRST_SEEN` offset;
  - N8: device-farm spacing;
  - N9: amounts.

### 4. Generator decisions frozen with this rule

These are recorded as `LPC-5` revision 2, §11, each with its check.

- **G1 — amounts.**
  - A scenario whose signature documents ordinary amounts, or none, draws from the account's own
    sampler: `VELOCITY_ATTACK` and `UNUSUAL_LOCATION_DEVICE` restricted to their documented regions,
    the others unrestricted.
  - `ACCOUNT_TAKEOVER` and the card-testing payoff draw from the same sampler inside "well above
    profile".
  - Card-testing probes and `ANOMALOUS_HIGH_VALUE` keep their documented absolute and far-beyond
    mechanisms.
- **G3 — only `IMPOSSIBLE_TRAVEL` is impossible.** Takeovers and unusual-location transactions are
  placed so that they imply no more than 900 km/h to the account's neighbouring transactions.
- **G4 — no guaranteed device novelty.**
  - Card testing uses one device, drawn the way the account's legitimate payments are.
  - Credential stuffing's logins share one device (the documented `DEVICE_SHARING`); its transacting
    accounts pay from their own devices.
  - Velocity attacks pay from the account's own devices (added by `LPC-5` revision 3, 2026-09-14): the
    signature documents a burst, not device multiplicity.
- **G5 — spacing.** Takeover, fraud-ring and device-farm transactions take independent uniform times
  inside their documented spans, because no burst is documented for them.
- **G6 — merchant collusion through structure.**
  - One price per instance.
  - Payers are drawn among accounts for whom that price is ordinary.
  - The signal is relational: many unrelated accounts paying one merchant the same amount.
- **G2 — coverage floor.**
  - Any pattern with fewer than 20 instances after the weighted mix is topped up to 20.
  - The natural and added counts are recorded.
  - A report on a topped-up dataset says its mix is a coverage floor, not natural prevalence.

### 5. Causal keys under the gate

These follow ADR-0030 rule 1: a key is listed only if the injection creates the signal.

| Scenario | eval-v1 (unchanged) | eval-v2 |
|---|---|---|
| `CREDENTIAL_STUFFING` | `IP_REPUTATION`, `DEVICE_SHARING`, `IDENTITY_CHANGE` | `AUTHENTICATION_ANOMALY`, `IP_REPUTATION`, `DEVICE_SHARING` |
| `CARD_TESTING` | `VELOCITY`, `AMOUNT_ANOMALY`, `MCC_ANOMALY`, `DEVICE_SHARING` | `VELOCITY`, `AMOUNT_ANOMALY`, `MCC_ANOMALY` |
| every other scenario | as listed in `docs/FRAUD_SCENARIOS.md` | unchanged |

- **Credential stuffing.** The mechanism emits failed and successful logins, and never an identity
  change (Q4e set), so `IDENTITY_CHANGE` was never causal. A login-activity signal needs a name, and
  the vocabulary has none: `EvidenceKind.AUTHENTICATION_ANOMALY` is added with the generator change.
  An enum addition is compatible; its consumers (Phase 6 onward) have not been built.
- **Card testing.** Once G4 draws the card-testing device from the account's own devices, the
  mechanism no longer creates device sharing. Keeping the key would make it uncausal.

  *Confirmed by the user on 2026-09-14 (Step 9 decisions), on the condition that the documented
  behaviour stays: probing "from one device" is unchanged and checked per instance (`LPC-5` S7a). Only
  the classification changes, because one device on one account is not device sharing.*
- **Documentation.** `docs/FRAUD_SCENARIOS.md` records both key sets per dataset version when the
  generator change lands. The catalogue test compares the code's gated and ungated key sets with it.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Keep eval-v1's constants: definitions unchanged, literally | Each constant is a label proxy measured in the Stage 1d audit. A detector would learn the generator rather than the fraud. |
| Treat everything a scenario does as its identity | Makes every nuisance constant untouchable, so no proxy could ever be removed without a new scenario. |
| Randomise freely, including undocumented behaviour | The benchmark drifts: per-pattern metrics compare different typologies across dataset versions, and a scenario could lose the signal its keys claim. |
| Allowlist the undocumented signals instead of removing them | Documents generator artefacts as fraud behaviour. `LPC-5` §6.1 admits only explicit catalogue citations. |
| Keep `DEVICE_SHARING` on card testing and keep a shared attacker device | A shared device is new to each victim, which is the device novelty the user decided not to guarantee (G4). |

## Consequences

**Positive.**
- Proxies that came from nuisance constants can be removed without a scenario change.
- `LPC-5` has a precise line between signal it must preserve and artefacts it must remove.
- Ground-truth keys stay causal in both dataset versions.

**Negative.**
- Per-scenario results on eval-v1 and eval-v2 are not comparable without a manifest-diff note:
  timing, amounts, devices and two key sets differ.
- The catalogue carries per-version key sets.
- The coverage floor distorts natural prevalence for rare patterns, disclosed wherever it applies.
- Adding an evidence kind touches the vocabulary later phases route on.

**Risks.**
- **A nuisance parameter turns out to be a signal the catalogue should have documented.** The route is
  a catalogue revision and a new dataset version, never a quiet exception. `LPC-5` §18 lists the
  candidates the code reading predicts.
- **Randomisation changes a scenario's signature statistics enough to fail its acceptance tests.** The
  S7a invariants and the signature tests catch it, and they run under the gate.

## Status

Proposed
