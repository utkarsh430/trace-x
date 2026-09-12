"""Domain enumerations.

Two families, and the difference is a security boundary:

**Wire enums** travel inside event payloads. `docs/EVENT_CONTRACTS.md` §4 allows
adding a value as a compatible change *provided consumers have an UNKNOWN
branch*, so every wire enum carries `UNKNOWN` and consumers degrade instead of
crashing on a value a newer producer emitted.

**Closed enums** are computed inside TRACE-X and never parsed from a peer.
They deliberately have **no** `UNKNOWN` member. Adding one to `ActionType`
would punch a hole straight through the action-safety control: the guarantee in
`docs/SECURITY.md` §5.5 is that a model *cannot* emit an action outside the
enum, and an UNKNOWN member would give it somewhere to land. The same reasoning
applies to `TrustTier` — a value whose tier is unknown must fail closed, not
travel as UNKNOWN. `tests/unit/test_domain_enums.py` asserts this split.
"""

from __future__ import annotations

from enum import StrEnum

# ============================================================ wire enums ====


class TransactionChannel(StrEnum):
    """How the transaction was presented."""

    CARD_PRESENT = "CARD_PRESENT"
    CARD_NOT_PRESENT = "CARD_NOT_PRESENT"
    ATM = "ATM"
    RECURRING = "RECURRING"
    UNKNOWN = "UNKNOWN"


class EntryMode(StrEnum):
    """How the card details were captured."""

    CHIP = "CHIP"
    CONTACTLESS = "CONTACTLESS"
    MAGSTRIPE = "MAGSTRIPE"
    MANUAL = "MANUAL"
    ECOMMERCE = "ECOMMERCE"
    TOKEN = "TOKEN"  # noqa: S105 - entry mode, not a credential
    UNKNOWN = "UNKNOWN"


class IdentityEventType(StrEnum):
    """A change to the account holder's identity or credentials."""

    # Both suppressions are for the same false positive from two different
    # tools: the member name looks credential-ish, and its value is its own
    # name. Suppressed inline rather than by excluding the file, so a real
    # secret added to this module later is still caught.
    PASSWORD_CHANGE = "PASSWORD_CHANGE"  # noqa: S105  # pragma: allowlist secret
    EMAIL_CHANGE = "EMAIL_CHANGE"
    PHONE_CHANGE = "PHONE_CHANGE"
    ADDRESS_CHANGE = "ADDRESS_CHANGE"
    MFA_RESET = "MFA_RESET"
    MFA_ENROLLED = "MFA_ENROLLED"
    LOGIN_FAILED = "LOGIN_FAILED"
    LOGIN_SUCCEEDED = "LOGIN_SUCCEEDED"
    UNKNOWN = "UNKNOWN"


class DeviceEventType(StrEnum):
    """A change in the device fingerprint an account presents."""

    FIRST_SEEN = "FIRST_SEEN"
    FINGERPRINT_CHANGED = "FINGERPRINT_CHANGED"
    ATTRIBUTE_CHANGED = "ATTRIBUTE_CHANGED"
    UNKNOWN = "UNKNOWN"


class AuthorizationOutcome(StrEnum):
    """What the upstream authorization system did with the transaction."""

    APPROVED = "APPROVED"
    DECLINED = "DECLINED"
    REVERSED = "REVERSED"
    UNKNOWN = "UNKNOWN"


# ========================================================== closed enums ====


class TrustTier(StrEnum):
    """Provenance tier of a value (docs/SECURITY.md §2).

    Stamped at ingestion and never removed. Deliberately has no UNKNOWN: a
    value whose provenance cannot be established must fail closed, not travel
    with a tier that means "we did not check".
    """

    SYSTEM = "SYSTEM"
    """Computed by TRACE-X: features, scores, aggregates."""

    DERIVED = "DERIVED"
    """Transformed from SYSTEM data."""

    UNTRUSTED = "UNTRUSTED"
    """Originated in an inbound event, or was persisted from one. Attacker-controlled."""


class RiskBand(StrEnum):
    """Synchronous scoring outcome band. HIGH and CRITICAL open an investigation."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class FeatureSource(StrEnum):
    """Whether a feature value has been reconciled by the warm path.

    Degradation must be visible, never silent: with the streaming profile down
    the hot path still works, and says so (ADR-0002).
    """

    ONLINE_ONLY = "ONLINE_ONLY"
    RECONCILED = "RECONCILED"


class FraudPattern(StrEnum):
    """The ten injected fraud scenarios (docs/FRAUD_SCENARIOS.md, ADR-0029).

    Ground truth. This never travels to the application or to an agent — it
    lives only in the `groundtruth` schema, which `trace_app` cannot read
    (ADR-0004). It is closed by design: a new pattern is a new dataset version
    with a new digest, never a value an old consumer must tolerate.
    """

    ACCOUNT_TAKEOVER = "ACCOUNT_TAKEOVER"
    CARD_TESTING = "CARD_TESTING"
    IMPOSSIBLE_TRAVEL = "IMPOSSIBLE_TRAVEL"
    VELOCITY_ATTACK = "VELOCITY_ATTACK"
    DEVICE_FARM = "DEVICE_FARM"
    FRAUD_RING = "FRAUD_RING"
    MERCHANT_COLLUSION = "MERCHANT_COLLUSION"
    CREDENTIAL_STUFFING = "CREDENTIAL_STUFFING"
    ANOMALOUS_HIGH_VALUE = "ANOMALOUS_HIGH_VALUE"
    UNUSUAL_LOCATION_DEVICE = "UNUSUAL_LOCATION_DEVICE"


class EvidenceKind(StrEnum):
    """Kinds of evidence agents produce and consume.

    This is the vocabulary the evidence-gap router maps over (ADR-0019) and the
    set `causal_evidence_keys` is drawn from, which makes evidence precision and
    recall set operations rather than an LLM judgement (docs/EVALUATION.md §5).
    Taken from the agent roster in docs/ARCHITECTURE.md §8.
    """

    # Behavioral
    SPEND_PROFILE = "SPEND_PROFILE"
    VELOCITY = "VELOCITY"
    AMOUNT_ANOMALY = "AMOUNT_ANOMALY"
    ACCOUNT_TENURE = "ACCOUNT_TENURE"
    # Device and identity
    DEVICE_NOVELTY = "DEVICE_NOVELTY"
    DEVICE_SHARING = "DEVICE_SHARING"
    IP_REPUTATION = "IP_REPUTATION"
    IDENTITY_CHANGE = "IDENTITY_CHANGE"
    # Merchant intelligence
    MERCHANT_RISK = "MERCHANT_RISK"
    MCC_ANOMALY = "MCC_ANOMALY"
    MERCHANT_PATTERN = "MERCHANT_PATTERN"
    # Graph
    GRAPH_CLUSTER = "GRAPH_CLUSTER"
    RING_SCORE = "RING_SCORE"
    LINK_PATH = "LINK_PATH"
    # Geography — named in the §8 worked example as a Skeptic refutation gap
    GEO_DISPERSION = "GEO_DISPERSION"
    # Historical
    HISTORICAL_MATCH = "HISTORICAL_MATCH"
    PRIOR_OUTCOME = "PRIOR_OUTCOME"
    # Adversarial review
    CHALLENGE = "CHALLENGE"
    ALTERNATIVE_EXPLANATION = "ALTERNATIVE_EXPLANATION"
    # Terminal
    DECISION = "DECISION"
    PROPOSED_ACTION = "PROPOSED_ACTION"


class ActionType(StrEnum):
    """Actions the Remediation agent may propose (Phase 8).

    Closed with no UNKNOWN, deliberately: structured output over this enum is
    what makes "a model cannot emit an out-of-enum action" true by construction
    (docs/SECURITY.md §5.5). Proposing is not executing — only the policy engine
    produces a ValidatedAction.
    """

    BLOCK_CARD = "BLOCK_CARD"
    FREEZE_ACCOUNT = "FREEZE_ACCOUNT"
    STEP_UP_AUTH = "STEP_UP_AUTH"
    REVERSE_TRANSACTION = "REVERSE_TRANSACTION"
    NOTIFY_CUSTOMER = "NOTIFY_CUSTOMER"
    FLAG_MERCHANT = "FLAG_MERCHANT"
    ADD_WATCHLIST = "ADD_WATCHLIST"
    OPEN_MANUAL_REVIEW = "OPEN_MANUAL_REVIEW"
    NO_ACTION = "NO_ACTION"


class RiskClassification(StrEnum):
    """Blast radius of a proposed action (docs/SECURITY.md §7).

    LOW auto-executes; MEDIUM and HIGH require human approval; PROHIBITED is
    rejected and audited.
    """

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    PROHIBITED = "PROHIBITED"


class Verdict(StrEnum):
    """An investigation's terminal finding.

    INSUFFICIENT_EVIDENCE is a first-class honest outcome, not a failure: a
    budget-exhausted investigation records it and goes to the human queue
    (CLAUDE.md §10.4).
    """

    FRAUD = "FRAUD"
    LEGITIMATE = "LEGITIMATE"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


# The enums that cross the wire, and therefore must tolerate an unseen value.
# Asserted in tests/unit/test_domain_enums.py, in both directions.
WIRE_ENUMS: frozenset[type[StrEnum]] = frozenset(
    {
        TransactionChannel,
        EntryMode,
        IdentityEventType,
        DeviceEventType,
        AuthorizationOutcome,
    }
)
