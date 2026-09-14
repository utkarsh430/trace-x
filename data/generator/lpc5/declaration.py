"""`LPC-5`, revision 2, as data.

The frozen criterion is `eval/track_a/criteria/lpc-5.md`. This module states its constants,
attributes, allowlist, maps and controls once. The checks in `data.generator.lpc5` read the
declaration instead of repeating its numbers.

`tests/unit/test_lpc5_declaration.py` pins every value here literally and pins the frozen file's
digest. It also checks every allowlist citation against `docs/FRAUD_SCENARIOS.md`. Changing either
side without a numbered revision fails the build.

Evaluation-side only. Nothing under `packages/`, `services/` or `mcp_servers/` may import this
package (asserted by test).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Final

from trace_core.contracts.topics import TX_AUTHORIZATION_V1
from trace_core.domain.enums import FraudPattern

CRITERION_ID: Final = "LPC-5"
REVISION: Final = 4
CRITERION_PATH: Final = "eval/track_a/criteria/lpc-5.md"
# The frozen document's public sha256, not a credential.
CRITERION_SHA256: Final = (
    "96dad012376f2b30aaca9450ed14c21a5803f9f3042e4bbb4f21ee672190ee28"  # pragma: allowlist secret
)

# ------------------------------------------------------------------ §3 statistics --------------
Z: Final = 1.645
ENRICHMENT_BOUND: Final = 2.0
MIN_EXCESS: Final = 0.02
LEGIT_SHARE_FLOOR: Final = 0.001
S1_TRIGGER: Final = 0.02
SUPPORT_MIN_ROWS: Final = 30
SUPPORT_MIN_ACCOUNTS: Final = 20
SUPPORT_MIN_SHARE: Final = 0.001
PRECISION_BOUND: Final = 0.25
STRATUM_MIN_ROWS: Final = 30
STRATUM_MIN_ACCOUNTS: Final = 20
S2_TOLERANCE: Final = 0.01
S3_SLICES: Final = 20
S3_MIN_LEGIT_SLICE_SHARE: Final = 0.01
S3_LOW_FACTOR: Final = 0.25
S3_HIGH_FACTOR: Final = 4.0
S3_SCENARIO_PARTS: Final = 3
S4_MAX_OFFSET_MS: Final = 86_400_000
S4_MIN_CLUSTERS: Final = 5
S4_TRIGGER: Final = 0.02
S4_CONCENTRATION: Final = 4.0
S4_WINDOW_HALF_BINS: Final = 1
S4_BACKGROUND_NEAR: Final = 2
S4_BACKGROUND_FAR: Final = 31
S5_TOLERANCE: Final = 0.02
S7_LIFT: Final = 2.0
S7_MARGIN: Final = 0.25
MIN_INSTANCES: Final = 20
R9_MIN_LEGIT_ROWS: Final = 30
R9_MIN_LEGIT_CLUSTERS: Final = 30
R9_MIN_PLANTED_TX_CLUSTERS: Final = 30

HOUR_MS: Final = 3_600_000
DAY_MS: Final = 86_400_000

LEGIT: Final = "LEGIT"
POOLED: Final = "POOLED"


class Population(StrEnum):
    TX = "TX"
    ID = "ID"
    DEV = "DEV"
    OUT = "OUT"


TOPICS: Final[Mapping[Population, str]] = MappingProxyType(
    {
        Population.TX: "tx.raw.v1",
        Population.ID: "identity.events.v1",
        Population.DEV: "device.events.v1",
    }
)
OUTCOME_STREAM: Final = TX_AUTHORIZATION_V1
"""The released topic of the OUT population (ADR-0049; released in Stage 2 step 7). A generation
without the stream -- eval-v1, or an ablated N12 -- has its outcome rows derived by ADR-0049 §7,
and a row under any other topic is refused."""


class Klass(StrEnum):
    BEHAVIOUR = "B"
    REPRESENTATION = "R"
    AVAILABILITY = "V"


@dataclass(frozen=True, slots=True)
class AttributeSpec:
    """One attribute of one population (§4)."""

    name: str
    klass: Klass
    values: tuple[str, ...] | None = None
    """The declared value labels; None where the values are observed (a released enum, an MCC)."""
    stratum: str | None = None
    """For a conditional attribute `name@stratum`: judged separately within each stratum value."""


# ------------------------------------------------------------------ §4 value labels ------------
HOURS: Final = tuple(str(h) for h in range(24))
DAYPARTS: Final = ("00-05", "06-11", "12-17", "18-23")
WEEKDAYS: Final = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
AMOUNT_VS_ACCOUNT: Final = ("<0.1", "[0.1,0.5)", "[0.5,2)", "[2,5)", "[5,20)", "≥20")
DECILES: Final = tuple(str(d) for d in range(10))
AMOUNT_Z: Final = (
    "<-2",
    "[-2,-1)",
    "[-1,-0.5)",
    "[-0.5,0)",
    "[0,0.5)",
    "[0.5,1)",
    "[1,2)",
    "≥2",
    "nonpositive",
)
DIGITS: Final = tuple(str(d) for d in range(10))
ROUNDNESS: Final = ("x1000", "x100", "x10", "other")
OUTCOME_EVENT: Final = ("APPROVED", "DECLINED", "none")
CARD: Final = ("first", "other")
MEMO: Final = ("absent-or-empty", "non-empty")
HABITUAL: Final = ("habitual", "unhabitual")
POPULARITY: Final = ("1", "2-3", "4-10", "11-30", "31-100", "101+")
COUNTRY_HOME: Final = ("home", "other")
MERCHANT_ACCOUNTS_1H: Final = ("1-4", "5-19", "20+")
MERCHANT_CV: Final = ("n<2", "<0.05", "[0.05,0.2)", "≥0.2")
SAME_AMOUNT_ACCOUNTS: Final = ("1", "2-4", "5+")
YES_NO: Final = ("yes", "no")
DISTANCE_HOME: Final = ("<5", "[5,25)", "[25,100)", "[100,500)", "≥500")
LOCATION_NOVEL: Final = ("no-prior", "<25", "[25,100)", "[100,500)", "≥500")
LEG_SPEED: Final = ("none", "<100", "[100,500)", "[500,1000)", "≥1000")
COORDINATE_DECIMALS: Final = ("≤2", "3-5", "≥6")
COORDINATE_REPEAT: Final = ("repeat", "new")
COORDINATE_HOME: Final = ("exact", "not")
GAP_PREV: Final = ("none", "<10s", "[10s,60s)", "[1,10)min", "[10,60)min", "[1,24)h", "≥24h")
TX_COUNT: Final = ("1", "2", "3-4", "5-9", "10-19", "20+")
DISTINCT_MERCHANTS: Final = ("1", "2", "3-4", "5-9", "10+")
DISTINCT_MCC: Final = ("1", "2", "3-4", "5+")
DISTINCT_FEW: Final = ("1", "2", "3+")
PRIOR_DECISIONS: Final = ("0", "1", "2-4", "5-9", "10+")
PRIOR_DECLINED_SHARE: Final = ("none", "0", "(0,0.4)", "≥0.4")
PROFILE_DEPTH: Final = ("0", "1-2", "3-19", "20-127", "128+")
ACCOUNT_ACTIVITY: Final = ("1-10", "11-20", "21-30", "31-50", "51+")
HOME: Final = ("home", "not")
TX_DEVICE_AGE: Final = ("first-use", "<1h", "[1h,24h)", "[1d,7d)", "≥7d")
SIDE_DEVICE_AGE: Final = ("first-reference", "<1h", "[1h,24h)", "[1d,7d)", "≥7d")
ACCOUNTS_DATASET: Final = ("1", "2", "3-5", "6+")
DEVICE_ACCOUNTS_24H: Final = ("1", "2", "3-4", "5+")
IP_ACCOUNTS_1H: Final = ("1", "2", "3-4", "5-9", "10+")
DATACENTER: Final = ("datacenter", "not")
IP_LOGIN_ACCOUNTS_1H: Final = ("0", "1", "2-4", "5+")
PRIOR_EVENTS: Final = ("identity-change", "device-event", "failed-login", "other-identity", "none")
HOURS_SINCE_CHANGE: Final = ("none", "<1h", "[1h,6h)", "[6h,24h)")
FAILED_LOGINS: Final = ("0", "1-4", "5-19", "20+")
SIDE_ACCOUNTS: Final = ("absent", "1", "2", "3-5", "6+")
EQUAL: Final = ("equal", "not")
DECISION_LATENCY: Final = (
    "<0",
    "[0,40)",
    "[40,100)",
    "[100,250)",
    "[250,500)",
    "[500,1000)",
    "≥1000",
)
IN_WINDOW: Final = ("inside", "outside")
SUBSECOND: Final = ("whole", "fractional")
TIMESTAMP_FORMAT: Final = ("ms-z", "other")
INGEST_LAG: Final = ("<0", "[0,40)", "[40,80)", "[80,120)", "[120,160)", "≥160")
TIE_RANK: Final = ("alone", "first", "later")
OK_NOT: Final = ("ok", "not")
UNIQUE: Final = ("unique", "shared")
MEMBERS: Final = ("1", "2", "3+")
AVAILABILITY: Final = ("AVAILABLE", "INSUFFICIENT_HISTORY", "UNAVAILABLE")

IDENTITY_CHANGE_TYPES: Final = frozenset(
    {"PASSWORD_CHANGE", "EMAIL_CHANGE", "PHONE_CHANGE", "ADDRESS_CHANGE", "MFA_RESET"}
)
LOGIN_TYPES: Final = frozenset({"LOGIN_FAILED", "LOGIN_SUCCEEDED"})
TIMESTAMP_PATTERN: Final = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"
GROUND_TRUTH_TOKENS: Final = (
    *(pattern.value for pattern in FraudPattern),
    "is_fraud",
    "fraud_pattern",
    "scenario_instance",
    "causal_evidence",
)
"""§8 S2c rule 2: no string field of any row may contain one of these, case-insensitively."""

B, R, V = Klass.BEHAVIOUR, Klass.REPRESENTATION, Klass.AVAILABILITY


def _common_r(*, keys_stratum: str | None, in_window: bool = True) -> tuple[AttributeSpec, ...]:
    specs = [
        AttributeSpec("subsecond", R, SUBSECOND),
        AttributeSpec("timestamp_format", R, TIMESTAMP_FORMAT),
        AttributeSpec("ingest_lag", R, INGEST_LAG),
        AttributeSpec("tie_rank", R, TIE_RANK),
        AttributeSpec("payload_keys", R, None, keys_stratum),
        AttributeSpec("identifier_formats", R, OK_NOT),
        AttributeSpec("envelope_constants", R),
        AttributeSpec("envelope_unique", R, UNIQUE),
        AttributeSpec("event_id_time", R, EQUAL),
        AttributeSpec("correlation_shape", R),
        AttributeSpec("trace_shape", R),
        AttributeSpec("correlation_members", R, MEMBERS),
        AttributeSpec("trace_members", R, MEMBERS),
    ]
    if in_window:
        specs.insert(0, AttributeSpec("in_window", R, IN_WINDOW))
    return tuple(specs)


def _calendar() -> tuple[AttributeSpec, ...]:
    return (
        AttributeSpec("hour", B, HOURS),
        AttributeSpec("daypart", B, DAYPARTS),
        AttributeSpec("weekday", B, WEEKDAYS),
    )


RELEASED_FEATURES: Final = (
    "account_tx_count_1m",
    "account_tx_count_5m",
    "account_tx_count_1h",
    "account_tx_count_24h",
    "account_amount_sum_1h",
    "card_tx_count_5m",
    "declined_ratio_1h",
    "account_distinct_merchants_1h",
    "account_distinct_mcc_5m",
    "account_distinct_devices_24h",
    "account_distinct_countries_24h",
    "device_distinct_accounts_24h",
    "ip_distinct_accounts_1h",
    "merchant_distinct_accounts_1h",
    "merchant_amount_cv_24h",
    "amount_zscore_vs_account",
    "account_tenure_days",
    "merchant_is_habitual",
    "mcc_is_habitual_for_account",
    "device_is_known_for_account",
    "distance_from_account_home_km",
    "geo_distance_from_last_km",
    "implied_speed_kmh_from_last",
    "seconds_since_last_transaction",
    "hours_since_identity_change",
    "failed_logins_1h",
)
"""The 26 released features of §4.6. A feature added later is not part of revision 2."""

AVAIL_PREFIX: Final = "avail:"

DEPENDS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "account_tx_count_1m": "tx_count_1m",
        "account_tx_count_5m": "tx_count_5m",
        "account_tx_count_1h": "tx_count_1h",
        "account_tx_count_24h": "tx_count_24h",
        "account_amount_sum_1h": "tx_count_1h",
        "card_tx_count_5m": "card_count_5m",
        "declined_ratio_1h": "prior_decisions_1h",
        "account_distinct_merchants_1h": "distinct_merchants_1h",
        "account_distinct_mcc_5m": "distinct_mcc_5m",
        "account_distinct_devices_24h": "distinct_devices_24h",
        "account_distinct_countries_24h": "distinct_countries_24h",
        "device_distinct_accounts_24h": "device_accounts_24h",
        "ip_distinct_accounts_1h": "ip_accounts_1h",
        "merchant_distinct_accounts_1h": "merchant_accounts_1h",
        "merchant_amount_cv_24h": "merchant_amount_cv_24h",
        "amount_zscore_vs_account": "profile_depth",
        "account_tenure_days": "profile_depth",
        "merchant_is_habitual": "profile_depth",
        "mcc_is_habitual_for_account": "profile_depth",
        "device_is_known_for_account": "profile_depth",
        "distance_from_account_home_km": "profile_depth",
        "geo_distance_from_last_km": "gap_prev",
        "implied_speed_kmh_from_last": "gap_prev",
        "seconds_since_last_transaction": "gap_prev",
        "hours_since_identity_change": "hours_since_identity_change",
        "failed_logins_1h": "failed_logins_1h",
    }
)

ATTRIBUTES: Final[Mapping[Population, tuple[AttributeSpec, ...]]] = MappingProxyType(
    {
        Population.TX: (
            *_calendar(),
            AttributeSpec("channel", B),
            AttributeSpec("entry_mode", B, None, "channel"),
            AttributeSpec("currency", B),
            AttributeSpec("amount_vs_account", B, AMOUNT_VS_ACCOUNT),
            AttributeSpec("amount_decile", B, DECILES),
            AttributeSpec("amount_z", B, AMOUNT_Z),
            AttributeSpec("amount_last_digit", R, DIGITS),
            AttributeSpec("amount_roundness", R, ROUNDNESS),
            AttributeSpec("outcome_field", R),
            AttributeSpec("outcome_event", B, OUTCOME_EVENT),
            AttributeSpec("card", B, CARD),
            AttributeSpec("user_agent", B),
            AttributeSpec("memo", B, MEMO),
            AttributeSpec("merchant_habitual", B, HABITUAL),
            AttributeSpec("merchant_popularity", B, POPULARITY, "merchant_habitual"),
            AttributeSpec("merchant_mcc", B),
            AttributeSpec("mcc_habitual", B, HABITUAL),
            AttributeSpec("merchant_country", B),
            AttributeSpec("merchant_country_home", B, COUNTRY_HOME),
            AttributeSpec("merchant_accounts_1h", B, MERCHANT_ACCOUNTS_1H),
            AttributeSpec("merchant_amount_cv_24h", B, MERCHANT_CV),
            AttributeSpec("merchant_same_amount_accounts_24h", B, SAME_AMOUNT_ACCOUNTS),
            AttributeSpec("shared_merchant_link", B, YES_NO),
            AttributeSpec("distance_home", B, DISTANCE_HOME),
            AttributeSpec("location_novel", B, LOCATION_NOVEL),
            AttributeSpec("leg_speed", B, LEG_SPEED),
            AttributeSpec("coordinate_decimals", R, COORDINATE_DECIMALS),
            AttributeSpec("coordinate_repeat", R, COORDINATE_REPEAT),
            AttributeSpec("coordinate_home", R, COORDINATE_HOME),
            AttributeSpec("gap_prev", B, GAP_PREV),
            AttributeSpec("tx_count_1m", B, TX_COUNT),
            AttributeSpec("tx_count_5m", B, TX_COUNT),
            AttributeSpec("tx_count_1h", B, TX_COUNT),
            AttributeSpec("tx_count_24h", B, TX_COUNT),
            AttributeSpec("card_count_5m", B, TX_COUNT),
            AttributeSpec("distinct_merchants_1h", B, DISTINCT_MERCHANTS),
            AttributeSpec("distinct_mcc_5m", B, DISTINCT_MCC),
            AttributeSpec("distinct_devices_24h", B, DISTINCT_FEW),
            AttributeSpec("distinct_countries_24h", B, DISTINCT_FEW),
            AttributeSpec("prior_decisions_1h", B, PRIOR_DECISIONS),
            AttributeSpec("prior_declined_share_1h", B, PRIOR_DECLINED_SHARE),
            AttributeSpec("profile_depth", B, PROFILE_DEPTH),
            AttributeSpec("account_activity", B, ACCOUNT_ACTIVITY),
            AttributeSpec("device_home", B, HOME),
            AttributeSpec("device_age", B, TX_DEVICE_AGE),
            AttributeSpec("device_account_tx", B, ACCOUNTS_DATASET),
            AttributeSpec("device_accounts", B, ACCOUNTS_DATASET),
            AttributeSpec("device_accounts_24h", B, DEVICE_ACCOUNTS_24H),
            AttributeSpec("ip_home", B, HOME),
            AttributeSpec("ip_accounts", B, ACCOUNTS_DATASET),
            AttributeSpec("ip_accounts_1h", B, IP_ACCOUNTS_1H),
            AttributeSpec("ip_datacenter", B, DATACENTER),
            AttributeSpec("ip_login_accounts_1h", B, IP_LOGIN_ACCOUNTS_1H),
            AttributeSpec("joint_link", B, YES_NO),
            AttributeSpec("prior_events_24h", B, PRIOR_EVENTS),
            AttributeSpec("hours_since_identity_change", B, HOURS_SINCE_CHANGE),
            AttributeSpec("failed_logins_1h", B, FAILED_LOGINS),
            *_common_r(keys_stratum=None),
            *(AttributeSpec(f"{AVAIL_PREFIX}{f}", V, AVAILABILITY) for f in RELEASED_FEATURES),
        ),
        Population.ID: (
            AttributeSpec("event_type", B),
            *_calendar(),
            AttributeSpec("user_agent", B),
            AttributeSpec("ip_datacenter", B, (*DATACENTER, "absent")),
            AttributeSpec("ip_login_accounts", B, SIDE_ACCOUNTS),
            AttributeSpec("device_home", B, (*HOME, "absent")),
            AttributeSpec("device_age", B, (*SIDE_DEVICE_AGE, "absent")),
            AttributeSpec("device_login_accounts", B, SIDE_ACCOUNTS),
            *_common_r(keys_stratum="event_type"),
        ),
        Population.DEV: (
            AttributeSpec("event_type", B),
            *_calendar(),
            AttributeSpec("platform", B),
            AttributeSpec("device_home", B, HOME),
            AttributeSpec("device_age", B, SIDE_DEVICE_AGE),
            AttributeSpec("platform_consistent", R, EQUAL),
            *_common_r(keys_stratum="event_type"),
        ),
        Population.OUT: (
            AttributeSpec("authorization_outcome", B),
            *_calendar(),
            AttributeSpec("decision_latency", R, DECISION_LATENCY),
            AttributeSpec("tx_link", R, ("one", "not")),
            AttributeSpec("account_match", R, EQUAL),
            AttributeSpec("transaction_time_match", R, EQUAL),
            *_common_r(keys_stratum=None, in_window=False),
        ),
    }
)


def attribute(population: Population, name: str) -> AttributeSpec:
    for spec in ATTRIBUTES[population]:
        if spec.name == name:
            return spec
    raise KeyError(f"{population} has no attribute {name!r}")


# ------------------------------------------------------------------ §6 allowlist ---------------
class Composition(StrEnum):
    JUDGED = "judged"
    DOCUMENTED = "documented"


class SourceKind(StrEnum):
    SIGNATURE = "S"
    NOTE = "N"
    KEY = "K"


@dataclass(frozen=True, slots=True)
class Source:
    kind: SourceKind
    fragments: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AllowRow:
    """One INTRINSIC_BEHAVIOURAL_SIGNAL entry (§6.4)."""

    row_id: str
    scenario: FraudPattern
    populations: tuple[Population, ...]
    attribute: str
    values: frozenset[str]
    e_min: float | None
    """The least lower bound S7b requires. None for an allowed effect with no minimum (§13,
    revision 3): the row keeps every other status and check."""
    sources: tuple[Source, ...]
    composition: Composition | None = None
    rare: frozenset[str] = frozenset()
    none: frozenset[str] = frozenset()
    consequences: Mapping[Population, tuple[str, ...]] = field(
        default_factory=lambda: MappingProxyType({})
    )
    consequence_rare: Mapping[str, frozenset[str]] = field(
        default_factory=lambda: MappingProxyType({})
    )
    """Exempt values of a consequence attribute, by attribute (§6.3, revision 3)."""
    consequence_none: Mapping[str, frozenset[str]] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def consequences_in(self, population: Population) -> tuple[str, ...]:
        return self.consequences.get(population, ())


def _s(*fragments: str) -> Source:
    return Source(SourceKind.SIGNATURE, fragments)


def _k(key: str) -> Source:
    return Source(SourceKind.KEY, (key,))


def _row(
    row_id: str,
    scenario: FraudPattern,
    populations: tuple[Population, ...],
    name: str,
    values: tuple[str, ...],
    e_min: float | None,
    sources: tuple[Source, ...],
    *,
    composition: Composition | None = None,
    rare: tuple[str, ...] = (),
    none: tuple[str, ...] = (),
    consequences: tuple[str, ...] | Mapping[Population, tuple[str, ...]] = (),
    consequence_rare: Mapping[str, tuple[str, ...]] | None = None,
    consequence_none: Mapping[str, tuple[str, ...]] | None = None,
) -> AllowRow:
    mapped = (
        dict(consequences)
        if isinstance(consequences, Mapping)
        else ({populations[0]: consequences} if consequences else {})
    )
    return AllowRow(
        row_id=row_id,
        scenario=scenario,
        populations=populations,
        attribute=name,
        values=frozenset(values),
        e_min=e_min,
        sources=sources,
        composition=composition,
        rare=frozenset(rare),
        none=frozenset(none),
        consequences=MappingProxyType(mapped),
        consequence_rare=MappingProxyType(
            {name: frozenset(v) for name, v in (consequence_rare or {}).items()}
        ),
        consequence_none=MappingProxyType(
            {name: frozenset(v) for name, v in (consequence_none or {}).items()}
        ),
    )


TX, ID, DEV, OUT = Population.TX, Population.ID, Population.DEV, Population.OUT
JUDGED, DOCUMENTED = Composition.JUDGED, Composition.DOCUMENTED
FP = FraudPattern

_BURST_TX = ("2", "3-4", "5-9", "10-19", "20+")
_CT_BURST = _s("Many sub-threshold authorisations", "inside a few minutes")
_VA_BURST = _s("A burst of transactions on one account far above its own short-window baseline")
_FR_POOL = _s("Several accounts sharing a small pool of devices and IPs")
_ATO_DEVICE = _s("a device the account has never used")
_AHV_AMOUNT = _s("far beyond the account's own amount distribution")
_ULD_DEVICE = _s("on a never-seen device")
_DF_MANY = _s("One device fingerprint used by many accounts")

ALLOWLIST: Final[tuple[AllowRow, ...]] = (
    # --- ACCOUNT_TAKEOVER (§3.1)
    _row(
        "ATO-1",
        FP.ACCOUNT_TAKEOVER,
        (TX,),
        "prior_events_24h",
        ("identity-change",),
        0.80,
        (_s("An identity change, then within hours"),),
    ),
    _row(
        "ATO-2",
        FP.ACCOUNT_TAKEOVER,
        (TX,),
        "hours_since_identity_change",
        ("<1h", "[1h,6h)"),
        0.80,
        (_s("then within hours"),),
        composition=DOCUMENTED,
        rare=("<1h", "[1h,6h)"),
    ),
    _row(
        "ATO-3",
        FP.ACCOUNT_TAKEOVER,
        (ID,),
        "event_type",
        tuple(sorted(IDENTITY_CHANGE_TYPES)),
        0.80,
        (_s("An identity change"),),
        composition=JUDGED,
        rare=("ADDRESS_CHANGE", "EMAIL_CHANGE", "PHONE_CHANGE"),
    ),
    _row(
        "ATO-4",
        FP.ACCOUNT_TAKEOVER,
        (TX, ID),
        "device_home",
        ("not",),
        0.80,
        (_ATO_DEVICE,),
        consequences={
            TX: (
                "device_accounts",
                "device_accounts_24h",
                "distinct_devices_24h",
                "device_account_tx",
            ),
            ID: ("device_login_accounts",),
        },
    ),
    _row(
        "ATO-5",
        FP.ACCOUNT_TAKEOVER,
        (TX, ID),
        "device_age",
        ("first-use", "first-reference", "<1h", "[1h,24h)"),
        0.80,
        (_ATO_DEVICE,),
        composition=JUDGED,
    ),
    _row(
        "ATO-6",
        FP.ACCOUNT_TAKEOVER,
        (TX,),
        "amount_vs_account",
        ("[2,5)", "[5,20)", "≥20"),
        0.80,
        (_s("spending well above profile"),),
        composition=JUDGED,
        rare=("≥20",),
        consequences=("amount_decile", "amount_z"),
    ),
    _row(
        "ATO-7",
        FP.ACCOUNT_TAKEOVER,
        (TX,),
        "merchant_habitual",
        ("unhabitual",),
        0.80,
        (_s("at unhabitual merchants"),),
        consequences=("mcc_habitual", "merchant_mcc"),
    ),
    _row(
        "ATO-8",
        FP.ACCOUNT_TAKEOVER,
        (TX,),
        "distance_home",
        ("[100,500)", "≥500"),
        0.80,
        (_s("away from home"),),
        composition=JUDGED,
        consequences=(
            "location_novel",
            "leg_speed",
            "merchant_country_home",
            "distinct_countries_24h",
        ),
    ),
    # --- CARD_TESTING (§3.2)
    _row(
        "CT-1",
        FP.CARD_TESTING,
        (TX,),
        "tx_count_1m",
        _BURST_TX,
        0.30,
        (_CT_BURST,),
        composition=DOCUMENTED,
        none=("3-4", "5-9", "10-19", "20+"),
    ),
    _row(
        "CT-2",
        FP.CARD_TESTING,
        (TX,),
        "tx_count_5m",
        _BURST_TX,
        0.60,
        (_CT_BURST,),
        composition=DOCUMENTED,
        rare=("3-4",),
        none=("5-9", "10-19", "20+"),
    ),
    _row(
        "CT-3",
        FP.CARD_TESTING,
        (TX,),
        "tx_count_1h",
        _BURST_TX,
        0.80,
        (_CT_BURST,),
        composition=DOCUMENTED,
        none=("5-9", "10-19", "20+"),
        consequences=("tx_count_24h",),
        consequence_rare={"tx_count_24h": ("10-19",)},
    ),
    _row(
        "CT-4",
        FP.CARD_TESTING,
        (TX,),
        "card_count_5m",
        _BURST_TX,
        0.60,
        (_CT_BURST,),
        composition=DOCUMENTED,
        rare=("3-4",),
        none=("5-9", "10-19", "20+"),
    ),
    _row(
        "CT-5",
        FP.CARD_TESTING,
        (TX,),
        "gap_prev",
        ("<10s", "[10s,60s)", "[1,10)min"),
        0.50,
        (_s("inside a few minutes"),),
        composition=JUDGED,
    ),
    _row(
        "CT-6",
        FP.CARD_TESTING,
        (TX,),
        "distinct_merchants_1h",
        ("2", "3-4", "5-9", "10+"),
        0.60,
        (_s("across many distinct merchants"),),
        composition=DOCUMENTED,
        none=("5-9", "10+"),
        consequences=("merchant_habitual", "merchant_popularity", "merchant_accounts_1h"),
    ),
    _row(
        "CT-7",
        FP.CARD_TESTING,
        (TX,),
        "distinct_mcc_5m",
        ("2", "3-4", "5+"),
        0.40,
        (_s("many distinct merchants and MCCs", "inside a few minutes"),),
        composition=DOCUMENTED,
        rare=("3-4",),
        none=("5+",),
        consequences=("merchant_mcc", "mcc_habitual"),
    ),
    _row(
        "CT-8",
        FP.CARD_TESTING,
        (TX,),
        "amount_vs_account",
        ("<0.1", "[0.1,0.5)"),
        0.60,
        (_s("Many sub-threshold authorisations"),),
        composition=DOCUMENTED,
        consequences=("amount_decile", "amount_z"),
    ),
    _row(
        "CT-9",
        FP.CARD_TESTING,
        (OUT,),
        "authorization_outcome",
        ("DECLINED",),
        0.20,
        (_s("a substantial share declined"),),
    ),
    _row(
        "CT-10",
        FP.CARD_TESTING,
        (TX,),
        "prior_decisions_1h",
        ("1", "2-4", "5-9", "10+"),
        0.60,
        (_CT_BURST,),
        composition=DOCUMENTED,
        none=("5-9", "10+"),
    ),
    _row(
        "CT-11",
        FP.CARD_TESTING,
        (TX,),
        "prior_declined_share_1h",
        ("0", "(0,0.4)", "≥0.4"),
        0.60,
        (_s("a substantial share declined"),),
        composition=DOCUMENTED,
        rare=("(0,0.4)",),
    ),
    # --- IMPOSSIBLE_TRAVEL (§3.3)
    _row(
        "IT-1",
        FP.IMPOSSIBLE_TRAVEL,
        (TX,),
        "channel",
        ("CARD_PRESENT",),
        0.80,
        (_s("Two card-present transactions"),),
    ),
    _row(
        "IT-2",
        FP.IMPOSSIBLE_TRAVEL,
        (TX,),
        "leg_speed",
        ("[500,1000)", "≥1000"),
        0.30,
        (_s("implies a speed no commercial travel achieves"),),
        composition=DOCUMENTED,
        none=("≥1000",),
        consequences=("gap_prev",),
    ),
    _row(
        "IT-3",
        FP.IMPOSSIBLE_TRAVEL,
        (TX,),
        "location_novel",
        ("[100,500)", "≥500"),
        0.30,
        (_s("great-circle distance"), _k("GEO_DISPERSION")),
        composition=JUDGED,
        consequences=("distance_home", "merchant_country_home", "distinct_countries_24h"),
    ),
    # --- VELOCITY_ATTACK (§3.4)
    _row(
        "VA-1",
        FP.VELOCITY_ATTACK,
        (TX,),
        "tx_count_1m",
        _BURST_TX,
        0.30,
        (_VA_BURST,),
        composition=DOCUMENTED,
        none=("3-4", "5-9", "10-19", "20+"),
    ),
    _row(
        "VA-2",
        FP.VELOCITY_ATTACK,
        (TX,),
        "tx_count_5m",
        _BURST_TX,
        0.60,
        (_VA_BURST,),
        composition=DOCUMENTED,
        rare=("3-4",),
        none=("5-9", "10-19", "20+"),
    ),
    _row(
        "VA-3",
        FP.VELOCITY_ATTACK,
        (TX,),
        "tx_count_1h",
        _BURST_TX,
        0.80,
        (_VA_BURST,),
        composition=DOCUMENTED,
        none=("5-9", "10-19", "20+"),
        consequences=("tx_count_24h",),
        consequence_rare={"tx_count_24h": ("10-19",)},
        consequence_none={"tx_count_24h": ("20+",)},
    ),
    _row(
        "VA-4",
        FP.VELOCITY_ATTACK,
        (TX,),
        "card_count_5m",
        _BURST_TX,
        0.60,
        (_VA_BURST,),
        composition=DOCUMENTED,
        rare=("3-4",),
        none=("5-9", "10-19", "20+"),
    ),
    _row(
        "VA-5",
        FP.VELOCITY_ATTACK,
        (TX,),
        "gap_prev",
        ("<10s", "[10s,60s)", "[1,10)min"),
        0.50,
        (_VA_BURST,),
        composition=JUDGED,
    ),
    _row(
        "VA-6",
        FP.VELOCITY_ATTACK,
        (TX,),
        "prior_decisions_1h",
        ("1", "2-4", "5-9", "10+"),
        0.60,
        (_VA_BURST,),
        composition=DOCUMENTED,
        none=("5-9", "10+"),
        consequences=("prior_declined_share_1h",),
    ),
    # --- DEVICE_FARM (§3.5)
    _row(
        "DF-1",
        FP.DEVICE_FARM,
        (TX,),
        "device_accounts",
        ("3-5", "6+"),
        0.80,
        (_DF_MANY,),
        composition=DOCUMENTED,
    ),
    _row(
        "DF-2",
        FP.DEVICE_FARM,
        (TX,),
        "device_accounts_24h",
        ("3-4", "5+"),
        0.50,
        (_DF_MANY,),
        composition=DOCUMENTED,
        rare=("5+",),
    ),
    _row(
        "DF-3",
        FP.DEVICE_FARM,
        (TX,),
        "device_account_tx",
        ("1", "2"),
        0.80,
        (_s("each account transacting only once or twice"),),
        composition=DOCUMENTED,
    ),
    _row(
        "DF-4",
        FP.DEVICE_FARM,
        (TX,),
        "device_home",
        ("not",),
        0.80,
        (_k("DEVICE_NOVELTY"),),
        consequences=("distinct_devices_24h",),
    ),
    _row(
        "DF-5",
        FP.DEVICE_FARM,
        (TX,),
        "device_age",
        ("first-use", "<1h"),
        None,
        (_k("DEVICE_NOVELTY"),),
        composition=JUDGED,
    ),
    # --- FRAUD_RING (§3.6)
    _row(
        "FR-1",
        FP.FRAUD_RING,
        (TX,),
        "device_accounts",
        ("3-5", "6+"),
        0.60,
        (_FR_POOL,),
        composition=JUDGED,
        consequences=(
            "device_home",
            "device_age",
            "device_accounts_24h",
            "distinct_devices_24h",
            "device_account_tx",
        ),
        consequence_rare={"device_accounts_24h": ("5+",)},
    ),
    _row(
        "FR-2",
        FP.FRAUD_RING,
        (TX,),
        "ip_accounts",
        ("6+",),
        0.50,
        (_FR_POOL,),
        consequences=("ip_home", "ip_accounts_1h"),
    ),
    _row(
        "FR-3",
        FP.FRAUD_RING,
        (TX,),
        "shared_merchant_link",
        ("yes",),
        0.60,
        (_s("converging on a shared merchant set"),),
        consequences=(
            "merchant_habitual",
            "merchant_popularity",
            "merchant_mcc",
            "mcc_habitual",
            "merchant_country",
            "merchant_country_home",
            "merchant_accounts_1h",
        ),
    ),
    _row("FR-4", FP.FRAUD_RING, (TX,), "joint_link", ("yes",), 0.60, (_FR_POOL,)),
    # --- MERCHANT_COLLUSION (§3.7)
    _row(
        "MC-1",
        FP.MERCHANT_COLLUSION,
        (TX,),
        "merchant_same_amount_accounts_24h",
        ("5+",),
        0.50,
        (_s("unusually uniform amounts from many unrelated accounts"),),
        consequences=("merchant_amount_cv_24h",),
    ),
    _row(
        "MC-2",
        FP.MERCHANT_COLLUSION,
        (TX,),
        "amount_decile",
        ("8", "9"),
        0.80,
        (_s("high, unusually uniform amounts"),),
        composition=JUDGED,
    ),
    _row(
        "MC-4",
        FP.MERCHANT_COLLUSION,
        (TX,),
        "merchant_habitual",
        ("unhabitual",),
        0.80,
        (_s("One merchant", "from many unrelated accounts"),),
        consequences=("merchant_popularity",),
    ),
    _row(
        "MC-5",
        FP.MERCHANT_COLLUSION,
        (TX,),
        "mcc_habitual",
        ("unhabitual",),
        None,
        (_k("MCC_ANOMALY"),),
        consequences=("merchant_mcc",),
    ),
    # --- CREDENTIAL_STUFFING (§3.8)
    _row(
        "CS-1",
        FP.CREDENTIAL_STUFFING,
        (ID,),
        "event_type",
        ("LOGIN_FAILED",),
        0.60,
        (_s("A burst of failed logins"),),
    ),
    _row(
        "CS-2",
        FP.CREDENTIAL_STUFFING,
        (ID,),
        "ip_datacenter",
        ("datacenter",),
        0.80,
        (_s("from a small datacenter IP pool"),),
    ),
    _row(
        "CS-3",
        FP.CREDENTIAL_STUFFING,
        (ID,),
        "ip_login_accounts",
        ("6+",),
        0.80,
        (_s("across many unrelated accounts"),),
    ),
    _row(
        "CS-4",
        FP.CREDENTIAL_STUFFING,
        (ID,),
        "device_login_accounts",
        ("3-5", "6+"),
        0.50,
        (_k("DEVICE_SHARING"),),
        composition=DOCUMENTED,
        consequences=("device_home", "device_age"),
    ),
    _row(
        "CS-5",
        FP.CREDENTIAL_STUFFING,
        (TX,),
        "ip_datacenter",
        ("datacenter",),
        0.80,
        (_k("IP_REPUTATION"),),
        consequences=("ip_home", "ip_accounts", "ip_accounts_1h"),
    ),
    _row(
        "CS-6",
        FP.CREDENTIAL_STUFFING,
        (TX,),
        "ip_login_accounts_1h",
        ("2-4", "5+"),
        0.50,
        (_s("A burst of failed logins across many unrelated accounts"),),
        composition=DOCUMENTED,
        none=("5+",),
    ),
    _row(
        "CS-7",
        FP.CREDENTIAL_STUFFING,
        (TX,),
        "prior_events_24h",
        ("other-identity", "failed-login"),
        0.60,
        (_s("a minority succeeding and transacting immediately"),),
        composition=JUDGED,
        consequences=("failed_logins_1h",),
    ),
    # --- ANOMALOUS_HIGH_VALUE (§3.9)
    _row(
        "AHV-1",
        FP.ANOMALOUS_HIGH_VALUE,
        (TX,),
        "amount_vs_account",
        ("≥20",),
        0.80,
        (_AHV_AMOUNT,),
        rare=("≥20",),
    ),
    _row("AHV-2", FP.ANOMALOUS_HIGH_VALUE, (TX,), "amount_z", ("≥2",), 0.80, (_AHV_AMOUNT,)),
    _row("AHV-3", FP.ANOMALOUS_HIGH_VALUE, (TX,), "amount_decile", ("9",), 0.80, (_AHV_AMOUNT,)),
    _row(
        "AHV-4",
        FP.ANOMALOUS_HIGH_VALUE,
        (TX,),
        "mcc_habitual",
        ("unhabitual",),
        0.80,
        (_s("at a merchant category it never uses"),),
        consequences=("merchant_habitual", "merchant_popularity", "merchant_mcc"),
    ),
    # --- UNUSUAL_LOCATION_DEVICE (§3.10)
    _row(
        "ULD-1",
        FP.UNUSUAL_LOCATION_DEVICE,
        (TX,),
        "device_home",
        ("not",),
        0.80,
        (_ULD_DEVICE,),
        consequences=(
            "device_account_tx",
            "device_accounts",
            "device_accounts_24h",
            "distinct_devices_24h",
        ),
    ),
    _row(
        "ULD-2",
        FP.UNUSUAL_LOCATION_DEVICE,
        (TX,),
        "device_age",
        ("first-use",),
        0.80,
        (_ULD_DEVICE,),
    ),
    _row(
        "ULD-3",
        FP.UNUSUAL_LOCATION_DEVICE,
        (TX,),
        "location_novel",
        ("[100,500)", "≥500"),
        0.80,
        (_s("in a never-seen place"),),
        composition=JUDGED,
        consequences=(
            "distance_home",
            "leg_speed",
            "merchant_country_home",
            "distinct_countries_24h",
        ),
    ),
)


class EpisodeKind(StrEnum):
    BEHAVIOURAL = "behavioural"
    INCIDENTAL = "incidental"
    """A consequence of the episode's documented span, never a fraud signature (§6.6)."""


@dataclass(frozen=True, slots=True)
class EpisodeConsequence:
    """§6.6 (revision 3): attributes a documented multi-event episode changes as a whole.

    Not an allowlist row and not a signature: no `E_min`, no composition, no S7b. The status skips
    S1-U's precision check, R7 and S1-B's sweep for the scenario; S1-U's support check stays."""

    row_id: str
    scenario: FraudPattern
    populations: tuple[Population, ...]
    attributes: frozenset[str]
    source: Source
    kind: EpisodeKind


EPISODE_CONSEQUENCES: Final[tuple[EpisodeConsequence, ...]] = (
    EpisodeConsequence(
        "ATO-E1",
        FP.ACCOUNT_TAKEOVER,
        (TX,),
        frozenset(
            {
                "tx_count_1h",
                "tx_count_24h",
                "gap_prev",
                "distinct_merchants_1h",
                "prior_decisions_1h",
                "prior_declined_share_1h",
            }
        ),
        _s("then within hours"),
        EpisodeKind.BEHAVIOURAL,
    ),
    EpisodeConsequence(
        "ATO-E2",
        FP.ACCOUNT_TAKEOVER,
        (TX, OUT),
        frozenset({"hour", "daypart"}),
        _s("then within hours"),
        EpisodeKind.INCIDENTAL,
    ),
    EpisodeConsequence(
        "IT-E1",
        FP.IMPOSSIBLE_TRAVEL,
        (TX, OUT),
        frozenset({"hour", "daypart"}),
        _s("great-circle distance over elapsed time"),
        EpisodeKind.INCIDENTAL,
    ),
)


@dataclass(frozen=True, slots=True)
class DocumentedConsequence:
    """§6.7 (revision 4): specific values a documented mechanism produces in attributes no row of
    the scenario names.

    Not an allowlist row and not a signature: no `E_min`, no composition, no S7b. With no `within`
    rows the values are DOCUMENTED CONSEQUENCES of the scenario (§6.3); with `within` rows they
    skip S1-B's sweep inside those rows' strata only. Every other value of the attribute keeps its
    status."""

    row_id: str
    scenario: FraudPattern
    population: Population
    attributes: frozenset[str]
    values: frozenset[str]
    source: Source
    within: frozenset[str] = frozenset()
    rare: frozenset[str] = frozenset()
    none: frozenset[str] = frozenset()


def _dc(
    row_id: str,
    scenario: FraudPattern,
    population: Population,
    attributes: tuple[str, ...],
    values: tuple[str, ...],
    source: Source,
    *,
    within: tuple[str, ...] = (),
    rare: tuple[str, ...] = (),
    none: tuple[str, ...] = (),
) -> DocumentedConsequence:
    return DocumentedConsequence(
        row_id=row_id,
        scenario=scenario,
        population=population,
        attributes=frozenset(attributes),
        values=frozenset(values),
        source=source,
        within=frozenset(within),
        rare=frozenset(rare),
        none=frozenset(none),
    )


_IT_PAIR = _s("Two card-present transactions")
_ATO_NEW_DEVICE = _s("then within hours a device the account has never used")
_CS_LOGINS = _s("A burst of failed logins across many unrelated accounts")
_CS_POOL = _s("from a small datacenter IP pool")
_DF_ONCE = _s("each account transacting only once or twice")
_MC_AMOUNTS = _s("high, unusually uniform amounts")
_AVAILABLE = ("AVAILABLE",)

DOCUMENTED_CONSEQUENCES: Final[tuple[DocumentedConsequence, ...]] = (
    _dc(
        "VA-C1",
        FP.VELOCITY_ATTACK,
        TX,
        ("distinct_merchants_1h",),
        ("3-4", "5-9", "10+"),
        _VA_BURST,
        none=("5-9", "10+"),
    ),
    _dc(
        "VA-C2",
        FP.VELOCITY_ATTACK,
        TX,
        ("distinct_mcc_5m",),
        ("3-4", "5+"),
        _VA_BURST,
        rare=("3-4",),
        none=("5+",),
    ),
    _dc(
        "VA-C3",
        FP.VELOCITY_ATTACK,
        TX,
        ("distinct_devices_24h", "distinct_countries_24h"),
        ("3+",),
        _VA_BURST,
    ),
    _dc("VA-C4", FP.VELOCITY_ATTACK, TX, ("device_age",), ("<1h",), _VA_BURST),
    _dc("VA-C5", FP.VELOCITY_ATTACK, TX, ("account_activity",), ("51+",), _VA_BURST),
    _dc("VA-C6", FP.VELOCITY_ATTACK, TX, ("profile_depth",), ("20-127",), _VA_BURST),
    _dc(
        "IT-C1",
        FP.IMPOSSIBLE_TRAVEL,
        TX,
        ("tx_count_1h", "distinct_merchants_1h"),
        ("2",),
        _IT_PAIR,
    ),
    _dc("IT-C2", FP.IMPOSSIBLE_TRAVEL, TX, ("prior_decisions_1h",), ("1",), _IT_PAIR),
    _dc("IT-C3", FP.IMPOSSIBLE_TRAVEL, TX, ("prior_declined_share_1h",), ("0",), _IT_PAIR),
    _dc("IT-C4", FP.IMPOSSIBLE_TRAVEL, TX, ("avail:declined_ratio_1h",), _AVAILABLE, _IT_PAIR),
    _dc(
        "IT-C5",
        FP.IMPOSSIBLE_TRAVEL,
        TX,
        ("tx_count_24h",),
        ("2", "3-4"),
        _IT_PAIR,
        within=("IT-3",),
    ),
    _dc(
        "IT-C6",
        FP.IMPOSSIBLE_TRAVEL,
        TX,
        ("distinct_countries_24h",),
        ("2", "3+"),
        _s("Two card-present transactions", "great-circle distance"),
        within=("IT-3",),
    ),
    _dc(
        "IT-C7",
        FP.IMPOSSIBLE_TRAVEL,
        TX,
        ("tx_count_1m", "tx_count_5m", "card_count_5m", "distinct_mcc_5m"),
        ("1",),
        _IT_PAIR,
        within=("IT-2",),
    ),
    _dc("ATO-C1", FP.ACCOUNT_TAKEOVER, DEV, ("event_type",), ("FIRST_SEEN",), _ATO_NEW_DEVICE),
    _dc("ATO-C2", FP.ACCOUNT_TAKEOVER, DEV, ("device_home",), ("not",), _ATO_NEW_DEVICE),
    _dc(
        "ATO-C3",
        FP.ACCOUNT_TAKEOVER,
        DEV,
        ("device_age",),
        ("<1h", "[1h,24h)"),
        _ATO_NEW_DEVICE,
        none=("<1h",),
    ),
    _dc(
        "CT-C1",
        FP.CARD_TESTING,
        TX,
        ("distinct_countries_24h",),
        ("3+",),
        _s("across many distinct merchants"),
    ),
    _dc("CT-C2", FP.CARD_TESTING, TX, ("account_activity",), ("31-50",), _CT_BURST),
    _dc("CT-C3", FP.CARD_TESTING, TX, ("profile_depth",), ("20-127",), _CT_BURST),
    _dc(
        "CT-C4",
        FP.CARD_TESTING,
        TX,
        ("outcome_event",),
        ("DECLINED",),
        _s("a substantial share declined"),
    ),
    _dc("MC-C1", FP.MERCHANT_COLLUSION, TX, ("amount_vs_account",), ("[2,5)",), _MC_AMOUNTS),
    _dc("MC-C2", FP.MERCHANT_COLLUSION, TX, ("amount_z",), ("[0.5,1)",), _MC_AMOUNTS),
    _dc(
        "FR-C1",
        FP.FRAUD_RING,
        TX,
        ("device_account_tx",),
        ("1", "2", "3-5"),
        _FR_POOL,
        within=("FR-1",),
    ),
    _dc(
        "FR-C2",
        FP.FRAUD_RING,
        TX,
        ("device_accounts_24h",),
        ("3-4", "5+"),
        _FR_POOL,
        within=("FR-1",),
    ),
    _dc(
        "FR-C3",
        FP.FRAUD_RING,
        TX,
        ("device_age",),
        ("first-use", "[1h,24h)"),
        _FR_POOL,
        within=("FR-1",),
    ),
    _dc("FR-C4", FP.FRAUD_RING, TX, ("device_home",), ("not",), _FR_POOL, within=("FR-1",)),
    _dc("FR-C5", FP.FRAUD_RING, TX, ("distinct_devices_24h",), ("3+",), _FR_POOL, within=("FR-1",)),
    _dc("FR-C6", FP.FRAUD_RING, TX, ("ip_home",), ("not",), _FR_POOL, within=("FR-2",)),
    _dc(
        "FR-C7",
        FP.FRAUD_RING,
        TX,
        ("merchant_popularity",),
        ("31-100", "101+"),
        _s("converging on a shared merchant set"),
        within=("FR-3",),
    ),
    _dc(
        "CS-C1",
        FP.CREDENTIAL_STUFFING,
        ID,
        ("device_age",),
        ("first-reference",),
        _CS_LOGINS,
        within=("CS-4",),
    ),
    _dc(
        "CS-C2",
        FP.CREDENTIAL_STUFFING,
        ID,
        ("device_home",),
        ("not",),
        _CS_LOGINS,
        within=("CS-4",),
    ),
    _dc(
        "CS-C3",
        FP.CREDENTIAL_STUFFING,
        TX,
        ("ip_accounts_1h",),
        ("2", "3-4"),
        _CS_POOL,
        within=("CS-5",),
    ),
    _dc("CS-C4", FP.CREDENTIAL_STUFFING, TX, ("ip_home",), ("not",), _CS_POOL, within=("CS-5",)),
    _dc(
        "DF-C1",
        FP.DEVICE_FARM,
        TX,
        (
            "avail:account_tenure_days",
            "avail:amount_zscore_vs_account",
            "avail:distance_from_account_home_km",
            "avail:mcc_is_habitual_for_account",
            "avail:merchant_is_habitual",
        ),
        _AVAILABLE,
        _DF_ONCE,
        within=("DF-5",),
    ),
    _dc("DF-C2", FP.DEVICE_FARM, TX, ("profile_depth",), ("3-19",), _DF_ONCE, within=("DF-5",)),
    _dc(
        "DF-C3",
        FP.DEVICE_FARM,
        TX,
        ("avail:device_is_known_for_account",),
        ("INSUFFICIENT_HISTORY",),
        _DF_ONCE,
        within=("DF-1", "DF-2"),
    ),
)
"""§6.7 (revision 4): exactly the findings the second diagnostic probe classified as documented
behaviour, value by value."""

OPTIONAL_FIELD_ATTRIBUTES: Final[Mapping[Population, Mapping[str, tuple[str, ...]]]] = (
    MappingProxyType(
        {
            ID: MappingProxyType(
                {
                    "ip": ("ip_datacenter", "ip_login_accounts"),
                    "device": ("device_home", "device_age", "device_login_accounts"),
                    "user_agent": ("user_agent",),
                }
            )
        }
    )
)
"""§4.8 (revision 3): an attribute derived from an optional field is not applicable to an event type
no row of which, legitimate or planted, carries that field, when the type occurs among legitimate
rows. Keyed by the `SideRow` field name."""
KEY_MAP: Final[Mapping[str, tuple[tuple[Population, str, frozenset[str]], ...]]] = MappingProxyType(
    {
        "DEVICE_SHARING": (
            (TX, "device_accounts", frozenset({"2", "3-5", "6+"})),
            (ID, "device_login_accounts", frozenset({"2", "3-5", "6+"})),
        ),
        "DEVICE_NOVELTY": (
            (TX, "device_home", frozenset({"not"})),
            (TX, "device_age", frozenset({"first-use", "<1h", "[1h,24h)"})),
            (ID, "device_home", frozenset({"not"})),
            (ID, "device_age", frozenset({"first-reference", "<1h", "[1h,24h)"})),
            (DEV, "device_home", frozenset({"not"})),
            (DEV, "device_age", frozenset({"first-reference", "<1h", "[1h,24h)"})),
        ),
        "IP_REPUTATION": (
            (TX, "ip_datacenter", frozenset({"datacenter"})),
            (ID, "ip_datacenter", frozenset({"datacenter"})),
        ),
        "MCC_ANOMALY": ((TX, "mcc_habitual", frozenset({"unhabitual"})),),
        "GEO_DISPERSION": ((TX, "location_novel", frozenset({"[100,500)", "≥500"})),),
    }
)
"""§6.1: the only admissible causal-key sources."""

CATALOGUE_SECTIONS: Final[Mapping[FraudPattern, str]] = MappingProxyType(
    {
        FP.ACCOUNT_TAKEOVER: "3.1",
        FP.CARD_TESTING: "3.2",
        FP.IMPOSSIBLE_TRAVEL: "3.3",
        FP.VELOCITY_ATTACK: "3.4",
        FP.DEVICE_FARM: "3.5",
        FP.FRAUD_RING: "3.6",
        FP.MERCHANT_COLLUSION: "3.7",
        FP.CREDENTIAL_STUFFING: "3.8",
        FP.ANOMALOUS_HIGH_VALUE: "3.9",
        FP.UNUSUAL_LOCATION_DEVICE: "3.10",
    }
)

# ------------------------------------------------------------------ §10 pair kinds -------------
PAIR_KINDS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "K1": "TX -> next TX of the same account",
        "K2": "Q4e ID -> next DEV FIRST_SEEN of the same account",
        "K3": "any ID -> next TX of the same account",
        "K4": "DEV -> next TX of the same account on that device",
        "K5": "TX -> next TX on the same device by a different account",
        "K6": "LOGIN_* -> next LOGIN_* from the same IP, any account",
        "K7": "TX -> its OUT row",
        "K8": "TX -> next TX at the same merchant, any account",
    }
)
DOCUMENTED_OFFSETS: Final[tuple[tuple[str, FraudPattern, int], ...]] = ()


# ------------------------------------------------------------------ §11 generator rules --------
class AmountMechanism(StrEnum):
    ORDINARY = "ORDINARY"
    ORDINARY_REGION = "ORDINARY-REGION"
    PROBE = "PROBE"
    HIGH_VALUE = "HIGH-VALUE"
    COLLUSION_PRICE = "COLLUSION-PRICE"


@dataclass(frozen=True, slots=True)
class AmountRule:
    mechanism: AmountMechanism
    lower_multiple: float | None = None
    """Inclusive lower bound on amount / typical, if any."""
    lower_exclusive: bool = False
    upper_multiple: float | None = None
    """Exclusive upper bound on amount / typical, if any."""


G1_AMOUNT_NAMESPACE: Final = "scenario-amount"
G1_MAX_DRAWS: Final = 10_000
G1_RULES: Final[Mapping[FraudPattern, AmountRule]] = MappingProxyType(
    {
        FP.IMPOSSIBLE_TRAVEL: AmountRule(AmountMechanism.ORDINARY),
        FP.DEVICE_FARM: AmountRule(AmountMechanism.ORDINARY),
        FP.FRAUD_RING: AmountRule(AmountMechanism.ORDINARY),
        FP.CREDENTIAL_STUFFING: AmountRule(AmountMechanism.ORDINARY),
        FP.VELOCITY_ATTACK: AmountRule(AmountMechanism.ORDINARY_REGION, upper_multiple=3.0),
        FP.UNUSUAL_LOCATION_DEVICE: AmountRule(
            AmountMechanism.ORDINARY_REGION,
            lower_multiple=0.5,
            lower_exclusive=True,
            upper_multiple=2.0,
        ),
        FP.ACCOUNT_TAKEOVER: AmountRule(AmountMechanism.ORDINARY_REGION, lower_multiple=3.0),
        FP.CARD_TESTING: AmountRule(AmountMechanism.PROBE),
        FP.ANOMALOUS_HIGH_VALUE: AmountRule(AmountMechanism.HIGH_VALUE),
        FP.MERCHANT_COLLUSION: AmountRule(AmountMechanism.COLLUSION_PRICE),
    }
)
G1_CARD_TESTING_PAYOFF: Final = AmountRule(AmountMechanism.ORDINARY_REGION, lower_multiple=3.0)
G1_PROBE_RANGE: Final = (1, 250)
G1_HIGH_VALUE_MULTIPLE: Final = (20.0, 60.0)
G6_PRICE_LOG_RANGE: Final = (8.3, 8.9)
G6_PRICE_MULTIPLIER: Final = (0.99, 1.01)
G6_PAYER_TOLERANCE_SIGMAS: Final = 1.0
G6_S7A_LOG_SLACK: Final = 0.011

G3_MAX_SPEED_KMH: Final = 900.0
G3_SCENARIOS: Final = frozenset({FP.ACCOUNT_TAKEOVER, FP.UNUSUAL_LOCATION_DEVICE})

G5_TIME_NAMESPACE: Final = "scenario-time"
G5_TAKEOVER_WINDOW_MS: Final = (20 * 60_000, 6 * HOUR_MS)
G5_RING_SPAN_MS: Final = (2 * DAY_MS, 7 * DAY_MS)
G5_FARM_SPAN_MS: Final = (HOUR_MS, DAY_MS)

G7_GATED_CAUSAL_KEYS: Final[Mapping[FraudPattern, frozenset[str]]] = MappingProxyType(
    {
        FP.CREDENTIAL_STUFFING: frozenset(
            {"AUTHENTICATION_ANOMALY", "IP_REPUTATION", "DEVICE_SHARING"}
        ),
        FP.CARD_TESTING: frozenset({"VELOCITY", "AMOUNT_ANOMALY", "MCC_ANOMALY"}),
    }
)

G2_NAMESPACE: Final = "coverage-floor"

DM1_NAMESPACE: Final = "authorization-latency"
DM1_FLOOR_MS: Final = 40
DM1_MEAN_MS: Final = 300.0
DM1_SD_MS: Final = 150.0

# ------------------------------------------------------------------ §14 controls ---------------


@dataclass(frozen=True, slots=True)
class Expectation:
    """A named failure a control must show: a check, and the cell it must fail on."""

    check: str
    population: Population | None = None
    attribute: str | None = None
    values: frozenset[str] = frozenset()
    scenario: FraudPattern | None = None
    note: str = ""


def _e(
    check: str,
    population: Population | None = None,
    attribute: str | None = None,
    values: tuple[str, ...] = (),
    scenario: FraudPattern | None = None,
    note: str = "",
) -> Expectation:
    return Expectation(check, population, attribute, frozenset(values), scenario, note)


NEGATIVE_CONTROL: Final[tuple[Expectation, ...]] = (
    _e("S0/LPC-1/R3", note="IDENTITY_CHANGE_24H and DEVICE_FIRST_SEEN_24H, lower bound > 0.25"),
    _e("S0/LPC-2/R3", note="each of the five TX signals, point precision > 0.25"),
    _e("S0/LPC-3/R3", note="M1 and M2, point precision > 0.25"),
    _e("S0/LPC-3/R6", note="M3, point ratio > 2"),
    _e("R7", TX, "hour"),
    _e("R7", TX, "merchant_habitual"),
    _e("S1-U(a)", TX, "ip_home", ("not",)),
    _e("S1-U(a)", TX, "distance_home", ("[100,500)", "≥500"), FP.ACCOUNT_TAKEOVER),
    _e("S2", TX, "subsecond"),
    _e("S2c/3"),
    _e("S2c/7"),
    _e("S3/pooled", note="slice 20"),
    _e("S4/K2", scenario=FP.ACCOUNT_TAKEOVER),
    _e("S5b", note="VELOCITY_ATTACK or UNUSUAL_LOCATION_DEVICE"),
)
"""§14.1. `R7 hour` stands for "hour or daypart" and `R7 merchant_habitual` for "merchant_habitual
or merchant_popularity"; the evaluator accepts either."""

ABLATIONS: Final[Mapping[str, Expectation]] = MappingProxyType(
    {
        "T1": _e("S0/LPC-2/R1|R2", note="TX_NON_HOME_DEVICE"),
        "T2": _e("S2", TX, "subsecond"),
        "T3": _e("S0/LPC-2/R1|R2", note="TX_DECLINED"),
        "M1": _e("S0/LPC-3/R3", note="TX_REPEATED_EXACT_COORDINATES"),
        "M2": _e("S0/LPC-3/R3'", note="TX_EXACT_HOME_POINT"),
        "M3": _e("S0/LPC-3/R6", note="TX_CNP_ECOMMERCE"),
        "M4": _e(
            "R7",
            TX,
            "hour",
            note="hour or daypart, outside ACCOUNT_TAKEOVER and IMPOSSIBLE_TRAVEL",
        ),
        "M5": _e("R7", TX, "merchant_popularity"),
        "M6": _e("R7", TX, "channel", note="a scenario other than IMPOSSIBLE_TRAVEL"),
        "N1": _e("S1-U(a)", TX, "ip_home", ("not",)),
        "N2": _e("S1-U(a)", TX, "distance_home", ("[100,500)", "≥500"), FP.ACCOUNT_TAKEOVER),
        "N3": _e("S1-U(a)", TX, "gap_prev", ("<10s", "[10s,60s)"), note="CT or VA"),
        "N4": _e(
            "S1-U(a)", TX, "merchant_same_amount_accounts_24h", ("5+",), FP.MERCHANT_COLLUSION
        ),
        "N5": _e("S1-U(a)", TX, "joint_link", ("yes",), FP.FRAUD_RING),
        "N6": _e("S3/pooled", note="slice 20"),
        "N7": _e("S4/K2", scenario=FP.ACCOUNT_TAKEOVER),
        "N8": _e("S4/K1", scenario=FP.DEVICE_FARM),
        "N9": _e("S5a"),
        "N10": _e("S2c/5"),
        "N11": _e("S2c/7", note="or S2b tie_rank"),
        "N12": _e("S2c/3"),
    }
)
"""§14.3: each correction, disabled alone, must fail its check."""
