"""Structural label-proxy measurements on the frozen eval-v1 dataset. Diagnostic; no run_id.

Every comparison is presence / support / coverage / distribution between fraud and legitimate
rows (or accounts), never model performance. Labels are read as trace_eval, after loading.
"""
import bisect
import collections
import datetime as dt
import json
import math
import os
import sys
import traceback
from pathlib import Path

import psycopg
import pyarrow.parquet as pq

R = Path("/Users/singh/Downloads/trace-x")
sys.path.insert(0, str(R))
from data.generator.config import GeneratorConfig  # noqa: E402
from data.generator.population import CITIES, build_universe  # noqa: E402
from trace_core.domain.geo import GeoPoint, haversine_km  # noqa: E402

OUT = {}
EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)


def ms(s):
    return (dt.datetime.fromisoformat(s.replace("Z", "+00:00")) - EPOCH) // dt.timedelta(milliseconds=1)


def idx(identifier):
    return int(identifier.rsplit("_", 1)[1])


def check(name):
    def wrap(fn):
        try:
            OUT[name] = fn()
        except Exception as exc:  # diagnostic: record and continue
            OUT[name] = {"error": f"{type(exc).__name__}: {exc}", "trace": traceback.format_exc()[-600:]}
        return fn
    return wrap


manifest = json.loads((R / "eval/track_a/eval-v1.manifest.json").read_text())
config = GeneratorConfig.from_mapping(manifest["config"])
universe = build_universe(config)
profiles = {p.account_id: p for p in universe.profiles}

dsn = "postgresql://{u}:{p}@localhost:{port}/{db}".format(
    u=os.environ.get("TRACE_EVAL_DB_USER", "trace_eval"), p=os.environ["TRACE_EVAL_DB_PASSWORD"],
    port=os.environ.get("POSTGRES_PORT", "5442"), db=os.environ.get("POSTGRES_DB", "tracex"))
with psycopg.connect(dsn) as c:
    label_rows = c.execute(
        "select l.transaction_id, l.is_fraud, l.fraud_pattern from groundtruth.transaction_labels l "
        "join groundtruth.datasets d using (dataset_id) where d.dataset_version='eval-v1'").fetchall()
labels = {t: (bool(f), p) for t, f, p in label_rows}

base = R / "data/generated/eval-v1"
tx = pq.read_table(base / "tx.raw.v1.parquet")
env = tx.column("envelope").combine_chunks()
pay = tx.column("payload").combine_chunks()
E = {n: env.field(n).to_pylist() for n in ("event_id", "event_type", "schema_version", "occurred_at", "ingested_at", "producer", "correlation_id")}
P = {n: pay.field(n).to_pylist() for n in ("transaction_id", "account_id", "card_id", "device_id", "merchant_id", "ip_id", "amount_minor", "currency", "channel", "entry_mode", "merchant_mcc", "merchant_country", "merchant_name", "latitude", "longitude", "user_agent", "memo", "authorization_outcome")}
N = len(P["transaction_id"])
Y = [labels[t][0] for t in P["transaction_id"]]
PAT = [labels[t][1] for t in P["transaction_id"]]
NF = sum(Y); NL = N - NF
OCC = [ms(s) for s in E["occurred_at"]]
ING = [ms(s) for s in E["ingested_at"]]
OUT["population"] = {"rows": N, "fraud": NF, "legit": NL, "unlabelled": sum(1 for t in P["transaction_id"] if t not in labels)}


def rate(pred):
    f = sum(1 for v, y in zip(pred, Y) if v and y)
    l = sum(1 for v, y in zip(pred, Y) if v and not y)
    return {"p_fraud": round(f / NF, 5), "p_legit": round(l / NL, 6), "fraud_rows": f, "legit_rows": l}


def by_pattern(pred):
    tot = collections.Counter(p for p, y in zip(PAT, Y) if y)
    hit = collections.Counter(p for v, p, y in zip(pred, PAT, Y) if y and v)
    return {p: round(hit[p] / tot[p], 3) for p in sorted(tot)}


def categorical(values, top=6):
    cf = collections.Counter(v for v, y in zip(values, Y) if y)
    cl = collections.Counter(v for v, y in zip(values, Y) if not y)
    keys = set(cf) | set(cl)
    tvd = 0.5 * sum(abs(cf[k] / NF - cl[k] / NL) for k in keys)
    fonly = [k for k in cf if k not in cl]
    lonly = [k for k in cl if k not in cf]
    ranked = sorted(keys, key=lambda k: -abs(cf[k] / NF - cl[k] / NL))[:top]
    return {"tvd": round(tvd, 4), "fraud_only_values": len(fonly), "share_of_fraud_on_fraud_only_values": round(sum(cf[k] for k in fonly) / NF, 4),
            "legit_only_values": len(lonly), "largest_gaps": [(str(k), round(cf[k] / NF, 4), round(cl[k] / NL, 5)) for k in ranked]}


# ---------------------------------------------------------------- raw fields
for name in ("currency", "channel", "entry_mode", "merchant_mcc", "merchant_country", "user_agent", "memo", "authorization_outcome"):
    check(f"tx.{name}")(lambda name=name: categorical(P[name]))
check("tx.channel|entry_mode")(lambda: categorical([f"{c}|{e}" for c, e in zip(P["channel"], P["entry_mode"])]))
for name in ("event_type", "schema_version", "producer"):
    check(f"envelope.{name}")(lambda name=name: categorical(E[name]))
check("envelope.occurred_at_string_length")(lambda: categorical([len(s) for s in E["occurred_at"]]))
check("null_or_empty_by_field")(lambda: {n: rate([v is None or v == "" for v in vals]) for n, vals in P.items() if any(v is None or v == "" for v in vals)})

# ---------------------------------------------------------------- timing
check("time.whole_second")(lambda: {**rate([o % 1000 == 0 for o in OCC]), "by_pattern": by_pattern([o % 1000 == 0 for o in OCC])})
check("time.whole_minute")(lambda: rate([o % 60000 == 0 for o in OCC]))
check("time.hour_of_day")(lambda: categorical([dt.datetime.fromtimestamp(o / 1000, dt.UTC).hour for o in OCC], top=8))
check("time.night_00_05")(lambda: {**rate([dt.datetime.fromtimestamp(o / 1000, dt.UTC).hour < 6 for o in OCC]), "by_pattern": by_pattern([dt.datetime.fromtimestamp(o / 1000, dt.UTC).hour < 6 for o in OCC])})
check("time.weekday")(lambda: categorical([dt.datetime.fromtimestamp(o / 1000, dt.UTC).weekday() for o in OCC]))
start_ms = ms(config.start_at.isoformat()); end_ms = ms(config.end_at.isoformat())
check("time.last_3_days_of_window")(lambda: rate([o >= end_ms - 3 * 86_400_000 for o in OCC]))
check("time.ingest_lag_ms")(lambda: {"fraud": sorted(i - o for i, o, y in zip(ING, OCC, Y) if y)[::max(1, NF // 10)][:11], "legit": sorted(i - o for i, o, y in zip(ING, OCC, Y) if not y)[::max(1, NL // 10)][:11]})


def uuid7_ms(u):
    return int(u.replace("-", "")[:12], 16)


check("envelope.event_id_time_matches_occurred")(lambda: rate([uuid7_ms(u) != o for u, o in zip(E["event_id"], OCC)]))

# ---------------------------------------------------------------- population membership
check("entity.device_not_home")(lambda: {**rate([d not in profiles[a].home_devices for a, d in zip(P["account_id"], P["device_id"])]), "by_pattern": by_pattern([d not in profiles[a].home_devices for a, d in zip(P["account_id"], P["device_id"])])})
check("entity.ip_not_home")(lambda: {**rate([i not in profiles[a].home_ips for a, i in zip(P["account_id"], P["ip_id"])]), "by_pattern": by_pattern([i not in profiles[a].home_ips for a, i in zip(P["account_id"], P["ip_id"])])})
check("entity.card_not_account")(lambda: {**rate([c not in {x.card_id for x in profiles[a].cards} for a, c in zip(P["account_id"], P["card_id"])]), "by_pattern": by_pattern([c not in {x.card_id for x in profiles[a].cards} for a, c in zip(P["account_id"], P["card_id"])])})
check("entity.merchant_not_habitual")(lambda: {**rate([m not in profiles[a].habitual_merchants for a, m in zip(P["account_id"], P["merchant_id"])]), "by_pattern": by_pattern([m not in profiles[a].habitual_merchants for a, m in zip(P["account_id"], P["merchant_id"])])})
check("entity.ids_outside_population")(lambda: {
    "account": rate([a not in profiles for a in P["account_id"]]),
    "device": rate([idx(d) >= config.device_count for d in P["device_id"]]),
    "ip": rate([idx(i) >= config.ip_count for i in P["ip_id"]]),
    "merchant": rate([idx(m) >= config.merchant_count for m in P["merchant_id"]]),
    "card": rate([idx(c) >= config.account_count * config.cards_per_account for c in P["card_id"]]),
})
check("entity.device_is_datacenter_ip")(lambda: {**rate([universe.ips[idx(i)].is_datacenter if idx(i) < len(universe.ips) else True for i in P["ip_id"]]), "by_pattern": by_pattern([universe.ips[idx(i)].is_datacenter if idx(i) < len(universe.ips) else True for i in P["ip_id"]])})
check("entity.merchant_popularity_rank_bucket")(lambda: categorical(["top1%" if idx(m) < config.merchant_count * 0.01 else "top10%" if idx(m) < config.merchant_count * 0.1 else "top50%" if idx(m) < config.merchant_count * 0.5 else "tail" for m in P["merchant_id"]]))
check("entity.merchant_name_consistent")(lambda: rate([n != universe.merchants[idx(m)].name for m, n in zip(P["merchant_id"], P["merchant_name"])]))
check("entity.mcc_country_consistent_with_registry")(lambda: rate([(c, k) != (universe.merchants[idx(m)].mcc, universe.merchants[idx(m)].country) for m, c, k in zip(P["merchant_id"], P["merchant_mcc"], P["merchant_country"])]))

# ---------------------------------------------------------------- amounts
check("amount.above_legit_clamp_5m")(lambda: rate([a > 5_000_000 for a in P["amount_minor"]]))
check("amount.below_100_minor")(lambda: {**rate([a < 100 for a in P["amount_minor"]]), "by_pattern": by_pattern([a < 100 for a in P["amount_minor"]])})
check("amount.round_100")(lambda: {**rate([a % 100 == 0 for a in P["amount_minor"]]), "by_pattern": by_pattern([a % 100 == 0 for a in P["amount_minor"]])})
check("amount.z_vs_profile_gt3")(lambda: {**rate([(math.log(max(a, 1)) - profiles[acc].amount_mu) / profiles[acc].amount_sigma > 3 for a, acc in zip(P["amount_minor"], P["account_id"])]), "by_pattern": by_pattern([(math.log(max(a, 1)) - profiles[acc].amount_mu) / profiles[acc].amount_sigma > 3 for a, acc in zip(P["amount_minor"], P["account_id"])])})

# ---------------------------------------------------------------- geography
def dist(a, lat, lon):
    return haversine_km(profiles[a].account.home, GeoPoint(lat, lon))


DIST = [dist(a, la, lo) for a, la, lo in zip(P["account_id"], P["latitude"], P["longitude"])]
check("geo.distance_from_home_bucket")(lambda: categorical(["<25" if d < 25 else "25-100" if d < 100 else "100-500" if d < 500 else ">=500" for d in DIST]))
check("geo.exactly_home")(lambda: {**rate([(la, lo) == (profiles[a].account.home.latitude, profiles[a].account.home.longitude) for a, la, lo in zip(P["account_id"], P["latitude"], P["longitude"])]), "by_pattern": by_pattern([(la, lo) == (profiles[a].account.home.latitude, profiles[a].account.home.longitude) for a, la, lo in zip(P["account_id"], P["latitude"], P["longitude"])])})
CITY_POINTS = {(round(c[1], 6), round(c[2], 6)) for c in CITIES}
check("geo.exactly_a_city_centre")(lambda: {**rate([(round(la, 6), round(lo, 6)) in CITY_POINTS for la, lo in zip(P["latitude"], P["longitude"])]), "by_pattern": by_pattern([(round(la, 6), round(lo, 6)) in CITY_POINTS for la, lo in zip(P["latitude"], P["longitude"])])})
COORD = collections.Counter(zip(P["latitude"], P["longitude"]))
check("geo.coordinate_pair_repeated")(lambda: {**rate([COORD[(la, lo)] > 1 for la, lo in zip(P["latitude"], P["longitude"])]), "by_pattern": by_pattern([COORD[(la, lo)] > 1 for la, lo in zip(P["latitude"], P["longitude"])])})
check("geo.decimal_places")(lambda: categorical([min(6, len(repr(la).split(".")[1]) if "." in repr(la) else 0) for la in P["latitude"]]))

# ---------------------------------------------------------------- sequence and ordering
order = sorted(range(N), key=lambda i: (P["account_id"][i], OCC[i], P["transaction_id"][i]))
gap_whole = [False] * N; gap_small = [False] * N; has_prev = [False] * N
for k in range(1, N):
    i, j = order[k - 1], order[k]
    if P["account_id"][i] == P["account_id"][j]:
        g = OCC[j] - OCC[i]
        has_prev[j] = True
        gap_whole[j] = g > 0 and g % 1000 == 0
        gap_small[j] = g < 60_000
check("sequence.gap_to_previous_same_account_whole_seconds")(lambda: {**rate(gap_whole), "by_pattern": by_pattern(gap_whole)})
check("sequence.gap_under_60s")(lambda: rate(gap_small))
ties = collections.defaultdict(list)
for i, o in enumerate(OCC):
    ties[o].append(i)
mixed = [g for g in ties.values() if len({Y[i] for i in g}) == 2]
check("ordering.tie_groups_with_both_labels")(lambda: {"groups": len(mixed), "fraud_after_all_legit": sum(1 for g in mixed if min(int(P["transaction_id"][i][3:]) for i in g if Y[i]) > max(int(P["transaction_id"][i][3:]) for i in g if not Y[i]))})
check("ordering.transaction_id_matches_time_order")(lambda: {"violations": sum(1 for k in range(1, N) if (OCC[k], k) < (OCC[k - 1], k - 1))})
txs_per_acct = collections.Counter(P["account_id"])
fraud_accts = {a for a, y in zip(P["account_id"], Y) if y}
check("account.tx_count_mean")(lambda: {"fraud_accounts": round(sum(txs_per_acct[a] for a in fraud_accts) / len(fraud_accts), 2), "other_accounts": round(sum(v for a, v in txs_per_acct.items() if a not in fraud_accts) / max(1, len(txs_per_acct) - len(fraud_accts)), 2), "fraud_account_count": len(fraud_accts)})

# ---------------------------------------------------------------- side events
side = {}
for topic in ("identity.events.v1", "device.events.v1"):
    t = pq.read_table(base / f"{topic}.parquet")
    e = t.column("envelope").combine_chunks(); p = t.column("payload").combine_chunks()
    side[topic] = {"env": {n: e.field(n).to_pylist() for n in ("occurred_at", "correlation_id")}, "pay": {f.name: p.field(f.name).to_pylist() for f in p.type}}
tx_accounts = set(P["account_id"])
all_accounts = [p.account_id for p in universe.profiles]
legit_only_accounts = [a for a in all_accounts if a not in fraud_accts]


def account_presence(accounts_with):
    return {"p_fraud_account": round(sum(1 for a in fraud_accts if a in accounts_with) / len(fraud_accts), 4),
            "p_legit_only_account": round(sum(1 for a in legit_only_accounts if a in accounts_with) / len(legit_only_accounts), 6),
            "accounts_with": len(accounts_with)}


ide = side["identity.events.v1"]; dev = side["device.events.v1"]
check("side.identity_event_account_presence")(lambda: account_presence(set(ide["pay"]["account_id"])))
check("side.identity_event_types")(lambda: {t: account_presence({a for a, x in zip(ide["pay"]["account_id"], ide["pay"]["identity_event_type"]) if x == t}) for t in sorted(set(ide["pay"]["identity_event_type"]))})
check("side.device_event_account_presence")(lambda: account_presence(set(dev["pay"]["account_id"])))
check("side.device_event_types")(lambda: dict(collections.Counter(dev["pay"]["device_event_type"])))
check("side.identity_optional_field_presence_by_type")(lambda: {t: {f: round(sum(1 for x, v in zip(ide["pay"]["identity_event_type"], ide["pay"].get(f, [None] * len(ide["pay"]["account_id"]))) if x == t and v not in (None, "")) / max(1, sum(1 for x in ide["pay"]["identity_event_type"] if x == t)), 3) for f in ("device_id", "ip_id")} for t in sorted(set(ide["pay"]["identity_event_type"]))})
check("side.whole_second_share")(lambda: {topic: round(sum(1 for s in side[topic]["env"]["occurred_at"] if ms(s) % 1000 == 0) / len(side[topic]["env"]["occurred_at"]), 4) for topic in side})
check("side.device_event_device_not_home")(lambda: round(sum(1 for a, d in zip(dev["pay"]["account_id"], dev["pay"]["device_id"]) if d not in profiles[a].home_devices) / len(dev["pay"]["device_id"]), 4))
check("side.device_event_platform_contradicts_registry")(lambda: round(sum(1 for d, pl in zip(dev["pay"]["device_id"], dev["pay"]["platform"]) if universe.devices[idx(d)].platform != pl) / len(dev["pay"]["device_id"]), 4))
side_corr = set(ide["env"]["correlation_id"]) | set(dev["env"]["correlation_id"])
check("envelope.correlation_id_shared_with_a_side_event")(lambda: rate([c in side_corr for c in E["correlation_id"]]))

# ---------------------------------------------------------------- account attributes of scenario selection
def tenure_bucket(a):
    days = (config.start_at - profiles[a].account.opened_at).days
    return "<90d" if days < 90 else "<1y" if days < 365 else "<3y" if days < 1095 else ">=3y"


def acct_categorical(fn):
    cf = collections.Counter(fn(a) for a in fraud_accts); cl = collections.Counter(fn(a) for a in legit_only_accounts)
    keys = set(cf) | set(cl)
    return {"tvd": round(0.5 * sum(abs(cf[k] / len(fraud_accts) - cl[k] / len(legit_only_accounts)) for k in keys), 4),
            "dist": {str(k): (round(cf[k] / len(fraud_accts), 3), round(cl[k] / len(legit_only_accounts), 3)) for k in sorted(keys, key=str)}}


check("account.tenure_bucket")(lambda: acct_categorical(tenure_bucket))
check("account.home_country")(lambda: acct_categorical(lambda a: profiles[a].account.country))
check("account.home_device_count")(lambda: acct_categorical(lambda a: len(profiles[a].home_devices)))
check("account.has_any_transaction")(lambda: {"legit_only_accounts_without_tx": sum(1 for a in legit_only_accounts if a not in tx_accounts)})
check("sanity.legit_rows_use_home_device")(lambda: round(sum(1 for a, d, y in zip(P["account_id"], P["device_id"], Y) if not y and d in profiles[a].home_devices) / NL, 6))

(Path("/private/tmp/claude-501/-Users-singh-Downloads-trace-x/b342966b-066f-48a1-b65e-5319fdbc026e/scratchpad") / "eval_v1_structural_audit.json").write_text(json.dumps(OUT, indent=1, default=str))
print(json.dumps(OUT, indent=1, default=str)[:60000])
