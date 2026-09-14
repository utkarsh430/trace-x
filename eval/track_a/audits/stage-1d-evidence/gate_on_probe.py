"""Gate-on probe of the CURRENT generator (Step E worktree): which proxies survive. Diagnostic, in memory.

120,000 transactions at eval-v1's population ratios, seed 42, BaselineIdentityConfig() defaults.
Labels come from the in-memory rows; nothing is written anywhere.
"""
import collections, datetime as dt, json, sys
from pathlib import Path
W = Path("/Users/singh/Downloads/trace-x/.claude/worktrees/agent-aae9faa608636c9c8")
sys.path[:0] = [str(W), str(W / "packages")]
from data.generator.config import BaselineIdentityConfig, GeneratorConfig
from data.generator.engine import generate_dataset
from data.generator.population import build_universe
from trace_core.domain.geo import GeoPoint, haversine_km

EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
ms = lambda s: (dt.datetime.fromisoformat(s.replace("Z", "+00:00")) - EPOCH) // dt.timedelta(milliseconds=1)
idx = lambda s: int(s.rsplit("_", 1)[1])
cfg = GeneratorConfig(seed=42, row_count=120_000, account_count=4_800, merchant_count=360, device_count=5_760,
                      ip_count=2_400, fraud_rate=0.005, baseline_identity=BaselineIdentityConfig())
universe = build_universe(cfg)
prof = {p.account_id: p for p in universe.profiles}
rows = list(generate_dataset(cfg))
tx = [r for r in rows if r.topic == "tx.raw.v1"]
side = [r for r in rows if r.topic != "tx.raw.v1"]
Y = [r.label.is_fraud for r in tx]
PAT = [r.label.fraud_pattern.value if r.label.fraud_pattern else "LEGIT" for r in tx]
pay = [r.event["payload"] for r in tx]; env = [r.event["envelope"] for r in tx]
OCC = [ms(e["occurred_at"]) for e in env]
NF, NL = sum(Y), len(Y) - sum(Y)
out = {"rows": {"tx": len(tx), "fraud": NF, "legit": NL, "side_events": len(side)}}


def share(pred, per_pattern=False):
    f = sum(1 for v, y in zip(pred, Y) if v and y); l = sum(1 for v, y in zip(pred, Y) if v and not y)
    res = {"fraud": round(f / NF, 4), "legit": round(l / NL, 5), "legit_rows": l}
    if per_pattern:
        tot = collections.Counter(p for p, y in zip(PAT, Y) if y); hit = collections.Counter(p for v, p, y in zip(pred, PAT, Y) if v and y)
        res["by_pattern"] = {p: round(hit[p] / tot[p], 3) for p in sorted(tot)}
    return res


out["whole_second"] = share([o % 1000 == 0 for o in OCC], True)
out["declined"] = share([p["authorization_outcome"] == "DECLINED" for p in pay], True)
out["device_not_home"] = share([p["device_id"] not in prof[p["account_id"]].home_devices for p in pay], True)
out["ip_not_home"] = share([p["ip_id"] not in prof[p["account_id"]].home_ips for p in pay], True)
dist = [haversine_km(prof[p["account_id"]].account.home, GeoPoint(p["latitude"], p["longitude"])) for p in pay]
out["distance_ge_100km"] = share([d >= 100 for d in dist], True)
out["night_00_05"] = share([dt.datetime.fromtimestamp(o / 1000, dt.UTC).hour < 6 for o in OCC], True)
out["card_not_present"] = share([p["channel"] == "CARD_NOT_PRESENT" for p in pay])
out["ecommerce_given_cnp"] = {"fraud": round(sum(1 for p, y in zip(pay, Y) if y and p["channel"] == "CARD_NOT_PRESENT" and p["entry_mode"] == "ECOMMERCE") / max(1, sum(1 for p, y in zip(pay, Y) if y and p["channel"] == "CARD_NOT_PRESENT")), 3),
                              "legit": round(sum(1 for p, y in zip(pay, Y) if not y and p["channel"] == "CARD_NOT_PRESENT" and p["entry_mode"] == "ECOMMERCE") / max(1, sum(1 for p, y in zip(pay, Y) if not y and p["channel"] == "CARD_NOT_PRESENT")), 3)}
end_ms = ms(cfg.end_at.isoformat())
out["last_3_days"] = share([o >= end_ms - 3 * 86_400_000 for o in OCC])
out["merchant_top1pct"] = share([idx(p["merchant_id"]) < cfg.merchant_count * 0.01 for p in pay], True)
out["coordinate_exactly_home"] = share([(p["latitude"], p["longitude"]) == (prof[p["account_id"]].account.home.latitude, prof[p["account_id"]].account.home.longitude) for p in pay], True)
seen = collections.defaultdict(set); rep = []
for i in sorted(range(len(tx)), key=lambda i: OCC[i]):
    key = (pay[i]["latitude"], pay[i]["longitude"]); a = pay[i]["account_id"]
    rep.append((i, key in seen[a])); seen[a].add(key)
rep_flag = [False] * len(tx)
for i, v in rep:
    rep_flag[i] = v
out["coordinate_repeats_earlier_on_account"] = share(rep_flag, True)
order = sorted(range(len(tx)), key=lambda i: (pay[i]["account_id"], OCC[i]))
lt60 = [False] * len(tx); near60 = [False] * len(tx)
for k in range(1, len(order)):
    i, j = order[k - 1], order[k]
    if pay[i]["account_id"] == pay[j]["account_id"]:
        g = OCC[j] - OCC[i]; lt60[j] = g < 60_000; near60[j] = 59_000 <= g <= 61_000
out["gap_to_previous_under_60s"] = share(lt60, True)
out["gap_to_previous_59_61s"] = share(near60, True)
side_corr = {r.event["envelope"]["correlation_id"] for r in side}
all_corr = collections.Counter([e["correlation_id"] for e in env] + [r.event["envelope"]["correlation_id"] for r in side])
out["correlation_id_shared"] = share([all_corr[e["correlation_id"]] > 1 for e in env])
# side events: planted vs legitimate
kinds = collections.Counter(); keysets = collections.defaultdict(collections.Counter); whole = collections.Counter()
for r in side:
    planted = r.scenario_instance is not None
    typ = r.event["payload"].get("identity_event_type") or r.event["payload"].get("device_event_type")
    kinds[(r.topic.split(".")[0], typ, "planted" if planted else "legit")] += 1
    keysets[(typ, "planted" if planted else "legit")][",".join(sorted(r.event["payload"]))] += 1
    whole["planted" if planted else "legit"] += ms(r.event["envelope"]["occurred_at"]) % 1000 == 0
out["side_event_counts"] = {f"{a}:{b}:{c}": n for (a, b, c), n in sorted(kinds.items())}
out["side_payload_keysets"] = {f"{t}:{k}": dict(v) for (t, k), v in sorted(keysets.items())}
out["side_whole_second_count"] = dict(whole)
ident = collections.defaultdict(list)
for r in side:
    if r.topic == "identity.events.v1":
        ident[r.event["payload"]["account_id"]].append(ms(r.event["envelope"]["occurred_at"]))
off = collections.Counter()
for r in side:
    if r.topic == "device.events.v1" and r.event["payload"].get("device_event_type") == "FIRST_SEEN":
        o = ms(r.event["envelope"]["occurred_at"]); prior = [x for x in ident[r.event["payload"]["account_id"]] if x <= o]
        if prior:
            g = o - max(prior)
            off[("planted" if r.scenario_instance is not None else "legit", "119-121s" if 119_000 <= g <= 121_000 else "other")] += 1
out["first_seen_offset_after_identity_event"] = {f"{a}:{b}": n for (a, b), n in sorted(off.items())}
plat = collections.Counter()
for r in side:
    if r.topic == "device.events.v1":
        ok = universe.devices[idx(r.event["payload"]["device_id"])].platform == r.event["payload"].get("platform")
        plat[("planted" if r.scenario_instance is not None else "legit", ok)] += 1
out["device_platform_consistent"] = {f"{a}:{b}": n for (a, b), n in sorted(plat.items())}
Path("/private/tmp/claude-501/-Users-singh-Downloads-trace-x/b342966b-066f-48a1-b65e-5319fdbc026e/scratchpad/gate_on_probe.json").write_text(json.dumps(out, indent=1))
