"""Construction-artefact follow-ups on eval-v1: fixed offsets and amount-ratio support. Diagnostic."""
import collections, datetime as dt, json, math, os, sys
from pathlib import Path
import psycopg, pyarrow.parquet as pq
R = Path("/Users/singh/Downloads/trace-x"); sys.path.insert(0, str(R))
from data.generator.config import GeneratorConfig
from data.generator.population import build_universe
EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
ms = lambda s: (dt.datetime.fromisoformat(s.replace("Z", "+00:00")) - EPOCH) // dt.timedelta(milliseconds=1)
config = GeneratorConfig.from_mapping(json.loads((R / "eval/track_a/eval-v1.manifest.json").read_text())["config"])
profiles = {p.account_id: p for p in build_universe(config).profiles}
dsn = "postgresql://{u}:{p}@localhost:{port}/{db}".format(u=os.environ.get("TRACE_EVAL_DB_USER", "trace_eval"), p=os.environ["TRACE_EVAL_DB_PASSWORD"], port=os.environ.get("POSTGRES_PORT", "5442"), db=os.environ.get("POSTGRES_DB", "tracex"))
with psycopg.connect(dsn) as c:
    labels = {t: (bool(f), p) for t, f, p in c.execute("select l.transaction_id, l.is_fraud, l.fraud_pattern from groundtruth.transaction_labels l join groundtruth.datasets d using (dataset_id) where d.dataset_version='eval-v1'")}
base = R / "data/generated/eval-v1"
t = pq.read_table(base / "tx.raw.v1.parquet", columns=["envelope", "payload"])
env = t.column("envelope").combine_chunks(); pay = t.column("payload").combine_chunks()
occ = [ms(s) for s in env.field("occurred_at").to_pylist()]
tid = pay.field("transaction_id").to_pylist(); acct = pay.field("account_id").to_pylist(); amt = pay.field("amount_minor").to_pylist()
Y = [labels[x][0] for x in tid]; PAT = [labels[x][1] for x in tid]
out = {}
# fixed same-account gaps
order = sorted(range(len(tid)), key=lambda i: (acct[i], occ[i], tid[i]))
bands = {"exactly_60000": lambda g: g == 60_000, "59000_61000": lambda g: 59_000 <= g <= 61_000}
counts = {b: collections.Counter() for b in bands}; tot = collections.Counter()
for k in range(1, len(order)):
    i, j = order[k - 1], order[k]
    if acct[i] != acct[j]:
        continue
    g = occ[j] - occ[i]
    key = PAT[j] if Y[j] else "LEGIT"
    tot[key] += 1
    for b, f in bands.items():
        if f(g):
            counts[b][key] += 1
out["same_account_gap_share_of_rows_with_a_previous_tx"] = {b: {k: round(counts[b][k] / tot[k], 5) for k in sorted(tot)} for b in bands}
# identity -> device offsets on the same account
side = {}
for topic in ("identity.events.v1", "device.events.v1"):
    tt = pq.read_table(base / f"{topic}.parquet"); e = tt.column("envelope").combine_chunks(); p = tt.column("payload").combine_chunks()
    side[topic] = list(zip(p.field("account_id").to_pylist(), [ms(s) for s in e.field("occurred_at").to_pylist()]))
ident = collections.defaultdict(list)
for a, o in side["identity.events.v1"]:
    ident[a].append(o)
offsets = collections.Counter()
for a, o in side["device.events.v1"]:
    prior = [x for x in ident.get(a, []) if x <= o]
    if prior:
        offsets[o - max(prior)] += 1
out["device_event_offset_after_latest_identity_event_ms"] = dict(offsets.most_common(6))
out["device_events_total"] = len(side["device.events.v1"])
# amount / typical ratio support
def bucket(r):
    return f"2^{math.floor(math.log2(r))}" if r > 0 else "0"
dist = collections.defaultdict(collections.Counter)
for a, x, y, p in zip(acct, amt, Y, PAT):
    typical = max(1, int(math.exp(profiles[a].amount_mu)))
    dist[p if y else "LEGIT"][bucket(x / typical)] += 1
out["amount_over_typical_log2_buckets"] = {k: {b: round(v / sum(c.values()), 3) for b, v in sorted(c.items(), key=lambda kv: float(kv[0][2:]) if kv[0] != "0" else -99)} for k, c in dist.items()}
print(json.dumps(out, indent=1))
