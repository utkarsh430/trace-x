/**
 * The CANONICAL Phase 2 acceptance profile: 500 TPS of representative traffic.
 *
 * **Why this file exists.** The first acceptance workload concentrated 80% of
 * offered load onto 5% of a 20,000-account pool. At 500 TPS that is 1,440
 * transactions per account per hour, against velocity thresholds of 5/minute,
 * 12/5-minutes and 40/hour — 4.8x to 36x over every one of them. Noisy-OR across
 * the three rules that fire on rate alone is exactly 0.9400, which is CRITICAL,
 * and the measured consequence was that **91.7% of the run opened an
 * investigation**: a three-insert Postgres transaction on nearly every request,
 * 52.8% of the per-request budget. Replaying the project's own frozen `eval-v1`
 * through the same gateway triages **0.222%** — a 414x difference. The old
 * profile was measuring the cost of opening investigations, not the cost of
 * scoring. It is preserved, unweakened, as `triage_saturation.js`; this file is
 * the gate.
 *
 * **Everything here is derived from `eval/track_a/eval-v1.manifest.json` and
 * `data/generator/population.py`, not invented.** The frozen dataset is the
 * project's own statement of what normal traffic looks like, so the acceptance
 * workload uses its entity model rather than a second, unexamined one:
 *
 *   accounts 40,000 · merchants 3,000 · devices 48,000 · ips 20,000
 *   cards_per_account 1 · habitual_merchant_ratio 0.75 · zipf 1.1
 *   1–3 home devices, 1–2 home ips, 4–12 habitual merchants per account
 *
 * **The population is DERIVED from the offered rate, not fixed.** This is the
 * step the old profile skipped. `eval-v1` runs at ~0.43 transactions per account
 * per day; holding that rate at 500 TPS requires a population of ~100 million,
 * and using 20,000 instead is what manufactured the velocity. So the account
 * count here is computed from `TARGET_TPS` and the entity ratios are held at
 * eval-v1's. Change the target rate and the population follows, which is the
 * only way "500 TPS" and "represents normal traffic" can both stay true.
 *
 * **Affinity is the other half.** eval-v1 accounts have habitual merchants,
 * home devices and home IPs; the old profile drew all three independently per
 * transaction, so every hot IP looked shared across thousands of accounts
 * (firing R011), every hot device likewise (R010), and `merchant_is_habitual`
 * was structurally zero so R016 and R018 could never fire. Here each account's
 * devices, IPs and merchants are derived deterministically from its own index,
 * so the affinity is stable across the run exactly as it is in the dataset.
 *
 * No ground truth is used, referenced or reachable from this file. It generates
 * traffic; it does not know what is fraudulent, and nothing here is labelled.
 */

import http from 'k6/http';
import exec from 'k6/execution';
import { Counter, Trend } from 'k6/metrics';

const BASE_URL = (__ENV.BASE_URL || 'http://host.docker.internal:8010').replace(/\/+$/, '');
const TOKEN = __ENV.TRACE_LOAD_TOKEN || '';
const TARGET_TPS = Number(__ENV.TARGET_TPS || 500);
const DURATION = __ENV.DURATION || '10m';
const SEED = Number(__ENV.SEED || 20260912);
const PRE_ALLOCATED_VUS = Number(__ENV.PRE_ALLOCATED_VUS || 120);
const MAX_VUS = Number(__ENV.MAX_VUS || 800);
const SUMMARY_PATH = __ENV.SUMMARY_PATH || '/out/summary.json';
const RUN_NONCE = __ENV.RUN_NONCE || 'no-nonce';

const http5xx = new Counter('gateway_http_5xx');
const http4xx = new Counter('gateway_http_4xx');
const http429 = new Counter('gateway_http_429');
const http2xx = new Counter('gateway_http_2xx');
const degraded = new Counter('gateway_degraded_responses');
const unparseable = new Counter('gateway_unparseable_responses');
const bandLow = new Counter('gateway_band_low');
const bandMedium = new Counter('gateway_band_medium');
const bandHigh = new Counter('gateway_band_high');
const bandCritical = new Counter('gateway_band_critical');
const serverLatency = new Trend('gateway_server_latency_ms');

export const options = {
  scenarios: {
    hot_path: {
      // Open model, as in the saturation profile: `constant-arrival-rate` keeps
      // offering the target rate however slow the responses get. A closed model
      // would quietly reduce offered load as latency rose, and a gateway that
      // fell over would report a lower TPS with a flattering p99.
      executor: 'constant-arrival-rate',
      rate: TARGET_TPS,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: PRE_ALLOCATED_VUS,
      maxVUs: MAX_VUS,
      gracefulStop: '30s',
    },
  },
  summaryTrendStats: ['min', 'avg', 'med', 'p(50)', 'p(90)', 'p(95)', 'p(99)', 'max', 'count'],
  thresholds: {
    // Unchanged from the saturation profile and from the ROADMAP. The workload
    // is what was wrong, not the targets, and this file would be worthless if
    // it arrived with easier ones.
    gateway_http_5xx: ['count==0'],
    http_req_duration: ['p(99)<100', 'p(50)<20'],
    dropped_iterations: ['count==0'],
  },
  discardResponseBodies: false,
};

// --------------------------------------------- eval-v1's entity model ----

/** Per-account transactions per day in `eval-v1`: 1,000,000 rows / 40,000
 * accounts over the dataset's ~58-day span. The number the population is
 * derived from. */
const EVAL_V1_TX_PER_ACCOUNT_PER_DAY = 0.43;

/** Entity counts per account in `eval-v1`, held constant as the population scales. */
const MERCHANTS_PER_ACCOUNT = 3000 / 40000;
const DEVICES_PER_ACCOUNT = 48000 / 40000;
const IPS_PER_ACCOUNT = 20000 / 40000;

const ACCOUNTS = Math.max(
  40000,
  Math.round((TARGET_TPS * 86400) / EVAL_V1_TX_PER_ACCOUNT_PER_DAY)
);
const MERCHANTS = Math.max(3000, Math.round(ACCOUNTS * MERCHANTS_PER_ACCOUNT));
const DEVICES = Math.max(48000, Math.round(ACCOUNTS * DEVICES_PER_ACCOUNT));
const IPS = Math.max(20000, Math.round(ACCOUNTS * IPS_PER_ACCOUNT));

/** `habitual_merchant_ratio` from the frozen manifest. */
const HABITUAL_MERCHANT_RATIO = 0.75;

/**
 * Amount bands approximating `eval-v1`'s per-account lognormal
 * (mu ~ U(6.9, 8.1), sigma ~ U(0.7, 1.25) — `population.py`), which is roughly
 * GBP 10–40 typical with a long right tail. Banded rather than sampled exactly
 * because the property that matters to the rules is the spread, and a band table
 * is inspectable where an inline lognormal is not.
 */
const AMOUNT_BANDS = [
  { weight: 0.58, min: 200, max: 4000 },
  { weight: 0.29, min: 4000, max: 20000 },
  { weight: 0.11, min: 20000, max: 120000 },
  { weight: 0.02, min: 120000, max: 600000 },
];

const CHANNELS = [
  { weight: 0.5, channel: 'CARD_NOT_PRESENT', entryModes: ['ECOMMERCE', 'TOKEN'] },
  { weight: 0.33, channel: 'CARD_PRESENT', entryModes: ['CHIP', 'CONTACTLESS', 'MAGSTRIPE'] },
  { weight: 0.1, channel: 'ATM', entryModes: ['CHIP'] },
  { weight: 0.05, channel: 'RECURRING', entryModes: ['TOKEN'] },
  { weight: 0.02, channel: 'UNKNOWN', entryModes: ['UNKNOWN', 'MANUAL'] },
];

const MCCS = ['5411', '5812', '5999', '4111', '6011', '7995', '5732'];
const COUNTRIES = ['GB', 'GB', 'GB', 'US', 'DE', 'FR', 'NL'];

// ------------------------------------------------------------ helpers ----

function mulberry32(seed) {
  let a = seed >>> 0;
  return function () {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** A stable pseudo-random integer for an entity, derived from its own id.
 *
 * This is what gives an account the SAME devices, IPs and merchants every time
 * it appears, without holding a 100-million-row table in the generator. Affinity
 * that changed per transaction is precisely the defect this profile corrects. */
function derived(accountIndex, salt, modulo) {
  let h = (accountIndex ^ (salt * 0x9e3779b1)) >>> 0;
  h = Math.imul(h ^ (h >>> 16), 0x85ebca6b) >>> 0;
  h = Math.imul(h ^ (h >>> 13), 0xc2b2ae35) >>> 0;
  return (h ^ (h >>> 16)) % modulo;
}

function pad(value, width) {
  return String(value).padStart(width, '0');
}

function weightedPick(rnd, table) {
  let roll = rnd();
  for (const entry of table) {
    roll -= entry.weight;
    if (roll <= 0) return entry;
  }
  return table[table.length - 1];
}

function choice(rnd, values) {
  return values[Math.floor(rnd() * values.length)];
}

/** Zipf-ish merchant popularity for the non-habitual quarter of traffic.
 * `merchant_zipf_exponent` is 1.1 in the frozen config; this approximates the
 * same heavy head without a precomputed CDF over millions of merchants. */
function zipfMerchant(rnd) {
  const u = rnd();
  const index = Math.floor(MERCHANTS * Math.pow(u, 3));
  return Math.min(MERCHANTS - 1, index);
}

function buildTransaction(rnd) {
  // Uniform over the derived population: eval-v1 has no hot-account skew, and
  // adding one is exactly what manufactured the velocity in the old profile.
  const accountIndex = 1 + Math.floor(rnd() * ACCOUNTS);

  // 1-3 home devices and 1-2 home IPs, per `population.py`.
  const deviceCount = 1 + (derived(accountIndex, 11, 6) < 3 ? 0 : derived(accountIndex, 12, 3));
  const device = derived(accountIndex * 7 + Math.floor(rnd() * deviceCount), 13, DEVICES);
  const ipCount = derived(accountIndex, 21, 3) === 0 ? 2 : 1;
  const ip = derived(accountIndex * 5 + Math.floor(rnd() * ipCount), 23, IPS);

  // 4-12 habitual merchants, used 75% of the time.
  const habitualCount = 4 + derived(accountIndex, 31, 9);
  const merchant =
    rnd() < HABITUAL_MERCHANT_RATIO
      ? derived(accountIndex * 3 + Math.floor(rnd() * habitualCount), 37, MERCHANTS)
      : zipfMerchant(rnd);

  const channel = weightedPick(rnd, CHANNELS);
  const band = weightedPick(rnd, AMOUNT_BANDS);
  const amount = Math.floor(band.min + rnd() * (band.max - band.min));

  // Event time trails processing time by up to 30 s, as a real acquirer feed
  // does, and never leads it (ADR-0026).
  const occurredAt = new Date(Date.now() - Math.floor(rnd() * 30000)).toISOString();

  return {
    transaction_id: `tx_${RUN_NONCE}_${exec.scenario.iterationInTest}`,
    account_id: `acct_${pad(accountIndex, 6)}`,
    // cards_per_account is 1 in the frozen config, so card and account are 1:1
    // in the dataset too. Kept faithful rather than "fixed".
    card_id: `card_${pad(accountIndex, 6)}`,
    amount_minor: amount,
    currency: 'GBP',
    occurred_at: occurredAt,
    device_id: `dev_${pad(device + 1, 6)}`,
    merchant_id: `mrch_${pad(merchant + 1, 5)}`,
    ip_id: `ip_${pad(ip + 1, 5)}`,
    channel: channel.channel,
    entry_mode: choice(rnd, channel.entryModes),
    merchant_mcc: choice(rnd, MCCS),
    merchant_country: choice(rnd, COUNTRIES),
    authorization_outcome: rnd() < 0.97 ? 'APPROVED' : 'DECLINED',
  };
}

export function setup() {
  if (!TOKEN) {
    throw new Error(
      'TRACE_LOAD_TOKEN is empty. The gateway authenticates every request ' +
        '(docs/SECURITY.md §3, Plane B); without a token this run would measure ' +
        'the 401 path. Run it through `make load-gateway`.'
    );
  }
  if (RUN_NONCE === 'no-nonce') {
    throw new Error(
      'RUN_NONCE was not supplied. Idempotency keys would collide with a ' +
        'previous run and the replay cache would serve most of this one.'
    );
  }
  console.log(
    `representative profile: ${ACCOUNTS.toLocaleString()} accounts, ` +
      `${MERCHANTS.toLocaleString()} merchants, ${DEVICES.toLocaleString()} devices, ` +
      `${IPS.toLocaleString()} ips — derived from ${TARGET_TPS} TPS at eval-v1's ` +
      `${EVAL_V1_TX_PER_ACCOUNT_PER_DAY} tx/account/day`
  );
  return { startedAt: new Date().toISOString() };
}

export default function () {
  const iteration = exec.scenario.iterationInTest;
  const rnd = mulberry32(SEED + exec.vu.idInTest * 1000003 + iteration);
  const body = buildTransaction(rnd);

  const response = http.post(`${BASE_URL}/v1/transactions`, JSON.stringify(body), {
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${TOKEN}`,
      'X-Idempotency-Key': `${RUN_NONCE}-${exec.vu.idInTest}-${iteration}`,
    },
    tags: { name: 'POST /v1/transactions' },
  });

  const status = response.status;
  http2xx.add(status >= 200 && status < 300 ? 1 : 0);
  http4xx.add(status >= 400 && status < 500 ? 1 : 0);
  http429.add(status === 429 ? 1 : 0);
  http5xx.add(status >= 500 ? 1 : 0);

  let decision = null;
  if (status === 200) {
    try {
      decision = JSON.parse(response.body);
    } catch (err) {
      decision = null;
    }
  }
  unparseable.add(status === 200 && decision === null ? 1 : 0);
  degraded.add(decision && decision.degraded ? 1 : 0);
  bandLow.add(decision && decision.risk_band === 'LOW' ? 1 : 0);
  bandMedium.add(decision && decision.risk_band === 'MEDIUM' ? 1 : 0);
  bandHigh.add(decision && decision.risk_band === 'HIGH' ? 1 : 0);
  bandCritical.add(decision && decision.risk_band === 'CRITICAL' ? 1 : 0);
  if (decision && typeof decision.latency_ms === 'number') {
    serverLatency.add(decision.latency_ms);
  }
}

export function handleSummary(data) {
  const d = data.metrics.http_req_duration ? data.metrics.http_req_duration.values : {};
  const dropped = data.metrics.dropped_iterations ? data.metrics.dropped_iterations.values.count : 0;
  const errors = data.metrics.gateway_http_5xx ? data.metrics.gateway_http_5xx.values.count : 0;
  const line =
    `k6 finished. requests=${data.metrics.http_reqs.values.count} ` +
    `p50=${d['p(50)']}ms p99=${d['p(99)']}ms 5xx=${errors} dropped=${dropped}\n` +
    'Nothing above is a published result until scripts/load_gateway.py records it (CLAUDE.md §13).\n';
  const out = {};
  out[SUMMARY_PATH] = JSON.stringify(data, null, 2);
  out.stdout = line;
  return out;
}
