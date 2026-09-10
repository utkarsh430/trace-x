# TRACE-X — Evaluation Specification

> Authoritative for how TRACE-X is measured and what may be claimed.
>
> ## Standing caveat — reproduce verbatim in every report header
>
> > *Synthetic benchmark results measure relative arm performance under known causal ground truth.
> > They do **not** estimate real-world fraud detection performance. See Track B (run_id …) for
> > external generalization.*

---

## 1. Two tracks, rigorously separated

Most agent "benchmarks" are self-graded on data the author generated. TRACE-X separates two different
claims that require two different datasets, and the separation is **enforced in code**, not promised.

| | **Track A — Controlled Synthetic Causal Benchmark** | **Track B — External Real-World Data Validation** |
|---|---|---|
| Dataset | TRACE-X generator, seeded, digest-pinned (`eval-v1`) | IEEE-CIS Fraud Detection |
| Scale | Configurable; ≥ 1 000 investigations per run | 590,540 transactions × 394 features, ~3.5% fraud; 144,233 identity rows |
| Ground truth | `is_fraud`, `fraud_pattern`, **`causal_evidence_keys`** | `isFraud` label only; features obfuscated (`V1–V339`, `C1–C14`, `D1–D15`, `M1–M9`) |
| Validates | Rules, ensemble, **agent reasoning, evidence precision/recall, unsupported-claim rate** | **Ingestion adaptability, schema adapters, feature engineering, ML generalization, distribution shift** |
| Full multi-agent investigation? | Yes — the only track with a causal evidence surface | **No.** Its evidence surface is too thin |
| Results table | `eval.synthetic_runs` | `eval.external_runs` |
| Report | `docs/eval/RESULTS-<run_id>.md` | `docs/eval/EXTERNAL-<run_id>.md` |
| May claim | Relative comparisons between arms on synthetic data | ML generalization outside generated data; distribution-shift behaviour |
| May **never** claim | That synthetic accuracy predicts real-world performance | **Any agent-quality or evidence-quality metric** |

**Enforcement:** disjoint tables that are never unioned, disjoint report templates, a runtime guard that
raises if an agent-quality metric is written to `eval.external_runs`, and a CI linter that fails the
build if a Track-B `run_id` is cited beside an agent-quality metric.

---

## 2. Frozen datasets and ground-truth isolation

**Frozen datasets.** `eval-v1` is generated once from a recorded seed and referenced by SHA-256 digest.
It is never regenerated in place. Any change produces `eval-v2` and a manifest-diff note; results across
dataset versions are never compared without that note.

**Ground-truth isolation is structural, not procedural.** Labels, `fraud_pattern` and
`causal_evidence_keys` live in the Postgres schema `groundtruth`. `trace_app` — the role the
application and every agent use — has **no grant on that schema at all**. Only `trace_eval` may read it,
and only the harness connects as `trace_eval`.

A test asserts `trace_app` receives `permission denied for schema groundtruth`. **That test failing
blocks release.** Leakage would silently invalidate every metric in the project, which is why the
control is a database grant rather than a code convention.

Agents are never given ground truth in any form, including indirectly through a derived feature.

---

## 3. Track A — arms

All arms run on the identical frozen dataset with the identical seed.

| Arm | Description | Purpose |
|---|---|---|
| **A** | Rules only | The floor |
| **B** | ML only | Is the model earning anything? |
| **C** | Rules + ML ensemble | The non-agentic production baseline |
| **D** | **Single-agent** — one LLM, all tools, ReAct loop | The honest agentic baseline |
| **E** | **TRACE-X multi-agent** | The system |
| **F** | TRACE-X **minus Skeptic** (ablation) | Does adversarial review pay for itself? |
| **G** | TRACE-X **minus Graph** (ablation) | Does the graph tier earn its cost? |

**D vs E** is the headline comparison. **F and G are what make it credible** — an improvement claimed
without ablations is an improvement of unknown origin.

## 4. Track B — experiments

| Exp | Description | Purpose |
|---|---|---|
| **E1** | External-native: train + test on IEEE-CIS temporal split | The achievable ceiling on real data |
| **E2** | Zero-shot transfer: synthetic-trained model scored on IEEE-CIS, intersecting features only | **Quantifies the synthetic-to-real gap** |
| **E3** | Fine-tune transfer: synthetic-pretrained → IEEE-CIS-adapted | Does synthetic pretraining help at all? |
| **E4** | Distribution shift: PSI + KS per shared feature; score-distribution stability | Characterises the shift |
| **E5** | Operational smoke: full pipeline runs on foreign data | **Pass/fail, not a metric** |

**E2 will very likely degrade substantially. That is the expected, scientifically honest result and it
is reported prominently.** Its entire purpose is to *quantify* the gap, which is the evidence that
prevents the unsupported claim this specification forbids. A large gap does not fail Phase 4B.
Concealing or re-framing it violates the project constitution.

Every transfer metric is reported alongside the **size of the intersecting feature subset**, so a thin
overlap visibly invalidates the metric rather than quietly weakening it.

---

## 5. Metrics

### ML metrics (both tracks)
precision · recall · F1 · **PR-AUC (primary)** · ROC-AUC (reported, not selected on) · FPR at fixed
recall · Brier score.

PR-AUC is primary because the positive rate is ~0.5% (Track A) / ~3.5% (Track B); ROC-AUC is
uninformative at that imbalance.

### Agent metrics (Track A only)

| Metric | Definition — mechanically computable |
|---|---|
| Investigation accuracy | verdict vs `is_fraud` |
| **Evidence precision** | `\|cited ∩ causal\| / \|cited\|` |
| **Evidence recall** | `\|cited ∩ causal\| / \|causal\|` |
| **Unsupported-claim rate** | fraction of `rationale[].claim` whose `evidence_ids` do not resolve to a real evidence record — schema-checkable, zero judgment |
| Tool-call success rate | successful ÷ attempted |
| **Unnecessary-tool-call rate** | calls producing evidence cited by no surviving hypothesis |
| Investigation latency | p50 / p95 wall clock |
| **Cost per investigation** | USD from token accounting on the `LLMProvider` port |
| Human override rate | analyst overrides ÷ decisions |
| Agent disagreement rate | investigations with surviving Skeptic dissent |
| Investigation steps | router iterations |
| Budget-exhaustion rate | `INSUFFICIENT_EVIDENCE` from budget ÷ total |

**None of these require an LLM judge.** `causal_evidence_keys` from the generator make evidence
precision/recall set operations, and mandatory `evidence_ids` citation makes unsupported-claim rate a
resolution check. An LLM judge is used only as a *secondary* signal against a human-labelled calibration
set, and never as a headline metric.

---

## 6. Run manifest — the complete reproducibility record

Every run, both tracks, emits a `RunManifest` to `eval.run_manifests` and commits it alongside its
report. **A run that cannot produce a complete manifest is not a valid run** and the harness refuses to
record it.

```yaml
run_id: 2026-…-a7f3          track: SYNTHETIC | EXTERNAL
# --- data ---
dataset_name / dataset_version / dataset_digest      # sha256 of the frozen dataset
generator_version                                    # git tag of data/generator (Track A)
fraud_scenario_config_digest                         # scenario mix, rates, seed
source_adapter_id / source_adapter_version           # Track B
# --- runtime ---
spark_version: 4.0.1        delta_version: 4.0.1     hadoop_version: 3.4.x
java_version: temurin-17    python_version: 3.12.x
env_lock_digest                                      # sha256 of the dependency lock
container_image_digests: {gateway, worker, stream}   # by sha256, never by tag
# --- model ---
ml_model_digest / ml_model_version / calibration_version
ensemble_threshold_version
# --- llm ---
llm_provider / llm_tier / llm_model_id
llm_inference_config                                 # temperature, top_p, max_tokens, seed
# --- agentic ---
agent_graph_digest          # topology + router version
agent_spec_digest           # all AgentSpec manifests
prompt_digest / prompt_version
tool_contract_digest / tool_registry_version
mcp_server_versions: {fraud-intelligence, identity, graph}
policy_version
# --- provenance ---
git_commit_sha              dirty_worktree: false
started_at / finished_at    cassette_digest
```

Reruns from a manifest are byte-comparable in `CI` tier. Any field differing between two runs appears
as a **manifest diff** in the regression report, so a metric movement is always attributable to a
specific input change rather than to noise.

---

## 7. Cost measurement

Token accounting middleware on the `LLMProvider` port records input/output tokens per agent per call,
priced by a committed rate table keyed on `llm_model_id`. Reported as cost per investigation (p50, p95,
mean) per arm. The rate table is versioned; a price change is a manifest diff, not a silent re-scoring.

Non-LLM cost — container time, storage, query cost — is reported separately for the cloud window only.

---

## 8. Benchmark integrity rules (enforced by `make check-claims`)

1. **No number appears in `README.md`, `docs/**`, a résumé, or a dashboard unless it cites a `run_id`
   that resolves in `eval.run_manifests`.**
2. **Quality numbers may only come from `llm_tier: EVAL`.** `SMOKE`, `DEV` and `CI` may publish
   latency, cost and operational metrics only — never accuracy or evidence quality.
3. **A Track-B `run_id` may never be cited beside an agent-quality metric.**
4. **`dirty_worktree: true` invalidates a run for publication.**
5. **No hardcoded expectations in benchmark tests.** `test_harness_integrity` swaps in a constant
   predictor; the harness must fail. If it passes, the benchmark measures nothing.

Additionally, by constitution (`CLAUDE.md` §13/§17):

- No document may imply synthetic accuracy represents real-world performance.
- Unfavourable results — a failed ablation, a large transfer gap, an anomaly detector beaten by a
  z-score — are **published as found**. Concealing or re-framing them is a violation.
- Results are reported *before* they are interpreted, and interpretation is labelled as such.

---

## 9. Known limitations, stated up front

| Limitation | Why it matters | How it is handled |
|---|---|---|
| Synthetic fraud is more separable than real fraud | Track A PR-AUC will look better than reality | Track B E2 quantifies the gap; the caveat is mandatory in every report |
| IEEE-CIS coverage is partial — no merchant id, no lat/lon, obfuscated timedeltas | Roughly half the canonical feature set is `UNAVAILABLE` | `field_coverage` makes it explicit; intersecting-subset size reported beside every transfer metric |
| IEEE-CIS is card-not-present e-commerce only | Does not represent card-present or ATM fraud | Stated in every Track-B report |
| A single external dataset is not "the real world" | Generalization to one dataset is not generalization | Claims are scoped to IEEE-CIS by name, never to "real-world fraud" |
| LLM nondeterminism | Reruns vary even at temperature 0 | Cassette replay for reproducibility; live runs report variance across repeats |
| Local `SMOKE` tier is materially weaker than `EVAL` | Demo output is not representative of quality | Tier gating blocks publication; the difference is stated wherever it could mislead |
