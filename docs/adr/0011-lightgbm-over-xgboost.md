# ADR-0011: LightGBM over XGBoost for the supervised model

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 4

## Context
The supervised fraud model trains on Gold features with high-cardinality categoricals (merchant, MCC,
country, device), a ~0.5% positive rate, and must be retrained frequently on a laptop within a
constrained memory budget. Inference must add under 15 ms p99 to the hot path.

## Decision
LightGBM, with:
- native categorical handling (no one-hot explosion on high-cardinality fields)
- `scale_pos_weight` for imbalance, and **PR-AUC as the selection metric** (ROC-AUC is reported but not
  selected on, being uninformative at this imbalance)
- temporal train/validation/test splits only
- isotonic calibration fitted on validation, with Brier score and a reliability curve reported

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| XGBoost | Comparable accuracy in practice. Rejected on operational grounds: slower training and higher memory on this hardware, and categorical support that arrived later and is less mature. **If measured PR-AUC differs materially, this decision should be revisited** — the difference is expected to be small |
| CatBoost | Excellent categorical handling; heavier dependency and slower training for no expected accuracy gain here |
| Logistic regression | Would be a fine interpretable baseline but is unlikely to capture the interaction structure the fraud scenarios encode. The rules arm (A) already provides an interpretable floor |
| A neural model | Tabular data at this scale does not justify it; slower, harder to explain, and worse on small data |

## Consequences
**Positive.** Fast retraining on constrained hardware. Native categoricals keep the feature matrix
small. SHAP integrates cleanly for per-decision explanations.
**Negative.** Another C++ build dependency. Categorical handling differences mean a model is not
directly portable to XGBoost without re-tuning.
**Risks.** Choosing on operational grounds rather than measured accuracy. Mitigation: Phase 4 records
both arms' PR-AUC; if XGBoost wins materially, a superseding ADR follows.

## Status
Accepted
