# ADR-0012: Isolation Forest for anomaly detection, with a robust z-score control

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 4

## Context
Supervised models only detect fraud resembling labelled history. Novel patterns need an unsupervised
signal. But unsupervised anomaly detection is where projects most often deceive themselves: an
Isolation Forest that flags "unusual" transactions looks impressive and may add nothing over a simple
per-account z-score on amount.

## Decision
Isolation Forest as the anomaly detector, **and a per-account robust z-score (median/MAD) as a
mandatory control** in the evaluation harness.

**An anomaly detector that cannot beat a robust z-score is not earning its place**, and that comparison
is a reported metric, not an assumption. If the control wins, the finding is published and the detector
is reconsidered.

Anomaly output enters the ensemble as a percentile, not a raw score, so its scale cannot silently
dominate the band thresholds.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Isolation Forest with no control | The self-deception this ADR exists to prevent. "It found anomalies" is not evidence of value |
| Autoencoder reconstruction error | Heavier to train and tune, harder to explain to an analyst, and no expected gain on tabular data at this scale |
| Local Outlier Factor | Poor scaling behaviour; expensive at scoring time on the hot path |
| One-class SVM | Does not scale to the training volume; sensitive to kernel choice |
| No anomaly detection at all | Defensible, but forfeits novel-pattern coverage, which is a stated product requirement |

## Consequences
**Positive.** Novel-pattern coverage with an honest measure of whether it adds anything. Percentile
entry keeps ensemble thresholds interpretable.
**Negative.** A third model to train, version, pin and monitor. Isolation Forest scores are not
naturally calibrated and are not probabilities.
**Risks.** The control beats the detector. That is an acceptable and publishable outcome — the risk
being managed here is *not knowing*, not *losing*.

## Status
Accepted
