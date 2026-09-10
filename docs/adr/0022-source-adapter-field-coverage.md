# ADR-0022: SourceAdapter with declared field_coverage — no silent imputation

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 1

## Context
Track B (ADR-0021) requires IEEE-CIS to flow through the same pipeline as generated data. But the two
datasets are shaped very differently: IEEE-CIS has **no true merchant id, no latitude/longitude, and
obfuscated timedeltas** rather than timestamps. It cannot populate the full canonical transaction shape.

The tempting shortcut is to fill missing fields with zeros or defaults so everything "just works". That
shortcut is how a cross-dataset generalization result becomes meaningless: a velocity feature computed
from imputed zeros is not a weak signal, it is a **fabricated** one, and it will look like a real
feature in every downstream report.

## Decision
A `SourceAdapter` port that every inbound dataset implements:

```
describe()      -> SourceProfile { name, version, field_coverage, row_count, digest }
to_canonical(b) -> DataFrame[CanonicalTransaction]
label_column()  -> str
```

`CanonicalTransaction` carries `source_dataset`, `source_row_id` and **`field_coverage`** — the set of
canonical fields this source actually supplies.

Correspondingly, **every feature declares `required_fields`.** A feature whose inputs are not covered
evaluates to **`UNAVAILABLE`** and propagates as null/absent — **never imputed to zero, never
defaulted.** A test asserts this.

Implementations: `GeneratorAdapter` (full coverage) and `IeeeCisAdapter` (partial coverage, with
`V*/C*/D*/M*` carried as a typed `opaque_features` map).

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Impute missing fields with zeros/defaults | Fabricates signal. Produces confident, meaningless transfer metrics — the exact failure this ADR prevents |
| Separate pipeline per dataset | Would make "ingestion adaptability" untestable; two pipelines can diverge and the medallion-unmodified proof becomes impossible |
| Restrict the canonical model to the intersection of all sources | Cripples Track A, which has rich data, to accommodate Track B's limitations |
| Nullable fields without declared coverage | Cannot distinguish "this source never provides it" from "this row happens to be missing it" — a semantically critical difference |

## Consequences
**Positive.** The coverage gap is explicit and measurable. Transfer metrics report the intersecting
feature subset alongside them, so a thin overlap invalidates the metric visibly. One medallion pipeline
serves both datasets, making the Phase 4B "empty git diff" proof possible.
**Negative.** Every feature must declare `required_fields` — real bookkeeping. Downstream code must
handle `UNAVAILABLE` explicitly rather than relying on a numeric default. Models must be trained on the
covered subset per dataset.
**Risks.** `UNAVAILABLE` being coerced to 0 somewhere downstream — reintroducing the exact failure.
Signal: the no-silent-imputation test. This is why that test exists as a phase gate rather than a
convention.

## Status
Accepted
