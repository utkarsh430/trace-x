# Evaluation harness

Track A (synthetic causal benchmark) and Track B (external real-world validation) live here, kept
structurally separate. See [`docs/EVALUATION.md`](../docs/EVALUATION.md).

| Path | Contents | Phase |
|---|---|---|
| `manifest/` | `RunManifest` builder and validator (ADR-0017). A run that cannot produce a complete manifest is rejected. Committed manifests also live here and are what `make check-claims` resolves against. | 9 |
| `track_a/` | Arms A–G on the frozen synthetic dataset | 9 |
| `track_b/` | Experiments E1–E5 on IEEE-CIS | 4B |
| `harness/` | Shared runner, metric implementations, report rendering | 9 |
| `cassettes/` | Recorded LLM traffic for deterministic replay. **Gitignored** — digests are committed, payloads are not. | 6 |

Not yet implemented. `make eval` and `make eval-external` exit non-zero until their phases land.
