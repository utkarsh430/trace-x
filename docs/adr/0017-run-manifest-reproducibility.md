# ADR-0017: Complete RunManifest — an unmanifested run is not a valid run

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 9

## Context
"No resume benchmark number may be invented" is a project requirement. But a rule against invention is
insufficient: the more common failure is a number that was *measured* and is now *unreproducible* —
nobody can say which dataset, model, prompt, tool contract or Spark version produced it. Such a number
is indistinguishable from an invented one, and both mislead.

Recording only the dataset and model digest is not enough. In this system a metric can move because of
a prompt edit, a tool-contract change, a router change, a Delta version bump, or a policy version — all
invisible in a dataset/model pair.

## Decision
Every evaluation run — Track A **and** Track B — emits a complete `RunManifest` recording **25 fields**:

- **Data:** `dataset_name`, `dataset_version`, `dataset_digest`, `generator_version`,
  `fraud_scenario_config_digest`, `source_adapter_id`, `source_adapter_version`
- **Runtime:** `spark_version`, `delta_version`, `hadoop_version`, `java_version`, `python_version`,
  `env_lock_digest`, `container_image_digests`
- **Model:** `ml_model_digest`, `ml_model_version`, `calibration_version`, `ensemble_threshold_version`
- **LLM:** `llm_provider`, `llm_tier`, `llm_model_id`, `llm_inference_config`
- **Agentic:** `agent_graph_digest`, `agent_spec_digest`, `prompt_digest`, `tool_contract_digest`,
  `mcp_server_versions`, `policy_version`
- **Provenance:** `git_commit_sha`, `dirty_worktree`, timestamps, `cassette_digest`

**A run that cannot produce a complete manifest is rejected by the harness and is not recorded.**
Manifests are **machine-assembled from live introspection**, never hand-written. Any field differing
between two runs appears as a **manifest diff** in the regression report, so a metric movement is always
attributable to a specific input change rather than to noise.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Dataset + model digest only | Cannot explain a metric movement caused by a prompt, tool-contract, router or runtime-version change — which is most of them in an agentic system |
| Git SHA only | Does not capture the dataset, the LLM model id, or the inference configuration, none of which live in the repository |
| Hand-written manifests | Would not survive a deadline. Machine assembly makes the manifest the *cheapest* path rather than the expensive one |
| Log everything to MLflow and stop there | MLflow captures the ML run well but not the agent graph, tool contracts, MCP server versions or policy version |

## Consequences
**Positive.** Every published number is reproducible and attributable. Regression reports explain
*why* a metric moved. `dirty_worktree` prevents publishing from an unrecorded state.
**Negative.** Manifest assembly touches many subsystems, each of which must expose a version or digest.
Introducing a new versioned component means extending the manifest.
**Risks.** The manifest becoming onerous enough to be bypassed. Mitigated by machine assembly and by
making rejection the default for incomplete runs — the path of least resistance is compliance.

## Status
Accepted
