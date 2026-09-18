# ============================================================================
# TRACE-X developer command interface
#
# `make verify` is the CANONICAL health check for this repository.
# Commands belonging to unimplemented phases fail LOUDLY with a phase message —
# they never pretend to succeed. See docs/ROADMAP.md.
# ============================================================================
.DEFAULT_GOAL := help
SHELL := /bin/bash
PY    := python3
VENV  := .venv
# The ONE interpreter every target runs, resolved here and nowhere else. `make
# setup` creates $(VENV) and installs into it, so on a developer machine that is
# the interpreter; on a machine without it -- CI, where setup-python installs
# into the interpreter on PATH -- the same targets run on $(PY) instead. Naming
# $(VENV)/bin/python directly worked on every laptop and failed the contracts
# job with "No such file or directory". Exported so scripts/verify.sh runs the
# same one. Asserted by tests/unit/test_toolchain_consistency.py.
VPY   := $(if $(wildcard $(VENV)/bin/python),$(VENV)/bin/python,$(PY))
export VPY
# `setup` only: it is the target that creates $(VENV), so it cannot resolve.
VPIP  := $(VENV)/bin/pip
# The ONE JDK every target runs on, resolved here the way VPY is above: the
# pinned Temurin when one is installed (scripts/java_home.py confirms the version
# by running the JDK, not by reading a directory name), otherwise whatever the
# caller had -- so `make doctor` reports a missing or wrong JDK instead of this
# line hiding it. Without it, a shell that does not source a profile (tooling, CI
# steps, another terminal) falls back to the system default, which on this
# project's reference machine is Java 25 and cannot run Spark 4.0.1.
JAVA_HOME := $(or $(shell $(PY) scripts/java_home.py 2>/dev/null),$(JAVA_HOME))
export JAVA_HOME
COMPOSE := docker compose -f deploy/compose.yml --env-file .env

.PHONY: help doctor toolchain stream-jars setup up up-streaming up-full down ps logs test-fast test e2e parity lint typecheck \
        secrets audit audit-full migrate migrate-down migrate-status lock ci-status codegen \
        verify eval eval-external demo seed fetch-external pull-model bench-layout \
        codegen-openapi contracts-check contracts-self-test load-gateway load-stream \
        bench-features \
        check-claims acceptance clean not-implemented

## ---------------------------------------------------------------------------
## Help
## ---------------------------------------------------------------------------
help: ## Show available commands
	@echo "TRACE-X — available commands"; echo
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "  Canonical health check:  make verify"
	@echo "  Current phase:           see docs/PROGRESS.md"

## ---------------------------------------------------------------------------
## Environment
## ---------------------------------------------------------------------------
doctor: ## Preflight: versions, pins, disk, RAM, ports, LLM tier
	@$(VPY) scripts/doctor.py

toolchain: ## Print the interpreter every target runs: the venv if `make setup` made it, else PATH
	@echo $(VPY)

stream-jars: ## Fetch and verify the pinned Spark JVM jars (packages/trace_core/stream/jars.lock)
	@$(VPY) scripts/stream_jars.py fetch

setup: ## Create the venv; install every dependency from the hashed locks; fetch verified Spark jars
	@test -d $(VENV) || $(PY) -m venv $(VENV)
	@# The same script every CI job runs. The lock covers every extra mypy's
	@# `files` scope imports (migrations -> db, the lazily-imported OTLP exporter
	@# -> obs, services/ -> api, trace_core.stream -> stream). Asserted by
	@# test_toolchain_consistency.
	@bash scripts/install_locked.sh $(VENV)/bin/python
	@$(VENV)/bin/python scripts/stream_jars.py fetch
	@test -f .env || (cp .env.example .env && echo "  created .env from template")
	@echo "setup complete — run 'make doctor' next"

## ---------------------------------------------------------------------------
## Local stack
## ---------------------------------------------------------------------------
up: ## Start the core profile (postgres, redis, gateway) and apply migrations
	@test -f .env || (cp .env.example .env && echo "created .env from template")
	@$(COMPOSE) --profile core up -d --wait
	@$(MAKE) --no-print-directory migrate
	@$(COMPOSE) --profile core ps

up-streaming: ## Start core + streaming (kafka, spark) -- needs ~10 GB free
	@$(MAKE) --no-print-directory up
	@# core is named too: `worker` depends on `postgres`, and compose refuses a project whose
	@# dependency is outside the active profiles ("depends on undefined service").
	@$(COMPOSE) --profile core --profile streaming up -d --wait

.PHONY: kafka-topics kafka-topics-verify kafka-topics-budget

kafka-topics: ## Create missing Kafka topics from deploy/kafka/topics.yaml, then verify (local broker only)
	@$(VPY) scripts/kafka_topics.py apply --environment local $(ARGS)

kafka-topics-verify: ## Compare the local broker with deploy/kafka/topics.yaml; non-zero on any drift
	@$(VPY) scripts/kafka_topics.py verify --environment local $(ARGS)

kafka-topics-budget: ## Print the local Kafka disk bound derived from deploy/kafka/topics.yaml
	@$(VPY) scripts/kafka_topics.py budget --environment local

up-full: ## Start every profile -- needs ~15 GB free and 8 GB Docker RAM
	@$(MAKE) --no-print-directory up
	@$(COMPOSE) --profile streaming --profile graph --profile ml --profile obs \
	            --profile llm up -d

down: ## Stop the stack (volumes preserved)
	@$(COMPOSE) --profile core --profile streaming --profile graph \
	            --profile ml --profile obs --profile llm down

ps: ## Show stack status
	@$(COMPOSE) ps

logs: ## Tail stack logs
	@$(COMPOSE) logs -f --tail=100

## ---------------------------------------------------------------------------
## Database
## ---------------------------------------------------------------------------
migrate: ## Apply migrations (creates schemas, roles and grants -- ADR-0004)
	@set -a; . ./.env; set +a; $(VPY) -m alembic upgrade head

migrate-down: ## Roll back all migrations
	@set -a; . ./.env; set +a; $(VPY) -m alembic downgrade base

migrate-status: ## Show the current migration revision
	@set -a; . ./.env; set +a; $(VPY) -m alembic current --verbose

lock: ## Regenerate both hashed lockfiles: build backends, then every dependency
	@$(VPY) -m piptools compile --quiet --generate-hashes --allow-unsafe \
	  --output-file=requirements-build.lock requirements-build.in
	@$(VPY) -m piptools compile --quiet --generate-hashes --strip-extras --allow-unsafe \
	  --output-file=requirements.lock --extra=dev --extra=db --extra=obs --extra=api \
	  --extra=gen --extra=stream pyproject.toml
	@$(VPY) scripts/runtime_lock.py
	@echo "requirements.lock updated ($$(shasum -a 256 requirements.lock | cut -c1-16)...)"

## ---------------------------------------------------------------------------
## Quality gates
## ---------------------------------------------------------------------------
codegen: ## Regenerate Pydantic event models FROM the committed JSON Schemas
	@$(VPY) scripts/generate_event_models.py

codegen-openapi: ## Regenerate the committed OpenAPI spec FROM the Pydantic models
	@$(VPY) scripts/generate_openapi.py

contracts-check: ## Breaking-change gate: diff the spec against a base (pinned oasdiff)
	@$(VPY) scripts/openapi_diff.py --base $${BASE:?set BASE=<path to the previous spec>}

contracts-self-test: ## Prove the breaking-change gate still rejects
	@$(VPY) scripts/openapi_diff.py --self-test

lint: ## ruff format check + lint
	@$(VPY) -m ruff format --check . && $(VPY) -m ruff check .

typecheck: ## mypy (strict on trace_core)
	@$(VPY) -m mypy

secrets: ## Secret scan — same definition CI runs
	@$(VPY) scripts/secret_scan.py

audit: ## Dependency vulnerability audit
	@$(VPY) -m pip_audit --skip-editable || true

audit-full: ## Static security scan including LOW severity findings
	@$(VPY) -m bandit -c pyproject.toml -r packages scripts eval migrations

test-fast: ## Unit + property + contract + conformance (no external services)
	@$(VPY) -m pytest -m "not integration and not e2e and not load and not chaos and not external and not cloud and not slow and not stream"

test: ## Full local suite except cloud
	@$(VPY) -m pytest -m "not cloud"

ci-status: ## Show GitHub Actions conclusions for the current commit
	@$(VPY) scripts/ci_status.py

acceptance: ## Render the machine-readable acceptance status
	@$(VPY) scripts/acceptance.py report

load-gateway: ## Measure the gateway against the Phase 2 targets (needs a running gateway)
	@set -a; [ -f .env ] && . ./.env; set +a; $(VPY) scripts/load_gateway.py $(ARGS)

load-stream: ## [Phase 3] Stream throughput + 2-min outage benchmark (needs up-streaming + kafka-topics)
	@set -a; [ -f .env ] && . ./.env; set +a; $(VPY) -m benchmarks.stream_throughput $(ARGS)

bench-features: ## Measure the hybrid distinct-cardinality strategy (ADR-0034)
	@$(VPY) benchmarks/features/bench_cardinality.py \
	  --host $${REDIS_HOST:-localhost} --port $${REDIS_PORT:-6389}

check-claims: ## Every published number must map to a reproducible run manifest
	@$(VPY) scripts/check_claims.py

verify: ## CANONICAL health check: doctor + lint + typecheck + test-fast + claims + acceptance
	@bash scripts/verify.sh

## ---------------------------------------------------------------------------
## Phase-gated commands (fail explicitly until their phase lands)
## ---------------------------------------------------------------------------
seed: ## Generate a Track A dataset, write ground truth, record the run
	@set -a; [ -f .env ] && . ./.env; set +a; $(VPY) -m data.generator.cli $(ARGS)
e2e:            ## [Phase 11] Full-stack end-to-end run, no mocks
	@$(PY) scripts/phase_guard.py e2e 11 "full compose stack with all services"
demo:           ## [Phase 7] Scripted end-to-end investigation (no API key required)
	@$(PY) scripts/phase_guard.py demo 7 "multi-agent investigation"
eval:           ## [Phase 9] Track A synthetic causal benchmark (arms A-G)
	@$(PY) scripts/phase_guard.py eval 9 "evaluation harness"
eval-external:  ## [Phase 4B] Track B external validation (IEEE-CIS, E1-E5)
	@$(PY) scripts/phase_guard.py eval-external 4B "external dataset validation"
fetch-external: ## [Phase 4B] Download + SHA-256 verify the IEEE-CIS dataset
	@$(PY) scripts/phase_guard.py fetch-external 4B "external dataset acquisition"
parity:         ## [Phase 3] Feature parity: unit + mutation self-tests, then the end-to-end run
	@$(VPY) -m pytest -m "parity and not integration and not stream"
	@set -a; [ -f .env ] && . ./.env; set +a; $(VPY) -m pytest -m "integration and stream and parity"
bench-layout:   ## [Phase 3] Delta layout benchmark backing ADR-0015 (heavy: Spark; ARGS="--rows 200000 --reps 2" smokes)
	@set -a; [ -f .env ] && . ./.env; set +a; $(VPY) benchmarks/delta_layout/run.py $(ARGS)
pull-model:     ## [Phase 6] Pull the local SMOKE-tier model via Ollama
	@$(PY) scripts/phase_guard.py pull-model 6 "local LLM runtime"

clean: ## Remove caches and build artifacts
	@rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage dist build
	@find . -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true
	@echo "cleaned"
