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
VPY   := $(VENV)/bin/python
VPIP  := $(VENV)/bin/pip
COMPOSE := docker compose -f deploy/compose.yml --env-file .env

.PHONY: help doctor setup up up-streaming up-full down ps logs test-fast test e2e lint typecheck \
        secrets audit audit-full migrate migrate-down migrate-status lock \
        verify eval eval-external demo seed fetch-external pull-model bench-layout \
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
	@$(PY) scripts/doctor.py

setup: ## Create venv and install dev + core dependencies
	@test -d $(VENV) || $(PY) -m venv $(VENV)
	@$(VPIP) install --quiet --upgrade pip setuptools wheel
	@$(VPIP) install --quiet -e ".[dev]"
	@test -f .env || (cp .env.example .env && echo "  created .env from template")
	@echo "setup complete — run 'make doctor' next"

## ---------------------------------------------------------------------------
## Local stack
## ---------------------------------------------------------------------------
up: ## Start the core profile (postgres, redis) and apply migrations
	@test -f .env || (cp .env.example .env && echo "created .env from template")
	@$(COMPOSE) --profile core up -d --wait
	@$(MAKE) --no-print-directory migrate
	@$(COMPOSE) --profile core ps

up-streaming: ## Start core + streaming (kafka, spark) -- needs ~10 GB free
	@$(MAKE) --no-print-directory up
	@$(COMPOSE) --profile streaming up -d --wait

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

lock: ## Regenerate the hashed dependency lockfile
	@$(VPY) -m piptools compile --quiet --generate-hashes --strip-extras --allow-unsafe \
	  --output-file=requirements.lock --extra=dev --extra=db --extra=obs --extra=api pyproject.toml
	@echo "requirements.lock updated ($$(shasum -a 256 requirements.lock | cut -c1-16)...)"

## ---------------------------------------------------------------------------
## Quality gates
## ---------------------------------------------------------------------------
lint: ## ruff format check + lint
	@$(VPY) -m ruff format --check . && $(VPY) -m ruff check .

typecheck: ## mypy (strict on trace_core)
	@$(VPY) -m mypy

secrets: ## Secret scan (detect-secrets)
	@$(VPY) -m detect_secrets scan --baseline .secrets.baseline 2>/dev/null \
	  || $(VPY) -m detect_secrets scan > .secrets.baseline

audit: ## Dependency vulnerability audit
	@$(VPY) -m pip_audit --skip-editable || true

audit-full: ## Static security scan including LOW severity findings
	@$(VPY) -m bandit -c pyproject.toml -r packages scripts eval migrations

test-fast: ## Unit + property + contract + conformance (no external services)
	@$(VPY) -m pytest -m "not integration and not e2e and not load and not chaos and not external and not cloud and not slow"

test: ## Full local suite except cloud
	@$(VPY) -m pytest -m "not cloud"

acceptance: ## Render the machine-readable acceptance status
	@$(VPY) scripts/acceptance.py report

check-claims: ## Every published number must map to a reproducible run manifest
	@$(VPY) scripts/check_claims.py

verify: ## CANONICAL health check: doctor + lint + typecheck + test-fast + claims + acceptance
	@bash scripts/verify.sh

## ---------------------------------------------------------------------------
## Phase-gated commands (fail explicitly until their phase lands)
## ---------------------------------------------------------------------------
seed:           ## [Phase 1] Generate and load a development dataset
	@$(PY) scripts/phase_guard.py seed 1 "transaction generator + ground truth"
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
bench-layout:   ## [Phase 3] Delta layout benchmark backing ADR-0015
	@$(PY) scripts/phase_guard.py bench-layout 3 "Delta layout benchmark"
pull-model:     ## [Phase 6] Pull the local SMOKE-tier model via Ollama
	@$(PY) scripts/phase_guard.py pull-model 6 "local LLM runtime"

clean: ## Remove caches and build artifacts
	@rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage dist build
	@find . -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true
	@echo "cleaned"
