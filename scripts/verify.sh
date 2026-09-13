#!/usr/bin/env bash
# ============================================================================
# `make verify` — the CANONICAL health check for this repository.
#
# Runs every gate that is applicable at the repo's current phase and prints one
# unambiguous verdict. Gates for phases that have not landed are reported as
# SKIP with their owning phase — never silently omitted.
# ============================================================================
set -uo pipefail
cd "$(dirname "$0")/.."

# The interpreter is resolved ONCE, in the Makefile (`VPY`), and arrives in the
# environment when this runs as `make verify`. Run directly, ask make for the
# same answer rather than keep a second copy of the rule here that could drift.
VPY="${VPY:-$(make -s toolchain)}"
have() { "$VPY" -m "$1" --version >/dev/null 2>&1; }   # is this dev tool installed for $VPY?

PASS=0; FAIL=0; SKIP=0
declare -a RESULTS=()

run_gate() {           # run_gate <name> <command...>
  local name="$1"; shift
  local out rc
  out="$("$@" 2>&1)"; rc=$?
  if [[ $rc -eq 0 ]]; then
    RESULTS+=("PASS|$name|"); ((PASS++))
  else
    RESULTS+=("FAIL|$name|$(echo "$out" | tail -5 | tr '\n' ' ')"); ((FAIL++))
  fi
}

skip_gate() {          # skip_gate <name> <reason>
  RESULTS+=("SKIP|$1|$2"); ((SKIP++))
}

echo "TRACE-X verify — $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "======================================================================"

run_gate "doctor"              "$VPY" scripts/doctor.py
run_gate "acceptance-status"   "$VPY" scripts/acceptance.py validate
run_gate "check-claims"        "$VPY" scripts/check_claims.py
# Generated event models must match their schemas. In `make verify` rather than
# CI alone, for the same reason the secret scan was moved here in Phase 0: a
# gate that only runs in CI lets a local run pass while CI goes red.
run_gate "codegen-drift"       "$VPY" scripts/generate_event_models.py --check
# The committed OpenAPI must match the Pydantic models it is generated from
# (docs/API_CONTRACTS.md §1). Same reasoning as the event-model gate above: a
# spec that only CI regenerates lets a local run pass while CI goes red.
run_gate "openapi-drift"       "$VPY" scripts/generate_openapi.py --check

if have ruff; then
  run_gate "ruff-format"       "$VPY" -m ruff format --check .
  run_gate "ruff-lint"         "$VPY" -m ruff check .
else
  skip_gate "ruff-format" "dev deps not installed — run 'make setup'"
  skip_gate "ruff-lint"   "dev deps not installed — run 'make setup'"
fi

if have mypy; then
  run_gate "mypy"              "$VPY" -m mypy
else
  skip_gate "mypy" "dev deps not installed — run 'make setup'"
fi

if have pytest; then
  run_gate "test-fast"         "$VPY" -m pytest -m "not integration and not e2e and not load and not chaos and not external and not cloud and not slow" -q
else
  skip_gate "test-fast" "dev deps not installed — run 'make setup'"
fi

if have bandit; then
  run_gate "bandit"            "$VPY" -m bandit -q -ll -c pyproject.toml -r packages scripts eval migrations
else
  skip_gate "bandit" "dev deps not installed — run 'make setup'"
fi

# The same script CI runs. Secret scanning belongs in the canonical gate: it was
# only in CI, so a local `make verify` could pass while CI went red.
if have detect_secrets; then
  run_gate "secret-scan"       "$VPY" scripts/secret_scan.py
else
  skip_gate "secret-scan" "dev deps not installed — run 'make setup'"
fi

echo
for r in "${RESULTS[@]}"; do
  IFS='|' read -r status name detail <<< "$r"
  case "$status" in
    PASS) printf "  \033[32mPASS\033[0m  %-22s\n" "$name" ;;
    FAIL) printf "  \033[31mFAIL\033[0m  %-22s %s\n" "$name" "${detail:0:120}" ;;
    SKIP) printf "  \033[2mSKIP\033[0m  %-22s \033[2m%s\033[0m\n" "$name" "$detail" ;;
  esac
done

echo "======================================================================"
PHASE=$("$VPY" -c "import json;print(json.load(open('tests/acceptance/status.json'))['current_phase'])" 2>/dev/null || echo "?")
printf "  phase %s   \033[32m%d passed\033[0m   \033[31m%d failed\033[0m   \033[2m%d skipped\033[0m\n" "$PHASE" "$PASS" "$FAIL" "$SKIP"
if [[ $FAIL -gt 0 ]]; then
  echo -e "  \033[31mVERIFY FAILED\033[0m"
  exit 1
fi
echo -e "  \033[32mVERIFY OK\033[0m"
