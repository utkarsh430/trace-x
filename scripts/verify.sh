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

VPY=".venv/bin/python"
[[ -x "$VPY" ]] || VPY="python3"

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

if [[ -x .venv/bin/ruff ]]; then
  run_gate "ruff-format"       .venv/bin/ruff format --check .
  run_gate "ruff-lint"         .venv/bin/ruff check .
else
  skip_gate "ruff-format" "dev deps not installed — run 'make setup'"
  skip_gate "ruff-lint"   "dev deps not installed — run 'make setup'"
fi

if [[ -x .venv/bin/mypy ]]; then
  run_gate "mypy"              .venv/bin/mypy
else
  skip_gate "mypy" "dev deps not installed — run 'make setup'"
fi

if [[ -x .venv/bin/pytest ]]; then
  run_gate "test-fast"         .venv/bin/pytest -m "not integration and not e2e and not load and not chaos and not external and not cloud and not slow" -q
else
  skip_gate "test-fast" "dev deps not installed — run 'make setup'"
fi

if [[ -x .venv/bin/bandit ]]; then
  run_gate "bandit"            .venv/bin/bandit -q -ll -c pyproject.toml -r packages scripts eval
else
  skip_gate "bandit" "dev deps not installed — run 'make setup'"
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
