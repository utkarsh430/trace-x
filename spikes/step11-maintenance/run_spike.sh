#!/usr/bin/env bash
# Runs the Step 11 Phase A spike under the shared heavy-suite lock. The lock is removed on exit.
#
# Portable: every path derives from this repository or from the environment.
#   VPY                interpreter (default: the repository venv, as `make setup` creates it)
#   TRACE_SCRATCH_DIR  where the heavy-suite lock and the spike's lake roots live
#                      (default: ${TMPDIR:-/tmp}/trace-x-scratch)
#   JAVA_HOME          resolved by scripts/java_home.py (the Makefile's resolver); the caller's
#                      value is kept when no pinned JDK is found, so the session factory refuses loudly
set -uo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SP="${TRACE_SCRATCH_DIR:-${TMPDIR:-/tmp}/trace-x-scratch}"
mkdir -p "$SP"
LOCK="$SP/heavy-suite.lock"
PY="${VPY:-$REPO/.venv/bin/python}"
if ! mkdir "$LOCK" 2>/dev/null; then
  echo "heavy-suite lock is held: $(cat "$LOCK/owner" 2>/dev/null)"; exit 75
fi
trap 'rm -rf "$LOCK"' EXIT
echo "Step 11 Phase A spike (pid $$, $(date -u +%FT%TZ))" > "$LOCK/owner"
cd "$REPO" || exit 1
PINNED_JDK="$("$PY" scripts/java_home.py 2>/dev/null || true)"
export JAVA_HOME="${PINNED_JDK:-${JAVA_HOME:-}}"; export PATH="$JAVA_HOME/bin:$PATH"
export PYTHONPATH=packages:.
set -a; . "$REPO/.env"; set +a
RUN_ROOT="$SP/step11-spike-lake/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN_ROOT"
export TRACE_DELTA_ROOT="$RUN_ROOT"
echo "lake root: $RUN_ROOT"
"$PY" -c "import trace_core; print('trace_core from', trace_core.__file__)"
"$PY" spikes/step11-maintenance/delta_retention_spike.py "$@"
status=$?
echo "spike exit status: $status"
exit $status
