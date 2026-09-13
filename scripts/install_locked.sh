#!/usr/bin/env bash
# ============================================================================
# The ONE way dependencies are installed: by `make setup` and by every CI job.
#
# From the hashed lockfiles, never from pyproject ranges. An install that
# resolves ranges gives each machine whatever was newest on the day it ran, and
# a gate that passes on one machine and fails on another is measuring the
# calendar. Asserted by tests/unit/test_toolchain_consistency.py.
#
#   scripts/install_locked.sh <python>
# ============================================================================
set -euo pipefail
PY="${1:?usage: scripts/install_locked.sh <python>}"
cd "$(dirname "$0")/.."

# 1. Build backends first, hash-checked, so nothing below needs pip to fetch an
#    unverified setuptools into an isolated build environment.
"$PY" -m pip install --quiet --require-hashes --no-deps -r requirements-build.lock
# 2. Every dependency, hash-checked, built against the backends installed in step 1.
"$PY" -m pip install --quiet --require-hashes --no-deps --no-build-isolation -r requirements.lock
# 3. The project itself, editable, with no dependency resolution: the lock
#    already installed every dependency.
"$PY" -m pip install --quiet --no-deps --no-build-isolation -e .
# --no-deps bypasses pip's resolver, so confirm the result is consistent.
"$PY" -m pip check
