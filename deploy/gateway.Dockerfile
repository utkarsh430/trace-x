# ============================================================================
# trace-gateway — the synchronous scoring surface (docs/ARCHITECTURE.md §14).
#
# Built from the REPOSITORY ROOT, not from deploy/:
#     docker build -f deploy/gateway.Dockerfile -t tracex-gateway:dev .
# `deploy/compose.yml` declares that context explicitly, and `.dockerignore`
# narrows it to the four paths this file copies.
#
# Two stages, because the runtime needs neither pip nor the sources the package
# was built from. What ships is a virtualenv and `services/`: a container that
# is compromised has no package manager to install with, and no build toolchain
# to compile with.
# ============================================================================

# Python 3.12 is a pin, not a preference ([tool.trace_x.pins], ADR-0018):
# requirements.lock was resolved against 3.12 and `make doctor` asserts it.
#
# Pinned by tag AND digest, for the reason ADR-0038 gives about the oasdiff and
# k6 images: a tag can be moved, a digest cannot, and `image_digests` is one of
# the fields every run manifest records (ADR-0017). This is the multi-platform
# index digest of python:3.12-slim-bookworm (3.12.14) as published on
# 2026-09-01, so it resolves on both the arm64 laptop and an amd64 CI runner.
# Refresh with: docker buildx imagetools inspect python:3.12-slim-bookworm
#
# -slim rather than -alpine: psycopg, uvloop and httptools publish manylinux
# wheels and no musl ones, so alpine would compile all three from source --
# a toolchain in the image, a far slower build, and a different binary than the
# one the lockfile's hashes cover.
ARG PYTHON_IMAGE=python:3.12-slim-bookworm@sha256:782412e85d0f0984994c290652577d4018aff08145c85b262bb63dc0c7522254


# --------------------------------------------------------------- builder ----
FROM ${PYTHON_IMAGE} AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# A virtualenv rather than the system interpreter, so the runtime stage copies
# one self-contained directory and inherits nothing else this stage installed.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

WORKDIR /src
COPY pyproject.toml requirements.lock ./

# requirements.lock supplies the VERSIONS. It cannot supply the hashes here, and
# that is a property of the committed lockfile rather than a shortcut taken in
# this file -- it was attempted first and it fails:
#
#   pip install --require-hashes -r requirements.lock
#   ERROR: In --require-hashes mode, all requirements must have their versions
#          pinned with ==. These do not:
#              greenlet>=1 (from sqlalchemy==2.0.52)
#
# `make lock` runs on the maintainer's macOS/arm64 machine, where SQLAlchemy's
# marker for greenlet (`platform_machine == "aarch64" or ... "x86_64" ...`) is
# false, so greenlet is absent from the lock. On linux/arm64 and linux/amd64 --
# every container and every CI runner -- the marker is true, pip must resolve a
# package the lock never named, and ANY hash in the file puts pip in
# hash-checking mode, where an unpinned transitive dependency is fatal. Stripping
# the hashes to constraints is therefore not a preference but the only form in
# which this lockfile installs on Linux at all. The fix belongs to `make lock`
# (compile for linux, or with universal markers); this file does not own it, and
# writing a second dependency set here would be worse -- a set nothing verifies.
#
# The requirement list is DERIVED from pyproject rather than restated below. A
# dependency added to `[db,api,obs]` and forgotten here would not fail the build;
# it would fail as an ImportError inside a container, on the hot path, at request
# time. Deriving it also keeps this layer cached against changes to packages/,
# which is the file that actually changes during development.
RUN python -c "import pathlib, re, tomllib; \
lock = pathlib.Path('requirements.lock').read_text().splitlines(); \
pathlib.Path('constraints.txt').write_text('\n'.join(l.split()[0] for l in lock if re.match(r'^[A-Za-z0-9][^ ]*==', l)) + '\n'); \
project = tomllib.loads(pathlib.Path('pyproject.toml').read_text())['project']; \
extras = project['optional-dependencies']; \
reqs = list(project['dependencies']) + [r for e in ('db', 'api', 'obs') for r in extras[e]]; \
pathlib.Path('runtime-requirements.txt').write_text('\n'.join(reqs) + '\n')" \
 && pip install -c constraints.txt -r runtime-requirements.txt

# `trace_core` is installed, not mounted: the wheel carries the rule packs and
# threshold config as package data (pyproject [tool.setuptools.package-data]),
# and a gateway whose rules had to be supplied separately is a gateway that
# cannot boot -- with the pack digest recorded on every decision resolving to
# nothing (ADR-0033, ADR-0034).
#
# --no-deps: everything it needs is already installed at a locked version, and
# without this flag the resolver may satisfy a range from PyPI and put a version
# in the image that the lockfile never saw.
COPY packages/ packages/
RUN pip install --no-deps .


# --------------------------------------------------------------- runtime ----
FROM ${PYTHON_IMAGE} AS runtime

# PYTHONUNBUFFERED: an operator tailing `docker logs` during an incident must
#   not be reading a buffered lie (docs/OPERATIONS.md §4).
# PYTHONDONTWRITEBYTECODE: the serving user cannot write to /app by design, so
#   the interpreter would fail to cache bytecode and retry on every import.
# PYTHONPATH: `services/` is not part of the installed package, so it is
#   imported from /app. Set explicitly rather than relying on uvicorn's
#   --app-dir default, which is a CLI convenience and not a contract.
ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Non-root, and the code is NOT writable by the account that serves traffic.
# The gateway is the system's public surface (docs/SECURITY.md §3): a process
# able to rewrite its own source turns any code-execution bug into persistence.
RUN useradd --system --uid 10001 --create-home --home-dir /app \
            --shell /usr/sbin/nologin trace

WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY services/ services/
USER trace

# 8010 inside and out (ADR-0024: 8000/8080 collide with whatever else the
# developer is running). The container listens on the port compose publishes so
# a port number in a log line, a probe and a runbook all mean the same thing.
EXPOSE 8010

# --workers 1 is deliberate. Each worker process installs its own meter
# provider, so /metrics would report whichever process the scrape happened to
# land on -- ARCHITECTURE §13 wants one definition of every counter. Scaling out
# is the HPA's job (§15), not a fork inside one container.
#
# --host 0.0.0.0 binds inside the container's own network namespace; the
# boundary is the published port in compose, not the bind address.
#
# The factory form, because create_app() builds state at start-up and raises
# when it cannot serve correctly: no service token or no rule pack must stop the
# process, not degrade it (services/gateway/app.py, ARCHITECTURE §18).
CMD ["uvicorn", "services.gateway.app:create_app", "--factory", \
     "--host", "0.0.0.0", "--port", "8010", "--workers", "1"]
