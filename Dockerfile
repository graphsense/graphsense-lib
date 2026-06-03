# syntax=docker/dockerfile:1.4

# =============================================================================
# Stage 1: builder — compiles the Python wheel and the Rust clustering wheel.
# Carries gcc/g++/make/cmake/curl/binutils/rust/libpq-dev; none of it leaks
# into the runtime image.
# =============================================================================
FROM python:3.13-slim-bookworm AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_ONLY_BINARY=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV GIT_PYTHON_REFRESH=quiet

# Version is computed on the host (where the full worktree + git tags are
# available) and handed in here. Inside the container only a subset of the
# tree is COPY'd, so an in-container `git describe` would see "deleted"
# tracked files and emit a dirty/dev0 version even on a clean tag.
ARG SETUPTOOLS_SCM_PRETEND_VERSION_FOR_GRAPHSENSE_LIB
ENV SETUPTOOLS_SCM_PRETEND_VERSION_FOR_GRAPHSENSE_LIB=${SETUPTOOLS_SCM_PRETEND_VERSION_FOR_GRAPHSENSE_LIB}

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    cmake \
    git \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Rust toolchain for the gs_clustering crate; minimal profile is enough.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /opt/graphsense/lib/
ADD ./src/ ./src
ADD ./Makefile ./
ADD ./pyproject.toml ./
ADD ./uv.lock ./
ADD ./rust/ ./rust

# Build the Python wheel (sets the version via SETUPTOOLS_SCM_PRETEND_VERSION_*)
# and the Rust clustering wheel via maturin.
RUN make build \
    && uv pip install --no-cache maturin --system \
    && (cd rust/gs_clustering && maturin build --release)

# =============================================================================
# Stage 2: runtime — fresh slim base, only the wheels and runtime OS deps.
# This is the image that ships.
# =============================================================================
FROM python:3.13-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

LABEL org.opencontainers.image.title="graphsense-lib"
LABEL org.opencontainers.image.maintainer="contact@iknaio.com"
LABEL org.opencontainers.image.url="https://www.iknaio.com/"
LABEL org.opencontainers.image.description="Dockerized Graphsense library for general purpose"
LABEL org.opencontainers.image.source="https://github.com/graphsense/graphsense-lib"

ENV UV_ONLY_BINARY=1
# Skip writing .pyc files anywhere; .pyc is recreated on import in the
# writable container layer if needed.
ENV PYTHONDONTWRITEBYTECODE=1

# REST API environment variables
ENV NUM_WORKERS=
ENV NUM_THREADS=
ENV CONFIG_FILE=./instance/config.yaml
ENV GIT_PYTHON_REFRESH=quiet

# Runtime-only OS deps. No compilers, no Rust, no -dev packages.
#   * openjdk-17-jre-headless: PySpark requires Java; Java 21 removed
#     DirectByteBuffer(long,int) which Arrow 12 needs for directBuffer().
#   * libpq5: psycopg's runtime libpq (`libpq-dev` would add headers we
#     don't need here).
#   * git/git-lfs/openssh-client: GitPython + tagpack repo operations
#     still run inside this container at runtime.
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    libpq5 \
    git \
    git-lfs \
    openssh-client \
    && rm -rf /var/lib/apt/lists/* \
    && git lfs install

# Pull the two wheels out of the builder stage. Globs work in COPY.
COPY --from=builder /opt/graphsense/lib/dist/graphsense_lib-*.whl /tmp/wheels/
COPY --from=builder /opt/graphsense/lib/rust/gs_clustering/target/wheels/graphsense_clustering-*manylinux*.whl /tmp/wheels/

# Install the wheels with all required extras, set up duckdb httpfs, then
# drop bytecode caches. We deliberately don't run `strip` on the installed
# .so files: numpy's bundled OpenBLAS ships with a hand-crafted ELF layout
# whose page-aligned LOAD segments get corrupted by `strip --strip-unneeded`,
# breaking `import numpy` with "ELF load command address/offset not
# page-aligned". The ~30 MB we'd save is not worth the breakage.
RUN uv pip install --no-cache --system /tmp/wheels/graphsense_clustering-*.whl \
    && uv pip install --no-cache --system "/tmp/wheels/$(ls /tmp/wheels/graphsense_lib-*.whl | xargs -n1 basename)[all,transformation]" \
    && uv pip install --no-cache --system gunicorn \
    && rm -rf /tmp/wheels \
    && mkdir -p /opt/duckdb/extensions \
    && python -c "import duckdb; con = duckdb.connect(); con.execute(\"SET extension_directory='/opt/duckdb/extensions';\"); con.execute('INSTALL httpfs;'); con.execute('LOAD httpfs;')" \
    && find /usr/local/lib/python3.13/site-packages -depth -type d -name "__pycache__" -exec rm -rf {} +

# Baked Spark-executor environment for Python-UDF jobs (e.g. pubkey-update).
# On a standalone cluster the executors are remote and lack graphsenselib + its
# native deps (coincurve, ...), so UDFs fail with ModuleNotFoundError. We ship a
# relocatable env via spark.archives. It is MINIMAL on purpose: only the crypto
# stack the UDFs import (graphsenselib's utils import path was made pandas-free),
# so this is ~10-20 MB rather than the ~250 MB full [all,transformation] env.
# The interpreter is copied (--copies) for relocatability; executors still need a
# compatible Python 3.13 present (venv does not bundle the stdlib). The import
# smoke test fails the build if the pack can't run the UDF entrypoints.
# Reference it from spark_config (file:// forces the driver's HTTP file server
# to distribute it to executors — no S3/HDFS needed; executors must have a
# compatible Python 3.13):
#   spark.archives: "file:///opt/graphsense/spark-env.tar.gz#environment"
#   spark.pyspark.python: "./environment/bin/python"
#   spark.pyspark.driver.python: "/usr/local/bin/python3"
COPY --from=builder /opt/graphsense/lib/dist/graphsense_lib-*.whl /tmp/pkwheel/
RUN python3 -m venv --copies /opt/spark-env-venv \
    && uv pip install --no-cache --python /opt/spark-env-venv/bin/python --no-deps \
        /tmp/pkwheel/graphsense_lib-*.whl \
    && uv pip install --no-cache --python /opt/spark-env-venv/bin/python \
        eth-account coincurve base58 bech32 ecdsa \
    && /opt/spark-env-venv/bin/python -c "import graphsenselib.pubkey.extract, graphsenselib.utils.pubkey_to_address, graphsenselib.utils.signature, coincurve, eth_account, eth_keys, ecdsa, base58, bech32; print('spark-env import smoke test OK')" \
    && uv pip install --no-cache --system venv-pack \
    && mkdir -p /opt/graphsense \
    && python -m venv_pack -p /opt/spark-env-venv -o /opt/graphsense/spark-env.tar.gz \
    && rm -rf /opt/spark-env-venv /tmp/pkwheel \
    && du -h /opt/graphsense/spark-env.tar.gz

# Inline gunicorn config for REST API
COPY <<EOF /opt/gunicorn-conf.py
import multiprocessing
import os

# Generous timeout for analytical endpoints. Wide BTC txs with
# include_heuristics=all can legitimately need more than 30s when the
# tagstore is warm but cold-cache. Set to 300s to match a typical APISIX
# proxy_read_timeout — workers that go past this are genuinely stuck.
timeout = 300
capture_output = True
accesslog = "-"
errorlog = "-"
loglevel = "debug"
bind = "0.0.0.0:9000"

num = multiprocessing.cpu_count() * 2
try:
    workers = int(os.getenv("NUM_WORKERS", num))
except ValueError:
    workers = num

try:
    threads = int(os.getenv("NUM_THREADS", num))
except ValueError:
    threads = num

try:
    backlog = int(os.getenv("GUNICORN_BACKLOG", "8192"))
except ValueError:
    backlog = 8192


def post_fork(server, worker):
    server.log.info("Worker spawned (pid: %s)", worker.pid)


def pre_fork(server, worker):
    pass


def pre_exec(server):
    server.log.info("Forked child, re-executing.")


def when_ready(server):
    server.log.info("Server is ready. Spawning workers")
EOF

RUN adduser --system --uid 1000 --home /home/graphsense graphsense
RUN chown -R graphsense /opt/duckdb/extensions
# The baked spark-env archive is read (and a Hadoop .crc sidecar written next to
# it) by the non-root runtime user when Spark serves it via spark.archives. Make
# the dir writable by that user, else: java.nio.file.AccessDeniedException.
RUN chown -R graphsense /opt/graphsense
RUN mkdir -p /srv/graphsense-rest/instance
RUN mkdir -p /srv/graphsense-rest/docs/static
ADD ./docs/static/ /srv/graphsense-rest/docs/static/
RUN chown -R graphsense /srv/graphsense-rest
USER graphsense
WORKDIR /srv/graphsense-rest/

# Default: run REST API with gunicorn
# Override with: docker run ... graphsense-cli --help
# Support both ./instance/config.yaml (legacy graphsense-rest) and /config.yaml (new)
CMD ["sh", "-c", "if [ ! -f ./instance/config.yaml ] && [ -f /config.yaml ]; then ln -s /config.yaml ./instance/config.yaml; fi && gunicorn -c /opt/gunicorn-conf.py 'graphsenselib.web.app:create_app()' --worker-class uvicorn.workers.UvicornWorker"]
