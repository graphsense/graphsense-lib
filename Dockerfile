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

# --- Rust clustering wheel FIRST. It depends only on ./rust, so a change to
# the Python sources (./src) no longer invalidates this expensive layer (a
# full release compile of the arrow/pyo3/rayon dependency tree ≈ the single
# biggest step in the build). The cargo registry, git and target dirs are
# BuildKit cache mounts, so those dependencies are compiled once and reused
# across builds rather than recompiled from scratch each time. Cache mounts
# are ephemeral (not part of the image layer), so the finished wheel is
# copied out to /wheels, which IS persisted for the runtime stage to grab.
ADD ./rust/ ./rust
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/opt/graphsense/lib/rust/gs_clustering/target \
    uv pip install --no-cache maturin --system \
    && (cd rust/gs_clustering && maturin build --release) \
    && mkdir -p /wheels \
    && cp rust/gs_clustering/target/wheels/graphsense_clustering-*.whl /wheels/

# --- Python wheel second. Depends on ./src + project metadata; the version is
# set via SETUPTOOLS_SCM_PRETEND_VERSION_*.
ADD ./src/ ./src
ADD ./Makefile ./
ADD ./pyproject.toml ./
ADD ./uv.lock ./
RUN make build

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

# Secondary Java 11 runtime (~136 MB) for Spark jobs submitted against the
# prod standalone cluster: its executors run Java 11 (the hosts are shared
# with Cassandra 4.x, which caps them there), and a Java-17 driver breaks
# Kryo task-result deserialization (java.io.EOFException in TaskResultGetter
# — Kryo writes raw JDK-internal field layouts, which differ across major
# Java versions). Temurin is self-contained incl. its own cacerts. Opt in
# per run with JAVA_HOME=/opt/java11; everything else keeps the default
# Java 17. Drop this layer once the cluster JVM moves to 17+ (needs
# Cassandra 5.x on the shared hosts first; Spark 4 will require it anyway).
COPY --from=eclipse-temurin:11-jre-jammy /opt/java/openjdk /opt/java11

# Pull the two wheels out of the builder stage. Globs work in COPY.
COPY --from=builder /opt/graphsense/lib/dist/graphsense_lib-*.whl /tmp/wheels/
COPY --from=builder /wheels/graphsense_clustering-*.whl /tmp/wheels/

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

# Baked Spark-executor packages for Python-UDF jobs (e.g. pubkey-update).
# On a standalone cluster the executors lack graphsenselib + its native deps
# (coincurve, ...), so UDFs fail with ModuleNotFoundError. We ship a FLAT
# site-packages dir (pip --target) via spark.archives and add it to the
# executors' PYTHONPATH. We deliberately do NOT ship a Python interpreter: the
# executors use their OWN python (the cluster venv), so there is no
# libpython/stdlib relocation problem — the native wheels just need a matching
# CPython ABI (built here on 3.13 => executors must run Python 3.13.x).
# It is MINIMAL (only the crypto stack the UDFs import; graphsenselib's utils
# import path was made pandas-free), so ~13 MB rather than the ~250 MB full env.
# The import smoke test fails the build if a UDF entrypoint can't be imported.
# Reference it from spark_config (file:// => the driver's HTTP file server
# distributes it; no S3/HDFS needed). Do NOT override spark.pyspark.python —
# keep the executors' own python; just put the shipped packages on PYTHONPATH:
#   spark.archives: "file:///opt/graphsense/spark-env.tar.gz#environment"
#   spark.executorEnv.PYTHONPATH: "./environment"
COPY --from=builder /opt/graphsense/lib/dist/graphsense_lib-*.whl /tmp/pkwheel/
RUN mkdir -p /opt/graphsense/spark-env \
    && uv pip install --no-cache --python /usr/local/bin/python3 \
        --target /opt/graphsense/spark-env --no-deps /tmp/pkwheel/graphsense_lib-*.whl \
    && uv pip install --no-cache --python /usr/local/bin/python3 \
        --target /opt/graphsense/spark-env eth-account coincurve base58 bech32 ecdsa \
    && PYTHONPATH=/opt/graphsense/spark-env /usr/local/bin/python3 -c "import graphsenselib.pubkey.extract, graphsenselib.utils.pubkey_to_address, graphsenselib.utils.signature, coincurve, eth_account, eth_keys, ecdsa, base58, bech32; import graphsenselib; assert graphsenselib.__file__.startswith('/opt/graphsense/spark-env'), graphsenselib.__file__; print('spark-env site-packages smoke test OK')" \
    && tar -C /opt/graphsense/spark-env -czf /opt/graphsense/spark-env.tar.gz . \
    && rm -rf /opt/graphsense/spark-env /tmp/pkwheel \
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
