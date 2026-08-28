# syntax=docker/dockerfile:1.4

# =============================================================================
# Pinned external images, declared as named stages.
#
# Both are pinned rather than floating (uv was :latest, temurin an unpinned
# tag): a digest move on either invalidates every layer below the COPY that
# consumes it, which made prod re-pull hundreds of MB of otherwise unchanged
# apt/Java/site-packages layers.
#
# They are stages instead of inline `COPY --from=ghcr.io/...` refs so each
# pin exists exactly once AND Dependabot can bump it: the docker ecosystem
# only parses FROM lines — image references inside COPY --from are still
# ignored (dependabot-core#5103, PR #12988 open). With the pin on a FROM
# line, the weekly docker updater opens a normal bump PR (tag for uv, digest
# for temurin) and CI proves it before it reaches prod.
# =============================================================================
FROM ghcr.io/astral-sh/uv:0.12.5 AS uv
FROM eclipse-temurin:11-jre-jammy@sha256:372c96aef3c1c32281ecffbf9aa10de22ef9d8335c60033c2498a2ed4edcdb6f AS java11

# =============================================================================
# Stage 1: builder — compiles the Python wheel and the Rust clustering wheel.
# Carries gcc/g++/make/cmake/curl/binutils/rust/libpq-dev; none of it leaks
# into the runtime image.
# =============================================================================
FROM python:3.13-slim-bookworm AS builder
COPY --from=uv /uv /uvx /bin/

ENV UV_ONLY_BINARY=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV GIT_PYTHON_REFRESH=quiet

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

# Version is computed on the host (where the full worktree + git tags are
# available) and handed in here. Inside the container only a subset of the
# tree is COPY'd, so an in-container `git describe` would see "deleted"
# tracked files and emit a dirty/dev0 version even on a clean tag.
# Declared only now, below the Rust step: the value changes on every commit,
# and an ARG/ENV earlier in the stage would invalidate the expensive
# clustering-wheel layer above on every build.
ARG SETUPTOOLS_SCM_PRETEND_VERSION_FOR_GRAPHSENSE_LIB
ENV SETUPTOOLS_SCM_PRETEND_VERSION_FOR_GRAPHSENSE_LIB=${SETUPTOOLS_SCM_PRETEND_VERSION_FOR_GRAPHSENSE_LIB}

# Locked third-party requirements for the runtime stage's dependency layer.
# --no-emit-project/--no-emit-package strip the two wheels built in this
# stage; everything else comes out version-pinned with hashes. The output
# depends only on pyproject.toml + uv.lock content (verified: it is
# identical across pretend versions), so the runtime layer this file feeds
# stays cached across code-only releases.
#
# The second export is the same resolution without hashes, used as a
# constraints file for the Spark-executor env further down (that install
# resolves its own small set of packages, and a hash-bearing constraints
# file would put the whole install into hash-checking mode).
ADD ./pyproject.toml ./uv.lock ./
RUN uv export --frozen --no-dev --no-emit-project \
    --no-emit-package graphsense-clustering \
    --extra all --extra transformation -o /tmp/requirements.txt \
    && uv export --frozen --no-dev --no-emit-project --no-hashes \
    --no-emit-package graphsense-clustering \
    --extra all --extra transformation -o /tmp/constraints.txt

# --- Python wheel second. Depends on ./src + project metadata; the version
# is set via SETUPTOOLS_SCM_PRETEND_VERSION_*.
ADD ./src/ ./src
ADD ./Makefile ./
RUN make build

# =============================================================================
# Stage 2: runtime — fresh slim base, only the wheels and runtime OS deps.
# This is the image that ships.
# =============================================================================
FROM python:3.13-slim-bookworm
COPY --from=uv /uv /uvx /bin/

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
# Pinned by digest in the java11 stage above: a moving tag re-ships this
# ~136 MB layer to prod on every upstream Temurin rebuild, even when nothing
# else changed.
COPY --from=java11 /opt/java/openjdk /opt/java11

# --- Third-party dependency layer, keyed on the exported lockfile only.
# This is the image's biggest layer (~500 MB: pyspark, arrow, pandas, ...).
# It is installed BEFORE the locally-built wheels so a code-only release
# leaves its digest unchanged and prod pulls only the small wheel layer
# below — previously wheels+deps were one fused layer that re-shipped
# entirely on every release. gunicorn arrives lockfile-pinned via the web
# extra (part of all). duckdb's httpfs extension is baked here because
# duckdb itself comes from this layer; the extension dir is chown'd in the
# same RUN (a standalone chown -R would duplicate the files into an extra
# layer — uid 1000 is the graphsense user created further down).
# We deliberately don't run `strip` on the installed .so files: numpy's
# bundled OpenBLAS ships with a hand-crafted ELF layout whose page-aligned
# LOAD segments get corrupted by `strip --strip-unneeded`, breaking
# `import numpy` with "ELF load command address/offset not page-aligned".
# The ~30 MB we'd save is not worth the breakage.
COPY --from=builder /tmp/requirements.txt /tmp/requirements.txt
RUN uv pip install --no-cache --system -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt \
    && mkdir -p /opt/duckdb/extensions \
    && python -c "import duckdb; con = duckdb.connect(); con.execute(\"SET extension_directory='/opt/duckdb/extensions';\"); con.execute('INSTALL httpfs;'); con.execute('LOAD httpfs;')" \
    && chown -R 1000 /opt/duckdb/extensions \
    && find /usr/local/lib/python3.13/site-packages -depth -type d -name "__pycache__" -exec rm -rf {} +

# --- Application layer: only the two locally-built wheels, installed with
# --no-deps (their dependency closure is already present above). Globs work
# in COPY.
#
# Two guards, because --no-deps means nothing verifies the wheels' own
# metadata against what the dependency layer installed:
#   * `uv pip check` catches an unsatisfied *base* requirement.
#   * the import assert catches an unsatisfied *extra* — pip/uv check ignore
#     extras, and cli/main.py imports every optional command group under
#     `try/except ImportError`, so a missing extra dep would otherwise
#     silently ship an image whose CLI is just missing `tagpack`, `web`,
#     `transformation` or `mcp` (no error, no exit code).
COPY --from=builder /wheels/graphsense_clustering-*.whl /tmp/wheels/
COPY --from=builder /opt/graphsense/lib/dist/graphsense_lib-*.whl /tmp/wheels/
RUN uv pip install --no-cache --system --no-deps /tmp/wheels/*.whl \
    && uv pip check \
    && python -c "import gs_clustering, graphsenselib.web.app; from graphsenselib.cli import main as m; missing = [n for n in ('tagpacktool', 'web', 'transformation', 'mcp') if not getattr(m, n + '_cli_available')]; assert not missing, 'CLI groups unavailable (extra dependency missing from the locked requirements): ' + ', '.join(missing); print('cli/extras smoke test OK')" \
    && rm -rf /tmp/wheels \
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
#
# This install resolves its own small dependency set, so it is constrained to
# the same uv.lock resolution the rest of the image uses — otherwise it picks
# whatever PyPI offers on build day and the executors silently run different
# versions than the driver. That is not hypothetical: before the constraint,
# a build shipped eth-utils/eth-typing 6.0.0 to the executors while the
# driver ran the locked 5.3.1/5.2.1 (plus four smaller skews). The parity
# assert below fails the build if any package still diverges.
# Reference it from spark_config (file:// => the driver's HTTP file server
# distributes it; no S3/HDFS needed). Do NOT override spark.pyspark.python —
# keep the executors' own python; just put the shipped packages on PYTHONPATH:
#   spark.archives: "file:///opt/graphsense/spark-env.tar.gz#environment"
#   spark.executorEnv.PYTHONPATH: "./environment"
COPY --from=builder /opt/graphsense/lib/dist/graphsense_lib-*.whl /tmp/pkwheel/
COPY --from=builder /tmp/constraints.txt /tmp/constraints.txt
RUN mkdir -p /opt/graphsense/spark-env \
    && uv pip install --no-cache --python /usr/local/bin/python3 \
        --target /opt/graphsense/spark-env --no-deps /tmp/pkwheel/graphsense_lib-*.whl \
    && uv pip install --no-cache --python /usr/local/bin/python3 \
        -c /tmp/constraints.txt \
        --target /opt/graphsense/spark-env eth-account coincurve base58 bech32 ecdsa \
    && PYTHONPATH=/opt/graphsense/spark-env /usr/local/bin/python3 -c "import graphsenselib.pubkey.extract, graphsenselib.utils.pubkey_to_address, graphsenselib.utils.signature, coincurve, eth_account, eth_keys, ecdsa, base58, bech32; import graphsenselib; assert graphsenselib.__file__.startswith('/opt/graphsense/spark-env'), graphsenselib.__file__; print('spark-env site-packages smoke test OK')" \
    && /usr/local/bin/python3 -c "import importlib.metadata as md; norm = lambda n: n.lower().replace('_', '-'); image = {norm(d.metadata['Name']): d.version for d in md.distributions()}; skew = sorted(f\"{norm(d.metadata['Name'])}: spark-env={d.version} image={image[norm(d.metadata['Name'])]}\" for d in md.distributions(path=['/opt/graphsense/spark-env']) if norm(d.metadata['Name']) != 'graphsense-lib' and norm(d.metadata['Name']) in image and d.version != image[norm(d.metadata['Name'])]); assert not skew, 'spark-env diverges from the locked image versions: ' + '; '.join(skew); print('spark-env/driver version parity OK')" \
    && tar -C /opt/graphsense/spark-env -czf /opt/graphsense/spark-env.tar.gz . \
    && rm -rf /opt/graphsense/spark-env /tmp/pkwheel /tmp/constraints.txt \
    && chown -R 1000 /opt/graphsense \
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

# Ownership is set with the numeric uid 1000 inside the layers that create
# the files — a standalone `chown -R` RUN duplicates every touched file
# into a fresh layer. /opt/duckdb/extensions and /opt/graphsense are
# chown'd in their creating RUNs above; /opt/graphsense must be *writable*
# by the runtime user because Spark writes a Hadoop .crc sidecar next to
# the spark-env archive when serving it via spark.archives (else:
# java.nio.file.AccessDeniedException). The uid must match adduser here.
RUN adduser --system --uid 1000 --home /home/graphsense graphsense
RUN mkdir -p /srv/graphsense-rest/instance /srv/graphsense-rest/docs/static \
    && chown -R 1000 /srv/graphsense-rest
ADD --chown=1000 ./docs/static/ /srv/graphsense-rest/docs/static/
USER graphsense
WORKDIR /srv/graphsense-rest/

# Default: run REST API with gunicorn
# Override with: docker run ... graphsense-cli --help
# Support both ./instance/config.yaml (legacy graphsense-rest) and /config.yaml (new)
CMD ["sh", "-c", "if [ ! -f ./instance/config.yaml ] && [ -f /config.yaml ]; then ln -s /config.yaml ./instance/config.yaml; fi && gunicorn -c /opt/gunicorn-conf.py 'graphsenselib.web.app:create_app()' --worker-class uvicorn.workers.UvicornWorker"]
