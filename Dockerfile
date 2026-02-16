# syntax=docker/dockerfile:1.4
FROM  python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
LABEL org.opencontainers.image.title="graphsense-lib"
LABEL org.opencontainers.image.maintainer="contact@iknaio.com"
LABEL org.opencontainers.image.url="https://www.iknaio.com/"
LABEL org.opencontainers.image.description="Dockerized Graphsense library for general purpose"
LABEL org.opencontainers.image.source="https://github.com/graphsense/graphsense-lib"

ENV UV_ONLY_BINARY=1

# REST API environment variables
ENV NUM_WORKERS=
ENV NUM_THREADS=
ENV CONFIG_FILE=./instance/config.yaml
ENV GIT_PYTHON_REFRESH=quiet

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    cmake \
    git \
    openssh-client \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/graphsense/
ADD ./src/ /opt/graphsense/lib/src
ADD ./.git/ /opt/graphsense/lib/.git
ADD ./Makefile /opt/graphsense/lib/
ADD ./pyproject.toml /opt/graphsense/lib/
ADD ./uv.lock /opt/graphsense/lib/

WORKDIR /opt/graphsense/lib/
RUN make build
RUN uv pip install $(ls dist/graphsense_lib-*.whl)[all] --system
RUN uv pip install gunicorn --system

RUN apt-get purge -y gcc g++ make cmake && apt-get autoremove -y
RUN rm -rf /opt/graphsense/

# Inline gunicorn config for REST API
COPY <<EOF /opt/gunicorn-conf.py
import multiprocessing
import os

timeout = 30
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
RUN mkdir -p /srv/graphsense-rest/instance && chown -R graphsense /srv/graphsense-rest
USER graphsense
WORKDIR /srv/graphsense-rest/

# Default: run REST API with gunicorn
# Override with: docker run ... graphsense-cli --help
# Support both ./instance/config.yaml (legacy graphsense-rest) and /config.yaml (new)
CMD ["sh", "-c", "if [ ! -f ./instance/config.yaml ] && [ -f /config.yaml ]; then ln -s /config.yaml ./instance/config.yaml; fi && gunicorn -c /opt/gunicorn-conf.py 'graphsenselib.web.app:create_app()' --worker-class uvicorn.workers.UvicornWorker"]
