FROM  python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
LABEL org.opencontainers.image.title="graphsense-lib"
LABEL org.opencontainers.image.maintainer="contact@ikna.io"
LABEL org.opencontainers.image.url="https://www.ikna.io/"
LABEL org.opencontainers.image.description="Dockerized Graphsense library for general purpose."
LABEL org.opencontainers.image.source="https://github.com/graphsense/graphsense-lib"

ENV UV_ONLY_BINARY=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    cmake \
    git \
    openssh-client \
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

RUN apt-get purge -y gcc g++ make cmake && apt-get autoremove -y
RUN rm -rf /opt/graphsense/

RUN adduser --system --uid 1000 --home /home/graphsense graphsense
USER graphsense
WORKDIR /home/graphsense/

# Make uv environment available to user
ENV PATH="/opt/graphsense/lib/.venv/bin:$PATH"
