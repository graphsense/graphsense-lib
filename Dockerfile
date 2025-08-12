FROM  python:3.11-alpine3.20
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
LABEL org.opencontainers.image.title="graphsense-lib"
LABEL org.opencontainers.image.maintainer="contact@ikna.io"
LABEL org.opencontainers.image.url="https://www.ikna.io/"
LABEL org.opencontainers.image.description="Dockerized Graphsense library for general purpose."
LABEL org.opencontainers.image.source="https://github.com/graphsense/graphsense-lib"

ENV UV_ONLY_BINARY=1

RUN apk --no-cache --update --virtual build-deps add \
    gcc \
    g++ \
    make \
    cmake \
    musl-dev \
    linux-headers \
    libuv-dev

RUN apk --no-cache --update add \
    bash \
    shadow \
    git \
    openssh

RUN mkdir -p /opt/graphsense/
ADD ./src/ /opt/graphsense/lib/src
ADD ./.git/ /opt/graphsense/lib/.git
ADD ./Makefile /opt/graphsense/lib/
ADD ./pyproject.toml /opt/graphsense/lib/
ADD ./uv.lock /opt/graphsense/lib/

WORKDIR /opt/graphsense/lib/
RUN make build
RUN uv pip install $(ls dist/graphsense_lib-*.whl)[all] --system

RUN apk del build-deps
RUN rm -rf /opt/graphsense/

RUN useradd -r -m -u 1000 graphsense
USER graphsense
WORKDIR /home/graphsense/

# Make uv environment available to user
ENV PATH="/opt/graphsense/lib/.venv/bin:$PATH"
