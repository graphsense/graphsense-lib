FROM  python:3.11-alpine3.20
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
LABEL org.opencontainers.image.title="graphsense-tagpack-tool"
LABEL org.opencontainers.image.maintainer="contact@ikna.io"
LABEL org.opencontainers.image.url="https://www.ikna.io/"
LABEL org.opencontainers.image.description="Dockerized Graphsense tagpack tool"
LABEL org.opencontainers.image.source="https://github.com/graphsense/graphsense-tagpack-tool"

RUN apk --no-cache --update --virtual buld-deps add \
    gcc \
    make \
    musl-dev \
    linux-headers \
    python3-dev \
    libuv-dev

RUN apk --no-cache --update add \
    bash \
    shadow \
    git \
    openssh \
    libuv

RUN mkdir -p /opt/graphsense/
ADD ./src/ /opt/graphsense/tool/src
ADD ./.git/ /opt/graphsense/tool/.git
ADD ./Makefile /opt/graphsense/tool/
ADD ./pyproject.toml /opt/graphsense/tool/
ADD ./uv.lock /opt/graphsense/tool/

WORKDIR /opt/graphsense/tool/
RUN make build
RUN pip install dist/tagpack_tool-*.whl

RUN apk del buld-deps
RUN rm -rf /opt/graphsense/

RUN useradd -r -m -u 1000 graphsense
USER graphsense
WORKDIR /home/graphsense/
