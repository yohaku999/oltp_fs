FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    binutils \
    ca-certificates \
    git \
    linux-perf \
    perl \
    && rm -rf /var/lib/apt/lists/* \
    && git clone --depth 1 https://github.com/brendangregg/FlameGraph.git /opt/FlameGraph

WORKDIR /work