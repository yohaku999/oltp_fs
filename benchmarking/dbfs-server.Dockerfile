FROM debian:bookworm-slim AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    bison \
    flex \
    libspdlog-dev \
    pkg-config \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN cmake -S . -B /tmp/dbfs-build \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_CXX_FLAGS_RELWITHDEBINFO='-O2 -g -fno-omit-frame-pointer' \
    && cmake --build /tmp/dbfs-build --target dbfs_server -- -j1

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    linux-perf \
    procps \
    libspdlog1.10 \
    netcat-openbsd \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /tmp/dbfs-build/dbfs_server /app/dbfs_server
EXPOSE 25432
CMD ["/app/dbfs_server"]