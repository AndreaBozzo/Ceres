# Stage 1: Builder
FROM rust:1.88-slim-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build --release --locked --bin ceres-server

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system ceres && useradd --system --gid ceres ceres

COPY --from=builder /app/target/release/ceres-server /usr/local/bin/ceres-server

USER ceres

EXPOSE 3000

ENTRYPOINT ["ceres-server"]
