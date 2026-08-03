FROM rust:1.95-slim-bookworm AS builder

ARG CERES_GIT_SHA=unknown

WORKDIR /app

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN CERES_GIT_SHA="${CERES_GIT_SHA}" \
    cargo build --release --locked --bin ceres --bin ceres-server

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system ceres && useradd --system --gid ceres ceres

COPY --from=builder /app/target/release/ceres /usr/local/bin/ceres
COPY --from=builder /app/target/release/ceres-server /usr/local/bin/ceres-server
COPY migrations /usr/local/share/ceres/migrations
COPY scripts/container-migrate.sh /usr/local/bin/ceres-migrate
COPY scripts/container-job.sh /usr/local/bin/ceres-job

RUN chmod 0755 /usr/local/bin/ceres-migrate /usr/local/bin/ceres-job

ENV HOST=0.0.0.0 \
    PORT=3000 \
    PORTALS_CONFIG=/etc/ceres/portals.toml

WORKDIR /var/lib/ceres

USER ceres

EXPOSE 3000

# The server is the backward-compatible default. Override the command with
# `ceres-job` for finite scheduled harvests, `ceres ...` for ad-hoc CLI jobs,
# or `ceres-migrate` for schema migrations.
CMD ["ceres-server"]
