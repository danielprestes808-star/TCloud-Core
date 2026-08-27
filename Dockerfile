# syntax=docker/dockerfile:1
FROM rust:1.97-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update     && apt-get install -y --no-install-recommends ca-certificates libsqlite3-0     && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/tcloud-core /usr/local/bin/tcloud-core
COPY migrations ./migrations

ENV TCLOUD_TELEGRAM_SESSION=/tmp/tcloud/data/tcloud-telegram.session
EXPOSE 10000

CMD ["tcloud-core"]
