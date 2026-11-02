FROM rust:1.95-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/feed-order-books /usr/local/bin/feed-order-books
COPY --from=builder /app/target/release/snapshot-watch /usr/local/bin/snapshot-watch
COPY config.yaml ./config.yaml
USER 65532:65532
ENTRYPOINT ["feed-order-books"]
