FROM rust:1.82 as builder
WORKDIR /app

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY static ./static
COPY providers.json ./providers.json

RUN cargo build --release

FROM debian:bookworm-slim
LABEL org.opencontainers.image.source="https://github.com/open-rpc-lb/orlb"
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/orlb /usr/local/bin/orlb
COPY --from=builder /app/providers.json /app/providers.json
COPY --from=builder /app/static /app/static

ENV ORLB_PROVIDERS_PATH=/app/providers.json
ENV RUST_LOG=orlb=info

EXPOSE 8080
CMD ["orlb"]
