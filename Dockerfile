# syntax=docker/dockerfile:1
#
# Builds the two long-running Apexes services as separate runtime images:
#   docker build --target engine  -t apexes-engine .
#   docker build --target gateway -t apexes-market-data-gateway .

# Pinned to match CI (.github/workflows/ci.yml); bump both together.
ARG RUST_VERSION=1.97
FROM rust:${RUST_VERSION} AS builder
WORKDIR /src
COPY . .
# Release binaries only; cache mounts keep rebuilds incremental.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p apexes-engine -p apexes-market-data-gateway \
 && mkdir -p /out \
 && cp target/release/apexes-engine target/release/apexes-market-data-gateway /out/

FROM debian:bookworm-slim AS runtime-base
# curl: container healthchecks. ca-certificates: TLS to exchanges.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --create-home --home-dir /home/apexes apexes \
 && mkdir -p /var/lib/apexes && chown apexes /var/lib/apexes
USER apexes
WORKDIR /home/apexes

FROM runtime-base AS engine
COPY --from=builder /out/apexes-engine /usr/local/bin/apexes-engine
# Fixed, reachable control/metrics port (a live session REQUIRES
# APEXES_CONTROL_TOKEN, so exposing it is safe by construction).
ENV APEXES_ENGINE_BIND=0.0.0.0:9100 \
    APEXES_LOG_FORMAT=json
EXPOSE 9100
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:9100/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/apexes-engine"]
CMD ["--launch-config", "/etc/apexes/launch-config.json"]

FROM runtime-base AS gateway
COPY --from=builder /out/apexes-market-data-gateway /usr/local/bin/apexes-market-data-gateway
ENV APEXES_LOG_FORMAT=json
EXPOSE 9001
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:9001/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/apexes-market-data-gateway"]
CMD ["--host", "0.0.0.0", "--port", "9001"]
