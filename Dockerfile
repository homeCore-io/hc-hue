# =============================================================================
# hc-hue — HomeCore Philips Hue Plugin
# Alpine Linux — minimal, static-friendly runtime
# =============================================================================
#
# Build:
#   docker build -t hc-hue:latest .
#
# Run:
#   docker run -d \
#     -v ./config/config.toml:/opt/hc-hue/config/config.toml:ro \
#     -v hc-hue-logs:/opt/hc-hue/logs \
#     hc-hue:latest
#
# Volumes:
#   /opt/hc-hue/config   config.toml (credentials, app_key)
#   /opt/hc-hue/logs     rolling log files
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1 — Build
# -----------------------------------------------------------------------------
FROM rust:1.95-alpine3.23@sha256:606fd313a0f49743ee2a7bd49a0914bab7deedb12791f3a846a34a4711db7ed2 AS builder

RUN apk upgrade --no-cache && apk add --no-cache musl-dev openssl-dev pkgconfig

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/

RUN cargo build --release --bin hc-hue

# -----------------------------------------------------------------------------
# Stage 2 — Runtime
# -----------------------------------------------------------------------------
FROM alpine:3.23@sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11

# `apk upgrade` first pulls CVE patches for packages baked into the
# alpine:3 base since the upstream image was last rebuilt. Defense
# in depth — without this, `apk add --no-cache` only refreshes the
# named packages, leaving busybox/musl/etc. on the base's frozen
# versions.
RUN apk upgrade --no-cache && \
    apk add --no-cache \
        ca-certificates \
        libssl3 \
        tzdata

RUN adduser -D -h /opt/hc-hue hchue

COPY --from=builder /build/target/release/hc-hue /usr/local/bin/hc-hue
RUN chmod 755 /usr/local/bin/hc-hue

RUN mkdir -p /opt/hc-hue/config /opt/hc-hue/logs

COPY config/config.toml.example /opt/hc-hue/config/config.toml.example

RUN chown -R hchue:hchue /opt/hc-hue

USER hchue
WORKDIR /opt/hc-hue

VOLUME ["/opt/hc-hue/config", "/opt/hc-hue/logs"]

ENV RUST_LOG=info

ENTRYPOINT ["hc-hue"]
