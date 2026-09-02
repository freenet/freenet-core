# Build the release binary from the checked-out source. The release workflow
# checks out the matching tag, so the image and GitHub release stay aligned.
FROM rust:1.94-slim-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        libclang-dev \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cargo build --locked --release --package freenet

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        gosu \
        libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 freenet \
    && useradd --uid 1000 --gid freenet --create-home --shell /usr/sbin/nologin freenet \
    && mkdir /data \
    && chown freenet:freenet /data

COPY --from=builder /src/target/release/freenet /usr/local/bin/freenet
COPY docker/freenet-node/freenet-node-startup.sh /usr/local/bin/freenet-node-startup
RUN chmod 755 /usr/local/bin/freenet-node-startup

ENV FREENET_CONFIG_DIR=/data/config \
    FREENET_DATA_DIR=/data/node

# The node transport listens on UDP 31338 by default. The control API is
# intentionally not published: it binds to loopback and is fully privileged.
EXPOSE 31338/udp
VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/freenet-node-startup"]
