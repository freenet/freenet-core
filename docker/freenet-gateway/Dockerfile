# syntax=docker/dockerfile:1

# Freenet gateway image.
#
# The node binary is NOT rebuilt here. It is the exact statically-linked musl
# artifact published on the GitHub release, verified against that release's
# SHA256SUMS.txt manifest and the manifest's ed25519 signature. This is the
# same binary and the same fetch-and-verify logic as docker/freenet-node's
# image, because a gateway IS a freenet node (started with `--is-gateway`),
# not a different build.
#
# Rebuilding from source would produce a second, independently-built binary
# that can silently drift from the one the release pipeline signed and
# exercised in its auto-update canary (.github/workflows/cross-compile.yml,
# "Gate A"). It would also cost a full uncached release build on every
# publish, and make multi-arch images expensive instead of free -- which is
# what the previous version of this Dockerfile did (`cargo install` against a
# freshly cloned freenet-core, on a now-EOL `bullseye` base image that no
# longer builds at all; see freenet/freenet-core#5634).

ARG FREENET_VERSION=latest

# Runs on the BUILD platform so a cross-architecture build downloads and
# verifies natively instead of under QEMU. TARGETARCH selects which release
# asset to fetch.
FROM --platform=$BUILDPLATFORM debian:bookworm-slim AS fetch

ARG FREENET_VERSION
ARG TARGETARCH
ARG FREENET_RELEASE_BASE_URL=https://github.com/freenet/freenet-core/releases

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        openssl \
    && rm -rf /var/lib/apt/lists/*

# Public half of the release-signing key. The private half exists only as the
# FREENET_RELEASE_SIGNING_KEY CI secret. Pinned against the copy compiled into
# the node by `release_pubkey_matches_docker_image_key` in
# crates/core/src/bin/commands/update.rs, so a key rotation that misses this
# file fails CI rather than breaking image builds with a bad signature.
COPY release-signing-key.der /etc/freenet/release-signing-key.der

RUN set -eu; \
    case "${TARGETARCH:-amd64}" in \
        amd64) rust_target='x86_64-unknown-linux-musl' ;; \
        arm64) rust_target='aarch64-unknown-linux-musl' ;; \
        *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    if [ "${FREENET_VERSION}" = 'latest' ]; then \
        base="${FREENET_RELEASE_BASE_URL}/latest/download"; \
    else \
        base="${FREENET_RELEASE_BASE_URL}/download/${FREENET_VERSION}"; \
    fi; \
    asset="freenet-${rust_target}.tar.gz"; \
    cd /tmp; \
    echo "Fetching ${asset} from ${base}"; \
    curl -fsSL --retry 5 --retry-delay 2 -o "${asset}" "${base}/${asset}"; \
    curl -fsSL --retry 5 --retry-delay 2 -o SHA256SUMS.txt "${base}/SHA256SUMS.txt"; \
    sig_status="$(curl -sSL --retry 3 --retry-delay 2 \
        -o SHA256SUMS.txt.sig -w '%{http_code}' \
        "${base}/SHA256SUMS.txt.sig" || echo 000)"; \
    case "${sig_status}" in \
        200) \
            openssl pkeyutl -verify -pubin -keyform DER \
                -inkey /etc/freenet/release-signing-key.der \
                -rawin -in SHA256SUMS.txt -sigfile SHA256SUMS.txt.sig; \
            echo "Release manifest signature verified"; \
            echo verified > /tmp/signature-status ;; \
        404) \
            echo "WARNING: ${FREENET_VERSION} publishes no SHA256SUMS.txt.sig; manifest is unsigned" >&2; \
            echo absent > /tmp/signature-status ;; \
        *) \
            echo "ERROR: could not fetch SHA256SUMS.txt.sig (HTTP ${sig_status}); refusing to build an unverified image" >&2; \
            exit 1 ;; \
    esac; \
    grep -E "[[:space:]]\\*?${asset}\$" SHA256SUMS.txt > asset.sha256; \
    test -s asset.sha256; \
    sha256sum -c asset.sha256; \
    tar xzf "${asset}"; \
    test -f freenet; \
    chmod 0755 freenet

FROM debian:bookworm-slim

ARG FREENET_VERSION

LABEL org.opencontainers.image.title="Freenet gateway" \
      org.opencontainers.image.description="Freenet network gateway, self-updating" \
      org.opencontainers.image.source="https://github.com/freenet/freenet-core" \
      org.opencontainers.image.url="https://freenet.org" \
      org.opencontainers.image.documentation="https://github.com/freenet/freenet-core/blob/main/docker/freenet-gateway/README.md" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0" \
      org.opencontainers.image.version="${FREENET_VERSION}"

# ca-certificates: TLS roots for the node's own update downloads.
# curl:            HEALTHCHECK below, and the usual in-container debugging.
# gosu:            drop from root to `freenet` after initializing the volume.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gosu \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 freenet \
    && useradd --uid 1000 --gid freenet --create-home --shell /usr/sbin/nologin freenet \
    && mkdir -p /data /usr/local/lib/freenet \
    && chown freenet:freenet /data

# The pristine, image-provided binary. The node runs from a COPY of this on the
# data volume (see the entrypoint) so that `freenet update` has somewhere
# writable to install to and so an applied update survives container recreation.
# Nothing ever writes to this path, which keeps the image layer immutable.
COPY --from=fetch /tmp/freenet /usr/local/lib/freenet/freenet
# Records whether the release manifest signature was verified or genuinely
# absent. Asserted by the publish workflow so a release image can never ship
# unverified.
COPY --from=fetch /tmp/signature-status /etc/freenet/signature-status
COPY freenet-gateway-startup.sh /usr/local/bin/freenet-gateway-startup
COPY healthcheck.sh /usr/local/bin/freenet-healthcheck

# `freenet` on PATH resolves to the RUNNING binary on the volume, so
# `docker exec <container> freenet --version` reports the version actually
# serving traffic rather than the one the image shipped with. Dangling until
# the entrypoint seeds it on first start.
RUN chmod 0755 /usr/local/lib/freenet/freenet /usr/local/bin/freenet-gateway-startup \
        /usr/local/bin/freenet-healthcheck \
    && ln -s /data/bin/freenet /usr/local/bin/freenet

ENV FREENET_CONFIG_DIR=/data/config \
    FREENET_DATA_DIR=/data/node \
    FREENET_BIN_DIR=/data/bin \
    NETWORK_PORT=31337 \
    WS_API_PORT=7509 \
    # Where the gateway's own identity lives. Auto-generated on first start if
    # missing (see the entrypoint) -- an operator does not have to pre-supply
    # anything, but the path is fixed and documented so an existing identity
    # can be backed up, restored, or bind-mounted from elsewhere.
    TRANSPORT_KEYPAIR=/data/keys/transport-keypair.pem \
    # Without this the node writes NOTHING to `docker logs`. Its console layer
    # is gated on stdout being a terminal, and a container's stdout is a pipe,
    # so the one deployment where stdout is the only log interface is exactly
    # the one the probe turns logging off for. This is additive: file logging
    # stays on, so `freenet service report` still has logs to collect.
    FREENET_LOG_TO_CONSOLE=1 \
    # Logs default to $HOME, which is inside the image layer, so they would be
    # discarded on every container recreate and be missing from diagnostic
    # reports. On the volume they persist, bounded by the node's own 512 MiB
    # log-directory budget (FREENET_LOG_DIR_MAX_BYTES).
    LOG_DIR=/data/logs

# UDP transport port. NETWORK_PORT above pins it, because with no explicit port
# the node binds a RANDOM free one (config.rs `default_network_api_port`) and no
# published port could reliably match it. A gateway additionally needs this
# port reachable from the public internet -- see README.md.
EXPOSE 31337/udp

# The client API is deliberately NOT exposed. It is fully privileged: it can
# read and modify contract state, identities and key material. It binds loopback
# by default, which under bridge networking is the CONTAINER's loopback and so
# is unreachable from the host at all. See README.md for how to reach it safely.

VOLUME ["/data"]

# The node drains in-flight operations for up to `shutdown_drain_secs`
# (30s default) on SIGTERM. Docker's default 10s stop timeout would kill it
# mid-drain, so compose sets a longer stop_grace_period.
STOPSIGNAL SIGTERM

# Liveness. The node exposes no dedicated health route, so this pairs the
# cheapest side-effect-free endpoint (/v1/version) with a check that OUR node
# process is the one running. See healthcheck.sh for why the API probe alone is
# not enough under host networking. It reports whether the gateway is up and
# serving, not whether it has peer connections.
HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD /usr/local/bin/freenet-healthcheck

ENTRYPOINT ["/usr/local/bin/freenet-gateway-startup"]
