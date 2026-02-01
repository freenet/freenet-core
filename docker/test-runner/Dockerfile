# Docker image for running freenet-core tests
# Provides Linux environment with full loopback range (127.x.x.x)
#
# Build (one-time):
#   docker build -t freenet-test-runner -f docker/test-runner/Dockerfile .
#
# Run tests (uses named volumes for fast cached builds):
#   docker/test-runner/run.sh cargo test -p freenet-ping-app --test run_app_blocked_peers -- --nocapture
#
# The run.sh script mounts source as read-only and uses Docker-managed
# volumes for target/ and cargo registry, avoiding slow macOS filesystem I/O.

FROM rust:latest

# Install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    cmake \
    git \
    rsync \
    && rm -rf /var/lib/apt/lists/*

# Install WASM target for contract compilation
RUN rustup target add wasm32-unknown-unknown

# Entrypoint syncs source from read-only mount to a local copy,
# then runs the requested command with native Linux filesystem speed.
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

WORKDIR /build

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["cargo", "test", "--workspace"]
