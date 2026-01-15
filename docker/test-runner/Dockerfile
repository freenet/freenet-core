# Docker image for running freenet-core tests
# Provides Linux environment with full loopback range (127.x.x.x)
#
# Usage:
#   docker build -t freenet-test-runner -f docker/test-runner/Dockerfile .
#   docker run --rm -v $(pwd):/workspace -w /workspace freenet-test-runner cargo test

FROM rust:latest

# Install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    cmake \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install WASM target for contract compilation
RUN rustup target add wasm32-unknown-unknown

# Set working directory
WORKDIR /workspace

# Note: Project is mounted as volume at runtime, not copied during build
# This allows fast iteration without rebuilding the image

# Default command: run all tests
CMD ["cargo", "test", "--workspace"]
