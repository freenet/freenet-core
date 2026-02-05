#!/bin/bash
set -e

# Run freenet-core tests inside Docker with fast cached builds.
#
# Usage:
#   docker/test-runner/run.sh cargo test -p freenet-ping-app --test run_app_blocked_peers
#   docker/test-runner/run.sh cargo test --workspace
#
# How it works:
#   - Source is mounted read-only at /workspace
#   - The entrypoint rsyncs source to /build (on a Docker volume)
#   - target/ and cargo registry use named Docker volumes (native Linux fs)
#   - First run compiles from scratch (~10-15 min); subsequent runs are incremental
#
# To reset cached builds:
#   docker volume rm freenet-test-target freenet-test-cargo freenet-test-build

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

docker run --rm \
    -v "$REPO_ROOT":/workspace:ro \
    -v freenet-test-build:/build \
    -v freenet-test-target:/build/target \
    -v freenet-test-cargo:/usr/local/cargo/registry \
    freenet-test-runner \
    "$@"
