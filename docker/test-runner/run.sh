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

# Detect local path dependencies and mount them into the container.
# This allows `freenet-stdlib = { path = "..." }` to resolve inside Docker.
EXTRA_MOUNTS=()
if grep -q 'path.*=.*"/Volumes/PRO-G40/projects/freenet-stdlib' "$REPO_ROOT/Cargo.toml" 2>/dev/null; then
    STDLIB_PATH="/Volumes/PRO-G40/projects/freenet-stdlib"
    if [ -d "$STDLIB_PATH" ]; then
        EXTRA_MOUNTS+=(-v "$STDLIB_PATH":/workspace-stdlib:ro)
    fi
fi

docker run --rm \
    -v "$REPO_ROOT":/workspace:ro \
    "${EXTRA_MOUNTS[@]}" \
    -v freenet-test-build:/build \
    -v freenet-test-target:/build/target \
    -v freenet-test-cargo:/usr/local/cargo/registry \
    freenet-test-runner \
    "$@"
