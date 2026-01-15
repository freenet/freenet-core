#!/bin/bash
# Run tests in Docker (Linux environment with full loopback range)
#
# Usage:
#   ./scripts/docker-test.sh                              # Run all tests
#   ./scripts/docker-test.sh operations                   # Run operations tests
#   ./scripts/docker-test.sh operations test_update_contract  # Run specific test
#   ./scripts/docker-test.sh --build                      # Force rebuild image
#
# Examples:
#   ./scripts/docker-test.sh operations test_update_contract
#   ./scripts/docker-test.sh operations test_put_with_subscribe_flag
#   ./scripts/docker-test.sh operations test_multiple_clients_subscription

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
IMAGE_NAME="freenet-test-runner"

# Parse arguments
BUILD_FLAG=""
TEST_SUITE=""
TEST_NAME=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --build)
            BUILD_FLAG="--no-cache"
            shift
            ;;
        *)
            if [ -z "$TEST_SUITE" ]; then
                TEST_SUITE="$1"
            else
                TEST_NAME="$1"
            fi
            shift
            ;;
    esac
done

cd "$PROJECT_ROOT"

# Build the Docker image if it doesn't exist or --build flag is set
if [ -n "$BUILD_FLAG" ] || ! docker image inspect "$IMAGE_NAME" &>/dev/null; then
    echo "Building Docker image..."
    # Use only the Dockerfile directory as context (fast, no project copy)
    docker build $BUILD_FLAG -t "$IMAGE_NAME" docker/test-runner/
fi

# Construct test command
if [ -z "$TEST_SUITE" ]; then
    # Run all tests
    TEST_CMD="cargo test --workspace"
elif [ -z "$TEST_NAME" ]; then
    # Run specific test suite
    TEST_CMD="cargo test -p freenet --test $TEST_SUITE -- --nocapture"
else
    # Run specific test within suite
    TEST_CMD="cargo test -p freenet --test $TEST_SUITE $TEST_NAME -- --nocapture"
fi

echo "Running: $TEST_CMD"
echo "---"

# Run the tests in Docker
# - Mount project as volume for faster iteration (no rebuild needed)
# - Use separate target dir to avoid conflicts with host builds
# - Use host network for simplicity (loopback works in Linux)
docker run --rm \
    -v "$PROJECT_ROOT:/workspace" \
    -w /workspace \
    -e CARGO_TARGET_DIR=/workspace/target-docker \
    --network host \
    "$IMAGE_NAME" \
    bash -c "$TEST_CMD"
