#!/bin/bash
set -e

# Sync source from the read-only mount to the local build directory.
# Uses rsync to only copy changed files on subsequent runs.
# The /build directory lives on a Docker-managed volume (native Linux fs).
if [ -d /workspace ]; then
    echo "Syncing source to build directory..."
    rsync -a --delete \
        --exclude='target/' \
        --exclude='.git/objects/' \
        /workspace/ /build/
    echo "Sync complete."
else
    echo "ERROR: /workspace not mounted. Use run.sh or mount source with -v." >&2
    exit 1
fi

exec "$@"
