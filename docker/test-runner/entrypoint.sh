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

# If freenet-stdlib is mounted, sync it and rewrite path dependencies
# from the host absolute path to the container-local copy.
if [ -d /workspace-stdlib ]; then
    echo "Syncing freenet-stdlib to build directory..."
    rsync -a --delete \
        --exclude='target/' \
        --exclude='.git/objects/' \
        /workspace-stdlib/ /build-stdlib/
    # Rewrite absolute host paths to container-local paths in all Cargo.toml files
    find /build -name 'Cargo.toml' -exec \
        sed -i 's|/Volumes/PRO-G40/projects/freenet-stdlib|/build-stdlib|g' {} +
    echo "stdlib sync complete."
fi

exec "$@"
