#!/bin/sh
set -eu

config_dir="${FREENET_CONFIG_DIR:-/data/config}"
data_dir="${FREENET_DATA_DIR:-/data/node}"
node_id="${FREENET_NODE_ID:-docker-${HOSTNAME:-node}}"

if [ "$(id -u)" -eq 0 ]; then
    # A freshly-created named volume is root-owned. Initialize it before
    # dropping privileges so the node never runs with write access as root.
    mkdir -p "$config_dir" "$data_dir"
    chown -R --no-dereference freenet:freenet "$config_dir" "$data_dir"
    exec gosu freenet freenet network \
        --id "$node_id" \
        --config-dir "$config_dir" \
        --data-dir "$data_dir" \
        --disable-auto-update \
        "$@"
fi

# A caller using --user owns the mounted paths and is responsible for making
# them writable. Do not attempt privileged ownership changes in that case.
exec freenet network \
    --id "$node_id" \
    --config-dir "$config_dir" \
    --data-dir "$data_dir" \
    --disable-auto-update \
    "$@"
