#!/bin/sh
#
# Container healthcheck.
#
# Checking only "does something answer on 127.0.0.1:7509" is wrong under the
# documented default of `network_mode: host`, where that address is the HOST's
# loopback. A native Freenet install on the same machine, which is a normal
# thing for an operator to have, would then make this container report healthy
# while its own node is dead or crash-looping.
#
# So the API probe is paired with a check that OUR node process is alive. The
# entrypoint records its child's pid, and only that process answering counts.
#
# The update window is treated as healthy on purpose. A self-update takes the
# node down for as long as the download and install need, and Docker's
# --start-period covers container start only. Without this, every routine update
# would flip the container to unhealthy, and anything acting on health (Swarm, an
# autoheal sidecar, compose `depends_on: service_healthy`) could kill the
# container in the middle of the update it is supposed to be applying.

set -eu

pid_file="${FREENET_PID_FILE:-/data/node.pid}"
updating_file="${FREENET_UPDATING_FILE:-/data/updating}"

if [ -f "$updating_file" ]; then
    echo "update in progress"
    exit 0
fi

if [ ! -f "$pid_file" ]; then
    echo "no node pid recorded" >&2
    exit 1
fi

pid="$(cat "$pid_file")"
if ! kill -0 "$pid" 2>/dev/null; then
    echo "recorded node process ${pid} is not running" >&2
    exit 1
fi

if ! curl -fsS "http://127.0.0.1:${WS_API_PORT:-7509}/v1/version" >/dev/null; then
    echo "node process ${pid} is running but its client API is not answering" >&2
    exit 1
fi

exit 0
