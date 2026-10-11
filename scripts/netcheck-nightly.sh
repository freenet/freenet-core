#!/usr/bin/env bash
#
# Run netcheck against the live network and publish the result.
#
# The nightly workflow is a thin wrapper around this script. The logic lives
# here because a workflow file cannot be exercised before it is merged
# (GitHub only dispatches workflows present on the default branch), while this
# can be run by hand over ssh on the host that will run it:
#
#     bash scripts/netcheck-nightly.sh
#
# Everything is overridable by environment so a manual run can point at a
# scratch manifest without disturbing the nightly's own state.
set -euo pipefail

GATEWAY_WS="${NETCHECK_GATEWAY_WS:-ws://127.0.0.1:7509}"
STATE_DIR="${NETCHECK_STATE_DIR:-$HOME/netcheck}"
MANIFEST="${NETCHECK_MANIFEST:-$STATE_DIR/manifest.json}"
RUN_DIR="${NETCHECK_RUN_DIR:-$STATE_DIR/run}"
REPORT="${NETCHECK_REPORT:-$STATE_DIR/last-run.jsonl}"
# UDP port opened on the gateway host for the ephemeral peer.
EPHEMERAL_PORT="${NETCHECK_EPHEMERAL_PORT:-32177}"
EPHEMERAL_WS_PORT="${NETCHECK_EPHEMERAL_WS_PORT:-7519}"
FREENET_BIN="${NETCHECK_FREENET_BIN:-/usr/local/bin/freenet}"
VANTAGE="${NETCHECK_VANTAGE:-$(hostname -s)}"
OTLP_ENDPOINT="${NETCHECK_OTLP_ENDPOINT:-}"
# The check shares a disk with a production node. Skipping a night is cheap;
# filling that disk is not.
MIN_FREE_GB="${NETCHECK_MIN_FREE_GB:-20}"

log() { echo "[netcheck-nightly] $*" >&2; }

mkdir -p "$STATE_DIR"
free_gb=$(df -BG --output=avail "$STATE_DIR" 2>/dev/null | tail -1 | tr -dc '0-9')
if [ -n "$free_gb" ] && [ "$free_gb" -lt "$MIN_FREE_GB" ]; then
    log "only ${free_gb}G free, need ${MIN_FREE_GB}G. Skipping this run."
    exit 0
fi

# A killed run leaves the ephemeral node's data behind, and its hosting cache
# is budgeted at RAM/8 up to 1 GiB.
rm -rf "$RUN_DIR"

# The getter must join through a gateway that did not receive the PUTs, or the
# GET can be answered by the node that just stored them. Resolved from the
# public index so a rotated address or key does not silently stale this script.
# The getter MUST be a different gateway from the one the PUTs go through
# (GATEWAY_WS above, ws://127.0.0.1:7509 = gw1 on network port 31337). A GET
# answered by the node that just stored the PUT proves transfer, not
# findability — see the --gateway-spec doc in crates/netcheck/src/main.rs.
#
# So this is gw2 on 31338, not gw1. An earlier revision of this change pointed
# it at gw1 and would have made the nightly check VACUOUS: same gateway for PUT
# and GET, always passing, testing nothing.
#
# Honest limitation: gw2 is a separate PROCESS but the same HOST as gw1, where
# the retired AWS gateway was a separate host entirely. The documented invariant
# ("a gateway OTHER than the one the PUTs go through") still holds, but this is
# a weaker check than before — it no longer crosses a machine or a network. That
# is a consequence of consolidating both gateways onto one box; if an off-host
# gateway ever exists again, point this at it.
INDEX_KEY_URL="${NETCHECK_GETTER_KEY_URL:-https://freenet.org/keys/public.gw2.pem}"
GETTER_HOST="${NETCHECK_GETTER_HOST:-gw2.freenet.org}"
GETTER_PORT="${NETCHECK_GETTER_PORT:-31338}"
getter_ip=$(getent hosts "$GETTER_HOST" | awk '{print $1}' | head -1)
getter_key=$(curl -fsS --max-time 30 "$INDEX_KEY_URL" | tr -d '[:space:]')
if [ -z "$getter_ip" ] || [ -z "$getter_key" ]; then
    log "could not resolve the getter gateway ($GETTER_HOST / $INDEX_KEY_URL)"
    exit 1
fi
log "getter joins through $GETTER_HOST ($getter_ip:$GETTER_PORT)"

cargo build --release --locked -p netcheck
NETCHECK_BIN="${CARGO_TARGET_DIR:-target}/release/netcheck"

set +e
"$NETCHECK_BIN" put-get \
    --gateway-ws "$GATEWAY_WS" \
    --gateway-spec "${getter_ip}:${GETTER_PORT},${getter_key}" \
    --freenet-bin "$FREENET_BIN" \
    --manifest "$MANIFEST" \
    --ephemeral-dir "$RUN_DIR" \
    --ephemeral-network-port "$EPHEMERAL_PORT" \
    --ephemeral-ws-port "$EPHEMERAL_WS_PORT" \
    | tee "$REPORT"
status=${PIPESTATUS[0]}
set -e

# Publishing the result must not change the verdict: a collector that is down
# is not a network regression.
if [ -n "$OTLP_ENDPOINT" ]; then
    if ! "$NETCHECK_BIN" emit \
            --endpoint "$OTLP_ENDPOINT" \
            --vantage "$VANTAGE" \
            --exit-code "$status" < "$REPORT"; then
        log "WARNING: could not publish results to $OTLP_ENDPOINT"
    fi
fi

rm -rf "$RUN_DIR"
log "netcheck exited with $status"
exit "$status"
