#!/usr/bin/env bash
#
# Run netcheck against the live network and publish the result.
#
#     bash scripts/netcheck-nightly.sh put       # on the gateway host
#     bash scripts/netcheck-nightly.sh get       # on any host OUTSIDE its /24
#     bash scripts/netcheck-nightly.sh publish   # on the gateway host
#
# The nightly workflow is a thin wrapper around this script, one phase per
# job. The logic lives here because a workflow file cannot be exercised before
# it is merged (GitHub only dispatches workflows present on the default
# branch), while each phase can be run by hand. A manual run moves the handoff
# file from the `put` host to the `get` host and the report back.
#
# Why two hosts: the PUTs need the gateway's loopback WebSocket API, so they
# run on the gateway host. The getter must not. A node's ring location is
# derived from its address with the host bits masked, so a getter on the
# gateway host shares the gateways' location, and the peers there will not
# accept a joiner at their own location. From 2026-10-16 that made the getter
# fail to join on most nights, which measured the gateway host's /24, not the
# network.
#
# Everything is overridable by environment so a manual run can point at a
# scratch manifest without disturbing the nightly's own state.
set -euo pipefail

PHASE="${1:-}"

STATE_DIR="${NETCHECK_STATE_DIR:-$HOME/netcheck}"
# Written by `put`, read by `get`.
HANDOFF="${NETCHECK_HANDOFF:-$STATE_DIR/handoff.json}"
# Written by `get`, read by `publish`.
REPORT="${NETCHECK_REPORT:-$STATE_DIR/last-run.jsonl}"
EXIT_CODE_FILE="${NETCHECK_EXIT_CODE_FILE:-$STATE_DIR/exit-code}"

log() { echo "[netcheck-nightly] $*" >&2; }

build_netcheck() {
    cargo build --release --locked -p netcheck >&2
    NETCHECK_BIN="${CARGO_TARGET_DIR:-target}/release/netcheck"
}

phase_put() {
    local gateway_ws="${NETCHECK_GATEWAY_WS:-ws://127.0.0.1:7509}"
    local manifest="${NETCHECK_MANIFEST:-$STATE_DIR/manifest.json}"
    # The check shares a disk with a production node. Skipping a night is
    # cheap; filling that disk is not.
    local min_free_gb="${NETCHECK_MIN_FREE_GB:-20}"

    mkdir -p "$STATE_DIR"
    # Never let a skipped or failed night hand over the previous night's
    # handoff: the workflow GETs whatever handoff it finds.
    rm -f "$HANDOFF"
    local free_gb
    free_gb=$(df -BG --output=avail "$STATE_DIR" 2>/dev/null | tail -1 | tr -dc '0-9')
    if [ -n "$free_gb" ] && [ "$free_gb" -lt "$min_free_gb" ]; then
        log "only ${free_gb}G free, need ${min_free_gb}G. Skipping this run."
        exit 0
    fi

    build_netcheck
    # Exits 0 once the handoff is written, even if a PUT failed: the failure
    # is in the handoff and the `get` phase's verdict covers it.
    "$NETCHECK_BIN" put \
        --gateway-ws "$gateway_ws" \
        --manifest "$manifest" \
        --handoff "$HANDOFF"
}

phase_get() {
    local run_dir="${NETCHECK_RUN_DIR:-$STATE_DIR/run}"
    local ephemeral_port="${NETCHECK_EPHEMERAL_PORT:-32177}"
    local ephemeral_ws_port="${NETCHECK_EPHEMERAL_WS_PORT:-7519}"
    local freenet_bin="${NETCHECK_FREENET_BIN:-freenet}"

    if [ ! -f "$HANDOFF" ]; then
        log "no handoff at $HANDOFF; run the put phase first"
        exit 1
    fi
    mkdir -p "$STATE_DIR" "$(dirname "$REPORT")" "$(dirname "$EXIT_CODE_FILE")"
    rm -rf "$run_dir"
    rm -f "$REPORT" "$EXIT_CODE_FILE"

    # The getter must join through a gateway that did not receive the PUTs,
    # or the GET can be answered by the node that just stored them. The PUTs
    # go through gw1 (ws://127.0.0.1:7509 on the gateway host, network port
    # 31337), so this is gw2 on 31338. A GET answered by the node that stored
    # the PUT proves transfer, not findability: see the --gateway-spec doc in
    # crates/netcheck/src/main.rs. Resolved from the public index so a rotated
    # address or key does not silently stale this script.
    local key_url="${NETCHECK_GETTER_KEY_URL:-https://freenet.org/keys/public.gw2.pem}"
    local getter_host="${NETCHECK_GETTER_HOST:-gw2.freenet.org}"
    local getter_port="${NETCHECK_GETTER_PORT:-31338}"
    local getter_ip getter_key
    getter_ip=$(getent hosts "$getter_host" | awk '{print $1}' | head -1)
    getter_key=$(curl -fsS --max-time 30 "$key_url" | tr -d '[:space:]')
    if [ -z "$getter_ip" ] || [ -z "$getter_key" ]; then
        log "could not resolve the getter gateway ($getter_host / $key_url)"
        exit 1
    fi
    log "getter joins through $getter_host ($getter_ip:$getter_port)"

    build_netcheck
    set +e
    "$NETCHECK_BIN" get \
        --handoff "$HANDOFF" \
        --gateway-spec "${getter_ip}:${getter_port},${getter_key}" \
        --freenet-bin "$freenet_bin" \
        --ephemeral-dir "$run_dir" \
        --ephemeral-network-port "$ephemeral_port" \
        --ephemeral-ws-port "$ephemeral_ws_port" \
        | tee "$REPORT"
    local status=${PIPESTATUS[0]}
    set -e

    # `publish` needs it to tell "the check could not run" from a pass.
    echo "$status" > "$EXIT_CODE_FILE"
    rm -rf "$run_dir"
    log "netcheck get exited with $status"
    return "$status"
}

phase_publish() {
    local otlp_endpoint="${NETCHECK_OTLP_ENDPOINT:-}"
    local vantage="${NETCHECK_VANTAGE:-$(hostname -s)}"

    if [ -z "$otlp_endpoint" ]; then
        log "NETCHECK_OTLP_ENDPOINT is not set; nothing to publish to"
        return 0
    fi
    if [ ! -f "$REPORT" ]; then
        log "no report at $REPORT; the get phase produced nothing to publish"
        return 0
    fi
    # A report without a recorded status came from a get phase that died
    # before finishing, so it must not be published as a clean exit.
    local status=1
    if [ -f "$EXIT_CODE_FILE" ]; then
        status=$(tr -dc '0-9' < "$EXIT_CODE_FILE")
        status=${status:-1}
    fi

    build_netcheck
    # Publishing the result must not change the verdict: a collector that is
    # down is not a network regression.
    if ! "$NETCHECK_BIN" emit \
            --endpoint "$otlp_endpoint" \
            --vantage "$vantage" \
            --exit-code "$status" < "$REPORT"; then
        log "WARNING: could not publish results to $otlp_endpoint"
    fi
}

case "$PHASE" in
    put) phase_put ;;
    get) phase_get ;;
    publish) phase_publish ;;
    *)
        log "usage: $0 put|get|publish"
        exit 2
        ;;
esac
