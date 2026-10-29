#!/usr/bin/env bash
#
# Run netcheck against the live network and publish the result.
#
#     bash scripts/netcheck-nightly.sh put       # on the gateway host
#     bash scripts/netcheck-nightly.sh get       # on any host OUTSIDE its /24
#     bash scripts/netcheck-nightly.sh publish   # on the gateway host
#
# The nightly workflow is a thin wrapper around this script, one phase per
# job, so each phase can also be run by hand; a manual run moves the handoff
# file from the `put` host to the `get` host and the report back. A change to
# the workflow itself can be run from its branch before merging, because the
# workflow is already on the default branch with `workflow_dispatch`:
#
#     gh workflow run netcheck-nightly.yml --ref <branch>
#
# Why the getter runs on another host: see "Production use" in
# crates/netcheck/README.md.
#
# Everything is overridable by environment so a manual run can point at a
# scratch manifest without disturbing the nightly's own state.
set -euo pipefail

PHASE="${1:-}"

STATE_DIR="${NETCHECK_STATE_DIR:-$HOME/netcheck}"
# Written by `put`, read by `get`.
HANDOFF="${NETCHECK_HANDOFF:-$STATE_DIR/handoff.json}"
# Written by `put` on the gateway host, and published from there by `publish`
# when the `get` phase left no report (the PUT phase failed, or `get` died).
PUT_REPORT="${NETCHECK_PUT_REPORT:-$STATE_DIR/put-report.jsonl}"
# Written by `get`, read by `publish`.
REPORT="${NETCHECK_REPORT:-$STATE_DIR/last-run.jsonl}"
EXIT_CODE_FILE="${NETCHECK_EXIT_CODE_FILE:-$STATE_DIR/exit-code}"

log() { echo "[netcheck-nightly] $*" >&2; }

build_netcheck() {
    cargo build --release --locked -p netcheck >&2
    NETCHECK_BIN="${CARGO_TARGET_DIR:-target}/release/netcheck"
}

# Tell the workflow, when there is one, where the handoff is. Reporting the
# path from here, rather than repeating it in the workflow, means an override
# of NETCHECK_STATE_DIR or NETCHECK_HANDOFF cannot leave the workflow looking
# in the wrong place and quietly skipping the GETs.
report_handoff() {
    if [ -n "${GITHUB_OUTPUT:-}" ] && [ -f "$HANDOFF" ]; then
        {
            echo "handoff=yes"
            echo "handoff_path=$HANDOFF"
        } >> "$GITHUB_OUTPUT"
    fi
}

phase_put() {
    local gateway_ws="${NETCHECK_GATEWAY_WS:-ws://127.0.0.1:7509}"
    local manifest="${NETCHECK_MANIFEST:-$STATE_DIR/manifest.json}"
    local vantage="${NETCHECK_PUT_VANTAGE:-$(hostname -s)}"
    # The check shares a disk with a production node. Skipping a night is
    # cheap; filling that disk is not.
    local min_free_gb="${NETCHECK_MIN_FREE_GB:-20}"

    mkdir -p "$STATE_DIR"
    # Never let a skipped or failed night hand over, or publish, the previous
    # night's files.
    rm -f "$HANDOFF" "$PUT_REPORT"
    local free_gb
    free_gb=$(df -BG --output=avail "$STATE_DIR" 2>/dev/null | tail -1 | tr -dc '0-9')
    if [ -n "$free_gb" ] && [ "$free_gb" -lt "$min_free_gb" ]; then
        log "only ${free_gb}G free, need ${min_free_gb}G. Skipping this run."
        exit 0
    fi

    build_netcheck
    # Exits 0 once the handoff is written, even if a PUT failed or the
    # manifest could not be updated: both are in the handoff, and the `get`
    # phase's verdict covers them.
    set +e
    "$NETCHECK_BIN" put \
        --gateway-ws "$gateway_ws" \
        --manifest "$manifest" \
        --handoff "$HANDOFF" \
        --vantage "$vantage" \
        | tee "$PUT_REPORT"
    local status=${PIPESTATUS[0]}
    set -e
    report_handoff
    return "$status"
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
    # address or key does not silently stale this script. IPv4 only: a hosted
    # runner has no IPv6 route, so an AAAA record would break the join.
    local key_url="${NETCHECK_GETTER_KEY_URL:-https://freenet.org/keys/public.gw2.pem}"
    local getter_host="${NETCHECK_GETTER_HOST:-gw2.freenet.org}"
    local getter_port="${NETCHECK_GETTER_PORT:-31338}"
    local getter_ip getter_key
    # `||`: under pipefail a failed lookup would exit here, before the
    # message below says which of the two failed.
    getter_ip=$(getent ahostsv4 "$getter_host" | awk '{print $1}' | head -1) || true
    # A curl that times out mid-body leaves a partial key: discard it.
    getter_key=$(curl -fsS --max-time 30 "$key_url" | tr -d '[:space:]') || getter_key=""
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
    # On a shared host the node's data must not pile up. A throwaway runner
    # keeps it, so the workflow can upload the node's log.
    if [ -z "${NETCHECK_KEEP_RUN_DIR:-}" ]; then
        rm -rf "$run_dir"
    fi
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
    local report="$REPORT"
    # A report without a recorded status came from a phase that did not
    # finish, so it must not be published as a clean exit.
    local status=1
    if [ -f "$report" ]; then
        if [ -f "$EXIT_CODE_FILE" ]; then
            status=$(tr -dc '0-9' < "$EXIT_CODE_FILE")
            status=${status:-1}
        fi
    elif [ -f "$PUT_REPORT" ]; then
        # The run ended before the GETs: publish what the PUT phase left, as
        # a run that could not complete, rather than nothing.
        log "no GET report; publishing the PUT phase's report from $PUT_REPORT"
        report="$PUT_REPORT"
        vantage="${NETCHECK_PUT_VANTAGE:-$(hostname -s)}"
    else
        log "no report at $REPORT or $PUT_REPORT; nothing to publish"
        return 0
    fi

    build_netcheck
    # Publishing the result must not change the verdict: a collector that is
    # down is not a network regression.
    if ! "$NETCHECK_BIN" emit \
            --endpoint "$otlp_endpoint" \
            --vantage "$vantage" \
            --exit-code "$status" < "$report"; then
        log "WARNING: could not publish results to $otlp_endpoint"
    fi
    # Consumed: a later night that fails before its PUT phase starts must not
    # publish this one again.
    rm -f "$PUT_REPORT"
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
