#!/bin/sh
#
# Freenet gateway container entrypoint and update supervisor.
#
# WHY THIS EXISTS
#
# This is the same supervisor as docker/freenet-node/freenet-node-startup.sh,
# because a gateway IS a freenet node (started with `--is-gateway
# --skip-load-from-network`), not a different binary or a different update
# story. Freenet ships releases frequently, sometimes several times a day, and
# peers are expected to converge on a new release within hours;
# `min-compatible-version` is enforced as a hard gate at the transport
# handshake (crates/core/src/transport/connection_handler/version_cmp.rs), so
# a gateway that falls behind has its connections refused and stops being a
# usable bootstrap point for anyone. See freenet-node's own startup script for
# the full rationale (systemd-unit contract, restart backoff, exit-code
# handling); it is not repeated here. This file duplicates that scaffolding
# rather than sourcing it, matching this repository's existing convention of
# each docker/ subdirectory being a self-contained build context (see
# freenet-node/README.md, "How the image is built").
#
# WHAT IS DIFFERENT FROM THE NODE
#
#   - `--is-gateway --skip-load-from-network` on the network invocation.
#   - `--transport-keypair`, `--public-network-address`, `--public-network-port`
#     are gateway-specific and required (PUBLIC_NETWORK_ADDRESS has no default;
#     see the check below).
#   - The transport keypair is generated on first start if it does not exist
#     yet (see ensure_transport_keypair). A regular node does this too, but
#     only when NO --transport-keypair is given at all (config/secret.rs,
#     `SecretArgs::build`'s no-explicit-path branch) -- which is exactly the
#     branch a gateway does not take, since its identity has to live at a
#     fixed, documented path an operator can back up or restore. Passing an
#     explicit path to a file that does not exist yet is fatal instead
#     (`read_transport_keypair` in the same file), so the generation has to
#     happen here.

set -eu

CONFIG_DIR="${FREENET_CONFIG_DIR:-/data/config}"
DATA_DIR="${FREENET_DATA_DIR:-/data/node}"
BIN_DIR="${FREENET_BIN_DIR:-/data/bin}"
LOG_DIR="${LOG_DIR:-/data/logs}"
PID_FILE="${FREENET_PID_FILE:-/data/node.pid}"
UPDATING_FILE="${FREENET_UPDATING_FILE:-/data/updating}"
IMAGE_BIN="${FREENET_IMAGE_BIN:-/usr/local/lib/freenet/freenet}"
RUN_BIN="${BIN_DIR}/freenet"

TRANSPORT_KEYPAIR="${TRANSPORT_KEYPAIR:-/data/keys/transport-keypair.pem}"
NETWORK_PORT="${NETWORK_PORT:-31337}"
# A gateway's public port is USUALLY the same port it listens on locally --
# distinct values only matter behind a NAT/port-forward that remaps it, so
# this defaults to NETWORK_PORT rather than making every operator repeat the
# same number twice.
PUBLIC_NETWORK_PORT="${PUBLIC_NETWORK_PORT:-$NETWORK_PORT}"

HOME="${FREENET_HOME_DIR:-/data/home}"
export HOME

RESTART_DELAY_MIN="${FREENET_RESTART_DELAY_MIN:-10}"
RESTART_DELAY_MAX="${FREENET_RESTART_DELAY_MAX:-300}"
HEALTHY_RUN_SECS="${FREENET_HEALTHY_RUN_SECS:-600}"
UPDATE_MIN_UPTIME_SECS="${FREENET_UPDATE_MIN_UPTIME_SECS:-30}"
RESTART_JITTER_PCT="${FREENET_RESTART_JITTER_PCT:-20}"

last_update_failed=0

log() {
    echo "[freenet-entrypoint] $*"
}

binary_version() {
    [ -x "$1" ] || return 0
    "$1" --version 2>/dev/null \
        | sed -n 's/^Freenet version: \([^ ][^ ]*\).*/\1/p' \
        | head -n 1
}

install_image_binary() {
    rm -f "${RUN_BIN}".new.* 2>/dev/null || true
    tmp="${RUN_BIN}.new.$$"
    cp "$IMAGE_BIN" "$tmp"
    chmod 0755 "$tmp"
    mv -f "$tmp" "$RUN_BIN"
}

seed_binary() {
    image_version="$(binary_version "$IMAGE_BIN" || true)"
    run_version="$(binary_version "$RUN_BIN" || true)"

    if [ -z "$image_version" ]; then
        log "FATAL: image binary ${IMAGE_BIN} is missing or unrunnable"
        exit 1
    fi

    if [ -z "$run_version" ]; then
        log "seeding ${RUN_BIN} from image binary ${image_version}"
        install_image_binary
        return
    fi

    if [ "$image_version" = "$run_version" ]; then
        return
    fi

    newest="$(printf '%s\n%s\n' "$image_version" "$run_version" | sort -V | tail -n 1)"
    if [ "$newest" = "$image_version" ]; then
        log "image binary ${image_version} is newer than ${run_version} on the volume; replacing"
        install_image_binary
    else
        log "keeping self-updated binary ${run_version} (image ships ${image_version})"
    fi
}

# Generates a fresh X25519 transport keypair matching TransportKeypair::save's
# on-disk format exactly: a 32-byte secret key, hex-encoded, no trailing
# newline (crates/core/src/transport/crypto.rs). The public key is not stored
# separately -- it is re-derived from the secret on every load, same as the
# node does.
#
# Deliberately NOT `freenet ... --transport-keypair "$path"` against a missing
# path: read_transport_keypair (config/secret.rs) treats a missing file as
# fatal when an explicit path is given. Only the no-explicit-path branch
# auto-generates, which a gateway cannot use (its identity must live at a
# fixed, documented path). Deliberately also not the old
# `openssl genpkey -algorithm RSA ...` this replaces: that produced a PEM the
# node has never accepted as a real key, only detected as a legacy format and
# silently replaced with a proper X25519 keypair on first use (same file,
# same code path referenced above) -- so the old script was not actually
# securing anything; it just deferred key generation to the first real start.
ensure_transport_keypair() {
    key="$1"
    [ -f "$key" ] && return 0

    key_dir="$(dirname "$key")"
    mkdir -p "$key_dir"
    tmp="${key}.new.$$"
    umask 077
    head -c 32 /dev/urandom | od -An -v -tx1 | tr -d ' \n' > "$tmp"
    chmod 0600 "$tmp"
    mv -f "$tmp" "$key"
    log "generated new transport keypair at ${key}"
}

update_after_exit() {
    stop_status="$1"
    case "$stop_status" in
        0 | 43)
            return 0
            ;;
    esac
    if [ -n "${FREENET_DISABLE_AUTO_UPDATE:-}" ]; then
        log "not updating after exit ${stop_status}: auto-update is disabled by configuration"
        return 0
    fi
    log "applying update after exit ${stop_status}"
    touch "$UPDATING_FILE" 2>/dev/null || true
    if FREENET_POST_STOP_EXIT_CODE="$stop_status" "$RUN_BIN" update --quiet; then
        log "update step completed"
        last_update_failed=0
    else
        last_update_failed=1
        log "update step did not complete; continuing with the current binary"
    fi
    rm -f "$UPDATING_FILE" 2>/dev/null || true
}

jitter() {
    base="$1"
    if [ "$base" -le 0 ] || [ "$RESTART_JITTER_PCT" -le 0 ]; then
        echo "$base"
        return 0
    fi
    spread=$(( base * RESTART_JITTER_PCT / 100 ))
    if [ "$spread" -le 0 ]; then
        spread=1
    fi
    rand=$(od -An -N2 -tu2 < /dev/urandom | tr -d ' ')
    offset=$(( (rand % (2 * spread + 1)) - spread ))
    result=$(( base + offset ))
    if [ "$result" -le 0 ]; then
        result=1
    fi
    echo "$result"
}

restart_delay() {
    consecutive="$1"
    delay="$RESTART_DELAY_MIN"
    i=1
    while [ "$i" -lt "$consecutive" ]; do
        delay=$((delay * 2))
        if [ "$delay" -ge "$RESTART_DELAY_MAX" ]; then
            jitter "$RESTART_DELAY_MAX"
            return 0
        fi
        i=$((i + 1))
    done
    jitter "$delay"
}

if [ -n "${FREENET_ENTRYPOINT_SOURCE_ONLY:-}" ]; then
    return 0
fi

# PUBLIC_NETWORK_ADDRESS has no sane default (there is no portable way to
# autodetect an operator's own public IP/hostname from inside a container),
# so a gateway started without it fails fast with a clear message rather than
# starting a gateway nobody can reach.
if [ -z "${PUBLIC_NETWORK_ADDRESS:-}" ]; then
    echo "[freenet-entrypoint] FATAL: PUBLIC_NETWORK_ADDRESS is not set." >&2
    echo "[freenet-entrypoint] FATAL: a gateway must be told its own publicly reachable" >&2
    echo "[freenet-entrypoint] FATAL: address/hostname so it can advertise it to peers." >&2
    exit 1
fi


# ---------------------------------------------------------------------------
# Privilege drop
# ---------------------------------------------------------------------------
if [ "$(id -u)" -eq 0 ]; then
    mkdir -p "$CONFIG_DIR" "$DATA_DIR" "$BIN_DIR" "$LOG_DIR" "$HOME" "$(dirname "$TRANSPORT_KEYPAIR")"
    for dir in "$CONFIG_DIR" "$DATA_DIR" "$BIN_DIR" "$LOG_DIR" "$HOME" "$(dirname "$TRANSPORT_KEYPAIR")"; do
        if [ "$(stat -c '%u:%g' "$dir")" != "1000:1000" ]; then
            chown --no-dereference freenet:freenet "$dir"
        fi
    done
    exec gosu freenet "$0" "$@"
fi

for dir in "$CONFIG_DIR" "$DATA_DIR" "$BIN_DIR" "$LOG_DIR" "$HOME" "$(dirname "$TRANSPORT_KEYPAIR")"; do
    if ! mkdir -p "$dir" 2>/dev/null; then
        log "FATAL: cannot create ${dir} as uid $(id -u)."
        log "FATAL: with 'docker run --user', make the mounted /data writable by"
        log "FATAL: that user first, or point the *_DIR/TRANSPORT_KEYPAIR variables"
        log "FATAL: somewhere it owns."
        exit 1
    fi
done
seed_binary
ensure_transport_keypair "$TRANSPORT_KEYPAIR"

auto_update_args=''
if [ -n "${FREENET_DISABLE_AUTO_UPDATE:-}" ]; then
    auto_update_args='--disable-auto-update'
    log "WARNING: auto-update is DISABLED by FREENET_DISABLE_AUTO_UPDATE."
    log "WARNING: this gateway will stay on its current version and will eventually"
    log "WARNING: be refused by peers once it falls below the network's minimum"
    log "WARNING: compatible version. Only do this on a private test network."
fi

export FREENET_SUPERVISED=1
export FREENET_SYSTEMD_FAST_CRASH=1

if [ -n "$auto_update_args" ]; then
    auto_update_state='DISABLED'
else
    auto_update_state='enabled'
fi

cat <<BANNER
[freenet-entrypoint] Freenet gateway container
[freenet-entrypoint]   version           : $(binary_version "$RUN_BIN")
[freenet-entrypoint]   image ships       : $(binary_version "$IMAGE_BIN")
[freenet-entrypoint]   binary            : ${RUN_BIN}
[freenet-entrypoint]   config dir        : ${CONFIG_DIR}
[freenet-entrypoint]   data dir          : ${DATA_DIR}
[freenet-entrypoint]   log dir           : ${LOG_DIR}
[freenet-entrypoint]   home              : ${HOME}
[freenet-entrypoint]   transport keypair : ${TRANSPORT_KEYPAIR}
[freenet-entrypoint]   transport         : UDP ${NETWORK_PORT}
[freenet-entrypoint]   public address    : ${PUBLIC_NETWORK_ADDRESS}:${PUBLIC_NETWORK_PORT}
[freenet-entrypoint]   client API        : 127.0.0.1:${WS_API_PORT:-7509} (loopback only)
[freenet-entrypoint]   auto-update       : ${auto_update_state}
BANNER

node_pid=''
stopping=0

forward_signal() {
    stopping=1
    if [ -n "$node_pid" ]; then
        kill -TERM "$node_pid" 2>/dev/null || true
    fi
}
trap forward_signal TERM INT

consecutive_failures=0

while :; do
    seed_binary

    started_at="$(date +%s)"

    # shellcheck disable=SC2086
    "$RUN_BIN" network \
        --config-dir "$CONFIG_DIR" \
        --data-dir "$DATA_DIR" \
        --is-gateway \
        --skip-load-from-network \
        --transport-keypair "$TRANSPORT_KEYPAIR" \
        --network-port "$NETWORK_PORT" \
        --public-network-address "$PUBLIC_NETWORK_ADDRESS" \
        --public-network-port "$PUBLIC_NETWORK_PORT" \
        $auto_update_args "$@" &
    node_pid=$!
    echo "$node_pid" > "$PID_FILE" 2>/dev/null || true
    if [ "$stopping" -eq 1 ]; then
        kill -TERM "$node_pid" 2>/dev/null || true
    fi

    while :; do
        if wait "$node_pid"; then
            exit_code=0
        else
            exit_code=$?
        fi
        if [ "$exit_code" -gt 128 ] && kill -0 "$node_pid" 2>/dev/null; then
            continue
        fi
        break
    done
    node_pid=''
    rm -f "$PID_FILE" 2>/dev/null || true

    if [ "$stopping" -eq 1 ]; then
        log "gateway stopped for container shutdown (exit ${exit_code})"
        exit 0
    fi

    ran_for=$(( $(date +%s) - started_at ))

    case "$exit_code" in
        0)
            log "gateway exited cleanly; stopping"
            exit 0
            ;;
        43)
            log "another Freenet instance is already running (exit 43); stopping"
            exit 43
            ;;
        42)
            log "update available (exit 42) after ${ran_for}s"
            ;;
        *)
            log "gateway exited with code ${exit_code} after ${ran_for}s"
            ;;
    esac

    update_after_exit "$exit_code"

    if [ "$stopping" -eq 1 ]; then
        log "shutdown requested during the update step"
        exit 0
    fi

    if [ "$exit_code" = 42 ] && [ "$ran_for" -lt "$UPDATE_MIN_UPTIME_SECS" ]; then
        log "exit 42 after only ${ran_for}s; treating as a crash rather than a routine update"
        exit_code_is_routine=0
    elif [ "$exit_code" = 42 ] && [ "${last_update_failed:-0}" = 1 ]; then
        log "exit 42 but the update did not install; backing off"
        exit_code_is_routine=0
    elif [ "$exit_code" = 42 ]; then
        exit_code_is_routine=1
    else
        exit_code_is_routine=0
    fi

    if [ "$exit_code_is_routine" = 1 ] || [ "$ran_for" -ge "$HEALTHY_RUN_SECS" ]; then
        consecutive_failures=0
        delay="$RESTART_DELAY_MIN"
    else
        consecutive_failures=$((consecutive_failures + 1))
        delay="$(restart_delay "$consecutive_failures")"
    fi

    log "restarting in ${delay}s"
    sleep "$delay" &
    sleep_pid=$!
    wait "$sleep_pid" 2>/dev/null || true
    if [ "$stopping" -eq 1 ]; then
        log "shutdown requested while waiting to restart"
        exit 0
    fi
done
