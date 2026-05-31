#!/bin/bash
# Post a message to the Freenet Official River room.
#
# Invoked by freenet-release-agent's `POST /announce/river` endpoint via:
#
#     sudo -n -u <river_announce_user> /usr/local/bin/announce-to-river.sh "<message>"
#
# The sudoers entry (see scripts/release-agent/sudoers.freenet-release-agent)
# narrows what `freenet-update` can invoke to this exact path with a single
# (sudoers-wildcard) message arg.
#
# Configuration is via environment variables with defaults appropriate for
# the nova install. Override them in `freenet-release-agent.service`'s
# Environment= lines or in /etc/freenet-release-agent/announce.env (sourced
# below if present).

set -euo pipefail

if [[ -f /etc/freenet-release-agent/announce.env ]]; then
    # shellcheck disable=SC1091
    source /etc/freenet-release-agent/announce.env
fi

RIVER_DIR="${RIVER_DIR:-$HOME/code/freenet/river/main}"
ROOM_OWNER_VK="${ROOM_OWNER_VK:-4uNUKFzZQCnzo4K2ecZ16cMsYEEfoaRS35z6exEsbvm4}"
SIGNING_KEY_FILE="${SIGNING_KEY_FILE:-$HOME/.config/freenet-river-official/room_owner_signing_key.bin}"
ROOMS_JSON="${ROOMS_JSON:-$HOME/.local/share/river/rooms.json}"
FREENET_NODE_URL="${FREENET_NODE_URL:-http://127.0.0.1:7509/}"
# Optional persistent log file. Empty by default — the release-agent already
# captures this script's stderr to journald, and the old /var/log default
# was not writable by the announce user, so every log() call emitted a
# spurious "Permission denied" line (issue #4208).
LOG_FILE="${LOG_FILE:-}"
# Node-reachability polling budget. A release announcement runs concurrently
# with the gateway update that restarts the local node, so it must wait that
# restart out (issue #4208). ATTEMPTS * INTERVAL is the wall-clock budget —
# default ~5 min, comfortably over the ~90s gateway down-window observed on
# the v0.2.61 release. Overridable (e.g. INTERVAL=0 in tests).
NODE_WAIT_ATTEMPTS="${NODE_WAIT_ATTEMPTS:-60}"
NODE_WAIT_INTERVAL="${NODE_WAIT_INTERVAL:-5}"

log() {
    local timestamp
    timestamp=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    echo "[$timestamp] $*" >&2
    # Optional best-effort persistent log; non-fatal if not writable.
    if [[ -n "$LOG_FILE" ]]; then
        echo "[$timestamp] $*" >> "$LOG_FILE" 2>/dev/null || true
    fi
}

usage() {
    cat >&2 <<EOF
Usage: $0 <message>

Posts <message> to the Freenet Official River room.
EOF
    exit 1
}

# Poll FREENET_NODE_URL until it responds, or NODE_WAIT_ATTEMPTS is reached.
# Returns 0 once reachable, 1 if it never became reachable.
#
# A release announcement runs concurrently with the gateway update that
# restarts the local node (down ~90s), so a single probe routinely races
# that window (issue #4208) — hence the polling.
#
# The polling also keeps this script alive past the release-agent's
# 1-second early-exit probe: the agent treats a sub-1s exit as a failure
# (HTTP 500) but returns 202 for a still-running script and reaps it in the
# background. So a node that is merely restarting still yields a completed
# announcement. The flip side: a node that stays down for the whole budget
# fails only in this script's journald log, not visibly to the release
# workflow (which already received its 202). A prolonged gateway outage on
# release day is independently visible, so that trade-off is acceptable;
# surfacing late failures to the workflow would need a release-agent change.
wait_for_node() {
    local attempt
    for ((attempt = 1; attempt <= NODE_WAIT_ATTEMPTS; attempt++)); do
        if curl -s --max-time 3 "$FREENET_NODE_URL" >/dev/null 2>&1; then
            return 0
        fi
        if [[ "$attempt" -eq 1 ]]; then
            log "Freenet node not reachable at $FREENET_NODE_URL yet — waiting (it is restarted by the concurrent gateway update)"
        elif (( attempt % 12 == 0 )); then
            log "still waiting for Freenet node ($attempt/$NODE_WAIT_ATTEMPTS)"
        fi
        # No sleep after the final attempt — nothing follows it.
        if (( attempt < NODE_WAIT_ATTEMPTS )); then
            sleep "$NODE_WAIT_INTERVAL"
        fi
    done
    return 1
}

# Post $MESSAGE to the room, signed by the room OWNER.
#
# The signing identity is load-bearing and the source of a long-standing
# silent failure. riverctl's normal send path runs the chat-delegate sync,
# which rewrites rooms.json's `signing_key_bytes` back to the locally-loaded
# identity (on nova that's an unauthorized "bot" key, not the owner) right
# before signing. If we sign with that identity, the room contract is valid
# at the request layer but SILENTLY DROPS the delta on merge — riverctl still
# prints "Message sent successfully" and exits 0, yet the message never
# converges into room state. Every release announcement before this fix was
# lost exactly this way (v0.2.67 and v0.2.68 were both absent from the room).
#
# Pre-patching rooms.json (what this script used to do) does NOT work: the
# delegate sync overwrites the patch in-process before signing. The fix is
# riverctl's global `--signing-key-file` override, which is in-memory and
# immune to the sync, so the delta is signed by the real owner and merges.
#
# `cargo run -p riverctl` from the source repo (not the installed binary) so
# the embedded room_contract.wasm — and thus the derived contract key —
# matches the currently-deployed room.
post_message() {
    cd "$RIVER_DIR" || return 1
    RIVER_SKIP_CONTRACT_CHECK=1 timeout 180 \
        cargo run --quiet -p riverctl -- \
            --signing-key-file "$SIGNING_KEY_FILE" \
            message send "$ROOM_OWNER_VK" "$MESSAGE"
}

if [[ $# -ne 1 ]]; then
    usage
fi

MESSAGE="$1"

# Defense in depth: empty/oversized payloads should have been rejected
# upstream, but the privilege-boundary script re-checks. The cap matches
# the agent's ANNOUNCE_MESSAGE_MAX_LEN (4 KiB).
if [[ -z "$MESSAGE" ]]; then
    log "ERROR: empty message"
    exit 1
fi
if [[ ${#MESSAGE} -gt 4096 ]]; then
    log "ERROR: message too long (${#MESSAGE} bytes; max 4096)"
    exit 1
fi

if [[ ! -d "$RIVER_DIR" ]]; then
    log "ERROR: river repo not found at $RIVER_DIR (set RIVER_DIR)"
    exit 1
fi
if [[ ! -f "$SIGNING_KEY_FILE" ]]; then
    log "ERROR: signing key not found at $SIGNING_KEY_FILE"
    exit 1
fi
if [[ ! -f "$ROOMS_JSON" ]]; then
    log "ERROR: rooms.json not found at $ROOMS_JSON (run 'riverctl room create' once)"
    exit 1
fi

# Wait for the local Freenet node before invoking riverctl (it hangs
# otherwise). See wait_for_node() above for why this polls rather than
# probing once.
if ! wait_for_node; then
    log "ERROR: Freenet node still not reachable at $FREENET_NODE_URL after $NODE_WAIT_ATTEMPTS attempts"
    exit 1
fi

log "posting to River room $ROOM_OWNER_VK (msg ${#MESSAGE} bytes)"

# Sign as the owner via the in-memory --signing-key-file override (see
# post_message above for why pre-patching rooms.json does not work).
if post_message; then
    log "OK"
    exit 0
else
    rc=$?
    log "FAILED with rc=$rc"
    exit "$rc"
fi
