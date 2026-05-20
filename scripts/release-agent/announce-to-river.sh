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
LOG_FILE="${LOG_FILE:-/var/log/freenet-release-agent-river.log}"

log() {
    local timestamp
    timestamp=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    echo "[$timestamp] $*" >&2
    # Best-effort persistent log; non-fatal if not writable.
    echo "[$timestamp] $*" >> "$LOG_FILE" 2>/dev/null || true
}

usage() {
    cat >&2 <<EOF
Usage: $0 <message>

Posts <message> to the Freenet Official River room.
EOF
    exit 1
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

# Check that the Freenet node is reachable. riverctl will hang otherwise.
if ! curl -s --max-time 3 "$FREENET_NODE_URL" >/dev/null 2>&1; then
    log "ERROR: Freenet node not reachable at $FREENET_NODE_URL"
    exit 1
fi

# Ensure the room owner signing key in rooms.json matches the on-disk
# key. This mirrors the equivalent block in scripts/release.sh — without
# it, posting fails if the local rooms.json drifted.
#
# Failure modes are logged and abort (no `|| true` swallowing): if the
# rooms.json schema drifted, posting with a stale key would be confusing
# at best and identity-impersonation at worst. We'd rather fail loud.
#
# Writes are atomic (temp file + os.replace) so a crash mid-rewrite
# can't leave rooms.json half-written for the human user.
if ! python3 - <<PY; then
import json, os, sys
signing_key_file = "$SIGNING_KEY_FILE"
rooms_file = "$ROOMS_JSON"
room_vk = "$ROOM_OWNER_VK"
try:
    with open(signing_key_file, 'rb') as f:
        key_bytes = list(f.read())
    with open(rooms_file, 'r') as f:
        data = json.load(f)
    if room_vk not in data.get('rooms', {}):
        # No-op if the room isn't in local storage — riverctl will
        # itself fail with a clear error in that case.
        sys.exit(0)
    current_key = data['rooms'][room_vk].get('signing_key_bytes', [])
    if current_key != key_bytes:
        data['rooms'][room_vk]['signing_key_bytes'] = key_bytes
        tmp = rooms_file + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(data, f)
        os.replace(tmp, rooms_file)
        print("rooms.json signing key resynced", file=sys.stderr)
except Exception as e:
    print(f"rooms.json sync failed: {e}", file=sys.stderr)
    sys.exit(1)
PY
    log "ERROR: rooms.json signing-key sync failed (see above)"
    exit 1
fi

log "posting to River room $ROOM_OWNER_VK (msg ${#MESSAGE} bytes)"

# Use `cargo run -p riverctl` from the source repo, NOT the installed
# binary. The installed binary embeds room_contract.wasm at install
# time, which can become stale when the contract WASM changes. The
# repo version uses build.rs to copy the current WASM at build time.
# RIVER_SKIP_CONTRACT_CHECK=1 bypasses the publish-time WASM staleness
# check (only relevant when publishing riverctl, not when running it).
cd "$RIVER_DIR"
if RIVER_SKIP_CONTRACT_CHECK=1 timeout 180 \
       cargo run --quiet -p riverctl -- message send "$ROOM_OWNER_VK" "$MESSAGE"; then
    log "OK"
    exit 0
else
    rc=$?
    log "FAILED with rc=$rc"
    exit "$rc"
fi
