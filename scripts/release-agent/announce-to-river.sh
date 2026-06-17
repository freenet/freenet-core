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
# Display-only since the readiness probe became a real room read: it appears
# in log lines for context, but riverctl reaches the node via its OWN config,
# not this URL. Changing FREENET_NODE_URL relabels the logs; it does NOT
# redirect where the room read / send actually connect.
FREENET_NODE_URL="${FREENET_NODE_URL:-http://127.0.0.1:7509/}"
# Optional persistent log file. Empty by default — the release-agent already
# captures this script's stderr to journald, and the old /var/log default
# was not writable by the announce user, so every log() call emitted a
# spurious "Permission denied" line (issue #4208).
LOG_FILE="${LOG_FILE:-}"
# Room-readability polling budget. A release announcement runs concurrently
# with the gateway update that restarts the local node, so it must wait that
# restart out (issue #4208).
#
# The per-attempt wall-clock is bounded by READ_PROBE_TIMEOUT, NOT just
# NODE_WAIT_INTERVAL: each probe is a real `riverctl message list` GET, and a
# FAILING GET during the down-window is slow — riverctl walks all ~24 legacy
# contract generations (~8s each) before giving up. Capping each probe at
# READ_PROBE_TIMEOUT keeps the worst case bounded: roughly
# ATTEMPTS * (READ_PROBE_TIMEOUT + INTERVAL). With the defaults that is
# ~60 * (15s + 5s) = ~20 min worst case if the node never recovers, while the
# happy path (room readable) returns in well under a second. The budget is
# comfortably over the ~90s gateway down-window observed on the v0.2.61
# release. Overridable (e.g. NODE_WAIT_INTERVAL=0 in tests).
NODE_WAIT_ATTEMPTS="${NODE_WAIT_ATTEMPTS:-60}"
NODE_WAIT_INTERVAL="${NODE_WAIT_INTERVAL:-5}"
# Per-probe timeout for each room read (readiness probe and post-send verify).
# Bounds the legacy-generation walk a failing GET performs during the
# down-window; the happy-path read is far faster. Overridable in tests.
READ_PROBE_TIMEOUT="${READ_PROBE_TIMEOUT:-15}"
# Set to any non-empty value to skip the post-send convergence re-read (the
# read that confirms the message actually landed in room state). Off by
# default; primarily a test hook.
SKIP_POST_VERIFY="${SKIP_POST_VERIFY:-}"

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

# Build riverctl ONCE, before the wait loop, so every subsequent
# `cargo run -p riverctl` (the read probe AND the send) is a fast no-op
# rebuild instead of a cold ~3min compile. Without this, each poll attempt
# in wait_for_room() would re-pay the build cost and the ~5min budget would
# only buy one or two real probes.
#
# RIVER_SKIP_CONTRACT_CHECK=1 matches every other riverctl invocation below
# (read_room_state, post_message, the post-send verify). riverctl's
# cli/build.rs panics ("room_contract.wasm out of date") when the flag is
# unset and a stale wasm artifact happens to be present in the workspace.
# Because this build is a *fatal* step (a failure aborts the whole announce),
# omitting the flag here would introduce a brand-new failure mode the old
# script never had — it only ever ran the skip-flagged `cargo run`. build.rs
# declares rerun-if-changed, so cargo does NOT rebuild merely because the flag
# toggles between this build and the later runs; the build-once optimization
# still holds.
build_riverctl() {
    cd "$RIVER_DIR" || return 1
    log "building riverctl once before the readiness probe"
    RIVER_SKIP_CONTRACT_CHECK=1 cargo build --quiet -p riverctl
}

# Read room state via riverctl (the same owner-signed `message list` GET that
# the post step relies on). Returns 0 iff riverctl exits 0, i.e. the room was
# actually retrieved from the local node. stdout/stderr is discarded — callers
# only care about reachability, not the message list.
read_room_state() {
    cd "$RIVER_DIR" || return 1
    RIVER_SKIP_CONTRACT_CHECK=1 timeout "$READ_PROBE_TIMEOUT" \
        cargo run --quiet -p riverctl -- \
            --signing-key-file "$SIGNING_KEY_FILE" \
            message list "$ROOM_OWNER_VK" >/dev/null 2>&1
}

# Emit the room's message list to stdout (the rendered text verify_converged
# greps). Factored out from verify_converged so the test can stub it. stderr is
# discarded; a riverctl failure yields empty output, which verify_converged
# treats as "not converged".
read_room_messages() {
    cd "$RIVER_DIR" || return 1
    RIVER_SKIP_CONTRACT_CHECK=1 timeout "$READ_PROBE_TIMEOUT" \
        cargo run --quiet -p riverctl -- \
            --signing-key-file "$SIGNING_KEY_FILE" \
            message list "$ROOM_OWNER_VK" 2>/dev/null
}

# Confirm the just-sent message converged into room state. riverctl prints
# "Message sent successfully" and exits 0 even when the room contract silently
# drops the delta (e.g. a non-owner signature — see post_message), so exit 0
# alone does NOT prove the message landed. Re-read the room and check the
# message text is present.
#
# IMPORTANT: capture the listing into a variable FIRST, then grep it — do NOT
# pipe `read_room_messages | grep`. Under `set -o pipefail`, `grep -q` exits as
# soon as it matches and closes the pipe; the still-writing producer then gets
# SIGPIPE (exit 141) and pipefail reports the whole pipeline as failed, so a
# genuinely-converged message would be misreported as a silent drop (exit 1) —
# the exact inverse of the bug this script fixes. Capturing first removes the
# pipe and the SIGPIPE race entirely.
#
# `grep -F` (fixed-string) so a message containing regex metacharacters or a
# leading `-` (guarded by `--`) still matches literally. The production
# announcement is a single line ("Freenet vX.Y.Z released: <url>", see
# release-announce.yml), so the whole text must appear contiguously. NOTE: for
# a hypothetical MULTI-line message, `grep -F` matches each pattern line
# independently (OR semantics), so this would pass if ANY one line converged —
# acceptable as a smoke check, but switch to an exact match if multi-line
# announcements are ever routed here.
verify_converged() {
    local listing
    listing="$(read_room_messages)"
    grep -qF -- "$MESSAGE" <<<"$listing"
}

# Poll until the room is actually READABLE, or NODE_WAIT_ATTEMPTS is reached.
# Returns 0 once a room GET succeeds, 1 if it never did.
#
# A WS-port check is NOT sufficient. A release announcement runs concurrently
# with the gateway update that restarts the local node (down ~90s, issue
# #4208). The OLD node keeps answering the WS port right up until the update
# stops the service, so a port probe passes, riverctl connects, and its room
# GET then hits the node mid-teardown — returning empty, which riverctl
# reports as "Room not found on the current contract or any of the 24 known
# previous contract generations" and exits 1. The release workflow already
# received its 202 by then, so the failure is silent: the v0.2.74 and
# v0.2.75 announcements were both lost exactly this way (riverctl exited 1
# in journald while the workflow showed green). The fix is to gate the post
# on a real room READ succeeding, not on the WS port answering — that proves
# the (restarted) node is up AND has the room state retrievable before we
# attempt the send.
#
# The polling also keeps this script alive past the release-agent's
# 1-second early-exit probe: the agent treats a sub-1s exit as a failure
# (HTTP 500) but returns 202 for a still-running script and reaps it in the
# background. So a node that is merely restarting still yields a completed
# announcement. The flip side: a node that stays unreadable for the whole
# budget fails only in this script's journald log, not visibly to the
# release workflow (which already received its 202). A prolonged gateway
# outage on release day is independently visible, so that trade-off is
# acceptable; surfacing late failures to the workflow would need a
# release-agent change.
wait_for_room() {
    local attempt
    for ((attempt = 1; attempt <= NODE_WAIT_ATTEMPTS; attempt++)); do
        if read_room_state; then
            if (( attempt > 1 )); then
                log "room readable after $attempt attempts"
            fi
            return 0
        fi
        if [[ "$attempt" -eq 1 ]]; then
            log "room not yet readable via $FREENET_NODE_URL — waiting (the node is restarted by the concurrent gateway update)"
        elif (( attempt % 12 == 0 )); then
            log "still waiting for room to become readable ($attempt/$NODE_WAIT_ATTEMPTS)"
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
# silent failure. Without an explicit key, riverctl signs with whatever
# identity rooms.json currently holds, and the chat-delegate sync can
# silently rewrite that `signing_key_bytes` field to a non-owner key (on
# nova it resolves to an unauthorized "bot" identity). riverctl documents
# this exact hazard and provides `--signing-key-file` as the remedy (see its
# `--help`: "signs at command time, without the UI's chat-delegate sync").
# When the message is signed by a non-owner, the room contract is valid at
# the request layer but SILENTLY DROPS the delta on merge — riverctl still
# prints "Message sent successfully" and exits 0, yet the message never
# converges into room state. Every release announcement before this fix was
# lost exactly this way (v0.2.67 and v0.2.68 were both absent from the room).
#
# Pre-patching rooms.json's `signing_key_bytes` (what this script used to do)
# is unreliable for the same reason — the sync can overwrite it before the
# send signs. The fix is the global `--signing-key-file` override, which is
# held in memory, never persisted, and is what riverctl returns from
# resolve_signing_key, so the delta is deterministically signed by the real
# owner and merges.
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

# Run post_message and propagate riverctl's REAL exit code on failure.
#
# Capture the code with a trailing `|| rc=$?` rather than `if ! post_message`:
# under `set -e` the `!`-negation resets `$?` to 0 inside the if-body, so a
# genuine riverctl failure would be seen as rc=0 — masking the failed send and
# letting the script fall through to the post-send verification as if it had
# succeeded. `cmd || rc=$?` preserves riverctl's actual exit code. Returns 0 on
# success and riverctl's non-zero code on failure; callers propagate it with
# `send_message_checked || exit $?`.
send_message_checked() {
    local rc=0
    post_message || rc=$?
    if (( rc != 0 )); then
        log "FAILED with rc=$rc"
        return "$rc"
    fi
    return 0
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

# Build riverctl once up front so the readiness probe and the send don't each
# re-pay the cold-compile cost (see build_riverctl above). A build failure is
# fatal — riverctl is required for everything below.
if ! build_riverctl; then
    log "ERROR: failed to build riverctl in $RIVER_DIR"
    exit 1
fi

# Wait until the room is actually READABLE before posting. A WS-port probe is
# not enough: the old node answers the port right up until the concurrent
# gateway update stops it, so a port-only check lets riverctl issue a room GET
# into a node mid-teardown and fail "room not found" (issue #4208 / the
# v0.2.74+v0.2.75 lost announcements). See wait_for_room() for the full race.
if ! wait_for_room; then
    log "ERROR: room $ROOM_OWNER_VK still not readable via $FREENET_NODE_URL after $NODE_WAIT_ATTEMPTS attempts"
    exit 1
fi

log "posting to River room $ROOM_OWNER_VK (msg ${#MESSAGE} bytes)"

# Sign as the owner via the in-memory --signing-key-file override (see
# post_message above for why pre-patching rooms.json does not work), and
# propagate riverctl's real exit code on failure (see send_message_checked
# for why the naive `if ! post_message` masks it as rc=0).
send_message_checked || exit $?

# Post-send convergence check (see verify_converged above): re-read room state
# and confirm the message text is present, so a future silent drop is visible
# in journald instead of masquerading as success. Skippable via SKIP_POST_VERIFY.
if [[ -n "$SKIP_POST_VERIFY" ]]; then
    log "OK (post-send verification skipped via SKIP_POST_VERIFY)"
    exit 0
fi

if verify_converged; then
    log "OK (verified message present in room state)"
    exit 0
else
    log "ERROR: riverctl reported success but the message did NOT converge into room state (silent drop)"
    exit 1
fi
