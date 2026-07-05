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
# happy path returns quickly once the node is ready. The budget is comfortably
# over the ~90s gateway down-window observed on the v0.2.61 release.
#
# Happy-path cost differs by readiness mode (see wait_for_room()):
#   - Version-aware (#4496): the gate returns on the FIRST attempt where the
#     node reports the target version AND the room reads — one extra cheap
#     `freenet --version` fork per probe, negligible next to the room GET.
#   - Streak fallback: the gate needs READ_STABLE_COUNT (default 3) consecutive
#     successful reads, so the happy path costs up to ~READ_STABLE_COUNT extra
#     room reads / intervals (a few seconds at the defaults) beyond a single
#     success. Both are well within the budget above.
# Overridable (e.g. NODE_WAIT_INTERVAL=0 in tests).
NODE_WAIT_ATTEMPTS="${NODE_WAIT_ATTEMPTS:-60}"
NODE_WAIT_INTERVAL="${NODE_WAIT_INTERVAL:-5}"
# Per-probe timeout for each room read (readiness probe and post-send verify).
# Bounds the legacy-generation walk a failing GET performs during the
# down-window; the happy-path read is far faster. Overridable in tests.
READ_PROBE_TIMEOUT="${READ_PROBE_TIMEOUT:-15}"
# Target release version this announcement is for, WITHOUT a leading `v`
# (e.g. "0.2.90"). Plumbed end-to-end from release-announce.yml's `resolve`
# job → the nova release-agent (ANNOUNCE_TARGET_VERSION env on the sudo
# invocation) → here. This is the #4496 fix: it lets wait_for_room() gate the
# post on the RESTARTED node reporting the TARGET version, deterministically
# closing the race instead of merely narrowing it with a read-streak heuristic.
#
# release-announce.yml and gateway-update.yml both fire on the same
# `release: published` event with no ordering between them. Without the target
# version, an early room read served by the OLD node (moments before the
# concurrent gateway self-update stops the service) looks identical to a read
# served by the restarted node, so the announce can drop straight into the
# teardown window it was meant to ride out. With the target version we require
# the running node's version to EQUAL the target before proceeding — and the
# gateway update deploys stop→swap-binary→start, so while the old node still
# serves the room the on-disk binary is still the OLD version. The old-node
# read therefore fails the version gate and the poll waits for the restart.
#
# Empty (the default, and what a legacy release-agent that predates this
# plumbing passes) falls back to the READ_STABLE_COUNT streak heuristic below,
# which narrows but does not fully close the race — see wait_for_room().
ANNOUNCE_TARGET_VERSION="${ANNOUNCE_TARGET_VERSION:-}"
# Strip an accidental leading `v` so "v0.2.90" and "0.2.90" both work.
ANNOUNCE_TARGET_VERSION="${ANNOUNCE_TARGET_VERSION#v}"
# Command whose `--version` output reports the version of the LOCALLY RUNNING
# freenet node. The gateway update swaps this on-disk binary during the
# restart, so once it reports the target version the swap has completed. The
# node runs from this same binary, so its version is the running node's
# version once the service has been restarted onto it. Overridable in tests
# (stubbed) and for non-default install layouts.
FREENET_BIN="${FREENET_BIN:-freenet}"
# Number of CONSECUTIVE successful room reads wait_for_room() must observe
# before it declares the node ready when NO target version is available
# (legacy fallback path only). A single successful read is NOT enough: an
# early read can be served by the OLD node before the concurrent gateway
# self-update stops the service (issue #4496). Requiring several successes
# SPANNING at least one NODE_WAIT_INTERVAL means a teardown that begins after
# the first probe resets the streak. This is a HEURISTIC — it narrows the race
# but cannot fully close it (a teardown that has not yet begun when the streak
# completes is not caught). The version gate above is the real fix; this only
# runs when the target version was not plumbed through. Overridable in tests.
#
# INVARIANT: READ_STABLE_COUNT must be <= NODE_WAIT_ATTEMPTS, otherwise the
# streak can never reach threshold and wait_for_room() would always fail on
# the fallback path. Clamped at use (see wait_for_room()).
READ_STABLE_COUNT="${READ_STABLE_COUNT:-3}"
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

# Report the version of the locally running freenet node by parsing
# `FREENET_BIN --version`. Echoes a bare semver (e.g. "0.2.90") on stdout, or
# nothing if the binary is missing / the output is unparseable (treated by the
# caller as "not yet the target"). Never fails the script.
node_version() {
    local out
    out="$("$FREENET_BIN" --version 2>/dev/null)" || return 0
    grep -oE '[0-9]+\.[0-9]+\.[0-9]+' <<<"$out" | head -1
}

# Poll until the local node is ready to receive the announcement, or
# NODE_WAIT_ATTEMPTS is reached. Returns 0 once ready, 1 if it never became
# ready within the budget.
#
# Readiness has two definitions depending on whether the TARGET VERSION was
# plumbed through (ANNOUNCE_TARGET_VERSION):
#
#   1. Version-aware (the #4496 fix, taken whenever ANNOUNCE_TARGET_VERSION is
#      set): ready == the running node reports the target version AND the room
#      is readable. release-announce.yml and gateway-update.yml fire on the
#      same `release: published` event with no ordering, so an early room read
#      can be served by the OLD node moments before the concurrent gateway
#      self-update stops the service. But the update deploys
#      stop→swap-binary→start, so while the OLD node still serves the room the
#      on-disk binary is still the OLD version — the version check rejects that
#      read and the poll waits for the restarted, target-version node. This
#      closes the race deterministically rather than heuristically.
#
#   2. Streak fallback (only when ANNOUNCE_TARGET_VERSION is empty — e.g. a
#      release-agent that predates the version plumbing): ready == the room has
#      been readable for READ_STABLE_COUNT consecutive probes. This narrows the
#      race (a teardown beginning after an early success resets the streak) but
#      does NOT fully close it: if teardown has not yet begun when the streak
#      completes, the read still runs against the old node. It is a best-effort
#      heuristic for the legacy path, not a guarantee.
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
    local attempt streak=0 stable_count="$READ_STABLE_COUNT"
    # Clamp the streak threshold to the attempt budget. A configuration with
    # READ_STABLE_COUNT > NODE_WAIT_ATTEMPTS could never reach the streak, so
    # the fallback path would ALWAYS fail regardless of node health. Clamping
    # keeps the gate satisfiable; the version-aware path does not use it.
    if (( stable_count > NODE_WAIT_ATTEMPTS )); then
        log "READ_STABLE_COUNT ($stable_count) > NODE_WAIT_ATTEMPTS ($NODE_WAIT_ATTEMPTS); clamping streak threshold to $NODE_WAIT_ATTEMPTS"
        stable_count="$NODE_WAIT_ATTEMPTS"
    fi
    if (( stable_count < 1 )); then
        stable_count=1
    fi

    if [[ -n "$ANNOUNCE_TARGET_VERSION" ]]; then
        log "waiting for the node to report target version $ANNOUNCE_TARGET_VERSION and the room to become readable (#4496 version-aware gate)"
    fi

    for ((attempt = 1; attempt <= NODE_WAIT_ATTEMPTS; attempt++)); do
        if [[ -n "$ANNOUNCE_TARGET_VERSION" ]]; then
            # ── Version-aware readiness (#4496 fix) ──
            # Require BOTH the running node on the target version AND the room
            # readable. The old node still serving the room reports the OLD
            # version (deploy swaps the binary only after stopping it), so its
            # read is rejected here and cannot satisfy the gate.
            local running
            running="$(node_version)"
            if [[ "$running" == "$ANNOUNCE_TARGET_VERSION" ]] && read_room_state; then
                log "node reports target version $ANNOUNCE_TARGET_VERSION and room is readable after $attempt attempts"
                return 0
            fi
            if [[ "$attempt" -eq 1 ]]; then
                log "node not yet on target version $ANNOUNCE_TARGET_VERSION (running: ${running:-unknown}) or room not readable — waiting for the gateway update to restart it"
            elif (( attempt % 12 == 0 )); then
                log "still waiting for target version $ANNOUNCE_TARGET_VERSION + readable room ($attempt/$NODE_WAIT_ATTEMPTS, running: ${running:-unknown})"
            fi
        else
            # ── Streak fallback (legacy, no target version) ──
            if read_room_state; then
                streak=$((streak + 1))
                if (( streak >= stable_count )); then
                    if (( attempt > stable_count )); then
                        log "room readable for $stable_count consecutive probes after $attempt attempts"
                    fi
                    return 0
                fi
            else
                if (( streak > 0 )); then
                    log "room read failed after $streak readable probe(s) — resetting streak (node likely mid-teardown for the gateway update)"
                fi
                streak=0
            fi
            if [[ "$attempt" -eq 1 ]]; then
                log "room not yet stably readable via $FREENET_NODE_URL — waiting (no target version plumbed; using streak heuristic)"
            elif (( attempt % 12 == 0 )); then
                log "still waiting for room to become stably readable ($attempt/$NODE_WAIT_ATTEMPTS, streak $streak/$stable_count)"
            fi
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

# Argument parsing. The FIRST positional is the message; an optional
# `--target-version <v>` flag (passed by the release-agent, see announcer.rs)
# supplies the release version this announcement is for and enables the #4496
# version-aware readiness gate. The flag overrides any ANNOUNCE_TARGET_VERSION
# inherited from the environment. Parsing tolerates the flag appearing before
# or after the message so callers need not fix an ordering.
#
# Extracted into a function (setting the MESSAGE / ANNOUNCE_TARGET_VERSION
# globals) so announce-to-river_test.sh can drive it directly — the
# order-tolerance and error branches are otherwise untestable. `usage` exits
# non-zero on a malformed invocation.
parse_args() {
    MESSAGE=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --target-version)
                if [[ $# -lt 2 ]]; then
                    log "ERROR: --target-version requires an argument"
                    usage
                fi
                ANNOUNCE_TARGET_VERSION="${2#v}"
                shift 2
                ;;
            --)
                shift
                if [[ -n "$MESSAGE" || $# -ne 1 ]]; then
                    usage
                fi
                MESSAGE="$1"
                shift
                ;;
            *)
                if [[ -n "$MESSAGE" ]]; then
                    log "ERROR: unexpected extra argument: $1"
                    usage
                fi
                MESSAGE="$1"
                shift
                ;;
        esac
    done
}

parse_args "$@"

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
