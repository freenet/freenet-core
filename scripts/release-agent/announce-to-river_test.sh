#!/usr/bin/env bash
# shellcheck disable=SC2034
# ^ FREENET_NODE_URL / NODE_WAIT_* / LOG_FILE are read by the functions
#   extracted from announce-to-river.sh via `eval`; shellcheck cannot see
#   into the eval and would otherwise flag them all as unused.
#
# Regression test for announce-to-river.sh (issues #4208 and the
# v0.2.74/v0.2.75 lost-announcement race).
#
# Covers the functions the fixes touch:
#   - wait_for_room(): the bounded readiness poll that gates the post on the
#     room actually being READABLE (a riverctl room GET succeeding), not just
#     on the WS port answering. The earlier wait_for_node() port probe passed
#     while the old node was being torn down for the gateway self-update, so
#     riverctl's room GET hit a mid-teardown node and failed "room not found"
#     — the silent drop that lost v0.2.74 and v0.2.75. wait_for_room() rides
#     out that window by retrying the real read.
#   - log(): now writes a persistent log file only when LOG_FILE is set
#     (the old unconditional /var/log default emitted "Permission denied"
#     on every call because the announce user cannot write there).
#   - post_message(): signs with the owner key via --signing-key-file.
#
# All functions are extracted verbatim from announce-to-river.sh and eval'd
# here, so the test exercises the real implementation and cannot drift from
# it. read_room_state is stubbed and NODE_WAIT_INTERVAL=0 keeps it instant.
#
# Run manually with: bash scripts/release-agent/announce-to-river_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ANNOUNCE_SH="$SCRIPT_DIR/announce-to-river.sh"

if [[ ! -f "$ANNOUNCE_SH" ]]; then
    echo "FAIL: $ANNOUNCE_SH not found" >&2
    exit 1
fi

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# Pull the two functions verbatim so the test runs the real code, not a
# copy that could drift. Mirrors scripts/release_state_restore_test.sh.
eval "$(awk '/^log\(\) \{/,/^}/' "$ANNOUNCE_SH")"
eval "$(awk '/^wait_for_room\(\) \{/,/^}/' "$ANNOUNCE_SH")"

FAILURES=0
check() {
    # check <description> <actual> <expected>
    if [[ "$2" == "$3" ]]; then
        echo "ok   - $1"
    else
        echo "FAIL - $1 (got '$2', expected '$3')" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

# ── LOG_FILE default ─────────────────────────────────────────────────

# The #4208 logging fix: with nothing in the environment, LOG_FILE must
# default to empty — the old default was an unwritable /var/log path.
# The default-assignment line is extracted and eval'd verbatim, so
# reverting it to a /var/log path fails this assertion.
unset LOG_FILE
eval "$(grep -E '^LOG_FILE=' "$ANNOUNCE_SH")"
check "LOG_FILE defaults to empty when unset" "$LOG_FILE" ""

# ── log() ────────────────────────────────────────────────────────────

# Empty LOG_FILE: log() must skip the file write entirely. The pre-fix
# code wrote unconditionally; with no file configured that write fails
# and bash prints a redirection diagnostic — the exact "Permission
# denied" spam issue #4208 reports. Assert log()'s stderr carries only
# the intended message line, no such diagnostic. (Removing the
# `[[ -n "$LOG_FILE" ]]` guard makes this assertion fail.)
LOG_FILE=""
log_stderr=$(log "hello" 2>&1)
check "log() with empty LOG_FILE emits no redirection error" \
    "$(printf '%s\n' "$log_stderr" | grep -cE 'No such file|Permission denied|ambiguous redirect' || true)" "0"

# LOG_FILE set: each call is appended.
LOG_FILE="$TMP/river.log"
log "first" 2>/dev/null
log "second" 2>/dev/null
check "log() appends every call to a set LOG_FILE" \
    "$(grep -c . "$LOG_FILE")" "2"
check "log() writes the message text" \
    "$(grep -c 'second' "$LOG_FILE")" "1"

# Unwritable LOG_FILE: log() is best-effort and must not abort the caller.
LOG_FILE="$TMP/no-such-dir/river.log"
rc=0
log "ignored" 2>/dev/null || rc=$?
check "log() survives an unwritable LOG_FILE" "$rc" "0"

# ── wait_for_room() ──────────────────────────────────────────────────
#
# The lost-announcement fix. The probe now gates on a real room READ
# (read_room_state, a riverctl GET) succeeding rather than on a WS-port
# answer, so it can't be fooled by the old node still answering the port
# while it is being torn down for the gateway self-update. The crucial
# difference from the old wait_for_node(): a node that is up at the port
# layer but whose room GET fails counts as NOT ready, so the poll keeps
# waiting through the restart instead of declaring success and letting the
# post hit a mid-teardown node (the v0.2.74/v0.2.75 silent drop).

FREENET_NODE_URL="http://stub/"
NODE_WAIT_INTERVAL=0
LOG_FILE=""

# read_room_state stub: fails for the first READ_FAIL_COUNT calls (room not
# yet retrievable — node restarting / mid-teardown), then succeeds. This
# stands in for the real riverctl `message list` GET. Stubbing the probe
# function directly keeps the test independent of riverctl/cargo while still
# exercising the genuine wait_for_room() loop body.
READ_CALLS=0
READ_FAIL_COUNT=0
read_room_state() {
    READ_CALLS=$((READ_CALLS + 1))
    (( READ_CALLS > READ_FAIL_COUNT ))
}

# Room already readable: succeeds on the first probe.
NODE_WAIT_ATTEMPTS=60
READ_CALLS=0
READ_FAIL_COUNT=0
rc=0
wait_for_room 2>/dev/null || rc=$?
check "wait_for_room() succeeds when the room is already readable" "$rc" "0"
check "wait_for_room() probes once when the room is readable" "$READ_CALLS" "1"

# Room unreadable then readable: the poll bridges the restart window. This is
# the case the old port-only probe got wrong — the read fails while the node
# is mid-teardown, and only succeeds once the restarted node has the room.
NODE_WAIT_ATTEMPTS=60
READ_CALLS=0
READ_FAIL_COUNT=3
rc=0
wait_for_room 2>/dev/null || rc=$?
check "wait_for_room() succeeds once the room becomes readable" "$rc" "0"
check "wait_for_room() keeps probing across the down-window" "$READ_CALLS" "4"

# Room never readable: the poll gives up after NODE_WAIT_ATTEMPTS.
NODE_WAIT_ATTEMPTS=3
READ_CALLS=0
READ_FAIL_COUNT=9999
rc=0
wait_for_room 2>/dev/null || rc=$?
check "wait_for_room() fails when the room never becomes readable" "$rc" "1"
check "wait_for_room() honours NODE_WAIT_ATTEMPTS as the bound" "$READ_CALLS" "3"

# ── post_message() ───────────────────────────────────────────────────
#
# The signing-identity fix. post_message MUST hand riverctl the owner key
# via the global `--signing-key-file` override, BEFORE the `message`
# subcommand. Without it riverctl signs with the identity rooms.json holds
# (on nova an unauthorized key the chat-delegate sync can rewrite in), and
# the room contract silently drops the delta on merge — the bug that lost
# the v0.2.67/v0.2.68 announcements while riverctl still exited 0. The
# function is extracted verbatim, so dropping the override (or misplacing it
# after the subcommand) fails these assertions.
eval "$(awk '/^post_message\(\) \{/,/^}/' "$ANNOUNCE_SH")"

RIVER_DIR="$TMP"
SIGNING_KEY_FILE="$TMP/owner_key.bin"
ROOM_OWNER_VK="OWNERVK111"
MESSAGE="Freenet vX.Y.Z released"
CARGO_ARGS_FILE="$TMP/cargo_args"
CARGO_SKIP_FILE="$TMP/cargo_skip"

# Stubs (inherited by the subshell that runs post_message): `timeout` drops
# its duration and runs the rest; `cargo` records its argv joined plus the
# RIVER_SKIP_CONTRACT_CHECK env it inherits from the command prefix.
timeout() { shift; "$@"; }
cargo() { echo "$*" > "$CARGO_ARGS_FILE"; echo "${RIVER_SKIP_CONTRACT_CHECK:-}" > "$CARGO_SKIP_FILE"; }

( post_message ) >/dev/null 2>&1 || true
cargo_args="$(cat "$CARGO_ARGS_FILE" 2>/dev/null || true)"
cargo_skip="$(cat "$CARGO_SKIP_FILE" 2>/dev/null || true)"

check "post_message signs with the owner key via --signing-key-file before the subcommand" \
    "$(printf '%s' "$cargo_args" | grep -cF -- "--signing-key-file $SIGNING_KEY_FILE message send" || true)" "1"
check "post_message sends to the owner VK" \
    "$(printf '%s' "$cargo_args" | grep -cF -- "message send $ROOM_OWNER_VK" || true)" "1"
check "post_message runs riverctl from the source repo" \
    "$(printf '%s' "$cargo_args" | grep -cF -- "run --quiet -p riverctl" || true)" "1"
check "post_message sets RIVER_SKIP_CONTRACT_CHECK for the riverctl run" \
    "$cargo_skip" "1"

# ── result ───────────────────────────────────────────────────────────

if (( FAILURES > 0 )); then
    echo "$FAILURES announce-to-river.sh test(s) failed" >&2
    exit 1
fi
echo "All announce-to-river.sh tests passed."
