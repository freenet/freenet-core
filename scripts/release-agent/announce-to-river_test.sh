#!/usr/bin/env bash
# shellcheck disable=SC2034
# ^ FREENET_NODE_URL / NODE_WAIT_* / LOG_FILE are read by the functions
#   extracted from announce-to-river.sh via `eval`; shellcheck cannot see
#   into the eval and would otherwise flag them all as unused.
#
# Regression test for announce-to-river.sh (issue #4208).
#
# Covers the two functions the #4208 fix touches:
#   - wait_for_node(): the bounded node-reachability poll that lets the
#     announce survive the gateway restarting the local node mid-release,
#     instead of failing on a single probe that races the restart window.
#   - log(): now writes a persistent log file only when LOG_FILE is set
#     (the old unconditional /var/log default emitted "Permission denied"
#     on every call because the announce user cannot write there).
#
# Both functions are extracted verbatim from announce-to-river.sh and
# eval'd here, so the test exercises the real implementation and cannot
# drift from it. curl is stubbed and NODE_WAIT_INTERVAL=0 keeps it instant.
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
eval "$(awk '/^wait_for_node\(\) \{/,/^}/' "$ANNOUNCE_SH")"

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

# ── wait_for_node() ──────────────────────────────────────────────────

FREENET_NODE_URL="http://stub/"
NODE_WAIT_INTERVAL=0
LOG_FILE=""

# curl stub: fails for the first CURL_FAIL_COUNT calls, then succeeds.
CURL_CALLS=0
CURL_FAIL_COUNT=0
curl() {
    CURL_CALLS=$((CURL_CALLS + 1))
    (( CURL_CALLS > CURL_FAIL_COUNT ))
}

# Node already up: succeeds on the first probe.
NODE_WAIT_ATTEMPTS=60
CURL_CALLS=0
CURL_FAIL_COUNT=0
rc=0
wait_for_node 2>/dev/null || rc=$?
check "wait_for_node() succeeds when the node is already up" "$rc" "0"
check "wait_for_node() probes once when the node is up" "$CURL_CALLS" "1"

# Node down then up: the poll bridges the restart window (issue #4208).
NODE_WAIT_ATTEMPTS=60
CURL_CALLS=0
CURL_FAIL_COUNT=3
rc=0
wait_for_node 2>/dev/null || rc=$?
check "wait_for_node() succeeds once the node returns" "$rc" "0"
check "wait_for_node() keeps probing across the down-window" "$CURL_CALLS" "4"

# Node never returns: the poll gives up after NODE_WAIT_ATTEMPTS.
NODE_WAIT_ATTEMPTS=3
CURL_CALLS=0
CURL_FAIL_COUNT=9999
rc=0
wait_for_node 2>/dev/null || rc=$?
check "wait_for_node() fails when the node never returns" "$rc" "1"
check "wait_for_node() honours NODE_WAIT_ATTEMPTS as the bound" "$CURL_CALLS" "3"

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
