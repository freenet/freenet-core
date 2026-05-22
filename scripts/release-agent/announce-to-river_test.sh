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

# ── log() ────────────────────────────────────────────────────────────

# Empty LOG_FILE (the new default): nothing is written to disk, and the
# call still succeeds.
LOG_FILE=""
log "hello" 2>/dev/null
check "log() with empty LOG_FILE writes no file" \
    "$(find "$TMP" -type f | wc -l)" "0"

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

# ── result ───────────────────────────────────────────────────────────

if (( FAILURES > 0 )); then
    echo "$FAILURES announce-to-river.sh test(s) failed" >&2
    exit 1
fi
echo "All announce-to-river.sh tests passed."
