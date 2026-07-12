#!/usr/bin/env bash
# shellcheck disable=SC2034,SC2317,SC2329
# ^ SC2034: FREENET_NODE_URL / NODE_WAIT_* / LOG_FILE are read by the functions
#   extracted from announce-to-river.sh via `eval`; shellcheck cannot see
#   into the eval and would otherwise flag them all as unused.
#   SC2317: the stub functions (read_room_messages, post_message, curl) are
#   invoked indirectly from those eval'd functions, so shellcheck wrongly
#   reports their bodies as unreachable.
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
eval "$(awk '/^node_version\(\) \{/,/^}/' "$ANNOUNCE_SH")"
eval "$(awk '/^wait_for_room\(\) \{/,/^}/' "$ANNOUNCE_SH")"
eval "$(awk '/^parse_args\(\) \{/,/^}/' "$ANNOUNCE_SH")"

# Default: no target version plumbed, so the streak-fallback tests below run
# the fallback path. The version-aware section sets it explicitly.
ANNOUNCE_TARGET_VERSION=""

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

# ══════════════════════════════════════════════════════════════════════
# Streak-fallback path (ANNOUNCE_TARGET_VERSION empty): the read-streak
# heuristic used only when the target version was NOT plumbed through (a
# legacy release-agent). It NARROWS the #4496 race but does not fully close
# it; the version-aware section further below is the deterministic fix.
# ══════════════════════════════════════════════════════════════════════
ANNOUNCE_TARGET_VERSION=""

# Room continuously readable: wait_for_room() must observe READ_STABLE_COUNT
# consecutive successes before returning (the streak heuristic), so with a
# clean node it probes exactly READ_STABLE_COUNT times.
READ_STABLE_COUNT=3
NODE_WAIT_ATTEMPTS=60
READ_CALLS=0
READ_FAIL_COUNT=0
rc=0
wait_for_room 2>/dev/null || rc=$?
check "wait_for_room() succeeds when the room is continuously readable" "$rc" "0"
check "wait_for_room() requires READ_STABLE_COUNT consecutive readable probes" "$READ_CALLS" "3"

# Room unreadable then readable: the poll bridges the restart window. This is
# the case the old port-only probe got wrong — the read fails while the node
# is mid-teardown, and only succeeds once the restarted node has the room.
# With READ_STABLE_COUNT=3 and 3 leading failures it needs 3 fails + 3
# successes = 6 probes.
READ_STABLE_COUNT=3
NODE_WAIT_ATTEMPTS=60
READ_CALLS=0
READ_FAIL_COUNT=3
rc=0
wait_for_room 2>/dev/null || rc=$?
check "wait_for_room() succeeds once the room becomes stably readable" "$rc" "0"
check "wait_for_room() keeps probing across the down-window" "$READ_CALLS" "6"

# ── #4496: early success against the OLD node must NOT satisfy the gate ──
#
# The residual race: release-announce and the gateway self-update fire on the
# same event with no ordering. A lone successful read can be served by the OLD
# node moments before the update stops the service. wait_for_room() must NOT
# proceed on that transient success — the mid-teardown failure that follows
# has to reset the streak so the post lands against the RESTARTED node.
#
# Stub the exact interleaving: success (old node) → 2 failures (teardown /
# restart) → sustained successes (restarted node). A first-success gate
# (pre-#4496) would return after probe 1 (READ_CALLS=1) and drop into the
# teardown window. The sustained gate must ride through: 1 (old) + 2 (down)
# + 3 (restarted streak) = 6 probes, and crucially MORE than 1.
READ_STABLE_COUNT=3
NODE_WAIT_ATTEMPTS=60
# Custom stub: readable on probe 1, unreadable on 2-3, readable thereafter.
read_room_state() {
    READ_CALLS=$((READ_CALLS + 1))
    if (( READ_CALLS == 1 )); then return 0; fi          # old node still up
    if (( READ_CALLS <= 3 )); then return 1; fi          # teardown window
    return 0                                             # restarted node
}
READ_CALLS=0
rc=0
wait_for_room 2>/dev/null || rc=$?
check "wait_for_room() rides out a teardown that follows an early success (#4496)" "$rc" "0"
check "wait_for_room() does not return on the first (old-node) success (#4496)" \
    "$( (( READ_CALLS > 1 )) && echo yes || echo no )" "yes"
check "wait_for_room() resets its streak on the teardown failure (#4496)" "$READ_CALLS" "6"

# Restore the simple counting stub for the remaining cases.
read_room_state() {
    READ_CALLS=$((READ_CALLS + 1))
    (( READ_CALLS > READ_FAIL_COUNT ))
}

# Room never readable: the poll gives up after NODE_WAIT_ATTEMPTS.
READ_STABLE_COUNT=3
NODE_WAIT_ATTEMPTS=3
READ_CALLS=0
READ_FAIL_COUNT=9999
rc=0
wait_for_room 2>/dev/null || rc=$?
check "wait_for_room() fails when the room never becomes readable" "$rc" "1"
check "wait_for_room() honours NODE_WAIT_ATTEMPTS as the bound" "$READ_CALLS" "3"

# A room readable only intermittently (never a full streak) must never satisfy
# the gate — a flapping node is treated as not ready. Alternating pass/fail
# with READ_STABLE_COUNT=3 can never reach a streak of 3.
READ_STABLE_COUNT=3
NODE_WAIT_ATTEMPTS=10
READ_CALLS=0
read_room_state() {
    READ_CALLS=$((READ_CALLS + 1))
    (( READ_CALLS % 2 == 0 ))   # fail, pass, fail, pass, ...
}
rc=0
wait_for_room 2>/dev/null || rc=$?
check "wait_for_room() never proceeds on a flapping (never-sustained) node (fallback)" "$rc" "1"
# Restore the counting stub.
read_room_state() {
    READ_CALLS=$((READ_CALLS + 1))
    (( READ_CALLS > READ_FAIL_COUNT ))
}

# Clamp invariant: READ_STABLE_COUNT > NODE_WAIT_ATTEMPTS must NOT make the
# fallback gate unsatisfiable. Without the clamp the streak could never reach
# threshold and a perfectly healthy, continuously-readable node would be
# reported as never-ready. With the clamp the gate returns success.
READ_STABLE_COUNT=100
NODE_WAIT_ATTEMPTS=5
READ_CALLS=0
READ_FAIL_COUNT=0
rc=0
wait_for_room 2>/dev/null || rc=$?
check "wait_for_room() clamps READ_STABLE_COUNT to NODE_WAIT_ATTEMPTS (satisfiable)" "$rc" "0"
check "wait_for_room() returns at the clamped threshold, not the raw count" "$READ_CALLS" "5"

# ══════════════════════════════════════════════════════════════════════
# Version-aware path (ANNOUNCE_TARGET_VERSION set): the #4496 deterministic
# fix. Readiness == the running node reports the TARGET version AND the room
# is readable. The key property: an early read served by the OLD node — which
# still reports the OLD version because the gateway update swaps the binary
# only after stopping the service — MUST NOT satisfy the gate.
# ══════════════════════════════════════════════════════════════════════

TARGET="0.2.90"
OLD="0.2.89"

# NODE_VERSION_SEQ drives the node_version stub: one entry consumed per call.
# The index is kept in a FILE, not a variable, because wait_for_room() invokes
# node_version() via command substitution (`running="$(node_version)"`), which
# runs in a subshell — a variable increment would be lost, but a file write
# survives. read_room_state is stubbed per scenario.
NV_IDX_FILE="$TMP/nv_idx"
NODE_VERSION_SEQ=()
node_version() {
    local idx
    idx="$(cat "$NV_IDX_FILE" 2>/dev/null || echo 0)"
    printf '%s' "${NODE_VERSION_SEQ[$idx]:-}"
    echo $((idx + 1)) > "$NV_IDX_FILE"
}
# Helper: reset the version-sequence cursor before each scenario.
nv_reset() { echo 0 > "$NV_IDX_FILE"; }

# Scenario A — the exact #4496 interleaving. The room is READABLE the whole
# time (old node up, then restarted node up), but the version transitions
# OLD → OLD → TARGET. A version-blind gate (or the streak heuristic if the
# teardown never dropped the read) would post against the OLD node on probe 1.
# The version gate must reject probes 1-2 (old version) and only proceed on
# probe 3, when the node reports TARGET.
ANNOUNCE_TARGET_VERSION="$TARGET"
NODE_WAIT_ATTEMPTS=60
READ_CALLS=0
read_room_state() { READ_CALLS=$((READ_CALLS + 1)); return 0; }   # always readable
NODE_VERSION_SEQ=("$OLD" "$OLD" "$TARGET" "$TARGET")
nv_reset
rc=0
wait_for_room 2>/dev/null || rc=$?
# node_version is checked FIRST and short-circuits read_room_state, so on the
# OLD-version probes 1-2 the room read is never even attempted — the announce
# provably cannot land against the old node. read_room_state fires only on
# probe 3 (version now TARGET), so exactly one read occurs.
check "wait_for_room() proceeds once the node reports the TARGET version (#4496)" "$rc" "0"
check "wait_for_room() does NOT read/post against the OLD-version node (#4496)" "$READ_CALLS" "1"
check "wait_for_room() consumed 3 version probes before proceeding (#4496)" \
    "$(cat "$NV_IDX_FILE")" "3"

# Scenario B — the room is readable AND the version is TARGET on probe 1 (the
# announce arrives after the update already finished). Must return immediately,
# on the very first probe — no gratuitous extra waiting once the node is on the
# target version.
ANNOUNCE_TARGET_VERSION="$TARGET"
NODE_WAIT_ATTEMPTS=60
READ_CALLS=0
read_room_state() { READ_CALLS=$((READ_CALLS + 1)); return 0; }
NODE_VERSION_SEQ=("$TARGET" "$TARGET")
nv_reset
rc=0
wait_for_room 2>/dev/null || rc=$?
check "wait_for_room() returns immediately when already on TARGET + readable (#4496)" "$rc" "0"
check "wait_for_room() does not over-poll once ready (#4496)" "$READ_CALLS" "1"

# Scenario C — node reaches TARGET version but the room is not YET readable
# (restarted, still loading room state). The gate must keep waiting until BOTH
# hold, not proceed on version alone.
ANNOUNCE_TARGET_VERSION="$TARGET"
NODE_WAIT_ATTEMPTS=60
READ_CALLS=0
# Room readable only from the 3rd probe; version is TARGET throughout.
read_room_state() { READ_CALLS=$((READ_CALLS + 1)); (( READ_CALLS >= 3 )); }
NODE_VERSION_SEQ=("$TARGET" "$TARGET" "$TARGET" "$TARGET")
nv_reset
rc=0
wait_for_room 2>/dev/null || rc=$?
check "wait_for_room() waits for the room even when version is TARGET (#4496)" "$rc" "0"
check "wait_for_room() proceeds only once the room is also readable (#4496)" "$READ_CALLS" "3"

# Scenario D — the node never reaches the target version within the budget
# (update stalled). The gate must fail (exit 1) rather than posting against the
# stuck old node.
ANNOUNCE_TARGET_VERSION="$TARGET"
NODE_WAIT_ATTEMPTS=4
READ_CALLS=0
read_room_state() { READ_CALLS=$((READ_CALLS + 1)); return 0; }   # readable but wrong version
NODE_VERSION_SEQ=("$OLD" "$OLD" "$OLD" "$OLD" "$OLD")
nv_reset
rc=0
wait_for_room 2>/dev/null || rc=$?
check "wait_for_room() fails if the node never reaches TARGET (#4496)" "$rc" "1"

# Restore the streak-fallback stubs / state for any later cases.
ANNOUNCE_TARGET_VERSION=""
node_version() { printf ''; }
read_room_state() {
    READ_CALLS=$((READ_CALLS + 1))
    (( READ_CALLS > READ_FAIL_COUNT ))
}

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

# ── verify_converged() ───────────────────────────────────────────────
#
# The post-send convergence check, the PR's SECOND silent-drop defense.
# riverctl exits 0 even when the room contract silently drops the delta, so
# the script re-reads room state and confirms the message text is present.
# verify_converged is extracted verbatim; read_room_messages (the riverctl
# `message list` GET) is stubbed so the test is hermetic.
#
# The load-bearing property: verify_converged must CAPTURE the listing into a
# variable before grepping, NOT pipe `read_room_messages | grep -qF`. Under
# `set -o pipefail`, a piped `grep -q` exits on first match and SIGPIPEs the
# still-writing producer (exit 141), so pipefail would report a genuinely
# converged message as a FAILURE — the inverse of the bug being fixed. The
# large-output test below pins that: the match is EARLY in a big listing, so a
# piped implementation gets SIGPIPE (rc=141) when grep exits before the
# producer finishes, while capture-then-grep returns 0. (A last-line match
# would NOT trigger it — grep must read all output — which is why the producer
# emits the match first, then a large tail.)
# shellcheck disable=SC2317  # stubs below are called indirectly via eval'd verify_converged
eval "$(awk '/^verify_converged\(\) \{/,/^}/' "$ANNOUNCE_SH")"

# Re-assert pipefail is on (matches the real script's `set -euo pipefail`), so
# the SIGPIPE-resistance test below is meaningful.
set -o pipefail

MESSAGE="Freenet v0.2.75 released: https://example/v0.2.75"

# Message present EARLY, followed by a large tail. A piped
# read_room_messages|grep -qF would SIGPIPE the still-writing producer (rc=141)
# under pipefail and report failure despite the match. Capture-then-grep
# returns 0. Verified: against a piped impl this assertion fails (rc=141);
# against capture-then-grep it passes.
read_room_messages() {
    echo "[15:23:44 - Room Owner]: $MESSAGE"
    for i in $(seq 1 500000); do echo "[old line $i]: filler chatter to overrun the 64KB pipe buffer"; done
}
rc=0
verify_converged || rc=$?
check "verify_converged() returns 0 when present early in large output (no SIGPIPE false-failure)" "$rc" "0"

# Message absent: the delta was silently dropped — must fail (exit nonzero).
read_room_messages() {
    echo "[15:00:00 - Room Owner]: Freenet v0.2.74 released: https://example/v0.2.74"
}
rc=0
verify_converged || rc=$?
check "verify_converged() returns nonzero when the message is absent" "$rc" "1"

# riverctl read itself fails (empty output): must be treated as not-converged.
read_room_messages() { return 1; }
rc=0
verify_converged || rc=$?
check "verify_converged() returns nonzero when the room read fails" "$rc" "1"

# grep -F literalness: a message with regex metacharacters / leading dash is
# matched literally (the `--` and `-F` are load-bearing).
MESSAGE="-n release [v1.2.3] (50%) a.b*c"
read_room_messages() { echo "noise"; echo "x $MESSAGE y"; }
rc=0
verify_converged || rc=$?
check "verify_converged() matches messages with regex/dash metacharacters literally" "$rc" "0"

# ── verify_converged_with_retry() ────────────────────────────────────
#
# The post-send verify must POLL, not read once: cross-node UPDATE convergence
# is ~6-7s and a concurrent gateway restart can delay the first readable read
# further, so a single immediate read false-negatives a message that DID land
# (the v0.2.96 "silent drop" that was actually converged). This wrapper re-reads
# up to VERIFY_ATTEMPTS times and only declares a drop after the budget.
#
# Extracted verbatim (depends on verify_converged, extracted above). The stubs
# key on the loop's VERIFY_ATTEMPT global — read-only in the command-substitution
# subshell, so no counter file needed. VERIFY_INTERVAL=0 keeps the tests instant.
# shellcheck disable=SC2317  # stubs are called indirectly via eval'd functions
eval "$(awk '/^verify_converged_with_retry\(\) \{/,/^}/' "$ANNOUNCE_SH")"
VERIFY_INTERVAL=0

MESSAGE="Freenet v0.2.96 released: https://example/v0.2.96"

# Present immediately: succeeds on attempt 1.
VERIFY_ATTEMPTS=5
read_room_messages() { echo "[t - Room Owner]: $MESSAGE"; }
rc=0; verify_converged_with_retry || rc=$?
check "verify_converged_with_retry() returns 0 when present on the first read" "$rc" "0"
check "verify_converged_with_retry() records attempt 1 when present immediately" "$VERIFY_ATTEMPT" "1"

# THE FIX: still propagating on the first two reads, converges on attempt 3 —
# a single-shot verify would have wrongly reported a silent drop here.
VERIFY_ATTEMPTS=5
read_room_messages() {
    if ((VERIFY_ATTEMPT >= 3)); then
        echo "[t - Room Owner]: $MESSAGE"
    else
        echo "[t]: still propagating, message not converged yet"
    fi
}
rc=0; verify_converged_with_retry || rc=$?
check "verify_converged_with_retry() converges after transient misses (no false silent-drop)" "$rc" "0"
check "verify_converged_with_retry() succeeds on the attempt the message first appears" "$VERIFY_ATTEMPT" "3"

# Genuine silent drop: never converges within the budget → still fails, so a
# real drop is not masked by the retry.
VERIFY_ATTEMPTS=4
read_room_messages() { echo "[t]: unrelated chatter only, message never lands"; }
rc=0; verify_converged_with_retry || rc=$?
check "verify_converged_with_retry() returns nonzero when the message never converges (real drop)" "$rc" "1"

# ── send_message_checked() exit-code preservation ────────────────────
#
# Regression for the Codex finding: the success path originally used
# `if ! post_message`, under which `set -e` resets `$?` to 0 inside the
# if-body, so a real riverctl failure was logged/exited as rc=0 — masking the
# failed announcement and skipping verification. The fix wraps the send in
# send_message_checked(), which captures the code with `post_message || rc=$?`
# and returns it. send_message_checked is extracted VERBATIM here, so reverting
# it to `if ! post_message` (which yields rc=0 on failure) fails this assertion.
eval "$(awk '/^send_message_checked\(\) \{/,/^}/' "$ANNOUNCE_SH")"

# riverctl send fails: send_message_checked must return riverctl's real code.
post_message() { return 7; }
rc=0
send_message_checked 2>/dev/null || rc=$?
check "send_message_checked() preserves a send failure code (not masked to 0)" "$rc" "7"

# riverctl send succeeds: send_message_checked returns 0.
post_message() { return 0; }
rc=0
send_message_checked 2>/dev/null || rc=$?
check "send_message_checked() returns 0 on a successful send" "$rc" "0"

# ── parse_args(): --target-version flag + positional message (#4496) ──
#
# The version-aware readiness gate hinges on ANNOUNCE_TARGET_VERSION being
# populated from the `--target-version <v>` flag the release-agent passes.
# These drive the REAL extracted parser (not a copy), covering order-
# tolerance, the leading-`v` strip, the `--` separator, and the two error
# branches. `usage` exits 1 in production; the tests stub it to exit 42 so a
# malformed invocation is observable as that code from a subshell without
# printing the usage banner.

# Flag BEFORE the message.
MESSAGE=""; ANNOUNCE_TARGET_VERSION=""
parse_args --target-version 0.2.90 "hello world"
check "parse_args: flag before message sets the message" "$MESSAGE" "hello world"
check "parse_args: flag before message sets the target version" "$ANNOUNCE_TARGET_VERSION" "0.2.90"

# Flag AFTER the message (order-tolerance).
MESSAGE=""; ANNOUNCE_TARGET_VERSION=""
parse_args "second msg" --target-version 0.2.91
check "parse_args: flag after message sets the message" "$MESSAGE" "second msg"
check "parse_args: flag after message sets the target version" "$ANNOUNCE_TARGET_VERSION" "0.2.91"

# Leading `v` is stripped (the agent may send `v0.2.92`).
MESSAGE=""; ANNOUNCE_TARGET_VERSION=""
parse_args --target-version v0.2.92 "msg"
check "parse_args: leading 'v' is stripped from the target version" "$ANNOUNCE_TARGET_VERSION" "0.2.92"

# No flag at all: message set, target version left empty (fallback path).
MESSAGE=""; ANNOUNCE_TARGET_VERSION=""
parse_args "plain message"
check "parse_args: bare message leaves target version empty" "$ANNOUNCE_TARGET_VERSION" ""
check "parse_args: bare message is captured" "$MESSAGE" "plain message"

# `--` separator: the following token is the message even if flag-shaped.
MESSAGE=""; ANNOUNCE_TARGET_VERSION=""
parse_args --target-version 0.2.93 -- "--looks-like-a-flag"
check "parse_args: -- lets a flag-shaped message through" "$MESSAGE" "--looks-like-a-flag"

# Error branch: --target-version with no argument → usage (exit).
rc=0
( usage() { exit 42; }; parse_args --target-version ) >/dev/null 2>&1 || rc=$?
check "parse_args: --target-version without an argument exits via usage" "$rc" "42"

# Error branch: a second positional → usage (exit).
rc=0
( usage() { exit 42; }; parse_args "first" "second" ) >/dev/null 2>&1 || rc=$?
check "parse_args: unexpected extra positional exits via usage" "$rc" "42"

# ── result ───────────────────────────────────────────────────────────

if (( FAILURES > 0 )); then
    echo "$FAILURES announce-to-river.sh test(s) failed" >&2
    exit 1
fi
echo "All announce-to-river.sh tests passed."
