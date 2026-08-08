#!/usr/bin/env bash
# Lifecycle regression tests for auto-update-canary.sh (#5222).
#
# auto-update-canary_test.sh covers the PURE functions (the two-sided log
# assertion, the update-trigger detector, the source pins). This file covers
# the part that actually runs a process: `run_node_until_check` and the
# `cmd_preflight` workdir/trap lifecycle. Both had real bugs that the pure
# tests could not see, which is why this file exists:
#
#   1. An early draft used `trap 'rm -rf "$work"' RETURN` against a `local`,
#      and under `set -u` EVERY gate returned 1 -- including a perfectly
#      healthy binary. A canary that fails on success is worse than no canary:
#      the first person to hit it learns to override the gate, and then it
#      never catches anything real. That specific bug was caught before it
#      reached a commit, so there is no commit to pin against and this file
#      does NOT claim to reproduce it. Case 1 guards the broader class it
#      belongs to -- a gate that can only ever go red -- which is checkable:
#      forcing the gate to fail makes case 1 fail (verified).
#
#   2. The node was left running after the gate returned. `kill $node_pid`
#      reaped only the subshell; `set -m` alone did not help because job
#      control is inherited, so the `timeout` inside started a process group of
#      its OWN. Every run leaked a node that held its ports and burned CPU,
#      which is what made a later attempt's node boot too slowly to log its
#      update check inside the window -- surfacing as a false "the startup
#      update check never ran" on a HEALTHY binary. Case 4 pins this one
#      directly: reintroducing the bug makes it fail (verified).
#
# Instead of booting a real Freenet node (slow, needs network, non-deterministic
# jitter), these drive the real functions against a FAKE node binary that emits
# the same log lines. The functions under test are the real ones, sourced.
#
# Run manually: bash scripts/auto-update-canary_lifecycle_test.sh
# Also wired into CI (the Fmt job in .github/workflows/ci.yml).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CANARY_SH="$SCRIPT_DIR/auto-update-canary.sh"

# Small budgets: the fake node logs within a second or two, so there is nothing
# to wait for. Must be exported BEFORE sourcing, since the script reads them at
# load time.
export CANARY_TIMEOUT_SECS=15
export CANARY_ATTEMPTS=1
export CANARY_RETRY_SLEEP=1
export CANARY_NETWORK_PORT=39901
export CANARY_WS_PORT=39902

# shellcheck source=scripts/auto-update-canary.sh
source "$CANARY_SH"

FAILURES=0
TMPROOT="$(mktemp -d)"
trap 'rm -rf "$TMPROOT"; cleanup' EXIT

CHECK_LINE='INFO freenet: Startup update check against GitHub current="0.2.122" jitter_secs=1'
PARSE_FAIL_LINE="WARN freenet::commands::auto_update: Startup update check: failed to parse latest version 'v0.2.123': unexpected character 'v' while parsing major version number"
TRIGGER_LINE='INFO freenet: Startup check: newer version on GitHub, triggering auto-update new_version=0.2.123'

# make_fake_node <path> <exit-code> <extra-log-line> [linger-seconds]
#
# Writes a stand-in for the freenet binary: it answers --version, parses just
# enough of the real flag set to find --log-dir, writes the log lines a real
# node would, then lingers before exiting with the requested code.
make_fake_node() {
    local path="$1" exit_code="$2" extra="$3" linger="${4:-0}"
    cat > "$path" <<FAKE
#!/usr/bin/env bash
if [ "\${1:-}" = "--version" ]; then
    echo "Freenet version: 0.2.122 (deadbeefcafe)"
    exit 0
fi
logdir=""
while [ \$# -gt 0 ]; do
    case "\$1" in
        --log-dir) logdir="\$2"; shift 2 ;;
        *) shift ;;
    esac
done
mkdir -p "\$logdir"
sleep 1
{
    echo "2026-08-08T02:00:00.000000Z  $CHECK_LINE"
    [ -n "$extra" ] && echo "2026-08-08T02:00:00.100000Z  $extra"
} >> "\$logdir/freenet.2026-08-08-02.log"
sleep $linger
exit $exit_code
FAKE
    chmod +x "$path"
}

ok()   { echo "ok   - $1"; }
bad()  { echo "FAIL - $1" >&2; FAILURES=$((FAILURES + 1)); }

# ---------------------------------------------------------------------------
# 1. A HEALTHY binary must make cmd_preflight exit 0.
#
#    The "can this gate ever go green?" side. A gate that fails on every input
#    would block the first release that ran it, and is indistinguishable from a
#    working one until that happens. Paired with case 2 below, which is the
#    "can it ever go red?" side -- neither is worth much alone.
# ---------------------------------------------------------------------------
FAKE_OK="$TMPROOT/fake-healthy"
make_fake_node "$FAKE_OK" 0 "" 0
if cmd_preflight "$FAKE_OK" >/dev/null 2>&1; then
    ok "cmd_preflight returns 0 for a healthy binary"
else
    bad "cmd_preflight returned non-zero for a HEALTHY binary -- the gate rejects everything"
fi

# ---------------------------------------------------------------------------
# 2. A binary whose updater cannot parse the tag must FAIL the gate.
#    Pairs with case 1: together they show the gate discriminates rather than
#    always-passing or always-failing.
# ---------------------------------------------------------------------------
FAKE_BAD="$TMPROOT/fake-parsefail"
make_fake_node "$FAKE_BAD" 0 "$PARSE_FAIL_LINE" 0
if cmd_preflight "$FAKE_BAD" >/dev/null 2>&1; then
    bad "cmd_preflight returned 0 for a binary with a BROKEN updater"
else
    ok "cmd_preflight fails a binary whose updater cannot parse the tag"
fi

# ---------------------------------------------------------------------------
# 3. NODE_EXIT must carry the node's OWN exit code when it exits by itself.
#    Gate B asserts exit 42; if the harness overwrites that with its own SIGTERM
#    (143) the assertion silently stops meaning anything.
# ---------------------------------------------------------------------------
FAKE_42="$TMPROOT/fake-exit42"
make_fake_node "$FAKE_42" 42 "$TRIGGER_LINE" 0
WORK42="$TMPROOT/work42"
mkdir -p "$WORK42"
run_node_until_check "$FAKE_42" "$WORK42" >/dev/null 2>&1
if [ "$NODE_EXIT" = "42" ]; then
    ok "NODE_EXIT preserves the node's own exit 42"
else
    bad "NODE_EXIT was '$NODE_EXIT', expected 42 (the harness clobbered the real exit code)"
fi

# ---------------------------------------------------------------------------
# 4. No node may outlive run_node_until_check.
#
#    The process-group regression. A leaked node holds its ports and burns CPU,
#    which is what made a later attempt's node boot too slowly to log its check
#    in time -- reported as "the startup update check never ran" on a HEALTHY
#    binary. Uses a marker in the fake's path so the search cannot match this
#    test's own shell (the `pgrep -f` self-match trap).
# ---------------------------------------------------------------------------
MARKER="canaryleak$$"
FAKE_LONG="$TMPROOT/$MARKER"
make_fake_node "$FAKE_LONG" 0 "" 30   # lingers well past the gate's return
WORKL="$TMPROOT/workleak"
mkdir -p "$WORKL"
run_node_until_check "$FAKE_LONG" "$WORKL" >/dev/null 2>&1
sleep 2
if pgrep -f "$MARKER" >/dev/null 2>&1; then
    bad "a node survived run_node_until_check (process-group regression); leftovers:"
    pgrep -af "$MARKER" >&2
    pkill -f "$MARKER" 2>/dev/null
else
    ok "no node survives run_node_until_check"
fi

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All auto-update-canary lifecycle assertions passed."
else
    echo "$FAILURES lifecycle assertion(s) FAILED." >&2
    exit 1
fi
