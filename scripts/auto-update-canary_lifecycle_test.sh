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
#   3. The gate could go green with no evidence (#5236). After the "check
#      started" line it slept a flat 5s and killed the node -- but that line is
#      logged BEFORE the network request, which production bounds at 10s
#      (PROBE_CHAIN_TIMEOUT). Any GitHub answer in the 5-10s band was killed
#      before it could log ANY outcome, and "no parse error in the log" was
#      then read as "parsing works". A binary with the exact #5221 bug would
#      have passed Gate A and shipped. Cases 5 and 6 pin it: 5 is the outcome
#      that arrives late (verified to fail against the pre-fix script), 6 is
#      the outcome that never arrives.
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
COMPLETE_LINE='INFO freenet: Startup update check complete: staying on the current version current="0.2.122"'
LATEST_SEEN_LINE='INFO freenet::commands::auto_update: Startup update check: GitHub reports latest release latest=0.2.121'

# Pin what the node is expected to have compared against, so cmd_preflight does
# not reach GitHub from a test.
#
# Safe HERE, and only here: the node is a synthetic fixture and this file
# chooses BOTH sides of the comparison on purpose. It is not safe on the
# release path -- a pinned value that happens to agree with a silently-wrong
# comparator makes the equality check confirm the wrong answer rather than
# catch it. (An earlier version of this comment said a pinned value "can only
# make the check FAIL". That is true of skipping it, not of passing it.)
# `release_canary_wiring_test.sh` pins that the workflow leaves it unset.
#
# Skipping is what a pinned value cannot do: that needs an EMPTY value, which
# cmd_preflight treats as unset and then resolves or refuses. Cases that fail
# earlier (parse failure, no outcome) never reach the check at all.
export CANARY_EXPECTED_LATEST=0.2.121

# make_fake_node <path> <exit-code> <extra-log-line> [linger-seconds] [extra-delay-seconds]
#
# Writes a stand-in for the freenet binary: it answers --version, parses just
# enough of the real flag set to find --log-dir, writes the log lines a real
# node would, then lingers before exiting with the requested code.
#
# `extra-delay-seconds` is the gap between the "check started" line and the
# outcome line. It models the only variable a real node has here: how long
# GitHub takes to answer. Production bounds that at PROBE_CHAIN_TIMEOUT (10s),
# and the whole of case 5 is a value inside that bound.
make_fake_node() {
    local path="$1" exit_code="$2" extra="$3" linger="${4:-0}" delay="${5:-0}"
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
echo "2026-08-08T02:00:00.000000Z  $CHECK_LINE" >> "\$logdir/freenet.2026-08-08-02.log"
if [ -n "$extra" ]; then
    sleep $delay
    echo "2026-08-08T02:00:00.100000Z  $extra" >> "\$logdir/freenet.2026-08-08-02.log"
fi
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
# Emits the observed-latest line as well as the completion line: a real
# post-#5236 healthy node logs both, and Gate A now requires both.
make_fake_node "$FAKE_OK" 0 "$LATEST_SEEN_LINE
2026-08-08T02:00:00.200000Z  $COMPLETE_LINE" 0
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
# 2b. A SILENTLY WRONG comparator must fail the gate too (#5236 finding 32).
#
#     This node does everything right except the one thing that matters: it
#     compares against the wrong release. It does not fail to parse, it does
#     not fail to fetch, it runs to completion -- so every assertion the canary
#     had before this change is satisfied and Gate A reported OK. Driven
#     through cmd_preflight rather than assert_detection_healthy so the
#     resolve/export wiring is exercised, not just the comparison.
# ---------------------------------------------------------------------------
FAKE_WRONG="$TMPROOT/fake-wrong-release"
make_fake_node "$FAKE_WRONG" 0 \
    'INFO freenet::commands::auto_update: Startup update check: GitHub reports latest release latest=0.2.1
2026-08-08T02:00:00.200000Z  '"$COMPLETE_LINE" 0
WRONG_OUT="$(cmd_preflight "$FAKE_WRONG" 2>&1)"
WRONG_RC=$?
if [ "$WRONG_RC" -eq 0 ]; then
    bad "cmd_preflight returned OK for a node that compared against the WRONG release (0.2.1 vs 0.2.121) -- the silently-wrong-comparator hole is open"
# Glob match, not `printf … | grep -qF`: that pipeline's status is 141 under
# `pipefail` once the producer has more than a pipe buffer left to write when
# `grep -q` short-circuits, so a diagnosis that IS present reads as absent and
# this branch reports "the wrong diagnosis" for the right one. cmd_preflight's
# output is small today, so this is latent rather than live -- which is exactly
# how the same defect survived in assert_detection_healthy until a real 3.65 MB
# node log hit it. See .claude/rules/bug-prevention-patterns.md.
elif [[ "$WRONG_OUT" == *"compared against the WRONG release"* ]]; then
    ok "cmd_preflight fails a node that compared against the wrong release, with the right diagnosis"
else
    bad "cmd_preflight failed the wrong-release node but with the wrong diagnosis: $WRONG_OUT"
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
make_fake_node "$FAKE_LONG" 0 "$COMPLETE_LINE" 30   # lingers well past the gate's return
WORKL="$TMPROOT/workleak"
mkdir -p "$WORKL"
LEAK_T0=$(date +%s)
run_node_until_check "$FAKE_LONG" "$WORKL" >/dev/null 2>&1
sleep 2
LEAK_ELAPSED=$(( $(date +%s) - LEAK_T0 ))
if pgrep -f "$MARKER" >/dev/null 2>&1; then
    bad "a node survived run_node_until_check (process-group regression); leftovers:"
    pgrep -af "$MARKER" >&2
    pkill -f "$MARKER" 2>/dev/null
elif [[ "$LEAK_ELAPSED" -ge "$CANARY_TIMEOUT_SECS" ]]; then
    # The fake is launched under `timeout $CANARY_TIMEOUT_SECS` (canary
    # script's run_node_until_check). Past that point it is dead whether or not
    # the process-group cleanup works, so "no survivors" stops being evidence
    # and this case silently proves nothing. Fail rather than report a pass we
    # did not earn: on a loaded runner this is exactly how a re-introduced leak
    # would go unnoticed.
    bad "case 4 was VACUOUS: ${LEAK_ELAPSED}s elapsed >= CANARY_TIMEOUT_SECS (${CANARY_TIMEOUT_SECS}s), so the fake was reaped by its own timeout rather than by the cleanup under test"
else
    ok "no node survives run_node_until_check (checked ${LEAK_ELAPSED}s in, well inside the ${CANARY_TIMEOUT_SECS}s timeout)"
fi

# ---------------------------------------------------------------------------
# 5. THE VACUOUS PASS (#5236). A broken updater whose outcome lands more than
#    five seconds after the "check started" line must still FAIL the gate.
#
#    The gate used to `sleep 5` after that line and then kill the node. The
#    line is logged BEFORE the network request, and the request is bounded by
#    PROBE_CHAIN_TIMEOUT = 10s, so any GitHub answer between 5s and 10s --
#    loaded runner, redirect hop, mild rate-limit backoff -- was SIGTERMed
#    before it could log success, a parse failure, or a fetch failure. The
#    assertion then saw "check ran, no parse error" and returned OK. A binary
#    carrying the exact #5221 bug this canary exists to catch would have
#    passed Gate A and shipped.
#
#    The fake below is that binary: it logs the parse failure 8s after the
#    check line -- inside what production allows, outside what the gate used
#    to watch. Verified to FAIL against the pre-fix script (which returned 0,
#    reporting a broken updater as healthy) and to pass after.
#
#    Asserting on the DIAGNOSIS, not just the exit code: the point is that the
#    canary now SEES the parse failure, not merely that it stopped saying OK.
# ---------------------------------------------------------------------------
FAKE_SLOW="$TMPROOT/fake-slow-parsefail"
make_fake_node "$FAKE_SLOW" 0 "$PARSE_FAIL_LINE" 0 8
WAS_TIMEOUT=$CANARY_TIMEOUT_SECS
WAS_WAIT=${CANARY_OUTCOME_WAIT_SECS:-20}
CANARY_TIMEOUT_SECS=40
CANARY_OUTCOME_WAIT_SECS=25
SLOW_OUT="$(cmd_preflight "$FAKE_SLOW" 2>&1)"
SLOW_RC=$?
CANARY_TIMEOUT_SECS=$WAS_TIMEOUT
CANARY_OUTCOME_WAIT_SECS=$WAS_WAIT
if [[ "$SLOW_RC" -eq 0 ]]; then
    bad "cmd_preflight returned OK for a binary whose updater FAILED TO PARSE, because the failure was logged 8s after the check started (the #5236 vacuous pass)"
elif [[ "$SLOW_OUT" != *"could not parse the version GitHub returned"* ]]; then
    bad "cmd_preflight failed the slow parse-failure binary but did not name the parse failure; got: $SLOW_OUT"
else
    ok "a parse failure logged 8s after the check still fails the gate, with the right diagnosis"
fi

# ---------------------------------------------------------------------------
# 6. NO OUTCOME AT ALL must not read as OK either.
#
#    Case 5 covers the outcome that arrives late. This covers the one that
#    never arrives -- a check wedged in its request, a node killed for any
#    other reason. There is nothing to detect here and the gate must not
#    pretend otherwise: it reports UNVERIFIED and refuses, rather than
#    reporting a pass it has no evidence for.
#
#    Short outcome budget on purpose: what is under test is the verdict for a
#    log that never settles, not how long the canary is willing to wait.
# ---------------------------------------------------------------------------
FAKE_SILENT="$TMPROOT/fake-silent"
make_fake_node "$FAKE_SILENT" 0 "" 20   # logs the check line and nothing else
WAS_WAIT=${CANARY_OUTCOME_WAIT_SECS:-20}
CANARY_OUTCOME_WAIT_SECS=4
if cmd_preflight "$FAKE_SILENT" >/dev/null 2>&1; then
    bad "cmd_preflight returned OK for a node that logged NO outcome -- absence of an answer is being read as success (#5236)"
else
    ok "a check that never logs an outcome is UNVERIFIED, not OK"
fi
CANARY_OUTCOME_WAIT_SECS=$WAS_WAIT

# ---------------------------------------------------------------------------
# 7. A PORT COLLISION must be diagnosed as one, not as an auto-update fault.
#
#    Reproduced before the fix: two `preflight` runs started 2s apart, the
#    second reporting "the startup update check never ran". Exit 43 is
#    EXIT_CODE_ALREADY_RUNNING -- the node found its WS port occupied and died
#    before the update task existed. `assert_detection_healthy` never consults
#    NODE_EXIT, so the log assertion is the only thing that spoke, and it named
#    the wrong subsystem on a release someone was waiting for.
#
#    Both halves are asserted, because the diagnosis is the point: the message
#    must name the collision AND must no longer claim the update check never
#    ran. Only the second of those was wrong before; a fix that added the new
#    wording while leaving the old would still send the reader to the wrong
#    place.
# ---------------------------------------------------------------------------
FAKE_43="$TMPROOT/fake-port-collision"
cat > "$FAKE_43" <<'FAKE43'
#!/usr/bin/env bash
if [ "${1:-}" = "--version" ]; then
    echo "Freenet version: 0.2.122 (deadbeefcafe)"
    exit 0
fi
logdir=""
while [ $# -gt 0 ]; do
    case "$1" in
        --log-dir) logdir="$2"; shift 2 ;;
        *) shift ;;
    esac
done
mkdir -p "$logdir"
# A real node dies here BEFORE the update task is spawned, but the tracer is
# already up -- so the log dir is non-empty and the "no logs at all" branch is
# not the one that fires. That is what makes this land on the update-check
# branch and get misdiagnosed.
echo "2026-08-08T02:00:00.000000Z  INFO freenet: another instance is already running" \
    >> "$logdir/freenet.2026-08-08-02.log"
exit 43
FAKE43
chmod +x "$FAKE_43"
COLLIDE_OUT="$(cmd_preflight "$FAKE_43" 2>&1)"
if [[ "$COLLIDE_OUT" != *"port collision"* ]]; then
    bad "cmd_preflight did not diagnose exit 43 as a port collision; got: $COLLIDE_OUT"
elif [[ "$COLLIDE_OUT" == *"the startup update check never ran"* ]]; then
    bad "cmd_preflight still reports a port collision as 'the startup update check never ran' -- the misdiagnosis is back"
else
    ok "exit 43 is diagnosed as a port collision, not as an auto-update fault"
fi

# ---------------------------------------------------------------------------
# 8. A BLOCKING gate must leave the node's own output behind.
#
#    `cleanup` rm -rf's the workdir on EXIT, and the two branches likeliest to
#    fire on a healthy release -- "the check never ran" and "started but never
#    logged an outcome" -- printed nothing from the node, unlike the parse-fail
#    and fetch-fail branches. So a real blocking run left no evidence at all,
#    while docs/RELEASING.md told the operator to "read the job log; it names
#    the offending line".
#
#    Driven through the "check never ran" branch specifically, because that is
#    one of the two that printed nothing: a fixture whose node logs something
#    identifiable but never the check line. Asserting the node's OWN line comes
#    back is what distinguishes a real dump from a header that says "evidence".
# ---------------------------------------------------------------------------
FAKE_NOCHECK="$TMPROOT/fake-no-check-line"
cat > "$FAKE_NOCHECK" <<'FAKENC'
#!/usr/bin/env bash
if [ "${1:-}" = "--version" ]; then
    echo "Freenet version: 0.2.122 (deadbeefcafe)"
    exit 0
fi
logdir=""
while [ $# -gt 0 ]; do
    case "$1" in
        --log-dir) logdir="$2"; shift 2 ;;
        *) shift ;;
    esac
done
mkdir -p "$logdir"
echo "2026-08-08T02:00:00.000000Z  INFO freenet: DISTINCTIVE_STARTUP_EVIDENCE_LINE" \
    >> "$logdir/freenet.2026-08-08-02.log"
sleep 1
exit 0
FAKENC
chmod +x "$FAKE_NOCHECK"
EVIDENCE_OUT="$(cmd_preflight "$FAKE_NOCHECK" 2>&1)"
if [[ "$EVIDENCE_OUT" != *"canary node evidence"* ]]; then
    bad "a blocking gate produced no evidence block; got: $EVIDENCE_OUT"
elif [[ "$EVIDENCE_OUT" != *"DISTINCTIVE_STARTUP_EVIDENCE_LINE"* ]]; then
    bad "the evidence block is present but does not contain the node's own log output; got: $EVIDENCE_OUT"
else
    ok "a blocking gate dumps the node's own log before the workdir is deleted"
fi

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All auto-update-canary lifecycle assertions passed."
else
    echo "$FAILURES lifecycle assertion(s) FAILED." >&2
    exit 1
fi
