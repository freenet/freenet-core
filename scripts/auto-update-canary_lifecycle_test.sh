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
# `stderr-line` (arg 6) models an `eprintln!` rather than a `tracing` event, and
# the distinction is the whole reason it exists: the node's two fatal-abort
# CRITICAL lines are `eprintln!`, so they land in `node.out` (via the subshell's
# `2>&1` in run_node_until_check) and NEVER in the log dir. The exit-42
# classification reads them from there, and until this parameter existed that
# premise was asserted only by inspection -- every behavioural case wrote
# `node.out` itself through the stub.
make_fake_node() {
    local path="$1" exit_code="$2" extra="$3" linger="${4:-0}" delay="${5:-0}"
    local stderr_line="${6:-}"
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
if [ -n "$stderr_line" ]; then
    # To STDERR, exactly as the node's eprintln! does. run_node_until_check
    # redirects the node's stdout AND stderr into node.out; if that redirect were
    # ever split or dropped, this line would stop arriving and the fatal-abort
    # classification would silently turn every environmental 42 into a hard block.
    echo "$stderr_line" >&2
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
# 2c. Exit 42 is OVERLOADED, and the discrimination must work through the REAL
#     harness -- not just through a stub that writes node.out itself.
#
#     `FATAL_LISTENER_EXIT_CODE` is also 42 (crates/core/src/node/p2p_impl.rs),
#     and both of its producers `eprintln!` a CRITICAL line before exiting. The
#     canary treats "42 + CRITICAL" as environmental (retryable) and "42 without
#     it" as a real downgrade bug, so the whole distinction rests on a premise
#     nothing was testing: that the node's STDERR reaches `node.out`.
#
#     auto-update-canary_test.sh cannot test that. Its stub writes node.out
#     directly, so it asserts the classification while ASSUMING the plumbing --
#     the same shape as the retry-attempts gap found in review, and as the #5271
#     fixture that wrapped a real log string in a type the system cannot emit:
#     the assertion was fine, the fixture could not produce the fault.
#
#     Here the fake node writes the line to fd 2 and the REAL run_node_until_check
#     captures it. A refactor that splits node stderr into its own file, or drops
#     the `2>&1`, turns every environmental 42 into a hard block on a healthy
#     release -- and would leave the other suite entirely green.
#
#     CANARY_ATTEMPTS is 1 here, so the environmental verdict exhausts the budget
#     and the gate returns non-zero; what is asserted is the DIAGNOSIS, which is
#     what differs between the two branches.
# ---------------------------------------------------------------------------
FAKE_FATAL42="$TMPROOT/fake-fatal-abort-42"
make_fake_node "$FAKE_FATAL42" 42 "$LATEST_SEEN_LINE
2026-08-08T02:00:00.200000Z  $COMPLETE_LINE" 0 0 \
    "CRITICAL: Network event listener exited (fatal): transport error: connection reset by peer"
FATAL42_OUT="$(cmd_preflight "$FAKE_FATAL42" 2>&1)"
FATAL42_RC=$?
if [ "$FATAL42_RC" -eq 0 ]; then
    bad "cmd_preflight returned OK for a node that exited 42 -- the exit-code observer is not running at all"
elif [[ "$FATAL42_OUT" == *"CRITICAL fatal abort"* ]]; then
    ok "a fatal-abort CRITICAL on the node's STDERR reaches node.out and is classified environmental (real harness)"
else
    bad "cmd_preflight blocked on exit 42 without seeing the fatal-abort CRITICAL the node printed to stderr. Either run_node_until_check no longer captures the node's stderr into node.out, or the marker drifted -- every environmental 42 is now a hard block on a HEALTHY release, blaming compare_versions_for_startup. Got: $FATAL42_OUT"
fi

# ...and the discriminating twin, through the same real harness: exit 42 with NO
# CRITICAL line is a genuine downgrade bug and must be named as one. Without this
# the case above is satisfied by a classifier that calls everything environmental.
FAKE_BARE42="$TMPROOT/fake-bare-exit-42"
make_fake_node "$FAKE_BARE42" 42 "$LATEST_SEEN_LINE
2026-08-08T02:00:00.200000Z  $COMPLETE_LINE" 0 0
BARE42_OUT="$(cmd_preflight "$FAKE_BARE42" 2>&1)"
BARE42_RC=$?
if [ "$BARE42_RC" -eq 0 ]; then
    bad "cmd_preflight returned OK for a node that exited 42 with a clean log and no fatal-abort line -- that is a self-downgrade request and must block"
elif [[ "$BARE42_OUT" == *"downgrade-and-restart loop"* ]]; then
    ok "exit 42 with no fatal-abort line is blocked as a self-downgrade (real harness)"
else
    bad "cmd_preflight blocked the bare exit-42 node but with the wrong diagnosis; got: $BARE42_OUT"
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

# ---------------------------------------------------------------------------
# 9. The canary must not let its own TMPDIR reach the node.
#
#    This is the #5290 fault, and it blocked v0.2.124 on a binary that was
#    perfectly healthy.
#
#    #5291 has since deleted the mkdir, so a node built from current main
#    cannot fail this way. The case stays, and the fake node below deliberately
#    keeps modelling the OLD behaviour, because the canary gates RELEASED
#    binaries: every release through v0.2.124 still panics like this, and the
#    canary has to be able to run them.
#
#    `client_api.rs` (through v0.2.124) unconditionally `create_dir_all`s
#    `std::env::temp_dir()/freenet/webs` when it builds the router and PANICS
#    (exit 101) if it cannot -- and `cross-compile.yml` stages the binary it is
#    about to gate at `/tmp/freenet`, which is exactly the path that mkdir
#    needs to be a directory. ENOTDIR, dead node, and Gate A reports "the
#    startup update check never ran" of a product that was fine.
#
#    The fake node here IS the regular file staged at `$TMPDIR/freenet`, which
#    is what makes this a real test rather than a story about one: the ENOTDIR
#    comes from the kernel refusing to mkdir under a file, not from a fixture
#    that prints a panic message on cue.
#
#    This case catches the isolation being ABSENT; case 10 catches it being
#    WRONG. Not a redundant pair, and mutation testing is what separated them:
#    rewriting the export to `TMPDIR=/tmp` leaves THIS case green on any host
#    where `/tmp/freenet` happens to be a usable directory (it is one on nova),
#    because the node's mkdir then succeeds somewhere useless rather than
#    failing. Case 10 reads the value the node actually got, so it fails on
#    that mutation unconditionally. Deleting the export fails both.
#
#    A source scrape in auto-update-canary_test.sh also pins `export TMPDIR`,
#    kept for the ordering it checks (before `exec`, before `freenet update`)
#    and for the Gate B half. It asserts TEXT, so it is green on BOTH mutations
#    above -- verified, not assumed.
# ---------------------------------------------------------------------------
CITMP="$TMPROOT/citmp"
mkdir -p "$CITMP"
FAKE_STAGED="$CITMP/freenet"
cat > "$FAKE_STAGED" <<FAKESTAGED
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
# Models client_api.rs THROUGH v0.2.124 (the mkdir was deleted in #5291):
# hardwired to temp_dir(), does NOT follow --data-dir, and panics rather than
# degrading when the directory cannot be created. Kept as-is on purpose -- this
# canary gates released binaries, which still behave this way.
webdir="\${TMPDIR:-/tmp}/freenet/webs"
if ! mkdir -p "\$webdir" 2>/dev/null; then
    echo "thread 'main' panicked at crates/core/src/server/client_api.rs:256:13:" >&2
    echo "Failed to create contract web directory at \$webdir: Not a directory (os error 20)" >&2
    exit 101
fi
sleep 1
echo "2026-08-08T02:00:00.000000Z  $CHECK_LINE" >> "\$logdir/freenet.2026-08-08-02.log"
echo "2026-08-08T02:00:00.100000Z  $LATEST_SEEN_LINE" >> "\$logdir/freenet.2026-08-08-02.log"
echo "2026-08-08T02:00:00.200000Z  $COMPLETE_LINE" >> "\$logdir/freenet.2026-08-08-02.log"
exit 0
FAKESTAGED
chmod +x "$FAKE_STAGED"

# Save and restore rather than running in a subshell: `cmd_preflight` sets
# NODE_EXIT and the workdir trap in THIS shell, and a subshell would discard
# both. `${TMPDIR+x}` distinguishes unset from empty, which matters under
# `set -u`.
STAGED_OUTER_SET=0
STAGED_OUTER_WAS=""
# shellcheck disable=SC2031  # the sourced canary sets TMPDIR inside its node
# subshell; THIS one is the caller's, which is exactly what the case has to
# manipulate to model a CI runner whose /tmp holds the staged binary.
if [ -n "${TMPDIR+x}" ]; then STAGED_OUTER_SET=1; STAGED_OUTER_WAS="$TMPDIR"; fi
# shellcheck disable=SC2031
export TMPDIR="$CITMP"
STAGED_OUT="$(cmd_preflight "$FAKE_STAGED" 2>&1)"
STAGED_RC=$?
if [ "$STAGED_OUTER_SET" -eq 1 ]; then export TMPDIR="$STAGED_OUTER_WAS"; else unset TMPDIR; fi

if [ "$STAGED_RC" -eq 0 ]; then
    ok "the gate passes a HEALTHY binary staged at \$TMPDIR/freenet (the canary isolates TMPDIR)"
elif [[ "$STAGED_OUT" == *"client_api.rs"* || "$STAGED_OUT" == *"contract web directory"* ]]; then
    bad "the canary let its own TMPDIR reach the node: the node died creating \$TMPDIR/freenet/webs against the binary under test, and a HEALTHY release was blocked (exit $STAGED_RC). This is #5290 -- CI stages the gated binary at /tmp/freenet."
else
    bad "the gate failed a healthy binary for some other reason (exit $STAGED_RC): $STAGED_OUT"
fi

# ---------------------------------------------------------------------------
# 10. The ENVIRONMENT handed to the node, asserted behaviourally.
#
#     Case 9 covers TMPDIR because TMPDIR is the one that broke a release. The
#     other three exports are in the same position it was: nothing observes
#     them, so deleting `export HOME=` or `export FREENET_SUPERVISED=1` leaves
#     every suite green while the canary quietly stops being isolated (HOME:
#     the node's GitHub poll bucket lands in the caller's home) or stops
#     testing the real fleet transition (FREENET_SUPERVISED: the node logs a
#     "no supervisor" error instead of taking the exit-42 path Gate B asserts).
#
#     The fake dumps its own environment, so this asserts what the node
#     actually receives rather than what the script appears to set -- which is
#     also what makes it the case that survives a WRONG value rather than only
#     a missing one (see case 9). Mutation-checked: deleting
#     `export FREENET_SUPERVISED=1` fails here and nowhere else.
# ---------------------------------------------------------------------------
ENVDUMP="$TMPROOT/node-env.txt"
FAKE_ENVDUMP="$TMPROOT/fake-envdump"
cat > "$FAKE_ENVDUMP" <<FAKEENV
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
{
  echo "HOME=\${HOME:-<unset>}"
  echo "TMPDIR=\${TMPDIR:-<unset>}"
  echo "FREENET_SUPERVISED=\${FREENET_SUPERVISED:-<unset>}"
  echo "FREENET_DISABLE_LOG_RATE_LIMIT=\${FREENET_DISABLE_LOG_RATE_LIMIT:-<unset>}"
} > "$ENVDUMP"
sleep 1
echo "2026-08-08T02:00:00.000000Z  $CHECK_LINE" >> "\$logdir/freenet.2026-08-08-02.log"
exit 0
FAKEENV
chmod +x "$FAKE_ENVDUMP"

ENVWORK="$TMPROOT/envwork"
mkdir -p "$ENVWORK"
run_node_until_check "$FAKE_ENVDUMP" "$ENVWORK" >/dev/null 2>&1

if [ ! -f "$ENVDUMP" ]; then
    bad "the env-dump fake node never ran, so the canary's exports are unverified"
else
    if grep -qaF "HOME=$ENVWORK/home" "$ENVDUMP"; then
        ok "the node's HOME is inside the canary workdir"
    else
        bad "the node's HOME is not isolated to the workdir -- its GitHub poll bucket lands in the caller's home, so one gate's budget throttles the other's check"
    fi
    if grep -qaF "TMPDIR=$ENVWORK/tmp" "$ENVDUMP"; then
        ok "the node's TMPDIR is inside the canary workdir"
    else
        bad "the node's TMPDIR is NOT isolated to the workdir: its \$TMPDIR/freenet/webs mkdir escapes into the caller's temp dir, and in CI collides with the binary under test (#5290)"
    fi
    if grep -qaF "FREENET_SUPERVISED=1" "$ENVDUMP"; then
        ok "the node runs with FREENET_SUPERVISED=1 (the exit-42 path Gate B asserts)"
    else
        bad "FREENET_SUPERVISED is not set, so the node logs a 'no supervisor' error and stays put instead of taking the exit-42 path Gate B asserts"
    fi
    if grep -qaF "FREENET_DISABLE_LOG_RATE_LIMIT=1" "$ENVDUMP"; then
        ok "the node runs with log rate limiting disabled (a dropped WARN cannot fake a green gate)"
    else
        bad "FREENET_DISABLE_LOG_RATE_LIMIT is not set: a dropped parse-failure WARN leaves every negative assertion satisfied, which is a false GREEN on a binary carrying the #5221 bug"
    fi
fi

# ---------------------------------------------------------------------------
# 8. Gate B RETRIES an indeterminate run and does NOT retry a real failure.
#
#    THE DANGEROUS HALF IS THE SECOND ONE. Each attempt wipes the tree and boots
#    a fresh node, so if Gate B retried a genuine detection fault, any fault
#    that is intermittent -- a race, a timing-dependent parse, a node that
#    reaches the update path only sometimes -- would eventually produce one
#    passing attempt and the release would be reported healthy. A flaky pass on
#    the post-publish gate is strictly worse than no gate: it is a green light
#    nobody re-examines. That is the disaster this canary exists to prevent,
#    arriving by way of the canary's own retry loop.
#
#    Counted BEHAVIOURALLY, by how many times the node is actually booted,
#    because that is the property. A source grep for `[ "$rc" -eq 2 ] || break`
#    would pass just as happily if the surrounding logic stopped reaching it.
#
#    cmd_selfupdate downloads and unpacks the previous release before the loop,
#    so `curl` and `tar` are shadowed by no-op functions -- the script is
#    SOURCED, so a function definition wins over the external command -- and the
#    fake node is pre-placed where the extract would have put it.
boot_count_for() {
    # boot_count_for <extra-log-line> -- boots Gate B against a fake node that
    # logs <extra-log-line>, and echoes how many times the node was started.
    local extra="$1" work counter
    work="$CANARY_WORKDIR/selfupdate"
    rm -rf "$work"
    mkdir -p "$work/bin"
    counter="$TMPROOT/boots.$$"
    : > "$counter"
    make_fake_node "$work/bin/freenet" 0 "$extra"
    # Count a boot on every invocation that is not `--version`.
    sed -i "2i if [ \"\${1:-}\" != \"--version\" ]; then echo x >> \"$counter\"; fi" \
        "$work/bin/freenet"
    (
        curl() { :; }
        tar()  { :; }
        cmd_selfupdate 0.2.121 0.2.122 >/dev/null 2>&1
    )
    grep -c x "$counter" 2>/dev/null || echo 0
}

SAVED_ATTEMPTS="$CANARY_ATTEMPTS"
SAVED_SLEEP="$CANARY_RETRY_SLEEP"
SAVED_OUTCOME="$CANARY_OUTCOME_WAIT_SECS"
CANARY_ATTEMPTS=2
CANARY_RETRY_SLEEP=0
# The fake node writes its lines and exits immediately, so the only thing the
# outcome poll can wait for here is its own clock. Cut to keep this file inside
# the Fmt job's 5-minute budget; the REAL bound is exercised by cases 5 and 6,
# which is where it means something.
CANARY_OUTCOME_WAIT_SECS=2

boots="$(boot_count_for "WARN freenet::commands::auto_update: Startup update check: failed to fetch latest version: error sending request. Continuing with current binary.")"
if [[ "$boots" == "2" ]]; then
    ok "Gate B retries an INDETERMINATE run (2 attempts, node booted twice)"
else
    bad "Gate B booted the node $boots time(s) for an indeterminate run, expected 2 (CANARY_ATTEMPTS). Without the retry, one transient blip in a ~40s window decides a release's post-publish verdict -- and the previous release's own startup fetch has no retry either, so the blip is routine."
fi

boots="$(boot_count_for "$PARSE_FAIL_LINE")"
if [[ "$boots" == "1" ]]; then
    ok "Gate B does NOT retry a REAL detection failure (node booted once)"
else
    bad "Gate B booted the node $boots time(s) for a genuine #5221 parse failure, expected exactly 1. Retrying a real assertion failure lets an INTERMITTENT fault produce a passing attempt, and Gate B then reports a broken release healthy -- a flaky pass on the post-publish gate, which is worse than no gate at all."
fi

CANARY_ATTEMPTS="$SAVED_ATTEMPTS"
CANARY_RETRY_SLEEP="$SAVED_SLEEP"
CANARY_OUTCOME_WAIT_SECS="$SAVED_OUTCOME"

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All auto-update-canary lifecycle assertions passed."
else
    echo "$FAILURES lifecycle assertion(s) FAILED." >&2
    exit 1
fi
