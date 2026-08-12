#!/usr/bin/env bash
# Regression test for the release driver's `gh` calls during the cross-compile
# wait -- the window in which a single transient GitHub blip could abandon a
# release that had already published.
#
# THE BUG. `scripts/release.sh` runs under `set -euo pipefail`, and a bare
# `var=$(cmd)` is a simple command whose exit status IS `cmd`'s. So every
# unguarded `$(gh ...)` in `wait_for_binaries` was a live abort:
#
#     $ bash -c 'set -e; x=$(false); echo reached'   # "reached" never prints
#
# `set -o pipefail` extends this to `$(gh ... | head -1)` -- `head` does not
# absorb `gh`'s failure. `wait_for_binaries` is called BARE at release.sh:1598,
# so errexit is genuinely armed inside it, and it polls for up to 20 minutes.
# One rate-limit or 5xx in that window killed the driver AFTER the release had
# published but BEFORE `trigger_gateway_updates`, `announce_to_matrix` and
# `announce_to_river` -- release.sh:1216 describes the consequence in its own
# words: "A release that published perfectly well would silently never be
# announced." Silent, and indistinguishable from a release nobody cut.
#
# WHAT IS ASSERTED. `attach_job_state` documents that empty output means "we do
# not know", explicitly including the case where "`gh` failed". These cases pin
# that the code actually delivers that contract: a failing `gh` must make the
# loop poll again, not tear the driver down. The negative cases (4, 5, 6) exist
# because "survive a `gh` failure" is one `|| true` away from "survive
# everything" -- a driver that never aborts is as broken as one that always
# does, just in the other direction.
#
# The real functions are extracted verbatim from release.sh (the technique
# release_state_restore_test.sh already uses) so this test cannot drift from
# the code the release actually runs.
#
# Run manually: bash scripts/release_wait_for_binaries_test.sh
# Also wired into CI (the Fmt job in .github/workflows/ci.yml).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RELEASE_SH="$SCRIPT_DIR/release.sh"

if [[ ! -f "$RELEASE_SH" ]]; then
    echo "FAIL: $RELEASE_SH not found" >&2
    exit 1
fi

FAILURES=0
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# --- the `gh` stub ----------------------------------------------------------
# A real executable on PATH, not a shell function, so it exercises the same
# invocation path as production and cannot be bypassed by `command gh`.
#
# Each scripted response is a file named <kind>.<call-number> whose first line
# is the exit code and whose remaining lines are stdout. An invocation with no
# scripted response is a hard error (exit 98) rather than a silent empty
# result, because several callers swallow `gh`'s stderr and would otherwise
# read a stub gap as a legitimate "not ready yet".
mkdir -p "$TMP/bin"
cat > "$TMP/bin/gh" <<'STUB'
#!/usr/bin/env bash
args="$*"
case "$args" in
    *"--json isDraft"*)    kind=isdraft ;;
    *"--json assets"*)     kind=assets ;;
    *"--json jobs"*)       kind=jobs ;;
    *"--json status"*)     kind=runstatus ;;
    *"--json databaseId"*) kind=runlist ;;
    *"--draft=false"*)     kind=releaseedit ;;
    *)
        echo "gh stub: unhandled invocation: $args" >&2
        exit 99
        ;;
esac

n=$(( $(cat "$GH_STUB_DIR/count.$kind" 2>/dev/null || echo 0) + 1 ))
echo "$n" > "$GH_STUB_DIR/count.$kind"
echo "$kind #$n" >> "$GH_STUB_DIR/calls.log"

resp="$GH_STUB_DIR/$kind.$n"
if [[ ! -f "$resp" ]]; then
    echo "gh stub: no scripted response for $kind call #$n" >&2
    exit 98
fi
code="$(head -1 "$resp")"
tail -n +2 "$resp"
exit "$code"
STUB
chmod +x "$TMP/bin/gh"

# --- the code under test ----------------------------------------------------
# Pulled verbatim from release.sh so a future edit to the real functions is
# what this test runs. `sleep` is neutralised so the 20-minute poll and the
# 2-minute run-discovery loop run instantly; nothing else is substituted.
DEFS="$TMP/defs.sh"
{
    grep -E "^ATTACH_JOB_NAME=" "$RELEASE_SH"
    awk '/^attach_job_state\(\) \{/,/^}/'          "$RELEASE_SH"
    awk '/^publish_draft_release\(\) \{/,/^}/'     "$RELEASE_SH"
    awk '/^verify_required_binaries\(\) \{/,/^}/'  "$RELEASE_SH"
    awk '/^wait_for_binaries\(\) \{/,/^}/'         "$RELEASE_SH"
    echo 'sleep() { :; }'
    echo 'VERSION="0.0.0-test"'
    echo 'DRY_RUN=false'
} > "$DEFS"

for fn in attach_job_state publish_draft_release verify_required_binaries wait_for_binaries; do
    if ! grep -q "^$fn() {" "$DEFS"; then
        echo "FAIL: could not extract $fn from release.sh -- has it been renamed or reindented?" >&2
        exit 1
    fi
done

# The 10 assets verify_required_binaries insists on, mirroring REQUIRED_BINARIES.
ALL_BINARIES=(
    "freenet-x86_64-unknown-linux-musl.tar.gz"
    "freenet-aarch64-unknown-linux-musl.tar.gz"
    "freenet-aarch64-apple-darwin.tar.gz"
    "freenet-x86_64-apple-darwin.tar.gz"
    "freenet-x86_64-pc-windows-msvc.zip"
    "fdev-x86_64-unknown-linux-musl.tar.gz"
    "fdev-aarch64-unknown-linux-musl.tar.gz"
    "fdev-aarch64-apple-darwin.tar.gz"
    "fdev-x86_64-apple-darwin.tar.gz"
    "fdev-x86_64-pc-windows-msvc.zip"
)

STUB_DIR=""

new_scenario() {
    STUB_DIR="$(mktemp -d "$TMP/scenario.XXXXXX")"
}

# respond <kind> <call-number> <exit-code> [stdout-line ...]
respond() {
    local kind="$1" n="$2" code="$3"
    shift 3
    {
        echo "$code"
        if [[ $# -gt 0 ]]; then
            printf '%s\n' "$@"
        fi
    } > "$STUB_DIR/$kind.$n"
}

# Runs wait_for_binaries EXACTLY as release.sh:1598 does -- bare, under
# `set -euo pipefail`. DRIVER_CONTINUED prints only if the driver would have
# gone on to update the gateways and announce the release; an errexit abort or
# a non-zero return both swallow it, which is the production consequence.
run_driver() {
    (
        set -euo pipefail
        export GH_STUB_DIR="$STUB_DIR"
        export PATH="$TMP/bin:$PATH"
        # shellcheck source=/dev/null
        source "$DEFS"
        wait_for_binaries
        echo "DRIVER_CONTINUED"
    ) 2>&1
}

# Dumps the exact `gh` call sequence the driver made. A failure here is
# otherwise reported only through the driver's own output, where a stub or
# environment problem is indistinguishable from a real product failure --
# a missed response reads as a plausible "Missing: <binary>".
dump_calls() {
    echo "       gh calls made: $(tr '\n' ',' < "$STUB_DIR/calls.log" 2>/dev/null || echo 'none recorded')" >&2
}

# check <description> <expect-continued: yes|no> <expected-rc> [expected-substring]
check() {
    local desc="$1" expect_cont="$2" expect_rc="$3" want_msg="${4:-}"
    local out rc got_cont
    out="$(run_driver)"
    rc=$?
    if [[ "$out" == *DRIVER_CONTINUED* ]]; then got_cont=yes; else got_cont=no; fi

    if [[ "$got_cont" != "$expect_cont" ]]; then
        echo "FAIL - $desc" >&2
        if [[ "$expect_cont" == "yes" ]]; then
            echo "       the driver ABORTED during the cross-compile wait (rc=$rc)." >&2
            echo "       In production that is a published release that never reaches the" >&2
            echo "       gateways and is never announced -- see release.sh:1216." >&2
        else
            echo "       the driver CONTINUED past a condition that must stop it (rc=$rc)." >&2
        fi
        echo "       output: $out" >&2
        dump_calls
        FAILURES=$((FAILURES + 1))
        return
    fi
    if [[ "$rc" != "$expect_rc" ]]; then
        echo "FAIL - $desc (got rc=$rc, expected $expect_rc)" >&2
        echo "       output: $out" >&2
        dump_calls
        FAILURES=$((FAILURES + 1))
        return
    fi
    if [[ -n "$want_msg" && "$out" != *"$want_msg"* ]]; then
        echo "FAIL - $desc (rc=$rc correct, but the diagnosis is missing)" >&2
        echo "       wanted output containing: $want_msg" >&2
        echo "       got: $out" >&2
        dump_calls
        FAILURES=$((FAILURES + 1))
        return
    fi
    echo "ok   - $desc"
}

# check_call_count <description> <kind> <expected>
check_call_count() {
    local desc="$1" kind="$2" expected="$3" actual
    actual="$(cat "$STUB_DIR/count.$kind" 2>/dev/null || echo 0)"
    if [[ "$actual" == "$expected" ]]; then
        echo "ok   - $desc"
    else
        echo "FAIL - $desc (gh '$kind' called $actual times, expected $expected)" >&2
        dump_calls
        FAILURES=$((FAILURES + 1))
    fi
}

# ===========================================================================
# 1. A transient `gh` failure while reading the attach job's state.
#
# This is the busiest call in the release: `job_state` stays empty for the
# whole multi-minute build window, so BOTH the `attach_job_state` read and the
# run-status read under it are hit on every 30s tick. One 5xx here used to end
# the release. It must simply poll again.
# ===========================================================================
new_scenario
respond assets    1 0                       # workflow still building
respond runlist   1 0 "9001"
respond jobs      1 1                       # <-- transient gh failure
respond runstatus 1 1                       # <-- and on the follow-up read
respond jobs      2 0 "completed:success"   # recovered
respond assets    2 0 "${ALL_BINARIES[@]}"
respond isdraft   1 0 "false"               # already published by the workflow
check "transient gh failure reading attach-job state -> keeps polling, release completes" \
    yes 0 "All required platform binaries attached"
check_call_count "  and it really did re-poll after the failure" jobs 2

# ===========================================================================
# 2. A transient `gh` failure while discovering the workflow run.
#
# `run_id=$(gh run list ... | head -1)` looks protected because `head` exits 0,
# but `set -o pipefail` propagates `gh`'s status out of the pipeline. This loop
# exists specifically to retry ("it takes a few seconds for GitHub to start the
# workflow"), so aborting on the first blip defeats its only purpose.
# ===========================================================================
new_scenario
respond assets    1 0
respond runlist   1 1                       # <-- transient gh failure
respond runlist   2 0 "9001"                # recovered on the next tick
respond jobs      1 0 "completed:success"
respond assets    2 0 "${ALL_BINARIES[@]}"
respond isdraft   1 0 "false"
check "transient gh failure finding the workflow run -> retries, release completes" \
    yes 0 "Workflow run ID: 9001"

# ===========================================================================
# 3. `gh` SUCCEEDS but returns nothing -- the job has not started yet.
#
# The guard must not change this path: empty-with-exit-0 already meant "keep
# waiting", and it must still mean that rather than being read as a pass.
# ===========================================================================
new_scenario
respond assets    1 0
respond runlist   1 0 "9001"
respond jobs      1 0                       # job not created yet (exit 0, empty)
respond runstatus 1 0 "in_progress"
respond jobs      2 0                       # still not created
respond runstatus 2 0 "in_progress"
respond jobs      3 0 "completed:success"
respond assets    2 0 "${ALL_BINARIES[@]}"
respond isdraft   1 0 "false"
check "gh returns empty while the job is pending -> keeps waiting, then succeeds" \
    yes 0 "Binaries attached and release published"
check_call_count "  and it waited through both pending ticks" jobs 3

# ===========================================================================
# 4. Empty job state, but the RUN has finished: the fast, loud exit.
#
# The job was cancelled before creation or renamed out from under
# ATTACH_JOB_NAME. Reported as UNKNOWN, never as a pass -- and the guard must
# not soften it into "poll until timeout".
# ===========================================================================
new_scenario
respond assets    1 0
respond runlist   1 0 "9001"
respond jobs      1 0
respond runstatus 1 0 "completed"
check "job never reported and the run finished -> stops loudly, driver does NOT continue" \
    no 1 "never reported a result"

# ===========================================================================
# 5. A genuinely failed attach job is still fatal.
#
# The blocking pre-flight canary (#5222) rejecting the binary lands here. If
# the errexit guard were over-applied, this is where it would show up as a
# release that announces a broken updater.
# ===========================================================================
new_scenario
respond assets    1 0
respond runlist   1 0 "9001"
respond jobs      1 0 "completed:failure"
check "attach job failed -> stops loudly, driver does NOT continue" \
    no 1 "failed (conclusion: failure)"

# ===========================================================================
# 6. A `gh` failure at the publish gate must REFUSE, with its diagnosis.
#
# publish_draft_release already treats an unknown gate state as "do not
# publish". Without the guard the `gh` failure aborted the driver one line
# before it could say so, turning a documented refusal into a silent death.
# The refusal is still fatal here -- what regressed was the operator's only
# clue about why.
# ===========================================================================
new_scenario
respond assets  1 0 "${ALL_BINARIES[@]}"    # all assets up; gate decides
respond isdraft 1 0 "true"                  # still a draft -> gate applies
respond runlist 1 0 "9001"
respond jobs    1 1                         # <-- transient gh failure at the gate
check "gh failure at the publish gate -> refuses to publish AND says why" \
    no 1 "NOT publishing v0.0.0-test"

# ===========================================================================
# 7. `gh` fails while reading isDraft -> refuse, do not report success.
#
# This one never published an ungated release, so it looked harmless. What it
# did was coerce the failure to "false" ("not a draft, nothing to gate") and
# return 0, so the driver went on to update the gateways and announce a release
# that may still have been an unpublished draft. Every other unknown in
# publish_draft_release refuses; this is the one that did not.
# ===========================================================================
new_scenario
respond assets  1 0 "${ALL_BINARIES[@]}"
respond isdraft 1 1                         # <-- gh failure, draft state unknown
check "gh failure reading isDraft -> refuses instead of announcing a maybe-draft" \
    no 1 "Cannot tell whether v0.0.0-test is still a draft"

# ===========================================================================
# 8. isDraft=false -> the workflow already published; nothing left to gate.
#
# Distinguishing this from case 7 is the whole point of the change: a real
# "false" must still short-circuit to success, or every release would refuse.
# ===========================================================================
new_scenario
respond assets  1 0 "${ALL_BINARIES[@]}"
respond isdraft 1 0 "false"
check "isDraft=false -> already published, driver continues" yes 0 \
    "All required platform binaries already available"

# ===========================================================================
# 9-12. One case per `job_state` value, pinning the DECISION each produces.
#
# release.sh:1206-1288 added ~100 lines of decision logic to the release
# critical path, in a function whose own comment records a prior fail-open
# ("Returning 0 here made the caller report success, so the driver went on to
# update the gateways and announce..."). The four values are the whole state
# space of that switch, and each maps to a different, load-bearing outcome.
#
# `in_progress:` is the one with no other coverage: it is neither empty (so the
# run-finished fast-exit does not apply) nor completed (so the terminal switch
# does not fire), and the only correct behaviour is to keep polling. If it ever
# fell through to "not completed, therefore fine", the driver would announce
# mid-build.
# ===========================================================================
new_scenario
respond assets    1 0
respond runlist   1 0 "9001"
respond jobs      1 0 "in_progress:"        # queued/running, no conclusion yet
respond jobs      2 0 "in_progress:"
respond jobs      3 0 "completed:success"
respond assets    2 0 "${ALL_BINARIES[@]}"
respond isdraft   1 0 "false"
check "job_state 'in_progress:' -> keeps polling, never treated as terminal" \
    yes 0 "Binaries attached and release published"
check_call_count "  and it polled through both in_progress ticks" jobs 3
# `in_progress:` must NOT reach the run-status branch: that branch is gated on
# an EMPTY job_state, and firing it here would mean the switch had lost track of
# a job that is plainly still running.
check_call_count "  and it never consulted run status for a running job" runstatus 0

new_scenario
respond assets    1 0
respond runlist   1 0 "9001"
respond jobs      1 0 "completed:cancelled"  # any non-success conclusion
check "job_state 'completed:cancelled' -> fails the release loudly" \
    no 1 "failed (conclusion: cancelled)"

# `completed:success` but the assets are NOT all there: the job lied, or an
# upload was lost. Publishing on this would ship a release that cannot be
# installed on the missing platform.
new_scenario
respond assets    1 0
respond runlist   1 0 "9001"
respond jobs      1 0 "completed:success"
respond assets    2 0 "${ALL_BINARIES[@]:0:9}"   # windows fdev zip missing
check "job_state 'completed:success' but an asset is missing -> fails" \
    no 1 "some required binaries are missing"

# The publish gate itself: still a draft, and the attach job did NOT succeed.
# Publishing here would turn the blocking pre-flight canary into no gate at all.
new_scenario
respond assets  1 0 "${ALL_BINARIES[@]}"
respond isdraft 1 0 "true"
respond runlist 1 0 "9001"
respond jobs    1 0 "completed:failure"
check "publish gate: still a draft and attach job failed -> refuses to publish" \
    no 1 "NOT publishing v0.0.0-test"

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All release wait_for_binaries assertions passed."
else
    echo "$FAILURES assertion(s) FAILED." >&2
    exit 1
fi
