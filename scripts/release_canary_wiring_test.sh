#!/usr/bin/env bash
# Regression test for the WIRING of the blocking auto-update pre-flight canary
# (#5222/#5236) -- as distinct from the canary's own logic, which
# auto-update-canary_test.sh covers.
#
# THE GAP THIS CLOSES. The canary only gates anything because of where it sits
# in .github/workflows/cross-compile.yml: inside the `attach-to-release` job,
# AFTER the assets are uploaded and BEFORE `gh release edit --draft=false`.
# That position is the entire mechanism. Delete the step, move the publish
# above it, or mark it `continue-on-error`, and the gate becomes a no-op while
# every other test in this repo stays green -- which is precisely the
# silently-removable-gate shape the canary was introduced to eliminate. A gate
# whose removal is invisible is not a gate.
#
# It also pins the two ends of a string that must agree across files:
# release.sh's ATTACH_JOB_NAME is how the release driver finds this job's
# status, and nothing else checks that the name still matches. Rename the job
# in the workflow and the driver waits for a job that will never appear, then
# times out ~20 minutes later reporting UNKNOWN -- on a release that in fact
# published fine.
#
# Same shape as release_mergequeue_test.sh, which greps release.yml for the
# `gh pr merge` invocation it must keep.
#
# Run manually: bash scripts/release_canary_wiring_test.sh
# Also wired into CI (the Fmt job in .github/workflows/ci.yml).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WF="$SCRIPT_DIR/../.github/workflows/cross-compile.yml"
RELEASE_SH="$SCRIPT_DIR/release.sh"

FAILURES=0

fail() {
    echo "FAIL - $1" >&2
    shift
    for line in "$@"; do echo "       $line" >&2; done
    FAILURES=$((FAILURES + 1))
}
pass() { echo "ok   - $1"; }

for f in "$WF" "$RELEASE_SH"; do
    if [[ ! -f "$f" ]]; then
        echo "FAIL: $f not found" >&2
        exit 1
    fi
done

# The `attach-to-release:` job block: from its own key to the next top-level
# job key. Job keys sit at exactly two spaces; everything inside the job is
# indented further, so this needs no YAML parser and cannot be fooled by a
# matching string in a comment elsewhere in the file.
# Comment-only lines are dropped, but the original line NUMBERS are kept, so
# ordering comparisons stay meaningful. This is load-bearing: the job's own
# comments discuss `--draft=false` (explaining the RELEASE_PAT coalesce) well
# above the step that runs it, so matching raw text would compare the canary
# against a sentence and report the gate inverted. A pin that fails on prose is
# no better than one that passes on prose.
JOB_BLOCK="$(awk '
    /^  attach-to-release:[[:space:]]*$/ { inblock = 1; print NR ":" $0; next }
    inblock && /^  [A-Za-z_.-]+:/        { inblock = 0 }
    inblock && $0 !~ /^[[:space:]]*#/    { print NR ":" $0 }
' "$WF")"

if [[ -z "$JOB_BLOCK" ]]; then
    fail "the 'attach-to-release' job no longer exists in cross-compile.yml" \
        "That job is where the canary gates publication; release.sh watches it by name."
    echo
    echo "$FAILURES assertion(s) FAILED." >&2
    exit 1
fi
pass "cross-compile.yml still has an 'attach-to-release' job"

# line_of <extended-regex> -- first matching line number INSIDE the job block.
line_of() {
    printf '%s\n' "$JOB_BLOCK" | grep -E "$1" | head -1 | cut -d: -f1
}

# --- 1. the canary step still runs ------------------------------------------
CANARY_LINE="$(line_of 'auto-update-canary\.sh preflight')"
if [[ -n "$CANARY_LINE" ]]; then
    pass "the pre-flight canary still runs in attach-to-release (line $CANARY_LINE)"
else
    fail "the pre-flight canary is GONE from the attach-to-release job" \
        "Nothing now blocks publication on the shipping binary's updater working." \
        "This is the #5222 regression the canary exists to prevent: v0.2.120 and" \
        "v0.2.121 both shipped with a dead updater and every signal stayed green."
fi

# --- 2. it runs BEFORE the release is un-drafted ----------------------------
# Presence alone does not gate anything: a canary that runs after the publish
# is a report, not a gate, and the release has already reached users by then.
PUBLISH_LINE="$(line_of '\-\-draft=false')"
if [[ -z "$PUBLISH_LINE" ]]; then
    fail "no 'gh release edit --draft=false' in the attach-to-release job" \
        "If publication moved elsewhere, the canary no longer gates it."
elif [[ -n "$CANARY_LINE" ]]; then
    if [[ "$CANARY_LINE" -lt "$PUBLISH_LINE" ]]; then
        pass "the canary runs BEFORE '--draft=false' (canary $CANARY_LINE < publish $PUBLISH_LINE)"
    else
        fail "the canary runs AFTER the release is published (canary $CANARY_LINE > publish $PUBLISH_LINE)" \
            "Steps in a job run in file order, so this is no longer a gate: the" \
            "release is public before the updater is ever exercised. The whole" \
            "point of Gate A is that a failure costs a stuck DRAFT, not a" \
            "stranded fleet."
    fi
fi

# --- 3. it is not neutered in place -----------------------------------------
# `continue-on-error: true` leaves the step present, running, and visibly
# green-ish in the UI while the job proceeds to publish regardless -- the
# cheapest way to disable this gate without appearing to remove it.
#
# The scan must cover the whole STEP, not the run: line onwards. Step keys
# (`continue-on-error`, `if`, `timeout-minutes`) sit ABOVE the `run:` that
# contains the invocation, so a scan anchored on the invocation line misses
# them entirely -- verified by mutation: this assertion did not fire until the
# bounds were widened to the step.
if [[ -n "$CANARY_LINE" ]]; then
    # Step boundaries: the `- name:` at or above the invocation, and the next
    # `- name:` below it (or the end of the job).
    STEP_START="$(printf '%s\n' "$JOB_BLOCK" \
        | awk -F: -v a="$CANARY_LINE" '$1 <= a && /^[0-9]+:      - name:/ { n = $1 } END { print n }')"
    STEP_END="$(printf '%s\n' "$JOB_BLOCK" \
        | awk -F: -v a="$CANARY_LINE" '$1 > a && /^[0-9]+:      - name:/ { print $1; exit }')"
    [[ -z "$STEP_END" ]] && STEP_END=999999
    NEUTERED="$(printf '%s\n' "$JOB_BLOCK" \
        | awk -F: -v a="$STEP_START" -v b="$STEP_END" '$1 >= a && $1 < b' \
        | grep -cE 'continue-on-error:[[:space:]]*true|^[0-9]+:        if:[[:space:]]*false')"
    if [[ "$NEUTERED" -eq 0 ]]; then
        pass "the canary step is not disabled in place (lines $STEP_START-$STEP_END)"
    else
        fail "the canary step is disabled in place ('continue-on-error: true' or 'if: false')" \
            "It still runs and still reports, but the job publishes the release" \
            "whatever it finds. That is a gate in appearance only."
    fi
fi

# --- 4. CI must not pre-set CANARY_EXPECTED_LATEST --------------------------
# Gate A resolves the expected release itself, from the same `releases/latest`
# redirect the node uses, and refuses if it cannot. A value supplied by the
# workflow would displace that resolution with a hand-maintained string.
#
# Note what this does and does not protect against, because the commit that
# introduced the skip branch overstated it. A pinned value can only make the
# check FAIL -- it cannot make it PASS vacuously -- provided the pinned value
# is WRONG. Pin it CORRECTLY (say to the tag being released, which during
# Gate A is not yet what `releases/latest` returns) and you have replaced a
# resolved fact with an asserted one: the gate then compares the node's answer
# against a constant somebody typed, which is precisely the class of check this
# canary exists to replace. Either way it should not be here, so pin its
# absence rather than reasoning about which failure mode it would cause.
#
# Nothing sets it today; that is the state being pinned.
ARMED="$(printf '%s\n' "$JOB_BLOCK" | grep -cE 'CANARY_EXPECTED_LATEST')"
if [[ "$ARMED" -eq 0 ]]; then
    pass "the workflow does not pre-set CANARY_EXPECTED_LATEST (Gate A resolves it)"
else
    fail "the attach-to-release job sets CANARY_EXPECTED_LATEST" \
        "Gate A resolves the expected release from the same redirect the node reads," \
        "and refuses if it cannot. A workflow-supplied value replaces that resolved" \
        "fact with a hand-maintained constant -- and if it is wrong, it fails a" \
        "healthy release for a difference that is not a bug." \
        "$(printf '%s\n' "$JOB_BLOCK" | grep -E 'CANARY_EXPECTED_LATEST')"
fi

# --- 5. release.sh and the workflow agree on the job name -------------------
# release.sh reads this job's status by DISPLAY NAME. Nothing else pins the
# pair, and a rename on either side is silent: the driver simply never sees the
# job, waits out its 20-minute timeout, and reports UNKNOWN for a release that
# published normally.
WF_JOB_NAME="$(printf '%s\n' "$JOB_BLOCK" \
    | sed -n 's/^[0-9]*:    name:[[:space:]]*//p' | head -1 \
    | sed "s/^['\"]//;s/['\"]$//")"
SH_JOB_NAME="$(sed -n "s/^ATTACH_JOB_NAME=//p" "$RELEASE_SH" | head -1 \
    | sed "s/^['\"]//;s/['\"]$//")"

if [[ -z "$WF_JOB_NAME" ]]; then
    fail "the attach-to-release job has no 'name:' in cross-compile.yml" \
        "release.sh matches on the display name, which defaults to the job KEY" \
        "when 'name:' is absent -- so removing it silently breaks the driver."
elif [[ -z "$SH_JOB_NAME" ]]; then
    fail "ATTACH_JOB_NAME not found in release.sh"
elif [[ "$WF_JOB_NAME" == "$SH_JOB_NAME" ]]; then
    pass "release.sh ATTACH_JOB_NAME matches the workflow job name ('$WF_JOB_NAME')"
else
    fail "release.sh and cross-compile.yml disagree on the attach job's name" \
        "cross-compile.yml: '$WF_JOB_NAME'" \
        "release.sh:        '$SH_JOB_NAME'" \
        "The driver polls for the workflow's name, so it would wait for a job" \
        "that never appears and time out reporting UNKNOWN."
fi

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All release canary wiring assertions passed."
else
    echo "$FAILURES assertion(s) FAILED." >&2
    exit 1
fi
