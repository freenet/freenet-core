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

# step_block <line> -- the whole STEP containing <line>, as `NNN:text` lines.
#
# Bounded by the `- name:` at or above <line> and the next one below it (or the
# end of the job). Step keys such as `if:` and `continue-on-error:` sit ABOVE
# the `run:` that holds the invocation, so anything anchored on the invocation
# line alone cannot see them -- which is exactly how the publish step's own
# `if:` went unpinned while the canary step's was covered.
step_block() {
    local at="$1" start end
    start="$(printf '%s\n' "$JOB_BLOCK" \
        | awk -F: -v a="$at" '$1 <= a && /^[0-9]+:      - name:/ { n = $1 } END { print n }')"
    end="$(printf '%s\n' "$JOB_BLOCK" \
        | awk -F: -v a="$at" '$1 > a && /^[0-9]+:      - name:/ { print $1; exit }')"
    [[ -z "$end" ]] && end=999999
    printf '%s\n' "$JOB_BLOCK" | awk -F: -v a="$start" -v b="$end" '$1 >= a && $1 < b'
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
    CANARY_STEP="$(step_block "$CANARY_LINE")"
    NEUTERED="$(printf '%s\n' "$CANARY_STEP" \
        | grep -cE 'continue-on-error:[[:space:]]*true|^[0-9]+:        if:[[:space:]]*false')"
    if [[ "$NEUTERED" -eq 0 ]]; then
        pass "the canary step is not disabled in place"
    else
        fail "the canary step is disabled in place ('continue-on-error: true' or 'if: false')" \
            "It still runs and still reports, but the job publishes the release" \
            "whatever it finds. That is a gate in appearance only."
    fi

    # --- 3a. ...and its shell does not swallow the canary's exit status ------
    # The step-key checks above are blind to the SHELL. Appending `|| true` to
    # the invocation leaves the step present, before the publish, and without
    # `continue-on-error` -- every assertion here passed under exactly that
    # mutation, because assertion 1 matches the invocation as a SUBSTRING and
    # anything appended to the line is invisible to it.
    #
    # This is the likelier of the two neutering routes, and the worse one.
    # `|| true` is the reflex fix when a gate false-positives at 2am, and it
    # does not read as disabling a gate -- which is exactly why it has to fail
    # loudly here. The value of this gate is that removing it cannot be quiet.
    #
    # Scanning the whole step rather than just the canary line: `set +e`
    # anywhere in the run block has the same effect, and the step is three
    # lines, so there is no legitimate use of these to trip over. Comment-only
    # lines were dropped when JOB_BLOCK was built, so a `# || true` in prose
    # cannot fire this.
    SWALLOWED="$(printf '%s\n' "$CANARY_STEP" \
        | grep -cE '\|\|[[:space:]]*(true|:)[[:space:]]*$|set[[:space:]]+\+e')"
    if [[ "$SWALLOWED" -eq 0 ]]; then
        pass "the canary step's shell does not swallow its own exit status"
    else
        fail "the canary step swallows its own exit status ('|| true', '|| :' or 'set +e')" \
            "$(printf '%s\n' "$CANARY_STEP" | grep -E '\|\|[[:space:]]*(true|:)[[:space:]]*$|set[[:space:]]+\+e')" \
            "The step still runs, still reports, and still sits before the publish," \
            "but it can no longer fail -- so nothing blocks publication. This is the" \
            "cheapest possible way to disable the gate and the least visible: it looks" \
            "like error handling, not like removing a release gate."
    fi
fi

# --- 3b. ...and the PUBLISH step is not made unconditional ------------------
# The mirror image of the check above, and the hole it left. Assertion 3 asks
# whether the CANARY is disabled; nothing asked whether the PUBLISH step was
# made to run regardless of it. Both produce the same outcome -- a red canary
# that no longer blocks publication -- and only one was pinned.
#
# Demonstrated on this branch: adding `if: always()` to the `Publish release`
# step neutered Gate A completely and all six assertions here stayed GREEN.
# release.sh's belt-and-suspenders check gives no cover either, because the
# workflow really did publish, so `isDraft` reads false.
#
# Steps default to running only if every earlier step in the job succeeded, and
# that default IS the gate. The publish step has no `if:` today, and this pins
# that state rather than trying to judge which conditionals are safe.
#
# Deliberately stricter than "no always()/failure()/cancelled()". Whether an
# expression can evaluate true after a failed step is not something a grep
# should be deciding -- `success() || github.actor == 'x'` overrides the default
# without naming any of those functions. An `if:` on the one step whose
# conditional execution IS the release gate deserves a human look, so any `if:`
# at all fails here. If a legitimate one is ever needed, the person adding it
# updates this assertion on purpose, which is the point.
if [[ -n "$PUBLISH_LINE" ]]; then
    PUBLISH_STEP="$(step_block "$PUBLISH_LINE")"
    PUBLISH_IF="$(printf '%s\n' "$PUBLISH_STEP" | grep -E '^[0-9]+:        if:')"
    if [[ -z "$PUBLISH_IF" ]]; then
        pass "the publish step has no 'if:', so it still runs only when the canary passed"
    else
        fail "the publish step has acquired an 'if:'" \
            "$(printf '%s\n' "$PUBLISH_IF")" \
            "Steps run only after every earlier step succeeded, and that default is" \
            "the ENTIRE mechanism by which the canary blocks publication. An 'if:'" \
            "here can override it -- 'if: always()' publishes the release even when" \
            "the canary failed, and the gate is gone without the canary step being" \
            "touched at all. If this conditional is genuinely wanted, confirm it" \
            "cannot evaluate true after a failed step, then update this assertion."
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

# --- 6. the failure notification still covers BOTH gates --------------------
# Gate A blocks publication by failing, which leaves the release stuck as a
# DRAFT -- a silent state. Nobody is watching the Actions tab during a release;
# the Matrix message is how anyone finds out. Gate B runs after publication and
# cannot block anything, so the notification is its ONLY output.
#
# Neither the notify job nor Gate B's job was referenced by any test in this
# repo before this. Demonstrated on this branch: deleting the two
# `needs.attach-to-release.result` clauses from the notify job's `if:` --
# reinstating exactly the silent-Gate-A regression the workflow comment
# describes -- left all four suites GREEN.
#
# Whole-file scan, not the attach-to-release block: these are separate
# top-level jobs.
notify_block="$(awk '
    /^  notify-auto-update-canary-failure:[[:space:]]*$/ { inblock = 1; print; next }
    inblock && /^  [A-Za-z_.-]+:/                        { inblock = 0 }
    inblock                                              { print }
' "$WF")"

if [[ -z "$notify_block" ]]; then
    fail "the 'notify-auto-update-canary-failure' job is gone from cross-compile.yml" \
        "A failed Gate A leaves the release stuck as a draft and says nothing;" \
        "a failed Gate B has no other output at all. This job is how either" \
        "one reaches a human."
else
    pass "cross-compile.yml still has the pre-flight failure notification job"

    # Each gate contributes two result states. `failure` alone is not enough:
    # a cancelled job is not a passed one, and treating it as passed is how a
    # timed-out gate goes unreported.
    missing=()
    for clause in \
        "needs.attach-to-release.result == 'failure'" \
        "needs.attach-to-release.result == 'cancelled'" \
        "needs.auto-update-selfupdate-canary.result == 'failure'" \
        "needs.auto-update-selfupdate-canary.result == 'cancelled'"
    do
        [[ "$notify_block" == *"$clause"* ]] || missing+=("$clause")
    done

    if [[ ${#missing[@]} -eq 0 ]]; then
        pass "the notification fires for failure AND cancellation of both gates"
    else
        fail "the notification no longer covers every gate outcome" \
            "Missing from the notify job's 'if:':" \
            "${missing[@]}" \
            "A gate whose failure notifies nobody is a gate nobody acts on. Gate A" \
            "failing leaves the release a silent DRAFT; Gate B has no other output."
    fi

    # `always()` is what lets the job run at all after a needed job failed.
    # Without it the notification is skipped in exactly the case it exists for.
    if [[ "$notify_block" == *'always()'* ]]; then
        pass "the notify job runs under always() (so a failed gate does not skip it)"
    else
        fail "the notify job's 'if:' no longer calls always()" \
            "A job whose dependency failed is SKIPPED unless its condition calls" \
            "always(). Without it this job never runs on the one path it exists for."
    fi

    # It must also still DEPEND on both, or the results it tests are always ''.
    for needed in attach-to-release auto-update-selfupdate-canary; do
        if [[ "$notify_block" == *"needs:"*"$needed"* ]]; then
            pass "the notify job still needs '$needed'"
        else
            fail "the notify job no longer lists '$needed' in 'needs:'" \
                "A result expression for a job that is not needed evaluates to the" \
                "empty string, so every clause above silently stops matching."
        fi
    done

    # --- 6b. the ENVIRONMENTAL split cannot swallow the real alarm ----------
    # The notify job now picks between two messages: a quiet one when the
    # canary classified the run as environmental (it could not reach GitHub, so
    # it learned nothing), and the loud #5221 one otherwise. Silencing the
    # channel is now a two-line edit -- widen the quiet branch's condition, or
    # delete the loud step -- and either would look like tidying.
    #
    # Three properties, each of which alone is enough to lose the alarm:
    #
    #   (a) the loud message still exists at all;
    #   (b) the quiet branch requires BOTH that the publish job succeeded and
    #       that the classification is exactly 'environmental'. Drop the first
    #       and a release stuck as a DRAFT gets the reassuring message; drop
    #       the second and everything does;
    #   (c) the loud branch is the NEGATION of the quiet one rather than its
    #       own enumeration of results. That is what puts every unanticipated
    #       state -- a cancelled job, an empty classification because the step
    #       never ran -- on the side that gets read.
    if [[ "$notify_block" == *'#5221 failure mode'* ]]; then
        pass "the notify job still carries the loud #5221 message"
    else
        fail "the loud '#5221 failure mode' message is gone from the notify job" \
            "That text is the only thing that tells the room a node on the previous" \
            "release may be unable to reach this one. If it was removed to stop" \
            "false alarms, the fix is the environmental classification, not silence."
    fi

    quiet_cond="needs.attach-to-release.result == 'success' &&"
    quiet_cond+=" needs.auto-update-selfupdate-canary.outputs.classification == 'environmental'"

    # PER-STEP, and that is not fussiness. The first version of this scanned the
    # whole job block for "$quiet_cond", which the LOUD step satisfies all by
    # itself -- its condition is `!(<quiet_cond>)`, so the quiet condition is a
    # substring of it. Mutation-tested: replacing the quiet step's `if:` with a
    # bare `result == 'failure'`, so the reassuring message covers every red
    # run including a real #5221, left this suite fully GREEN. A pin whose
    # subject can be satisfied by the thing it is distinguishing from is not a
    # pin, which is the whole subject of #5303.
    #
    # Steps are split on their leading `- name:` / `- uses:` key and identified
    # by text from their own message, so neither depends on step ORDER.
    notify_steps="$(printf '%s\n' "$notify_block" | awk '
        /^      - (name|uses):/ { n++ }
        { print n "\t" $0 }
    ')"
    step_containing() {
        # step_containing <needle> -- the whole step chunk holding <needle>,
        # whitespace-collapsed, or empty if no single step holds it.
        local needle="$1" n
        n="$(printf '%s\n' "$notify_steps" | grep -F -- "$needle" | head -1 | cut -f1)"
        [[ -n "$n" ]] || return 0
        printf '%s\n' "$notify_steps" | awk -F'\t' -v n="$n" '$1 == n { print $2 }' \
            | tr -s '[:space:]' ' '
    }

    # Located by `id:`, not by a phrase from the message. Locating them by
    # message text was tried and an ordinary reword of the quiet message broke
    # the pin on the first edit -- the same "expectation stored as a copy"
    # shape this PR exists to remove. An `id:` is structural and exists to be
    # referenced; a rename of one still fails here, but loudly and for a reason
    # the message names.
    quiet_step="$(step_containing 'id: notify-environmental')"
    loud_step="$(step_containing 'id: notify-fault')"

    if [[ -z "$quiet_step" ]]; then
        fail "the environmental (quiet) notification step is gone from the notify job" \
            "Without it every red Gate B run takes the loud #5221 branch again, so a" \
            "runner that could not reach github.com tells the room the fleet may be" \
            "stranded. That is the alarm-fatigue failure this split exists to stop."
    elif [[ "$quiet_step" == *"$quiet_cond"* && "$quiet_step" != *'!('* ]]; then
        pass "the quiet step requires a successful publish AND an environmental classification"
    else
        fail "the environmental notification's own condition changed" \
            "Its step must be conditioned on exactly:" \
            "  $quiet_cond" \
            "Widened, the reassuring 'nothing to worry about' message starts covering" \
            "real faults and a release stuck as a DRAFT. Its condition is currently:" \
            "  $quiet_step"
    fi

    if [[ -n "$loud_step" && "$loud_step" == *"!($quiet_cond)"* ]]; then
        pass "the loud step is the exact negation of the quiet one (unknown states stay loud)"
    else
        fail "the loud notification is no longer the exact negation of the quiet one" \
            "Written as its own list of results instead, any state neither branch" \
            "anticipated -- a cancelled job, an empty classification because the" \
            "step never ran -- falls through both and nothing is sent at all."
    fi
fi

# --- 6c. Gate B's step still classifies, and still re-raises its exit code ---
# The classification only reaches the notify job because the canary step writes
# it to GITHUB_OUTPUT and the job re-exports it. Two ways to break that
# silently: drop the job-level `outputs:` (every clause above reads '' and the
# loud branch fires for everything, which is merely noisy), or swallow the
# script's exit code so the job goes GREEN on a failed canary, which is not
# noisy at all.
selfupdate_block="$(awk '
    /^  auto-update-selfupdate-canary:[[:space:]]*$/ { inblock = 1; print; next }
    inblock && /^  [A-Za-z_.-]+:/                    { inblock = 0 }
    inblock                                          { print }
' "$WF")"

# shellcheck disable=SC2016  # literal workflow text: the `${{ }}` must not expand
if [[ "$selfupdate_block" == *'classification: ${{ steps.canary.outputs.classification }}'* ]]; then
    pass "Gate B's job still exports the 'classification' output"
else
    fail "Gate B's job no longer exports 'classification'" \
        "The notify job reads it to choose between the quiet and the loud message." \
        "Missing, it evaluates to '' and every run takes the loud #5221 branch."
fi

# --- 6d. the environmental exit code is the SAME number on both sides -------
# The canary defines `EXIT_UNVERIFIED_ENVIRONMENTAL` and the workflow tests a
# LITERAL. They are one contract written in two files, and nothing made them
# agree. Change the constant and every environmental run silently takes the
# loud #5221 branch instead of the quiet one -- the direction is safe, which is
# exactly why it would go unnoticed. Read from the canary rather than restated
# here, so this test cannot be the thing that is stale.
canary_env_code="$(sed -n 's/^EXIT_UNVERIFIED_ENVIRONMENTAL=\([0-9]\{1,\}\).*/\1/p' \
    "$SCRIPT_DIR/auto-update-canary.sh" | head -1)"
if [[ -z "$canary_env_code" ]]; then
    fail "could not read EXIT_UNVERIFIED_ENVIRONMENTAL from auto-update-canary.sh" \
        "Without it this assertion would compare the workflow against an empty" \
        "string and pass on anything."
elif [[ "$selfupdate_block" == *"-eq $canary_env_code "* ]]; then
    pass "the workflow tests the same environmental exit code the canary returns ($canary_env_code)"
else
    fail "cross-compile.yml does not test exit $canary_env_code, the canary's EXIT_UNVERIFIED_ENVIRONMENTAL" \
        "The two are one contract in two files. If they disagree, every" \
        "environmental run is classified 'fault' and takes the loud #5221" \
        "branch -- safe, and therefore silent."
fi

# --- 6e. the step CAPTURES the exit status as well as re-raising it ---------
# BOTH halves, because pinning only the second leaves the cheaper mutation wide
# open. Confirmed by mutation: change `|| rc=$?` to `|| true` and `rc` stays 0,
# `classification` is reported as `ok`, `exit "$rc"` exits 0 -- so Gate B goes
# GREEN on a genuinely broken updater, the job result is `success`, and the
# notify job never fires. The `exit "$rc"` pin sees nothing, because that line
# is untouched.
#
# Assertion 3a closes the same neutering route for GATE A only: it scans
# `$CANARY_STEP`, the preflight invocation inside `attach-to-release`. Gate B is
# a different top-level job and was never scanned.
#
# One directive for the whole compound command: shellcheck rejects a directive
# in front of an individual `elif` branch (SC1123).
# shellcheck disable=SC2016  # literal workflow text; `$rc` must not expand
if [[ "$selfupdate_block" != *'|| rc=$?'* ]]; then
    fail "Gate B's step no longer captures the canary's exit status into \$rc" \
        "It must run the canary as '... || rc=\$?' and re-raise at the end." \
        "Replaced with '|| true' the step exits 0 whatever the canary found:" \
        "Gate B is green on a broken updater and the notify job never fires," \
        "because the job result is 'success'. The 'exit \$rc' pin below cannot" \
        "see this -- that line is left untouched by the mutation."
elif [[ "$selfupdate_block" != *'exit "$rc"'* ]]; then
    fail "Gate B's step no longer re-raises the canary's exit status" \
        "It runs the canary with '|| rc=\$?' so it can classify exit 75. That is" \
        "only safe while the code is raised again at the end of the step. Without" \
        "the re-raise the step always exits 0, Gate B is green on a broken" \
        "updater, and nothing notifies -- the exact silent fail this file exists" \
        "to prevent, reached by deleting one line."
else
    swallowed_b="$(printf '%s\n' "$selfupdate_block" \
        | grep -cE '\|\|[[:space:]]*(true|:)[[:space:]]*$|set[[:space:]]+\+e')"
    if [[ "$swallowed_b" -eq 0 ]]; then
        pass "Gate B's step captures AND re-raises the canary's exit code, and swallows nothing"
    else
        fail "Gate B's step swallows an exit status ('|| true', '|| :' or 'set +e')" \
            "$(printf '%s\n' "$selfupdate_block" | grep -E '\|\|[[:space:]]*(true|:)[[:space:]]*$|set[[:space:]]+\+e')" \
            "Same route assertion 3a closes for Gate A: the step still runs and" \
            "still reports, but it can no longer fail."
    fi
fi

# --- 7. Gate B's job still exists -------------------------------------------
# The notify job's clauses are only meaningful if the job they name is real.
if grep -qE '^  auto-update-selfupdate-canary:[[:space:]]*$' "$WF"; then
    pass "cross-compile.yml still has the 'auto-update-selfupdate-canary' job (Gate B)"
else
    fail "the 'auto-update-selfupdate-canary' job (Gate B) is gone from cross-compile.yml" \
        "Gate B is what proves a node on the PREVIOUS release can actually reach" \
        "this one -- the #5221 failure mode. Nothing else covers it."
fi

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All release canary wiring assertions passed."
else
    echo "$FAILURES assertion(s) FAILED." >&2
    exit 1
fi
