#!/usr/bin/env bash
# Regression test for the Rule Lint shell-assertion counter in
# .github/workflows/ci.yml -- the grep that decides whether a `fix:` PR whose
# regression test is a shell self-test has any test at all.
#
# WHY THIS FILE EXISTS. That alternation has been wrong three times, and each
# time it was widened only as far as the file in front of whoever was editing.
# The most recent miss was structural rather than a missing spelling: the
# `[[:space:]]` immediately after `(check|ok|pass)` requires the helper's name
# to END there, so EVERY underscore-suffixed helper scored zero --
# `gate_b_arm_case`, `version_ge_case`, `pin_marker`, `check_vs_expected`,
# `check_call_count`, `check_fallback`, `test_restores_persisted_value` and
# more. Measured across every `*_test.sh` in the repo, the original form saw
# roughly three quarters of the assertion call sites; the great majority of the
# misses were in `auto-update-canary_test.sh`, which is written almost entirely
# in that style.
#
# Deliberately no exact figures: they moved three times inside the PR that added
# this file, and a stale number in a justification is the sentence that tells
# the next reader not to re-measure. The computed checks at the bottom of this
# file are where the real numbers live, and they fail naming the files.
#
# Nothing in the repo exercised the regex, so each of those misses was found by
# a human noticing a wrong number rather than by CI. The failure direction is
# the bad one: an under-counting lint REJECTS a `fix:` PR that does have a
# regression test, and the path of least resistance for the author is to reshape
# a good test until the grep likes it, or to reach for `test-exempt`.
#
# WHAT IT PINS. Not the regex text -- that would be a copy, and a copy is
# rewritten by the same edit it is supposed to guard (the failure mode #5303
# spent two rounds on). It EXTRACTS the live regex out of ci.yml and runs it
# against call-site shapes taken verbatim from the repo's own test files, in
# both directions. A widening that starts matching plumbing, and a narrowing
# that stops matching an existing convention, both fail here.
#
# Run manually: bash scripts/rule_lint_shell_assertion_counter_test.sh
# Also wired into CI (the Fmt job in .github/workflows/ci.yml).

# Every single-quoted string below the fixture header is VERBATIM diff text --
# a line as the counter's grep will see it, `$SRC` and trailing backslashes
# included. Not expanding them is the entire point, so SC2016 ("expressions
# don't expand in single quotes") and SC1003 (a literal trailing backslash read
# as an attempted quote-escape) are both correct observations about code that is
# deliberately literal. Disabled file-wide because the fixtures are the bulk of
# the file; there is no other single-quoted `$` here for the rule to catch.
# shellcheck disable=SC2016,SC1003

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WF="$SCRIPT_DIR/../.github/workflows/ci.yml"
FAILURES=0

if [[ ! -f "$WF" ]]; then
    echo "FAIL - ci.yml not found at $WF" >&2
    exit 1
fi

# The counter's own line, identified by the two things that make it that line
# rather than any other grep in the file: it pipes into `grep -cE` and its
# alternation mentions PASS.
COUNTER_LINE="$(grep -F "| grep -cE '" "$WF" | grep -F 'PASS' | tail -1)"
FRAG="${COUNTER_LINE#*| grep -cE }"
FRAG="${FRAG% || true)}"

# THE VACUITY GUARD, and it is not boilerplate: the first draft of this file
# extracted the fragment with a `grep -oP` lookbehind that stopped at the
# embedded `'"'"'`, leaving a truncated pattern. An EMPTY pattern makes
# `grep -cE ''` match every line, so all ten negative cases below "passed" while
# reporting 1. A test that cannot fail is exactly what this file exists to stop
# someone else shipping.
if [[ -z "$COUNTER_LINE" || -z "$FRAG" ]]; then
    echo "FAIL - could not find the Rule Lint shell-assertion counter in ci.yml." >&2
    echo "       Looked for a line piping into \`grep -cE '...'\` whose pattern mentions PASS." >&2
    echo "       If the step was reshaped, update this extraction; if it was DELETED, a fix:" >&2
    echo "       PR whose only test is a shell self-test can no longer be recognised at all." >&2
    exit 1
fi
if [[ "$FRAG" != \'*\' ]]; then
    echo "FAIL - the extracted pattern is not a single-quoted shell word: $FRAG" >&2
    echo "       Extraction is out of step with ci.yml; every case below would be unreliable." >&2
    exit 1
fi

# `eval` because FRAG is still shell-quoted, exactly as ci.yml writes it
# (including the `'"'"'` dance for the embedded single quote). Re-quoting it by
# hand here would be the copy this file is built to avoid.
counter_matches() {
    local line="$1" n
    n="$(eval "printf '%s\n' \"\$line\" | grep -cE $FRAG")"
    printf '%s' "$n"
}

expect() {
    # expect <want 0|1> <what the line is> <line, as it appears in a diff>
    local want="$1" desc="$2" line="$3" got
    got="$(counter_matches "$line")"
    if [[ "$got" == "$want" ]]; then
        echo "ok   - $desc"
    else
        echo "FAIL - $desc" >&2
        echo "         line:  $line" >&2
        echo "         wanted the counter to score it $want, it scored $got" >&2
        if [[ "$want" == 1 ]]; then
            echo "       This is a convention the repo's *_test.sh files ALREADY use, so a" >&2
            echo "       fix: PR whose regression test is written this way now scores zero" >&2
            echo "       and is rejected for having no test. Widen the alternation in" >&2
            echo "       ci.yml rather than reshaping the test." >&2
        else
            echo "       The counter now scores a line that is not an assertion, so a fix:" >&2
            echo "       PR can satisfy the test requirement without adding a test." >&2
        fi
        FAILURES=$((FAILURES + 1))
    fi
}

# --- MUST COUNT: every call-site convention in use across scripts/**/*_test.sh
# Each line is a real one from the file named, with a leading '+' as the diff
# would present it. Add a row here when a new convention appears; that is the
# enumeration the ci.yml comment tells you to re-run.
expect 1 "bare 'check' (most files)" \
    '+check "healthy: check ran, parsed, triggered -> pass" 0 "$HEALTHY"'
expect 1 "bare 'ok' (lifecycle)" \
    '+ok "the canary refuses a node that never checked"'
expect 1 "bare 'pass' (3 files)" \
    '+pass "cross-compile.yml still has the pre-flight failure notification job"'
# THIS file's own convention, and the case that proves the point it makes.
# When this file was first added, EVERY ONE of its `expect` calls scored ZERO --
# the very under-count it exists to prevent, reintroduced by the fix for it,
# inside the same PR, and found by re-measuring rather than by reading. The
# computed check at the bottom of this file is what now catches that shape. Safe as a bare
# name only because nothing in scripts/ drives the TCL `expect(1)`.
expect 1 "bare 'expect' (this file)" \
    '+expect 1 "bare check (most files)" \'
expect 1 "inline echo \"ok   - ...\"" \
    '+    echo "ok   - MARKER_PARSE_FAIL frozen against a rename sweep"'
expect 1 "inline echo 'PASS ...' (release_state_restore_test.sh)" \
    "+echo \"PASS restores the persisted value\""
expect 1 "'*_case' helper: gate_b_arm_case" \
    '+gate_b_arm_case "0.2.126"                   yes   # every release after it'
expect 1 "'*_case' helper: version_ge_case" \
    '+version_ge_case "0.2.123" "0.2.124" no    # strictly below'
expect 1 "'*_case' helper: trigger_case" \
    '+trigger_case "urgent path" "triggering immediate auto-update" yes'
expect 1 "'pin_' helper: pin_marker" \
    '+pin_marker "source pin: disabled marker"        "$SRC"    "$MARKER_DISABLED"'
expect 1 "'pin_' helper: pin_warn_literal, continued on the next line" \
    '+pin_warn_literal "source pin: parse-failure marker (latest-version arm)" \'
expect 1 "'check_' helper: check_vs_expected" \
    '+check_vs_expected "equality: node saw the published tag -> pass" 0 "$SEEN_OK" 0.2.122'
expect 1 "'check_' helper: check_call_count" \
    '+check_call_count "  and it really did re-poll after the failure" jobs 2'
expect 1 "'check_' helper: check_fallback" \
    '+check_fallback "success-fallback + allow empty -> hold (fail closed)" \'
expect 1 "'test_' helper called with arguments" \
    '+test_restores_persisted_value "v0.2.42 regression" "0.3.205" "0.3.206" "0.3.205"'
expect 1 "'test_' helper called with none" \
    '+test_no_persisted_keeps_tentative'

# --- MUST NOT COUNT ---------------------------------------------------------
# A `fix:` PR must not be able to satisfy the test requirement without adding a
# test, so each of these has to score zero.
expect 0 "a helper DEFINITION, not a call" \
    '+check_vs_expected() {'
expect 0 "a '*_case' helper definition" \
    '+gate_b_arm_case() {'
expect 0 "a bare-name helper definition" \
    '+pass() { echo "ok   - $1"; }'
expect 0 "a FAIL branch (deliberate: an error message is not a test)" \
    '+    echo "FAIL - the frozen #5221 signature text changed." >&2'
expect 0 "plumbing: run_driver" \
    '+run_driver "scenario" 3'
expect 0 "plumbing: make_fake_node" \
    '+make_fake_node "$dir" 42'
expect 0 "plumbing: a stub redefinition" \
    '+read_room_state() { READ_CALLS=$((READ_CALLS + 1)); return 0; }'
expect 0 "an ordinary local assignment that happens to start with 'check'" \
    '+local checked=1'
expect 0 "prose in a comment" \
    '+# check that the marker is still frozen'
expect 0 "echo that merely starts with the letters 'ok'" \
    '+    echo "okay, starting the node"'
expect 0 "a bare string added to a table (a KNOWN blind spot, documented in ci.yml)" \
    '+    "$SCRIPT_DIR/release.sh"'

# --- COMPUTED, not asserted in prose ----------------------------------------
# Everything above is a hand-written case. These two are measurements, and they
# are here because the claims they replace were both WRONG in a comment while
# the suite was green.
#
# The rule they come from: a justification that states a COUNT or a GREP RESULT
# is the one kind of comment whose staleness actively suppresses the check that
# would catch it -- it tells the next reader they may stop looking. So compute
# it where it can go red, and let the prose point here.

# 1. EVERY *_test.sh the counter can see must contain at least one line it
#    recognises. A file scoring zero means the counter cannot see that file's
#    convention AT ALL, so a `fix:` PR whose only regression test lives there is
#    rejected for having no test. This is not hypothetical: when
#    THIS file was added, its 26 bare `expect` calls scored zero -- the miss it
#    exists to prevent, in the file written to prevent it, caught by measuring.
invisible=()
unrun=()
CI_YML="$SCRIPT_DIR/../.github/workflows/ci.yml"
while IFS= read -r f; do
    [[ -f "$f" ]] || continue
    # Same `eval` path as `counter_matches`, so this measures the LIVE pattern
    # rather than a re-quoted copy of it.
    if [[ "$(eval "sed 's/^/+/' \"\$f\" | grep -cE $FRAG")" -eq 0 ]]; then
        invisible+=("$f")
    fi
    # ...AND it must actually be RUN. A test file that no workflow invokes is
    # scored by the counter above and executed by nothing: it looks like
    # coverage in a listing and gates nothing at all.
    #
    # Not hypothetical, and not caught by inspection either time. It was found
    # by hand that `release_state_restore_test.sh` had existed and passed for a
    # long time while ci.yml referenced it only in a COMMENT -- so the v0.2.42
    # regression it was written for had no running gate behind it. A later
    # review then confirmed "no other orphans" by inspection, and demonstrated
    # in the same breath why inspection is not enough: it added a probe test
    # with a real assertion and no `run:` line, and all suites stayed green.
    # This loop is what makes that fail closed.
    #
    # Matched against `run:` LINES specifically, not the whole file, so a
    # mention in a comment -- exactly how the orphan hid -- does not count as
    # being run.
    if [[ -f "$CI_YML" ]]; then
        _base="$(basename "$f")"
        if [[ "$(grep -cE "^[[:space:]]*run:.*$_base" "$CI_YML")" -eq 0 ]]; then
            unrun+=("$f")
        fi
    fi
    # Filesystem glob, NOT `git ls-files`, and the difference matters: a
    # contributor adding a test file has not necessarily staged it yet, and that
    # unstaged file is precisely the one whose convention the counter may not
    # know. Mutation-tested -- with `git ls-files` an untracked probe file was
    # invisible to this very check, so the check that exists to find invisible
    # files could not see the newest one.
done < <(find "$SCRIPT_DIR" -name '*_test.sh' -type f | sort)

if [[ ! -f "$CI_YML" ]]; then
    echo "FAIL - ci.yml not found at $CI_YML; cannot check that test files are run." >&2
    FAILURES=$((FAILURES + 1))
elif [[ ${#unrun[@]} -eq 0 ]]; then
    echo "ok   - every *_test.sh is invoked by a 'run:' line in ci.yml"
else
    echo "FAIL - these *_test.sh files are never RUN by ci.yml:" >&2
    for f in "${unrun[@]}"; do echo "         $f" >&2; done
    echo "       They are scored by the assertion counter and executed by nothing," >&2
    echo "       so they look like coverage in a listing while gating nothing. Add a" >&2
    echo "       'run: bash scripts/<name>' step to the Fmt job in ci.yml. A mention" >&2
    echo "       in a COMMENT does not count -- that is exactly how the previous" >&2
    echo "       orphan stayed hidden." >&2
    FAILURES=$((FAILURES + 1))
fi

if [[ ${#invisible[@]} -eq 0 ]]; then
    echo "ok   - every tracked *_test.sh contains at least one assertion the counter sees"
else
    echo "FAIL - these *_test.sh files are INVISIBLE to the shell-assertion counter:" >&2
    for f in "${invisible[@]}"; do echo "         $f" >&2; done
    echo "       A fix: PR whose only regression test is in one of them scores zero and" >&2
    echo "       is rejected for having no test. Either the file uses a convention the" >&2
    echo "       alternation does not know, or it has no assertions at all. Widen the" >&2
    echo "       alternation in ci.yml -- do not reshape the tests to please the grep." >&2
    FAILURES=$((FAILURES + 1))
fi

# 2. The `printf`-style reporter, which ci.yml lists as a known blind spot. That
#    entry was once DELETED on the stated ground that the grep returned nothing;
#    it returns two, in `test-*.sh` files the counter's `**/*_test.sh` glob does
#    not reach. The sentence was wrong and nothing could tell. So: if that
#    convention ever appears in a file the counter DOES scan, the blind spot has
#    stopped being theoretical and this fails.
printf_in_scanned=()
while IFS= read -r hit; do
    printf_in_scanned+=("$hit")
done < <(grep -rnE "printf[[:space:]]+.*['\"](ok|PASS)" \
    "$SCRIPT_DIR"/*_test.sh "$SCRIPT_DIR"/release-agent/*_test.sh 2>/dev/null || true)

if [[ ${#printf_in_scanned[@]} -eq 0 ]]; then
    echo "ok   - no printf-style success reporter in any file the counter scans (blind spot still theoretical)"
else
    echo "FAIL - a printf-style success reporter now exists in a file the counter SCANS:" >&2
    for hit in "${printf_in_scanned[@]}"; do echo "         $hit" >&2; done
    echo "       ci.yml lists this as a known-but-unreached blind spot. It is now" >&2
    echo "       reached: those assertions score zero. Add a printf pattern to the" >&2
    echo "       alternation and a MUST-COUNT row above, then update ci.yml's list." >&2
    echo "       (The two pre-existing hits, scripts/test-install-sh.sh and" >&2
    echo "       scripts/test-uninstall-sh.sh, are OUT of scope by filename: the diff" >&2
    echo "       is globbed to '**/*_test.sh' and they are 'test-*.sh'.)" >&2
    FAILURES=$((FAILURES + 1))
fi

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All Rule Lint shell-assertion counter assertions passed."
else
    echo "$FAILURES assertion(s) FAILED." >&2
    exit 1
fi
