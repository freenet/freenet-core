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
# more. Measured across all 11 `*_test.sh` files in the repo at the time:
# 329 assertion call sites, 275 seen, 54 missed, 42 of those in
# `auto-update-canary_test.sh` alone.
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
expect 1 "bare 'check' (7 files)" \
    '+check "healthy: check ran, parsed, triggered -> pass" 0 "$HEALTHY"'
expect 1 "bare 'ok' (lifecycle)" \
    '+ok "the canary refuses a node that never checked"'
expect 1 "bare 'pass' (3 files)" \
    '+pass "cross-compile.yml still has the pre-flight failure notification job"'
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

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All Rule Lint shell-assertion counter assertions passed."
else
    echo "$FAILURES assertion(s) FAILED." >&2
    exit 1
fi
