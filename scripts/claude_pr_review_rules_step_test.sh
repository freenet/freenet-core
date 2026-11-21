#!/usr/bin/env bash
# Regression tests for the "Determine applicable rules" step of
# .github/workflows/claude-pr-review.yml (#6181).
#
# WHY THIS FILE EXISTS. The step decides which rules apply by diffing the PR
# against its base. It used to read that diff as
#
#     CHANGED_FILES=$(git diff --name-only origin/$BASE...HEAD 2>/dev/null || echo "")
#
# so a diff that FAILED (missing base ref, shallow fetch, transient git error)
# produced an empty list, indistinguishable from a PR that changed nothing. The
# step then set skip=true, every later step was skipped, and "Set status to
# success if skipped" posted `rule-review/warnings` as SUCCESS with "No
# relevant files changed". A review that never ran read as one that ran and
# found nothing to review: the same failure-reads-as-pass shape as #5628, one
# step earlier, and not covered by #6180, which only changed the post step.
#
# WHAT IT PINS. Two halves, because neither is sufficient alone:
#
#   1. The STEP'S SCRIPT, extracted from the live workflow (not copied) and run
#      against a stubbed `git`. A failing diff must set diff_failed=true; an
#      empty diff must not.
#   2. The STEP CONDITIONS in the YAML, because the bash cannot prove what is
#      posted. skip=true alone must no longer reach the success status, and the
#      failure status must be gated on diff_failed.
#
# A fix to the bash with the old `if:` left in place still posts success, and a
# fixed `if:` over the old bash never sets the output it tests. Pinning one and
# not the other is how this bug survives its own fix.
#
# Checks here assert EXACT values, not "something changed": the step's exit
# status alongside its outputs (the step writes skip=false before the rule
# selection runs, so a crash after that point would otherwise still look like a
# pass), the whole `if:` line rather than a substring of it (a condition
# weakened with an added `|| ...` still contains the substring), and each
# mutation's exact expected output (a mutation that merely broke shell syntax
# would otherwise read as caught). The stubbed `git` checks the arguments it was
# called with, so the `origin/$BASE_REF...HEAD` form is pinned too.
#
# Each mutation below reverses one half and must make a NAMED check fail: a
# guard nobody has seen fail is not known to guard.
#
# Needs: bash, awk, sed, git. Run manually with:
#   bash scripts/claude_pr_review_rules_step_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKFLOW="$REPO_ROOT/.github/workflows/claude-pr-review.yml"
TEST_TMP=$(mktemp -d)
trap 'rm -rf "$TEST_TMP"' EXIT

# The real step is well over a hundred lines of rule selection. A truncated or
# empty extraction (a renamed step, a changed indent) must fail loudly rather
# than make every case below pass against nothing.
MIN_SCRIPT_LINES=80

FAILURES=0
check() {
    # check <description> <actual> <expected>
    local desc="$1" actual="$2" expected="$3"
    if [[ "$actual" == "$expected" ]]; then
        echo "ok   - $desc"
    else
        echo "FAIL - $desc (got '$actual', expected '$expected')" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

# extract_script <workflow-file> <out-file>: the `run: |` body of the rules
# step, de-indented. No YAML parser: this repo's linters avoid that dependency.
extract_script() {
    awk '
        /^      - name: Determine applicable rules/ { in_step = 1; next }
        in_step && /^        run: \|/ { in_body = 1; next }
        in_body {
            if ($0 ~ /^          / || $0 ~ /^[[:space:]]*$/) { sub(/^          /, ""); print; next }
            exit
        }
    ' "$1" > "$2"
}

# step_condition <workflow-file> <step name>: that step's `if:` line.
step_condition() {
    awk -v name="      - name: $2" '
        index($0, name) == 1 { in_step = 1; next }
        in_step && /^        if: / { sub(/^        if: /, ""); print; exit }
        in_step && /^      - name: / { exit }
    ' "$1"
}

# The files the stubbed `git` reports in its "files" mode, and the rules the
# step must select for them. `ring/cache.rs` is under `crates/core/src/ring/`,
# so it sets the Rust, core and ring flags; `docs/x.md` sets none. Pinning the
# selection (not just "it did not skip") is what makes the files case fail if
# the rule logic breaks.
STUB_FILES=(crates/core/src/ring/cache.rs docs/x.md)
EXPECTED_RULES="git-workflow.md code-style.md testing.md ring.md"

# run_step <workflow-file> <git-mode>: run the extracted script with a stubbed
# `git`. Sets RUN_STATUS, RUN_OUTPUT and RUN_LOG for the caller.
#
# `bash -e` because that is what Actions runs (`bash -e {0}`); the step must
# behave the same here as it does in CI.
RUN_STATUS=0
RUN_OUTPUT=""
RUN_LOG=""
run_step() {
    local workflow="$1" mode="$2"
    local work="$TEST_TMP/run"
    rm -rf "$work"; mkdir -p "$work/bin"

    # The stub checks its arguments. A step that stopped diffing against
    # `origin/$BASE_REF...HEAD` (a dropped quote, a changed range, a lost
    # BASE_REF) would otherwise be served the same canned answer and pass.
    cat > "$work/bin/git" <<STUB
#!/usr/bin/env bash
if [ "\$*" != "diff --name-only origin/main...HEAD" ]; then
  echo "stub git: unexpected arguments: \$*" >&2
  exit 99
fi
case "$mode" in
  files) printf '%s\n' ${STUB_FILES[*]}; exit 0 ;;
  empty) exit 0 ;;
  fail)  echo "fatal: bad revision" >&2; exit 128 ;;
esac
STUB
    chmod +x "$work/bin/git"

    extract_script "$workflow" "$work/step.sh"
    : > "$work/output"
    set +e
    (
        PATH="$work/bin:$PATH" \
        BASE_REF=main \
        GITHUB_OUTPUT="$work/output" \
        bash -e "$work/step.sh" >"$work/log" 2>&1
    )
    RUN_STATUS=$?
    set -e
    RUN_OUTPUT="$work/output"
    RUN_LOG="$work/log"
}

# run_summary <workflow-file> <git-mode>: the step's exit status and decision
# outputs as one sorted line. The exit status is part of it because the step
# writes skip=false before selecting any rules, so a crash afterwards would
# leave an output that reads like success.
run_summary() {
    run_step "$1" "$2"
    { echo "exit=$RUN_STATUS"; grep -E '^(skip|diff_failed)=' "$RUN_OUTPUT" || true; } \
        | sort | tr '\n' ' ' | sed 's/ $//'
}

# step_block <workflow-file> <step name>: that step's YAML, up to the next step.
step_block() {
    awk -v name="      - name: $2" '
        index($0, name) == 1 { in_step = 1; next }
        in_step && index($0, "      - name: ") == 1 { exit }
        in_step { print }
    ' "$1"
}

echo "=== extraction ==="
extract_script "$WORKFLOW" "$TEST_TMP/step.sh"
LINES=$(wc -l < "$TEST_TMP/step.sh")
if [[ "$LINES" -lt "$MIN_SCRIPT_LINES" ]]; then
    echo "FAIL - extracted only $LINES lines, expected at least $MIN_SCRIPT_LINES: the step was renamed or re-indented" >&2
    exit 1
fi
echo "ok   - extracted $LINES lines of the live step"

# The harness substitutes nothing. A `${{ }}` expression reintroduced into the
# body would run here as literal text, so the cases below would stop testing
# what CI runs. Pass PR-controlled values through `env:` instead, as BASE_REF is.
# shellcheck disable=SC2016  # the needle is LITERAL `${{`, the text GitHub
# substitutes before the shell ever sees it, so it must not expand here.
check "step body has no \${{ }} expressions" \
    "$(grep -c '\${{' "$TEST_TMP/step.sh" || true)" "0"

echo "=== the step's own behaviour ==="
check "a diff with files runs to completion and does not skip" \
    "$(run_summary "$WORKFLOW" files)" "exit=0 skip=false"
check "it selects the rules those files call for" \
    "$(run_step "$WORKFLOW" files; sed -n 's/^rule_names=//p' "$RUN_OUTPUT")" "$EXPECTED_RULES"
check "an empty diff skips, without claiming the diff failed" \
    "$(run_summary "$WORKFLOW" empty)" "exit=0 skip=true"
check "a FAILED diff skips and says so, without failing the job" \
    "$(run_summary "$WORKFLOW" fail)" "diff_failed=true exit=0 skip=true"
check "a failed diff is annotated on the run" \
    "$(run_step "$WORKFLOW" fail; grep -c '^::error::' "$RUN_LOG" || true)" "1"

echo "=== the conditions that decide what gets posted ==="
# Whole-line comparisons, not substring matches: a condition weakened with an
# added `|| github.actor == ...` still contains the needle it was checked for.
BASE_IF="github.event_name != 'merge_group' && steps.override.outputs.overridden != 'true' && steps.rules.outputs.skip == 'true'"
check "the success status needs a skip that an empty diff caused" \
    "$(step_condition "$WORKFLOW" "Set status to success if skipped")" \
    "$BASE_IF && steps.rules.outputs.diff_failed != 'true'"
check "the not-run status needs a skip that a failed diff caused" \
    "$(step_condition "$WORKFLOW" "Set status to failure if the diff did not run")" \
    "$BASE_IF && steps.rules.outputs.diff_failed == 'true'"

NOT_RUN_BLOCK=$(step_block "$WORKFLOW" "Set status to failure if the diff did not run")
check "that step posts a failure, not a success" \
    "$(echo "$NOT_RUN_BLOCK" | grep -c "state: 'failure'" || true)" "1"
check "it posts under the status the success step uses" \
    "$(echo "$NOT_RUN_BLOCK" | grep -c "context: 'rule-review/warnings'" || true)" "1"

echo "=== mutations: each must make a named check above fail ==="
mutate() {
    # mutate <description> <sed-expression> <git-mode> <expected-mutant-summary>
    #
    # The expectation is what the MUTANT must produce, not merely "something
    # other than the healthy output". A mutation that broke the step's shell
    # syntax would differ from healthy too, and would be recorded as this
    # mutation being caught while pinning nothing.
    local desc="$1" expr="$2" mode="$3" expected="$4"
    local mutated="$TEST_TMP/mutated.yml"
    sed "$expr" "$WORKFLOW" > "$mutated"
    if cmp -s "$WORKFLOW" "$mutated"; then
        echo "FAIL - mutation '$desc' no longer matches the workflow; update its sed expression" >&2
        FAILURES=$((FAILURES + 1))
        return
    fi
    local got
    got=$(run_summary "$mutated" "$mode")
    if [[ "$got" == "$expected" ]]; then
        echo "ok   - mutation caught: $desc (gave '$got')"
    else
        echo "FAIL - mutation '$desc' gave '$got', expected the mutant to give '$expected'" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

# The original bug's behaviour: swallow the diff's exit status.
# shellcheck disable=SC2016  # both sed expressions below match LITERAL workflow
# source text, so `$(...)` and `$GITHUB_OUTPUT` must reach sed unexpanded.
mutate "diff failure swallowed by || echo \"\"" \
    's|if ! CHANGED_FILES=$(git diff --name-only "origin/${BASE_REF}...HEAD"); then|CHANGED_FILES=$(git diff --name-only "origin/${BASE_REF}...HEAD" 2>/dev/null \|\| echo ""); if false; then|' \
    fail "exit=0 skip=true"

# The output the conditions key on, dropped while the branch still exists.
# shellcheck disable=SC2016  # literal source text again, as above.
mutate "diff_failed output dropped" \
    '/echo "diff_failed=true" >> "\$GITHUB_OUTPUT"/d' \
    fail "exit=0 skip=true"

# The stub checks its arguments, so a changed diff range is caught as well: the
# stub refuses, and the step reads the refusal as a failed diff.
# shellcheck disable=SC2016  # literal source text.
mutate "diff range no longer origin/BASE...HEAD" \
    's|"origin/${BASE_REF}...HEAD"|"HEAD~1"|' \
    files "diff_failed=true exit=0 skip=true"

# Rule selection, which the files case pins as an exact list rather than as
# "the step did not skip".
RULES_MUTANT="$TEST_TMP/rules-mutant.yml"
# shellcheck disable=SC2016  # literal source text.
sed 's|RULES="$RULES ring.md"|RULES="$RULES"|' "$WORKFLOW" > "$RULES_MUTANT"
if cmp -s "$WORKFLOW" "$RULES_MUTANT"; then
    echo "FAIL - mutation 'ring rule dropped' no longer matches; update its sed expression" >&2
    FAILURES=$((FAILURES + 1))
else
    run_step "$RULES_MUTANT" files
    GOT_RULES=$(sed -n 's/^rule_names=//p' "$RUN_OUTPUT")
    if [[ "$GOT_RULES" == "git-workflow.md code-style.md testing.md" ]]; then
        echo "ok   - mutation caught: ring rule dropped (gave '$GOT_RULES')"
    else
        echo "FAIL - mutation 'ring rule dropped' gave '$GOT_RULES'" >&2
        FAILURES=$((FAILURES + 1))
    fi
fi

# A condition weakened rather than reverted. This is why the checks above
# compare the whole `if:` line: the needle a substring check looks for is still
# present here, so only an exact comparison rejects it.
WEAKENED="$TEST_TMP/weakened.yml"
sed "s|&& steps.rules.outputs.diff_failed != 'true'|\&\& (steps.rules.outputs.diff_failed != 'true' \|\| github.actor == 'nobody')|" "$WORKFLOW" > "$WEAKENED"
if cmp -s "$WORKFLOW" "$WEAKENED"; then
    echo "FAIL - mutation 'success condition weakened' no longer matches; update its sed expression" >&2
    FAILURES=$((FAILURES + 1))
else
    WEAK_IF=$(step_condition "$WEAKENED" "Set status to success if skipped")
    if [[ "$WEAK_IF" == "$BASE_IF && steps.rules.outputs.diff_failed != 'true'" ]]; then
        echo "FAIL - mutation 'success condition weakened' was not caught" >&2
        FAILURES=$((FAILURES + 1))
    elif echo "$WEAK_IF" | grep -q "steps.rules.outputs.diff_failed != 'true'"; then
        echo "ok   - mutation caught: success condition weakened (a substring check would have passed it)"
    else
        echo "FAIL - mutation 'success condition weakened' did not keep the needle, so it does not test what it claims" >&2
        FAILURES=$((FAILURES + 1))
    fi
fi

# The YAML half. Reverting the success step's condition cannot change the
# step's outputs, so it is checked directly rather than through run_step.
REVERTED=$TEST_TMP/reverted.yml
sed "s|&& steps.rules.outputs.skip == 'true' && steps.rules.outputs.diff_failed != 'true'|\&\& steps.rules.outputs.skip == 'true'|" "$WORKFLOW" > "$REVERTED"
if cmp -s "$WORKFLOW" "$REVERTED"; then
    echo "FAIL - mutation 'success step condition reverted' no longer matches; update its sed expression" >&2
    FAILURES=$((FAILURES + 1))
else
    GOT=$(step_condition "$REVERTED" "Set status to success if skipped" | grep -c "diff_failed" || true)
    if [[ "$GOT" == "0" ]]; then
        echo "ok   - mutation caught: success step condition reverted (gate gone)"
    else
        echo "FAIL - mutation 'success step condition reverted' was not caught" >&2
        FAILURES=$((FAILURES + 1))
    fi
fi

echo
if [[ "$FAILURES" -gt 0 ]]; then
    echo "$FAILURES check(s) failed" >&2
    exit 1
fi
echo "all checks passed"
