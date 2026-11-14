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

# run_step <workflow-file> <git-mode>: run the extracted script with a stubbed
# `git`, and echo the step outputs it wrote as a sorted one-line summary.
run_step() {
    local workflow="$1" mode="$2"
    local work="$TEST_TMP/run.$$"
    rm -rf "$work"; mkdir -p "$work/bin"

    cat > "$work/bin/git" <<STUB
#!/usr/bin/env bash
case "$mode" in
  files) printf '%s\n' 'crates/core/src/ring.rs' 'docs/x.md'; exit 0 ;;
  empty) exit 0 ;;
  fail)  echo "fatal: bad revision" >&2; exit 128 ;;
esac
STUB
    chmod +x "$work/bin/git"

    extract_script "$workflow" "$work/step.sh"
    : > "$work/output"
    (
        PATH="$work/bin:$PATH" \
        BASE_REF=main \
        GITHUB_OUTPUT="$work/output" \
        bash "$work/step.sh" >"$work/log" 2>&1
    ) || true
    # Only the decision outputs matter here; rule_names has its own checks.
    grep -E '^(skip|diff_failed)=' "$work/output" | sort | tr '\n' ' ' | sed 's/ $//'
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
check "a diff with files does not skip" \
    "$(run_step "$WORKFLOW" files)" "skip=false"
check "an empty diff skips, without claiming the diff failed" \
    "$(run_step "$WORKFLOW" empty)" "skip=true"
check "a FAILED diff skips and says so" \
    "$(run_step "$WORKFLOW" fail)" "diff_failed=true skip=true"

echo "=== the conditions that decide what gets posted ==="
SUCCESS_IF=$(step_condition "$WORKFLOW" "Set status to success if skipped")
check "the success status is gated on the diff having run" \
    "$(echo "$SUCCESS_IF" | grep -c "steps.rules.outputs.diff_failed != 'true'" || true)" "1"
FAILURE_IF=$(step_condition "$WORKFLOW" "Set status to failure if the diff did not run")
check "a not-run status step exists and is gated on diff_failed" \
    "$(echo "$FAILURE_IF" | grep -c "steps.rules.outputs.diff_failed == 'true'" || true)" "1"
check "that step posts a failure, not a success" \
    "$(awk '/^      - name: Set status to failure if the diff did not run/,/^      - name: [^S]/' "$WORKFLOW" | grep -c "state: 'failure'" || true)" "1"

echo "=== mutations: each must make a named check above fail ==="
mutate() {
    # mutate <description> <sed-expression> <git-mode> <expected-summary>
    local desc="$1" expr="$2" mode="$3" expected="$4"
    local mutated="$TEST_TMP/mutated.yml"
    sed "$expr" "$WORKFLOW" > "$mutated"
    if cmp -s "$WORKFLOW" "$mutated"; then
        echo "FAIL - mutation '$desc' no longer matches the workflow; update its sed expression" >&2
        FAILURES=$((FAILURES + 1))
        return
    fi
    local got
    got=$(run_step "$mutated" "$mode")
    if [[ "$got" == "$expected" ]]; then
        echo "FAIL - mutation '$desc' was not caught (still '$got')" >&2
        FAILURES=$((FAILURES + 1))
    else
        echo "ok   - mutation caught: $desc (gave '$got')"
    fi
}

# The original bug's behaviour: swallow the diff's exit status.
# shellcheck disable=SC2016  # both sed expressions below match LITERAL workflow
# source text, so `$(...)` and `$GITHUB_OUTPUT` must reach sed unexpanded.
mutate "diff failure swallowed by || echo \"\"" \
    's|if ! CHANGED_FILES=$(git diff --name-only "origin/${BASE_REF}...HEAD"); then|CHANGED_FILES=$(git diff --name-only "origin/${BASE_REF}...HEAD" 2>/dev/null \|\| echo ""); if false; then|' \
    fail "diff_failed=true skip=true"

# The output the conditions key on, dropped while the branch still exists.
# shellcheck disable=SC2016  # literal source text again, as above.
mutate "diff_failed output dropped" \
    '/echo "diff_failed=true" >> "\$GITHUB_OUTPUT"/d' \
    fail "diff_failed=true skip=true"

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
