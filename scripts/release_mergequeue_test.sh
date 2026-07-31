#!/usr/bin/env bash
# Regression tests for release workflow interactions with the merge queue.
#
# release.yml's `update_versions` job auto-merges the version-bump PR with
# `gh pr merge ... --squash --auto`. The repo's merge queue (strict-strategy
# enforcement enabled between v0.2.92 and v0.2.93) OWNS the merge strategy and
# rejects an explicit `--squash`, so `gh pr merge` exits non-zero. Under
# `shell: bash -e` that fails the step, fails the `update_versions` job, and
# skips the ENTIRE publish cascade (wait_for_pr -> publish_crates -> tag ->
# gateway update). Net effect: the bump PR is created but nothing publishes.
#
# The fix drops `--squash` (the queue applies its configured strategy). This
# test pins that no `gh pr merge` invocation in release.yml re-adds `--squash`,
# and that the auto-merge step still enables auto-merge with `--auto`.
#
# Issue #5084 added a second failure mode: when merge-group CI failed, the
# waiter ignored that terminal result and reported only an opaque one-hour
# timeout. The behavioral checks below execute the same script as the workflow
# with mocked `gh` responses, proving that the newest CI attempt for this PR
# controls the decision and that its failure URL is surfaced before any sleep.
#
# Run manually with: bash scripts/release_mergequeue_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RELEASE_YML="$SCRIPT_DIR/../.github/workflows/release.yml"
WAIT_SCRIPT="$SCRIPT_DIR/wait_for_release_pr.sh"
TEST_TMP=$(mktemp -d)
trap 'rm -rf "$TEST_TMP"' EXIT

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

# Non-comment `gh pr merge` invocations in release.yml.
MERGE_LINES=$(grep -E 'gh pr merge' "$RELEASE_YML" | grep -vE '^\s*#' || true)

check "release.yml still has a gh pr merge invocation" \
  "$([ -n "$MERGE_LINES" ] && echo present || echo missing)" "present"

# The merge queue owns the strategy; an explicit --squash makes `gh pr merge`
# exit non-zero and skips the whole publish cascade (broke v0.2.93).
check "gh pr merge does NOT pass --squash" \
  "$(echo "$MERGE_LINES" | grep -q -- '--squash' && echo has-squash || echo no-squash)" "no-squash"

# Auto-merge must still be enabled, or wait_for_pr times out.
check "gh pr merge still enables --auto" \
  "$(echo "$MERGE_LINES" | grep -q -- '--auto' && echo has-auto || echo no-auto)" "has-auto"

# The waiter reads Actions workflow runs. Top-level `permissions:` makes every
# unspecified GITHUB_TOKEN scope `none`, so this job must explicitly opt into
# Actions read access. Use the job's least-privilege GITHUB_TOKEN rather than a
# RELEASE_PAT that may not carry the Actions permission.
WAIT_JOB=$(sed -n '/^  wait_for_pr:/,/^  publish_crates:/p' "$RELEASE_YML")
check "wait_for_pr grants Actions read access" \
  "$(grep -qE '^      actions: read$' <<< "$WAIT_JOB" && echo present || echo missing)" "present"
# The GitHub expression is intentionally matched literally.
# shellcheck disable=SC2016
check "wait_for_pr uses its least-privilege GITHUB_TOKEN" \
  "$(grep -qF 'GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}' <<< "$WAIT_JOB" && echo correct || echo wrong)" "correct"
check "wait_for_pr invokes the tested waiter script" \
  "$(grep -qF 'run: scripts/wait_for_release_pr.sh' <<< "$WAIT_JOB" && echo correct || echo wrong)" "correct"

MOCK_BIN="$TEST_TMP/bin"
mkdir -p "$MOCK_BIN"

cat > "$MOCK_BIN/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-} ${2:-}" in
  "api graphql")
    printf '%s\n' "$MOCK_PR_JSON"
    ;;
  "run list")
    printf '%s\n' "$MOCK_RUNS_JSON"
    ;;
  *)
    echo "unexpected gh invocation: $*" >&2
    exit 2
    ;;
esac
EOF

cat > "$MOCK_BIN/sleep" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
: > "$MOCK_SLEEP_MARKER"
EOF
chmod +x "$MOCK_BIN/gh" "$MOCK_BIN/sleep"

PR_JSON='{"data":{"repository":{"pullRequest":{"state":"OPEN","mergedAt":null,"mergeStateStatus":"BLOCKED","isInMergeQueue":false,"baseRefName":"main","url":"https://example.invalid/pr/5084","mergeQueueEntry":null}}}}'
TARGET_FAILURE_URL="https://example.invalid/runs/target-failure"
UNRELATED_FAILURE_URL="https://example.invalid/runs/unrelated-failure"
OLDER_SUCCESS_URL="https://example.invalid/runs/older-success"

run_wait_block() {
    local runs_json="$1" output_file="$2" sleep_marker="$3"
    local status
    set +e
    PATH="$MOCK_BIN:$PATH" \
      GITHUB_REPOSITORY="freenet/freenet-core" \
      PR_NUMBER="5084" \
      MAX_WAIT="1" \
      WAIT_INTERVAL="1" \
      MOCK_PR_JSON="$PR_JSON" \
      MOCK_RUNS_JSON="$runs_json" \
      MOCK_SLEEP_MARKER="$sleep_marker" \
      bash --noprofile --norc "$WAIT_SCRIPT" > "$output_file" 2>&1
    status=$?
    set -e
    echo "$status"
}

FAIL_RUNS=$(printf '[
  {"headBranch":"gh-readonly-queue/main/pr-9999-aaaa","status":"completed","conclusion":"failure","url":"%s","createdAt":"2026-07-31T14:03:00Z"},
  {"headBranch":"gh-readonly-queue/main/pr-5084-bbbb","status":"completed","conclusion":"failure","url":"%s","createdAt":"2026-07-31T14:02:00Z"},
  {"headBranch":"gh-readonly-queue/main/pr-5084-aaaa","status":"completed","conclusion":"success","url":"%s","createdAt":"2026-07-31T14:01:00Z"}
]' "$UNRELATED_FAILURE_URL" "$TARGET_FAILURE_URL" "$OLDER_SUCCESS_URL")

FAIL_OUTPUT="$TEST_TMP/failure-output.txt"
FAIL_SLEEP_MARKER="$TEST_TMP/failure-slept"
FAIL_STATUS=$(run_wait_block "$FAIL_RUNS" "$FAIL_OUTPUT" "$FAIL_SLEEP_MARKER")

check "failed merge-group CI exits nonzero" \
  "$([ "$FAIL_STATUS" -ne 0 ] && echo failed || echo passed)" "failed"
check "failed merge-group CI surfaces its run URL" \
  "$(grep -qF "$TARGET_FAILURE_URL" "$FAIL_OUTPUT" && echo present || echo missing)" "present"
check "merge-group lookup excludes unrelated PR runs" \
  "$(grep -qF "$UNRELATED_FAILURE_URL" "$FAIL_OUTPUT" && echo leaked || echo excluded)" "excluded"
check "failed merge-group CI stops before sleeping" \
  "$([ -e "$FAIL_SLEEP_MARKER" ] && echo slept || echo immediate)" "immediate"

# A newer attempt suppresses an older failure even when GitHub reuses the same
# synthetic branch name. This is the shape PR #5083 had after it was requeued.
IN_PROGRESS_URL="https://example.invalid/runs/new-attempt"
STALE_FAILURE_URL="https://example.invalid/runs/stale-failure"
REQUEUE_RUNS=$(printf '[
  {"headBranch":"gh-readonly-queue/main/pr-5084-aaaa","status":"in_progress","conclusion":"","url":"%s","createdAt":"2026-07-31T14:04:00Z"},
  {"headBranch":"gh-readonly-queue/main/pr-5084-aaaa","status":"completed","conclusion":"failure","url":"%s","createdAt":"2026-07-31T14:02:00Z"}
]' "$IN_PROGRESS_URL" "$STALE_FAILURE_URL")

REQUEUE_OUTPUT="$TEST_TMP/requeue-output.txt"
REQUEUE_SLEEP_MARKER="$TEST_TMP/requeue-slept"
REQUEUE_STATUS=$(run_wait_block "$REQUEUE_RUNS" "$REQUEUE_OUTPUT" "$REQUEUE_SLEEP_MARKER")

check "newer in-progress attempt suppresses stale failure" \
  "$(grep -qF 'stopping instead of waiting' "$REQUEUE_OUTPUT" && echo false-fail || echo ignored)" "ignored"
check "nonterminal attempt exits nonzero at timeout" \
  "$([ "$REQUEUE_STATUS" -ne 0 ] && echo failed || echo passed)" "failed"
check "timeout reports merge and queue state" \
  "$(grep -qF 'mergeStateStatus=BLOCKED, inMergeQueue=false' "$REQUEUE_OUTPUT" && echo present || echo missing)" "present"
check "timeout links the newest merge-group CI run" \
  "$(grep -qF "$IN_PROGRESS_URL" "$REQUEUE_OUTPUT" && echo present || echo missing)" "present"
check "nonterminal attempt reaches the bounded timeout" \
  "$(grep -qF 'Timeout after 1s' "$REQUEUE_OUTPUT" && echo timed-out || echo missing)" "timed-out"
check "nonterminal attempt polls again" \
  "$([ -e "$REQUEUE_SLEEP_MARKER" ] && echo slept || echo no-sleep)" "slept"

if [ "$FAILURES" -eq 0 ]; then
    echo "PASS: release.yml is merge-queue-compatible and reports failed merge-group CI."
else
    echo "$FAILURES check(s) failed" >&2
    exit 1
fi
