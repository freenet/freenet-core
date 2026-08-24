#!/usr/bin/env bash
# Regression tests for release workflow interactions with the merge queue.
#
# release.yml's `update_versions` job auto-merges the version-bump PR with
# `gh pr merge ... --squash --auto`. The repo's merge queue (strict-strategy
# enforcement enabled between v0.2.92 and v0.2.93) OWNS the merge strategy and
# rejects an explicit `--squash`, so `gh pr merge` exits non-zero. Under
# `shell: bash -e` that fails the step, fails the `update_versions` job, and
# skips the ENTIRE publish cascade (wait_for_pr -> verify_publishable -> tag ->
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
WAIT_JOB=$(sed -n '/^  wait_for_pr:/,/^  verify_publishable:/p' "$RELEASE_YML")
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
    for expected in \
      '-F owner=freenet' \
      '-F repo=freenet-core' \
      '-F number=5084' \
      'mergeQueueEntry{state position enqueuedAt}'; do
      if [[ "$*" != *"$expected"* ]]; then
        echo "missing expected GraphQL argument: $expected" >&2
        exit 2
      fi
    done
    count=0
    if [ -s "$MOCK_API_COUNT" ]; then
      count=$(< "$MOCK_API_COUNT")
    fi
    count=$((count + 1))
    printf '%s\n' "$count" > "$MOCK_API_COUNT"
    if [ "$count" -gt 1 ] && [ -n "${MOCK_CONFIRM_PR_JSON:-}" ]; then
      printf '%s\n' "$MOCK_CONFIRM_PR_JSON"
    else
      printf '%s\n' "$MOCK_PR_JSON"
    fi
    ;;
  "run list")
    expected='run list --repo freenet/freenet-core --event merge_group --limit 100 --json workflowName,headBranch,status,conclusion,url,createdAt'
    if [ "$*" != "$expected" ]; then
      echo "unexpected run-list arguments: $*" >&2
      exit 2
    fi
    count=0
    if [ -s "$MOCK_RUN_LIST_COUNT" ]; then
      count=$(< "$MOCK_RUN_LIST_COUNT")
    fi
    count=$((count + 1))
    printf '%s\n' "$count" > "$MOCK_RUN_LIST_COUNT"
    if [ "$count" -gt 1 ] && [ -n "${MOCK_CONFIRM_RUNS_JSON:-}" ]; then
      printf '%s\n' "$MOCK_CONFIRM_RUNS_JSON"
    else
      printf '%s\n' "$MOCK_RUNS_JSON"
    fi
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
QUEUED_PR_JSON='{"data":{"repository":{"pullRequest":{"state":"OPEN","mergedAt":null,"mergeStateStatus":"BLOCKED","isInMergeQueue":true,"baseRefName":"main","url":"https://example.invalid/pr/5084","mergeQueueEntry":{"state":"AWAITING_CHECKS","position":2,"enqueuedAt":"2026-07-31T14:05:00Z"}}}}}'
TARGET_FAILURE_URL="https://example.invalid/runs/target-failure"
UNRELATED_FAILURE_URL="https://example.invalid/runs/unrelated-failure"
OLDER_SUCCESS_URL="https://example.invalid/runs/older-success"

run_wait_block() {
    local runs_json="$1" output_file="$2" sleep_marker="$3"
    local pr_json="${4:-$PR_JSON}" confirm_runs_json="${5:-}" confirm_pr_json="${6:-}"
    local run_list_count="${output_file}.run-list-count"
    local api_count="${output_file}.api-count"
    local status
    set +e
    PATH="$MOCK_BIN:$PATH" \
      GITHUB_REPOSITORY="freenet/freenet-core" \
      PR_NUMBER="5084" \
      MAX_WAIT="1" \
      WAIT_INTERVAL="1" \
      MOCK_PR_JSON="$pr_json" \
      MOCK_CONFIRM_PR_JSON="$confirm_pr_json" \
      MOCK_RUNS_JSON="$runs_json" \
      MOCK_CONFIRM_RUNS_JSON="$confirm_runs_json" \
      MOCK_API_COUNT="$api_count" \
      MOCK_RUN_LIST_COUNT="$run_list_count" \
      MOCK_SLEEP_MARKER="$sleep_marker" \
      bash --noprofile --norc "$WAIT_SCRIPT" > "$output_file" 2>&1
    status=$?
    set -e
    echo "$status"
}

FAIL_RUNS=$(printf '[
  {"workflowName":"CI","headBranch":"gh-readonly-queue/main/pr-9999-aaaa","status":"completed","conclusion":"failure","url":"%s","createdAt":"2026-07-31T14:03:00Z"},
  {"workflowName":"CI","headBranch":"gh-readonly-queue/main/pr-5084-bbbb","status":"completed","conclusion":"failure","url":"%s","createdAt":"2026-07-31T14:02:00Z"},
  {"workflowName":"CI","headBranch":"gh-readonly-queue/main/pr-5084-aaaa","status":"completed","conclusion":"success","url":"%s","createdAt":"2026-07-31T14:01:00Z"}
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

# Required merge-group checks span multiple workflows, and any blocking
# terminal conclusion must be actionable rather than becoming a timeout.
MACOS_TIMEOUT_URL="https://example.invalid/runs/macos-timeout"
MULTI_WORKFLOW_RUNS=$(printf '[
  {"workflowName":"macOS DMG-swap E2E","headBranch":"gh-readonly-queue/main/pr-5084-bbbb","status":"completed","conclusion":"timed_out","url":"%s","createdAt":"2026-07-31T14:06:00Z"},
  {"workflowName":"CI","headBranch":"gh-readonly-queue/main/pr-5084-bbbb","status":"completed","conclusion":"success","url":"https://example.invalid/runs/ci-success","createdAt":"2026-07-31T14:05:00Z"}
]' "$MACOS_TIMEOUT_URL")
MULTI_OUTPUT="$TEST_TMP/multi-workflow-output.txt"
MULTI_SLEEP_MARKER="$TEST_TMP/multi-workflow-slept"
MULTI_STATUS=$(run_wait_block "$MULTI_WORKFLOW_RUNS" "$MULTI_OUTPUT" "$MULTI_SLEEP_MARKER")

check "blocking conclusion in another required workflow exits nonzero" \
  "$([ "$MULTI_STATUS" -ne 0 ] && echo failed || echo passed)" "failed"
check "non-CI workflow failure reports workflow, conclusion, and URL" \
  "$(grep -qF 'macOS DMG-swap E2E concluded timed_out' "$MULTI_OUTPUT" && grep -qF "$MACOS_TIMEOUT_URL" "$MULTI_OUTPUT" && echo present || echo missing)" "present"
check "blocking terminal conclusion stops before sleeping" \
  "$([ -e "$MULTI_SLEEP_MARKER" ] && echo slept || echo immediate)" "immediate"

# A requeue can happen after the initial PR fetch but before failure
# confirmation. The confirming PR state must win that race.
CONFIRM_REQUEUE_OUTPUT="$TEST_TMP/confirm-requeue-output.txt"
CONFIRM_REQUEUE_SLEEP_MARKER="$TEST_TMP/confirm-requeue-slept"
CONFIRM_REQUEUE_STATUS=$(run_wait_block \
  "$FAIL_RUNS" "$CONFIRM_REQUEUE_OUTPUT" "$CONFIRM_REQUEUE_SLEEP_MARKER" "$PR_JSON" "$FAIL_RUNS" "$QUEUED_PR_JSON")

check "queue entry created during confirmation suppresses stale failure" \
  "$(grep -qF 'Blocking run predates a queue entry created during confirmation' "$CONFIRM_REQUEUE_OUTPUT" && echo ignored || echo terminal)" "ignored"
check "confirmation requeue race reaches bounded timeout" \
  "$([ "$CONFIRM_REQUEUE_STATUS" -ne 0 ] && [ -e "$CONFIRM_REQUEUE_SLEEP_MARKER" ] && echo timed-out || echo stopped)" "timed-out"

# A transient PR API failure during confirmation cannot safely prove that the
# pull request is still unmerged, so it must retry instead of failing.
TRANSIENT_OUTPUT="$TEST_TMP/transient-confirm-output.txt"
TRANSIENT_SLEEP_MARKER="$TEST_TMP/transient-confirm-slept"
TRANSIENT_STATUS=$(run_wait_block \
  "$FAIL_RUNS" "$TRANSIENT_OUTPUT" "$TRANSIENT_SLEEP_MARKER" "$PR_JSON" "$FAIL_RUNS" 'not-json')

check "transient confirmation API failure does not report terminal failure" \
  "$(grep -qF 'PR state was unavailable while confirming' "$TRANSIENT_OUTPUT" && ! grep -qF 'stopping instead of waiting' "$TRANSIENT_OUTPUT" && echo retried || echo terminal)" "retried"
check "transient confirmation API failure reaches bounded timeout" \
  "$([ "$TRANSIENT_STATUS" -ne 0 ] && [ -e "$TRANSIENT_SLEEP_MARKER" ] && echo timed-out || echo stopped)" "timed-out"

# A newer attempt suppresses an older failure even when GitHub reuses the same
# synthetic branch name. This is the shape PR #5083 had after it was requeued.
IN_PROGRESS_URL="https://example.invalid/runs/new-attempt"
STALE_FAILURE_URL="https://example.invalid/runs/stale-failure"
REQUEUE_RUNS=$(printf '[
  {"workflowName":"CI","headBranch":"gh-readonly-queue/main/pr-5084-aaaa","status":"in_progress","conclusion":"","url":"%s","createdAt":"2026-07-31T14:04:00Z"},
  {"workflowName":"CI","headBranch":"gh-readonly-queue/main/pr-5084-aaaa","status":"completed","conclusion":"failure","url":"%s","createdAt":"2026-07-31T14:02:00Z"}
]' "$IN_PROGRESS_URL" "$STALE_FAILURE_URL")

# A second lookup is required before a failure is terminal. GitHub can expose a
# newer attempt between the first lookup and that confirmation lookup.
CONFIRM_RACE_OUTPUT="$TEST_TMP/confirm-race-output.txt"
CONFIRM_RACE_SLEEP_MARKER="$TEST_TMP/confirm-race-slept"
CONFIRM_RACE_STATUS=$(run_wait_block \
  "$FAIL_RUNS" "$CONFIRM_RACE_OUTPUT" "$CONFIRM_RACE_SLEEP_MARKER" "$PR_JSON" "$REQUEUE_RUNS")

check "confirmation lookup notices a newer attempt" \
  "$(grep -qF 'A newer CI attempt appeared' "$CONFIRM_RACE_OUTPUT" && echo noticed || echo missed)" "noticed"
check "confirmation race does not report terminal failure" \
  "$(grep -qF 'stopping instead of waiting' "$CONFIRM_RACE_OUTPUT" && echo false-fail || echo continued)" "continued"
check "confirmation race reaches the bounded timeout" \
  "$([ "$CONFIRM_RACE_STATUS" -ne 0 ] && [ -e "$CONFIRM_RACE_SLEEP_MARKER" ] && echo timed-out || echo stopped)" "timed-out"

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

# Immediately after a requeue, the previous failed run can remain newest until
# GitHub creates the current queue entry's run. Its timestamp must keep it from
# terminating the new attempt during that gap.
STALE_ONLY_RUNS=$(printf '[
  {"workflowName":"CI","headBranch":"gh-readonly-queue/main/pr-5084-aaaa","status":"completed","conclusion":"failure","url":"%s","createdAt":"2026-07-31T14:02:00Z"}
]' "$STALE_FAILURE_URL")
PRE_RUN_OUTPUT="$TEST_TMP/pre-run-output.txt"
PRE_RUN_SLEEP_MARKER="$TEST_TMP/pre-run-slept"
PRE_RUN_STATUS=$(run_wait_block "$STALE_ONLY_RUNS" "$PRE_RUN_OUTPUT" "$PRE_RUN_SLEEP_MARKER" "$QUEUED_PR_JSON")

check "failed run before current enqueue does not terminate the requeue" \
  "$(grep -qF 'Blocking run predates the current queue entry' "$PRE_RUN_OUTPUT" && echo ignored || echo terminal)" "ignored"
check "pre-run requeue gap reaches the bounded timeout" \
  "$([ "$PRE_RUN_STATUS" -ne 0 ] && [ -e "$PRE_RUN_SLEEP_MARKER" ] && echo timed-out || echo stopped)" "timed-out"
check "queued timeout reports queue position and enqueue time" \
  "$(grep -qF 'queue=state=AWAITING_CHECKS, position=2, enqueuedAt=2026-07-31T14:05:00Z' "$PRE_RUN_OUTPUT" && echo present || echo missing)" "present"

# ---------------------------------------------------------------------------
# #5233: the release must be cut from ONE immutable commit.
#
# `verify_publishable` (then named `publish_crates`) and `create_release` used
# to check out `ref: main` and then
# `git pull origin main`. Both run AFTER `wait_for_pr`, which blocks ~20 minutes
# on the merge queue, so any commit that landed on main during that window was
# silently published and tagged. v0.2.122 shipped an unrelated commit this way.
# ---------------------------------------------------------------------------

RESOLVE_SCRIPT="$SCRIPT_DIR/resolve_release_sha.sh"
VERIFY_SCRIPT="$SCRIPT_DIR/verify_release_checkout.sh"

# Job bodies, bounded to the job. An unbounded grep over the whole file would
# match a neighbouring job and pass vacuously.
PUBLISH_JOB=$(sed -n '/^  verify_publishable:/,/^  create_release:/p' "$RELEASE_YML")
CREATE_RELEASE_JOB=$(sed -n '/^  create_release:/,/^  summary:/p' "$RELEASE_YML")

check "verify_publishable job body was located" \
  "$([ -n "$PUBLISH_JOB" ] && echo present || echo missing)" "present"
check "create_release job body was located" \
  "$([ -n "$CREATE_RELEASE_JOB" ] && echo present || echo missing)" "present"

# A sed range whose closing anchor is renamed silently runs to EOF, so the
# job-scoped pins below would start matching neighbouring jobs and pass
# vacuously. Assert each range actually terminated on its anchor.
check "verify_publishable range is bounded by the next job" \
  "$([ "$(tail -n1 <<< "$PUBLISH_JOB")" = "  create_release:" ] && echo bounded || echo ran-to-eof)" "bounded"
check "create_release range is bounded by the next job" \
  "$([ "$(tail -n1 <<< "$CREATE_RELEASE_JOB")" = "  summary:" ] && echo bounded || echo ran-to-eof)" "bounded"
# WAIT_JOB is extracted by the pre-existing merge-queue checks above and needs
# the same guard: renaming verify_publishable silently widens it to EOF, after
# which every wait_for_pr pin starts matching text from other jobs.
check "wait_for_pr range is bounded by the next job" \
  "$([ "$(tail -n1 <<< "$WAIT_JOB")" = "  verify_publishable:" ] && echo bounded || echo ran-to-eof)" "bounded"

# The bug itself: no job may resolve a moving reference.
#
# Every needle below is matched against EXECUTABLE lines (`^[^#]*`) or in a
# whole-line anchored form. A bare `grep -qF` substring would be satisfied by
# `# TODO: re-enable <the thing>`, so commenting a guard out would keep this
# suite green — which is exactly the regression these pins exist to catch.
# `github.ref` is included because on `workflow_dispatch` it IS `refs/heads/main`,
# so `ref: ${{ github.ref }}` is the same bug wearing an expression.
check "release.yml never checks out a moving 'main' reference" \
  "$(grep -qE "^[[:space:]]*ref:[[:space:]]*(['\"]?(main|refs/heads/main)['\"]?|\\\$\\{\\{[[:space:]]*github\\.ref[[:space:]]*\\}\\})[[:space:]]*(#.*)?$" "$RELEASE_YML" && echo moving-ref || echo pinned)" "pinned"

# `git pull origin main` was the original second half of the bug, but ANY
# shell-level re-pointing of the checkout reopens the same window — and does so
# where the runtime verifier, which has already run, cannot see it.
for moving_cmd in \
    'git pull[[:space:]]' \
    'git checkout[[:space:]]+(-[^[:space:]]+[[:space:]]+)*main' \
    'git reset[[:space:]]+--hard' \
    'git merge[[:space:]]+origin/main'; do
    check "release.yml never re-points the checkout with '$moving_cmd'" \
      "$(grep -qE "^[^#]*${moving_cmd}" "$RELEASE_YML" && echo moves || echo pinned)" "pinned"
done

# The pin has to come from somewhere: wait_for_pr resolves it once and exports it.
# The GitHub expressions below are intentionally matched literally.
# shellcheck disable=SC2016
check "wait_for_pr exports the resolved release SHA" \
  "$(grep -qE '^[[:space:]]*release_sha: \$\{\{ steps\.release_sha\.outputs\.sha \}\}[[:space:]]*$' <<< "$WAIT_JOB" && echo present || echo missing)" "present"
check "wait_for_pr invokes the tested resolver script" \
  "$(grep -qE '^[[:space:]]*run: (bash )?scripts/resolve_release_sha\.sh[[:space:]]*$' <<< "$WAIT_JOB" && echo present || echo missing)" "present"

# Both downstream jobs must consume that SHA and re-check it after checkout.
#
# The GUARDED act each job must never reach unverified. For `create_release`
# that act is still irreversible: pushing the tag. For `verify_publishable` it
# no longer is -- the real `cargo publish` moved to cross-compile.yml's
# `attach-to-release`, downstream of the blocking pre-flight canary, and what
# remains here is `cargo publish --dry-run`.
#
# The check is KEPT for it anyway, and deliberately so. A dry run exists to
# predict whether the real publish will succeed, and it can only predict that
# for the tree it actually ran against: performed on a tree that drifted off
# the pinned release commit, it reports publishability for something else and
# the prediction is worthless. The needle is unchanged (`cargo publish` matches
# the dry run too), so this is a rename of the LABEL to something true, not a
# relaxation of the assertion.
#
# The real irreversible act's ordering is pinned where it now lives, in
# scripts/release_canary_wiring_test.sh (assertions 2b and 2c): the crates.io
# publish must sit between Gate A and the un-draft, and release.yml must
# contain no non-dry-run `cargo publish` at all. The coverage moved files
# along with the step it covers; nothing is left unpinned.
#
# The regex needles below are intentionally literal shell text, not expansions.
# shellcheck disable=SC2016
for job_name in verify_publishable create_release; do
    case "$job_name" in
      verify_publishable) JOB_BODY="$PUBLISH_JOB"; GUARDED='cargo publish'
                          GUARDED_DESC='dry-run publish' ;;
      create_release)     JOB_BODY="$CREATE_RELEASE_JOB"; GUARDED='git push origin "v\$VERSION"'
                          GUARDED_DESC='irreversible step' ;;
    esac

    check "$job_name checks out needs.wait_for_pr.outputs.release_sha" \
      "$(grep -qE '^[[:space:]]*ref: \$\{\{ needs\.wait_for_pr\.outputs\.release_sha' <<< "$JOB_BODY" && echo pinned || echo missing)" "pinned"
    check "$job_name verifies the checkout" \
      "$(grep -qE '^[[:space:]]*run: (bash )?scripts/verify_release_checkout\.sh[[:space:]]*$' <<< "$JOB_BODY" && echo present || echo missing)" "present"
    check "$job_name exports RELEASE_SHA for the verifier" \
      "$(grep -qE '^[[:space:]]*RELEASE_SHA: \$\{\{ needs\.wait_for_pr\.outputs\.release_sha' <<< "$JOB_BODY" && echo present || echo missing)" "present"
    # Without this the verifier degenerates to comparing the checkout ref
    # against itself, which cannot fail.
    check "$job_name gives the verifier an independent version oracle" \
      "$(grep -qE '^[[:space:]]*EXPECTED_VERSION: \$\{\{ needs\.validate\.outputs\.version \}\}[[:space:]]*$' <<< "$JOB_BODY" && echo present || echo missing)" "present"
    # Lets the verifier distinguish the `|| github.sha` fallback from a
    # genuinely wrong tree.
    check "$job_name tells the verifier whether a SHA was actually resolved" \
      "$(grep -qE '^[[:space:]]*RELEASE_SHA_RESOLVED: \$\{\{ needs\.wait_for_pr\.outputs\.release_sha \}\}[[:space:]]*$' <<< "$JOB_BODY" && echo present || echo missing)" "present"

    # Presence is not enough. A guard placed AFTER the guarded step verifies
    # nothing — for create_release the tag has already been pushed, and for
    # verify_publishable the dry run has already reported on whatever tree was
    # checked out — so assert ORDER. Line numbers are relative to the extracted
    # job body.
    VERIFY_LINE=$(grep -nE '^[[:space:]]*run: (bash )?scripts/verify_release_checkout\.sh[[:space:]]*$' <<< "$JOB_BODY" | head -1 | cut -d: -f1 || true)
    CHECKOUT_LINE=$(grep -nE '^[[:space:]]*ref: \$\{\{ needs\.wait_for_pr\.outputs\.release_sha' <<< "$JOB_BODY" | head -1 | cut -d: -f1 || true)
    GUARDED_LINE=$(grep -nE "^[^#]*${GUARDED}" <<< "$JOB_BODY" | head -1 | cut -d: -f1 || true)

    check "$job_name's $GUARDED_DESC was located" \
      "$([ -n "$GUARDED_LINE" ] && echo found || echo missing)" "found"
    check "$job_name verifies BEFORE its $GUARDED_DESC, not after" \
      "$([ -n "$VERIFY_LINE" ] && [ -n "$GUARDED_LINE" ] && [ "$VERIFY_LINE" -lt "$GUARDED_LINE" ] && echo before || echo after)" "before"
    check "$job_name verifies AFTER the checkout, not before" \
      "$([ -n "$VERIFY_LINE" ] && [ -n "$CHECKOUT_LINE" ] && [ "$VERIFY_LINE" -gt "$CHECKOUT_LINE" ] && echo after || echo before)" "after"
done

# create_release can only read the output if wait_for_pr is in its needs.
check "create_release depends on wait_for_pr so the SHA is in scope" \
  "$(grep -qE '^    needs: \[validate, wait_for_pr, verify_publishable\]$' <<< "$CREATE_RELEASE_JOB" && echo present || echo missing)" "present"

# The notes must describe the commit being released, not whatever main is now.
# The shell variable reference is intentionally matched literally.
# shellcheck disable=SC2016
check "release notes are generated for the pinned commit" \
  "$(grep -qE '^[^#]*-f target_commitish="\$RELEASE_SHA"' <<< "$CREATE_RELEASE_JOB" && echo pinned || echo missing)" "pinned"
check "release notes never fall back to target_commitish=main" \
  "$(grep -qE '^[^#]*-f target_commitish=main' <<< "$CREATE_RELEASE_JOB" && echo moving || echo pinned)" "pinned"

# --- behavioural: scripts/verify_release_checkout.sh -------------------------

RELEASE_VERSION="0.2.123"
VERIFY_REPO="$TEST_TMP/verify-repo"
mkdir -p "$VERIFY_REPO/crates/core"
printf '[package]\nname = "freenet"\nversion = "%s"\n' "$RELEASE_VERSION" \
  > "$VERIFY_REPO/crates/core/Cargo.toml"
(
  cd "$VERIFY_REPO"
  git init -q .
  git add crates/core/Cargo.toml
  git -c user.email=t@example.invalid -c user.name=t commit -q -m "build: release $RELEASE_VERSION"
) >/dev/null 2>&1
VERIFY_HEAD=$(cd "$VERIFY_REPO" && git rev-parse HEAD)

run_verify() {
    local expected_sha="$1" expected_version="$2" output_file="$3" status
    set +e
    ( cd "$VERIFY_REPO" && RELEASE_SHA="$expected_sha" EXPECTED_VERSION="$expected_version" \
        RELEASE_SHA_RESOLVED="$expected_sha" \
        bash --noprofile --norc "$VERIFY_SCRIPT" ) > "$output_file" 2>&1
    status=$?
    set -e
    echo "$status"
}

VERIFY_OK_OUTPUT="$TEST_TMP/verify-ok.txt"
check "verifier accepts the pinned commit carrying the release version" \
  "$(run_verify "$VERIFY_HEAD" "$RELEASE_VERSION" "$VERIFY_OK_OUTPUT")" "0"

VERIFY_DRIFT_OUTPUT="$TEST_TMP/verify-drift.txt"
DRIFT_SHA="0123456789012345678901234567890123456789"
check "verifier rejects a checkout that drifted off the pinned commit" \
  "$([ "$(run_verify "$DRIFT_SHA" "$RELEASE_VERSION" "$VERIFY_DRIFT_OUTPUT")" -ne 0 ] && echo failed || echo passed)" "failed"
check "verifier names both SHAs when it rejects" \
  "$(grep -qF "$DRIFT_SHA" "$VERIFY_DRIFT_OUTPUT" && grep -qF "$VERIFY_HEAD" "$VERIFY_DRIFT_OUTPUT" && echo present || echo missing)" "present"

# The oracle that is NOT a restatement of the checkout ref: the `|| github.sha`
# fallback would put us on the launch commit, whose manifest still carries the
# PREVIOUS version. HEAD would match RELEASE_SHA and only this check fires.
VERIFY_WRONG_TREE_OUTPUT="$TEST_TMP/verify-wrong-tree.txt"
check "verifier rejects the right SHA carrying the wrong version" \
  "$([ "$(run_verify "$VERIFY_HEAD" "0.2.122" "$VERIFY_WRONG_TREE_OUTPUT")" -ne 0 ] && echo failed || echo passed)" "failed"
check "verifier explains which version it found versus expected" \
  "$(grep -qF "declares version '$RELEASE_VERSION'" "$VERIFY_WRONG_TREE_OUTPUT" && grep -qF 'this release is 0.2.122' "$VERIFY_WRONG_TREE_OUTPUT" && echo present || echo missing)" "present"

# The `|| github.sha` fallback path: wait_for_pr resolved nothing, so the job
# is sitting on the launch commit. The failure must name THAT, not report a
# version mismatch and send the operator to inspect the tree.
VERIFY_FALLBACK_OUTPUT="$TEST_TMP/verify-fallback.txt"
set +e
( cd "$VERIFY_REPO" && RELEASE_SHA="$VERIFY_HEAD" EXPECTED_VERSION="$RELEASE_VERSION" \
    RELEASE_SHA_RESOLVED="" bash --noprofile --norc "$VERIFY_SCRIPT" ) \
  > "$VERIFY_FALLBACK_OUTPUT" 2>&1
VERIFY_FALLBACK_STATUS=$?
set -e
check "verifier refuses to act when no release commit was resolved" \
  "$([ "$VERIFY_FALLBACK_STATUS" -ne 0 ] && echo failed || echo passed)" "failed"
check "verifier names wait_for_pr as the cause, not the tree" \
  "$(grep -qF 'wait_for_pr did not resolve a release commit' "$VERIFY_FALLBACK_OUTPUT" && echo correct || echo misattributed)" "correct"

# A manifest with no `^version = "` line must produce the explanatory error,
# not die silently inside the pipeline that reads it.
VERIFY_NOVER_REPO="$TEST_TMP/verify-noversion-repo"
mkdir -p "$VERIFY_NOVER_REPO/crates/core"
printf '[package]\nname = "freenet"\nversion.workspace = true\n' \
  > "$VERIFY_NOVER_REPO/crates/core/Cargo.toml"
(
  cd "$VERIFY_NOVER_REPO"
  git init -q .
  git add crates/core/Cargo.toml
  git -c user.email=t@example.invalid -c user.name=t commit -q -m "workspace-inherited version"
) >/dev/null 2>&1
VERIFY_NOVER_HEAD=$(cd "$VERIFY_NOVER_REPO" && git rev-parse HEAD)
VERIFY_NOVER_OUTPUT="$TEST_TMP/verify-noversion.txt"
set +e
( cd "$VERIFY_NOVER_REPO" && RELEASE_SHA="$VERIFY_NOVER_HEAD" EXPECTED_VERSION="$RELEASE_VERSION" \
    RELEASE_SHA_RESOLVED="$VERIFY_NOVER_HEAD" bash --noprofile --norc "$VERIFY_SCRIPT" ) \
  > "$VERIFY_NOVER_OUTPUT" 2>&1
VERIFY_NOVER_STATUS=$?
set -e
check "verifier fails when the manifest has no readable version" \
  "$([ "$VERIFY_NOVER_STATUS" -ne 0 ] && echo failed || echo passed)" "failed"
check "verifier explains itself rather than dying silently in a pipeline" \
  "$(grep -qF '::error' "$VERIFY_NOVER_OUTPUT" && echo explained || echo silent)" "explained"

VERIFY_NO_MANIFEST_OUTPUT="$TEST_TMP/verify-no-manifest.txt"
set +e
( cd "$VERIFY_REPO" && RELEASE_SHA="$VERIFY_HEAD" EXPECTED_VERSION="$RELEASE_VERSION" \
    RELEASE_SHA_RESOLVED="$VERIFY_HEAD" \
    MANIFEST="crates/core/Absent.toml" bash --noprofile --norc "$VERIFY_SCRIPT" ) \
  > "$VERIFY_NO_MANIFEST_OUTPUT" 2>&1
VERIFY_NO_MANIFEST_STATUS=$?
set -e
check "verifier rejects a checkout with no freenet manifest" \
  "$([ "$VERIFY_NO_MANIFEST_STATUS" -ne 0 ] && echo failed || echo passed)" "failed"

for missing_var in RELEASE_SHA EXPECTED_VERSION; do
    VERIFY_UNSET_OUTPUT="$TEST_TMP/verify-unset-$missing_var.txt"
    set +e
    ( cd "$VERIFY_REPO" && RELEASE_SHA="$VERIFY_HEAD" EXPECTED_VERSION="$RELEASE_VERSION" \
        RELEASE_SHA_RESOLVED="$VERIFY_HEAD" \
        env -u "$missing_var" bash --noprofile --norc "$VERIFY_SCRIPT" ) \
      > "$VERIFY_UNSET_OUTPUT" 2>&1
    VERIFY_UNSET_STATUS=$?
    set -e
    check "verifier fails closed when $missing_var is unset" \
      "$([ "$VERIFY_UNSET_STATUS" -ne 0 ] && echo failed || echo passed)" "failed"
done

# --- behavioural: scripts/resolve_release_sha.sh -----------------------------

RESOLVE_BIN="$TEST_TMP/resolve-bin"
mkdir -p "$RESOLVE_BIN"
# The mock asserts the REAL invocation shape. Without this a typo in
# `--json mergeCommit` or in the jq filter would never turn the suite red — it
# would only fail closed in production, mid-release.
cat > "$RESOLVE_BIN/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  *"pr view"*)
    for expected in \
      '--repo freenet/freenet-core' \
      '--json mergeCommit' \
      '--jq .mergeCommit.oid // empty'; do
      if [[ "$*" != *"$expected"* ]]; then
        echo "missing expected gh pr view argument: $expected" >&2
        exit 3
      fi
    done
    # Fail the first N calls to exercise the retry path.
    calls=0
    if [ -s "$MOCK_RESOLVE_CALLS" ]; then calls=$(< "$MOCK_RESOLVE_CALLS"); fi
    calls=$((calls + 1))
    printf '%s\n' "$calls" > "$MOCK_RESOLVE_CALLS"
    if [ "$calls" -le "${MOCK_FAIL_FIRST:-0}" ]; then
      echo "simulated transient API failure" >&2
      exit 1
    fi
    # Real `gh pr view --json mergeCommit --jq '.mergeCommit.oid // empty'` on
    # an UNMERGED PR exits 0 and prints nothing. Modelling that as a non-zero
    # exit would route the test down the API-failure branch and leave the
    # PR-has-no-merge-commit branch with no coverage at all.
    if [ "${MOCK_EMPTY_OID:-0}" = "1" ]; then
      exit 0
    fi
    [ -n "${MOCK_MERGE_SHA:-}" ] || exit 2
    printf '%s\n' "$MOCK_MERGE_SHA"
    ;;
  *compare*)
    if [ "${MOCK_COMPARE_FAILS:-0}" = "1" ]; then
      echo "simulated compare API failure" >&2
      exit 1
    fi
    printf '%s' "${MOCK_COMPARE:-}"
    ;;
  *)
    echo "unexpected gh invocation: $*" >&2
    exit 2
    ;;
esac
EOF
chmod +x "$RESOLVE_BIN/gh"

MERGED_SHA="1111111111111111111111111111111111111111"
LAUNCHED_SHA="2222222222222222222222222222222222222222"

run_resolve() {
    # run_resolve <merge_sha> <compare_body> <output_file> <github_output_file>
    #             [fail_first] [attempts] [empty_oid] [compare_fails]
    local merge_sha="$1" compare="$2" output_file="$3" gh_output="$4"
    local fail_first="${5:-0}" attempts="${6:-1}"
    local empty_oid="${7:-0}" compare_fails="${8:-0}" status
    : > "$gh_output"
    : > "${gh_output}.calls"
    set +e
    PATH="$RESOLVE_BIN:$PATH" \
      GITHUB_REPOSITORY="freenet/freenet-core" \
      PR_NUMBER="5231" \
      LAUNCH_SHA="$LAUNCHED_SHA" \
      GITHUB_OUTPUT="$gh_output" \
      MOCK_MERGE_SHA="$merge_sha" \
      MOCK_COMPARE="$compare" \
      MOCK_FAIL_FIRST="$fail_first" \
      MOCK_EMPTY_OID="$empty_oid" \
      MOCK_COMPARE_FAILS="$compare_fails" \
      MOCK_RESOLVE_CALLS="${gh_output}.calls" \
      RESOLVE_ATTEMPTS="$attempts" \
      RESOLVE_RETRY_INTERVAL="0" \
      bash --noprofile --norc "$RESOLVE_SCRIPT" > "$output_file" 2>&1
    status=$?
    set -e
    echo "$status"
}

BUMP_ONLY=$'  aaaaaaaaa build: release 0.2.123\n'
RACED=$'  aaaaaaaaa build: release 0.2.123\n  bbbbbbbbb fix(diagnostics): unrelated change\n  ccccccccc feat: another unrelated change\n'

RESOLVE_OK_OUTPUT="$TEST_TMP/resolve-ok.txt"
RESOLVE_OK_GH_OUTPUT="$TEST_TMP/resolve-ok.env"
check "resolver exits zero when the bump PR has a merge commit" \
  "$(run_resolve "$MERGED_SHA" "$BUMP_ONLY" "$RESOLVE_OK_OUTPUT" "$RESOLVE_OK_GH_OUTPUT")" "0"
check "resolver exports the merge commit as the release SHA" \
  "$(grep -qF "sha=$MERGED_SHA" "$RESOLVE_OK_GH_OUTPUT" && echo present || echo missing)" "present"
check "resolver stays quiet when only the version bump landed" \
  "$(grep -qF 'main moved during this release' "$RESOLVE_OK_OUTPUT" && echo warned || echo quiet)" "quiet"

RESOLVE_RACE_OUTPUT="$TEST_TMP/resolve-race.txt"
RESOLVE_RACE_GH_OUTPUT="$TEST_TMP/resolve-race.env"
check "resolver still succeeds when main moved" \
  "$(run_resolve "$MERGED_SHA" "$RACED" "$RESOLVE_RACE_OUTPUT" "$RESOLVE_RACE_GH_OUTPUT")" "0"
check "resolver warns loudly when main moved during the release" \
  "$(grep -qF '::warning title=main moved during this release' "$RESOLVE_RACE_OUTPUT" && echo warned || echo silent)" "warned"
check "resolver lists the commits that raced in" \
  "$(grep -qF 'fix(diagnostics): unrelated change' "$RESOLVE_RACE_OUTPUT" && echo listed || echo hidden)" "listed"
check "resolver still pins a single SHA when main moved" \
  "$(grep -qF "sha=$MERGED_SHA" "$RESOLVE_RACE_GH_OUTPUT" && echo present || echo missing)" "present"

# Failing closed is the whole point: a release must never silently fall back to
# a moving reference because the merge commit could not be read.
RESOLVE_FAIL_OUTPUT="$TEST_TMP/resolve-fail.txt"
RESOLVE_FAIL_GH_OUTPUT="$TEST_TMP/resolve-fail.env"
check "resolver fails when the merge commit cannot be resolved" \
  "$([ "$(run_resolve "" "$BUMP_ONLY" "$RESOLVE_FAIL_OUTPUT" "$RESOLVE_FAIL_GH_OUTPUT")" -ne 0 ] && echo failed || echo passed)" "failed"
check "resolver exports no SHA when it cannot resolve one" \
  "$(grep -qF 'sha=' "$RESOLVE_FAIL_GH_OUTPUT" && echo leaked || echo empty)" "empty"

RESOLVE_JUNK_OUTPUT="$TEST_TMP/resolve-junk.txt"
RESOLVE_JUNK_GH_OUTPUT="$TEST_TMP/resolve-junk.env"
check "resolver rejects a non-SHA merge commit rather than checking it out" \
  "$([ "$(run_resolve "main" "$BUMP_ONLY" "$RESOLVE_JUNK_OUTPUT" "$RESOLVE_JUNK_GH_OUTPUT")" -ne 0 ] && echo failed || echo passed)" "failed"

# This step runs after the bump PR has already merged, so a single transient
# API failure must not strand the release half-done.
RESOLVE_RETRY_OUTPUT="$TEST_TMP/resolve-retry.txt"
RESOLVE_RETRY_GH_OUTPUT="$TEST_TMP/resolve-retry.env"
check "resolver survives transient API failures and still resolves" \
  "$(run_resolve "$MERGED_SHA" "$BUMP_ONLY" "$RESOLVE_RETRY_OUTPUT" "$RESOLVE_RETRY_GH_OUTPUT" 2 4)" "0"
check "resolver exports the SHA it eventually resolved" \
  "$(grep -qF "sha=$MERGED_SHA" "$RESOLVE_RETRY_GH_OUTPUT" && echo present || echo missing)" "present"
check "resolver reports each retry attempt" \
  "$(grep -qF 'attempt 1/4' "$RESOLVE_RETRY_OUTPUT" && echo present || echo missing)" "present"
check "resolver actually retried rather than succeeding first try" \
  "$(cat "${RESOLVE_RETRY_GH_OUTPUT}.calls")" "3"

# Exhausting the budget must blame the API, not the pull request.
RESOLVE_EXHAUST_OUTPUT="$TEST_TMP/resolve-exhaust.txt"
RESOLVE_EXHAUST_GH_OUTPUT="$TEST_TMP/resolve-exhaust.env"
check "resolver fails when every attempt hits an API failure" \
  "$([ "$(run_resolve "$MERGED_SHA" "$BUMP_ONLY" "$RESOLVE_EXHAUST_OUTPUT" "$RESOLVE_EXHAUST_GH_OUTPUT" 3 3)" -ne 0 ] && echo failed || echo passed)" "failed"
check "resolver blames the API, not the PR, when the API was unreachable" \
  "$(grep -qF 'GitHub API could not be queried after 3 attempts' "$RESOLVE_EXHAUST_OUTPUT" && echo correct || echo misattributed)" "correct"
check "resolver tells the operator the bump already merged and to re-run" \
  "$(grep -qF 'gh run rerun --failed' "$RESOLVE_EXHAUST_OUTPUT" && echo present || echo missing)" "present"

# The branch the retry loop exists FOR: gh works fine, the PR simply has no
# merge commit yet. Real gh exits 0 printing nothing, so this must not be
# reported as an API failure.
RESOLVE_EMPTY_OUTPUT="$TEST_TMP/resolve-empty-oid.txt"
RESOLVE_EMPTY_GH_OUTPUT="$TEST_TMP/resolve-empty-oid.env"
check "resolver fails when the PR reports no merge commit at all" \
  "$([ "$(run_resolve "$MERGED_SHA" "$BUMP_ONLY" "$RESOLVE_EMPTY_OUTPUT" "$RESOLVE_EMPTY_GH_OUTPUT" 0 2 1)" -ne 0 ] && echo failed || echo passed)" "failed"
check "resolver blames the PR, not the API, when the API answered cleanly" \
  "$(grep -qF "reported merge commit ''" "$RESOLVE_EMPTY_OUTPUT" && echo correct || echo misattributed)" "correct"
check "resolver does not claim the API was unreachable when it was not" \
  "$(grep -qF 'GitHub API could not be queried' "$RESOLVE_EMPTY_OUTPUT" && echo misattributed || echo correct)" "correct"
check "resolver exports no SHA when the PR has no merge commit" \
  "$(grep -qF 'sha=' "$RESOLVE_EMPTY_GH_OUTPUT" && echo leaked || echo empty)" "empty"

# A failed compare must not be reported as "main did not move". #5233's
# complaint was a silent divergence; a FALSE all-clear is worse, because it
# ends the investigation.
RESOLVE_CMPFAIL_OUTPUT="$TEST_TMP/resolve-compare-fail.txt"
RESOLVE_CMPFAIL_GH_OUTPUT="$TEST_TMP/resolve-compare-fail.env"
check "resolver still pins the SHA when the compare call fails" \
  "$(run_resolve "$MERGED_SHA" "$RACED" "$RESOLVE_CMPFAIL_OUTPUT" "$RESOLVE_CMPFAIL_GH_OUTPUT" 0 1 0 1)" "0"
check "resolver never claims 'main did not move' on a failed compare" \
  "$(grep -qF 'main did not move' "$RESOLVE_CMPFAIL_OUTPUT" && echo false-allclear || echo honest)" "honest"
check "resolver says the divergence check was inconclusive" \
  "$(grep -qF 'Could not check whether main moved' "$RESOLVE_CMPFAIL_OUTPUT" && echo present || echo missing)" "present"

# The number an operator reads must be the count of SURPRISES, not the total:
# the version bump is expected and is not an extra.
check "resolver counts extra commits, excluding the version bump" \
  "$(grep -oE 'this release::[0-9]+ commit' "$RESOLVE_RACE_OUTPUT" | grep -oE '[0-9]+')" "2"

# A misconfigured budget must not be reported as a PR problem.
RESOLVE_BADCFG_OUTPUT="$TEST_TMP/resolve-badcfg.txt"
RESOLVE_BADCFG_GH_OUTPUT="$TEST_TMP/resolve-badcfg.env"
check "resolver rejects a non-positive retry budget" \
  "$([ "$(run_resolve "$MERGED_SHA" "$BUMP_ONLY" "$RESOLVE_BADCFG_OUTPUT" "$RESOLVE_BADCFG_GH_OUTPUT" 0 0)" -ne 0 ] && echo failed || echo passed)" "failed"
check "resolver blames its own configuration, not the PR" \
  "$(grep -qF 'RESOLVE_ATTEMPTS must be a positive integer' "$RESOLVE_BADCFG_OUTPUT" && echo correct || echo misattributed)" "correct"

# --- the REAL manifest must satisfy the verifier's version grep -------------
#
# The verifier's fixture is synthetic, so nothing else pins that `^version = "`
# picks the PACKAGE version out of the real manifest. A move to
# `version.workspace = true`, or a sub-table whose `version = "..."` sorts
# first, would break every release at the guard with no other warning.
REAL_MANIFEST="$SCRIPT_DIR/../crates/core/Cargo.toml"
REAL_VERSION=$(grep -m1 -E '^version = "' "$REAL_MANIFEST" | cut -d'"' -f2 || true)
check "the verifier's grep finds a version in the real core manifest" \
  "$([[ "$REAL_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+ ]] && echo found || echo "missing($REAL_VERSION)")" "found"
check "the real core manifest's first '^version =' is the package version" \
  "$(awk '/^\[/{sec=$0} /^version = "/{print sec; exit}' "$REAL_MANIFEST")" "[package]"

if [ "$FAILURES" -eq 0 ]; then
    echo "PASS: release.yml is merge-queue-compatible, reports failed merge-group CI, and is pinned to one validated commit."
else
    echo "$FAILURES check(s) failed" >&2
    exit 1
fi
