#!/usr/bin/env bash
# Wait for a release version-bump PR to merge, failing promptly when its
# merge-group CI attempt fails.

set -euo pipefail

PR_NUMBER="${PR_NUMBER:?PR_NUMBER is required}"
# Stays under the workflow job timeout to leave headroom for shutdown.
MAX_WAIT="${MAX_WAIT:-3600}"
WAIT_INTERVAL="${WAIT_INTERVAL:-30}"
ELAPSED=0
REPOSITORY="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
OWNER="${REPOSITORY%%/*}"
REPO="${REPOSITORY#*/}"
LAST_PR_JSON=""
LAST_CI_RUNS='[]'

fetch_pr() {
    # GraphQL variables are intentionally literal for the API to expand.
    # shellcheck disable=SC2016
    gh api graphql \
      -F owner="$OWNER" \
      -F repo="$REPO" \
      -F number="$PR_NUMBER" \
      -f query='query($owner:String!,$repo:String!,$number:Int!){repository(owner:$owner,name:$repo){pullRequest(number:$number){state mergedAt mergeStateStatus isInMergeQueue baseRefName url mergeQueueEntry{state position enqueuedAt}}}}'
}

fetch_merge_group_runs() {
    gh run list \
      --repo "$REPOSITORY" \
      --event merge_group \
      --limit 100 \
      --json workflowName,headBranch,status,conclusion,url,createdAt
}

filter_latest_runs() {
    local queue_prefix="$1"
    jq -c --arg prefix "$queue_prefix" \
      '[.[] | select((.headBranch // "") | startswith($prefix))] |
       group_by(.workflowName) | map(max_by(.createdAt)) |
       sort_by(.createdAt) | reverse'
}

print_pr_diagnostics() {
    local pr_json="$1"
    if [ -z "$pr_json" ]; then
        echo "  PR state: unavailable (GitHub API did not return a valid response)"
        return
    fi
    jq -r '.data.repository.pullRequest |
      "  PR: \(.url)\n  state=\(.state), mergeStateStatus=\(.mergeStateStatus), inMergeQueue=\(.isInMergeQueue)\n  queue=" +
      (if .mergeQueueEntry == null then "not queued"
       else "state=\(.mergeQueueEntry.state), position=\(.mergeQueueEntry.position), enqueuedAt=\(.mergeQueueEntry.enqueuedAt)"
       end)' <<< "$pr_json"
}

print_ci_diagnostics() {
    local runs_json="$1"
    if [ "$runs_json" = '[]' ]; then
        echo "  merge-group CI runs: none found for this PR"
        return
    fi
    echo "  Latest merge-group workflow runs:"
    jq -r '.[] | "    \(.workflowName): \(.createdAt), status=\(.status), conclusion=\(.conclusion // "pending") - \(.url)"' <<< "$runs_json"
}

echo "⏳ Waiting for PR #$PR_NUMBER to merge..."

while [ "$ELAPSED" -lt "$MAX_WAIT" ]; do
    # A transient API failure is retryable. The overall budget still bounds
    # persistent failures and preserves the v0.2.71 release behavior.
    PR_JSON=$(fetch_pr 2>/dev/null || true)
    if ! jq -e '.data.repository.pullRequest | type == "object"' \
        >/dev/null 2>&1 <<< "$PR_JSON"; then
        echo "  ⚠️  transient gh error querying PR state, will retry (${ELAPSED}s elapsed)"
        sleep "$WAIT_INTERVAL"
        ELAPSED=$((ELAPSED + WAIT_INTERVAL))
        continue
    fi

    LAST_PR_JSON="$PR_JSON"
    PR_STATE=$(jq -r '.data.repository.pullRequest.state' <<< "$PR_JSON")
    MERGED_AT=$(jq -r '.data.repository.pullRequest.mergedAt // empty' <<< "$PR_JSON")
    MERGE_STATE=$(jq -r '.data.repository.pullRequest.mergeStateStatus' <<< "$PR_JSON")
    IN_MERGE_QUEUE=$(jq -r '.data.repository.pullRequest.isInMergeQueue' <<< "$PR_JSON")
    BASE_REF=$(jq -r '.data.repository.pullRequest.baseRefName' <<< "$PR_JSON")
    QUEUE_ENQUEUED_AT=$(jq -r '.data.repository.pullRequest.mergeQueueEntry.enqueuedAt // empty' <<< "$PR_JSON")

    if [ -n "$MERGED_AT" ] || [ "$PR_STATE" == "MERGED" ]; then
        echo "✅ PR #$PR_NUMBER merged successfully!"
        exit 0
    elif [ "$PR_STATE" == "CLOSED" ]; then
        echo "❌ PR #$PR_NUMBER was closed without merging"
        print_pr_diagnostics "$PR_JSON"
        exit 1
    fi

    QUEUE_PREFIX="gh-readonly-queue/${BASE_REF}/pr-${PR_NUMBER}-"
    CI_RUNS_JSON=$(fetch_merge_group_runs 2>/dev/null || true)
    FILTERED_CI_RUNS=$(filter_latest_runs "$QUEUE_PREFIX" <<< "$CI_RUNS_JSON" 2>/dev/null || true)
    if [ -n "$FILTERED_CI_RUNS" ]; then
        LAST_CI_RUNS="$FILTERED_CI_RUNS"
        BLOCKING_RUN=$(jq -c '[.[] | select(
          .status == "completed" and
          (.conclusion as $conclusion |
           ["failure", "cancelled", "timed_out", "action_required", "startup_failure", "stale"] |
           index($conclusion))
        )] | first // empty' <<< "$FILTERED_CI_RUNS")
    else
        echo "  ⚠️  transient gh error querying merge-group CI, will retry (${ELAPSED}s elapsed)"
        BLOCKING_RUN=""
    fi

    if [ -n "$BLOCKING_RUN" ]; then
        CI_WORKFLOW=$(jq -r '.workflowName' <<< "$BLOCKING_RUN")
        CI_CONCLUSION=$(jq -r '.conclusion' <<< "$BLOCKING_RUN")
        CI_URL=$(jq -r '.url' <<< "$BLOCKING_RUN")
        CI_CREATED_AT=$(jq -r '.createdAt' <<< "$BLOCKING_RUN")

        if [ -n "$QUEUE_ENQUEUED_AT" ] && [[ "$CI_CREATED_AT" < "$QUEUE_ENQUEUED_AT" ]]; then
            echo "  Blocking run predates the current queue entry; waiting for its merge-group workflows"
            sleep "$WAIT_INTERVAL"
            ELAPSED=$((ELAPSED + WAIT_INTERVAL))
            continue
        fi

        # Confirm this run is still the latest for its workflow before failing.
        # A requeue can leave an older failure on the same synthetic branch.
        CONFIRM_RUNS_JSON=$(fetch_merge_group_runs 2>/dev/null || true)
        CONFIRM_FILTERED=$(filter_latest_runs "$QUEUE_PREFIX" <<< "$CONFIRM_RUNS_JSON" 2>/dev/null || true)
        CONFIRM_MATCH=$(jq -c --arg url "$CI_URL" '.[] | select(.url == $url)' <<< "${CONFIRM_FILTERED:-[]}" 2>/dev/null || true)
        CONFIRM_CREATED_AT=$(jq -r '.createdAt // empty' <<< "${CONFIRM_MATCH:-{}}" 2>/dev/null || true)
        CONFIRM_STATE=$(fetch_pr 2>/dev/null || true)
        CONFIRM_VALID=false
        if ! jq -e '.data.repository.pullRequest | type == "object"' \
            >/dev/null 2>&1 <<< "$CONFIRM_STATE"; then
            echo "  PR state was unavailable while confirming the failure; continuing to wait"
        else
            CONFIRM_VALID=true
            CONFIRM_PR_STATE=$(jq -r '.data.repository.pullRequest.state' <<< "$CONFIRM_STATE")
            CONFIRM_MERGED_AT=$(jq -r '.data.repository.pullRequest.mergedAt // empty' <<< "$CONFIRM_STATE")
            CONFIRM_ENQUEUED_AT=$(jq -r '.data.repository.pullRequest.mergeQueueEntry.enqueuedAt // empty' <<< "$CONFIRM_STATE")
            LAST_PR_JSON="$CONFIRM_STATE"
        fi

        if [ "$CONFIRM_VALID" != true ]; then
            :
        elif [ -n "${CONFIRM_MERGED_AT:-}" ] || [ "${CONFIRM_PR_STATE:-}" == "MERGED" ]; then
            echo "✅ PR #$PR_NUMBER merged successfully!"
            exit 0
        elif [ -n "${CONFIRM_ENQUEUED_AT:-}" ] && \
            [[ "$CONFIRM_CREATED_AT" < "$CONFIRM_ENQUEUED_AT" ]]; then
            echo "  Blocking run predates a queue entry created during confirmation; continuing to wait"
            LAST_CI_RUNS="${CONFIRM_FILTERED:-$LAST_CI_RUNS}"
        elif [ -n "$CONFIRM_MATCH" ]; then
            LAST_CI_RUNS="${CONFIRM_FILTERED:-$LAST_CI_RUNS}"
            echo "::error title=Merge-group check failed::PR #$PR_NUMBER cannot merge because $CI_WORKFLOW concluded $CI_CONCLUSION: $CI_URL"
            echo "❌ Merge-group check failed for PR #$PR_NUMBER; stopping instead of waiting for the timeout"
            print_pr_diagnostics "$LAST_PR_JSON"
            print_ci_diagnostics "$LAST_CI_RUNS"
            exit 1
        else
            echo "  A newer $CI_WORKFLOW attempt appeared while confirming the failure; continuing to wait"
            LAST_CI_RUNS="${CONFIRM_FILTERED:-$LAST_CI_RUNS}"
        fi
    elif [ -n "$FILTERED_CI_RUNS" ] && [ "$FILTERED_CI_RUNS" != '[]' ]; then
        RUN_SUMMARY=$(jq -r 'map("\(.workflowName)=\(.status)/\(.conclusion // "pending")") | join(", ")' <<< "$FILTERED_CI_RUNS")
        echo "  Still waiting... mergeStateStatus=$MERGE_STATE, inMergeQueue=$IN_MERGE_QUEUE, latest merge-group workflows: $RUN_SUMMARY (${ELAPSED}s elapsed)"
    else
        echo "  Still waiting... mergeStateStatus=$MERGE_STATE, inMergeQueue=$IN_MERGE_QUEUE, no merge-group workflow run found yet (${ELAPSED}s elapsed)"
    fi

    sleep "$WAIT_INTERVAL"
    ELAPSED=$((ELAPSED + WAIT_INTERVAL))
done

echo "::error title=Release PR merge timeout::Timeout after ${MAX_WAIT}s waiting for PR #$PR_NUMBER to merge"
echo "❌ Timeout after ${MAX_WAIT}s waiting for PR #$PR_NUMBER to merge"
print_pr_diagnostics "$LAST_PR_JSON"
print_ci_diagnostics "$LAST_CI_RUNS"
exit 1
