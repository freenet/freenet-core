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

fetch_ci_runs() {
    gh run list \
      --repo "$REPOSITORY" \
      --workflow ci.yml \
      --event merge_group \
      --limit 100 \
      --json headBranch,status,conclusion,url,createdAt
}

filter_ci_runs() {
    local queue_prefix="$1"
    jq -c --arg prefix "$queue_prefix" \
      '[.[] | select((.headBranch // "") | startswith($prefix))] | sort_by(.createdAt) | reverse'
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
    echo "  Recent merge-group CI runs:"
    jq -r '.[:5][] | "    \(.createdAt): status=\(.status), conclusion=\(.conclusion // "pending") - \(.url)"' <<< "$runs_json"
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

    if [ -n "$MERGED_AT" ] || [ "$PR_STATE" == "MERGED" ]; then
        echo "✅ PR #$PR_NUMBER merged successfully!"
        exit 0
    elif [ "$PR_STATE" == "CLOSED" ]; then
        echo "❌ PR #$PR_NUMBER was closed without merging"
        print_pr_diagnostics "$PR_JSON"
        exit 1
    fi

    QUEUE_PREFIX="gh-readonly-queue/${BASE_REF}/pr-${PR_NUMBER}-"
    CI_RUNS_JSON=$(fetch_ci_runs 2>/dev/null || true)
    FILTERED_CI_RUNS=$(filter_ci_runs "$QUEUE_PREFIX" <<< "$CI_RUNS_JSON" 2>/dev/null || true)
    if [ -n "$FILTERED_CI_RUNS" ]; then
        LAST_CI_RUNS="$FILTERED_CI_RUNS"
        LATEST_CI_RUN=$(jq -c 'first // empty' <<< "$FILTERED_CI_RUNS")
    else
        echo "  ⚠️  transient gh error querying merge-group CI, will retry (${ELAPSED}s elapsed)"
        LATEST_CI_RUN=""
    fi

    if [ -n "$LATEST_CI_RUN" ]; then
        CI_STATUS=$(jq -r '.status' <<< "$LATEST_CI_RUN")
        CI_CONCLUSION=$(jq -r '.conclusion // empty' <<< "$LATEST_CI_RUN")
        CI_URL=$(jq -r '.url' <<< "$LATEST_CI_RUN")
        echo "  Still waiting... mergeStateStatus=$MERGE_STATE, inMergeQueue=$IN_MERGE_QUEUE, latest merge-group CI=$CI_STATUS/${CI_CONCLUSION:-pending} (${ELAPSED}s elapsed)"

        if [ "$CI_STATUS" == "completed" ] && [ "$CI_CONCLUSION" == "failure" ]; then
            # Confirm the same run is still newest before failing. A requeue
            # can leave an older failure on the same synthetic branch.
            CONFIRM_RUNS_JSON=$(fetch_ci_runs 2>/dev/null || true)
            CONFIRM_FILTERED=$(filter_ci_runs "$QUEUE_PREFIX" <<< "$CONFIRM_RUNS_JSON" 2>/dev/null || true)
            CONFIRM_LATEST=$(jq -c 'first // empty' <<< "${CONFIRM_FILTERED:-[]}" 2>/dev/null || true)
            CONFIRM_URL=$(jq -r '.url // empty' <<< "${CONFIRM_LATEST:-{}}" 2>/dev/null || true)
            CONFIRM_STATE=$(fetch_pr 2>/dev/null || true)
            CONFIRM_MERGED_AT=$(jq -r '.data.repository.pullRequest.mergedAt // empty' <<< "$CONFIRM_STATE" 2>/dev/null || true)

            if [ -n "$CONFIRM_MERGED_AT" ]; then
                echo "✅ PR #$PR_NUMBER merged successfully!"
                exit 0
            elif [ "$CONFIRM_URL" = "$CI_URL" ]; then
                LAST_PR_JSON="${CONFIRM_STATE:-$LAST_PR_JSON}"
                LAST_CI_RUNS="${CONFIRM_FILTERED:-$LAST_CI_RUNS}"
                echo "::error title=Merge-group CI failed::PR #$PR_NUMBER cannot merge because its merge-group CI run failed: $CI_URL"
                echo "❌ Merge-group CI failed for PR #$PR_NUMBER; stopping instead of waiting for the timeout"
                print_pr_diagnostics "$LAST_PR_JSON"
                print_ci_diagnostics "$LAST_CI_RUNS"
                exit 1
            else
                echo "  A newer merge-group CI attempt appeared while confirming the failure; continuing to wait"
            fi
        fi
    else
        echo "  Still waiting... mergeStateStatus=$MERGE_STATE, inMergeQueue=$IN_MERGE_QUEUE, no merge-group CI run found yet (${ELAPSED}s elapsed)"
    fi

    sleep "$WAIT_INTERVAL"
    ELAPSED=$((ELAPSED + WAIT_INTERVAL))
done

echo "::error title=Release PR merge timeout::Timeout after ${MAX_WAIT}s waiting for PR #$PR_NUMBER to merge"
echo "❌ Timeout after ${MAX_WAIT}s waiting for PR #$PR_NUMBER to merge"
print_pr_diagnostics "$LAST_PR_JSON"
print_ci_diagnostics "$LAST_CI_RUNS"
exit 1
