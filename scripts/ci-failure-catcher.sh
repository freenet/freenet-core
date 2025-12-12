#!/bin/bash
# CI Failure Catcher - Re-runs CI until failure and saves logs
# Usage: ./scripts/ci-failure-catcher.sh [max_attempts]

set -e

REPO="freenet/freenet-core"
BRANCH="fix/hop-by-hop-routing"
MAX_ATTEMPTS="${1:-20}"
LOG_DIR="/tmp/ci-failure-logs"
ATTEMPT=0

mkdir -p "$LOG_DIR"

echo "=== CI Failure Catcher ==="
echo "Branch: $BRANCH"
echo "Max attempts: $MAX_ATTEMPTS"
echo "Logs will be saved to: $LOG_DIR"
echo ""

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    ATTEMPT=$((ATTEMPT + 1))
    echo "--- Attempt $ATTEMPT/$MAX_ATTEMPTS ---"

    # Trigger a new CI run by re-running the latest workflow
    LATEST_RUN=$(gh run list -R "$REPO" --branch "$BRANCH" --limit 1 --json databaseId -q '.[0].databaseId')

    echo "Re-running workflow $LATEST_RUN..."
    gh run rerun "$LATEST_RUN" -R "$REPO" --failed 2>/dev/null || gh run rerun "$LATEST_RUN" -R "$REPO"

    # Wait for the new run to start
    sleep 5

    # Get the new run ID
    NEW_RUN=$(gh run list -R "$REPO" --branch "$BRANCH" --limit 1 --json databaseId -q '.[0].databaseId')
    echo "Watching run $NEW_RUN..."

    # Wait for completion (suppress output, just wait)
    if gh run watch "$NEW_RUN" -R "$REPO" --exit-status >/dev/null 2>&1; then
        echo "✓ Run $NEW_RUN passed"
    else
        echo "✗ Run $NEW_RUN FAILED!"
        echo ""
        echo "=== FAILURE DETECTED ==="
        echo "Saving logs to $LOG_DIR/run-$NEW_RUN/"

        mkdir -p "$LOG_DIR/run-$NEW_RUN"

        # Download all logs
        gh run view "$NEW_RUN" -R "$REPO" --log > "$LOG_DIR/run-$NEW_RUN/full.log" 2>&1 || true

        # Get failed jobs
        gh run view "$NEW_RUN" -R "$REPO" --json jobs -q '.jobs[] | select(.conclusion == "failure") | .name' > "$LOG_DIR/run-$NEW_RUN/failed-jobs.txt"

        # Save summary
        gh run view "$NEW_RUN" -R "$REPO" > "$LOG_DIR/run-$NEW_RUN/summary.txt" 2>&1 || true

        # Extract test failure info
        echo "Extracting test failure details..."
        grep -A 50 "FAILED\|panicked\|error\[" "$LOG_DIR/run-$NEW_RUN/full.log" > "$LOG_DIR/run-$NEW_RUN/failures.txt" 2>/dev/null || true

        # Extract connect operation logs
        grep -i "connect.*target_connections\|register_acceptance\|Transaction timed out\|Connect operation" "$LOG_DIR/run-$NEW_RUN/full.log" > "$LOG_DIR/run-$NEW_RUN/connect-ops.txt" 2>/dev/null || true

        echo ""
        echo "Logs saved! Key files:"
        echo "  - $LOG_DIR/run-$NEW_RUN/full.log (complete logs)"
        echo "  - $LOG_DIR/run-$NEW_RUN/failures.txt (failure excerpts)"
        echo "  - $LOG_DIR/run-$NEW_RUN/connect-ops.txt (connect operation logs)"
        echo ""
        echo "Run ID: $NEW_RUN"
        echo "View online: https://github.com/$REPO/actions/runs/$NEW_RUN"

        exit 0
    fi

    echo ""
done

echo "=== No failures after $MAX_ATTEMPTS attempts ==="
exit 1
