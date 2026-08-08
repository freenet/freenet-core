#!/usr/bin/env bash
# Resolve the ONE immutable commit a release is cut from.
#
# release.yml's publish and tag jobs used to check out `ref: main`, which is a
# MOVING reference. `wait_for_pr` blocks for ~20 minutes on the merge queue, so
# anything that landed on main in that window silently became part of the
# release and of the tag, with no signal in the run log (#5233 — v0.2.122
# shipped an unrelated commit this way, defeating a deliberate scope decision).
#
# The commit we want is the one the version-bump PR actually merged AS. That is
# the tree the merge queue's full-suite release gate validated, and it is
# immutable: later merges move `main` past it but never change it.
#
# Emits `sha=<40-hex>` to $GITHUB_OUTPUT for downstream jobs to check out
# explicitly. Fails closed — it never falls back to a moving reference.
#
# Also reports (as a warning, not a failure) when main moved between the commit
# the release run was launched from and the commit it will ship, so the "this
# release contains more than the version bump" case stops being silent.
#
# Run manually with: bash scripts/resolve_release_sha.sh
# Tests: scripts/release_mergequeue_test.sh

set -euo pipefail

PR_NUMBER="${PR_NUMBER:?PR_NUMBER is required}"
REPOSITORY="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
OUTPUT_FILE="${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"
# The commit the release run was dispatched from. Best-effort context for the
# divergence report; its absence must not block the release.
LAUNCH_SHA="${LAUNCH_SHA:-}"
# This step runs AFTER the bump PR has already merged to main, so failing here
# leaves a half-done release (version bump landed, nothing published or
# tagged) that needs a human to re-run. The waiter before it retries transient
# API failures for its whole budget; do the same rather than letting one blip
# strand the release. GitHub can also lag briefly in exposing `mergeCommit`
# after a merge-queue merge.
ATTEMPTS="${RESOLVE_ATTEMPTS:-6}"
RETRY_INTERVAL="${RESOLVE_RETRY_INTERVAL:-10}"
# A zero or non-numeric budget would run the loop zero times and then blame the
# PR for what is really a configuration error.
if [[ ! "$ATTEMPTS" =~ ^[1-9][0-9]*$ ]]; then
    echo "::error title=Bad resolver configuration::RESOLVE_ATTEMPTS must be a positive integer, got '$ATTEMPTS'."
    exit 1
fi

MERGE_SHA=""
GH_UNREACHABLE=false
# Keep the last stderr rather than discarding it: when this fails it fails
# mid-release, and "could not be queried" without the 403/404/rate-limit body
# is the least useful message possible at that moment.
GH_STDERR=$(mktemp)
trap 'rm -f "$GH_STDERR"' EXIT

for attempt in $(seq 1 "$ATTEMPTS"); do
    if MERGE_SHA=$(gh pr view "$PR_NUMBER" --repo "$REPOSITORY" \
        --json mergeCommit --jq '.mergeCommit.oid // empty' 2>"$GH_STDERR"); then
        GH_UNREACHABLE=false
    else
        # Distinguish "the API call failed" from "the PR has no merge commit";
        # otherwise the error blames the PR for an API fault.
        GH_UNREACHABLE=true
        MERGE_SHA=""
    fi

    if [[ "$MERGE_SHA" =~ ^[0-9a-f]{40}$ ]]; then
        break
    fi

    if [ "$attempt" -lt "$ATTEMPTS" ]; then
        echo "  ⚠️  merge commit for PR #$PR_NUMBER not resolvable yet (attempt $attempt/$ATTEMPTS), retrying in ${RETRY_INTERVAL}s"
        sleep "$RETRY_INTERVAL"
    fi
done

if [[ ! "$MERGE_SHA" =~ ^[0-9a-f]{40}$ ]]; then
    if [ "$GH_UNREACHABLE" = true ]; then
        REASON="the GitHub API could not be queried after $ATTEMPTS attempts"
    else
        REASON="PR #$PR_NUMBER reported merge commit '${MERGE_SHA}', which is not a commit SHA"
    fi
    echo "::error title=Could not resolve the release commit::${REASON}. Refusing to fall back to a moving 'main' reference — that is the #5233 bug. The version bump has already merged, so re-run this job (\`gh run rerun --failed\`) once the PR's merge commit is visible."
    if [ "$GH_UNREACHABLE" = true ] && [ -s "$GH_STDERR" ]; then
        echo "Last error from gh:"
        sed 's/^/  /' "$GH_STDERR"
    fi
    exit 1
fi

echo "sha=$MERGE_SHA" >> "$OUTPUT_FILE"
echo "📌 Release pinned to $MERGE_SHA (merge commit of version-bump PR #$PR_NUMBER)"

# Everything below is advisory. A failure here must not fail the release.
if [ -z "$LAUNCH_SHA" ] || [ "$LAUNCH_SHA" = "$MERGE_SHA" ]; then
    exit 0
fi

# `.commits` is capped at 250 by the compare API. That is unreachable inside a
# ~20-minute merge-queue window, and this whole section is advisory anyway, so
# counting the listed commits is fine; if it ever saturates, the count would
# understate and `.total_commits` would be the authoritative field.
if ! RANGE=$(gh api "repos/$REPOSITORY/compare/$LAUNCH_SHA...$MERGE_SHA" \
    --jq '.commits[] | "  \(.sha[0:9]) \(.commit.message | split("\n")[0])"' 2>/dev/null); then
    # Never claim "main did not move" on the strength of a call that failed.
    # #5233's complaint was that the divergence was silent; a FALSE all-clear is
    # worse, because it terminates the investigation.
    echo "::warning title=Could not check whether main moved::The compare API call failed, so it is unknown whether commits other than the version bump are in this release. The release is still pinned to $MERGE_SHA."
    exit 0
fi

# The version-bump commit itself is expected; anything beyond it won the race
# with the merge queue while `wait_for_pr` was blocked.
COMMIT_COUNT=$(printf '%s' "$RANGE" | grep -c . || true)
COMMIT_COUNT=${COMMIT_COUNT:-0}
if [ "$COMMIT_COUNT" -le 1 ]; then
    echo "✅ main did not move during this release: the version bump is the only new commit."
    exit 0
fi

# Report the number of UNINTENDED commits, not the total. The bump is not an
# extra, and an operator reading "3 commits" would count three surprises.
EXTRA_COUNT=$((COMMIT_COUNT - 1))
echo "::warning title=main moved during this release::${EXTRA_COUNT} commit(s) landed on main between the launch commit ($LAUNCH_SHA) and the release commit ($MERGE_SHA) in ADDITION to the version bump, so this release contains more than was intended. The release is still cut from ONE validated commit; this is a scope notice, not a correctness failure. Full range:"
printf '%s\n' "$RANGE"
