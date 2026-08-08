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

MERGE_SHA=$(gh pr view "$PR_NUMBER" --repo "$REPOSITORY" \
  --json mergeCommit --jq '.mergeCommit.oid // empty' 2>/dev/null || true)

if [[ ! "$MERGE_SHA" =~ ^[0-9a-f]{40}$ ]]; then
    echo "::error title=Could not resolve the release commit::Version-bump PR #$PR_NUMBER reported merge commit '${MERGE_SHA}', which is not a commit SHA. Refusing to fall back to a moving 'main' reference — that is the #5233 bug. Re-run the release once the PR's merge commit is visible."
    exit 1
fi

echo "sha=$MERGE_SHA" >> "$OUTPUT_FILE"
echo "📌 Release pinned to $MERGE_SHA (merge commit of version-bump PR #$PR_NUMBER)"

# Everything below is advisory. A failure here must not fail the release.
if [ -z "$LAUNCH_SHA" ] || [ "$LAUNCH_SHA" = "$MERGE_SHA" ]; then
    exit 0
fi

RANGE=$(gh api "repos/$REPOSITORY/compare/$LAUNCH_SHA...$MERGE_SHA" \
  --jq '.commits[] | "  \(.sha[0:9]) \(.commit.message | split("\n")[0])"' 2>/dev/null || true)

# The version-bump commit itself is expected; anything beyond it is a commit
# that won the race with the merge queue during `wait_for_pr`.
EXTRA_COUNT=$(printf '%s' "$RANGE" | grep -c . || true)
if [ "${EXTRA_COUNT:-0}" -le 1 ]; then
    echo "✅ main did not move during this release: the version bump is the only new commit."
    exit 0
fi

echo "::warning title=main moved during this release::${EXTRA_COUNT} commits landed between the launch commit ($LAUNCH_SHA) and the release commit ($MERGE_SHA), so this release contains more than the version bump. The release is still cut from ONE validated commit; this is a scope notice, not a correctness failure."
printf '%s\n' "$RANGE"
