#!/usr/bin/env bash
# Merge-back after a release: bring main's version files up to the published
# release, as a PR through the normal merge queue. Run by release-sync.yml on
# `release: published` (or by hand, via its workflow_dispatch).
#
# release.yml ships ship_sha plus a version bump on branch `release/vX.Y.Z`, so
# the bump never reaches main by itself. This script regenerates the SAME bump
# on the CURRENT main (scripts/apply_version_bump.sh, the one copy of those
# rewrites) rather than cherry-picking the release commit, so it cannot
# conflict. It:
#
#   - does nothing (beyond closing superseded PRs) when main's crates/core
#     version is already >= VERSION, e.g. a later release synced first;
#   - otherwise commits the bump on branch `release-sync/vVERSION` and
#     force-pushes it, so a re-run replaces the branch with a bump of the main
#     it now sees;
#   - opens the PR titled `build: sync main to VERSION after release` if none
#     is open for that branch, and enables auto-merge (no --squash: the merge
#     queue owns the strategy, and an explicit one fails, v0.2.93). The title
#     must NOT start with `build: release`: ci.yml treats that prefix as the
#     legacy release merge-group gate and would run the full suite again;
#   - closes open `release-sync/v*` PRs for lower versions (superseded), and
#     the one for VERSION itself when main already has it;
#   - does nothing (beyond closing its own PR) when an open `release-sync/v*`
#     PR for a HIGHER version exists: that one supersedes this sync, and a
#     lower sync merging after it would conflict with it on the lockfiles.
#     Syncs for different versions can run concurrently (release-sync.yml's
#     concurrency group is per version), so the superseding pass runs again
#     after this PR is opened: whichever of two racing syncs finishes second
#     closes the lower PR.
#
# fdev's version comes from the release tag's tree (what was published), or
# main's if main is somehow ahead, so the sync never moves fdev backwards.
#
# Inputs (env): VERSION (X.Y.Z), GITHUB_REPOSITORY, GH_TOKEN. Runs in a
# checkout of main whose `origin` can be pushed to. Needs cargo on PATH.
# Tests: scripts/release_sync_test.sh

set -euo pipefail

VERSION="${VERSION:?VERSION is required}"
VERSION="${VERSION#v}"
REPO="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "::error::VERSION must be X.Y.Z, got '$VERSION'"
    exit 1
fi
BRANCH="release-sync/v$VERSION"
TITLE="build: sync main to $VERSION after release"
TOOLS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

package_version() {
    awk '/^\[package\]/ {p = 1; next} /^\[/ {p = 0}
         p && /^version *=/ {gsub(/.*= *"|".*/, ""); print; exit}'
}
# version_lt A B: A < B in version order.
version_lt() {
    [ "$1" != "$2" ] && [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -1)" = "$1" ]
}

MAIN_VERSION=$(package_version < crates/core/Cargo.toml)
if [[ ! "$MAIN_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "::error::Could not read main's version from crates/core/Cargo.toml (got '$MAIN_VERSION')."
    exit 1
fi
NOOP=false
if ! version_lt "$MAIN_VERSION" "$VERSION"; then
    NOOP=true
fi

# --- close superseded sync PRs ----------------------------------------------
# supersede_pass: close lower-version sync PRs (and our own when main already
# has VERSION or a higher sync is open). Sets HIGHER to a higher open sync's
# version, if any.
HIGHER=""
supersede_pass() {
    local open number head other reason
    open=$(gh pr list --repo "$REPO" --state open --limit 200 --json number,headRefName \
        --jq '.[] | select(.headRefName | startswith("release-sync/v")) | "\(.number) \(.headRefName)"')
    HIGHER=""
    while read -r number head; do
        [ -n "${number:-}" ] || continue
        other="${head#release-sync/v}"
        if version_lt "$VERSION" "$other"; then
            HIGHER="$other"
        fi
    done <<< "$open"
    while read -r number head; do
        [ -n "${number:-}" ] || continue
        other="${head#release-sync/v}"
        if version_lt "$other" "$VERSION"; then
            reason="Superseded by the sync to $VERSION."
        elif [ "$other" = "$VERSION" ] && [ "$NOOP" = true ]; then
            reason="main is already at $MAIN_VERSION, so there is nothing left to sync."
        elif [ "$other" = "$VERSION" ] && [ -n "$HIGHER" ]; then
            reason="Superseded by the open sync to $HIGHER."
        else
            continue
        fi
        echo "Closing #$number ($head): $reason"
        gh pr close "$number" --repo "$REPO" --delete-branch --comment "$reason"
    done <<< "$open"
}
supersede_pass

if [ -n "$HIGHER" ]; then
    echo "✅ An open sync to $HIGHER supersedes this one: nothing to sync."
    exit 0
fi
if [ "$NOOP" = true ]; then
    echo "✅ main is at $MAIN_VERSION, not below $VERSION: nothing to sync."
    exit 0
fi

# --- regenerate the bump on main ---------------------------------------------
git fetch --no-tags --depth=1 origin "+refs/tags/v$VERSION:refs/tags/v$VERSION"
TAG_FDEV=$(git show "v$VERSION:crates/fdev/Cargo.toml" | package_version)
MAIN_FDEV=$(package_version < crates/fdev/Cargo.toml)
for v in "$TAG_FDEV" "$MAIN_FDEV"; do
    if [[ ! "$v" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "::error::Could not read an fdev version (tag v$VERSION: '$TAG_FDEV', main: '$MAIN_FDEV')."
        exit 1
    fi
done
FDEV_VERSION=$(printf '%s\n%s\n' "$TAG_FDEV" "$MAIN_FDEV" | sort -V | tail -1)

BASE=$(git rev-parse HEAD)
git checkout -B "$BRANCH"
bash "$TOOLS/apply_version_bump.sh" "$VERSION" "$FDEV_VERSION"
cargo update --workspace
cargo metadata --format-version 1 --manifest-path apps/freenet-ping/Cargo.toml > /dev/null
git add -u
git commit -m "$TITLE

Sync main's version files to the published release v$VERSION:
- freenet: $MAIN_VERSION → $VERSION
- fdev: → $FDEV_VERSION

Generated by .github/workflows/release-sync.yml on main at $BASE."
git push --force origin "$BRANCH"

# --- PR + auto-merge ----------------------------------------------------------
PR=$(gh pr list --repo "$REPO" --state open --head "$BRANCH" --json number --jq '.[0].number // empty')
if [ -z "$PR" ]; then
    BODY="Syncs main's version files to the published release **v$VERSION** (freenet $VERSION, fdev $FDEV_VERSION).

release.yml ships the soaked commit plus a version bump on \`release/v$VERSION\`, which never reaches main by itself. This PR is that bump regenerated on main by \`.github/workflows/release-sync.yml\`. It touches only version files and lockfiles. See docs/RELEASING.md, 'Merge-back'."
    URL=$(gh pr create --repo "$REPO" --base main --head "$BRANCH" --title "$TITLE" --body "$BODY")
    PR="${URL##*/}"
    echo "Opened #$PR"
else
    echo "Reusing open PR #$PR (branch force-pushed with a bump of current main)"
fi
if [[ ! "$PR" =~ ^[0-9]+$ ]]; then
    echo "::error::Could not determine the sync PR number (got '$PR')."
    exit 1
fi
# main may have reached VERSION while this ran (another sync, or a hand bump
# merging). Read it again right before arming auto-merge, so a PR that would
# bump main to a version it already has is closed instead. This narrows the
# race rather than closing it: main can still reach VERSION between this read
# and `gh pr merge --auto`; the merge queue and the next sync's supersede pass
# cover what is left.
MAIN_NOW=""
if git fetch --no-tags --depth=1 origin main; then
    MAIN_NOW=$(git show FETCH_HEAD:crates/core/Cargo.toml | package_version) || MAIN_NOW=""
fi
if [[ ! "$MAIN_NOW" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "::error::Could not re-read main's version before enabling auto-merge (got '$MAIN_NOW'); #$PR is left open without auto-merge."
    exit 1
fi
if ! version_lt "$MAIN_NOW" "$VERSION"; then
    echo "Closing #$PR: main reached $MAIN_NOW while this sync ran."
    gh pr close "$PR" --repo "$REPO" --delete-branch --comment "main is already at $MAIN_NOW, so there is nothing left to sync."
    echo "✅ main is at $MAIN_NOW, not below $VERSION: nothing to sync."
    exit 0
fi
AUTO=$(gh pr view "$PR" --repo "$REPO" --json autoMergeRequest --jq '.autoMergeRequest != null')
if [ "$AUTO" != "true" ]; then
    gh pr merge "$PR" --repo "$REPO" --auto
fi
# A sync for another version may have opened its PR while this one ran.
supersede_pass
if [ -n "$HIGHER" ]; then
    echo "✅ A sync to $HIGHER opened meanwhile and supersedes #$PR (closed above)."
    exit 0
fi
echo "✅ #$PR syncs main to $VERSION and will merge through the queue."
