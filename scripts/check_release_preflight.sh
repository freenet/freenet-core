#!/usr/bin/env bash
# Pre-flight for a LIVE release.yml run, in the `validate` job, before anything
# is pushed. Refuses the cuts that would otherwise fail half-done:
#
#   1. Another release.yml run is queued or in progress, or a version-bump PR
#      (`release/v*` head, or a `build: release` title) is already open. Two
#      cuts racing each other produce conflicting bump PRs, and the loser's
#      failure lands after its own bump may already be queued. This is the
#      freenet:release skill's P0 check, enforced instead of remembered.
#   2. Tag v$VERSION already exists on origin. `create_release` would fail at
#      `git tag`, AFTER the bump PR merged, leaving main bumped and untagged.
#   3. freenet $VERSION is already on crates.io. The version is spent; the
#      crates.io publish at the end of cross-compile.yml would fail after the
#      whole build and Gate A.
#   4. $VERSION is not above the version in crates/core/Cargo.toml. A lower
#      version is a downgrade; an equal one makes the bump commit empty, so
#      `update_versions` dies at `git commit`.
#
# Every lookup FAILS CLOSED: an answer this script cannot read refuses the run
# with a message naming what it could not check. Nothing has been created yet,
# so a refusal costs one re-dispatch.
#
# Inputs (env): RELEASE_VERSION (X.Y.Z), GITHUB_REPOSITORY, GH_TOKEN,
# GITHUB_RUN_ID (this run, excluded from check 1). Optional for tests:
# CARGO_TOML (default crates/core/Cargo.toml).
# Tests: scripts/check_release_preflight_test.sh

set -euo pipefail

VERSION="${RELEASE_VERSION:?RELEASE_VERSION is required}"
REPO="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
RUN_ID="${GITHUB_RUN_ID:-}"
CARGO_TOML="${CARGO_TOML:-crates/core/Cargo.toml}"

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "::error::RELEASE_VERSION must be X.Y.Z, got '$VERSION'"
    exit 1
fi

FAILED=0
refuse() {
    echo "::error title=$1::$2"
    FAILED=1
}

# --- 1. concurrent cut -------------------------------------------------------
if ! RUNS=$(gh run list --repo "$REPO" --workflow release.yml --limit 20 \
        --json databaseId,status); then
    refuse "Could not list release runs" "Could not ask GitHub whether another release.yml run is in progress, so a concurrent cut cannot be ruled out."
else
    OTHERS=$(jq -r --arg me "$RUN_ID" \
        '[.[] | select((.databaseId | tostring) != $me)
              | select(.status != "completed")
              | .databaseId] | join(" ")' <<<"$RUNS")
    if [ -n "$OTHERS" ]; then
        refuse "Another release run is active" "release.yml run(s) $OTHERS are queued or in progress. Two cuts race each other's bump PRs; wait for it to finish or cancel it (a dry run counts too: it is short)."
    fi
fi

if ! PRS=$(gh pr list --repo "$REPO" --state open --limit 200 \
        --json number,headRefName,title); then
    refuse "Could not list open PRs" "Could not ask GitHub for open version-bump PRs, so a concurrent cut cannot be ruled out."
else
    OPEN_BUMPS=$(jq -r '[.[] | select((.headRefName | startswith("release/v"))
                                      or (.title | startswith("build: release")))
                             | "#\(.number) (\(.headRefName))"] | join(", ")' <<<"$PRS")
    if [ -n "$OPEN_BUMPS" ]; then
        refuse "A version-bump PR is already open" "Open: $OPEN_BUMPS. Another cut is in flight, or a failed one left its PR behind; resolve that first (scripts/RELEASE_RECOVERY.md)."
    fi
fi

# --- 2. tag already exists ---------------------------------------------------
if ! TAGS=$(git ls-remote --tags "https://github.com/$REPO.git" "refs/tags/v$VERSION"); then
    refuse "Could not list tags" "Could not ask origin whether tag v$VERSION exists."
elif [ -n "$TAGS" ]; then
    refuse "Tag v$VERSION already exists" "Tag v$VERSION is already on origin. If it is a blocked cut being redone, follow scripts/RELEASE_RECOVERY.md; otherwise pick the next version."
fi

# --- 3. already on crates.io -------------------------------------------------
# Three-state, as in RELEASE_RECOVERY.md: 200 = published, 404 = not, anything
# else is UNKNOWN. The User-Agent is required: crates.io answers 403 without one.
if ! CODE=$(curl -sS -o /dev/null -w '%{http_code}' -A 'freenet-release-driver' \
        --max-time 30 --retry 3 --retry-all-errors \
        "https://crates.io/api/v1/crates/freenet/$VERSION"); then
    CODE="curl-failed"
fi
case "$CODE" in
    404) ;;
    200) refuse "freenet $VERSION is already on crates.io" "That version is spent and cannot be published again. Cut the next patch." ;;
    *) refuse "Could not check crates.io" "crates.io answered '$CODE' for freenet $VERSION, so whether that version is already spent is unknown." ;;
esac

# --- 4. version must move forward -------------------------------------------
CURRENT=$(awk '/^\[package\]/ {p = 1; next} /^\[/ {p = 0}
               p && /^version *=/ {gsub(/.*= *"|".*/, ""); print; exit}' "$CARGO_TOML")
if [[ ! "$CURRENT" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    refuse "Could not read the current version" "No [package] version found in $CARGO_TOML (got '$CURRENT')."
elif [ "$CURRENT" = "$VERSION" ]; then
    refuse "main is already at $VERSION" "crates/core/Cargo.toml already says $VERSION, so the bump commit would be empty and update_versions would fail. Cut the next patch."
elif [ "$(printf '%s\n%s\n' "$CURRENT" "$VERSION" | sort -V | tail -1)" != "$VERSION" ]; then
    refuse "$VERSION is a downgrade" "crates/core/Cargo.toml is at $CURRENT, above the requested $VERSION."
fi

if [ "$FAILED" -ne 0 ]; then
    echo "Refusing to start release $VERSION; nothing has been created."
    exit 1
fi
echo "✅ Pre-flight for $VERSION: no other cut in flight, tag and crates.io version unused, $CURRENT -> $VERSION."
