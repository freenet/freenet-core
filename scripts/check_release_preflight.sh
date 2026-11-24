#!/usr/bin/env bash
# Pre-flight for a LIVE release.yml run, in the `validate` job, before anything
# is pushed. Refuses the cuts that would otherwise fail half-done:
#
#   1. Another release.yml run is queued or in progress, or a legacy
#      version-bump PR (`release/v*` head, or a `build: release` title, from
#      the older scripts/release.sh flow) is still open. Two cuts racing each
#      other force-push and tag over each other's release branches. This is
#      the freenet:release skill's P0 check, enforced instead of remembered.
#   2. Tag v$VERSION already exists on origin. `create_release` would fail at
#      its tag step, after the release branch's whole CI run.
#   3. freenet $VERSION is already on crates.io. The version is spent; the
#      crates.io publish at the end of cross-compile.yml would fail after the
#      whole build and Gate A.
#   4. $VERSION is not above the version in the SHIP commit's
#      crates/core/Cargo.toml (CARGO_TOML, written by resolve_release_ship.sh).
#      A lower version is a downgrade; an equal one makes the bump commit
#      empty, so `update_versions` dies at `git commit`.
#   5. $VERSION is not above the latest published release (the highest-semver
#      published non-prerelease, scripts/release_lib.sh; never GitHub's
#      movable /releases/latest marker). main's Cargo version lags a release
#      until its merge-back PR (release-sync.yml) lands, so check 4 alone
#      would accept a version that is already out.
#   6. Another release is unfinished: a vY tag, or a `release/vY` branch
#      with no tag yet, for some Y above the latest
#      published release and other than $VERSION. Cutting past it would let
#      both ship, or leave the first one half-done. The SAME version is
#      exempt on purpose: a re-cut after a Gate A block deletes the tag
#      (check 2 refuses until it has) and the draft, then reuses
#      `release/v$VERSION`, which update_versions force-pushes. An abandoned
#      cut is cleared by deleting its tag, draft and branch
#      (scripts/RELEASE_RECOVERY.md). Drafts alone are ignored here:
#      validate's read-only token is not shown them. Every cut pushes its tag
#      before it creates its draft, so the tag check covers a cut in flight,
#      and create_release's tag step refuses, before pushing, a same-version
#      draft older than its run.
#
# Every lookup FAILS CLOSED: an answer this script cannot read refuses the run
# with a message naming what it could not check. Nothing has been created yet,
# so a refusal costs one re-dispatch.
#
# Inputs (env): RELEASE_VERSION (X.Y.Z), GITHUB_REPOSITORY, GH_TOKEN,
# GITHUB_RUN_ID (this run, excluded from check 1), CARGO_TOML (the ship
# commit's crates/core/Cargo.toml; defaults to the checkout's, for tests).
# Tests: scripts/check_release_preflight_test.sh

set -euo pipefail

VERSION="${RELEASE_VERSION:?RELEASE_VERSION is required}"
REPO="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
# shellcheck source=scripts/release_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/release_lib.sh"
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
        refuse "Another release run is active" "release.yml run(s) $OTHERS are queued or in progress. Two cuts race each other's release branches and tags; wait for it to finish or cancel it (a dry run counts too: it is short)."
    fi
fi

PR_LIMIT=1000
if ! PRS=$(gh pr list --repo "$REPO" --state open --limit "$PR_LIMIT" \
        --json number,headRefName,title); then
    refuse "Could not list open PRs" "Could not ask GitHub for open legacy version-bump PRs, so a concurrent cut cannot be ruled out."
elif [ "$(jq length <<<"$PRS")" -ge "$PR_LIMIT" ]; then
    refuse "Too many open PRs to check" "$PR_LIMIT or more PRs are open, so a legacy bump PR beyond the listing cannot be ruled out."
else
    # The title form is anchored on a version so an unrelated "build: release-…"
    # PR does not block every cut.
    OPEN_BUMPS=$(jq -r '[.[] | select((.headRefName | startswith("release/v"))
                                      or (.title | test("^build: release [0-9]")))
                             | "#\(.number) (\(.headRefName))"] | join(", ")' <<<"$PRS")
    if [ -n "$OPEN_BUMPS" ]; then
        refuse "A version-bump PR is already open" "Open: $OPEN_BUMPS. A legacy release.sh cut is in flight, or a failed one left its PR behind; resolve that first (scripts/RELEASE_RECOVERY.md)."
    fi
fi

# --- 2. tag already exists ---------------------------------------------------
if ! TAGS=$(git ls-remote --tags "https://github.com/$REPO.git" "refs/tags/v$VERSION"); then
    refuse "Could not list tags" "Could not ask origin whether tag v$VERSION exists."
elif [ -n "$TAGS" ]; then
    refuse "Tag v$VERSION already exists" "Tag v$VERSION is already on origin. Resuming a failed run? Use 'Re-run failed jobs', not 'Re-run all jobs' (which re-runs this check). A blocked cut being redone: scripts/RELEASE_RECOVERY.md. Otherwise pick the next version."
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
    refuse "The ship commit is already at $VERSION" "crates/core/Cargo.toml at the ship commit already says $VERSION, so this bump would be an empty commit. That version was synced to main after its release, or bumped there by the older PR flow; cut the next patch (scripts/RELEASE_RECOVERY.md)."
elif [ "$(printf '%s\n%s\n' "$CURRENT" "$VERSION" | sort -V | tail -1)" != "$VERSION" ]; then
    refuse "$VERSION is a downgrade" "crates/core/Cargo.toml at the ship commit is at $CURRENT, above the requested $VERSION."
fi

# --- 5. version must be above the latest published release -------------------
LATEST_TAG=""
if ROWS=$(rel_release_rows 2>/dev/null); then
    LATEST_TAG=$(rel_latest_published "$ROWS" || true)
fi
LATEST="${LATEST_TAG#v}"
if ! rel_is_semver_tag "$LATEST_TAG"; then
    refuse "Could not read the latest release" "No published vX.Y.Z release could be read (got '$LATEST_TAG'), so whether $VERSION moves forward is unknown."
elif ! rel_version_gt "$VERSION" "$LATEST"; then
    refuse "$VERSION is not above the latest release" "The latest published release is $LATEST_TAG. Pick a version above it."
fi

# --- 6. no other release unfinished ------------------------------------------
if [ -n "$LATEST" ]; then
    # Published releases (prereleases included) are finished, whatever their
    # version; only an unpublished one above the latest is in flight.
    PUBLISHED=$(awk -F'\t' '$2 == "false" {print $1}' <<< "$ROWS" | sed -n 's/^v//p')
    INFLIGHT=()
    if ! ALL_TAGS=$(rel_remote_tags); then
        refuse "Could not list tags" "Could not list origin's release tags, so an unfinished release cannot be ruled out."
    else
        while read -r t; do
            [ -n "$t" ] || continue
            if rel_version_gt "$t" "$LATEST" && [ "$t" != "$VERSION" ] && ! grep -qxF "$t" <<< "$PUBLISHED"; then
                INFLIGHT+=("tag v$t")
            fi
        done <<< "$ALL_TAGS"
    fi
    if ! BRANCHES=$(rel_remote_release_branches); then
        refuse "Could not list release branches" "Could not list origin's release/v* branches, so an unfinished cut cannot be ruled out."
    else
        while read -r b; do
            [ -n "$b" ] || continue
            if rel_version_gt "$b" "$LATEST" && [ "$b" != "$VERSION" ] && ! grep -qxF "$b" <<< "${ALL_TAGS:-}"; then
                INFLIGHT+=("branch release/v$b (no tag yet)")
            fi
        done <<< "$BRANCHES"
    fi
    if [ "${#INFLIGHT[@]}" -gt 0 ]; then
        refuse "Another release is unfinished" "Above the latest published release $LATEST_TAG: ${INFLIGHT[*]}. Finish that release, or abandon it by deleting its tag, draft and release/vY branch (scripts/RELEASE_RECOVERY.md), before cutting $VERSION."
    fi
fi

if [ "$FAILED" -ne 0 ]; then
    echo "Refusing to start release $VERSION; nothing has been created."
    exit 1
fi
echo "✅ Pre-flight for $VERSION: no other cut in flight, tag and crates.io version unused, $CURRENT -> $VERSION."
