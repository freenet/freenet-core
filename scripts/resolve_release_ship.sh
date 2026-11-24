#!/usr/bin/env bash
# Resolve and check the commit a release ships (`ship_sha`), in release.yml's
# `validate` job, before anything is created.
#
# The release is cut from ship_sha plus a version bump, never from whatever
# `main` is when the run starts. So ship_sha is what was soaked, and the checks
# here are what make that safe:
#
#   1. ship_sha is a full 40-hex commit id. A live run REQUIRES it; a dry run
#      defaults it to the dispatch commit (LAUNCH_SHA).
#   2. ship_sha is reachable from main (compare ship_sha...main is `ahead` or
#      `identical`), so it went through the merge queue and its CI.
#   3. ship_sha contains the previous release's ship commit. That commit is the
#      latest published (non-draft, non-prerelease) tag's commit when the tag
#      is on main (releases cut before ship_sha existed), and that commit's
#      first parent otherwise (the tag sits on a `release/v*` bump commit whose
#      parent is the previous ship_sha). Without this, a release could silently
#      drop a fix the previous one shipped.
#   4. The fdev version: max(fdev in ship_sha, fdev in the latest published
#      tag) + 1 patch. main's fdev version lags a release until the merge-back
#      PR (release-sync.yml) lands, so ship_sha alone could reuse a published
#      fdev version.
#
# Checks 2 and 3 FAIL CLOSED in a live run: any lookup this script cannot read
# refuses the run. In a dry run they only warn, so a dry run dispatched from a
# feature branch (to rehearse a release.yml change) still runs to the end.
# Check 1 and the reads behind check 4 fail in both modes: nothing after this
# step can run without them.
#
# Every lookup goes through the GitHub API (no fetch), so the job needs no
# history and the tests can stub `gh`.
#
# Inputs (env): SHIP_SHA_INPUT (may be empty), RELEASE_LIVE, LAUNCH_SHA,
# GITHUB_REPOSITORY, GH_TOKEN, GITHUB_OUTPUT, RUNNER_TEMP.
# Outputs: ship_sha, fdev_version, prev_tag, prev_ship_sha, ship_cargo_toml
# (a copy of ship_sha's crates/core/Cargo.toml, for check_release_preflight.sh).
# Tests: scripts/resolve_release_ship_test.sh

set -euo pipefail

REPO="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
LAUNCH_SHA="${LAUNCH_SHA:?LAUNCH_SHA is required}"
OUT="${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"
TMPDIR_OUT="${RUNNER_TEMP:?RUNNER_TEMP is required}"
LIVE=false
[ "${RELEASE_LIVE:-}" = "true" ] && LIVE=true

SHIP="${SHIP_SHA_INPUT:-}"
SHIP="${SHIP//[[:space:]]/}"
if [ -z "$SHIP" ]; then
    if [ "$LIVE" = true ]; then
        echo "::error title=ship_sha is required::A live release must name the exact commit it ships (the soaked commit), as the ship_sha input. Without it the release would be cut from whatever main is now, which is not what was soaked."
        exit 1
    fi
    SHIP="$LAUNCH_SHA"
    echo "::notice::Dry run without ship_sha: rehearsing on the dispatch commit $SHIP."
fi
SHIP=$(tr '[:upper:]' '[:lower:]' <<< "$SHIP")
if [[ ! "$SHIP" =~ ^[0-9a-f]{40}$ ]]; then
    echo "::error title=ship_sha is not a commit id::ship_sha must be a full 40-character commit SHA, got '$SHIP'. A branch or tag name could move between the soak and the cut."
    exit 1
fi

FAILED=0
# A failed ancestry check: refuses a live run, warns in a dry run.
gate() {
    if [ "$LIVE" = true ]; then
        echo "::error title=$1::$2"
        FAILED=1
    else
        echo "::warning title=$1 (dry run, not enforced)::$2"
    fi
}

# compare_status BASE HEAD: GitHub's compare status (ahead, behind, identical,
# diverged) of HEAD relative to BASE, or empty if the API could not answer.
compare_status() {
    gh api "repos/$REPO/compare/$1...$2" --jq '.status' 2>/dev/null || true
}

# --- 2. reachable from main ---------------------------------------------------
STATUS=$(compare_status "$SHIP" main)
case "$STATUS" in
    ahead|identical) echo "✅ ship_sha $SHIP is on main ($STATUS)." ;;
    "") gate "Could not check ship_sha is on main" "The compare API did not answer for $SHIP...main, so whether ship_sha went through the merge queue is unknown." ;;
    *) gate "ship_sha is not on main" "ship_sha $SHIP is not reachable from main (compare status '$STATUS'). Release only commits that merged through the queue." ;;
esac

# --- 3. contains the previous release's ship commit --------------------------
PREV_TAG=$(gh api "repos/$REPO/releases/latest" --jq '.tag_name' 2>/dev/null || true)
PREV_SHIP=""
if [[ ! "$PREV_TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    gate "Could not find the previous release" "The latest published release's tag is '$PREV_TAG', not vX.Y.Z, so the previous ship commit cannot be checked."
    PREV_TAG=""
else
    PREV_COMMIT=$(gh api "repos/$REPO/commits/$PREV_TAG" --jq '.sha' 2>/dev/null || true)
    if [[ ! "$PREV_COMMIT" =~ ^[0-9a-f]{40}$ ]]; then
        gate "Could not resolve $PREV_TAG" "The commit of $PREV_TAG could not be read (got '$PREV_COMMIT')."
    else
        PREV_ON_MAIN=$(compare_status "$PREV_COMMIT" main)
        case "$PREV_ON_MAIN" in
            ahead|identical) PREV_SHIP="$PREV_COMMIT" ;;
            behind|diverged)
                PREV_SHIP=$(gh api "repos/$REPO/commits/$PREV_COMMIT" --jq '.parents[0].sha' 2>/dev/null || true)
                if [[ ! "$PREV_SHIP" =~ ^[0-9a-f]{40}$ ]]; then
                    gate "Could not resolve the previous ship commit" "$PREV_TAG's commit $PREV_COMMIT is not on main, and its first parent could not be read (got '$PREV_SHIP')."
                    PREV_SHIP=""
                fi ;;
            *) gate "Could not check $PREV_TAG against main" "The compare API did not answer for $PREV_COMMIT...main (got '$PREV_ON_MAIN')." ;;
        esac
    fi
    if [ -n "$PREV_SHIP" ]; then
        CONTAINS=$(compare_status "$PREV_SHIP" "$SHIP")
        case "$CONTAINS" in
            ahead|identical) echo "✅ ship_sha contains $PREV_TAG's ship commit $PREV_SHIP ($CONTAINS)." ;;
            "") gate "Could not check ship_sha against $PREV_TAG" "The compare API did not answer for $PREV_SHIP...$SHIP." ;;
            *) gate "ship_sha drops commits $PREV_TAG shipped" "ship_sha $SHIP does not contain $PREV_SHIP, the commit $PREV_TAG shipped (compare status '$CONTAINS'). This release would silently revert whatever $PREV_TAG added after ship_sha." ;;
        esac
    fi
fi

# --- 4. versions read from ship_sha's tree ------------------------------------
# read_file REF PATH: the raw file at REF, or exit 1.
read_file() {
    gh api -H 'Accept: application/vnd.github.raw+json' "repos/$REPO/contents/$2?ref=$1"
}
package_version() {
    awk '/^\[package\]/ {p = 1; next} /^\[/ {p = 0}
         p && /^version *=/ {gsub(/.*= *"|".*/, ""); print; exit}' "$1"
}

SHIP_CORE="$TMPDIR_OUT/ship-core-Cargo.toml"
SHIP_FDEV="$TMPDIR_OUT/ship-fdev-Cargo.toml"
if ! read_file "$SHIP" crates/core/Cargo.toml > "$SHIP_CORE" \
    || ! read_file "$SHIP" crates/fdev/Cargo.toml > "$SHIP_FDEV"; then
    echo "::error title=Could not read ship_sha's manifests::crates/core/Cargo.toml or crates/fdev/Cargo.toml could not be read at $SHIP."
    exit 1
fi
FDEV_SHIP=$(package_version "$SHIP_FDEV")
if [[ ! "$FDEV_SHIP" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "::error title=Could not read fdev's version::No [package] version in crates/fdev/Cargo.toml at $SHIP (got '$FDEV_SHIP')."
    exit 1
fi
FDEV_BASE="$FDEV_SHIP"
if [ -n "$PREV_TAG" ]; then
    TAG_FDEV="$TMPDIR_OUT/tag-fdev-Cargo.toml"
    FDEV_TAG=""
    if read_file "$PREV_TAG" crates/fdev/Cargo.toml > "$TAG_FDEV"; then
        FDEV_TAG=$(package_version "$TAG_FDEV")
    fi
    if [[ ! "$FDEV_TAG" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        # Not an ancestry check: an fdev version this cannot read could be one
        # already on crates.io, so refuse in both modes.
        echo "::error title=Could not read $PREV_TAG's fdev version::crates/fdev/Cargo.toml at $PREV_TAG gave '$FDEV_TAG', so the next fdev version cannot be chosen safely."
        exit 1
    fi
    FDEV_BASE=$(printf '%s\n%s\n' "$FDEV_SHIP" "$FDEV_TAG" | sort -V | tail -1)
fi
IFS=. read -r FM FN FP <<< "$FDEV_BASE"
FDEV_VERSION="$FM.$FN.$((FP + 1))"

if [ "$FAILED" -ne 0 ]; then
    echo "Refusing to release $SHIP; nothing has been created."
    exit 1
fi

{
    echo "ship_sha=$SHIP"
    echo "fdev_version=$FDEV_VERSION"
    echo "prev_tag=$PREV_TAG"
    echo "prev_ship_sha=$PREV_SHIP"
    echo "ship_cargo_toml=$SHIP_CORE"
} >> "$OUT"
echo "✅ Ship commit $SHIP; fdev $FDEV_SHIP (ship) / ${FDEV_TAG:-none} (${PREV_TAG:-no tag}) -> $FDEV_VERSION"
