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
#   4. ship_sha's tree carries the exact-SHA release flow: release-sync.yml
#      exists and ci.yml's push trigger admits `release/v*`. The tag- and
#      release-triggered workflows (cross-compile, release-announce,
#      gateway-update, release-sync) run from the TAG's tree, which is
#      ship_sha plus the bump, and without a ci.yml push trigger B gets no CI
#      at all. An older ship_sha would release with the old cascade.
#   5. The fdev version: one patch above the highest of fdev in ship_sha, fdev
#      in the highest-version release TAG (drafts and unfinished cuts
#      included, since their fdev may already be built), and fdev's newest
#      version on crates.io. main's fdev lags a release until the merge-back
#      (release-sync.yml), so ship_sha alone could reuse a published one.
#
# "The latest release" is the highest-semver published, non-prerelease release
# (scripts/release_lib.sh), never GitHub's movable /releases/latest marker.
#
# Checks 2, 3 and 4 FAIL CLOSED in a live run: any lookup this script cannot
# read refuses the run. In a dry run they only warn, so a dry run dispatched
# from a feature branch (to rehearse a release.yml change) still runs to the
# end. Check 1 and the reads behind check 5 fail in both modes: nothing after
# this step can run without them.
#
# Every lookup goes through the GitHub API (no fetch), so the job needs no
# history and the tests can stub `gh`.
#
# Inputs (env): SHIP_SHA_INPUT (may be empty), RELEASE_LIVE, LAUNCH_SHA,
# RELEASE_VERSION (X.Y.Z, required: check 4 tests the real release/vX.Y.Z
# branch name), GITHUB_REPOSITORY, GH_TOKEN, GITHUB_OUTPUT,
# RUNNER_TEMP.
# Outputs: ship_sha, fdev_version, prev_tag, prev_ship_sha, ship_cargo_toml
# (a copy of ship_sha's crates/core/Cargo.toml, for check_release_preflight.sh).
# Tests: scripts/resolve_release_ship_test.sh

set -euo pipefail

REPO="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
# shellcheck source=scripts/release_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/release_lib.sh"
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
PREV_TAG=""
if ROWS=$(rel_release_rows 2>/dev/null); then
    PREV_TAG=$(rel_latest_published "$ROWS" || true)
fi
PREV_SHIP=""
if ! rel_is_semver_tag "$PREV_TAG"; then
    gate "Could not find the previous release" "No published vX.Y.Z release could be read (got '$PREV_TAG'), so the previous ship commit cannot be checked."
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

# read_file REF PATH: the raw file at REF, or exit 1.
read_file() {
    gh api -H 'Accept: application/vnd.github.raw+json' "repos/$REPO/contents/$2?ref=$1"
}

# --- 4. ship_sha carries the exact-SHA release flow -----------------------------
SHIP_CI="$TMPDIR_OUT/ship-ci.yml"
if ! read_file "$SHIP" .github/workflows/release-sync.yml > /dev/null 2>&1; then
    gate "ship_sha predates the exact-SHA release flow" "$SHIP has no .github/workflows/release-sync.yml (or it could not be read). The tag-triggered workflows run from the release commit's tree, so this release would ship without the merge-back. Ship a commit that contains it (docs/RELEASING.md, 'First release under this flow')."
fi
# The branch this release's commit is pushed to. validate resolves the
# version (auto-bumping a blank input) before this script runs, in both modes.
if [[ ! "${RELEASE_VERSION:-}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "::error::RELEASE_VERSION must be X.Y.Z (got '${RELEASE_VERSION:-}')."
    exit 1
fi
RELEASE_BRANCH="release/v$RELEASE_VERSION"
# Parsed as YAML (flow or block lists, any quoting, `on:` read by PyYAML as
# the boolean key True), with GitHub's branch-filter semantics for `*` and
# `**`: no filter admits every branch, `branches` patterns are evaluated in
# order with `!` negation (the last match wins), `branches-ignore` excludes.
# `*` stops at `/`, `**` does not. The other glob characters (`?`, `+`,
# `[...]`) are matched literally, and `paths`/`paths-ignore` are not
# evaluated; both can only cause a false refusal here or a wait_for_ci
# no-run timeout later, never an ungated tag. PyYAML ships on ubuntu-latest.
# Exit 0: a push to the release branch runs ci.yml; 1: it does not; 3: the
# file could not be read as a workflow.
ci_admits_release_push() {
    python3 - "$1" "$2" <<'PY_CI'
import re, sys
try:
    import yaml
except ImportError:
    print("PyYAML is not installed", file=sys.stderr)
    sys.exit(3)
try:
    doc = yaml.safe_load(open(sys.argv[1]))
except Exception as e:
    print(f"unparseable: {e}", file=sys.stderr)
    sys.exit(3)
branch = sys.argv[2]
def main():
    if not isinstance(doc, dict):
        return 3
    on = doc.get("on", doc.get(True))
    if isinstance(on, str):
        on = {on: None}
    elif isinstance(on, list):
        on = {k: None for k in on}
    if not isinstance(on, dict) or "push" not in on:
        return 1
    push = on["push"] or {}
    if not isinstance(push, dict):
        return 3
    def glob(p):
        out, i = "", 0
        while i < len(p):
            if p.startswith("**", i): out += ".*"; i += 2
            elif p[i] == "*": out += "[^/]*"; i += 1
            else: out += re.escape(p[i]); i += 1
        return re.compile(out + "$")
    def as_list(v):
        return [v] if isinstance(v, str) else list(v or [])
    if "branches" in push:
        admitted = False
        for pat in as_list(push["branches"]):
            pat = str(pat)
            neg = pat.startswith("!")
            if glob(pat[1:] if neg else pat).match(branch):
                admitted = not neg
        return 0 if admitted else 1
    if "branches-ignore" in push:
        return 1 if any(glob(str(p)).match(branch) for p in as_list(push["branches-ignore"])) else 0
    if "tags" in push or "tags-ignore" in push:
        return 1  # a tags-only push filter runs on no branch push
    return 0
try:
    sys.exit(main())
except Exception as e:  # a malformed workflow is unreadable, not "does not gate"
    print(f"malformed: {e!r}", file=sys.stderr)
    sys.exit(3)
PY_CI
}
if ! read_file "$SHIP" .github/workflows/ci.yml > "$SHIP_CI" 2>/dev/null; then
    gate "Could not read ship_sha's ci.yml" "Whether ci.yml at $SHIP runs on pushes to release/v* is unknown."
else
    rc=0
    ci_admits_release_push "$SHIP_CI" "$RELEASE_BRANCH" 2> "$TMPDIR_OUT/ci-parse.err" || rc=$?
    case "$rc" in
        0) ;;
        1) gate "ship_sha's CI does not gate release branches" "ci.yml at $SHIP does not run on pushes to $RELEASE_BRANCH, so the release commit would get no CI and wait_for_ci would fail after its no-run timeout. Ship a commit that contains the exact-SHA flow." ;;
        *) gate "Could not parse ship_sha's ci.yml" "ci.yml at $SHIP could not be read as a workflow ($(head -c 200 "$TMPDIR_OUT/ci-parse.err")), so whether it gates release branches is unknown." ;;
    esac
fi

# --- 5. versions --------------------------------------------------------------
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
# fdev in the highest-version tag on origin: a draft or an unfinished cut has
# already chosen its fdev version, which must not be chosen again.
if ! TAGS=$(rel_remote_tags); then
    echo "::error title=Could not list tags::The release tags on origin could not be listed, so the next fdev version cannot be chosen safely."
    exit 1
fi
TOP_TAG=$(rel_max_version <<< "$TAGS")
if [ -z "$TOP_TAG" ] && [ -n "$PREV_TAG" ]; then
    # A published release exists, so its tag must: an empty listing is a
    # lookup that went wrong, not a repository without releases.
    echo "::error title=No release tags found::Origin lists no vX.Y.Z tag, yet $PREV_TAG is published, so the next fdev version cannot be chosen safely."
    exit 1
fi
FDEV_TAG=""
if [ -n "$TOP_TAG" ]; then
    TOP_TAG="v$TOP_TAG"
    TAG_FDEV="$TMPDIR_OUT/tag-fdev-Cargo.toml"
    if read_file "$TOP_TAG" crates/fdev/Cargo.toml > "$TAG_FDEV" 2>/dev/null; then
        FDEV_TAG=$(package_version "$TAG_FDEV")
    fi
    if [[ ! "$FDEV_TAG" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        # Not an ancestry check: an fdev version this cannot read could be one
        # already on crates.io, so refuse in both modes.
        echo "::error title=Could not read $TOP_TAG's fdev version::crates/fdev/Cargo.toml at $TOP_TAG gave '$FDEV_TAG', so the next fdev version cannot be chosen safely."
        exit 1
    fi
fi
# ...and fdev's newest version on crates.io. Three-state like the freenet
# check in check_release_preflight.sh: an answer this cannot read refuses.
FDEV_CRATES=$(curl -sS -A 'freenet-release-driver' --max-time 30 --retry 3 --retry-all-errors \
    https://crates.io/api/v1/crates/fdev 2>/dev/null | jq -r '.crate.max_version // empty' 2>/dev/null || true)
if [[ ! "$FDEV_CRATES" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "::error title=Could not read fdev's version on crates.io::crates.io gave '$FDEV_CRATES' for fdev's newest version, so the next fdev version cannot be chosen safely."
    exit 1
fi
FDEV_BASE=$(printf '%s\n' "$FDEV_SHIP" "$FDEV_TAG" "$FDEV_CRATES" | rel_max_version)
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
echo "✅ Ship commit $SHIP; fdev $FDEV_SHIP (ship) / ${FDEV_TAG:-none} (${TOP_TAG:-no tag}) / $FDEV_CRATES (crates.io) -> $FDEV_VERSION"
