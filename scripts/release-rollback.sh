#!/bin/bash
# Freenet Release Rollback Script
# Rolls back a failed or problematic release

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Find the git repository root
if ! PROJECT_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null)"; then
    echo "Error: Not in a git repository"
    exit 1
fi

VERSION=""
FDEV_VERSION=""
DRY_RUN=false
YANK_CRATES=false

# Somewhere to capture a command's stderr WITHOUT merging it into the value the
# script then tests. Merging the two is its own bug: `x="$(cmd 2>&1)"` followed
# by `[[ -z "$x" ]]` reads any transport banner ("Warning: Permanently added
# 'github.com' ... to the list of known hosts") as a result.
STDERR_FILE="$(mktemp)"
trap 'rm -f "$STDERR_FILE"' EXIT

# Step failures are accumulated here and decide the exit status.
#
# Every step reports its own outcome and keeps going, because a rollback that
# stops at the first problem leaves the release half-torn-down. What must NOT
# happen is the script printing "Rollback complete" over the top of a step that
# failed -- this is recovery tooling, and a rollback that silently did nothing
# is exactly the moment an operator cannot afford to be told it worked.
FAILURE_COUNT=0
FAILED_STEPS=()
record_failure() {
    FAILURE_COUNT=$((FAILURE_COUNT + 1))
    FAILED_STEPS+=("$1")
}

# Indent a captured command's output so a multi-line error stays readable under
# the step it belongs to. A here-string, never a pipe: this script sets
# `pipefail`, under which a producer piped into a short-circuiting reader dies
# of SIGPIPE and takes the pipeline's status with it (see
# .claude/rules/bug-prevention-patterns.md).
indent_output() {
    [[ -n "$1" ]] || return 0
    local line
    while IFS= read -r line; do
        echo "      $line" >&2
    done <<<"$1"
}

show_help() {
    echo "Freenet Release Rollback Script"
    echo
    echo "Usage: $0 --version X.Y.Z [options]"
    echo
    echo "Rollback actions:"
    echo "  • Delete git tag (local and remote)"
    echo "  • Delete GitHub release"
    echo "  • Optionally yank crates from crates.io (--yank-crates)"
    echo
    echo "Options:"
    echo "  --version X.Y.Z      Version to rollback (required)"
    echo "  --yank-crates        Yank crates from crates.io (optional, use with caution)"
    echo "  --fdev-version X.Y.Z fdev version to yank. Only needed when the release tag"
    echo "                       is no longer available locally or on origin; otherwise"
    echo "                       it is read from crates/fdev/Cargo.toml at the tag."
    echo "  --dry-run            Show what would be done without executing"
    echo "  --help               Show this help"
    echo
    echo "Example: $0 --version 0.1.32"
    echo
    echo "⚠️  WARNING: This is a destructive operation!"
    echo "    Use with caution. Yanking from crates.io cannot be undone."
}

require_value() {
    if [[ $2 -lt 2 ]]; then
        echo "Error: $1 requires a value"
        exit 1
    fi
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --version)
            require_value --version $#
            VERSION="$2"
            shift 2
            ;;
        --fdev-version)
            require_value --fdev-version $#
            FDEV_VERSION="$2"
            shift 2
            ;;
        --yank-crates)
            YANK_CRATES=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

if [[ -z "$VERSION" ]]; then
    echo "Error: --version is required"
    show_help
    exit 1
fi

# Validate version format
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Error: Version must be in format X.Y.Z (e.g., 0.1.32)"
    exit 1
fi

if [[ -n "$FDEV_VERSION" && ! "$FDEV_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Error: --fdev-version must be in format X.Y.Z (e.g., 0.3.293)"
    exit 1
fi

TAG="v$VERSION"

# The [package] version out of a Cargo.toml on stdin.
#
# Anchored to the [package] section rather than taking the first line-anchored
# `version = `: a dependency version has the same SHAPE, so a shape check alone
# would not notice if the two ever swapped places, and this value feeds an
# irreversible yank. It reads the same field .github/workflows/release.yml's
# validate step reads when it bumps fdev; if that ever moves to
# `version.workspace = true`, this returns nothing and the script stops and asks
# for --fdev-version rather than guessing.
package_version() {
    awk '
        /^\[/ { in_pkg = ($0 ~ /^\[package\][[:space:]]*$/); next }
        in_pkg && /^version[[:space:]]*=/ {
            sub(/^version[[:space:]]*=[[:space:]]*"/, "")
            sub(/".*$/, "")
            print
            exit
        }
    '
}

fdev_version_from_ref() {
    local toml
    toml="$(git -C "$PROJECT_ROOT" show "$1:crates/fdev/Cargo.toml" 2>/dev/null)" || return 1
    local parsed
    parsed="$(package_version <<<"$toml")"
    [[ "$parsed" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || return 1
    echo "$parsed"
}

# Which fdev version shipped WITH freenet vX.Y.Z?
#
# fdev's version is independent of freenet's: release.yml bumps fdev's OWN patch
# number in crates/fdev/Cargo.toml on each release, so the two have drifted far
# apart (freenet 0.2.131 shipped alongside fdev 0.3.293). Any arithmetic
# relationship between them is a coincidence of the first few releases -- which
# is what this used to rely on, computing `0.$((minor + 2)).$patch` and asking
# crates.io to yank fdev 0.4.129 for freenet 0.2.129. No such version has ever
# existed, so the yank never yanked anything.
#
# Read the number rather than computing it, and read it from the RELEASE TAG
# rather than from the working tree: by the time anyone is rolling a release
# back, main has usually bumped fdev again, so the working tree names the
# version that shipped with a DIFFERENT release.
#
# ORIGIN'S tag wins over a local tag of the same name. The local one can be
# stale -- release.sh skips tag creation when a tag of that name already exists,
# so an aborted run leaves one behind, while the tag that actually shipped is
# the one CI pushed. Reading a stale local tag would name a different release's
# fdev, and a yank cannot be undone. `git fetch <remote> refs/tags/<tag>` writes
# FETCH_HEAD only; it does not create or move the local tag.
resolve_fdev_version() {
    local from_origin="" from_local=""

    if git -C "$PROJECT_ROOT" fetch --quiet origin "refs/tags/$TAG" 2>/dev/null; then
        from_origin="$(fdev_version_from_ref FETCH_HEAD)" || from_origin=""
    fi
    from_local="$(fdev_version_from_ref "refs/tags/$TAG")" || from_local=""

    if [[ -n "$from_origin" ]]; then
        if [[ -n "$from_local" && "$from_local" != "$from_origin" ]]; then
            echo "warning: local $TAG names fdev $from_local, origin's names $from_origin." >&2
            echo "         Using origin's -- that is the tag the release was cut from." >&2
        fi
        echo "$from_origin"
        return 0
    fi

    if [[ -n "$from_local" ]]; then
        echo "warning: could not read $TAG from origin; using the LOCAL tag, which may" >&2
        echo "         be stale. Pass --fdev-version to be certain." >&2
        echo "$from_local"
        return 0
    fi

    return 1
}

# Resolve BEFORE anything destructive runs: steps 1 to 3 delete the tag and the
# release this reads from, so a version that cannot be established afterwards
# cannot be established at all. Done even without --yank-crates, so the hints at
# the end of a tag-only rollback can carry the number the next run will need.
RESOLVED_FDEV=""
if RESOLVED_FDEV="$(resolve_fdev_version)"; then
    [[ "$RESOLVED_FDEV" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || RESOLVED_FDEV=""
else
    RESOLVED_FDEV=""
fi

if [[ -n "$FDEV_VERSION" && -n "$RESOLVED_FDEV" && "$FDEV_VERSION" != "$RESOLVED_FDEV" ]]; then
    # Not fatal -- an operator may deliberately be yanking a version the tag does
    # not name -- but said loudly, above the confirmation prompt, because
    # adjacent fdev patch versions all exist on crates.io and a near-miss yanks
    # a GOOD release.
    echo "⚠️  --fdev-version $FDEV_VERSION does not match $TAG, which names fdev $RESOLVED_FDEV."
    echo "    Yanking $FDEV_VERSION as instructed."
    echo
fi
[[ -n "$FDEV_VERSION" ]] || FDEV_VERSION="$RESOLVED_FDEV"

if [[ "$YANK_CRATES" == "true" && -z "$FDEV_VERSION" ]]; then
    echo "Error: cannot determine which fdev version shipped with $TAG."
    echo
    echo "  crates/fdev/Cargo.toml could not be read at $TAG (the tag is missing"
    echo "  locally and on origin, or the manifest could not be parsed)."
    echo
    echo "  crates.io lists every published fdev version at"
    echo "  https://crates.io/crates/fdev/versions, and the one that shipped with"
    echo "  freenet $VERSION is named in that release's announcement. Then:"
    echo "    $0 --version $VERSION --yank-crates --fdev-version X.Y.Z"
    exit 1
fi

echo "Freenet Release Rollback"
echo "========================"
echo "Version:     $VERSION"
echo "Tag:         $TAG"
if [[ "$DRY_RUN" == "true" ]]; then
    echo "Mode:        DRY RUN"
fi
if [[ "$YANK_CRATES" == "true" ]]; then
    echo "Yank crates: YES"
    echo "fdev:        $FDEV_VERSION"
fi
echo

# Confirmation prompt
if [[ "$DRY_RUN" == "false" ]]; then
    echo "⚠️  WARNING: This will rollback release $VERSION"
    if [[ "$YANK_CRATES" == "true" ]]; then
        echo "⚠️  This includes YANKING crates from crates.io (cannot be undone)"
    fi
    echo
    # An `if`, because a bare `read` that hits EOF (a non-interactive caller, or
    # `</dev/null`) is a non-zero command under `set -e`: the script would die
    # before printing anything about why.
    if ! read -p "Are you sure you want to continue? (yes/no): " -r; then
        echo
        echo "Aborted (no answer given)."
        exit 1
    fi
    if [[ ! $REPLY =~ ^yes$ ]]; then
        echo "Aborted."
        exit 1
    fi
fi

# Delete local git tag
#
# `refs/tags/$TAG` rather than a bare `$TAG`, so a branch or a file of the same
# name cannot be mistaken for the tag.
echo -n "[1/4] Deleting local git tag... "
if git -C "$PROJECT_ROOT" rev-parse -q --verify "refs/tags/$TAG" >/dev/null 2>&1; then
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY RUN]"
    elif step_output="$(git -C "$PROJECT_ROOT" tag -d "$TAG" 2>&1)"; then
        echo "✓"
    else
        echo "✗ (failed)"
        indent_output "$step_output"
        record_failure "delete local tag $TAG"
    fi
else
    echo "not found, skipping"
fi

# Delete remote git tag
#
# Ask origin for the ONE ref, and keep the three answers apart -- present,
# absent, and "could not ask". The previous form piped `ls-remote --tags` into
# `grep -q`: under `pipefail` the short-circuiting grep kills git with SIGPIPE
# once it matches, so a PRESENT tag reads as absent and the deletion is silently
# skipped. Measured on this repo (34 KB of `ls-remote --tags` output): 10 of 10
# lookups of tags that DO exist returned 141. A failed lookup read as "absent"
# would skip the deletion just as quietly, for a different reason.
echo -n "[2/4] Deleting remote git tag... "
if ! remote_refs="$(git -C "$PROJECT_ROOT" ls-remote --tags origin "refs/tags/$TAG" 2>"$STDERR_FILE")"; then
    echo "✗ (could not query origin)"
    indent_output "$(cat "$STDERR_FILE")"
    record_failure "delete remote tag $TAG (could not query origin)"
elif [[ -z "$remote_refs" ]]; then
    echo "not found, skipping"
elif [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN]"
elif step_output="$(git -C "$PROJECT_ROOT" push origin --delete "refs/tags/$TAG" 2>&1)"; then
    echo "✓"
else
    echo "✗ (failed)"
    indent_output "$step_output"
    record_failure "delete remote tag $TAG"
fi

# Delete GitHub release
#
# Three answers here too, for the same reason as step 2: `gh release view` exits
# non-zero for a release that does not exist AND for gh being absent, the token
# having expired, a rate limit, or an outage. Reading all of those as "not
# found, skipping" leaves the release and its binaries live while the script
# reports a completed rollback. "release not found" is gh's own wording for the
# genuinely-absent case.
echo -n "[3/4] Deleting GitHub release... "
if gh release view "$TAG" --repo freenet/freenet-core --json id >/dev/null 2>"$STDERR_FILE"; then
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY RUN]"
    elif step_output="$(gh release delete "$TAG" --repo freenet/freenet-core --yes 2>&1)"; then
        echo "✓"
    else
        echo "✗ (failed)"
        indent_output "$step_output"
        record_failure "delete GitHub release $TAG"
    fi
elif grep -qi "release not found" "$STDERR_FILE"; then
    echo "not found, skipping"
else
    echo "✗ (could not query GitHub)"
    indent_output "$(cat "$STDERR_FILE")"
    record_failure "delete GitHub release $TAG (could not query GitHub)"
fi

# Is <crate> <version> on crates.io?
#   0 = published, 1 = genuinely absent, 2 = UNKNOWN (do not act on it)
#
# The same tri-state probe scripts/RELEASE_RECOVERY.md documents for the manual
# path. The `-A` is load-bearing: crates.io answers 403 to a request carrying no
# descriptive User-Agent, and a two-state form reads that 403 as "not published"
# for every version ever released. Telling 404 apart from every other status is
# also what stops an outage or a rate-limit from reading as "nothing to yank".
#
# Deliberately a fourth copy rather than a shared helper: release.sh's
# equivalent (crate_version_on_crates_io) is two-state ON PURPOSE, with a long
# comment explaining why its fail-closed direction makes that gap safe there,
# and unifying them means rewriting that function's behavioural fixtures. That
# is a change of its own; this one keeps to the tri-state form the docs already
# specify.
crate_version_state() {
    local code rc=0
    code="$(curl -sS -o /dev/null -w '%{http_code}' -A 'freenet-release-driver' \
        --max-time 30 --retry 3 --retry-all-errors \
        "https://crates.io/api/v1/crates/$1/$2" 2>/dev/null)" || rc=$?
    if [[ $rc -ne 0 ]]; then
        echo "curl could not reach crates.io for $1 $2 (exit $rc) -- UNKNOWN, not 'absent'" >&2
        return 2
    fi
    case "$code" in
        200) return 0 ;;
        404) return 1 ;;
        *)
            echo "crates.io answered HTTP $code for $1 $2 -- UNKNOWN, not 'absent'" >&2
            return 2
            ;;
    esac
}

# Yank one crate version, keeping the three outcomes that matter distinct:
# yanked (or already yanked) is fine, genuinely never published is fine, and
# anything else is a failure that has to reach the exit status.
#
# The crates.io probe runs under --dry-run as well. It is a read-only GET, and a
# dry run whose whole job is previewing an irreversible step should be able to
# say whether the version it resolved is actually there.
yank_crate() {
    local crate="$1" version="$2"

    echo -n "  Yanking $crate v$version... "

    local state=0
    crate_version_state "$crate" "$version" || state=$?
    case "$state" in
        1)
            echo "not published, skipping"
            return 0
            ;;
        2)
            echo "✗ (crates.io status unknown -- NOT yanked)"
            record_failure "yank $crate v$version (could not determine publish state)"
            return 0
            ;;
    esac

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY RUN] (published, would be yanked)"
        return 0
    fi

    # Judge by cargo's EXIT STATUS. The previous form grepped cargo's output for
    # "successfully yanked", a string cargo no longer prints, so even a yank that
    # worked was reported as "✗ (failed or not published)" -- and since nothing
    # consumed that verdict, the script printed "Rollback complete!" underneath
    # it either way.
    local output
    if output="$(cargo yank --version "$version" "$crate" 2>&1)"; then
        echo "✓"
    elif grep -qi "already yanked" <<<"$output"; then
        echo "✓ (already yanked)"
    else
        echo "✗ (yank failed)"
        indent_output "$output"
        record_failure "yank $crate v$version"
    fi

    # Explicit, not incidental: this function must never end on a non-zero
    # status, or `set -e` would abort the rollback between the two yanks and the
    # failure summary would never print.
    return 0
}

# Yank crates from crates.io
if [[ "$YANK_CRATES" == "true" ]]; then
    echo "[4/4] Yanking crates from crates.io..."
    yank_crate freenet "$VERSION"
    yank_crate fdev "$FDEV_VERSION"
else
    echo "[4/4] Skipping crate yanking (use --yank-crates to enable)"
fi

echo
# Counted separately from the array: `${#FAILED_STEPS[@]}` on an EMPTY array is
# an unbound-variable error under `set -u` on bash 3.2, which is what a mac
# operator's /bin/bash is -- and a rollback that dies on its own summary line is
# a poor way to find that out.
if [[ $FAILURE_COUNT -gt 0 ]]; then
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "❌ Dry run INCOMPLETE -- $FAILURE_COUNT step(s) could not be checked:"
    else
        echo "❌ Rollback INCOMPLETE -- $FAILURE_COUNT step(s) failed:"
    fi
    for step in "${FAILED_STEPS[@]}"; do
        echo "  • $step"
    done
    echo
    echo "The release is only partially rolled back. Fix the cause and re-run, or"
    echo "finish the failed steps by hand (see scripts/RELEASE_RECOVERY.md)."
    if [[ -n "$FDEV_VERSION" ]]; then
        echo
        echo "A re-run may no longer be able to read the fdev version from $TAG,"
        echo "so pass it explicitly:"
        echo "  $0 --version $VERSION --yank-crates --fdev-version $FDEV_VERSION"
    fi
    exit 1
fi

echo "✅ Rollback complete!"
echo
echo "Next steps:"
echo "  • Verify the tag and release are gone: gh release list --repo freenet/freenet-core"
echo "  • Check crates.io: https://crates.io/crates/freenet"
if [[ "$YANK_CRATES" == "false" ]]; then
    if [[ -n "$FDEV_VERSION" ]]; then
        # --fdev-version is included because THIS run deleted the tag the number
        # is read from; without it the suggested command stops with an error.
        echo "  • To yank crates, run: $0 --version $VERSION --yank-crates --fdev-version $FDEV_VERSION"
    else
        echo "  • To yank crates, run: $0 --version $VERSION --yank-crates"
    fi
fi
