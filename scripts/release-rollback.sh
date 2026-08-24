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

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --version)
            VERSION="$2"
            shift 2
            ;;
        --fdev-version)
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

# Which fdev version shipped WITH freenet vX.Y.Z?
#
# fdev's version is independent of freenet's: release.yml bumps fdev's OWN
# patch number in crates/fdev/Cargo.toml on each release, so the two have
# drifted far apart (freenet 0.2.131 shipped alongside fdev 0.3.293). Any
# arithmetic relationship between them is a coincidence of the first few
# releases -- which is what this used to rely on, computing
# `0.$((minor + 2)).$patch` and asking crates.io to yank fdev 0.4.129 for
# freenet 0.2.129. No such version has ever existed, so the yank never yanked
# anything.
#
# Read the number rather than computing it, and read it from the RELEASE TAG
# rather than from the working tree: by the time anyone is rolling a release
# back, main has usually bumped fdev again, so the working tree names the
# version that shipped with a DIFFERENT release. A yank cannot be undone, so
# the source has to be the tree the release was actually cut from.
resolve_fdev_version() {
    local toml=""
    if toml="$(git -C "$PROJECT_ROOT" show "$TAG:crates/fdev/Cargo.toml" 2>/dev/null)"; then
        :
    elif git -C "$PROJECT_ROOT" fetch --quiet origin "refs/tags/$TAG" 2>/dev/null &&
        toml="$(git -C "$PROJECT_ROOT" show FETCH_HEAD:crates/fdev/Cargo.toml 2>/dev/null)"; then
        # The tag is gone locally but still on origin -- a re-run after a
        # partially completed rollback. FETCH_HEAD is read-only; this does not
        # recreate the local tag.
        :
    else
        return 1
    fi

    # The [package] version is the first `version = "..."` at the start of a
    # line; dependency versions all appear later and are indented or inline.
    local parsed
    parsed="$(awk -F'"' '/^version = "/ { print $2; exit }' <<<"$toml")"
    [[ "$parsed" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || return 1
    echo "$parsed"
}

# Resolve BEFORE anything destructive runs: steps 1 and 2 delete the very tag
# this reads from, and discovering the version is unavailable after the tag is
# gone would leave the operator with nothing to look it up from.
if [[ "$YANK_CRATES" == "true" && -z "$FDEV_VERSION" ]]; then
    if ! FDEV_VERSION="$(resolve_fdev_version)"; then
        echo "Error: cannot determine which fdev version shipped with $TAG."
        echo
        echo "  crates/fdev/Cargo.toml could not be read at $TAG (the tag is"
        echo "  missing locally and on origin, or the manifest is unreadable)."
        echo
        echo "  Look the version up on the release page or crates.io, then pass it:"
        echo "    $0 --version $VERSION --yank-crates --fdev-version X.Y.Z"
        exit 1
    fi
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
    read -p "Are you sure you want to continue? (yes/no): " -r
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
# once it matches, so on a repo with enough tags a PRESENT tag reads as absent
# and the deletion is silently skipped. A failed lookup read as "absent" would
# do the same thing for a different reason.
echo -n "[2/4] Deleting remote git tag... "
if ! step_output="$(git -C "$PROJECT_ROOT" ls-remote --tags origin "refs/tags/$TAG" 2>&1)"; then
    echo "✗ (could not query origin)"
    indent_output "$step_output"
    record_failure "delete remote tag $TAG (could not query origin)"
elif [[ -z "$step_output" ]]; then
    echo "not found, skipping"
elif [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN]"
elif step_output="$(git -C "$PROJECT_ROOT" push origin --delete "$TAG" 2>&1)"; then
    echo "✓"
else
    echo "✗ (failed)"
    indent_output "$step_output"
    record_failure "delete remote tag $TAG"
fi

# Delete GitHub release
echo -n "[3/4] Deleting GitHub release... "
if gh release view "$TAG" --repo freenet/freenet-core >/dev/null 2>&1; then
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY RUN]"
    elif step_output="$(gh release delete "$TAG" --repo freenet/freenet-core --yes 2>&1)"; then
        echo "✓"
    else
        echo "✗ (failed)"
        indent_output "$step_output"
        record_failure "delete GitHub release $TAG"
    fi
else
    echo "not found, skipping"
fi

# Is <crate> <version> on crates.io?
#   0 = published, 1 = genuinely absent, 2 = UNKNOWN (do not act on it)
#
# The same tri-state probe scripts/RELEASE_RECOVERY.md documents for the manual
# path. The `-A` is load-bearing: crates.io answers 403 to a request carrying no
# descriptive User-Agent, and a two-state form reads that 403 as "not published"
# for every version ever released. Telling 404 apart from every other status is
# also what stops an outage or a rate-limit from reading as "nothing to yank".
crate_version_state() {
    local code
    code="$(curl -sS -o /dev/null -w '%{http_code}' -A 'freenet-release-driver' \
        --max-time 30 --retry 3 --retry-all-errors \
        "https://crates.io/api/v1/crates/$1/$2" 2>/dev/null)" || return 2
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
yank_crate() {
    local crate="$1" version="$2"

    echo -n "  Yanking $crate v$version... "
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY RUN]"
        return 0
    fi

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
# operator's /bin/bash is -- and a rollback that dies on its own summary line
# is a poor way to find that out.
if [[ $FAILURE_COUNT -gt 0 ]]; then
    echo "❌ Rollback INCOMPLETE -- $FAILURE_COUNT step(s) failed:"
    for step in "${FAILED_STEPS[@]}"; do
        echo "  • $step"
    done
    echo
    echo "The release is only partially rolled back. Fix the cause and re-run, or"
    echo "finish the failed steps by hand (see scripts/RELEASE_RECOVERY.md)."
    exit 1
fi

echo "✅ Rollback complete!"
echo
echo "Next steps:"
echo "  • Verify the tag and release are gone: gh release list --repo freenet/freenet-core"
echo "  • Check crates.io: https://crates.io/crates/freenet"
if [[ "$YANK_CRATES" == "false" ]]; then
    echo "  • To yank crates, run: $0 --version $VERSION --yank-crates"
fi
