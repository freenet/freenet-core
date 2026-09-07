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
indent_note() {
    [[ -n "$1" ]] || return 0
    local line
    while IFS= read -r line; do
        echo "      $line"
    done <<<"$1"
}

# The same, on stderr, for a step's error text.
indent_output() {
    indent_note "$1" >&2
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
    echo "  --fdev-version X.Y.Z fdev version to yank. Normally read from"
    echo "                       crates/fdev/Cargo.toml at ORIGIN's release tag. Needed"
    echo "                       whenever origin cannot supply it: the tag is gone from"
    echo "                       origin, origin is unreachable, or origin is not"
    echo "                       freenet/freenet-core. A LOCAL tag is only ever a hint --"
    echo "                       an aborted release leaves stale ones, and a near-miss"
    echo "                       yanks a good release's fdev."
    echo "  --dry-run            Show what would be done without executing"
    echo "  --help               Show this help"
    echo
    echo "Example: $0 --version 0.1.32"
    echo
    echo "⚠️  WARNING: This is a destructive operation!"
    echo "    Use with caution. A yank is reversible (\`cargo yank --undo --version"
    echo "    X.Y.Z <crate>\`), but while it stands it breaks dependency resolution"
    echo "    for everyone building against that version."
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
# the one CI pushed. Reading a stale local tag names a DIFFERENT release's fdev,
# and yanking a good release's crate breaks every build that resolves it until
# somebody notices and undoes it. `git fetch <remote> refs/tags/<tag>` writes
# FETCH_HEAD only; it does not create or move the local tag -- but only with
# --no-tags, because `remote.origin.tagOpt = --tags` otherwise turns even this
# single-ref fetch into a full tag download (demonstrated: a DRY RUN created a
# local v9.9.9).
#
# The resolution's PROVENANCE is recorded alongside the number, because the two
# sources are not interchangeable. Origin's tag (or the operator's own
# --fdev-version) is evidence; a local tag is a hint that may be stale, and this
# number is handed to an irreversible-in-practice `cargo yank`.
RESOLVED_FDEV=""
RESOLVED_FDEV_SOURCE=""

# Is `origin` really freenet/freenet-core?
#
# Steps 2 and 3 delete from whatever origin happens to be, and from a HARDCODED
# --repo freenet/freenet-core -- but the version this resolves is passed to
# `cargo yank`, which always reaches the REAL crates.io. So with origin pointed
# at a fork, a tag naming a different fdev would take a good release's crate off
# the real registry while every other step operated on the fork. Origin is a
# version source only when it is the project's own repository.
#
# The string half is split out so it can be table-tested. It has to be: an
# end-to-end case whose origin is the real `git@github.com:freenet/freenet-core`
# would have step 2 `ls-remote` and `push --delete` against the REAL repository,
# so the only branch production ever takes (the colon form -- the sandbox's
# origin is a filesystem path, which is the slash form) is untestable any other
# way. Verified by deletion: dropping the colon branch left the whole suite
# green before `url_is_freenet_core` existed.
#
# The match is a SUFFIX, deliberately: the test fixture's origin is a bare repo
# at `<sandbox>/github.com/freenet/freenet-core.git`, which is what lets the
# suite exercise this matcher instead of a test-only backdoor in a destructive
# script. Do not anchor it to the start of the string without also rebuilding
# that fixture. It IS anchored on the delimiter -- the character before
# `github.com` must be `/` or `@` -- so a lookalike host like
# `evilgithub.com/freenet/freenet-core` does not match. That is tidiness, not a
# security boundary: anyone who can rewrite your git config can do worse.
url_is_freenet_core() {
    local url="$1"
    # Trailing slash first, so a URL ending `.git/` still loses its `.git`.
    url="${url%/}"
    url="${url%.git}"
    url="${url%/}"
    # The leading "/" gives a bare `github.com:freenet/freenet-core` (a valid
    # scp-like URL with no user) the delimiter the pattern requires.
    local delimited="/$url"
    [[ "$delimited" == *[/@]"github.com/freenet/freenet-core" \
        || "$delimited" == *[/@]"github.com:freenet/freenet-core" ]]
}

origin_is_freenet_core() {
    local url
    # `git remote get-url` EXPANDS `url.<base>.insteadOf`, so an operator using
    # a host alias (`fnalias:core` rewritten to the real repository) is not
    # refused on the genuine repo. Measured on git 2.43.0, since the obvious
    # reading is the opposite and this was queried in review: with
    # `url.git@github.com:freenet/freenet-core.git.insteadOf = fnalias:core`,
    # `remote get-url origin` returns the REWRITTEN url, identical to
    # `ls-remote --get-url`. Do NOT "fix" this to `git config --get
    # remote.origin.url`, which returns the raw configured string and would
    # refuse the genuine repository mid-incident.
    url="$(git -C "$PROJECT_ROOT" remote get-url origin 2>/dev/null)" || return 1
    [[ -n "$url" ]] || return 1
    url_is_freenet_core "$url"
}

# Why origin did not supply a version: "unusable" (refused -- not freenet-core)
# or "unreadable" (fetch failed / no such tag). The distinction matters in the
# message, because "could not read origin" sends an operator to check the
# network when the actual answer is that their remote points somewhere else.
ORIGIN_SOURCE_PROBLEM=""

resolve_fdev_version() {
    local from_origin="" from_local=""

    if ! origin_is_freenet_core; then
        ORIGIN_SOURCE_PROBLEM="unusable"
        echo "warning: origin is not freenet/freenet-core, so its tags are not used as a" >&2
        echo "         source for the fdev version to yank (a yank always reaches the" >&2
        echo "         real crates.io, whatever origin points at)." >&2
    elif git -C "$PROJECT_ROOT" fetch --quiet --no-tags origin "refs/tags/$TAG" 2>/dev/null; then
        from_origin="$(fdev_version_from_ref FETCH_HEAD)" || from_origin=""
    fi
    [[ -n "$from_origin" || -n "$ORIGIN_SOURCE_PROBLEM" ]] || ORIGIN_SOURCE_PROBLEM="unreadable"
    from_local="$(fdev_version_from_ref "refs/tags/$TAG")" || from_local=""

    if [[ -n "$from_origin" ]]; then
        if [[ -n "$from_local" && "$from_local" != "$from_origin" ]]; then
            echo "warning: local $TAG names fdev $from_local, origin's names $from_origin." >&2
            echo "         Using origin's -- that is the tag the release was cut from." >&2
        fi
        RESOLVED_FDEV="$from_origin"
        RESOLVED_FDEV_SOURCE="origin"
        return 0
    fi

    if [[ -n "$from_local" ]]; then
        if [[ "$ORIGIN_SOURCE_PROBLEM" == "unusable" ]]; then
            echo "warning: origin cannot be used as a version source (above). The LOCAL $TAG" >&2
        else
            echo "warning: could not read $TAG from origin. The LOCAL $TAG" >&2
        fi
        echo "         names fdev $from_local, but a local tag can be stale, so that is a" >&2
        echo "         HINT only -- not a version this script will yank on." >&2
        RESOLVED_FDEV="$from_local"
        RESOLVED_FDEV_SOURCE="local"
        return 0
    fi

    return 1
}

# Resolve BEFORE anything destructive runs: steps 1 to 3 delete the tag and the
# release this reads from, so a version that cannot be established afterwards
# cannot be established at all. Done even without --yank-crates, so the hints at
# the end of a tag-only rollback can carry the number the next run will need.
resolve_fdev_version || true
if [[ ! "$RESOLVED_FDEV" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    RESOLVED_FDEV=""
    RESOLVED_FDEV_SOURCE=""
fi

# Where the number that reaches `cargo yank` actually came from: "operator",
# "origin", "local", or empty. Everything downstream that either ACTS on the
# number or hands it back to the operator for a re-run consults this, so a
# local-tag guess can never be laundered into something that reads as fact.
FDEV_VERSION_SOURCE=""
if [[ -n "$FDEV_VERSION" ]]; then
    FDEV_VERSION_SOURCE="operator"
    if [[ -n "$RESOLVED_FDEV" && "$FDEV_VERSION" != "$RESOLVED_FDEV" ]]; then
        # Not fatal -- an operator may deliberately be yanking a version the tag
        # does not name -- but said loudly, above the confirmation prompt,
        # because adjacent fdev patch versions all exist on crates.io and a
        # near-miss yanks a GOOD release.
        if [[ "$RESOLVED_FDEV_SOURCE" == "local" ]]; then
            # Same fork as the hard stop below. Saying "origin could not be
            # read" over the top of the "origin is not freenet/freenet-core"
            # warning printed three lines earlier gives an operator two
            # contradictory explanations directly above a yank confirmation.
            echo "⚠️  --fdev-version $FDEV_VERSION does not match the LOCAL $TAG, which names"
            echo "    fdev $RESOLVED_FDEV."
            if [[ "$ORIGIN_SOURCE_PROBLEM" == "unusable" ]]; then
                echo "    Origin is not freenet/freenet-core, so it could not confirm either"
                echo "    number, and the local tag may itself be the stale one."
            else
                echo "    Origin could not be read, so the local tag may itself be the stale one."
            fi
            echo "    Yanking $FDEV_VERSION as instructed."
        else
            echo "⚠️  --fdev-version $FDEV_VERSION does not match $TAG, which names fdev $RESOLVED_FDEV."
            echo "    Yanking $FDEV_VERSION as instructed."
        fi
        echo
    fi
else
    FDEV_VERSION="$RESOLVED_FDEV"
    FDEV_VERSION_SOURCE="$RESOLVED_FDEV_SOURCE"
fi

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

# A LOCAL tag is never enough to yank on.
#
# Both preconditions are routine and they compose: release.sh skips tag creation
# when a local tag of that name exists, so an aborted run leaves a stale one
# behind, and an origin that cannot be reached mid-incident is ordinary. With
# only the local tag readable the script would otherwise print a stale number as
# fact and yank a GOOD release's fdev -- adjacent fdev patch versions all exist
# on crates.io, so the yank succeeds. Stop and make the operator name it.
if [[ "$YANK_CRATES" == "true" && "$FDEV_VERSION_SOURCE" == "local" ]]; then
    echo "Error: cannot determine which fdev version shipped with $TAG."
    echo
    if [[ "$ORIGIN_SOURCE_PROBLEM" == "unusable" ]]; then
        echo "  Origin is not freenet/freenet-core, so it is not a version source, and"
        echo "  the only thing left is the LOCAL $TAG, which names fdev $FDEV_VERSION."
    else
        echo "  Origin could not be read, so the only source left is the LOCAL $TAG,"
        echo "  which names fdev $FDEV_VERSION."
    fi
    echo "  That is a hint, not evidence:"
    echo "  release.sh skips tag creation when a local tag already exists, so an"
    echo "  aborted run leaves one pointing at a different release -- and yanking"
    echo "  on it takes a GOOD release's fdev off crates.io."
    echo
    echo "  Fix origin and re-run, or confirm the version by hand"
    echo "  (https://crates.io/crates/fdev/versions, and the release announcement"
    echo "  for freenet $VERSION) and pass it explicitly:"
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
    # Never a bare number: the whole point of the gate above is that where this
    # came from decides whether it may be acted on.
    case "$FDEV_VERSION_SOURCE" in
        origin)   echo "fdev:        $FDEV_VERSION (read from origin's $TAG)" ;;
        operator) echo "fdev:        $FDEV_VERSION (given on the command line)" ;;
        *)        echo "fdev:        $FDEV_VERSION" ;;
    esac
fi
echo

# Confirmation prompt
if [[ "$DRY_RUN" == "false" ]]; then
    echo "⚠️  WARNING: This will rollback release $VERSION"
    if [[ "$YANK_CRATES" == "true" ]]; then
        echo "⚠️  This includes YANKING crates from crates.io."
        echo "    A yank is reversible (\`cargo yank --undo --version X.Y.Z <crate>\`), but"
        echo "    until it is undone it breaks dependency resolution for anyone building"
        echo "    against that version."
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
        # `git tag -d` prints "Deleted tag 'vX.Y.Z' (was <sha>)". That SHA is the
        # only record of where the tag pointed, and the next two steps delete the
        # tag from origin and delete the release page -- so print it on SUCCESS,
        # not only when something goes wrong.
        echo "✓"
        indent_note "$step_output"
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
    # Only a version that came from origin or from the operator is echoed back
    # as something to re-run with. A local-tag reading is a hint, and printing it
    # inside a ready-to-paste command turns it into an instruction -- which on
    # the second run also silences the mismatch warning, because the operator is
    # now "explicitly" passing the very guess this script produced.
    if [[ -n "$FDEV_VERSION" && "$FDEV_VERSION_SOURCE" != "local" ]]; then
        echo
        echo "A re-run may no longer be able to read the fdev version from $TAG,"
        echo "so pass it explicitly:"
        echo "  $0 --version $VERSION --yank-crates --fdev-version $FDEV_VERSION"
    elif [[ "$FDEV_VERSION_SOURCE" == "local" ]]; then
        echo
        echo "The local $TAG names fdev $FDEV_VERSION, but origin did not confirm it,"
        echo "so that is a hint and not a version to yank on. Confirm which fdev shipped"
        echo "with freenet $VERSION (https://crates.io/crates/fdev/versions, and the"
        echo "release announcement) before re-running with --yank-crates."
    fi
    exit 1
fi

echo "✅ Rollback complete!"
echo
echo "Next steps:"
echo "  • Verify the tag and release are gone: gh release list --repo freenet/freenet-core"
echo "  • Check crates.io: https://crates.io/crates/freenet"
if [[ "$YANK_CRATES" == "false" ]]; then
    if [[ -n "$FDEV_VERSION" && "$FDEV_VERSION_SOURCE" != "local" ]]; then
        # --fdev-version is included because THIS run deleted the tag the number
        # is read from; without it the suggested command stops with an error.
        echo "  • To yank crates, run: $0 --version $VERSION --yank-crates --fdev-version $FDEV_VERSION"
    else
        # No version this run is willing to stand behind -- either nothing
        # resolved, or only a local tag did. A bare --yank-crates re-run would
        # hard-error (the tag it reads from was just deleted), so do not suggest
        # one: name the lookup and the placeholder instead.
        if [[ "$FDEV_VERSION_SOURCE" == "local" ]]; then
            echo "  • The local $TAG named fdev $FDEV_VERSION, but origin did not confirm it,"
            echo "    so treat that as a hint only."
        fi
        echo "  • To yank crates, look up the fdev version that shipped with freenet $VERSION"
        echo "    (https://crates.io/crates/fdev/versions, or the release announcement) and run:"
        echo "      $0 --version $VERSION --yank-crates --fdev-version X.Y.Z"
    fi
fi
