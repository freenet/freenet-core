#!/bin/bash
# Freenet Release Script
# Handles version bumping, testing, publishing, tagging, cross-compilation, and deployment

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Find the git repository root (works from any directory in the repo)
if ! PROJECT_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null)"; then
    echo "Error: Not in a git repository"
    exit 1
fi

# Save original branch so we can restore it on exit (avoids leaving user on release branch)
ORIGINAL_BRANCH="$(git -C "$PROJECT_ROOT" branch --show-current 2>/dev/null || echo "")"
restore_branch() {
    local current
    current="$(git -C "$PROJECT_ROOT" branch --show-current 2>/dev/null || echo "")"
    if [[ -n "$ORIGINAL_BRANCH" && "$current" != "$ORIGINAL_BRANCH" ]]; then
        echo ""
        echo "Restoring original branch ($ORIGINAL_BRANCH)..."
        git -C "$PROJECT_ROOT" checkout "$ORIGINAL_BRANCH" 2>/dev/null || true
    fi
}
trap restore_branch EXIT

# Parse arguments
VERSION=""
MIN_COMPATIBLE=""
DRY_RUN=false
SKIP_TESTS=false
# No DEPLOY_LOCAL / DEPLOY_REMOTE: `--deploy-local` and `--deploy-remote` are
# deprecated and their handler below only prints a note, so the two variables
# were written once and never read again.

# Release steps for state tracking (in execution order).
#
# CRATES_PUBLISHED moved below RELEASE_CREATED: the crates.io upload is the one
# irreversible step and now runs downstream of the blocking pre-flight canary,
# in cross-compile.yml's attach-to-release job. This list is only printed
# (the "Steps to execute" summary), but printing them out of order would
# describe a pipeline that no longer exists.
RELEASE_STEPS=(
    "PR_CREATED"
    "PR_MERGED"
    "TAG_CREATED"
    "RELEASE_CREATED"
    "CRATES_PUBLISHED"
    "GATEWAYS_UPDATED"
    "MATRIX_ANNOUNCED"
    "RIVER_ANNOUNCED"
)

# Completed steps (populated by auto-detection)
declare -A COMPLETED_STEPS

show_help() {
    echo "Freenet Release Script"
    echo
    echo "Usage: $0 --version X.Y.Z [options]"
    echo
    echo "This script automates the complete release process:"
    echo "• Version bumping → Release PR → GitHub CI → Auto-merge"
    echo "• Tag + DRAFT GitHub release → cross-compilation (fired by the tag)"
    echo "• Then, inside cross-compile.yml: attach binaries → blocking auto-update"
    echo "  pre-flight canary → publish to crates.io → un-draft the release"
    echo
    echo "Options:"
    echo "  --version X.Y.Z           Target version (required)"
    echo "  --min-compatible X.Y.Z    Minimum compatible version for range-based"
    echo "                            version checking (default: previous release)"
    echo "  --skip-tests              Skip pre-release tests"
    echo "  --dry-run                 Show what would be done without executing"
    echo "  --help                    Show this help"
    echo
    echo "Resumption:"
    echo "  The script automatically detects completed steps and skips them."
    echo "  If a release fails mid-way, simply re-run with the same --version."
    echo "  State is also saved to /tmp/release-X.Y.Z.state for inspection."
    echo
    echo "Notes:"
    echo "  • Relies on GitHub CI for testing (more reliable than local tests)"
    echo "  • Cross-compilation triggered automatically when version tag is pushed"
    echo "  • Gateways are updated immediately after binaries are available (no 10-min wait)"
    echo
    echo "Example: $0 --version 0.1.18"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --version)
            VERSION="$2"
            shift 2
            ;;
        --deploy-local|--deploy-remote)
            echo "Note: --deploy-local and --deploy-remote are deprecated."
            echo "      Gateways are now updated automatically after binaries are available."
            shift
            ;;
        --min-compatible)
            MIN_COMPATIBLE="$2"
            shift 2
            ;;
        --skip-tests)
            SKIP_TESTS=true
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
    echo "Error: Version must be in format X.Y.Z (e.g., 0.1.17)"
    exit 1
fi

# Default min-compatible to the current version in Cargo.toml (the version before the bump).
# This means gateways running the new version will still accept peers on the previous version.
if [[ -z "$MIN_COMPATIBLE" ]]; then
    MIN_COMPATIBLE=$(grep "^version" "$PROJECT_ROOT/crates/core/Cargo.toml" 2>/dev/null | head -1 | cut -d'"' -f2)
    echo "Min-compatible version: $MIN_COMPATIBLE (defaulting to current version before bump)"
else
    echo "Min-compatible version: $MIN_COMPATIBLE (explicitly set)"
fi
export FREENET_MIN_COMPATIBLE_VERSION="$MIN_COMPATIBLE"

# Get the most recently published version from crates.io (most authoritative source).
#
# `cargo search` is CORRECT here and must not be "fixed" to the registry
# endpoint: this genuinely wants the NEWEST published version, which is exactly
# what it reports. The rule is that `cargo search` may only answer questions
# about the newest version -- fine for this comparison, wrong wherever the
# question is "is version X published?", which is why
# crate_version_on_crates_io() below uses the per-version endpoint instead.
echo -n "Checking latest published version on crates.io... "
PUBLISHED_VERSION=$(cargo search freenet --limit 1 2>/dev/null | grep "^freenet =" | head -1 | cut -d'"' -f2)
if [[ -z "$PUBLISHED_VERSION" ]]; then
    echo "⚠️  Could not query crates.io"
    echo "Warning: Unable to verify against published version"
    PUBLISHED_VERSION="0.0.0"  # Fallback to allow any version
else
    echo "v$PUBLISHED_VERSION"
fi

# Version comparison function
version_compare() {
    local v1="$1"
    local v2="$2"

    local v1_major
    v1_major=$(echo "$v1" | cut -d. -f1) || true
    local v1_minor
    v1_minor=$(echo "$v1" | cut -d. -f2) || true
    local v1_patch
    v1_patch=$(echo "$v1" | cut -d. -f3) || true

    local v2_major
    v2_major=$(echo "$v2" | cut -d. -f1) || true
    local v2_minor
    v2_minor=$(echo "$v2" | cut -d. -f2) || true
    local v2_patch
    v2_patch=$(echo "$v2" | cut -d. -f3) || true

    if [[ $v1_major -gt $v2_major ]]; then echo "1"; return; fi
    if [[ $v1_major -lt $v2_major ]]; then echo "-1"; return; fi
    if [[ $v1_minor -gt $v2_minor ]]; then echo "1"; return; fi
    if [[ $v1_minor -lt $v2_minor ]]; then echo "-1"; return; fi
    if [[ $v1_patch -gt $v2_patch ]]; then echo "1"; return; fi
    if [[ $v1_patch -lt $v2_patch ]]; then echo "-1"; return; fi
    echo "0"
}

# Download release binary from GitHub releases
# This ensures we deploy the exact same binary that users download
# Returns: path to downloaded binary on stdout (status messages go to stderr)
download_release_binary() {
    local version="$1"
    local target_dir="${2:-/tmp}"

    # Detect architecture
    local arch
    arch=$(uname -m) || true
    local asset_name=""

    case "$arch" in
        x86_64)
            asset_name="freenet-x86_64-unknown-linux-musl.tar.gz"
            ;;
        aarch64|arm64)
            asset_name="freenet-aarch64-unknown-linux-musl.tar.gz"
            ;;
        *)
            echo "  ⚠️  Unsupported architecture: $arch" >&2
            return 1
            ;;
    esac

    local download_url="https://github.com/freenet/freenet-core/releases/download/v${version}/${asset_name}"
    # Use a unique subdirectory to avoid conflicts with gateway's /tmp/freenet runtime dir
    local extract_dir="${target_dir}/freenet-release-extract-$$"
    local tar_file="${extract_dir}/${asset_name}"
    local binary_path="${target_dir}/freenet-release-${version}"

    echo "  Downloading release binary from GitHub..." >&2
    echo "    URL: $download_url" >&2

    # Create extraction directory
    mkdir -p "$extract_dir"

    # Download the tarball
    if ! curl -L -s -o "$tar_file" "$download_url"; then
        echo "  ⚠️  Failed to download release binary" >&2
        rm -rf "$extract_dir"
        return 1
    fi

    # Extract the binary to our isolated directory
    if ! tar -xzf "$tar_file" -C "$extract_dir"; then
        echo "  ⚠️  Failed to extract release binary" >&2
        rm -rf "$extract_dir"
        return 1
    fi

    # The tarball contains just "freenet" binary
    if [[ -f "${extract_dir}/freenet" ]]; then
        mv "${extract_dir}/freenet" "$binary_path"
        chmod +x "$binary_path"
        rm -rf "$extract_dir"
        echo "    Binary: $binary_path" >&2

        # Verify the binary
        local dl_version
        dl_version=$("$binary_path" --version 2>/dev/null | head -1) || true
        echo "    Version: $dl_version" >&2

        # Output only the path to stdout for capture
        echo "$binary_path"
        return 0
    else
        echo "  ⚠️  Binary not found in tarball" >&2
        rm -rf "$extract_dir"
        return 1
    fi
}

# Validate requested version against published version
VERSION_CMP=$(version_compare "$VERSION" "$PUBLISHED_VERSION")

if [[ "$VERSION_CMP" == "-1" ]]; then
    echo "Error: Cannot release v$VERSION - published version is v$PUBLISHED_VERSION (would be a downgrade)"
    echo "  Published version on crates.io: v$PUBLISHED_VERSION"
    echo "  Requested version:              v$VERSION"
    exit 1
elif [[ "$VERSION_CMP" == "0" ]]; then
    echo "Note: Requested version v$VERSION matches published version on crates.io"
    echo "  This appears to be a re-run of a previous release attempt"
    echo "  The script will skip already-completed steps (e.g., crates.io publish)"
elif [[ "$VERSION_CMP" == "1" ]]; then
    echo "Releasing new version: v$PUBLISHED_VERSION → v$VERSION"
fi

# State file for tracking progress (backup for manual inspection)
STATE_FILE="/tmp/release-${VERSION}.state"

# Provisional fdev version: bump the current Cargo.toml patch by one. On a
# fresh run this is the target version. On a resume `load_state_file` will
# overwrite FDEV_VERSION with the value persisted when the release PR was
# created, so we never double-bump when the local Cargo.toml already reflects
# the just-released version. See the v0.2.42 incident for the bug this guards
# against: the summary printed 0.3.206, crates.io shipped 0.3.205.
CURRENT_FDEV_VERSION=$(grep "^version" "$PROJECT_ROOT/crates/fdev/Cargo.toml" 2>/dev/null | cut -d'"' -f2)
if [[ -n "$CURRENT_FDEV_VERSION" ]]; then
    FDEV_MAJOR=$(echo "$CURRENT_FDEV_VERSION" | cut -d. -f1)
    FDEV_MINOR=$(echo "$CURRENT_FDEV_VERSION" | cut -d. -f2)
    FDEV_PATCH=$(echo "$CURRENT_FDEV_VERSION" | cut -d. -f3)
    FDEV_NEW_PATCH=$((FDEV_PATCH + 1))
    FDEV_VERSION="${FDEV_MAJOR}.${FDEV_MINOR}.${FDEV_NEW_PATCH}"
else
    # Fallback if can't read current version
    echo "Warning: Could not read current fdev version, using 0.3.1"
    FDEV_VERSION="0.3.1"
fi

# ============================================================================
# State Management Functions
# ============================================================================

# Save current state to file
save_state() {
    local step="$1"
    COMPLETED_STEPS["$step"]=1

    # Write all completed steps to state file
    {
        echo "# Release state for v$VERSION"
        echo "# Generated: $(date -Iseconds)"
        echo "VERSION=$VERSION"
        echo "FDEV_VERSION=$FDEV_VERSION"
        for s in "${!COMPLETED_STEPS[@]}"; do
            echo "COMPLETED_$s=1"
        done
    } > "$STATE_FILE"
}

# Load state from file (for inspection, auto-detect is primary)
load_state_file() {
    if [[ -f "$STATE_FILE" ]]; then
        echo "  Found existing state file: $STATE_FILE"
        while IFS='=' read -r key value; do
            if [[ "$key" =~ ^COMPLETED_ ]]; then
                local step="${key#COMPLETED_}"
                COMPLETED_STEPS["$step"]=1
            elif [[ "$key" == "FDEV_VERSION" && -n "$value" ]]; then
                # Restore FDEV_VERSION from state so resumes after the release
                # PR has already merged don't bump the already-published version
                # a second time. Without this override, the top-level compute at
                # script startup reads the just-released Cargo.toml value and
                # adds 1, leaving the final summary printing a version that is
                # one ahead of what actually shipped (v0.2.42 incident).
                if [[ "$FDEV_VERSION" != "$value" ]]; then
                    echo "  Restoring FDEV_VERSION from state: $value (was $FDEV_VERSION)"
                    FDEV_VERSION="$value"
                fi
            fi
        done < "$STATE_FILE"
    fi
}

# Check if a step is completed
is_step_completed() {
    local step="$1"
    [[ "${COMPLETED_STEPS[$step]:-}" == "1" ]]
}

# Mark step completed and save state
mark_completed() {
    local step="$1"
    save_state "$step"
    echo "  ✓ [$step] completed"
}

# ============================================================================
# Auto-Detection Functions
# ============================================================================

# Detect release PR state
detect_pr_state() {
    local branch_name="release/v$VERSION"

    # Check for existing PR
    local pr_info
    pr_info=$(gh pr list --head "$branch_name" --state all --limit 1 \
        --json number,state 2>/dev/null | jq -r '.[0] | "\(.number)|\(.state)"' 2>/dev/null || echo "")

    if [[ -z "$pr_info" || "$pr_info" == "null|null" ]]; then
        return  # No PR exists
    fi

    local pr_number
    pr_number=$(echo "$pr_info" | cut -d'|' -f1) || true
    local pr_state
    pr_state=$(echo "$pr_info" | cut -d'|' -f2) || true

    if [[ -n "$pr_number" && "$pr_number" != "null" ]]; then
        COMPLETED_STEPS["PR_CREATED"]=1

        if [[ "$pr_state" == "MERGED" ]]; then
            COMPLETED_STEPS["PR_MERGED"]=1
        fi
    fi
}

# Detect if tag exists
#
# No `... | grep -q` anywhere below: this script sets `pipefail`, and a
# short-circuiting reader makes the producer die with SIGPIPE (141), which
# pipefail then promotes to the pipeline's status -- so a tag that IS present
# reads as ABSENT. See the SIGPIPE section of .claude/rules/bug-prevention-patterns.md.
# Ask git for the one ref we care about and test whether the answer is empty.
detect_tag_state() {
    # Check local tags
    if [[ -n "$(git tag -l "v$VERSION")" ]]; then
        COMPLETED_STEPS["TAG_CREATED"]=1
        return
    fi

    # Check remote tags
    if [[ -n "$(git ls-remote --tags origin "refs/tags/v$VERSION" 2>/dev/null)" ]]; then
        COMPLETED_STEPS["TAG_CREATED"]=1
    fi
}

# Is <crate> <version> already on crates.io?
#
# Asks the REGISTRY endpoint for that exact version, NOT `cargo search`.
# `cargo search` reads the SEARCH index, which lags the registry index, and
# `--limit 1` only ever reports a crate's newest version. Before the publish
# moved downstream of Gate A this guard was near-always "no" and the lag did not
# matter; now the workflow has normally published already, so it is near-always
# "yes" and the lag decides the answer. A lagging search index would report "no"
# for a version that IS published, this function's caller would publish, cargo
# would reject the duplicate, `run_cmd` would exit 1 -- and a fully successful
# release would show up as a red driver, repeatedly, until the index caught up.
#
# Same endpoint and same reasoning as cross-compile.yml's publish step; keep the
# two in step. `jq` reads a variable via a here-string rather than a pipe:
# under `set -o pipefail` a short-circuiting reader makes the producer die 141
# and a PRESENT answer read as ABSENT, which is the very failure being fixed.
# DEFERRED, deliberately: this is two-state (present / not-present) while
# `RELEASE_RECOVERY.md`'s `published()` helper is three-state (200 / 404 /
# UNKNOWN). A 403, 429 or 5xx therefore reads here as "not published".
#
# Left as-is for now because the direction is FAIL-CLOSED and the docs' is not.
# A false "absent" here makes the caller attempt a duplicate upload, which
# crates.io rejects, which fails the step loudly and leaves the release a draft.
# The dangerous inverse -- a false "present" causing a publish to be skipped --
# needs a genuine 200 body and is unreachable. An operator acting on a false
# "not published" in the docs, by contrast, re-tags a spent version, which is
# irreversible; that is why the stricter form was applied there first.
#
# It is also protected in practice by the User-Agent lint in
# release_canary_wiring_test.sh: the realistic way to get a non-200/404 here is
# a missing or empty UA, and that is now pinned across every crates.io call site
# in the repo.
#
# Worth doing properly, and the tri-state helper to copy already exists. Not
# done in this PR because the matching change in cross-compile.yml's publish
# step would require rewriting the behavioural fixtures that stub `curl` with
# response BODIES, and adding an unverified surface late is a worse trade than
# a documented fail-closed gap.
crate_version_on_crates_io() {
    local body
    body="$(curl -sS -A 'freenet-release-driver' --max-time 30 \
        --retry 3 --retry-all-errors \
        "https://crates.io/api/v1/crates/$1/$2" 2>/dev/null)" || return 1
    jq -e '.version.num? // empty' >/dev/null 2>&1 <<<"$body"
}

# Detect if crates are published
detect_crates_state() {
    # BOTH crates, via the REGISTRY endpoint rather than `cargo search` -- see
    # crate_version_on_crates_io above.
    #
    # The `&&` is load-bearing, and the reason is worth reading before anyone
    # "simplifies" it back to a single check.
    #
    # This sets the CRATES_PUBLISHED resume flag, and `publish_crates` RETURNS
    # EARLY on that flag -- above its own per-crate logic. So a flag set on
    # freenet alone makes the fdev branch unreachable, and the driver reports a
    # successful release having never published fdev. The scenario is one this
    # repo now documents as expected: `attach-to-release` publishes freenet,
    # `cargo publish -p fdev` fails (the #4240 class, which docs/RELEASING.md
    # records as having no pre-flight anywhere), the operator resumes
    # release.sh, and it declares the crates step already complete.
    #
    # This checked freenet only for as long as it used `cargo search`, whose
    # index lag usually answered "not published" -- so the flag went unset,
    # publish_crates ran, and its per-crate branch published fdev. The
    # INACCURACY was the only thing holding that gap shut. Making the check
    # correct without widening it to both crates would have turned an unlikely
    # path into the reliable one.
    #
    # General form, because this is not the only place it can bite: before
    # making a check more correct, ask what currently depends on it being
    # wrong.
    if crate_version_on_crates_io freenet "$VERSION" \
       && crate_version_on_crates_io fdev "$FDEV_VERSION"; then
        COMPLETED_STEPS["CRATES_PUBLISHED"]=1
    fi
}

# Detect if GitHub release exists
detect_release_state() {
    if gh release view "v$VERSION" &>/dev/null; then
        COMPLETED_STEPS["RELEASE_CREATED"]=1
    fi
}

# Run all auto-detection
auto_detect_state() {
    echo "Detecting release state for v$VERSION:"

    # First load any saved state file
    load_state_file

    # Then run auto-detection (may update/override)
    echo -n "  Checking PR status... "
    detect_pr_state
    if is_step_completed "PR_MERGED"; then
        echo "merged ✓"
    elif is_step_completed "PR_CREATED"; then
        echo "exists (not merged)"
    else
        echo "not found"
    fi

    echo -n "  Checking tag v$VERSION... "
    detect_tag_state
    if is_step_completed "TAG_CREATED"; then
        echo "exists ✓"
    else
        echo "not found"
    fi

    echo -n "  Checking crates.io... "
    detect_crates_state
    if is_step_completed "CRATES_PUBLISHED"; then
        echo "published ✓"
    else
        echo "not published"
    fi

    echo -n "  Checking GitHub release... "
    detect_release_state
    if is_step_completed "RELEASE_CREATED"; then
        echo "exists ✓"
    else
        echo "not found"
    fi

    # Print summary of what will be skipped
    local skipped=()
    local pending=()
    for step in "${RELEASE_STEPS[@]}"; do
        if is_step_completed "$step"; then
            skipped+=("$step")
        else
            pending+=("$step")
        fi
    done

    echo
    if [[ ${#skipped[@]} -gt 0 ]]; then
        echo "Steps to skip: ${skipped[*]}"
    fi
    if [[ ${#pending[@]} -gt 0 ]]; then
        echo "Steps to execute: ${pending[*]}"
    fi
}

# ============================================================================
# Helper Functions
# ============================================================================

run_cmd() {
    local desc="$1"
    shift
    
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] $desc"
        return 0
    fi
    
    echo -n "  $desc... "
    if output=$("$@" 2>&1); then
        echo "✓"
    else
        echo "✗"
        echo "Error: $desc failed"
        echo "Command: $*"
        echo "Output:"
        echo "$output"
        echo
        echo "💡 Tip: You can fix the issue and re-run the script - it will skip completed steps"
        exit 1
    fi
}

check_prerequisites() {
    echo "Checking prerequisites:"
    
    # Change to project directory first
    cd "$PROJECT_ROOT"
    
    # Check if we're on main branch
    current_branch=$(git branch --show-current)
    if [[ "$current_branch" != "main" ]]; then
        echo "  ✗ Must be on main branch (currently on: $current_branch)"
        exit 1
    fi
    echo "  ✓ On main branch"
    
    # Check for uncommitted changes
    if ! git diff-index --quiet HEAD --; then
        echo "  ✗ Uncommitted changes detected"
        exit 1
    fi
    echo "  ✓ Working directory clean"
    
    # Check if we're up to date with origin
    if [[ "$DRY_RUN" == "false" ]]; then
        git fetch origin main
        if [[ $(git rev-parse HEAD) != $(git rev-parse origin/main) ]]; then
            echo "  ✗ Local main is not up to date with origin/main"
            exit 1
        fi
    fi
    echo "  ✓ Up to date with origin"

    # Check if main branch CI is green (required before skipping tests on release PR)
    # Note: GitHub Actions uses Check Runs API, not the legacy Status API
    if [[ "$DRY_RUN" == "false" ]]; then
        local ci_max_wait=600  # 10 minutes max wait for pending CI
        local ci_elapsed=0
        local ci_interval=30

        while true; do
            echo -n "  Checking main branch CI status... "
            local check_runs_json
            check_runs_json=$(gh api repos/freenet/freenet-core/commits/main/check-runs 2>/dev/null || echo "{}")

            # Exclude scheduled workflows and their cascade notifiers from the
            # release pre-flight check. These run on a cron (not on commit
            # merge) but attach their check runs to the HEAD commit of main,
            # which makes them look like PR CI failures to the naive query.
            # Neither blocks correctness of the commit under release:
            #  - "Large Scale Simulation" / "Simulation Tests (Nightly)" are
            #    scheduled soak tests with a long history of flakiness that
            #    is tracked separately.
            #  - "Notify Matrix on Failure" is a cascade notifier that fires
            #    whenever any other workflow fails — it's not itself a test.
            # If any of these are genuinely broken, fix them on their own
            # schedule; they should never gate a release of a PR-CI-green
            # merge commit.
            local total_checks in_progress_count failed_count
            total_checks=$(echo "$check_runs_json" | jq '.total_count // 0')
            # `Dependabot` runs on a cron and attaches to the HEAD commit
            # of main, which made a still-running Dependabot job block
            # the release pre-flight wait indefinitely even though
            # `failed_count` already excluded it below. Align the two
            # filters so a Dependabot run in either state never gates a
            # release (release blocked by unrelated Dependabot cron,
            # 2026-04-14 v0.2.45 release).
            in_progress_count=$(echo "$check_runs_json" | jq '[.check_runs[] | select(.status != "completed" and .name != "Dependabot" and (.name | startswith("Build for") | not) and .name != "claude" and .name != "Large Scale Simulation" and .name != "Simulation Tests (Nightly)" and .name != "Notify Matrix on Failure" and .name != "mirror / mirror")] | length')
            failed_count=$(echo "$check_runs_json" | jq '[.check_runs[] | select(.status == "completed" and .conclusion != "success" and .conclusion != "skipped" and .name != "Dependabot" and (.name | startswith("Build for") | not) and .name != "claude" and .name != "Large Scale Simulation" and .name != "Simulation Tests (Nightly)" and .name != "Notify Matrix on Failure" and .name != "mirror / mirror")] | length')

            if [[ "$total_checks" == "0" ]]; then
                echo "⚠️  (no checks found)"
                echo "  ⚠️  Could not find any CI checks for main branch"
                echo "     Proceeding anyway - verify CI manually if needed"
                break
            elif [[ "$in_progress_count" -gt 0 ]]; then
                if [[ $ci_elapsed -ge $ci_max_wait ]]; then
                    echo "⚠️  (still pending after ${ci_max_wait}s)"
                    echo "  ⚠️  Main branch CI is still running after ${ci_max_wait}s"
                    echo "     Release PRs skip slow tests, so main must be green first"
                    exit 1
                fi
                echo "⏳ ($in_progress_count checks running, waiting... ${ci_elapsed}s/${ci_max_wait}s)"
                sleep $ci_interval
                ci_elapsed=$((ci_elapsed + ci_interval))
            elif [[ "$failed_count" -gt 0 ]]; then
                echo "✗ ($failed_count checks failed)"
                local failed_names
                failed_names=$(echo "$check_runs_json" | jq -r '[.check_runs[] | select(.status == "completed" and .conclusion != "success" and .conclusion != "skipped")] | .[].name' | head -5)
                echo "  ✗ Main branch CI is failing - cannot release"
                echo "     Failed checks: $failed_names"
                exit 1
            else
                echo "✓ (green - $total_checks checks passed)"
                break
            fi
        done
    fi

    # Check required tools
    for tool in cargo gh; do
        if ! command -v "$tool" &> /dev/null; then
            echo "  ✗ Required tool '$tool' not found"
            exit 1
        fi
    done
    echo "  ✓ Required tools available"

    # Note: Pre-release testing is handled by GitHub CI
    # The release PR will run all tests before merging
    if [[ "$SKIP_TESTS" == "false" ]]; then
        echo ""
        echo "ℹ️  Pre-release tests will be run by GitHub CI during the release PR"
    else
        echo ""
        echo "⚠️  Skipping pre-release tests (--skip-tests specified)"
    fi
}

update_versions() {
    echo "Updating versions:"

    # If PR is merged, versions are already in main
    if is_step_completed "PR_MERGED"; then
        echo "  ✓ Versions already in main (PR merged)"
        return 0
    fi

    # Check if version is already updated in Cargo.toml
    if grep -q "^version = \"$VERSION\"" "$PROJECT_ROOT/crates/core/Cargo.toml" 2>/dev/null; then
        echo "  ✓ Versions already updated (skipping)"
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would update freenet to $VERSION"
        echo "  [DRY RUN] Would update fdev to $FDEV_VERSION"
        return 0
    fi

    # Portable sed function (works on both GNU and BSD sed)
    sed_inplace() {
        local pattern="$1"
        local file="$2"
        sed "$pattern" "$file" > "$file.tmp" && mv "$file.tmp" "$file"
    }

    # Update freenet version
    echo -n "  Updating freenet to $VERSION... "
    sed_inplace "s/^version = \".*\"/version = \"$VERSION\"/" "$PROJECT_ROOT/crates/core/Cargo.toml"
    echo "✓"

    # Update min-compatible-version in Cargo.toml metadata so cross-compile CI
    # builds (which don't have the env var) get the correct min-compatible version.
    echo -n "  Setting min-compatible-version to $MIN_COMPATIBLE... "
    sed_inplace "s/^min-compatible-version = \".*\"/min-compatible-version = \"$MIN_COMPATIBLE\"/" "$PROJECT_ROOT/crates/core/Cargo.toml"
    echo "✓"

    # Update fdev version and its freenet dependency
    echo -n "  Updating fdev to $FDEV_VERSION... "
    sed_inplace "s/^version = \".*\"/version = \"$FDEV_VERSION\"/" "$PROJECT_ROOT/crates/fdev/Cargo.toml"
    sed_inplace "s/\(freenet = { path = \"..\/core\", version = \)\"[^\"]*\"/\1\"$VERSION\"/" "$PROJECT_ROOT/crates/fdev/Cargo.toml"
    # fdev's crate version is independent of the freenet release tag, so the
    # binstall pkg-url embeds `vX.Y.Z` (the freenet version) literally rather
    # than `v{ version }`. Re-point it to the current release each bump so
    # that `cargo binstall fdev` resolves to the actual GitHub release assets
    # (issue #3995). The `/g` flag is load-bearing — there are two pkg-url
    # lines (default + Windows override) that both need the rewrite. Mirror
    # in `.github/workflows/release.yml`; tests in
    # `crates/fdev/tests/binstall_metadata.rs::release_sed_rewrite` keep both
    # copies of the regex in lockstep.
    sed_inplace "s|releases/download/v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/fdev-|releases/download/v${VERSION}/fdev-|g" "$PROJECT_ROOT/crates/fdev/Cargo.toml"
    echo "✓"

    # Update Cargo.lock to match new versions
    echo -n "  Updating Cargo.lock... "
    if cargo update --workspace --quiet 2>/dev/null; then
        echo "✓"
    else
        echo "✗"
        echo "Failed to update Cargo.lock. This may cause CI failures."
        exit 1
    fi

    # Quick sanity check that the version-bumped workspace still type-checks.
    # This is intentionally `cargo check`, not `cargo build --release` or
    # `cargo test --release`: the PR opened below runs the full build and test
    # suite on GitHub CI, and that is the gate auto-merge waits on. The local
    # step only needs to catch trivially broken states (bad Cargo.toml edit,
    # version mismatch, `cargo update` pulling in an incompatible transitive)
    # before we round-trip through CI — codegen and the test suite are pure
    # duplication here and can cost 5–15+ minutes of local wall time per
    # release depending on cache state.
    echo -n "  Running cargo check on workspace... "
    if cargo check --workspace --quiet 2>/dev/null; then
        echo "✓"
    else
        echo "✗"
        echo ""
        echo "  ⚠️  ERROR: Workspace failed to type-check with new versions!"
        echo "     Running cargo check to see errors:"
        echo ""
        cargo check --workspace
        echo ""
        echo "  Please fix errors before releasing."
        exit 1
    fi
}

create_release_pr() {
    echo "Creating release PR:"

    local branch_name="release/v$VERSION"
    local pr_number=""
    local skip_pr_creation=false

    # Skip if PR is already merged
    if is_step_completed "PR_MERGED"; then
        echo "  ✓ [PR_MERGED] Release PR already merged (skipping)"
        # Ensure we're on main with latest
        if [[ $(git branch --show-current) != "main" ]]; then
            run_cmd "Switching to main branch" git checkout main
        fi
        run_cmd "Pulling latest changes" git pull origin main
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would create branch $branch_name"
        echo "  [DRY RUN] Would create auto-merge PR"
        echo "  [DRY RUN] Would wait for GitHub CI"
        return 0
    fi

    # Check if a release PR for this version already exists or was merged
    echo -n "  Checking for existing release PR... "
    local existing_pr
    existing_pr=$(gh pr list --head "$branch_name" --state all --limit 1 --json number,state,title --jq '.[] | "\(.number)|\(.state)|\(.title)"' 2>/dev/null || echo "")

    if [[ -n "$existing_pr" ]]; then
        pr_number=$(echo "$existing_pr" | cut -d'|' -f1)
        local pr_state
        pr_state=$(echo "$existing_pr" | cut -d'|' -f2) || true
        echo "found #$pr_number ($pr_state)"

        if [[ "$pr_state" == "MERGED" ]]; then
            mark_completed "PR_CREATED"
            mark_completed "PR_MERGED"
            echo "  ✓ Release PR #$pr_number already merged, skipping PR creation"

            # Make sure we're on main with the merged changes
            if [[ $(git branch --show-current) != "main" ]]; then
                run_cmd "Switching to main branch" git checkout main
            fi
            run_cmd "Pulling latest changes" git pull origin main

            # Clean up release branch if it exists
            if git show-ref --verify --quiet "refs/heads/$branch_name"; then
                git branch -d "$branch_name" 2>/dev/null || true
            fi

            return 0
        elif [[ "$pr_state" == "OPEN" ]]; then
            mark_completed "PR_CREATED"
            echo "  ℹ️  Release PR #$pr_number already exists and is open"
            echo "     Monitoring existing PR instead of creating a new one..."
            skip_pr_creation=true
        else
            echo "  ⚠️  Release PR #$pr_number exists but is $pr_state — aborting"
            exit 1
        fi
    else
        echo "not found"
    fi

    if [[ "$skip_pr_creation" != "true" ]]; then
        # Check if there are any changes to commit
        git add -A
        if git diff --cached --quiet; then
            echo "  ✓ No version changes needed (already at $VERSION)"

            # Check if we're already on main
            if [[ $(git branch --show-current) != "main" ]]; then
                run_cmd "Switching to main branch" git checkout main
            fi

            return 0
        fi

        # Create release branch if it doesn't exist
        if git show-ref --verify --quiet "refs/heads/$branch_name"; then
            echo "  ℹ️  Branch $branch_name already exists, using it"
            git checkout "$branch_name"
        else
            run_cmd "Creating release branch" git checkout -b "$branch_name"
        fi

        # Only commit if there are uncommitted changes (handles re-run after partial failure)
        if git diff --quiet HEAD; then
            echo "  ℹ️  No changes to commit (version bump already committed)"
        else
            # Stage only the files modified by the version bump — never git add -A
            git add crates/core/Cargo.toml crates/fdev/Cargo.toml Cargo.lock
            run_cmd "Committing version bump" git commit -m "build: bump versions to $VERSION

- freenet: → $VERSION
- fdev: → $FDEV_VERSION

🤖 Automated release commit"
        fi

        run_cmd "Pushing branch" git push origin "$branch_name"

        echo -n "  Creating auto-merge PR... "
        pr_number=$(gh pr create \
            --title "build: release $VERSION" \
            --body "**Automated release PR**

- freenet: → **$VERSION**
- fdev: → **$FDEV_VERSION**

This PR will auto-merge once GitHub CI passes.
Generated by: \`scripts/release.sh\`" \
            --base main \
            --head "$branch_name" \
            --assignee @me 2>/dev/null | grep -o '[0-9]\+$')
        echo "✓ (#$pr_number)"
        mark_completed "PR_CREATED"
    fi

    if [[ -z "$pr_number" ]]; then
        echo "  ✗ Failed to determine PR number — aborting"
        exit 1
    fi

    # Idempotent — gh treats re-enabling auto-merge as a no-op.
    echo -n "  Enabling auto-merge... "
    gh pr merge "$pr_number" --squash --auto >/dev/null 2>&1 || true
    echo "✓"

    echo "  ⏳ Waiting for GitHub CI and auto-merge..."
    echo "     You can monitor at: https://github.com/freenet/freenet-core/pull/$pr_number"
    echo "     💡 Note: You can manually merge the PR if needed - the script will detect it and continue"
    echo

    # Monitor CI with enhanced failure reporting
    local wait_time=30
    local total_wait=0
    local max_total=1800  # 30 minutes max (increased from 20)

    while true; do
        if [[ $total_wait -gt $max_total ]]; then
            echo "  ⚠️  Timeout waiting for PR to merge after 30 minutes"
            echo "    The PR may still merge. Check: https://github.com/freenet/freenet-core/pull/$pr_number"
            echo "    If the PR merged, you can continue the release manually with:"
            echo "      git checkout main && git pull origin main"
            echo "      git tag -a 'v$VERSION' -m 'Release v$VERSION' && git push origin 'v$VERSION'"
            echo "      gh release create 'v$VERSION' --title 'v$VERSION' --notes 'Release $VERSION' --draft"
            echo "    Then let cross-compile.yml (fired by the tag) attach the binaries, run"
            echo "    the blocking pre-flight canary, publish to crates.io and un-draft."
            echo "    Do NOT 'cargo publish' by hand first: the publish is downstream of the"
            echo "    canary on purpose, and doing it early is what cost v0.2.124 its version"
            echo "    number. See scripts/RELEASE_RECOVERY.md."
            # Don't exit with error - the PR might have merged
            return 0
        fi
        
        pr_state=$(gh pr view "$pr_number" --json state --jq '.state')
        
        case "$pr_state" in
            "MERGED")
                mark_completed "PR_MERGED"
                echo "  ✓ PR merged successfully!"
                break
                ;;
            "CLOSED")
                echo "  ✗ PR was closed without merging"
                exit 1
                ;;
            "OPEN")
                # Use structured statusCheckRollup so we don't false-positive
                # on transient strings or job names containing "fail"/"error".
                # Each check has status (QUEUED|IN_PROGRESS|COMPLETED) and,
                # when COMPLETED, a conclusion (SUCCESS|FAILURE|NEUTRAL|
                # CANCELLED|SKIPPED|TIMED_OUT|ACTION_REQUIRED). We only
                # treat FAILURE / TIMED_OUT / CANCELLED / ACTION_REQUIRED
                # as actual CI failures.
                local rollup
                rollup=$(gh pr view "$pr_number" --json statusCheckRollup --jq '.statusCheckRollup' 2>/dev/null || echo "[]")
                local failed_checks_json
                failed_checks_json=$(echo "$rollup" | jq -c '[.[] | select((.conclusion // .state // "") | test("^(FAILURE|TIMED_OUT|CANCELLED|ACTION_REQUIRED)$"))]')
                local failed_count
                failed_count=$(echo "$failed_checks_json" | jq 'length')

                if [[ "$failed_count" -gt 0 ]]; then
                    echo "  ✗ CI checks failed!"
                    echo "    Failed checks:"
                    echo "$failed_checks_json" | jq -r '.[] | "      \(.name // .context // "?")  \(.conclusion // .state // "?")"'
                    echo

                    # Get workflow run details for better error reporting
                    echo "  📋 Fetching failure logs..."
                    run_id=$(gh run list --branch "release/v$VERSION" --limit 1 --json databaseId --jq '.[0].databaseId' 2>/dev/null || echo "")
                    if [[ -n "$run_id" ]]; then
                        echo "    Workflow run: https://github.com/freenet/freenet-core/actions/runs/$run_id"

                        # Try to get job logs for failed jobs
                        failed_jobs=$(gh run view "$run_id" --json jobs --jq '.jobs[] | select(.conclusion == "failure") | .name' 2>/dev/null || echo "")
                        if [[ -n "$failed_jobs" ]]; then
                            echo "    Failed jobs:"
                            while IFS= read -r _job; do
                                echo "      - $_job"
                            done <<< "$failed_jobs"
                            echo
                            echo "    To view logs: gh run view $run_id --log-failed"
                        fi
                    fi

                    echo "    💡 Fix the issues and the PR will auto-merge once CI passes"
                    exit 1
                else
                    local pending_count passed_count
                    pending_count=$(echo "$rollup" | jq '[.[] | select(((.status // "") | test("^(QUEUED|IN_PROGRESS|PENDING)$")) or ((.state // "") == "PENDING"))] | length')
                    passed_count=$(echo "$rollup" | jq '[.[] | select((.conclusion // .state // "") | test("^(SUCCESS|NEUTRAL|SKIPPED)$"))] | length')
                    if [[ "$pending_count" -gt 0 ]]; then
                        echo "  ⏳ Waiting... ($passed_count checks passed, $pending_count in progress, ${total_wait}s elapsed)"
                    else
                        echo "  ⏳ All checks passed, waiting for auto-merge... (${total_wait}s elapsed)"
                    fi
                fi
                ;;
        esac

        sleep $wait_time
        total_wait=$((total_wait + wait_time))
    done
    
    run_cmd "Updating local main" git checkout main
    run_cmd "Pulling merged changes" git pull origin main
    run_cmd "Cleaning up branch" git branch -d "$branch_name"
}

generate_release_notes() {
    local version="$1"

    # Find the previous release to determine what PRs to include
    local prev_version
    prev_version=$(gh release list --limit 50 --json tagName,createdAt --jq 'sort_by(.createdAt) | reverse | .[].tagName' 2>/dev/null | grep -v "^v${version}$" | head -1 | sed 's/^v//') || true

    if [[ -z "$prev_version" ]]; then
        # Fallback to basic release notes if we can't find previous release
        echo "Release $version

## Changes
- Version bump to $version
- fdev updated to $FDEV_VERSION

See commit history for detailed changes.

[AI-assisted debugging and comment]"
        return
    fi

    local prev_date
    prev_date=$(gh release view "v${prev_version}" --json createdAt --jq '.createdAt' 2>/dev/null) || true

    # Fetch merged PRs since the previous release
    local prs
    prs=$(gh pr list --search "is:pr is:merged merged:>${prev_date}" --limit 100 --json number,title --jq '.[] | "#\(.number)|\(.title)"' 2>/dev/null || echo "")

    if [[ -z "$prs" ]]; then
        echo "Release $version

## Changes
- Version bump to $version
- fdev updated to $FDEV_VERSION

[AI-assisted debugging and comment]"
        return
    fi

    # Categorize PRs
    local fixes=""
    local features=""
    local maintenance=""

    while IFS= read -r pr; do
        local number
        number=$(echo "$pr" | cut -d'|' -f1) || true
        local title
        title=$(echo "$pr" | cut -d'|' -f2-) || true

        # Skip the release PR itself
        if [[ "$title" =~ ^🚀\ Release || "$title" =~ ^Release\ v ]]; then
            continue
        fi

        # Categorize based on conventional commit prefixes
        if [[ "$title" =~ ^fix: || "$title" =~ ^fix\( || "$title" =~ Fix\ |fix\] ]]; then
            fixes="${fixes}- **${number}**: ${title#fix: }\n"
        elif [[ "$title" =~ ^feat: || "$title" =~ ^feat\( || "$title" =~ Feature\ |feat\] ]]; then
            features="${features}- **${number}**: ${title#feat: }\n"
        elif [[ "$title" =~ ^chore\(deps\): || "$title" =~ ^chore: || "$title" =~ Bump\  ]]; then
            maintenance="${maintenance}- **${number}**: ${title#chore: }\n"
        elif [[ "$title" =~ ^ci: || "$title" =~ ^test: || "$title" =~ ^docs: ]]; then
            maintenance="${maintenance}- **${number}**: ${title}\n"
        elif [[ "$title" =~ Remove\  || "$title" =~ Refactor\  ]]; then
            maintenance="${maintenance}- **${number}**: ${title}\n"
        else
            # Default to fixes if unclear
            fixes="${fixes}- **${number}**: ${title}\n"
        fi
    done <<< "$prs"

    # Build the release notes
    local notes="# Release $version\n\n"

    if [[ -n "$fixes" ]]; then
        notes="${notes}## 🐛 Bug Fixes\n\n${fixes}\n"
    fi

    if [[ -n "$features" ]]; then
        notes="${notes}## ✨ Features\n\n${features}\n"
    fi

    if [[ -n "$maintenance" ]]; then
        notes="${notes}## 🧹 Maintenance\n\n${maintenance}\n"
    fi

    notes="${notes}---\n\n**Full Changelog**: https://github.com/freenet/freenet-core/compare/v${prev_version}...v${version}\n\n[AI-assisted debugging and comment]"

    echo -e "$notes"
}

# crates.io publish -- a VERIFY-WITH-BACKSTOP step, and no longer where the
# release normally publishes from.
#
# This used to run before `create_github_release`, i.e. before the tag existed
# and long before the blocking auto-update pre-flight canary (Gate A, #5222)
# had a binary to inspect. That ordering is what made a Gate A block cost a
# version number instead of a re-run: when the gate blocked v0.2.124 its crates
# were already permanent on crates.io, so the release could only stay a draft
# and 0.2.125 was cut in its place. The publish now lives in
# `cross-compile.yml`'s `attach-to-release` job, between Gate A and the
# un-draft, and the tag push this script performs is what triggers it.
#
# So by the time `wait_for_binaries` returns, the workflow has normally already
# published both crates and the checks below simply confirm it.
#
# IT RUNS ONLY ON THE SUCCESS PATH. It is NOT a fallback for a broken workflow
# and must not be made into one.
#
# Two earlier versions of this comment claimed otherwise -- that it "stays a
# real publish" for a `CARGO_REGISTRY_TOKEN` that never reached CI, and that
# the call site was `if ! wait_for_binaries; then publish_crates; exit 1; fi`.
# Both are false, and the second is the shape a maintainer would restore from
# reading them, so they are corrected rather than merely appended to.
#
# WHY IT MUST NOT RUN ON FAILURE. `wait_for_binaries` fails on six modes, only
# one of which ("assets missing after a successful attach") leaves Gate A known
# to have passed. The canonical enumeration, with each mode's Gate A
# implication, lives at the refusal branch in the main flow -- deliberately in
# ONE place, because the previous two copies of it drifted apart and both
# undercounted. This function checks the resume flag, DRY_RUN and crates.io
# presence -- never the gate's verdict -- and on the rejected path the crate is
# genuinely absent, so the already-published check says "no" and it really does
# upload.
#
# The token justification was also unachievable: cross-compile.yml checks the
# credential BEFORE running the canary, so a missing token fails the job before
# the gate; and at the call site a missing token and a canary rejection arrive
# as the same non-success conclusion, indistinguishable.
#
# PINNED BY the refusal case in release_driver_test.sh -- which asserts this
# function does NOT run when wait_for_binaries fails, the exact opposite of what
# an earlier reachability case asserted -- NOT by the ordering assertion in
# release_canary_wiring_test.sh, which compares line numbers and stayed green
# throughout the period this was dead.
#
# MUST be called AFTER wait_for_binaries, and ONLY on its success path. Calling
# it earlier reinstates the ordering described above; calling it on the FAILURE
# path publishes a version Gate A never passed, because this function checks
# only the resume flag, DRY_RUN and crates.io presence -- never the gate's
# verdict. The main flow refuses instead; see the comment at that call site.
publish_crates() {
    echo "Confirming crates.io publish (normally already done by cross-compile.yml):"

    # Skip if already completed
    if is_step_completed "CRATES_PUBLISHED"; then
        echo "  ✓ [CRATES_PUBLISHED] Crates already published (skipping)"
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would publish freenet $VERSION"
        echo "  [DRY RUN] Would publish fdev $FDEV_VERSION"
        return 0
    fi

    local freenet_published=false
    local fdev_published=false

    # Check if freenet is already published
    echo -n "  Checking if freenet $VERSION is already published... "
    if crate_version_on_crates_io freenet "$VERSION"; then
        echo "yes"
        echo "  ✓ freenet $VERSION already published to crates.io"
        freenet_published=true
    else
        echo "no"
        run_cmd "Publishing freenet $VERSION" cargo publish -p freenet
        freenet_published=true

        # Wait a bit for crates.io to propagate
        echo -n "  Waiting for crates.io propagation... "
        sleep 30
        echo "✓"
    fi

    # Check if fdev is already published
    echo -n "  Checking if fdev $FDEV_VERSION is already published... "
    if crate_version_on_crates_io fdev "$FDEV_VERSION"; then
        echo "yes"
        echo "  ✓ fdev $FDEV_VERSION already published to crates.io"
        fdev_published=true
    else
        echo "no"
        run_cmd "Publishing fdev $FDEV_VERSION" cargo publish -p fdev
        fdev_published=true
    fi

    # Mark complete if both are published
    if [[ "$freenet_published" == "true" && "$fdev_published" == "true" ]]; then
        mark_completed "CRATES_PUBLISHED"
    fi
}

create_github_release() {
    echo "Creating GitHub release:"

    # Skip if already completed
    if is_step_completed "RELEASE_CREATED"; then
        echo "  ✓ [RELEASE_CREATED] GitHub release already exists (skipping)"
        release_url=$(gh release view "v$VERSION" --json url --jq '.url' 2>/dev/null || echo "")
        if [[ -n "$release_url" ]]; then
            echo "  Release URL: $release_url"
        fi
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would create tag v$VERSION"
        echo "  [DRY RUN] Would create GitHub release"
        return 0
    fi

    # Check if release already exists
    echo -n "  Checking if release v$VERSION already exists... "
    if gh release view "v$VERSION" &>/dev/null; then
        echo "yes"
        mark_completed "TAG_CREATED"
        mark_completed "RELEASE_CREATED"
        echo "  ✓ GitHub release v$VERSION already exists"
        release_url=$(gh release view "v$VERSION" --json url --jq '.url')
        echo "  Release URL: $release_url"
        return 0
    else
        echo "no"
    fi

    # Check if tag already exists
    if [[ -n "$(git tag -l "v$VERSION")" ]]; then
        echo "  ℹ️  Tag v$VERSION already exists locally"
        mark_completed "TAG_CREATED"
    else
        run_cmd "Creating tag v$VERSION" git tag -a "v$VERSION" -m "Release v$VERSION"
    fi

    # Check if tag exists on remote
    if [[ -n "$(git ls-remote --tags origin "refs/tags/v$VERSION" 2>/dev/null)" ]]; then
        echo "  ℹ️  Tag v$VERSION already exists on remote"
        mark_completed "TAG_CREATED"
    else
        run_cmd "Pushing tag" git push origin "v$VERSION"
        mark_completed "TAG_CREATED"
    fi

    echo -n "  Generating release notes... "
    local release_notes
    release_notes=$(generate_release_notes "$VERSION") || true
    echo "✓"

    echo -n "  Creating GitHub release... "
    release_url=$(gh release create "v$VERSION" --title "v$VERSION" --notes "$release_notes" --draft)
    echo "✓ (draft)"
    mark_completed "RELEASE_CREATED"

    echo "  Draft release created: $release_url"
    echo "  Release will be published after cross-compile binaries are attached."
}

# Note: Cross-compilation is now triggered automatically when a version tag is pushed
# See .github/workflows/cross-compile.yml - it has `tags: ['v*']` trigger
# trigger_cross_compile function is no longer needed

trigger_gateway_updates() {
    # Trigger immediate update on all known gateways by running gateway-auto-update.sh --force
    # This avoids the 10-minute polling delay, ensuring gateways are updated before users
    # install the new version (version mismatch = failed connections).
    #
    # Future: this stage will be replaced by .github/workflows/gateway-update.yml,
    # which signs an HTTP request to each gateway's `freenet-release-agent`
    # instead of SSH'ing. Tracked in #4073 (Phase 1 currently shipping). The
    # SSH path remains the production trigger until the new workflow has been
    # validated against 1-2 real releases on nova.

    # The gateway-update.yml workflow auto-rolls out updates on
    # release.published. Setting FREENET_RELEASE_SKIP_GATEWAY_SSH=1
    # opts the local script out so we don't double-update.
    if [[ "${FREENET_RELEASE_SKIP_GATEWAY_SSH:-0}" == "1" ]]; then
        echo "  ⏭  Skipping SSH-based gateway update (FREENET_RELEASE_SKIP_GATEWAY_SSH=1 — workflow handles it)"
        mark_completed "GATEWAYS_UPDATED"
        return 0
    fi

    if is_step_completed "GATEWAYS_UPDATED"; then
        echo "  ✓ [GATEWAYS_UPDATED] Gateways already updated (skipping)"
        return 0
    fi

    echo "Triggering immediate gateway updates:"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would SSH into gateways and run gateway-auto-update.sh --force"
        return 0
    fi

    # Known gateways: host, SSH user, SSH options
    # The AWS gateway was retired 2026-09; its entry is removed so a fallback
    # release does not SSH to a host that no longer exists and record a failed
    # update on every run. nova's second gateway (freenet-gateway-2) needs no
    # entry of its own: it carries WantedBy=freenet-gateway.service, so the same
    # stop/start cycle brings it up, and gateway-auto-update.sh now verifies
    # companion units from the .wants symlinks rather than reporting success
    # while one is down.
    local -a GATEWAYS=(
        "nova.locut.us:ian:"
    )

    local all_ok=true

    for gw_info in "${GATEWAYS[@]}"; do
        IFS=':' read -r host user ssh_opts <<< "$gw_info"

        echo -n "  Updating $host... "

        # SSH in, run gateway-auto-update.sh --force as root
        local output
        if output=$(ssh -o ConnectTimeout=10 -o BatchMode=yes ${ssh_opts:+$ssh_opts} "${user}@${host}" \
            "sudo /usr/local/bin/gateway-auto-update.sh --force" 2>&1); then
            # Extract version from output
            local new_ver
            new_ver=$(echo "$output" | grep -oE 'Successfully updated to v[0-9.]+' | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' || echo "")
            if [[ -n "$new_ver" ]]; then
                echo "✓ (v$new_ver)"
            else
                # Maybe already at target version
                local already
                already=$(echo "$output" | grep -c "Already running latest" || true)
                if [[ "$already" -gt 0 ]]; then
                    echo "✓ (already up to date)"
                else
                    echo "✓"
                fi
            fi
        else
            echo "⚠️  failed (non-fatal)"
            echo "     $output" | head -3
            echo "     Manual: ssh ${user}@${host} 'sudo gateway-auto-update.sh --force'"
            all_ok=false
        fi
    done

    if [[ "$all_ok" == "true" ]]; then
        mark_completed "GATEWAYS_UPDATED"
    fi
}

# The display name of the cross-compile job that uploads the assets, runs the
# BLOCKING pre-flight canary, and un-drafts the release. Must match `name:` on
# the `attach-to-release` job in .github/workflows/cross-compile.yml.
ATTACH_JOB_NAME='Attach binaries to GitHub release'

# That job's own "status:conclusion", empty when it cannot be determined.
#
# Deliberately NOT the run's aggregate. The same run also contains the
# post-publish self-update canary (Gate B, #5222), which is by design
# NON-blocking: it starts only after `attach-to-release` has already published
# the release, and its job exists to report, not to gate. Reading the run's
# status therefore makes this script (a) keep waiting after the release is
# published and (b) treat a Gate B failure as a failed release -- and since
# `wait_for_binaries` is called bare under `set -e`, that aborts the driver
# before it updates the gateways or announces to Matrix and River. A release
# that published perfectly well would silently never be announced.
#
# Empty output means "we do not know" -- the job has not started, was renamed,
# or `gh` failed -- and every caller must treat it as such rather than as a
# pass. A rename shows up as a wait that times out loudly; it cannot fail open.
#
# CALLERS MUST WRITE `$(attach_job_state "$id" || echo "")`. This function ends
# in a bare `gh`, so a `gh` failure IS its exit status, and `var=$(cmd)` is a
# simple command whose status is `cmd`'s -- under `set -e` that aborts the whole
# driver rather than yielding the "we do not know" this comment promises. The
# same guard is needed on every bare `$(gh ...)` in this file, including
# `$(gh ... | head -1)`, which `set -o pipefail` makes fail too. Pinned by
# scripts/release_wait_for_binaries_test.sh.
attach_job_state() {
    local run_id="$1"
    gh run view "$run_id" --repo freenet/freenet-core --json jobs \
        --jq "[.jobs[] | select(.name == \"$ATTACH_JOB_NAME\")] | .[0] | select(. != null) | \"\(.status):\(.conclusion)\"" 2>/dev/null
}

publish_draft_release() {
    # Publish the draft release (idempotent -- no-op if already published).
    #
    # The cross-compile workflow publishes via `gh release edit --draft=false`
    # as its final step, AFTER the blocking auto-update pre-flight canary
    # (#5222). This belt-and-suspenders copy must therefore never fire while
    # that workflow is still deciding: between asset upload and the canary's
    # verdict there is now a multi-minute window in which every asset is
    # present but the release has deliberately NOT been published. The caller
    # below reaches this function on exactly that condition ("all required
    # binaries already available"), so without this guard the local driver
    # would race in and publish a release whose updater the gate was in the
    # middle of rejecting -- silently turning a blocking gate into no gate.
    local is_draft
    is_draft=$(gh release view "v$VERSION" --repo freenet/freenet-core --json isDraft --jq '.isDraft' 2>/dev/null || echo "unknown")
    if [[ "$is_draft" == "false" ]]; then
        return 0   # already published by the workflow -- nothing left to gate
    fi
    if [[ "$is_draft" != "true" ]]; then
        # `gh` failed, so we do not know whether this is still a draft. Every
        # other unknown in this function refuses, and this one must too: it
        # coerced to "false" and returned 0, which never published an ungated
        # release, but DID report success to the caller -- so the driver went on
        # to update the gateways and announce a release that may still have been
        # an unpublished draft.
        echo "  ⏸  Cannot tell whether v$VERSION is still a draft ('gh' failed)." >&2
        echo "     Refusing to report success: on an unknown the driver would" >&2
        echo "     otherwise update the gateways and announce to Matrix and River" >&2
        echo "     a release that may still be an unpublished draft." >&2
        return 1
    fi

    # It IS still a draft, so the gate's verdict decides. Anything other than a
    # successfully-concluded ATTACH job means "we do not know that the canary
    # passed", and publishing on an unknown gate state is the fail-open this
    # guard exists to prevent. A missing run, a missing job, or a `gh` failure
    # all yield "" -- all of them are "we do not know", and all must refuse.
    local run_id job_state
    run_id=$(gh run list --repo freenet/freenet-core \
        --workflow=cross-compile.yml --branch "v$VERSION" \
        --json databaseId --jq '.[0].databaseId // empty' 2>/dev/null || echo "")
    job_state=""
    if [[ -n "$run_id" ]]; then
        # `|| echo ""` per attach_job_state's contract: without it a `gh` blip
        # aborts the driver here instead of printing the refusal below.
        job_state=$(attach_job_state "$run_id" || echo "")
    fi
    if [[ "$job_state" != "completed:success" ]]; then
        echo "  ⏸  NOT publishing v$VERSION: '$ATTACH_JOB_NAME' is '${job_state:-unknown}'." >&2
        echo "     Publication is gated on the auto-update pre-flight canary (#5222)," >&2
        echo "     which runs between asset upload and un-draft. Let the workflow" >&2
        echo "     publish, or fix the gate. Do NOT un-draft by hand -- a release" >&2
        echo "     whose updater is broken cannot deliver its own fix." >&2
        # MUST be non-zero. Returning 0 here made the caller report success, so
        # the driver went on to update the gateways and announce to Matrix and
        # River a release that was still an unpublished draft.
        return 1
    fi

    echo -n "  Publishing draft release... "
    gh release edit "v$VERSION" --repo freenet/freenet-core --draft=false > /dev/null
    echo "✓"
}

verify_required_binaries() {
    local required=("$@")
    local assets
    assets=$(gh release view "v$VERSION" --repo freenet/freenet-core --json assets --jq '.assets[].name' 2>/dev/null)
    if [[ -z "$assets" ]]; then
        return 1
    fi
    local missing=()
    # Whole-line match against the asset list, done with a bash glob rather than
    # `echo "$assets" | grep -xqF`. Under `pipefail` that form makes `echo` take
    # SIGPIPE the moment `grep -q` short-circuits on a match, and 141 becomes the
    # pipeline's status -- so a binary that IS present reads as MISSING. Measured
    # here at 46 hits in 20000 iterations under 24-way CPU load (0 in a quiet
    # window), i.e. it fires exactly on the contended runners this gate runs on.
    # The consequence is the one the comment above warns about: wait_for_binaries
    # returns 1 and the driver dies AFTER publishing but BEFORE the gateway
    # updates and announcements.
    local assets_nl=$'\n'"$assets"$'\n'
    for bin in "${required[@]}"; do
        if [[ "$assets_nl" != *$'\n'"$bin"$'\n'* ]]; then
            missing+=("$bin")
        fi
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        for m in "${missing[@]}"; do
            echo "  ✗ Missing: $m"
        done
        return 1
    fi
    return 0
}

wait_for_binaries() {
    echo "Waiting for cross-compile workflow to complete:"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would wait for cross-compile workflow"
        return 0
    fi

    # Required platform binaries that must be present in the release.
    # Both freenet and fdev must be present for all platforms.
    local REQUIRED_BINARIES=(
        "freenet-x86_64-unknown-linux-musl.tar.gz"
        "freenet-aarch64-unknown-linux-musl.tar.gz"
        "freenet-aarch64-apple-darwin.tar.gz"
        "freenet-x86_64-apple-darwin.tar.gz"
        "freenet-x86_64-pc-windows-msvc.zip"
        "fdev-x86_64-unknown-linux-musl.tar.gz"
        "fdev-aarch64-unknown-linux-musl.tar.gz"
        "fdev-aarch64-apple-darwin.tar.gz"
        "fdev-x86_64-apple-darwin.tar.gz"
        "fdev-x86_64-pc-windows-msvc.zip"
    )

    # Check if all required binaries are already available
    if verify_required_binaries "${REQUIRED_BINARIES[@]}"; then
        echo "  ✓ All required platform binaries already available"
        # GUARDED, not bare. `publish_draft_release` returns 1 on two deliberate
        # refusals -- draft state unknown, and still-a-draft with the Gate A
        # canary not concluded -- and those must reach the caller.
        #
        # Bare, they did not. The call site in the main flow is now
        # `if ! wait_for_binaries`, and bash suspends errexit for the entire
        # DYNAMIC EXTENT of a command in an `if`/`!` condition, which includes
        # the whole body of the called function. So a bare failing call here no
        # longer aborted: control fell through to the `return 0` below,
        # `wait_for_binaries` reported SUCCESS, and the driver went on to
        # publish crates, update gateways and announce -- for a release still
        # sitting as an unpublished draft, possibly one the blocking canary had
        # rejected. Verified empirically, both call conventions.
        #
        # That is a FAIL-OPEN regression, strictly worse than the fail-closed
        # bug the guard was added to fix. `return 1` inside the `if` is
        # unaffected by errexit suspension, so this restores the refusal under
        # either convention.
        if ! publish_draft_release; then
            return 1
        fi
        return 0
    fi

    echo "  ⏳ Cross-compile workflow in progress..."
    echo "     Monitor at: https://github.com/freenet/freenet-core/actions/workflows/cross-compile.yml"

    # Find the workflow run for this tag — retry because it takes a few seconds
    # for GitHub to start the workflow after the tag is pushed.
    local run_id=""
    local find_elapsed=0
    local find_max=120  # 2 minutes to find the run
    while [[ $find_elapsed -lt $find_max ]]; do
        # `|| echo ""` so a transient `gh` failure retries on the next tick
        # instead of aborting the driver. `set -o pipefail` propagates `gh`'s
        # status out of the pipeline, so `head -1` does NOT absorb it.
        run_id=$(gh run list --workflow=cross-compile.yml --repo freenet/freenet-core --json databaseId,headBranch --jq ".[] | select(.headBranch == \"v$VERSION\") | .databaseId" 2>/dev/null | head -1 || echo "")
        if [[ -n "$run_id" ]]; then
            break
        fi
        printf "  Waiting for workflow to appear... (%ds)\r" "$find_elapsed"
        sleep 10
        find_elapsed=$((find_elapsed + 10))
    done

    if [[ -z "$run_id" ]]; then
        echo "  ✗ Could not find cross-compile workflow run for v$VERSION after ${find_max}s"
        echo "     Release binaries will NOT be available for auto-update."
        echo "     Check: https://github.com/freenet/freenet-core/actions/workflows/cross-compile.yml"
        return 1
    fi

    echo "  Workflow run ID: $run_id"

    # Wait up to 20 minutes for workflow to complete
    local max_wait=1200
    local elapsed=0
    local interval=30

    while [[ $elapsed -lt $max_wait ]]; do
        # Watch the JOB that attaches and publishes, not the whole RUN. The run
        # also carries the post-publish self-update canary (Gate B, #5222),
        # which starts only after this job has published the release and is
        # explicitly non-blocking -- see attach_job_state for what waiting on
        # the run instead costs. An empty state means the job has not started
        # yet (it waits on all six build jobs), so keep waiting.
        # `|| echo ""` per attach_job_state's contract. A single rate-limit or
        # 5xx anywhere in this multi-minute wait would otherwise abort the
        # driver mid-release: the release publishes, but the gateways are never
        # updated and it is never announced. Empty just means "poll again".
        local job_state status conclusion
        job_state=$(attach_job_state "$run_id" || echo "")
        status="${job_state%%:*}"
        conclusion="${job_state#*:}"

        # No job at all, and the RUN has finished: the job was cancelled before
        # it was created, or renamed out from under ATTACH_JOB_NAME. Nothing is
        # ever going to appear, so stop rather than burn the full timeout --
        # watching the job instead of the run must not cost us this fast exit.
        # Reported as UNKNOWN, never as a pass.
        if [[ -z "$job_state" ]]; then
            # Same guard, and it matters most here: this branch is the whole
            # build window (job_state is empty until the six build jobs finish),
            # so it is the busiest `gh` call in the release.
            local run_status
            run_status=$(gh run view "$run_id" --repo freenet/freenet-core --json status --jq '.status' 2>/dev/null || echo "")
            if [[ "$run_status" == "completed" ]]; then
                echo "  ✗ '$ATTACH_JOB_NAME' never reported a result, and the run has finished"
                echo "     (cancelled before the job started, or the job was renamed --"
                echo "     if renamed, update ATTACH_JOB_NAME in this script)."
                echo "     Check: https://github.com/freenet/freenet-core/actions/runs/$run_id"
                return 1
            fi
        fi

        if [[ "$status" == "completed" ]]; then
            if [[ "$conclusion" == "success" ]]; then
                echo "  ✓ Binaries attached and release published"

                # Verify all required platform binaries are uploaded
                sleep 5  # Brief delay for asset upload
                if verify_required_binaries "${REQUIRED_BINARIES[@]}"; then
                    echo "  ✓ All required platform binaries attached"
                    # Guarded for the same reason as the other call site above:
                    # bare, its refusal is swallowed when `wait_for_binaries` is
                    # itself called from an `if !` condition, and the adjacent
                    # `return 0` then reports success for an unpublished draft.
                    if ! publish_draft_release; then
                        return 1
                    fi
                    return 0
                else
                    echo "  ✗ Attach job succeeded but some required binaries are missing"
                    echo "     Check: https://github.com/freenet/freenet-core/actions/runs/$run_id"
                    return 1
                fi
            else
                echo "  ✗ '$ATTACH_JOB_NAME' failed (conclusion: $conclusion)"
                echo "     Either a build is missing or the BLOCKING auto-update"
                echo "     pre-flight canary (#5222) rejected the binary. Binaries"
                echo "     will NOT be available for auto-update."
                echo "     Check: https://github.com/freenet/freenet-core/actions/runs/$run_id"
                return 1
            fi
        fi

        printf "  Waiting... (%ds elapsed, status: %s)\r" "$elapsed" "${status:-pending}"
        sleep $interval
        elapsed=$((elapsed + interval))
    done

    echo
    echo "  ✗ Timeout waiting for cross-compile workflow after ${max_wait}s"
    echo "     Binaries may not be available for auto-update."
    echo "     Check: https://github.com/freenet/freenet-core/actions/runs/$run_id"
    return 1
}

announce_to_matrix() {
    echo "Announcing release to Matrix:"

    # If the GitHub workflow path is wired up (release-announce.yml has
    # been merged and the gateway has the Matrix secret), the workflow
    # will post automatically when the release is undrafted. Skip here
    # to avoid a duplicate Matrix message. Setting
    # FREENET_RELEASE_SKIP_ANNOUNCEMENTS=1 in your env (or sourcing it
    # from .freenet-release-rc) opts out of the local announcements.
    if [[ "${FREENET_RELEASE_SKIP_ANNOUNCEMENTS:-0}" == "1" ]]; then
        echo "  ⏭  Skipping (FREENET_RELEASE_SKIP_ANNOUNCEMENTS=1 — workflow handles announcements)"
        mark_completed "MATRIX_ANNOUNCED"
        return 0
    fi

    # Skip if already announced
    if is_step_completed "MATRIX_ANNOUNCED"; then
        echo "  ✓ [MATRIX_ANNOUNCED] Matrix announcement already sent (skipping)"
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would announce to #freenet-locutus:matrix.org"
        return 0
    fi

    # Check if matrix-commander is available
    if ! command -v matrix-commander &> /dev/null; then
        echo "  ⚠️  matrix-commander not found, skipping Matrix announcement"
        return 0
    fi

    # Simple 2-3 line announcement
    local announcement="Freenet v$VERSION released: https://github.com/freenet/freenet-core/releases/tag/v$VERSION
Published to crates.io (freenet v$VERSION, fdev v$FDEV_VERSION)"

    echo -n "  Sending announcement to Matrix... "
    # Use room ID instead of alias for reliability, add timeout
    if timeout 30 matrix-commander -r '!ygHfYcXtXmivTbOwjX:matrix.org' -m "$announcement" &>/dev/null; then
        echo "✓"
        mark_completed "MATRIX_ANNOUNCED"
    else
        echo "✗"
        echo "  ⚠️  Failed to send Matrix announcement (non-critical)"
    fi
}

announce_to_river() {
    echo "Announcing release to River (Freenet Official room):"

    # Same workflow-handoff gate as announce_to_matrix. When the
    # release-announce.yml workflow is wired up, it posts via nova's
    # /announce/river endpoint. Local riverctl post would duplicate.
    if [[ "${FREENET_RELEASE_SKIP_ANNOUNCEMENTS:-0}" == "1" ]]; then
        echo "  ⏭  Skipping (FREENET_RELEASE_SKIP_ANNOUNCEMENTS=1 — workflow handles announcements)"
        mark_completed "RIVER_ANNOUNCED"
        return 0
    fi

    # Skip if already announced
    if is_step_completed "RIVER_ANNOUNCED"; then
        echo "  ✓ [RIVER_ANNOUNCED] River announcement already sent (skipping)"
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would announce to Freenet Official River room"
        return 0
    fi

    # Check if river repo is available (we use cargo run, not installed binary)
    local RIVER_DIR="$HOME/code/freenet/river/main"
    if [[ ! -d "$RIVER_DIR/cli" ]]; then
        echo "  ⚠️  River repo not found at $RIVER_DIR, skipping River announcement"
        return 0
    fi

    # Check if Freenet node is running (required for riverctl)
    if ! curl -s --max-time 2 http://127.0.0.1:7509/ &>/dev/null; then
        echo "  ⚠️  Freenet node not running on localhost:7509"
        echo "     Skipping River announcement (riverctl requires local node)"
        return 0
    fi

    # River Official room details
    # Room Owner VK is also the room ID for riverctl
    local ROOM_OWNER_VK="4uNUKFzZQCnzo4K2ecZ16cMsYEEfoaRS35z6exEsbvm4"
    local SIGNING_KEY_FILE="$HOME/.config/freenet-river-official/room_owner_signing_key.bin"
    local ROOMS_JSON="$HOME/.local/share/river/rooms.json"

    # Check if we have the room in local storage with signing key
    if [[ ! -f "$ROOMS_JSON" ]]; then
        echo "  ⚠️  River rooms.json not found at $ROOMS_JSON"
        echo "     Setup required: run 'riverctl room create' first"
        return 0
    fi

    if [[ ! -f "$SIGNING_KEY_FILE" ]]; then
        echo "  ⚠️  Room owner signing key not found at $SIGNING_KEY_FILE"
        echo "     Skipping River announcement"
        return 0
    fi

    # Simple announcement
    local announcement="Freenet v$VERSION released! https://github.com/freenet/freenet-core/releases/tag/v$VERSION"

    echo -n "  Sending announcement to River... "
    # IMPORTANT: Must use `cargo run -p riverctl` from the river repo, NOT the installed
    # `riverctl` binary. The installed binary embeds room_contract.wasm at install time,
    # which becomes stale when the contract WASM changes. The repo version uses a build
    # script that copies the current WASM from ui/public/contracts/ at build time.
    #
    # RIVER_SKIP_CONTRACT_CHECK=1 disables riverctl's build-time staleness check that
    # compares `ui/public/contracts/room_contract.wasm` against
    # `target/wasm32-unknown-unknown/release/room_contract.wasm`. That check catches
    # out-of-date WASM before *publishing* riverctl to crates.io, but here we are only
    # *running* riverctl locally to send a chat message. A developer with a freshly
    # rebuilt room-contract in their workspace will otherwise hit a panic unrelated to
    # sending the message.
    #
    # stderr is captured into a log instead of discarded so future failures are
    # diagnosable from the release log.
    #
    # --signing-key-file forces signing as the room owner. Without it riverctl
    # signs with whatever identity rooms.json currently holds (the chat-delegate
    # sync can silently rewrite that to a non-owner), and the room contract then
    # drops the message on merge while riverctl still exits 0 — the silent
    # failure that lost the v0.2.67/v0.2.68 announcements. The override is
    # in-memory and never persisted, so it is immune to that drift. This mirrors
    # the canonical announce-to-river.sh path used by the release-agent.
    local RIVER_DIR="$HOME/code/freenet/river/main"
    local RIVER_LOG="/tmp/release-$VERSION-river.log"
    if [[ -d "$RIVER_DIR" ]]; then
        if (cd "$RIVER_DIR" && RIVER_SKIP_CONTRACT_CHECK=1 timeout 180 cargo run -p riverctl -- --signing-key-file "$SIGNING_KEY_FILE" message send "$ROOM_OWNER_VK" "$announcement" >"$RIVER_LOG" 2>&1); then
            echo "✓"
            mark_completed "RIVER_ANNOUNCED"
        else
            local rc=$?
            echo "✗"
            echo "  ⚠️  Failed to send River announcement (non-critical, rc=$rc)"
            echo "     Last log lines from $RIVER_LOG:"
            tail -15 "$RIVER_LOG" 2>/dev/null | sed 's/^/       /'
            echo "     Manual: cd $RIVER_DIR && RIVER_SKIP_CONTRACT_CHECK=1 cargo run -p riverctl -- --signing-key-file $SIGNING_KEY_FILE message send $ROOM_OWNER_VK \"$announcement\""
        fi
    else
        echo "⚠️  River repo not found at $RIVER_DIR"
        echo "     Manual: cd <river-repo> && RIVER_SKIP_CONTRACT_CHECK=1 cargo run -p riverctl -- --signing-key-file $SIGNING_KEY_FILE message send $ROOM_OWNER_VK \"$announcement\""
    fi
}

# Main execution
echo "Freenet Release Script"
echo "======================"
echo "Project root: $PROJECT_ROOT"
echo "State file: $STATE_FILE"
echo

# Auto-detect what's already completed. load_state_file runs inside
# auto_detect_state and may restore FDEV_VERSION from a persisted value — so
# the "Target version" line must be printed *after* that, not before.
auto_detect_state
echo
echo "Target version: freenet $VERSION, fdev $FDEV_VERSION"
echo

check_prerequisites
update_versions
create_release_pr
create_github_release
# GUARDED, not bare, and that is the whole point of the `if`.
#
# This script runs under `set -euo pipefail`, and `wait_for_binaries` fails on
# six modes (enumerated once, at the refusal branch below). Called bare, every
# one of those aborted the script HERE -- so `publish_crates`, whose own comment
# called it the backstop for when "the workflow path is broken", was unreachable
# in precisely the cases where the workflow path is broken.
#
# NOTE the last item, and how it is phrased. An earlier version of this comment
# enumerated "five paths" and listed only explicit `return 1` statements inside
# `wait_for_binaries` itself. That enumeration is what hid the bug this guard
# introduced: it counted RETURN STATEMENTS rather than WAYS THE FUNCTION CAN
# FAIL, and so did not name failure INHERITED FROM A BARE CALLEE -- which is
# the one path errexit suspension breaks. It also mis-attributed "draft state
# unknown", which is `publish_draft_release`'s return, not this function's.
# Both inner call sites are now guarded so the refusal propagates regardless.
#
# The ordering pin in release_canary_wiring_test.sh could not see this: it
# compares the LINE NUMBERS of the two calls, which were correct while the
# second call was dead. release_driver_test.sh drives it behaviourally instead
# -- and asserts that `publish_crates` does NOT run when `wait_for_binaries`
# fails. An earlier version of that case asserted the opposite, which is the
# unsafe property; see the refusal branch below and the note on publish_crates.
#
# REFUSES TO PUBLISH. This branch does NOT call publish_crates, and that is the
# whole point of it.
#
# An earlier revision called publish_crates here, on the theory that it was a
# harmless confirmation and a backstop for a CARGO_REGISTRY_TOKEN that never
# reached CI. Both halves were wrong.
#
# WHY IT IS UNSAFE. THIS IS THE CANONICAL ENUMERATION of how
# `wait_for_binaries` can fail; anywhere else that needs it refers here rather
# than restating it. Six modes, and publishing is wrong on five:
#
#   no workflow run found         -- Gate A never ran       -> publish ungated
#   attach job never reported     -- Gate A state unknown   -> publish ungated
#   attach conclusion != success  -- Gate A REJECTED it     -> publish what the
#                                                              gate blocked
#   timeout                       -- Gate A undecided       -> pre-empt the gate
#   refusal from publish_draft_release
#                                 -- draft state unknown,
#                                    or canary not concluded
#                                                           -> publish ungated
#   assets missing, attach OK     -- Gate A passed          -> the only safe one
#
# The sixth is the one two earlier versions of these comments left out, in
# opposite places, while both claiming "five paths" -- the inherited refusal.
# It is not an explicit `return 1` in this function's own body, so an
# enumeration written by reading `return` statements misses it. That is exactly
# the mistake recorded below as having hidden the round-7 bug, and it recurred
# HERE, in the comment describing it, twice, with different members each time.
# Restating a list is how the copies drift; hence one canonical copy.
#
# `publish_crates` consults exactly three things: the resume flag, DRY_RUN, and
# whether the version is already on crates.io. It never looks at the attach
# job's conclusion, the canary, or the draft state. And on the rejected path the
# crate is genuinely ABSENT -- cross-compile.yml runs the canary (:772) before
# its publish step (:803), so a rejected binary means nothing was uploaded --
# which is precisely when the already-published check says "no" and the fallback
# really does upload it. Five of the six modes publish a version Gate A never
# passed, including the one where it actively rejected the binary.
#
# WHY THE STATED JUSTIFICATION WAS UNACHIEVABLE. The missing-token case cannot
# reach here: cross-compile.yml checks the credential at step :705, BEFORE the
# canary at :772, so a missing token fails the job before the gate ever runs.
# And at this call site missing-token and canary-rejected are INDISTINGUISHABLE
# -- both arrive as the same `conclusion != success` on the same job. One would
# warrant publishing; the other is the unrecoverable spent-version state. A
# branch that cannot tell them apart must not publish.
#
# The deeper lesson, and it is the same one recorded on `detect_crates_state`
# above: this fallback was previously DEAD, because `wait_for_binaries` was
# called bare under `set -e`. Making it reachable removed the accident that was
# preventing an unsafe publish. Before making dead code live, ask what its being
# dead was protecting.
#
# Precedent for refusing rather than guessing: `publish_draft_release` already
# refuses on anything other than `completed:success`, for the same reason.
if ! wait_for_binaries; then
    echo
    echo "❌ The cross-compile workflow did not complete successfully." >&2
    echo >&2
    echo "   NOT publishing to crates.io from here, deliberately. This point is" >&2
    echo "   reached for any of six reasons -- Gate A never ran, its verdict is" >&2
    echo "   unknown, it timed out, it REJECTED the binary, the draft-publish step" >&2
    echo "   refused, or the assets are missing though Gate A passed -- and this" >&2
    echo "   branch cannot tell them apart. Only the last is safe to publish on." >&2
    echo "   Publishing on any of them uploads a version the blocking auto-update" >&2
    echo "   pre-flight canary never passed, which permanently spends the version" >&2
    echo "   number (the v0.2.124 state)." >&2
    echo >&2
    echo "   Find out WHY first:" >&2
    echo "     gh run list --repo freenet/freenet-core --workflow=cross-compile.yml \\" >&2
    echo "       --branch \"v$VERSION\" --limit 3" >&2
    echo >&2
    echo "   Then follow scripts/RELEASE_RECOVERY.md -- Step 4 if Gate A passed and" >&2
    echo "   only the crates publish is missing, Step 4b if Gate A blocked it." >&2
    exit 1
fi

# AFTER wait_for_binaries, not before create_github_release. The crates.io
# publish is the one irreversible step in a release, and it now sits downstream
# of the blocking pre-flight canary (in cross-compile.yml, which the tag push
# above triggers). By this point the workflow has normally published already and
# this call just confirms it; see the comment on publish_crates. Moving it back
# up is what cost v0.2.124 its version number.
publish_crates
trigger_gateway_updates
# Announce AFTER binaries are confirmed available and gateways updated,
# so users can actually update when they see the announcement.
announce_to_matrix
announce_to_river

echo
echo "🎉 Release $VERSION completed successfully!"
echo
echo "Summary:"
echo "- freenet $VERSION published to crates.io"
echo "- fdev $FDEV_VERSION published to crates.io"
echo "- GitHub release created: https://github.com/freenet/freenet-core/releases/tag/v$VERSION"
echo "- Cross-compiled binaries attached to release"
echo "- Gateways updated immediately"
echo "- Announcement sent to Matrix (#freenet-locutus)"
echo "- Announcement sent to River (Freenet Official room)"

echo
echo "Next steps:"
echo "- Update any dependent projects to use the new version"
echo
echo "⚠️  IMPORTANT: Post-release log review required!"
echo "   Wait 5-10 minutes, then check gateway logs for issues:"
echo
echo "   Quick check commands:"
echo "   ssh ian@nova.locut.us 'sudo /usr/local/bin/freenet --version'"
echo "   ssh ian@nova.locut.us 'systemctl is-active freenet-gateway freenet-gateway-2'"
echo
echo "   Look for: log spam, rapid log growth, new error patterns"
echo "   See: ~/.claude/skills/freenet-release/SKILL.md for full checklist"
echo
echo "State file: $STATE_FILE"
echo "  (Can be deleted now that release is complete)"