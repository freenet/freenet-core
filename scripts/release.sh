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

# Parse arguments
VERSION=""
DRY_RUN=false
SKIP_TESTS=false
DEPLOY_LOCAL=false
DEPLOY_REMOTE=false
RESUME=false
STATE_FILE=""

# Release steps for state tracking
declare -A RELEASE_STEPS=(
    [1]="PR_CREATED"
    [2]="PR_MERGED"
    [3]="TAG_CREATED"
    [4]="RELEASE_CREATED"
    [5]="CRATES_PUBLISHED"
    [6]="LOCAL_DEPLOYED"
    [7]="MATRIX_ANNOUNCED"
)

CURRENT_STEP=0

show_help() {
    echo "Freenet Release Script"
    echo
    echo "Usage: $0 --version X.Y.Z [options]"
    echo
    echo "This script automates the complete release process:"
    echo "• Version bumping → Release PR → GitHub CI → Auto-merge"
    echo "• Publishing to crates.io → GitHub release → Automatic cross-compilation"
    echo
    echo "Options:"
    echo "  --version X.Y.Z     Target version (required)"
    echo "  --deploy-local      Deploy to local gateway after release (optional)"
    echo "  --deploy-remote     Deploy to remote gateways after release (optional)"
    echo "  --skip-tests        Skip pre-release tests"
    echo "  --resume STATE_FILE Resume failed release from state file"
    echo "  --dry-run           Show what would be done without executing"
    echo "  --help              Show this help"
    echo
    echo "Notes:"
    echo "  • Relies on GitHub CI for testing (more reliable than local tests)"
    echo "  • Cross-compilation triggered automatically when version tag is pushed"
    echo "  • Can deploy to local gateway (--deploy-local) or remote (--deploy-remote)"
    echo "  • Manual deployment: scripts/deploy-local-gateway.sh or scripts/update-remote-gws.sh"
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
        --deploy-local)
            DEPLOY_LOCAL=true
            shift
            ;;
        --deploy-remote)
            DEPLOY_REMOTE=true
            shift
            ;;
        --skip-tests)
            SKIP_TESTS=true
            shift
            ;;
        --resume)
            RESUME=true
            STATE_FILE="$2"
            shift 2
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

# Get the most recently published version from crates.io (most authoritative source)
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

    local v1_major=$(echo "$v1" | cut -d. -f1)
    local v1_minor=$(echo "$v1" | cut -d. -f2)
    local v1_patch=$(echo "$v1" | cut -d. -f3)

    local v2_major=$(echo "$v2" | cut -d. -f1)
    local v2_minor=$(echo "$v2" | cut -d. -f2)
    local v2_patch=$(echo "$v2" | cut -d. -f3)

    if [[ $v1_major -gt $v2_major ]]; then echo "1"; return; fi
    if [[ $v1_major -lt $v2_major ]]; then echo "-1"; return; fi
    if [[ $v1_minor -gt $v2_minor ]]; then echo "1"; return; fi
    if [[ $v1_minor -lt $v2_minor ]]; then echo "-1"; return; fi
    if [[ $v1_patch -gt $v2_patch ]]; then echo "1"; return; fi
    if [[ $v1_patch -lt $v2_patch ]]; then echo "-1"; return; fi
    echo "0"
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

# Get current fdev version and increment patch version
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

# Helper functions
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

# Check if a step was already completed
step_completed() {
    local step="$1"
    case "$step" in
        "version_update")
            # Check if version is already updated in Cargo.toml
            grep -q "version = \"$VERSION\"" "$PROJECT_ROOT/crates/core/Cargo.toml" 2>/dev/null
            ;;
        "github_release")
            # Check if tag already exists
            git tag | grep -q "^v$VERSION$" 2>/dev/null
            ;;
        "crates_published")
            # Check if version exists on crates.io (simple check)
            cargo search freenet --limit 1 | grep -q "freenet = \"$VERSION\"" 2>/dev/null
            ;;
        *)
            return 1
            ;;
    esac
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

    if step_completed "version_update"; then
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

    # Update fdev version and its freenet dependency
    echo -n "  Updating fdev to $FDEV_VERSION... "
    sed_inplace "s/^version = \".*\"/version = \"$FDEV_VERSION\"/" "$PROJECT_ROOT/crates/fdev/Cargo.toml"
    sed_inplace "s/freenet = { path = \"..\/core\", version = \".*\" }/freenet = { path = \"..\/core\", version = \"$VERSION\" }/" "$PROJECT_ROOT/crates/fdev/Cargo.toml"
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

    # Verify that the project compiles with the new versions
    echo -n "  Verifying compilation... "
    if cargo build --release --quiet 2>/dev/null; then
        echo "✓"
    else
        echo "✗"
        echo ""
        echo "  ⚠️  ERROR: Project failed to compile with new versions!"
        echo "     Running cargo build to see errors:"
        echo ""
        cargo build --release
        echo ""
        echo "  Please fix compilation errors before releasing."
        exit 1
    fi

    # Run tests to ensure nothing is broken
    if [[ "$SKIP_TESTS" == "false" ]]; then
        echo -n "  Running tests... "
        if cargo test --release --quiet 2>/dev/null; then
            echo "✓"
        else
            echo "✗"
            echo ""
            echo "  ⚠️  ERROR: Tests failed with new versions!"
            echo "     Running cargo test to see failures:"
            echo ""
            cargo test --release
            echo ""
            echo "  Please fix test failures before releasing."
            exit 1
        fi
    else
        echo "  ⚠️  Skipping tests (--skip-tests specified)"
    fi
}

create_release_pr() {
    echo "Creating release PR:"

    local branch_name="release/v$VERSION"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would create branch $branch_name"
        echo "  [DRY RUN] Would create auto-merge PR"
        echo "  [DRY RUN] Would wait for GitHub CI"
        return 0
    fi

    # Check if a release PR for this version already exists or was merged
    echo -n "  Checking for existing release PR... "
    local existing_pr=$(gh pr list --head "$branch_name" --state all --limit 1 --json number,state,title --jq '.[] | "\(.number)|\(.state)|\(.title)"' 2>/dev/null || echo "")

    if [[ -n "$existing_pr" ]]; then
        local pr_number=$(echo "$existing_pr" | cut -d'|' -f1)
        local pr_state=$(echo "$existing_pr" | cut -d'|' -f2)
        local pr_title=$(echo "$existing_pr" | cut -d'|' -f3)
        echo "found #$pr_number ($pr_state)"

        if [[ "$pr_state" == "MERGED" ]]; then
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
            echo "  ℹ️  Release PR #$pr_number already exists and is open"
            echo "     Monitoring existing PR instead of creating a new one..."
            # Continue with the existing PR monitoring logic below
        else
            echo "  ⚠️  Release PR #$pr_number exists but is $pr_state"
        fi
    else
        echo "not found"
    fi

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

    run_cmd "Committing version bump" git commit -m "build: bump versions to $VERSION

- freenet: → $VERSION
- fdev: → $FDEV_VERSION

🤖 Automated release commit"

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
    
    echo -n "  Enabling auto-merge... "
    gh pr merge "$pr_number" --squash --auto >/dev/null 2>&1
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
            echo "      cargo publish -p freenet"
            echo "      sleep 30 && cargo publish -p fdev"
            echo "      git tag -a 'v$VERSION' -m 'Release v$VERSION' && git push origin 'v$VERSION'"
            echo "      gh release create 'v$VERSION' --title 'v$VERSION' --notes 'Release $VERSION'"
            # Don't exit with error - the PR might have merged
            return 0
        fi
        
        pr_state=$(gh pr view "$pr_number" --json state --jq '.state')
        
        case "$pr_state" in
            "MERGED")
                echo "  ✓ PR merged successfully!"
                break
                ;;
            "CLOSED")
                echo "  ✗ PR was closed without merging"
                exit 1
                ;;
            "OPEN")
                # Check for CI failures and show logs if any
                checks_output=$(gh pr checks "$pr_number" 2>/dev/null || echo "")
                failed_checks=$(echo "$checks_output" | grep -E "fail|error" | head -5)
                
                if [[ -n "$failed_checks" ]]; then
                    echo "  ✗ CI checks failed!"
                    echo "    Failed checks:"
                    echo "$failed_checks" | sed 's/^/      /'
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
                            echo "$failed_jobs" | sed 's/^/      - /'
                            echo
                            echo "    To view logs: gh run view $run_id --log-failed"
                        fi
                    fi
                    
                    echo "    💡 Fix the issues and the PR will auto-merge once CI passes"
                    exit 1
                else
                    # Show status for pending/in-progress checks
                    pending_checks=$(echo "$checks_output" | grep -E "pending|in_progress" | wc -l)
                    passed_checks=$(echo "$checks_output" | grep -E "pass" | wc -l)
                    if [[ $pending_checks -gt 0 ]]; then
                        echo "  ⏳ Waiting... ($passed_checks checks passed, $pending_checks in progress, ${total_wait}s elapsed)"
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
    local prev_version=$(gh release list --limit 50 --json tagName,createdAt --jq 'sort_by(.createdAt) | reverse | .[].tagName' 2>/dev/null | grep -v "^v${version}$" | head -1 | sed 's/^v//')

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

    local prev_date=$(gh release view "v${prev_version}" --json createdAt --jq '.createdAt' 2>/dev/null)

    # Fetch merged PRs since the previous release
    local prs=$(gh pr list --search "is:pr is:merged merged:>${prev_date}" --limit 100 --json number,title --jq '.[] | "#\(.number)|\(.title)"' 2>/dev/null || echo "")

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
        local number=$(echo "$pr" | cut -d'|' -f1)
        local title=$(echo "$pr" | cut -d'|' -f2-)

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

publish_crates() {
    echo "Publishing to crates.io:"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would publish freenet $VERSION"
        echo "  [DRY RUN] Would publish fdev $FDEV_VERSION"
        return 0
    fi

    # Check if freenet is already published
    echo -n "  Checking if freenet $VERSION is already published... "
    if cargo search freenet --limit 1 2>/dev/null | grep -q "freenet = \"$VERSION\""; then
        echo "yes"
        echo "  ✓ freenet $VERSION already published to crates.io"
    else
        echo "no"
        run_cmd "Publishing freenet $VERSION" cargo publish -p freenet

        # Wait a bit for crates.io to propagate
        echo -n "  Waiting for crates.io propagation... "
        sleep 30
        echo "✓"
    fi

    # Check if fdev is already published
    echo -n "  Checking if fdev $FDEV_VERSION is already published... "
    if cargo search fdev --limit 1 2>/dev/null | grep -q "fdev = \"$FDEV_VERSION\""; then
        echo "yes"
        echo "  ✓ fdev $FDEV_VERSION already published to crates.io"
    else
        echo "no"
        run_cmd "Publishing fdev $FDEV_VERSION" cargo publish -p fdev
    fi
}

create_github_release() {
    echo "Creating GitHub release:"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would create tag v$VERSION"
        echo "  [DRY RUN] Would create GitHub release"
        return 0
    fi

    # Check if release already exists
    echo -n "  Checking if release v$VERSION already exists... "
    if gh release view "v$VERSION" &>/dev/null; then
        echo "yes"
        echo "  ✓ GitHub release v$VERSION already exists"
        release_url=$(gh release view "v$VERSION" --json url --jq '.url')
        echo "  Release URL: $release_url"
        return 0
    else
        echo "no"
    fi

    # Check if tag already exists
    if git tag | grep -q "^v$VERSION$"; then
        echo "  ℹ️  Tag v$VERSION already exists locally"
    else
        run_cmd "Creating tag v$VERSION" git tag -a "v$VERSION" -m "Release v$VERSION"
    fi

    # Check if tag exists on remote
    if git ls-remote --tags origin | grep -q "refs/tags/v$VERSION$"; then
        echo "  ℹ️  Tag v$VERSION already exists on remote"
    else
        run_cmd "Pushing tag" git push origin "v$VERSION"
    fi

    echo -n "  Generating release notes... "
    local release_notes=$(generate_release_notes "$VERSION")
    echo "✓"

    echo -n "  Creating GitHub release... "
    release_url=$(gh release create "v$VERSION" --title "v$VERSION" --notes "$release_notes")
    echo "✓"

    echo "  Release created: $release_url"
}

# Note: Cross-compilation is now triggered automatically when a version tag is pushed
# See .github/workflows/cross-compile.yml - it has `tags: ['v*']` trigger
# trigger_cross_compile function is no longer needed

deploy_gateways() {
    # Skip if no deployment requested
    if [[ "$DEPLOY_LOCAL" == "false" ]] && [[ "$DEPLOY_REMOTE" == "false" ]]; then
        echo "Skipping gateway deployment (use --deploy-local or --deploy-remote to enable)"
        return 0
    fi

    echo "Gateway Deployment:"

    # Deploy to local gateway
    if [[ "$DEPLOY_LOCAL" == "true" ]]; then
        echo
        echo "Deploying to local gateway:"

        if [[ "$DRY_RUN" == "true" ]]; then
            echo "  [DRY RUN] Would run: $SCRIPT_DIR/deploy-local-gateway.sh"
        else
            local deploy_script="$SCRIPT_DIR/deploy-local-gateway.sh"
            if [[ ! -f "$deploy_script" ]]; then
                echo "  ⚠️  Deployment script not found: $deploy_script"
                echo "     Skipping local deployment"
            else
                if "$deploy_script" --binary "$PROJECT_ROOT/target/release/freenet"; then
                    echo "  ✓ Local gateway deployed successfully"
                else
                    echo "  ⚠️  Local gateway deployment failed (non-fatal)"
                    echo "     You can deploy manually with: $deploy_script"
                fi
            fi
        fi
    fi

    # Deploy to remote gateways
    if [[ "$DEPLOY_REMOTE" == "true" ]]; then
        echo
        echo "Deploying to remote gateways:"

        if [[ "$DRY_RUN" == "true" ]]; then
            echo "  [DRY RUN] Would run: $SCRIPT_DIR/update-remote-gws.sh"
        else
            local deploy_script="$SCRIPT_DIR/update-remote-gws.sh"
            if [[ ! -f "$deploy_script" ]]; then
                echo "  ⚠️  Deployment script not found: $deploy_script"
                echo "     Skipping remote deployment"
            else
                echo "  Running remote deployment script..."
                if "$deploy_script"; then
                    echo "  ✓ Remote gateways deployed successfully"
                else
                    echo "  ⚠️  Remote gateway deployment failed (non-fatal)"
                    echo "     You can deploy manually with: $deploy_script"
                fi
            fi
        fi
    fi
}

announce_to_matrix() {
    echo "Announcing release to Matrix:"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would announce to #freenet-locutus:matrix.org"
        return 0
    fi

    # Check if matrix-commander is available
    if ! command -v matrix-commander &> /dev/null; then
        echo "  ⚠️  matrix-commander not found, skipping Matrix announcement"
        return 0
    fi

    # Count bug fixes, features, and maintenance items from release notes
    local release_notes=$(generate_release_notes "$VERSION")
    local fix_count=$(echo "$release_notes" | grep -c "^- \*\*#.*Bug Fixes" || echo "0")
    local feature_count=$(echo "$release_notes" | grep -c "^- \*\*#.*Features" || echo "0")

    # Extract key highlights (first 3 fixes or features)
    local highlights=$(echo "$release_notes" | grep "^- \*\*#" | head -3 | sed 's/^- /  - /')

    local announcement="Freenet v$VERSION Released!

Published to crates.io:
  - freenet v$VERSION
  - fdev v$FDEV_VERSION

Release: https://github.com/freenet/freenet-core/releases/tag/v$VERSION

Key highlights:
$highlights

Full changelog available at the release link above.

[AI-assisted release announcement]"

    echo -n "  Sending announcement to Matrix... "
    # Use room ID instead of alias for reliability, add timeout
    if timeout 30 matrix-commander -r '!ygHfYcXtXmivTbOwjX:matrix.org' -m "$announcement" &>/dev/null; then
        echo "✓"
    else
        echo "✗"
        echo "  ⚠️  Failed to send Matrix announcement (non-critical)"
    fi
}

# Main execution
echo "Freenet Release Script"
echo "======================"
echo "Target version: freenet $VERSION, fdev $FDEV_VERSION"
echo "Project root: $PROJECT_ROOT"
echo

check_prerequisites
update_versions
create_release_pr
publish_crates
create_github_release
deploy_gateways
announce_to_matrix

echo
echo "🎉 Release $VERSION completed successfully!"
echo
echo "Summary:"
echo "- freenet $VERSION published to crates.io"
echo "- fdev $FDEV_VERSION published to crates.io"
echo "- GitHub release created: https://github.com/freenet/freenet-core/releases/tag/v$VERSION"
echo "- Announcement sent to Matrix (#freenet-locutus)"
echo "- Cross-compilation workflow triggered automatically by tag push"
echo "- Binaries will be attached to release when workflow completes"

# Show deployment status
if [[ "$DEPLOY_LOCAL" == "true" ]]; then
    echo "- Local gateway deployed to $(hostname)"
fi
if [[ "$DEPLOY_REMOTE" == "true" ]]; then
    echo "- Remote gateways deployment initiated"
fi

echo
echo "Next steps:"
echo "- Monitor cross-compile workflow: https://github.com/freenet/freenet-core/actions/workflows/cross-compile.yml"

# Suggest deployment options if not used
if [[ "$DEPLOY_LOCAL" == "false" ]] && [[ "$DEPLOY_REMOTE" == "false" ]]; then
    echo "- Deploy locally: scripts/deploy-local-gateway.sh"
    echo "- Deploy to remote gateways: scripts/update-remote-gws.sh"
elif [[ "$DEPLOY_LOCAL" == "false" ]]; then
    echo "- Deploy locally: scripts/deploy-local-gateway.sh"
elif [[ "$DEPLOY_REMOTE" == "false" ]]; then
    echo "- Deploy to remote gateways: scripts/update-remote-gws.sh"
fi

echo "- Update any dependent projects to use the new version"