#!/bin/bash
# Freenet Release Script
# Handles version bumping, testing, publishing, tagging, cross-compilation, and deployment

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTING_TOOLS_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Find the main freenet-core repository
if [[ -d "$TESTING_TOOLS_ROOT/../freenet-core/main" ]]; then
    PROJECT_ROOT="$(cd "$TESTING_TOOLS_ROOT/../freenet-core/main" && pwd)"
elif [[ -d "$HOME/code/freenet/freenet-core/main" ]]; then
    PROJECT_ROOT="$HOME/code/freenet/freenet-core/main"
else
    echo "Error: Could not find freenet-core/main repository"
    echo "Expected at: $HOME/code/freenet/freenet-core/main"
    exit 1
fi

# Parse arguments
VERSION=""
SKIP_DEPLOY=false
DRY_RUN=false
SKIP_TESTS=false

show_help() {
    echo "Freenet Release Script"
    echo
    echo "Usage: $0 --version X.Y.Z [options]"
    echo
    echo "This script automates the complete release process:"
    echo "• Version bumping → Release PR → GitHub CI → Auto-merge"  
    echo "• Publishing to crates.io → GitHub release → Cross-compilation → Gateway deployment"
    echo
    echo "Options:"
    echo "  --version X.Y.Z     Target version (required)"
    echo "  --skip-deploy       Skip gateway deployment"
    echo "  --skip-tests        Skip pre-release tests"
    echo "  --dry-run           Show what would be done without executing"
    echo "  --help              Show this help"
    echo
    echo "Note: Relies on GitHub CI for testing (more reliable than local tests)"
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
        --skip-deploy)
            SKIP_DEPLOY=true
            shift
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
    
    # Run pre-release testing checklist
    if [[ "$SKIP_TESTS" == "false" ]]; then
        echo ""
        echo "Running pre-release tests:"
        PRE_RELEASE_SCRIPT="$TESTING_TOOLS_ROOT/gateway-testing/pre_release_checklist.sh"
        if [[ -f "$PRE_RELEASE_SCRIPT" ]]; then
            echo "  Running pre-release checklist..."
            if ! "$PRE_RELEASE_SCRIPT"; then
                echo "  ✗ Pre-release tests failed"
                echo "    Please fix all issues before releasing"
                exit 1
            fi
        else
            echo "  ⚠️  Warning: Pre-release checklist not found at $PRE_RELEASE_SCRIPT"
            echo "    Continuing without pre-release tests (not recommended)"
            echo -n "    Continue anyway? (y/n) "
            read -r response
            if [[ "$response" != "y" ]]; then
                echo "    Release cancelled"
                exit 1
            fi
        fi
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

    # Update freenet version
    echo -n "  Updating freenet to $VERSION... "
    sed -i "s/^version = \".*\"/version = \"$VERSION\"/" "$PROJECT_ROOT/crates/core/Cargo.toml"
    echo "✓"

    # Update fdev version and its freenet dependency
    echo -n "  Updating fdev to $FDEV_VERSION... "
    sed -i "s/^version = \".*\"/version = \"$FDEV_VERSION\"/" "$PROJECT_ROOT/crates/fdev/Cargo.toml"
    sed -i "s/freenet = { path = \"..\/core\", version = \".*\" }/freenet = { path = \"..\/core\", version = \"$VERSION\" }/" "$PROJECT_ROOT/crates/fdev/Cargo.toml"
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
    
    run_cmd "Creating release branch" git checkout -b "$branch_name"
    run_cmd "Adding changes" git add -A
    run_cmd "Committing version bump" git commit -m "chore: bump versions to $VERSION

- freenet: → $VERSION
- fdev: → $FDEV_VERSION

🤖 Automated release commit"
    
    run_cmd "Pushing branch" git push origin "$branch_name"
    
    echo -n "  Creating auto-merge PR... "
    pr_number=$(gh pr create \
        --title "🚀 Release $VERSION" \
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
                    if [[ $pending_checks -gt 0 ]]; then
                        echo "  ⏳ Waiting... ($pending_checks checks in progress)"
                    else
                        echo "  ⏳ Waiting for auto-merge..."
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

    # Publish freenet first (fdev depends on it)
    run_cmd "Publishing freenet $VERSION" cargo publish -p freenet

    # Wait a bit for crates.io to propagate
    echo -n "  Waiting for crates.io propagation... "
    sleep 30
    echo "✓"

    run_cmd "Publishing fdev $FDEV_VERSION" cargo publish -p fdev
}

create_github_release() {
    echo "Creating GitHub release:"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would create tag v$VERSION"
        echo "  [DRY RUN] Would create GitHub release"
        return 0
    fi

    run_cmd "Creating and pushing tag" git tag -a "v$VERSION" -m "Release v$VERSION"
    run_cmd "Pushing tag" git push origin "v$VERSION"

    echo -n "  Generating release notes... "
    local release_notes=$(generate_release_notes "$VERSION")
    echo "✓"

    echo -n "  Creating GitHub release... "
    release_url=$(gh release create "v$VERSION" --title "v$VERSION" --notes "$release_notes")
    echo "✓"

    echo "  Release created: $release_url"
}

trigger_cross_compile() {
    echo "Triggering cross-compilation:"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would trigger Build and Cross-Compile workflow"
        return 0
    fi
    
    run_cmd "Triggering workflow" gh workflow run "Build and Cross-Compile" --ref main
    
    echo -n "  Waiting for workflow to complete... "
    sleep 10  # Give workflow time to start
    
    local run_id=""
    while [[ -z "$run_id" ]]; do
        run_id=$(gh run list --workflow="Build and Cross-Compile" --limit 1 --json databaseId --jq '.[0].databaseId' 2>/dev/null || echo "")
        sleep 5
    done
    
    # Use gh run watch for better experience
    echo
    echo "  Monitoring workflow run $run_id..."
    if gh run watch "$run_id"; then
        echo "  ✓ Cross-compilation completed!"
    else
        echo "  ✗ Cross-compilation failed"
        exit 1
    fi
}

deploy_gateways() {
    if [[ "$SKIP_DEPLOY" == "true" ]]; then
        echo "Skipping gateway deployment (--skip-deploy specified)"
        return 0
    fi

    echo "Deploying to remote gateways:"
    echo "  ℹ️  Remote gateways (vega, ziggy) are no longer active"
    echo "  ℹ️  Only local gateway (nova) will be updated"
}

update_local_gateway() {
    if [[ "$SKIP_DEPLOY" == "true" ]]; then
        echo "Skipping local gateway update (--skip-deploy specified)"
        return 0
    fi

    echo "Updating local gateway:"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would update local gateway service"
        return 0
    fi

    # Check if local gateway service exists
    if ! systemctl status freenet-gateway.service &> /dev/null; then
        echo "  ⚠️  Local gateway service not found, skipping"
        return 0
    fi

    # Build the release binary if it doesn't exist
    local binary_path="$PROJECT_ROOT/target/release/freenet"
    if [[ ! -f "$binary_path" ]]; then
        echo -n "  Building release binary... "
        if cargo build --release -p freenet --quiet 2>/dev/null; then
            echo "✓"
        else
            echo "✗"
            echo "  ⚠️  Failed to build release binary, skipping local gateway update"
            return 1
        fi
    fi

    run_cmd "Stopping local gateway service" sudo systemctl stop freenet-gateway.service
    run_cmd "Installing new binary" sudo cp "$binary_path" /usr/local/bin/freenet
    run_cmd "Setting permissions" sudo chown root:root /usr/local/bin/freenet
    run_cmd "Setting executable" sudo chmod 755 /usr/local/bin/freenet

    # Clear old logs for cleaner verification
    echo -n "  Clearing old logs... "
    sudo journalctl --vacuum-time=1s -u freenet-gateway.service &>/dev/null || true
    echo "✓"

    run_cmd "Starting local gateway service" sudo systemctl start freenet-gateway.service

    # Wait for service to start
    echo -n "  Waiting for service startup... "
    sleep 3
    echo "✓"

    # Verify the service is running
    echo -n "  Verifying deployment... "
    if systemctl is-active --quiet freenet-gateway.service; then
        local version=$(/usr/local/bin/freenet --version 2>&1 | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' || echo "unknown")
        echo "✓"
        echo "  ✓ Version: $version"
        echo "  ✓ Service: Running"
    else
        echo "✗"
        echo "  ⚠️  Service failed to start, check logs:"
        echo "      sudo journalctl -u freenet-gateway.service -n 50"
        return 1
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
    local highlights=$(echo "$release_notes" | grep "^- \*\*#" | head -3 | sed 's/^- /  • /')

    local announcement="🎉 **Freenet v$VERSION Released!**

📦 Published to crates.io:
  • freenet v$VERSION
  • fdev v$FDEV_VERSION

🔗 Release: https://github.com/freenet/freenet-core/releases/tag/v$VERSION

Key highlights:
$highlights

Full changelog available at the release link above.

[AI-assisted release announcement]"

    echo -n "  Sending announcement to Matrix... "
    if matrix-commander -r '#freenet-locutus:matrix.org' -m "$announcement" &>/dev/null; then
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
trigger_cross_compile
deploy_gateways
update_local_gateway
announce_to_matrix

echo
echo "🎉 Release $VERSION completed successfully!"
echo
echo "Summary:"
echo "- freenet $VERSION published to crates.io"
echo "- fdev $FDEV_VERSION published to crates.io"
echo "- GitHub release created: https://github.com/freenet/freenet-core/releases/tag/v$VERSION"
if [[ "$SKIP_DEPLOY" == "false" ]]; then
    echo "- Local gateway (nova) updated to v$VERSION"
fi
echo "- Announcement sent to Matrix (#freenet-locutus)"
echo
echo "Next steps:"
echo "- Monitor gateway logs: sudo journalctl -u freenet-gateway.service -f"
echo "- Update any dependent projects to use the new version"