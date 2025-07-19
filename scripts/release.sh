#!/bin/bash
# Freenet Release Script
# Handles version bumping, testing, publishing, tagging, cross-compilation, and deployment

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse arguments
VERSION=""
SKIP_DEPLOY=false
DRY_RUN=false

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

# Derive fdev version (increment minor)
FDEV_MAJOR=$(echo "$VERSION" | cut -d. -f1)
FDEV_MINOR=$(echo "$VERSION" | cut -d. -f2)
FDEV_PATCH=$(echo "$VERSION" | cut -d. -f3)
FDEV_NEW_MINOR=$((FDEV_MINOR + 1))
FDEV_VERSION="${FDEV_MAJOR}.${FDEV_NEW_MINOR}.0"

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
            grep -q "version = \"$VERSION\"" crates/core/Cargo.toml 2>/dev/null
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
    sed -i "s/^version = \".*\"/version = \"$VERSION\"/" crates/core/Cargo.toml
    echo "✓"
    
    # Update fdev version and its freenet dependency
    echo -n "  Updating fdev to $FDEV_VERSION... "
    sed -i "s/^version = \".*\"/version = \"$FDEV_VERSION\"/" crates/fdev/Cargo.toml
    sed -i "s/freenet = { path = \"..\/core\", version = \".*\" }/freenet = { path = \"..\/core\", version = \"$VERSION\" }/" crates/fdev/Cargo.toml
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
    local max_total=1200  # 20 minutes max
    
    while true; do
        if [[ $total_wait -gt $max_total ]]; then
            echo "  ✗ Timeout waiting for PR to merge after 20 minutes"
            echo "    Check: https://github.com/freenet/freenet-core/pull/$pr_number"
            exit 1
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
    
    local release_notes="Release $VERSION

## Changes
- Version bump to $VERSION
- fdev updated to $FDEV_VERSION

See commit history for detailed changes since last release."
    
    run_cmd "Creating and pushing tag" git tag -a "v$VERSION" -m "Release v$VERSION"
    run_cmd "Pushing tag" git push origin "v$VERSION"
    
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
    
    echo "Deploying to gateways:"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would deploy to gateways"
        return 0
    fi
    
    run_cmd "Deploying to gateways" "$SCRIPT_DIR/deploy-to-gateways.sh" --skip-tests
}

# Main execution
echo "Freenet Release Script"
echo "======================"
echo "Target version: freenet $VERSION, fdev $FDEV_VERSION"
echo

check_prerequisites
update_versions
create_release_pr
publish_crates
create_github_release
trigger_cross_compile
deploy_gateways

echo
echo "🎉 Release $VERSION completed successfully!"
echo
echo "Summary:"
echo "- freenet $VERSION published to crates.io"
echo "- fdev $FDEV_VERSION published to crates.io"
echo "- GitHub release created: https://github.com/freenet/freenet-core/releases/tag/v$VERSION"
if [[ "$SKIP_DEPLOY" == "false" ]]; then
    echo "- Deployed to production gateways"
fi
echo
echo "Next steps:"
echo "- Monitor gateway logs for any issues"
echo "- Update any dependent projects to use the new version"