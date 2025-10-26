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
DRY_RUN=false
YANK_CRATES=false

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
    echo "  --version X.Y.Z  Version to rollback (required)"
    echo "  --yank-crates    Yank crates from crates.io (optional, use with caution)"
    echo "  --dry-run        Show what would be done without executing"
    echo "  --help           Show this help"
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

TAG="v$VERSION"

echo "Freenet Release Rollback"
echo "========================"
echo "Version:     $VERSION"
echo "Tag:         $TAG"
if [[ "$DRY_RUN" == "true" ]]; then
    echo "Mode:        DRY RUN"
fi
if [[ "$YANK_CRATES" == "true" ]]; then
    echo "Yank crates: YES"
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
echo -n "[1/4] Deleting local git tag... "
if git rev-parse "$TAG" >/dev/null 2>&1; then
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY RUN]"
    else
        git tag -d "$TAG"
        echo "✓"
    fi
else
    echo "not found, skipping"
fi

# Delete remote git tag
echo -n "[2/4] Deleting remote git tag... "
if git ls-remote --tags origin | grep -q "refs/tags/$TAG"; then
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY RUN]"
    else
        git push origin --delete "$TAG" 2>&1 | grep -v "^remote:" || true
        echo "✓"
    fi
else
    echo "not found, skipping"
fi

# Delete GitHub release
echo -n "[3/4] Deleting GitHub release... "
if gh release view "$TAG" --repo freenet/freenet-core >/dev/null 2>&1; then
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY RUN]"
    else
        gh release delete "$TAG" --repo freenet/freenet-core --yes
        echo "✓"
    fi
else
    echo "not found, skipping"
fi

# Yank crates from crates.io
if [[ "$YANK_CRATES" == "true" ]]; then
    echo "[4/4] Yanking crates from crates.io..."

    # Calculate fdev version
    IFS='.' read -r major minor patch <<< "$VERSION"
    minor_plus_2=$((minor + 2))
    FDEV_VERSION="0.${minor_plus_2}.${patch}"

    echo -n "  Yanking freenet v$VERSION... "
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY RUN]"
    else
        if cargo yank --vers "$VERSION" freenet 2>&1 | grep -q "successfully yanked\|already yanked"; then
            echo "✓"
        else
            echo "✗ (failed or not published)"
        fi
    fi

    echo -n "  Yanking fdev v$FDEV_VERSION... "
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY RUN]"
    else
        if cargo yank --vers "$FDEV_VERSION" fdev 2>&1 | grep -q "successfully yanked\|already yanked"; then
            echo "✓"
        else
            echo "✗ (failed or not published)"
        fi
    fi
else
    echo "[4/4] Skipping crate yanking (use --yank-crates to enable)"
fi

echo
echo "✅ Rollback complete!"
echo
echo "Next steps:"
echo "  • Verify the tag and release are gone: gh release list --repo freenet/freenet-core"
echo "  • Check crates.io: https://crates.io/crates/freenet"
if [[ "$YANK_CRATES" == "false" ]]; then
    echo "  • To yank crates, run: $0 --version $VERSION --yank-crates"
fi
