#!/bin/bash
# Validates that current directory is not the main worktree
# Usage: ./scripts/validate-worktree.sh

set -e

CURRENT_DIR=$(pwd)
CURRENT_BRANCH=$(git branch --show-current 2>/dev/null || echo "unknown")

# Check if we're in the main worktree directory
if [[ "$CURRENT_DIR" == */freenet-core/main ]]; then
    echo "❌ ERROR: You are in the main worktree!"
    echo ""
    echo "Current directory: $CURRENT_DIR"
    echo "Current branch: $CURRENT_BRANCH"
    echo ""
    echo "The main worktree should stay on 'main' branch for reference."
    echo "Multiple agents working in main will conflict and corrupt branches."
    echo ""
    echo "To fix this:"
    echo "  1. Create a worktree for your branch:"
    echo "     cd ~/code/freenet/freenet-core/main"
    echo "     git worktree add ../fix-<issue-number> <your-branch-name>"
    echo ""
    echo "  2. Switch to the new worktree:"
    echo "     cd ../fix-<issue-number>"
    echo ""
    echo "  3. Continue your work there"
    echo ""
    exit 1
fi

# Verify we're in a freenet-core worktree (but not main)
if [[ "$CURRENT_DIR" == */freenet-core/* ]] && [[ "$CURRENT_DIR" != */freenet-core/main ]]; then
    echo "✅ OK: Working in separate worktree"
    echo "   Directory: $CURRENT_DIR"
    echo "   Branch: $CURRENT_BRANCH"
    exit 0
fi

# If we're somewhere else entirely, warn but don't fail
echo "⚠️  WARNING: Not in a freenet-core directory"
echo "   Current: $CURRENT_DIR"
echo "   Expected: ~/code/freenet/freenet-core/<branch-name>"
exit 0
