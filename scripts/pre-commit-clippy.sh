#!/bin/bash
# Pre-commit hook that runs cargo clippy only on packages with changed files
# This avoids running clippy on the entire workspace which can be slow and
# may fail due to issues in unrelated packages.

set -e

# Get list of changed Rust files from staged changes
changed_files=$(git diff --cached --name-only --diff-filter=ACMR | grep '\.rs$' || true)

if [ -z "$changed_files" ]; then
    echo "No Rust files changed, skipping clippy"
    exit 0
fi

# Extract unique package names from changed files
packages=""
for file in $changed_files; do
    # Find the Cargo.toml for this file by walking up the directory tree
    dir=$(dirname "$file")
    while [ "$dir" != "." ] && [ "$dir" != "/" ]; do
        if [ -f "$dir/Cargo.toml" ]; then
            # Extract package name from Cargo.toml
            pkg_name=$(grep '^name' "$dir/Cargo.toml" | head -1 | sed 's/.*= *"\([^"]*\)".*/\1/')
            if [ -n "$pkg_name" ]; then
                # Add to list if not already present
                if [[ ! " $packages " =~ " $pkg_name " ]]; then
                    packages="$packages $pkg_name"
                fi
            fi
            break
        fi
        dir=$(dirname "$dir")
    done
done

if [ -z "$packages" ]; then
    echo "No packages to check"
    exit 0
fi

echo "Running clippy on packages:$packages"
for pkg in $packages; do
    echo "  Checking $pkg..."
    cargo clippy -p "$pkg" --all-targets -- -D warnings || exit 1
done

echo "Clippy passed for all changed packages"
