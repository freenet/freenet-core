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
    # Only the 'freenet' package has the 'bench' feature needed for benchmarks
    # (benchmarks use test-only modules gated behind #[cfg(feature = "bench")])
    if [ "$pkg" = "freenet" ]; then
        cargo clippy -p "$pkg" --all-targets --features bench -- -D warnings || exit 1
    else
        # Check if this package is in the workspace or standalone
        if cargo metadata --no-deps --format-version=1 2>/dev/null | grep -q "\"name\":\"$pkg\""; then
            cargo clippy -p "$pkg" --all-targets -- -D warnings || exit 1
        else
            # Package is standalone - find and run clippy from its directory
            pkg_dir=$(find tests apps crates -name Cargo.toml -exec grep -l "name = \"$pkg\"" {} \; 2>/dev/null | head -1 | xargs dirname 2>/dev/null)
            if [ -n "$pkg_dir" ] && [ -d "$pkg_dir" ]; then
                echo "    (standalone package at $pkg_dir)"
                (cd "$pkg_dir" && cargo clippy --all-targets -- -D warnings) || exit 1
            else
                echo "    Warning: Could not find package '$pkg', skipping"
            fi
        fi
    fi
done

echo "Clippy passed for all changed packages"
