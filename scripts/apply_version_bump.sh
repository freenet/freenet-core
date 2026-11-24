#!/usr/bin/env bash
# Rewrite the release version into the manifests of the tree in the current
# directory:
#
#   - crates/core/Cargo.toml   package version -> VERSION
#   - crates/fdev/Cargo.toml   package version -> FDEV_VERSION
#                              `freenet = { path = "../core", version = ... }` -> VERSION
#                              both binstall pkg-url lines -> releases/download/vVERSION/
#
# It does NOT touch the lockfiles; the caller runs `cargo update --workspace`
# and re-resolves apps/freenet-ping afterwards.
#
# The ONE copy of these rewrites for the release workflows. release.yml applies
# it to the ship commit to make the release commit, and release-sync.yml
# applies it to main after the release is published, so the two cannot drift.
# scripts/release.sh (the older local driver) keeps its own BRE copies;
# crates/fdev/tests/binstall_metadata.rs pins that the BRE and ERE forms
# rewrite identically (#3995, #4119).
#
# Every rewrite is verified afterwards: a sed that silently matched nothing is
# how fdev's freenet dependency stayed pinned to an old release (#4119).
#
# Usage: apply_version_bump.sh VERSION FDEV_VERSION
# Tests: scripts/resolve_release_ship_test.sh (on the real manifests), and
# scripts/release_dry_run_test.sh and scripts/release_sync_test.sh (in use).

set -euo pipefail

VERSION="${1:?usage: apply_version_bump.sh VERSION FDEV_VERSION}"
FDEV_VERSION="${2:?usage: apply_version_bump.sh VERSION FDEV_VERSION}"

for v in "$VERSION" "$FDEV_VERSION"; do
    if [[ ! "$v" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "::error::apply_version_bump.sh: '$v' is not X.Y.Z"
        exit 1
    fi
done

CORE=crates/core/Cargo.toml
FDEV=crates/fdev/Cargo.toml
for f in "$CORE" "$FDEV"; do
    [ -f "$f" ] || { echo "::error::apply_version_bump.sh: $f not found in $(pwd)"; exit 1; }
done

sed "s/^version = \".*\"/version = \"$VERSION\"/" "$CORE" > "$CORE.tmp"
mv "$CORE.tmp" "$CORE"

sed "s/^version = \".*\"/version = \"$FDEV_VERSION\"/" "$FDEV" > "$FDEV.tmp"
mv "$FDEV.tmp" "$FDEV"

# Anchor on the version field, NOT a trailing ` }`. The line in
# crates/fdev/Cargo.toml carries a `, features = [...]` suffix, so a pattern
# that requires the closing brace silently no-ops, leaving fdev's freenet-dep
# version pinned to the previous release (#4119).
sed -E "s/(freenet = \\{ path = \"\\.\\.\\/core\", version = )\"[^\"]*\"/\\1\"$VERSION\"/" "$FDEV" > "$FDEV.tmp"
mv "$FDEV.tmp" "$FDEV"

# fdev's crate version is independent of the freenet release tag, so
# `[package.metadata.binstall].pkg-url` embeds the freenet version literally
# (`v0.2.X`) rather than `v{ version }`. Re-point both the default and the
# Windows override to the new release tag so `cargo binstall fdev` resolves to
# the actual GitHub release assets (#3995). The `/g` is load-bearing.
sed -E "s|releases/download/v[0-9]+\.[0-9]+\.[0-9]+/fdev-|releases/download/v${VERSION}/fdev-|g" "$FDEV" > "$FDEV.tmp"
mv "$FDEV.tmp" "$FDEV"

# --- verify -------------------------------------------------------------------
package_version() {
    awk '/^\[package\]/ {p = 1; next} /^\[/ {p = 0}
         p && /^version *=/ {gsub(/.*= *"|".*/, ""); print; exit}' "$1"
}
FAILED=0
if [ "$(package_version "$CORE")" != "$VERSION" ]; then
    echo "::error::$CORE does not declare version $VERSION after the rewrite."
    FAILED=1
fi
if [ "$(package_version "$FDEV")" != "$FDEV_VERSION" ]; then
    echo "::error::$FDEV does not declare version $FDEV_VERSION after the rewrite."
    FAILED=1
fi
if ! grep -qE "^freenet = \{ path = \"\.\./core\", version = \"$VERSION\"" "$FDEV"; then
    echo "::error::fdev's freenet dependency was not rewritten to $VERSION (#4119)."
    FAILED=1
fi
URLS=$(grep -c "releases/download/v$VERSION/fdev-" "$FDEV" || true)
if [ "$URLS" -ne 2 ]; then
    echo "::error::Expected 2 fdev binstall pkg-url lines pointing at v$VERSION, found $URLS (#3995)."
    FAILED=1
fi
[ "$FAILED" -eq 0 ] || exit 1
echo "✅ Manifests bumped: freenet $VERSION, fdev $FDEV_VERSION"
