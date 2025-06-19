#!/bin/bash
set -e

# Add changelog and create tag
cd /home/ian/code/freenet/freenet-stdlib
git add CHANGELOG.md
git commit -m "chore: update changelog for v0.1.9"
git tag -a v0.1.9 -m "Release v0.1.9 - Add missing NodeQuery APIs with panic fix"
git push origin main
git push origin v0.1.9

# Publish to crates.io
cd rust
cargo publish

# Create GitHub release
cd ..
gh release create v0.1.9 --title "v0.1.9 - Add missing NodeQuery APIs with panic fix" --notes "## What's Changed

### 🚀 Features
- Added NodeQuery enum with ConnectedPeers and SubscriptionInfo variants
- Added SubscriptionInfo struct for tracking contract subscriptions  
- Added NetworkDebugInfo struct for network debugging information
- Added QueryResponse::NetworkDebug variant for debugging responses

### 🐛 Bug Fixes (from v0.1.8)
- Fixed panic in \`APIVersion::from_u64()\` when encountering unsupported version numbers
- Now returns proper error instead of panicking

### 📝 Notes
- These APIs were present in published v0.1.7 but missing from main branch
- This release combines the panic fix from v0.1.8 with the restored APIs from v0.1.7
- Required by freenet-core v0.1.13+ for network debugging functionality

**Full Changelog**: https://github.com/freenet/freenet-stdlib/compare/v0.1.8...v0.1.9"