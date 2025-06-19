#!/bin/bash
set -e

cd /home/ian/code/freenet/freenet-stdlib

# Commit the changes
git add -A
git commit -m "feat: add NodeQuery, SubscriptionInfo, and NetworkDebugInfo APIs

- Add NodeQuery enum with ConnectedPeers and SubscriptionInfo variants
- Add SubscriptionInfo struct for tracking subscriptions
- Add NetworkDebugInfo struct for network debugging
- Update QueryResponse enum to include NetworkDebug variant
- These APIs were present in 0.1.7 but missing from main branch
- Required by freenet-core for network debugging functionality"

git push origin main