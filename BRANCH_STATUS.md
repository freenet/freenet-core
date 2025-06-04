# Branch and PR Status Overview

## Current Branch
- **Branch**: replace-network-topology-tests
- **Uncommitted Changes**: 
  - crates/core/src/transport/connection_handler.rs (transport channel fixes)
  - apps/freenet-ping/Cargo.lock

## Open PRs
- **PR #1622**: fix-issue-1616-update-propagation → debug-update-issues
  - Status: Uses non-main base branch
  - Purpose: Fix update propagation and connection handling
  
- **PR #1612**: debug-update-issues-clean → main
  - Status: Appears to be cleaned up version
  - Purpose: Integration test fixes and connection latency

## Uncommitted Work
Transport layer fixes in connection_handler.rs:
1. Connection health checks
2. try_send with error recovery
3. Increased buffer sizes (1 → 100)
4. Fixed ownership issues

## Issues Being Addressed
- #1616: Update propagation issues
- #1624: Network topology test failures ("channel closed")
- #1623: Flaky subscription tests
- #1614: Integration test failures