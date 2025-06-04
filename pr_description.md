# Fix: Ensure connection callbacks are always notified

## Problem

When establishing connections between peers, callbacks could be dropped without notification in several scenarios, leading to "channel closed" errors when operations waited for responses. The root issue was that callbacks stored in `awaiting_connection` were only removed on specific success/failure paths, but not in many error scenarios.

## Solution

This PR adds comprehensive callback cleanup to ensure all connection callbacks are notified with either success or failure:

### 1. Added cleanup helper method
- `cleanup_awaiting_connection` ensures callbacks are notified and removed
- Also cleans up transaction tracking

### 2. Fixed timeout handling in `handle_connect_peer`
- Callbacks are now cleaned up when `establish_conn` times out (10s timeout)
- Callbacks are cleaned up when `establish_conn` returns an error

### 3. Added cleanup on peer disconnection
- When `TransportError::ConnectionClosed` occurs
- When explicitly dropping a connection via `NodeEvent::DropConnection`

### 4. Transaction timeout handling
- Added `connection_tx` HashMap to track transaction IDs for each connection attempt
- Callbacks are cleaned up when `NodeEvent::TransactionTimedOut` is received

### 5. Handshake handler notifications
- Modified `start_outbound_connection` to push failure events instead of silently returning
- Now handles "already connected" and "connection in progress" cases properly

## Technical Details

### Files Changed:
- `crates/core/src/node/network_bridge/p2p_protoc.rs` - Main callback management improvements
- `crates/core/src/node/network_bridge/handshake.rs` - Ensures all code paths generate events
- `crates/core/src/node/network_bridge.rs` - Added `AlreadyConnected` and `ConnectionInProgress` error variants

### Key Improvements:
- Callbacks are guaranteed to be notified on all error paths
- Memory leaks from orphaned callbacks are prevented
- Better error messages for connection failures

## Testing

While the connection callback issue is fixed, the ping tests still fail due to a separate transport layer issue where channels are closed during connection establishment. This will be addressed in a follow-up PR.

## Related Issues
- Fixes part of #1616 (connection callback dropping)
- Transport layer issues will be addressed separately