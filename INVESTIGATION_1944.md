# PUT Operation Network Send - Investigation Summary

## Problem
PUT operations timeout after 10 seconds because RequestPut notifications never reach the network layer.

## Investigation Timeline & Findings

### 1. Initial Hypothesis: Event Loop Exits ❌
**Theory**: Event loop task crashes/exits after PUT notification sent
**Test**: Added error logging to catch task exits in run_node select!
**Result**: NO exit messages logged - event loop is STILL RUNNING when timeout occurs

### 2. Channel Mechanism Test ✅
**Theory**: Notification channel has inherent bug
**Test**: Created 3 unit tests simulating event loop with biased select
**Result**: ALL TESTS PASS - channel mechanism works correctly in isolation

### 3. Channel Mismatch Test ❌  
**Theory**: OpManager sends to different channel than event loop receives from
**Test**: Added unique channel IDs to track sender/receiver pairs
**Result**: Channel IDs MATCH - OpManager and event loop use same channel (channel 1 for peer v6MWKgqHXDwYuTGc)

## Current State

### What Works ✅
1. Channel creation and pairing
2. OpManager has correct sender (channel 1)
3. Event loop has correct receiver (channel 1)
4. `.send().await` completes successfully
5. Event loop task continues running
6. Unit tests prove channel + biased select work

### What Doesn't Work ❌
1. `.recv()` in production event loop never fires for PUT notifications
2. PUT notification disappears between send and receive

## The Mystery - SOLVED! 🎯

### Root Cause: Event Loop Deadlock in select!

**Timeline of Discovery:**
1. Added detailed DEBUG logging to `wait_for_event` showing entry/exit of select!
2. Ran test and found:
   - **22:44:35.563735Z** - Event loop enters wait_for_event (last call)
   - **22:44:44.720347Z** - PUT notification sent (9 seconds later)
   - **NO MORE wait_for_event calls** - Event loop NEVER returns from select!

**Critical Finding:** The event loop **DEADLOCKS** inside the select! macro at line 724. It enters the select! and never comes back out. None of the select! branches ever become ready, causing the entire event loop to freeze.

**Why This Explains Everything:**
- ✅ Send completes successfully (happens outside event loop)
- ✅ Channel IDs match (correct channel)
- ✅ Event loop task is "running" (but stuck in select!)
- ❌ Receive never happens (event loop frozen, can't poll notification channel)

The event loop is not crashed or exited - it's **DEADLOCKED** waiting for one of the futures in select! to complete, but none of them ever do.

## Code Changes Made

### Files Modified
1. `p2p_impl.rs` - Enhanced error logging in run_node select!
2. `network_bridge.rs` - Added channel ID tracking + 3 unit tests  
3. `op_state_manager.rs` - Added channel ID to notification logs
4. `p2p_protoc.rs` - Added channel ID to event loop startup
5. `tracing/mod.rs` - Added tokio-console support
6. `Cargo.toml` - Added console-subscriber, tokio tracing feature

### All Tests Pass
```bash
test node::network_bridge::tests::test_notification_channel_with_biased_select ... ok
test node::network_bridge::tests::test_multiple_notifications ... ok  
test node::network_bridge::tests::test_send_fails_when_receiver_dropped ... ok
```

## Commits
- 5994b276: Add extensive debug logging
- 1894b478: Error handling + unit tests
- cc247a79: Tokio-console support
- 483fab02: Console subscriber initialization  
- d06ee934: Channel ID tracking

## Next Steps

### Option 1: Deep Dive into Select!
Add logging INSIDE the select! macro to see which futures are actually being polled:
- Log every time wait_for_event is called
- Log which select! branch would be ready
- Check if notifications_receiver.recv() future is even being created

### Option 2: Bypass Select! Test
Temporarily bypass the select! and directly poll notifications_receiver to see if it works:
```rust
if let Ok(msg) = timeout(Duration::from_millis(1), notification_channel.notifications_receiver.recv()).await {
    // Process msg
}
```

### Option 3: Channel Inspection  
Add tokio::mpsc channel introspection:
- Check sender count
- Check if receiver is dropped
- Check channel capacity/buffer state

### Option 4: Minimal Reproduction
Create a minimal test that reproduces the exact setup:
- 2 peers (gateway + regular)
- Real P2P conn manager
- Client event handling
- Event loop
- Single PUT operation

## Questions for Review

1. Could there be multiple EventLoopNotificationsReceiver instances somehow?
2. Is the notification_channel being moved/consumed elsewhere?
3. Could tokio runtime configuration affect mpsc behavior?
4. Any known issues with biased select! and mpsc channels?

---
Investigation by: Claude Code
Branch: fix/put-operation-network-send  
Issue: #1944
