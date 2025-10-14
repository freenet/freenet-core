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

### Deeper Analysis: Nested Select! Deadlock

**The Smoking Gun:**
Looking at the select! branches in `wait_for_event` (p2p_protoc.rs:724), one branch is:
```rust
handshake_event_res = handshake_handler.wait_for_events() => { ... }
```

The `handshake_handler.wait_for_events()` function (handshake.rs:275) contains its OWN internal `select!` loop:
```rust
pub async fn wait_for_events(&mut self) -> Result<Event, HandshakeError> {
    loop {
        tokio::select! {
            new_conn = self.inbound_conn_handler.next_connection() => { ... }
            outbound_conn = self.ongoing_outbound_connections.next(), if !... => { ... }
            unconfirmed_inbound_conn = self.unconfirmed_inbound_connections.next(), if !... => { ... }
        }
    }
}
```

**The Deadlock Mechanism:**
1. Outer select! polls `handshake_handler.wait_for_events()` as one of its branches
2. Inner select! inside `wait_for_events()` waits for handshake events
3. If ALL handshake channels are empty (no pending connections/handshakes), inner select! returns Poll::Pending
4. This makes the outer select! branch return Poll::Pending
5. Outer select! checks other branches, but they're also Pending
6. **Outer select! parks the task, waiting for ANY branch to wake it up**
7. When PUT notification is sent, notification channel tries to wake the task
8. **BUT**: Task waker is associated with handshake handler's inner select!, not the notification channel!
9. Result: Task never wakes up, deadlock forever

**The Fundamental Issue:**
The handshake handler has a blocking API (`wait_for_events()`) that can wait indefinitely when there are no pending operations. This starves all other event sources in the main event loop.

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

## Solution: Fix Nested Select! Deadlock

### Option 1: Restructure HandshakeHandler API (Recommended)
Change `wait_for_events()` from blocking to non-blocking by splitting it into separate futures:

```rust
// Instead of one blocking wait_for_events(), expose individual futures:
pub fn poll_inbound(&mut self) -> impl Future<Output = ...> { ... }
pub fn poll_outbound(&mut self) -> impl Future<Output = ...> { ... }
pub fn poll_unconfirmed(&mut self) -> impl Future<Output = ...> { ... }
```

Then in the main select!:
```rust
select! {
    biased;
    msg = notification_channel.notifications_receiver.recv() => { ... }
    // ... other branches ...
    new_conn = handshake_handler.inbound_conn_handler.next_connection() => { ... }
    outbound_conn = handshake_handler.ongoing_outbound_connections.next(), if !... => { ... }
    // etc - flatten the handshake futures into main select!
}
```

### Option 2: Add Timeout to Handshake (Quick Fix)
Wrap the handshake branch with a timeout:

```rust
handshake_event_res = timeout(Duration::from_millis(100), handshake_handler.wait_for_events()) => {
    match handshake_event_res {
        Ok(Ok(event)) => { /* handle event */ }
        Ok(Err(e)) => { /* handle error */ }
        Err(_timeout) => {
            // Timeout - continue to next select! iteration
            // This allows other branches to be checked
            continue;
        }
    }
}
```

This ensures the handshake branch doesn't block forever, allowing the notification channel to be polled regularly.

### Option 3: Make Handshake Optional Based on Activity
Only poll handshake when there's actual work to do:

```rust
select! {
    biased;
    msg = notification_channel.notifications_receiver.recv() => { ... }
    // ... other branches ...
    handshake_event_res = handshake_handler.wait_for_events(), if handshake_handler.has_pending_work() => { ... }
}
```

Add `has_pending_work()` method that returns true only if there are pending connections/handshakes.

## Recommended Approach

**Use Option 2 (Timeout) first** as an immediate fix to unblock development, then **refactor to Option 1** (Restructure API) for a proper long-term solution.

The timeout approach is minimal, safe, and immediately fixes the deadlock while we work on the proper architectural fix.

## Testing Results: Timeout Fix Partially Successful

### What the Timeout Fix Accomplished ✅
- Event loop no longer deadlocks completely
- `wait_for_event` is now called regularly (every ~100ms)
- Handshake branch times out correctly, allowing loop to continue
- 249 "ENTERING" logs vs 9 "SELECTED" logs - proves loop is running

### What Still Doesn't Work ❌
**NEW CRITICAL ISSUE**: Notification channel receiver NEVER wakes up!

Timeline from test:
- `22:49:41.490418Z` - PUT notification sent successfully to channel 1
- `22:49:32.313322Z` - Last "SELECTED" log (9 seconds BEFORE PUT!)
- After this point: 240 more "ENTERING" logs, but 0 "SELECTED" logs

**The Problem:**
1. Event loop enters select! 240 times after connections established
2. ALL branches return Poll::Pending (including notifications_receiver.recv())
3. Handshake timeout fires after 100ms, returns Continue
4. Loop repeats - but notification channel NEVER becomes ready
5. Even though messages are sent to the channel, `.recv()` never wakes up

**This is NOT a deadlock** - it's a **waker registration failure**. The notification receiver's waker is not being triggered when messages are sent to the channel.

### Possible Root Causes
1. **Receiver moved/cloned incorrectly**: EventLoopNotificationsReceiver might be cloned somewhere, creating multiple receivers
2. **Waker not registered**: The `.recv()` future might not be properly registering its waker with the channel
3. **Channel closed**: The sender might have been dropped, closing the channel
4. **Tokio bug**: Edge case in tokio::mpsc with biased select! and timeouts
5. **Receiver consumed**: The receiver might be moved out of the struct between iterations

## The REAL Root Cause: Waker Registration Failure 🎯🎯🎯

### Critical Discovery - 2025-10-14

After implementing detailed logging and manual polling experiments, we've identified the **true root cause**:

**The notification channel receiver's waker is NOT being registered/triggered when the event loop is already blocked in select!**

### Evidence from Latest Testing

#### Test Run: Regular Mode with Detailed Logging
Timeline:
- `09:57:19.007109Z` - Last "ENTERING select!" log for peer v6MWKgqJE4HoFnZW
- `09:57:19.058305Z` - PUT notification **sent successfully** (51ms later)
- **ZERO "SELECTED" logs after this point** - notification never received

**Key Finding**: The event loop entered select! BEFORE the PUT was sent (at 09:57:19.007), then 51ms later the PUT notification was sent. The notification channel tried to wake the task, but **the waker was never triggered**.

All select! branches (including `notifications_receiver.recv()`) returned Poll::Pending and stayed that way forever, despite messages being successfully sent to the channel.

#### Test Run: Manual Polling Mode
Created a manual polling implementation that explicitly polls each future:
- Used `Box::pin()` + noop waker to manually poll `notifications_receiver.recv()`
- Result: **332,936 iterations** in 25 seconds
- Only **1** notification ever became ready (iteration 1)
- All subsequent polls returned Poll::Pending

### The Waker Registration Problem

When `tokio::select!` polls futures:

1. Each future (including `notifications_receiver.recv()`) must register a waker
2. When data arrives in the channel, the sender calls `waker.wake()` to notify the task
3. The task gets scheduled, select! polls again, and the ready future is selected

**What's Going Wrong**:
- The notification receiver IS being polled
- The waker IS being registered (initially)
- But when the sender tries to wake the task, **the waker is not firing**
- This could be due to:
  - Waker being replaced by another future's waker
  - Waker reference being dropped/invalidated
  - Race condition in waker registration
  - Tokio bug with nested select! + timeouts

### Why the Timeout "Fix" Doesn't Work

The timeout wrapper (PR #1950) prevents complete deadlock by forcing the loop to continue every 100ms. However:

**The timeout doesn't fix the underlying waker issue** - it just masks it by creating a busy-wait loop. The notification channel's waker is still not being triggered, so the only way messages get processed is if they arrive EXACTLY when select! is polling (which is why we see a few SELECTED logs early on).

### Hypothesis: Nested Select! + Timeout Breaks Waker Chain

The combination of:
1. Outer select! with `biased`
2. Inner select! in `handshake_handler.wait_for_events()`
3. `timeout()` wrapper around handshake branch

...may be causing Tokio's waker registration to fail. The timeout creates its own future that wraps the handshake future, which contains another select!. This nesting might be confusing Tokio's waker bookkeeping.

### Next Steps to Investigate

1. **Test without timeout**: Remove the timeout wrapper and see if wakers work correctly when there ARE pending handshake events
2. **Test without handshake**: Comment out the handshake branch entirely and see if notification channel works
3. **Test with FuturesUnordered**: Replace nested select! with FuturesUnordered to avoid nesting
4. **Check Tokio version**: Look for known bugs with nested select! or waker registration issues

### Why Unit Tests Pass But Production Fails

The unit tests work because they don't have:
- Nested select! loops
- Multiple competing futures
- Timeout wrappers
- Complex waker interaction patterns

They test the channel in isolation, which works fine. The bug only manifests when the channel is used inside a complex select! with multiple nested async boundaries.

## Questions for Review

1. Could there be multiple EventLoopNotificationsReceiver instances somehow?
2. Is the notification_channel being moved/consumed elsewhere?
3. Could tokio runtime configuration affect mpsc behavior?
4. Any known issues with biased select! and mpsc channels?
5. **NEW**: Does Tokio have known issues with waker registration in nested select! with timeouts?
6. **NEW**: Can we simplify the select! structure to avoid nesting?

---
Investigation by: Claude Code
Branch: fix/put-operation-network-send
Issue: #1944
Last Updated: 2025-10-14
