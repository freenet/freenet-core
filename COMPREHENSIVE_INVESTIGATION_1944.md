# Comprehensive Investigation: Issue #1944 - PUT Operations Fail to Send Over Network

**Issue**: https://github.com/freenet/freenet-core/issues/1944
**Investigation Branch**: `fix/put-operation-network-send`
**PR Created**: #1950 (partial fix for deadlock)

---

## Executive Summary

PUT operations timeout after 10 seconds because the event loop experiences a deadlock, preventing RequestPut notifications from being received and processed. The investigation revealed **two distinct but related issues**:

1. **Complete Deadlock** (FIXED in PR #1950): Handshake handler blocks event loop indefinitely
2. **Waker Registration Failure** (UNSOLVED): Even with timeout fix, notification channel never receives messages after handshakes complete

---

## Problem Statement

### Symptoms
- PUT operations fail to send messages over the network
- Timeout occurs after 10 seconds
- GET operations work correctly (same code pattern)
- Issue occurs in River chat application ubertest

### What Works ✅
- Client request received
- Optimal target peer selected
- `RequestPut` message created
- `notify_op_change()` called
- `.send().await` completes successfully

### What Fails ❌
- RequestPut notification never reaches `process_message()`
- `SeekNode` message never created
- PUT never goes out over network

---

## Investigation Timeline

### Phase 1: Initial Hypothesis - Event Loop Exits

**Theory**: Event loop task crashes or exits after PUT notification sent

**Test**: Added error logging to catch task exits in `run_node` select!
```rust
tokio::select!(
    r = f => {
       let Err(e) = r;
       tracing::error!("Network event listener exited: {}", e);
       Err(e)
    }
    // ... etc
)
```

**Result**: ❌ **NO exit messages logged** - event loop is STILL RUNNING when timeout occurs

**Conclusion**: Event loop doesn't exit, crash, or get cancelled

---

### Phase 2: Channel Mechanism Verification

**Theory**: Notification channel has inherent bug

**Test**: Created 3 comprehensive unit tests simulating event loop with biased select
- `test_notification_channel_with_biased_select`
- `test_multiple_notifications`
- `test_send_fails_when_receiver_dropped`

**Result**: ✅ **ALL TESTS PASS** - channel mechanism works correctly in isolation

**Conclusion**: Channel implementation is sound

---

### Phase 3: Channel Mismatch Investigation

**Theory**: OpManager sends to different channel than event loop receives from

**Test**: Added unique channel IDs to track sender/receiver pairs
```rust
pub(crate) struct EventLoopNotificationsReceiver {
    pub(crate) notifications_receiver: Receiver<Either<NetMessage, NodeEvent>>,
    pub(crate) op_execution_receiver: Receiver<(Sender<NetMessage>, NetMessage)>,
    pub(crate) channel_id: u64,  // ← Added
}
```

**Result**: ✅ **Channel IDs MATCH** - OpManager and event loop use same channel (channel 1 for peer v6MWKgqHXDwYuTGc)

**Evidence from logs**:
```
Channel 1 created for peer v6MWKgqHXDwYuTGc
↓
Event loop opens, receiving from channel 1
↓
PUT notification sent to channel 1
```

**Conclusion**: Not a channel mismatch issue

---

### Phase 4: BREAKTHROUGH - Event Loop Deadlock Discovery

**Test**: Added detailed DEBUG logging to `wait_for_event` showing entry/exit

**Critical Finding**: Event loop **DEADLOCKS** inside the select! macro

**Timeline Evidence**:
```
22:44:35.563735Z - Event loop enters wait_for_event (last call)
22:44:44.720347Z - PUT notification sent (9 seconds later)
NO MORE wait_for_event calls - Event loop NEVER returns from select!
```

**Root Cause Identified**: Nested Select! Deadlock

The handshake handler's `wait_for_events()` contains an internal `loop { select! { ... } }`:

```rust
// handshake.rs
pub async fn wait_for_events(&mut self) -> Result<Event, HandshakeError> {
    loop {  // ← Internal loop blocks indefinitely!
        tokio::select! {
            new_conn = self.inbound_conn_handler.next_connection() => { ... }
            outbound_conn = self.ongoing_outbound_connections.next() => { ... }
            unconfirmed = self.unconfirmed_inbound_connections.next() => { ... }
        }
    }
}
```

When used in the main event loop:
```rust
// p2p_protoc.rs
select! {
    biased;
    msg = notifications_receiver.recv() => { ... }
    // ... other branches ...
    handshake_event_res = handshake_handler.wait_for_events() => { ... }  // ← BLOCKS HERE
}
```

**The Deadlock Mechanism**:
1. Main select! reaches handshake branch (checked last with `biased`)
2. Handshake enters internal `loop { select! { ... } }`
3. If all handshake channels empty, inner select! returns Poll::Pending
4. Outer select! parks the task, waiting for ANY branch to wake it
5. **When PUT notification sent, notification channel tries to wake task**
6. **BUT**: Task waker is associated with handshake's inner select!, not notification channel!
7. **Result**: Task never wakes up, deadlock forever

---

### Phase 5: Fix Attempt - Timeout Wrapper (PR #1950)

**Solution**: Wrap handshake branch with 100ms timeout

```rust
handshake_event_res = timeout(Duration::from_millis(100), handshake_handler.wait_for_events()) => {
    match handshake_event_res {
        Ok(Ok(event)) => { /* handle event */ }
        Ok(Err(e)) => { /* handle error */ }
        Err(_timeout) => {
            // No pending events - continue to next iteration
            Ok(EventResult::Continue)
        }
    }
}
```

**Results**:
- ✅ Event loop no longer deadlocks completely
- ✅ `wait_for_event` called regularly (every ~100ms)
- ✅ Event loop processes keep-alive messages
- ✅ Test completes (doesn't freeze forever)
- ❌ **PUT operations still timeout**

**Metrics**:
- 249 "ENTERING select!" logs - event loop running
- 9 "SELECTED" logs - only 9 events processed
- Last SELECTED at 22:49:32.313322Z
- PUT sent at 22:49:41.490418Z (9 seconds later)
- 240 more loop iterations, but 0 new SELECTED events

---

### Phase 6: NEW CRITICAL ISSUE - Waker Registration Failure

**Discovery**: Even with timeout fix preventing complete deadlock, notification channel NEVER receives messages after initial handshakes complete

**The Problem**:
1. Event loop enters select! 240+ times after connections established
2. **ALL branches return Poll::Pending** (not just notifications!)
3. Handshake timeout fires every 100ms, returns Continue
4. Loop repeats - but **NO branch EVER becomes ready**
5. Messages sent to channels don't wake the task

**This is NOT a deadlock** - it's a **systemic waker registration failure**

---

### Phase 7: Testing Hypothesis - Is `biased` the Culprit?

**Theory**: `biased` select + nested blocking handshake prevents waker registration for earlier branches

**How `biased` works**:
```rust
select! {
    biased;  // Check branches SEQUENTIALLY
    branch_1 = fut1 => { }  // Check first
    branch_2 = fut2 => { }  // Check if fut1 not ready
    branch_N = futN => { }  // Check last - PROBLEM if it blocks!
}
```

**Test**: Temporarily removed `biased` from main select!

**Result**: ❌ **IDENTICAL BEHAVIOR**
- 249 ENTERING logs (same)
- 9 SELECTED logs (same)
- PUT still times out (same)
- No branches become ready after handshakes (same)

**Conclusion**: `biased` is NOT the root cause!

---

## Current State of Knowledge

### What We Know For Certain ✅

1. **Channel mechanism works** - Unit tests pass
2. **Channels match** - Verified with channel IDs
3. **Event loop runs** - Doesn't exit, verified with error logging
4. **Send completes** - `.send().await` succeeds
5. **Timeout fix prevents complete deadlock** - Event loop continues looping
6. **`biased` is not the cause** - Same behavior without it

### The Unsolved Mystery ❌

After initial handshakes (~9 events), **ALL select! branches stop becoming ready**:
- ❌ `notifications_receiver.recv()` never ready (even when messages sent)
- ❌ `op_execution_receiver.recv()` never ready
- ❌ `peer_connections.next()` never ready
- ❌ `conn_bridge_rx.recv()` never ready
- ✅ Only handshake timeout fires (every 100ms)

**Critical Observation**: This affects ALL channels, not just notifications! This suggests a systemic issue with the event loop task or tokio runtime, not individual channel problems.

---

## Hypotheses to Test

### 1. Channel Senders Dropped
Maybe all senders are being dropped after handshakes complete, closing the channels.

**How to test**: Add logging to check `sender.is_closed()` or check channel state

### 2. Event Loop Task Cancelled/Suspended
Maybe tokio runtime suspends the task after some timeout or resource limit.

**How to test**: Use tokio-console to watch task state transitions

### 3. Receiver Moved/Consumed
Maybe the receivers are being moved out of their structs, leaving invalid references.

**How to test**: Add assertions to verify receiver validity

### 4. Tokio Runtime Issue
Maybe multi-threaded runtime has a bug with mpsc channels and nested select!

**How to test**: Try single-threaded runtime or different tokio version

### 5. Select! Future State Corruption
Maybe the timeout wrapper corrupts the select! future state, preventing proper waker registration.

**How to test**: Remove timeout wrapper and try different anti-deadlock approach

---

## Code Changes Made

### Files Modified

1. **p2p_impl.rs** (lines 193-207)
   - Added error logging for task exits
   - Shows "Network event listener exited", "Client events task exited", etc.

2. **network_bridge.rs**
   - Added channel ID tracking (lines 90-116)
   - Added 3 unit tests (lines 139-263)
   - All tests pass ✅

3. **op_state_manager.rs** (lines 146-181)
   - Added channel_id to all notify_op_change logs

4. **p2p_protoc.rs** (lines 715-800)
   - Added DEBUG logging showing select! entry/branches selected
   - Added timeout wrapper to handshake branch (PR #1950)

5. **handshake.rs** (lines 275-281)
   - Added `has_pending_operations()` method

6. **tracing/mod.rs** (lines 1264-1276)
   - Added console subscriber initialization

7. **Cargo.toml**
   - Added `console-subscriber` optional dependency
   - Enabled tokio `tracing` feature

### Test Results

All unit tests pass:
```
test node::network_bridge::tests::test_notification_channel_with_biased_select ... ok
test node::network_bridge::tests::test_multiple_notifications ... ok
test node::network_bridge::tests::test_send_fails_when_receiver_dropped ... ok
```

Integration test still fails:
```
test test_basic_room_creation ... FAILED
Error: Timeout waiting for PUT response after 10 seconds
```

---

## Solutions Implemented

### Partial Fix: PR #1950 (Timeout Wrapper)

**Status**: Merged/Open
**Impact**: Prevents complete deadlock, but doesn't fix PUT timeout

**Implementation**:
```rust
handshake_event_res = timeout(Duration::from_millis(100), handshake_handler.wait_for_events()) => {
    match handshake_event_res {
        Ok(Ok(event)) => { /* handle */ }
        Ok(Err(e)) => { /* handle error */ }
        Err(_timeout) => Ok(EventResult::Continue)  // Allow loop to continue
    }
}
```

**Benefits**:
- ✅ Prevents complete event loop deadlock
- ✅ Minimal, safe change (16 lines)
- ✅ Suitable for backporting
- ✅ Allows system to remain responsive

**Limitations**:
- ❌ Doesn't fix PUT timeout issue
- ⚠️ Adds 100ms latency to handshake processing
- ⚠️ Doesn't address root cause

---

## Proposed Long-Term Solutions

### Option 1: Refactor HandshakeHandler (Recommended)

Remove internal loop from `wait_for_events()` and let outer loop control iteration:

**Current (Wrong)**:
```rust
pub async fn wait_for_events() -> Result<Event, HandshakeError> {
    loop {  // ← Remove this!
        select! {
            // Process events
        }
    }
}
```

**Proposed (Correct)**:
```rust
pub async fn wait_for_events() -> Result<Event, HandshakeError> {
    select! {
        // Return after ONE event
    }
}
```

**Benefits**:
- ✅ No nested select! blocking
- ✅ Proper async design (functions return promptly)
- ✅ Fixes root cause
- ✅ Better performance

**Complexity**: Medium - requires refactoring handshake handler logic

---

### Option 2: Flatten Handshake Futures

Expose handshake's internal futures directly in main select!:

```rust
select! {
    biased;
    msg = notification_channel.notifications_receiver.recv() => { ... }
    // Flatten handshake futures - no nesting!
    new_conn = handshake_handler.inbound_conn_handler.next_connection() => { ... }
    outbound = handshake_handler.ongoing_outbound_connections.next() => { ... }
    unconfirmed = handshake_handler.unconfirmed_inbound_connections.next() => { ... }
}
```

**Benefits**:
- ✅ Eliminates nested select! entirely
- ✅ All futures at same level
- ✅ Better performance

**Complexity**: High - breaks handshake handler encapsulation

---

### Option 3: Investigate Systemic Waker Issue

Focus on why ALL channels stop receiving after handshakes:

**Investigation steps**:
1. Add channel state inspection (sender count, closed status)
2. Use tokio-console to monitor task lifecycle
3. Check if tokio runtime has known bugs with this pattern
4. Try single-threaded runtime to eliminate concurrency bugs
5. Add instrumentation to see if wakers are being dropped

**Benefits**:
- ✅ Addresses the deeper issue
- ✅ May reveal tokio bug or misuse

**Complexity**: High - requires deep async runtime knowledge

---

## Diagnostic Tools Available

### 1. Tokio Console

Monitor tasks in real-time:
```bash
# Terminal 1
tokio-console

# Terminal 2
RUSTFLAGS="--cfg tokio_unstable" cargo build --features console-subscriber --test ubertest
RUSTFLAGS="--cfg tokio_unstable" TOKIO_CONSOLE=1 cargo test --features console-subscriber --test ubertest
```

Shows:
- Task state (running/sleeping/blocked)
- Resource usage
- Task lifecycle

### 2. Enhanced Debug Logging

Added comprehensive logging at DEBUG level:
- Channel ID tracking
- Select! entry/exit
- Branch selection
- Message send/receive

### 3. Unit Tests

Three passing tests proving channel mechanism works:
- `test_notification_channel_with_biased_select`
- `test_multiple_notifications`
- `test_send_fails_when_receiver_dropped`

---

## Next Steps

### Immediate (High Priority)

1. **Run tokio-console while test runs**
   - Watch event loop task state when PUT is sent
   - Identify if task is blocked/sleeping/cancelled

2. **Add channel state logging**
   - Check if senders are dropped (channel closed)
   - Verify receiver is still valid

3. **Test without timeout wrapper**
   - See if timeout is causing waker corruption
   - Try different anti-deadlock approach

### Short-Term (Medium Priority)

4. **Implement Option 1** (Remove internal loop from wait_for_events)
   - Proper architectural fix
   - Addresses nested select! issue

5. **Test with single-threaded runtime**
   - Eliminate concurrency as variable
   - May help identify race conditions

### Long-Term (Low Priority)

6. **Implement Option 2** (Flatten handshake futures)
   - More invasive but cleaner
   - Better performance

7. **Report to tokio project** if tokio bug found
   - Create minimal reproduction
   - Submit issue with details

---

## Summary

**What we fixed**: Complete event loop deadlock (PR #1950)

**What remains broken**: PUT operations still timeout due to systemic waker registration failure affecting ALL channels after initial handshakes

**Key insight**: The problem is NOT `biased` select, NOT channel mismatch, NOT event loop exiting - it's that ALL select! futures stop becoming ready after ~9 events, suggesting either:
- Tokio runtime issue
- Channel senders all dropped
- Select! future state corruption from timeout wrapper
- Undiscovered race condition

**Recommended next action**: Use tokio-console to watch task state in real-time to identify why ALL channels stop receiving after handshakes.

---

## References

- **Issue**: https://github.com/freenet/freenet-core/issues/1944
- **PR**: https://github.com/freenet/freenet-core/pull/1950
- **Investigation Branch**: `fix/put-operation-network-send`
- **Files**: `INVESTIGATION_1944.md`, `SOLUTION_PROPOSAL.md`

---

**Investigation by**: Claude Code with @iduartgomez
**Last Updated**: 2025-10-14
