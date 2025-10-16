# Ubertest Debugging Session - Event Loop Stops Before PUT Request

**Date**: 2025-10-16
**Test**: `test_basic_room_creation` in `crates/core/tests/ubertest.rs`
**Status**: FAILING - PUT operation times out after 10 seconds

## Executive Summary

The `test_basic_room_creation` test fails because **the event loop stops running before it can process the PUT request**. The `notify_op_change` successfully sends the notification to the channel, but the event loop has already exited or deadlocked ~8 seconds earlier, so the notification is never received.

## What Works ✅

1. **priority_select waker registration** - Fixed in previous session with BoxFuture fields using pin-project
2. **Comprehensive unit tests** - Added 8 tests for PrioritySelectFuture (including 1700-message stress test)
3. **PUT request creation** - `request_put()` executes successfully
4. **notify_op_change** - Successfully pushes operation and sends notification to channel
5. **Channel send operation** - Notification is sent without errors

## What Fails ❌

1. **Event loop lifecycle** - The event loop for the peer stops running prematurely
2. **PUT timeout** - Client times out after 10 seconds waiting for response
3. **Message never processed** - The PUT notification sits in the channel unread

## Detailed Timeline (Example Run)

### Transaction ID: `01K7PN10WGDFF6CPAWHN1KBB01`
### Peer: `v6MWKgqJYypQCUd6`

```
13:58:11.517562 - Event loop: wait_for_event called, using priority select
13:58:11.517693 - PrioritySelect: notifications_receiver READY, msg_present=true
13:58:11.533306 - PrioritySelect: notifications_receiver READY, msg_present=true
13:58:11.533454 - PrioritySelect: conn_bridge_rx READY
13:58:11.536870 - PrioritySelect: peer_connections READY, num_connections=0
13:58:12.139726 - PrioritySelect: peer_connections READY, num_connections=0
                  ⚠️  LAST EVENT LOOP POLL - NO MORE ACTIVITY AFTER THIS

13:58:20.816992 - PUT: Creating RequestPut message to target peer
13:58:20.817001 - PUT: Calling notify_op_change to send RequestPut
13:58:20.817010 - notify_op_change: Pushing operation and sending notification
13:58:20.817031 - notify_op_change: Operation pushed, sending to event listener
13:58:20.817041 - notify_op_change: Notification sent successfully ✓
13:58:20.817048 - request_put: notify_op_change succeeded ✓
                  ❌ BUT EVENT LOOP IS NOT RUNNING - NOTIFICATION SITS UNREAD

13:58:30.820260 - Timeout - Connection handlers close
                  Error: Timeout waiting for PUT response after 10 seconds
```

**Gap**: 8-9 seconds between last event loop poll (13:58:12) and PUT notification (13:58:20)

## Evidence

### 1. Notification Was Sent Successfully

```rust
// From logs with enhanced debugging (crates/core/src/node/op_state_manager.rs)
[INFO] notify_op_change: Pushing operation and sending notification
       tx=01K7PN10WGDFF6CPAWHN1KBB01
       peer=v6MWKgqJYypQCUd6

[INFO] notify_op_change: Operation pushed, sending to event listener
       tx=01K7PN10WGDFF6CPAWHN1KBB01

[INFO] notify_op_change: Notification sent successfully
       tx=01K7PN10WGDFF6CPAWHN1KBB01
```

### 2. Event Loop Stopped Running

```bash
# Last PrioritySelect activity for peer v6MWKgqJYypQCUd6
$ grep -E "PrioritySelect.*v6MWKgqJYypQCUd6" /tmp/ubertest_output.log | tail -1
13:58:12.139726 PrioritySelect: peer_connections READY, num_connections=0

# No activity after 13:58:12 despite notification sent at 13:58:20
$ grep "v6MWKgqJYypQCUd6" /tmp/ubertest_output.log | \
  awk '/13:58:12/,/13:58:25/' | wc -l
# Returns very few lines - only context from other spans

# No "Event loop still running" logs (prints every 100 iterations)
$ grep "Event loop still running.*v6MWKgqJYypQCUd6" /tmp/ubertest_output.log
# No output - loop never reached 100 iterations
```

### 3. Test Setup

```rust
// crates/core/tests/ubertest.rs:257
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_basic_room_creation() -> anyhow::Result<()> {
    // 1 gateway + 1 peer setup
    // Nodes bootstrap for 25 seconds
    // Then riverctl creates room via PUT operation
    // PUT times out after 10 seconds
}
```

## Architecture Context

### Event Loop Flow

```
network_event_listener (p2p_protoc.rs:211)
  └─> loop {
        └─> wait_for_event()  // Uses PrioritySelectFuture
              └─> PrioritySelectFuture::new()
                    ├─> notifications_receiver  (priority 1 - highest)
                    ├─> op_execution_receiver   (priority 2)
                    ├─> peer_connections        (priority 3)
                    ├─> conn_bridge_rx          (priority 4)
                    ├─> handshake_handler       (priority 5)
                    ├─> node_controller_rx      (priority 6)
                    ├─> client_transaction_rx   (priority 7)
                    └─> executor_transaction_rx (priority 8 - lowest)
      }
```

### PUT Request Flow

```
Client (riverctl)
  └─> WebSocket → freenet gateway
       └─> client_events::process_client_request()
            └─> request_put() [operations/put.rs:942]
                 └─> notify_op_change() [op_state_manager.rs:146]
                      ├─> push operation to stack
                      └─> send to notifications_sender
                           └─> notifications_receiver (in event loop)
                                └─> ❌ NEVER RECEIVED - event loop stopped
```

## Debugging Changes In Place

### Enhanced Logging Added

1. **crates/core/src/node/network_bridge/p2p_protoc.rs**
   - Loop iteration counter (logs every 100 iterations)
   - `wait_for_event` entry/exit tracing
   - Message routing details with transaction IDs
   - Target peer information

2. **crates/core/src/node/op_state_manager.rs**
   - `notify_op_change`: Changed DEBUG → INFO level
   - Three log points: start, after push, after send

3. **crates/core/src/operations/put.rs**
   - `request_put`: Enhanced logging at key points
   - Transaction ID tracking through entire flow
   - Target peer and routing information

### Files Modified (not committed)

```bash
$ git status
Changes not staged for commit:
  modified:   crates/core/src/node/network_bridge/p2p_protoc.rs
  modified:   crates/core/src/node/op_state_manager.rs
  modified:   crates/core/src/operations/put.rs
```

## Hypotheses for Why Event Loop Stops

### Hypothesis 1: Early Return or Error
The event loop hits an error condition and returns early from the loop.

**Evidence Needed**:
- Check for error logs between 13:58:12 and 13:58:20
- Look for early returns in `network_event_listener`
- Check if `.await?` propagates an error up

**Investigation**:
```bash
# Check for errors for this peer
$ awk '/v6MWKgqJYypQCUd6/,/13:58:20/' /tmp/ubertest_output.log | \
  grep -E "(error|Error|ERROR)" | tail -10
# Result: No errors found for this peer in that timeframe
```

### Hypothesis 2: Deadlock or Hang
The event loop is waiting on something that never completes.

**Evidence Needed**:
- Check what the last event processed was (peer_connections at 13:58:12)
- See if processing peer_connections can block indefinitely
- Check if there's a `.await` that never resolves

**Clues**:
- Last event: `PrioritySelect: peer_connections READY, num_connections=0`
- This suggests the FuturesUnordered for peer connections returned
- Need to trace what happens after peer_connections event is handled

### Hypothesis 3: Task Cancellation
The event loop task gets cancelled by the tokio runtime or parent task.

**Evidence Needed**:
- Check if node shutdown is initiated
- Look for task cancellation logs
- See if there's a timeout that kills the event loop

**Evidence Against**:
- Other peer's event loops continue running (gateway peer still active)
- No shutdown logs found

### Hypothesis 4: Race Condition
The event loop completes its setup phase and is waiting, but something about the timing causes it to miss wakeups.

**Evidence Needed**:
- Check if this is related to the bootstrap phase
- Test happens at 25 seconds after start (nodes bootstrapping)
- Maybe event loop thinks it has no work and exits?

## Next Steps for Investigation

### 1. Add More Detailed Logging in Event Loop

Add logging around the main loop in `network_event_listener`:

```rust
// p2p_protoc.rs - around line 214
loop {
    loop_count += 1;
    if loop_count % 10 == 0 {  // Log every 10 iterations instead of 100
        tracing::info!(
            peer = %self.bridge.op_manager.ring.connection_manager.pub_key,
            loop_count,
            "Event loop iteration checkpoint"
        );
    }

    tracing::trace!(
        peer = %self.bridge.op_manager.ring.connection_manager.pub_key,
        loop_count,
        "About to call wait_for_event"
    );

    let event = self.wait_for_event(...).await?;

    tracing::trace!(
        peer = %self.bridge.op_manager.ring.connection_manager.pub_key,
        loop_count,
        event_type = ?event,
        "wait_for_event returned"
    );

    match event {
        EventResult::Continue => {
            tracing::trace!("Event loop continuing (Continue result)");
            continue;
        },
        EventResult::Event(event) => {
            tracing::debug!(
                event_type = ?event,
                "Processing event"
            );
            // ... process event
        }
    }
}
```

### 2. Check Event Processing After peer_connections

Since the last event was `peer_connections READY`, trace what happens when processing that event:

- Does it return `EventResult::Continue`?
- Does it return an error?
- Does it somehow cause the loop to exit?

### 3. Add Assertions or Panics

Add defensive checks to ensure event loop doesn't silently exit:

```rust
// Before the loop
tracing::info!("Event loop STARTING for peer {}", peer_id);

// After the loop (should never reach here in normal operation)
tracing::error!("Event loop EXITED unexpectedly for peer {}", peer_id);
panic!("Event loop should not exit normally");
```

### 4. Test with Shorter Bootstrap Time

Current test waits 25 seconds for bootstrap. Try with shorter time (10s) to see if issue still occurs:

```rust
// ubertest.rs:322
sleep(Duration::from_secs(10)).await;  // Instead of 25
```

### 5. Test with Single Threaded Runtime

The test uses `multi_thread` runtime. Try with single-threaded to eliminate concurrency issues:

```rust
#[tokio::test(flavor = "current_thread")]  // Instead of multi_thread
async fn test_basic_room_creation() -> anyhow::Result<()> {
```

### 6. Add Channel Capacity Monitoring

Check if the notification channel is full:

```rust
// When creating the channel (network_bridge.rs:100)
let (notifications_sender, notifications_receiver) = mpsc::channel(100);
tracing::info!(
    "Created notification channel with capacity 100, channel_id={}",
    channel_id
);

// In notify_op_change, before sending
tracing::info!(
    "Channel capacity check - about to send notification, tx={}",
    tx
);
```

## Related Issues & Context

### Previous Session Work

1. **Fixed `priority_select` waker registration** (commit: f4deff6d)
   - Issue: Futures were being recreated on each poll, losing waker registrations
   - Solution: Used BoxFuture fields with pin-project to maintain wakers
   - Result: Unit tests pass, but integration test still fails (different issue)

2. **Added comprehensive unit tests** (commit: f4deff6d)
   - 8 tests for PrioritySelectFuture
   - Stress test with 1700 messages (100x scale-up)
   - All tests pass in ~4 seconds

### test_three_node_network_connectivity

This test is also flaky:
- Sometimes passes (event loop stays alive)
- Sometimes fails with "channel closed" error
- Suggests similar lifecycle issues with event loops

## Files to Focus On

### Primary Investigation Targets

1. **crates/core/src/node/network_bridge/p2p_protoc.rs**
   - `network_event_listener()` - Main event loop (line ~211)
   - `wait_for_event()` - Priority select wrapper
   - Event processing match arms

2. **crates/core/src/node/network_bridge/priority_select.rs**
   - `PrioritySelectFuture::poll()` - The core select logic
   - Already fixed for waker registration, but double-check

3. **crates/core/src/node/op_state_manager.rs**
   - `notify_op_change()` - Sends notifications (line 146)
   - Channel creation and management

### Secondary Files

4. **crates/core/src/node/p2p_impl.rs**
   - Node startup and task spawning
   - Event loop lifecycle management

5. **crates/core/tests/ubertest.rs**
   - Test setup and timing
   - Bootstrap delays

## How to Reproduce

```bash
# Run the failing test
cargo test --test "*" test_basic_room_creation --features redb -- --nocapture --test-threads=1

# Save output for analysis
timeout 50 cargo test --test "*" test_basic_room_creation --features redb \
  -- --nocapture --test-threads=1 > /tmp/ubertest_output.log 2>&1

# Find the PUT transaction ID
grep "request_put.*Creating RequestPut message" /tmp/ubertest_output.log | head -1

# Track that transaction through the system
TX_ID="01K7PN..." # Replace with actual ID from above
grep "$TX_ID" /tmp/ubertest_output.log

# Find which peer should process it
grep "notify_op_change.*$TX_ID" /tmp/ubertest_output.log | grep "peer="

# Check if that peer's event loop stopped
PEER_ID="v6MWKgq..." # Replace with actual peer ID
grep "PrioritySelect.*$PEER_ID" /tmp/ubertest_output.log | tail -5
```

## Success Criteria

The test will pass when:
1. Event loop continues running throughout the test
2. PUT notification is received by the event loop
3. PUT request is processed and forwarded to target peer
4. Response is received back through the network
5. Client (riverctl) receives successful response within 10 seconds

## Additional Notes

- The notification channel has capacity 100 (created in network_bridge.rs:100)
- The test creates 1 gateway + 1 peer (minimal setup)
- Bootstrap time is 25 seconds (quite generous)
- riverctl timeout is 10 seconds for PUT response
- The test consistently fails with same pattern (event loop stops)

## Key Insight

**The issue is NOT with message delivery or priority selection - those work correctly. The issue is with EVENT LOOP LIFECYCLE - it stops running before the PUT request arrives.**

This is why:
- `notify_op_change` succeeds (channel send works)
- No channel errors or full channel (capacity 100, only 1 message)
- PrioritySelect logs show it was working correctly when active
- The problem is the loop simply stops polling ~8 seconds before needed

The fix needs to focus on **why the event loop exits or deadlocks**, not on message passing mechanisms.
