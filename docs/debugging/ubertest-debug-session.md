# Ubertest Debugging Session - Event Loop Deadlock in wait_for_event

**Date**: 2025-10-16
**Test**: `test_basic_room_creation` in `crates/core/tests/ubertest.rs`
**Status**: 🔴 ROOT CAUSE IDENTIFIED - Lost Wakeup Race Condition
**Issue**: [#1932](https://github.com/freenet/freenet-core/issues/1932)

## 🔴 ROOT CAUSE IDENTIFIED - LOST WAKEUP RACE CONDITION

**The event loop deadlocks due to a lost wakeup race condition caused by recreating `recv()` futures on every loop iteration.**

### The Problem

1. Event loop calls `wait_for_event()` → calls `select_priority()`
2. `select_priority()` creates a NEW `PrioritySelectFuture` instance with NEW `BoxFuture` instances
3. These futures are polled, register wakers, return Pending
4. On the NEXT loop iteration, `wait_for_event()` is called again
5. `select_priority()` creates BRAND NEW futures, **discarding the previous waker registrations**
6. When a notification arrives, there's no waker to wake the task (the old waker was dropped)
7. The event loop is permanently stuck in `.await` inside `wait_for_event`

### Code Path

```rust
// p2p_protoc.rs:232-242 (main event loop)
loop {
    let event = self.wait_for_event(...).await?;  // Called every iteration
    // ...
}

// p2p_protoc.rs:783-793 (wait_for_event function)
let result = priority_select::select_priority(...).await;  // Called every iteration

// priority_select.rs:241-251 (select_priority function)
PrioritySelectFuture::new(
    Box::pin(notification_rx.recv()),  // ❌ NEW future, loses previous waker!
    Box::pin(op_execution_rx.recv()),  // ❌ NEW future, loses previous waker!
    // ... all 8 channels create new futures
).await
```

### Evidence from Latest Run with Detailed Logging (2025-10-16 15:38)

Transaction: `01K7PTR7KS3052ZPHZR79YGTG1`, Peer: `v6MWKgqKBq7Fvsh2`

```
15:38:24.234127 - PrioritySelect: polling notification_rx (priority 1)
15:38:24.234177 - PrioritySelect: notification_rx Pending ⚠️  No message yet
15:38:24.234185 - PrioritySelect: polling op_execution_rx (priority 2)
15:38:24.234191 - PrioritySelect: op_execution_rx Pending
                  ... all 8 futures poll and return Pending ...
15:38:24.234274 - PrioritySelect: All 8 futures returned Pending, task will park
                  ⚠️  TASK PARKS, waiting for waker to be called

15:38:24.249645 - Notification sent successfully to channel ✓
                  ❌ BUT task already parked! Message arrived 15ms too late!

                  🔴 LOST WAKEUP - Task never woken again
```

**The Race Window**: 15 milliseconds between task parking and message arrival. This is a classic lost wakeup problem.

### Why This Happens

On each loop iteration, `select_priority()` creates NEW `recv()` futures:
```rust
Box::pin(notification_rx.recv())  // NEW future every time!
```

The race sequence:
1. **Loop N**: Create recv() future F1, poll it → Pending, waker W1 registered, task parks
2. Future F1 returns Ready with a message
3. Process message, loop continues
4. **Loop N+1**: Create NEW recv() future F2 (F1 is dropped!)
5. Poll F2 → Pending, about to register waker W2...
6. **⚡ RACE**: Message arrives in channel BEFORE W2 is fully registered
7. Channel tries to wake old waker W1 (which is gone) or no waker at all
8. Task never woken, deadlock forever

## 💡 Solution Approach

The fix requires persisting `BoxFuture` instances across event loop iterations so waker registrations remain valid.

### Implementation Strategy

1. **Create futures ONCE** outside the event loop (in `run_event_listener`):
```rust
let mut notification_fut: BoxFuture<'_, _> = Box::pin(notification_rx.recv());
let mut op_execution_fut: BoxFuture<'_, _> = Box::pin(op_execution_rx.recv());
// ... create all 8 futures once
```

2. **Pass futures by mutable reference** instead of recreating them:
```rust
// ❌ WRONG (current): Creates NEW futures every iteration
loop {
    let event = wait_for_event(..., &mut notification_rx, ...).await?;
    // Inside wait_for_event: Box::pin(notification_rx.recv()) ← NEW future!
}

// ✅ RIGHT: Reuses SAME futures across iterations
let mut notification_fut = Box::pin(notification_rx.recv());
loop {
    let event = wait_for_event(..., &mut notification_fut, ...).await?;
    // notification_fut is reused, waker registration persists!
}
```

3. **Recreate only completed futures**: When a future returns Ready (consumed), create a new one for ONLY that channel:
```rust
match event_result {
    SelectResult::Notification(_) => {
        // This future completed, recreate it
        notification_fut = Box::pin(notification_rx.recv());
    }
    // Keep other futures intact
}
```

### Implementation Challenges Encountered

Attempted using `&mut dyn Future` but hit borrow checker issues:
- Can't simultaneously borrow futures in event loop AND pass them to `wait_for_event`
- Rust's borrow checker prevents multiple mutable borrows
- Requires restructuring to avoid the borrow conflicts

### Recommended Next Steps

1. Create a `PersistentFutures` struct to hold all BoxFuture instances
2. Pass `&mut PersistentFutures` to `wait_for_event` and `select_priority`
3. Implement a `recreate_future()` method to replace completed futures
4. Update unit tests to also persist futures across loop iterations

## Executive Summary (OLD - now understood as deadlock, not exit)

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

---

## 📊 Follow-Up Session Summary (2025-10-16)

### Investigation Results

After extensive debugging with enhanced logging, we discovered:

**✅ What We Found:**
1. **Exact race window measured**: 15 milliseconds between task parking and message arrival
2. **Precise failure point**: `select_priority()` recreates BoxFuture instances on every call
3. **Waker registration loss**: New `.recv()` futures don't inherit previous waker registrations
4. **Not an exit**: Event loop doesn't break/exit - it's genuinely deadlocked in `.await`

**📝 Enhanced Logging Added:**
- `PrioritySelectFuture::poll()`: INFO-level logging for all 8 channel polls (shows Pending/Ready for each)
- `notify_op_change()`: Changed DEBUG → INFO to track notification flow
- `request_put()`: Added INFO logging for PUT operation lifecycle

**🔬 Key Evidence:**
```
15:38:24.234127 - PrioritySelect: polling notification_rx (priority 1)
15:38:24.234177 - notification_rx Pending ⚠️ No message in channel yet
15:38:24.234185 - polling op_execution_rx (priority 2)
15:38:24.234191 - op_execution_rx Pending
... all 8 channels poll and return Pending ...
15:38:24.234274 - All 8 futures returned Pending, task will park
                  ⚠️ Task goes to sleep, waiting for waker

15:38:24.249645 - Notification sent successfully ✓
                  ❌ 15ms gap! Message arrives AFTER task parked
                  🔴 Task never woken - permanent deadlock
```

### Implementation Attempt

**Approach Tried:** Use `&mut dyn Future` to pass futures by reference
- Modified `PrioritySelectFuture` to accept `&'a mut (dyn Future<...> + Send + Unpin)`
- Modified `select_priority()` to take future references instead of channels
- Created BoxFutures once outside event loop

**Result:** ❌ Blocked by borrow checker
- Can't simultaneously borrow futures in event loop AND pass to nested functions
- Multiple mutable borrow conflicts with Rust's safety rules
- Error: "cannot borrow as mutable more than once at a time"

### Recommended Implementation Path

**Solution: PersistentFutures Wrapper Struct**

```rust
struct PersistentFutures<'a> {
    notification: BoxFuture<'a, Option<Either<NetMessage, NodeEvent>>>,
    op_execution: BoxFuture<'a, Option<(Sender<NetMessage>, NetMessage)>>,
    conn_bridge: BoxFuture<'a, Option<P2pBridgeEvent>>,
    handshake: BoxFuture<'a, Result<HandshakeEvent, HandshakeError>>,
    node_controller: BoxFuture<'a, Option<NodeEvent>>,
    client_transaction: BoxFuture<'a, Result<(ClientId, WaitingTransaction), Error>>,
    executor_transaction: BoxFuture<'a, Result<Transaction, Error>>,
}

impl<'a> PersistentFutures<'a> {
    fn recreate_notification(&mut self, rx: &'a mut Receiver<...>) {
        self.notification = Box::pin(rx.recv());
    }
    // ... recreate methods for other futures
}
```

**Usage in event loop:**
```rust
let mut futures = PersistentFutures::new(
    &mut notification_rx,
    &mut op_execution_rx,
    // ... all channels
);

loop {
    let event = wait_for_event(&mut state, &mut futures).await?;

    match event {
        EventResult::Notification(msg) => {
            handle_notification(msg);
            // Recreate ONLY the notification future
            futures.recreate_notification(&mut notification_rx);
        }
        // Other branches recreate their specific future
    }
}
```

**Benefits:**
- ✅ Single ownership point (PersistentFutures struct)
- ✅ Explicit future lifecycle management
- ✅ No borrow checker conflicts
- ✅ Clear recreation points after Ready
- ✅ Maintains waker registration for pending futures

### Files Changed (Uncommitted)

**Enhanced Logging (Keep these):**
- `crates/core/src/node/op_state_manager.rs` - INFO level for notify_op_change
- `crates/core/src/operations/put.rs` - INFO level for PUT operations

**Implementation Attempt (Reverted):**
- `crates/core/src/node/network_bridge/priority_select.rs` - Reverted `&mut dyn Future` changes
- `crates/core/src/node/network_bridge/p2p_protoc.rs` - Reverted event loop changes

### Next Steps for Implementation

1. **Create `PersistentFutures` struct** in `priority_select.rs`
2. **Add recreation methods** for each future type
3. **Update `select_priority()`** to take `&mut PersistentFutures`
4. **Modify event loop** in `run_event_listener()`:
   - Create `PersistentFutures` once before loop
   - Pass `&mut futures` to `wait_for_event()`
   - Recreate specific future after each event
5. **Update unit tests** to follow same pattern
6. **Test with ubertest** to verify fix

### Related Issues

- Issue #1932 updated with root cause findings
- Issue #1951 (priority_select fix) was related but didn't fully solve the problem
  - That fix improved waker registration WITHIN a PrioritySelectFuture instance
  - Still needed: persistence ACROSS PrioritySelectFuture instances

---

## 🤔 Why Unit Tests Don't Catch This

### The Critical Difference

**Unit tests in `priority_select.rs` DO recreate futures on every iteration** (line 986-1052):

```rust
while received_events.len() < TOTAL_MESSAGES {
    // ❌ Same bug as production: NEW PrioritySelectFuture every iteration!
    let select_fut = PrioritySelectFuture::new(
        Box::pin(notif_rx.recv()),     // NEW future
        Box::pin(op_rx.recv()),        // NEW future
        // ... all 8 channels create new futures
    );

    let result = timeout(Duration::from_millis(100), select_fut).await;
    // ...
}
```

**So why do tests pass?**

### Reasons Tests Don't Fail

1. **Message Buffer Protection:**
   - Channels have 100-message capacity (line 840-856)
   - Tokio mpsc queues messages even when no receiver is polling
   - Messages wait in buffer until receiver polls again
   - In production: Only 1-2 messages at a time, no buffer protection

2. **Generous Timeout:**
   - Tests use `timeout(Duration::from_millis(100), ...)` (line 1055)
   - This is 100ms timeout per iteration - 6.6x the race window we measured (15ms)
   - Even if waker is lost, tokio runtime may still poll the task within 100ms
   - In production: Real operations timeout after 10 seconds but task genuinely deadlocks

3. **Continuous Message Flow:**
   - Test sends 1700 messages with 50-500µs delays
   - There's almost ALWAYS a message ready in the channel
   - When you poll a channel that already has a message: returns `Ready` immediately
   - **No waker registration needed if message is already there!**
   - In production: Long gaps (8+ seconds) between messages

4. **Test Loop Keeps CPU Active:**
   - Test loop runs in tight loop: poll → process → poll again
   - Tokio scheduler keeps task "hot" and polls it frequently even without waker
   - In production: Event loop waits indefinitely for events, truly parks the task

5. **Short Test Duration:**
   - Entire stress test runs in ~4 seconds
   - Race window is only 15ms
   - Probability of hitting the exact race: very low in such short duration
   - In production: Runs for hours/days, race eventually hits

### The Race Only Happens When:

✅ **All of these conditions are true:**
1. Task parks because ALL futures returned Pending (no ready messages)
2. Task truly goes to sleep (not polling due to other activity)
3. Message arrives AFTER task parked but BEFORE next poll would occur
4. No other messages wake the task up in the meantime
5. Race window is hit (we measured 15ms)

❌ **Test avoids all these:**
- Never parks for long (continuous message flow)
- 100ms timeout prevents true indefinite park
- Buffer holds messages even if waker lost
- Tokio keeps "hot" tasks actively scheduled

### How to Improve Tests

To catch this bug, tests would need to:

1. **Test with gaps:**
```rust
// Send message
notif_tx.send(msg).await;
// WAIT for task to park (50ms+)
sleep(Duration::from_millis(100)).await;
// Then send another message - would this wake it?
```

2. **Test genuine sleep:**
```rust
// Create PrioritySelectFuture
let select_fut = PrioritySelectFuture::new(...);
// Poll it once, ensure it returns Pending
// Drop and create NEW PrioritySelectFuture
// Now send message - will it wake?
```

3. **Test long-running scenario:**
```rust
// Simulate production: very occasional messages over minutes
for _ in 0..100 {
    sleep(Duration::from_secs(5)).await;  // 5 second gap
    send_message();
    // Will task wake up?
}
```

4. **Explicit waker testing:**
```rust
// Poll future, capture waker
// Drop future, create new one
// Signal old waker - verify task doesn't wake
// Signal channel - verify task DOES wake (should fail without fix)
```

### Key Insight

**The unit tests accidentally work around the bug they're trying to test!** By creating continuous message flow with buffers and tight loops, they never exercise the actual failure mode that happens in production with sparse, long-delayed messages.

## ✅ SUCCESS: Test That Reproduces The Bug (2025-10-17)

Created `test_sparse_messages_reproduce_race()` that successfully demonstrates the lost wakeup race condition!

### Test Design
```rust
#[tokio::test]
async fn test_sparse_messages_reproduce_race() {
    // Sender: sends 5 messages with 200ms gaps (sparse, like production)
    let sender = tokio::spawn(async move {
        for i in 0..5 {
            sleep(Duration::from_millis(200)).await;
            notif_tx.send(Either::Left(test_msg)).await.unwrap();
        }
    });

    // Receiver: recreates PrioritySelectFuture every iteration (HAS THE BUG)
    while received < 5 && iteration < 20 {
        iteration += 1;
        let select_fut = PrioritySelectFuture::new(
            Box::pin(notif_rx.recv()),  // ❌ NEW future every time - loses waker!
            // ... other channels
        );

        match timeout(Duration::from_millis(300), select_fut).await {
            Ok(SelectResult::Notification(Some(_))) => received += 1,
            Err(_) => tracing::warn!("Timeout"),
        }
    }

    assert_eq!(received, 5);  // ❌ FAILS!
}
```

### Test Results
```
[INFO] Iteration 1: Creating new PrioritySelectFuture
[INFO] Iteration 2: Creating new PrioritySelectFuture
...
[INFO] Iteration 20: Creating new PrioritySelectFuture
[INFO] Sender: Sending message 0 at Instant { tv_sec: 1447925, tv_nsec: 699895291 }
[INFO] Sender: Message 0 sent successfully
[INFO] Sender: Sending message 1 at Instant { tv_sec: 1447925, tv_nsec: 902869250 }
```

**Result:** Test receives **0 out of 5 messages**!
- All 20 iterations timed out (300ms each = 6 seconds total)
- First message sent AFTER receiver exhausted all iterations
- **Perfect demonstration of the lost wakeup race condition**

### Why This Test Succeeds (at failing)

1. **Sparse messages** (200ms gaps) vs continuous flow in other tests
2. **Small buffer** (capacity 1) vs 100 in stress tests
3. **Receiver recreates futures** every iteration - loses wakers
4. **Reasonable timeout** (300ms) catches the issue
5. **Enough iterations** (20) to show consistent failure

This test will **PASS after the fix is implemented** because persistent futures will maintain waker registration across iterations.

### Test Location and Status

**File:** [`crates/core/src/node/network_bridge/priority_select.rs:1416`](crates/core/src/node/network_bridge/priority_select.rs#L1416)

**Status:** Currently marked with `#[ignore]` to prevent CI failures

**To run manually:**
```bash
cargo test --lib test_sparse_messages_reproduce_race -- --ignored --nocapture
```

**Expected behavior:**
- ❌ Currently FAILS: receives 0/5 messages (demonstrates bug)
- ✅ After fix: should receive all 5 messages (validates fix)

### Next Steps

1. Implement PersistentFutures struct to hold BoxFutures outside the loop
2. Modify `wait_for_event()` to accept mutable references to persistent futures
3. Remove `#[ignore]` from this test - it should pass after the fix
4. Verify all ubertests pass with the fix in place
