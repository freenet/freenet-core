---
paths:
  - "crates/core/src/operations/**"
  - "crates/core/src/node/op_state_manager.rs"
---

# Operations Module Rules

## Execution Model

Every operation runs as a task-per-transaction driver: each `Transaction`
is owned and driven by a single spawned task; state lives in task
locals, never in `OpManager.ops.*`. The only exception is **CONNECT**,
which still uses a `OpManager.ops.connect` DashMap + `Operation` impl
for joiner-side branches reachable from in-file tests and the legacy
stateless `ObservedAddress` side-effect path.

Driver entry points:

| Op | Entry points |
|----|--------------|
| CONNECT | `connect/op_ctx_task.rs::start_client_connect`, `start_relay_connect` |
| GET | `get/op_ctx_task.rs::start_client_get`, `start_relay_get`, `start_sub_op_get`, `start_targeted_sub_op_get` |
| PUT | `put/op_ctx_task.rs::start_client_put`, `start_relay_put`, `start_relay_put_streaming` |
| UPDATE | `update/op_ctx_task.rs::start_client_update`, `start_relay_request_update`, `start_relay_broadcast_to`, `start_relay_request_update_streaming`, `start_relay_broadcast_to_streaming` |
| SUBSCRIBE | `subscribe/op_ctx_task.rs::start_client_subscribe`, `run_executor_subscribe`, `run_renewal_subscribe`, `start_relay_subscribe`; `subscribe.rs::handle_unsubscribe_inbound` for `Unsubscribe` |

The shared retry-loop driver lives in `op_ctx.rs` (`RetryDriver` trait
+ `drive_retry_loop`). UPDATE is fire-and-forget (no retry loop, no
upstream reply); GET/PUT/SUBSCRIBE share the retry driver.

## Wire-variant dispatch

`node.rs::handle_pure_network_message_v1` is the single dispatch site
for every inbound wire message. Pattern per op:

1. **Reply bypass.** If a terminal reply variant arrives and a
   `pending_op_results` callback is registered, forward it via
   `try_forward_task_per_tx_reply` and return. For GET/PUT/SUBSCRIBE
   the gate is `Response | ResponseStreaming` only. CONNECT forwards
   all four non-`Request` variants (multi-reply fan-in).
2. **Relay dispatch.** Spawn the matching `start_relay_*` driver.
   Originator-loopback (`source_addr=None`) is mapped to
   `upstream_addr=own_addr` for GET/PUT/SUBSCRIBE so the same driver
   handles both relay hops and originator loopback. UPDATE has no
   loopback (fire-and-forget end-to-end).
3. **No legacy fallthrough** for GET/PUT/UPDATE/SUBSCRIBE — every wire
   variant either bypasses to a waiter, dispatches a driver, or hits a
   dedicated free-function handler (e.g. `handle_unsubscribe_inbound`,
   `ForwardingAck` no-op). CONNECT still has a
   `handle_op_request::<ConnectOp>` fallthrough for joiner-side legacy
   re-entries.

## OpEnum and DashMaps

- `OpEnum::Connect` and `OpManager.ops.connect` are the only surviving
  DashMap-backed op state. Used by joiner-side legacy paths.
- `OpEnum::{Get,Put,Update,Subscribe}` are gone. `ops.{get,put,update,subscribe}`
  DashMaps are gone. `has_{get,put,update,subscribe}_op` accessors are
  gone.
- `pending_op_counts()` returns `[connect, 0, 0, 0, 0]` — kept for
  telemetry API stability.

## Critical Invariant: Initialize-Before-Send

```
All task-local state the reply handler will read (retry counters,
visited-peers filter, routing state) MUST be initialized BEFORE
calling `OpCtx::send_and_await`. The reply may arrive on the very
next poll.
```

For CONNECT's surviving legacy path: state MUST be pushed via
`op_manager.push(tx, state)` before any `network_bridge.send(...)` —
a fast response that beats the push would hit `OpNotPresent`.

## State Machine Rules

### WHEN implementing a new operation state

```
1. Define states as enum variants with clear names
2. Each state must have a clear transition trigger
3. Document what message causes each transition
4. Handle unexpected messages gracefully (log + ignore)
```

### WHEN transitioning state

```
CORRECT:
  let next_state = match (current_state, message) {
      (State::A, Msg::X) => State::B,
      (State::B, Msg::Y) => State::C,
      _ => {
          tracing::warn!("unexpected message in state");
          return Ok(OperationResult::default());
      }
  };

WRONG:
  // Don't panic on unexpected messages
  panic!("invalid state transition");
```

### WHEN an operation completes

```
1. Call op_manager.completed(tx)
2. Do NOT call push() after completed()
3. If sub-operations exist, wait for all to complete
4. Send result to client via result router
```

## Sub-Operation Rules

### WHEN spawning a sub-operation

```
Sub-operations are identified structurally — create them via
Transaction::new_child_of::<MsgType>(&parent_tx). The parent field is
set at construction; Transaction::is_sub_operation() returns true
without any DashMap registration.

Either await the child inline (blocking) or fire-and-forget (async).
There is no central tracker; failure propagation is the child driver's
responsibility (publish HostResult::Err on its own task).

The is_sub_operation guards at p2p_protoc.rs, node.rs, and subscribe.rs
all use the structural check.
```

## Streaming Rules

### WHEN payload exceeds streaming_threshold (default 64KB)

```
1. Use RequestStreaming/ResponseStreaming message variants
2. Implement "piped streaming" - forward while receiving
3. Handle OrphanStreamClaimFailed error (stream metadata arrived first)
4. Clean up stream handles on error
```

### WHEN implementing streaming

```
MUST:
  - Check op_manager.should_use_streaming(payload_size)
  - Fork stream handle for parallel forwarding
  - Deserialize and store locally while piping
  - Handle stream cancellation gracefully

DON'T:
  - Buffer entire payload in memory (defeats streaming purpose)
  - Forget to claim orphan streams when fragments arrive first
```

## Transaction Rules

### WHEN creating transactions

```
For new operations: Transaction::new::<OpType::Message>()
For sub-operations: Transaction::new_child_of::<OpType::Message>(&parent)
```

### WHEN checking timeout

```
Use: tx.timed_out()
→ Uses simulation time (TimeSource), NOT wall clock
→ Ensures deterministic testing
```

## State Consistency

### WHEN a connection is removed

```
All failure paths that remove a connection MUST also clean up
pending operation state for that peer.

MUST:
  → Remove any in-flight operations targeting the disconnected peer
  → Fail pending sub-operations that depend on the peer
  → Log stale operation cleanup at debug level

WHY: Orphaned operations to dead peers cause timeouts and resource leaks.
```

### WHEN syncing peer lists across nodes

```
Sync protocols that exchange peer lists MUST filter out
peers not currently in the live connection set.

WRONG:
  send_peer_list(all_known_peers)  // Includes long-dead peers

CORRECT:
  send_peer_list(connected_peers.filter(|p| is_live_connection(p)))
```

## Error Handling

### WHEN encountering OpNotPresent

```
This is usually benign (duplicate message, already completed)
→ Log at debug level
→ Return Ok(None)
→ Do NOT treat as error
```

Do NOT "fix" `load_or_init`'s `OpNotPresent` for non-CONNECT ops by
trying to look up the tx there — the bypass at the top of
`handle_pure_network_message_v1` already forwarded the reply to the
driver's waiter, and `load_or_init` no longer runs for those ops.

### WHEN encountering InvalidStateTransition

```
→ Log with backtrace (debug builds)
→ Return error to caller
→ Do NOT panic
→ Consider: Is the message from malicious peer?
```

## Testing Checklist

```
□ Test state machine transitions with all message types
□ Test timeout handling (advance simulation time)
□ Test sub-operation completion order (child before/after parent)
□ Test streaming with payloads at threshold boundary
□ Test race conditions (fast responses)
```

## Common Patterns

### Load or Initialize (CONNECT only)

```rust
let init = Op::load_or_init(op_manager, msg, source_addr).await?;
match init {
    OpInitialization::Existing { op } => /* existing operation */,
    OpInitialization::New { op } => /* new operation */,
}
```

### Handle Operation Result (CONNECT only)

```rust
let result = op.process_message(conn_manager, op_manager, msg, source_addr).await?;

if let Some(return_msg) = result.return_msg {
    // Push state BEFORE sending
    if let Some(state) = result.state {
        op_manager.push(tx, state).await?;
    }
    conn_manager.send(result.next_hop.unwrap(), return_msg).await?;
} else if result.state.is_none() {
    // Operation finished
    op_manager.completed(tx);
}
```

## Documentation

- Architecture: `docs/architecture/operations/README.md`
- OpManager: `crates/core/src/node/op_state_manager.rs`
- Operation trait: `crates/core/src/operations.rs:32-56`
- Round-trip primitive: `crates/core/src/operations/op_ctx.rs`
