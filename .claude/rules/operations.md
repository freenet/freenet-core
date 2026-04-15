---
paths:
  - "crates/core/src/operations/**"
  - "crates/core/src/node/op_state_manager.rs"
---

# Operations Module Rules

> **Path-scoped rule, legacy state-machine path.** As of #1454 Phase 2b
> (PR #3806) SUBSCRIBE's client-initiated path was migrated to a
> task-per-transaction driver in `operations/subscribe/op_ctx_task.rs`.
> Phase 3a (PR #3843) migrated client-initiated PUT to
> `operations/put/op_ctx_task.rs`, and Phase 3b migrated
> client-initiated GET to `operations/get/op_ctx_task.rs`, all using
> the shared `RetryDriver` trait from `op_ctx.rs`. On these paths,
> op state lives in task locals and is never pushed into
> `OpManager.ops.*`, so rules below that talk about "pushing state" /
> `load_or_init` / `handle_op_result` apply only to the **legacy
> state-machine path** (still used by GET relay/GC/UPDATE-auto-fetch
> paths — #3883 tracks relay-GET migration — PUT relay/GC paths,
> UPDATE, CONNECT, and by SUBSCRIBE's renewal / PUT-sub-op /
> executor / intermediate-peer entry points).
>
> Task-per-tx drivers have their own invariants documented in the
> `op_ctx_task.rs` module doc and in `OpCtx::send_and_await`'s rustdoc.
> The spirit of push-before-send is preserved (initialize task-local
> state before calling `send_and_await`), but the mechanical
> `op_manager.push(...)` call is absent. Phase 6 of #1454 will rewrite
> this rules file in full once all ops have migrated.

## Critical Invariant: Push-Before-Send (legacy path)

**This is the most important rule for code on the legacy state-machine path.**

```
ALWAYS save state BEFORE sending network message:

CORRECT:
  op_manager.push(tx, updated_state).await?;  // 1. SAVE FIRST
  network_bridge.send(target, msg).await?;    // 2. THEN SEND

WRONG:
  network_bridge.send(target, msg).await?;    // NO! Race condition!
  op_manager.push(tx, updated_state).await?;  // Response may arrive first
```

**Why:** If response arrives before state is saved, `load_or_init` will fail because the operation doesn't exist in storage.

**Task-per-tx equivalent (#1454 Phase 2b+):** callers of
`OpCtx::send_and_await` must have all fields the reply handler will read
(retry counters, visited-peers filter, routing state) set in task locals
BEFORE calling `send_and_await`. State is never published to the
`OpManager` DashMap; the conceptual ordering rule is the same. See the
`OpCtx::send_and_await` docstring for the full reasoning.

**GET-specific note (Phase 3b):** for client-initiated GETs the
bypass at `node.rs::handle_pure_network_message_v1` returns BEFORE
`handle_op_request` runs, so `process_message` does NOT execute on
the originator for `GetMsg::Response` / `ResponseStreaming` replies.
This is by design — there is no `GetOp` in `OpManager.ops.get` for a
task-per-tx transaction, so `load_or_init` would return
`OpNotPresent`. The driver (`operations/get/op_ctx_task.rs`) therefore
owns the originator-side side effects that the legacy Response{Found}
branch at `get.rs:2329` does: `PutQuery` to cache the state,
`record_get_access`, `mark_local_client_access`,
`announce_contract_hosted`, `register_local_hosting`, and
`broadcast_change_interests`. If you add a new side effect to the
legacy Response{Found} branch, mirror it in
`op_ctx_task.rs::cache_contract_locally`. Relay GETs still go through
`process_message` — the bypass only fires when the originator's
`pending_op_results` callback is installed, which relays never have.

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
1. FIRST: Register with expect_and_register_sub_operation()
2. THEN: Create and push the child operation
3. WHY: Prevents race where child completes before parent knows about it

CORRECT:
  op_manager.expect_and_register_sub_operation(parent_tx, child_tx);
  let child_op = SubscribeOp::new(...);
  op_manager.push(child_tx, child_op).await?;

WRONG:
  let child_op = SubscribeOp::new(...);
  op_manager.push(child_tx, child_op).await?;  // Child might complete here!
  op_manager.expect_and_register_sub_operation(parent_tx, child_tx);  // Too late
```

### WHEN a sub-operation fails

```
→ Call sub_operation_failed(child_tx, error_msg)
→ This propagates failure to parent
→ Parent should handle gracefully (may still complete partially)
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

**Note (#1454 Phase 2b/3a/3b):** For op kinds with a task-per-tx driver
(SUBSCRIBE, PUT, and GET client-initiated), the pure-network-message
handler checks `pending_op_results` FIRST and forwards the reply to
the awaiting task via `node::try_forward_task_per_tx_reply` before
reaching `load_or_init`. Do NOT "fix" `load_or_init`'s `OpNotPresent`
handling by trying to look up a task-owned tx there — it will never
find one, and it shouldn't. The bypass is the load-bearing piece;
confirm by reading the SUBSCRIBE, PUT, and GET branches of
`handle_pure_network_message_v1` in `node.rs`. For GET the bypass is
gated on `GetMsg::Response | GetMsg::ResponseStreaming` only —
non-terminal GetMsg variants (Request, ResponseStreamingAck,
ForwardingAck) fall through to the legacy state machine, and relay
nodes (which have no entry in `pending_op_results` for the tx)
continue handling forwarded messages via `process_message`.

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

### Load or Initialize

```rust
let init = Op::load_or_init(op_manager, msg, source_addr).await?;
match init {
    OpInitialization::Existing { op } => /* existing operation */,
    OpInitialization::New { op } => /* new operation */,
}
```

### Handle Operation Result

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
