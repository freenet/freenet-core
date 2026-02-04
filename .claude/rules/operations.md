---
paths:
  - "crates/core/src/operations/**"
  - "crates/core/src/node/op_state_manager.rs"
---

# Operations Module Rules

## Critical Invariant: Push-Before-Send

**This is the most important rule in this module.**

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

## Error Handling

### WHEN encountering OpNotPresent

```
This is usually benign (duplicate message, already completed)
→ Log at debug level
→ Return Ok(None)
→ Do NOT treat as error
```

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
□ Use --test-threads=1 for deterministic scheduling
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
- Operation trait: `crates/core/src/operations/mod.rs:32-56`
