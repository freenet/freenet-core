---
paths:
  - "crates/core/src/contract/**"
  - "crates/core/src/wasm_runtime/**"
---

# Contracts Module Rules

## WASM Execution Rules

### WHEN executing contract code

```
ALWAYS:
  - Execute in sandboxed Wasmer runtime
  - Set memory limits (prevent DoS)
  - Set execution time limits
  - Validate WASM module before execution

NEVER:
  - Execute untrusted code outside sandbox
  - Allow contracts to access filesystem directly
  - Allow contracts to make network calls directly
```

### WHEN exposing host functions

```
Host functions (callable from WASM):
  - MUST validate all inputs
  - MUST handle panics gracefully
  - MUST NOT leak host memory to guest
  - SHOULD be idempotent where possible

Pattern:
  fn host_function(ctx: &mut Ctx, args: ...) -> Result<...> {
      // Validate args
      // Execute in controlled manner
      // Return result (not host memory pointer)
  }
```

## State Management Rules

### WHEN storing contract state

```
USE: StateStore trait
Backends: redb (default), sqlite

Operations:
  - get_state(key) → Option<State>
  - put_state(key, state) → Result
  - delete_state(key) → Result

MUST:
  - Handle missing state gracefully (new contract)
  - Validate state before storing
  - Log state changes for debugging
```

### WHEN updating contract state

```
1. Load current state
2. Execute contract with delta
3. Validate new state (contract's validate_state function)
4. Store new state
5. Emit BroadcastStateChange if changed

If validation fails:
  → Reject update
  → Return error to caller
  → Do NOT store invalid state
```

### WHEN merging states

```
Contracts may receive concurrent updates.
State merging rules:
  - Contract defines merge semantics
  - Merge MUST be commutative: merge(a, b) == merge(b, a)
  - Merge MUST be associative: merge(merge(a, b), c) == merge(a, merge(b, c))
  - Invalid merges should return error, not panic
```

## Contract Handler Rules

### WHEN processing contract events

```
ContractHandlerEvent variants:
  - PutQuery: Store new contract
  - GetQuery: Retrieve contract state
  - UpdateQuery: Apply state delta
  - RegisterSubscriberListener: Set up update notifications

Each MUST:
  - Validate inputs
  - Execute in bounded time
  - Return appropriate response variant
```

### WHEN handling executor callbacks

```
Contracts can request network operations via host functions.
These return via callback channel.

Pattern:
  1. Contract calls host function (e.g., get_related_contract)
  2. Host queues network operation
  3. Callback channel receives result
  4. Resume contract execution with result
```

## RuntimePool Rules

### WHEN using the executor pool

```
RuntimePool manages worker threads for WASM execution.

MUST:
  - Limit concurrent executions (prevent resource exhaustion)
  - Queue excess requests
  - Handle worker panics gracefully
  - Clean up resources on shutdown
```

### WHEN contract execution times out

```
→ Terminate WASM execution
→ Return timeout error to caller
→ Do NOT retry automatically
→ Log for debugging (may indicate DoS attempt)
```

## Related Contracts Rules

### WHEN a contract references other contracts

```
Contracts can depend on other contracts (related_contracts).

MUST:
  - Fetch related contracts before execution
  - Handle missing related contracts (may not exist yet)
  - Prevent circular dependencies (track visited set)
  - Limit depth of related contract chain
```

## Error Handling

### WHEN contract execution fails

```
ContractError types:
  - ValidationError: State doesn't pass contract's validation
  - ExecutionError: WASM execution failed
  - StorageError: Backend storage issue
  - TimeoutError: Execution exceeded time limit

Handling:
  - Log error with context
  - Return error to caller (don't panic)
  - Clean up any partial state
```

### WHEN WASM traps (panics)

```
→ Catch trap in host
→ Convert to ContractError::ExecutionError
→ Return to caller
→ Do NOT propagate panic to main thread
```

## Testing Checklist

```
□ Test contract validation (valid and invalid states)
□ Test state merging (commutativity, associativity)
□ Test execution timeout handling
□ Test related contract fetching
□ Test concurrent contract updates
□ Test WASM memory limits
□ Test host function input validation
```

## Common Patterns

### Execute Contract

```rust
let executor = RuntimePool::new(config, op_sender, op_manager, pool_size).await?;

let result = executor.execute(
    contract_key,
    ExecutionParams { state, delta, related },
).await?;

match result {
    ExecutionResult::Success { new_state } => ...,
    ExecutionResult::ValidationFailed { reason } => ...,
    ExecutionResult::Error { error } => ...,
}
```

### Contract Handler Channel

```rust
// Send query
ch_outbound.send_to_handler(ContractHandlerEvent::GetQuery {
    instance_id,
    return_contract_code: true,
}).await?;

// Receive response
let response = ch_inbound.recv_from_sender().await?;
match response {
    ContractHandlerEvent::GetResponse { key, response } => ...,
    _ => return Err(unexpected_response_error()),
}
```

## Pitfalls to Avoid

```
DON'T: Trust contract-provided data without validation
WHY: Contracts run untrusted code
VALIDATE: All inputs and outputs

DON'T: Execute contracts on main async runtime
WHY: WASM execution blocks; can deadlock event loop
USE: RuntimePool with dedicated threads

DON'T: Store partial/invalid state
WHY: Corrupts contract state, breaks invariants
VALIDATE: Before every store operation

DON'T: Allow unbounded related contract chains
WHY: DoS via deep dependency graphs
LIMIT: Maximum depth and total contracts fetched
```

## Documentation

- Handler: `crates/core/src/contract/handler.rs`
- Executor: `crates/core/src/contract/executor.rs`
- Runtime: `crates/core/src/wasm_runtime/mod.rs`
- Storage: `crates/core/src/contract/storages.rs`
