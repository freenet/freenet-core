# Operations Module - Technical Notes

## Update Operation

### Key Issues Fixed

1. **Update Propagation Without State Change**
   - The `update_contract` function must handle `UpdateNoChange` events from the contract handler
   - Updates that don't change state should not be propagated to prevent circular loops
   - Compare state before and after update to determine if broadcasting is needed

2. **Routing to Unconnected Peers**
   - Update operations were selecting subscribers without checking connectivity
   - Always use `closest_potentially_caching` which filters to connected peers only
   - Don't just pick any subscriber with `.pop()` - must verify connectivity

### Implementation Details

```rust
// BAD: Picks any subscriber without checking connection
let target = subscribers.clone().pop()

// GOOD: Uses routing to find connected peer
let target = op_manager.ring.closest_potentially_caching(key, skip_list)
```

## General Operation Patterns

- Operations use transaction IDs to prevent duplicate processing
- `op_manager.pop(msg.id())` ensures each transaction is only processed once
- State machines track operation progress through various states
- Always verify peer connectivity before setting as target