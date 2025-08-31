# Local Contract Subscription Failure on Gateways

## Summary

Gateways that cache contracts locally cannot handle client subscriptions to those contracts. When a client attempts to subscribe to a locally-cached contract, the operation times out without notifying the client, even though the contract exists on the gateway.

## Context

PR #1781 introduced the ability for gateways to cache contracts locally when they are the optimal storage location. This fixed the "Ran out of caching peers" error by allowing nodes to consider themselves as valid caching targets through a new `CachingTarget` enum.

However, this exposed a fundamental issue: the subscription protocol was designed assuming network communication between different nodes. It cannot handle the case where the subscribing node already has the contract locally.

## Problem Description

### What Works ✅
- Gateways can store contracts locally when they're the best location (PUT operations)
- Updates to locally-stored contracts work correctly (UPDATE operations)
- Subscriptions to contracts on remote peers work normally

### What Fails ❌
- Client subscriptions to locally-cached contracts timeout
- Test `test_multiple_clients_subscription` fails consistently
- No mechanism exists to notify clients when subscribing to local contracts

## Technical Analysis

### The Subscription Message Flow

Normal (working) subscription flow with remote peer:
```
Client → Gateway → [RequestSub] → Remote Peer
                                 ↓
Client ← Gateway ← [ReturnSub] ← Remote Peer
```

Broken flow with local contract:
```
Client → Gateway (has contract locally)
           ↓
    [No ReturnSub generated]
           ↓
Client timeout (no response received)
```

### Root Cause

The subscription state machine has these states:
1. `PrepareRequest` - Initial state when client requests subscription
2. `AwaitingResponse` - Waiting for response from remote peer  
3. `ReceivedRequest` - Processing incoming subscription request
4. `Completed` - Subscription successful

When we detect a local contract, we transition directly from `PrepareRequest` to `Completed`:

```rust
// In crates/core/src/operations/subscribe.rs
if super::has_contract(op_manager, *key).await? {
    let completed_op = SubscribeOp {
        id: *id,
        state: Some(SubscribeState::Completed { key: *key }),
    };
    op_manager.push(*id, OpEnum::Subscribe(completed_op)).await?;
    return Ok(());
}
```

**The Problem**: Setting state to `Completed` doesn't trigger any client notification. The client is left waiting for a response that never comes.

### Why Remote Subscriptions Work

Remote subscriptions work because they generate a `ReturnSub` message that flows back to the client:

```rust
// When receiving successful subscription from remote peer
Some(SubscribeState::ReceivedRequest) => {
    return_msg = Some(SubscribeMsg::ReturnSub {
        sender: target.clone(),
        target: subscriber.clone(),
        id: *id,
        key: *key,
        subscribed: true,
    });
}
```

This `ReturnSub` message triggers the client notification mechanism.

## Impact

### Affected Systems
- **Gateway Operations**: Any gateway that stores contracts locally cannot serve subscriptions for those contracts
- **River Chat**: Multi-user chat rooms fail when the gateway caches the room contract
- **Delta-Sync Protocol**: Breaks Freenet's eventual consistency mechanism that relies on subscriptions for update propagation

### Test Failures
- `test_multiple_clients_subscription` - Times out waiting for subscription response
- Any integration test where gateways cache contracts locally

## Proposed Solutions

### Option 1: Simulate Remote Flow Locally
When detecting a local contract, simulate the message flow that would occur with a remote peer:

```rust
if super::has_contract(op_manager, *key).await? {
    // Transition through states as if we received a request
    // Generate a ReturnSub message to trigger client notification
    let return_sub = SubscribeMsg::ReturnSub {
        sender: op_manager.ring.connection_manager.own_location(),
        target: op_manager.ring.connection_manager.own_location(), 
        id: *id,
        key: *key,
        subscribed: true,
    };
    // Process this message to trigger client notification
}
```

**Pros**: 
- Reuses existing message handling code
- Minimal changes required
- Maintains consistency with remote flow

**Cons**: 
- Somewhat hacky (sending messages to self)
- May have unexpected side effects

### Option 2: Add Explicit Local Subscription Handling
Create a new state or path specifically for local subscriptions:

```rust
enum SubscribeState {
    // ... existing states ...
    LocalSubscription { 
        id: Transaction,
        key: ContractKey,
    },
}

// Handle local subscription with direct client notification
if super::has_contract(op_manager, *key).await? {
    // Directly trigger client notification mechanism
    notify_client_of_subscription_success(id, key);
}
```

**Pros**: 
- Cleaner architecture
- Explicit handling of local case
- No message-to-self hack

**Cons**: 
- Requires understanding and modifying client notification system
- More extensive changes
- Risk of missing edge cases

### Option 3: Always Forward to Remote Peers
Disable local caching for subscriptions - always forward to a remote peer even if we have the contract:

```rust
// In subscription handling only
if is_gateway {
    // Never use CachingTarget::Local for subscriptions
    always_use_remote_peer()
}
```

**Pros**: 
- Simple workaround
- No protocol changes needed

**Cons**: 
- Defeats purpose of local caching
- Inefficient (unnecessary network traffic)
- May still fail if no remote peers available

## Recommendation

**Option 1 (Simulate Remote Flow)** appears to be the most pragmatic solution:

1. It leverages existing, tested code paths
2. Minimal risk of introducing new bugs
3. Can be implemented quickly to unblock PR #1781
4. Can be refined later if needed

The key insight is that we need to generate the same client notification that occurs when receiving a `ReturnSub` message, and the safest way is to actually generate that message and process it through the existing flow.

## Questions for Team

1. **Client Notification Mechanism**: Can someone explain exactly how clients get notified of subscription success? Is it solely through `ReturnSub` message processing?

2. **Message-to-Self Safety**: Are there any concerns with a node sending network messages to itself? Are there guards against this we need to handle?

3. **State Machine Assumptions**: Are there other assumptions in the subscription state machine that break with local caching?

4. **Long-term Architecture**: Should we eventually redesign the subscription protocol to properly handle local contracts, or is the message simulation approach acceptable?

## Next Steps

1. Team review and feedback on proposed solutions
2. Consensus on approach (recommend Option 1)
3. Implementation in PR #1781 or separate PR
4. Ensure `test_multiple_clients_subscription` passes
5. Document the solution for future reference

## Related Links

- PR #1781: [Fix gateway contract storage](https://github.com/freenet/freenet-core/pull/1781)
- Original issue discussion in PR comments
- Test failure: `test_multiple_clients_subscription` timeout

---

*Note: This issue was identified while implementing local caching support for gateways. The caching itself works correctly for PUT/UPDATE operations, but the subscription protocol needs adjustment to handle local contracts.*