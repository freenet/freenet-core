# Event Loop Implementation Analysis: Duplicate NodeEvent Handlers

## Problem Statement

PR #2558 identified a source of test flakiness caused by implementation disparity between the production event loop (`p2p_protoc.rs`) and the testing/simulation event loop (`testing_impl.rs`). Specifically, the `ClientDisconnected` handler in the testing implementation was discarding the return value from `remove_client_from_all_subscriptions()` and not sending prune notifications to upstream peers.

This document analyzes the root cause and provides recommendations for refactoring to prevent similar issues.

## Current Architecture

### Two Separate Event Loops

The codebase has two distinct event loop implementations for handling `NodeEvent` messages:

1. **Production**: `crates/core/src/node/network_bridge/p2p_protoc.rs`
   - Uses real UDP transport
   - Full implementation of all event handlers
   - Runs in production nodes

2. **Testing/Simulation**: `crates/core/src/node/testing_impl.rs`
   - Uses in-memory transport (`MemoryConnManager`)
   - Partial implementation of event handlers
   - Used by `SimNetwork` for integration tests

### NodeEvent Enum

Both implementations must handle the same set of events defined in `crates/core/src/message.rs`:

```rust
pub(crate) enum NodeEvent {
    DropConnection(SocketAddr),
    ConnectPeer { peer, tx, callback, is_gw },
    Disconnect { cause },
    QueryConnections { callback },
    QuerySubscriptions { callback },
    QueryNodeDiagnostics { config, callback },
    TransactionTimedOut(Transaction),
    TransactionCompleted(Transaction),
    LocalSubscribeComplete { tx, key, subscribed },
    ExpectPeerConnection { addr },
    BroadcastProximityCache { message },
    ClientDisconnected { client_id },
}
```

### Implementation Comparison

| NodeEvent | p2p_protoc.rs | testing_impl.rs |
|-----------|---------------|-----------------|
| `DropConnection` | Full (prune + orphan handling) | Full |
| `ConnectPeer` | Full (initiates connection) | Minimal (just logs) |
| `Disconnect` | Full | Full |
| `QueryConnections` | Full (returns peer list) | `unimplemented!()` |
| `QuerySubscriptions` | Full (returns subscriptions) | `unimplemented!()` |
| `QueryNodeDiagnostics` | Full (extensive diagnostics) | `unimplemented!()` |
| `TransactionTimedOut` | Full (cleanup) | `unimplemented!()` |
| `TransactionCompleted` | Full (cleanup) | `unimplemented!()` |
| `LocalSubscribeComplete` | Full (sends to client) | `unimplemented!()` |
| `ExpectPeerConnection` | Full (sets up handshake) | Ignored (logs debug) |
| `BroadcastProximityCache` | Full (broadcasts to peers) | Ignored (logs debug) |
| `ClientDisconnected` | Full (prune + notify) | **BUG** (ignores notifications) |

## Root Cause Analysis

### Why Two Implementations Exist

The separation exists because:

1. **Transport Layer Differences**: Production uses real UDP sockets while testing uses in-memory channels
2. **Complexity Avoidance**: Fully implementing the testing version seemed unnecessary since "tests were passing"
3. **Historical Evolution**: Features added to production weren't always ported to testing

### Why This Causes Flakiness

1. **Silent Failures**: The testing implementation silently ignores functionality, making bugs only manifest under specific timing conditions
2. **No Compile-Time Enforcement**: Nothing forces both implementations to handle events identically
3. **Regression Risk**: Every new `NodeEvent` or handler change requires manual updates to both files

## Recommendations

### Option 1: Extract Shared Event Handler Module (Recommended)

Create a shared module that encapsulates the core logic for each event handler:

```rust
// crates/core/src/node/event_handler.rs

use crate::message::NodeEvent;

/// Trait for sending messages to peers
pub trait MessageSender: Send + Sync {
    async fn send(&self, target: SocketAddr, msg: NetMessage) -> Result<()>;
}

/// Trait for sending prune notifications
pub trait PruneNotifier: Send + Sync {
    async fn send_prune_notifications(
        &self,
        notifications: Vec<(ContractKey, PeerKeyLocation)>,
    );
}

/// Shared handler for ClientDisconnected event
pub async fn handle_client_disconnected<N: PruneNotifier>(
    client_id: ClientId,
    ring: &Ring,
    notifier: &N,
) {
    tracing::debug!(%client_id, "Client disconnected");
    let notifications = ring.remove_client_from_all_subscriptions(client_id);
    notifier.send_prune_notifications(notifications).await;
}

// ... similar handlers for other events
```

**Benefits:**
- Single source of truth for event handling logic
- Compile-time enforcement via trait bounds
- Easy to test handlers in isolation

**Implementation Steps:**
1. Create `event_handler.rs` module
2. Extract handler logic into free functions
3. Define traits for dependencies (sender, notifier, etc.)
4. Update both `p2p_protoc.rs` and `testing_impl.rs` to use shared handlers

### Option 2: Unified Event Loop with Pluggable Transport

Refactor to have a single event loop implementation that accepts a transport abstraction:

```rust
// Extend NetworkBridge trait
pub trait NetworkBridge: Send + Sync {
    async fn send(&self, target: SocketAddr, msg: NetMessage) -> Result<()>;
    async fn recv(&mut self) -> Result<(NetMessage, Option<SocketAddr>)>;
    async fn drop_connection(&mut self, peer: SocketAddr) -> Result<()>;

    // Add prune notification support
    async fn send_prune_notifications(
        &self,
        notifications: Vec<(ContractKey, PeerKeyLocation)>,
    ) {
        // Default implementation using send()
        for (key, upstream) in notifications {
            if let Some(addr) = upstream.socket_addr() {
                let _ = self.send(addr, unsubscribe_msg(key)).await;
            }
        }
    }
}

// Single event loop used by both
async fn run_event_loop<B: NetworkBridge>(
    bridge: B,
    op_manager: Arc<OpManager>,
    // ...
) -> Result<()> {
    // Unified event handling
}
```

**Benefits:**
- Eliminates duplicate code entirely
- Guarantees behavior parity
- Reduces maintenance burden

**Challenges:**
- Larger refactoring scope
- May require adapting existing tests

### Option 3: Property-Based Testing (Supplementary)

Add property tests that verify both implementations produce equivalent side effects:

```rust
#[proptest]
fn both_implementations_handle_client_disconnected_identically(
    client_id: ClientId,
) {
    // Create identical initial states
    let (prod_state, test_state) = setup_identical_states();

    // Apply event to both
    prod_state.handle_client_disconnected(client_id);
    test_state.handle_client_disconnected(client_id);

    // Verify equivalent outcomes
    assert_eq!(
        prod_state.prune_notifications_sent,
        test_state.prune_notifications_sent
    );
}
```

**Benefits:**
- Catches divergence as regression tests
- Documents expected behavior parity

**Limitations:**
- Doesn't prevent divergence, only detects it
- Should be combined with Option 1 or 2

## Immediate Actions

### Short-term (PR #2558 Fix)

The immediate fix in PR #2558 addresses the `ClientDisconnected` handler by:
1. Capturing the notifications list from `remove_client_from_all_subscriptions()`
2. Sending prune notifications (requires implementing a sender in testing context)

### Medium-term

1. **Audit other handlers**: Review all `unimplemented!()` and ignored handlers in `testing_impl.rs`:
   - `ConnectPeer` - minimal implementation
   - `ExpectPeerConnection` - ignored
   - `BroadcastProximityCache` - ignored

   Determine if these gaps affect test coverage.

2. **Add CI check**: Consider adding a lint or test that verifies all `NodeEvent` variants are handled meaningfully in both implementations.

### Long-term

Implement Option 1 (Extract Shared Handler Module) to prevent future divergence.

## Related Files

- `crates/core/src/node/testing_impl.rs` - Testing event loop
- `crates/core/src/node/network_bridge/p2p_protoc.rs` - Production event loop
- `crates/core/src/node/network_bridge/in_memory.rs` - In-memory transport
- `crates/core/src/message.rs` - NodeEvent definition
- `docs/architecture/simulation-testing.md` - Simulation framework docs

## Appendix: Full Handler Audit

### Handlers That Need Attention

1. **`ConnectPeer`** - Testing impl just logs; production actually connects
   - Impact: Tests may not verify connection establishment behavior

2. **`ExpectPeerConnection`** - Testing impl ignores; production sets up handshake expectation
   - Impact: Relay/NAT traversal scenarios not tested

3. **`BroadcastProximityCache`** - Testing impl ignores; production broadcasts
   - Impact: Proximity cache propagation not tested

4. **`TransactionTimedOut`/`TransactionCompleted`** - Testing impl panics
   - Impact: Transaction cleanup not tested

5. **`LocalSubscribeComplete`** - Testing impl panics
   - Impact: Standalone subscription path not tested

6. **`QueryConnections`/`QuerySubscriptions`/`QueryNodeDiagnostics`** - Testing impl panics
   - Impact: Diagnostic queries not tested (likely acceptable)

## Implementation Progress (Option 2)

We are implementing Option 2 (Unified Event Loop with Pluggable Transport) using `InMemorySocket`.

### Completed

1. **InMemorySocket** (`crates/core/src/transport/in_memory_socket.rs`)
   - Implements the `Socket` trait for in-memory packet routing
   - Global registry for socket address → inbox mapping
   - Packet-level fault injection via `SocketFaultInjector`
   - VirtualTime integration for deterministic latency

2. **Generic HandshakeHandler** (`crates/core/src/node/network_bridge/handshake.rs`)
   - Made `Event<S>`, `HandshakeHandler<S>` generic over socket type
   - Default type parameter preserves backward compatibility

### Remaining Work

3. **Make `priority_select.rs` generic**
   - `SelectResult::Handshake` needs to carry `Event<S>`
   - `PrioritySelectStream` needs socket type parameter

4. **Make `p2p_protoc.rs` generic**
   - `run_event_listener` → `run_event_listener::<S: Socket>`
   - `handle_handshake_action` → generic over socket
   - `handle_successful_connection` → generic over socket
   - `peer_connection_listener` → generic over socket

5. **Update SimNetwork**
   - Use `P2pConnManager::run_event_listener::<InMemorySocket>`
   - Wire up `SocketFaultInjector` instead of message-level fault injection

6. **Cleanup**
   - Remove `MemoryConnManager`
   - Remove `testing_impl::run_event_listener`
   - Remove `NetworkBridgeExt` trait

### Type Propagation Map

```
InMemorySocket
    ↓
create_connection_handler::<InMemorySocket>()
    ↓
(OutboundConnectionHandler<InMemorySocket>, InboundConnectionHandler<InMemorySocket>)
    ↓
HandshakeHandler<InMemorySocket>
    ↓
Event<InMemorySocket>
    ↓
SelectResult::Handshake(Event<InMemorySocket>)
    ↓
PrioritySelectStream<HandshakeHandler<InMemorySocket>, ...>
    ↓
handle_handshake_action(Event<InMemorySocket>, ...)
    ↓
handle_successful_connection(PeerConnection<InMemorySocket>, ...)
    ↓
peer_connection_listener(PeerConnection<InMemorySocket>, ...)
```

### Estimated Remaining Effort

- `priority_select.rs` changes: ~50 lines
- `p2p_protoc.rs` generic changes: ~200 lines
- SimNetwork integration: ~100 lines
- Cleanup: -500 lines (net reduction)

Total: Approximately 1 week of focused work.
