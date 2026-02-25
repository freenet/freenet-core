---
paths:
  - "crates/core/src/ring/**"
  - "crates/core/src/router/**"
---

# Ring/DHT Module Rules

## Critical Invariants

### Location Calculations

```
WHEN calculating peer location from IP:
  → MUST mask IP bytes for sybil resistance (see location.rs:15-51)
  → MUST include port ONLY for localhost/link-local addresses
  → NEVER use full IP without masking

WHEN calculating contract location:
  → USE hash of contract key bytes
  → Result MUST be in [0.0, 1.0] range
```

### Connection Management

```
WHEN accepting a new connection:
  1. CHECK: Is this a self-connection? → REJECT
  2. CHECK: Are we below min_connections (25)? → ACCEPT
  3. CHECK: Are we at max_connections (200)? → REJECT
  4. OTHERWISE: Evaluate via TopologyManager

WHEN closing a connection:
  → MUST remove from connections_by_location
  → MUST remove from location_for_peer
  → MUST update connection count atomically
```

### Routing Decisions

```
WHEN selecting next hop for message:
  1. Filter out: requester, visited peers, transient connections
  2. Use Router to select peer closest to target
  3. If HTL > 7: Consider random walk instead of greedy

WHEN routing fails (no peers):
  → Return RingError::EmptyRing
  → Do NOT panic or unwrap
```

## State Consistency Invariants

### Cross-Validation of Related Data Structures

```
connections_by_location and the transport connection set MUST be
periodically cross-validated. They can drift apart when failure
paths skip cleanup.

WHEN removing a connection (any failure path):
  → MUST clean up location_for_peer
  → MUST clean up connections_by_location
  → MUST clean up any pending operation state
  → Missing any of these creates orphaned entries that block future operations

WHEN exchanging peer lists in sync protocols (e.g., interest sync):
  → MUST filter out peers not currently in the live connection set
  → Stale peer entries cause operations to be sent to dead nodes
```

### Cleanup Exemptions Must Be Time-Bounded

```
Any condition that exempts an entry from garbage collection
(is_transient, has_pending, etc.) MUST:
  1. Expire via TTL, OR
  2. Be overridden by an absolute age threshold

Unbounded exemptions create permanent GC blind spots.

WRONG:
  retain(|entry| !entry.is_zombie() || entry.is_transient)  // Transient = forever

CORRECT:
  retain(|entry| {
      let dominated_by_age = entry.age() > ABSOLUTE_ZOMBIE_THRESHOLD;
      (!entry.is_zombie() || entry.is_transient) && !dominated_by_age
  })
```

**Audit targets:**
- All `prune_connection` / `drop_connection` call sites — verify all related maps are cleaned up
- All `retain()` / sweep loops — verify exemption conditions have TTL or absolute age bounds
- `ConnectionManager` fields — verify each map has a documented cleanup owner

## Trigger-Action Rules

### BEFORE modifying ConnectionManager

```
1. Check: Will this affect connection counting?
   → Ensure atomic operations for connection counters
   → Test with concurrent connection attempts

2. Check: Does this touch location lookups?
   → BTreeMap operations must handle missing keys gracefully
   → Always use get() not index operator []
```

### BEFORE modifying Location

```
1. Check: Does this change hash function?
   → THIS BREAKS NETWORK COMPATIBILITY
   → Requires network-wide upgrade coordination
   → Document in CHANGELOG as BREAKING

2. Check: Does this affect distance calculation?
   → Must maintain circular distance property
   → distance(a, b) must equal distance(b, a)
```

### WHEN implementing accept-only-at-terminus

```
The rule: Only accept connections at terminus (can't forward to closer peer)

CORRECT:
  if can_route_closer(target) {
      forward_only();  // Don't accept
  } else {
      evaluate_acceptance();  // May accept
  }

WRONG:
  accept_all_requests();  // Breaks small-world topology
```

## Common Patterns

### Peer Selection

```rust
// Get k best peers for a contract location
let peers = connection_manager.k_closest_potentially_caching(
    contract_location,
    &visited_peers,  // Skip already visited
);
```

### Location Distance

```rust
// Distance on circular ring (handles wrap-around)
let distance = location_a.distance(&location_b);
// distance.as_f64() is always in [0.0, 0.5]
```

### Subscription Management

```rust
// Subscriptions are lease-based (8 min lease, 2 min renewal)
ring.subscribe(contract_key, subscriber_id)?;
// Background task handles expiry - don't manually expire
```

## Pitfalls to Avoid

```
DON'T: Use raw SocketAddr as peer identifier
WHY: Multiple peers can share IP (NAT), use PeerKeyLocation instead

DON'T: Assume connections_by_location has entry for a location
WHY: Peers disconnect; always use .get() and handle None

DON'T: Skip visited peer filtering in routing
WHY: Creates routing loops, wastes bandwidth

DON'T: Accept connections without checking should_accept()
WHY: Breaks topology optimization, may cause resource exhaustion
```

## Testing Checklist

```
□ Test with 0 connections (cold start)
□ Test at min_connections boundary (25)
□ Test at max_connections boundary (200)
□ Test self-connection rejection
□ Test location calculation determinism
□ Test routing with visited peer filtering
```

## Documentation

- Architecture: `docs/architecture/ring/README.md`
- Location: `crates/core/src/ring/location.rs`
- ConnectionManager: `crates/core/src/ring/connection_manager.rs`
- Router: `crates/core/src/router/mod.rs`
