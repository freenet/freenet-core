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
WHEN accepting a new connection (should_accept):
  1. CHECK: Is this a self-connection? → REJECT
  2. CHECK: Are we at max_connections (open + pending)? → REJECT
  3. Compute Kleinberg gap score (small_world_rand::kleinberg_score):
     → Map all connection distances to log-space (1/d = uniform in log)
     → Score = min distance to nearest neighbor in log-space
     → Candidates filling the largest gap score highest
     → Candidates outside [D_MIN, D_MAX] score 0 (including Sybil-close peers)
  4. Below min_connections: probabilistic acceptance (soft filter)
     → Below KLEINBERG_FILTER_MIN_CONNECTIONS (3): always ACCEPT
     → Above that: accept_prob = 0.5 + gap_score (50% floor)
     → Use ACTUAL open count, NOT speculative totals
  5. At/above min_connections: strict ConnectionEvaluator
     → Feed gap score into evaluator (accepts best candidate per window)

WHEN closing a connection:
  → MUST remove from connections_by_location
  → MUST remove from location_for_peer
  → MUST update connection count atomically
```

### Speculative State vs Actual State

```
Speculative state (pending reservations, in-flight operations) MUST be
treated differently from actual state (open connections, completed ops):

  - "Do I need more?" checks → use ACTUAL state only
    (e.g., open < min_connections)
  - "Am I over-committed?" checks → use speculative state
    (e.g., total_conn >= max_connections)

WHY: Speculative state inflates counts. A node with 8 open connections
and 2 pending reservations appears to have 11 connections, but only 8
are real. Using the inflated count for "need more?" decisions prevents
the node from reaching min_connections.

See: connection_manager.rs should_accept(), issue #3414
```

### Backoff Target Must Match Failure Cause

```
WHEN recording connection backoff:
  → The backoff target MUST reflect what actually failed

WRONG:
  // acquire_new returns None because WE have no routing candidates
  self.record_connection_failure(target_location, RoutingFailed);
  // This backs off the TARGET, but the target isn't at fault

CORRECT:
  // Only back off the target when the TARGET caused the failure
  // (timeout, rejection, NAT punch failed)
  // If the failure is local (no routing candidates), don't record backoff

WHY: Backing off a target for a local problem prevents connecting to it
later when local conditions improve (e.g., more connections acquired).

See: ring.rs connection_maintenance, issue #3414
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

### Thresholds Must Derive From Configuration

```
WHEN introducing a connection count threshold or limit:
  → MUST derive from min_connections / max_connections, NOT hardcode a number
  → Hardcoded thresholds silently break when configuration changes

WRONG:
  const BOOTSTRAP_THRESHOLD: usize = 4;  // Assumes min_connections is small

CORRECT:
  let bootstrap_threshold = connection_manager.min_connections;

WHY: A hardcoded threshold of 4 caused a 9-month latent bug where nodes
plateaued far below min_connections=10+ because gateway-directed CONNECTs
stopped too early.

See: operations/connect.rs initial_join_procedure, issue #3414
```

## Testing Checklist

```
□ Test with 0 connections (cold start)
□ Test at min_connections boundary
□ Test at max_connections boundary
□ Test self-connection rejection
□ Test location calculation determinism
□ Test routing with visited peer filtering
```

## Documentation

- Architecture: `docs/architecture/ring/README.md`
- Location: `crates/core/src/ring/location.rs`
- ConnectionManager: `crates/core/src/ring/connection_manager.rs`
- Router: `crates/core/src/router.rs`
