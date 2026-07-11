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
  1b. CHECK: Is this peer already connected or pending? → REJECT
      (Forces CONNECT to route uphill, discovering new unconnected peers
       instead of silently no-op'ing on already-known ones. See PR #3557.)
      Note: uphill routing is bounded by an explicit uphill_budget counter
      (default 8, separate from TTL). Each uphill hop decrements budget by 1.
      See PR #3621, which replaced the TTL halving from PR #3582.
  2. CHECK: Are we at max_connections (open + pending)? → REJECT
     EXCEPTION (nearest-neighbor lattice, mechanism 3): a candidate that
     would become this peer's new per-side NEAREST ring neighbor — a TIGHTEN
     of an already-held side OR a FILL of an empty one (any strictly-closer
     successor or predecessor) — is force-accepted even at/over max, up to a
     HARD over-max ceiling (max_connections + LATTICE_OVERMAX_SLACK). It
     displaces a farther non-lattice long link,
     which the topology maintenance loop's over-max prune sheds next tick
     (protected lattice edges are never the prune victim). Applied via the
     shared predicate admits_lattice_edge_over_cap() in should_accept, in
     add_connection, AND in BOTH connection-lifecycle promotion gates
     (p2p_protoc/connection_lifecycle.rs) so the exception is live on the
     real CONNECT path, not just should_accept. See connection_manager.rs.
     MINIMUM-DEGREE FLOOR: the whole lattice feature (this exception, the
     per-side acceptance clause, the route-to-self discovery probe, and the
     topology.rs retention exemption) is GATED OFF when this peer's CONFIGURED
     max_connections is below NN_LATTICE_MIN_MAX_CONNECTIONS (= 25 =
     DEFAULT_MIN_CONNECTIONS). Two-tier routing needs enough long links left
     after the 2-slot base-lattice reserve; below the network's own
     minimum-degree target a node can't sustain the structure. The gate reads
     CONFIGURED max (200 in production), NOT the live connection count, so
     production peers are always active and the floor's real job is to keep the
     sparse simulation suite (configs top out at max=16) at its pre-lattice
     baseline — only the production-scale max=200 sim tests exercise the lattice.
     The single activation gate is nn_lattice_active_for(max_connections) /
     ConnectionManager::nn_lattice_active(); the test-only override that flips
     the stock/fix validation arm also force-activates below the floor (so the
     dedicated benefit tests exercise the ON path at max=5).
     TIGHTENING-AT-CAPACITY: over-cap admission fires for any strictly-closer
     per-side nearest — a TIGHTEN of an already-held side OR a FILL of an empty
     one — so the lattice CONTINUOUSLY pulls each peer toward its true nearest
     ring neighbors, not merely filling empty sides. The superseded
     former-nearest demotes into the long-link pool automatically (the reserved
     slot is derived on demand from the live connection set). No reservation-aware
     clause: each admitted candidate ADVANCES the per-side current-nearest, so the
     admitted sequence is strictly decreasing (a short records chain) and the
     absolute ceiling already hard-bounds the ESTABLISHED set; the earlier F1
     one-fill-per-side clause was DROPPED (its concern was a concurrent fill flood
     on an EMPTY side of a FULL node — a near-non-case, since a node at max almost
     always has both ring sides populated). The route-to-self discovery probe
     likewise runs CONTINUOUSLY (decaying toward tau_max, never stopping when both
     sides are filled) so a filled-but-loose edge keeps tightening. See
     connection_manager.rs and ring.rs.
  3. Compute Kleinberg gap score (small_world_rand::kleinberg_score):
     → Map all connection distances to log-space (1/d = uniform in log)
     → Score = min distance to nearest neighbor in log-space
     → Candidates filling the largest gap score highest
     → Candidates outside [D_MIN, D_MAX] score 0 (including Sybil-close peers)
     → Note: connection acceptance uses non-directional scoring because
       directional analysis during bootstrap has too few data points per side.
       Directional (CW/CCW) awareness is applied in steady-state topology
       management (swaps, pruning) via topology.rs.
  4. Below min_connections: probabilistic acceptance (soft filter)
     → Below KLEINBERG_FILTER_MIN_CONNECTIONS (3): always ACCEPT
     → Above that: sliding floor from 0.9 (at KLEINBERG_FILTER_MIN_CONNECTIONS) to 0.3 (at min_connections), accept_prob = floor + gap_score
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

COROLLARY (under-min backoff escape, #4348 + #4362 refinement):
  A node still BELOW min_connections must not let the full escalating
  30s→600s location backoff trap it: its under-connection is by definition
  a local capacity problem, and a `Rejected`-stamped escalating backoff on
  a poorly-positioned node would park it permanently below min.

  This plays out differently on the two paths that consult location backoff:

  - MAINTENANCE loop (ring.rs connection_maintenance): IGNORES per-target
    location backoff entirely while under min, gated by
    should_respect_location_backoff() (honor backoff only at/above min).

  - CONNECT DRIVER retry path (operations/connect/op_ctx_task.rs, #4362):
    on a capacity `Rejected` while under min it does NOT ignore location
    backoff entirely — it stamps a SHORT, fixed, NON-escalating reject
    backoff (UNDER_MIN_REJECT_BACKOFF = 3s, time-bounded via TrackedBackoff::
    record_short_backoff, which never escalates the failure count and never
    shortens an already-longer escalated backoff). This is what provides
    storm safety on the driver path: skipping the backoff entirely (an
    earlier #4362 attempt) let under-min joiners re-probe every fast-tick
    and produced a ~12x terminus-rejection storm. The short fixed pause
    throttles the retry cadence without ramping toward the 600s trap, so the
    joiner keeps probing and the 2b widened re-route can find spare capacity.

  At/above min, BOTH paths honor the full escalating backoff (there a
  `Rejected` is a genuine "stop probing this region" signal). On the
  maintenance loop, storm safety under min is still the adaptive fast-tick
  backoff + max_concurrent cap (not location backoff).

See: ring.rs connection_maintenance + should_respect_location_backoff;
operations/connect/op_ctx_task.rs Rejected arm + ConnectionBackoff::
record_short_reject_backoff; issues #3414, #4348, #4362
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

WHEN summarizing contracts in the InterestSync heartbeat handlers
(handle_interest_sync_message: Interests / Summaries / ChangeInterests):
  → ONLY call get_contract_summary for contracts we host OR actively serve
    AND for which we actually hold state:
    (is_hosting_contract || contract_in_use) && contract_state_present — go
    through summary_if_hosted_or_in_use, never a bare get_contract_summary. The
    same composed predicate gates the broadcast path
    (should_broadcast_contract, broadcast_queue.rs).
  → A node carries phantom peer-interest in contracts it neither hosts nor
    serves (placement-migration after-effect, #4404). Summarizing those issues a
    GetSummaryQuery round-trip on the serial contract_handling loop that returns
    "state not found" every heartbeat — the #4473/#4145 summarize storm
    (~40/sec vs <10 real updates/hr) that wedged gateways. The ResyncRequest arm
    is exempt: it is state-gated and not heartbeat-driven.
  → #4610: the (is_hosting || in_use) gate ALONE is NOT sufficient. The inbound
    relay-SUBSCRIBE / placement path marks a contract is_hosting/in_use WITHOUT
    its state ever being fetched/stored, so phantom (interested-but-stateless)
    contracts still passed and re-drove the storm (~70-80/sec on 0.2.84/0.2.85).
    contract_state_present (a cheap on-disk STATE-store existence check, NOT the
    in-memory hosting cache) is the term that excludes them. It MUST read the
    state store so an evicted-but-on-disk contract (state present, not in the
    hosting cache, still in_use) keeps summarizing — keying on is_hosting_contract
    there would wrongly drop it.
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

### Cross-DashMap Lock Discipline (`significant_drop_tightening` exception)

```
Some methods mutate a primary DashMap entry and then write to a SECOND
DashMap that mirrors that state (a reverse index, a hash index, etc.).
For those methods, the primary entry's shard guard MUST be held across
the secondary write.

This intentionally lifts the `significant_drop_tightening` clippy lint:
the guard is load-bearing for atomicity against a concurrent remover,
not for serializing unrelated callers. A blind `cargo clippy --fix` (or
agent applying the lint) will silently re-introduce a race.

WRONG (PR #4129 shape):
  let became = {
      let mut entry = self.primary.entry(key).or_default();
      entry.add_reason()
  };  // <-- guard dropped here
  self.secondary_index.insert(key);  // <-- racy: a concurrent remover
                                     //     can run `cleanup_if_no_reasons`
                                     //     between the drop and the insert,
                                     //     no-op-removing the not-yet-present
                                     //     secondary entry, then this insert
                                     //     leaks a zombie.

CORRECT (PR #4171 shape):
  let mut entry = self.primary.entry(key).or_default();
  let became = entry.add_reason();
  self.secondary_index.insert(key);  // <-- still under the primary guard
  drop(entry);                       // explicit for documentation only
  became
```

Concrete cases in this crate:
- `InterestManager::{register_peer_interest, register_local_hosting,
  add_local_client, add_downstream_subscriber, register_local_interest}`
  hold `interested_peers` or `local_interests` across
  `index_contract_hash` so a concurrent `remove_*` cannot run
  `cleanup_contract_if_no_interest` → `unindex_contract_hash` in the
  window between guard drop and indexing. See PR #4129 (introduced the
  race) and PR #4171 (restored the discipline).

Audit grep when adding a new method that touches multiple DashMaps:
- Identify the primary map whose presence is the source of truth for
  interest/membership.
- Identify any secondary map that mirrors that state.
- Confirm the primary guard is held across the secondary write.
- Add an inline `// hold the X guard across Y to prevent the Z race`
  comment so a future clippy pass leaves a footprint when it tries to
  drop the guard.

CAVEAT — this rule only protects primary-origin removers (removers
that take the primary map's guard first, then touch the secondary).
A secondary-origin remover — one that starts from the secondary
index and works back to the primary — is NOT protected by this
discipline. Those need their own atomicity story: either share a
lock order with the add path, batch the primary-side updates under a
single guard, or use a reconciliation/two-phase remove protocol.

A secondary-origin remover that broke this way was
`InterestManager::remove_all_peer_interests` (issue #4174). Its old
shape removed the `peer_contracts[peer]` reverse entry up front, then
iterated a stale snapshot mutating `interested_peers` directly — a
concurrent `register_peer_interest` could re-create one side after the
snapshot, leaving a one-sided ghost. The fix (PR for #4174) does NOT
add a new atomicity primitive: it makes the secondary-origin remover
delegate each per-contract cleanup to the primary-origin remover
`remove_peer_interest`, which already holds the
`interested_peers[contract]` shard guard across its `peer_contracts`
update. So the safe pattern for a secondary-origin bulk remover is:
snapshot the secondary index (read-only, do NOT remove it up front),
then call the existing per-key primary-origin remover for each entry.
```

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
let peers = connection_manager.k_closest_potentially_hosting(
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
