# Global Bandwidth Pool - Future Enhancement

**Status**: Design documented, implementation deferred to separate PR
**Priority**: Medium (Phase 2 - after current LEDBAT work stabilizes)
**Estimated effort**: 2-4 weeks

## Problem Statement

Users want to configure total bandwidth (e.g., "I have 500 Mbps, let Freenet use 50 MB/s total") rather than per-connection limits.

**Current behavior:**
- Each connection gets independent `bandwidth_limit` (default 10 MB/s)
- N connections can use N × 10 MB/s total bandwidth
- Users must reason about "per-connection" instead of "total available"

**Desired behavior:**
```toml
[network-api]
total_bandwidth_limit = 50000000  # 50 MB/s total across all connections
```

## Recommended Design: Simple Global Rate Derivation

After Opus review, the recommended approach is **atomic connection counting** rather than hierarchical token buckets.

### Architecture

```rust
/// Global bandwidth manager using atomic connection counting
pub struct GlobalBandwidthManager {
    /// Total bandwidth cap (bytes/sec)
    total_limit: usize,

    /// Active connection count (lock-free)
    connection_count: AtomicUsize,

    /// Optional: Minimum rate per connection
    min_per_connection: Option<usize>,
}

impl GlobalBandwidthManager {
    /// Register new connection, returns per-connection rate
    pub fn register_connection(&self) -> usize {
        let count = self.connection_count.fetch_add(1, Ordering::AcqRel) + 1;
        self.compute_per_connection_rate(count)
    }

    /// Unregister closed connection, returns new per-connection rate
    pub fn unregister_connection(&self) -> usize {
        let count = self.connection_count.fetch_sub(1, Ordering::AcqRel) - 1;
        self.compute_per_connection_rate(count.max(1))
    }

    /// Compute fair share per connection
    fn compute_per_connection_rate(&self, count: usize) -> usize {
        let fair_share = self.total_limit / count;

        // Honor minimum if set
        if let Some(min) = self.min_per_connection {
            fair_share.max(min)
        } else {
            fair_share
        }
    }

    /// Get current per-connection rate (for existing connections)
    pub fn current_per_connection_rate(&self) -> usize {
        let count = self.connection_count.load(Ordering::Acquire).max(1);
        self.compute_per_connection_rate(count)
    }
}
```

### Integration Points

**In `connection_handler.rs` when creating RemoteConnection:**

```rust
// Current (per-connection):
let initial_rate = bandwidth_limit.unwrap_or(10_000_000);
let token_bucket = Arc::new(TokenBucket::new(10_000, initial_rate));

// With global pool:
let per_conn_rate = if let Some(global_mgr) = &global_bandwidth_manager {
    global_mgr.register_connection()
} else {
    bandwidth_limit.unwrap_or(10_000_000)
};
let token_bucket = Arc::new(TokenBucket::new(10_000, per_conn_rate));
```

**In `PeerConnection::recv()` rate update loop:**

```rust
// Periodically update rate based on current connection count
if let Some(global_mgr) = &self.global_bandwidth_manager {
    let current_rate = global_mgr.current_per_connection_rate();
    let ledbat_rate = self.ledbat.current_rate(rtt);

    // Take minimum of global fair share and LEDBAT's computed rate
    let effective_rate = current_rate.min(ledbat_rate);
    self.token_bucket.set_rate(effective_rate);
}
```

**On connection close:**

```rust
if let Some(global_mgr) = &self.global_bandwidth_manager {
    global_mgr.unregister_connection();
}
```

### Configuration

```rust
// In config/mod.rs - NetworkApiConfig
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkApiConfig {
    // ... existing fields ...

    /// Total bandwidth limit across all connections (bytes/sec)
    /// If set, per-connection rate is derived as: total / active_connections
    /// Overrides per-connection bandwidth_limit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_bandwidth_limit: Option<usize>,

    /// Bandwidth limit per connection (bytes/sec)
    /// Used if total_bandwidth_limit is not set
    /// Default: 10 MB/s (10,000,000 bytes/second)
    pub bandwidth_limit: Option<usize>,

    /// Minimum bandwidth per connection when using total_bandwidth_limit
    /// Prevents starvation with many connections
    /// Default: 1 MB/s (1,000,000 bytes/second)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_bandwidth_per_connection: Option<usize>,
}
```

### Behavior Examples

**User with 500 Mbps (62.5 MB/s) connection:**

```toml
[network-api]
total_bandwidth_limit = 50000000           # 50 MB/s total
min_bandwidth_per_connection = 1000000     # 1 MB/s minimum
```

| Active Connections | Per-Connection Rate | Total Usage |
|-------------------|---------------------|-------------|
| 1 | 50 MB/s | 50 MB/s |
| 5 | 10 MB/s | 50 MB/s |
| 10 | 5 MB/s | 50 MB/s |
| 50 | 1 MB/s (min enforced) | 50 MB/s |
| 100 | 1 MB/s (min enforced) | ~100 MB/s (exceeds cap) |

**Backward compatibility (current behavior):**

```toml
[network-api]
bandwidth_limit = 10000000  # 10 MB/s per connection (current)
# No total_bandwidth_limit set = current behavior
```

## Why This Design Over Hierarchical Token Buckets

### Rejected: Full Hierarchical Design

The initial proposal used two-layer token buckets:
1. Per-connection bucket (fairness layer)
2. Global bucket (total cap layer)

**Why rejected:**
- ❌ Creates 3 rate controllers: LEDBAT + per-connection bucket + global bucket
- ❌ Can confuse LEDBAT's delay-based feedback (artificial delays not from network)
- ❌ Race conditions in rebalancing logic
- ❌ Lock contention on global bucket (all sends compete)
- ❌ Complexity outweighs benefits for P2P background traffic

### Approved: Simple Rate Derivation

**Why better:**
- ✅ Lock-free (just atomic counter operations)
- ✅ LEDBAT remains primary controller (network-responsive)
- ✅ Minimal code changes to existing architecture
- ✅ Predictable: N connections = total/N per connection
- ✅ Easy to debug and reason about

**Trade-off accepted:**
- Brief overages possible if all connections burst simultaneously
- But LEDBAT + TokenBucket smooth bursts naturally
- Acceptable for background traffic use case

## Implementation Checklist

When implementing this in a future PR:

### Phase 1: Core Implementation
- [ ] Add `GlobalBandwidthManager` struct to `connection_handler.rs`
- [ ] Add configuration fields to `NetworkApiConfig`
- [ ] Integrate `register_connection()` in connection creation paths
- [ ] Integrate `unregister_connection()` in connection cleanup
- [ ] Add rate update logic to `PeerConnection::recv()` loop

### Phase 2: Polish
- [ ] Add telemetry/metrics for bandwidth allocation debugging
- [ ] Add grace period for new connections (don't immediately throttle)
- [ ] Handle edge case: connection count reaches zero
- [ ] Add config validation (total_limit > min_per_connection)
- [ ] Update user documentation with examples

### Phase 3: Testing
- [ ] Unit tests: GlobalBandwidthManager rate calculations
- [ ] Integration test: Multiple connections share bandwidth fairly
- [ ] Integration test: Connection churn (add/remove) rebalances rates
- [ ] Benchmark: Verify no performance regression vs per-connection
- [ ] Test: Verify LEDBAT still yields to foreground traffic

### Phase 4: Migration
- [ ] Ensure backward compatibility with `bandwidth_limit`
- [ ] Add migration guide for users
- [ ] Consider making `total_bandwidth_limit` the default in future major version

## Open Questions

1. **Rebalancing frequency**: How often should existing connections check for rate updates?
   - Current rate update loop: every 50-500ms (RTT-adaptive)
   - Could piggyback on existing rate update logic

2. **Connection counting**: Count all connections or only active senders?
   - **Recommendation**: All connections (simpler, more predictable)
   - Idle connections shouldn't suddenly burst when active

3. **Minimum enforcement**: What if `min_per_connection × N` exceeds `total_limit`?
   - **Recommendation**: Honor minimums, log warning
   - User should increase total_limit or reduce min

4. **Integration with rate_update_interval adaptation**:
   - Current code adjusts update interval based on RTT (50-500ms)
   - Should global rate checks use same adaptive timing?

## References

- Current implementation: `connection_handler.rs:959-973` (token bucket init)
- LEDBAT rate calculation: `peer_connection.rs:684-692` (rate update loop)
- Token bucket: `token_bucket.rs` (pacing implementation)
- Opus design review: See conversation history (2025-12-17)

## Related Issues

- Original issue: [TODO: Add issue number]
- Current PR: [TODO: Add PR number]
- Future PR: Will implement this design (Phase 2)
