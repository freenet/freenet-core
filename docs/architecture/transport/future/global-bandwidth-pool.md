# Global Bandwidth Pool

> **✅ IMPLEMENTED** - This feature was implemented in PR #2342 (December 2025).

**Status**: Complete
**Module**: `crates/core/src/transport/global_bandwidth.rs`

## Problem Statement

Users want to configure total bandwidth (e.g., "I have 500 Mbps, let Freenet use 50 MB/s total") rather than per-connection limits.

**Previous behavior:**
- Each connection gets independent `bandwidth_limit` (default 10 MB/s)
- N connections can use N × 10 MB/s total bandwidth
- Users must reason about "per-connection" instead of "total available"

**New behavior:**
```toml
[network-api]
total-bandwidth-limit = 50000000           # 50 MB/s total across all connections
min-bandwidth-per-connection = 1000000     # 1 MB/s minimum per connection
```

## Design: Simple Global Rate Derivation

The implementation uses **atomic connection counting** rather than hierarchical token buckets. This approach was chosen because:

- ✅ Lock-free (just atomic counter operations)
- ✅ LEDBAT remains primary controller (network-responsive)
- ✅ Minimal code changes to existing architecture
- ✅ Predictable: N connections = total/N per connection
- ✅ Easy to debug and reason about

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    PeerConnection                           │
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │    LEDBAT    │    │   Global     │    │  TokenBucket │  │
│  │  Controller  │    │  Bandwidth   │    │   (Pacer)    │  │
│  │              │    │   Manager    │    │              │  │
│  │ Measures RTT │    │ Counts conns │    │ Paces packets│  │
│  │ & queuing    │    │ Divides fair │    │ at final     │  │
│  │ delay        │    │ share        │    │ rate         │  │
│  └──────┬───────┘    └──────┬───────┘    └──────▲───────┘  │
│         │                   │                   │          │
│         │   ledbat_rate     │   global_rate     │          │
│         └─────────┬─────────┘                   │          │
│                   │                             │          │
│                   ▼                             │          │
│            ┌──────────────┐                     │          │
│            │  min(a, b)   │─────────────────────┘          │
│            └──────────────┘     final_rate                 │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Core Implementation

```rust
/// Global bandwidth manager using atomic connection counting.
pub struct GlobalBandwidthManager {
    /// Total bandwidth cap (bytes/sec)
    total_limit: usize,

    /// Active connection count (lock-free)
    connection_count: AtomicUsize,

    /// Minimum rate per connection (prevents starvation)
    min_per_connection: usize,
}

impl GlobalBandwidthManager {
    /// Register new connection, returns per-connection rate
    pub fn register_connection(&self) -> usize {
        let count = self.connection_count.fetch_add(1, Ordering::AcqRel) + 1;
        self.compute_per_connection_rate(count)
    }

    /// Unregister closed connection (uses fetch_update to prevent underflow)
    pub fn unregister_connection(&self) -> usize {
        let result = self.connection_count.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |count| if count > 0 { Some(count - 1) } else { None },
        );
        // ... returns new per-connection rate
    }

    /// Compute fair share: total_limit / active_connections
    fn compute_per_connection_rate(&self, count: usize) -> usize {
        let fair_share = self.total_limit / count.max(1);
        fair_share.max(self.min_per_connection)
    }
}
```

### Integration Points

**1. Connection registration** (`connection_handler.rs`):
```rust
let initial_rate = if let Some(ref global) = global_bandwidth {
    global.register_connection()
} else {
    bandwidth_limit.unwrap_or(10_000_000)
};
let token_bucket = Arc::new(TokenBucket::new(10_000, initial_rate));
```

**2. Connection cleanup** (`peer_connection.rs` - Drop impl):
```rust
impl<S> Drop for RemoteConnection<S> {
    fn drop(&mut self) {
        if let Some(ref global) = self.global_bandwidth {
            global.unregister_connection();
        }
    }
}
```

**3. Rate updates** (`peer_connection.rs` - recv loop):
```rust
// Take minimum of LEDBAT rate and global fair share
let (new_rate, global_limit) = if let Some(ref global) = self.remote_conn.global_bandwidth {
    let global_rate = global.current_per_connection_rate();
    (ledbat_rate.min(global_rate), Some(global_rate))
} else {
    (ledbat_rate, None)
};
self.remote_conn.token_bucket.set_rate(new_rate);
```

## Configuration

### CLI Arguments

```bash
freenet --total-bandwidth-limit 50000000 --min-bandwidth-per-connection 1000000
```

### Config File (TOML)

```toml
[network-api]
total-bandwidth-limit = 50000000           # 50 MB/s total
min-bandwidth-per-connection = 1000000     # 1 MB/s minimum
```

### Rust API

```rust
pub struct NetworkApiConfig {
    /// Total bandwidth limit across ALL connections (bytes/sec).
    /// When set, per-connection rates are computed as: total / active_connections.
    /// This overrides the per-connection bandwidth_limit.
    pub total_bandwidth_limit: Option<usize>,

    /// Minimum bandwidth per connection (bytes/sec).
    /// Prevents connection starvation when many connections are active.
    /// Default: 1 MB/s (1,000,000 bytes/second)
    pub min_bandwidth_per_connection: Option<usize>,

    /// Per-connection bandwidth limit (bytes/sec).
    /// Used if total_bandwidth_limit is not set.
    /// Default: 10 MB/s
    pub bandwidth_limit: Option<usize>,
}
```

## Behavior Examples

### User with 500 Mbps (62.5 MB/s) connection:

```toml
[network-api]
total-bandwidth-limit = 50000000           # 50 MB/s total
min-bandwidth-per-connection = 1000000     # 1 MB/s minimum
```

| Active Connections | Per-Connection Rate | Total Usage |
|-------------------|---------------------|-------------|
| 1 | 50 MB/s | 50 MB/s |
| 5 | 10 MB/s | 50 MB/s |
| 10 | 5 MB/s | 50 MB/s |
| 50 | 1 MB/s (min enforced) | 50 MB/s |
| 100 | 1 MB/s (min enforced) | ~100 MB/s (exceeds cap) |

### Backward compatibility (current behavior):

```toml
[network-api]
bandwidth-limit = 10000000  # 10 MB/s per connection
# No total-bandwidth-limit set = per-connection behavior
```

## LEDBAT Integration

The global bandwidth pool is designed to **not interfere** with LEDBAT's delay-based congestion control:

1. **LEDBAT calculates rate** based on measured one-way delay
2. **Global pool calculates rate** based on `total / active_connections`
3. **TokenBucket uses `min(ledbat_rate, global_rate)`**

This means:
- LEDBAT remains the primary congestion controller (responds to network conditions)
- Global pool acts as a ceiling (enforces operator's bandwidth budget)
- No artificial delay that could confuse LEDBAT's delay measurements

### Rate Update Frequency

Rates are updated on an RTT-adaptive schedule:
- Low RTT (< 10ms): Update every ~50ms
- Medium RTT (10-100ms): Update every ~100-200ms
- High RTT (> 100ms): Update every ~500ms

## Testing

The implementation includes comprehensive tests:

### Unit Tests (11 total in `global_bandwidth.rs`)

| Test | Description |
|------|-------------|
| `test_single_connection` | Single connection gets full bandwidth |
| `test_multiple_connections_fair_share` | Fair distribution across N connections |
| `test_minimum_enforcement` | Minimum rate prevents starvation |
| `test_connection_handle_auto_unregister` | RAII handle cleanup |
| `test_handle_current_rate` | Rate updates visible to all handles |
| `test_underflow_protection` | Safe behavior when count is 0 |
| `test_handle_clone_registers_new_connection` | Clone semantics documented |
| `test_concurrent_register_unregister` | 50 threads × 100 ops stress test |
| `test_concurrent_rate_queries_during_churn` | Concurrent reads during writes |
| `test_large_connection_count` | Scale test with 10,000 connections |
| `test_zero_total_limit` | Edge case handling |

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
- ✅ Lock-free (atomic `fetch_update` prevents underflow)
- ✅ LEDBAT remains primary controller (network-responsive)
- ✅ Minimal code changes to existing architecture
- ✅ Predictable: N connections = total/N per connection
- ✅ Easy to debug and reason about

**Trade-off accepted:**
- Brief overages possible if all connections burst simultaneously
- But LEDBAT + TokenBucket smooth bursts naturally
- Acceptable for background traffic use case

## Source Code Locations

- **GlobalBandwidthManager**: `crates/core/src/transport/global_bandwidth.rs`
- **Integration (handler)**: `crates/core/src/transport/connection_handler.rs:965-975`
- **Integration (cleanup)**: `crates/core/src/transport/peer_connection.rs:84-96`
- **Integration (rate update)**: `crates/core/src/transport/peer_connection.rs:717-726`
- **Configuration**: `crates/core/src/config/mod.rs` (NetworkApiConfig)

## References

- LEDBAT: [RFC 6817](https://tools.ietf.org/html/rfc6817)
- TokenBucket: `crates/core/src/transport/token_bucket.rs`
- Implementation PR: #2342
