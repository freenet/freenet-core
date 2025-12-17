# Transport Bandwidth Configuration

## Summary

The Freenet transport layer now supports configurable bandwidth limits through the `bandwidth_limit` parameter in `create_connection_handler()`.

## Key Findings

### Bottleneck Analysis

After implementing LEDBAT slow start with IW26 and cwnd enforcement, we discovered that the **token bucket rate limiter** was the actual throughput ceiling, not LEDBAT's congestion window.

| Component | Configuration | Theoretical Max |
|-----------|---------------|-----------------|
| LEDBAT cwnd | 1 GB max | ~10 GB/s @ 100ms RTT |
| Token Bucket | 10 MB/s default | **10 MB/s** (bottleneck) |

At 10ms RTT with high bandwidth:
- **LEDBAT alone** could theoretically support ~100 GB/s (1GB cwnd / 10ms RTT)
- **Token bucket** limits actual throughput to 10 MB/s

### Configuration Interface

```rust
// In connection_handler.rs
pub(crate) async fn create_connection_handler<S: Socket>(
    socket: Arc<S>,
    keypair: TransportKeypair,
    listen_host: IpAddr,
    listen_port: u16,
    is_gateway: bool,
    bandwidth_limit: Option<usize>,  // ← Controls token bucket rate (bytes/sec)
) -> Result<(OutboundConnectionHandler, InboundConnectionHandler), TransportError>
```

**Default behavior:**
- `None` → 10 MB/s (10,000,000 bytes/sec)
- `Some(100_000_000)` → 100 MB/s

### New Test Infrastructure

Added `create_mock_peer_with_bandwidth()` for testing:

```rust
// In mock_transport module
pub async fn create_mock_peer_with_bandwidth(
    packet_drop_policy: PacketDropPolicy,
    packet_delay_policy: PacketDelayPolicy,
    channels: Channels,
    bandwidth_limit: Option<usize>,
) -> anyhow::Result<(TransportPublicKey, OutboundConnectionHandler, SocketAddr)>
```

### Benchmark Added

New benchmark in `transport_perf.rs`:

```rust
slow_start_validation::bench_high_bandwidth_throughput
```

This tests 1MB transfers with:
- 100 MB/s bandwidth limit (10x default)
- 10ms RTT
- IW26 slow start
- Full cwnd enforcement

**Expected results:**
- With 10 MB/s limit: ~10 MB/s throughput (bottlenecked by token bucket)
- With 100 MB/s limit: >10 MB/s throughput (now cwnd-gated, should see improvement)

## Implications for >3 MB/s Goal

### Current State

With the default 10 MB/s token bucket:
- ✅ Per-stream throughput can reach **up to 10 MB/s** (well above 3 MB/s target)
- ✅ LEDBAT slow start + IW26 helps reach this limit faster
- ✅ cwnd enforcement ensures "good network citizen" behavior
- ✅ Multiple connections share bandwidth fairly through per-connection token buckets

### High-Bandwidth Scenarios

For users on gigabit+ connections:
1. **Configuration is available** - `bandwidth_limit` parameter exists
2. **Needs exposure** - Currently only accessible at connection handler creation
3. **Testing required** - Benchmarks should verify >10 MB/s achievable

### Fairness Between Connections

The current design provides fairness:
- Each connection has its own token bucket (initialized from `bandwidth_limit`)
- Each connection has its own LEDBAT controller
- Connections compete for actual network resources naturally
- LEDBAT's delay-based signaling helps connections yield to foreground traffic

## Next Steps

1. **Expose bandwidth_limit** in higher-level APIs (e.g., node configuration)
2. **Run high-bandwidth benchmarks** to verify >10 MB/s achievable
3. **Document configuration** for users who need custom bandwidth limits
4. **Consider adaptive limits** - could auto-detect available bandwidth

## Files Modified

### Core Implementation
- `crates/core/src/transport/connection_handler.rs`
  - Added `new_test_with_bandwidth()` (line 244-253)
  - Updated `create_mock_peer_internal()` to accept bandwidth_limit (line 1818-1859)
  - Added `create_mock_peer_with_bandwidth()` (line 1790-1799)

- `crates/core/src/transport/peer_connection/outbound_stream.rs`
  - Lines 62-97: cwnd enforcement with exponential backoff
  - Lines 107-117: Token bucket rate limiting

- `crates/core/src/transport/ledbat.rs`
  - Lines 70-84: IW26 configuration (38KB initial window)

### Benchmarks
- `crates/core/benches/transport_perf.rs`
  - Lines 1786-1858: `bench_high_bandwidth_throughput()`
  - Line 1976: Added to `slow_start` criterion_group

## References

- Phase 1 optimization: IW26 + cwnd enforcement (outbound_stream.rs)
- Token bucket implementation: connection_handler.rs:959-963
- LEDBAT configuration: ledbat.rs:70-84
