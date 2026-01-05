# Benchmark Coverage Gaps

This document tracks known gaps in the transport benchmark suite and their potential impact on regression detection.

## Current Coverage Summary

| Category | CI (`transport_ci`) | Extended (`transport_extended`) | Notes |
|----------|---------------------|--------------------------------|-------|
| Cold start throughput | ✅ 16KB | ✅ + cwnd evolution | |
| Warm connection | ✅ 16KB | ✅ 1KB-16KB | Small warmup (1KB) to avoid cwnd exhaustion |
| Connection establishment | ✅ | - | |
| Streaming buffer ops | ✅ | - | Lock-free buffer critical path |
| RTT scenarios | - | ✅ 0-50ms | See gap #4 below |
| Sustained throughput | - | ✅ 16KB-256KB | |
| Packet loss | - | ✅ 1% | |
| Large files | - | ✅ 512KB-1MB | |
| Micro-benchmarks | - | ✅ AES, serialization, allocation | |

## Critical Gaps

### Gap 1: Memory Usage ❌ NOT MEASURED

**What's missing**: No benchmarks track memory consumption during transfers.

**Potential regressions undetected**:
- Memory leaks in long-running transfers
- Excessive buffer allocations in streaming
- Growing hash maps in connection/stream tracking
- Peak memory during large file transfers

**Impact**: HIGH - Could ship code that slowly consumes all memory on gateways handling many connections.

**Suggested implementation**:
```rust
// Track peak RSS during benchmark
use jemalloc_ctl::{stats, epoch};

fn measure_peak_memory<F: FnOnce()>(f: F) -> usize {
    epoch::advance().unwrap();
    let before = stats::allocated::read().unwrap();
    f();
    epoch::advance().unwrap();
    let after = stats::allocated::read().unwrap();
    after.saturating_sub(before)
}
```

**Effort**: Medium (requires jemalloc or similar allocator instrumentation)

---

### Gap 2: Bidirectional Simultaneous Transfer ❌ NOT MEASURED

**What's missing**: All benchmarks are unidirectional (A→B). Real P2P traffic is often bidirectional.

**Potential regressions undetected**:
- Deadlocks when both peers send simultaneously
- ACK processing issues under bidirectional load
- cwnd calculation errors with mixed send/recv
- Buffer contention in `PeerConnection`

**Impact**: HIGH - Bidirectional transfer is the common case in P2P applications.

**Suggested implementation**:
```rust
fn bench_bidirectional_transfer(c: &mut Criterion) {
    // Spawn both directions simultaneously
    let (send_a_to_b, send_b_to_a) = tokio::join!(
        async { conn_a.send(message_a).await },
        async { conn_b.send(message_b).await },
    );

    let (recv_a, recv_b) = tokio::join!(
        async { conn_a.recv().await },
        async { conn_b.recv().await },
    );
}
```

**Effort**: Low (straightforward extension of existing benchmarks)

---

### Gap 3: Connection Churn / Many Short Connections ❌ NOT MEASURED

**What's missing**: No benchmark tests rapid connect/disconnect cycles or many concurrent connection establishments.

**Potential regressions undetected**:
- Resource leaks on connection close
- Slowdown with many concurrent handshakes
- Connection pool exhaustion
- Cryptographic key generation bottlenecks

**Impact**: HIGH - Gateways handle hundreds of peers connecting/disconnecting continuously.

**Suggested implementation**:
```rust
fn bench_connection_churn(c: &mut Criterion) {
    // Measure: 100 sequential connect → transfer → disconnect cycles
    for _ in 0..100 {
        let conn = peer_a.connect(peer_b_pub, peer_b_addr).await?;
        conn.send(small_message).await?;
        drop(conn);  // Disconnect
    }
}

fn bench_concurrent_connections(c: &mut Criterion) {
    // Measure: 50 simultaneous connection establishments
    let futures: Vec<_> = (0..50)
        .map(|i| create_and_connect_peer(i))
        .collect();
    futures::future::join_all(futures).await;
}
```

**Effort**: Medium (requires careful resource management in test setup)

---

### Gap 4: High RTT Paths (100ms+) ⚠️ PARTIALLY COVERED

**What's missing**: RTT scenarios only test up to 50ms. Intercontinental paths can have 100-300ms RTT.

**Current coverage** (in `slow_start.rs`):
```rust
for &rtt_ms in &[0, 5, 10, 25, 50] {  // Missing: 100, 200, 300ms
```

**Potential regressions undetected**:
- LEDBAT "ssthresh death spiral" on high-latency paths
- Timeout calculation errors at high RTT
- Slow start behavior degradation
- BDP estimation issues

**Impact**: MEDIUM - Affects users on intercontinental connections.

**Suggested implementation**:
```rust
// Extend RTT scenarios
for &rtt_ms in &[0, 5, 10, 25, 50, 100, 200, 300] {
    // ... existing benchmark code
}
```

**Effort**: Low (just extend existing benchmark)

**Note**: High RTT benchmarks are slow with real-time delays. This is a good candidate for **virtual time optimization** - see [Future: Virtual Time Benchmarks](#future-virtual-time-benchmarks).

---

### Gap 5: Timeout/Retransmission Behavior ❌ NOT DIRECTLY MEASURED

**What's missing**: Packet loss benchmark (1%) tests retransmission indirectly, but doesn't measure:
- RTO (Retransmission Timeout) calculation accuracy
- Time to recover from packet loss bursts
- Behavior under sustained loss (5-10%)

**Potential regressions undetected**:
- RTO calculation bugs causing premature or delayed retransmissions
- Slow recovery from packet loss bursts
- Throughput collapse under moderate loss

**Impact**: MEDIUM - Affects reliability on lossy networks (WiFi, mobile).

**Suggested implementation**:
```rust
fn bench_loss_recovery_time(c: &mut Criterion) {
    // Measure time from packet loss to successful delivery
    // Use PacketDropPolicy::Ranges to drop specific packets
    let policy = PacketDropPolicy::Ranges(vec![10..15]);  // Drop packets 10-14

    let start = Instant::now();
    conn.send(large_message).await?;
    conn.recv().await?;  // Blocks until retransmission succeeds
    let recovery_time = start.elapsed();
}
```

**Effort**: Medium (requires careful timing and packet drop coordination)

---

### Gap 6: Multiple Concurrent Connections (Different Peers) ⚠️ PARTIALLY COVERED

**What's missing**: `bench_concurrent_streams` tests multiple streams on the **same connection**, not multiple connections to **different peers**.

**Potential regressions undetected**:
- Token bucket fairness between different connections
- Global bandwidth pool contention
- Per-connection vs global rate limiting bugs

**Impact**: MEDIUM - Gateways serve many peers simultaneously.

**Suggested implementation**:
```rust
fn bench_multi_peer_fairness(c: &mut Criterion) {
    // Create 10 different peer pairs
    let peers: Vec<_> = (0..10)
        .map(|_| create_connected_peers())
        .collect();

    // All transfer simultaneously
    let futures: Vec<_> = peers.iter_mut()
        .map(|p| p.conn_a.send(message.clone()))
        .collect();

    futures::future::join_all(futures).await;

    // Verify fairness: all should complete within 2x of each other
}
```

**Effort**: Medium

---

### Gap 7: Backpressure / Flow Control ❌ NOT MEASURED

**What's missing**: No benchmark tests behavior when the receiver is slow (application backpressure).

**Potential regressions undetected**:
- Send buffer overflow when receiver can't keep up
- Deadlocks when cwnd fills and receiver doesn't drain
- Memory growth when sends queue up

**Impact**: LOW-MEDIUM - Affects applications with slow message processing.

**Suggested implementation**:
```rust
fn bench_slow_receiver(c: &mut Criterion) {
    // Receiver adds artificial delay
    let receiver = tokio::spawn(async move {
        loop {
            let msg = conn_b.recv().await?;
            tokio::time::sleep(Duration::from_millis(100)).await;  // Slow processing
        }
    });

    // Sender sends as fast as possible
    for _ in 0..100 {
        conn_a.send(message.clone()).await?;  // Should eventually block
    }
}
```

**Effort**: Medium (requires careful synchronization)

---

## Gap Priority Matrix

| Gap | Risk Level | Detection Value | Effort | Virtual Time Helps? |
|-----|------------|-----------------|--------|---------------------|
| Memory usage | High | High | Medium | No |
| Bidirectional transfer | High | High | Low | Yes |
| Connection churn | High | High | Medium | Yes |
| High RTT (100ms+) | Medium | High | Low | **Yes (10x faster)** |
| Timeout behavior | Medium | Medium | Medium | **Yes (instant)** |
| Multi-peer concurrent | Medium | Medium | Medium | Yes |
| Backpressure | Low | Medium | Medium | Partially |

---

## Future: Virtual Time Benchmarks

Several gaps (especially high RTT and timeout testing) would benefit from **virtual time benchmarks** that don't require real wall-clock delays.

### Concept

Instead of:
```rust
// Real time: 300ms RTT = benchmark takes minutes
PacketDelayPolicy::Fixed(Duration::from_millis(150))  // 150ms one-way = 300ms RTT
tokio::time::sleep(delay).await;  // Actually waits 150ms
```

Use:
```rust
// Virtual time: 300ms RTT = benchmark completes in milliseconds
time_source.sleep(delay).await;  // Returns immediately
time_source.advance(Duration::from_millis(150));  // Advances virtual clock
// Measure: bytes_transferred / virtual_time_elapsed
```

### Benefits

- **High RTT scenarios**: Test 500ms RTT without waiting 500ms per round-trip
- **Timeout testing**: Test 30-second timeouts in milliseconds
- **Deterministic**: Same seed = same results, no runner variance
- **Faster CI**: Extended benchmarks could run 10-100x faster

### Requirements

1. Make `MockSocket` generic over `TimeSource`
2. Use `time_source.sleep()` instead of `tokio::time::sleep()`
3. Custom Criterion measurement that reports virtual time throughput
4. See: `src/simulation/time.rs` for existing `VirtualTime` implementation

### Implementation Status

- [x] `VirtualTime` exists in `simulation/time.rs`
- [x] Transport components (`LedbatController`, `TokenBucket`, etc.) support `TimeSource` generic
- [ ] `MockSocket` uses `tokio::time::sleep()` directly (needs refactor)
- [ ] Criterion custom measurement adapter (needs implementation)
- [ ] Virtual time benchmark variants (needs implementation)

---

## Changelog

| Date | Change |
|------|--------|
| 2025-01-05 | Initial gap analysis |
