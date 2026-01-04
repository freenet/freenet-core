# Missing Benchmarks: Gap Analysis

## Current Coverage vs Real-World Needs

### What We Have ✅

**Micro-benchmarks (Level 0):**
- AES encryption/decryption
- Serialization
- Nonce generation
- Packet allocation

**Single Connection (Level 1-2):**
- Small message latency
- Warm connection throughput
- Cold start throughput
- Connection establishment
- Stream fragmentation/reassembly

**Component Tests:**
- Lock-free buffer operations
- Channel throughput

### What's MISSING ❌

## 1. **Multiple Concurrent Connections** (Critical for P2P)

**Problem**: Freenet is a P2P network - nodes maintain connections to many peers simultaneously.

**What exists**: All current benchmarks test single peer-to-peer connection

**What's missing**:
```rust
bench_multiple_connections() {
    // Test N concurrent connections (10, 50, 100 peers)
    // Measure:
    // - Aggregate throughput across all connections
    // - Per-connection fairness (does one connection starve others?)
    // - Global bandwidth pool behavior
    // - Memory usage scaling
    // - CPU usage scaling
}
```

**Why it matters**:
- Global bandwidth pool needs testing under real load
- Connection fairness (LEDBAT++ per-connection)
- Resource contention (channels, crypto, buffers)
- Scheduler/tokio runtime behavior

**Real-world scenario**: Node with 50 peers, each sending/receiving simultaneously

---

## 2. **Network Degradation Scenarios** (Resilience)

**Problem**: Real networks have packet loss, jitter, latency spikes

**What exists**: Tests with clean mock sockets (no packet loss)

**What's missing**:
```rust
bench_packet_loss() {
    // Test with 1%, 5%, 10% packet loss
    // Measure:
    // - Throughput degradation
    // - Retransmission overhead
    // - RTT estimation stability
    // - LEDBAT congestion window behavior
}

bench_latency_variation() {
    // Test with realistic jitter (±10ms, ±50ms)
    // Measure:
    // - LEDBAT queue delay tracking
    // - Throughput stability
    // - RTT variance impact
}

bench_latency_spike() {
    // Inject sudden latency spike (10ms → 200ms → 10ms)
    // Measure:
    // - Recovery time
    // - Connection stability
    // - Timeout behavior
}
```

**Why it matters**:
- Tests reliability layer (ack/retransmit)
- Validates LEDBAT++ behavior under adversity
- Real-world networks are never perfect

**Note**: `PacketDropPolicy` exists in mock transport but isn't benchmarked!

---

## 3. **Memory and Resource Efficiency**

**Problem**: Speed isn't everything - memory/CPU matter too

**What exists**: Time-based benchmarks (latency, throughput)

**What's missing**:
```rust
bench_memory_per_connection() {
    // Measure baseline memory per connection
    // With and without active transfers
}

bench_memory_under_load() {
    // Measure memory growth during sustained transfer
    // Detect memory leaks
    // Test buffer pool behavior
}

bench_cpu_efficiency() {
    // Measure CPU cycles per MB transferred
    // Compare encrypted vs baseline
    // Identify hot paths
}

bench_allocation_rate() {
    // Count allocations during transfer
    // Zero-copy vs copying paths
}
```

**Why it matters**:
- Nodes run 24/7 - memory leaks are fatal
- CPU efficiency affects battery life (mobile nodes)
- Allocation pressure causes GC pauses

**Tools**: Can use `criterion-perf-events`, `dhat`, or manual instrumentation

---

## 4. **Real-World Traffic Patterns**

**Problem**: Current tests use uniform message sizes or single transfers

**What exists**: Fixed sizes (64, 256, 1024, 1364, 4KB, 16KB, etc.)

**What's missing**:
```rust
bench_mixed_message_sizes() {
    // Realistic mix: 90% small (metadata), 10% large (data)
    // Measure average latency and throughput
}

bench_bursty_traffic() {
    // Idle → burst → idle pattern
    // Tests slow start recovery, idle detection
}

bench_bidirectional_transfer() {
    // Simultaneous send/receive on same connection
    // Tests channel contention, lock contention
}

bench_request_response_pattern() {
    // Small request → large response (DHT lookup pattern)
    // Tests realistic Freenet usage
}
```

**Why it matters**:
- Freenet isn't just bulk file transfer
- Contract operations: small requests, variable responses
- DHT lookups: tiny request, small response
- Delegate updates: medium-sized transfers

---

## 5. **Long-Running Stability**

**Problem**: All benchmarks run for seconds/minutes, not hours

**What exists**: Short-duration tests (3-30s)

**What's missing**:
```rust
bench_sustained_connection() {
    // Keep connection alive for 1+ hours
    // Periodic transfers (every 10s)
    // Measure:
    // - Latency drift over time
    // - Memory growth
    // - Connection stability
}

bench_connection_churn() {
    // Peers joining/leaving continuously
    // Measure:
    // - New connection overhead
    // - Cleanup efficiency
    // - Memory leak detection
}
```

**Why it matters**:
- Memory leaks only show up over time
- Timer drift, state accumulation
- Real P2P nodes run for days/weeks

**Implementation**: Not suitable for CI, but valuable for soak testing

---

## 6. **Large Transfers** (Contract/Delegate Distribution)

**Problem**: Largest current test is 1MB

**What exists**: Up to 1MB in `bench_1mb_transfer_validation`

**What's missing**:
```rust
bench_large_file_transfer() {
    // Test 10MB, 100MB, 1GB transfers
    // Measure:
    // - Sustained throughput
    // - Memory usage (streaming vs buffering)
    // - LEDBAT congestion window evolution
    // - Time to complete
}

bench_concurrent_large_transfers() {
    // Multiple large transfers simultaneously
    // Tests bandwidth sharing, fairness
}
```

**Why it matters**:
- Contracts can be large (MB-scale code bundles)
- Delegates contain data
- Tests streaming infrastructure under real load

---

## 7. **Backpressure and Flow Control**

**Problem**: What happens when receiver is slow?

**What exists**: Tests assume receiver keeps up

**What's missing**:
```rust
bench_slow_receiver() {
    // Receiver processes messages slower than sender sends
    // Measure:
    // - Backpressure propagation
    // - Buffer growth
    // - Sender throttling
}

bench_receiver_stall() {
    // Receiver stops processing entirely
    // Measure:
    // - Timeout behavior
    // - Buffer limits
    // - Connection recovery or failure
}
```

**Why it matters**:
- Real receivers have varying processing rates
- Prevents unbounded memory growth
- Tests channel backpressure mechanisms

---

## 8. **LEDBAT-Specific Validation**

**Problem**: LEDBAT++ has specific behaviors that need validation

**What exists**: Basic throughput tests

**What's missing**:
```rust
bench_ledbat_fairness() {
    // Multiple LEDBAT++ flows competing
    // Measure fairness (should be equal share)
}

bench_ledbat_competing_tcp() {
    // LEDBAT flow + TCP flow (simulated)
    // LEDBAT should yield to TCP
}

bench_ledbat_queue_delay_tracking() {
    // Verify queue delay measurement accuracy
    // Test against known artificial delays
}

bench_periodic_slowdown() {
    // Measure slowdown frequency and impact
    // Verify 4x reduction, 9x interval
}
```

**Why it matters**:
- LEDBAT++ is complex - implementation bugs possible
- Fairness is a core property
- Validates RFC compliance

---

## 9. **Encryption Overhead Under Load**

**Problem**: Crypto tests are micro-benchmarks, not integrated

**What exists**: `bench_aes_gcm_encrypt` tests AES in isolation

**What's missing**:
```rust
bench_encryption_overhead() {
    // Compare encrypted vs unencrypted throughput
    // Measure CPU % spent in crypto
}

bench_crypto_scaling() {
    // How does crypto cost scale with:
    // - Number of connections
    // - Message size
    // - Message rate
}
```

**Why it matters**:
- Understand real-world crypto tax
- Identify crypto as bottleneck (or not)

---

## 10. **Fragmentation and Reassembly**

**Problem**: Streaming tests exist but limited coverage

**What exists**: `bench_stream_throughput` (4KB, 16KB, 64KB)

**What's missing**:
```rust
bench_fragment_sizes() {
    // Test optimal fragment size
    // Measure throughput at different MSS values
}

bench_out_of_order_fragments() {
    // Fragments arrive out of order
    // Measure reassembly overhead
}

bench_lost_fragments() {
    // Some fragments lost, require retransmit
    // Measure recovery efficiency
}
```

**Why it matters**:
- Real UDP delivers out-of-order
- Fragment loss is common
- Tests streaming buffer under stress

---

## Priority Ranking

### P0: Critical for Real-World Validation

1. **Multiple concurrent connections** - Core P2P use case
2. **Packet loss scenarios** - Real networks aren't perfect
3. **Large file transfers (10MB+)** - Contract distribution
4. **Backpressure handling** - Prevent memory exhaustion

### P1: Important for Production Readiness

5. **Memory efficiency** - Long-running nodes
6. **Mixed traffic patterns** - Realistic usage
7. **Long-running stability** - Soak tests

### P2: Nice to Have

8. **LEDBAT-specific validation** - Already validated manually
9. **Encryption overhead** - Already know it's acceptable
10. **Advanced fragmentation** - Streaming works in practice

---

## Implementation Recommendations

### Phase 1: Add to CI (Quick Wins)

```rust
// crates/core/benches/transport_ci.rs
criterion_main!(
    warm_throughput_ci,        // ADD: Already exists!
    connection_setup_ci,       // Keep: Cold start matters
    streaming_buffer_ci,       // Keep: Critical path
    multiple_connections_ci,   // NEW: 10-50 concurrent peers
    packet_loss_1pct_ci,       // NEW: Test with 1% loss
);
```

**Effort**: 2-3 days to implement multi-connection and packet-loss benchmarks

### Phase 2: Extended Test Suite (Not CI)

```rust
// crates/core/benches/transport_soak.rs (manual only)
criterion_main!(
    large_file_transfers,      // 10MB, 100MB, 1GB
    long_running_stability,    // 1+ hour tests
    memory_leak_detection,     // Measure growth over time
);
```

**Run**: Weekly/monthly on dedicated hardware, not in CI

### Phase 3: Specialized Validation

```rust
// crates/core/benches/transport_ledbat_validation.rs (extended)
criterion_main!(
    ledbat_fairness,           // Multiple flows
    ledbat_vs_tcp,             // Yielding behavior
    queue_delay_accuracy,      // Measurement validation
);
```

**Run**: During LEDBAT changes, not every PR

---

## Existing Infrastructure We Can Leverage

**Already exists in codebase:**
- `PacketDropPolicy` - Can simulate packet loss!
- `MockSocket` with delay - Can add jitter
- `create_mock_peer` - Can create multiple peers
- `Channels` (DashMap) - Already supports multiple connections

**Just need to write the benchmark functions!**

---

## Recommended Next Steps

1. **Immediate**: Add `bench_warm_connection_throughput` to CI (already exists!)
2. **Week 1**: Implement `bench_multiple_connections` (10, 25, 50 peers)
3. **Week 2**: Implement `bench_packet_loss` (1%, 5% using existing `PacketDropPolicy`)
4. **Week 3**: Implement `bench_large_file_transfer` (10MB baseline)
5. **Month 2**: Design soak test infrastructure (not CI, scheduled runs)

Total estimated effort: ~2 weeks for P0 benchmarks
