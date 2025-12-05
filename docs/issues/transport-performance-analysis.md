# Transport Layer Performance Analysis and Benchmark Suite

## Summary

This PR introduces a comprehensive performance analysis of the Freenet transport layer, identifying critical bottlenecks and providing a benchmark suite for ongoing performance monitoring.

**Key Finding**: Syscall overhead (70% of per-packet time) is the primary bottleneck, not encryption or serialization.

---

## Benchmark Results

### Per-Packet Time Breakdown (1364 bytes)

| Component | Time | % of Total |
|-----------|------|------------|
| **UDP syscall (send)** | **12.7µs** | **70%** |
| Serialization (bincode) | 3.82µs | 21% |
| Encryption (AES-128-GCM) | 1.14µs | 6% |
| Channel/async overhead | ~0.5µs | 3% |
| **Total** | **~18µs** | 100% |

### Current Throughput Limits

| Metric | Value |
|--------|-------|
| Max packets/sec | ~80,000 pps |
| Max bandwidth | ~870 Mbps |
| Theoretical (no syscall bottleneck) | ~2.2 Gbps |

---

## Critical Issues Identified

### 1. Channel Buffer Size = 1 (HIGH PRIORITY)

**Location**: `crates/core/src/transport/peer_connection.rs:592`

```rust
let (sender, receiver) = mpsc::channel(1);  // BOTTLENECK
```

**Impact**: Benchmark shows 27x throughput difference between `channel(1)` and `channel(100)`:

| Buffer Size | Throughput |
|-------------|------------|
| 1 | 80K elem/s |
| 100 | **2.2M elem/s** |

**Fix**: Change to `mpsc::channel(100)` - trivial, zero-risk change.

---

### 2. Random Nonce Generation (MEDIUM PRIORITY)

**Location**: `crates/core/src/transport/packet_data.rs:151`

```rust
let nonce: [u8; NONCE_SIZE] = RNG.with(|rng| rng.borrow_mut().random());
```

**Benchmark**:
| Method | Time |
|--------|------|
| Random | 34.7ns |
| Counter | 6.3ns |

**Impact**: 5.5x faster with counter-based nonces. Counter nonces are equally secure for AES-GCM when combined with a unique key per connection.

**Fix**: Use atomic counter + connection ID for nonce generation.

---

### 3. No Syscall Batching (HIGH PRIORITY - LARGER EFFORT)

**Current**: One `send()` syscall per packet = 12.7µs overhead each.

**Solution**: Use `sendmmsg()`/`recvmmsg()` to batch multiple packets per syscall.

**Expected Impact**: 10x reduction in syscall overhead, potentially reaching 800K+ pps.

**Implementation**: Requires `socket2` crate and platform-specific code paths.

---

### 4. Serialization Overhead (MEDIUM PRIORITY - LARGER EFFORT)

Bincode serialization is 21% of packet creation time (3.82µs for 1364 bytes).

**Potential Optimizations**:
- Pre-computed message templates for common patterns
- Zero-copy serialization (rkyv, flatbuffers)
- Avoid Vec allocations in hot path

---

## Files Added

### Documentation
- `docs/architecture/transport_perf_analysis.md` - Full architecture analysis
- `docs/architecture/transport_benchmark_methodology.md` - Benchmarking methodology

### Benchmarks
- `crates/core/benches/transport_perf.rs` - Criterion benchmark suite

### Scripts
- `scripts/run_benchmarks.sh` - Helper script with environment validation

---

## Benchmark Suite

### Levels

| Level | What it Measures | Noise Level | CI Safe |
|-------|-----------------|-------------|---------|
| 0 | Pure computation (encrypt, serialize) | <2% | ✅ |
| 1 | Protocol logic (channels, routing) | ~5% | ✅ |
| 2 | Syscall overhead (loopback UDP) | ~10% | ⚠️ |
| 3 | System limits (stress tests) | ~15% | ❌ |

### Running

```bash
# All benchmarks (without tracing for accurate results)
cargo bench --bench transport_perf --no-default-features --features "redb,websocket"

# Specific levels
cargo bench --bench transport_perf --no-default-features --features "redb,websocket" -- "level0/"

# Using helper script
./scripts/run_benchmarks.sh level0 level1
```

---

## Immediate Action Items

### Trivial Fixes (< 1 hour each)

- [ ] **Change channel buffer 1 → 100** in `peer_connection.rs:592`
  - Impact: 27x channel throughput
  - Risk: None

- [ ] **Switch to counter-based nonces** in `packet_data.rs`
  - Impact: 5.5x faster nonce generation
  - Risk: None (counter + connection ID is standard practice)

### Medium-Term (1-2 days each)

- [ ] **Implement sendmmsg/recvmmsg batching**
  - Impact: ~10x syscall efficiency
  - Requires: `socket2` crate, platform detection

- [ ] **Increase inbound channel buffers** in `connection_handler.rs`
  - Related to documented packet drops (2251 packets in 10s)

### Longer-Term

- [ ] **Zero-copy serialization** investigation
- [ ] **GSO/GRO support** for kernel offload
- [ ] **io_uring** for Linux 5.1+

---

## Validation

The benchmark suite allows validating any optimization:

```bash
# Before change
git stash
./scripts/run_benchmarks.sh level0 level1

# After change
git stash pop
./scripts/run_benchmarks.sh level0 level1

# Criterion automatically shows delta
```

---

## Related

- Packet drop warnings in `connection_handler.rs:339`
- Disabled rate limiter comment in `connection_handler.rs:155-160`
- TODO comments in `peer_connection.rs:35` and `outbound_stream.rs:53-57`
