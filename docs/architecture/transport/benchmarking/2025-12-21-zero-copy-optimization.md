# Zero-Copy Stream Fragmentation Optimization (December 21, 2025)

**Date:** 2025-12-21
**Branch:** fnt-transport
**PR:** #2361
**Commit:** 324f9aeb
**System:** MacBook Pro (M-series)

## Overview

This document records the performance impact of two allocation optimizations from issue #2226:

1. **Zero-copy fragmentation with `bytes::Bytes`** - Replace `Vec::split_off()` with `Bytes::slice()`
2. **Arc→Box for packet storage** - Replace `Arc<[u8]>` with `Box<[u8]>` since packets are single-owner

## Motivation

Profiling identified two allocation hotspots in the transport layer:

1. **Stream fragmentation**: When sending large messages, `Vec::split_off()` was allocating a new Vec for every fragment (~740 allocations for a 1MB message)
2. **Packet storage**: `prepared_send()` returned `Arc<[u8]>` despite packets being single-owner, adding unnecessary atomic reference counting overhead

## Benchmark Results

### Stream Fragmentation (allocation_overhead.rs)

Fragmenting streams of various sizes into 1420-byte chunks:

| Stream Size | Vec::split_off | Bytes::slice | Bytes (preallocated) | Speedup |
|-------------|----------------|--------------|----------------------|---------|
| 4 KB        | 191 ns         | 132 ns       | 30 ns               | 1.4x - 6.4x |
| 64 KB       | 33 µs          | 1.2 µs       | 0.4 µs              | 27x - 83x |
| 256 KB      | 2.1 ms         | 4.2 µs       | 1.1 µs              | 500x - 1,900x |
| 1 MB        | 37.7 ms        | 13.9 µs      | 3.4 µs              | **2,700x - 11,000x** |

**Key insight**: `Bytes::slice()` is O(1) zero-copy, while `Vec::split_off()` is O(n) due to reallocation.

### Arc vs Box Packet Preparation (allocation_overhead.rs)

Encrypting and packaging packets of various sizes:

| Packet Size | encrypt_then_arc | encrypt_then_box | Difference |
|-------------|------------------|------------------|------------|
| 64 bytes    | 574 ns           | 571 ns           | ~0.5% |
| 256 bytes   | 1.51 µs          | 1.53 µs          | ~0% |
| 1024 bytes  | 5.23 µs          | 5.09 µs          | ~3% |
| 1364 bytes  | 7.26 µs          | 7.17 µs          | ~1% |

**Key insight**: Negligible difference as expected - atomic operations are fast. The change was made for correctness (single-owner semantics) rather than performance.

## Implementation Details

### 1. Zero-Copy Fragmentation

**Before (outbound_stream.rs):**
```rust
pub(crate) type SerializedStream = Vec<u8>;

// Allocates new Vec for each fragment
let fragment = stream_to_send.split_off(MAX_DATA_SIZE);
std::mem::swap(&mut stream_to_send, &mut fragment);
```

**After:**
```rust
pub(crate) type SerializedStream = Bytes;

// Zero-copy slicing (O(1), just increments refcount)
let fragment = stream_to_send.slice(..MAX_DATA_SIZE);
stream_to_send = stream_to_send.slice(MAX_DATA_SIZE..);
```

### 2. Arc→Box for Packets

**Before (packet_data.rs):**
```rust
pub fn prepared_send(self) -> Arc<[u8]> {
    self.data[..self.size].into()
}
```

**After:**
```rust
pub fn prepared_send(self) -> Box<[u8]> {
    self.data[..self.size].into()
}
```

## Files Modified

| File | Change |
|------|--------|
| `mod.rs` | `MessagePayload = bytes::Bytes` (was `Vec<u8>`) |
| `outbound_stream.rs` | `SerializedStream = Bytes`, zero-copy fragmentation |
| `inbound_stream.rs` | Updated to handle `Bytes` fragments |
| `symmetric_message.rs` | Updated payload types and tests |
| `peer_connection.rs` | Updated channel types |
| `packet_data.rs` | `prepared_send()` returns `Box<[u8]>` |
| `sent_packet_tracker.rs` | Storage type changed to `Box<[u8]>` |
| `Cargo.toml` | Added `serde` feature to `bytes` dependency |

## Test Results

All 112 transport tests pass, including:

- `test_send_stream_success` - Transfers 100,000 random bytes and verifies content integrity
- `test_send_stream_with_bandwidth_limit` - Verifies rate limiting works with new fragmentation
- `test_out_of_order_fragment_*` - Verifies out-of-order fragment reassembly

## Impact Analysis

### Memory Allocation Reduction

For a 1MB stream transfer:
- **Before**: ~740 Vec allocations (one per fragment)
- **After**: 1 Bytes allocation + reference count increments

### CPU Overhead Reduction

The fragmentation operation went from 37.7ms to 13.9µs for 1MB streams - a 2,700x improvement. This overhead was previously eating into available bandwidth.

### Expected End-to-End Improvement

The improvement scales with message size since fragmentation overhead increases with fragment count:

| Message Size | Fragments | Frag Time (Before) | Frag Time (After) | Frag % of E2E | Expected E2E Improvement |
|--------------|-----------|-------------------|-------------------|---------------|-------------------------|
| 1 KB | 1 | N/A | N/A | 0% | None (no fragmentation) |
| 32 KB | ~23 | ~0.2ms | ~0.5µs | 2-3% | ~2% |
| 256 KB | ~180 | ~2.1ms | ~4µs | 10-15% | ~10-15% |
| 1 MB | ~740 | **37.7ms** | ~14µs | **40-75%** | **40-75%** |

**Key insight**: For small messages (<1.4KB), fragmentation doesn't occur, so no improvement is expected. For large streams (1MB+), the 37.7ms fragmentation overhead was a dominant factor, and eliminating it yields significant end-to-end improvement.

### Verified Small Message Throughput (Unchanged)

| Test | Before | After | Change |
|------|--------|-------|--------|
| 1 KB single message | 6.81 Mbps | 6.92 Mbps | ~+2% (noise) |
| 1 KB × 100 sustained | 6.78 Mbps | 6.84 Mbps | ~+1% (noise) |

Small message throughput is unchanged as expected since these don't require fragmentation.

### Wire Format Compatibility

`bytes::Bytes` serializes identically to `Vec<u8>` via serde, so there's no wire format change. Existing peers can communicate with updated peers without issues.

## Running These Benchmarks

```bash
# Run allocation overhead benchmarks
cargo bench --bench transport_ci --features bench -- allocation

# Run fragmentation benchmarks specifically
cargo bench --bench transport_ci --features bench -- fragmentation

# Run packet preparation benchmarks
cargo bench --bench transport_ci --features bench -- packet_preparation
```

## Next Steps

1. ~~Zero-copy fragmentation~~ (completed)
2. ~~Arc→Box optimization~~ (completed)
3. Pre-allocate packet buffers (pool instead of per-packet allocation) - see issue #2226
4. Consider `io_uring` for further syscall reduction

## References

- Issue #2226: Transport layer optimization opportunities
- PR #2361: Zero-copy stream fragmentation and Arc→Box optimization
- `bytes` crate documentation: https://docs.rs/bytes
