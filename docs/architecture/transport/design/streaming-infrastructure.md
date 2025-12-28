# Streaming Infrastructure Design

## Overview

The streaming infrastructure provides lock-free, high-performance fragment reassembly for large message transfers. This is Phase 1 of the streaming transport implementation (Issue #1452).

## Problem Statement

The original `InboundStream` implementation used `RwLock<HashMap>` for fragment storage, creating contention under concurrent access. Benchmarks from the spike (PR iduartgomez/freenet-core#204) showed:

| Metric | RwLock Approach | Lock-Free Approach | Improvement |
|--------|-----------------|-------------------|-------------|
| Insert throughput | 23 MB/s | 2,235 MB/s | **96×** |
| First-fragment latency | 103μs | 25μs | **4×** |

## Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Transport Layer                              │
│                (receives encrypted fragments)                    │
└───────────────────────────┬─────────────────────────────────────┘
                            │
              ┌─────────────▼─────────────┐
              │     StreamRegistry        │
              │  (stream_id → StreamHandle)│
              │      [DashMap]            │  ← Lock-free concurrent map
              └─────────────┬─────────────┘
                            │
              ┌─────────────▼─────────────┐
              │      StreamHandle         │
              │  ┌─────────────────────┐  │
              │  │ LockFreeStreamBuffer│  │  ← Lock-free fragment storage
              │  │   (Arc<...>)        │  │
              │  └─────────────────────┘  │
              │  ┌─────────────────────┐  │
              │  │     SyncState       │  │  ← Cancelled flag + wakers
              │  │ (parking_lot::RwLock)│ │
              │  └─────────────────────┘  │
              └─────────────┬─────────────┘
                            │
              ┌─────────────▼─────────────┐
              │  StreamingInboundStream   │
              │   (futures::Stream impl)  │
              │   [independent read pos]  │
              └───────────────────────────┘
```

### Key Components

#### 1. LockFreeStreamBuffer

The core lock-free buffer using `OnceLock<Bytes>` slots. See `streaming_buffer.rs:39-53` for the struct definition.

**Key Properties:**
- **Lock-free writes**: `OnceLock::set()` is a single atomic CAS
- **Idempotent inserts**: Duplicates are automatic no-ops
- **Zero-copy**: Uses `Bytes` for reference-counted data
- **Pre-allocated**: Buffer size determined by stream header

**Operations:**
- `insert(fragment_index, data)` - O(1) lock-free CAS (`streaming_buffer.rs:102-123`)
- `get(fragment_index)` - O(1) direct array access (`streaming_buffer.rs:195-201`)
- `assemble()` - O(n) concatenation when complete (`streaming_buffer.rs:216-230`)
- `iter_contiguous()` - Iterator over contiguous fragments (`streaming_buffer.rs:244-246`)

#### 2. StreamHandle

Cloneable handle for accessing an inbound stream. See `streaming.rs:107-117` for the struct definition.

**Design Decision**: The buffer is stored outside the lock because:
1. Fragment insertion is the hot path (high frequency)
2. The buffer is already lock-free internally
3. Only `cancelled` flag and wakers need synchronization

#### 3. StreamingInboundStream

A `futures::Stream` implementation for incremental consumption. See `streaming.rs:274-281` for the struct and `streaming.rs:303-347` for the `poll_next` implementation.

Multiple consumers can create independent streams from the same handle, each maintaining their own read position.

#### 4. StreamRegistry

Global registry for transport-to-operations handoff. See `streaming.rs:352-368` for the struct definition.

**Key Design Decisions:**
- Uses `DashMap` for lock-free concurrent access (avoiding global bottleneck)
- `cancel_all()` also clears the registry to prevent memory leaks
- `remove()` should be called when streams complete for explicit cleanup

## Data Flow

### Fragment Insertion (Producer Path)

1. Transport receives encrypted fragment
2. Decrypt and extract (stream_id, fragment_index, payload)
3. `registry.get_or_register(stream_id, total_bytes)`
4. `handle.push_fragment(fragment_index, payload)` - see `streaming.rs:186-210`
   - Check cancelled (read lock)
   - `buffer.insert(index, data)` ← Lock-free CAS
   - `advance_frontier()` ← Lock-free CAS loop
   - `data_available.notify_waiters()`
   - `sync.wake_all()` ← Wake poll_next waiters

### Fragment Consumption (Consumer Path)

1. Application gets handle from registry
2. Create stream with `handle.stream()`
3. Consume with `while let Some(bytes) = stream.next().await`

The `poll_next` implementation (`streaming.rs:306-347`):
- Check cancelled state
- Check if stream is exhausted
- Try to get next fragment (lock-free)
- If not available, register waker and return Pending

## Lock-Free Frontier Tracking

The `contiguous_fragments` atomic tracks how many fragments from the start are contiguous. See `streaming_buffer.rs:129-155` for the `advance_frontier()` implementation.

This uses a CAS loop that:
1. Loads current frontier
2. Checks if next fragment exists
3. Attempts CAS to advance
4. Continues until gap found or all received

This enables:
- `is_complete()` - O(1) check (`streaming_buffer.rs:158-160`)
- `iter_contiguous()` - Safe iteration without locking

## Memory Layout

For a 1 MB stream with ~1,424-byte fragments (~702 fragments):

| Component | Size |
|-----------|------|
| fragments array (`OnceLock<Bytes>`) | ~11 KB (16 bytes each) |
| total_size, total_fragments | 12 bytes |
| contiguous_fragments | 4 bytes |
| data_available (Notify) | ~48 bytes |
| **Total overhead** | ~11 KB (1.1% of 1 MB) |

## Error Handling

See `streaming_buffer.rs:264-287` for `InsertError` and `streaming.rs:44-66` for `StreamError`.

## Async Notification

The buffer uses `tokio::sync::Notify` for efficient async waiting:

1. **On insert**: `data_available.notify_waiters()` wakes all async waiters
2. **On cancel**: Also notifies to unblock `assemble()` calls (`streaming.rs:212-221`)
3. **poll_next**: Uses synchronous wakers stored in `SyncState`

## Thread Safety

| Component | Synchronization | Access Pattern |
|-----------|-----------------|----------------|
| `StreamRegistry.streams` | `DashMap` | Lock-free concurrent access |
| `fragments` array | `AtomicPtr` (CAS) | Many writers, many readers, clearable |
| `contiguous_fragments` | `AtomicU32` (CAS loop) | Many writers, many readers |
| `consumed_frontier` | `AtomicU32` (fetch_max) | Single writer, many readers |
| `cancelled` flag | `parking_lot::RwLock` | Rare write, frequent read |
| `wakers` | `parking_lot::RwLock` | Write on wait, read on wake |

## Progressive Memory Reclamation

Unlike `OnceLock`, `AtomicPtr` allows clearing slots after consumption. This enables
progressive memory reclamation for large streams.

### API Options

| Method | Behavior | Use Case |
|--------|----------|----------|
| `stream()` | Clone fragments, keep in buffer | Multiple consumers, fork support |
| `stream_with_reclaim()` | Take fragments, clear slots | Single consumer, memory-efficient |
| `mark_consumed(up_to)` | Manually clear slots up to index | Fine-grained control |
| `take(index)` | Take single fragment, clear slot | Custom consumption patterns |

### Memory Behavior Example

```
10 MB stream (7,000 fragments)

Without reclaim:
- All 10 MB held until stream dropped
- Multiple consumers can read

With reclaim (stream_with_reclaim):
- Fragment 1 read → Fragment 1 freed (9.999 MB)
- Fragment 2 read → Fragment 2 freed (9.998 MB)
- ...at completion, 0 bytes held
```

### Warning

`stream_with_reclaim()` is incompatible with `fork()` - once a fragment is taken,
no other consumer can read it. Use only for single-consumer scenarios.

## Performance Characteristics

| Operation | Complexity | Synchronization |
|-----------|------------|-----------------|
| `insert()` | O(1) | Lock-free CAS |
| `get()` | O(1) | Atomic load |
| `take()` | O(1) | Atomic swap |
| `mark_consumed(n)` | O(n) | n atomic swaps |
| `is_complete()` | O(1) | Atomic load |
| `inserted_count()` | O(n) | None (iteration) |
| `assemble()` | O(n) | None |
| `push_fragment()` | O(1) | Read lock (cancelled check) |
| `cancel()` | O(w) | Write lock (w = waker count) |

## Integration Points

### With PeerConnection

See `peer_connection.rs` for:
- `streaming_registry()` method
- `recv_stream_handle()` method

### With process_inbound

When a fragment arrives:
1. Push to legacy `InboundStream` (for backward compatibility)
2. Push to `StreamHandle` (for new streaming API)

## Testing

The implementation includes comprehensive tests covering:

- **Edge cases**: Zero-byte streams, single-byte streams, exact boundaries
- **Concurrency**: Parallel inserts, concurrent consumers, race conditions
- **Error handling**: Cancelled streams, invalid indices, assembly failures
- **Async behavior**: Notification timing, cancel during wait

Run tests with:
```bash
cargo test -p freenet --lib -- streaming
```

## Running Benchmarks

```bash
# Lock-free buffer benchmarks
cargo bench --bench transport_ci -- streaming_buffer

# Full streaming pipeline benchmarks
cargo bench --bench transport_ci -- transport/streaming
```

## Future Improvements (Phase 2+)

1. **Piped Streams**: Forward fragments before complete receipt
2. **Flow Control**: Backpressure from slow consumers
3. **Memory Limits**: Cap total buffered data across streams
4. **Metrics**: Expose fragment loss, reordering statistics

## Source Code

| Component | Location |
|-----------|----------|
| Lock-free buffer | `crates/core/src/transport/peer_connection/streaming_buffer.rs` |
| StreamHandle, Registry | `crates/core/src/transport/peer_connection/streaming.rs` |
| Integration | `crates/core/src/transport/peer_connection.rs` |
| Benchmarks | `crates/core/benches/transport/streaming_buffer.rs` |

## References

- Issue #1452: Streaming transport implementation
- PR iduartgomez/freenet-core#204: Spike implementation with benchmarks
- RFC 6817: LEDBAT (related congestion control)
