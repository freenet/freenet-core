# Streaming Infrastructure Design

## Overview

The streaming infrastructure provides lock-free, high-performance fragment reassembly for large message transfers (Issue #1452).

## Implementation Status

| Phase | Status | PR | Description |
|-------|--------|-----|-------------|
| Phase 1: Lock-Free StreamBuffer | **COMPLETE** | #2443 | Lock-free fragment storage with `AtomicPtr<Bytes>` |
| Phase 2: Piped Forwarding | **COMPLETE** | #2458/2465 | Intermediate node forwarding without reassembly |
| Phase 3: Message Layer | **COMPLETE** | #2476 | Streaming message variants and OrphanStreamRegistry |
| Phase 4: Streaming Handlers | **IN PROGRESS** | - | Implement handlers in put.rs/get.rs |
| Phase 5: Capability Negotiation | Not started | - | Backward compatibility with non-streaming nodes |
| Phase 6: Rollout | Not started | - | Shadow mode, metrics, gradual enablement |

## Problem Statement

The original `InboundStream` implementation used `RwLock<HashMap>` for fragment storage, creating contention under concurrent access. Benchmarks from the spike (PR iduartgomez/freenet-core#204) showed:

| Metric | RwLock Approach | Lock-Free Approach | Improvement |
|--------|-----------------|-------------------|-------------|
| Insert throughput | 23 MB/s | 2,235 MB/s | **96x** |
| First-fragment latency | 103us | 25us | **4x** |

---

## Phase 1: Lock-Free StreamBuffer (COMPLETE)

### Architecture

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

The core lock-free buffer using `OnceLock<Bytes>` slots. See `streaming_buffer.rs:39-53`.

**Key Properties:**
- **Lock-free writes**: `OnceLock::set()` is a single atomic CAS
- **Idempotent inserts**: Duplicates are automatic no-ops
- **Zero-copy**: Uses `Bytes` for reference-counted data
- **Pre-allocated**: Buffer size determined by stream header

**Operations:**
- `insert(fragment_index, data)` - O(1) lock-free CAS
- `get(fragment_index)` - O(1) direct array access
- `assemble()` - O(n) concatenation when complete
- `iter_contiguous()` - Iterator over contiguous fragments

#### 2. StreamHandle

Cloneable handle for accessing an inbound stream. See `streaming.rs:107-117`.

**Design Decision**: The buffer is stored outside the lock because:
1. Fragment insertion is the hot path (high frequency)
2. The buffer is already lock-free internally
3. Only `cancelled` flag and wakers need synchronization

#### 3. StreamingInboundStream

A `futures::Stream` implementation for incremental consumption. Multiple consumers can create independent streams from the same handle.

#### 4. StreamRegistry

Global registry for transport-to-operations handoff using `DashMap` for lock-free concurrent access.

### Performance Characteristics

| Operation | Complexity | Synchronization |
|-----------|------------|-----------------|
| `insert()` | O(1) | Lock-free CAS |
| `get()` | O(1) | Atomic load |
| `take()` | O(1) | Atomic swap |
| `is_complete()` | O(1) | Atomic load |
| `assemble()` | O(n) | None |

### Progressive Memory Reclamation

| Method | Behavior | Use Case |
|--------|----------|----------|
| `stream()` | Clone fragments, keep in buffer | Multiple consumers |
| `stream_with_reclaim()` | Take fragments, clear slots | Single consumer, memory-efficient |
| `mark_consumed(up_to)` | Manually clear slots | Fine-grained control |

---

## Phase 2: Piped Forwarding (COMPLETE)

Enables intermediate nodes to forward fragments without full reassembly.

### Architecture

```
┌──────────────┐     fragments     ┌──────────────┐     forward      ┌──────────────┐
│   Source     │  ───────────────▶ │ Intermediate │  ─────────────▶  │ Destination  │
│    Node      │   (1, 2, 4, 3)    │    Node      │   (1, 2, 3, 4)   │    Node      │
└──────────────┘                   └──────────────┘                  └──────────────┘
                                          │
                                   ┌──────┴──────┐
                                   │ PipedStream │
                                   │ next_to_fwd │
                                   │ buffered:{4}│ ← out-of-order
                                   └─────────────┘
```

### Key Components

#### PipedStream (`piped_stream.rs`)

Buffers out-of-order fragments and forwards in-order fragments immediately.

#### PipedStreamConfig

| Setting | Default | Purpose |
|---------|---------|---------|
| `max_buffered_fragments` | 100 | Prevent unbounded buffering |
| `max_buffered_bytes` | 1 MB | Memory pressure limit |
| `max_concurrent_sends` | 10 | Per-target send concurrency |

### Memory Efficiency

| Scenario | Memory Usage |
|----------|--------------|
| In-order delivery | 0 bytes buffer |
| Realistic reorder (chunks of 10) | ~12 KB |
| Worst case (fully reversed) | Full stream |

---

## Phase 3: Message Layer Infrastructure (COMPLETE)

Adds streaming message variants and the orphan stream registry for handling race conditions.

### Message Variants

#### PUT Messages (`operations/put.rs`)

```rust
pub enum PutMsg {
    Request { ... },      // Existing
    Response { ... },     // Existing

    // Streaming variants (Phase 3)
    RequestStreaming {
        id: Transaction,
        stream_id: StreamId,
        contract_key: ContractKey,
        total_size: u64,
        htl: usize,
        skip_list: HashSet<SocketAddr>,
        subscribe: bool,
    },
    ResponseStreaming {
        id: Transaction,
        key: ContractKey,
        continue_forwarding: bool,
    },
}
```

#### GET Messages (`operations/get.rs`)

```rust
pub enum GetMsg {
    Request { ... },      // Existing
    Response { ... },     // Existing

    // Streaming variants (Phase 3)
    ResponseStreaming {
        id: Transaction,
        instance_id: ContractInstanceId,
        stream_id: StreamId,
        key: ContractKey,
        total_size: u64,
        includes_contract: bool,
    },
    ResponseStreamingAck {
        id: Transaction,
        stream_id: StreamId,
    },
}
```

### OrphanStreamRegistry (`operations/orphan_streams.rs`)

Handles race conditions when stream fragments arrive before their metadata messages.

**Two orderings handled:**
1. **Stream arrives first**: Transport calls `register_orphan()` when fragments arrive
2. **Metadata arrives first**: Operations calls `claim_or_wait()` when metadata arrives

```rust
pub struct OrphanStreamRegistry {
    // Streams awaiting metadata
    orphan_streams: DashMap<StreamId, (StreamHandle, Instant)>,
    // Waiters for streams that haven't arrived yet
    stream_waiters: DashMap<StreamId, oneshot::Sender<StreamHandle>>,
}
```

**Key Constants:**
- `ORPHAN_STREAM_TIMEOUT`: 30 seconds - unclaimed streams are garbage collected
- `STREAM_CLAIM_TIMEOUT`: 10 seconds - timeout when waiting for stream

### State Machine Extensions

#### PutState additions

- `AwaitingStreamData { stream_id, contract_key, total_size, subscribe, htl }`
- `ForwardingStream { upstream_stream_id, downstream_stream_id, contract_key, next_hop, subscribe }`

#### GetState additions

- `AwaitingStreamData { stream_id, key, instance_id, total_size, includes_contract, subscribe }`
- `SendingStreamResponse { stream_id, key, instance_id, target_addr }`

### Configuration

```rust
streaming_enabled: bool,        // Default: false
streaming_threshold: usize,     // Default: 64KB
```

Streaming is used when: `streaming_enabled && payload_size > streaming_threshold`

---

## Phase 4: Streaming Handlers (IN PROGRESS)

Implements the actual message handlers that use the Phase 3 infrastructure.

### Goals

1. Wire `PeerConnection` to call `orphan_stream_registry.register_orphan()` when streams arrive
2. Implement `PutMsg::RequestStreaming` handler
3. Implement `PutMsg::ResponseStreaming` handler
4. Implement `GetMsg::ResponseStreaming` handler
5. Implement `GetMsg::ResponseStreamingAck` handler
6. Add periodic GC task for orphan streams

### Data Flow

```
                    STREAMING PUT FLOW

Sender                      Intermediate                   Final Node
  │                              │                              │
  │─── RequestStreaming ────────►│                              │
  │─── Stream fragments ────────►│                              │
  │                              │─── RequestStreaming ────────►│
  │                              │─── Stream fragments ────────►│
  │                              │                              │ (stores)
  │                              │◄── ResponseStreaming ────────│
  │◄── ResponseStreaming ────────│                              │


                    STREAMING GET FLOW

Requester                   Intermediate                   Owner
  │                              │                              │
  │─── Request ─────────────────►│                              │
  │                              │─── Request ─────────────────►│
  │                              │◄── ResponseStreaming ────────│
  │                              │◄── Stream fragments ─────────│
  │◄── ResponseStreaming ────────│                              │
  │◄── Stream fragments ─────────│                              │
  │─── ResponseStreamingAck ────►│─── ResponseStreamingAck ────►│
```

---

## Phase 5: Capability Negotiation (NOT STARTED)

Backward compatibility with non-streaming nodes via handshake negotiation.

### Planned Features

- Add `PeerCapabilities` to handshake protocol
- Store negotiated transport mode per connection
- Gate streaming features behind capability check
- Protocol version bump for streaming support

---

## Phase 6: Rollout (NOT STARTED)

Gradual enablement with monitoring.

### Planned Features

- Feature flags: `FREENET_STREAMING`, `FREENET_STREAMING_THRESHOLD`
- Shadow mode for A/B validation
- Prometheus metrics for streaming health
- Gradual enablement: shadow -> opt-in -> default

---

## Testing

```bash
# Run streaming tests
cargo test -p freenet --lib -- streaming

# Run benchmarks
cargo bench --bench transport_ci -- streaming_buffer
cargo bench --bench transport_ci -- transport/streaming
```

## Source Code

| Component | Location |
|-----------|----------|
| Lock-free buffer | `crates/core/src/transport/peer_connection/streaming_buffer.rs` |
| StreamHandle, Registry | `crates/core/src/transport/peer_connection/streaming.rs` |
| Piped forwarding | `crates/core/src/transport/peer_connection/piped_stream.rs` |
| Orphan streams | `crates/core/src/operations/orphan_streams.rs` |
| Integration | `crates/core/src/transport/peer_connection.rs` |

## References

- Issue #1452: Streaming transport implementation
- PR iduartgomez/freenet-core#204: Spike implementation with benchmarks
- RFC 6817: LEDBAT (related congestion control)
