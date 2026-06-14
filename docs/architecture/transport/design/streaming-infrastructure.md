# Streaming Infrastructure Design

## Overview

The streaming infrastructure provides lock-free, high-performance fragment reassembly for large message transfers (Issue #1452). It is **shipped and active**: a PUT/GET whose payload exceeds `streaming_threshold` (default 64 KB) is sent as a fragment stream rather than a single message — see [Selection](#selection).

This document describes the design of the components that make that work: the lock-free reassembly buffer, piped forwarding, the streaming message variants and orphan registry, the operation-layer handlers, and stream-abort / flight-size handling. It is a design reference, not an implementation plan.

## Problem Statement

The original `InboundStream` implementation used `RwLock<HashMap>` for fragment storage, creating contention under concurrent access. Benchmarks from the spike (PR iduartgomez/freenet-core#204) showed:

| Metric | RwLock Approach | Lock-Free Approach | Improvement |
|--------|-----------------|-------------------|-------------|
| Insert throughput | 23 MB/s | 2,235 MB/s | **96x** |
| First-fragment latency | 103us | 25us | **4x** |

---

## Lock-Free StreamBuffer

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

The core lock-free buffer using `AtomicPtr<Bytes>` slots. See `streaming_buffer.rs` (`LockFreeStreamBuffer`).

**Key Properties:**
- **Lock-free writes**: insertion is a single atomic CAS on the slot's `AtomicPtr`; unlike `OnceLock`, an `AtomicPtr` slot can be cleared after consumption (`mark_consumed`), enabling progressive memory reclamation
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

## Piped Forwarding

Enables intermediate nodes to forward fragments without full reassembly. Relay
`ResponseStreaming` forwarding (fork + pipe) is implemented (#4307). One piece
remains deferred: the `PipedStream` *primitive*'s target-failure tracking (which
targets are dead) and retry logic — per its rustdoc, these wait until `PipedStream`
itself is wired into the main forwarding path.

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

## Message Layer

Streaming message variants and the orphan stream registry for handling race conditions.

### Message Variants

#### PUT Messages (`operations/put.rs`)

```rust
pub enum PutMsg {
    Request { ... },      // Existing
    Response { ... },     // Existing

    // Streaming variants
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

    // Streaming variants
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
- `ORPHAN_STREAM_TIMEOUT`: 60 seconds - unclaimed streams are garbage collected
- `STREAM_CLAIM_TIMEOUT`: 60 seconds - timeout when waiting for stream

Both are 60s: on resource-constrained CI runners stream fragments can be delayed
significantly (concurrent WASM compilation, transport-level rate limiting, Docker
NAT overhead), so this provides headroom while still failing promptly on genuinely
broken connections. `ORPHAN_STREAM_TIMEOUT` must be `>= STREAM_CLAIM_TIMEOUT` to
avoid a race where a waiter registers just as the orphan is being cleaned up.

### State Machine Extensions

#### PutState additions

- `AwaitingStreamData { stream_id, contract_key, total_size, subscribe, htl }`
- `ForwardingStream { upstream_stream_id, downstream_stream_id, contract_key, next_hop, subscribe }`

#### GetState additions

- `AwaitingStreamData { stream_id, key, instance_id, total_size, includes_contract, subscribe }`
- `SendingStreamResponse { stream_id, key, instance_id, target_addr }`

### Selection

```rust
streaming_threshold: usize,     // Default: 64KB (network_api.streaming_threshold)
```

A transfer uses streaming when `payload_size > streaming_threshold`
(`operations::should_use_streaming`). There is no separate on/off flag — the
threshold is the sole gate, so streaming is always available and engages by size.

---

## Streaming Handlers

The message handlers that use the infrastructure above, delivered as part of the
per-transaction operation drivers (#1454).

The wire-format streaming variants are *defined* in `operations/put.rs` and
`operations/get.rs` (the `PutMsg`/`GetMsg` enums); the actual *handling* lives in
the per-transaction drivers under `*/op_ctx_task.rs`, with dispatch in `node.rs` —
which is where the #1454 refactor moved it.

1. `PeerConnection` registers arriving streams via `orphan_stream_registry.register_orphan()` (`crates/core/src/transport/peer_connection.rs`).
2. `PutMsg::RequestStreaming` handler (`crates/core/src/operations/put/op_ctx_task.rs`; dispatch in `crates/core/src/node.rs`).
3. `PutMsg::ResponseStreaming` handler (`crates/core/src/operations/put/op_ctx_task.rs`).
4. `GetMsg::ResponseStreaming` handler (`crates/core/src/operations/get/op_ctx_task.rs`).
5. `GetMsg::ResponseStreamingAck` — handled by the transport/stream layer, **not** the GET op driver: the driver classifies it as `AttemptOutcome::Unexpected` by design (`get/op_ctx_task.rs`, `node.rs` — "ResponseStreamingAck handled by stream layer").
6. Periodic orphan GC task — `gc_expired()` runs every 5s, removing streams older than `ORPHAN_STREAM_TIMEOUT` (`crates/core/src/operations/orphan_streams.rs`).

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

### Stream Abort and Flight-Size Release (#4345 / #4393 / #4374)

An outbound stream can abort mid-transfer. Triggers:

- **cwnd-wait timeout**: the congestion window stays closed past `CWND_WAIT_TIMEOUT`.
- **upstream stall/error** (relay-pipe path): the inbound feed stalls past
  `STREAM_INACTIVITY_TIMEOUT` or yields an error.
- **mid-send `packet_sending` failure**: a fragment fails to leave the socket.

#### Flight-size release on abort

A stream's already-sent, unacked fragments are still counted in the
connection-wide flight size. Left pinned, they stay counted until each fragment is
independently ACKed or ages out via `MAX_PACKET_RETRANSMITS` (~6s) — and FixedRate's
loss-pause caps cwnd at the frozen flight size, so a single aborted stream starves
every subsequent stream on the same connection (the #4345 symptom).

To avoid this, `release_aborted_stream_flightsize()`
(`outbound_stream.rs`) calls `SentPacketTracker::drop_stream(stream_id)`
(`sent_packet_tracker.rs`), which atomically removes all of that stream's tracked
packets and returns their exact byte total. That total is handed to
`CongestionControl::release_flightsize(bytes)`, freeing the stranded bytes in one
step so the next stream sees an open cwnd. (The tracker lock is dropped before
calling into the congestion controller, mirroring the ACK path's lock ordering.)

**Resurrection-safety invariant (anti-double-decrement):** once a packet leaves the
tracker via `drop_stream`, a late ACK or the per-packet abandon path finds nothing
and is a no-op. Each packet therefore contributes to flight-size accounting exactly
once. See [`.claude/rules/transport.md`](../../../../.claude/rules/transport.md)
("Flight-size release invariant") for the full invariant — it is not duplicated here.

#### Error semantics: `OutboundStreamFailed`, not `ConnectionClosed`

A stream abort returns `TransportError::OutboundStreamFailed(peer_addr)`, a
**stream-scoped** error — NOT `ConnectionClosed`. The connection survives, so
subsequent streams on it can proceed or retry. The operations layer classifies it
as a transient send failure (`is_transient_send_failure()`): the idle timeout
remains the sole authority on connection liveness, and the op layer times out and
retries against another candidate (#4374).

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
