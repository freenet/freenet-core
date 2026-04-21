# Freenet Core Crate

## Trigger-Action Rules

### BEFORE writing any new code in this crate

```
Does this code need current time?
  → DO NOT use: std::time::Instant::now(), tokio::time::sleep()
  → USE: TimeSource trait from src/simulation/

Does this code need randomness?
  → DO NOT use: rand::random(), rand::thread_rng()
  → USE: GlobalRng from src/config.rs

Is this network code for tests?
  → DO NOT use: tokio::net::UdpSocket
  → USE: SimulationSocket from src/transport/in_memory_socket.rs
```

### WHEN adding a test

```
Is it testing network behavior?
  → Use #[freenet_test] macro for SimNetwork

Is it a unit test?
  → Use mocks: MockNetworkBridge, MockRing

Need deterministic results?
  → Use GlobalRng seeding (see src/config.rs)
```

### WHEN modifying transport/

```
Read first: docs/architecture/transport/README.md
Key decision points:
  - Connection state: peer_connection.rs
  - Rate limiting: rate_limiter.rs
  - Congestion control: congestion/ (LEDBAT++, BBR, FixedRate)
```

### WHEN modifying operations/

```
Each file is a state machine:
  connect.rs    → CONNECT operation
  get.rs        → GET operation
  put.rs        → PUT operation
  update.rs     → UPDATE operation
  subscribe.rs  → SUBSCRIBE operation (legacy state-machine path)

Plus task-per-transaction drivers from #1454 Phase 2b onwards:
  subscribe/op_ctx_task.rs → client-initiated SUBSCRIBE driver
                             + fresh inbound relay SUBSCRIBE driver
                             (Phase 5 follow-up slice A, `start_relay_subscribe`)
  put/op_ctx_task.rs       → client-initiated PUT driver (Phase 3a)
                             + fresh inbound non-streaming relay PUT
                             driver (Phase 5 follow-up slice A, PR #3917,
                             `start_relay_put`)
                             + fresh inbound streaming relay PUT
                             driver (Phase 5 follow-up slice B,
                             `start_relay_put_streaming`: claims
                             inbound stream via orphan_stream_registry,
                             pipes fragments downstream via
                             NetworkBridge::pipe_stream, bubbles a
                             downgraded non-streaming Response upstream)
  get/op_ctx_task.rs       → client-initiated GET driver (Phase 3b)
                             + fresh inbound relay GET driver (Phase 5,
                             PR #3896, `start_relay_get`)
  update/op_ctx_task.rs    → client-initiated UPDATE driver (Phase 4,
                             fire-and-forget) + fresh inbound non-streaming
                             relay UPDATE drivers (Phase 5 follow-up slice
                             A, PR #3910: `start_relay_request_update`,
                             `start_relay_broadcast_to`)
    All four relay drivers share the per-node dedup gate pattern
    (`active_relay_{get,update,put,subscribe}_txs`) and the
    `Relay*InflightGuard` RAII. PUT streaming relays now run on the
    task-per-tx path (slice B, `start_relay_put_streaming`).
    Streaming UPDATE relay variants (`RequestUpdateStreaming`,
    `BroadcastToStreaming`) and the deprecated `Broadcasting` wire
    variant remain on legacy pending future slices. SUBSCRIBE has no
    streaming variants but renewals, PUT sub-op subscribes, executor
    auto-subscribe paths, `Unsubscribe`, and `ForwardingAck` all stay
    on the legacy state machine.
```

## Module Map

| Module | Entry Point | What It Does |
|--------|-------------|--------------|
| `node/` | `node.rs` | Event loop (start here for data flow) |
| `operations/` | `operations.rs` | Transaction state machines |
| `contract/` | `handler.rs` | WASM execution |
| `transport/` | `connection_handler.rs` | Networking layer |
| `ring/` | `ring.rs` | DHT routing |
| `server/` | `server.rs` | Client API |
| `simulation/` | `time.rs`, `rng.rs` | DST abstractions |

## Commands

```bash
cargo test -p freenet                           # All tests
cargo bench --bench transport_perf -- level0    # Quick benchmark
```
