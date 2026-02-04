# Freenet Core Crate

This is the main runtime crate containing the Freenet node implementation.

## Module Overview

| Module | Purpose | Key Files |
|--------|---------|-----------|
| `node/` | Event loop, coordination | `node.rs`, `network_bridge/` |
| `operations/` | Transaction state machines | `get.rs`, `put.rs`, `update.rs`, `subscribe.rs`, `connect.rs` |
| `contract/` | WASM contract execution | `executor.rs`, `handler.rs`, `storages/` |
| `transport/` | UDP networking | `connection_handler.rs`, `peer_connection/` |
| `ring/` | DHT topology | `mod.rs`, `seeding_cache.rs` |
| `server/` | WebSocket/HTTP API | `mod.rs`, `http_gateway.rs` |
| `simulation/` | DST framework | `time.rs`, `rng.rs` |
| `wasm_runtime/` | WASM sandbox | Runtime interface |
| `tracing/` | Observability | Event logging |

## Deterministic Simulation Testing (DST)

When writing code in this crate, follow DST rules to ensure reproducible tests.

### Time Sources

```rust
// GOOD: Use TimeSource trait
fn new(time_source: impl TimeSource) -> Self { ... }

// BAD: Real wall-clock time
std::time::Instant::now()
tokio::time::sleep()
```

### Random Sources

```rust
// GOOD: Seeded RNG
GlobalRng::random_u64()
GlobalRng::fill_bytes(&mut buf)
GlobalRng::with_rng(|rng| distribution.sample(rng))

// BAD: System entropy
rand::random()
rand::thread_rng()
```

### Network I/O (in simulation tests)

```rust
// GOOD: In-memory socket
SimulationSocket::bind(addr).await

// BAD: Real network
tokio::net::UdpSocket::bind(addr).await
```

## Test Commands

```bash
cargo test -p freenet                           # All tests
cargo test -p freenet --lib                     # Unit tests
cargo test -p freenet --test simulation_integration  # SimNetwork
cargo test -p freenet -- --test-threads=1       # Deterministic
```

## Benchmarks

```bash
cargo bench --bench transport_perf -- level0    # CI-friendly
cargo bench --bench transport_perf -- transport # Transport suite
```

## Documentation

- Transport layer: `docs/architecture/transport/README.md`
- Testing guide: `docs/architecture/testing/README.md`
- Simulation testing: `docs/architecture/testing/simulation-testing.md`
