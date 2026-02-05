# Freenet Core Crate

## Trigger-Action Rules

### BEFORE writing any new code in this crate

```
Does this code need current time?
  → DO NOT use: std::time::Instant::now(), tokio::time::sleep()
  → USE: TimeSource trait from src/simulation/

Does this code need randomness?
  → DO NOT use: rand::random(), rand::thread_rng()
  → USE: GlobalRng from src/config/mod.rs

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
  → Run with: cargo test -- --test-threads=1
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
  connect.rs → CONNECT operation
  get.rs     → GET operation
  put.rs     → PUT operation
  update.rs  → UPDATE operation
  subscribe.rs → SUBSCRIBE operation
```

## Module Map

| Module | Entry Point | What It Does |
|--------|-------------|--------------|
| `node/` | `node.rs` | Event loop (start here for data flow) |
| `operations/` | `mod.rs` | Transaction state machines |
| `contract/` | `handler.rs` | WASM execution |
| `transport/` | `connection_handler.rs` | Networking layer |
| `ring/` | `mod.rs` | DHT routing |
| `server/` | `mod.rs` | Client API |
| `simulation/` | `time.rs`, `rng.rs` | DST abstractions |

## Commands

```bash
cargo test -p freenet                           # All tests
cargo test -p freenet -- --test-threads=1       # Deterministic
cargo bench --bench transport_perf -- level0    # Quick benchmark
```
