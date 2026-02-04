# Testing Rules

## Quick Reference: Which Approach to Use

| Scenario | Approach |
|----------|----------|
| Single function/algorithm | Unit tests with `#[test]` |
| Transport/ring logic | Unit tests with mocks (`MockNetworkBridge`, `MockRing`) |
| Contract operations | `#[freenet_test]` macro with single gateway |
| Multi-node connectivity | `#[freenet_test]` macro with multiple nodes |
| Fault tolerance | SimNetwork with FaultConfig |
| Deterministic replay | SimNetwork with fixed seed |
| Scale testing (20+ peers) | `freenet-test-network` |

## Deterministic Simulation Testing (DST)

The codebase uses DST for reproducible tests. Follow these rules when working in `crates/core/`:

### Time Sources

**Use `TimeSource` trait, not real time:**
```rust
// Good: TimeSource injection
fn new(time_source: impl TimeSource) -> Self { ... }

// Bad: Real wall-clock time
std::time::Instant::now()
tokio::time::sleep()
```

**Examples:** `crates/core/src/ring/seeding_cache.rs:76`, `crates/core/src/transport/peer_connection.rs:1674`

### Random Sources

**Use `GlobalRng`, not system entropy:**
```rust
// Good: Seeded RNG
GlobalRng::random_u64()
GlobalRng::fill_bytes(&mut buf)

// Bad: Non-deterministic
rand::random()
rand::thread_rng()
```

**Examples:** `crates/core/src/ring/location.rs:68`, `crates/core/src/transport/peer_connection.rs:1667`

### Network I/O

**Use `SimulationSocket` for simulation tests:**
```rust
// Good: In-memory socket
SimulationSocket::bind(addr).await

// Bad: Real network (for simulation tests)
tokio::net::UdpSocket::bind(addr).await
```

**Implementation:** `crates/core/src/transport/in_memory_socket.rs`

### Time Advancement Pattern

```rust
// In simulation tests
let mut sim = SimNetwork::new(...).await;
for _ in 0..iterations {
    sim.advance_time(step).await;
    tokio::task::yield_now().await;
}
```

**Examples:** `crates/core/tests/simulation_smoke.rs:29-42`

## Test Commands

```bash
cargo test -p freenet                           # All tests
cargo test -p freenet --lib                     # Unit tests only
cargo test -p freenet --test simulation_integration  # SimNetwork tests
cargo test -p freenet -- --test-threads=1       # Deterministic scheduling
```

## Documentation

- Full testing guide: `docs/architecture/testing/README.md`
- Simulation testing: `docs/architecture/testing/simulation-testing.md`
- Testing matrix: `docs/architecture/testing/testing-matrix.md`
