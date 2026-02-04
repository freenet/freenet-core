---
paths:
  - "crates/core/**"
---

# Testing Rules

## Decision Tree: Choosing Test Approach

```
Is it a single function/algorithm?
  → YES: Unit test with #[test]
  → NO: Continue...

Does it need network simulation?
  → NO: Unit test with mocks (MockNetworkBridge, MockRing)
  → YES: Continue...

How many nodes?
  → 1 gateway: #[freenet_test] macro
  → 2-10 nodes: #[freenet_test] with multiple nodes
  → 20+ nodes: freenet-test-network

Need fault injection?
  → YES: SimNetwork with FaultConfig
```

## Trigger-Action Rules

### When writing new code in `crates/core/`

**BEFORE writing any function that needs current time:**
→ Check: Am I using `TimeSource` trait?
→ If using `std::time::Instant::now()` or `tokio::time::sleep()`: STOP. Refactor to accept `impl TimeSource`.

**BEFORE writing any code that needs randomness:**
→ Check: Am I using `GlobalRng`?
→ If using `rand::random()` or `rand::thread_rng()`: STOP. Use `GlobalRng::random_u64()` or `GlobalRng::fill_bytes()`.

**BEFORE writing any socket code in tests:**
→ Check: Is this a simulation test?
→ If YES and using `tokio::net::UdpSocket`: STOP. Use `SimulationSocket::bind()`.

### When a test fails

```
Test failed?
  → DO NOT delete the test
  → DO NOT comment it out without marker
  → Instead:
    1. Add #[ignore] attribute
    2. Add comment: // TODO-MUST-FIX: [reason] #[issue-number]
    3. Create GitHub issue for follow-up
```

### When running tests

```
Running simulation tests?
  → Use: cargo test -p freenet -- --test-threads=1
  → Why: Ensures deterministic scheduling

Running all tests?
  → Use: cargo test -p freenet

Running specific integration test?
  → Use: cargo test -p freenet --test simulation_integration
```

## Reference Patterns

**Time injection:**
```rust
fn new(time_source: impl TimeSource) -> Self { ... }
```
See: `crates/core/src/ring/seeding_cache.rs:76`

**RNG usage:**
```rust
GlobalRng::random_u64()
GlobalRng::fill_bytes(&mut buf)
```
See: `crates/core/src/ring/location.rs:68`

**Simulation socket:**
```rust
SimulationSocket::bind(addr).await
```
See: `crates/core/src/transport/in_memory_socket.rs`
