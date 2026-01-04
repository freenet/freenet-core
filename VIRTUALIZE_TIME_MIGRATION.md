# Transport Time Virtualization Migration

## Status
This document tracks the progress of migrating the transport module to use virtual time (`simulation::TimeSource`) instead of direct system time calls.

## Completed

### ledbat.rs ✓
**Commit: 9904ade**

Migration pattern applied:
- Changed import from `util::time_source::{InstantTimeSrc, TimeSource}` to `simulation::{RealTime, TimeSource}`
- Updated struct bounds: `<T: TimeSource + Clone = InstantTimeSrc>` → `<T: TimeSource = RealTime>`
- Changed field: `epoch: Instant` → `epoch_nanos: u64`
- Updated timing: `time_source.now().duration_since(self.epoch)` → `time_source.now_nanos() - self.epoch_nanos`
- Test harness: `SharedMockTimeSource` → `VirtualTime` with `advance()` method
- All tests use virtual time for deterministic timing

## Migration Pattern

For each file requiring migration, follow this pattern:

### 1. Update Imports
```rust
// Before
use crate::util::time_source::{InstantTimeSrc, TimeSource};

// After
use crate::simulation::{RealTime, TimeSource, VirtualTime};
```

### 2. Update Generic Bounds
```rust
// Before (old API)
pub struct MyController<T: TimeSource + Clone = InstantTimeSrc>

// After (new API)
pub struct MyController<T: TimeSource = RealTime>
```

### 3. Replace Instant-based Timing
```rust
// Before
let epoch = time_source.now();
let elapsed = time_source.now().duration_since(epoch).as_nanos() as u64;

// After
let epoch_nanos = time_source.now_nanos();
let elapsed = time_source.now_nanos() - epoch_nanos;
```

### 4. Update Constructor Calls
```rust
// Production (now default)
let controller = MyController::new(config); // Uses RealTime by default

// Tests
let time_source = VirtualTime::new();
let controller = MyController::new_with_time_source(config, time_source.clone());
time_source.advance(Duration::from_millis(100));
```

### 5. Handle Async Operations
For files using `tokio::time::sleep()` or `tokio::time::timeout()`:
- Add a `time_source: T` field to the struct
- Use `time_source.sleep(duration)` instead of `tokio::time::sleep(duration)`
- Use `time_source.timeout(duration, future)` instead of `tokio::time::timeout(duration, future)`

## Remaining Files

### High Priority (async heavy)
1. **connection_handler.rs** - 90+ tokio::time calls
   - Replace all `tokio::time::sleep()` with `time_source.sleep()`
   - Replace all `tokio::time::timeout()` with `time_source.timeout()`
   - Add generic `<T: TimeSource = RealTime>` parameter

2. **outbound_stream.rs** - Timing telemetry
   - Replace `Instant::now()` with `time_source.now_nanos()`
   - Replace `tokio::time::sleep()` for exponential backoff

3. **token_bucket.rs** - Direct Instant::now() usage
   - Replace all `Instant::now()` calls
   - Add `time_source: T` field

### Medium Priority (moderate async)
4. **streaming.rs** - 10+ sleep/timeout calls
5. **streaming_buffer.rs** - 5+ sleep/timeout calls
6. **piped_stream.rs** - 5+ sleep/timeout calls
7. **in_memory_socket.rs** - Network delay simulation

### Low Priority (minimal usage)
8. **packet trackers** - Already generic, may just need import updates
9. **mod.rs** - Single blocking sleep (examine carefully)

### Integration Points
10. **peer_connection.rs** - Thread TimeSource through struct hierarchy
11. **peer_connection/*.rs** submodules - Update type propagation

## Testing Strategy

Each file should:
1. Maintain backwards compatibility via default generic parameter = `RealTime`
2. Have test code inject `VirtualTime` for deterministic testing
3. Use `time_source.advance()` instead of `std::thread::sleep()` or `tokio::time::sleep()` in tests

## Known Challenges

1. **Async/await complexity** - Files with heavy async usage (connection_handler.rs) need careful refactoring
2. **Type propagation** - TimeSource generics must propagate through struct constructors
3. **Default parameters** - Ensure production code is not affected by default `RealTime` usage

## Verification

Once migration complete:
```bash
cargo test -p freenet
cargo test -p freenet --test large_network -- --ignored
cargo clippy --all-targets --all-features
cargo fmt --check
```

## Design Notes

### Why simulation::TimeSource over util::time_source?

The `simulation::TimeSource` already provides:
- ✓ Async operations (sleep, timeout)
- ✓ VirtualTime implementation (fully controllable)
- ✓ RealTime implementation (production usage)
- ✓ Deterministic wakeup tracking
- ✓ Battle-tested in simulation crate

The `util::time_source` only provided:
- Basic Instant-based interface
- No async support
- No virtual time option

### Benefits of this approach

1. **Test speed**: Virtual time skips real delays entirely
2. **Determinism**: Same time sequence produces same behavior
3. **Property testing**: Can systematically vary time sequences
4. **No runtime overhead**: Monomorphization at compile time
5. **Backwards compatible**: Default to RealTime for production

