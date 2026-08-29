# Contract Execution Architecture

## Overview

Freenet executes untrusted WASM contracts in a sandboxed environment. Contracts define shared state and update logic, while delegates provide private computation for users.

## WASM Runtime Backend

Freenet uses Wasmtime as its WASM runtime backend, enabled via the `wasmtime-backend` feature (on by default).

```bash
cargo build  # wasmtime-backend is enabled by default
```

**Compiler:** Cranelift (optimizing compiler)

**Characteristics:**
- **Memory-efficient:** Frees compiled code when modules are dropped
- Compact code generation from Cranelift
- On-demand instance allocation
- Expected: <200 MB for 20-30 contracts

## Security Considerations

### Untrusted Code Execution

The backend is designed for executing untrusted WASM code:

**WebAssembly Sandbox:**
- Inaccessible call stack (prevents stack-smashing)
- Memory isolation with bounds checking
- Type-checked control transfers
- Restricted I/O (explicit imports/exports only)

**Wasmtime Defense-in-Depth:**
- Explicit bounds checks on every memory access (the per-instance
  reservation is sized to the limiter cap, so guard-page-based check
  elimination is intentionally disabled — see "Memory Management")
- One-page guard region as a backstop for tiny-offset overruns
- Stack overflow guard pages
- Memory zeroing between instantiations
- Spectre mitigations for memory bounds checks
- Written in Rust (memory safety guarantees)

### Cranelift Compiler Security

**Why Cranelift is Safe for Untrusted Code:**

Cranelift is explicitly hardened for compiling untrusted WASM modules:

1. **No undefined behavior in IR** (by design)
2. **Guards against JIT bombs** – pathological compilation times
   - Avoids input-length-bounded recursion
   - No quadratic or higher algorithmic complexity
3. **Security-first optimizations**
   - Consciously avoids riskier optimization techniques
   - Abstractions prevent bugs and enforce invariants
4. **Production-vetted** for untrusted code:
   - Fastly Compute@Edge (serverless at scale)
   - Firefox SpiderMonkey WASM baseline compiler
   - Shopify Functions

**References:**
- [Security and Correctness in Wasmtime](https://bytecodealliance.org/articles/security-and-correctness-in-wasmtime)
- [Fastly's Cranelift Security Vetting](https://www.fastly.com/blog/how-we-vetted-cranelift-for-secure-sandboxing-in-compute-edge)
- [Wasmtime Security Documentation](https://docs.wasmtime.dev/security.html)

### Compiler Optimization Level

**Current choice: `OptLevel::None`**

We disable Cranelift optimizations for maximum simplicity and safety:

```rust
// Simpler compiler = smaller attack surface
wasmtime_config.cranelift_opt_level(OptLevel::None);
```

**Rationale:**
- Optimization passes add complexity to the compiler
- For untrusted code, simplicity > performance
- Memory benefits come from compact code generation, not optimizations

**Alternative considered:**
- `OptLevel::SpeedAndSize` – more optimizations, slightly higher attack surface
- Cranelift's optimizations are security-hardened, so this is also safe
- We chose `None` to minimize risk for untrusted contracts

**Future option: Winch**

Wasmtime's baseline compiler "Winch":
- Single-pass, no optimizations
- Even simpler than Cranelift with `OptLevel::None`
- Could be added as a configuration option for maximum security

## Memory Management

### On-Demand Instance Allocation

Uses wasmtime's on-demand allocation — each instance gets its own
mmap'd memory region, allocated at instantiation. The per-instance
reservation is bounded to `DEFAULT_MAX_MEMORY_PAGES * WASM_PAGE_SIZE`
(256 MiB) instead of wasmtime's 4 GiB default; the trailing guard
region is a single wasm page (64 KiB). This keeps virtual memory
manageable on low-RAM hosts (#3986 — Pi 4 4 GB triggers `mmap failed
to reserve` after a few concurrent contract instantiations under
default tuning). The `ResourceLimiter` on `HostState` is the source
of truth for the cap: it rejects any `memory.grow` past
`DEFAULT_MAX_MEMORY_PAGES`, including the implicit grow-from-zero at
instantiation. See `crates/core/src/wasm_runtime/engine/wasmtime_engine.rs`
(`create_engine`) for the configuration.

### Compact Code Generation (Cranelift)

Generates efficient machine code with low per-contract footprint.

### Proper Memory Cleanup

Wasmtime frees compiled code when modules are dropped, so memory is
properly reclaimed.

**Expected Footprint:**
- User peers (20-30 contracts): <200 MB
- Gateway (50-100 contracts): <500 MB
- Memory is reclaimed when contracts are removed

### Memory Tests

Tests verify proper memory behavior:

```rust
#[test]
fn test_module_drop_frees_memory() {
    // Compile multiple modules and drop them
    for _ in 0..10 {
        let module = Module::new(&engine, SIMPLE_WASM).unwrap();
        drop(module);
    }
    // Wasmtime properly frees memory on drop
}

#[test]
#[ignore] // Run manually to observe memory behavior
fn test_memory_leak_comparison() {
    // Compile 100 modules, drop them, observe memory
    // Memory should return to baseline
}
```

## Resource Limits

### Memory Limits

Three layers of memory protection:

1. **Config-level** (pooling strategy)
   ```rust
   pooling.max_memory_size(256 * 1024 * 1024); // 256 MiB per instance
   ```

2. **ResourceLimiter trait** (runtime enforcement)
   ```rust
   impl ResourceLimiter for HostState {
       fn memory_growing(&mut self, current: usize, desired: usize, _maximum: Option<usize>)
           -> anyhow::Result<bool>
       {
           Ok(desired <= self.memory_limit_bytes)
       }
   }
   ```

3. **Stack limits**
   ```rust
   wasmtime_config.max_wasm_stack(8 * 1024 * 1024); // 8 MiB
   ```

### Execution Limits

**Fuel-based metering:**
```rust
wasmtime_config.consume_fuel(true);
store.set_fuel(max_fuel);  // Computed from max_execution_seconds
```

**Timeout protection:**
- Contract calls: `call_*_blocking()` with timeout
- Delegate calls: Synchronous (bounded by fuel)
- Prevents infinite loops and DoS

## Host Functions

Contracts/delegates call into the host via registered functions:

### Namespaces

| Namespace | Purpose | Version |
|-----------|---------|---------|
| `freenet_log` | Logging | V1, V2 |
| `freenet_random` | RNG | V1, V2 |
| `freenet_time` | UTC timestamp (**delegates only; deprecated for contracts**, see below) | V1, V2 |
| `freenet_delegate_context` | Delegate state | V1, V2 |
| `freenet_delegate_secrets` | Secret storage | V1, V2 |
| `freenet_delegate_contracts` | Contract access | V2 only |

### Contracts must not read the host clock

**A contract must not call `freenet_time::__frnt__time__utc_now`.** Doing so is
deprecated as of this release and will be **refused** in a future one: the node
will decline to load a contract that can reach the clock while producing state.
Delegates are unaffected and may keep using it.

#### Why

A contract's `update_state` is required to be a function of its inputs. That is
not a style preference, it is the reason replicas converge: two peers given the
same updates in any order have to arrive at the same state, and the merge laws
(`freenet::conformance`) are the statement of that requirement. A merge that
reads the wall clock is not a function of its inputs, so those laws are not
merely violated by such a contract; they are not well-formed statements about
it. Two peers whose clocks differ by eleven minutes can produce different states
from the same delta, and neither of them is wrong.

This is not hypothetical. Of 33 deployed contracts measured for issue #5465,
11 do not merely *reject* future-dated entries but silently **prune** them
inside `update_state`, so the resulting state is a function of the evaluating
peer's clock. That is the exact defect class contract conformance exists to
detect, produced by the capability rather than prevented by it.

The clock is not fixable by feeding the contract the operation's timestamp
instead. Every measured use is "reject if a signed timestamp is later than now
plus K", and an originator-supplied `now` is attacker-controlled: set it high
and the check passes. Determinism and trustworthiness are in direct conflict for
this check, and these contracts need the trustworthy one, which means it does
not belong inside the merge at all.

#### What to do instead

**Carry a client-signed timestamp in the state, and have the contract enforce
only monotonicity.** The timestamp becomes part of the signed payload rather
than something the contract reads from its host, so every peer evaluating the
same update sees the same value and reaches the same state.

`freenet-weather` is the worked example: `BeaconState.timestamp_ms` is signed by
the client, and the contract's only check is `new > current`. There is no clock
call anywhere in it.

For the two shapes that show up in practice:

- **Anti-grief on a per-author log** (the common case): cap the log by count, or
  key it on a monotonic counter. A client-supplied timestamp is an untrusted
  hint for ordering and ranking; it should not be an eviction key.
- **Capability expiry** (rare, and the genuinely hard one): there is no clean
  in-contract substitute, because the party asserting the time is the party
  being checked. The available shapes are sequence numbers plus a revocation
  record, or enforcing expiry outside the contract.

#### How to check a contract

- `fdev verify-merge --wasm contract.wasm --state s1.bin --state s2.bin` reports
  a `host_clock_import` **code diagnostic** when the module imports the clock.
  It is a diagnostic, not a violation: it does not fail the command today, and
  it is never grounds for removing a contract from the network.
- A node logs a warning naming the contract key the first time it loads a
  clock-reading contract.

Both answers come from the same detector
(`freenet::conformance::host_clock::imports_host_clock`), so the
developer-facing answer and the node-facing answer cannot disagree. Both are
**import**-level today: a module that imports the function without calling it is
reported too. The later release that refuses to load such a contract is the one
that needs call-graph reachability from `update_state`, `summarize_state` and
`get_state_delta`.

#### Delegates are unaffected

A delegate holds private per-node state, is never replicated, and has no merge
laws, so reading the clock in one raises no convergence question at all. A
deployed delegate does exactly this to do hourly rate limiting, and that is
fine. Removing the namespace for contracts while keeping it for delegates
requires the contract and delegate linkers to be split; until then the host
function remains registered for both.

### Delegate API Versions

**V1 (Synchronous):**
- Delegates use request/response pattern for contract access
- All host functions are synchronous
- Thread-local state via `CURRENT_DELEGATE_INSTANCE`

**V2 (Async Host Functions):**
- Delegates call `ctx.get_contract_state()` directly
- Host functions registered as async (via `func_wrap_async`)
- ReDb reads wrapped in async blocks
- Requires wasmtime's `async_support(true)`

**Detection:**
```rust
if module.imports().any(|i| i.module() == "freenet_delegate_contracts") {
    // V2 delegate - use call_3i64_async_imports()
} else {
    // V1 delegate - use call_3i64()
}
```

## Async Execution

### Wasmtime Async Support

**Configuration:**
```rust
wasmtime_config.async_support(true);  // Required for V2 delegates
```

**Implication:**
With `async_support(true)`, **all** function calls must use `call_async()`:

```rust
// Correct (with async_support enabled):
block_on_async(func.call_async(&mut store, args))

// Incorrect (will panic):
func.call(&mut store, args)
```

**Why:**
- Wasmtime's async support changes the Store type internally
- Even for synchronous operations, must use async calling convention
- We wrap with `block_on_async()` to maintain synchronous interface

### Blocking Execution

Contract operations use `spawn_blocking` with timeout:

```rust
fn execute_wasm_blocking<F>(f: F, max_execution_seconds: f64) -> BlockingResult
where F: FnOnce() -> WasmResult + Send + 'static
{
    // 1. Spawn blocking task (tokio or std::thread)
    // 2. Poll for completion with 10ms interval
    // 3. Return Timeout if exceeded
    // 4. Store is moved into/out of blocking context
}
```

**Rationale:**
- Contract execution can take seconds (state updates, validation)
- Must not block async runtime
- Timeout protects against DoS

## Backend Selection

### Compile-Time Feature Flag

The `wasmtime-backend` feature must be enabled:

```rust
#[cfg(not(feature = "wasmtime-backend"))]
compile_error!("The wasmtime-backend feature must be enabled.");
```

### Type Aliases

Backend-agnostic code uses type aliases:

```rust
#[cfg(feature = "wasmtime-backend")]
pub(crate) type Engine = wasmtime_engine::WasmtimeEngine;
```

All wasm_runtime code outside `engine/` uses these aliases via the `WasmEngine` trait.

### WasmEngine Trait

Backend-agnostic interface:

```rust
pub(crate) trait WasmEngine: Send {
    type Module: Clone + Send;

    // Lifecycle
    fn new(config: &RuntimeConfig, host_mem: bool) -> Result<Self, ContractError>;
    fn is_healthy(&self) -> bool;

    // Compilation
    fn compile(&mut self, code: &[u8]) -> Result<Self::Module, WasmError>;

    // Module inspection
    fn module_has_async_imports(&self, module: &Self::Module) -> bool;

    // Instance lifecycle
    fn create_instance(...) -> Result<InstanceHandle, WasmError>;
    fn drop_instance(&mut self, handle: &InstanceHandle);

    // Memory access
    fn memory_info(&mut self, handle: &InstanceHandle) -> Result<(*const u8, usize), WasmError>;
    fn initiate_buffer(&mut self, handle: &InstanceHandle, size: u32) -> Result<i64, WasmError>;

    // Execution
    fn call_void(&mut self, handle: &InstanceHandle, name: &str) -> Result<(), WasmError>;
    fn call_3i64(&mut self, ...) -> Result<i64, WasmError>;
    fn call_3i64_async_imports(&mut self, ...) -> Result<i64, WasmError>;
    fn call_2i64_blocking(&mut self, ...) -> Result<i64, WasmError>;
    fn call_3i64_blocking(&mut self, ...) -> Result<i64, WasmError>;
}
```

## Future Improvements

### Security

1. **Add Winch compiler support**
   - Wasmtime's baseline compiler
   - Even simpler than Cranelift with `OptLevel::None`
   - Configuration option for maximum security

2. **Compiler strategy configuration**
   ```rust
   pub enum CompilerStrategy {
       Baseline,           // Winch (when available)
       CraneliftNoOpt,     // Cranelift + OptLevel::None (current)
       CraneliftOptimized, // Cranelift + optimizations
   }
   ```

3. **Per-contract security profiles**
   - Trusted contracts: enable optimizations
   - Untrusted contracts: baseline compiler
   - Gateway-provided contracts: middle ground

### Memory

1. **Dynamic pooling configuration**
   - Adjust pool size based on contract count
   - Shrink pool when idle

2. **Memory pressure monitoring**
   - Track RSS, compiled code size
   - Evict cached modules under pressure
   - Metrics for memory efficiency

3. **Memory benchmarks**
   - Automated memory profiling tests
   - Regression detection

## References

**Wasmtime Documentation:**
- [Security](https://docs.wasmtime.dev/security.html)
- [Fast Compilation (Winch)](https://docs.wasmtime.dev/examples-fast-compilation.html)
- [API Docs](https://docs.rs/wasmtime/27.0.0/wasmtime/)

**Bytecode Alliance:**
- [Security and Correctness in Wasmtime](https://bytecodealliance.org/articles/security-and-correctness-in-wasmtime)
- [Wasmtime and Cranelift in 2023](https://bytecodealliance.org/articles/wasmtime-and-cranelift-in-2023)

**Production Use Cases:**
- [Fastly's Cranelift Vetting](https://www.fastly.com/blog/how-we-vetted-cranelift-for-secure-sandboxing-in-compute-edge)
