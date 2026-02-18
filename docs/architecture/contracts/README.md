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
- 2GB guard region before linear memories
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

Uses wasmtime's default on-demand allocation — each instance gets its own
mmap'd memory region, allocated at instantiation and freed on drop.

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
| `freenet_time` | UTC timestamp | V1, V2 |
| `freenet_delegate_context` | Delegate state | V1, V2 |
| `freenet_delegate_secrets` | Secret storage | V1, V2 |
| `freenet_delegate_contracts` | Contract access | V2 only |

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
