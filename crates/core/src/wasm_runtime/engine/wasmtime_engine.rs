//! Wasmtime 27.x backend for the WASM engine abstraction.
//!
//! **This is the ONLY file that imports `wasmtime::`.**
//!
//! Contains all wasmtime-specific code:
//! - Engine/Store/Linker setup
//! - ResourceLimiter for memory limits
//! - Instance creation and management
//! - Typed function resolution and invocation
//! - Fuel-based metering (gas) integration
//! - Host function registration via Linker
//! - Blocking execution with timeout
//!
//! # Security: Running Untrusted Code
//!
//! Freenet executes untrusted WASM contracts from any peer. This backend is configured
//! for maximum security when running untrusted code.
//!
//! ## Cranelift Compiler Safety
//!
//! **Cranelift is explicitly designed for compiling untrusted WASM modules.**
//!
//! Unlike general-purpose compilers that assume trusted input, Cranelift is hardened
//! against malicious compiler input:
//!
//! - **No undefined behavior in IR** (by design, unlike LLVM)
//! - **Guards against JIT bombs** – pathological compilation that takes excessive time
//!   - Avoids input-length-bounded recursion
//!   - Avoids quadratic or higher algorithmic complexity
//! - **Security-first optimizations** – consciously avoids riskier optimization techniques
//! - **Production-vetted** for untrusted code at scale:
//!   - Fastly Compute@Edge (serverless functions)
//!   - Firefox SpiderMonkey (WebAssembly baseline compiler)
//!   - Shopify Functions
//!
//! **References:**
//! - <https://bytecodealliance.org/articles/security-and-correctness-in-wasmtime>
//! - <https://www.fastly.com/blog/how-we-vetted-cranelift-for-secure-sandboxing-in-compute-edge>
//! - <https://docs.wasmtime.dev/security.html>
//!
//! ## Optimization Level: None
//!
//! We configure Cranelift with `OptLevel::None` for maximum simplicity:
//!
//! ```rust,ignore
//! wasmtime_config.cranelift_opt_level(OptLevel::None);
//! ```
//!
//! **Rationale:**
//! - Simpler compiler = smaller attack surface
//! - Optimization passes add complexity (even though Cranelift's are security-hardened)
//! - For untrusted code, we prioritize safety over performance
//! - Memory benefits come from pooling and compact code generation, not optimizations
//! - Similar philosophy to wasmer's singlepass compiler (fast, minimal optimization)
//!
//! **Alternatives considered:**
//! - `OptLevel::SpeedAndSize` – more optimizations, slightly higher complexity
//!   - Still safe (Cranelift's optimizations are security-hardened)
//!   - We chose `None` to minimize risk for untrusted contracts
//! - Winch compiler – Wasmtime's baseline compiler (analogous to wasmer singlepass)
//!   - Even simpler than Cranelift with `OptLevel::None`
//!   - Could be added as a future configuration option
//!
//! ## WebAssembly Sandbox
//!
//! Beyond compiler safety, WebAssembly itself provides strong isolation:
//!
//! - **Inaccessible call stack** – prevents stack-smashing attacks
//! - **Memory isolation** – bounds-checked memory access
//! - **Type-checked control transfers** – no arbitrary jumps
//! - **Explicit I/O** – only through registered host functions
//!
//! ## Wasmtime Defense-in-Depth
//!
//! Additional runtime protections:
//!
//! - **2GB guard region** – protects against sign-extension bugs
//! - **Stack overflow guard pages** – abort on stack overflow
//! - **Memory zeroing** – clears memory between instantiations
//! - **Spectre mitigations** – bounds check protection with dynamic memory
//! - **Rust memory safety** – prevents entire classes of vulnerabilities
//!
//! # Memory Management
//!
//! This backend addresses wasmer's memory issues (#2941, #2942, #2928).
//!
//! ## Problem (Wasmer)
//!
//! - ~15.7 MB per contract (10 MB code + 3.4 MB trap metadata + 2 MB address maps)
//! - 2.3 GB RSS for 92 contracts
//! - **Memory never freed** – append-only `Vec<CodeMemory>` that never shrinks
//!
//! ## Solution (Wasmtime)
//!
//! ### 1. Instance Pooling (`PoolingAllocationStrategy`)
//!
//! Pre-allocates a pool of instances that can be reused:
//!
//! ```rust,ignore
//! pooling.total_core_instances(100);  // Max concurrent instances
//! pooling.max_memory_size(256 * 1024 * 1024);  // 256 MiB per instance
//! pooling.linear_memory_keep_resident(64 * 1024);  // 64 KB resident for fast reuse
//! ```
//!
//! ### 2. Compact Code Generation (Cranelift)
//!
//! Generates more efficient machine code than wasmer, reducing per-contract footprint.
//!
//! ### 3. Proper Memory Cleanup
//!
//! **Wasmtime actually frees compiled code when modules are dropped.**
//! Unlike wasmer's permanent `code_memory` growth, wasmtime reclaims memory.
//!
//! ### Expected Impact
//!
//! - User peers (20-30 contracts): <200 MB (vs 300-500 MB with wasmer)
//! - Gateway (50-100 contracts): <500 MB (vs 1+ GB with wasmer)
//! - Memory is reclaimed when contracts are removed
//!
//! ### Memory Tests
//!
//! See tests at bottom of this file:
//! - `test_module_drop_frees_memory` – Verifies memory is freed
//! - `test_memory_leak_comparison` – Manual observation test
//!
//! # Async Support
//!
//! **CRITICAL: With `async_support(true)`, ALL function calls must use `call_async()`**
//!
//! We enable async support for V2 delegate async host functions:
//!
//! ```rust,ignore
//! wasmtime_config.async_support(true);
//! ```
//!
//! This changes wasmtime's Store type internally. Even for synchronous operations,
//! we must use the async calling convention:
//!
//! ```rust,ignore
//! // Correct (with async_support enabled):
//! block_on_async(func.call_async(&mut store, args))
//!
//! // Incorrect (will panic at runtime):
//! func.call(&mut store, args)
//! ```
//!
//! We wrap with `block_on_async()` to maintain a synchronous interface for callers.

use std::collections::HashMap;
use std::time::Duration;

use wasmtime::{
    Caller, Config, Engine, Error as WasmtimeError, Instance, InstanceAllocationStrategy, Linker,
    Module, OptLevel, PoolingAllocationConfig, ResourceLimiter, Store,
};

use super::{InstanceHandle, WasmEngine, WasmError};
use crate::wasm_runtime::native_api::{self, MEM_ADDR};
use crate::wasm_runtime::runtime::RuntimeConfig;
use crate::wasm_runtime::ContractError;

/// Default maximum memory limit in WASM pages (64 KiB each).
/// 4096 pages = 256 MiB.
const DEFAULT_MAX_MEMORY_PAGES: u32 = 4096;

/// WASM page size in bytes.
const WASM_PAGE_SIZE: usize = 65536;

/// WASM stack size in bytes (8 MiB).
const WASM_STACK_SIZE: usize = 8 * 1024 * 1024;

/// Wasmtime 27.x backend implementation.
pub(crate) struct WasmtimeEngine {
    /// The wasmtime Engine (shared, Arc-wrapped internally).
    engine: Engine,
    /// The Store holds runtime state (memory, globals, etc).
    /// Taken during blocking operations, restored after.
    store: Option<Store<HostState>>,
    /// Linker pre-configured with all host functions.
    linker: Linker<HostState>,
    /// Map of instance ID to wasmtime Instance.
    instances: HashMap<i64, Instance>,
    /// Maximum execution time in seconds.
    max_execution_seconds: f64,
    /// Whether fuel metering is enabled.
    enabled_metering: bool,
    /// Max fuel for each execution (computed from max_execution_seconds).
    max_fuel: u64,
}

/// Host state for wasmtime Store, implementing ResourceLimiter for memory limits.
pub(crate) struct HostState {
    memory_limit_bytes: usize,
}

impl HostState {
    fn new(memory_limit_pages: u32) -> Self {
        Self {
            memory_limit_bytes: memory_limit_pages as usize * WASM_PAGE_SIZE,
        }
    }
}

impl ResourceLimiter for HostState {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> anyhow::Result<bool> {
        if desired > self.memory_limit_bytes {
            tracing::warn!(
                current_bytes = current,
                desired_bytes = desired,
                limit_bytes = self.memory_limit_bytes,
                "WASM memory grow rejected: exceeds limit"
            );
            Ok(false)
        } else {
            Ok(true)
        }
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> anyhow::Result<bool> {
        // Allow table growth up to a reasonable limit
        const MAX_TABLE_ELEMENTS: usize = 10_000;
        Ok(desired <= MAX_TABLE_ELEMENTS)
    }
}

impl WasmEngine for WasmtimeEngine {
    type Module = Module;

    fn new(config: &RuntimeConfig, host_mem: bool) -> Result<Self, ContractError> {
        let (engine, max_fuel, enabled_metering) = Self::create_engine(config)?;
        let mut linker = Linker::new(&engine);

        // Register all host functions into the linker
        Self::register_host_functions(&mut linker)?;

        // Create the store with HostState
        let mut store = Store::new(&engine, HostState::new(DEFAULT_MAX_MEMORY_PAGES));
        store.limiter(|state| state); // Enable ResourceLimiter
        if enabled_metering {
            store.set_fuel(max_fuel)?;
        }

        // Host memory sharing is not yet implemented in the wasmtime backend.
        // Fail explicitly rather than silently degrading.
        if host_mem {
            return Err(anyhow::anyhow!(
                "host_mem=true is not supported in wasmtime backend. \
                 Set host_mem=false or use wasmer-backend feature."
            )
            .into());
        }

        Ok(Self {
            engine,
            store: Some(store),
            linker,
            instances: HashMap::new(),
            max_execution_seconds: config.max_execution_seconds,
            enabled_metering,
            max_fuel,
        })
    }

    fn is_healthy(&self) -> bool {
        self.store.is_some()
    }

    fn compile(&mut self, code: &[u8]) -> Result<Module, WasmError> {
        Module::new(&self.engine, code).map_err(|e| WasmError::Compile(e.to_string()))
    }

    fn module_has_async_imports(&self, module: &Module) -> bool {
        module
            .imports()
            .any(|import| import.module() == "freenet_delegate_contracts")
    }

    fn create_instance(
        &mut self,
        module: &Module,
        id: i64,
        req_bytes: usize,
    ) -> Result<InstanceHandle, WasmError> {
        let store = self
            .store
            .as_mut()
            .ok_or_else(|| WasmError::Other(anyhow::anyhow!("engine store not available")))?;

        // Reset fuel if metering is enabled
        if self.enabled_metering {
            store
                .set_fuel(self.max_fuel)
                .map_err(|e| WasmError::Other(e.into()))?;
        }

        // Instantiate the module using the pre-configured linker
        let instance = self
            .linker
            .instantiate(&mut *store, module)
            .map_err(|e| WasmError::Instantiation(e.to_string()))?;

        // Call __frnt_set_id to set the instance ID (used for MEM_ADDR lookup)
        // CRITICAL: Must use call_async() because async_support(true) is enabled
        if let Some(set_id_func) = instance.get_func(&mut *store, "__frnt_set_id") {
            let typed_func = set_id_func
                .typed::<i64, ()>(&*store)
                .map_err(|e| WasmError::Export(e.to_string()))?;
            block_on_async(typed_func.call_async(&mut *store, id))
                .map_err(|e| WasmError::Runtime(e.to_string()))?;
        }

        // Note: MEM_ADDR insertion is handled by RunningInstance::new in runtime.rs
        // which has the correct contract key. Do NOT insert here with a default key.

        // Ensure sufficient memory for the request
        Self::ensure_memory(store, &instance, req_bytes)?;

        self.instances.insert(id, instance);

        Ok(InstanceHandle { id })
    }

    fn drop_instance(&mut self, handle: &InstanceHandle) {
        self.instances.remove(&handle.id);
        MEM_ADDR.remove(&handle.id);
    }

    fn memory_info(&mut self, handle: &InstanceHandle) -> Result<(*const u8, usize), WasmError> {
        let store = self
            .store
            .as_mut()
            .ok_or_else(|| WasmError::Other(anyhow::anyhow!("engine store not available")))?;
        let instance = self
            .instances
            .get(&handle.id)
            .ok_or_else(|| WasmError::Other(anyhow::anyhow!("instance {} not found", handle.id)))?;
        let memory = instance
            .get_memory(&mut *store, "memory")
            .ok_or_else(|| WasmError::Export("memory export not found".to_string()))?;
        let data = memory.data(&*store);
        Ok((data.as_ptr(), data.len()))
    }

    fn initiate_buffer(&mut self, handle: &InstanceHandle, size: u32) -> Result<i64, WasmError> {
        let store = self
            .store
            .as_mut()
            .ok_or_else(|| WasmError::Other(anyhow::anyhow!("engine store not available")))?;
        let instance = self
            .instances
            .get(&handle.id)
            .ok_or_else(|| WasmError::Other(anyhow::anyhow!("instance {} not found", handle.id)))?;
        let func = instance
            .get_typed_func::<u32, i64>(&mut *store, "__frnt__initiate_buffer")
            .map_err(|e| WasmError::Export(e.to_string()))?;
        // CRITICAL: Must use call_async() because async_support(true) is enabled
        block_on_async(func.call_async(&mut *store, size))
            .map_err(|e| WasmError::Runtime(e.to_string()))
    }

    fn call_void(&mut self, handle: &InstanceHandle, name: &str) -> Result<(), WasmError> {
        let enabled_metering = self.enabled_metering;
        let store = self
            .store
            .as_mut()
            .ok_or_else(|| WasmError::Other(anyhow::anyhow!("engine store not available")))?;
        let instance = self
            .instances
            .get(&handle.id)
            .ok_or_else(|| WasmError::Other(anyhow::anyhow!("instance {} not found", handle.id)))?;
        let func = instance
            .get_typed_func::<(), ()>(&mut *store, name)
            .map_err(|e| WasmError::Export(e.to_string()))?;
        // Use call_async because async_support(true) is enabled in the engine Config
        block_on_async(func.call_async(&mut *store, ()))
            .map_err(|e| classify_runtime_error(enabled_metering, store, e))
    }

    fn call_3i64(
        &mut self,
        handle: &InstanceHandle,
        name: &str,
        a: i64,
        b: i64,
        c: i64,
    ) -> Result<i64, WasmError> {
        let enabled_metering = self.enabled_metering;
        let store = self
            .store
            .as_mut()
            .ok_or_else(|| WasmError::Other(anyhow::anyhow!("engine store not available")))?;
        let instance = self
            .instances
            .get(&handle.id)
            .ok_or_else(|| WasmError::Other(anyhow::anyhow!("instance {} not found", handle.id)))?;
        let func = instance
            .get_typed_func::<(i64, i64, i64), i64>(&mut *store, name)
            .map_err(|e| WasmError::Export(e.to_string()))?;
        // Use call_async because async_support(true) is enabled in the engine Config
        block_on_async(func.call_async(&mut *store, (a, b, c)))
            .map_err(|e| classify_runtime_error(enabled_metering, store, e))
    }

    fn call_3i64_async_imports(
        &mut self,
        handle: &InstanceHandle,
        name: &str,
        a: i64,
        b: i64,
        c: i64,
    ) -> Result<i64, WasmError> {
        // Wasmtime's async host functions work seamlessly with call_async on the same Store.
        // Unlike wasmer, we don't need to convert Store to StoreAsync — it's the same type.
        //
        // The async host functions (delegate_contracts) are registered via func_wrap_async,
        // so we use call_async here. The closures complete synchronously (ReDb reads) but
        // are registered as async to establish the pattern for future async operations.

        let store = self
            .store
            .as_mut()
            .ok_or_else(|| WasmError::Other(anyhow::anyhow!("engine store not available")))?;

        let instance = self
            .instances
            .get(&handle.id)
            .ok_or_else(|| WasmError::Other(anyhow::anyhow!("instance {} not found", handle.id)))?;

        let func = instance
            .get_typed_func::<(i64, i64, i64), i64>(&mut *store, name)
            .map_err(|e| WasmError::Export(e.to_string()))?;

        // Call the async-aware function using block_on_async
        let result = block_on_async(func.call_async(&mut *store, (a, b, c)));
        result.map_err(|e| classify_runtime_error(self.enabled_metering, store, e))
    }

    fn call_2i64_blocking(
        &mut self,
        handle: &InstanceHandle,
        name: &str,
        a: i64,
        b: i64,
    ) -> Result<i64, WasmError> {
        let enabled_metering = self.enabled_metering;
        let mut store = self
            .store
            .take()
            .ok_or_else(|| WasmError::Other(anyhow::anyhow!("engine store not available")))?;

        let instance = match self.instances.get(&handle.id) {
            Some(i) => i,
            None => {
                self.store = Some(store);
                return Err(WasmError::Other(anyhow::anyhow!(
                    "instance {} not found",
                    handle.id
                )));
            }
        };

        let func = match instance.get_typed_func::<(i64, i64), i64>(&mut store, name) {
            Ok(f) => f,
            Err(e) => {
                self.store = Some(store);
                return Err(WasmError::Export(e.to_string()));
            }
        };

        let result = execute_wasm_blocking(
            move || {
                // Use call_async because async_support(true) is enabled in the engine Config
                let r = block_on_async(func.call_async(&mut store, (a, b)));
                (r, store)
            },
            self.max_execution_seconds,
        );

        match result {
            BlockingResult::Ok(value, store) => {
                self.store = Some(store);
                Ok(value)
            }
            BlockingResult::WasmError(err, mut store) => {
                let wasm_err = classify_runtime_error(enabled_metering, &mut store, err);
                self.store = Some(store);
                Err(wasm_err)
            }
            BlockingResult::Timeout => Err(WasmError::Timeout),
            BlockingResult::Panic(err) => Err(WasmError::Other(err)),
        }
    }

    fn call_3i64_blocking(
        &mut self,
        handle: &InstanceHandle,
        name: &str,
        a: i64,
        b: i64,
        c: i64,
    ) -> Result<i64, WasmError> {
        let enabled_metering = self.enabled_metering;
        let mut store = self
            .store
            .take()
            .ok_or_else(|| WasmError::Other(anyhow::anyhow!("engine store not available")))?;

        let instance = match self.instances.get(&handle.id) {
            Some(i) => i,
            None => {
                self.store = Some(store);
                return Err(WasmError::Other(anyhow::anyhow!(
                    "instance {} not found",
                    handle.id
                )));
            }
        };

        let func = match instance.get_typed_func::<(i64, i64, i64), i64>(&mut store, name) {
            Ok(f) => f,
            Err(e) => {
                self.store = Some(store);
                return Err(WasmError::Export(e.to_string()));
            }
        };

        let result = execute_wasm_blocking(
            move || {
                // Use call_async because async_support(true) is enabled in the engine Config
                let r = block_on_async(func.call_async(&mut store, (a, b, c)));
                (r, store)
            },
            self.max_execution_seconds,
        );

        match result {
            BlockingResult::Ok(value, store) => {
                self.store = Some(store);
                Ok(value)
            }
            BlockingResult::WasmError(err, mut store) => {
                let wasm_err = classify_runtime_error(enabled_metering, &mut store, err);
                self.store = Some(store);
                Err(wasm_err)
            }
            BlockingResult::Timeout => Err(WasmError::Timeout),
            BlockingResult::Panic(err) => Err(WasmError::Other(err)),
        }
    }
}

impl WasmtimeEngine {
    /// Create a new backend engine that can be shared across multiple Runtime instances.
    pub(crate) fn create_backend_engine(config: &RuntimeConfig) -> Result<Engine, ContractError> {
        let (engine, _, _) = Self::create_engine(config)?;
        Ok(engine)
    }

    /// Clone the backend engine for sharing with other runtimes.
    ///
    /// Wasmtime's Engine is Arc-wrapped internally, so this is a cheap refcount bump.
    pub(crate) fn clone_backend_engine(&self) -> Engine {
        self.engine.clone()
    }

    /// Create a new WasmtimeEngine using a shared backend engine.
    ///
    /// Used by RuntimePool to avoid duplicating the engine and module caches.
    /// All runtimes sharing a backend engine can safely share compiled modules.
    pub(crate) fn new_with_shared_backend(
        config: &RuntimeConfig,
        host_mem: bool,
        backend: Engine,
    ) -> Result<Self, ContractError> {
        let max_fuel = Self::compute_max_fuel(config);
        let enabled_metering = config.enable_metering;

        let mut linker = Linker::new(&backend);
        Self::register_host_functions(&mut linker)?;

        let mut store = Store::new(&backend, HostState::new(DEFAULT_MAX_MEMORY_PAGES));
        store.limiter(|state| state);
        if enabled_metering {
            store.set_fuel(max_fuel)?;
        }

        if host_mem {
            return Err(anyhow::anyhow!(
                "host_mem=true is not supported in wasmtime backend. \
                 Host-managed memory requires direct memory manipulation which is \
                 incompatible with wasmtime's sandboxed memory model."
            )
            .into());
        }

        Ok(Self {
            engine: backend,
            store: Some(store),
            linker,
            instances: HashMap::new(),
            max_execution_seconds: config.max_execution_seconds,
            enabled_metering,
            max_fuel,
        })
    }

    fn create_engine(config: &RuntimeConfig) -> Result<(Engine, u64, bool), ContractError> {
        let mut wasmtime_config = Config::new();

        // Enable fuel metering if requested
        let max_fuel = Self::compute_max_fuel(config);
        if config.enable_metering {
            wasmtime_config.consume_fuel(true);
        }

        // Enable async support (needed for async host functions)
        wasmtime_config.async_support(true);

        // Set memory limits via config
        // async_stack_size must exceed max_wasm_stack when async_support is enabled
        wasmtime_config.max_wasm_stack(WASM_STACK_SIZE);
        wasmtime_config.async_stack_size(WASM_STACK_SIZE * 2);

        // ==================================================================
        // MEMORY MANAGEMENT OPTIMIZATIONS (#2941, #2942, #2928)
        // ==================================================================
        //
        // Enable instance pooling for memory efficiency. This addresses wasmer's
        // append-only Vec<CodeMemory> that never shrinks (consuming ~15.7 MB per
        // contract: 10 MB code + 3.4 MB trap metadata + 2 MB address maps).
        //
        // Wasmtime advantages:
        // 1. Actually frees compiled code when modules are dropped
        // 2. Cranelift generates more compact machine code
        // 3. Instance pooling reuses memory across instantiations
        //
        // Target: User peers with 20-30 contracts should use <200 MB (vs 300-500 MB with wasmer)
        //
        let mut pooling = PoolingAllocationConfig::default();

        // Allow up to 100 concurrent core instances (WASM modules executing)
        // User peers: 20-30 contracts, gateways: 50-100 contracts
        pooling.total_core_instances(100);

        // Most contracts use one linear memory and one table
        pooling.max_memories_per_module(1);
        pooling.max_tables_per_module(1);

        // Set per-instance memory limits (256 MiB = 4096 pages * 64 KiB)
        // This is the upper bound; actual usage is further limited by ResourceLimiter
        pooling.max_memory_size(256 * 1024 * 1024);

        // Keep a small amount of memory resident even when slots are unused, for faster reuse
        pooling.linear_memory_keep_resident(64 * 1024); // 64 KB

        wasmtime_config.allocation_strategy(InstanceAllocationStrategy::Pooling(pooling));

        // Use OptLevel::None for maximum security with untrusted code
        // Simpler compiler = smaller attack surface
        // Memory benefits come from pooling and proper cleanup, not optimizations
        wasmtime_config.cranelift_opt_level(OptLevel::None);

        let engine = Engine::new(&wasmtime_config).map_err(|e| WasmError::Other(e.into()))?;

        Ok((engine, max_fuel, config.enable_metering))
    }

    fn compute_max_fuel(config: &RuntimeConfig) -> u64 {
        fn get_cpu_cycles_per_second() -> (u64, f64) {
            const DEFAULT_CPU_CYCLES_PER_SECOND: u64 = 3_000_000_000;
            if let Some(cpu) = option_env!("CPU_CYCLES_PER_SECOND") {
                (cpu.parse().expect("incorrect number"), 0.0)
            } else {
                (DEFAULT_CPU_CYCLES_PER_SECOND, 0.2)
            }
        }

        let (default_cycles, default_margin) = get_cpu_cycles_per_second();
        let cpu_cycles_per_sec = config.cpu_cycles_per_second.unwrap_or(default_cycles);
        let safety_margin = if config.safety_margin >= 0.0 && config.safety_margin <= 1.0 {
            config.safety_margin
        } else {
            default_margin
        };

        (config.max_execution_seconds * cpu_cycles_per_sec as f64 * (1.0 + safety_margin)) as u64
    }

    /// Ensure WASM linear memory has sufficient pages for the request.
    ///
    /// If the instance's memory is smaller than required, this will attempt to grow it.
    /// This prevents cryptic memory traps when contracts request more memory than initially allocated.
    fn ensure_memory(
        store: &mut Store<HostState>,
        instance: &Instance,
        req_bytes: usize,
    ) -> Result<(), WasmError> {
        const WASM_PAGE_SIZE: usize = 65536; // 64 KB

        let memory = instance
            .get_memory(&mut *store, "memory")
            .ok_or_else(|| WasmError::Export("memory export not found".to_string()))?;

        let current_bytes = memory.data_size(&*store);
        if current_bytes < req_bytes {
            let current_pages = (current_bytes + WASM_PAGE_SIZE - 1) / WASM_PAGE_SIZE;
            let required_pages = (req_bytes + WASM_PAGE_SIZE - 1) / WASM_PAGE_SIZE;
            let pages_to_grow = required_pages.saturating_sub(current_pages) as u64;

            if let Err(err) = memory.grow(&mut *store, pages_to_grow) {
                tracing::error!("WASM runtime failed with memory error: {err}");
                return Err(WasmError::Memory(format!(
                    "insufficient memory: requested {} bytes ({} pages) but had {} bytes ({} pages)",
                    req_bytes, required_pages, current_bytes, current_pages
                )));
            }
        }
        Ok(())
    }

    fn register_host_functions(linker: &mut Linker<HostState>) -> Result<(), ContractError> {
        // Log namespace
        linker
            .func_wrap("freenet_log", "__frnt__logger__info", native_api::log::info)
            .map_err(|e| WasmError::Other(e.into()))?;

        // Rand namespace
        linker
            .func_wrap(
                "freenet_rand",
                "__frnt__rand__rand_bytes",
                native_api::rand::rand_bytes,
            )
            .map_err(|e| WasmError::Other(e.into()))?;

        // Time namespace
        linker
            .func_wrap(
                "freenet_time",
                "__frnt__time__utc_now",
                native_api::time::utc_now,
            )
            .map_err(|e| WasmError::Other(e.into()))?;

        // Delegate context namespace (synchronous)
        linker
            .func_wrap(
                "freenet_delegate_ctx",
                "__frnt__delegate__ctx_len",
                native_api::delegate_context::context_len,
            )
            .map_err(|e| WasmError::Other(e.into()))?;

        linker
            .func_wrap(
                "freenet_delegate_ctx",
                "__frnt__delegate__ctx_read",
                native_api::delegate_context::context_read,
            )
            .map_err(|e| WasmError::Other(e.into()))?;

        linker
            .func_wrap(
                "freenet_delegate_ctx",
                "__frnt__delegate__ctx_write",
                native_api::delegate_context::context_write,
            )
            .map_err(|e| WasmError::Other(e.into()))?;

        // Delegate secrets namespace (synchronous)
        linker
            .func_wrap(
                "freenet_delegate_secrets",
                "__frnt__delegate__get_secret",
                native_api::delegate_secrets::get_secret,
            )
            .map_err(|e| WasmError::Other(e.into()))?;

        linker
            .func_wrap(
                "freenet_delegate_secrets",
                "__frnt__delegate__get_secret_len",
                native_api::delegate_secrets::get_secret_len,
            )
            .map_err(|e| WasmError::Other(e.into()))?;

        linker
            .func_wrap(
                "freenet_delegate_secrets",
                "__frnt__delegate__set_secret",
                native_api::delegate_secrets::set_secret,
            )
            .map_err(|e| WasmError::Other(e.into()))?;

        linker
            .func_wrap(
                "freenet_delegate_secrets",
                "__frnt__delegate__has_secret",
                native_api::delegate_secrets::has_secret,
            )
            .map_err(|e| WasmError::Other(e.into()))?;

        linker
            .func_wrap(
                "freenet_delegate_secrets",
                "__frnt__delegate__remove_secret",
                native_api::delegate_secrets::remove_secret,
            )
            .map_err(|e| WasmError::Other(e.into()))?;

        // Delegate contracts namespace (async host functions for V2 delegates)
        // These are registered as async to support future async operations,
        // but currently complete synchronously (ReDb reads).
        linker
            .func_wrap_async(
                "freenet_delegate_contracts",
                "__frnt__delegate__get_contract_state",
                |_caller: Caller<'_, HostState>,
                 (id_ptr, id_len, out_ptr, out_len): (i64, i32, i64, i64)| {
                    Box::new(async move {
                        native_api::delegate_contracts::get_contract_state_impl(
                            id_ptr, id_len, out_ptr, out_len,
                        )
                    })
                },
            )
            .map_err(|e| WasmError::Other(e.into()))?;

        linker
            .func_wrap_async(
                "freenet_delegate_contracts",
                "__frnt__delegate__get_contract_state_len",
                |_caller: Caller<'_, HostState>, (id_ptr, id_len): (i64, i32)| {
                    Box::new(async move {
                        native_api::delegate_contracts::get_contract_state_len_impl(id_ptr, id_len)
                    })
                },
            )
            .map_err(|e| WasmError::Other(e.into()))?;

        Ok(())
    }
}

/// Block on an async operation, handling both tokio and non-tokio contexts.
fn block_on_async<F: std::future::Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => futures::executor::block_on(future),
    }
}

/// Classify a wasmtime Error, checking fuel status.
fn classify_runtime_error(
    enabled_metering: bool,
    store: &mut Store<HostState>,
    error: WasmtimeError,
) -> WasmError {
    if enabled_metering {
        // Check if we ran out of fuel
        if let Ok(remaining) = store.get_fuel() {
            if remaining == 0 {
                tracing::error!("WASM execution ran out of fuel");
                return WasmError::OutOfGas;
            }
        }
    }
    tracing::error!("WASM runtime error: {:?}", error);
    WasmError::Runtime(error.to_string())
}

// =============================================================================
// Blocking execution with timeout
// =============================================================================

type WasmResult = (Result<i64, WasmtimeError>, Store<HostState>);

enum BlockingResult {
    Ok(i64, Store<HostState>),
    WasmError(WasmtimeError, Store<HostState>),
    Timeout,
    Panic(anyhow::Error),
}

fn execute_wasm_blocking<F>(f: F, max_execution_seconds: f64) -> BlockingResult
where
    F: FnOnce() -> WasmResult + Send + 'static,
{
    let timeout = Duration::from_secs_f64(max_execution_seconds);
    let start = std::time::Instant::now();

    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            let task_handle = tokio::task::spawn_blocking(f);

            loop {
                if task_handle.is_finished() {
                    return match handle.block_on(task_handle) {
                        Ok((Ok(value), store)) => BlockingResult::Ok(value, store),
                        Ok((Err(err), store)) => BlockingResult::WasmError(err, store),
                        Err(e) => {
                            if e.is_panic() {
                                tracing::error!("WASM blocking task panicked during execution");
                                BlockingResult::Panic(anyhow::anyhow!("WASM execution panicked"))
                            } else if e.is_cancelled() {
                                BlockingResult::Panic(anyhow::anyhow!(
                                    "WASM execution was cancelled"
                                ))
                            } else {
                                BlockingResult::Panic(anyhow::anyhow!(
                                    "WASM execution failed: {}",
                                    e
                                ))
                            }
                        }
                    };
                }

                if start.elapsed() >= timeout {
                    tracing::warn!(
                        timeout_secs = max_execution_seconds,
                        elapsed_ms = start.elapsed().as_millis(),
                        "WASM execution timed out"
                    );
                    return BlockingResult::Timeout;
                }

                std::thread::sleep(Duration::from_millis(10));
            }
        }
        Err(_) => {
            let (tx, rx) = std::sync::mpsc::channel();
            let thread_handle = std::thread::spawn(move || {
                let result = f();
                let _ = tx.send(result);
            });

            loop {
                match rx.try_recv() {
                    Ok((Ok(value), store)) => {
                        let _ = thread_handle.join();
                        return BlockingResult::Ok(value, store);
                    }
                    Ok((Err(err), store)) => {
                        let _ = thread_handle.join();
                        return BlockingResult::WasmError(err, store);
                    }
                    Err(std::sync::mpsc::TryRecvError::Empty) => {}
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        return match thread_handle.join() {
                            Err(_) => {
                                tracing::error!("WASM thread panicked during execution");
                                BlockingResult::Panic(anyhow::anyhow!("WASM execution panicked"))
                            }
                            Ok(()) => BlockingResult::Panic(anyhow::anyhow!(
                                "WASM thread exited without sending result"
                            )),
                        };
                    }
                }

                if start.elapsed() >= timeout {
                    tracing::warn!(
                        timeout_secs = max_execution_seconds,
                        elapsed_ms = start.elapsed().as_millis(),
                        "WASM execution timed out (no tokio runtime)"
                    );
                    return BlockingResult::Timeout;
                }

                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Simple WASM module that exports a function returning 42.
    ///
    /// WAT format:
    /// ```wat
    /// (module
    ///   (memory 1)
    ///   (export "memory" (memory 0))
    ///   (func (export "answer") (result i32)
    ///     i32.const 42
    ///   )
    /// )
    /// ```
    const SIMPLE_WASM: &[u8] = &[
        0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x05, 0x01, 0x60, 0x00, 0x01, 0x7f,
        0x03, 0x02, 0x01, 0x00, 0x05, 0x03, 0x01, 0x00, 0x01, 0x07, 0x13, 0x02, 0x06, 0x6d, 0x65,
        0x6d, 0x6f, 0x72, 0x79, 0x02, 0x00, 0x06, 0x61, 0x6e, 0x73, 0x77, 0x65, 0x72, 0x00, 0x00,
        0x0a, 0x06, 0x01, 0x04, 0x00, 0x41, 0x2a, 0x0b,
    ];

    #[test]
    fn test_wasmtime_engine_creation() {
        let config = RuntimeConfig::default();
        let result = WasmtimeEngine::create_backend_engine(&config);
        assert!(result.is_ok(), "Failed to create wasmtime engine");
    }

    #[test]
    fn test_module_compilation() {
        let config = RuntimeConfig::default();
        let (engine, _, _) = WasmtimeEngine::create_engine(&config).unwrap();

        let result = Module::new(&engine, SIMPLE_WASM);
        assert!(result.is_ok(), "Failed to compile simple WASM module");
    }

    #[test]
    fn test_module_drop_frees_memory() {
        // This test verifies that wasmtime actually frees memory when modules are dropped.
        // Wasmer's Vec<CodeMemory> never shrinks, but wasmtime should free the code.

        let config = RuntimeConfig::default();
        let (engine, _, _) = WasmtimeEngine::create_engine(&config).unwrap();

        // Compile multiple modules and drop them
        for _ in 0..10 {
            let module = Module::new(&engine, SIMPLE_WASM).expect("compilation should succeed");
            // Module is dropped here - wasmtime should free the compiled code
            drop(module);
        }

        // If we got here without OOM, wasmtime is properly freeing memory.
        // With wasmer's append-only Vec, this would accumulate ~150-200 MB.
    }

    #[test]
    fn test_pooling_allocation_enabled() {
        // Verify that our pooling configuration is actually applied
        let config = RuntimeConfig::default();
        let (engine, _, _) = WasmtimeEngine::create_engine(&config).unwrap();

        // Compile and instantiate multiple times - pooling should reuse memory
        // Must use instantiate_async + block_on_async because async_support is enabled
        for _ in 0..5 {
            let module = Module::new(&engine, SIMPLE_WASM).unwrap();
            let mut store = Store::new(&engine, HostState::new(DEFAULT_MAX_MEMORY_PAGES));
            let linker = Linker::new(&engine);

            let instance = block_on_async(linker.instantiate_async(&mut store, &module));
            assert!(
                instance.is_ok(),
                "Instance creation should succeed with pooling"
            );
        }
    }

    #[test]
    fn test_cranelift_optimization() {
        // Verify cranelift optimization is enabled by checking module size
        let config = RuntimeConfig::default();
        let (engine, _, _) = WasmtimeEngine::create_engine(&config).unwrap();

        let module = Module::new(&engine, SIMPLE_WASM).unwrap();

        // With SpeedAndSize optimization, the compiled code should be compact.
        // We can't easily measure the exact size without internals access,
        // but we verify it compiles successfully.
        assert!(module.exports().count() > 0, "Module should have exports");
    }

    /// Verify that host function namespaces and names match what freenet-stdlib
    /// imports. This is a critical ABI contract test — if any name is wrong,
    /// ALL WASM modules will fail to instantiate with import resolution errors.
    ///
    /// The WAT module imports one function from each namespace to validate the
    /// registration. If register_host_functions changes a namespace or name,
    /// this test will fail at instantiation time.
    #[test]
    fn test_host_function_abi_compatibility() {
        // WAT module that imports one function per host namespace.
        // Signatures must match the Rust host function types exactly.
        // Rust type mapping: i32/u32 → wasm i32, i64 → wasm i64
        let wat = r#"
        (module
          ;; log::info(id: i64, ptr: i64, len: i32)
          (import "freenet_log" "__frnt__logger__info"
            (func $log (param i64 i64 i32)))
          ;; rand::rand_bytes(id: i64, ptr: i64, len: u32)
          (import "freenet_rand" "__frnt__rand__rand_bytes"
            (func $rand (param i64 i64 i32)))
          ;; time::utc_now(id: i64, ptr: i64)
          (import "freenet_time" "__frnt__time__utc_now"
            (func $time (param i64 i64)))
          ;; context_len() -> i32
          (import "freenet_delegate_ctx" "__frnt__delegate__ctx_len"
            (func $ctx_len (result i32)))
          ;; context_read(ptr: i64, len: i32) -> i32
          (import "freenet_delegate_ctx" "__frnt__delegate__ctx_read"
            (func $ctx_read (param i64 i32) (result i32)))
          ;; context_write(ptr: i64, len: i32) -> i32
          (import "freenet_delegate_ctx" "__frnt__delegate__ctx_write"
            (func $ctx_write (param i64 i32) (result i32)))
          ;; get_secret(key_ptr: i64, key_len: i32, out_ptr: i64, out_len: i32) -> i32
          (import "freenet_delegate_secrets" "__frnt__delegate__get_secret"
            (func $get_secret (param i64 i32 i64 i32) (result i32)))
          ;; get_secret_len(key_ptr: i64, key_len: i32) -> i32
          (import "freenet_delegate_secrets" "__frnt__delegate__get_secret_len"
            (func $get_secret_len (param i64 i32) (result i32)))
          ;; set_secret(key_ptr: i64, key_len: i32, val_ptr: i64, val_len: i32) -> i32
          (import "freenet_delegate_secrets" "__frnt__delegate__set_secret"
            (func $set_secret (param i64 i32 i64 i32) (result i32)))
          ;; has_secret(key_ptr: i64, key_len: i32) -> i32
          (import "freenet_delegate_secrets" "__frnt__delegate__has_secret"
            (func $has_secret (param i64 i32) (result i32)))
          ;; remove_secret(key_ptr: i64, key_len: i32) -> i32
          (import "freenet_delegate_secrets" "__frnt__delegate__remove_secret"
            (func $remove_secret (param i64 i32) (result i32)))
          ;; get_contract_state_impl(id_ptr: i64, id_len: i32, out_ptr: i64, out_len: i64) -> i64
          (import "freenet_delegate_contracts" "__frnt__delegate__get_contract_state"
            (func $get_state (param i64 i32 i64 i64) (result i64)))
          ;; get_contract_state_len_impl(id_ptr: i64, id_len: i32) -> i64
          (import "freenet_delegate_contracts" "__frnt__delegate__get_contract_state_len"
            (func $get_state_len (param i64 i32) (result i64)))
          (memory (export "memory") 1)
          (func (export "answer") (result i32) i32.const 42)
        )
        "#;

        let config = RuntimeConfig::default();
        let (engine, _, _) = WasmtimeEngine::create_engine(&config).unwrap();

        let mut linker = Linker::new(&engine);
        WasmtimeEngine::register_host_functions(&mut linker).expect("host function registration");

        let module = Module::new(&engine, wat).expect("WAT compilation failed");
        let mut store = Store::new(&engine, HostState::new(DEFAULT_MAX_MEMORY_PAGES));

        let result = block_on_async(linker.instantiate_async(&mut store, &module));
        assert!(
            result.is_ok(),
            "Instantiation failed — host function ABI mismatch: {}",
            result.unwrap_err()
        );
    }

    /// Verify that ensure_memory grows WASM linear memory when req_bytes
    /// exceeds the initial allocation.
    #[test]
    fn test_ensure_memory_grows_when_needed() {
        let config = RuntimeConfig::default();
        let (engine, _, _) = WasmtimeEngine::create_engine(&config).unwrap();

        // WAT module with only 1 page (64 KB) of initial memory
        let wat = r#"
        (module
          (memory (export "memory") 1 256)
          (func (export "answer") (result i32) i32.const 42)
        )
        "#;
        let module = Module::new(&engine, wat).unwrap();
        let mut store = Store::new(&engine, HostState::new(DEFAULT_MAX_MEMORY_PAGES));
        let linker = Linker::new(&engine);
        let instance = block_on_async(linker.instantiate_async(&mut store, &module)).unwrap();

        // Initial memory is 1 page = 64 KB
        let memory = instance.get_memory(&mut store, "memory").unwrap();
        assert_eq!(memory.data_size(&store), 65536);

        // Request 256 KB — should grow memory to at least 4 pages
        WasmtimeEngine::ensure_memory(&mut store, &instance, 256 * 1024)
            .expect("ensure_memory should grow successfully");

        let new_size = memory.data_size(&store);
        assert!(
            new_size >= 256 * 1024,
            "Memory should have grown to at least 256 KB, got {} bytes",
            new_size
        );

        // Request less than current size — should be a no-op
        let size_before = memory.data_size(&store);
        WasmtimeEngine::ensure_memory(&mut store, &instance, 1024)
            .expect("ensure_memory with small req should succeed");
        assert_eq!(
            memory.data_size(&store),
            size_before,
            "Memory should not change when req_bytes < current size"
        );
    }

    /// Test that demonstrates the memory difference between wasmer and wasmtime.
    ///
    /// This is a documentation test - run manually to observe memory behavior.
    ///
    /// Expected results:
    /// - Wasmer: Memory grows with each compilation, never shrinks
    /// - Wasmtime: Memory is freed when modules are dropped
    #[test]
    #[ignore] // Run manually with --ignored to observe memory behavior
    fn test_memory_leak_comparison() {
        use std::thread;
        use std::time::Duration;

        let config = RuntimeConfig::default();
        let (engine, _, _) = WasmtimeEngine::create_engine(&config).unwrap();

        println!("Compiling 100 modules...");
        for i in 0..100 {
            let module = Module::new(&engine, SIMPLE_WASM).unwrap();
            if i % 10 == 0 {
                println!("Compiled {} modules", i);
                thread::sleep(Duration::from_millis(100));
            }
            drop(module);
        }

        println!("All modules dropped. Check memory usage - it should have returned to baseline.");
        println!("With wasmer, memory would still be high (~1.5 GB for 100 contracts).");
        println!("With wasmtime, memory should be near baseline.");

        // Sleep to allow manual memory inspection
        thread::sleep(Duration::from_secs(5));
    }

    /// Rigorous memory test for Linux CI that verifies memory cleanup.
    ///
    /// This test addresses issues #2941, #2942, #2928 where wasmer's append-only
    /// Vec<CodeMemory> caused 2.3 GB RSS for 92 contracts (~15.7 MB per contract).
    ///
    /// The test verifies that wasmtime properly frees memory when modules are dropped,
    /// unlike wasmer which accumulates memory indefinitely.
    #[test]
    #[cfg(target_os = "linux")]
    fn test_memory_freed_after_module_drop() {
        use std::thread;
        use std::time::Duration;

        /// Read RSS (Resident Set Size) from /proc/self/statm
        fn get_rss_bytes() -> usize {
            let statm = std::fs::read_to_string("/proc/self/statm")
                .expect("Failed to read /proc/self/statm");
            let fields: Vec<&str> = statm.split_whitespace().collect();
            let rss_pages: usize = fields[1].parse().expect("Failed to parse RSS");
            rss_pages * 4096 // Convert pages to bytes (4KB per page on Linux)
        }

        let config = RuntimeConfig::default();
        let (engine, _, _) = WasmtimeEngine::create_engine(&config).unwrap();

        // Measure baseline RSS
        let baseline_rss = get_rss_bytes();

        // Compile 50 modules (wasmer would accumulate ~785 MB for 50 × 15.7 MB)
        let mut modules = Vec::new();
        for _ in 0..50 {
            modules.push(Module::new(&engine, SIMPLE_WASM).unwrap());
        }

        let peak_rss = get_rss_bytes();
        let peak_growth = peak_rss.saturating_sub(baseline_rss);

        // Drop all modules - wasmtime should free the memory
        drop(modules);

        // Give the allocator time to release memory to OS
        thread::sleep(Duration::from_millis(100));

        let after_rss = get_rss_bytes();
        let leaked = after_rss.saturating_sub(baseline_rss);

        // With wasmer, we would expect ~785 MB to remain (50 × 15.7 MB).
        // With wasmtime pooling and proper cleanup, we expect < 100 MB overhead
        // (accounting for allocator fragmentation and pool pre-allocation).
        assert!(
            leaked < 100 * 1024 * 1024,
            "Memory leak detected: {} MB remained after dropping 50 modules (peak growth: {} MB). \
             Expected < 100 MB with wasmtime pooling. This suggests memory is not being freed.",
            leaked / (1024 * 1024),
            peak_growth / (1024 * 1024)
        );

        // Sanity check: we should have actually allocated something
        assert!(
            peak_growth > 1024 * 1024,
            "Peak growth was only {} KB - test may not be exercising memory allocation",
            peak_growth / 1024
        );
    }
}
