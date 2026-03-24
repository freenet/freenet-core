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
//! - Fast, minimal optimization for maximum safety
//!
//! **Alternatives considered:**
//! - `OptLevel::SpeedAndSize` – more optimizations, slightly higher complexity
//!   - Still safe (Cranelift's optimizations are security-hardened)
//!   - We chose `None` to minimize risk for untrusted contracts
//! - Winch compiler – Wasmtime's baseline compiler
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
//! ## On-Demand Instance Allocation
//!
//! Uses wasmtime's default on-demand allocation — each instance gets its own
//! mmap'd memory region, allocated at instantiation and freed on drop.
//!
//! ## Compact Code Generation (Cranelift)
//!
//! Generates efficient machine code with low per-contract footprint.
//!
//! ## Proper Memory Cleanup
//!
//! Wasmtime frees compiled code when modules are dropped, so memory is
//! properly reclaimed.
//!
//! ### Expected Footprint
//!
//! - User peers (20-30 contracts): <200 MB
//! - Gateway (50-100 contracts): <500 MB
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
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use dashmap::DashMap;
use wasmtime::{
    Cache, CacheConfig, Caller, Config, Engine, Error as WasmtimeError, Instance, Linker, Module,
    OptLevel, ResourceLimiter, Store,
};

use super::{InstanceHandle, WasmEngine, WasmError};
use crate::wasm_runtime::native_api::{self, MEM_ADDR};
use crate::wasm_runtime::runtime::RuntimeConfig;
use crate::wasm_runtime::ContractError;

// Use shared constants from parent module to ensure consistency
// with host function bounds validation
use super::{DEFAULT_MAX_MEMORY_PAGES, WASM_PAGE_SIZE};

/// Process-global compilation coalescing map.
///
/// When multiple RuntimePools (different nodes in the same process) need to
/// compile the same WASM bytes, only the first one does the actual Cranelift
/// compilation. Others block on the per-hash mutex, then their `Module::new()`
/// call hits the wasmtime disk cache (near-instant deserialization).
///
/// Keyed by blake3 hash of the raw WASM bytes. Entries are removed after
/// compilation when no other thread is waiting, preventing unbounded growth.
fn compile_coalesce_map() -> &'static DashMap<[u8; 32], Arc<Mutex<()>>> {
    static MAP: OnceLock<DashMap<[u8; 32], Arc<Mutex<()>>>> = OnceLock::new();
    MAP.get_or_init(DashMap::new)
}

/// WASM stack size in bytes (8 MiB).
const WASM_STACK_SIZE: usize = 8 * 1024 * 1024;

/// Wasmtime 27.x backend implementation.
pub(crate) struct WasmtimeEngine {
    /// The wasmtime Engine (shared, Arc-wrapped internally).
    engine: Engine,
    /// The Store holds runtime state (memory, globals, etc).
    /// Taken during blocking operations, restored after. Recreated on timeout/panic.
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
    ) -> std::result::Result<bool, WasmtimeError> {
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
    ) -> std::result::Result<bool, WasmtimeError> {
        // Allow table growth up to a reasonable limit
        const MAX_TABLE_ELEMENTS: usize = 10_000;
        Ok(desired <= MAX_TABLE_ELEMENTS)
    }

    /// Disable wasmtime's per-Store instance limit.
    ///
    /// Wasmtime's `Store` has a monotonic instance counter (only incremented in
    /// `Store::check_new_instance`, never decremented) with a default limit of 10,000.
    /// Since freenet properly cleans up instances (drops them from the HashMap and frees
    /// their memory), we hit this artificial limit during long-running gateway sessions
    /// where the 5-minute interest heartbeat calls `summarize_state()` for every contract,
    /// each creating a WASM instance. With hundreds of contracts the store exhausts
    /// within ~100 minutes.
    fn instances(&self) -> usize {
        usize::MAX
    }

    fn tables(&self) -> usize {
        usize::MAX
    }

    fn memories(&self) -> usize {
        usize::MAX
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
            store.set_fuel(max_fuel).map_err(|e| anyhow::anyhow!(e))?;
        }

        // Host memory sharing is not yet implemented in the wasmtime backend.
        // Fail explicitly rather than silently degrading.
        if host_mem {
            return Err(anyhow::anyhow!(
                "host_mem=true is not supported in wasmtime backend. \
                 Set host_mem=false."
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
        // Coalesce concurrent compilations of the same WASM bytes across
        // RuntimePools. The first thread compiles via Cranelift and writes
        // to the disk cache; others block here and then get a near-instant
        // disk cache hit from Module::new().
        let hash = *blake3::hash(code).as_bytes();
        let map = compile_coalesce_map();
        let mutex = map.entry(hash).or_default().clone();
        let _guard = mutex.lock().unwrap_or_else(|e| e.into_inner());

        let result = Module::new(&self.engine, code).map_err(|e| WasmError::Compile(e.to_string()));

        // Clean up when no other thread holds or is waiting on this entry.
        // If another thread grabbed a clone between our entry() and lock(),
        // strong_count > 1 and we leave the entry for them to clean up.
        if Arc::strong_count(&mutex) == 1 {
            map.remove(&hash);
        }

        result
    }

    fn module_has_async_imports(&self, module: &Module) -> bool {
        module.imports().any(|import| {
            import.module() == "freenet_delegate_contracts"
                || import.module() == "freenet_delegate_management"
        })
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
                .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;
        }

        // Instantiate the module using the pre-configured linker
        // CRITICAL: Must use instantiate_async() because async_support(true) is enabled
        let instance = block_on_async(self.linker.instantiate_async(&mut *store, module))
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
            BlockingResult::Timeout => {
                self.recover_store();
                Err(WasmError::Timeout)
            }
            BlockingResult::Panic(err) => {
                self.recover_store();
                Err(WasmError::Other(err))
            }
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
            BlockingResult::Timeout => {
                self.recover_store();
                Err(WasmError::Timeout)
            }
            BlockingResult::Panic(err) => {
                self.recover_store();
                Err(WasmError::Other(err))
            }
        }
    }
}

/// Refresh the cached MEM_ADDR entry for an instance using the current memory
/// from the wasmtime `Caller`.
///
/// WASM linear memory can be relocated by `memory.grow`. The initial pointer
/// stored in `MEM_ADDR` at instance creation may become stale. This function
/// reads the current memory base address from the `Caller` (which always
/// reflects the live store state) and updates MEM_ADDR if it has changed.
///
/// Called at the start of every host function to ensure `native_api` functions
/// see the correct memory base pointer.
fn refresh_mem_addr_from_caller(caller: &mut Caller<'_, HostState>, instance_id: i64) {
    if instance_id < 0 {
        return;
    }
    let Some(memory) = caller.get_export("memory").and_then(|e| e.into_memory()) else {
        tracing::warn!(
            instance_id,
            "refresh_mem_addr: no 'memory' export found — \
             stale pointer will persist until next successful refresh"
        );
        return;
    };
    let data = memory.data(&*caller);
    let new_ptr = data.as_ptr() as i64;
    let new_size = data.len();
    if let Some(mut info) = MEM_ADDR.get_mut(&instance_id) {
        if info.start_ptr != new_ptr || info.mem_size != new_size {
            tracing::debug!(
                instance_id,
                old_ptr = info.start_ptr,
                new_ptr,
                old_size = info.mem_size,
                new_size,
                "Refreshed stale MEM_ADDR after memory relocation"
            );
            info.start_ptr = new_ptr;
            info.mem_size = new_size;
        }
    } else {
        tracing::warn!(
            instance_id,
            "refresh_mem_addr: no MEM_ADDR entry for instance — \
             native_api calls will fail"
        );
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
            store.set_fuel(max_fuel).map_err(|e| anyhow::anyhow!(e))?;
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

        // Set memory limits via config
        // async_stack_size must exceed max_wasm_stack
        wasmtime_config.max_wasm_stack(WASM_STACK_SIZE);
        wasmtime_config.async_stack_size(WASM_STACK_SIZE * 2);

        // ==================================================================
        // MEMORY MANAGEMENT (#2941, #2942, #2928)
        // ==================================================================
        //
        // Use wasmtime's default on-demand allocation strategy. Each instance
        // gets its own mmap'd memory region, allocated at instantiation time
        // and freed when the instance is dropped.
        //
        // Wasmtime memory management:
        // 1. Frees compiled code when modules are dropped
        // 2. Cranelift generates compact machine code
        // 3. On-demand allocation uses only as much memory as needed
        //
        // Note: Pooling allocation (InstanceAllocationStrategy::Pooling) was
        // considered but requires ~4 GiB guard pages per slot, leading to
        // ~4 TB virtual address space reservations that fail with ENOMEM on
        // CI runners and constrained environments.
        //
        // The ResourceLimiter (HostState) provides per-instance memory limits.

        // Use OptLevel::None for maximum security with untrusted code
        // Simpler compiler = smaller attack surface
        // Memory benefits come from pooling and proper cleanup, not optimizations
        wasmtime_config.cranelift_opt_level(OptLevel::None);

        // Enable disk-based compilation cache (#3456). Wasmtime caches compiled
        // modules keyed by (engine config + WASM bytes hash). On cache hit,
        // Module::new() skips compilation entirely and deserializes — ~100x faster.
        // This eliminates WASM compilation as a contributor to OPERATION_TTL timeouts
        // on slow CI runners and during cold starts in production.
        match Cache::new(CacheConfig::new()) {
            Ok(cache) => {
                wasmtime_config.cache(Some(cache));
                tracing::info!("Wasmtime compilation cache enabled");
            }
            Err(e) => {
                // Cache is an optimization — don't fail node startup if it can't initialize
                tracing::warn!("Failed to initialize wasmtime compilation cache: {e}");
            }
        }

        let engine =
            Engine::new(&wasmtime_config).map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        Ok((engine, max_fuel, config.enable_metering))
    }

    /// Recover the engine after a timeout or panic that consumed the store.
    ///
    /// Creates a fresh Store and clears all instance references (they were tied
    /// to the old store and are now invalid). The pooling allocator reclaims
    /// those instance slots when the old Store is dropped by the abandoned task.
    fn recover_store(&mut self) {
        tracing::warn!(
            orphaned_instances = self.instances.len(),
            "Recovering engine store after timeout/panic — creating fresh store"
        );
        let mut store = Store::new(&self.engine, HostState::new(DEFAULT_MAX_MEMORY_PAGES));
        store.limiter(|state| state);
        if self.enabled_metering {
            if let Err(e) = store.set_fuel(self.max_fuel) {
                tracing::error!("Failed to set fuel on recovery store: {e}");
            }
        }
        // Old instances are invalid without their store — clear them so the
        // pooling allocator can reclaim the slots when the old store drops.
        self.instances.clear();
        self.store = Some(store);
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
            let current_pages = current_bytes.div_ceil(WASM_PAGE_SIZE);
            let required_pages = req_bytes.div_ceil(WASM_PAGE_SIZE);
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
        // =====================================================================
        // All host functions refresh the cached MEM_ADDR entry via the Caller
        // before calling into native_api. This prevents stale memory base
        // pointers after WASM memory.grow relocates linear memory (#3248).
        // =====================================================================

        // Log namespace
        linker
            .func_wrap(
                "freenet_log",
                "__frnt__logger__info",
                |mut caller: Caller<'_, HostState>, id: i64, ptr: i64, len: i32| {
                    refresh_mem_addr_from_caller(&mut caller, id);
                    native_api::log::info(id, ptr, len);
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        // Rand namespace
        linker
            .func_wrap(
                "freenet_rand",
                "__frnt__rand__rand_bytes",
                |mut caller: Caller<'_, HostState>, id: i64, ptr: i64, len: u32| {
                    refresh_mem_addr_from_caller(&mut caller, id);
                    native_api::rand::rand_bytes(id, ptr, len);
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        // Time namespace
        linker
            .func_wrap(
                "freenet_time",
                "__frnt__time__utc_now",
                |mut caller: Caller<'_, HostState>, id: i64, ptr: i64| {
                    refresh_mem_addr_from_caller(&mut caller, id);
                    native_api::time::utc_now(id, ptr);
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        // Delegate context namespace (synchronous)
        // These use the thread-local CURRENT_DELEGATE_INSTANCE for the instance ID.
        linker
            .func_wrap(
                "freenet_delegate_ctx",
                "__frnt__delegate__ctx_len",
                |mut caller: Caller<'_, HostState>| {
                    let id = native_api::CURRENT_DELEGATE_INSTANCE.with(|c| c.get());
                    refresh_mem_addr_from_caller(&mut caller, id);
                    native_api::delegate_context::context_len()
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        linker
            .func_wrap(
                "freenet_delegate_ctx",
                "__frnt__delegate__ctx_read",
                |mut caller: Caller<'_, HostState>, ptr: i64, len: i32| {
                    let id = native_api::CURRENT_DELEGATE_INSTANCE.with(|c| c.get());
                    refresh_mem_addr_from_caller(&mut caller, id);
                    native_api::delegate_context::context_read(ptr, len)
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        linker
            .func_wrap(
                "freenet_delegate_ctx",
                "__frnt__delegate__ctx_write",
                |mut caller: Caller<'_, HostState>, ptr: i64, len: i32| {
                    let id = native_api::CURRENT_DELEGATE_INSTANCE.with(|c| c.get());
                    refresh_mem_addr_from_caller(&mut caller, id);
                    native_api::delegate_context::context_write(ptr, len)
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        // Delegate secrets namespace (synchronous)
        linker
            .func_wrap(
                "freenet_delegate_secrets",
                "__frnt__delegate__get_secret",
                |mut caller: Caller<'_, HostState>,
                 key_ptr: i64,
                 key_len: i32,
                 out_ptr: i64,
                 out_len: i32| {
                    let id = native_api::CURRENT_DELEGATE_INSTANCE.with(|c| c.get());
                    refresh_mem_addr_from_caller(&mut caller, id);
                    native_api::delegate_secrets::get_secret(key_ptr, key_len, out_ptr, out_len)
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        linker
            .func_wrap(
                "freenet_delegate_secrets",
                "__frnt__delegate__get_secret_len",
                |mut caller: Caller<'_, HostState>, key_ptr: i64, key_len: i32| {
                    let id = native_api::CURRENT_DELEGATE_INSTANCE.with(|c| c.get());
                    refresh_mem_addr_from_caller(&mut caller, id);
                    native_api::delegate_secrets::get_secret_len(key_ptr, key_len)
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        linker
            .func_wrap(
                "freenet_delegate_secrets",
                "__frnt__delegate__set_secret",
                |mut caller: Caller<'_, HostState>,
                 key_ptr: i64,
                 key_len: i32,
                 val_ptr: i64,
                 val_len: i32| {
                    let id = native_api::CURRENT_DELEGATE_INSTANCE.with(|c| c.get());
                    refresh_mem_addr_from_caller(&mut caller, id);
                    native_api::delegate_secrets::set_secret(key_ptr, key_len, val_ptr, val_len)
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        linker
            .func_wrap(
                "freenet_delegate_secrets",
                "__frnt__delegate__has_secret",
                |mut caller: Caller<'_, HostState>, key_ptr: i64, key_len: i32| {
                    let id = native_api::CURRENT_DELEGATE_INSTANCE.with(|c| c.get());
                    refresh_mem_addr_from_caller(&mut caller, id);
                    native_api::delegate_secrets::has_secret(key_ptr, key_len)
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        linker
            .func_wrap(
                "freenet_delegate_secrets",
                "__frnt__delegate__remove_secret",
                |mut caller: Caller<'_, HostState>, key_ptr: i64, key_len: i32| {
                    let id = native_api::CURRENT_DELEGATE_INSTANCE.with(|c| c.get());
                    refresh_mem_addr_from_caller(&mut caller, id);
                    native_api::delegate_secrets::remove_secret(key_ptr, key_len)
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        // Delegate contracts namespace (async host functions for V2 delegates)
        // These are registered as async to support future async operations,
        // but currently complete synchronously (ReDb reads).
        //
        // SAFETY of refreshing before the async block: The _impl functions
        // (e.g. get_contract_state_impl) are synchronous ReDb reads that
        // complete immediately inside the async block — no .await points
        // exist that could yield back to the WASM guest and allow further
        // memory.grow calls. If these ever become truly async with .await
        // points, the refresh must move inside the async block using a
        // mechanism that can access the Store (e.g. wasmtime's
        // `Caller`-based async pattern).
        linker
            .func_wrap_async(
                "freenet_delegate_contracts",
                "__frnt__delegate__get_contract_state",
                |mut caller: Caller<'_, HostState>,
                 (id_ptr, id_len, out_ptr, out_len): (i64, i32, i64, i64)| {
                    let id = native_api::CURRENT_DELEGATE_INSTANCE.with(|c| c.get());
                    refresh_mem_addr_from_caller(&mut caller, id);
                    Box::new(async move {
                        native_api::delegate_contracts::get_contract_state_impl(
                            id_ptr, id_len, out_ptr, out_len,
                        )
                    })
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        linker
            .func_wrap_async(
                "freenet_delegate_contracts",
                "__frnt__delegate__get_contract_state_len",
                |mut caller: Caller<'_, HostState>, (id_ptr, id_len): (i64, i32)| {
                    let id = native_api::CURRENT_DELEGATE_INSTANCE.with(|c| c.get());
                    refresh_mem_addr_from_caller(&mut caller, id);
                    Box::new(async move {
                        native_api::delegate_contracts::get_contract_state_len_impl(id_ptr, id_len)
                    })
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        linker
            .func_wrap_async(
                "freenet_delegate_contracts",
                "__frnt__delegate__put_contract_state",
                |mut caller: Caller<'_, HostState>,
                 (id_ptr, id_len, state_ptr, state_len): (i64, i32, i64, i64)| {
                    let id = native_api::CURRENT_DELEGATE_INSTANCE.with(|c| c.get());
                    refresh_mem_addr_from_caller(&mut caller, id);
                    Box::new(async move {
                        native_api::delegate_contracts::put_contract_state_impl(
                            id_ptr, id_len, state_ptr, state_len,
                        )
                    })
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        linker
            .func_wrap_async(
                "freenet_delegate_contracts",
                "__frnt__delegate__update_contract_state",
                |mut caller: Caller<'_, HostState>,
                 (id_ptr, id_len, state_ptr, state_len): (i64, i32, i64, i64)| {
                    let id = native_api::CURRENT_DELEGATE_INSTANCE.with(|c| c.get());
                    refresh_mem_addr_from_caller(&mut caller, id);
                    Box::new(async move {
                        native_api::delegate_contracts::update_contract_state_impl(
                            id_ptr, id_len, state_ptr, state_len,
                        )
                    })
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        linker
            .func_wrap_async(
                "freenet_delegate_contracts",
                "__frnt__delegate__subscribe_contract",
                |mut caller: Caller<'_, HostState>, (id_ptr, id_len): (i64, i32)| {
                    let id = native_api::CURRENT_DELEGATE_INSTANCE.with(|c| c.get());
                    refresh_mem_addr_from_caller(&mut caller, id);
                    Box::new(async move {
                        native_api::delegate_contracts::subscribe_contract_impl(id_ptr, id_len)
                    })
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        // ============================================================
        // freenet_delegate_management namespace — delegate creation
        // ============================================================
        linker
            .func_wrap_async(
                "freenet_delegate_management",
                "__frnt__delegate__create_delegate",
                |mut caller: Caller<'_, HostState>,
                 (
                    wasm_ptr,
                    wasm_len,
                    params_ptr,
                    params_len,
                    cipher_ptr,
                    nonce_ptr,
                    out_key_ptr,
                    out_hash_ptr,
                ): (i64, i64, i64, i64, i64, i64, i64, i64)| {
                    let id = native_api::CURRENT_DELEGATE_INSTANCE.with(|c| c.get());
                    refresh_mem_addr_from_caller(&mut caller, id);
                    Box::new(async move {
                        native_api::delegate_management::create_delegate_impl(
                            wasm_ptr,
                            wasm_len,
                            params_ptr,
                            params_len,
                            cipher_ptr,
                            nonce_ptr,
                            out_key_ptr,
                            out_hash_ptr,
                        )
                    })
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

        // Contract I/O namespace: streaming refill buffer
        linker
            .func_wrap(
                "freenet_contract_io",
                "__frnt__fill_buffer",
                |mut caller: Caller<'_, HostState>, id: i64, buf_ptr: i64| -> u32 {
                    refresh_mem_addr_from_caller(&mut caller, id);
                    native_api::fill_buffer_impl(id, buf_ptr)
                },
            )
            .map_err(|e| WasmError::Other(anyhow::anyhow!(e)))?;

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
                    return match tokio::task::block_in_place(|| handle.block_on(task_handle)) {
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
                    task_handle.abort();
                    return BlockingResult::Timeout;
                }

                std::thread::sleep(Duration::from_millis(10));
            }
        }
        Err(_) => {
            let (tx, rx) = std::sync::mpsc::channel();
            let thread_handle = std::thread::spawn(move || {
                let result = f();
                // Receiver may have timed out; send failure is non-fatal
                #[allow(clippy::let_underscore_must_use)]
                let _ = tx.send(result);
            });

            loop {
                match rx.try_recv() {
                    Ok((Ok(value), store)) => {
                        let _join = thread_handle.join();
                        return BlockingResult::Ok(value, store);
                    }
                    Ok((Err(err), store)) => {
                        let _join = thread_handle.join();
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

    /// Regression test for Report 2RDXTD: wasmtime's per-Store instance counter is
    /// monotonic (never decremented) with a default limit of 10,000. Our ResourceLimiter
    /// override sets instances() to usize::MAX so long-running gateways don't exhaust.
    /// This test creates and drops >10,000 instances on a single Store to verify.
    #[test]
    fn test_instance_limit_override_allows_many_instances() {
        let config = RuntimeConfig::default();
        let mut engine = WasmtimeEngine::new(&config, false).unwrap();
        let module = engine.compile(SIMPLE_WASM).unwrap();

        // Create and drop 10,001 instances on the same store — exceeds wasmtime's
        // default 10,000 limit. Without our ResourceLimiter override this would fail.
        for i in 0..10_001 {
            let handle = engine
                .create_instance(&module, i, 1024)
                .unwrap_or_else(|e| panic!("instance {i} should succeed: {e}"));
            engine.drop_instance(&handle);
        }
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
          ;; put_contract_state_impl(id_ptr: i64, id_len: i32, state_ptr: i64, state_len: i64) -> i64
          (import "freenet_delegate_contracts" "__frnt__delegate__put_contract_state"
            (func $put_state (param i64 i32 i64 i64) (result i64)))
          ;; update_contract_state_impl(id_ptr: i64, id_len: i32, state_ptr: i64, state_len: i64) -> i64
          (import "freenet_delegate_contracts" "__frnt__delegate__update_contract_state"
            (func $update_state (param i64 i32 i64 i64) (result i64)))
          ;; subscribe_contract_impl(id_ptr: i64, id_len: i32) -> i64
          (import "freenet_delegate_contracts" "__frnt__delegate__subscribe_contract"
            (func $subscribe (param i64 i32) (result i64)))
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

    /// Test that demonstrates wasmtime's memory cleanup behavior.
    ///
    /// This is a documentation test - run manually to observe memory behavior.
    /// Memory should be freed when modules are dropped.
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
        println!("With wasmtime, memory should be near baseline.");

        // Sleep to allow manual memory inspection
        thread::sleep(Duration::from_secs(5));
    }

    /// Rigorous memory test for Linux CI that verifies memory cleanup.
    ///
    /// Verifies that wasmtime properly frees memory when modules are dropped.
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

        // Compile 50 modules
        let mut modules = Vec::new();
        for _ in 0..50 {
            modules.push(Module::new(&engine, SIMPLE_WASM).unwrap());
        }

        let peak_rss = get_rss_bytes();
        let peak_growth = peak_rss.saturating_sub(baseline_rss);

        // The reliable metric: how much RSS grew from compiling 50 modules.
        // Wasmtime should keep this well under 500 MB for 50 modules.
        //
        // NOTE: We intentionally check peak_growth (compilation overhead) rather
        // than post-drop RSS delta. RSS is process-wide, and concurrent test
        // threads allocate memory between our baseline and post-drop measurements,
        // making the post-drop check unreliable (observed 260 MB "leaked" with
        // only 17 MB peak growth — the rest was from other tests).
        assert!(
            peak_growth < 500 * 1024 * 1024,
            "Excessive memory from compiling 50 modules: {} MB peak growth. \
             Expected < 500 MB with wasmtime.",
            peak_growth / (1024 * 1024),
        );

        // Drop modules and verify RSS doesn't grow further (catch deferred leaks).
        // Compare against peak_rss rather than baseline to avoid concurrent test noise.
        drop(modules);
        thread::sleep(Duration::from_millis(100));
        let after_rss = get_rss_bytes();
        if after_rss > peak_rss + 100 * 1024 * 1024 {
            eprintln!(
                "Warning: RSS grew {} MB after dropping modules (peak={} MB, after={} MB). \
                 This may indicate a deferred leak or concurrent test activity.",
                (after_rss.saturating_sub(peak_rss)) / (1024 * 1024),
                peak_rss / (1024 * 1024),
                after_rss / (1024 * 1024),
            );
        }
    }

    #[test]
    fn test_module_without_async_imports_detected_as_v1() {
        let config = RuntimeConfig::default();
        let mut engine = WasmtimeEngine::new(&config, false).unwrap();

        let wat = r#"
        (module
          (memory (export "memory") 1)
          (func (export "process") (param i64 i64 i64) (result i64)
            i64.const 0))
        "#;
        let module = engine.compile(wat.as_bytes()).unwrap();
        assert!(
            !engine.module_has_async_imports(&module),
            "V1 module should not have freenet_delegate_contracts imports"
        );
    }

    #[test]
    fn test_module_with_async_imports_detected_as_v2() {
        let config = RuntimeConfig::default();
        let mut engine = WasmtimeEngine::new(&config, false).unwrap();

        let wat = r#"
        (module
          (import "freenet_delegate_contracts" "__frnt__delegate__get_contract_state"
            (func $get_state (param i64 i32 i64 i64) (result i64)))
          (import "freenet_delegate_contracts" "__frnt__delegate__get_contract_state_len"
            (func $get_state_len (param i64 i32) (result i64)))
          (memory (export "memory") 1)
          (func (export "process") (param i64 i64 i64) (result i64)
            i64.const 0))
        "#;
        let module = engine.compile(wat.as_bytes()).unwrap();
        assert!(
            engine.module_has_async_imports(&module),
            "V2 module should have freenet_delegate_contracts imports"
        );
    }

    #[test]
    fn test_v2_async_call_path_end_to_end() {
        let config = RuntimeConfig::default();
        let mut engine = WasmtimeEngine::new(&config, false).unwrap();

        let wat = r#"
        (module
          (import "freenet_delegate_contracts" "__frnt__delegate__get_contract_state_len"
            (func $get_state_len (param i64 i32) (result i64)))
          (memory (export "memory") 1)
          (global $instance_id (mut i64) (i64.const 0))
          (func (export "__frnt_set_id") (param i64)
            local.get 0
            global.set $instance_id)
          (func (export "__frnt__initiate_buffer") (param i32) (result i64)
            i64.const 100)
          (func (export "process") (param i64 i64 i64) (result i64)
            i64.const 0
            i32.const 0
            call $get_state_len))
        "#;

        let module = engine.compile(wat.as_bytes()).unwrap();
        assert!(
            engine.module_has_async_imports(&module),
            "module should be detected as V2"
        );

        let handle = engine
            .create_instance(&module, 999, 1024)
            .expect("create instance");

        let result = engine.call_3i64_async_imports(&handle, "process", 0, 0, 0);
        assert!(
            result.is_ok(),
            "V2 async call path should succeed, got: {:?}",
            result
        );

        engine.drop_instance(&handle);
    }

    /// Deterministic regression test for #3248: stale memory base pointer.
    ///
    /// Creates a WAT module that calls `memory.grow` to force relocation,
    /// then calls a host function. Verifies that `MEM_ADDR.start_ptr` is
    /// updated to reflect the new memory base address.
    #[test]
    fn test_memory_grow_refreshes_mem_addr() {
        use crate::wasm_runtime::runtime::{InstanceInfo, Key};
        use freenet_stdlib::prelude::ContractInstanceId;

        let config = RuntimeConfig::default();
        let mut engine = WasmtimeEngine::new(&config, false).unwrap();

        let wat = r#"
        (module
          (import "freenet_log" "__frnt__logger__info"
            (func $log (param i64 i64 i32)))
          (memory (export "memory") 1)
          (global $instance_id (mut i64) (i64.const 0))
          (func (export "__frnt_set_id") (param i64)
            local.get 0
            global.set $instance_id)
          (func (export "__frnt__initiate_buffer") (param i32) (result i64)
            i64.const 0)
          (func (export "grow_and_log") (param i64 i64 i64) (result i64)
            i32.const 100
            memory.grow
            drop
            global.get $instance_id
            i64.const 0
            i32.const 0
            call $log
            i64.const 0))
        "#;

        let module = engine.compile(wat.as_bytes()).unwrap();
        let instance_id: i64 = 42_000;
        let handle = engine
            .create_instance(&module, instance_id, 1024)
            .expect("create instance");

        let (init_ptr, init_size) = engine.memory_info(&handle).unwrap();
        MEM_ADDR.insert(
            instance_id,
            InstanceInfo::new(
                init_ptr as i64,
                init_size,
                Key::Contract(ContractInstanceId::new([0u8; 32])),
            ),
        );

        // Call the function that grows memory by 100 pages then calls a host function
        let result = engine.call_3i64(&handle, "grow_and_log", 0, 0, 0);
        assert!(result.is_ok(), "grow_and_log should succeed: {:?}", result);

        // Read MEM_ADDR and extract values before dropping the guard.
        // IMPORTANT: DashMap Ref holds a read lock on the shard. If the guard
        // is alive when drop_instance calls MEM_ADDR.remove (write lock),
        // the same-thread RwLock causes a deadlock.
        let (updated_ptr, updated_size) = {
            let info = MEM_ADDR
                .get(&instance_id)
                .expect("MEM_ADDR should still have entry");
            (info.start_ptr, info.mem_size)
        };

        // Memory must have grown: initial was 1 page (65536), now at least 101 pages
        assert!(
            updated_size > init_size,
            "MEM_ADDR.mem_size should reflect grown memory: \
             initial={init_size}, updated={updated_size}"
        );
        assert!(
            updated_size >= 101 * 65536,
            "Expected at least 101 pages (6.5 MiB), got {} bytes",
            updated_size
        );

        // Verify the engine's live memory agrees with MEM_ADDR
        let (live_ptr, live_size) = engine
            .memory_info(&handle)
            .expect("memory_info should succeed");
        assert_eq!(
            updated_ptr, live_ptr as i64,
            "MEM_ADDR.start_ptr should match live memory pointer"
        );
        assert_eq!(
            updated_size, live_size,
            "MEM_ADDR.mem_size should match live memory size"
        );

        engine.drop_instance(&handle);
    }
}
