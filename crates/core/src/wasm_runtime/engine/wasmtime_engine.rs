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

use std::collections::HashMap;
use std::time::Duration;

use wasmtime::{
    Caller, Config, Engine, Error as WasmtimeError, Extern, Func, Instance,
    InstanceAllocationStrategy, Linker, Module, OptLevel, PoolingAllocationConfig, ResourceLimiter,
    Store, TypedFunc,
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

// =============================================================================
// WasmtimeEngine
// =============================================================================

pub(crate) struct WasmtimeEngine {
    /// Wasmtime engine (shared, cheap to clone).
    engine: Engine,
    /// Wasmtime store with HostState — taken out during blocking calls, restored after.
    store: Option<Store<HostState>>,
    /// Pre-configured linker with all host functions registered.
    linker: Linker<HostState>,
    /// Live instances by their assigned ID.
    instances: HashMap<i64, Instance>,
    /// Maximum execution time for blocking calls.
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

        let instance = self
            .linker
            .instantiate(&mut *store, module)
            .map_err(|e| WasmError::Instantiation(e.to_string()))?;

        // Call __frnt_set_id to assign this instance's ID
        let set_id = instance
            .get_typed_func::<i64, ()>(&mut *store, "__frnt_set_id")
            .map_err(|e| WasmError::Export(e.to_string()))?;
        set_id
            .call(&mut *store, id)
            .map_err(|e| WasmError::Runtime(e.to_string()))?;

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
        func.call(&mut *store, size)
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

// =============================================================================
// WasmtimeEngine private implementation
// =============================================================================

impl WasmtimeEngine {
    /// Create a standalone backend engine with default configuration.
    ///
    /// Used by the first executor in a RuntimePool when no shared engine exists yet.
    pub(crate) fn create_backend_engine() -> Engine {
        let (engine, _, _) = Self::create_engine(&RuntimeConfig::default())
            .expect("failed to create default wasmtime engine");
        engine
    }

    /// Get a clone of the underlying wasmtime engine for sharing with other instances.
    ///
    /// Wasmtime's Engine is internally Arc-wrapped, so clone() is cheap and safe.
    pub(crate) fn clone_backend_engine(&self) -> Engine {
        self.engine.clone()
    }

    /// Create a new WasmtimeEngine that shares the backend engine with other instances.
    ///
    /// The shared engine ensures all instances use the same code cache and configuration.
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
            tracing::warn!("host_mem=true not fully supported in wasmtime backend yet");
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
        wasmtime_config.max_wasm_stack(8 * 1024 * 1024); // 8 MiB stack

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

        // Enable Cranelift optimizations for compact machine code
        // SpeedAndSize balances performance with memory footprint.
        // Expected to generate significantly more compact code than wasmer's ~10 MB per contract.
        wasmtime_config.cranelift_opt_level(OptLevel::SpeedAndSize);

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

        // Delegate context namespace
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

        // Delegate secrets namespace
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

        // Delegate contracts namespace (async host functions)
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

    fn ensure_memory(
        store: &mut Store<HostState>,
        instance: &Instance,
        req_bytes: usize,
    ) -> Result<(), WasmError> {
        let memory = instance
            .get_memory(&mut *store, "memory")
            .ok_or_else(|| WasmError::Export("memory export not found".to_string()))?;
        let current_size_bytes = memory.data_size(&*store);
        let req_pages = (req_bytes + WASM_PAGE_SIZE - 1) / WASM_PAGE_SIZE;
        let current_pages = current_size_bytes / WASM_PAGE_SIZE;
        if current_pages < req_pages {
            let grow_by = (req_pages - current_pages) as u64;
            if let Err(err) = memory.grow(&mut *store, grow_by) {
                tracing::error!("wasm runtime failed with memory error: {err}");
                return Err(WasmError::Memory(format!(
                    "insufficient memory: requested {} bytes but had {} bytes",
                    req_bytes, current_size_bytes
                )));
            }
        }
        Ok(())
    }
}

/// Bridge from sync to async, using the right executor for the current context.
///
/// - Inside a tokio runtime: `block_in_place` + `Handle::block_on`
/// - Outside tokio: `futures::executor::block_on`
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
                    break;
                }
                if start.elapsed() >= timeout {
                    task_handle.abort();
                    tracing::warn!(
                        timeout_secs = max_execution_seconds,
                        elapsed_ms = start.elapsed().as_millis(),
                        "WASM execution timed out, aborting task"
                    );
                    return BlockingResult::Timeout;
                }
                std::thread::sleep(Duration::from_millis(10));
            }

            let join_result = tokio::task::block_in_place(|| handle.block_on(task_handle));
            match join_result {
                Ok((Ok(value), store)) => BlockingResult::Ok(value, store),
                Ok((Err(err), store)) => BlockingResult::WasmError(err, store),
                Err(e) => {
                    if e.is_panic() {
                        tracing::error!("WASM blocking task panicked during execution");
                        BlockingResult::Panic(anyhow::anyhow!("WASM execution panicked"))
                    } else if e.is_cancelled() {
                        BlockingResult::Panic(anyhow::anyhow!("WASM execution was cancelled"))
                    } else {
                        BlockingResult::Panic(anyhow::anyhow!("WASM execution failed: {}", e))
                    }
                }
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
