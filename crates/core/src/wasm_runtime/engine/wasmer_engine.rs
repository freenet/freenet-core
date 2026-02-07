//! Wasmer 7.x backend for the WASM engine abstraction.
//!
//! **This is the ONLY file that imports `wasmer::`.**
//!
//! Contains all wasmer-specific code:
//! - Store/Engine/Compiler setup
//! - LimitingTunables for memory limits
//! - Instance creation and management
//! - TypedFunction resolution and invocation
//! - Metering (gas) integration
//! - Host function registration
//! - Blocking execution with timeout

use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::Arc;
use std::time::Duration;

use wasmer::sys::vm::{
    MemoryError, MemoryStyle, TableStyle, VMMemory, VMMemoryDefinition, VMTable, VMTableDefinition,
};
use wasmer::sys::{BaseTunables, CompilerConfig, EngineBuilder, Tunables};
use wasmer::{
    imports, Bytes, Imports, Instance, Memory, MemoryType, Module, Pages, Store, TableType,
    TypedFunction,
};
use wasmer_compiler_singlepass::Singlepass;
use wasmer_middlewares::metering::{get_remaining_points, MeteringPoints};
use wasmer_middlewares::Metering;

use super::{InstanceHandle, WasmEngine, WasmError};
use crate::wasm_runtime::native_api::{self, MEM_ADDR};
use crate::wasm_runtime::runtime::RuntimeConfig;
use crate::wasm_runtime::ContractError;

/// Default maximum memory limit in WASM pages (64 KiB each).
/// 4096 pages = 256 MiB.
const DEFAULT_MAX_MEMORY_PAGES: u32 = 4096;

// =============================================================================
// WasmerEngine
// =============================================================================

pub(crate) struct WasmerEngine {
    /// Wasmer store — taken out during blocking calls, restored after.
    store: Option<Store>,
    /// Pre-registered host function imports.
    imports: Imports,
    /// Shared host memory (when host_mem=true).
    host_memory: Option<Memory>,
    /// Live instances by their assigned ID.
    instances: HashMap<i64, Instance>,
    /// Maximum execution time for blocking calls.
    max_execution_seconds: f64,
    /// Whether gas metering is enabled.
    enabled_metering: bool,
}

impl WasmEngine for WasmerEngine {
    type Module = Module;

    fn new(config: &RuntimeConfig, host_mem: bool) -> Result<Self, ContractError> {
        let mut store = Self::create_store(config);
        let (host_memory, mut top_level_imports) = if host_mem {
            let mem = Self::create_host_memory(&mut store)?;
            let imports = imports! {
                "env" => {
                    "memory" =>  mem.clone(),
                },
            };
            (Some(mem), imports)
        } else {
            (None, imports! {})
        };

        // Register all host functions
        Self::register_host_functions(&mut store, &mut top_level_imports);

        Ok(Self {
            store: Some(store),
            imports: top_level_imports,
            host_memory,
            instances: HashMap::new(),
            max_execution_seconds: config.max_execution_seconds,
            enabled_metering: config.enable_metering,
        })
    }

    fn is_healthy(&self) -> bool {
        self.store.is_some()
    }

    fn compile(&mut self, code: &[u8]) -> Result<Module, WasmError> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| WasmError::Other(anyhow::anyhow!("engine store not available")))?;
        with_suppressed_stderr(|| Module::new(store, code))
            .map_err(|e| WasmError::Compile(e.to_string()))
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
        let imports = &self.imports;

        let instance = with_suppressed_stderr(|| Instance::new(store, module, imports))
            .map_err(|e| WasmError::Instantiation(e.to_string()))?;

        // Call __frnt_set_id to assign this instance's ID
        let set_id: TypedFunction<i64, ()> = instance
            .exports
            .get_typed_function(&*store, "__frnt_set_id")
            .map_err(|e| WasmError::Export(e.to_string()))?;
        set_id
            .call(store, id)
            .map_err(|e| WasmError::Runtime(e.to_string()))?;

        // Ensure sufficient memory for the request
        Self::ensure_memory(self.host_memory.as_ref(), store, &instance, req_bytes)?;

        self.instances.insert(id, instance);

        Ok(InstanceHandle { id })
    }

    fn drop_instance(&mut self, handle: &InstanceHandle) {
        self.instances.remove(&handle.id);
        MEM_ADDR.remove(&handle.id);
    }

    fn memory_info(&self, handle: &InstanceHandle) -> Result<(*const u8, usize), WasmError> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| WasmError::Other(anyhow::anyhow!("engine store not available")))?;
        let instance = self
            .instances
            .get(&handle.id)
            .ok_or_else(|| WasmError::Other(anyhow::anyhow!("instance {} not found", handle.id)))?;
        let memory = self.host_memory.as_ref().map(Ok).unwrap_or_else(|| {
            instance
                .exports
                .get_memory("memory")
                .map_err(|e| WasmError::Export(e.to_string()))
        })?;
        let view = memory.view(store);
        Ok((view.data_ptr() as *const u8, view.data_size() as usize))
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
        let func: TypedFunction<u32, i64> = instance
            .exports
            .get_typed_function(&*store, "__frnt__initiate_buffer")
            .map_err(|e| WasmError::Export(e.to_string()))?;
        func.call(store, size)
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
        let func: TypedFunction<(), ()> = instance
            .exports
            .get_typed_function(&*store, name)
            .map_err(|e| WasmError::Export(e.to_string()))?;
        func.call(store)
            .map_err(|e| classify_runtime_error(enabled_metering, store, instance, e))
    }

    fn call_2i64(
        &mut self,
        handle: &InstanceHandle,
        name: &str,
        a: i64,
        b: i64,
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
        let func: TypedFunction<(i64, i64), i64> = instance
            .exports
            .get_typed_function(&*store, name)
            .map_err(|e| WasmError::Export(e.to_string()))?;
        func.call(store, a, b)
            .map_err(|e| classify_runtime_error(enabled_metering, store, instance, e))
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
        let func: TypedFunction<(i64, i64, i64), i64> = instance
            .exports
            .get_typed_function(&*store, name)
            .map_err(|e| WasmError::Export(e.to_string()))?;
        func.call(store, a, b, c)
            .map_err(|e| classify_runtime_error(enabled_metering, store, instance, e))
    }

    fn call_3i64_async_imports(
        &mut self,
        handle: &InstanceHandle,
        name: &str,
        a: i64,
        b: i64,
        c: i64,
    ) -> Result<i64, WasmError> {
        // Take the Store out so we can convert it to StoreAsync.
        let store = self
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

        let func: TypedFunction<(i64, i64, i64), i64> =
            match instance.exports.get_typed_function(&store, name) {
                Ok(f) => f,
                Err(e) => {
                    self.store = Some(store);
                    return Err(WasmError::Export(e.to_string()));
                }
            };

        // Convert Store to StoreAsync for async host function support.
        // Use futures::executor::block_on as the bridge from sync to async.
        // This works because the current async host functions complete
        // synchronously (ReDb reads). When truly async operations are added
        // (network fetches, subscriptions), the DelegateRuntimeInterface
        // trait should be made async and the bridge removed.
        let store_async = store.into_async();
        let call_result = futures::executor::block_on(func.call_async(&store_async, a, b, c));

        // Convert StoreAsync back to Store and restore it
        let store = store_async
            .into_store()
            .expect("StoreAsync should convert back to Store after call_async completes");
        self.store = Some(store);

        call_result.map_err(|e| WasmError::Runtime(e.to_string()))
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
        let func: TypedFunction<(i64, i64), i64> =
            match instance.exports.get_typed_function(&store, name) {
                Ok(f) => f,
                Err(e) => {
                    self.store = Some(store);
                    return Err(WasmError::Export(e.to_string()));
                }
            };

        let result = execute_wasm_blocking(
            move || {
                let r = func.call(&mut store, a, b);
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
                let instance = self.instances.get(&handle.id).unwrap();
                let wasm_err = classify_runtime_error(enabled_metering, &mut store, instance, err);
                self.store = Some(store);
                Err(wasm_err)
            }
            BlockingResult::Timeout => {
                // Store is lost — engine becomes unhealthy
                Err(WasmError::Timeout)
            }
            BlockingResult::Panic(err) => {
                // Store is lost — engine becomes unhealthy
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
        let func: TypedFunction<(i64, i64, i64), i64> =
            match instance.exports.get_typed_function(&store, name) {
                Ok(f) => f,
                Err(e) => {
                    self.store = Some(store);
                    return Err(WasmError::Export(e.to_string()));
                }
            };

        let result = execute_wasm_blocking(
            move || {
                let r = func.call(&mut store, a, b, c);
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
                let instance = self.instances.get(&handle.id).unwrap();
                let wasm_err = classify_runtime_error(enabled_metering, &mut store, instance, err);
                self.store = Some(store);
                Err(wasm_err)
            }
            BlockingResult::Timeout => Err(WasmError::Timeout),
            BlockingResult::Panic(err) => Err(WasmError::Other(err)),
        }
    }

    fn is_out_of_gas(&self, handle: &InstanceHandle) -> bool {
        if !self.enabled_metering {
            return false;
        }
        if self.store.is_none() {
            return false;
        }
        if !self.instances.contains_key(&handle.id) {
            return false;
        }
        // get_remaining_points needs &mut Store but we only have &Store.
        // We can't check from an immutable reference. This is a limitation.
        // The actual check happens in classify_runtime_error after a call fails.
        false
    }
}

// =============================================================================
// WasmerEngine private implementation
// =============================================================================

impl WasmerEngine {
    fn create_store(config: &RuntimeConfig) -> Store {
        fn get_cpu_cycles_per_second() -> (u64, f64) {
            const DEFAULT_CPU_CYCLES_PER_SECOND: u64 = 3_000_000_000;
            if let Some(cpu) = option_env!("CPU_CYCLES_PER_SECOND") {
                (cpu.parse().expect("incorrect number"), 0.0)
            } else {
                get_cpu_cycles_per_second_runtime()
                    .map(|x| (x, 0.0))
                    .unwrap_or((DEFAULT_CPU_CYCLES_PER_SECOND, 0.2))
            }
        }

        fn operation_cost(_operator: &wasmer::wasmparser::Operator) -> u64 {
            1
        }

        let (default_cycles, default_margin) = get_cpu_cycles_per_second();
        let cpu_cycles_per_sec = config.cpu_cycles_per_second.unwrap_or(default_cycles);
        let safety_margin = if config.safety_margin >= 0.0 && config.safety_margin <= 1.0 {
            config.safety_margin
        } else {
            default_margin
        };

        let max_cycles: u64 = (config.max_execution_seconds
            * cpu_cycles_per_sec as f64
            * (1.0 + safety_margin)) as u64;

        let metering = Arc::new(Metering::new(max_cycles, operation_cost));
        let mut compiler_config = Singlepass::default();
        if config.enable_metering {
            compiler_config.push_middleware(metering.clone());
        }

        let mut engine = EngineBuilder::new(compiler_config).engine();

        let base_tunables = BaseTunables::for_target(engine.target());
        let limiting_tunables =
            LimitingTunables::new(base_tunables, Pages(DEFAULT_MAX_MEMORY_PAGES));
        engine.set_tunables(limiting_tunables);

        Store::new(engine)
    }

    fn create_host_memory(store: &mut Store) -> Result<Memory, ContractError> {
        Memory::new(
            store,
            MemoryType::new(20u32, Some(DEFAULT_MAX_MEMORY_PAGES), false),
        )
        .map_err(|e| WasmError::Memory(e.to_string()).into())
    }

    fn register_host_functions(store: &mut Store, imports: &mut Imports) {
        native_api::log::prepare_export(store, imports);
        native_api::rand::prepare_export(store, imports);
        native_api::time::prepare_export(store, imports);
        native_api::delegate_context::prepare_export(store, imports);
        native_api::delegate_secrets::prepare_export(store, imports);
        native_api::delegate_contracts::prepare_export(store, imports);
    }

    fn ensure_memory(
        host_memory: Option<&Memory>,
        store: &mut Store,
        instance: &Instance,
        req_bytes: usize,
    ) -> Result<(), WasmError> {
        let memory = host_memory.map(Ok).unwrap_or_else(|| {
            instance
                .exports
                .get_memory("memory")
                .map_err(|e| WasmError::Export(e.to_string()))
        })?;
        let req_pages: Pages = Bytes::from(req_bytes).try_into().unwrap();
        if memory.view(&*store).size() < req_pages {
            if let Err(err) = memory.grow(store, req_pages) {
                tracing::error!("wasm runtime failed with memory error: {err}");
                return Err(WasmError::Memory(format!(
                    "insufficient memory: requested {} bytes but had {} bytes",
                    req_pages.0 as usize * wasmer::WASM_PAGE_SIZE,
                    memory.view(&*store).size().0 as usize * wasmer::WASM_PAGE_SIZE,
                )));
            }
        }
        Ok(())
    }
}

/// Classify a wasmer RuntimeError, checking metering status.
///
/// Standalone function (not a method) to avoid borrow conflicts when both
/// `store` and `instance` are borrowed from the same `WasmerEngine`.
fn classify_runtime_error(
    enabled_metering: bool,
    store: &mut Store,
    instance: &Instance,
    error: wasmer::RuntimeError,
) -> WasmError {
    if enabled_metering {
        let remaining = get_remaining_points(store, instance);
        match remaining {
            MeteringPoints::Remaining(..) => {
                tracing::error!("WASM runtime error: {:?}", error);
                WasmError::Runtime(error.to_string())
            }
            MeteringPoints::Exhausted => {
                tracing::error!("WASM execution ran out of gas");
                WasmError::OutOfGas
            }
        }
    } else {
        WasmError::Runtime(error.to_string())
    }
}

// =============================================================================
// Blocking execution with timeout (moved from contract.rs)
// =============================================================================

type WasmResult = (Result<i64, wasmer::RuntimeError>, Store);

enum BlockingResult {
    Ok(i64, Store),
    WasmError(wasmer::RuntimeError, Store),
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

// =============================================================================
// Stderr suppression
// =============================================================================

#[cfg(unix)]
fn with_suppressed_stderr<T, F: FnOnce() -> T>(f: F) -> T {
    let _gag = gag::Gag::stderr();
    f()
}

#[cfg(not(unix))]
fn with_suppressed_stderr<T, F: FnOnce() -> T>(f: F) -> T {
    f()
}

// =============================================================================
// CPU speed detection (moved from runtime.rs)
// =============================================================================

#[cfg(target_os = "macos")]
fn get_cpu_cycles_per_second_runtime() -> Option<u64> {
    use std::process::Command;

    fn parse_sysctl_output(output: &str) -> Option<u64> {
        let parts: Vec<&str> = output.split(':').collect();
        if parts.len() == 2 {
            if let Ok(freq) = parts[1].trim().parse() {
                return Some(freq);
            }
        }
        None
    }

    let output = Command::new("sysctl")
        .arg("hw.cpufrequency_max")
        .output()
        .ok()?;

    if output.status.success() {
        let output_str = String::from_utf8_lossy(&output.stdout);
        tracing::debug!("sysctl output (hw.cpufrequency_max): {}", output_str);
        if let Some(freq) = parse_sysctl_output(&output_str) {
            return Some(freq);
        }
    } else {
        tracing::debug!("sysctl command failed with status: {}", output.status);
    }

    let output = Command::new("sysctl").arg("hw.tbfrequency").output().ok()?;

    if output.status.success() {
        let output_str = String::from_utf8_lossy(&output.stdout);
        tracing::debug!("sysctl output (hw.tbfrequency): {}", output_str);
        if let Some(freq) = parse_sysctl_output(&output_str) {
            return Some(freq);
        }
    } else {
        tracing::debug!("sysctl command failed with status: {}", output.status);
    }
    None
}

#[cfg(target_os = "linux")]
fn get_cpu_cycles_per_second_runtime() -> Option<u64> {
    use std::fs::File;
    use std::io::{self, BufRead};

    if let Ok(file) = File::open("/proc/cpuinfo") {
        for line in io::BufReader::new(file).lines().map_while(Result::ok) {
            if line.starts_with("cpu MHz") {
                let parts: Vec<&str> = line.split(':').collect();
                if parts.len() == 2 {
                    let mhz: f64 = parts[1].trim().parse().ok()?;
                    return Some((mhz * 1_000_000.0) as u64);
                }
            }
        }
    }
    None
}

#[cfg(target_os = "windows")]
fn get_cpu_cycles_per_second_runtime() -> Option<u64> {
    use serde::Deserialize;
    use wmi::WMIConnection;

    #[derive(Deserialize, Debug)]
    struct Win32Processor {
        #[serde(rename = "MaxClockSpeed")]
        max_clock_speed: u64,
    }

    let wmi_con = WMIConnection::new().ok()?;

    let results: Vec<Win32Processor> = wmi_con
        .raw_query("SELECT MaxClockSpeed FROM Win32_Processor")
        .ok()?;

    if let Some(cpu) = results.first() {
        return Some(cpu.max_clock_speed * 1_000_000);
    }

    None
}

// =============================================================================
// LimitingTunables (moved from tunables.rs)
// =============================================================================

struct LimitingTunables<T: Tunables> {
    limit: Pages,
    base: T,
}

impl<T: Tunables> LimitingTunables<T> {
    fn new(base: T, limit: Pages) -> Self {
        Self { limit, base }
    }

    fn adjust_memory(&self, requested: &MemoryType) -> MemoryType {
        let mut adjusted = *requested;
        if requested.maximum.is_none() {
            adjusted.maximum = Some(self.limit);
        }
        adjusted
    }

    fn validate_memory(&self, ty: &MemoryType) -> Result<(), MemoryError> {
        if ty.minimum > self.limit {
            return Err(MemoryError::Generic(format!(
                "Minimum memory ({} pages) exceeds the allowed limit ({} pages)",
                ty.minimum.0, self.limit.0
            )));
        }

        if let Some(max) = ty.maximum {
            if max > self.limit {
                return Err(MemoryError::Generic(format!(
                    "Maximum memory ({} pages) exceeds the allowed limit ({} pages)",
                    max.0, self.limit.0
                )));
            }
        } else {
            return Err(MemoryError::Generic(
                "Maximum memory unset after adjustment".to_string(),
            ));
        }

        Ok(())
    }
}

impl<T: Tunables> Tunables for LimitingTunables<T> {
    fn memory_style(&self, memory: &MemoryType) -> MemoryStyle {
        let adjusted = self.adjust_memory(memory);
        self.base.memory_style(&adjusted)
    }

    fn table_style(&self, table: &TableType) -> TableStyle {
        self.base.table_style(table)
    }

    fn create_host_memory(
        &self,
        ty: &MemoryType,
        style: &MemoryStyle,
    ) -> Result<VMMemory, MemoryError> {
        let adjusted = self.adjust_memory(ty);
        self.validate_memory(&adjusted)?;
        self.base.create_host_memory(&adjusted, style)
    }

    unsafe fn create_vm_memory(
        &self,
        ty: &MemoryType,
        style: &MemoryStyle,
        vm_definition_location: NonNull<VMMemoryDefinition>,
    ) -> Result<VMMemory, MemoryError> {
        let adjusted = self.adjust_memory(ty);
        self.validate_memory(&adjusted)?;
        unsafe {
            self.base
                .create_vm_memory(&adjusted, style, vm_definition_location)
        }
    }

    fn create_host_table(&self, ty: &TableType, style: &TableStyle) -> Result<VMTable, String> {
        self.base.create_host_table(ty, style)
    }

    unsafe fn create_vm_table(
        &self,
        ty: &TableType,
        style: &TableStyle,
        vm_definition_location: NonNull<VMTableDefinition>,
    ) -> Result<VMTable, String> {
        unsafe { self.base.create_vm_table(ty, style, vm_definition_location) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adjust_memory_adds_maximum() {
        let base = BaseTunables {
            static_memory_bound: Pages(2048),
            static_memory_offset_guard_size: 128,
            dynamic_memory_offset_guard_size: 256,
        };
        let tunables = LimitingTunables::new(base, Pages(4096));

        // Memory without maximum should get one added
        let mem_type = MemoryType::new(10, None, false);
        let adjusted = tunables.adjust_memory(&mem_type);
        assert_eq!(adjusted.maximum, Some(Pages(4096)));

        // Memory with smaller maximum should keep it
        let mem_type = MemoryType::new(10, Some(100), false);
        let adjusted = tunables.adjust_memory(&mem_type);
        assert_eq!(adjusted.maximum, Some(Pages(100)));
    }

    #[test]
    fn test_validate_memory_rejects_too_large() {
        let base = BaseTunables {
            static_memory_bound: Pages(2048),
            static_memory_offset_guard_size: 128,
            dynamic_memory_offset_guard_size: 256,
        };
        let tunables = LimitingTunables::new(base, Pages(100));

        // Minimum exceeds limit
        let mem_type = MemoryType::new(200, Some(200), false);
        assert!(tunables.validate_memory(&mem_type).is_err());

        // Maximum exceeds limit
        let mem_type = MemoryType::new(10, Some(200), false);
        assert!(tunables.validate_memory(&mem_type).is_err());

        // Within limit should pass
        let mem_type = MemoryType::new(10, Some(50), false);
        assert!(tunables.validate_memory(&mem_type).is_ok());
    }
}
