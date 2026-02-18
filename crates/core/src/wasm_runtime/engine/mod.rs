//! WASM engine abstraction layer.
//!
//! This module defines the [`WasmEngine`] trait that abstracts over WASM
//! runtimes. All WASM-runtime-specific code lives behind this trait, keeping
//! the rest of the codebase runtime-agnostic.
//!
//! # Architecture
//!
//! - [`WasmEngine`] trait: defines lifecycle, compilation, memory, and execution operations
//! - [`WasmtimeEngine`]: the wasmtime-based backend
//! - [`InstanceHandle`]: opaque handle to a live WASM instance
//! - [`WasmError`]: unified error type for all WASM operations
//!
//! # Execution Modes
//!
//! The engine provides two calling conventions, both synchronous at the trait level:
//!
//! - **Synchronous** (`call_3i64`, `call_3i64_async_imports`): Used for delegate
//!   `process()` calls. Runs on the current thread. Wasmtime internally uses
//!   `call_async()` via a `block_on_async` helper.
//!   `call_3i64_async_imports` is the variant for V2 delegates with async host
//!   function imports.
//!
//! - **Blocking with timeout** (`call_2i64_blocking`, `call_3i64_blocking`): Offloads
//!   WASM to a blocking thread with timeout. Used for contract operations that may
//!   take seconds.

#[cfg(not(feature = "wasmtime-backend"))]
compile_error!("The wasmtime-backend feature must be enabled.");

#[cfg(feature = "wasmtime-backend")]
mod wasmtime_engine;

use super::runtime::RuntimeConfig;
use super::ContractError;

/// Default maximum memory limit in WASM pages (64 KiB each).
/// 4096 pages = 256 MiB.
///
/// This limit is enforced by the engine's ResourceLimiter and used by
/// host function bounds validation.
pub(crate) const DEFAULT_MAX_MEMORY_PAGES: u32 = 4096;

/// WASM page size in bytes (64 KiB).
pub(crate) const WASM_PAGE_SIZE: usize = 65536;

/// Maximum WASM memory in bytes (256 MiB by default).
/// Calculated from DEFAULT_MAX_MEMORY_PAGES * WASM_PAGE_SIZE.
pub(crate) const MAX_WASM_MEMORY_BYTES: usize = DEFAULT_MAX_MEMORY_PAGES as usize * WASM_PAGE_SIZE;

/// Opaque handle to a live WASM instance managed by the engine.
///
/// Instances are created via [`WasmEngine::create_instance`] and must be
/// explicitly dropped via [`WasmEngine::drop_instance`] to clean up resources.
pub(crate) struct InstanceHandle {
    pub(super) id: i64,
}

/// Unified error type for WASM engine operations.
///
/// Backend implementations map their native errors into these categories.
#[derive(Debug, thiserror::Error)]
pub(crate) enum WasmError {
    /// Module compilation failed (syntax error, unsupported features).
    #[error("compile: {0}")]
    Compile(String),

    /// Requested export not found in the WASM module.
    #[error("export not found: {0}")]
    Export(String),

    /// Instance creation failed (import resolution, memory allocation).
    #[error("instantiation: {0}")]
    Instantiation(String),

    /// Memory operation failed (grow, access).
    #[error("memory: {0}")]
    Memory(String),

    /// WASM execution failed (trap, stack overflow).
    #[error("runtime: {0}")]
    Runtime(String),

    /// Metering exhausted — the contract ran out of gas.
    #[error("out of gas")]
    OutOfGas,

    /// WASM execution exceeded the configured time limit.
    #[error("execution timeout")]
    Timeout,

    /// Catch-all for backend-specific errors.
    #[error(transparent)]
    Other(anyhow::Error),
}

/// Abstraction over a WASM runtime engine.
///
/// Implementors manage the full lifecycle of WASM modules and instances:
/// compilation, instantiation, memory management, and function execution.
pub(crate) trait WasmEngine: Send {
    /// Compiled module type, cached in LRU caches by the runtime.
    type Module: Clone + Send;

    // -- Lifecycle --

    /// Create a new engine instance with the given configuration.
    ///
    /// `host_mem` controls whether a shared host memory is allocated
    /// (used when running delegates that share memory with the host).
    fn new(config: &RuntimeConfig, host_mem: bool) -> Result<Self, ContractError>
    where
        Self: Sized;

    /// Returns true if the engine is in a healthy state and can execute WASM.
    fn is_healthy(&self) -> bool;

    // -- Compilation --

    /// Compile WASM bytecode into an executable module.
    fn compile(&mut self, code: &[u8]) -> Result<Self::Module, WasmError>;

    // -- Module inspection --

    /// Check if a compiled module imports async host functions.
    ///
    /// Returns `true` if the module imports the `freenet_delegate_contracts`
    /// namespace, indicating it's a V2 delegate that needs `call_async`.
    fn module_has_async_imports(&self, module: &Self::Module) -> bool;

    // -- Instance lifecycle --

    /// Create a WASM instance from a compiled module.
    ///
    /// Sets up imports, calls `__frnt_set_id`, records memory address,
    /// and ensures sufficient memory for `req_bytes`.
    fn create_instance(
        &mut self,
        module: &Self::Module,
        id: i64,
        req_bytes: usize,
    ) -> Result<InstanceHandle, WasmError>;

    /// Clean up a WASM instance and remove its MEM_ADDR entry.
    fn drop_instance(&mut self, handle: &InstanceHandle);

    // -- Memory --

    /// Get `(data_ptr, data_size)` of the instance's linear memory.
    fn memory_info(&mut self, handle: &InstanceHandle) -> Result<(*const u8, usize), WasmError>;

    /// Call `__frnt__initiate_buffer(size)` to allocate a WASM-side buffer.
    fn initiate_buffer(&mut self, handle: &InstanceHandle, size: u32) -> Result<i64, WasmError>;

    // -- Synchronous WASM function calls --
    // Used for delegate process() calls where thread-local state is needed.

    /// Call a WASM function `name() -> ()` synchronously (no args, no return).
    #[allow(dead_code)] // Used in tests via concrete Engine type
    fn call_void(&mut self, handle: &InstanceHandle, name: &str) -> Result<(), WasmError>;

    /// Call a WASM function `name(a, b, c) -> i64` synchronously.
    fn call_3i64(
        &mut self,
        handle: &InstanceHandle,
        name: &str,
        a: i64,
        b: i64,
        c: i64,
    ) -> Result<i64, WasmError>;

    // -- Async-imports WASM function calls --
    // Used for V2 delegates that have async host function imports.
    // This is still a blocking call from Rust's perspective.

    /// Call a WASM function `name(a, b, c) -> i64` using the async calling convention.
    ///
    /// Required when the module has async host function imports (e.g., V2 delegate
    /// contract access functions registered via `func_wrap_async`).
    fn call_3i64_async_imports(
        &mut self,
        handle: &InstanceHandle,
        name: &str,
        a: i64,
        b: i64,
        c: i64,
    ) -> Result<i64, WasmError>;

    // -- Blocking WASM function calls with timeout --
    // Used for contract operations. Offloads execution to a blocking thread.

    /// Call a WASM function `name(a, b) -> i64` on a blocking thread with timeout.
    ///
    /// Uses `spawn_blocking` (tokio) or `std::thread::spawn` (no runtime).
    /// Returns `WasmError::Timeout` if execution exceeds `max_execution_seconds`.
    fn call_2i64_blocking(
        &mut self,
        handle: &InstanceHandle,
        name: &str,
        a: i64,
        b: i64,
    ) -> Result<i64, WasmError>;

    /// Call a WASM function `name(a, b, c) -> i64` on a blocking thread with timeout.
    fn call_3i64_blocking(
        &mut self,
        handle: &InstanceHandle,
        name: &str,
        a: i64,
        b: i64,
        c: i64,
    ) -> Result<i64, WasmError>;
}

// Backend selection via type alias — no generics leak outside wasm_runtime/
#[cfg(feature = "wasmtime-backend")]
pub(crate) type Engine = wasmtime_engine::WasmtimeEngine;

/// The underlying backend engine type shared across RuntimePool executors.
///
/// All executors in a pool MUST share the same backend engine because compiled
/// modules store references to the compiling engine's internal data structures.
/// Using a Module compiled by one Engine with a different Engine can cause errors.
#[cfg(feature = "wasmtime-backend")]
pub(crate) type BackendEngine = wasmtime::Engine;
