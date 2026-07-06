use super::{
    RuntimeResult,
    contract_store::ContractStore,
    delegate_api::DelegateApiVersion,
    delegate_store::DelegateStore,
    engine::{BackendEngine, Engine, InstanceHandle, WasmEngine},
    error::RuntimeInnerError,
    native_api,
    secrets_store::SecretsStore,
};
use freenet_stdlib::{
    memory::{
        WasmLinearMem,
        buf::{BufferBuilder, BufferMut},
    },
    prelude::*,
};
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex};

use super::ModuleCache;

/// A compiled WASM module cache shared across multiple `Runtime` instances.
///
/// The backend is wasmtime: a `Module` owns its compiled machine code via an
/// internal `Arc<CodeMemory>`, so clones are cheap (an Arc refcount bump) and
/// dropping the last clone frees the compiled code (verified by
/// `wasmtime_engine::tests::test_module_drop_frees_memory`). Sharing one cache
/// across the `RuntimePool` avoids compiling and storing the same contract N
/// times (once per pool executor).
///
/// The cache is bounded by the total compiled **byte size** of its entries
/// (see [`ModuleCache`] and
/// [`default_module_cache_budget_bytes`](super::default_module_cache_budget_bytes)),
/// not by a fixed entry count. A byte budget is the correct bound here because:
///
/// - It scales with how many contracts a node actually hosts: a node hosting
///   thousands of small contracts no longer thrashes the way the old 1024-entry
///   *count* cap did (the eviction-recompilation cycle behind issue #4441).
/// - It bounds the cache's absolute memory footprint regardless of contract
///   count, which a count cap could not (1024 large modules ≫ 1024 small ones).
pub(crate) type SharedModuleCache<K> = Arc<Mutex<ModuleCache<K, <Engine as WasmEngine>::Module>>>;

static INSTANCE_ID: AtomicI64 = AtomicI64::new(0);

/// A live WASM instance with RAII cleanup.
///
/// On drop, removes the MEM_ADDR entry. The WASM `Instance` is cleaned
/// up by calling [`Runtime::drop_running_instance`] after the instance is
/// no longer needed.
pub(super) struct RunningInstance {
    pub id: i64,
    pub handle: InstanceHandle,
    /// Whether the contract imports `freenet_contract_io` (streaming buffer support).
    /// Contracts compiled against stdlib >= 0.3.4 have this; older ones don't.
    pub supports_streaming: bool,
    /// Set to true when the engine instance has been explicitly cleaned up.
    dropped_from_engine: bool,
}

impl RunningInstance {
    fn new(
        engine: &mut Engine,
        module: &<Engine as WasmEngine>::Module,
        key: Key,
        req_bytes: usize,
    ) -> RuntimeResult<Self> {
        let id = INSTANCE_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let handle = engine.create_instance(module, id, req_bytes)?;

        // Record memory address and size for host function pointer arithmetic
        let (ptr, size) = engine.memory_info(&handle)?;
        native_api::MEM_ADDR.insert(id, InstanceInfo::new(ptr as i64, size, key));

        // Detect if the contract supports streaming buffers by checking
        // whether it imports the freenet_contract_io namespace. Contracts
        // compiled against stdlib >= 0.3.4 have this import.
        let supports_streaming = engine.module_has_streaming_io(module);

        Ok(Self {
            id,
            handle,
            supports_streaming,
            dropped_from_engine: false,
        })
    }
}

impl Drop for RunningInstance {
    fn drop(&mut self) {
        if !self.dropped_from_engine {
            tracing::debug!(
                instance_id = self.id,
                "RunningInstance dropped without engine cleanup — MEM_ADDR cleaned up, \
                 but WASM Instance will leak until engine is dropped"
            );
        }
        // Always clean up MEM_ADDR as a safety net (idempotent — engine may have already removed it)
        let _ = native_api::MEM_ADDR.remove(&self.id);
    }
}

pub(crate) struct InstanceInfo {
    pub start_ptr: i64,
    pub mem_size: usize,
    key: Key,
}

impl InstanceInfo {
    pub(crate) fn new(start_ptr: i64, mem_size: usize, key: Key) -> Self {
        Self {
            start_ptr,
            mem_size,
            key,
        }
    }

    pub fn key(&self) -> String {
        match &self.key {
            Key::Contract(k) => k.encode(),
            Key::Delegate(k) => k.encode(),
        }
    }
}

pub(super) enum Key {
    Contract(ContractInstanceId),
    Delegate(DelegateKey),
}

#[derive(thiserror::Error, Debug)]
pub enum ContractExecError {
    #[error(transparent)]
    ContractError(#[from] ContractError),

    #[error("Attempted to perform a put for an already put contract ({0}), use update instead")]
    DoublePut(ContractKey),

    #[error("could not cast array length of {0} to max size (i32::MAX)")]
    InvalidArrayLength(usize),

    #[error("unexpected result from contract interface")]
    UnexpectedResult,

    #[error(
        "The operation ran out of gas. This might be caused by an infinite loop or an inefficient computation."
    )]
    OutOfGas,

    #[error("The operation exceeded the maximum allowed compute time")]
    MaxComputeTimeExceeded,
}

pub struct RuntimeConfig {
    /// Maximum allowed execution time for WASM code in seconds
    pub max_execution_seconds: f64,
    /// Optional override for CPU cycles per second
    pub cpu_cycles_per_second: Option<u64>,
    /// Safety margin for CPU speed variations (0.0 to 1.0)
    pub safety_margin: f64,
    pub enable_metering: bool,
    /// Byte budget for the compiled-WASM **contract** module cache. The
    /// delegate cache is sized to a fraction of this in `RuntimePool::new`
    /// (`DELEGATE_MODULE_CACHE_BUDGET_DIVISOR`). LRU entries are evicted on
    /// insert until the cache's tracked compiled-byte total is within budget.
    /// See [`default_module_cache_budget_bytes`](super::default_module_cache_budget_bytes).
    pub module_cache_budget_bytes: usize,
    /// Production opt-in to offload a cache-miss compile to a blocking thread.
    ///
    /// When `true`, `engine.compile` *may* run the Cranelift compile on a
    /// `spawn_blocking` thread so a cold-contract compile doesn't stall the
    /// current worker's other tasks (issue #4441's whole-node HANG). Whether it
    /// actually offloads is decided from the live runtime flavor inside
    /// `wasmtime_engine::compile_offloaded`: it offloads only on a MULTI-THREAD
    /// runtime and compiles inline under a current_thread / no runtime. So this
    /// flag is a safe opt-in everywhere — it can never panic and stays
    /// deterministic in the `current_thread` + `start_paused` sim runner even
    /// if set. Production sets it `true`; tests/sim leave it `false`.
    pub offload_compilation: bool,
    /// Directory for the wasmtime compile cache (#4683). `Some` relocates the
    /// cache onto the data-dir mount (so it shares the mount that sizes the disk
    /// budget and is measurable as freenet's own usage). `None` keeps wasmtime's
    /// default OS-cache location — every test / `default()` site leaves it
    /// unset, so their behavior is unchanged. An absolute path is required by
    /// wasmtime's `CacheConfig::with_directory`.
    pub wasmtime_cache_dir: Option<std::path::PathBuf>,
    /// Soft size limit (bytes) for the wasmtime compile cache (#4683). `Some`
    /// overrides wasmtime's 512 MiB default via
    /// `CacheConfig::with_files_total_size_soft_limit`; `None` keeps the default.
    pub wasmtime_cache_size_bytes: Option<u64>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_execution_seconds: 5.0,
            cpu_cycles_per_second: None,
            safety_margin: 0.2,
            enable_metering: false,
            module_cache_budget_bytes: super::default_module_cache_budget_bytes(),
            // Default off so that any code path building a `RuntimeConfig`
            // without explicitly opting in (tests, sim) keeps the deterministic
            // inline compile. Production opts in explicitly — see
            // `RuntimePool::new` / `from_config_with_shared_modules`.
            offload_compilation: false,
            // Default: keep wasmtime's own OS-cache location + 512 MiB soft
            // limit. Only the production `from_config*` path relocates + sizes
            // it, so tests and sims see unchanged wasmtime cache behavior.
            wasmtime_cache_dir: None,
            wasmtime_cache_size_bytes: None,
        }
    }
}

/// Callback invoked after a successful state write from a V2 delegate host
/// function (`put_contract_state_sync` or `update_contract_state_sync`).
///
/// V2 delegate writes go through `db.store_state_sync` / `db.update_state_sync`
/// directly and bypass the executor's `state_store.{store,update}` chokepoints
/// where the bump+refresh+report sites live. Without this callback those
/// three side effects never fire on a V2 delegate write, leaving the
/// EvictContract re-host race open AND undercounting StateBytesWritten in
/// the topology meter for that path. The wiring lives outside `wasm_runtime/`
/// (Ring lives in `crates/core/src/ring.rs`) so the callback is plumbed via
/// a trait object owned by `Runtime` to keep `wasm_runtime` independent of
/// the ring.
///
/// The closure SHOULD delegate to `Ring::commit_state_write(key, state_size)`
/// — see `RuntimePool::contract_state_write_callback` for the production
/// wiring. The `state_size` argument is the on-disk byte count of the
/// newly-written state and is fed into the StateBytesWritten meter axis
/// for governance scoring.
pub type StateWriteCallback =
    Arc<dyn Fn(&freenet_stdlib::prelude::ContractKey, usize) + Send + Sync + 'static>;

pub struct Runtime {
    /// The WASM engine backend (wasmtime).
    pub(super) engine: Engine,

    pub(super) secret_store: SecretsStore,
    pub(super) delegate_store: DelegateStore,
    /// LRU cache of compiled delegate modules (shared across pool executors).
    pub(super) delegate_modules: SharedModuleCache<DelegateKey>,
    /// Persisted `ctx.write()` bytes per delegate, shared across pool
    /// executors so a prompt round-trip routed to a different `Runtime` still
    /// sees the pending state. See `native_api::DelegateContextCache`.
    pub(super) delegate_contexts: super::native_api::DelegateContextCache,

    /// Local contract storage.
    pub(crate) contract_store: ContractStore,
    /// LRU cache of compiled contract modules (shared across pool executors).
    pub(super) contract_modules: SharedModuleCache<ContractKey>,

    /// Optional state storage backend for V2 delegate contract access.
    pub(crate) state_store_db: Option<crate::contract::storages::Storage>,

    /// Optional callback invoked after a successful V2 delegate state write,
    /// used to bump the per-contract generation token and refresh the
    /// hosting-cache snapshot from the V2 path (which bypasses the executor
    /// chokepoints). See `StateWriteCallback`.
    pub(crate) state_write_callback: Option<StateWriteCallback>,
}

impl Runtime {
    /// Check if the runtime is in a healthy state and can execute WASM.
    pub fn is_healthy(&self) -> bool {
        self.engine.is_healthy()
    }

    /// Get a clone of the backend engine for sharing with other runtimes.
    pub(crate) fn clone_backend_engine(&self) -> BackendEngine {
        self.engine.clone_backend_engine()
    }

    /// Set the state storage backend for V2 delegate contract access.
    pub fn set_state_store_db(&mut self, db: crate::contract::storages::Storage) {
        self.state_store_db = Some(db);
    }

    /// Install a callback invoked after each successful V2 delegate state
    /// write. See `StateWriteCallback`. Without this, V2 PUT/UPDATE bypass
    /// the executor's bump+refresh chokepoints and the EvictContract
    /// re-host race stays open for V2 delegate writes.
    pub fn set_state_write_callback(&mut self, cb: StateWriteCallback) {
        self.state_write_callback = Some(cb);
    }

    /// Export every secret under `scope` from this runtime's secrets store into
    /// an encrypted [`super::secret_export`] bundle (the live counterpart of the
    /// offline `freenet secrets export` CLI). The bundle is sealed under
    /// `material` so the user can later re-import it with the same key.
    ///
    /// This is the ONLY route to the `pub(super) secret_store` from outside the
    /// `wasm_runtime` module: the executor (`contract::executor`) lives in a
    /// different module tree and cannot touch the field directly, so it wraps
    /// secret access in `Runtime` methods exactly as `register_delegate` /
    /// `inbound_app_message` do. Used by the hosted-mode export endpoint
    /// (P3-live of #4381) to export a single hosted user's per-user secrets.
    ///
    /// Plaintext exists only in the `Zeroizing` buffers inside `export_bundle`;
    /// the returned bytes are encrypted at rest.
    ///
    /// PERFORMANCE / DoS (#4381 P5, addressed): this enumerates AND
    /// AEAD-decrypts EVERY secret in `scope`, synchronously. Two guards keep an
    /// authenticated token-holder from wedging the node with it:
    /// - **Per-user bound**: `export_bundle` rejects (before the heavy work) an
    ///   export exceeding `MAX_EXPORT_SECRET_COUNT` /
    ///   `MAX_EXPORT_TOTAL_PLAINTEXT_BYTES`, bounding the worst-case work.
    /// - **Off-loop execution**: the hosted-export caller
    ///   (`RuntimePool::export_user_secrets`) runs this on a blocking thread
    ///   (`spawn_blocking`, runtime-flavor-gated), so it does NOT stall the
    ///   single-threaded contract-handling loop while it runs.
    ///
    /// Broader per-user rate/quota limiting (repeated exports over time) remains
    /// part of the wider P5 abuse work tracked under #4381.
    pub(crate) fn export_secret_bundle(
        &self,
        scope: super::secrets_store::SecretScope<'_>,
        material: &super::secret_export::BundleKeyMaterial<'_>,
    ) -> Result<Vec<u8>, super::secret_export::ExportError> {
        super::secret_export::export_bundle(&self.secret_store, scope, material)
    }

    /// Import secrets from an encrypted [`super::secret_export`] bundle into this
    /// runtime's secrets store at `target_scope` (the live counterpart of the
    /// offline `freenet secrets import` CLI — but without stopping the node,
    /// P3-live of #4592).
    ///
    /// The MUTATING analogue of [`Self::export_secret_bundle`], and the ONLY
    /// route to the `pub(super) secret_store` for a write from outside the
    /// `wasm_runtime` module (the executor lives in a different module tree and
    /// cannot touch the field directly, so it wraps secret access in `Runtime`
    /// methods exactly as `register_delegate` / `export_secret_bundle` do).
    ///
    /// All-or-nothing on the KEY: [`super::secret_export::import_bundle`] calls
    /// `open_bundle` (which decrypts the WHOLE bundle and authenticates it)
    /// BEFORE any write, so a wrong key / corrupt bundle returns an error with
    /// NOTHING written. Plaintext exists only in the `Zeroizing` buffers inside
    /// `import_bundle`; the re-encrypted-at-rest blobs are written under this
    /// node's per-delegate DEK.
    ///
    /// PERFORMANCE: this decrypts every entry AND writes each to disk (one ReDb
    /// index update + one file per secret), synchronously. The live-import caller
    /// (`RuntimePool::import_secrets`) runs it ON the contract loop (serialized
    /// with delegate `store_secret`) — DELIBERATELY on-loop, because the import
    /// WRITES and the store write path assumes node-wide write serialization
    /// (running it off-loop would let it race another writer on the same secret
    /// file). The loop-block is acceptable: the endpoint is loopback +
    /// dashboard-gated (a one-shot operator migration), not the authenticated-
    /// remote DoS surface that justified moving the read-only EXPORT off-loop.
    pub(crate) fn import_secret_bundle(
        &mut self,
        bundle: &[u8],
        material: &super::secret_export::BundleKeyMaterial<'_>,
        target_scope: &super::secret_export::TargetScope,
        overwrite: bool,
    ) -> Result<super::secret_export::ImportReport, super::secret_export::ExportError> {
        super::secret_export::import_bundle(
            &mut self.secret_store,
            bundle,
            material,
            target_scope,
            overwrite,
        )
    }

    pub fn build_with_config(
        contract_store: ContractStore,
        delegate_store: DelegateStore,
        secret_store: SecretsStore,
        host_mem: bool,
        config: RuntimeConfig,
    ) -> RuntimeResult<Self> {
        let budget = config.module_cache_budget_bytes;

        let engine = Engine::new(&config, host_mem)?;

        Ok(Self {
            engine,

            secret_store,
            delegate_store,
            contract_modules: Arc::new(Mutex::new(ModuleCache::new(budget))),

            contract_store,
            delegate_modules: Arc::new(Mutex::new(ModuleCache::new(budget))),
            delegate_contexts: super::native_api::new_delegate_context_cache(),
            state_store_db: None,
            state_write_callback: None,
        })
    }

    pub fn build(
        contract_store: ContractStore,
        delegate_store: DelegateStore,
        secret_store: SecretsStore,
        host_mem: bool,
    ) -> RuntimeResult<Self> {
        Self::build_with_config(
            contract_store,
            delegate_store,
            secret_store,
            host_mem,
            RuntimeConfig::default(),
        )
    }

    /// Build a runtime that shares compiled module caches AND the backend engine
    /// with other runtimes.
    ///
    /// Used by `RuntimePool` to avoid duplicating compiled WASM modules across
    /// pool executors. Each executor gets its own Store (runtime state:
    /// memories, globals, instances), but all share the same backend engine
    /// (compiler) and module cache.
    ///
    /// # Safety requirement
    ///
    /// All runtimes sharing a module cache MUST use the same backend engine.
    /// Compiled modules store references to the compiling Engine's internal data
    /// structures. Using a Module compiled by one Engine in a Store backed by a
    /// different Engine causes SIGSEGV.
    // Each parameter is a distinct shared resource the pool wires through
    // explicitly; bundling them into a struct just to satisfy the lint
    // would obscure which executor sees which cache.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_with_shared_module_caches(
        contract_store: ContractStore,
        delegate_store: DelegateStore,
        secret_store: SecretsStore,
        host_mem: bool,
        contract_modules: SharedModuleCache<ContractKey>,
        delegate_modules: SharedModuleCache<DelegateKey>,
        delegate_contexts: super::native_api::DelegateContextCache,
        shared_backend: BackendEngine,
        config: &RuntimeConfig,
    ) -> RuntimeResult<Self> {
        // The pre-built `contract_modules`/`delegate_modules` caches carry the
        // byte budget (the pool sizes them in `RuntimePool::new`). `config`
        // here carries `offload_compilation` (and execution/metering knobs)
        // through to the engine — previously this hardcoded
        // `RuntimeConfig::default()`, which left `offload_compilation` dead on
        // the production pool path.
        let engine = Engine::new_with_shared_backend(config, host_mem, shared_backend)?;
        Ok(Self {
            engine,
            secret_store,
            delegate_store,
            contract_modules,
            contract_store,
            delegate_modules,
            delegate_contexts,
            state_store_db: None,
            state_write_callback: None,
        })
    }

    /// Explicitly clean up a running instance from the engine.
    ///
    /// This removes the WASM `Instance` from the engine's HashMap and
    /// the MEM_ADDR entry. Should be called after the instance is no longer
    /// needed (after all WASM calls are complete).
    pub(super) fn drop_running_instance(&mut self, running: &mut RunningInstance) {
        self.engine.drop_instance(&running.handle);
        running.dropped_from_engine = true;
    }

    pub(super) fn init_buf<T>(
        &mut self,
        handle: &InstanceHandle,
        data: T,
    ) -> RuntimeResult<BufferMut<'_>>
    where
        T: AsRef<[u8]>,
    {
        let data = data.as_ref();
        let builder_ptr = self.engine.initiate_buffer(handle, data.len() as u32)?;
        let linear_mem = self.linear_mem(handle)?;
        // SAFETY: `builder_ptr` is returned by the WASM allocator and points to a valid
        // `BufferBuilder` within the instance's linear memory described by `linear_mem`.
        unsafe {
            Ok(BufferMut::from_ptr(
                builder_ptr as *mut BufferBuilder,
                linear_mem,
            ))
        }
    }

    pub(super) fn init_buf_with_capacity(
        &mut self,
        handle: &InstanceHandle,
        capacity: usize,
    ) -> RuntimeResult<BufferMut<'_>> {
        let builder_ptr = self.engine.initiate_buffer(handle, capacity as u32)?;
        let linear_mem = self.linear_mem(handle)?;
        // SAFETY: `builder_ptr` is returned by the WASM allocator and points to a valid
        // `BufferBuilder` within the instance's linear memory described by `linear_mem`.
        unsafe {
            Ok(BufferMut::from_ptr(
                builder_ptr as *mut BufferBuilder,
                linear_mem,
            ))
        }
    }

    /// Write data into a streaming buffer with a `[total_len: u32]` header.
    ///
    /// Allocates a buffer of at most `max_cap` bytes, writes the header and
    /// as much data as fits. If the data exceeds the buffer capacity, the
    /// remainder is stored in `CONTRACT_IO` for on-demand refill.
    pub(super) fn write_streaming_buf(
        &mut self,
        handle: &InstanceHandle,
        instance_id: i64,
        data: &[u8],
        max_cap: usize,
    ) -> RuntimeResult<*mut BufferBuilder> {
        use super::native_api::{CONTRACT_IO, PendingContractData};

        // Header: 4 bytes for total payload length (LE u32)
        let header_size = 4usize;
        debug_assert!(max_cap >= header_size, "max_cap must be >= {header_size}");
        if data.len() > u32::MAX as usize {
            return Err(super::ContractExecError::InvalidArrayLength(data.len()).into());
        }
        let buf_cap = max_cap.min(data.len().saturating_add(header_size));
        let mut buf = self.init_buf_with_capacity(handle, buf_cap)?;

        let total_len = data.len() as u32;
        buf.write(total_len.to_le_bytes())?;

        // Write as much data as fits in the remaining capacity
        let first_chunk_size = data.len().min(buf_cap - header_size);
        buf.write(&data[..first_chunk_size])?;

        let ptr = buf.ptr();

        // Store remainder for the fill callback if data didn't fit
        if first_chunk_size < data.len() {
            CONTRACT_IO.insert(
                (instance_id, ptr as i64),
                PendingContractData {
                    data: data[first_chunk_size..].to_vec(),
                    cursor: 0,
                },
            );
        }

        Ok(ptr)
    }

    /// Write data into a WASM buffer, choosing between the streaming protocol
    /// (for contracts compiled against stdlib >= 0.3.4) and the legacy one-shot
    /// protocol (for older contracts).
    pub(super) fn write_contract_buf(
        &mut self,
        running: &RunningInstance,
        data: &[u8],
        max_cap: usize,
    ) -> RuntimeResult<*mut BufferBuilder> {
        if running.supports_streaming {
            self.write_streaming_buf(&running.handle, running.id, data, max_cap)
        } else {
            let mut buf = self.init_buf(&running.handle, data)?;
            buf.write(data)?;
            Ok(buf.ptr())
        }
    }

    /// Write bincode-serialized data into a WASM buffer, choosing between
    /// streaming and legacy protocols.
    pub(super) fn write_contract_buf_serialized<T: serde::Serialize + ?Sized>(
        &mut self,
        running: &RunningInstance,
        value: &T,
        max_cap: usize,
    ) -> RuntimeResult<*mut BufferBuilder> {
        if running.supports_streaming {
            let serialized = bincode::serialize(value)?;
            self.write_streaming_buf(&running.handle, running.id, &serialized, max_cap)
        } else {
            let size = bincode::serialized_size(value)? as usize;
            let mut buf = self.init_buf_with_capacity(&running.handle, size)?;
            bincode::serialize_into(&mut buf, value)?;
            Ok(buf.ptr())
        }
    }

    pub(super) fn linear_mem(&mut self, handle: &InstanceHandle) -> RuntimeResult<WasmLinearMem> {
        let (ptr, size) = self.engine.memory_info(handle)?;
        // SAFETY: `ptr` and `size` come from the engine's live memory export for this
        // instance, so they describe a valid, allocated linear memory region.
        Ok(unsafe { WasmLinearMem::new(ptr, size as u64) })
    }

    pub(super) fn prepare_contract_call(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters,
        req_bytes: usize,
    ) -> RuntimeResult<RunningInstance> {
        // Check shared cache first. The lock is held only for the duration of
        // the lookup + Module clone (an Arc bump) and is ALWAYS dropped before
        // the compile below — never held across the blocking compile.
        let cached = self.contract_modules.lock().unwrap().get(key).cloned();
        let module = if let Some(module) = cached {
            tracing::debug!(contract = %key, "Module cache hit");
            module
        } else {
            tracing::info!(contract = %key, "Module cache miss — compiling");
            // Cache miss — fetch the code and compile with the lock released so
            // the (potentially multi-hundred-millisecond) Cranelift compile
            // never blocks other executors waiting on the shared cache. When
            // `offload_compilation` is set, `engine.compile` further offloads
            // the compile to a blocking thread so it does not pin the
            // single-threaded contract-handling loop (issue #4441).
            let contract = self
                .contract_store
                .fetch_contract(key, parameters)
                .ok_or_else(|| {
                    tracing::error!(
                        contract = %key,
                        key_code_hash = ?key.code_hash(),
                        phase = "prepare_contract_call_failed",
                        "Contract not found in store during WASM execution"
                    );
                    RuntimeInnerError::ContractNotFound(*key)
                })?;
            let code = match contract {
                ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract_v1)) => {
                    contract_v1.code().data().to_vec()
                }
                ContractContainer::Wasm(_) | _ => unimplemented!(),
            };
            let module = self.engine.compile(&code)?;
            let compiled_size = self.engine.module_compiled_size(&module);
            // Re-check cache: the lock was released before compilation, so
            // another executor may have compiled and cached this contract
            // (the per-hash coalescing mutex in the engine prevents the
            // duplicate Cranelift work, but two distinct misses can still race
            // to this insert). Prefer the already-cached clone if present.
            let mut cache = self.contract_modules.lock().unwrap();
            if let Some(existing) = cache.get(key).cloned() {
                existing
            } else {
                cache.insert(*key, module.clone(), compiled_size);
                module
            }
        };
        RunningInstance::new(
            &mut self.engine,
            &module,
            Key::Contract(*key.id()),
            req_bytes,
        )
    }

    /// Prepare a delegate for execution and detect its API version.
    ///
    /// Returns the running instance and the detected API version (V1 or V2).
    /// V2 is detected by inspecting whether the WASM module imports the
    /// `freenet_delegate_contracts` namespace (async host functions).
    pub(super) fn prepare_delegate_call(
        &mut self,
        params: &Parameters,
        key: &DelegateKey,
        req_bytes: usize,
    ) -> RuntimeResult<(RunningInstance, DelegateApiVersion)> {
        // Lock held only for the lookup + Module clone; always dropped before
        // the compile below (never held across the blocking compile).
        let cached = self.delegate_modules.lock().unwrap().get(key).cloned();
        let module = if let Some(module) = cached {
            tracing::debug!(delegate = %key, "Module cache hit");
            module
        } else {
            tracing::info!(delegate = %key, "Module cache miss — compiling");
            let delegate = self
                .delegate_store
                .fetch_delegate(key, params)
                .ok_or_else(|| RuntimeInnerError::DelegateNotFound(key.clone()))?;
            let code = delegate.code().as_ref().to_vec();
            let module = self.engine.compile(&code)?;
            let compiled_size = self.engine.module_compiled_size(&module);
            // Re-check cache: the lock was released before compilation, so
            // another executor may have compiled and cached this delegate.
            let mut cache = self.delegate_modules.lock().unwrap();
            if let Some(existing) = cache.get(key).cloned() {
                existing
            } else {
                cache.insert(key.clone(), module.clone(), compiled_size);
                module
            }
        };

        let api_version = if self.engine.module_has_async_imports(&module) {
            DelegateApiVersion::V2
        } else {
            DelegateApiVersion::V1
        };

        let running = RunningInstance::new(
            &mut self.engine,
            &module,
            Key::Delegate(key.clone()),
            req_bytes,
        )?;
        Ok((running, api_version))
    }
}

impl super::contract::ContractStoreBridge for Runtime {
    fn code_hash_from_id(&self, id: &ContractInstanceId) -> Option<CodeHash> {
        self.contract_store.code_hash_from_id(id)
    }

    fn fetch_contract_code(
        &self,
        key: &ContractKey,
        params: &Parameters<'_>,
    ) -> Option<ContractContainer> {
        self.contract_store.fetch_contract(key, params)
    }

    fn store_contract(&mut self, contract: ContractContainer) -> Result<(), anyhow::Error> {
        self.contract_store.store_contract(contract)?;
        Ok(())
    }

    fn remove_contract(&mut self, key: &ContractKey) -> Result<(), anyhow::Error> {
        self.contract_store.remove_contract(key)?;
        Ok(())
    }

    fn ensure_key_indexed(&mut self, key: &ContractKey) -> Result<(), anyhow::Error> {
        self.contract_store.ensure_key_indexed(key)?;
        Ok(())
    }
}

impl super::contract::ContractRuntimeBridge for Runtime {}
