use super::{
    contract_store::ContractStore,
    delegate_api::DelegateApiVersion,
    delegate_store::DelegateStore,
    engine::{BackendEngine, Engine, InstanceHandle, WasmEngine},
    error::RuntimeInnerError,
    native_api,
    secrets_store::SecretsStore,
    RuntimeResult,
};
use freenet_stdlib::{
    memory::{
        buf::{BufferBuilder, BufferMut},
        WasmLinearMem,
    },
    prelude::*,
};
use lru::LruCache;
use std::sync::{Arc, Mutex};
use std::{num::NonZeroUsize, sync::atomic::AtomicI64};

/// A compiled WASM module cache shared across multiple `Runtime` instances.
///
/// Wasmer `Module` wraps `Arc<Artifact>`, so clones are cheap (just an Arc
/// refcount bump). Sharing the cache across the `RuntimePool` avoids compiling
/// and storing the same contract N times (once per pool executor).
pub(crate) type SharedModuleCache<K> = Arc<Mutex<LruCache<K, <Engine as WasmEngine>::Module>>>;

static INSTANCE_ID: AtomicI64 = AtomicI64::new(0);

/// Default capacity for each compiled WASM module cache.
///
/// This limits how many compiled contract/delegate modules are kept in memory.
/// When a cache is full, the least recently used module is evicted.
///
/// **Current value: 1024 modules per cache**
///
/// # Why 1024?
///
/// Wasmer's internal `code_memory: Vec<CodeMemory>` only grows — compiled machine
/// code persists even after a `Module` is dropped. Memory is only freed when the
/// entire `Engine` is dropped. Every eviction-recompilation cycle permanently grows
/// `code_memory`, causing unbounded memory growth proportional to total compilations
/// over the Engine's lifetime (see #2941).
///
/// A capacity of 1024 avoids evictions on production gateways (~92 contracts as of
/// Feb 2026), preventing the eviction-recompilation cycles that drive `code_memory`
/// growth.
///
/// # Memory Impact
///
/// Each compiled `Module` is typically 100KB-1MB. With shared caches (one instance
/// per cache type across all pool executors), actual memory usage is bounded by the
/// number of unique contracts/delegates on the network, not the capacity.
pub const DEFAULT_MODULE_CACHE_CAPACITY: usize = 1024;

/// A live WASM instance with RAII cleanup.
///
/// On drop, removes the MEM_ADDR entry. The WASM `Instance` is cleaned
/// up by calling [`Runtime::drop_running_instance`] after the instance is
/// no longer needed.
pub(super) struct RunningInstance {
    pub id: i64,
    pub handle: InstanceHandle,
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
        native_api::MEM_ADDR.insert(
            id,
            InstanceInfo {
                start_ptr: ptr as i64,
                mem_size: size,
                key,
            },
        );

        Ok(Self {
            id,
            handle,
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

    #[error("The operation ran out of gas. This might be caused by an infinite loop or an inefficient computation.")]
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
    /// Maximum number of compiled modules to keep in each cache.
    pub module_cache_capacity: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_execution_seconds: 5.0,
            cpu_cycles_per_second: None,
            safety_margin: 0.2,
            enable_metering: false,
            module_cache_capacity: DEFAULT_MODULE_CACHE_CAPACITY,
        }
    }
}

pub struct Runtime {
    /// The WASM engine backend (wasmtime).
    pub(super) engine: Engine,

    pub(super) secret_store: SecretsStore,
    pub(super) delegate_store: DelegateStore,
    /// LRU cache of compiled delegate modules (shared across pool executors).
    pub(super) delegate_modules: SharedModuleCache<DelegateKey>,

    /// Local contract storage.
    pub(crate) contract_store: ContractStore,
    /// LRU cache of compiled contract modules (shared across pool executors).
    pub(super) contract_modules: SharedModuleCache<ContractKey>,

    /// Optional state storage backend for V2 delegate contract access.
    pub(crate) state_store_db: Option<crate::contract::storages::Storage>,
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

    pub fn build_with_config(
        contract_store: ContractStore,
        delegate_store: DelegateStore,
        secret_store: SecretsStore,
        host_mem: bool,
        config: RuntimeConfig,
    ) -> RuntimeResult<Self> {
        let cache_capacity =
            NonZeroUsize::new(config.module_cache_capacity).unwrap_or(NonZeroUsize::MIN);

        let engine = Engine::new(&config, host_mem)?;

        Ok(Self {
            engine,

            secret_store,
            delegate_store,
            contract_modules: Arc::new(Mutex::new(LruCache::new(cache_capacity))),

            contract_store,
            delegate_modules: Arc::new(Mutex::new(LruCache::new(cache_capacity))),
            state_store_db: None,
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
    pub(crate) fn build_with_shared_module_caches(
        contract_store: ContractStore,
        delegate_store: DelegateStore,
        secret_store: SecretsStore,
        host_mem: bool,
        contract_modules: SharedModuleCache<ContractKey>,
        delegate_modules: SharedModuleCache<DelegateKey>,
        shared_backend: BackendEngine,
    ) -> RuntimeResult<Self> {
        let engine =
            Engine::new_with_shared_backend(&RuntimeConfig::default(), host_mem, shared_backend)?;
        Ok(Self {
            engine,
            secret_store,
            delegate_store,
            contract_modules,
            contract_store,
            delegate_modules,
            state_store_db: None,
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
        unsafe {
            Ok(BufferMut::from_ptr(
                builder_ptr as *mut BufferBuilder,
                linear_mem,
            ))
        }
    }

    pub(super) fn linear_mem(&mut self, handle: &InstanceHandle) -> RuntimeResult<WasmLinearMem> {
        let (ptr, size) = self.engine.memory_info(handle)?;
        Ok(unsafe { WasmLinearMem::new(ptr, size as u64) })
    }

    pub(super) fn prepare_contract_call(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters,
        req_bytes: usize,
    ) -> RuntimeResult<RunningInstance> {
        // Check shared cache first (lock held briefly for Arc clone)
        let cached = self.contract_modules.lock().unwrap().get(key).cloned();
        let module = if let Some(module) = cached {
            tracing::debug!(contract = %key, "Module cache hit");
            module
        } else {
            tracing::info!(contract = %key, "Module cache miss — compiling");
            // Cache miss — compile outside the lock to avoid blocking other executors
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
                _ => unimplemented!(),
            };
            let module = self.engine.compile(&code)?;
            // Re-check cache: another executor may have compiled this contract
            // while we were blocked on the shared Engine Mutex during compilation.
            let mut cache = self.contract_modules.lock().unwrap();
            if let Some(existing) = cache.get(key).cloned() {
                existing
            } else {
                if let Some((evicted_key, _)) = cache.push(*key, module.clone()) {
                    tracing::warn!(
                        evicted_contract = %evicted_key,
                        cache_capacity = cache.cap().get(),
                        "Module cache eviction. \
                         Consider increasing DEFAULT_MODULE_CACHE_CAPACITY"
                    );
                }
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
            // Re-check cache: another executor may have compiled this delegate
            // while we were blocked on the shared Engine Mutex during compilation.
            let mut cache = self.delegate_modules.lock().unwrap();
            if let Some(existing) = cache.get(key).cloned() {
                existing
            } else {
                if let Some((evicted_key, _)) = cache.push(key.clone(), module.clone()) {
                    tracing::warn!(
                        evicted_delegate = %evicted_key,
                        cache_capacity = cache.cap().get(),
                        "Delegate cache eviction. \
                         Consider increasing DEFAULT_MODULE_CACHE_CAPACITY"
                    );
                }
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
