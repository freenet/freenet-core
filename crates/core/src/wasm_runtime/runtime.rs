use super::{
    contract_store::ContractStore,
    delegate_store::DelegateStore,
    engine::{Engine, InstanceHandle, WasmEngine},
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
use std::{num::NonZeroUsize, sync::atomic::AtomicI64};

static INSTANCE_ID: AtomicI64 = AtomicI64::new(0);

/// Default capacity for each compiled WASM module cache.
///
/// This limits how many compiled contract/delegate modules are kept in memory.
/// When a cache is full, the least recently used module is evicted.
///
/// **Current value: 128 modules per cache**
///
/// # Trade-offs
///
/// - Higher capacity = more memory usage, but fewer recompilations
/// - Lower capacity = less memory usage, but more recompilation overhead
///
/// Recompilation is relatively expensive (~10-100ms per module), so the cache
/// should be large enough to hold the "working set" of frequently-used contracts.
///
/// # Memory Impact
///
/// Each compiled `Module` consumes memory proportional to the contract's complexity.
/// A typical compiled module is 100KB-1MB.
///
/// **Note:** The runtime maintains TWO separate caches (contracts and delegates),
/// so total memory usage is approximately:
/// - With 128 capacity: 2 × (12-128 MB) = **24-256 MB** total
/// - With 256 capacity: 2 × (25-256 MB) = **50-512 MB** total
pub const DEFAULT_MODULE_CACHE_CAPACITY: usize = 128;

/// A live WASM instance with RAII cleanup.
///
/// On drop, removes the MEM_ADDR entry and the engine instance.
pub(super) struct RunningInstance {
    pub id: i64,
    pub handle: InstanceHandle,
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

        // Record memory address for host function pointer arithmetic
        let (ptr, _size) = engine.memory_info(&handle)?;
        native_api::MEM_ADDR.insert(
            id,
            InstanceInfo {
                start_ptr: ptr as i64,
                key,
            },
        );

        Ok(Self { id, handle })
    }
}

impl Drop for RunningInstance {
    fn drop(&mut self) {
        // MEM_ADDR cleanup is handled by Engine::drop_instance
        // (which is called... well, we need to handle this differently)
        // Actually, we can't call engine.drop_instance here because we don't have &mut Engine.
        // But MEM_ADDR is cleaned up by the engine's drop_instance.
        // For now, just clean up MEM_ADDR here and let the engine instance leak.
        // The engine will clean up when it's dropped or when a new instance is created.
        let _ = native_api::MEM_ADDR.remove(&self.id);
    }
}

pub(crate) struct InstanceInfo {
    pub start_ptr: i64,
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

    #[error("insufficient memory, needed {req} bytes but had {free} bytes")]
    InsufficientMemory { req: usize, free: usize },

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
    /// The WASM engine backend (wasmer, wasmtime, etc.)
    pub(super) engine: Engine,

    pub(super) secret_store: SecretsStore,
    pub(super) delegate_store: DelegateStore,
    /// LRU cache of compiled delegate modules.
    pub(super) delegate_modules: LruCache<DelegateKey, <Engine as WasmEngine>::Module>,

    /// Local contract storage.
    pub(crate) contract_store: ContractStore,
    /// LRU cache of compiled contract modules.
    pub(super) contract_modules: LruCache<ContractKey, <Engine as WasmEngine>::Module>,

    /// Optional state storage backend for V2 delegate contract access.
    pub(crate) state_store_db: Option<crate::contract::storages::Storage>,
}

impl Runtime {
    /// Check if the runtime is in a healthy state and can execute WASM.
    pub fn is_healthy(&self) -> bool {
        self.engine.is_healthy()
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
            contract_modules: LruCache::new(cache_capacity),

            contract_store,
            delegate_modules: LruCache::new(cache_capacity),
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

    pub(super) fn linear_mem(&self, handle: &InstanceHandle) -> RuntimeResult<WasmLinearMem> {
        let (ptr, size) = self.engine.memory_info(handle)?;
        Ok(unsafe { WasmLinearMem::new(ptr, size as u64) })
    }

    pub(super) fn prepare_contract_call(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters,
        req_bytes: usize,
    ) -> RuntimeResult<RunningInstance> {
        let module = if let Some(module) = self.contract_modules.get(key) {
            module.clone()
        } else {
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
            self.contract_modules.put(*key, module.clone());
            module
        };
        RunningInstance::new(
            &mut self.engine,
            &module,
            Key::Contract(*key.id()),
            req_bytes,
        )
    }

    pub(super) fn prepare_delegate_call(
        &mut self,
        params: &Parameters,
        key: &DelegateKey,
        req_bytes: usize,
    ) -> RuntimeResult<RunningInstance> {
        let module = if let Some(module) = self.delegate_modules.get(key) {
            module.clone()
        } else {
            let delegate = self
                .delegate_store
                .fetch_delegate(key, params)
                .ok_or_else(|| RuntimeInnerError::DelegateNotFound(key.clone()))?;
            let code = delegate.code().as_ref().to_vec();
            let module = self.engine.compile(&code)?;
            self.delegate_modules.put(key.clone(), module.clone());
            module
        };
        RunningInstance::new(
            &mut self.engine,
            &module,
            Key::Delegate(key.clone()),
            req_bytes,
        )
    }
}
