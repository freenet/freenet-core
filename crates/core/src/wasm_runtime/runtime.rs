use std::{collections::HashMap, sync::atomic::AtomicI64};

use freenet_stdlib::{
    memory::{
        buf::{BufferBuilder, BufferMut},
        WasmLinearMem,
    },
    prelude::*,
};
use wasmer::{imports, Bytes, Imports, Instance, Memory, MemoryType, Module, Store, TypedFunction};

use super::{
    contract_store::ContractStore, delegate_store::DelegateStore, error::RuntimeInnerError,
    native_api, secrets_store::SecretsStore, RuntimeResult,
};

static INSTANCE_ID: AtomicI64 = AtomicI64::new(0);

pub(super) struct RunningInstance {
    pub id: i64,
    pub instance: Instance,
}

impl Drop for RunningInstance {
    fn drop(&mut self) {
        let _ = native_api::MEM_ADDR.remove(&self.id);
    }
}

pub(super) struct InstanceInfo {
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

enum Key {
    Contract(ContractInstanceId),
    Delegate(DelegateKey),
}

impl RunningInstance {
    fn new(rt: &mut Runtime, instance: Instance, key: Key) -> RuntimeResult<Self> {
        let memory = rt
            .host_memory
            .as_ref()
            .map(Ok)
            .unwrap_or_else(|| instance.exports.get_memory("memory"))?;
        let set_id: TypedFunction<i64, ()> = instance
            .exports
            .get_typed_function(&rt.wasm_store, "__frnt_set_id")
            .unwrap();
        let id = INSTANCE_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        set_id.call(&mut rt.wasm_store, id).unwrap();
        let ptr = memory.view(&rt.wasm_store).data_ptr() as i64;
        native_api::MEM_ADDR.insert(
            id,
            InstanceInfo {
                start_ptr: ptr,
                key,
            },
        );
        Ok(Self { instance, id })
    }
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
}

pub struct Runtime {
    /// Working memory store used by the inner engine
    pub(super) wasm_store: Store,
    /// includes all the necessary imports to interact with the native runtime environment
    pub(super) top_level_imports: Imports,
    /// assigned growable host memory
    pub(super) host_memory: Option<Memory>,

    pub(super) secret_store: SecretsStore,
    pub(super) delegate_store: DelegateStore,
    /// loaded delegate modules
    pub(super) delegate_modules: HashMap<DelegateKey, Module>,

    /// Local contract storage.
    pub(crate) contract_store: ContractStore,
    /// loaded contract modules
    pub(super) contract_modules: HashMap<ContractKey, Module>,
}

impl Runtime {
    pub fn build(
        contract_store: ContractStore,
        delegate_store: DelegateStore,
        secret_store: SecretsStore,
        host_mem: bool,
    ) -> RuntimeResult<Self> {
        let mut store = Self::instance_store();
        let (host_memory, mut top_level_imports) = if host_mem {
            let mem = Self::instance_host_mem(&mut store)?;
            let imports = imports! {
                "env" => {
                    "memory" =>  mem.clone(),
                },
            };
            (Some(mem), imports)
        } else {
            (None, imports! {})
        };
        native_api::log::prepare_export(&mut store, &mut top_level_imports);
        native_api::rand::prepare_export(&mut store, &mut top_level_imports);
        native_api::time::prepare_export(&mut store, &mut top_level_imports);

        Ok(Self {
            wasm_store: store,
            top_level_imports,
            host_memory,

            secret_store,
            delegate_store,
            contract_modules: HashMap::new(),

            contract_store,
            delegate_modules: HashMap::new(),
        })
    }

    pub(super) fn init_buf<T>(&mut self, instance: &Instance, data: T) -> RuntimeResult<BufferMut>
    where
        T: AsRef<[u8]>,
    {
        let data = data.as_ref();
        let initiate_buffer: TypedFunction<u32, i64> = instance
            .exports
            .get_typed_function(&self.wasm_store, "__frnt__initiate_buffer")?;
        let builder_ptr = initiate_buffer.call(&mut self.wasm_store, data.len() as u32)?;
        let linear_mem = self.linear_mem(instance)?;
        unsafe {
            Ok(BufferMut::from_ptr(
                builder_ptr as *mut BufferBuilder,
                linear_mem,
            ))
        }
    }

    pub(super) fn linear_mem(&self, instance: &Instance) -> RuntimeResult<WasmLinearMem> {
        let memory = self
            .host_memory
            .as_ref()
            .map(Ok)
            .unwrap_or_else(|| instance.exports.get_memory("memory"))?
            .view(&self.wasm_store);
        Ok(unsafe { WasmLinearMem::new(memory.data_ptr() as *const _, memory.data_size()) })
    }

    pub(super) fn prepare_contract_call(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters,
        req_bytes: usize,
    ) -> RuntimeResult<RunningInstance> {
        let module = if let Some(module) = self.contract_modules.get(key) {
            module
        } else {
            let contract = self
                .contract_store
                .fetch_contract(key, parameters)
                .ok_or_else(|| RuntimeInnerError::ContractNotFound(key.clone()))?;
            let module = match contract {
                ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract_v1)) => {
                    Module::new(&self.wasm_store, contract_v1.code().data())?
                }
                _ => unimplemented!(),
            };
            self.contract_modules.insert(key.clone(), module);
            self.contract_modules.get(key).unwrap()
        }
        .clone();
        let instance = self.prepare_instance(&module)?;
        self.set_instance_mem(req_bytes, &instance)?;
        RunningInstance::new(self, instance, Key::Contract(*key.id()))
    }

    pub(super) fn prepare_delegate_call(
        &mut self,
        params: &Parameters,
        key: &DelegateKey,
        req_bytes: usize,
    ) -> RuntimeResult<RunningInstance> {
        let module = if let Some(module) = self.delegate_modules.get(key) {
            module
        } else {
            let delegate = self
                .delegate_store
                .fetch_delegate(key, params)
                .ok_or_else(|| RuntimeInnerError::DelegateNotFound(key.clone()))?;
            let module = Module::new(&self.wasm_store, delegate.code().as_ref())?;
            self.delegate_modules.insert(key.clone(), module);
            self.delegate_modules.get(key).unwrap()
        }
        .clone();
        let instance = self.prepare_instance(&module)?;
        self.set_instance_mem(req_bytes, &instance)?;
        RunningInstance::new(self, instance, Key::Delegate(key.clone()))
    }

    fn set_instance_mem(&mut self, req_bytes: usize, instance: &Instance) -> RuntimeResult<()> {
        let memory = self
            .host_memory
            .as_ref()
            .map(Ok)
            .unwrap_or_else(|| instance.exports.get_memory("memory"))?;
        let req_pages = Bytes::from(req_bytes).try_into().unwrap();
        if memory.view(&self.wasm_store).size() < req_pages {
            if let Err(err) = memory.grow(&mut self.wasm_store, req_pages) {
                tracing::error!("wasm runtime failed with memory error: {err}");
                return Err(ContractExecError::InsufficientMemory {
                    req: (req_pages.0 as usize * wasmer::WASM_PAGE_SIZE),
                    free: (memory.view(&self.wasm_store).size().0 as usize
                        * wasmer::WASM_PAGE_SIZE),
                }
                .into());
            }
        }
        Ok(())
    }

    fn instance_host_mem(store: &mut Store) -> RuntimeResult<Memory> {
        // todo: max memory assigned for this runtime
        Ok(Memory::new(store, MemoryType::new(20u32, None, false))?)
    }

    fn prepare_instance(&mut self, module: &Module) -> RuntimeResult<Instance> {
        Ok(Instance::new(
            &mut self.wasm_store,
            module,
            &self.top_level_imports,
        )?)
    }

    fn instance_store() -> Store {
        use wasmer::Cranelift;
        Store::new(Cranelift::new())
    }

    // #[cfg(not(test))]
    // fn instance_store() -> Store {
    //     use wasmer::Universal;
    //     if cfg!(target_arch = "aarch64") {
    //         use wasmer::Cranelift;
    //         Store::new(&Universal::new(Cranelift::new()).engine())
    //     } else {
    //         use wasmer_compiler_llvm::LLVM;
    //         Store::new(&Universal::new(LLVM::new()).engine())
    //     }
    //     // use wasmer::Dylib;
    //     // Store::new(&Dylib::headless().engine())
    // }
}
