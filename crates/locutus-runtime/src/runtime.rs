use std::{collections::HashMap, sync::atomic::AtomicI64};

use locutus_stdlib::{
    buf::{BufferBuilder, BufferMut},
    prelude::*,
};
use wasmer::{imports, Bytes, Imports, Instance, Memory, MemoryType, Module, Store, TypedFunction};

use crate::{
    component_store::ComponentStore, contract_store::ContractStore, error::RuntimeInnerError,
    native_api, secrets_store::SecretsStore, RuntimeResult,
};

static INSTANCE_ID: AtomicI64 = AtomicI64::new(0);

pub(crate) struct RunningInstance {
    pub id: i64,
    pub instance: Instance,
}

impl Drop for RunningInstance {
    fn drop(&mut self) {
        let _ = native_api::MEM_ADDR.remove(&self.id);
    }
}

impl RunningInstance {
    fn new(rt: &mut Runtime, instance: Instance) -> RuntimeResult<Self> {
        let memory = rt
            .host_memory
            .as_ref()
            .map(Ok)
            .unwrap_or_else(|| instance.exports.get_memory("memory"))?;
        let set_id: TypedFunction<i64, ()> = instance
            .exports
            .get_typed_function(&rt.wasm_store, "__locutus_set_id")
            .unwrap();
        let id = INSTANCE_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        set_id.call(&mut rt.wasm_store, id).unwrap();
        let ptr = memory.view(&rt.wasm_store).data_ptr() as i64;
        native_api::MEM_ADDR.insert(id, ptr);
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
    pub(crate) wasm_store: Store,
    /// includes all the necessary imports to interact with the native runtime environment
    pub(crate) top_level_imports: Imports,
    /// assigned growable host memory
    pub(crate) host_memory: Option<Memory>,
    #[cfg(test)]
    pub(crate) enable_wasi: bool,

    pub(crate) secret_store: SecretsStore,
    pub(crate) component_store: ComponentStore,
    /// loaded component modules
    pub(crate) component_modules: HashMap<ComponentKey, Module>,

    /// Local contract storage.
    pub contract_store: ContractStore,
    /// loaded contract modules
    pub(crate) contract_modules: HashMap<ContractKey, Module>,
}

impl Runtime {
    pub fn build(
        contract_store: ContractStore,
        component_store: ComponentStore,
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
        native_api::time::prepare_export(&mut store, &mut top_level_imports);

        Ok(Self {
            wasm_store: store,
            top_level_imports,
            host_memory,
            #[cfg(test)]
            enable_wasi: false,

            secret_store,
            component_store,
            contract_modules: HashMap::new(),

            contract_store,
            component_modules: HashMap::new(),
        })
    }

    pub(crate) fn init_buf<T>(&mut self, instance: &Instance, data: T) -> RuntimeResult<BufferMut>
    where
        T: AsRef<[u8]>,
    {
        let data = data.as_ref();
        let initiate_buffer: TypedFunction<u32, i64> = instance
            .exports
            .get_typed_function(&self.wasm_store, "initiate_buffer")?;
        let builder_ptr = initiate_buffer.call(&mut self.wasm_store, data.len() as u32)?;
        let linear_mem = self.linear_mem(instance)?;
        unsafe {
            Ok(BufferMut::from_ptr(
                builder_ptr as *mut BufferBuilder,
                linear_mem,
            ))
        }
    }

    pub(crate) fn linear_mem(&self, instance: &Instance) -> RuntimeResult<WasmLinearMem> {
        let memory = self
            .host_memory
            .as_ref()
            .map(Ok)
            .unwrap_or_else(|| instance.exports.get_memory("memory"))?
            .view(&self.wasm_store);
        Ok(WasmLinearMem {
            start_ptr: memory.data_ptr() as *const _,
            size: memory.data_size(),
        })
    }

    pub(crate) fn prepare_contract_call(
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
                ContractContainer::Wasm(WasmAPIVersion::V1(contract_v1)) => {
                    Module::new(&self.wasm_store, contract_v1.code().data())?
                }
            };
            self.contract_modules.insert(key.clone(), module);
            self.contract_modules.get(key).unwrap()
        }
        .clone();
        let instance = self.prepare_instance(&module)?;
        self.set_instance_mem(req_bytes, &instance)?;
        RunningInstance::new(self, instance)
    }

    pub(crate) fn prepare_component_call(
        &mut self,
        key: &ComponentKey,
        req_bytes: usize,
    ) -> RuntimeResult<RunningInstance> {
        let module = if let Some(module) = self.component_modules.get(key) {
            module
        } else {
            let contract = self
                .component_store
                .fetch_component(key)
                .ok_or_else(|| RuntimeInnerError::ComponentNotFound(key.clone()))?;
            let module = Module::new(&self.wasm_store, contract.as_ref())?;
            self.component_modules.insert(key.clone(), module);
            self.component_modules.get(key).unwrap()
        }
        .clone();
        let instance = self.prepare_instance(&module)?;
        self.set_instance_mem(req_bytes, &instance)?;
        RunningInstance::new(self, instance)
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

    #[cfg(not(test))]
    fn prepare_instance(&mut self, module: &Module) -> RuntimeResult<Instance> {
        Ok(Instance::new(
            &mut self.wasm_store,
            module,
            &self.top_level_imports,
        )?)
    }

    #[cfg(test)]
    // this fn enables WASI env for debuggability
    fn prepare_instance(&mut self, module: &Module) -> RuntimeResult<Instance> {
        use wasmer::namespace;
        use wasmer_wasi::WasiState;

        if !self.enable_wasi {
            return Ok(Instance::new(
                &mut self.wasm_store,
                module,
                &self.top_level_imports,
            )?);
        }
        let mut wasi_env = WasiState::new("locutus").finalize(&mut self.wasm_store)?;
        let mut imports = wasi_env.import_object(&mut self.wasm_store, module)?;
        if let Some(mem) = &self.host_memory {
            imports.register_namespace("env", namespace!("memory" => mem.clone()));
        }

        let mut namespaces = HashMap::new();
        for ((module, name), import) in self.top_level_imports.into_iter() {
            let namespace: &mut wasmer::Exports = namespaces.entry(module).or_default();
            namespace.insert(name, import);
        }
        for (module, ns) in namespaces {
            imports.register_namespace(&module, ns);
        }

        let instance = Instance::new(&mut self.wasm_store, module, &imports)?;
        wasi_env.initialize(&mut self.wasm_store, &instance)?;

        Ok(instance)
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
