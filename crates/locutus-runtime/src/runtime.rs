use std::collections::HashMap;

use wasmer::{
    imports, Bytes, Cranelift, ImportObject, Instance, Memory, MemoryType, Module, NativeFunc,
    Store, Universal,
};

use crate::{
    ContractKey, ContractRuntimeError, ContractStore, ExecError, RuntimeInterface, RuntimeResult,
};

pub struct Runtime {
    /// includes all the necessary imports to interact with the native runtime environment
    top_level_imports: ImportObject,
    /// working memory store used by the inner engine
    store: Store,
    /// local contract disc store
    contracts: ContractStore,
    /// loaded modules
    modules: HashMap<ContractKey, Module>,
    /// assigned growable host memory
    host_memory: Memory,
}

impl Runtime {
    pub fn build(contracts: ContractStore) -> Result<Self, ContractRuntimeError> {
        let store = Store::new(&Universal::new(Cranelift::default()).engine());
        let host_memory = Self::get_host_mem(&store)?;
        let top_level_imports = imports! {
            "locutus" => {
                "memory" =>  host_memory.clone(),
            }
        };

        Ok(Self {
            top_level_imports,
            store,
            contracts,
            modules: HashMap::new(),
            host_memory,
        })
    }

    fn get_host_mem(store: &Store) -> Result<Memory, ContractRuntimeError> {
        // todo: max memory assigned for this runtime
        Ok(Memory::new(store, MemoryType::new(1u32, None, false))?)
    }

    fn get_module(&mut self, key: &ContractKey) -> Result<(), ContractRuntimeError> {
        let contract = self
            .contracts
            .fetch_contract(key)?
            .ok_or_else(|| ContractRuntimeError::ContractNotFound(*key))?;
        let module = Module::new(&self.store, contract.data())?;
        self.modules.insert(*key, module);
        Ok(())
    }
}

impl RuntimeInterface for Runtime {
    fn validate_value(&mut self, key: &ContractKey, value: &[u8]) -> RuntimeResult<bool> {
        let module = if let Some(module) = self.modules.get(key) {
            module
        } else {
            self.get_module(key)?;
            self.modules.get(key).unwrap()
        };

        let instance = Instance::new(module, &self.top_level_imports)?;
        let req_pages = Bytes::from(value.len()).try_into().unwrap();
        if self.host_memory.size() < req_pages {
            return Err(ExecError::InsufficientMemory {
                req: (req_pages.0 as usize * wasmer::WASM_PAGE_SIZE) / 1024,
                free: (self.host_memory.size().0 as usize * wasmer::WASM_PAGE_SIZE) / 1024,
            }
            .into());
        }
        {
            let data = unsafe {
                // SAFETY: this is safe because is guaranteed to live for the duration of self
                // and unique write access is guaranteed by the &mut self reference.
                self.host_memory.data_unchecked_mut()
            };
            data.copy_from_slice(value);
        }

        let ptr = self.host_memory.data_ptr() as i32;
        let len = i32::try_from(value.len()).map_err(|_| {
            ContractRuntimeError::ExecError(ExecError::InvalidArrayLength(value.len()))
        })?;
        let validate_func: NativeFunc<(i32, i32), u8> =
            instance.exports.get_native_function("validate_value")?;
        let is_valid = validate_func.call(ptr, len)? != 0;
        Ok(is_valid)
    }

    fn update_value(
        &mut self,
        key: &ContractKey,
        value: &[u8],
        value_update: &[u8],
    ) -> RuntimeResult<Vec<u8>> {
        todo!()
    }

    fn related_contracts(
        &mut self,
        key: &ContractKey,
        value_update: &[u8],
    ) -> RuntimeResult<Vec<ContractKey>> {
        todo!()
    }

    fn extract(
        &mut self,
        key: &ContractKey,
        extractor: Option<&[u8]>,
        value: &[u8],
    ) -> RuntimeResult<Vec<u8>> {
        todo!()
    }
}

#[repr(C)]
pub struct WasmSlice {
    pub ptr: u32,
    pub len: u32,
}

enum ContractResults {}

type UpdateResult = u32;

fn update_value(ptr: u32, len: u32) {
    let value: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, len as usize) };
}
