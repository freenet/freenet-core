use std::collections::HashMap;

use wasmer::{
    imports, Bytes, ImportObject, Instance, Memory, MemoryType, Module, NativeFunc, Store,
    Universal, LLVM,
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
        let store = Store::new(&Universal::new(LLVM::new()).engine());
        let host_memory = Self::get_host_mem(&store)?;
        let top_level_imports = imports! {
            "locutus" => {
                "mem" =>  host_memory.clone(),
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

    fn copy_data(&mut self, data: &[u8], offset: usize) {
        // SAFETY: this is safe because is guaranteed to live for the duration of self
        // and unique write access is guaranteed by the &mut self reference.
        unsafe {
            let mem = self.host_memory.data_unchecked_mut();
            (&mut mem[offset..data.len()]).copy_from_slice(data);
        }
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
        self.copy_data(value, 0);

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

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use wasmer::wat2wasm;

    use super::*;
    use crate::Contract;

    fn test_dir() -> PathBuf {
        let test_dir = std::env::temp_dir().join("locutus").join("contracts");
        if !test_dir.exists() {
            std::fs::create_dir_all(&test_dir).unwrap();
        }
        test_dir
    }

    fn test_contract() -> Contract {
        const CONTRACTS_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let contracts = PathBuf::from(CONTRACTS_DIR);
        let mut dirs = contracts.ancestors();
        let path = dirs.nth(2).unwrap();
        let contract_path = path
            .join("contracts")
            .join("test_contract")
            .join("test_contract.wasm");
        Contract::try_from(contract_path).expect("contract found")
    }

    #[test]
    fn validate() -> Result<(), Box<dyn std::error::Error>> {
        let wasm_bytes = wat2wasm(r#"
        (module
            (import "locutus" "mem" (memory $locutus_mem 1))
            (type $validate_value_t (func (param i32) (param i32) (result i32)))

            (func $validate_value (type $validate_value_t) (param $ptr i32) (param $len i32) (result i32) 
                                  (local $start i32) (
                set_local $start (i32.const 0)
                (set_local $start (i32.load8_u (get_local $start)))

                i32.const 1
                get_local $start 
                i32.eq return
            ))

            (export "validate_value" (func $validate_value))
        )"#.as_bytes())?;

        let mut store = ContractStore::new(test_dir(), 10_000);
        let contract = Contract::new(wasm_bytes.to_vec());
        let key = contract.key();
        store.store_contract(contract)?;

        let mut runtime = Runtime::build(store).unwrap();
        let not_valid = !runtime.validate_value(&key, &[3])?;
        assert!(not_valid);
        let valid = runtime.validate_value(&key, &[1])?;
        assert!(valid);
        Ok(())
    }

    #[test]
    fn validate_compiled() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = ContractStore::new(test_dir(), 10_000);
        let contract = test_contract();
        let key = contract.key();
        store.store_contract(contract)?;

        let mut runtime = Runtime::build(store).unwrap();
        let is_valid = runtime.validate_value(&key, &[1, 2, 3, 4])?;
        assert!(is_valid);
        Ok(())
    }
}
