use std::collections::HashMap;

use wasmer::{
    imports, Bytes, ImportObject, Instance, Memory, MemoryType, Module, NativeFunc, Store,
    Universal, WasmPtr, LLVM,
};

use crate::{
    ContractKey, ContractRuntimeError, ContractStore, ExecError, RuntimeInterface, RuntimeResult,
};

pub struct Runtime {
    /// working memory store used by the inner engine
    store: Store,
    /// local contract disc store
    contracts: ContractStore,
    /// loaded modules
    modules: HashMap<ContractKey, Module>,
    // /// includes all the necessary imports to interact with the native runtime environment
    top_level_imports: ImportObject,
    // /// assigned growable host memory
    host_memory: Option<Memory>,
    #[cfg(test)]
    enable_wasi: bool,
}

impl Runtime {
    pub fn build(contracts: ContractStore, host_mem: bool) -> Result<Self, ContractRuntimeError> {
        let store = Store::new(&Universal::new(LLVM::new()).engine());

        let (host_memory, top_level_imports) = if host_mem {
            let mem = Self::instance_host_mem(&store)?;
            let imports = imports! {
                "env" => {
                    "memory" =>  mem.clone(),
                }
            };
            (Some(mem), imports)
        } else {
            (None, imports! {})
        };

        Ok(Self {
            store,
            contracts,
            modules: HashMap::new(),
            top_level_imports,
            host_memory,
            #[cfg(test)]
            enable_wasi: false,
        })
    }

    fn instance_host_mem(store: &Store) -> Result<Memory, ContractRuntimeError> {
        // todo: max memory assigned for this runtime
        Ok(Memory::new(store, MemoryType::new(20u32, None, false))?)
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

    fn copy_data(&mut self, memory: &Memory, data: &[u8], offset: usize) {
        // SAFETY: this is safe because is guaranteed to live for the duration of self
        // and unique write access is guaranteed by the &mut self reference.
        unsafe {
            let mem = memory.data_unchecked_mut();
            (&mut mem[offset..data.len()]).copy_from_slice(data);
        }
    }

    #[cfg(not(test))]
    fn prepare_instance(&self, module: &Module) -> RuntimeResult<Instance> {
        Ok(Instance::new(module, &self.top_level_imports)?)
    }

    #[cfg(test)]
    // this fn enables WASI env for debuggability
    fn prepare_instance(&self, module: &Module) -> RuntimeResult<Instance> {
        use wasmer::namespace;
        use wasmer_wasi::WasiState;

        if !self.enable_wasi {
            return Ok(Instance::new(module, &self.top_level_imports)?);
        }
        let mut wasi_env = WasiState::new("locutus").finalize()?;
        let mut imports = wasi_env.import_object(module)?;
        if let Some(mem) = &self.host_memory {
            imports.register("env", namespace!("memory" => mem.clone()));
        }

        let mut namespaces = HashMap::new();
        for (module, name, import) in self.top_level_imports.externs_vec() {
            let namespace: &mut wasmer::Exports = namespaces.entry(module).or_default();
            namespace.insert(name, import);
        }
        for (module, ns) in namespaces {
            imports.register(module, ns);
        }

        let instance = Instance::new(module, &imports)?;
        Ok(instance)
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

        let instance = self.prepare_instance(module)?;
        let memory = self
            .host_memory
            .clone()
            .map(Ok)
            .unwrap_or_else(|| instance.exports.get_memory("memory").map(|m| m.clone()))?;
        let req_pages = Bytes::from(value.len()).try_into().unwrap();
        if memory.size() < req_pages {
            if let Err(err) = memory.grow(req_pages - memory.size()) {
                tracing::error!("wasm runtime failed with memory error: {err}");
                return Err(ExecError::InsufficientMemory {
                    req: (req_pages.0 as usize * wasmer::WASM_PAGE_SIZE) / 1024,
                    free: (memory.size().0 as usize * wasmer::WASM_PAGE_SIZE) / 1024,
                }
                .into());
            }
        }
        self.copy_data(&memory, value, 0);

        let ptr = memory.data_ptr();
        let len = i32::try_from(value.len()).map_err(|_| {
            ContractRuntimeError::ExecError(ExecError::InvalidArrayLength(value.len()))
        })?;
        let validate_func: NativeFunc<(WasmPtr<&[u8]>, i32), u8> =
            instance.exports.get_native_function("validate_value")?;
        eprintln!("passing ptr: ({ptr:p}, {len}) -> {}", ptr as i32);
        let is_valid = validate_func.call(WasmPtr::new(0), len)? != 0;
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

    fn test_contract(contract_path: &str) -> Contract {
        const CONTRACTS_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let contracts = PathBuf::from(CONTRACTS_DIR);
        let mut dirs = contracts.ancestors();
        let path = dirs.nth(2).unwrap();
        let contract_path = path
            .join("contracts")
            .join("test_contract")
            .join(contract_path);
        Contract::try_from(contract_path).expect("contract found")
    }

    const VALIDATE_WAT: &str = r#"
    (module
        <DECL MEM>
        (type $validate_value_t (func (param i32) (param i32) (result i32)))

        (func $validate_value (type $validate_value_t) (param $ptr i32) (param $len i32) (result i32) 
                              (local $start i32) (
            set_local $start (i32.const 0)
            (set_local $start (i32.load8_u (get_local $start)))

            i32.const 3
            get_local $start 
            i32.eq return
        ))

        (export "validate_value" (func $validate_value))
    )"#;

    #[test]
    fn validate_with_host_mem() -> Result<(), Box<dyn std::error::Error>> {
        let module = VALIDATE_WAT.to_owned().replace(
            "<DECL MEM>",
            r#"(import "env" "memory" (memory $locutus_mem 17))"#,
        );
        let wasm_bytes = wat2wasm(module.as_bytes())?;
        let contract = Contract::new(wasm_bytes.to_vec());
        let key = contract.key();
        let mut store = ContractStore::new(test_dir(), 10_000);
        store.store_contract(contract)?;

        let mut runtime = Runtime::build(store.clone(), true).unwrap();
        let not_valid = !runtime.validate_value(&key, &[1])?;
        assert!(not_valid);
        let valid = runtime.validate_value(&key, &[3])?;
        assert!(valid);

        Ok(())
    }

    #[test]
    fn validate_with_program_mem() -> Result<(), Box<dyn std::error::Error>> {
        let module = VALIDATE_WAT.to_owned().replace(
            "<DECL MEM>",
            r#"(memory $locutus_mem (export "memory") 17)"#,
        );
        let wasm_bytes = wat2wasm(module.as_bytes())?;
        let contract = Contract::new(wasm_bytes.to_vec());
        let key = contract.key();
        let mut store = ContractStore::new(test_dir(), 10_000);
        store.store_contract(contract)?;

        let mut runtime = Runtime::build(store.clone(), false).unwrap();
        let not_valid = !runtime.validate_value(&key, &[1])?;
        assert!(not_valid);
        let valid = runtime.validate_value(&key, &[3])?;
        assert!(valid);

        Ok(())
    }

    #[test]
    fn validate_compiled_with_program_mem() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = ContractStore::new(test_dir(), 10_000);
        let contract = test_contract("test_contract_prog.wasm");
        let key = contract.key();
        store.store_contract(contract)?;

        let mut runtime = Runtime::build(store, false).unwrap();
        // runtime.enable_wasi = true; // ENABLE FOR DEBUGGING
        let is_valid = runtime.validate_value(&key, &[1, 2, 3, 4])?;
        assert!(is_valid);
        let not_valid = !runtime.validate_value(&key, &[1, 0, 0, 1])?;
        assert!(not_valid);
        Ok(())
    }

    #[test]
    fn validate_compiled_with_host_mem() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = ContractStore::new(test_dir(), 10_000);
        let contract = test_contract("test_contract_host.wasm");
        let key = contract.key();
        store.store_contract(contract)?;

        let mut runtime = Runtime::build(store, true).unwrap();
        // runtime.enable_wasi = true; // ENABLE FOR DEBUGGING
        let is_valid = runtime.validate_value(&key, &[1, 2, 3, 4])?;
        assert!(is_valid);
        let not_valid = !runtime.validate_value(&key, &[1, 0, 0, 1])?;
        assert!(not_valid);
        Ok(())
    }
}
