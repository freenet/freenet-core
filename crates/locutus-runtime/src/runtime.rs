use std::collections::HashMap;

use wasmer::{
    imports, Bytes, ImportObject, Instance, Memory, MemoryType, Module, NativeFunc, Store,
};

use crate::{
    buffer::{BufferBuilder, BufferMut},
    interface::{Parameters, State, StateDelta, StateSummary, UpdateResult},
    ContractKey, ContractRuntimeError, ContractStore, ExecError, RuntimeResult,
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
        let store = Self::instance_store();
        let (host_memory, top_level_imports) = if host_mem {
            let mem = Self::instance_host_mem(&store)?;
            let imports = imports! {
                "env" => {
                    "memory" =>  mem.clone(),
                },
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

    fn instance_host_mem(store: &Store) -> RuntimeResult<Memory> {
        // todo: max memory assigned for this runtime
        Ok(Memory::new(store, MemoryType::new(20u32, None, false))?)
    }

    fn get_module(&mut self, key: &ContractKey) -> RuntimeResult<()> {
        let contract = self
            .contracts
            .fetch_contract(key)?
            .ok_or(ContractRuntimeError::ContractNotFound(*key))?;
        let module = Module::new(&self.store, contract.data())?;
        self.modules.insert(*key, module);
        Ok(())
    }

    fn init_buf<T>(buf: &Instance, data: T) -> RuntimeResult<BufferMut>
    where
        T: AsRef<[u8]>,
    {
        let data = data.as_ref();
        BufferMut::new(
            buf,
            data.len().try_into().map_err(|_| {
                ContractRuntimeError::ExecError(ExecError::InvalidArrayLength(data.len()))
            })?,
        )
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

    #[cfg(not(test))]
    fn instance_store() -> Store {
        use wasmer::Dylib;
        Store::new(&Dylib::headless().engine())
    }

    #[cfg(test)]
    fn instance_store() -> Store {
        use wasmer::{Cranelift, Universal};
        Store::new(&Universal::new(Cranelift::new()).engine())
    }

    fn prepare_call(&mut self, key: &ContractKey, req_bytes: usize) -> RuntimeResult<Instance> {
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
        let req_pages = Bytes::from(req_bytes).try_into().unwrap();
        if memory.size() < req_pages {
            if let Err(err) = memory.grow(req_pages - memory.size()) {
                tracing::error!("wasm runtime failed with memory error: {err}");
                return Err(ExecError::InsufficientMemory {
                    req: (req_pages.0 as usize * wasmer::WASM_PAGE_SIZE),
                    free: (memory.size().0 as usize * wasmer::WASM_PAGE_SIZE),
                }
                .into());
            }
        }
        Ok(instance)
    }

    /// Determine whether this state is valid for this contract.
    // when do we know we need to validate the whole state
    pub fn validate_state<'a>(
        &mut self,
        key: &ContractKey,
        parameters: Parameters<'a>,
        state: State<'a>,
    ) -> RuntimeResult<bool> {
        let req_bytes = parameters.size() + state.size();
        let instance = self.prepare_call(key, req_bytes)?;
        let mut param_buf = Self::init_buf(&instance, &parameters)?;
        param_buf.write(parameters)?;
        let mut state_buf = Self::init_buf(&instance, &state)?;
        state_buf.write(state)?;

        let validate_func: NativeFunc<(i64, i64), i32> =
            instance.exports.get_native_function("validate_state")?;
        let is_valid =
            validate_func.call(param_buf.builder_ptr as i64, state_buf.builder_ptr as i64)? != 0;
        Ok(is_valid)
    }

    /// Determine whether this delta is valid for this contract.
    // when do we know we need to validate only the delta
    fn validate_delta<'a>(
        &mut self,
        key: &ContractKey,
        parameters: Parameters<'a>,
        delta: crate::interface::StateDelta<'a>,
    ) -> RuntimeResult<bool> {
        // todo: if we keep this hot in memory on next calls overwrite the buffer with new delta
        let req_bytes = parameters.size() + delta.size();
        let instance = self.prepare_call(key, req_bytes)?;
        let mut param_buf = Self::init_buf(&instance, &parameters)?;
        param_buf.write(parameters)?;
        let mut delta_buf = Self::init_buf(&instance, &delta)?;
        delta_buf.write(delta)?;

        let validate_func: NativeFunc<(i64, i64), i32> =
            instance.exports.get_native_function("validate_delta")?;
        let is_valid =
            validate_func.call(param_buf.builder_ptr as i64, delta_buf.builder_ptr as i64)? != 0;
        Ok(is_valid)
    }

    /// Determine whether this delta is a valid update for this contract. If it is, return the modified state,
    /// else return error.
    ///
    /// The contract must be implemented in a way such that this function call is idempotent:
    /// - If the same `update_state` is applied twice to a value, then the second will be ignored.
    /// - Application of `update_state` is "order invariant", no matter what the order in which the values are
    ///   applied, the resulting value must be exactly the same.
    fn update_state<'a>(
        &mut self,
        key: &ContractKey,
        parameters: Parameters<'a>,
        state: State<'a>,
        delta: crate::interface::StateDelta<'a>,
    ) -> RuntimeResult<State<'a>> {
        // todo: if we keep this hot in memory some things to take into account:
        //       - over subsequent requests state size may change
        //       - the delta may not be necessarily the same size
        let req_bytes = parameters.size() + state.size() + delta.size();
        let instance = self.prepare_call(key, req_bytes)?;
        let mut param_buf = Self::init_buf(&instance, &parameters)?;
        param_buf.write(parameters)?;
        let mut state_buf = Self::init_buf(&instance, &state)?;
        state_buf.write(state.clone())?;
        let mut delta_buf = Self::init_buf(&instance, &delta)?;
        delta_buf.write(delta)?;

        let validate_func: NativeFunc<(i64, i64), i32> =
            instance.exports.get_native_function("update_state")?;
        let update_res = UpdateResult::try_from(
            validate_func.call(param_buf.builder_ptr as i64, delta_buf.builder_ptr as i64)?,
        )
        .map_err(|_| ContractRuntimeError::from(ExecError::UnexpectedResult))?;
        match update_res {
            UpdateResult::ValidNoChange => Ok(state),
            UpdateResult::ValidUpdate => {
                let mut state_buf = state_buf.flip_ownership();
                // todo: get diff from buf and only then read and append if necessary
                let new_state = state_buf.read_bytes(state.size());
                Ok(State::from(new_state.to_vec()))
            }
            UpdateResult::Invalid => Err(ExecError::InvalidPutValue.into()),
        }
    }

    /// Used to communicate the current state to other nodes so they can keep track of.
    fn summarize_state<'a>(
        &mut self,
        key: &ContractKey,
        parameters: Parameters<'a>,
        state: State<'a>,
    ) -> RuntimeResult<Vec<u8>> {
        let req_bytes = parameters.size() + state.size();
        let instance = self.prepare_call(key, req_bytes)?;
        let mut param_buf = Self::init_buf(&instance, &parameters)?;
        param_buf.write(parameters)?;
        let mut state_buf = Self::init_buf(&instance, &state)?;
        state_buf.write(state.clone())?;

        let validate_func: NativeFunc<(i64, i64), i64> =
            instance.exports.get_native_function("summarize_state")?;
        let res_ptr = validate_func
            .call(param_buf.builder_ptr as i64, state_buf.builder_ptr as i64)?
            as *mut BufferBuilder;
        let summary_buf = BufferMut::from(res_ptr);
        let summary: StateSummary = summary_buf.read_bytes(summary_buf.size()).into();
        Ok(summary.into())
    }

    /// Used to return a delta to subscribers when there are updates.
    fn get_state_delta<'a>(
        &mut self,
        key: &ContractKey,
        parameters: Parameters<'a>,
        state: State<'a>,
        summary: crate::interface::StateSummary<'a>,
    ) -> RuntimeResult<Vec<u8>> {
        let req_bytes = parameters.size() + state.size() + summary.size();
        let instance = self.prepare_call(key, req_bytes)?;
        let mut param_buf = Self::init_buf(&instance, &parameters)?;
        param_buf.write(parameters)?;
        let mut state_buf = Self::init_buf(&instance, &state)?;
        state_buf.write(state.clone())?;
        let mut summary_buf = Self::init_buf(&instance, &summary)?;
        summary_buf.write(summary)?;

        let get_state_delta_func: NativeFunc<(i64, i64, i64), i64> =
            instance.exports.get_native_function("get_state_delta")?;
        let res_ptr = get_state_delta_func.call(
            param_buf.builder_ptr as i64,
            state_buf.builder_ptr as i64,
            summary_buf.builder_ptr as i64,
        )? as *mut BufferBuilder;
        let delta_buf = BufferMut::from(res_ptr);
        let delta: StateDelta = delta_buf.read_bytes(delta_buf.size()).into();
        Ok(delta.into())
    }

    fn update_state_from_summary<'a>(
        &mut self,
        key: &ContractKey,
        parameters: Parameters<'a>,
        current_state: State<'a>,
        current_summary: StateSummary<'a>,
    ) -> RuntimeResult<State<'a>> {
        let req_bytes = parameters.size() + current_state.size() + current_summary.size();
        let instance = self.prepare_call(key, req_bytes)?;
        let mut param_buf = Self::init_buf(&instance, &parameters)?;
        param_buf.write(parameters)?;
        let mut state_buf = Self::init_buf(&instance, &current_state)?;
        state_buf.write(current_state.clone())?;
        let mut summary_buf = Self::init_buf(&instance, &current_summary)?;
        summary_buf.write(current_summary)?;

        let validate_func: NativeFunc<(i64, i64, i64), i32> = instance
            .exports
            .get_native_function("update_state_from_summary")?;
        let update_res = UpdateResult::try_from(validate_func.call(
            param_buf.builder_ptr as i64,
            state_buf.builder_ptr as i64,
            summary_buf.builder_ptr as i64,
        )?)
        .map_err(|_| ContractRuntimeError::from(ExecError::UnexpectedResult))?;
        match update_res {
            UpdateResult::ValidNoChange => Ok(current_state),
            UpdateResult::ValidUpdate => {
                let mut state_buf = state_buf.flip_ownership();
                let new_state = state_buf.read_bytes(current_state.size());
                Ok(State::from(new_state.to_vec()))
            }
            UpdateResult::Invalid => Err(ExecError::InvalidPutValue.into()),
        }
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
            (return (i32.eq (i32.const 3) (i32.load8_u (i32.const 0))))
        )

        (export "validate_value" (func $validate_value))
    )"#;

    #[test]
    fn validate_with_host_mem() -> Result<(), Box<dyn std::error::Error>> {
        let module = VALIDATE_WAT.to_owned().replace(
            "<DECL MEM>",
            r#"(import "env" "memory" (memory $locutus_mem 20))"#,
        );
        let wasm_bytes = wat2wasm(module.as_bytes())?;
        let contract = Contract::new(wasm_bytes.to_vec());
        let key = contract.key();
        let mut store = ContractStore::new(test_dir(), 10_000);
        store.store_contract(contract)?;

        let mut runtime = Runtime::build(store.clone(), true).unwrap();
        let not_valid = !runtime.validate_state(&key, &[1])?;
        assert!(not_valid);
        let valid = runtime.validate_state(&key, &[3])?;
        assert!(valid);

        Ok(())
    }

    #[test]
    fn validate_with_program_mem() -> Result<(), Box<dyn std::error::Error>> {
        let module = VALIDATE_WAT.to_owned().replace(
            "<DECL MEM>",
            r#"(memory $locutus_mem (export "memory") 20)"#,
        );
        let wasm_bytes = wat2wasm(module.as_bytes())?;
        let contract = Contract::new(wasm_bytes.to_vec());
        let key = contract.key();
        let mut store = ContractStore::new(test_dir(), 10_000);
        store.store_contract(contract)?;

        let mut runtime = Runtime::build(store.clone(), false).unwrap();
        let not_valid = !runtime.validate_state(&key, &[1])?;
        assert!(not_valid);
        let valid = runtime.validate_state(&key, &[3])?;
        assert!(valid);

        Ok(())
    }

    #[test]
    fn validate_compiled_with_guest_mem() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = ContractStore::new(test_dir(), 10_000);
        let contract = test_contract("test_contract_guest.wasm");
        let key = contract.key();
        store.store_contract(contract)?;

        let mut runtime = Runtime::build(store, false).unwrap();
        // runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires buding for wasi
        let is_valid = runtime.validate_state(&key, &[1, 2, 3, 4])?;
        assert!(is_valid);
        let not_valid = !runtime.validate_state(&key, &[1, 0, 0, 1])?;
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
        // runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi
        let is_valid = runtime.validate_state(&key, &[1, 2, 3, 4])?;
        assert!(is_valid);
        let not_valid = !runtime.validate_state(&key, &[1, 0, 0, 1])?;
        assert!(not_valid);
        Ok(())
    }
}
