use std::collections::HashMap;

use locutus_stdlib::{
    buf::{BufferBuilder, BufferMut},
    prelude::*,
};
use wasmer::{
    imports, Bytes, ImportObject, Instance, Memory, MemoryType, Module, NativeFunc, Store,
};

use crate::{
    contract::WrappedState, contract_store::ContractStore, ContractRuntimeError, RuntimeResult,
};

pub trait RuntimeInterface {
    fn validate_state<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        state: &WrappedState,
    ) -> RuntimeResult<bool>;

    fn validate_delta<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        delta: &StateDelta<'a>,
    ) -> RuntimeResult<bool>;

    fn update_state<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        state: &WrappedState,
        delta: &StateDelta<'a>,
    ) -> RuntimeResult<WrappedState>;

    fn summarize_state<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        state: &WrappedState,
    ) -> RuntimeResult<StateSummary<'a>>;

    fn get_state_delta<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        state: &WrappedState,
        delta_to: &StateSummary<'a>,
    ) -> RuntimeResult<StateDelta<'a>>;
}

#[derive(thiserror::Error, Debug)]
pub enum ExecError {
    #[error("invalid put value")]
    InvalidPutValue,

    #[error("insufficient memory, needed {req} bytes but had {free} bytes")]
    InsufficientMemory { req: usize, free: usize },

    #[error("could not cast array length of {0} to max size (i32::MAX)")]
    InvalidArrayLength(usize),

    #[error("unexpected result from contract interface")]
    UnexpectedResult,

    #[error("Attempted to perform a put for an already put contract ({0}), use update instead")]
    DoublePut(ContractKey),
}

#[derive(Clone)]
pub struct Runtime {
    /// working memory store used by the inner engine
    store: Store,
    /// Local contract storage.
    pub contracts: ContractStore,
    /// loaded modules
    modules: HashMap<ContractKey, Module>,
    // /// includes all the necessary imports to interact with the native runtime environment
    top_level_imports: ImportObject,
    // /// assigned growable host memory
    host_memory: Option<Memory>,
    #[cfg(test)]
    enable_wasi: bool,
}

impl RuntimeInterface for Runtime {
    fn validate_state<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        state: &WrappedState,
    ) -> RuntimeResult<bool> {
        <Self>::validate_state(self, key, parameters, state)
    }

    fn validate_delta<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        delta: &StateDelta<'a>,
    ) -> RuntimeResult<bool> {
        <Self>::validate_delta(self, key, parameters, delta)
    }

    fn update_state<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        state: &WrappedState,
        delta: &StateDelta<'a>,
    ) -> RuntimeResult<WrappedState> {
        <Self>::update_state(self, key, parameters, state, delta)
    }

    fn summarize_state<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        state: &WrappedState,
    ) -> RuntimeResult<StateSummary<'a>> {
        <Self>::summarize_state(self, key, parameters, state)
    }

    fn get_state_delta<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        state: &WrappedState,
        delta_to: &StateSummary<'a>,
    ) -> RuntimeResult<StateDelta<'a>> {
        <Self>::get_state_delta(self, key, parameters, state, delta_to)
    }
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

    fn get_module(&mut self, key: &ContractKey, parameters: &Parameters) -> RuntimeResult<()> {
        let contract = self
            .contracts
            .fetch_contract(key, parameters)
            .ok_or(ContractRuntimeError::ContractNotFound(*key))?;
        let module = Module::new(&self.store, contract.code().data())?;
        self.modules.insert(*key, module);
        Ok(())
    }

    fn init_buf<T>(&self, instance: &Instance, data: T) -> RuntimeResult<BufferMut>
    where
        T: AsRef<[u8]>,
    {
        let data = data.as_ref();
        let initiate_buffer: NativeFunc<u32, i64> =
            instance.exports.get_native_function("initiate_buffer")?;
        let builder_ptr = initiate_buffer.call(data.len() as u32)?;
        let memory = self
            .host_memory
            .as_ref()
            .map(Ok)
            .unwrap_or_else(|| instance.exports.get_memory("memory"))?;
        unsafe {
            Ok(BufferMut::from_ptr(
                builder_ptr as *mut BufferBuilder,
                (memory.data_ptr() as _, memory.data_size()),
            ))
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

    #[cfg(not(test))]
    fn instance_store() -> Store {
        use wasmer::Universal;

        if cfg!(target_arch = "aarch64") {
            use wasmer::Cranelift;

            Store::new(&Universal::new(Cranelift::new()).engine())
        } else {
            use wasmer_compiler_llvm::LLVM;

            Store::new(&Universal::new(LLVM::new()).engine())
        }

        // use wasmer::Dylib;
        // Store::new(&Dylib::headless().engine())
    }

    #[cfg(test)]
    fn instance_store() -> Store {
        use wasmer::{Cranelift, Universal};
        Store::new(&Universal::new(Cranelift::new()).engine())
    }

    fn prepare_call(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters,
        req_bytes: usize,
    ) -> RuntimeResult<Instance> {
        let module = if let Some(module) = self.modules.get(key) {
            module
        } else {
            self.get_module(key, parameters)?;
            self.modules.get(key).unwrap()
        };
        let instance = self.prepare_instance(module)?;
        let memory = self
            .host_memory
            .as_ref()
            .map(Ok)
            .unwrap_or_else(|| instance.exports.get_memory("memory"))?;
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

    /// Verify that the state is valid, given the parameters. This will be used before a peer
    /// caches a new state.
    pub fn validate_state<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        state: &WrappedState,
    ) -> RuntimeResult<bool> {
        let req_bytes = parameters.size() + state.size();
        let instance = self.prepare_call(key, parameters, req_bytes)?;
        let mut param_buf = self.init_buf(&instance, parameters)?;
        param_buf.write(parameters)?;
        let mut state_buf = self.init_buf(&instance, &state)?;
        state_buf.write(state)?;

        let validate_func: NativeFunc<(i64, i64), i32> =
            instance.exports.get_native_function("validate_state")?;
        let is_valid = validate_func.call(param_buf.ptr() as i64, state_buf.ptr() as i64)? != 0;
        Ok(is_valid)
    }

    /// Verify that a delta is valid - at least as much as possible. The goal is to prevent DDoS of
    /// a contract by sending a large number of invalid delta updates. This allows peers
    /// to verify a delta before forwarding it.
    pub fn validate_delta<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        delta: &StateDelta<'a>,
    ) -> RuntimeResult<bool> {
        // todo: if we keep this hot in memory on next calls overwrite the buffer with new delta
        let req_bytes = parameters.size() + delta.size();
        let instance = self.prepare_call(key, parameters, req_bytes)?;
        let mut param_buf = self.init_buf(&instance, &parameters)?;
        param_buf.write(parameters)?;
        let mut delta_buf = self.init_buf(&instance, &delta)?;
        delta_buf.write(delta)?;

        let validate_func: NativeFunc<(i64, i64), i32> =
            instance.exports.get_native_function("validate_delta")?;
        let is_valid = validate_func.call(param_buf.ptr() as i64, delta_buf.ptr() as i64)? != 0;
        Ok(is_valid)
    }

    /// Determine whether this delta is a valid update for this contract. If it is, return the modified state,
    /// else return error.
    ///
    /// The contract must be implemented in a way such that this function call is idempotent:
    /// - If the same `update_state` is applied twice to a value, then the second will be ignored.
    /// - Application of `update_state` is "order invariant", no matter what the order in which the values are
    ///   applied, the resulting value must be exactly the same.
    pub fn update_state<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        state: &WrappedState,
        delta: &StateDelta<'a>,
    ) -> RuntimeResult<WrappedState> {
        // todo: if we keep this hot in memory some things to take into account:
        //       - over subsequent requests state size may change
        //       - the delta may not be necessarily the same size
        let req_bytes = parameters.size() + state.size() + delta.size();
        let instance = self.prepare_call(key, parameters, req_bytes)?;
        let mut param_buf = self.init_buf(&instance, &parameters)?;
        param_buf.write(parameters)?;
        let mut state_buf = self.init_buf(&instance, &state)?;
        state_buf.write(state.clone())?;
        let mut delta_buf = self.init_buf(&instance, &delta)?;
        delta_buf.write(delta)?;

        let validate_func: NativeFunc<(i64, i64, i64), i32> =
            instance.exports.get_native_function("update_state")?;
        let update_res = UpdateResult::try_from(validate_func.call(
            param_buf.ptr() as i64,
            state_buf.ptr() as i64,
            delta_buf.ptr() as i64,
        )?)
        .map_err(|_| ContractRuntimeError::from(ExecError::UnexpectedResult))?;
        match update_res {
            UpdateResult::ValidNoChange => Ok(state.clone()),
            UpdateResult::ValidUpdate => {
                let mut state_buf = state_buf.shared();
                let new_state = state_buf.read_all();
                Ok(WrappedState::new(new_state.to_vec()))
            }
            UpdateResult::Invalid => Err(ExecError::InvalidPutValue.into()),
        }
    }

    /// Generate a concise summary of a state that can be used to create deltas relative to this state.
    ///
    /// This allows flexible and efficient state synchronization between peers.
    pub fn summarize_state<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        state: &WrappedState,
    ) -> RuntimeResult<StateSummary<'static>> {
        let req_bytes = parameters.size() + state.size();
        let instance = self.prepare_call(key, parameters, req_bytes)?;
        let mut param_buf = self.init_buf(&instance, parameters)?;
        param_buf.write(parameters)?;
        let mut state_buf = self.init_buf(&instance, state)?;
        state_buf.write(state)?;

        let summary_func: NativeFunc<(i64, i64), i64> =
            instance.exports.get_native_function("summarize_state")?;
        let res_ptr = summary_func.call(param_buf.ptr() as i64, state_buf.ptr() as i64)?
            as *mut BufferBuilder;
        let memory = self
            .host_memory
            .as_ref()
            .map(Ok)
            .unwrap_or_else(|| instance.exports.get_memory("memory"))?;
        let summary_buf =
            unsafe { BufferMut::from_ptr(res_ptr, (memory.data_ptr() as _, memory.data_size())) };
        let summary: StateSummary = summary_buf.read_bytes(summary_buf.size()).into();
        Ok(StateSummary::from(summary.to_vec()))
    }

    /// Generate a state delta using a summary from the current state.
    /// This along with [`Self::summarize_state`] allows flexible and efficient
    /// state synchronization between peers.
    pub fn get_state_delta<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        state: &WrappedState,
        summary: &StateSummary<'a>,
    ) -> RuntimeResult<StateDelta<'static>> {
        let req_bytes = parameters.size() + state.size() + summary.size();
        let instance = self.prepare_call(key, parameters, req_bytes)?;
        let mut param_buf = self.init_buf(&instance, &parameters)?;
        param_buf.write(parameters)?;
        let mut state_buf = self.init_buf(&instance, &state)?;
        state_buf.write(state.clone())?;
        let mut summary_buf = self.init_buf(&instance, &summary)?;
        summary_buf.write(summary)?;

        let get_state_delta_func: NativeFunc<(i64, i64, i64), i64> =
            instance.exports.get_native_function("get_state_delta")?;
        let res_ptr = get_state_delta_func.call(
            param_buf.ptr() as i64,
            state_buf.ptr() as i64,
            summary_buf.ptr() as i64,
        )? as *mut BufferBuilder;
        let memory = self
            .host_memory
            .as_ref()
            .map(Ok)
            .unwrap_or_else(|| instance.exports.get_memory("memory"))?;
        let delta_buf =
            unsafe { BufferMut::from_ptr(res_ptr, (memory.data_ptr() as _, memory.data_size())) };
        let mut delta = delta_buf.shared();
        Ok(StateDelta::from(delta.read_all().to_vec()))
    }

    /// Updates the current state from the provided summary.
    pub fn update_state_from_summary<'a>(
        &mut self,
        key: &ContractKey,
        parameters: Parameters<'a>,
        current_state: WrappedState,
        current_summary: StateSummary<'a>,
    ) -> RuntimeResult<WrappedState> {
        let req_bytes = parameters.size() + current_state.size() + current_summary.size();
        let instance = self.prepare_call(key, &parameters, req_bytes)?;
        let mut param_buf = self.init_buf(&instance, &parameters)?;
        param_buf.write(parameters)?;
        let mut state_buf = self.init_buf(&instance, &current_state)?;
        state_buf.write(current_state.clone())?;
        let mut summary_buf = self.init_buf(&instance, &current_summary)?;
        summary_buf.write(current_summary)?;

        let validate_func: NativeFunc<(i64, i64, i64), i32> = instance
            .exports
            .get_native_function("update_state_from_summary")?;
        let update_res = UpdateResult::try_from(validate_func.call(
            param_buf.ptr() as i64,
            state_buf.ptr() as i64,
            summary_buf.ptr() as i64,
        )?)
        .map_err(|_| ContractRuntimeError::from(ExecError::UnexpectedResult))?;
        match update_res {
            UpdateResult::ValidNoChange => Ok(current_state),
            UpdateResult::ValidUpdate => {
                let mut state_buf = state_buf.shared();
                let new_state = state_buf.read_all();
                Ok(WrappedState::new(new_state.to_vec()))
            }
            UpdateResult::Invalid => Err(ExecError::InvalidPutValue.into()),
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use crate::contract::WrappedContract;

    use super::*;

    fn test_dir() -> PathBuf {
        let test_dir = std::env::temp_dir().join("locutus").join("contracts");
        if !test_dir.exists() {
            std::fs::create_dir_all(&test_dir).unwrap();
        }
        test_dir
    }

    fn test_contract(contract_path: &str) -> WrappedContract {
        const CONTRACTS_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let contracts = PathBuf::from(CONTRACTS_DIR);
        let mut dirs = contracts.ancestors();
        let path = dirs.nth(2).unwrap();
        let contract_path = path
            .join("contracts")
            .join("test-contract")
            .join(contract_path);
        WrappedContract::try_from((&*contract_path, Parameters::from(vec![])))
            .expect("contract found")
    }

    fn get_guest_test_contract() -> RuntimeResult<(ContractStore, ContractKey)> {
        let mut store = ContractStore::new(test_dir(), 10_000)?;
        // FIXME: Generate required test contract
        let contract = test_contract("test_contract_guest.wasi.wasm");
        let key = *contract.key();
        store.store_contract(contract)?;
        Ok((store, key))
    }

    #[ignore]
    #[test]
    fn update_state() -> Result<(), Box<dyn std::error::Error>> {
        let (store, key) = get_guest_test_contract()?;
        let mut runtime = Runtime::build(store, false).unwrap();
        runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi
        let new_state = runtime.update_state(
            &key,
            &Parameters::from([].as_ref()),
            &WrappedState::new(vec![5, 2, 3]),
            &StateDelta::from([4].as_ref()),
        )?;
        assert_eq!(new_state.as_ref().len(), 4);
        assert!(new_state.as_ref()[3] == 4);
        Ok(())
    }

    #[ignore]
    #[test]
    fn summarize_state() -> Result<(), Box<dyn std::error::Error>> {
        let (store, key) = get_guest_test_contract()?;
        let mut runtime = Runtime::build(store, false).unwrap();
        runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi
        let summary = runtime.summarize_state(
            &key,
            &Parameters::from([].as_ref()),
            &WrappedState::new(vec![5, 2, 3, 4]),
        )?;
        assert!(summary.as_ref().len() == 1);
        assert!(summary.as_ref()[0] == 5);
        Ok(())
    }
}
