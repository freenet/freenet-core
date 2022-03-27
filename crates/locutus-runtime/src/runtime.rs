use std::collections::HashMap;

use locutus_stdlib::prelude::*;
use wasmer::{
    imports, Bytes, ImportObject, Instance, Memory, MemoryType, Module, NativeFunc, Store,
};

use crate::{ContractKey, ContractRuntimeError, ContractStore, RuntimeResult};

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
}

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
                Some(memory.data_ptr()),
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
        parameters: Parameters<'a>,
        state: State<'a>,
    ) -> RuntimeResult<bool> {
        let req_bytes = parameters.size() + state.size();
        let instance = self.prepare_call(key, req_bytes)?;
        let mut param_buf = self.init_buf(&instance, &parameters)?;
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
        parameters: Parameters<'a>,
        delta: StateDelta<'a>,
    ) -> RuntimeResult<bool> {
        // todo: if we keep this hot in memory on next calls overwrite the buffer with new delta
        let req_bytes = parameters.size() + delta.size();
        let instance = self.prepare_call(key, req_bytes)?;
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
        parameters: Parameters<'a>,
        state: State<'a>,
        delta: StateDelta<'a>,
    ) -> RuntimeResult<State<'a>> {
        // todo: if we keep this hot in memory some things to take into account:
        //       - over subsequent requests state size may change
        //       - the delta may not be necessarily the same size
        let req_bytes = parameters.size() + state.size() + delta.size();
        let instance = self.prepare_call(key, req_bytes)?;
        let mut param_buf = self.init_buf(&instance, &parameters)?;
        param_buf.write(parameters)?;
        let mut state_buf = self.init_buf(&instance, &state)?;
        state_buf.write(state.clone())?;
        let mut delta_buf = self.init_buf(&instance, &delta)?;
        delta_buf.write(delta)?;

        let validate_func: NativeFunc<(i64, i64), i32> =
            instance.exports.get_native_function("update_state")?;
        let update_res = UpdateResult::try_from(
            validate_func.call(param_buf.ptr() as i64, delta_buf.ptr() as i64)?,
        )
        .map_err(|_| ContractRuntimeError::from(ExecError::UnexpectedResult))?;
        match update_res {
            UpdateResult::ValidNoChange => Ok(state),
            UpdateResult::ValidUpdate => {
                let mut state_buf = state_buf.flip_ownership();
                let new_state = state_buf.read_bytes(state.size());
                Ok(State::from(new_state.to_vec()))
            }
            UpdateResult::Invalid => Err(ExecError::InvalidPutValue.into()),
        }
    }

    /// Generate a concise summary of a state that can be used to create deltas
    /// relative to this state.
    ///
    /// This allows flexible and efficient state synchronization between peers.
    pub fn summarize_state<'a>(
        &mut self,
        key: &ContractKey,
        parameters: Parameters<'a>,
        state: State<'a>,
    ) -> RuntimeResult<StateSummary<'a>> {
        let req_bytes = parameters.size() + state.size();
        let instance = self.prepare_call(key, req_bytes)?;
        let mut param_buf = self.init_buf(&instance, &parameters)?;
        param_buf.write(parameters)?;
        let mut state_buf = self.init_buf(&instance, &state)?;
        state_buf.write(state)?;

        let validate_func: NativeFunc<(i64, i64), i64> =
            instance.exports.get_native_function("summarize_state")?;
        let res_ptr = validate_func.call(param_buf.ptr() as i64, state_buf.ptr() as i64)?
            as *mut BufferBuilder;
        let memory = self
            .host_memory
            .as_ref()
            .map(Ok)
            .unwrap_or_else(|| instance.exports.get_memory("memory"))?;
        let summary_buf = unsafe { BufferMut::from_ptr(res_ptr, Some(memory.data_ptr())) };
        let summary: StateSummary = summary_buf.read_bytes(summary_buf.size()).into();
        Ok(StateSummary::from(summary.to_vec()))
    }

    /// Generate a state delta using a summary from the current state.
    /// This along with [`Self::summarize_state`] allows flexible and efficient
    /// state synchronization between peers.
    pub fn get_state_delta<'a>(
        &mut self,
        key: &ContractKey,
        parameters: Parameters<'a>,
        state: State<'a>,
        summary: StateSummary<'a>,
    ) -> RuntimeResult<StateDelta<'a>> {
        let req_bytes = parameters.size() + state.size() + summary.size();
        let instance = self.prepare_call(key, req_bytes)?;
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
        let delta_buf = unsafe { BufferMut::from_ptr(res_ptr, Some(memory.data_ptr())) };
        let delta = delta_buf.read_bytes(delta_buf.size());
        Ok(StateDelta::from(delta.to_owned()))
    }

    /// Updates the current state from the provided summary.
    pub fn update_state_from_summary<'a>(
        &mut self,
        key: &ContractKey,
        parameters: Parameters<'a>,
        current_state: State<'a>,
        current_summary: StateSummary<'a>,
    ) -> RuntimeResult<State<'a>> {
        let req_bytes = parameters.size() + current_state.size() + current_summary.size();
        let instance = self.prepare_call(key, req_bytes)?;
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
                let mut state_buf = state_buf.flip_ownership();
                let new_state = state_buf.read_bytes(current_state.size());
                Ok(State::from(new_state.to_vec()))
            }
            UpdateResult::Invalid => Err(ExecError::InvalidPutValue.into()),
        }
    }
}
