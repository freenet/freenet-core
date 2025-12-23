use std::time::Duration;

use super::worker_pool::{get_worker_pool, WasmExecutionError, WasmExecutionHandle};
use super::{ContractExecError, RuntimeResult};
use freenet_stdlib::prelude::{
    ContractInterfaceResult, ContractKey, Parameters, RelatedContracts, StateDelta, StateSummary,
    UpdateData, UpdateModification, ValidateResult, WrappedState,
};
use wasmer::{Instance, TypedFunction};

type FfiReturnTy = i64;

pub(crate) trait ContractRuntimeInterface {
    /// Verify that the state is valid, given the parameters. This will be used before a peer
    /// caches a new state.
    fn validate_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
        related: &RelatedContracts<'_>,
    ) -> RuntimeResult<ValidateResult>;

    /// Determine whether this delta is a valid update for this contract. If it is, return the modified state,
    /// else return error.
    ///
    /// The contract must be implemented in a way such that this function call is idempotent:
    /// - If the same `update_state` is applied twice to a value, then the second will be ignored.
    /// - Application of `update_state` is "order invariant", no matter what the order in which the values are
    ///   applied, the resulting value must be exactly the same.
    fn update_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
        update_data: &[UpdateData<'_>],
    ) -> RuntimeResult<UpdateModification<'static>>;

    /// Generate a concise summary of a state that can be used to create deltas relative to this state.
    ///
    /// This allows flexible and efficient state synchronization between peers.
    fn summarize_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
    ) -> RuntimeResult<StateSummary<'static>>;

    /// Generate a state delta using a summary from the current state.
    /// This along with [`Self::summarize_state`] allows flexible and efficient
    /// state synchronization between peers.
    fn get_state_delta(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
        delta_to: &StateSummary<'_>,
    ) -> RuntimeResult<StateDelta<'static>>;
}

impl ContractRuntimeInterface for super::Runtime {
    fn validate_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
        related: &RelatedContracts<'_>,
    ) -> RuntimeResult<ValidateResult> {
        let req_bytes = parameters.size() + state.size();
        let running = self.prepare_contract_call(key, parameters, req_bytes)?;
        let linear_mem = self.linear_mem(&running.instance)?;

        let param_buf_ptr = {
            let mut param_buf = self.init_buf(&running.instance, parameters)?;
            param_buf.write(parameters)?;
            param_buf.ptr()
        };
        let state_buf_ptr = {
            let mut state_buf = self.init_buf(&running.instance, state)?;
            state_buf.write(state)?;
            state_buf.ptr()
        };
        let related_buf_ptr = {
            let serialized = bincode::serialize(related)?;
            let mut related_buf = self.init_buf(&running.instance, &serialized)?;
            related_buf.write(serialized)?;
            related_buf.ptr()
        };

        let mut wasm_store = self.wasm_store.take().unwrap();
        let validate_func: TypedFunction<(i64, i64, i64), FfiReturnTy> = match running
            .instance
            .exports
            .get_typed_function(&wasm_store, "validate_state")
        {
            Ok(f) => f,
            Err(e) => {
                self.wasm_store = Some(wasm_store);
                return Err(e.into());
            }
        };

        let param_buf_ptr = param_buf_ptr as i64;
        let state_buf_ptr = state_buf_ptr as i64;
        let related_buf_ptr = related_buf_ptr as i64;
        let handle = get_worker_pool().execute(move || {
            let r = validate_func.call(
                &mut wasm_store,
                param_buf_ptr,
                state_buf_ptr,
                related_buf_ptr,
            );
            (r, wasm_store)
        });
        let r = handle_execution_call(handle, self);

        let result = match_err(self, &running.instance, r)?;
        let is_valid = unsafe {
            ContractInterfaceResult::from_raw(result, &linear_mem)
                .unwrap_validate_state_res(linear_mem)
                .map_err(Into::<ContractExecError>::into)?
        };
        Ok(is_valid)
    }

    fn update_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
        update_data: &[UpdateData<'_>],
    ) -> RuntimeResult<UpdateModification<'static>> {
        // todo: if we keep this hot in memory some things to take into account:
        //       - over subsequent requests state size may change
        //       - the delta may not be necessarily the same size
        let req_bytes =
            parameters.size() + state.size() + update_data.iter().map(|e| e.size()).sum::<usize>();
        let running = self.prepare_contract_call(key, parameters, req_bytes)?;
        let linear_mem = self.linear_mem(&running.instance)?;

        let param_buf_ptr = {
            let mut param_buf = self.init_buf(&running.instance, parameters)?;
            param_buf.write(parameters)?;
            param_buf.ptr()
        };
        let state_buf_ptr = {
            let mut state_buf = self.init_buf(&running.instance, state)?;
            state_buf.write(state.clone())?;
            state_buf.ptr()
        };
        let update_data_buf_ptr = {
            let serialized = bincode::serialize(update_data)?;
            let mut update_data_buf = self.init_buf(&running.instance, &serialized)?;
            update_data_buf.write(serialized)?;
            update_data_buf.ptr()
        };

        let mut wasm_store = self.wasm_store.take().unwrap();
        let update_state_func: TypedFunction<(i64, i64, i64), FfiReturnTy> = match running
            .instance
            .exports
            .get_typed_function(&wasm_store, "update_state")
        {
            Ok(f) => f,
            Err(e) => {
                self.wasm_store = Some(wasm_store);
                return Err(e.into());
            }
        };

        let param_buf_ptr = param_buf_ptr as i64;
        let state_buf_ptr = state_buf_ptr as i64;
        let update_data_buf_ptr = update_data_buf_ptr as i64;
        let handle = get_worker_pool().execute(move || {
            let r = update_state_func.call(
                &mut wasm_store,
                param_buf_ptr,
                state_buf_ptr,
                update_data_buf_ptr,
            );
            (r, wasm_store)
        });
        let r = handle_execution_call(handle, self);

        let result = match_err(self, &running.instance, r)?;
        let update_res = unsafe {
            ContractInterfaceResult::from_raw(result, &linear_mem)
                .unwrap_update_state(linear_mem)
                .map_err(Into::<ContractExecError>::into)?
        };

        Ok(update_res)
    }

    fn summarize_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
    ) -> RuntimeResult<StateSummary<'static>> {
        let req_bytes = parameters.size() + state.size();
        let running = self.prepare_contract_call(key, parameters, req_bytes)?;
        let linear_mem = self.linear_mem(&running.instance)?;

        let param_buf_ptr = {
            let mut param_buf = self.init_buf(&running.instance, parameters)?;
            param_buf.write(parameters)?;
            param_buf.ptr()
        };
        let state_buf_ptr = {
            let mut state_buf = self.init_buf(&running.instance, state)?;
            state_buf.write(state.clone())?;
            state_buf.ptr()
        };

        let mut wasm_store = self.wasm_store.take().unwrap();
        let summary_func: TypedFunction<(i64, i64), FfiReturnTy> = match running
            .instance
            .exports
            .get_typed_function(&wasm_store, "summarize_state")
        {
            Ok(f) => f,
            Err(e) => {
                self.wasm_store = Some(wasm_store);
                return Err(e.into());
            }
        };

        let param_buf_ptr = param_buf_ptr as i64;
        let state_buf_ptr = state_buf_ptr as i64;
        let handle = get_worker_pool().execute(move || {
            let r = summary_func.call(&mut wasm_store, param_buf_ptr, state_buf_ptr);
            (r, wasm_store)
        });
        let r = handle_execution_call(handle, self);

        let result = match_err(self, &running.instance, r)?;
        let result = unsafe {
            ContractInterfaceResult::from_raw(result, &linear_mem)
                .unwrap_summarize_state(linear_mem)
                .map_err(Into::<ContractExecError>::into)?
        };

        Ok(result)
    }

    fn get_state_delta<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        state: &WrappedState,
        summary: &StateSummary<'a>,
    ) -> RuntimeResult<StateDelta<'static>> {
        let req_bytes = parameters.size() + state.size() + summary.size();
        let running = self.prepare_contract_call(key, parameters, req_bytes)?;
        let linear_mem = self.linear_mem(&running.instance)?;

        let param_buf_ptr = {
            let mut param_buf = self.init_buf(&running.instance, parameters)?;
            param_buf.write(parameters)?;
            param_buf.ptr()
        };
        let state_buf_ptr = {
            let mut state_buf = self.init_buf(&running.instance, state)?;
            state_buf.write(state.clone())?;
            state_buf.ptr()
        };
        let summary_buf_ptr = {
            let mut summary_buf = self.init_buf(&running.instance, summary)?;
            summary_buf.write(summary)?;
            summary_buf.ptr()
        };

        let mut wasm_store = self.wasm_store.take().unwrap();
        let get_state_delta_func: TypedFunction<(i64, i64, i64), FfiReturnTy> = running
            .instance
            .exports
            .get_typed_function(&wasm_store, "get_state_delta")?;

        let param_buf_ptr = param_buf_ptr as i64;
        let state_buf_ptr = state_buf_ptr as i64;
        let summary_buf_ptr = summary_buf_ptr as i64;
        let handle = get_worker_pool().execute(move || {
            let r = get_state_delta_func.call(
                &mut wasm_store,
                param_buf_ptr,
                state_buf_ptr,
                summary_buf_ptr,
            );
            (r, wasm_store)
        });
        let r = handle_execution_call(handle, self);

        let result = match_err(self, &running.instance, r)?;
        let result = unsafe {
            ContractInterfaceResult::from_raw(result, &linear_mem)
                .unwrap_get_state_delta(linear_mem)
                .map_err(Into::<ContractExecError>::into)?
        };

        Ok(result)
    }
}

/// Handle the execution of a WASM call using the worker pool.
///
/// This function waits for the WASM execution to complete, handling timeout
/// and worker panics. It uses pure synchronous polling to avoid any interaction
/// with tokio's thread pool.
///
/// # Why we use a worker pool
///
/// Using `std::thread::spawn` for each WASM call creates overhead from thread
/// creation/destruction. Using a fixed-size worker pool:
/// 1. Eliminates thread spawning overhead
/// 2. Provides bounded parallelism (pool size = CPU count)
/// 3. Reuses worker threads across many WASM calls
/// 4. Isolates WASM panics from the main thread
fn handle_execution_call(
    handle: WasmExecutionHandle,
    rt: &mut super::Runtime,
) -> Result<i64, Errors> {
    // Calculate timeout based on max_execution_seconds
    let timeout = Duration::from_secs_f64(rt.max_execution_seconds);
    let start = std::time::Instant::now();

    // Poll every 10ms until completion or timeout
    // This is a sync operation - no tokio involvement
    loop {
        if handle.is_finished() {
            break;
        }

        if start.elapsed() >= timeout {
            // Timeout exceeded - the worker is still running but we won't wait anymore
            // Note: The worker will continue executing. When it completes, the result
            // will be dropped since we're no longer waiting for it.
            tracing::warn!(
                timeout_secs = rt.max_execution_seconds,
                elapsed_ms = start.elapsed().as_millis(),
                "WASM execution timed out, abandoning worker result"
            );
            return Err(Errors::MaxComputeTimeExceeded);
        }

        // Sleep a short duration before checking again
        // Using std::thread::sleep to avoid any async runtime involvement
        std::thread::sleep(Duration::from_millis(10));
    }

    // Worker completed - get results
    let (result, store) = handle.join().map_err(|e| match e {
        WasmExecutionError::WorkerPanicked => {
            tracing::error!("WASM worker panicked during execution");
            Errors::Other(anyhow::anyhow!("WASM worker panicked"))
        }
    })?;

    // Return the store to the runtime
    rt.wasm_store = Some(store);

    result.map_err(Errors::Wasmer)
}

fn match_err(
    rt: &mut super::Runtime,
    instance: &Instance,
    r: Result<i64, Errors>,
) -> RuntimeResult<i64> {
    match r {
        Ok(result) => Ok(result),
        Err(Errors::Wasmer(e)) => Err(rt.handle_contract_error(e, instance, "get_state_delta")),
        Err(Errors::MaxComputeTimeExceeded) => {
            Err(ContractExecError::MaxComputeTimeExceeded.into())
        }
        Err(Errors::Other(e)) => Err(e.into()),
    }
}

enum Errors {
    Wasmer(wasmer::RuntimeError),
    MaxComputeTimeExceeded,
    Other(anyhow::Error),
}
