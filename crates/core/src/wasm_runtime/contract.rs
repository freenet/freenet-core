use std::sync::OnceLock;
use std::time::Duration;

use super::{ContractExecError, RuntimeResult};
use freenet_stdlib::prelude::{
    ContractInterfaceResult, ContractKey, Parameters, RelatedContracts, StateDelta, StateSummary,
    UpdateData, UpdateModification, ValidateResult, WrappedState,
};
use tokio::sync::Semaphore;
use wasmer::{Instance, Store, TypedFunction};

/// Semaphore to limit concurrent WASM operations.
///
/// While tokio's blocking thread pool is bounded (default 512), we use a tighter
/// limit matching CPU count. This prevents overwhelming the system with concurrent
/// WASM executions and provides backpressure to callers.
static WASM_SEMAPHORE: OnceLock<Semaphore> = OnceLock::new();

fn get_wasm_semaphore() -> &'static Semaphore {
    WASM_SEMAPHORE.get_or_init(|| {
        let permits = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);
        tracing::info!(
            target: "freenet::wasm_runtime",
            permits,
            "Initializing WASM execution semaphore"
        );
        Semaphore::new(permits)
    })
}

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

/// Result from WASM execution: (call_result, store)
type WasmCallResult = (Result<i64, wasmer::RuntimeError>, Store);

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

        let wasm_store = self.wasm_store.take().unwrap();
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
        let timeout = Duration::from_secs_f64(self.max_execution_seconds);

        let r = execute_wasm_blocking(
            wasm_store,
            move |store| validate_func.call(store, param_buf_ptr, state_buf_ptr, related_buf_ptr),
            timeout,
            "validate_state",
        );

        let result = process_wasm_result(self, &running.instance, r, "validate_state")?;
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

        let wasm_store = self.wasm_store.take().unwrap();
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
        let timeout = Duration::from_secs_f64(self.max_execution_seconds);

        let r = execute_wasm_blocking(
            wasm_store,
            move |store| {
                update_state_func.call(store, param_buf_ptr, state_buf_ptr, update_data_buf_ptr)
            },
            timeout,
            "update_state",
        );

        let result = process_wasm_result(self, &running.instance, r, "update_state")?;
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

        let wasm_store = self.wasm_store.take().unwrap();
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
        let timeout = Duration::from_secs_f64(self.max_execution_seconds);

        let r = execute_wasm_blocking(
            wasm_store,
            move |store| summary_func.call(store, param_buf_ptr, state_buf_ptr),
            timeout,
            "summarize_state",
        );

        let result = process_wasm_result(self, &running.instance, r, "summarize_state")?;
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

        let wasm_store = self.wasm_store.take().unwrap();
        let get_state_delta_func: TypedFunction<(i64, i64, i64), FfiReturnTy> = match running
            .instance
            .exports
            .get_typed_function(&wasm_store, "get_state_delta")
        {
            Ok(f) => f,
            Err(e) => {
                self.wasm_store = Some(wasm_store);
                return Err(e.into());
            }
        };

        let param_buf_ptr = param_buf_ptr as i64;
        let state_buf_ptr = state_buf_ptr as i64;
        let summary_buf_ptr = summary_buf_ptr as i64;
        let timeout = Duration::from_secs_f64(self.max_execution_seconds);

        let r = execute_wasm_blocking(
            wasm_store,
            move |store| {
                get_state_delta_func.call(store, param_buf_ptr, state_buf_ptr, summary_buf_ptr)
            },
            timeout,
            "get_state_delta",
        );

        let result = process_wasm_result(self, &running.instance, r, "get_state_delta")?;
        let result = unsafe {
            ContractInterfaceResult::from_raw(result, &linear_mem)
                .unwrap_get_state_delta(linear_mem)
                .map_err(Into::<ContractExecError>::into)?
        };

        Ok(result)
    }
}

/// Outcome of WASM execution
enum WasmOutcome {
    /// Successful execution with result and store returned
    Success(WasmCallResult),
    /// Execution timed out - store is lost
    Timeout,
    /// Thread panicked - store is lost
    Panicked(String),
}

/// Execute a WASM function on tokio's blocking thread pool.
///
/// This is the core execution function that:
/// 1. Acquires a semaphore permit to limit concurrent WASM operations
/// 2. Spawns the work onto tokio's blocking thread pool via `spawn_blocking`
/// 3. Applies a timeout to prevent runaway executions
///
/// Using `spawn_blocking` instead of `block_in_place` avoids triggering tokio's
/// worker compensation mechanism, which was the root cause of thread explosion
/// (issue #2381). `spawn_blocking` moves work to a separate bounded thread pool
/// rather than blocking a worker thread.
fn execute_wasm_blocking<F>(
    store: Store,
    wasm_call: F,
    timeout: Duration,
    operation: &str,
) -> WasmOutcome
where
    F: FnOnce(&mut Store) -> Result<i64, wasmer::RuntimeError> + Send + 'static,
{
    // Use block_on to run async code from sync context.
    // This is safe because we're on a blocking thread pool thread, not a tokio worker.
    tokio::runtime::Handle::current()
        .block_on(async { execute_wasm_async(store, wasm_call, timeout, operation).await })
}

/// Async implementation of WASM execution.
async fn execute_wasm_async<F>(
    store: Store,
    wasm_call: F,
    timeout: Duration,
    operation: &str,
) -> WasmOutcome
where
    F: FnOnce(&mut Store) -> Result<i64, wasmer::RuntimeError> + Send + 'static,
{
    // Acquire semaphore permit - this is now proper async, no polling!
    let _permit = match get_wasm_semaphore().acquire().await {
        Ok(permit) => permit,
        Err(_) => {
            // Semaphore closed - shouldn't happen in normal operation
            tracing::error!(
                target: "freenet::wasm_runtime",
                operation,
                "WASM semaphore closed unexpectedly"
            );
            return WasmOutcome::Panicked("WASM semaphore closed".to_string());
        }
    };

    tracing::debug!(
        target: "freenet::wasm_runtime",
        operation,
        "Acquired WASM execution permit"
    );

    // Execute WASM on blocking thread pool with timeout
    let result = tokio::time::timeout(timeout, async {
        tokio::task::spawn_blocking(move || {
            let mut store = store;
            let result = wasm_call(&mut store);
            (result, store)
        })
        .await
    })
    .await;

    match result {
        Ok(Ok((wasm_result, store))) => {
            // Success - WASM completed within timeout
            tracing::debug!(
                target: "freenet::wasm_runtime",
                operation,
                success = wasm_result.is_ok(),
                "WASM execution completed"
            );
            WasmOutcome::Success((wasm_result, store))
        }
        Ok(Err(join_error)) => {
            // spawn_blocking task panicked or was cancelled
            let panic_msg = if join_error.is_panic() {
                format_panic_payload(join_error.into_panic())
            } else {
                "Task cancelled".to_string()
            };
            tracing::error!(
                target: "freenet::wasm_runtime",
                operation,
                error = %panic_msg,
                "WASM execution panicked"
            );
            WasmOutcome::Panicked(panic_msg)
        }
        Err(_elapsed) => {
            // Timeout - the blocking task is still running but we're not waiting
            tracing::warn!(
                target: "freenet::wasm_runtime",
                operation,
                timeout_secs = timeout.as_secs_f64(),
                "WASM execution timed out"
            );
            WasmOutcome::Timeout
        }
    }
}

/// Format a panic payload into a human-readable message.
fn format_panic_payload(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "Unknown panic".to_string()
    }
}

/// Process the WASM execution outcome and restore state.
fn process_wasm_result(
    rt: &mut super::Runtime,
    instance: &Instance,
    outcome: WasmOutcome,
    operation: &str,
) -> RuntimeResult<i64> {
    match outcome {
        WasmOutcome::Success((wasm_result, store)) => {
            // Restore the store for future operations
            rt.wasm_store = Some(store);
            match wasm_result {
                Ok(result) => Ok(result),
                Err(e) => Err(rt.handle_contract_error(e, instance, operation)),
            }
        }
        WasmOutcome::Timeout => {
            // Store is lost - it's still in the running task
            // The Runtime is now in an invalid state for this contract
            Err(ContractExecError::MaxComputeTimeExceeded.into())
        }
        WasmOutcome::Panicked(msg) => {
            // Store is lost due to panic
            Err(anyhow::anyhow!("WASM execution panicked: {}", msg).into())
        }
    }
}
